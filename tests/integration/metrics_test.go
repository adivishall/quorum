package integration

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/metrics"
)

// Phase 16 (docs/OBSERVABILITY.md): real dkvd processes serve /metrics, and
// what they report matches what the test did to them.

// scrapeHTTP fetches and parses one node's /metrics.
func scrapeHTTP(t *testing.T, addr string) (metrics.Samples, error) {
	t.Helper()
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != metrics.ContentType {
		return nil, fmt.Errorf("status %d, content type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	return metrics.Parse(resp.Body)
}

// newMeteredRCluster is newRCluster with every node serving /metrics on a port
// of its own, on every start.
func newMeteredRCluster(t *testing.T, n int) (*rcluster, map[string]string) {
	t.Helper()
	c := &rcluster{
		t: t, bin: buildDkvd(t), addrs: map[string]string{}, kvAddrs: map[string]string{}, dirs: map[string]string{},
		proxies: map[[2]string]*tcpProxy{}, procs: map[string]*dkvNode{},
	}
	metricsAt := map[string]string{}
	root := t.TempDir()
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.addrs[id], c.kvAddrs[id], metricsAt[id] = freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
		c.dirs[id] = filepath.Join(root, id)
	}
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			c.proxies[[2]string{a, b}] = startProxy(t, c.addrs[b])
		}
	}
	for _, id := range c.ids {
		c.startWith(id, "-metrics-listen", metricsAt[id])
	}
	t.Cleanup(c.killAll)
	return c, metricsAt
}

// meteredLeader waits until exactly one running node's role gauge says leader,
// in a check during which no running node's term changed, and returns it.
func meteredLeader(t *testing.T, c *rcluster, at map[string]string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		terms := map[string]float64{}
		var leaders []string
		ok := true
		for _, id := range c.ids {
			if c.procs[id] == nil {
				continue
			}
			ss, err := scrapeHTTP(t, at[id])
			if err != nil {
				ok = false
				break
			}
			terms[id], _ = ss.Get("dkv_raft_term", "group", "0")
			if v, _ := ss.Get("dkv_raft_role", "group", "0", "role", "leader"); v == 1 {
				leaders = append(leaders, id)
			}
		}
		if ok && len(leaders) == 1 {
			stable := true
			for id, term := range terms {
				ss, err := scrapeHTTP(t, at[id])
				now, _ := ss.Get("dkv_raft_term", "group", "0")
				stable = stable && err == nil && now == term
			}
			if stable {
				return leaders[0]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no single leader by the role gauges")
	return ""
}

// TestRealProcessesServeTruthfulMetrics: a three-process -raft group with
// -metrics-listen. Writes sent through a follower are counted once each, at
// the follower's front, as forwarded by it and as served by the leader; every
// replica counts their execution; each process reports exactly the peers its
// own log shows connected, and its own CPU and heap. When the leader is
// SIGKILLed, a survivor counts the election it wins.
func TestRealProcessesServeTruthfulMetrics(t *testing.T) {
	for attempt := 1; ; attempt++ {
		ok, why := realMetricsScenario(t)
		if ok {
			return
		}
		if attempt == 3 {
			t.Fatalf("premise failed on three fresh clusters, last: %s", why)
		}
		t.Logf("attempt %d: %s (premise voided); starting over on a fresh cluster", attempt, why)
	}
}

// termsOf reads every running node's term from its metrics.
func termsOf(t *testing.T, c *rcluster, at map[string]string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, id := range c.ids {
		if c.procs[id] == nil {
			continue
		}
		ss, err := scrapeHTTP(t, at[id])
		if err != nil {
			t.Fatal(err)
		}
		out[id], _ = ss.Get("dkv_raft_term", "group", "0")
	}
	return out
}

// realMetricsScenario runs the scenario once on a settled cluster; it reports
// false, with the reason, and asserts nothing, if a premise did not hold: an
// election while the writes went through the follower, or a link that
// changed around the scrape its connected-peers gauge is compared with.
func realMetricsScenario(t *testing.T) (bool, string) {
	c, at := newMeteredRCluster(t, 3)
	settled, _ := c.waitSettled(20 * time.Second)
	leader := meteredLeader(t, c, at)
	if leader != settled {
		return false, fmt.Sprintf("the role gauges name %s leader; the logs had settled on %s", leader, settled)
	}
	termsBefore := termsOf(t, c, at)
	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	cl, err := kv.Dial(follower, c.kvAddrs[follower])
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	const puts = 20
	ok := 0
	for i := 0; i < puts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := cl.Do(ctx, kv.Request{Op: kv.ReqPut, Group: 0, Key: []byte(fmt.Sprintf("m%d", i)), Value: []byte("v")})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if resp.Status == kv.StatusOK {
			ok++
		}
	}
	if ok != puts {
		t.Fatalf("%d of %d puts through the follower succeeded", ok, puts)
	}
	for id, term := range termsOf(t, c, at) {
		if term != termsBefore[id] {
			return false, fmt.Sprintf("an election ran during the writes (%s: term %v -> %v)", id, termsBefore[id], term)
		}
	}
	scrapes := map[string]metrics.Samples{}
	for _, id := range c.ids {
		ss, err := scrapeHTTP(t, at[id])
		if err != nil {
			t.Fatal(err)
		}
		scrapes[id] = ss
	}
	var counted, forwarded, served float64
	for _, id := range c.ids {
		counted += scrapes[id].Sum("dkv_kv_requests_total", "op", "put", "status", "ok")
		forwarded += scrapes[id].Sum("dkv_kv_forwards_total", "result", "answered")
		served += scrapes[id].Sum("dkv_kv_forwarded_requests_total", "op", "put", "status", "ok")
	}
	if counted != puts || forwarded != puts || served != puts {
		t.Fatalf("%d puts through %s: requests counted %v, forwards answered %v, forwarded served %v", puts, follower, counted, forwarded, served)
	}
	if v, _ := scrapes[follower].Get("dkv_kv_requests_total", "group", "0", "op", "put", "status", "ok"); v != puts {
		t.Fatalf("the follower's front counted %v of the %d puts it answered", v, puts)
	}
	for _, id := range c.ids {
		deadline := time.Now().Add(10 * time.Second)
		for {
			// The connected-peers gauge is compared with the node's own log,
			// read on both sides of the scrape: a link that came or went
			// meanwhile (the transport tears down a link idle for 120 ticks
			// and redials it) leaves the comparison undefined.
			before := c.connectedPeers(id)
			ss, err := scrapeHTTP(t, at[id])
			if err != nil {
				t.Fatal(err)
			}
			if v, _ := ss.Get("dkv_kv_apply_decisions_total", "group", "0", "decision", "executed"); v == puts {
				after := c.connectedPeers(id)
				if !maps.Equal(before, after) {
					return false, fmt.Sprintf("%s's connections changed around the scrape: %v -> %v", id, before, after)
				}
				if p, _ := ss.Get("dkv_transport_peers", "state", "connected"); p != float64(len(after)) {
					t.Fatalf("%s reports %v connected peers; its log shows %d connected (%v)", id, p, len(after), after)
				}
				if cpu, _ := ss.Get("process_cpu_seconds_total"); cpu <= 0 {
					t.Fatalf("%s reports %v CPU seconds", id, cpu)
				}
				if h, _ := ss.Get("go_heap_inuse_bytes"); h <= 0 {
					t.Fatalf("%s reports %v heap bytes", id, h)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s applied executed %v, want %d", id, ss.All("dkv_kv_apply_decisions_total"), puts)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	// A new leader after a SIGKILL: the winner is a survivor, which counts the
	// election it won beyond what it had won before.
	wonBefore := map[string]float64{}
	for _, id := range c.ids {
		wonBefore[id] = scrapes[id].Sum("dkv_raft_elections_won_total", "group", "0")
	}
	c.kill(leader)
	next := meteredLeader(t, c, at)
	ss, err := scrapeHTTP(t, at[next])
	if err != nil {
		t.Fatal(err)
	}
	if won := ss.Sum("dkv_raft_elections_won_total", "group", "0"); won < wonBefore[next]+1 {
		t.Fatalf("%s leads after %s was killed, but its elections won went %v -> %v", next, leader, wonBefore[next], won)
	}
	return true, ""
}

// TestSettledStartWaitsForEveryLink: the settled-cluster premise is real. Two
// of three nodes elect a leader while the third is not running; once it
// starts, waitSettled returns only when every node follows one leader in the
// highest term seen and every pair of processes has logged a connection —
// including the pair of followers, which Raft never makes talk to each other.
// Whether the late node forced an election on its way in is logged, not
// asserted: that is the liveness hazard PreVote would remove.
func TestSettledStartWaitsForEveryLink(t *testing.T) {
	c := newRCluster(t, 3)
	c.kill("n3")
	first, firstTerm := c.waitLeader([]string{"n1", "n2"}, 0, 20*time.Second)
	c.start("n3")
	leader, term := c.waitSettled(20 * time.Second)
	for _, id := range c.ids {
		if conns := c.connectedPeers(id); len(conns) != 2 || conns[id] {
			t.Fatalf("%s is connected to %v after the cluster settled", id, conns)
		}
		if id != leader && !strings.Contains(c.procs[id].out.String(), fmt.Sprintf("event=raft_follower node=%s term=%d leader=%s", id, term, leader)) {
			t.Fatalf("%s does not follow %s in term %d after the cluster settled\n%s", id, leader, term, c.outputs())
		}
	}
	if leader != first || term != firstTerm {
		t.Logf("the late node forced an election: %s led term %d before it started; the cluster settled on %s at term %d", first, firstTerm, leader, term)
	}
}
