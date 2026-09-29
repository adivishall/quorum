package integration

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
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
// replica counts their execution; each process reports two connected peers and
// its own CPU and heap. When the leader is SIGKILLed, a survivor counts the
// election it wins.
func TestRealProcessesServeTruthfulMetrics(t *testing.T) {
	for attempt := 1; ; attempt++ {
		if realMetricsScenario(t) {
			return
		}
		if attempt == 3 {
			t.Fatal("premise: an election ran during the writes on three fresh clusters")
		}
		t.Logf("attempt %d: an election ran during the writes (premise voided); starting over on a fresh cluster", attempt)
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

// realMetricsScenario runs the scenario once; it reports false (and asserts
// nothing) if its premise — no election while the writes go through the
// follower — did not hold.
func realMetricsScenario(t *testing.T) bool {
	c, at := newMeteredRCluster(t, 3)
	leader := meteredLeader(t, c, at)
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
			return false
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
			ss, err := scrapeHTTP(t, at[id])
			if err != nil {
				t.Fatal(err)
			}
			if v, _ := ss.Get("dkv_kv_apply_decisions_total", "group", "0", "decision", "executed"); v == puts {
				if p, _ := ss.Get("dkv_transport_peers", "state", "connected"); p != 2 {
					t.Fatalf("%s reports %v connected peers, want 2", id, p)
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
	return true
}
