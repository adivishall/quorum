package kv_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/replication"
)

// Phase 16 (docs/OBSERVABILITY.md): the client API's metrics against what the
// test itself sent and received.

func scrapeKV(t *testing.T, r *metrics.Registry) metrics.Samples {
	t.Helper()
	var b bytes.Buffer
	if err := r.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	ss, err := metrics.Parse(&b)
	if err != nil {
		t.Fatal(err)
	}
	return ss
}

// TestKVMetricsMatchTheResponses: every response a node's front returned is
// counted once, under its operation and status; duplicates as duplicates;
// every forward a follower counted answered was served — once — by a peer;
// and every replica counted the decisions it applied.
func TestKVMetricsMatchTheResponses(t *testing.T) {
	for attempt := 1; ; attempt++ {
		if kvMetricsScenario(t) {
			return
		}
		if attempt == 3 {
			t.Fatal("premise: the group's term changed during every attempt")
		}
		t.Logf("attempt %d: an election ran during the scenario (premise voided); starting over on a fresh cluster", attempt)
	}
}

// kvMetricsScenario runs the scenario once. Its premise is that no election
// runs while it does — the retry of request 5 is a duplicate only if the
// original executed, which a leader change can prevent — read from every
// node's term; it reports false, asserting nothing, if the premise failed.
func kvMetricsScenario(t *testing.T) bool {
	c := startMultiCluster(t, 1)
	regs := map[multiraft.NodeID]*metrics.Registry{}
	for _, id := range c.ids {
		regs[id] = metrics.NewRegistry()
		m := kv.NewMetrics(regs[id])
		c.fronts[id].SetMetrics(m)
		m.Observe(c.store(id, 0), 0)
	}
	ctx := context.Background()
	var leader multiraft.NodeID
	deadline := time.Now().Add(10 * time.Second)
	for leader == "" && time.Now().Before(deadline) {
		for _, id := range c.ids {
			if s := c.fronts[id].Server(0); s != nil && s.Node().Role() == raft.Leader {
				leader = id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if leader == "" {
		t.Fatal("no leader")
	}
	term := c.fronts[leader].Server(0).Node().Term()
	stable := func() bool {
		for _, id := range c.ids {
			if c.fronts[id].Server(0).Node().Term() != term {
				return false
			}
		}
		return true
	}
	var follower multiraft.NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	// The test's own tally of what each front answered.
	type key struct{ op, status string }
	tally := map[multiraft.NodeID]map[key]int{}
	dups := map[multiraft.NodeID]int{}
	via := 0
	send := func(id multiraft.NodeID, req kv.Request) kv.Response {
		t.Helper()
		resp, err := c.fronts[id].Do(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if tally[id] == nil {
			tally[id] = map[key]int{}
		}
		tally[id][key{lower(req.Op.String()), lower(resp.Status.String())}]++
		if resp.Duplicate {
			dups[id]++
		}
		if resp.Via == string(follower) {
			via++
		}
		return resp
	}
	reg := send(leader, kv.Request{Op: kv.ReqRegister, Group: 0})
	if reg.Status != kv.StatusOK {
		t.Fatalf("register: %+v", reg)
	}
	id := reg.ClientID
	const puts = 10
	executed := 0
	for i := 1; i <= puts; i++ {
		resp := send(follower, kv.Request{Op: kv.ReqPut, Group: 0, ClientID: id, RequestID: uint64(i), AckedBelow: 1,
			Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")})
		if resp.Status == kv.StatusOK && !resp.Duplicate {
			executed++
		}
	}
	dup := send(leader, kv.Request{Op: kv.ReqPut, Group: 0, ClientID: id, RequestID: 5, AckedBelow: 1, Key: []byte("k5"), Value: []byte("v")})
	if !stable() {
		return false
	}
	if dup.Status != kv.StatusOK || !dup.Duplicate {
		t.Fatalf("the retry of request 5 was not answered as a duplicate: %+v", dup)
	}
	send(leader, kv.Request{Op: kv.ReqGet, Group: 0, Key: []byte("k1")})
	send(follower, kv.Request{Op: kv.ReqGet, Group: 0, Key: []byte("missing")})

	var answered, served float64
	for _, n := range c.ids {
		ss := scrapeKV(t, regs[n])
		for k, want := range tally[n] {
			if got, _ := ss.Get("dkv_kv_requests_total", "group", "0", "op", k.op, "status", k.status); int(got) != want {
				t.Fatalf("%s: %s %s counted %v, the front answered %d", n, k.op, k.status, got, want)
			}
		}
		total := 0
		for _, v := range tally[n] {
			total += v
		}
		if got := ss.Sum("dkv_kv_requests_total"); int(got) != total {
			t.Fatalf("%s: %v requests counted, the front answered %d", n, got, total)
		}
		if got := ss.Sum("dkv_kv_request_seconds_count"); int(got) != total {
			t.Fatalf("%s: %v requests timed, %d answered", n, got, total)
		}
		if got := ss.Sum("dkv_kv_duplicate_responses_total"); int(got) != dups[n] {
			t.Fatalf("%s: %v duplicates counted, %d returned", n, got, dups[n])
		}
		if got, _ := ss.Get("dkv_kv_inflight_requests"); got != 0 {
			t.Fatalf("%s: %v requests in flight after all were answered", n, got)
		}
		answered += ss.Sum("dkv_kv_forwards_total", "result", "answered")
		served += ss.Sum("dkv_kv_forwarded_requests_total")
	}
	if int(answered) != via || served != answered {
		t.Fatalf("forwards: %v counted answered, %v served; %d responses came back via the follower", answered, served, via)
	}

	// Every replica applied the registration, the executed puts and the
	// duplicate's entry, and counted each decision once.
	for _, n := range c.ids {
		want := map[string]int{"registered": 1, "executed": executed, "duplicate": 1}
		deadline := time.Now().Add(10 * time.Second)
		for {
			ss := scrapeKV(t, regs[n])
			ok := true
			for d, w := range want {
				v, _ := ss.Get("dkv_kv_apply_decisions_total", "group", "0", "decision", d)
				ok = ok && int(v) == w
			}
			if ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: decisions %v, want %v", n, ss.All("dkv_kv_apply_decisions_total"), want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	return true
}

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// TestDecisionCountsSurviveARestore: the store's own ApplyStats reset when a
// snapshot is restored; the observed decision counts do not — they count what
// this process applied.
func TestDecisionCountsSurviveARestore(t *testing.T) {
	r := metrics.NewRegistry()
	m := kv.NewMetrics(r)
	s := kv.NewStore()
	m.Observe(s, 7)
	put := func(i uint64) []byte {
		return kv.Command{Op: kv.OpPut, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}.Encode()
	}
	for i := uint64(1); i <= 3; i++ {
		if err := s.Apply(i, put(i)); err != nil {
			t.Fatal(err)
		}
	}
	idx, data, err := s.EncodeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RestoreSnapshot(idx, data); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Executed != 0 {
		t.Fatalf("premise: the store's own stats reset on restore, got %+v", s.Stats())
	}
	if err := s.Apply(4, put(4)); err != nil {
		t.Fatal(err)
	}
	if v, _ := scrapeKV(t, r).Get("dkv_kv_apply_decisions_total", "group", "7", "decision", "executed"); v != 4 {
		t.Fatalf("executed decisions counted %v across a restore, want 4", v)
	}
}

// TestClientsCannotCreateMetricSeries (audit M7): the group a request names is
// the client's choice. Requests naming a thousand groups this node does not
// host are counted under group="other" — one series per op and status — not
// one series per group id, which let any client grow the node's metrics
// without bound.
func TestClientsCannotCreateMetricSeries(t *testing.T) {
	r := metrics.NewRegistry()
	front := kv.NewFront("n1", nil)
	front.SetMetrics(kv.NewMetrics(r))
	ctx := context.Background()
	for g := 1; g <= 1000; g++ {
		resp, _ := front.Do(ctx, kv.Request{Op: kv.ReqRegister, Group: replication.GroupID(g)})
		if resp.Status != kv.StatusNotLeader {
			t.Fatalf("REGISTER naming unhosted group %d: %+v", g, resp)
		}
	}
	series, other := 0, 0.0
	for _, s := range scrapeKV(t, r) {
		if s.Name != "dkv_kv_requests_total" {
			continue
		}
		series++
		if s.Labels["group"] != "other" {
			t.Fatalf("a series labelled with an unhosted group: %+v", s)
		}
		other += s.Value
	}
	if series != 1 || other != 1000 {
		t.Fatalf("%d series counting %v requests, want 1 counting 1000", series, other)
	}
}
