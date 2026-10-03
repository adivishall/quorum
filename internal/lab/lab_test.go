package lab

import (
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/netproxy"
)

// TestSummarize: median of odd and even counts, spread, and p95 only once
// there are enough runs to have one.
func TestSummarize(t *testing.T) {
	s := Summarize([]float64{30, 10, 20})
	if s.N != 3 || s.Median != 20 || s.Min != 10 || s.Max != 30 || s.Spread != 1 || s.P95 != 0 {
		t.Fatalf("%+v", s)
	}
	if s := Summarize([]float64{1, 2, 3, 4}); s.Median != 2.5 {
		t.Fatalf("even count: %+v", s)
	}
	var many []float64
	for i := 1; i <= 20; i++ {
		many = append(many, float64(i))
	}
	if s := Summarize(many); s.P95 != 19 {
		t.Fatalf("p95 of 1..20: %+v", s)
	}
	if s := Summarize(nil); s != (Stat{}) {
		t.Fatalf("empty: %+v", s)
	}
}

// TestScenariosAndSuite: every scenario builds; actions fall inside the run;
// the report suite covers the matrix its documentation names.
func TestScenariosAndSuite(t *testing.T) {
	base := Experiment{Cluster: ClusterConfig{Mode: "raft", Nodes: 3}, Load: load.Config{Duration: 20 * time.Second, Warmup: 3 * time.Second}}
	for _, name := range []string{"steady", "leader-kill", "rolling-restart", "membership", "snapshots"} {
		e, err := Scenario(name, base)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range e.Actions {
			if a.At <= base.Load.Warmup || a.At >= base.Load.Warmup+base.Load.Duration {
				t.Fatalf("%s: action at %s is outside the measured window", name, a.At)
			}
		}
		if name == "membership" && e.Cluster.Spares != 1 {
			t.Fatal("membership has no spare to add")
		}
		if name == "snapshots" && e.Cluster.SnapshotEvery == 0 {
			t.Fatal("the snapshots scenario takes no snapshots")
		}
	}
	if _, err := Scenario("nonsense", base); err == nil {
		t.Fatal("an unknown scenario was accepted")
	}
	exps, err := Suite("report", base, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(exps) != 9+3+5 {
		t.Fatalf("%d experiments in the report suite", len(exps))
	}
	names := map[string]bool{}
	for _, e := range exps {
		if names[e.Name] {
			t.Fatalf("two experiments named %s", e.Name)
		}
		names[e.Name] = true
	}
}

// TestUsageFromScrapes: a node's resource use is the difference between two
// of its scrapes — or the later scrape alone when the node restarted between
// them (its counters started again from zero).
func TestUsageFromScrapes(t *testing.T) {
	before := metrics.Samples{{Name: "process_cpu_seconds_total", Labels: map[string]string{}, Value: 2},
		{Name: "dkv_raft_persist_seconds_count", Labels: map[string]string{"group": "0"}, Value: 10},
		{Name: "dkv_raft_persist_seconds_sum", Labels: map[string]string{"group": "0"}, Value: 0.01}}
	after := metrics.Samples{{Name: "process_cpu_seconds_total", Labels: map[string]string{}, Value: 6},
		{Name: "dkv_raft_persist_seconds_count", Labels: map[string]string{"group": "0"}, Value: 110},
		{Name: "dkv_raft_persist_seconds_sum", Labels: map[string]string{"group": "0"}, Value: 0.21},
		{Name: "dkv_raft_snapshots_created_total", Labels: map[string]string{"group": "0", "trigger": "periodic"}, Value: 4},
		{Name: "dkv_raft_snapshots_created_total", Labels: map[string]string{"group": "1", "trigger": "periodic"}, Value: 2}}
	for _, ss := range []*metrics.Samples{&before, &after} {
		commit, frames, votes := 40.0, 7.0, 1000.0
		if ss == &after {
			commit, frames, votes = 90, 57, 1500
		}
		*ss = append(*ss, metrics.Sample{Name: "dkv_raft_commit_index", Labels: map[string]string{"group": "0"}, Value: commit},
			metrics.Sample{Name: "dkv_transport_frames_sent_total", Labels: map[string]string{"kind": "append_entries"}, Value: frames},
			metrics.Sample{Name: "dkv_transport_frames_sent_total", Labels: map[string]string{"kind": "request_vote"}, Value: votes})
	}
	u := usage(before, after, 2*time.Second, false)
	if u.CPUPerSec != 2 || u.Persists != 100 || u.PersistMeanUs < 1999 || u.PersistMeanUs > 2001 || u.SnapshotsCreated != 6 {
		t.Fatalf("%+v", u)
	}
	if u.CommitAdvance != 50 || u.AppendFramesSent != 50 {
		t.Fatalf("commit advance %v, append frames %v; want 50 and 50 (other kinds excluded)", u.CommitAdvance, u.AppendFramesSent)
	}
	u = usage(before, after, 2*time.Second, true)
	if u.CPUPerSec != 3 || u.Persists != 110 || u.CommitAdvance != 50 {
		t.Fatalf("restarted: %+v (the commit index is durable: its advance is a difference even across a restart)", u)
	}
}

// TestLinksFollowTheDialer: with Links, the dialing side of each pair (the
// smaller id, ADR-014) reaches the other through the pair's proxy and the
// accepting side keeps the real address it never dials; Cut and Heal name a
// link in either order; Isolate cuts exactly the links touching its node;
// Cuts lists them sorted; HealAll restores everything; a cluster without
// Links refuses to partition.
func TestLinksFollowTheDialer(t *testing.T) {
	c := &Cluster{byID: map[string]*Node{}, links: map[[2]string]*netproxy.Proxy{}}
	for _, id := range []string{"n1", "n2", "n3"} {
		n := &Node{ID: id, Addr: "127.0.0.1:1" + id[1:]}
		c.nodes = append(c.nodes, n)
		c.byID[id] = n
	}
	for _, k := range [][2]string{{"n1", "n2"}, {"n1", "n3"}, {"n2", "n3"}} {
		p, err := netproxy.Start(c.byID[k[1]].Addr)
		if err != nil {
			t.Fatal(err)
		}
		c.links[k] = p
	}
	defer c.Close()
	if got := c.peerAddr("n1", c.byID["n2"]); got != c.links[[2]string{"n1", "n2"}].Addr() {
		t.Fatalf("n1 dials n2 at %s, not through the link's proxy", got)
	}
	if got := c.peerAddr("n2", c.byID["n1"]); got != c.byID["n1"].Addr {
		t.Fatalf("n2's entry for n1 is %s, want n1's own address (never dialed)", got)
	}
	if err := c.Cut("n3", "n2"); err != nil {
		t.Fatal(err)
	}
	if got := c.Cuts(); len(got) != 1 || got[0] != [2]string{"n2", "n3"} {
		t.Fatalf("cuts after Cut(n3, n2): %v", got)
	}
	if err := c.Heal("n2", "n3"); err != nil || len(c.Cuts()) != 0 {
		t.Fatalf("heal: %v, cuts %v", err, c.Cuts())
	}
	if err := c.Isolate("n2"); err != nil {
		t.Fatal(err)
	}
	if got := c.Cuts(); len(got) != 2 || got[0] != [2]string{"n1", "n2"} || got[1] != [2]string{"n2", "n3"} {
		t.Fatalf("cuts after Isolate(n2): %v", got)
	}
	c.HealAll()
	if got := c.Cuts(); len(got) != 0 {
		t.Fatalf("cuts after HealAll: %v", got)
	}
	if err := c.Cut("n1", "n9"); err == nil {
		t.Fatal("a link to a node that does not exist was cut")
	}
	if err := c.Isolate("n9"); err == nil {
		t.Fatal("a node that does not exist was isolated")
	}
	bare := &Cluster{byID: c.byID, nodes: c.nodes}
	if err := bare.Cut("n1", "n2"); err == nil {
		t.Fatal("a cluster started without Links was partitioned")
	}
	if err := bare.Isolate("n1"); err == nil {
		t.Fatal("a cluster started without Links isolated a node")
	}
}
