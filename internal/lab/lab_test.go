package lab

import (
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/metrics"
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
		{Name: "dkv_raft_persist_seconds_sum", Labels: map[string]string{"group": "0"}, Value: 0.21}}
	u := usage(before, after, 2*time.Second, false)
	if u.CPUPerSec != 2 || u.Persists != 100 || u.PersistMeanUs < 1999 || u.PersistMeanUs > 2001 {
		t.Fatalf("%+v", u)
	}
	u = usage(before, after, 2*time.Second, true)
	if u.CPUPerSec != 3 || u.Persists != 110 {
		t.Fatalf("restarted: %+v", u)
	}
}
