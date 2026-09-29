package integration

import (
	"context"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/load"
)

// TestRealLoadAgainstARaftCluster (#2): the load generator drives a real
// three-process -raft group through its client ports: identified operations
// through every node (followers forward), sessions registered before the
// clock, operations completed throughout the window.
func TestRealLoadAgainstARaftCluster(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	var eps []load.Endpoint
	for _, id := range c.ids {
		eps = append(eps, load.Endpoint{Name: id, Addr: c.kvAddrs[id]})
	}
	res, err := load.Run(context.Background(), load.Config{Endpoints: eps, Clients: 4, Duration: 2 * time.Second,
		Warmup: 500 * time.Millisecond, ReadPct: 50, Keys: 100, Seed: 5, Groups: nil})
	if err != nil {
		t.Fatal(err)
	}
	done := res.Classes[load.ClassOK] + res.Classes[load.ClassNotFound]
	if done == 0 || res.Ops["put"].Latency.Count == 0 || res.Ops["get"].Latency.Count == 0 {
		t.Fatalf("outcomes %v", res.Classes)
	}
	if res.LongestOutage >= 2*time.Second {
		t.Fatalf("nothing completed for %s of a 2 s window", res.LongestOutage)
	}
	if res.Ops["put"].Latency.P50 <= 0 || res.Ops["put"].Latency.Max < res.Ops["put"].Latency.P50 {
		t.Fatalf("put latency %+v", res.Ops["put"].Latency)
	}
}
