package integration

import (
	"context"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/lab"
	"github.com/adivishall/quorum/internal/load"
)

// TestLabLeaderKillOnRealProcesses (#3): the lab runs a short leader-kill
// experiment on three real dkvd processes under a closed-loop load. The
// clients kept completing operations, none took effect twice (the only
// outcomes are known ones or unknown ones), the leader's death shows as a
// measured election, the killed node came back and caught up, and every
// node's usage was read from its own metrics.
func TestLabLeaderKillOnRealProcesses(t *testing.T) {
	bin := buildDkvd(t)
	e, err := lab.Scenario("leader-kill", lab.Experiment{
		// Ports from this binary's own range (testport.Integration, below every
		// ephemeral range): 29000 and up belong to internal/raftnode's tests,
		// which `go test ./...` runs concurrently with this package.
		Cluster: lab.ClusterConfig{Bin: bin, Mode: "raft", Nodes: 3, Tick: 25 * time.Millisecond, DataRoot: t.TempDir(), PortBase: 25000},
		Load:    load.Config{Clients: 4, Duration: 6 * time.Second, Warmup: time.Second, ReadPct: 50, Keys: 200, Seed: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := lab.Run(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	if res.Load.Classes[load.ClassOK] == 0 {
		t.Fatalf("no operation succeeded: %v", res.Load.Classes)
	}
	if len(res.Actions) != 2 || res.Actions[0].Action.Kind != "kill-leader" || res.Actions[1].Action.Kind != "restart" {
		t.Fatalf("actions: %+v", res.Actions)
	}
	kill, restart := res.Actions[0], res.Actions[1]
	if kill.Err != "" || kill.Settle <= 0 || kill.Settle > 10*time.Second {
		t.Fatalf("the election after the kill: %+v", kill)
	}
	if restart.Err != "" || restart.Node != kill.Node {
		t.Fatalf("the killed leader's restart: %+v", restart)
	}
	if len(res.Nodes) != 3 {
		t.Fatalf("usage for %d nodes", len(res.Nodes))
	}
	for id, u := range res.Nodes {
		if u.CPUPerSec <= 0 || u.MaxRSSBytes <= 0 {
			t.Fatalf("%s: %+v", id, u)
		}
	}
	if !res.Nodes[kill.Node].Restarted {
		t.Fatalf("%s was restarted but its usage is not marked so", kill.Node)
	}
}
