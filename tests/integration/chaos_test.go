package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/lab"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/load"
)

// chaosConfig is a short chaos run on three real processes (and a spare)
// started through the integration launcher.
func chaosConfig(t *testing.T, bin string, seed int64) lab.ChaosConfig {
	return lab.ChaosConfig{
		Name: t.Name(),
		Cluster: lab.ClusterConfig{Bin: bin, Mode: "raft", Nodes: 3, Spares: 1, Tick: 25 * time.Millisecond,
			SnapshotEvery: 200, DataRoot: t.TempDir(), PortBase: 25000, Start: labStart(t)},
		Load: load.Config{Clients: 4, Duration: 12 * time.Second, Warmup: time.Second, ReadPct: 50, DeletePct: 10, Keys: 16},
		Seed: seed, Every: 1500 * time.Millisecond, Hold: time.Second, Quiet: 1500 * time.Millisecond,
	}
}

// requireChaosOK fails the test with the run's evidence unless it passed:
// the artifacts are written where the integration tests keep theirs, and the
// failure names how to re-check the history and replay the schedule.
func requireChaosOK(t *testing.T, res *lab.ChaosResult, err error) {
	t.Helper()
	if res == nil {
		t.Fatalf("the chaos run could not be carried out: %v", err)
	}
	if err == nil && res.OK() {
		return
	}
	dir, derr := artifactDir("dkv-chaos-", t.Name())
	if derr == nil {
		derr = lab.WriteArtifacts(dir, res)
	}
	var b strings.Builder
	for _, e := range res.Events {
		fmt.Fprintf(&b, "  %7s pos=%-6d %-9s %-3s %s\n", e.At.Round(time.Millisecond), e.Pos, e.Kind, e.Node, e.Detail)
	}
	t.Fatalf("chaos run failed (err %v): errors=%d linearizable=%v unchecked=%v converged=%v (%s)\n%s\n%s\nevents:\n%s"+
		"--- artifacts: %s (%v); re-check: go run ./cmd/lincheck %s; replay: dkvlab -scenario chaos -schedule %s ---",
		err, res.Errors(), res.Check.Linearizable, res.Check.Unchecked, res.Converged, res.ConvergeErr, res.Check.Reason, res.Check.Counterexample, b.String(),
		dir, derr, filepath.Join(dir, "history.txt"), filepath.Join(dir, "schedule.json"))
}

// TestChaosLeaderLossUnderLoadIsLinearizable: clients write, read and delete
// while the leader is SIGKILLed and later the next leader is cut off from
// both peers. Each time another node takes over, the clients keep retrying
// under their request identities, and the history — unknown outcomes
// included — is linearizable. Its premises are checked, not assumed: each
// fault deposed a leader (a new leader in a higher term followed it), and
// operations succeeded after the last recovery.
func TestChaosLeaderLossUnderLoadIsLinearizable(t *testing.T) {
	cc := chaosConfig(t, buildDkvd(t), 1)
	cc.Schedule = []lab.Fault{
		{At: 3 * time.Second, Kind: lab.FaultKill, Node: lab.TargetLeader, Hold: 1500 * time.Millisecond},
		{At: 7 * time.Second, Kind: lab.FaultIsolate, Node: lab.TargetLeader, Hold: 1500 * time.Millisecond},
	}
	res, err := lab.RunChaos(context.Background(), cc)
	requireChaosOK(t, res, err)

	// Premise: each fault deposed the leader it targeted.
	var injected []lab.ChaosEvent
	for _, e := range res.Events {
		if e.Kind == "inject" {
			injected = append(injected, e)
		}
	}
	if len(injected) != 2 {
		t.Fatalf("injected %d faults, want 2: %+v", len(injected), res.Events)
	}
	for _, inj := range injected {
		deposed := false
		for _, e := range res.Events {
			if e.Kind == "leader" && e.Pos > inj.Pos && e.Node != inj.Node {
				deposed = true
				break
			}
		}
		if !deposed {
			t.Fatalf("premise: no other node led after %s's fault (%s)", inj.Node, inj.Detail)
		}
	}
	// Premise: the clients kept going after the last recovery.
	var lastRecover int64
	for _, e := range res.Events {
		if e.Kind == "recover" && e.Pos > lastRecover {
			lastRecover = e.Pos
		}
	}
	after := 0
	for _, op := range res.History.Ops {
		if op.Invoke > lastRecover && op.Outcome == lincheck.OK {
			after++
		}
	}
	if after == 0 {
		t.Fatal("premise: no operation succeeded after the last recovery")
	}
	// Unknown outcomes are kept in the history, never dropped.
	if inc := res.Check.Counts.Incomplete; int64(inc) < res.Load.Classes[load.ClassUnknown] {
		t.Fatalf("%d unknown outcomes in the window but %d incomplete operations in the history", res.Load.Classes[load.ClassUnknown], inc)
	}
	t.Logf("%d operations, %d leader changes, %d unknown, %d succeeded after the last recovery",
		res.Check.Counts.Total, res.LeaderChanges(), res.Check.Counts.Incomplete, after)
}

// TestChaosSeedsAreLinearizable: schedules drawn from three seeds over every
// fault kind — kill, stop, crash at a persistence boundary, pause, isolate,
// cut, snapshot, add-member — each run checked: every fault carried out, the
// history linearizable, the cluster converged.
func TestChaosSeedsAreLinearizable(t *testing.T) {
	bin := buildDkvd(t)
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			res, err := lab.RunChaos(context.Background(), chaosConfig(t, bin, seed))
			requireChaosOK(t, res, err)
			kinds := map[string]int{}
			for _, f := range res.Schedule {
				kinds[f.Kind]++
			}
			t.Logf("schedule %v: %d operations, %d leader changes, %d unknown", kinds, res.Check.Counts.Total, res.LeaderChanges(), res.Check.Counts.Incomplete)
		})
	}
}
