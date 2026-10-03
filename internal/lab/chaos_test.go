package lab

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/multiraft"
)

func chaosBase(seed int64) ChaosConfig {
	return ChaosConfig{Seed: seed, Load: load.Config{Duration: 20 * time.Second, Warmup: 2 * time.Second}}
}

// TestPlanChaosIsAFunctionOfItsSeed: the same seed and configuration always
// draw the same schedule — the property a replay relies on — and other seeds
// draw other schedules.
func TestPlanChaosIsAFunctionOfItsSeed(t *testing.T) {
	nodes, spares, groups := []string{"n1", "n2", "n3"}, []string{"n4"}, []multiraft.GroupID{0, 1}
	a, err := PlanChaos(chaosBase(7), nodes, spares, groups)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PlanChaos(chaosBase(7), nodes, spares, groups)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("seed 7 drew two schedules:\n%v\n%v", a, b)
	}
	differs := 0
	for seed := int64(8); seed < 18; seed++ {
		c, err := PlanChaos(chaosBase(seed), nodes, spares, groups)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, c) {
			differs++
		}
	}
	if differs < 9 {
		t.Fatalf("only %d of 10 other seeds drew a different schedule", differs)
	}
}

// TestPlanChaosKeepsOneImpairmentAtATime: across many seeds every schedule
// starts after the warmup, ends before the quiet tail, never overlaps one
// fault's hold with the next, targets only genesis nodes (or the leader),
// cuts only links between two distinct genesis nodes, adds each spare at most
// once, and uses only the kinds it was given.
func TestPlanChaosKeepsOneImpairmentAtATime(t *testing.T) {
	nodes, spares := []string{"n1", "n2", "n3", "n4", "n5"}, []string{"n6", "n7"}
	genesis := map[string]bool{}
	for _, n := range nodes {
		genesis[n] = true
	}
	for seed := int64(1); seed <= 200; seed++ {
		cc := chaosBase(seed)
		cc.Kinds = FaultKinds
		fs, err := PlanChaos(cc, nodes, spares, []multiraft.GroupID{0, 1, 2})
		if err != nil {
			t.Fatal(err)
		}
		cc.defaults()
		end := cc.Load.Warmup + cc.Load.Duration - cc.Quiet
		added := map[string]bool{}
		for i, f := range fs {
			if f.At < cc.Load.Warmup || f.At+f.Hold > end {
				t.Fatalf("seed %d fault %d (%s) is outside [%s, %s]", seed, i, f, cc.Load.Warmup, end)
			}
			if i > 0 && fs[i-1].At+fs[i-1].Hold >= f.At {
				t.Fatalf("seed %d: fault %d (%s) starts before fault %d (%s) ends", seed, i, f, i-1, fs[i-1])
			}
			switch f.Kind {
			case FaultKill, FaultStop, FaultCrash, FaultPause, FaultIsolate:
				if f.Node != TargetLeader && !genesis[f.Node] || f.Hold <= 0 {
					t.Fatalf("seed %d: %s", seed, f)
				}
			case FaultCut:
				if !genesis[f.Node] || !genesis[f.Peer] || f.Node == f.Peer || f.Hold <= 0 {
					t.Fatalf("seed %d: %s", seed, f)
				}
			case FaultSnapshot:
				if f.Node != TargetLeader || f.Hold != 0 {
					t.Fatalf("seed %d: %s", seed, f)
				}
			case FaultAddMember:
				if genesis[f.Node] || added[f.Node] || f.Hold != 0 {
					t.Fatalf("seed %d: %s (added before: %v)", seed, f, added)
				}
				added[f.Node] = true
			default:
				t.Fatalf("seed %d: unknown kind in %s", seed, f)
			}
		}
	}
	cc := chaosBase(3)
	cc.Kinds = []string{FaultCut}
	fs, err := PlanChaos(cc, nodes, nil, []multiraft.GroupID{0})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fs {
		if f.Kind != FaultCut {
			t.Fatalf("a cut-only schedule holds %s", f)
		}
	}
	if _, err := PlanChaos(ChaosConfig{Kinds: []string{"meteor"}, Load: cc.Load}, nodes, nil, []multiraft.GroupID{0}); err == nil {
		t.Fatal("an unknown fault kind was accepted")
	}
	if _, err := PlanChaos(cc, []string{"n1", "n2"}, nil, []multiraft.GroupID{0}); err == nil {
		t.Fatal("a two-node cluster was given a schedule: an impaired node would leave no quorum")
	}
	short := ChaosConfig{Load: load.Config{Duration: 2 * time.Second, Warmup: time.Second}}
	if _, err := PlanChaos(short, nodes, nil, []multiraft.GroupID{0}); err == nil {
		t.Fatal("a window too short for any fault drew a schedule")
	}
}

// TestScheduleRoundTrips: a schedule written with a run's artifacts reads back
// identical — from schedule.json or from the whole chaos.json — so a run can
// be replayed exactly.
func TestScheduleRoundTrips(t *testing.T) {
	fs, err := PlanChaos(chaosBase(11), []string{"n1", "n2", "n3"}, []string{"n4"}, []multiraft.GroupID{0})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res := &ChaosResult{Config: chaosBase(11), Schedule: fs, Logs: map[string]string{"n1": "x"}, Links: map[string]string{"n1-n2": "y"}}
	if err := WriteArtifacts(dir, res); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"schedule.json", "chaos.json"} {
		got, err := ReadSchedule(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, fs) {
			t.Fatalf("%s: read back %v, wrote %v", name, got, fs)
		}
	}
	for _, f := range []string{"history.txt", "nodes/n1.log", "links/n1-n2.log"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
	var whole map[string]json.RawMessage
	b, _ := os.ReadFile(filepath.Join(dir, "chaos.json"))
	if err := json.Unmarshal(b, &whole); err != nil || whole["schedule"] == nil || whole["check"] == nil {
		t.Fatalf("chaos.json: %v, keys %v", err, whole)
	}
}

// TestFirstLeaderIsNotAChange: a group's first leader, seen when sampling
// starts, is recorded but not counted as a leader change.
func TestFirstLeaderIsNotAChange(t *testing.T) {
	res := &ChaosResult{Events: []ChaosEvent{{Kind: "first-leader", Node: "n1"}, {Kind: "inject", Node: "n1"}, {Kind: "leader", Node: "n2"}, {Kind: "leader", Node: "n3"}}}
	if n := res.LeaderChanges(); n != 2 {
		t.Fatalf("%d leader changes, want 2", n)
	}
}
