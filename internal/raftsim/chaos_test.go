package raftsim

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Seeded randomized fault schedules (docs/FAULTS.md §7). Every run is a pure
// function of (profile, seed) and records its full script, so any failure prints
// the exact command that replays it and a minimized script.
//
//	go test ./internal/raftsim -run TestRandomizedFaultSchedules            # default seed set
//	go test ./internal/raftsim -run TestRandomizedFaultSchedules -raftsim.seeds=500
//	go test ./internal/raftsim -run TestRandomizedFaultSchedules -raftsim.profile=disk -raftsim.seed=42 -v
//	go test ./internal/raftsim -run TestReplayScript -raftsim.replay=failure.txt -raftsim.nodes=5 -raftsim.seed=42
var (
	flagSeed    = flag.Int64("raftsim.seed", 0, "run only this seed (0 = the default seed set)")
	flagSeeds   = flag.Int("raftsim.seeds", 0, "seeds per profile (0 = 6, or 2 with -short)")
	flagSteps   = flag.Int("raftsim.steps", 0, "chaos steps per run (0 = the profile's own)")
	flagProfile = flag.String("raftsim.profile", "", "run only this profile")
	flagReplay  = flag.String("raftsim.replay", "", "script file for TestReplayScript")
	flagNodes   = flag.Int("raftsim.nodes", 3, "cluster size for TestReplayScript")
	flagVerbose = flag.Bool("raftsim.verbose", false, "print every trace line of TestReplayScript")
)

func selectedProfiles(t *testing.T) []Profile {
	if *flagProfile == "" {
		return Profiles
	}
	p, ok := ProfileByName(*flagProfile)
	if !ok {
		t.Fatalf("unknown profile %q", *flagProfile)
	}
	return []Profile{p}
}

func selectedSeeds() []int64 {
	if *flagSeed != 0 {
		return []int64{*flagSeed}
	}
	n := *flagSeeds
	if n == 0 {
		n = 6
		if testing.Short() {
			n = 2
		}
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i + 1)
	}
	return out
}

// reproCommand is the exact command that re-runs one seed.
func reproCommand(p Profile, seed int64) string {
	return fmt.Sprintf("go test ./internal/raftsim -run 'TestRandomizedFaultSchedules/%s/seed=%d$' -raftsim.profile=%s -raftsim.seed=%d -raftsim.steps=%d -count=1 -v",
		p.Name, seed, p.Name, seed, p.Steps)
}

// TestRandomizedFaultSchedules is scenario J: long seeded schedules mixing every
// fault type, with every continuous invariant checked after every event and the
// post-fault convergence check (INV-F3) at the end. No run may pass vacuously:
// each must commit and must inject faults, and across the seed set every fault
// type its profile weights must actually occur (a rare fault — a power loss is a
// sub-case of an already-rare crash — can legitimately be absent from one seed,
// so that part is checked on the whole set, not per run).
func TestRandomizedFaultSchedules(t *testing.T) {
	for _, p := range selectedProfiles(t) {
		if *flagSteps > 0 {
			p.Steps = *flagSteps
		}
		seeds := selectedSeeds()
		var tally Stats
		for _, seed := range seeds {
			p, seed := p, seed
			t.Run(fmt.Sprintf("%s/seed=%d", p.Name, seed), func(t *testing.T) {
				r := Run(p, seed)
				if r.Violation != nil {
					cfg := Config{Nodes: p.Nodes, Seed: seed}
					min := Minimize(cfg, r.Script, 300)
					t.Fatalf("%s--- minimized script (%d of %d events; save it and replay with -raftsim.replay=FILE -raftsim.nodes=%d -raftsim.seed=%d) ---\n%s",
						r.Report(reproCommand(p, seed)), len(min), len(r.Script), p.Nodes, seed, FormatScript(min))
				}
				requireProgressAndFaults(t, p, r.Stats)
				tally = addStats(tally, r.Stats)
				if testing.Verbose() {
					t.Logf("events=%d trace=%s %+v", len(r.Script), r.TraceHash[:16], r.Stats)
				}
			})
		}
		if len(seeds) >= 5 {
			requireEveryFaultOccurred(t, p, tally)
		}
	}
}

// requireProgressAndFaults fails a run that never committed, never elected, or
// injected no fault at all.
func requireProgressAndFaults(t *testing.T, p Profile, s Stats) {
	t.Helper()
	injected := s.DroppedInjected + s.Duplicated + s.Delayed + s.DroppedPartition + s.ProcessCrashes +
		s.PowerLosses + s.PersistFailures + s.Pauses + s.PointCrashes
	if s.MaxCommit <= 1 || s.LeaderElections == 0 || injected == 0 {
		t.Fatalf("profile %s run proved nothing: commit=%d elections=%d faults injected=%d (%+v)",
			p.Name, s.MaxCommit, s.LeaderElections, injected, s)
	}
}

// requireEveryFaultOccurred fails a profile whose seed set never produced one of
// the faults it weights: then it would not be exercising what it claims.
func requireEveryFaultOccurred(t *testing.T, p Profile, s Stats) {
	t.Helper()
	var missing []string
	need := func(weighted bool, ok bool, what string) {
		if weighted && !ok {
			missing = append(missing, what)
		}
	}
	need(p.Drop > 0, s.DroppedInjected > 0, "an injected drop")
	need(p.Duplicate > 0, s.Duplicated > 0, "a duplicate")
	need(p.Delay > 0, s.Delayed > 0, "a delay")
	need(p.Partition > 0, s.DroppedPartition > 0, "a message lost to a partition")
	need(p.Crash > 0, s.ProcessCrashes > 0 && s.Restarts > 0, "a crash and a restart")
	need(p.Crash > 0 && p.PowerLossPercent > 0, s.PowerLosses > 0, "a power loss")
	need(p.FailPersist > 0, s.PersistFailures > 0, "a persistence failure")
	need(p.Pause > 0, s.Pauses > 0, "a pause")
	need(p.CrashAt > 0, s.PointCrashes > 0 && s.Restarts > 0, "a crash at a crash point and a restart")
	need(p.CrashAt > 0 && p.PowerLossPercent > 0, s.PowerLosses > 0, "a power loss at a crash point")
	if len(missing) > 0 {
		t.Fatalf("profile %s: no run in the seed set produced %s — it does not exercise what it claims (%+v)",
			p.Name, strings.Join(missing, ", "), s)
	}
}

func addStats(a, b Stats) Stats {
	a.DroppedInjected += b.DroppedInjected
	a.Duplicated += b.Duplicated
	a.Delayed += b.Delayed
	a.DroppedPartition += b.DroppedPartition
	a.ProcessCrashes += b.ProcessCrashes
	a.Restarts += b.Restarts
	a.PowerLosses += b.PowerLosses
	a.PersistFailures += b.PersistFailures
	a.Pauses += b.Pauses
	a.PointCrashes += b.PointCrashes
	return a
}

// TestSameSeedSameTrace is the reproducibility guarantee: the same profile and seed
// produce the same script and the same trace hash; replaying the recorded script
// on a fresh cluster reproduces the identical trace; and different seeds do not.
func TestSameSeedSameTrace(t *testing.T) {
	for _, p := range Profiles {
		a, b := Run(p, 7), Run(p, 7)
		if a.TraceHash != b.TraceHash || len(a.Script) != len(b.Script) {
			t.Fatalf("%s: same seed, different runs: %s/%d vs %s/%d", p.Name, a.TraceHash, len(a.Script), b.TraceHash, len(b.Script))
		}
		if FormatScript(a.Script) != FormatScript(b.Script) {
			t.Fatalf("%s: same seed produced different scripts", p.Name)
		}
		re := Replay(Config{Nodes: p.Nodes, Seed: 7}, a.Script)
		if re.TraceHash != a.TraceHash {
			t.Fatalf("%s: replaying the recorded script gave trace %s, the run gave %s", p.Name, re.TraceHash, a.TraceHash)
		}
		// The script survives a round trip through its text form.
		parsed, err := ParseScript(FormatScript(a.Script))
		if err != nil {
			t.Fatalf("%s: script does not parse back: %v", p.Name, err)
		}
		if again := Replay(Config{Nodes: p.Nodes, Seed: 7}, parsed); again.TraceHash != a.TraceHash {
			t.Fatalf("%s: replay from the text script diverged", p.Name)
		}
		if c := Run(p, 8); c.TraceHash == a.TraceHash {
			t.Fatalf("%s: seeds 7 and 8 produced the same trace — the seed is not driving the run", p.Name)
		}
	}
}

// goldenMixedSeed1 is the trace hash of profile "mixed", seed 1. It is recorded
// so that CI (linux/amd64) and a developer machine (e.g. darwin/arm64) are held to
// the SAME trace: a seed must replay identically everywhere, not just twice on one
// machine. If a deliberate change to the simulator, the trace format, or Raft's
// behaviour changes it, re-record it (docs/FAULTS.md §8) and say why in the commit.
const goldenMixedSeed1 = "3afd238b760eab9e822ea45699db10832d6977abf38518cec16c55ee8a3527ba"

func TestTraceIsPlatformIndependent(t *testing.T) {
	p, _ := ProfileByName("mixed")
	r := Run(p, 1)
	if r.Violation != nil {
		t.Fatal(r.Violation)
	}
	if r.TraceHash != goldenMixedSeed1 {
		t.Fatalf("mixed/seed=1 trace hash is %s, recorded %s (GOOS/GOARCH-dependent behaviour, or an unrecorded behaviour change)", r.TraceHash, goldenMixedSeed1)
	}
}

// TestMinimizeShrinksWhileKeepingTheFailure checks the minimizer itself against an
// artificial failure predicate ("some node reached term 3"): the result must be
// much shorter, still satisfy the predicate on replay, and be locally minimal
// enough that deleting any single event breaks it.
func TestMinimizeShrinksWhileKeepingTheFailure(t *testing.T) {
	p, _ := ProfileByName("partitions")
	r := Run(p, 3)
	cfg := Config{Nodes: p.Nodes, Seed: 3}
	reached := func(s []Event) bool { return Replay(cfg, s).Stats.MaxTerm >= 3 }
	if !reached(r.Script) {
		t.Fatal("precondition: partitions/seed=3 no longer reaches term 3; pick a seed that does")
	}
	min := MinimizeFunc(cfg, r.Script, 2000, reached)
	if !reached(min) {
		t.Fatal("the minimized script no longer reproduces the property")
	}
	if len(min)*10 > len(r.Script) {
		t.Fatalf("minimized %d events to %d; expected at least a 10x reduction", len(r.Script), len(min))
	}
	for i := range min {
		cand := append(append([]Event(nil), min[:i]...), min[i+1:]...)
		if reached(cand) {
			t.Fatalf("event %d (%s) of the minimized script is unnecessary", i, min[i])
		}
	}
}

// TestReplayScript replays a saved script (a minimized failure, say):
//
//	go test ./internal/raftsim -run TestReplayScript -raftsim.replay=f.txt -raftsim.nodes=5 -raftsim.seed=42 -raftsim.verbose -v
func TestReplayScript(t *testing.T) {
	if *flagReplay == "" {
		t.Skip("no -raftsim.replay script given")
	}
	text, err := os.ReadFile(*flagReplay)
	if err != nil {
		t.Fatal(err)
	}
	script, err := ParseScript(string(text))
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{Nodes: *flagNodes, Seed: *flagSeed})
	if err != nil {
		t.Fatal(err)
	}
	if *flagVerbose {
		c.Trace().Mirror(os.Stdout)
	}
	for _, e := range script {
		c.Apply(e)
	}
	if v := c.Violation(); v != nil {
		t.Fatalf("%v", v)
	}
	t.Logf("replayed %d events without a violation; trace %s", len(script), c.Trace().Hash())
}
