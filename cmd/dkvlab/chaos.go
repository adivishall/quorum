package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/lab"
)

// chaosOpts are dkvlab's chaos flags.
type chaosOpts struct {
	faults     *string
	every      *time.Duration
	hold       *time.Duration
	quiet      *time.Duration
	crashPoint *string
	schedule   *string
	artifacts  *string
}

func chaosFlags(fs *flag.FlagSet) chaosOpts {
	return chaosOpts{
		faults:     fs.String("faults", "", "chaos: the fault kinds the schedule may draw, comma-separated ("+strings.Join(lab.FaultKinds, ",")+"; default all)"),
		every:      fs.Duration("fault-every", 2*time.Second, "chaos: mean gap between one fault's recovery and the next fault"),
		hold:       fs.Duration("fault-hold", 1500*time.Millisecond, "chaos: mean time a fault is held before it is undone"),
		quiet:      fs.Duration("fault-quiet", 2*time.Second, "chaos: the end of the window in which no fault starts"),
		crashPoint: fs.String("crash-point", "after-save", "chaos: the driver crash point a crash fault dies at (docs/CRASH_RECOVERY.md)"),
		schedule:   fs.String("schedule", "", "chaos: replay this schedule (a run's schedule.json or chaos.json) instead of drawing one"),
		artifacts:  fs.String("artifacts", "", "chaos: write every run's evidence under this directory (a failing run's always goes somewhere: a temporary directory by default)"),
	}
}

// chaosReport is what -out writes for chaos runs.
type chaosReport struct {
	Env  bench.Environment  `json:"env"`
	Runs []*lab.ChaosResult `json:"runs"`
}

// runChaos runs -runs chaos runs, seeds seed, seed+1, … (or one replayed
// schedule each time), printing a verdict per run. It exits 1 if any run was
// not linearizable, did not converge, or could not be carried out.
func runChaos(ctx context.Context, base lab.Experiment, o chaosOpts, set map[string]bool, runs int, root, repo, out string, stdout, stderr io.Writer) int {
	// Chaos defaults where the flag was not given: a few clients on few keys
	// (contention, so the history says something), some deletes, frequent
	// snapshots (restarted nodes may catch up by one), one spare to add.
	if !set["clients"] {
		base.Load.Clients = 4
	}
	if !set["keys"] {
		base.Load.Keys = 16
	}
	if !set["delete"] {
		base.Load.DeletePct = 10
	}
	if !set["snapshot-every"] {
		base.Cluster.SnapshotEvery = 200
	}
	if !set["spares"] {
		base.Cluster.Spares = 1
	}
	var kinds []string
	if *o.faults != "" {
		kinds = strings.Split(*o.faults, ",")
	}
	var schedule []lab.Fault
	if *o.schedule != "" {
		s, err := lab.ReadSchedule(*o.schedule)
		if err != nil {
			fmt.Fprintf(stderr, "dkvlab: %v\n", err)
			return 2
		}
		schedule = s
	}
	report := chaosReport{Env: bench.CaptureEnvironment(repo)}
	failed := 0
	for r := 0; r < runs; r++ {
		seed := base.Load.Seed + int64(r)
		cc := lab.ChaosConfig{Name: fmt.Sprintf("chaos/seed-%d", seed), Cluster: base.Cluster, Load: base.Load, Seed: seed, Kinds: kinds,
			Every: *o.every, Hold: *o.hold, Quiet: *o.quiet, CrashPoint: *o.crashPoint, Schedule: schedule}
		cc.Load.Seed = seed
		cc.Cluster.DataRoot = filepath.Join(root, fmt.Sprintf("chaos-%d", r))
		cc.Cluster.PortBase = 31000
		if *o.artifacts != "" {
			cc.Dir = filepath.Join(*o.artifacts, fmt.Sprintf("seed-%d", seed))
			if schedule != nil {
				cc.Dir = filepath.Join(*o.artifacts, fmt.Sprintf("replay-%d", r))
			}
		}
		fmt.Fprintf(stdout, "== %s run %d/%d\n", cc.Name, r+1, runs)
		res, err := lab.RunChaos(ctx, cc)
		if err != nil {
			fmt.Fprintf(stderr, "dkvlab: %s: %v\n", cc.Name, err)
		}
		if res == nil {
			failed++
			if ctx.Err() != nil {
				return 1
			}
			continue
		}
		if !res.OK() && res.Artifacts == "" {
			dir, derr := os.MkdirTemp("", "dkvlab-chaos-")
			if derr == nil && lab.WriteArtifacts(dir, res) == nil {
				res.Artifacts = dir
			}
		}
		printChaos(stdout, res)
		if !res.OK() {
			failed++
		}
		report.Runs = append(report.Runs, res)
	}
	if out != "" {
		b, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(out, append(b, '\n'), 0o644)
		}
		if err != nil {
			fmt.Fprintf(stderr, "dkvlab: writing %s: %v\n", out, err)
			return 1
		}
	}
	fmt.Fprintf(stdout, "\n%d of %d chaos runs passed\n", runs-failed, runs)
	if failed > 0 {
		return 1
	}
	return 0
}

// printChaos writes one run's verdict, its faults and its client outcomes.
func printChaos(w io.Writer, res *lab.ChaosResult) {
	for _, e := range res.Events {
		if e.Kind == "inject" || e.Kind == "skip" || e.Kind == "error" || e.Kind == "leader" || e.Kind == "first-leader" {
			fmt.Fprintf(w, "   %7s %-7s %-3s %s\n", e.At.Round(time.Millisecond), e.Kind, e.Node, e.Detail)
		}
	}
	l := res.Load
	c := res.Check.Counts
	fmt.Fprintf(w, "   clients: %d ok, %d not_found, %d refused, %d unknown; %d leader changes\n",
		l.Classes["ok"], l.Classes["not_found"], l.Classes["refused"], l.Classes["unknown"], res.LeaderChanges())
	fmt.Fprintf(w, "   history: %d operations (%d incomplete, %d rejected), %d attempts; linearizable=%v unchecked=%v converged=%v\n",
		c.Total, c.Incomplete, c.Rejected, c.Attempts, res.Check.Linearizable, res.Check.Unchecked, res.Converged)
	if res.Check.Reason != "" && !res.Check.Linearizable {
		fmt.Fprintf(w, "   VIOLATION: %s\n%s\n", res.Check.Reason, res.Check.Counterexample)
	}
	if res.ConvergeErr != "" {
		fmt.Fprintf(w, "   NOT CONVERGED: %s\n", res.ConvergeErr)
	}
	if res.Artifacts != "" {
		fmt.Fprintf(w, "   artifacts: %s (re-check: go run ./cmd/lincheck %s)\n", res.Artifacts, filepath.Join(res.Artifacts, "history.txt"))
	}
	verdict := "PASS"
	if !res.OK() {
		verdict = "FAIL"
	}
	fmt.Fprintf(w, "   %s\n", verdict)
}
