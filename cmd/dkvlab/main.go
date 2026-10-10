// Command dkvlab runs reproducible experiments on real Quorum clusters (#3,
// docs/CLUSTER_BENCHMARKS.md): it builds dkvd, launches real processes on
// loopback, drives them with a dkvload workload, acts on them while the load
// runs, repeats each configuration, and writes every run and a summary per
// configuration as JSON.
//
//	dkvlab -scenario leader-kill -nodes 3 -runs 5 -out leader-kill.json
//	dkvlab -suite report -out report.json      # the matrix docs/CLUSTER_BENCHMARKS.md reports
//	dkvlab -scenario steady -nodes 1,3,5 -read 5,50,95 -runs 3 -out matrix.json   # every combination
//	dkvlab -scenario rolling-restart -rate 50 -trace -max-attempts 8,30 -out rr.json
//	dkvlab -scenario chaos -seed 7 -runs 3 -artifacts chaos/   # docs/CHAOS.md
//	dkvlab -scenario chaos -schedule chaos/seed-7/schedule.json  # replay one schedule
//
// Scenarios: steady, leader-kill, rolling-restart, membership, snapshots,
// chaos (a seeded fault schedule under a recorded, linearizability-checked
// workload).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/lab"
	"github.com/adivishall/quorum/internal/load"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dkvlab", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		scenario  = fs.String("scenario", "steady", "steady, leader-kill, rolling-restart, membership, snapshots or chaos")
		suite     = fs.String("suite", "", "run a named matrix instead of one scenario: report")
		mode      = fs.String("mode", "raft", "raft (one group) or cluster (one group per shard)")
		nodes     = fs.String("nodes", "3", "genesis nodes (a comma list runs each)")
		shards    = fs.String("shards", "4", "cluster mode: shards (= groups; a comma list runs each)")
		rf        = fs.Int("rf", 3, "cluster mode: replication factor")
		tick      = fs.Duration("tick", 50*time.Millisecond, "dkvd -tick-interval")
		snapEv    = fs.Uint64("snapshot-every", 0, "dkvd -snapshot-every (0: dkvd's default)")
		clients   = fs.String("clients", "16", "load: concurrent clients (a comma list runs each)")
		duration  = fs.Duration("duration", 20*time.Second, "load: measured window")
		warmup    = fs.Duration("warmup", 3*time.Second, "load: warmup")
		rate      = fs.Float64("rate", 0, "load: open-loop rate (0: closed loop)")
		faultRate = fs.Float64("fault-rate", 50, "the report suite: the open-loop rate of its fault scenarios (keep it below the cluster's capacity)")
		read      = fs.String("read", "50", "load: percentage of GETs (a comma list runs each)")
		keys      = fs.Int("keys", 10000, "load: distinct keys")
		dist      = fs.String("dist", "uniform", "load: key distribution, uniform or zipf")
		value     = fs.String("value", "100", "load: value bytes (a comma list runs each)")
		attempts  = fs.String("max-attempts", "8", "load: attempts per request before an unsettled one is unknown (a comma list runs each)")
		trace     = fs.Bool("trace", false, "load: keep the attempt trace of every operation that ended refused or unknown, and classify the unknown ones")
		seed      = fs.Int64("seed", 1, "load: seed")
		attempt   = fs.Duration("attempt-timeout", 2*time.Second, "load: one attempt's budget (a request stuck at a dead node waits this long before it is retried elsewhere)")
		runs      = fs.Int("runs", 3, "runs per configuration")
		out       = fs.String("out", "", "write every run and the summaries as JSON")
		repo      = fs.String("repo", ".", "the repository to build dkvd from and record the commit of")
		data      = fs.String("data", "", "where the nodes' data directories go (default: a temporary directory)")
		del       = fs.Int("delete", 0, "load: percentage of DELETEs")
		spares    = fs.Int("spares", 0, "extra nodes a scenario may add (chaos: 1, for add-member)")
		smKind    = fs.String("state-machine", "", "dkvd -state-machine for every node: memory (the default) or lsm (S2)")
		chaosF    = chaosFlags(fs)
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	dims := map[string][]int{}
	for name, v := range map[string]string{"nodes": *nodes, "shards": *shards, "clients": *clients, "read": *read, "value": *value, "max-attempts": *attempts} {
		list, err := intList(v)
		if err != nil {
			fmt.Fprintf(stderr, "dkvlab: -%s: %v\n", name, err)
			return 2
		}
		dims[name] = list
	}
	root := *data
	if root == "" {
		d, err := os.MkdirTemp("", "dkvlab-")
		if err != nil {
			fmt.Fprintf(stderr, "dkvlab: %v\n", err)
			return 2
		}
		defer os.RemoveAll(d)
		root = d
	}
	bin, err := lab.Build(*repo, root)
	if err != nil {
		fmt.Fprintf(stderr, "dkvlab: %v\n", err)
		return 2
	}
	mk := func(n, g, c, r, v, a int) lab.Experiment {
		return lab.Experiment{
			Cluster: lab.ClusterConfig{Bin: bin, Mode: *mode, Nodes: n, Spares: *spares, StateMachine: *smKind, Shards: g, RF: *rf, Tick: *tick, SnapshotEvery: *snapEv},
			Load: load.Config{Clients: c, Duration: *duration, Warmup: *warmup, Rate: *rate, ReadPct: r, DeletePct: *del,
				Keys: *keys, KeyDist: *dist, ValueSize: v, Seed: *seed, AttemptTimeout: *attempt, MaxAttempts: a, Trace: *trace},
		}
	}
	first := func(name string) int { return dims[name][0] }
	base := mk(first("nodes"), first("shards"), first("clients"), first("read"), first("value"), first("max-attempts"))
	if *scenario == "chaos" && *suite == "" {
		return runChaos(ctx, base, chaosF, set, *runs, root, *repo, *out, stdout, stderr)
	}
	var exps []lab.Experiment
	if *suite != "" {
		if exps, err = lab.Suite(*suite, base, *faultRate); err != nil {
			fmt.Fprintf(stderr, "dkvlab: %v\n", err)
			return 2
		}
	} else {
		// Every combination of the list-valued flags, in a fixed order; a
		// dimension that varies is named in the configuration.
		for _, n := range dims["nodes"] {
			for _, g := range dims["shards"] {
				for _, c := range dims["clients"] {
					for _, r := range dims["read"] {
						for _, v := range dims["value"] {
							for _, a := range dims["max-attempts"] {
								e, err := lab.Scenario(*scenario, mk(n, g, c, r, v, a))
								if err != nil {
									fmt.Fprintf(stderr, "dkvlab: %v\n", err)
									return 2
								}
								for _, d := range []struct {
									flag, tag string
									val       int
								}{{"clients", "c", c}, {"value", "v", v}, {"max-attempts", "a", a}} {
									if len(dims[d.flag]) > 1 {
										e.Name += fmt.Sprintf("/%s%d", d.tag, d.val)
									}
								}
								exps = append(exps, e)
							}
						}
					}
				}
			}
		}
	}
	report := lab.Report{Env: bench.CaptureEnvironment(*repo), Runs: *runs}
	for i, e := range exps {
		var results []*lab.RunResult
		for r := 0; r < *runs; r++ {
			e := e
			e.Cluster.DataRoot = filepath.Join(root, fmt.Sprintf("e%d-r%d", i, r))
			e.Cluster.PortBase = 31000
			fmt.Fprintf(stdout, "== %s run %d/%d\n", e.Name, r+1, *runs)
			res, err := lab.Run(ctx, e)
			if err != nil {
				fmt.Fprintf(stderr, "dkvlab: %s run %d: %v\n", e.Name, r+1, err)
				if res == nil {
					if ctx.Err() != nil {
						return 1
					}
					continue
				}
			}
			load.PrintSummary(stdout, res.Load)
			for _, a := range res.Actions {
				fmt.Fprintf(stdout, "   %s %s at %s: settled in %s %s %s\n", a.Action.Kind, a.Node, a.Start.Round(time.Millisecond), a.Settle.Round(time.Millisecond), a.Detail, a.Err)
			}
			causes := map[string]int{}
			for _, t := range res.Load.Traces {
				if t.Class == load.ClassUnknown {
					causes[lab.ClassifyUnknown(t)]++
				}
			}
			if len(causes) > 0 {
				fmt.Fprintf(stdout, "   unknown outcomes by cause: %v\n", causes)
			}
			results = append(results, res)
		}
		report.Add(e, results)
	}
	lab.PrintReport(stdout, &report)
	if *out != "" {
		b, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(*out, append(b, '\n'), 0o644)
		}
		if err != nil {
			fmt.Fprintf(stderr, "dkvlab: writing %s: %v\n", *out, err)
			return 1
		}
	}
	return 0
}

// intList parses a comma-separated list of positive integers.
func intList(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 0 {
			return nil, fmt.Errorf("%q is not a list of non-negative integers", s)
		}
		out = append(out, n)
	}
	return out, nil
}
