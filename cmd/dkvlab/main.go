// Command dkvlab runs reproducible experiments on real Quorum clusters (#3,
// docs/CLUSTER_BENCHMARKS.md): it builds dkvd, launches real processes on
// loopback, drives them with a dkvload workload, acts on them while the load
// runs, repeats each configuration, and writes every run and a summary per
// configuration as JSON.
//
//	dkvlab -scenario leader-kill -nodes 3 -runs 5 -out leader-kill.json
//	dkvlab -suite report -out report.json      # the matrix docs/CLUSTER_BENCHMARKS.md reports
//
// Scenarios: steady, leader-kill, rolling-restart, membership, snapshots.
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
		scenario  = fs.String("scenario", "steady", "steady, leader-kill, rolling-restart, membership or snapshots")
		suite     = fs.String("suite", "", "run a named matrix instead of one scenario: report")
		mode      = fs.String("mode", "raft", "raft (one group) or cluster (one group per shard)")
		nodes     = fs.Int("nodes", 3, "genesis nodes")
		shards    = fs.Int("shards", 4, "cluster mode: shards (= groups)")
		rf        = fs.Int("rf", 3, "cluster mode: replication factor")
		tick      = fs.Duration("tick", 50*time.Millisecond, "dkvd -tick-interval")
		snapEv    = fs.Uint64("snapshot-every", 0, "dkvd -snapshot-every (0: dkvd's default)")
		clients   = fs.Int("clients", 16, "load: concurrent clients")
		duration  = fs.Duration("duration", 20*time.Second, "load: measured window")
		warmup    = fs.Duration("warmup", 3*time.Second, "load: warmup")
		rate      = fs.Float64("rate", 0, "load: open-loop rate (0: closed loop)")
		faultRate = fs.Float64("fault-rate", 50, "the report suite: the open-loop rate of its fault scenarios (keep it below the cluster's capacity)")
		read      = fs.Int("read", 50, "load: percentage of GETs")
		keys      = fs.Int("keys", 10000, "load: distinct keys")
		value     = fs.Int("value", 100, "load: value bytes")
		seed      = fs.Int64("seed", 1, "load: seed")
		attempt   = fs.Duration("attempt-timeout", 2*time.Second, "load: one attempt's budget (a request stuck at a dead node waits this long before it is retried elsewhere)")
		runs      = fs.Int("runs", 3, "runs per configuration")
		out       = fs.String("out", "", "write every run and the summaries as JSON")
		repo      = fs.String("repo", ".", "the repository to build dkvd from and record the commit of")
		data      = fs.String("data", "", "where the nodes' data directories go (default: a temporary directory)")
	)
	if err := fs.Parse(args); err != nil {
		return 2
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
	base := lab.Experiment{
		Cluster: lab.ClusterConfig{Bin: bin, Mode: *mode, Nodes: *nodes, Shards: *shards, RF: *rf, Tick: *tick, SnapshotEvery: *snapEv},
		Load: load.Config{Clients: *clients, Duration: *duration, Warmup: *warmup, Rate: *rate, ReadPct: *read,
			Keys: *keys, ValueSize: *value, Seed: *seed, AttemptTimeout: *attempt},
	}
	var exps []lab.Experiment
	if *suite != "" {
		if exps, err = lab.Suite(*suite, base, *faultRate); err != nil {
			fmt.Fprintf(stderr, "dkvlab: %v\n", err)
			return 2
		}
	} else {
		e, err := lab.Scenario(*scenario, base)
		if err != nil {
			fmt.Fprintf(stderr, "dkvlab: %v\n", err)
			return 2
		}
		exps = []lab.Experiment{e}
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
