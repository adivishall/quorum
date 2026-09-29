// Command dkvload puts a client workload on a running Quorum cluster and
// reports what the clients observed (#2, docs/LOAD_TESTING.md).
//
//	dkvload -endpoints n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003 \
//	        [-shards 4 -rf 3 -nodes n1,n2,n3] -clients 16 -duration 30s -warmup 5s \
//	        [-rate 2000] -read 50 -delete 5 -keys 10000 -dist zipf -value 100 -seed 1 \
//	        -out result.json
//
// Without -shards every key is group 0 (a dkvd -raft cluster); with it, the
// routing must be the cluster's (-shards, -rf, -nodes as every dkvd was
// started with). -rate 0 is closed loop. The JSON result records the
// configuration, the environment and every number the summary prints.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/routing"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dkvload", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		endpoints = fs.String("endpoints", "", "the cluster's client ports: id=host:port,... (required)")
		shards    = fs.Int("shards", 0, "the cluster's -shards (0: every key is group 0, a -raft cluster)")
		rf        = fs.Int("rf", 3, "the cluster's -rf")
		nodes     = fs.String("nodes", "", "the cluster's -nodes (default: the endpoint ids)")
		clients   = fs.Int("clients", 8, "concurrent simulated clients, each with its own connections and sessions")
		duration  = fs.Duration("duration", 10*time.Second, "measured window")
		warmup    = fs.Duration("warmup", 2*time.Second, "issued before the measured window, not recorded")
		rate      = fs.Float64("rate", 0, "target operations per second in total (open loop); 0 runs closed loop")
		read      = fs.Int("read", 50, "percentage of GETs")
		del       = fs.Int("delete", 0, "percentage of DELETEs (the rest are PUTs)")
		keys      = fs.Int("keys", 10000, "distinct keys")
		dist      = fs.String("dist", "uniform", "key distribution: uniform or zipf")
		zipfS     = fs.Float64("zipf-s", 1.1, "zipf exponent (> 1)")
		value     = fs.Int("value", 100, "value size in bytes")
		seed      = fs.Int64("seed", 1, "seed of every random choice of the workload")
		anonymous = fs.Bool("anonymous", false, "send anonymous requests (no session, one attempt)")
		timeout   = fs.Duration("attempt-timeout", 2*time.Second, "one attempt's budget")
		attempts  = fs.Int("max-attempts", 8, "attempts per identified request")
		out       = fs.String("out", "", "write the JSON result to this file")
		repo      = fs.String("repo", ".", "the repository, for the recorded git commit")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg := load.Config{Clients: *clients, Duration: *duration, Warmup: *warmup, Rate: *rate,
		ReadPct: *read, DeletePct: *del, Keys: *keys, KeyDist: *dist, ZipfS: *zipfS, ValueSize: *value, Seed: *seed,
		Anonymous: *anonymous, AttemptTimeout: *timeout, MaxAttempts: *attempts}
	var ids []routing.NodeID
	for _, part := range strings.Split(*endpoints, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			fmt.Fprintf(stderr, "dkvload: bad endpoint %q (want id=host:port)\n", part)
			return 2
		}
		cfg.Endpoints = append(cfg.Endpoints, load.Endpoint{Name: id, Addr: addr})
		ids = append(ids, routing.NodeID(id))
	}
	if len(cfg.Endpoints) == 0 {
		fmt.Fprintln(stderr, "dkvload: -endpoints is required")
		return 2
	}
	if *shards > 0 {
		if *nodes != "" {
			ids = nil
			for _, n := range strings.Split(*nodes, ",") {
				ids = append(ids, routing.NodeID(strings.TrimSpace(n)))
			}
		}
		a, err := multiraft.NewAssignment(routing.Config{ShardCount: *shards, ReplicationFactor: *rf, Nodes: ids})
		if err != nil {
			fmt.Fprintf(stderr, "dkvload: the cluster's routing: %v\n", err)
			return 2
		}
		cfg.Route = a.GroupOf
		cfg.Groups = a.Groups()
		cfg.RouteSpec = fmt.Sprintf("shards=%d rf=%d nodes=%v", *shards, *rf, ids)
	} else {
		cfg.Groups = []replication.GroupID{0}
	}
	res, err := load.Run(ctx, cfg)
	if err != nil && res == nil {
		fmt.Fprintf(stderr, "dkvload: %v\n", err)
		return 2
	}
	report := struct {
		Env    bench.Environment `json:"env"`
		Result *load.Result      `json:"result"`
	}{bench.CaptureEnvironment(*repo), res}
	load.PrintSummary(stdout, res)
	if *out != "" {
		b, jerr := json.MarshalIndent(report, "", "  ")
		if jerr == nil {
			jerr = os.WriteFile(*out, append(b, '\n'), 0o644)
		}
		if jerr != nil {
			fmt.Fprintf(stderr, "dkvload: writing %s: %v\n", *out, jerr)
			return 1
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "dkvload: interrupted: %v\n", err)
		return 1
	}
	return 0
}
