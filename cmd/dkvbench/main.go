// Command dkvbench runs Quorum's storage benchmarks and emits machine-readable
// results plus a human-readable summary. It is the orchestrator for Phase 5;
// the methodology and the recorded numbers live in docs/BENCHMARKS.md.
//
// Every benchmark opens a real LSMStore in a fresh directory, drives a defined
// workload through the public Store API, and records what happened — timing from
// a monotonic clock and storage counters from the engine. Nothing is estimated.
//
// Usage:
//
//	dkvbench -suite <name> [flags]
//
// Suites: put, get, delete, mixed, scaling, wal, compaction, writeamp, readamp,
// startup, manifest, concurrency, all. See -h for flags.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/storage/wal"
)

type harness struct {
	baseDir     string
	repoDir     string
	env         bench.Environment
	seed        int64
	runs        int
	dataset     int
	valueSize   int
	concurrency int
	onTmpfs     bool
	verbose     bool
	results     []bench.Result
	peakLatN    int // largest per-benchmark latency-sample count seen
	out         io.Writer
}

func main() {
	var (
		suite   = flag.String("suite", "all", "which suite: put,get,delete,mixed,scaling,wal,compaction,writeamp,readamp,startup,manifest,concurrency,all")
		dataset = flag.Int("dataset", 100_000, "base dataset size (keys)")
		value   = flag.Int("value", 100, "value size in bytes for the general suites")
		conc    = flag.Int("concurrency", 1, "writer/reader goroutines for single-config suites")
		runs    = flag.Int("runs", 3, "repetitions of each measured benchmark")
		seed    = flag.Int64("seed", 1, "base random seed")
		dir     = flag.String("dir", filepath.Join(os.TempDir(), "dkvbench"), "base directory for benchmark data")
		out     = flag.String("out", "", "write JSON results array to this file (also printed to stdout unless -quiet)")
		quiet   = flag.Bool("quiet", false, "suppress the JSON dump on stdout")
		verbose = flag.Bool("verbose", false, "print per-run lines, not just summaries")
		tmpfs   = flag.Bool("tmpfs", false, "record that -dir is on tmpfs/ram (annotation only; does not mount anything)")
	)
	flag.Parse()

	repoDir, _ := os.Getwd()
	h := &harness{
		baseDir:     *dir,
		repoDir:     repoDir,
		env:         bench.CaptureEnvironment(repoDir),
		seed:        *seed,
		runs:        *runs,
		dataset:     *dataset,
		valueSize:   *value,
		concurrency: *conc,
		onTmpfs:     *tmpfs,
		verbose:     *verbose,
		out:         os.Stdout,
	}
	if err := os.MkdirAll(h.baseDir, 0o755); err != nil {
		fatal(err)
	}
	defer func() { _ = os.RemoveAll(h.baseDir) }()

	h.printEnv()

	start := time.Now()
	if err := h.dispatch(*suite); err != nil {
		fatal(err)
	}
	fmt.Fprintf(h.out, "\nran %d result(s) in %s\n", len(h.results), time.Since(start).Round(time.Millisecond))
	// Measured, not assumed: the largest single latency collector this run used.
	// 8 B/sample is the payload; real footprint is somewhat larger (slice growth,
	// per-worker slices). A run stays practical as long as this stays small.
	fmt.Fprintf(h.out, "peak latency samples in one benchmark: %d (%.1f MiB payload at 8 B/sample)\n",
		h.peakLatN, float64(h.peakLatN*8)/(1<<20))

	if *out != "" {
		if err := h.writeResults(*out); err != nil {
			fatal(err)
		}
		fmt.Fprintf(h.out, "wrote %s\n", *out)
	}
	if !*quiet && *out == "" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(h.results)
	}
}

func (h *harness) dispatch(suite string) error {
	switch suite {
	case "put":
		return h.suitePut()
	case "get":
		return h.suiteGet()
	case "delete":
		return h.suiteDelete()
	case "mixed":
		return h.suiteMixed()
	case "scaling":
		return h.suiteScaling()
	case "wal":
		return h.suiteWAL()
	case "compaction":
		return h.suiteCompaction()
	case "writeamp":
		return h.suiteWriteAmp()
	case "readamp":
		return h.suiteReadAmp()
	case "startup":
		return h.suiteStartup()
	case "manifest":
		return h.suiteManifest()
	case "concurrency":
		return h.suiteConcurrency()
	case "all":
		for _, fn := range []func() error{
			h.suitePut, h.suiteGet, h.suiteDelete, h.suiteMixed,
			h.suiteWAL, h.suiteCompaction, h.suiteWriteAmp, h.suiteReadAmp,
			h.suiteStartup, h.suiteManifest, h.suiteConcurrency, h.suiteScaling,
		} {
			if err := fn(); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown suite %q", suite)
	}
}

// record attaches the shared metadata to a phase's result and stores it.
func (h *harness) record(r bench.Result, cfg bench.Config, seed int64, runIdx int, sm bench.StorageMetrics, note string) bench.Result {
	r.Env = h.env
	r.Config = cfg
	r.Seed = seed
	r.RunIndex = runIdx
	r.Storage = sm
	r.Note = note
	if r.Latency.Count > h.peakLatN {
		h.peakLatN = r.Latency.Count
	}
	h.results = append(h.results, r)
	if h.verbose {
		fmt.Fprintf(h.out, "  run %d: %s  %.0f ops/s  p50=%.1f p99=%.1fus\n",
			runIdx, r.Benchmark, r.OpsPerSec, r.Latency.P50US, r.Latency.P99US)
	}
	return r
}

func (h *harness) writeResults(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(h.results)
}

func (h *harness) printEnv() {
	e := h.env
	dirty := ""
	if e.GitDirty {
		dirty = " (dirty tree — results not reproducible from this commit)"
	}
	fmt.Fprintf(h.out, "dkvbench\n")
	fmt.Fprintf(h.out, "  host    %s  %s/%s\n", e.Hostname, e.OS, e.Arch)
	fmt.Fprintf(h.out, "  cpu     %s  (%d logical", e.CPUModel, e.CPUsLogical)
	if e.CPUsPhysical > 0 {
		fmt.Fprintf(h.out, ", %d physical", e.CPUsPhysical)
	}
	fmt.Fprintf(h.out, ")\n")
	if e.RAMBytes > 0 {
		fmt.Fprintf(h.out, "  ram     %.1f GiB\n", float64(e.RAMBytes)/(1<<30))
	}
	fmt.Fprintf(h.out, "  go      %s\n", e.GoVersion)
	fmt.Fprintf(h.out, "  commit  %s%s\n", short(e.GitCommit), dirty)
	fmt.Fprintf(h.out, "  data    %s (tmpfs=%v)\n", h.baseDir, h.onTmpfs)
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// syncModeName parses a sync-mode flag value.
func syncModeName(s string) (wal.SyncMode, error) {
	switch s {
	case "off":
		return wal.SyncOff, nil
	case "batch":
		return wal.SyncBatch, nil
	case "sync", "always":
		return wal.SyncAlways, nil
	default:
		return 0, fmt.Errorf("unknown sync mode %q (off|batch|sync)", s)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "dkvbench:", err)
	os.Exit(1)
}
