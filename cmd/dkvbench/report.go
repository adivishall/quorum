package main

import (
	"fmt"

	"github.com/adivishall/quorum/internal/bench"
)

// section prints a titled divider for a suite's human-readable output.
func (h *harness) section(title string) {
	fmt.Fprintf(h.out, "\n== %s ==\n", title)
}

// aggRow prints one aggregate line: throughput (median with spread) and tail
// latency, so the human summary shows variance, not a single number.
func (h *harness) aggRow(label string, a bench.Aggregate) {
	fmt.Fprintf(h.out, "  %-40s %10.0f ops/s  (min %.0f, max %.0f, spread %.0f%%)  p99 %.1f us  [%d runs]\n",
		label, a.OpsPerSecMed, a.OpsPerSecMin, a.OpsPerSecMax, a.SpreadPct, a.P99USMed, a.Runs)
}

// runRepeated runs fn h.runs times with per-run seeds, records every run, and
// prints the aggregate. fn returns the measured result (its Benchmark set to
// label) and a storage snapshot.
func (h *harness) runRepeated(label string, cfg bench.Config, note string, fn func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error)) (bench.Aggregate, error) {
	return h.runRepeatedOpts(label, cfg, note, true, fn)
}

// runRepeatedOpts is runRepeated with control over whether it prints its own
// aggregate row. A suite that renders its own table (concurrency) passes false
// and prints the row itself, avoiding a duplicate line.
func (h *harness) runRepeatedOpts(label string, cfg bench.Config, note string, print bool, fn func(runIdx int, seed int64) (bench.Result, bench.StorageMetrics, error)) (bench.Aggregate, error) {
	var set bench.RunSet
	set.Benchmark = label
	for i := 0; i < h.runs; i++ {
		seed := h.seed + int64(i)
		r, sm, err := fn(i, seed)
		if err != nil {
			return bench.Aggregate{}, fmt.Errorf("%s run %d: %w", label, i, err)
		}
		r.Benchmark = label
		r = h.record(r, cfg, seed, i, sm, note)
		set.Add(r)
	}
	a := set.Summary()
	if print {
		h.aggRow(label, a)
	}
	return a, nil
}

// single records a one-shot measurement (no repetition) and prints a line. Used
// for measurements whose point is the storage counters, not throughput variance.
func (h *harness) single(label string, cfg bench.Config, note string, r bench.Result, sm bench.StorageMetrics) bench.Result {
	r.Benchmark = label
	return h.record(r, cfg, h.seed, 0, sm, note)
}
