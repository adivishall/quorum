# bench

Phase 5 benchmark harness output and reproducibility notes.

The benchmark code lives in:

- `internal/bench/` — the reusable harness library (workloads, latency,
  environment capture, result format), unit-tested for correctness.
- `cmd/dkvbench/` — the orchestrator command that drives a real `LSMStore`
  through each workload and emits results.

The methodology, configuration and recorded numbers are in
[`docs/BENCHMARKS.md`](../docs/BENCHMARKS.md).

## Reproducing

```
make benchsuite
```

writes a JSON results array to `bench/results/latest.json` (git-ignored) and
prints a human-readable summary. Override the defaults, e.g.:

```
make benchsuite DATASET=200000 VALUE=1024 RUNS=7 BENCHOUT=bench/results/big.json
```

Or run one suite directly:

```
go run ./cmd/dkvbench -suite compaction -dataset 100000 -runs 5
```

Suites: `put get delete mixed scaling wal apply compaction writeamp readamp
startup manifest concurrency all`.

## Files

- `phase5-baseline.json` — the committed reference snapshot the numbers in
  `docs/BENCHMARKS.md` were read from, so a future phase can compare. It is one
  run on one machine; treat it as a reference point, not a guarantee.
- `s1-apply.json` — `dkvbench -suite apply -runs 3` at commit `30b84cd` (Apple M4,
  macOS, APFS): the cost of S1's atomic apply batches against the legacy
  Put + SetAppliedIndex pair (`docs/STORAGE_INTEGRATION.md` §7.10).
- `results/` — git-ignored scratch output from local runs.
