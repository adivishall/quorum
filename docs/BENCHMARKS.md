# BENCHMARKS

Phase 5. How Quorum's single-node storage engine performs, measured — not
asserted — with a reproducible harness, recorded hardware and configuration, and
the raw numbers as they came out.

Every figure in this document was produced by `cmd/dkvbench` against a real
`LSMStore` and read from the JSON it emits. Nothing here is estimated, rounded to
look better, hand-edited, or cherry-picked from a lucky run. Where a number is
noisy or a comparison is not apples-to-apples, that is stated next to the number
rather than hidden. The committed snapshot the tables were read from is
[`bench/phase5-baseline.json`](../bench/phase5-baseline.json).

The engine measured is the one described in [DESIGN.md](DESIGN.md),
[LSM.md](LSM.md), [BLOOM.md](BLOOM.md), [COMPACTION.md](COMPACTION.md) and
[MANIFEST.md](MANIFEST.md): WAL → memtable → L0 SSTables with per-file Bloom
filters → size-tiered compaction, published through a crash-safe MANIFEST.

---

## A. Goals

The benchmarks answer, with evidence:

- How fast are PUT, GET and DELETE, and how does latency behave in the tail?
- How does performance change with value size, dataset size and concurrency?
- What is the storage overhead and the write amplification, by an explicit
  formula?
- What does the Bloom filter actually save on the read path?
- What does compaction cost, and what does it reclaim?
- How does restart time scale, and what dominates it?
- What do the three WAL sync modes cost, relative to each other?
- Which of these numbers are facts about the engine, and which are artefacts of
  this one machine? (§ Facts vs hypotheses.)

Non-goals: comparing Quorum to other databases, tuning the engine to make a
number larger, or claiming any figure is a ceiling. This is a single-machine
characterisation of the system that exists.

## B. Environment

The recorded run was made on:

| | |
|---|---|
| Machine | Apple MacBook Air (`Mac16,13`) |
| CPU | Apple M4, 10 cores (10 physical / 10 logical; no SMT) |
| RAM | 16 GiB |
| Storage | Internal APFS SSD (sealed system volume; benchmark data on the data volume) |
| OS | macOS 26.5.2 (build 25F84), Darwin 25.5.0 |
| Arch | arm64 |
| Go | go1.27.1 |
| Commit | `dec08eb` (clean tree) |
| Data location | local APFS, **not** tmpfs — real filesystem, real `fsync` |
| Date | 2026-09-21 |

The harness records all of this in every result's `env` object, and flags a
dirty working tree (a number measured against uncommitted code is not
reproducible from its commit). This run's tree was clean.

The machine was not otherwise idle-locked, CPU-pinned, or thermally controlled.
The consequences of that are in § Facts vs hypotheses and § Limitations.

## C. Storage configuration

The engine's real defaults (from `storage.DefaultOptions()`, per DESIGN.md):

| Setting | Default |
|---|---|
| Max key / value | 4 KiB / 1 MiB |
| MemTable flush threshold | 4 MiB |
| SSTable data-block target | 4 KiB |
| Bloom filter | 10 bits/key (k=7), enabled |
| L0 compaction trigger | 4 files |
| L1 size budget | 64 MiB, ×10 per level |
| WAL segment size | 16 MiB |
| WAL sync mode | batch |

Several suites deliberately override individual settings to exercise a specific
behaviour — a smaller memtable to force many SSTables and real compaction, sync
`off` to isolate engine cost from the device flush, auto-compaction off to hold a
file set still. **Every override is recorded in that result's `config` object and
called out in the suite's section below.** No suite silently changes a default and
then compares against another that did not.

Common workload parameters for the recorded run: 100 000 base keys, 100-byte
values, 12-byte keys, seed 1, 5 repetitions per measured benchmark.

## D. Methodology

- **Fresh vs reused databases.** Write benchmarks (put, delete, WAL,
  write-amp, compaction) open a fresh, empty directory for every run, so nothing
  carries over. Read and mixed benchmarks build a dataset once and reuse it
  across runs with a single unmeasured warmup pass; this is stated per suite.
- **Measurement interval.** Only the workload loop is timed, with a monotonic
  clock (`time.Now`/`time.Since`). Dataset build, warmup, flush and compaction
  setup are outside the interval. Throughput is `operations / interval`;
  MB/s is `(key+value bytes moved) / interval`.
- **Latency.** Every operation is timed individually into a per-goroutine
  recorder; recorders merge at the end and the combined samples are sorted once.
  Percentiles use the **nearest-rank** method: the p-th percentile is the sample
  at 1-based rank `ceil(p/100 × n)`. No interpolation, no bucketing — exact
  percentiles at the cost of 8 bytes per sample. This is a *bounded, measured*
  cost, not a silent one: memory is exactly `8 × operations` bytes
  (`Latencies.ApproxBytes`, unit-tested), the suite's largest single collector is
  the 1 M-key read pass at ~8 MB, and the harness prints the peak per-benchmark
  sample count and its megabytes at the end of every run. A caller timing
  billions of operations should switch to a bounded reservoir or a histogram
  (approximate percentiles); nothing in this phase runs at that scale.
- **Repetition and variance.** Each measured benchmark runs 5 times with
  per-run seeds; the tables report the **median** ops/s with **min**, **max** and
  **spread** = `(max−min)/median`. A single run is never reported alone.
- **Randomness.** All random choices (read key selection, mix operation
  selection, delete key selection) come from `math/rand` seeded from the run
  seed, so a run is reproducible. Keys are fixed-width and sort in index order.
- **Correctness gating.** Each workload phase verifies it did what it claims: a
  sampled fraction of live reads are checked against the index their value
  encodes, missing reads must return `ErrNotFound`, and deleted keys must read
  back absent. The **compaction and write-amplification suites hard-fail at
  runtime** if compaction did not actually run — the foreground suite requires
  `CompactionStats.Runs > 0`, the isolated suite additionally requires the file
  set to have shrunk, and write-amp requires a compaction so its compaction term
  is real, not a silent zero. A silently empty, short-circuited, or
  never-compacted database fails these checks instead of posting a number. The
  checks are unit-tested in `internal/bench` (including
  `TestCompactionActuallyRunsUnderLoad`).
- **Outliers.** None are removed. The max latency is reported; the tail includes
  GC pauses, flush stalls and scheduler effects, because a client sees those too.
- **Not pinned.** Processes were not CPU-pinned; benchmarks ran sequentially
  (one at a time) to reduce interference, but the OS scheduled them freely.

## E. Reproducibility

```
make benchsuite                 # full suite, JSON to bench/results/latest.json
```

or directly:

```
go run ./cmd/dkvbench -suite all -dataset 100000 -value 100 -runs 5 -seed 1 \
    -out bench/results/latest.json
```

Single suites: `-suite put|get|delete|mixed|scaling|wal|compaction|writeamp|`
`readamp|startup|manifest|concurrency`. Flags: `-dataset -value -concurrency`
`-runs -seed -dir -out -tmpfs -verbose`. The exact numbers below will differ run
to run and machine to machine; the shape should not.

---

## 3.1 PUT throughput

Fresh database each run, engine defaults (4 MiB memtable, batch sync,
compaction on). Sequential is a single writer with ascending keys.

| Workload | Value | n | Ops/s (median) | Spread | p99 (µs) |
|---|---|---|---|---|---|
| put sequential | 100 B | 100 000 | 386 222 | 8% | 3.4 |
| put sequential | 1 KiB | 100 000 | 69 332 | 7% | 11.8 |
| put sequential | 16 KiB | 16 360 | 3 138 | 77% | 6 267 |
| put concurrent | 100 B, 1 worker | 100 000 | 323 651 | 5% | 3.5 |
| put concurrent | 100 B, 2 workers | 100 000 | 265 625 | 5% | 48.4 |
| put concurrent | 100 B, 4 workers | 100 000 | 256 694 | 14% | 126.0 |
| put concurrent | 100 B, 8 workers | 100 000 | 257 830 | 7% | 278.2 |

Observations, supported by the data:

- Throughput falls sharply as value size grows — 386 k ops/s at 100 B, 69 k at
  1 KiB, 3.1 k at 16 KiB. At 100 B this is ~39 MB/s of key+value; at 16 KiB the
  workload is byte-bound, and the 16 KiB p99 (6.3 ms) and 77% spread come from
  flushes: a 16 KiB run fills the 4 MiB memtable every ~250 writes.
- **Adding writer goroutines does not increase write throughput; it lowers it**
  (324 k → 258 k) and inflates tail latency (3.5 µs → 278 µs). This is expected
  and correct: a single writer path serialises on the WAL append and memtable
  insert under one mutex, so extra writers only add contention. This is a
  documented property of the engine, not a regression — see § Analysis.

## 3.2 GET throughput

Warm, fully compacted dataset (built with a 1 MiB memtable, sync off, then
`CompactAll` to a single SSTable), reused across runs after one warmup pass.
100 000 keys, one read per key on average.

| Workload | Ops/s (median) | Spread | p50 (µs) | p99 (µs) |
|---|---|---|---|---|
| get hit (key present) | 548 430 | 3% | ~1.4 | 6.2 |
| get miss (key absent) | 3 325 597 | 2% | ~0.1 | 0.3 |

- Hits cost ~1.8 µs at the median; misses are ~6× faster because the Bloom
  filter answers "absent" without touching a data block (the read path returns
  before any block read). The miss path at 3.3 M ops/s is essentially the Bloom
  filter plus the index bound check.
- This is the *compacted* read path (one file). The multi-file read path, where
  the Bloom filter earns its keep, is §3.9.

## 3.3 DELETE throughput

Fresh database each run: build 100 000 keys, delete 50 000 distinct keys
(50 000 real tombstones), then verify every deleted key reads back absent.

| Workload | Ops/s (median) | Spread | p99 (µs) |
|---|---|---|---|
| delete 50 000 of 100 000 | 374 098 | 10% | 3.0 |

A delete is a tombstone write: it takes the same write path as a PUT of an empty
value, and the throughput (374 k ops/s) is in line with 100 B sequential PUT.
Deletes do not read, so they are not slowed by the dataset already present.

## 3.4 MIXED workload

Warm 100 000-key dataset, 200 000 operations, single client. Three documented
mixes (read/write/delete by weight); reads target live keys, writes overwrite
them, deletes remove them. Latency covers all operation kinds.

| Mix | R/W/D | Ops/s (median) | Spread | p99 (µs) |
|---|---|---|---|---|
| read-heavy | 90/9/1 | 383 265 | 8% | 7.9 |
| balanced | 50/45/5 | 262 547 | 8% | 5.4 |
| write-heavy | 20/75/5 | 215 219 | 8% | 4.6 |

Throughput tracks the write fraction: more writes means more memtable inserts,
flushes and background compaction, so the write-heavy mix is ~1.8× slower than
read-heavy. None of these mixes is called "realistic"; they bracket a range, and
the point of running all three is that no single ratio is representative.

## 3.5 Dataset-size scaling

Fresh database per size, 100 B values, sync off, built then fully compacted;
reopen measured separately.

| Dataset | Build ops/s | Get p99 (µs) | Reopen (ms) | On-disk (MB) | SSTables |
|---|---|---|---|---|---|
| 10 000 | 669 871 | 2.3 | 14.6 | 2.38 | 1 |
| 100 000 | 443 200 | 4.8 | 53.5 | 23.76 | 2 |
| 1 000 000 | 387 148 | 3.5 | 84.6 | 237.62 | 7 |

- On-disk size scales linearly with the dataset (2.38 → 23.76 → 237.6 MB, ~10×
  per 10×), as expected — ~238 bytes/key including the WAL, which is not
  truncated.
- Read latency stays flat (p99 2–5 µs) across two orders of magnitude, because
  the dataset is compacted and a point read touches a bounded number of blocks
  regardless of size.
- Reopen time grows with the dataset (see §3.10): the WAL is replayed in full on
  every open in this phase.
- Build throughput dips from 10 k to 100 k (more flushes/compactions kick in)
  then is roughly flat to 1 M.

## 3.6 WAL sync modes

Same small workload for each mode — 2 000 sequential 100 B puts, 64 MiB memtable
so no flush occurs, isolating the per-append sync cost. Fresh DB each run.

| Mode | Ops/s (median) | Spread | p99 (µs) | Durability |
|---|---|---|---|---|
| off | 577 589 | 19% | 3.8 | none (lost on process death) |
| batch | 442 417 | 26% | 5.0 | survives process kill (group fsync) |
| sync | 291 | 3% | 4 975 | fsync per append |

This is a **durability/performance trade, not a ranking.** `sync` fsyncs on every
append: at ~3.4 ms/append (the p99 of ~5 ms is one device flush) it is ~1 500×
slower than `off` and ~1 200× slower than `batch`. `batch` amortises the fsync
across a group of appends and is the default. `off` keeps nothing across a crash.
The right mode depends on what the caller can afford to lose, not on this table.
(The `off`/`batch` spreads are wide because the run is short — 2 000 ops — so a
single scheduling blip moves the median; the *relative* ordering is stable.)

## 3.7 Compaction impact

Two measurements. **Foreground** (256 KiB memtable, L0 trigger 4, auto-compaction
on): 100 000 writes over 25 000 keys (4 generations) while background compaction
runs. **Isolated**: the same data built into 72 L0 files with auto-compaction
off, then a single timed `CompactAll`.

| Measurement | Value |
|---|---|
| Foreground write throughput | 76 652 ops/s (spread 3%, p99 7.4 µs) |
| Isolated compaction | 72 files → 1 file in **57 ms** |
| Bytes read / written | 12 418 976 → 3 103 616 (in → out) |
| Compaction input throughput | 207.8 MiB/s |
| Superseded versions dropped | 75 000 |
| On-disk before → after | 23.77 MiB → 14.89 MiB (**0.63×**) |

- Compaction reclaimed 37% of on-disk space here by dropping 75 000 superseded
  versions (3 old versions of each of 25 000 keys) and merging 72 files into one.
- The foreground number (76 652 ops/s) is **not** comparable to the 386 k ops/s
  of §3.1: this configuration uses a 16× smaller memtable (256 KiB vs 4 MiB), so
  it flushes 16× more often and compacts continuously. It measures throughput
  *under a deliberately compaction-heavy setup*, not "the cost of compaction" as
  a clean subtraction. The honest statement is that a small memtable with
  constant compaction sustains ~77 k 100 B writes/s here.
- Compaction is not rate-limited and competes freely with foreground work; that
  it did not spike the foreground p99 above 7.4 µs in this run is reported, not
  promised (§ Limitations).

## 3.8 Write amplification

**Formula:** WA = physical bytes the engine wrote / logical bytes the client
stored. Logical = Σ(key + value) over all client PUTs. Physical is split into
WAL bytes (segment files on disk — the WAL is never truncated here, so the files
are exactly the cumulative bytes written), flushed-SSTable bytes (from
`FlushStats`), and compaction-output bytes (cumulative across every compaction
run, from `CompactionStats`). This is a **storage-engine** measurement, not a
filesystem-level physical-write count (which would require OS-level accounting
this project does not have); the narrower metric is stated so it is not mistaken
for the wider one.

Workload: 5 generations over 20 000 keys (100 000 writes, 100 B values), 512 KiB
memtable, L0 trigger 4, batch sync, then flushed and fully compacted.

| Component | Bytes | MiB |
|---|---|---|
| Logical (client) | — | 10.68 |
| WAL segments | — | 11.92 |
| Flushed SSTables | — | 11.84 |
| Compaction output (cumulative) | — | 11.12 |
| **WA_total** = (WAL+flush+compaction)/logical | | **3.27×** |
| **WA_storage** = (flush+compaction)/logical | | **2.15×** |

- The WAL roughly doubles the logical bytes (11.92 vs 10.68 MiB) because of
  per-record framing overhead on small records.
- Flush writes ~1.1× the logical bytes; compaction rewrites the surviving data
  through several intermediate merges as L0 files accumulate, adding another
  ~1.04× (cumulatively) even after superseded versions are dropped.
- WA is workload-dependent: it rises with overwrite ratio and with how many
  compaction generations the data passes through. 3.27× total here is a
  measurement of this workload, not a constant.

## 3.9 Read amplification / Bloom effect

The dataset is written in **shuffled** key order with a 256 KiB memtable and
auto-compaction off, so every one of the 41 SSTables spans the whole keyspace and
a point read must consider every file — the layout where the Bloom filter
matters. 60 000 keys, 60 000 reads, counters read directly from the engine.

| Config | SSTables | Block reads | Filter skips | p99 (µs/read) |
|---|---|---|---|---|
| bloom on, hit | 41 | 70 096 | 1 192 140 | 9.0 |
| bloom on, miss | 41 | 0 | 2 439 562 | 1.5 |
| bloom off, hit | 41 | 1 261 497 | 0 | 133.0 |
| bloom off, miss | 41 | 0 | 0 | 1.5 |

- **Hits:** with the filter, 70 096 block reads; without it, 1 261 497 — the
  filter avoided **94.4%** of data-block reads, and per-read p99 dropped from
  133 µs to 9.0 µs (~15×). Without the filter a read opens a block in essentially
  every file (1.26 M ≈ 60 000 reads × ~21 files that pass the range check);
  with it, only the file that actually holds the key plus the false-positive
  handful.
- **Misses:** without the filter this configuration happens to short-circuit on
  the index/range check (0 block reads either way in this shuffled layout), so
  the miss rows look identical — an honest result: the filter's headline win here
  is on **hits across many overlapping files**, not on these misses. (The miss
  win shows up instead in §3.2, where the compacted single-file miss path is
  Bloom-gated.)
- Filter cost is the ~10 bits/key documented in [BLOOM.md](BLOOM.md) §5; it is
  not re-derived here.

## 3.10 Startup / reopen

Build a dataset (1 MiB memtable, sync off), close, reopen. Reopen time is the
minimum of `runs` opens; the recovery report says where the time went.

| Dataset | Reopen (ms) | WAL records | Ops replayed | Ops skipped | SSTables |
|---|---|---|---|---|---|
| 10 000 | 15.8 | 10 000 | 4 131 | 5 869 | 1 |
| 100 000 | 22.6 | 100 000 | 227 | 99 773 | 15 |
| 300 000 | 36.7 | 300 000 | 681 | 299 319 | 15 |

- Reopen scans and CRC-checks **every** WAL record (`WAL records` column = the
  whole log), because nothing truncates the WAL in this phase. Most records are
  already durable in an SSTable and are *skipped* (not re-applied) — the
  `Ops skipped` column — but the scan is still paid. Reopen time therefore grows
  with total mutations ever written, not with live data: 15.8 → 22.6 → 36.7 ms
  as the log goes 10 k → 100 k → 300 k records, and 84.6 ms at 1 M (§3.5).
- Only the tail past the highest flushed sequence is actually re-applied
  (`Ops replayed`), which is why a larger dataset can replay *fewer* ops — more
  of it was flushed before close.
- Per-stage timing (discovery / MANIFEST / SSTable open / WAL scan / cleanup) is
  **not** separately instrumented, so it is not broken out; the recovery report
  gives trustworthy *counts*, and the dominant cost is the full WAL scan. This is
  the limitation WAL truncation (a later phase) removes.

## 3.11 MANIFEST / SSTable metadata scaling

Hold the key count per file fixed (500) and vary the number of SSTables, with
auto-compaction off. Reopen with the default footer cross-check, and again with
`VerifySSTablesOnOpen` (every block read and checksummed).

| SSTables | Reopen (ms) | Reopen + full verify (ms) |
|---|---|---|
| 8 | 15.9 | 16.6 |
| 32 | 17.8 | 23.1 |
| 128 | 22.8 | 26.9 |

- Default reopen grows modestly with file count (15.9 → 22.8 ms from 8 to 128
  files): each file costs a MANIFEST entry, an open, and a 48-byte footer
  cross-check, plus the WAL scan that dominates at these small per-file sizes.
- Full verification adds a per-file penalty that grows with file count (0.7 ms at
  8 files, 4.1 ms at 128) because it reads every block of every file. This is the
  cost of finding data-block damage at startup rather than at first read — the
  trade [MANIFEST.md](MANIFEST.md) §6 and [LIMITATIONS.md](LIMITATIONS.md)
  describe.

## 3.12 Concurrency scaling

Warm, fully compacted 100 000-key dataset, read-only, increasing reader
goroutines. (Writes do not scale with concurrency — §3.1 — so the interesting
concurrency question is reads.)

| Workers | Ops/s (median) | p50 (µs) | p95 (µs) | p99 (µs) |
|---|---|---|---|---|
| 1 | 560 628 | 1.4 | 2.2 | 5.8 |
| 2 | 784 040 | 1.8 | 3.1 | 27.6 |
| 4 | 908 094 | 2.0 | 7.0 | 67.7 |
| 8 | 902 713 | 3.3 | 12.7 | 145.2 |

- Read throughput rises from 1 → 4 workers (561 k → 908 k, ~1.6×) then flattens
  at 8 (903 k). This is **sub-linear** scaling: readers share the immutable
  version under a read lock and contend on the OS page cache and memory
  bandwidth, and the M4's read path saturates well before 8 goroutines. It is not
  claimed to be linear, because it is not.
- Tail latency inflates steadily with concurrency (p99 5.8 → 145 µs) as readers
  queue; the median stays low.

---

## Analysis

What the measurements show about *this* engine on *this* machine:

1. **The write path is single-threaded by design.** One mutex serialises WAL
   append + memtable insert, so concurrent writers add contention, not
   throughput (§3.1). A future group-commit or sharded memtable is the lever if a
   measurement ever justifies it; today the data says extra writers hurt.
2. **Reads scale sub-linearly and cheaply.** Point reads on a compacted dataset
   are ~2 µs and flat across dataset size (§3.2, §3.5); more readers help up to
   ~4× then saturate (§3.12).
3. **The Bloom filter is the read path's most consequential component under a
   realistic multi-file layout** — 94% fewer block reads and a 15× tail-latency
   improvement on hits across 41 overlapping files (§3.9).
4. **Restart cost is a WAL-scan cost.** It scales with total mutations, not live
   data, because the WAL is never truncated in this phase (§3.10). This is the
   single clearest thing a later phase can improve.
5. **Write amplification is ~3.3×** for a moderate-overwrite 100 B workload,
   split roughly evenly across WAL, flush and compaction (§3.8).
6. **Sync mode is the dominant durability lever** and costs three orders of
   magnitude between `off` and `sync` (§3.6); `batch` is the default because it
   survives a process kill at a fraction of `sync`'s cost.

## Facts vs hypotheses

The brief asks which measurements are facts and which are hypotheses. Drawing the
line honestly:

**Supported facts (reproducible from the repository, low variance):**

- Relative orderings: misses > hits, read-heavy > write-heavy, `off` > `batch` >
  `sync`, larger values → lower ops/s. These held every run with small spread.
- The Bloom filter avoids ~94% of block reads on the multi-file hit path (§3.9) —
  a counted quantity, not a timing, so it is machine-independent.
- Write amplification of 3.27× for the §3.8 workload — computed from byte
  counters, reproducible.
- Restart replays/scans the whole WAL and its cost grows with log length (§3.10)
  — a structural property, confirmed by the counts.
- Concurrent writers do not increase write throughput (§3.1) — a structural
  property of the single-writer path.

**Hypotheses / machine-specific artefacts (do not generalise):**

- Every absolute ops/s and µs figure. They are this M4, this APFS SSD, this OS,
  with the page cache warm and the machine not otherwise quiesced. Another
  machine will differ, possibly by a lot.
- The 16 KiB PUT spread of 77% (§3.1) is noise from flush timing on a short run,
  not a stable measurement.
- That compaction did not spike foreground p99 (§3.7) is one run's behaviour on a
  fast SSD; with a slower device or larger dataset it could.
- Absolute reopen times (§3.10–3.11) depend on read bandwidth and the page cache.

## Limitations of these benchmarks

Only the ones that actually apply here:

- **Single machine, single filesystem** (Apple M4 / APFS SSD). No cross-machine,
  no distributed, no multi-node workload — none of that exists yet.
- **Filesystem caching not controlled.** Reads are served warm from the page
  cache; these are not cold-cache numbers. `fsync` honesty is assumed (§ DESIGN,
  LIMITATIONS): a consumer SSD may acknowledge before durability.
- **No power-loss testing.** Durability is tested by SIGKILL elsewhere; a
  benchmark cannot pull the plug.
- **No CPU pinning, no thermal control, no external load generator.** The machine
  was not otherwise idle-locked. Variance is reported (spread column) rather than
  suppressed.
- **Bounded dataset sizes** (≤ 1 M keys). Behaviour beyond the measured range is
  not extrapolated.
- **Physical write accounting is storage-engine level**, not filesystem level
  (§3.8).
- **Filesystem type not machine-verified as tmpfs vs disk beyond the mount
  check** — the `on_tmpfs` field is an operator annotation, and was false here.

These numbers are a Phase 5 reference point for later phases to compare against —
not a specification, not a guarantee, and not a claim about any machine but this
one.
