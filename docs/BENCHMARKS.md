# BENCHMARKS

Phase 5. How Quorum's single-node storage engine performs, measured — not
asserted — with a reproducible harness, recorded hardware and configuration, and
the raw numbers as they came out.

Every figure in this document was produced by `cmd/dkvbench` against a real
`LSMStore` and read from the JSON it emits. Nothing here is estimated, rounded to
look better, hand-edited, or cherry-picked from a lucky run. Where a number is
noisy or a comparison is not apples-to-apples, that is stated next to the number
rather than hidden. The committed snapshot the tables were read from is
[`bench/phase5-baseline.json`](../bench/phase5-baseline.json) (seed 1, commit
`06e5560`).

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
| Commit | `06e5560` (clean tree) |
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
| WAL batch threshold (`SyncBytes`) | 1 MiB, or 100 ms, whichever first |

Several suites deliberately override individual settings to exercise a specific
behaviour — a smaller memtable to force many SSTables and real compaction, sync
`off` to isolate engine cost from the device flush, auto-compaction off to hold a
file set still, and (WAL suite only) a **reduced `SyncBytes` of 16 KiB** so a
short workload actually crosses the batch-flush threshold inside the timed
interval. **Every override is recorded in that result's `config` object and
called out in the suite's section below.** No suite silently changes a default and
then compares against another that did not.

Common workload parameters for the recorded run: 100 000 base keys, 100-byte
values, 12-byte keys, seed 1, 5 repetitions per measured timing benchmark.

## D. Methodology

- **Fresh vs reused databases.** Write and mutate benchmarks (put, delete,
  mixed, WAL, write-amp, compaction) open a fresh, empty directory for every
  run, so nothing carries over — the mixed suite in particular rebuilds the same
  populated baseline before each run, because its writes and deletes would
  otherwise leave run *n+1* looking at whatever run *n* did. Read-only benchmarks
  (get, concurrency) build a dataset once and reuse it across runs with a single
  unmeasured warmup pass; this is stated per suite.
- **Setup is never timed.** Only the workload loop is timed, with a monotonic
  clock (`time.Now`/`time.Since`). Dataset build, warmup, flush and compaction
  setup are outside the interval. Throughput is `operations / interval`;
  MB/s is `(bytes moved) / interval`, where "bytes moved" is accounted **per
  operation** — a put moves key+value, a delete moves only its key, a read moves
  its key plus whatever value came back (zero on a miss).
- **Latency.** Every operation is timed individually into a per-goroutine
  recorder; recorders merge at the end and the combined samples are sorted once.
  Percentiles use the **nearest-rank** method: the p-th percentile is the sample
  at 1-based rank `ceil(p/100 × n)`. No interpolation, no bucketing — exact
  percentiles at the cost of an 8-byte sample payload per operation. This is a
  *bounded, measured* cost, not a silent one: the payload is `8 × operations`
  bytes (`Latencies.ApproxBytes`, unit-tested; the real footprint is somewhat
  larger — slice growth, per-worker slices), the suite's largest single collector
  is the 1 M-key read pass at ~7.6 MiB, and the harness prints the peak
  per-benchmark sample count at the end of every run. A caller timing billions of
  operations should switch to a bounded reservoir or a histogram; nothing here
  runs at that scale.
- **Repeated-timing vs single-shot measurements — which is which.** The two are
  not treated the same, and the document does not pretend they are:
  - *Repeated timing, median + variance* — put, get, delete, mixed, WAL,
    concurrency, the **dataset-scaling build** throughput, and the startup and
    MANIFEST **reopen** times: run 5× (reopens per size), reported as **median**
    ops/s (or ms) with **min**, **max** and **spread** = `(max−min)/median`. Every
    run is recorded in the JSON. Build throughput is a wall-clock timing like any
    other, so §3.5 measures it 5× per size (fresh DB, identical deterministic
    workload each run) rather than once.
  - *Single-shot structural / counter measurements* — write amplification, the
    Bloom block-read and filter-skip counts, the isolated `CompactAll` byte
    movement, and the dataset-scaling **on-disk sizes** (and the read-latency and
    reopen figures sampled alongside them in §3.5): these are exact byte- and
    count-based quantities, or structural properties of a deterministic build, so
    they are measured once and reported as-is, not dressed up with a fake
    distribution.
  The startup suite specifically reports the **median** reopen, not the minimum,
  and preserves every reopen in the JSON.
- **Randomness.** All random choices (read key selection, mix operation
  selection, delete key selection) come from `math/rand` seeded from the run
  seed, so a run is reproducible. Keys are fixed-width and sort in index order.
- **Correctness gating.** Each workload phase verifies it did what it claims: a
  sampled fraction of live reads are checked against the index their value
  encodes, missing reads must return `ErrNotFound`, and deleted keys must read
  back absent. The **compaction and write-amp suites hard-fail at runtime** if
  compaction did not actually run, and the **WAL suite asserts the fsync counter**
  matches each mode's contract (off = 0, always = one per append, batch = several)
  so a run that never batched cannot be reported as a batch result. The checks are
  unit-tested in `internal/bench` and `internal/storage/wal`.
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

The tables were read from a seed-1 run (`bench/phase5-baseline.json`); an
independent seed-2 run on the same machine reproduced every **deterministic**
quantity exactly — write amplification 3.27×, compaction 72→1 files / 0.63×
on-disk, the Bloom block-read counts (≈70 k with the filter vs ≈1.26 M without),
the WAL fsync counts (0 / 15 / 2 000), and the on-disk sizes — with the timing
quantities inside their reported spread. That is the intended reproducibility:
the byte- and count-based results are exact, the wall-clock results are stable in
shape and bounded in variance.

---

## 3.1 PUT throughput

Fresh database each run, engine defaults (4 MiB memtable, batch sync,
compaction on). Sequential is a single writer with ascending keys.

| Workload | Value | n | Ops/s (median) | Spread | p99 (µs) |
|---|---|---|---|---|---|
| put sequential | 100 B | 100 000 | 380 380 | 7% | 3.4 |
| put sequential | 1 KiB | 100 000 | 65 990 | 3% | 11.8 |
| put sequential | 16 KiB | 16 360 | 3 088 | 69% | 6 289 |
| put concurrent | 100 B, 1 worker | 100 000 | 302 217 | 10% | 3.8 |
| put concurrent | 100 B, 2 workers | 100 000 | 261 345 | 6% | 48.7 |
| put concurrent | 100 B, 4 workers | 100 000 | 259 588 | 11% | 127.9 |
| put concurrent | 100 B, 8 workers | 100 000 | 252 510 | 2% | 280.7 |

Observations, supported by the data:

- Throughput falls sharply as value size grows — 380 k ops/s at 100 B, 66 k at
  1 KiB, 3.1 k at 16 KiB. At 100 B this is ~41 MB/s of key+value; at 16 KiB the
  workload is byte-bound, and the 16 KiB p99 (6.3 ms) and 69% spread come from
  flushes: a 16 KiB run fills the 4 MiB memtable every ~250 writes.
- **Adding writer goroutines does not increase write throughput; it lowers it**
  (302 k → 253 k) and inflates tail latency (3.8 µs → 281 µs). This is expected
  and correct: a single writer path serialises on the WAL append and memtable
  insert under one mutex, so extra writers only add contention. This is a
  documented property of the engine, not a regression — see § Analysis.

## 3.2 GET throughput

Warm, fully compacted dataset (built with a 1 MiB memtable, sync off, then
`CompactAll` to a single SSTable), reused across runs after one warmup pass.
100 000 keys, one read per key on average.

| Workload | Ops/s (median) | Spread | p50 (µs) | p99 (µs) |
|---|---|---|---|---|
| get hit (key present) | 567 107 | 3% | 1.4 | 5.1 |
| get miss (key absent) | 3 451 494 | 2% | 0.2 | 0.3 |

- Hits cost 1.4 µs at the median; misses are ~6× faster (0.2 µs median) because
  the Bloom filter answers "absent" without touching a data block — the read path
  returns before any block read. The miss path at 3.5 M ops/s is essentially the
  Bloom filter plus the index bound check.
- This is the *compacted* read path (one file). The multi-file read path, where
  the Bloom filter earns its keep, is §3.9.

## 3.3 DELETE throughput

Fresh database each run: build 100 000 keys, delete 50 000 distinct keys
(50 000 real tombstones), then verify every deleted key reads back absent.

| Workload | Ops/s (median) | Spread | p99 (µs) |
|---|---|---|---|
| delete 50 000 of 100 000 | 328 060 | 11% | 3.2 |

A delete is a tombstone write: it takes the same write path as a PUT of an empty
value, and the throughput (328 k ops/s) is in line with 100 B sequential PUT.
Deletes do not read, so they are not slowed by the dataset already present.

## 3.4 MIXED workload

Each run rebuilds the same populated 100 000-key baseline (outside the timed
interval), then runs 200 000 operations under the mix — so the five runs are
comparable rather than drifting as writes and deletes accumulate. Three
documented mixes (read/write/delete by weight); MB/s is accounted per operation
(a read moves its value out, a write moves key+value in, a delete moves only its
key). Latency covers all operation kinds.

| Mix | R/W/D | Ops/s (median) | Spread | p99 (µs) |
|---|---|---|---|---|
| read-heavy | 90/9/1 | 384 733 | 2% | 7.9 |
| balanced | 50/45/5 | 239 923 | 24% | 5.8 |
| write-heavy | 20/75/5 | 190 427 | 35% | 4.8 |

Throughput tracks the write fraction: more writes means more memtable inserts,
flushes and background compaction, so the write-heavy mix is ~2× slower than
read-heavy. None of these mixes is called "realistic"; they bracket a range, and
the point of running all three is that no single ratio is representative.

## 3.5 Dataset-size scaling

Fresh database per size **and per run**, 100 B values, sync off. Build
throughput is a wall-clock timing, so it is the **median of 5 runs** (min/max/
spread in the JSON, like every other timing suite); the get-p99, reopen and
on-disk figures are structural samples taken from the representative (last)
build, which is byte-identical across runs because the sequential build is
deterministic — §3.2 and §3.10 carry the repeated-timing get and reopen numbers.
On-disk is MiB (2²⁰ bytes).

| Dataset | Build ops/s (median) | Spread | Get p99 (µs) | Reopen (ms) | On-disk (MiB) | SSTables |
|---|---|---|---|---|---|---|
| 10 000 | 686 479 | 3% | 2.2 | 18.0 | 2.38 | 1 |
| 100 000 | 465 214 | 6% | 5.1 | 25.3 | 23.76 | 2 |
| 1 000 000 | 372 056 | 14% | 3.5 | 94.4 | 237.62 | 7 |

- On-disk size scales linearly with the dataset (2.38 → 23.76 → 237.6 MiB, ~10×
  per 10×). At 1 M keys the footprint is **≈249 bytes/key** (124.2 MB of SSTable
  + 125.0 MB of WAL, decimal). Note the WAL is roughly **half** of that and is as
  large as the live SSTable data — because it is never truncated in this phase
  (§3.10), the on-disk cost is inflated by a full copy of every mutation's log.
- Read latency stays flat (p99 2–5 µs) across two orders of magnitude, because
  the dataset is compacted and a point read touches a bounded number of blocks
  regardless of size.
- Reopen time grows with the dataset (see §3.10): the WAL is scanned in full on
  every open in this phase.
- Build throughput falls as the dataset grows (686 k → 465 k → 372 k median) as
  more flushes and compaction kick in; the 1 M run has the widest spread (14%),
  from compaction timing over the longer run. Every build run is in the JSON.

## 3.6 WAL sync modes

Same small workload for each mode — 2 000 sequential 100 B puts, 64 MiB memtable
so no memtable flush occurs, isolating the per-append WAL sync cost. Fresh DB
each run. The batch arm runs with a **reduced `SyncBytes` of 16 KiB** (a
documented benchmark configuration, not a production change): the default 1 MiB
threshold would not be crossed by this 240 KB workload, so "batch" would never
actually flush inside the timed interval. The WAL's fsync counter confirms what
each mode did.

| Mode | Ops/s (median) | Spread | p99 (µs) | fsyncs / 2 000 appends | Durability |
|---|---|---|---|---|---|
| off | 539 029 | 14% | 4.2 | **0** | survives process kill; nothing forced to disk |
| batch | 25 879 | 18% | 57.9 | **15** | survives process kill; a bounded window reaches the device |
| sync | 239 | 11% | 6 540 | **2 000** | fsync per append |

This is a **durability/performance trade, not a ranking**, and the fsync counts
are the proof each arm did its job. `off` never fsyncs (the counter is 0); its
appends still reach the kernel, so they **survive process death (SIGKILL)** — the
project's documented WAL semantics — but nothing is forced to the device, so a
power loss or OS crash could lose them. `batch` fsynced 15 times as the 16 KiB
threshold was crossed, costing ~20× the throughput of `off` but bounding how much
un-flushed data a device-level failure could lose. `sync` fsynced on every append
(2 000 times), at ~4 ms/append (the p99 of ~6.5 ms is one device flush), ~108×
slower than `batch`. **All three survive process death**; they differ in flushing
to the device, and power-loss durability is untested for any of them
([WAL.md](WAL.md) §9). The right mode depends on what a caller can afford to lose
to a *power* failure, not on this table.

(The spreads are wide — 11–18% — because the run is short, 2 000 ops, so a single
scheduling blip moves the median; the relative ordering across modes is stable.)

## 3.7 Compaction impact

Two measurements. **Foreground** (256 KiB memtable, L0 trigger 4, auto-compaction
on): 100 000 writes over 25 000 keys (4 generations) while background compaction
runs; the run asserts a compaction actually happened. **Isolated**: the same data
built into 72 L0 files with auto-compaction off, then a single timed `CompactAll`
that asserts the file set shrank.

| Measurement | Value |
|---|---|
| Foreground write throughput | 62 595 ops/s (spread 10%, p99 9.5 µs) |
| Isolated compaction | 72 files → 1 file in **56 ms** |
| Bytes read / written | 12 418 976 → 3 103 616 (in → out) |
| Compaction input throughput | 211.2 MiB/s |
| Superseded versions dropped | 75 000 |
| On-disk before → after | 23.77 MiB → 14.89 MiB (**0.63×**) |

- Compaction reclaimed 37% of on-disk space here by dropping 75 000 superseded
  versions (3 old versions of each of 25 000 keys) and merging 72 files into one.
- The foreground number (62 595 ops/s) is **not** comparable to the 380 k ops/s
  of §3.1: this configuration uses a 16× smaller memtable (256 KiB vs 4 MiB), so
  it flushes 16× more often and compacts continuously. It measures throughput
  *under a deliberately compaction-heavy setup*, not "the cost of compaction" as
  a clean subtraction. The honest statement is that a small memtable with
  constant compaction sustains ~63 k 100 B writes/s here.
- Compaction is not rate-limited and competes freely with foreground work; that
  it did not spike the foreground p99 above 9.5 µs in this run is reported, not
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
| Logical (client) | 11 200 000 | 10.68 |
| WAL segments | 12 500 000 | 11.92 |
| Flushed SSTables | 12 415 880 | 11.84 |
| Compaction output (cumulative) | 11 663 072 | 11.12 |
| **WA_total** = (WAL+flush+compaction)/logical | | **3.27×** |
| **WA_storage** = (flush+compaction)/logical | | **2.15×** |

- The WAL adds ~12% over the logical bytes (12.50 vs 11.20 MB, a ratio of
  **1.12×**) — per-record framing overhead on small records, not a second full
  copy.
- Flush writes ~1.11× the logical bytes; compaction rewrites the surviving data
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
| bloom on, hit | 41 | 70 096 | 1 192 140 | 8.7 |
| bloom on, miss | 41 | 0 | 2 439 562 | 1.5 |
| bloom off, hit | 41 | 1 261 497 | 0 | 130.0 |
| bloom off, miss | 41 | 0 | 0 | 1.5 |

- **Hits:** with the filter, 70 096 block reads; without it, 1 261 497 — the
  filter avoided **94.4%** of data-block reads, and per-read p99 dropped from
  130 µs to 8.7 µs (~15×). Without the filter a read opens a block in essentially
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

Build a dataset (1 MiB memtable, sync off), close, reopen 5×. The **median**
reopen is reported with min/max/spread (every reopen is in the JSON); the
recovery report says where the time went.

| Dataset | Reopen med (ms) | min | max | spread | WAL records | Ops replayed | SSTables |
|---|---|---|---|---|---|---|---|
| 10 000 | 16.0 | 15.0 | 17.7 | 17% | 10 000 | 4 131 | 1 |
| 100 000 | 24.8 | 23.8 | 24.9 | 4% | 100 000 | 227 | 5 |
| 300 000 | 38.5 | 37.3 | 40.1 | 7% | 300 000 | 681 | 15 |

- Reopen scans and CRC-checks **every** WAL record (`WAL records` column = the
  whole log), because nothing truncates the WAL in this phase. Most records are
  already durable in an SSTable and are *skipped* (not re-applied) — the
  `Ops replayed` column is small — but the scan is still paid. Reopen time
  therefore grows with total mutations ever written, not with live data: 16.0 →
  24.8 → 38.5 ms as the log goes 10 k → 100 k → 300 k records, and 94.4 ms at 1 M
  (§3.5).
- Only the tail past the highest flushed sequence is actually re-applied, which
  is why a larger dataset can replay *fewer* ops — more of it was flushed before
  close.
- Per-stage timing (discovery / MANIFEST / SSTable open / WAL scan / cleanup) is
  **not** separately instrumented, so it is not broken out; the recovery report
  gives trustworthy *counts*, and the dominant cost is the full WAL scan. This is
  the limitation WAL truncation (a later phase) removes.

## 3.11 MANIFEST / SSTable metadata scaling

Hold the **total logical data constant** at 64 000 keys and vary only how many
SSTables it is split across, so the experiment isolates per-file metadata cost
rather than confounding it with a larger dataset (and a longer WAL). Reopen with
the default footer cross-check, and again with `VerifySSTablesOnOpen` (every
block read and checksummed); both are medians of 5 reopens.

| SSTables | Keys/file | Reopen med (ms) | Reopen + full verify (ms) |
|---|---|---|---|
| 8 | 8 000 | 20.8 | 28.0 |
| 32 | 2 000 | 23.8 | 29.1 |
| 128 | 500 | 28.4 | 32.2 |

- With total data fixed, default reopen still grows with file count (20.8 → 28.4
  ms from 8 to 128 files): each additional file costs a MANIFEST entry, an open,
  and a 48-byte footer cross-check. The growth is modest — ~7.6 ms across a 16×
  increase in file count — because the constant full-WAL scan dominates the
  absolute time.
- Full verification adds ~4–7 ms on top (7.2 ms at 8 files, 3.7 ms at 128).
  With total data fixed it reads about the same total bytes regardless of how
  many files they are split into, so this cost is roughly flat; the run-to-run
  scatter (the 128-file default reopen has a 42% spread) is larger than any
  trend across the rows. It is the cost of finding data-block damage at startup
  rather than at first read — the trade [MANIFEST.md](MANIFEST.md) §6 and
  [LIMITATIONS.md](LIMITATIONS.md) describe.

## 3.12 Concurrency scaling

Warm, fully compacted 100 000-key dataset, read-only, increasing reader
goroutines. (Writes do not scale with concurrency — §3.1 — so the interesting
concurrency question is reads.)

| Workers | Ops/s (median) | p50 (µs) | p95 (µs) | p99 (µs) |
|---|---|---|---|---|
| 1 | 564 931 | 1.4 | 2.2 | 5.0 |
| 2 | 805 768 | 1.8 | 3.0 | 27.7 |
| 4 | 917 458 | 2.1 | 5.7 | 68.9 |
| 8 | 881 846 | 3.5 | 12.2 | 151.2 |

- Read throughput rises from 1 → 4 workers (565 k → 917 k, ~1.6×) then flattens —
  in fact dips slightly — at 8 (882 k). This is **sub-linear** scaling: readers
  share the immutable version under a read lock and contend on the OS page cache
  and memory bandwidth, and the M4's read path saturates well before 8
  goroutines. It is not claimed to be linear, because it is not.
- Tail latency inflates steadily with concurrency (p99 5.0 → 151 µs) as readers
  queue; the median stays low.

---

## Analysis

What the measurements show about *this* engine on *this* machine:

1. **The write path is single-threaded by design.** One mutex serialises WAL
   append + memtable insert, so concurrent writers add contention, not
   throughput (§3.1). A future group-commit or sharded memtable is the lever if a
   measurement ever justifies it; today the data says extra writers hurt.
2. **Reads scale sub-linearly and cheaply.** Point reads on a compacted dataset
   are ~1.4 µs and flat across dataset size (§3.2, §3.5); more readers help up to
   ~4× then saturate (§3.12).
3. **The Bloom filter is the read path's most consequential component under a
   realistic multi-file layout** — 94% fewer block reads and a 15× tail-latency
   improvement on hits across 41 overlapping files (§3.9).
4. **Restart cost is a WAL-scan cost.** It scales with total mutations, not live
   data, because the WAL is never truncated in this phase (§3.10). The same
   untruncated WAL is also why the 1 M-key on-disk footprint is ~half log (§3.5).
   This is the single clearest thing a later phase can improve.
5. **Write amplification is ~3.3×** for a moderate-overwrite 100 B workload,
   split roughly evenly across WAL (1.12×), flush and compaction (§3.8).
6. **Sync mode is the dominant durability lever** and costs three orders of
   magnitude between `off` and `sync` (§3.6), now with the fsync counter proving
   each arm did what its name says. `batch` is the default because it bounds
   power-loss exposure at ~20× the cost of `off`, and every mode already survives
   a process kill.

## Facts vs hypotheses

The brief asks which measurements are facts and which are hypotheses. Drawing the
line honestly:

**Supported facts (reproducible from the repository, low variance):**

- Relative orderings: misses > hits, read-heavy > write-heavy, `off` > `batch` >
  `sync`, larger values → lower ops/s. These held every run with small spread.
- The Bloom filter avoids ~94% of block reads on the multi-file hit path (§3.9) —
  a counted quantity, not a timing, so it is machine-independent.
- Write amplification of 3.27× for the §3.8 workload — computed from byte
  counters, reproducible exactly.
- The WAL fsync counts (0 / 15 / 2 000) — counted, and the reason batch is far
  slower than off is that it genuinely flushed 15 times.
- Restart scans the whole WAL and its cost grows with log length (§3.10); the 1 M
  footprint is ~half untruncated WAL (§3.5) — structural properties, confirmed by
  the counts and bytes.
- Concurrent writers do not increase write throughput (§3.1) — a structural
  property of the single-writer path.

**Hypotheses / machine-specific artefacts (do not generalise):**

- Every absolute ops/s and µs figure. They are this M4, this APFS SSD, this OS,
  with the page cache warm and the machine not otherwise quiesced. Another
  machine will differ, possibly by a lot.
- The 16 KiB PUT spread of 69% (§3.1) and the write-heavy / balanced mix spreads
  of 35% / 24% (§3.4) are flush-timing noise on short runs, not stable
  measurements.
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
- **No power-loss testing.** Durability is tested by SIGKILL elsewhere, which only
  proves process-death survival; a benchmark cannot pull the plug, and no mode's
  power-loss durability is claimed.
- **No CPU pinning, no thermal control, no external load generator.** The machine
  was not otherwise idle-locked. Variance is reported (spread column) rather than
  suppressed.
- **Bounded dataset sizes** (≤ 1 M keys). Behaviour beyond the measured range is
  not extrapolated.
- **Physical write accounting is storage-engine level**, not filesystem level
  (§3.8).
- **The WAL suite runs batch with a reduced 16 KiB `SyncBytes`** so a short
  workload crosses the threshold; the absolute batch throughput reflects that
  configuration, which is recorded in the result's `config`.

These numbers are a Phase 5 reference point for later phases to compare against —
not a specification, not a guarantee, and not a claim about any machine but this
one.
