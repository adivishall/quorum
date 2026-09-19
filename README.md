# Quorum

A distributed key-value database built from scratch in Go.

Quorum is currently implementing its durable storage engine. No Raft library, no embedded
database, no consensus service — the storage engine and the consensus implementation are
the project, and they are being built in that order.

> **Status: Phase 4 of 25 — single-node durable LSM-backed key-value store.**
>
> **Implemented:** a write-ahead log, an ordered memtable, immutable on-disk SSTables, Bloom
> filters, size-tiered compaction, crash-safe MANIFEST-based file publication, and restart
> recovery across all of it. Acknowledged writes survive the process being killed, including a
> kill during a flush and a kill during a compaction.
>
> **Not implemented:** sharding, replication, Raft, a distributed cluster, linearizable reads,
> networking, an HTTP API, a dashboard. Those are Phases 6 and later. See
> [docs/ROADMAP.md](docs/ROADMAP.md) for exactly what is done and what is not.
>
> The binary is still called `dkv`; that is the command name, not the project name.

---

## What it is meant to do

Expose three operations on opaque keys — `PUT`, `GET`, `DELETE` — and handle the distributed
parts invisibly: which shard owns a key, which replica leads that shard, how the write is
replicated, when it is committed, and where the bytes physically land.

```
client ─▶ HTTP API ─▶ router ─▶ shard leader ─▶ Raft ─▶ LSM engine
          └──────────── not built yet ────────────┘      └─ Phase 3 ─┘
```

## What exists today

```
                    ┌─────────────── implemented, Phase 4 ───────────────┐
Put / Delete ──────▶│  WAL ──▶ MemTable ──▶ SSTable(L0) ──▶ compaction   │
Get ───────────────▶│  MemTable ▸ immutable MemTables ▸ SSTables          │
                    │             (each gated by a Bloom filter)          │
                    │  MANIFEST decides which files are the database      │
                    └────────────────────────────────────────────────────┘

                    ┌──────────────── not implemented ───────────────────┐
                    │  sharding · replication · Raft · networking        │ Phases 6+
                    │  HTTP API · dashboard                              │ Phases 15+
                    └────────────────────────────────────────────────────┘
```

A write is appended to the log before it becomes visible in memory. When the memtable reaches its
size limit it is frozen and written out as an immutable, checksummed SSTable — temporary name,
fsync, rename, fsync the directory, then one fsynced MANIFEST record — so a reader can never see a
partially written file, and a file is part of the database at exactly one instant. A read consults
the memtable, then any frozen memtable, then each SSTable newest first, skipping any whose Bloom
filter says the key is definitely absent, and stops at the first version it finds, including a
tombstone.

In the background, compaction merges each level into the next, dropping superseded versions and —
only where nothing older could still hold a value — tombstones. It runs concurrently with reads and
writes and synchronises only to publish its result.

On restart, the MANIFEST says which files are live, each is cross-checked against the metadata the
MANIFEST records for it, anything on disk it does not name is deleted as an orphan, and the WAL is
replayed with the mutations the tables already cover skipped. That works because sequence numbers
are assigned deterministically in log order, so replay re-derives exactly the numbering the
original writes received.

The details, including what every crash window leaves on disk: [docs/LSM.md](docs/LSM.md),
[docs/BLOOM.md](docs/BLOOM.md), [docs/COMPACTION.md](docs/COMPACTION.md),
[docs/MANIFEST.md](docs/MANIFEST.md).

## The one thing this project refuses to do

Claim a guarantee it has not verified. Specifically:

- The consistency model is **linearizable single-key operations**, with the conditions and
  the retry caveat spelled out in [docs/CONSISTENCY.md](docs/CONSISTENCY.md). Not "strong
  consistency", which is a marketing phrase, not a model.
- Every guarantee maps to a numbered invariant in [docs/INVARIANTS.md](docs/INVARIANTS.md),
  and every invariant names the test that checks it. An invariant with no passing test is
  marked `PLANNED` and may not be cited as a guarantee anywhere else.
- The things it cannot do are enumerated in [docs/LIMITATIONS.md](docs/LIMITATIONS.md) —
  including the ones that are inherent (no Byzantine tolerance, no liveness under full
  asynchrony) and the ones that are a choice (no dynamic membership, no transactions).

## Quickstart

Requires Go 1.27+.

```bash
make build
./bin/dkv shell
```

```
dkv> put user:123 Adi
OK
dkv> get user:123
Adi
dkv> delete user:123
OK
dkv> get user:123
(not found)
dkv> exit
```

One-shot form, with exit codes a script can branch on
(`0` ok, `1` not found, `2` usage, `3` invalid input, `4` internal):

```bash
./bin/dkv put user:123 Adi
./bin/dkv get user:123
./bin/dkv delete user:123
```

**The CLI is in-memory**: each invocation gets a fresh store, so state does not survive
process exit. The CLI says so on every mutating command. The storage layer is durable; the
CLI is not wired to it until Phase 15. Full CLI contract: [docs/CLI.md](docs/CLI.md).

## Durability, stated exactly

An acknowledged write survives the **process being destroyed**. That is tested, not asserted:
`tests/integration/crash_test.go` starts a real child process, has it perform writes that each
return `nil`, kills it with `SIGKILL` — no flush, no `Close`, no deferred functions — verifies
it really died by signal, then reopens the directory and checks every write is there.

| `wal.sync` | Survives SIGKILL | Survives OS crash / power loss |
|---|---|---|
| `off` | **yes** (tested) | no |
| `batch` (default) | **yes** (tested) | may lose up to 100 ms or 1 MiB — *untested* |
| `sync` | **yes** (tested) | claimed via `F_FULLFSYNC`, **not tested** |

The row that teaches the most is the first one. `sync=off` loses nothing on SIGKILL — because
`write(2)` had already handed the bytes to the kernel, and the kernel outlives the process.
**Process death is not power loss**, and no test here proves power-loss durability for any
mode. `docs/WAL.md` §9 spells out what was and was not established.

Cost of each mode, measured on an Apple M4 (100-byte values, indicative only — Phase 5 does
benchmarking properly):

| Mode | ns/append | approx. appends/s |
|---|---|---|
| `off` | 3,879 | 258,000 |
| `batch` | 5,456 | 183,000 |
| `sync` | 3,870,493 | 258 |

A device-level flush per write costs roughly **700×**. That is the honest price of power-loss
durability, and it is why `batch` is the default.

> **The CLI is not wired to a data directory yet** — it still constructs an in-memory store,
> so `dkv` remains ephemeral even though the storage layer is not. That wiring belongs to
> Phase 15.

### The storage engine, stated exactly

An acknowledged write also survives a crash **during a flush**. That is tested the same way:
a child process fills a memtable, reports that every write returned `nil`, starts writing the
SSTable, and is destroyed mid-write. The test then classifies what the crash actually left on
disk — a partial `*.sst.tmp`, a published `*.sst`, or neither — and fails if the mid-flush
window was never hit, because a crash test that never hits its window passes for the wrong
reason.

An acknowledged write also survives a crash **during a compaction**, tested the same way: a child
process builds four SSTables, starts a compaction, and is destroyed mid-merge. The test classifies
what the crash actually left on disk by reading the MANIFEST — not by asking the engine — and
requires that the mid-compaction window was really hit.

What is **not** claimed:

| | |
|---|---|
| Power-loss durability | Untested in every mode, for the WAL and now the MANIFEST alike. The crash tests destroy a process, which proves the bytes reached the kernel, not the platter. |
| Performance | The numbers below and in `docs/BLOOM.md`, `docs/COMPACTION.md` and `docs/MANIFEST.md` are development measurements on one laptop. Phase 5 owns benchmarking; nothing here may be quoted as a result. |
| WAL truncation | The log is still never truncated, so startup replays every mutation ever written even though compaction absorbed most of them. The MANIFEST records `SetLogNumber` and does not act on it. |
| Startup corruption detection | Startup no longer reads every data block, so damage inside one is found at the read that needs it rather than at open. It is still found, and still reported as corruption rather than as a missing key. `VerifySSTablesOnOpen` restores the old behaviour. |

### What Bloom filters and compaction actually bought

Development measurements, go1.27.1 / darwin-arm64, same data and same workload in both arms
(`docs/BLOOM.md` §5). Indicative only:

| | Filter disabled (Phase 3) | Filter enabled |
|---|---|---|
| Data blocks read, 4,000 lookups over 17 SSTables | 52,329 | **2,395** |
| p50 / p95 / p99 lookup | 11.0 / 19.2 / 22.1 µs | **1.4 / 2.6 / 3.3 µs** |

95.4% of block reads avoided, for a filter costing exactly 10.00 bits per key (2.33% of the file).
One compaction then turned 12 files and 12,000 entries into 1 file and 2,471 entries
(`docs/COMPACTION.md` §8).

The figure worth keeping is the block count, not the microseconds: it is a property of the
algorithm, whereas the timings are a property of this laptop's page cache.

## API semantics

The `storage.Store` contract, settled in Phase 1 because nothing since — the WAL, the LSM engine,
Bloom filters, compaction or the MANIFEST — has been allowed to change any of it:

| Question | Answer |
|---|---|
| Does `Get` return a copy or internal memory? | A fresh copy. Mutating it cannot affect stored data. |
| Does `Put` copy its input? | Yes, both key and value. The caller may reuse its buffers immediately. |
| Is `DELETE` idempotent? | Yes, and it never reports whether the key existed — an LSM deletes by writing a tombstone without reading, so promising existence here would mean breaking that promise later. |
| Maximum key / value size | 4 KiB / 1 MiB, enforced identically by `Put`, `Get` and `Delete`. |
| Are keys case-sensitive? | Yes. Keys are opaque bytes compared bytewise, with no normalisation; whitespace, newlines, NULs and invalid UTF-8 are all valid keys. |
| Is an empty value the same as no key? | No. An empty value is a present key; `Get` returns a zero-length non-nil slice with a nil error. |
| Concurrency guarantee | Safe for concurrent use; each operation is atomic with respect to every other. No atomicity *across* operations — no transactions, no CAS, no batches. |

These are numbered INV-A1..A9 in [docs/INVARIANTS.md](docs/INVARIANTS.md) and enforced by a
conformance suite that the Phase 3 LSM engine inherited unchanged — including in a
configuration where every single mutation becomes its own SSTable, so that no assertion in
it is being answered out of memory.

## Documentation

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Layers, ownership boundaries, shard model, concurrency model, request lifecycle |
| [DESIGN.md](docs/DESIGN.md) | On-disk formats, record framing, SSTable layout, Raft state transitions, wire protocol, recovery sequence |
| [CONSISTENCY.md](docs/CONSISTENCY.md) | The consistency model, its conditions, what is explicitly not claimed, how it gets verified |
| [FAILURE_MODEL.md](docs/FAILURE_MODEL.md) | Crash/network/clock/disk assumptions and the fault-injection matrix |
| [INVARIANTS.md](docs/INVARIANTS.md) | Numbered, falsifiable invariants bound to tests |
| [DECISIONS.md](docs/DECISIONS.md) | ADRs — what was chosen, what was rejected, what it costs |
| [LIMITATIONS.md](docs/LIMITATIONS.md) | What it does not do |
| [ROADMAP.md](docs/ROADMAP.md) | 25 phases, exit criteria, and why the order is what it is |
| [CLI.md](docs/CLI.md) | Command surface, exit codes, stream discipline, shell behaviour |
| [WAL.md](docs/WAL.md) | Record format, segmentation, sync modes, append path, replay, corruption policy, what crash testing established |
| [LSM.md](docs/LSM.md) | Internal keys, sequence numbers, memtable, SSTable format, flush and its crash windows, the read path, measurements |
| [BLOOM.md](docs/BLOOM.md) | Filter format, the hash and why it is that one, what may and may not be eliminated, measured false-positive rate and work avoided |
| [COMPACTION.md](docs/COMPACTION.md) | Size-tiered policy, the streaming k-way merge, version and tombstone elimination, every publication crash window, concurrency |
| [MANIFEST.md](docs/MANIFEST.md) | Why a directory scan cannot work, the edit format, the publication protocol, orphan and corruption policy, startup |

## Development

Requires Go 1.27+.

```bash
make check        # gofmt + gitignore guard + go vet + go test -race — the phase gate
make build
make test
make race
make integration  # real-process SIGKILL crash recovery tests
make bench        # indicative WAL measurements
```

Extended fuzzing of the storage round-trip contract:

```bash
go test ./internal/storage -run=Fuzz -fuzz=FuzzPutGetRoundTrip -fuzztime=60s
```

## License

Not yet chosen.
