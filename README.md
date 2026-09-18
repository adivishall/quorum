# Quorum

A distributed key-value database built from scratch in Go.

Quorum is currently implementing its durable storage engine. No Raft library, no embedded
database, no consensus service — the storage engine and the consensus implementation are
the project, and they are being built in that order.

> **Status: Phase 3 of 25 — single-node durable LSM storage engine.**
>
> **Implemented:** a single-node key-value store with a write-ahead log, an ordered
> memtable, immutable on-disk SSTables, flush, and restart recovery across the two.
> Acknowledged writes survive the process being killed, including a kill during a flush.
>
> **Not implemented:** Bloom filters, compaction, a MANIFEST, sharding, replication, Raft,
> a distributed cluster, linearizable reads, an HTTP API, a dashboard. Those are Phases 4
> and later. See [docs/ROADMAP.md](docs/ROADMAP.md) for exactly what is done and what is not.
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
                       ┌──────────── implemented, Phase 3 ────────────┐
Put / Delete ─────────▶│  WAL  ──▶  MemTable  ──▶  SSTable (immutable) │
Get ──────────────────▶│  MemTable ▸ immutable MemTables ▸ SSTables    │
                       └──────────────────────────────────────────────┘

                       ┌──────────────── not implemented ─────────────┐
                       │  Bloom filters · compaction · MANIFEST       │ Phase 4
                       │  sharding · replication · Raft · HTTP API    │ Phases 6+
                       └──────────────────────────────────────────────┘
```

A write is appended to the log before it becomes visible in memory. When the memtable
reaches its size limit it is frozen and written out as an immutable, checksummed SSTable —
temporary name, fsync, rename, fsync the directory — so a reader can never see a partially
written file. A read consults the memtable, then any frozen memtable, then each SSTable
newest first, and stops at the first version it finds, including a tombstone.

On restart, the SSTables are verified, the WAL is replayed, and the mutations the tables
already cover are skipped. That works because sequence numbers are assigned deterministically
in log order, so replay re-derives exactly the numbering the original writes received.

The details, including what each crash window leaves on disk: [docs/LSM.md](docs/LSM.md).

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

What is **not** claimed:

| | |
|---|---|
| Bloom filters | The SSTable format reserves a filter block; Phase 3 writes it **empty**. No read is accelerated by it. |
| Compaction | None. The file count grows without bound, and so does read cost (~1.5 µs per SSTable consulted, measured). |
| MANIFEST | None. The live file set is inferred from the directory and every SSTable is read in full at startup. |
| Power-loss durability | Untested in every mode, as in Phase 2. |

## API semantics

The `storage.Store` contract, settled now because the LSM engine that replaces the
in-memory implementation in Phase 3 must not change any of it:

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
| [LSM.md](docs/LSM.md) | Internal keys, sequence numbers, memtable, SSTable format, flush and its crash windows, the read path, recovery without a MANIFEST, measurements |

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
