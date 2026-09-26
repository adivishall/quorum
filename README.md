# Quorum

A distributed key-value database built from scratch in Go.

Quorum is currently implementing its durable storage engine. No Raft library, no embedded
database, no consensus service — the storage engine and the consensus implementation are
the project, and they are being built in that order.

> **Status: Phase 13 of 25 — durable single-node LSM engine, a routing library, real node processes on a TCP transport, a local replicated-log model, a working Raft consensus core, deterministic fault injection, a proven crash-recovery model for the Raft node, client-visible linearizability of single-key operations on one Raft group, and safe client retries: request identity, deduplication at apply and request forwarding, checked on real client histories.**
>
> **Implemented:** a write-ahead log, an ordered memtable, immutable on-disk SSTables, Bloom
> filters, size-tiered compaction, crash-safe MANIFEST-based file publication, and restart
> recovery across all of it. Acknowledged writes survive the process being killed, including a
> kill during a flush and a kill during a compaction. A pure consistent-hash routing library
> (`internal/routing`): `key → shard` and `shard → replica group` metadata, with a ring
> visualization (`cmd/dkvring`). Phase 7 added real node processes (`cmd/dkvd`) and an internal
> TCP transport (`internal/transport`): checksummed framing, a version handshake, one
> bidirectional connection per peer pair, and `Probe`/`ProbeResponse` liveness — proven by three
> real processes exchanging probes over TCP and shutting down cleanly. Phase 8 adds a **local
> replicated-log model** (`internal/replication`): an immutable `ReplicaGroup` consumed from the
> routing metadata, and a small `Log` interface (with an in-memory `MemoryLog`) that Raft drives —
> 1-based contiguous indexes, deterministic conflicting-suffix replacement that cannot overwrite a
> committed entry, and monotonic commit/apply bookkeeping. Phase 9 adds **Raft**
> (`internal/raft`, `internal/raftlog`, `internal/raftnode`): a pure deterministic consensus core
> (elections, RequestVote, AppendEntries with a term-based conflict hint, the §5.4.2 commit rule,
> the mandatory election no-op), a durable Raft log + HardState, and a node driver that runs a real
> group over TCP. Its safety properties (INV-R1..R10) are verified in a deterministic simulation,
> and durable state survives a real SIGKILL — proven by a 3-process election and a crash-recovery
> test. Phase 10 adds **fault injection** (`internal/fault`, `internal/raftsim`): a deterministic,
> seed-replayable simulator that drives the real Raft core, durable log and driver ordering through
> dropped, duplicated, delayed and reordered messages, partitions, process crashes, a modeled power
> loss, restarts, pauses and disk failures, checking every safety invariant after every event —
> plus real-driver and real-process fault tests (SIGKILL, SIGSTOP, TCP-level partitions). It found
> and fixed three real bugs, including one that let a restarted node acknowledge entries a power
> loss could still erase. Phase 11 adds the **crash-recovery model** (`docs/CRASH_RECOVERY.md`):
> named crash points at every boundary of the node's persist → send → advance → apply cycle and
> between the record writes of one durable-log Save; a bounded exhaustive matrix that kills a node
> at every point a scenario reaches, in every crash mode, and checks each recovery against an
> independent record of what was persisted; seeded crash schedules; in-process and real-process
> (`dkvd -crash-at`, a real SIGKILL at the exact point) crash tests. It found and fixed a window
> that made a node unable to restart, and pins application as at-least-once across restarts.
> Phase 12 adds **client-visible linearizability** (`docs/LINEARIZABILITY.md`): a replicated
> in-memory key-value state machine (`internal/kv`), ReadIndex reads in the pure core, writes
> acknowledged only when committed and applied in their proposal's term, a minimal test-facing
> PUT/GET/DELETE protocol (`dkvd -client-listen`), and a linearizability checker
> (`internal/lincheck`, `cmd/lincheck`) — validated first against an independent oracle, a
> known-good/known-bad corpus and fuzzing — that checks client histories recorded from real
> processes (concurrent clients, leader and follower SIGKILL, partitions, a minority leader that
> still has a follower, a SIGKILL at every point of a write's life), the real driver, and 1,400
> seeded simulator runs. 27 mutants of ReadIndex, write completion, the client and the checker
> are killed.
> Phase 13 makes **retries safe** (`docs/CLIENT_SEMANTICS.md`, `docs/DEDUP.md`, `docs/API.md`): a
> client registers a session (its ClientID is the log index of the REGISTER entry) and numbers its
> requests; the replicated state machine decides at apply — identically on every replica, from a
> bounded session table rebuilt by replay — whether an entry executes or is a duplicate of an earlier
> one (answered with the original's index), a conflicting reuse, or from an expired session; a
> non-leader forwards a request one hop to the leader; a session client retries every unknown
> outcome under the same identity. A request's sends are checked as ONE logical operation: the
> hardest case (committed, reply lost, new leader, another write, retry) at all eight crash windows
> of a real process, forwarders killed before relaying, concurrent copies through every node, a
> full-cluster restart, and 1,200 more seeded simulator runs in which every replica's every
> apply-time decision matches an independent model. 31 more mutants are killed.
>
> **What that claim is, exactly:** single-key PUT/GET/DELETE on **one** Raft group; every
> recorded finite history linearizable, plus an argument with named assumptions — not a proof
> over every execution; retries are inside the claim for identified writes (at most one execution
> per request — exactly one if it executes; not exactly-once delivery), and anonymous writes keep
> Phase 12's semantics. **Not implemented:** the HTTP API, multi-group routing, snapshots, dynamic
> membership, a dashboard, the LSM engine as the replicated state machine. See
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

                    ┌─────────── implemented, Phase 6 (library) ─────────┐
key ───────────────▶│  sha256 ▸ token ▸ shard ring ▸ ShardID             │
                    │  shard ▸ anchor ▸ node ring ▸ replica group (meta) │
                    └────────────────────────────────────────────────────┘

                    ┌─────────── implemented, Phase 7 (processes) ───────┐
dkvd node ─TCP────▶ │  framed TCP · handshake · one conn per peer pair   │
                    │  Probe / ProbeResponse liveness · clean shutdown   │
                    └────────────────────────────────────────────────────┘

                    ┌─────── implemented, Phase 8 (local model) ─────────┐
replica group ─────▶│  ReplicaGroup (from routing metadata)              │
local Log ─────────▶│  1-based entries · suffix replace · commit/apply   │
                    │  in-memory MemoryLog — no consensus, no network    │
                    └────────────────────────────────────────────────────┘

                    ┌─────────── implemented, Phase 9 (consensus) ───────┐
raft group ────────▶│  elections · RequestVote · AppendEntries · commit  │
durable log ───────▶│  no-op on election · conflict hint · HardState      │
                    │  deterministic core + node driver over real TCP    │
                    └────────────────────────────────────────────────────┘

                    ┌───────── implemented, Phase 10 (fault injection) ──┐
seed / script ─────▶│  deterministic simulator · drop/dup/delay/reorder  │
                    │  partitions · crash · power-loss model · disk I/O  │
                    │  real driver + real processes: kill/stop/partition │
                    └────────────────────────────────────────────────────┘

                    ┌───────── implemented, Phase 11 (crash recovery) ───┐
crash point ───────▶│  named points in persist·send·advance·apply cycle  │
                    │  exhaustive crash matrix · seeded crash schedules  │
                    │  real SIGKILL at the point (dkvd -crash-at)        │
                    └────────────────────────────────────────────────────┘

                    ┌───────── implemented, Phase 12 (linearizability) ──┐
client history ────▶│  ReadIndex · completion at commit+apply in term    │
                    │  kv state machine · -client-listen protocol        │
                    │  validated checker over real/driver/sim histories  │
                    └────────────────────────────────────────────────────┘

                    ┌───────── implemented, Phase 13 (client semantics) ─┐
retry ─────────────▶│  sessions · request ids · dedup at apply           │
                    │  one-hop forwarding · wire v2 · session client     │
                    │  logical-operation checking · session model        │
                    └────────────────────────────────────────────────────┘

                    ┌──────────────── not implemented ───────────────────┐
                    │  snapshots                                         │ Phase 14
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

Alongside the engine, Phase 6 adds a pure routing library: a key is hashed (SHA-256, first 8
bytes, big-endian) to a 64-bit token, a consistent-hash ring of the fixed shard set maps that
token to a shard, and a second ring of the node set assigns each shard an ordered replica
group — declarative metadata only, because replication and consensus are later phases. A
one-node membership change moves ≈ 1/N of the key space instead of reshuffling it. `dkvring`
renders the ring (`-format svg|text`) deterministically. The algorithm, golden vectors, and
the explicit list of what it does *not* do: [docs/ROUTING.md](docs/ROUTING.md).

Phase 7 makes nodes real. `cmd/dkvd` is one OS process per node; `internal/transport` is the
internal node-to-node link: messages framed with the same checksummed record format the logs
use, a `"DKV1"` version handshake, one long-lived bidirectional TCP connection per peer pair
(the lower node id dials), automatic reconnect, concurrent-safe sends, and per-connection frame
ordering. Unlike the WAL, a torn network frame is a failed connection, not a repairable tail.
Phase 7 sends only `Probe`/`ProbeResponse` (Phase 9 later activated the Raft message kinds).
It serves no clients. The wire format, handshake, sizes, timeouts,
ordering guarantees, and the explicit boundary: [docs/TRANSPORT.md](docs/TRANSPORT.md).

```bash
dkvd -id node-1 -listen 127.0.0.1:7001 -peers node-2=127.0.0.1:7002,node-3=127.0.0.1:7003
```

Phase 8 defines the **local replicated-log model** Raft will drive. `internal/replication` turns
the routing layer's `shard → replica group` metadata into an immutable, validated `ReplicaGroup`,
and defines a small `Log` interface — 1-based contiguous entries, deterministic conflicting-suffix
replacement that cannot overwrite a committed entry, and monotonic commit/apply watermarks — with
an in-memory `MemoryLog` that exercises every edge case. It is deliberately *local*: it records
that an index *is* committed but does not decide, replicate across nodes, or elect. What it models,
what it explicitly does not guarantee, and the invariants (INV-P1..P9):
[docs/REPLICATION.md](docs/REPLICATION.md).

Phase 9 implements **Raft**. `internal/raft` is a pure, deterministic consensus core — no sockets,
no clock, no goroutines, no global randomness — that drives the Phase 8 log: it runs elections
(randomized timeouts from an injected source), RequestVote and AppendEntries with a term-based
conflict hint, the §5.4.2 commit rule and the mandatory election no-op that makes it safe, and the
apply path. `internal/raftlog` makes its log and HardState durable in the shared record framing
(a torn tail truncates; any other damage refuses to open). `internal/raftnode` is the driver that
persists before it replies, sends over the transport, ticks, and applies. A whole simulated
cluster runs in one goroutine, replayable from a seed, so the paper's figures (including Figure 8)
are deterministic tests and the safety invariants are checked after every step. What Phase 9 proves
and — as carefully — what it does not: [docs/RAFT.md](docs/RAFT.md). Run a real 3-node group with
`dkvd -raft`.

Phase 10 injects **faults** at the system's real boundaries, never inside the Raft core. The durable
log does its file I/O through a small filesystem seam (`internal/vfs`), under which
`internal/fault` puts a crash-consistent disk model — a process crash keeps every written byte, a
modeled power loss keeps only fsynced ones — and armed write/fsync failures; a transport decorator
drops, duplicates, holds and blocks messages. `internal/raftsim` composes these with the real core,
the real durable log, and the driver's own persist-then-send and recovery functions into a
single-goroutine cluster whose every run is a pure function of a seed or a script: traced, hashed,
replayable, and shrinkable to a minimal failing schedule. Real processes are killed, frozen and
partitioned through TCP proxies. Persistence failure is fail-stop (`dkvd` exits 1). The fault
model, what each tier proves, the bugs it found, and what stays untested (real power loss above
all): [docs/FAULTS.md](docs/FAULTS.md).

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

Cost of each mode, an early development measurement on an Apple M4 (100-byte values, indicative
only — `docs/BENCHMARKS.md` §3.6 has the Phase 5 numbers with methodology and variance):

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
| Performance | Reproducible measurements now live in `docs/BENCHMARKS.md` (harness: `internal/bench`, `cmd/dkvbench`). They are a single-machine reference point, not a guarantee or a ceiling. The scattered development numbers below and in `docs/BLOOM.md`, `docs/COMPACTION.md` and `docs/MANIFEST.md` predate that document and are indicative only. |
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
| [ROUTING.md](docs/ROUTING.md) | The token rule, the two consistent-hash rings, ownership/wrap/collision rules, redistribution numbers, the visualization, and what Phase 6 is not |
| [TRANSPORT.md](docs/TRANSPORT.md) | Framing, handshake, message kinds, sizes, codec, connection model, timeouts, shutdown, ordering semantics, failure behavior, and what Phase 7 is not |
| [REPLICATION.md](docs/REPLICATION.md) | The replica-group model, the local replicated-log interface and its index/term/copy semantics, conflicting-suffix rules, commit/apply bookkeeping, the state-machine seam, the INV-P invariants, and what Phase 8 explicitly does not guarantee |
| [RAFT.md](docs/RAFT.md) | The deterministic core, persistent state and election timing, RequestVote/AppendEntries, the conflict hint, the commit rule and no-op, the apply path, persistence ordering and recovery, the simulated network, the INV-R invariants, and what Phase 9 does and does not prove |
| [FAULTS.md](docs/FAULTS.md) | The fault model and its three tiers, the deterministic simulator, crash/power-loss/persistence-failure semantics, seeds/replay/minimization, the INV-F invariants, the bugs Phase 10 found, and what remains untested |
| [CRASH_RECOVERY.md](docs/CRASH_RECOVERY.md) | The node's crash points, what each crash window leaves on disk and what recovery makes of it, the exhaustive crash matrix, the INV-CR invariants, and the Save record order Phase 11 fixed |
| [LINEARIZABILITY.md](docs/LINEARIZABILITY.md) | The client-visible contract: the object model, write completion, ReadIndex and its safety argument, incomplete operations and retries, the checker and how it was validated, every real-process and simulator scenario, the mutants, and exactly what is and is not verified; §15: logical operations under retries and deduplication |
| [CLIENT_SEMANTICS.md](docs/CLIENT_SEMANTICS.md) | The Phase 13 contract: logical requests, ClientID and RequestID, what happens to an identified write, reads, the eleven statuses, unknown outcomes, bounds, forwarding, and what the guarantee is and is not |
| [DEDUP.md](docs/DEDUP.md) | How the server keeps it: the session table inside the replicated state machine, the decision at apply, recovery by replay and every crash window, concurrency, bounds and eviction, verification, measured cost, mutants, limitations |
| [API.md](docs/API.md) | The client wire protocol v2: framing, messages, operations, validation, status codes, forwarding and redirect-only mode, the session client library |

## Development

Requires Go 1.27+.

```bash
make check        # gofmt + gitignore guard + go vet + go test -race — the phase gate
make build
make test
make race
make integration  # real-process tests: SIGKILL recovery, Raft over TCP, kill/stop/partition faults, linearizability
make faults       # the deterministic fault schedules and client workloads at a large seed budget (FAULT_SEEDS=200)
make mutation     # mutation testing: every rule-violating edit must be caught
make fuzz         # every fuzz target in the repository (FUZZTIME=10s each)
make bench        # indicative WAL measurements
```

Re-check a saved client history (a failing test's artifact, or a corpus file):

```bash
go run ./cmd/lincheck internal/lincheck/testdata/corpus/bad/stale-leader-read.hist
```

Extended fuzzing of the storage round-trip contract:

```bash
go test ./internal/storage -run=Fuzz -fuzz=FuzzPutGetRoundTrip -fuzztime=60s
```

## License

Not yet chosen.
