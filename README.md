# Quorum

[![CI](https://github.com/adivishall/quorum/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/adivishall/quorum/actions/workflows/ci.yml?query=branch%3Amain)

A distributed key-value database built from scratch in Go.

No Raft library, no embedded database, no consensus service — the storage engine and the
consensus implementation are the project, and they were built in that order.

**Thesis.** Quorum is a correctness-first distributed key-value database built from scratch.
Consensus, durability, failure handling and client semantics are implemented independently and
then connected. The defining engineering problem is integrating the durable LSM engine with the
replicated state machine without weakening the guarantees the tests already establish.

| Layer | State |
|---|---|
| 1. Correctness foundation: Raft, crash recovery, linearizability, sessions, snapshots, membership, Multi-Raft | built, and checked by simulation, crash matrices, recorded histories, real-process chaos, mutation and fuzzing |
| 2. Durable storage integration: the LSM engine as the replicated state machine | **S1, S2 and S3 of six done:** `dkvd -state-machine lsm` runs each group's state machine on the engine, one apply batch per Raft cycle, recovering from the engine's applied index; the applied index and last sequence are authoritative in the MANIFEST, the WAL is truncated behind each flush, and the Raft log is never compacted past the engine's durable applied index (R2). The in-process and real-process tiers pass on both machines; the deterministic simulator and its crash matrices run the in-memory machine. Memory is still the default; engine-backed snapshots (S4) are in review ([#27](https://github.com/adivishall/quorum/pull/27)). Design and status: [`docs/STORAGE_INTEGRATION.md`](docs/STORAGE_INTEGRATION.md) |
| 3. Distributed performance | baseline measured ([`docs/CLUSTER_BENCHMARKS.md`](docs/CLUSTER_BENCHMARKS.md)); nothing optimized yet |
| 4. Operational and demo surface | `dkvctl`, health and readiness, chaos and load labs; the rest waits for layers 2 and 3 |

A lower layer comes first ([`docs/ENGINEERING_ROADMAP.md`](docs/ENGINEERING_ROADMAP.md) §0).

## Status on `main`

| | |
|---|---|
| **Works today** | Raft groups over the project's own TCP transport, one group per shard (`dkvd -cluster`): elections, ReadIndex reads, joint-consensus membership with learners, snapshots and InstallSnapshot; client sessions with deduplication at apply and one-hop forwarding; per-node metrics, `dkvctl` health and readiness, a load generator and a chaos lab |
| **Opt-in** | `-state-machine lsm`: each group's state machine in the LSM engine — one atomic apply batch per Raft cycle, the applied index authoritative in the MANIFEST, the WAL truncated behind each flush, Raft log compaction clamped to the engine's durable applied index (S1–S3). Memory is still the default |
| **In review** | S4: engine checkpoints and streamed snapshot installation ([#27](https://github.com/adivishall/quorum/pull/27)) |
| **Not built** | an HTTP API, a networked `dkv` client, rebalancing, a dashboard, a Docker demo, PreVote/CheckQuorum, TLS and authentication |
| **The claim** | Single-key PUT/GET/DELETE are linearizable per Raft group on every recorded history checked, with retries inside the claim for identified writes (at most one execution per request). Tested, not proven; nothing is claimed across groups ([docs/CONSISTENCY.md](docs/CONSISTENCY.md), [docs/LINEARIZABILITY.md](docs/LINEARIZABILITY.md)) |

What each phase built, and what it found and fixed: [docs/ROADMAP.md](docs/ROADMAP.md).
The binary is still called `dkv`; that is the command name, not the project name.

## How correctness is checked

| Tier | What runs | Faults | CI job |
|---|---|---|---|
| Deterministic simulation | the real Raft core, durable log and driver ordering in one goroutine, replayable from a seed, every safety invariant checked after every event; bounded exhaustive crash matrices of 1,440, 2,976 and 6,660 crashes with 0 failures ([docs/CRASH_RECOVERY.md](docs/CRASH_RECOVERY.md)) | dropped, duplicated, delayed and reordered messages, partitions, process crash, modeled power loss and torn power loss, disk failures | `check`, `faults` (200 seeds) |
| Real processes | race-built `dkvd` processes on localhost; client histories recorded under faults and checked by the project's linearizability checker | `SIGKILL` (including at named crash points, `dkvd -crash-at`), `SIGSTOP`, partitions through TCP proxies | `integration`, `integration-lsm` (89 tests each) |
| Storage engine | real `SIGKILL` mid-flush and mid-compaction; WAL and MANIFEST write and fsync order checked against a software power-loss model (`fault.MemFS`) | a crash at every write and fsync point | `check`, `integration` |
| The checker itself | agreement with an independent oracle on 20,000 random small histories, a known-good/known-bad corpus, fuzzing | — | `check`, `fuzz` |
| Mutation and fuzzing | 373 rule-violating edits, each of which its named test must catch; 35 fuzz targets | — | `mutation`, `fuzz` |

**Not tested:** power loss on a real device, and the LSM-backed state machine inside the
deterministic simulator, which runs the in-memory machine. Everything else the system does not
do or has not shown is in [docs/LIMITATIONS.md](docs/LIMITATIONS.md).

---

## Quickstart

Requires Go 1.27.2+ (the patch release `go.mod` pins; with the default `GOTOOLCHAIN=auto` an older Go 1.27 fetches it).
No other dependencies.

**One seeded chaos run against real processes.** `dkvlab` builds `dkvd`, starts a cluster on
localhost, applies a fault schedule drawn from the seed (kills, pauses, crashes at driver points,
snapshots, membership changes) under a recorded client workload, and checks the history for
linearizability. It takes about 30 seconds on an M4 laptop:

```bash
go run ./cmd/dkvlab -scenario chaos -seed 1 -runs 1
```

A cluster by hand: the `dkvd -cluster` example in [What exists today](#what-exists-today), then
`dkvctl health` ([docs/OPERATIONS.md](docs/OPERATIONS.md)). The full test tiers are under
[Development](#development).

**The single-node CLI.** `dkv` is the Phase 1 store: in memory, one process, no cluster.

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
CLI is not wired to it yet (the API + CLI phase is not scheduled, `docs/ROADMAP.md`). Full CLI contract: [docs/CLI.md](docs/CLI.md).

## What it is meant to do

Expose three operations on opaque keys — `PUT`, `GET`, `DELETE` — and handle the distributed
parts invisibly: which shard owns a key, which replica leads that shard, how the write is
replicated, when it is committed, and where the bytes physically land.

```
client ─▶ framed TCP protocol ─▶ front: key → shard → group ─▶ group leader ─▶ Raft log (fsync)
                                                                   └─▶ state machine: in memory (default)
                                                                       or the LSM engine (-state-machine lsm)
LSM engine (WAL · memtable · SSTables · compaction): durable and crash-tested on its own, and since
S2–S3 able to host each group's state machine; engine-backed snapshots (S4) are in review
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

                    ┌───────── implemented, Phase 14 (snapshots) ────────┐
log growth ────────▶│  snapshot + atomic publish · log compaction        │
                    │  InstallSnapshot streaming · recovery reconcile    │
                    │  session table in the snapshot · crash matrix      │
                    └────────────────────────────────────────────────────┘

                    ┌──────── implemented, Phase 15 (membership) ────────┐
membership ────────▶│  joint consensus · learners · one change at a time │
many groups ───────▶│  node host · group envelope · group identity file  │
                    │  key → shard → group · wire v3 · group sessions    │
                    └────────────────────────────────────────────────────┘

                    ┌──────────────── not implemented ───────────────────┐
                    │  HTTP API · dashboard · rebalancing                │ later phases
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
group — declarative metadata only, because replication and consensus came in later phases. A
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
are deterministic tests and the safety invariants are checked after every step. What Phase 9 establishes
and — as carefully — what it does not: [docs/RAFT.md](docs/RAFT.md). Run a real 3-node group with
`dkvd -raft`, or, since Phase 15, one group per shard with `dkvd -cluster` (identical `-shards`,
`-rf` and `-nodes` on every node) and change a group's members through its JSON-line admin port.
A node's first start initializes its data directory (`-init -cluster-id NAME`); every later start
omits `-init` (`docs/MULTI_RAFT.md` §5). Each group's state machine lives in memory by default,
rebuilt from the snapshot and the log at every start; `-state-machine lsm` puts it in the storage
engine under the group's directory, one apply batch per Raft cycle, recovered from the engine's
applied index at a restart (`docs/STORAGE_INTEGRATION.md` §8):

```bash
dkvd -id n1 -listen 127.0.0.1:7001 -peers n2=127.0.0.1:7002,n3=127.0.0.1:7003 -data-dir d1 \
     -init -cluster-id demo -state-machine lsm \
     -cluster -shards 4 -rf 3 -client-listen 127.0.0.1:8001 -admin-listen 127.0.0.1:9001 \
     -metrics-listen 127.0.0.1:9101
echo '{"op":"add-learner","group":0,"id":"n4","addr":"127.0.0.1:7004"}' | nc 127.0.0.1 9001
curl -s http://127.0.0.1:9101/metrics | grep dkv_raft_role
```

The membership protocol, its quorum rules and every case it handles:
[docs/MEMBERSHIP.md](docs/MEMBERSHIP.md); the node host, message routing and client routing:
[docs/MULTI_RAFT.md](docs/MULTI_RAFT.md). Every metric a node exports — client requests and
latency, elections, replication lag, persistence and snapshot cost, dropped frames, membership
transitions — and how each is tied to ground truth: [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md).

Phase 10 injects **faults** at the system's real boundaries, never inside the Raft core. The durable
log does its file I/O through a small filesystem seam (`internal/vfs`), under which
`internal/fault` puts a crash-consistent disk model — a process crash keeps every written byte, a
modeled power loss keeps only fsynced ones — and armed write/fsync failures; a transport decorator
drops, duplicates, holds and blocks messages. `internal/raftsim` composes these with the real core,
the real durable log, and the driver's own persist-then-send and recovery functions into a
single-goroutine cluster whose every run is a pure function of a seed or a script: traced, hashed,
replayable, and shrinkable to a minimal failing schedule. Real processes are killed, frozen and
partitioned through TCP proxies. Persistence failure is fail-stop (`dkvd` exits 1). The fault
model, what each tier shows, the bugs it found, and what stays untested (real power loss above
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
  asynchrony) and the ones that are a choice (no rebalancing, no transactions).

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

Cost of each mode on one Apple M4 laptop (`docs/BENCHMARKS.md` §3.6: 2,000 sequential 100-byte
puts, medians over repeated runs; the `batch` arm uses a reduced 16 KiB sync threshold so that it
actually syncs inside the timed interval):

| Mode | appends/s (median) | p99 | fsyncs per 2,000 appends |
|---|---|---|---|
| `off` | 539,029 | 4.2 µs | 0 |
| `batch` | 25,879 | 57.9 µs | 15 |
| `sync` | 239 | 6.54 ms | 2,000 |

A device-level flush on every append costs more than three orders of magnitude in throughput on
this machine. That is the honest price of power-loss durability, and it is why `batch` is the
default.

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
| Power-loss durability | Untested on a real device in every mode, for the WAL and the MANIFEST alike. The crash tests destroy a process, which shows the bytes reached the kernel, not the platter. Since S1 the WAL and MANIFEST write and fsync order is checked against a software power-loss model (`fault.MemFS`) — evidence about the code, not about a disk. |
| Performance | Reproducible measurements now live in `docs/BENCHMARKS.md` (harness: `internal/bench`, `cmd/dkvbench`). They are a single-machine reference point, not a guarantee or a ceiling. The scattered development numbers below and in `docs/BLOOM.md`, `docs/COMPACTION.md` and `docs/MANIFEST.md` predate that document and are indicative only. |
| WAL growth | Since S3 every flush records a log number and deletes the WAL segments below it, so startup replays only the unflushed tail; between flushes the active segment still grows ([docs/STORAGE_INTEGRATION.md](docs/STORAGE_INTEGRATION.md) §9). |
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
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | The original specification — layers, ownership boundaries, shard model, concurrency model, request lifecycle. Design intent, not a description: parts of it, such as the HTTP API, are not built |
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
| [RAFT.md](docs/RAFT.md) | The deterministic core, persistent state and election timing, RequestVote/AppendEntries, the conflict hint, the commit rule and no-op, the apply path, persistence ordering and recovery, the simulated network, the INV-R invariants, and what Phase 9 does and does not establish |
| [FAULTS.md](docs/FAULTS.md) | The fault model and its three tiers, the deterministic simulator, crash/power-loss/persistence-failure semantics, seeds/replay/minimization, the INV-F invariants, the bugs Phase 10 found, and what remains untested |
| [CRASH_RECOVERY.md](docs/CRASH_RECOVERY.md) | The node's crash points, what each crash window leaves on disk and what recovery makes of it, the exhaustive crash matrix, the INV-CR invariants, and the Save record order Phase 11 fixed |
| [LINEARIZABILITY.md](docs/LINEARIZABILITY.md) | The client-visible contract: the object model, write completion, ReadIndex and its safety argument, incomplete operations and retries, the checker and how it was validated, every real-process and simulator scenario, the mutants, and exactly what is and is not verified; §15: logical operations under retries and deduplication |
| [CLIENT_SEMANTICS.md](docs/CLIENT_SEMANTICS.md) | The Phase 13 contract: logical requests, ClientID and RequestID, what happens to an identified write, reads, the eleven statuses, unknown outcomes, bounds, forwarding, and what the guarantee is and is not |
| [DEDUP.md](docs/DEDUP.md) | How the server keeps it: the session table inside the replicated state machine, the decision at apply, recovery by replay and every crash window, concurrency, bounds and eviction, verification, measured cost, mutants, limitations |
| [OBSERVABILITY.md](docs/OBSERVABILITY.md) | Every metric a node exports on `-metrics-listen`: where it is produced, what it means and does not mean, its measured cost, and how each is verified against ground truth |
| [CLUSTER_BENCHMARKS.md](docs/CLUSTER_BENCHMARKS.md) | The first cluster baseline: real `dkvd` processes at 1, 3 and 5 nodes, 1–16 groups, three read/write mixes, a leader kill, a rolling restart, a membership change and snapshots — medians and ranges, the environment, the raw results, and where the time goes |
| [BENCHMARKS.md](docs/BENCHMARKS.md) | The single-node storage engine's measurements (Phase 5): methodology, hardware, variance |
| [ENGINEERING_ROADMAP.md](docs/ENGINEERING_ROADMAP.md) | The audit of the system as built, its verified defects, and the ranked engineering work that follows |
| [CHAOS.md](docs/CHAOS.md) | Real-process chaos: seeded fault schedules (kill, stop, crash at a driver point, pause, isolate, cut, snapshot, add-member) under a recorded workload checked for linearizability; what a seed reproduces and what it cannot; partitions; the evidence a failing run leaves |
| [OPERATIONS.md](docs/OPERATIONS.md) | `dkvctl`: status, leaders, configurations, lag, health and readiness over the admin protocol; the exact health and readiness rules, what each cannot see, and the exit codes |
| [STORAGE_INTEGRATION.md](docs/STORAGE_INTEGRATION.md) | Making the LSM engine the replicated state machine — S1–S3 implemented and opt-in (§7–§9), S4–S6 designed: who owns order, apply results and the applied index; one apply cycle and its crash windows; exactly-once recovery; the two-log options and what decides between them; snapshots as checkpoints; milestones S1–S6 |
| [LOAD_TESTING.md](docs/LOAD_TESTING.md) | How `dkvload` puts a real cluster under load: closed and open loop (coordinated omission accounted for), exact percentiles, outcome classes, the outage timeline, reproducibility, and the generator's measured ceiling |
| [API.md](docs/API.md) | The client wire protocol v3: framing, messages, the request's group, operations, validation, status codes, forwarding and redirect-only mode, the session and sharded client libraries |
| [SNAPSHOTS.md](docs/SNAPSHOTS.md) | Snapshot state and format, creation and compaction order, recovery's reconciliation, crash windows, follower installation, chunking, dedup preservation, corruption policy, measurements, and snapshots with membership |
| [MEMBERSHIP.md](docs/MEMBERSHIP.md) | Joint-consensus membership with learners: the configuration as replicated state, the three configurations a node distinguishes, quorum rules, the operations, every role and case, snapshots, the bugs found, the INV-MB invariants and their evidence |
| [MULTI_RAFT.md](docs/MULTI_RAFT.md) | Many Raft groups per node: identities, the node host, the group envelope, the persistence layout, client routing and group-local sessions, the admin protocol, evidence, measured costs |

## Development

Requires Go 1.27.2+ (the patch release `go.mod` pins; with the default `GOTOOLCHAIN=auto` an older Go 1.27 fetches it).

```bash
make check        # gofmt + gitignore guard + go vet + the unit packages under -race — the phase gate
make build
make test
make race
make integration  # real-process tests (tests/integration, -race; dkvd race-built too): SIGKILL recovery, Raft over TCP, kill/stop/partition faults, linearizability, membership, metrics, load, the lab
make faults       # the deterministic fault schedules and client workloads at a large seed budget (FAULT_SEEDS=200)
make chaos        # real-process chaos under a recorded, checked workload (CHAOS_SEEDS=5; docs/CHAOS.md)
make mutation     # mutation testing: every rule-violating edit must be caught
make fuzz         # every fuzz target in the repository (FUZZTIME=10s each)
make bench        # indicative WAL measurements
```

Operate a running cluster (`docs/OPERATIONS.md`) — exit codes 0 healthy/ready, 1 degraded, 3
unavailable/not ready, 4 unreachable:

```bash
go build -o bin/dkvctl ./cmd/dkvctl
bin/dkvctl -nodes n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 health
bin/dkvctl -admin 127.0.0.1:9001 ready
```

Measure a real cluster (`docs/CLUSTER_BENCHMARKS.md`, `docs/LOAD_TESTING.md`), or put it through
seeded chaos (`docs/CHAOS.md`):

```bash
go build -o bin/dkvlab ./cmd/dkvlab && bin/dkvlab -scenario leader-kill -nodes 3 -runs 5 -rate 50 -out lk.json
bin/dkvlab -scenario chaos -seed 1 -runs 5 -artifacts chaos-artifacts/
go build -o bin/dkvload ./cmd/dkvload && bin/dkvload -endpoints n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003 -clients 16 -duration 30s
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
