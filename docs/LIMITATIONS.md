# LIMITATIONS

The things this system does not do, cannot do, or has not proven. Kept current: an item may be
removed only when a test exists showing it is no longer true.

**Status: Phase 10.** A single-node key-value store with a durable write-ahead log and an
LSM storage engine — memtable, immutable SSTables with Bloom filters, flush, size-tiered
compaction, crash-safe MANIFEST-based file publication, restart recovery — exists and is
benchmarked (`docs/BENCHMARKS.md`). Phase 6 added a **pure, deterministic routing library**
(`internal/routing`, `docs/ROUTING.md`): `key → shard` over a consistent-hash ring, and
`shard → replica group` as declarative metadata, with a ring visualization (`cmd/dkvring`).
Phase 7 added **real node processes and an internal TCP transport** (`cmd/dkvd`,
`internal/transport`, `docs/TRANSPORT.md`): a checksummed framed-TCP protocol, a version
handshake, one bidirectional connection per peer pair, and `Probe`/`ProbeResponse` liveness,
demonstrated by three real processes exchanging probes and shutting down cleanly. Phase 8 adds a
**local replicated-log model** (`internal/replication`, `docs/REPLICATION.md`, ADR-015): an
immutable `ReplicaGroup` consumed from the routing metadata, and a small `Log` interface (with an
in-memory `MemoryLog`) that Phase 9's Raft will drive — 1-based contiguous indexes, deterministic
suffix replacement that cannot overwrite a committed entry, and monotonic commit/apply
bookkeeping. It is **local** and in-memory; nothing replicates across nodes, decides when an entry
may commit, or persists. Phase 9 adds **Raft** (`internal/raft`, `internal/raftlog`,
`internal/raftnode`, `docs/RAFT.md`, ADR-016): a pure deterministic consensus core (elections,
RequestVote, AppendEntries with a term-based conflict hint, the §5.4.2 commit rule, the mandatory
election no-op), a durable Raft log + HardState, and a node driver that runs a real group over the
transport — verified in deterministic simulation (INV-R1..R10) and by a real 3-process election
and a SIGKILL log-recovery test. Phase 10 adds **fault injection** (`internal/fault`,
`internal/raftsim`, `docs/FAULTS.md`, ADR-017): a deterministic, seed-replayable simulator that
drives the real core, durable log and driver ordering through drops, duplicates, delays,
reordering, partitions, process crashes, a modeled power loss, restarts, pauses and persistence
failures with every invariant checked continuously; plus real-driver and real-process fault tests
(SIGKILL, SIGSTOP, TCP-level partitions). But Raft functioning under faults is **not** the finished
Quorum consistency model: there is no end-to-end linearizability verification, no client/HTTP API,
no request forwarding or dedup, no linearizable-read serving, no snapshots, and no dynamic
membership. **No distributed consistency guarantee is claimed as verified end-to-end.**

### True right now, and temporary

| Limitation | Removed in |
|---|---|
| **The entire log is replayed on every open.** Nothing truncates the WAL, so startup scans every mutation ever written even though compaction absorbed most of them long ago. The MANIFEST has a `SetLogNumber` field and Phase 4 records it without acting on it. | a later phase |
| **The WAL grows without bound.** Nothing reclaims segments. | a later phase |
| **Power-loss durability is claimed for `sync` mode but untested, and untested for every other mode.** The crash tests destroy a real process with SIGKILL, which only proves data reached the kernel. This now covers the MANIFEST too. | not testable here — see `docs/WAL.md` §9 |
| **Damage inside an SSTable data block is found at the read that needs it, not at startup.** Phase 3 read every block of every file at open, because a file's sequence range had nowhere else to live; the MANIFEST records it, so startup cross-checks footers instead. The damage is still found and still reported as corruption rather than as a missing key. `Options.VerifySSTablesOnOpen` restores the Phase 3 behaviour. | deliberate trade — `docs/MANIFEST.md` §6 |
| **Compaction has no rate limit and no I/O budget**, so it competes freely with foreground reads and writes. | measured (`docs/BENCHMARKS.md` §3.7): unbounded; it did not spike foreground p99 above ~7 µs on this SSD in one run, which is reported, not promised. A later phase if it matters |
| **One compaction at a time**, and a compaction's output is a single file however large, so one SSTable's size grows with the dataset. `sstable.MaxMetaBlockSize` (256 MiB of filter or index) is the practical ceiling on a single file. | deferred — `docs/COMPACTION.md` §10 |
| **Tombstone dropping is conservative**: it uses the input set's position in the global sequence ordering rather than per-key range checks, so some tombstones outlive their usefulness and cost space. | deferred — the correct-but-conservative rule is the one that is easy to prove |
| A MANIFEST grows within one session (one record per flush and per compaction) and is only compacted by reopening, which installs a fresh one holding a snapshot. | deferred — a within-session rotation threshold would be the fix |
| A Phase 3 data directory (SSTables with no `CURRENT`) needs an explicit `Options.AdoptLegacySSTables` to open. | deliberate — `docs/MANIFEST.md` §8 |
| `bitsPerKey` is global rather than per level, and filters are rebuilt from scratch on every compaction. | deferred; a measurement would have to justify per-level tuning |
| The flush is synchronous: the writer that triggers it pays for it and other writers wait. Readers do not. Compaction, unlike the flush, does run in the background. | measured (`docs/BENCHMARKS.md` §3.1): visible as the 16 KiB-value tail (p99 ~6 ms) and as why concurrent writers do not scale. A later phase if it matters |
| Segments missing from the *start* of the WAL sequence are undetectable (gaps in the middle are refused). SSTables ahead of the log **are** detected. | a later phase (MANIFEST log number) |
| A length field corrupted within the 64 MiB range can cause a torn-tail/corruption misclassification in the newest segment | inherent to this framing; `docs/WAL.md` §8 |
| `sync` mode serialises writers behind the device flush (~3.4 ms/append, ~291 appends/s, measured on an M4 — `docs/BENCHMARKS.md` §3.6) | a later phase, if group commit is measured to be worth it |
| **The transport is a generic byte carrier** (ADR-013): `internal/transport` frames and moves bytes tagged with a kind and knows nothing of terms, log indexes, or leaders. As of Phase 9 it carries real **Raft** traffic — `dkvd -raft` runs elections, replication, and commit over it (`internal/raftnode`, `docs/RAFT.md`) — and the `RequestVote`/`AppendEntries` kinds are active. Still not carried over it: request forwarding, shard/client serving, and storage bytes; the `InstallSnapshot`/`Forward` kinds remain reserved with no codec. A `dkvd` node still hosts no LSM engine. | Phases 13–14 (`docs/TRANSPORT.md` §11) |
| **The transport is unauthenticated plaintext TCP.** No TLS, no authentication; the handshake node id is a protocol label, not a cryptographic identity. Bounded frame/handshake/id sizes and malformed-input rejection are enforced regardless. | out of scope for v1 (`docs/TRANSPORT.md` §10) |
| **Raft is verified under injected faults, within stated bounds.** Phase 10 checks INV-R1..R10 and INV-F1..F5 across seeded fault schedules in a deterministic simulator, on the real driver, and across real processes (`docs/FAULTS.md`). Not yet done: a systematic crash-window harness for a node that hosts the storage engine (Phase 11) and end-to-end linearizability checking of client histories (Phase 12). The simulator is replayable from a seed; the real-driver and real-process fault tests are not (real timing), and assert only properties that hold under any timing. | Phases 11–12 |
| **The durable Raft log grows without bound.** `internal/raftlog` is append-only; a suffix replacement appends rather than rewrites, and nothing compacts or truncates it. Snapshotting (which would cap it) is Phase 14. | Phase 14 (snapshots) |
| **A node hosts one Raft group and no state machine of consequence.** `internal/raftnode` drives a single group with a minimal (often nil / recording) state machine; it is not wired to the LSM engine, hosts no shards, and the multi-Raft (one group per shard) node is a later phase. `appliedIndex` is volatile and re-applied from the recovered log on restart. | Phases 11+ (per-shard node, engine wiring) |
| No end-to-end cross-node consistency verification, no request forwarding, no client serving, no HTTP API, no linearizable-read serving (ReadIndex), no dashboard. (Leader/follower failover is tested under faults since Phase 10, but only at the Raft level — no client observes it.) | Phases 12–13, 15–17 |
| **Real power loss is untested; Phase 10's power loss is a software model.** `fault.MemFS` assumes a successful fsync is honest and that lost un-synced data is a prefix (never holes, reordered sectors, or bit rot). It proves the code issues its writes and fsyncs in the right order, not what a device does. Kernel fsync-error semantics ("fsyncgate": pages dropped after a write-back error) are not modelled; fail-stop is the only defence. | not testable here — `docs/FAULTS.md` §14 |
| **Real processes are partitioned by resetting connections, not by silently dropping packets**, and one-way partitions exist only in the simulator and the in-process decorator (TCP is bidirectional). A real disk error is never injected into a real process; the fail-stop path is proven in-process on the real driver and on `dkvd`'s raft-mode code. | a later phase, if kernel-level fault tooling is justified |
| **A node whose durable log fails stops and stays stopped** (fail-stop, INV-F1): no write is retried; `dkvd` exits 1 and an operator restarts it, which truncates any torn record and fsyncs the recovered state. | deliberate — ADR-017 |
| **No PreVote or CheckQuorum.** A node that inflated its term while partitioned forces one extra election when it rejoins (it cannot win with a stale log), and a leader cut off from its followers keeps believing it leads until it hears a higher term — it can never commit meanwhile. Liveness is claimed only after faults stop (INV-F3). | a later phase, if measured to matter |
| **A peer whose writes stay blocked loses messages beyond its outbox** (`raftnode.OutboxSize` = 256). The actor never blocks (INV-F5); Raft retransmits once the peer drains. | deliberate |
| **Routing is a library, not a running system.** `internal/routing` computes which shard owns a key and which nodes *would* form each shard's replica group, but no node hosts a shard, no data is placed or moved, and a "membership change" is a new `Config` compared against the old one, never a live cluster mutation (ADR-005, ADR-012). The replica group is declarative metadata; replication, leader election, forwarding, and availability do not exist. | Phases 7–9; `docs/ROUTING.md` §9 |
| `dkv put` cannot carry a maximum-size (1 MiB) value, because `ARG_MAX` is 1 MiB on macOS and the kernel rejects the exec. `dkv shell` can. This is an OS limit, not a dkv limit. | not applicable — use `dkv shell`, or the HTTP API from Phase 15 |
| The CLI is still in-memory-only: it does not yet open a data directory, so `dkv` remains ephemeral even though the storage layer is not | Phase 15 (CLI wiring) |
| The interactive shell cannot express keys containing whitespace, because it splits on whitespace. The one-shot form and the Go API can. | Phase 15 (HTTP API) |
| `WALStore` (the Phase 2 engine, memory-resident logical state) still exists alongside `LSMStore`. It is kept because it is a second, much simpler implementation of the same contract, which keeps the conformance suite honest about being implementation-independent. It is not the engine to build on. | — |

### The list below is what will still be true when v1 is complete.

---

## Not implemented (by decision)

| Limitation | Why | Reference |
|---|---|---|
| No dynamic cluster membership; no adding/removing nodes at runtime | Joint consensus + data migration is a large subsystem orthogonal to the demo | ADR-005 |
| No shard rebalancing | Same | ADR-005 |
| No cross-shard transactions, no multi-key atomicity | Deliberate: it is what makes the linearizability argument hold | ADR-008 |
| No range scans / iterators in the client API | Not needed; would complicate the consistency story | ADR-008 |
| No authentication, authorization, or TLS | Out of scope; the project is about storage and consensus | — |
| No compression, no block cache, no prefix compression | Deferred until a benchmark justifies them | DESIGN §11 |
| No leveled compaction | Size-tiered first, with measurements. Implemented in Phase 4; the comparison that would justify changing is Phase 5's | ADR-007 |
| No leader leases | Would require a clock-drift assumption | ADR-004 |

## Inherent / not claimable

| Limitation | Explanation |
|---|---|
| Not Byzantine fault tolerant | Raft is not a BFT protocol. A malicious or memory-corrupted node can break the cluster. |
| Liveness requires partial synchrony | FLP: no consensus protocol makes progress in a fully asynchronous network with failures. Safety holds regardless; liveness does not. |
| Losing a majority of a shard's replicas permanently loses that shard's data | No replication factor survives permanent loss of a quorum. |
| `fsync` honesty is assumed | Consumer SSDs with volatile write caches can acknowledge before durability. Undetectable from userspace. |
| Power-loss durability is untested | We test SIGKILL (process death). We cannot test power loss on a laptop, so `wal.sync=batch` is documented as surviving process kill only. |
| Corrupted SSTables are detected, not repaired | Repair = wipe the replica and re-sync from the leader. Manual in v1. |
| Dedup table is bounded | A retry arriving after its session is evicted degrades to at-least-once. The bound will be stated with a number once implemented. |
| Single-machine Docker demo is not a durability demo | All containers share one disk. It demonstrates topology, routing, election, and recovery — not independent hardware failure. |

## Scale limits (measured, not guessed)

Phase 5 measured single-node storage performance on one machine (Apple M4, APFS SSD,
`docs/BENCHMARKS.md`). These are **measurements, not ceilings**: they are what the engine did
on that machine under those workloads, not a proven maximum. Read the methodology before
quoting any of them.

- Single-writer PUT throughput: ~386 k ops/s at 100-byte values, ~69 k at 1 KiB, ~3 k at
  16 KiB (`docs/BENCHMARKS.md` §3.1). Concurrent writers do **not** raise this — the write path
  is single-threaded by design.
- Point-read throughput: ~548 k hit / ~3.3 M miss ops/s single-threaded on a compacted
  dataset; ~908 k reads/s across 4 goroutines before it saturates (§3.2, §3.12).
- Write amplification: ~3.3× for a moderate-overwrite 100-byte workload (§3.8).
- Restart cost scales with total WAL length, not live data (§3.10) — the WAL-truncation
  limitation above, made concrete.
- Maximum value size: 1 MiB (enforced limit, `docs/DESIGN.md` §1)
- Maximum key size: 4 KiB (enforced limit)

Distributed scale limits (shard count per node, leader election time) remain *unmeasured*
until Phase 19; they are left blank rather than estimated.

The scattered development measurements Phases 3 and 4 collected while building the engine
(`docs/LSM.md` §10, `docs/BLOOM.md` §5, `docs/COMPACTION.md` §8, `docs/MANIFEST.md` §9) predate
`docs/BENCHMARKS.md` and are superseded by it as the place a storage number is quoted from.

## Not production-ready

Stated plainly so it is never implied otherwise: this system has not run in production, has no
operational tooling, no backup/restore, no upgrade path, no security model, and no track record.
It is an engineering exercise built to be correct and explainable, not to be deployed.
