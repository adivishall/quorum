# LIMITATIONS

The things this system does not do, cannot do, or has not proven. Kept current: an item may be
removed only when a test exists showing it is no longer true.

**Status: Phase 8.** A single-node key-value store with a durable write-ahead log and an
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
may commit, or persists. The transport still carries bytes; it runs no Raft, replicates nothing,
serves no clients, and hosts no storage. Nothing that requires consensus exists — no distributed
replication, no election, no request serving. **No distributed consistency guarantee exists.**

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
| **The transport carries bytes, nothing more.** `internal/transport` and `cmd/dkvd` give real processes a checksummed framed-TCP link with a handshake, reconnect, and `Probe`/`ProbeResponse`. There is no replication, no Raft, no leader election, no request forwarding, no shard serving, and no storage over the wire. The Raft/`Forward` message kinds are reserved identifiers with no codec or semantics. A node process hosts no LSM engine. | Phases 8–9, 13 (`docs/TRANSPORT.md` §11) |
| **The transport is unauthenticated plaintext TCP.** No TLS, no authentication; the handshake node id is a protocol label, not a cryptographic identity. Bounded frame/handshake/id sizes and malformed-input rejection are enforced regardless. | out of scope for v1 (`docs/TRANSPORT.md` §10) |
| **The replication model is local and in-memory.** `internal/replication` defines a `ReplicaGroup` and a `Log`/`MemoryLog` that Phase 9's Raft will drive. It replicates nothing across nodes, decides nothing about *when* an entry may be committed (`Commit` only *records* that it is), elects nothing, and does not persist — the log lives in memory and is lost on restart. The state-machine seam (`StateMachine.Apply`) is defined but not wired to the LSM engine, and no driver pumps committed entries into it. It carries no distributed or consistency guarantee. | Phase 9 (Raft), 14 (persistence/snapshots); `docs/REPLICATION.md` §11 |
| No distributed replication, no consensus, no cross-node consistency, no leader election, no failover, no request forwarding, no client serving, no HTTP API, no dashboard | Phases 9, 13, 15–17 |
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
