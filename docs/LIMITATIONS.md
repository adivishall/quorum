# LIMITATIONS

The things this system does not do, cannot do, or has not proven. Kept current: an item may be
removed only when a test exists showing it is no longer true.

**Status: Phase 16, and Phase 19's load generator and first cluster baseline (#2, #3).** A single-node key-value store with a durable write-ahead log and an
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
(SIGKILL, SIGSTOP, TCP-level partitions). Phase 11 adds the **crash-window model and harness**
(`docs/CRASH_RECOVERY.md`, ADR-018): the node is killed at every boundary of its persist → send →
advance → apply cycle, between the record writes of a Save, and during recovery itself — in a
bounded exhaustive matrix and seeded schedules in the simulator, in-process on the real driver,
and on real processes with `dkvd -crash-at` — and every restart is checked against an independent
record of what it had persisted (INV-CR1..4). Phase 12 adds **client-visible linearizability**
(`docs/LINEARIZABILITY.md`, ADR-019): a replicated key-value state machine (`internal/kv`, in
memory), ReadIndex reads, writes completed at commit-and-apply in their proposal's term, a minimal
test-facing operation protocol (`dkvd -client-listen`), and a validated linearizability checker
over client histories from real processes, the real driver and the simulator under faults. What
that verifies is stated there with its bounds: single-key operations, **one** Raft group, recorded
finite histories, honest recording of retries. Phase 13 added request identity (sessions),
deduplication at apply, one-hop forwarding and a retrying session client (`docs/CLIENT_SEMANTICS.md`,
`docs/DEDUP.md`, `docs/API.md`): retries of identified writes are now inside the guarantee. Phase 14
added snapshots and log compaction (`docs/SNAPSHOTS.md`): the Raft log and restart replay are
bounded by the snapshot interval, a lagging follower catches up by an installed snapshot, and the
session table travels in the snapshot. Phase 15 added dynamic membership and multi-Raft
(`docs/MEMBERSHIP.md`, `docs/MULTI_RAFT.md`, ADR-022, ADR-023): a group adds, promotes and removes
members one at a time by joint consensus, a node hosts one Raft group per shard it serves, and
clients are routed key → shard → group with group-local sessions. There is still no HTTP API and no
LSM engine behind Raft, and groups are never split, merged, moved or rebalanced.

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
| **The transport is a generic byte carrier** (ADR-013): `internal/transport` frames and moves bytes tagged with a kind and knows nothing of terms, log indexes, or leaders. As of Phase 9 it carries real **Raft** traffic — `dkvd -raft` runs elections, replication, and commit over it (`internal/raftnode`, `docs/RAFT.md`) — and the `RequestVote`/`AppendEntries` kinds are active. Since Phase 13 it also carries client-request forwarding (kinds 32/33). Since Phase 14 it carries snapshots: `InstallSnapshot` (kind 20) moves a snapshot in chunks of at most 1 MiB, `InstallSnapshotResponse` (kind 21) is the core's `MsgSnapshotResponse` (`docs/SNAPSHOTS.md` §9). Since Phase 15 every payload of a group travels inside a group envelope, the transport still interpreting none of it, and its peer set changes at runtime as configurations do (`docs/MULTI_RAFT.md` §3–§4). Still not carried over it: storage-engine bytes. A `dkvd` node still hosts no LSM engine. | the engine-hosting phase |
| **The transport is unauthenticated plaintext TCP.** No TLS, no authentication; the handshake node id is a protocol label, not a cryptographic identity. Bounded frame/handshake/id sizes and malformed-input rejection are enforced regardless. | out of scope for v1 (`docs/TRANSPORT.md` §10) |
| **Raft is verified under injected faults and exact-boundary crashes, within stated bounds.** Phase 10 checks INV-R1..R10 and INV-F1..F5 across seeded fault schedules; Phase 11 checks INV-F2 and INV-CR1..4 at every crash window of the node (`docs/CRASH_RECOVERY.md`). Not yet done: the crash windows of the storage engine while hosted by a node (the engine is not hosted yet). The simulator is replayable from a seed; the real-driver and real-process tests are not (real timing), and assert only properties that hold under any timing — their evidence (seed, options, fault events at history positions, the history, node logs) is saved on failure and re-checkable (`cmd/lincheck`). | the engine-hosting phase |
| **Linearizability is verified on recorded, finite histories — not proven for every execution.** Every history recorded by the real-process, in-process and simulator tiers is linearizable and the write-completion and ReadIndex mechanisms are argued with named assumptions (`docs/LINEARIZABILITY.md` §3, §5.2), but runs are minutes long, clusters have 3 to 5 nodes, histories come from at most 4 groups at once (2 on real processes), and ≤ 8 clients. Each group's history is checked on its own; nothing is claimed across groups. Longer and larger histories are untested. Seeded random schedules are a net, not a proof: they found the no-quorum mutant only after a two-sided-split profile existed, and then in ~9% of runs (the scripted attacks are the deterministic killers). | more tiers in later phases (soak runs, larger clusters) |
| **An anonymous write retried after an unknown outcome — or delivered twice by a network that duplicates messages (the real transport does not, and a forward is sent once) — can be applied twice.** Identified writes (a session's ClientID and RequestID) are deduplicated (INV-X2); anonymous ones (ClientID 0) keep Phase 12's semantics — recorded as two operations the history is linearizable, collapsed into one it is not (`TestRealIncompleteWriteThenRetry`). | deliberate — the anonymous mode exists to keep the Phase 12 contract meaningful |
| **Sessions are not authenticated.** A client presenting another client's ClientID is that client: it can have its requests answered as the other's duplicates, conflict with them, or move its watermark. The contract assumes each client uses only its own ids (`docs/CLIENT_SEMANTICS.md` §2). | with authentication (not planned for v1) |
| **A retry after its session was evicted learns nothing.** Eviction is LRU by log position with at most 1024 sessions (default): 1024 newer sessions used since the client's last request evict it, and its retry is refused `SESSION_EXPIRED` — never re-executed, but an earlier unknown attempt then stays unknown. A client that registers in a loop evicts everyone else's sessions. No time-based expiry exists (it would need replica-agreed clocks). | deliberate — `docs/DEDUP.md` §6 |
| **The session limits must be identical on every node, and nothing checks it.** `-session-max`/`-session-max-unacked` are part of the replicated state machine's definition; two nodes with different values decide some entries differently and their states diverge. | a later phase (limits carried in the log or the configuration) |
| **The session table costs up to 13.4 MiB at the default limits**, in memory and in every snapshot. Phase 14 carries it in the snapshot (a retry after a restore, a compaction and a restart is still a duplicate — `docs/SNAPSHOTS.md` §10); a restart restores it from the snapshot and replays only the suffix. | — (bounded by the limits) |
| **Unknown or invalid ClientIDs cost a log entry.** A request naming a session that does not exist is decided `SESSION_EXPIRED` at apply, after replication — a client can make the cluster log requests that do nothing. | with authentication / admission control (a later phase) |
| **The checker is exponential in mutually concurrent writes on one key.** ~14 concurrent writes cost ~1.4 M states and ~180 MiB to refute; at 16 the default budget is reached and the answer is UNCHECKED (never a verdict). The system's histories have ≤ 8 concurrent operations per key. | deliberate for now — measured, not optimized (`docs/LINEARIZABILITY.md` §6.3) |
| **The client protocol is framed binary TCP, not an HTTP API.** `dkvd -client-listen` speaks wire protocol v3 (`docs/API.md`): PUT/GET/DELETE/REGISTER naming their group, with request identity, one-hop forwarding within the group (or redirect-only), eleven statuses — with no authentication, no TLS and no version negotiation. | the API + CLI phase, which Phase 15 displaced and which is not scheduled (`docs/ROADMAP.md`) |
| **An isolated leader's clients wait for their deadline.** With no CheckQuorum, a leader cut off from its peers keeps accepting reads (which it can never confirm) and writes (which it can never commit) until it learns a higher term; the clients see deadlines (unknown), never a wrong answer, and the node's pending reads and write waiters grow meanwhile. | a later phase, if measured to matter |
| **The replicated state machine is in memory** (`kv.Store`): a restart restores the latest snapshot and replays the log after it — fewer than `-snapshot-every` entries (Phase 14) — but the whole state must fit in memory, and a snapshot is held in memory while it is created, sent and received, bounded at 512 MiB of state (`docs/SNAPSHOTS.md` §16). Snapshot creation runs on the node's actor goroutine and pauses Raft processing while it encodes and publishes (≈ 50 ms at 100k keys on the measured machine). | engine wiring (the LSM engine as the state machine) |
| **The durable Raft log is bounded only while snapshots are on.** With `-snapshot-every 0`, or a state beyond the 512 MiB snapshot bound (the node logs `event=raft_snapshot_skipped`), nothing compacts it and it grows without bound, as before Phase 14. | — (by configuration); a larger state needs streaming snapshots |
| **A node's groups host no state machine of consequence.** Each group's state machine is the in-memory `kv.Store` (or, in driver tests, a minimal recording one); none is wired to the LSM engine. Since Phase 15 a node hosts one group per shard it serves (`dkvd -cluster`). **`appliedIndex` is volatile beyond the snapshot:** every restart restores the published snapshot (Phase 14) and re-applies the recovered committed entries after it, so the application of those entries is **at-least-once across restarts** and exactly-once only within an incarnation (Phase 11, INV-CR4). A state machine that needs idempotence must record its own applied index (the engine will, `docs/DESIGN.md` §10 step 7); nothing in the driver deduplicates. (Client-level deduplication, Phase 13, is a different thing: the snapshot and the replay after it rebuild the session table, and a retried request is recognized as a duplicate of an earlier *entry* — `docs/DEDUP.md` §4.) | per-shard node, engine wiring |
| **A PUT near the 1 MiB value limit breaks the group** (found by the audit after #3; reproduced on an in-process three-node group). The client protocol accepts a key up to 4 KiB and a value up to 1 MiB, but the Raft log and the AppendEntries decoder accept an entry of at most 1 MiB, and the encoded command adds its key and a few bytes of framing. A PUT whose encoded command exceeds 1 MiB is appended and fsynced by the leader, which nothing checks. Every follower then refuses the AppendEntries carrying it, an election replaces the leader, and the write ends `LOST`. The old leader can never restart: its own log replay refuses the record (`raftlog: corrupt log: length … out of range`). Values a few bytes below the limit are affected, and so is any large value with a long key. | the next work unit (`docs/ENGINEERING_ROADMAP.md`): one entry budget enforced at the front, at proposal and at the durable log |
| **Concurrent writes share nothing** (measured, `docs/CLUSTER_BENCHMARKS.md` §6). The driver persists each event's Ready before the next, so every write costs its own fsync; a leader fsyncs twice per committed entry, because the commit index is persisted before apply (INV-CR3). One group is bounded near one write per fsync whatever the concurrency, and a leader resends every unacknowledged entry with every AppendEntries, with no byte budget per message. | group commit and replication budgets (`docs/ENGINEERING_ROADMAP.md`) |
| **Metrics are per node, pull-only and in memory** (Phase 16, `docs/OBSERVABILITY.md`). Each `dkvd` serves its own `/metrics`; nothing aggregates, stores, graphs or alerts on them, and counters restart with the process. Latencies are server-side; client-observed latency is what `dkvload` measures (#2, `docs/LOAD_TESTING.md`). There are no storage-engine metrics, because the engine is not behind the node, and no tracing. | Phase 17 (dashboard) |
| No HTTP API, no stale-mode reads, no dashboard. (Phases 12–15 serve PUT/GET/DELETE with linearizable reads, request identity, deduplication and forwarding on the framed protocol, per group — see the rows above for its bounds.) | HTTP API: not scheduled; dashboard: Phase 17 |
| **Real power loss is untested; Phase 10's power loss is a software model.** `fault.MemFS` assumes a successful fsync is honest and that lost un-synced data is a prefix (never holes, reordered sectors, or bit rot). It proves the code issues its writes and fsyncs in the right order, not what a device does. Kernel fsync-error semantics ("fsyncgate": pages dropped after a write-back error) are not modelled; fail-stop is the only defence. | not testable here — `docs/FAULTS.md` §14 |
| **Real processes are partitioned by resetting connections, not by silently dropping packets**, and one-way partitions exist only in the simulator and the in-process decorator (TCP is bidirectional). A real disk error is never injected into a real process; the fail-stop path is proven in-process on the real driver and on `dkvd`'s raft-mode code. | a later phase, if kernel-level fault tooling is justified |
| **A node whose durable log fails stops and stays stopped** (fail-stop, INV-F1): no write is retried; `dkvd` exits 1 and an operator restarts it, which truncates any torn record and fsyncs the recovered state. | deliberate — ADR-017 |
| **`dkvd -crash-at` is a test seam in the production binary.** It makes the process SIGKILL itself at a named crash point so the real-process crash tests need no timing guesses; since Phase 12 it can also be armed by SIGUSR1 (`-crash-armed-by-signal`) and fire at the client protocol's reply boundary (`before-reply`, `after-reply`). Unset, it installs nothing and costs nothing; it is documented as a test seam and is not an operational feature. | deliberate — ADR-018, ADR-019 |
| **A torn `write(2)` inside a real process is not produced.** `-crash-at` kills between operations; a record cut mid-way is produced only by the simulator's short writes and the `raftlog` artifact tests (which cut an `Entry` and a `HardState` record at every byte offset). | the simulator and `raftlog` cover it; a real mid-syscall kill has no portable trigger |
| **No PreVote or CheckQuorum.** A node that inflated its term while partitioned forces one extra election when it rejoins (it cannot win with a stale log), and a leader cut off from its followers keeps believing it leads until it hears a higher term — it can never commit meanwhile. Liveness is claimed only after faults stop (INV-F3). | a later phase, if measured to matter |
| **A peer whose writes stay blocked loses messages beyond its outbox** (`raftnode.OutboxSize` = 256). The actor never blocks (INV-F5); Raft retransmits once the peer drains. | deliberate |
| **Routing is fixed at bootstrap and must be identical everywhere, and nothing checks it.** Since Phase 15 `dkvd -cluster` hosts each shard's group, whose genesis voters are the routing's replica group; the routing configuration (`-shards`, `-rf`, `-nodes`) is never compared across processes. A group already on disk keeps the genesis its identity file records, so a changed routing only changes which new groups a node creates and which group its front calls a key's — a client using the other routing is refused `INVALID_REQUEST`, never served in the wrong group. No data is placed or moved: a group's membership changes, its shard never does. | deliberate for v1 (ADR-023); rebalancing is not planned |
| **Membership changes are operator-driven, one member at a time.** The admin protocol adds, promotes and removes one member per operation; nothing detects a failed node or replaces it automatically, and a change stalls — safely — while majorities of both voter sets of a joint configuration are unreachable (`docs/MEMBERSHIP.md` §4, §10). A removed or retired group's files are kept until an operator deletes them. | deliberate (ADR-022) |
| **Every group costs goroutines, heartbeats and fsyncs of its own.** Measured on one machine: 2 goroutines per group replica on one node plus one per peer, about 1 ms/s of idle CPU per replica at 128 groups, no fsync batching across groups (`docs/MULTI_RAFT.md` §9). 128 groups per node is the largest configuration run; no claim is made beyond it. | a later phase, if measured to matter |
| **A node that does not host a key's group cannot point to one that does.** It answers `NOT_LEADER` with no hint and the client tries another node; there is no routing directory beyond each client's copy of the routing. | a later phase, if measured to matter |
| `dkv put` cannot carry a maximum-size (1 MiB) value, because `ARG_MAX` is 1 MiB on macOS and the kernel rejects the exec. `dkv shell` can. This is an OS limit, not a dkv limit. | not applicable — use `dkv shell` |
| The CLI is still in-memory-only: it does not yet open a data directory, so `dkv` remains ephemeral even though the storage layer is not | the API + CLI phase (not scheduled) |
| The interactive shell cannot express keys containing whitespace, because it splits on whitespace. The one-shot form and the Go API can. | the API + CLI phase (not scheduled) |
| `WALStore` (the Phase 2 engine, memory-resident logical state) still exists alongside `LSMStore`. It is kept because it is a second, much simpler implementation of the same contract, which keeps the conformance suite honest about being implementation-independent. It is not the engine to build on. | — |

### The list below is what will still be true when v1 is complete.

---

## Not implemented (by decision)

| Limitation | Why | Reference |
|---|---|---|
| No automatic membership management: no failure detector that replaces nodes, no placement driver, no changes of several members at once | Membership changes exist (Phase 15), operator-driven and one member at a time; automation is orchestration, not consensus | ADR-022 |
| No shard rebalancing, split, merge or data movement between groups | A data-migration protocol is a large subsystem orthogonal to the demo; groups change members, never shards | ADR-005, ADR-023 |
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
| Dedup table is bounded | At most 1024 sessions × 128 unacknowledged results by default (13.4 MiB, measured). A retry arriving after its session is evicted is refused `SESSION_EXPIRED` — it never degrades to at-least-once, but the outcome of its earlier attempts stays unknown. |
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

Phase 15 measured the cost of groups on one machine (`docs/MULTI_RAFT.md` §9): 2 goroutines per
group replica on one node plus one sender per peer, ≈113 KiB of heap per idle replica on one node
(≈215 KiB on three), ≈1 ms/s of idle CPU per replica at 128 groups; a membership change of one entry
in ≈17 ms at the median with fsync, a promotion in ≈26 ms; a new member catching up 20,002 entries in
≈158 ms by entries and ≈23 ms by a snapshot. 128 groups per node is the largest configuration run.

The first cluster baseline (#3, `docs/CLUSTER_BENCHMARKS.md`) measured real `dkvd` processes on the
same machine, all on one SSD. These are measurements under the stated configuration, not
capacities:
- **Closed-loop throughput, 16 clients, one group:**
  - one node: 4,895 ops/s at 95% reads and 268 ops/s at 5% reads;
  - three nodes: 1,046 and 65 ops/s;
  - five nodes: 778 and 64 ops/s.
- **Three nodes, 50% reads:** 119 ops/s, rising to 214 ops/s with 16 groups.
- **A leader SIGKILL at 50 ops/s:** an election in 521 ms (median of 10; range 453–617 ms) and a
  client-visible outage of 450 ms (400–600 ms).

The limit in every configuration is persistence: one fsync per write, two at the leader, never
batched.

The scattered development measurements Phases 3 and 4 collected while building the engine
(`docs/LSM.md` §10, `docs/BLOOM.md` §5, `docs/COMPACTION.md` §8, `docs/MANIFEST.md` §9) predate
`docs/BENCHMARKS.md` and are superseded by it as the place a storage number is quoted from.

## Not production-ready

Stated plainly so it is never implied otherwise: this system has not run in production, has no
operational tooling, no backup/restore, no upgrade path, no security model, and no track record.
It is an engineering exercise built to be correct and explainable, not to be deployed.
