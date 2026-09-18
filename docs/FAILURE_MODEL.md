# FAILURE MODEL

Status: **Phase 0 — specification.**

A distributed system is only meaningful relative to the failures it claims to survive. This
document states the assumptions. Anything not assumed here is something we do **not** tolerate,
and saying so is the point.

---

## 1. Node failures

**Model: crash-recovery, not fail-stop, not Byzantine.**

- A node may stop at any instant — between any two instructions, including in the middle of a
  write to disk. Crashes are modeled as `SIGKILL`, never as graceful shutdown, in every
  crash test.
- A crashed node may restart with its durable state intact and rejoin. It may do so any number
  of times.
- A node does **not** lie. It does not send messages it did not compute, does not forge terms,
  does not corrupt data on purpose. We do not implement Byzantine fault tolerance and we do
  not claim it. Raft is not a BFT protocol.
- A node may be **arbitrarily slow** (GC pause, CPU starvation, slow disk). Slowness is
  indistinguishable from failure by remote observers, and the system must remain *safe* when
  it guesses wrong. This is why failure detection can only affect liveness, never safety.

Tolerance: a shard with 2*f*+1 replicas tolerates *f* simultaneous node failures without losing
committed data and without violating §consistency. At *f*+1 failures it becomes unavailable,
by design.

---

## 2. Network failures

**Model: asynchronous network with partial synchrony for liveness only.**

The network may:
- **drop** messages arbitrarily,
- **delay** messages arbitrarily and unboundedly,
- **reorder** messages,
- **duplicate** messages,
- **partition** any subset of nodes from any other subset, in either or both directions
  (asymmetric partitions included — these break naive implementations and are explicitly in
  the fault-injection matrix).

The network may **not**:
- corrupt message contents undetectably (we rely on TCP checksums plus our own framing; a
  frame that fails to parse closes the connection),
- fabricate messages from a node that did not send them.

**Safety holds under full asynchrony.** No safety property in this system depends on a timeout
firing correctly, on clock synchronization, or on bounded message delay. Timeouts only cause
elections, and an election in the absence of a real failure costs availability for a moment,
never correctness.

**Liveness requires partial synchrony.** If the network never delivers a message within an
election timeout, no leader can be elected and no progress is made. This is the FLP result;
it is not a defect we can engineer away, and any system claiming otherwise is wrong.

---

## 3. Clocks

**No assumption on clock synchronization, drift, or monotonicity across nodes.**

- Wall-clock time is used only for logs, metrics, and human-readable output. It never enters a
  correctness decision.
- Within a node, elapsed time comes from `time.Ticker` driving logical ticks. A node whose
  clock jumps forward simply campaigns sooner; a node whose clock stalls is treated by peers
  as slow, which is already covered by §1.
- We reject **leader leases** for reads specifically because they would require assuming
  bounded clock drift across nodes. That assumption is usually fine and occasionally
  catastrophic, and taking it would make §2 of `docs/CONSISTENCY.md` conditional on a hardware
  property we cannot verify.

---

## 4. Disk and filesystem

- We assume `fsync`/`F_FULLFSYNC` returns only after data is durable **to the extent the
  hardware honors the barrier.** Consumer SSDs with volatile caches can lie. We cannot detect
  this and we do not claim to.
- We assume `rename(2)` is atomic within a directory (POSIX) and use it for `CURRENT`.
- We **do not** assume that a partially-written file is detectable by length alone. Every
  record carries a CRC32C (`docs/DESIGN.md` §2).
- We assume the filesystem does not silently reorder our writes across an fsync barrier. We do
  not assume anything about ordering *without* a barrier, which is why directory fsyncs appear
  explicitly in the compaction commit protocol.
- **Bit rot / silent data corruption** is *detected* (CRC on every record and every SSTable
  block) but not *repaired*. A corrupted SSTable makes that replica refuse to serve; repair
  means wiping the replica's data directory and letting it re-sync from the leader via
  snapshot. That is the documented operational procedure, not an automatic one in v1.
- **Full disk** and **I/O error** are surfaced as errors that fail the write and, for a Raft
  log write, stop the node rather than acknowledging something that is not durable. A node
  that cannot persist must not vote and must not acknowledge AppendEntries.

---

## 5. Client failures

- A client may crash between sending a request and receiving a response. The request may or may
  not have committed. See `docs/CONSISTENCY.md` C4.
- A client may retry any request any number of times, including after a partition heals, so a
  very old duplicate can arrive much later. The dedup table (Phase 13) must therefore be
  bounded and its eviction policy must be stated — an evicted session's retry degrades back to
  at-least-once, and that boundary is documented rather than hidden.
- A client may send malformed input, oversized keys or values, or many concurrent requests.
  Handled by validation and limits (Phase 21), not by trust.

---

## 6. Failure detection

Failure detection is **heartbeat-based and unreliable, by necessity.** A node is suspected when
it has not responded within a timeout. Suspicion is:
- **never** used to make a safety decision (we never "remove" a node from a quorum because we
  think it is dead — the quorum size is fixed by the configured replica set),
- used to trigger elections,
- used to mark a node's health in the dashboard as `suspect` (distinct from `down` and `up`),
  because "we have not heard from it" is genuinely different from "it is gone".

There is no perfect failure detector in an asynchronous network. The dashboard will show
suspicion, not truth, and will be labeled that way.

---

## 7. The fault-injection matrix (Phase 10)

Every row must be reproducible from a seed and runnable in CI:

| Fault | Mechanism | Primary property under test |
|---|---|---|
| Node crash (SIGKILL) | process kill | committed data survives; election happens |
| Node restart | process restart | recovery, catch-up, no divergence |
| Leader crash mid-write | kill during proposal storm | no lost acknowledged write, no phantom write |
| Symmetric partition | transport drop-list | minority unavailable; majority progresses; no split brain |
| Asymmetric partition | one-way drop-list | no livelock; stale leader steps down |
| Message drop (p%) | transport hook | retry/backoff correctness |
| Message delay | transport hook | stale message handling |
| Message duplication | transport hook | idempotence of RPC handlers |
| Message reorder | transport hook | no assumption of ordering beyond TCP per-connection |
| Slow node | artificial latency | leader does not block on slowest follower |
| Disk full | injected `ENOSPC` | node fails loudly, does not ack |
| Corrupt WAL tail | byte mutation | truncate-and-continue |
| Corrupt WAL middle | byte mutation | refuse to start, clear error |
| Crash during compaction | kill between protocol steps | orphan sweep; no reader sees a partial file set |
| Crash during flush | kill mid-flush | WAL replay reconstructs the memtable |

---

## 8. What we do not tolerate

Stated plainly:

- More than *f* simultaneous replica failures in a shard → that shard is unavailable, and if
  the failures are permanent (disks destroyed), committed data in that shard is **lost**.
  No replication factor protects against losing a majority permanently.
- Byzantine nodes.
- Correlated whole-cluster loss (single machine, single disk in the Docker demo). The Docker
  demo is a *topology* demonstration, not a durability demonstration.
- Undetectable hardware lying about fsync.
- Malicious clients — there is no authentication in v1.
