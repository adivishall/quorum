# FAILURE MODEL

Status: **specification (Phase 0); the fault-injection matrix in §7 is implemented and verified
in Phase 10** (`docs/FAULTS.md`, ADR-017), with the scope and qualifiers stated there; **the crash
windows of the Raft node are characterised and verified in Phase 11** (`docs/CRASH_RECOVERY.md`,
ADR-018) — a crash at every boundary of persistence, message emission, commit and apply, and
recovery, with the durable state each leaves and what recovery makes of it.

A distributed system is only meaningful relative to the failures it claims to survive. This
document states the assumptions. Anything not assumed here is something we do **not** tolerate,
and saying so is the point.

---

## 1. Node failures

**Model: crash-recovery, not fail-stop, not Byzantine.**

- A node may stop at any instant — between any two instructions, including in the middle of a
  write to disk. Crashes are modeled as `SIGKILL`, never as graceful shutdown, in every
  crash test. *Phase 11 makes "any instant" concrete:* the process is killed at every named
  boundary of its persist → send → advance → apply cycle and between the record writes of one
  durable-log `Save`, in the simulator exhaustively and on real processes with `dkvd -crash-at`.
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
  that cannot persist must not vote and must not acknowledge AppendEntries. *Implemented and
  verified in Phase 10 (INV-F1):* a failed write or fsync latches the durable log, the driver
  sends none of the dependent messages and stops, and `dkvd` exits 1; a restart truncates any
  torn record and makes the recovered state durable (fsync) before acting on it. This rests on
  the fsync assumption above: after a failed fsync a kernel may drop the affected pages
  ("fsyncgate"), in which case records a restarted process read back from the page cache can
  still be lost — fail-stop is the only defence taken.

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

Implemented in Phase 10; `docs/FAULTS.md` has the mechanisms and every test. Rows run in CI in
three tiers: the **simulator** (`internal/raftsim`) is reproducible from a seed or a script; the
**real driver** (`internal/raftnode`) and **real process** (`tests/integration`) tiers run real
goroutines, timers and TCP, so they are not seed-replayable — they assert properties that hold
under any timing.

| Fault | Mechanism | Primary property under test | Evidence | Status |
|---|---|---|---|---|
| Node crash (SIGKILL) | real process kill; simulated process crash | committed data survives; election happens | `TestRealLeaderCrashAndReelection`, `TestRealFollowerCrashAndCatchUp`, `TestRealRepeatedCrashRestart`; `TestLeaderCrashAndReelection`, `crashes` profile | VERIFIED |
| Node restart | real process restart on the same data dir; simulated restart via `raftnode.Recover` | recovery, catch-up, no divergence | INV-F2 at every simulated restart; `TestRealFollowerCrashAndCatchUp`, `TestRealRestartWhileIsolated` | VERIFIED |
| Crash at an exact boundary (Phase 11) | `raftnode.Point` crash points and I/O boundaries: before/after a Save, between its record writes, before its fsync, after each message, around Advance, around Apply/AppliedTo, during recovery's own truncate and fsync | recovered state = last completed Save + a prefix of the interrupted one; term/vote never regress; committed prefix stable; commit durable before apply; replay exact and at-least-once | `TestCrashMatrix` (1,440 cells, 0 failures), the `crashpoints` chaos profile, the scenario tests in `internal/raftsim/crash_test.go`, `internal/raftnode/crash_test.go`; real processes: `TestRealCrashAtPoints`, `TestRealCrashAtEveryEarlyPointIsRecoverable` | VERIFIED (simulation incl. modeled power loss; process kill) |
| Leader crash mid-write | crashes while proposals are in flight (chaos profiles); Phase 11: the leader dies before its Save of a proposal, after it, between its record writes, before its fsync, after one AppendEntries | no lost acknowledged write, no phantom write | no committed entry lost (INV-R4) and no fabricated command (INV-F4) under every schedule; `TestLeaderCrashBeforeSavingAProposal`, `TestLeaderCrashAfterSavingBeforeSending`, `TestLeaderCrashAfterSendingToOnePeer`, `TestLeaderCrashBetweenRecordWritesOfOneSave` pin what each window loses; an *acknowledged* write needs client acks (Phases 12/13) | PARTIAL (no client acks yet) |
| Symmetric partition | simulator links; `fault.Network`; TCP proxy cut | minority unavailable; majority progresses; no split brain | `TestLeaderIsolatedFromMajority`, `TestIsolatedLeaderCannotCommitAndRejoins`, `TestRealIsolatedLeaderRejoinsAfterPartition`, `partitions` profile | VERIFIED |
| Asymmetric partition | one-way simulator link; one-way `fault.Network` link | no livelock; stale leader steps down | `partitions`/`mixed` profiles (one-way blocks) with convergence after healing (INV-F3). TCP is bidirectional, so a real one-way cut is not produced | VERIFIED (simulation) |
| Message drop (p%) | simulator `drop`; `fault.Network` Drop | retry/backoff correctness | `TestMessageLoss`, `messages` profile | VERIFIED |
| Message delay | simulator `delay`; `fault.Network` Hold/Release | stale message handling | `TestDelayedOldTermAppendIsInert`, INV-R10 at every stale delivery, `messages` profile | VERIFIED |
| Message duplication | simulator `dup`; `fault.Network` Duplicate | idempotence of RPC handlers | `TestDuplicatedVoteDoesNotCountTwice`, `TestDuplicatedReplicationTraffic`, `TestDuplicatedAndReorderedTrafficAppliesOnce`, INV-F4 | VERIFIED |
| Message reorder | random delivery order; reversed release | no assumption of ordering beyond TCP per-connection | `TestStaleSuccessDoesNotRegressReplication`, `messages` profile | VERIFIED |
| Slow node | wedged sends (`fault.Network` Block); pause; SIGSTOP | leader does not block on slowest follower | `TestWedgedPeerDoesNotStallTheLeader` (INV-F5), `TestPausedLeaderStepsDownOnResume`, `TestRealFrozenLeaderStepsDown` | VERIFIED |
| Disk full | injected `ENOSPC` (`fault.InjectFS`) | node fails loudly, does not ack | `TestPersistFailureIsFailStop`, `TestDiskFullOnLeaderStopsItAndClusterMovesOn`, `TestRaftModeExitsNonZeroWhenTheLogFails` (INV-F1) | VERIFIED (injected) |
| Failed fsync | injected `EIO` on fsync | nothing dependent is sent; restart makes recovered state durable | `TestPersistFailureIsFailStop`, `TestVoteNotSentWhenItCannotBePersisted`, `TestNoAckOfUnsyncedEntriesAfterFailedFsync` | VERIFIED (injected) |
| Torn write | injected short write; torn tail after a modeled power loss | truncate on reopen; nothing built on it | `TestFailedWriteLatchesAndLogStaysRecoverable`, `TestTornWriteIsTruncatedOnRestart`, `disk`/`crashes` profiles | VERIFIED |
| Power loss | `fault.MemFS` model: only fsynced bytes (+ a torn prefix) survive | nothing acknowledged is lost | fsync form of INV-R6 at every send; `TestAckedEntrySurvivesPowerLoss` | VERIFIED **in the model only**; real power loss untested |
| Connection loss / flapping | TCP proxy cut/heal, repeatedly | reconnect; convergence; no crash | `TestRealConnectionFlapping` | VERIFIED |
| Corrupt WAL tail | byte mutation | truncate-and-continue | storage WAL: `TestBadChecksumInFinalRecordIsRepaired`; Raft log: `TestTornTailIsTruncated` | VERIFIED (unit) |
| Corrupt WAL middle | byte mutation | refuse to start, clear error | storage WAL: `TestBadChecksumInMiddleRecordIsRefused`; Raft log: `TestMidCorruptionIsFatal` | VERIFIED (unit) |
| Crash during compaction | kill between protocol steps | orphan sweep; no reader sees a partial file set | `TestCrashDuringCompaction` (Phase 4, standalone engine) | VERIFIED standalone; hosted-engine windows are Phase 11 |
| Crash during flush | kill mid-flush | WAL replay reconstructs the memtable | `TestCrashDuringFlush` (Phase 3, standalone engine) | VERIFIED standalone; Phase 11 |

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
