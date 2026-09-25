# CRASH RECOVERY — crash windows of a Raft node (Phase 11)

Status: **implemented and verified (Phase 11)** — the crash-point seam in `internal/raftnode`
(`Point`, `Hook`, `DrainReadyAt`, `ApplyCommitted`), the I/O observation point in
`internal/fault` (`Injection.At`), the `crashat` event, the bounded exhaustive crash matrix and
the `crashpoints` chaos profile in `internal/raftsim`, `dkvd -crash-at`, the real-process crash
tests in `tests/integration/raft_crash_test.go`, the record-order fix in `internal/raftlog`, and
INV-CR1..CR4; ADR-018. This document is the crash-window model: for every boundary at which the
process can die, what is on disk, what recovery makes of it, and what the protocol does next.

> **Scope boundary, stated once.** Phase 11 characterises and proves the crash windows of the
> **Raft node as it exists**: the pure core, the durable log, the driver, and the state-machine
> seam it drives. It adds no client API, no request forwarding or deduplication, no linearizability
> checking, no snapshots, no dynamic membership, and no storage engine hosted by the node. Every
> conclusion about "the state machine" below is about the seam and the recording state machines
> the tests drive; the LSM engine is not yet behind that seam.

---

## 1. Why crash windows are their own phase

Phase 10 crashed nodes *between* events and lost power on their disks, and proved the invariants
held. But a process does not die between events; it dies between two `write(2)` calls, after an
fsync but before the reply that depended on it, after a message reached one peer but not the
next, after the state machine applied an entry but before the driver recorded that it had. Each
of those is a distinct window with a distinct durable state, and a system is only as
crash-correct as its worst window. Phase 11 enumerates the windows, dies in every one of them,
and checks the result against an independent record of what should be on disk. It found one
window that bricked a node (§5).

## 2. The lifecycle, and where the boundaries are

One driver cycle (`internal/raftnode`, the actor goroutine), after any input to the core:

```
   input: Tick / Step(msg) / Propose
        │
        ▼
   for core.HasReady():
        rd := core.Ready()                      HardState?, Entries, Messages
        ┌─ before-save ─────────────────────────────────────────────┐
        │  Save(hs, entries)   ─ raftlog ─┐  write record 1          │  I/O boundaries:
        │                                 │  write record 2 …        │  before the Nth
        │                                 │  fsync                   │  write / fsync
        └─ after-save ──────────────────────────────────────────────┘
        send(msg 0)  ─ after-send #0
        send(msg 1)  ─ after-send #1  …
        ─ before-advance
        core.Advance()
        ─ after-advance
   for e in core.NextApply():
        ─ before-apply(i)
        sm.Apply(i, data)
        ─ after-apply(i)
        core.AppliedTo(i)
        ─ after-applied-to(i)
```

Recovery (`raftnode.Recover` → `raftlog.Open`): open the file (fsync its directory if it was just
created); replay every record; truncate a torn tail (`truncate`); fsync the recovered state
(`fsync`); rebuild the in-memory log; clamp the commit to the log; construct the core as a
**Follower** at the recovered term and vote. The applied index starts at 0.

Every named boundary is a **crash point** (`raftnode.Point`), the same object in the simulator,
in an in-process driver test, and in a real `dkvd` process that kills itself there. The I/O
boundaries inside one `Save` and inside `Open` are addressed at the `vfs` seam
(`fault.Injection.At`): "before the Nth write / fsync / truncate of the log". A syncdir happens
once, at a fresh node's first boot, before any crash can be armed; its one window is pinned in
`internal/raftlog` (§9).

## 3. Durable and volatile state

| State | Where | Across a crash |
|---|---|---|
| `currentTerm`, `votedFor` | `HardState` record | durable once its `Save` returned (fsync); a crash before that keeps the previous one |
| log entries | `Entry` records | durable once their `Save` returned; a record is all-or-nothing on recovery |
| `commitIndex` | `HardState.Commit` | persisted as an optimization; clamped to the recovered log; never trusted beyond it |
| `appliedIndex` | nowhere | **volatile**: every restart starts at 0 and re-applies the committed prefix (§6) |
| role, `leaderID`, election timer | memory | reset: a restarted node is a Follower with no leader |
| `nextIndex[]`, `matchIndex[]`, votes received | memory | forgotten: a new leader recomputes them; a candidate starts over |
| un-drained `Ready`, un-sent messages | memory | lost; Raft retransmits whatever mattered |
| messages already sent | the network | in flight; they arrive at the new incarnation (§8, F) |
| the state machine's state | outside the node | whatever the state machine made durable (nothing, in this phase's recording machines) |

Nothing volatile can create a safety violation after recovery because nothing volatile is an
input to any safety rule: votes and terms are read from `HardState`, the log from its records,
and a leader's replication state is rebuilt from followers' replies. INV-R1..R10 are checked
continuously across every simulated crash and restart, exactly as in Phase 10.

## 4. Crash modes, kept distinct

- **A. Process crash after kernel-visible persistence** (`SIGKILL`; simulator `crash … process` or
  a crash point without `power`): every byte a `write` accepted survives, fsynced or not. Recovery
  sees the completed prefix of any interrupted `Save`.
- **B. Crash while persistence is in progress**: a crash point inside `Save` (`write:N`,
  `fsync:N`) or a torn write (`failpersist short`). Records written before the point survive a
  process crash; a record cut mid-way is a torn tail and is dropped whole.
- **C. Modeled power loss** (simulator only, `fault.MemFS`): only fsynced bytes survive, plus at
  most N torn bytes of the un-synced tail. Since `Save` fsyncs before returning, this differs from
  A only for a `Save` that had not completed — the state recovered is the last completed `Save`'s,
  possibly plus a torn prefix of the interrupted one.
- **D. Explicit persistence failure** (Phase 10, INV-F1): the log latches, the node fail-stops,
  nothing dependent is sent; the operator restarts it.

Which conclusions apply where: everything in this document is proven for **A** on real processes
(§8) and in the simulator, and for **B** and **C** in the simulator and at the `raftlog` level on
generated artifacts (§9). **Real power loss is not tested** — `MemFS` assumes a successful fsync is
honest and models un-synced loss as a prefix, never holes or reordered sectors (`docs/FAULTS.md`
§14). "SIGKILL proves durability" is never claimed: SIGKILL proves the bytes reached the kernel.

## 5. Persistence semantics — the record order inside a Save

A `Save` is a sequence of records followed by one fsync. Its order matters exactly when the
process dies between two of them, and two constraints pull in opposite directions:

1. **A term change must precede entries of the new term.** `currentTerm` may never be below the
   term of an entry in the log — a leader of that term existed — and the core refuses such a log
   (`ErrTermRegression`). With the pre-Phase-11 order (entries, then `HardState`), a crash between
   the entry record and the `HardState` record of a `Save` carrying both a term change and entries
   of the new term left exactly that log, and **the node could never restart**. Reachable: a
   single-node group's election is one such `Save` (term, self-vote and no-op together); the crash
   matrix found it on its first run (`TestSingleNodeCrashInsideItsElectionSave`).
2. **A commit index must never be durable before the entries it covers.** On a suffix replacement
   the old, conflicting entries are still on disk until the replacing records land; a `HardState`
   with the new commit written before them would, after a crash, mark those old entries committed
   and the restarted node would apply entries the cluster never committed.

`raftlog.SavePlan` is the order that satisfies both, as one pure function the log writes by and
the simulator's crash model reads (never a copy):

```
   hs == nil                     : entries
   no entries                    : HardState(term, vote, commit)
   entries, same term and vote   : entries, HardState(term, vote, commit)
   entries, term or vote changed : HardState(term, vote, PREVIOUS commit), entries,
                                   HardState(term, vote, commit)   ← only if the commit changed
```

Then, at every record boundary, what is on disk is one of: the previous state; the new term and
vote over the old log and old commit; those plus a prefix of the new entries; or everything. Each
is a state the core accepts and Raft permits (a node that persisted a term or a vote and died,
or one that appended entries it never acknowledged). `TestTermChangeIsDurableBeforeEntriesOfThatTerm`
and `TestCommitNeverCoversEntriesTheSaveHadNotWritten` pin the two constraints at the log level;
the matrix pins them at every boundary of a scenario; mutants 25–26 remove each in turn.

Everything else about the log is unchanged from Phase 9/10: a torn final record — an incomplete
record, or one whose checksum fails with **no bytes after it** — is truncated and the state is
the prefix before it; any other damage refuses to open (§9).

## 6. Replay semantics — what the state machine sees

`appliedIndex` is volatile. A restarted node re-applies its **entire recovered committed
prefix**, from index 1, before it does anything else. Consequently:

- **At-least-once across restarts, exactly-once within an incarnation.** Every committed index
  is applied once per incarnation that reaches it. A crash after `Apply(i)` but before
  `AppliedTo(i)` is not a special window: `i` would be re-applied after restart *anyway*, as
  would every index below it. There is no window in which an applied index is skipped.
- **The replay is identical.** Whenever two incarnations apply the same index they apply the
  identical entry (index, term, bytes) — INV-R5 across restarts, checked by the harness for every
  apply of every incarnation.
- **Commit is durable before apply (INV-CR3).** An entry is applied only in the cycle that
  persisted (fsynced) a `HardState` whose commit covers it, so a crash at any apply point
  recovers a commit at least as high as anything the dead incarnation applied. No incarnation
  ever applies an entry the next one would not consider committed.

This is the C4 "stage one" contract of `docs/CONSISTENCY.md`: application is at-least-once; a
state machine that must be idempotent by log index (the engine, when it is hosted, records
`appliedIndex` itself and skips at or below it — `docs/DESIGN.md` §10 step 7) will get that
guarantee from its own durable applied index, not from the driver. **Phase 11 adds no
deduplication and claims no exactly-once application**; it states exactly what the driver
guarantees, and tests it (`TestCrashAroundApply`, `TestCrashAfterApplyReappliesOnRestart`,
`TestCrashBeforeApplyAppliesOnceOnRestart`).

## 7. The bounded exhaustive crash matrix (simulator)

`raftsim.RunCrashMatrix(cfg, scenario, modes)`: run the scenario once with point recording on,
collecting every `(node, point, occurrence)` any node reached — every driver point and every
I/O boundary; then, for each of those and each crash mode, run it again on a fresh cluster with
`crashat node point n [power torn]` armed first. The node dies exactly there; the harness records
the durable state before the crash (the shadow's last completed `Save`) and the `Save` it
interrupted, restarts the node at once through `raftnode.Recover` (a crash *during* recovery — at
the torn-tail truncate or the fsync of the recovered state — costs one more restart), runs the
rest of the scenario (events for a down node are skipped, never errors), stabilizes, and requires
convergence (INV-F3). Every restart is checked against the shadow: INV-F2 and INV-CR1..3.

The report (`MatrixReport`, JSON via `-raftsim.matrix.out=FILE`, text via `Text()`) has one row
per crash:

```
node point#nth mode step persisted=t/v/c/n@t interrupted=[…] recovered=t/v/c/n@t reapplied=k restarts=r result
```

`TestCrashMatrix` runs the built-in scenario — elections and term bumps, commits on all nodes and
on a majority, a lagging follower catching up, a deposed leader's suffix replacement, a torn
append and the restart that repairs it, commits after all of it — in three modes (process crash;
power loss; power loss keeping a 13-byte torn tail). No cell may pass vacuously: every driver
point and every I/O boundary must occur in the scenario, every armed crash must fire, and a failing
cell is reported with its point, mode, seed and the `crashat` event that reproduces it. Size and
result at the final HEAD: **1,440 crashes (480 points × 3 modes), 0 failures**, in well under a
second — cheap enough to run on every `go test`.

## 8. What each tier proves

| Tier | Mechanism | Proves | Does not prove |
|---|---|---|---|
| Deterministic simulator (`internal/raftsim`) | `crashat` at driver and I/O points; the matrix; the `crashpoints` chaos profile; fuzz | the exact durable state at every boundary (INV-F2, INV-CR1..3), convergence after every crash, INV-R1..R10 throughout, all replayable from a seed or script | real timing, real TCP, real hardware |
| Real driver, in-process (`internal/raftnode`) | `Config.Hook` aborts the actor at a point; a new `Node` on the same log and the *same* state machine object | the real actor stops cleanly at the point, real files hold what a SIGKILL would leave, the real recovery path accepts them, and the state machine sees the documented replay (§6) | power loss (real files) |
| Real processes (`tests/integration`) | `dkvd -crash-at=POINT[:N]`: the process logs `event=crash_point` and SIGKILLs itself there | the seam fires where it says on a real process; the log a real SIGKILL leaves reopens and is coherent (no entry above the recovered term, commit within the log, term never below what was durable); the restarted process recovers exactly what the file holds; the group re-converges over real TCP with every committed prefix intact | which `Save` is "the Nth" (real timing decides); power loss |

Real-process points exercised (`TestRealCrashAtPoints`: a deposed leader rejoining a live
3-process group, whose catch-up Saves the new term and the new leader's no-op, sends replies,
and re-applies its recovered prefix at boot — a follower rejoining a quiet group would never
Save again, since `dkvd` has no client and the only entries are election no-ops; and only points
that occur on every timing path are targeted, since the catch-up is one Save or two depending on
whether the node's election timer fires before the leader's first AppendEntries):
`before-save:1`, `after-save:1`, `after-send:1`, `before-advance:2`, `after-advance:2`,
`before-apply:1`, `after-apply:1`, `after-applied-to:1`, `write:2`, `write:3`, `fsync:1` (a
crash during recovery's own fsync), `fsync:2`; and (`TestRealCrashAtEveryEarlyPointIsRecoverable`, a single-node group, whose election
`Save` is the §5 window) the first occurrence of every driver point and the first three writes and
two fsyncs — each must leave a log that reopens with no entry above its term, and each restart must
climb to a higher term. The Phase 9/10 SIGKILL tests remain
(`TestRaftLogSurvivesSIGKILL`, `TestHardStateSurvivesSIGKILL`, `TestCurrentTermMonotonicAcrossRestart`,
and the Phase 10 crash/restart scenarios).

## 9. Corruption and crash artifacts (`internal/raftlog`)

Policy (unchanged, now pinned per case): a **torn final record** — incomplete, or failing its
checksum with no bytes following — is truncated and everything before it is recovered
(`TestTornTailIsTruncated`, `TestCorruptedFinalRecordIsTreatedAsTorn`); repair is limited to that
tail, and the repaired file is fsynced before use. **Mid-log corruption** — a checksum failure with
records after it, a zero-filled header with data after it, an unknown record kind, a malformed
payload, an index gap — refuses to open (`TestMidCorruptionIsFatal`, `TestUnknownRecordKindIsFatal`,
`TestGapInIndexesIsFatal`). Recovery never rewrites history: reopening a log that needs no repair
changes no byte, and repeated reopens recover the same state
(`TestReplayIsIdempotentAcrossReopens`). A commit beyond the recovered log is clamped; a repeated
`HardState` lets the last one win (`TestCommitBeyondRecoveredLogIsClamped`).

Artifacts generated by *actually interrupting* persistence: an `Entry` record and a `HardState`
record each cut at every byte offset by a short write, then reopened — the partial record is
dropped whole, everything before it survives, `Inspect` (read-only) and `Open` agree
(`TestPartialRecordsFromInterruptedSaves`); a crash before each record write of a term-changing
`Save` (`TestTermChangeIsDurableBeforeEntriesOfThatTerm`); a crash before the directory fsync of a
brand-new log, after which a power loss removes the file and the node boots fresh — safe because
`Open` syncs the directory before any `Save` can happen (`TestCrashBeforeDirectorySyncLosesOnlyAFreshLog`).
Recovery refuses a log whose last entry's term exceeds its `HardState` term rather than inventing
a term (`TestRecoverRefusesATermBelowItsLog`) — the §5 order guarantees the node never writes one.

## 10. Invariants

The Phase 11 invariants are the **CR** series (crash recovery), a fresh namespace; INV-F2 stays
the umbrella and INV-R1..R10 stay in force. Each CR is checked in the simulator at the instant a
node boots, against the shadow's independent record of every `Save` it completed, in every
scenario, every matrix cell and every chaos seed.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-CR1 | **Term and vote never regress across a crash.** The recovered `currentTerm` is at least the last durably established one; if equal, a vote durably cast in it is recovered (a vote an interrupted `Save` was casting may or may not have landed). | `raftsim`: `checkRecovered` at every boot; `TestFollowerCrashAfterAdoptingAHigherTerm`, `TestVoterCrashAroundPersistingItsVote`, `TestSingleNodeCrashInsideItsElectionSave`; `raftlog`: `TestTermChangeIsDurableBeforeEntriesOfThatTerm`; real processes: the inspected term is never below the term the node had established | VERIFIED (simulation incl. modeled power loss; process kill) |
| INV-CR2 | **The durably committed prefix is recovered bit-identical**, and the recovered commit is never below the durable one. | `raftsim`: `checkRecovered` at every boot; `TestCrashDuringSuffixReplacement`; `raftlog`: `TestCommitNeverCoversEntriesTheSaveHadNotWritten` | VERIFIED (simulation; process kill) |
| INV-CR3 | **Commit is durable before apply**: an entry is applied only after a `HardState` whose commit covers it is fsynced, so the recovered commit is never below what the dead incarnation applied. | `raftsim`: `checkRecovered` at every boot, `TestCommitIsDurableBeforeApply`; `raftnode`: `TestCrashAfterApplyReappliesOnRestart` | VERIFIED |
| INV-CR4 | **Replay is exact and at-least-once**: after a restart `appliedIndex` is 0 and the node re-applies exactly its recovered committed prefix, in order, each index once per incarnation, with entries identical to those any earlier incarnation applied there. No exactly-once claim across restarts. | `raftsim`: `checkApply` (INV-P9 per incarnation, INV-R5 across), `TestCrashAroundApply`, `TestRepeatedCrashesAtPoints`; `raftnode`: `TestCrashAfterApplyReappliesOnRestart`, `TestCrashBeforeApplyAppliesOnceOnRestart` | VERIFIED |

INV-F2 (recovered state = last completed `Save` + a prefix of the interrupted one, in `SavePlan`
order) is now checked at every one of the matrix's crash points, not only between events.

## 11. Mutation testing

Nine Phase 11 mutants (`scripts/mutation.sh` 25–33), each a real edit that breaks one recovery
rule, each with a killer that fails; `make mutation` runs all 33.

| Mutant | Bug represented | Killed by |
|---|---|---|
| term-durable-before-entries-of-that-term | the old entries-first order: a crash bricks the node | `TestTermChangeIsDurableBeforeEntriesOfThatTerm`, `TestSingleNodeCrashInsideItsElectionSave` |
| commit-not-durable-before-its-entries | the leading `HardState` carries the new commit over old entries | `TestCommitNeverCoversEntriesTheSaveHadNotWritten` |
| recovered-commit-clamped-to-log | a persisted commit beyond the log is trusted | `TestCommitBeyondRecoveredLogIsClamped` |
| torn-tail-truncated-on-recovery | the torn tail stays and the next append lands behind it | `TestTornTailIsTruncated` |
| mid-log-corruption-is-fatal | every damage treated as a torn tail (silent skipping) | `TestMidCorruptionIsFatal` |
| recover-refuses-term-below-log | recovery accepts an incoherent log | `TestRecoverRefusesATermBelowItsLog` |
| restart-as-follower-never-leader | volatile leadership restored on restart | `TestCrashMatrix`, `TestCrashDuringSuffixReplacement` |
| applied-recorded-only-after-apply | `AppliedTo` before `Apply` (a crash loses an application) | `TestCrashPointAbortStopsExactlyThere`, `TestApplyFailureIsWrappedAndLeavesTheRestPending` |
| recover-restores-vote | the durable vote is forgotten on restart | `TestVoterCrashAroundPersistingItsVote`, `TestCrashMatrix` |

Considered and rejected as meaningless: "recovery ignores the persisted commit" — the commit is an
optimization the node re-learns; no invariant depends on it, so no honest killer exists.

## 12. Reproduction workflow

```bash
go test ./internal/raftsim -run TestCrashMatrix -v                 # the matrix, with a coverage summary
go test ./internal/raftsim -run TestCrashMatrix -raftsim.matrix.out=matrix.json   # the full report
go test ./internal/raftsim -run 'TestRandomizedFaultSchedules/crashpoints' -raftsim.profile=crashpoints -raftsim.seeds=200
# a failing seed prints its trace tail, the exact reproducing command and a minimized script:
go test ./internal/raftsim -run TestReplayScript -raftsim.replay=min.txt -raftsim.nodes=3 -raftsim.seed=42 -raftsim.verbose -v
go test ./internal/raftlog ./internal/raftnode -run 'Crash|Torn|Corrupt|Partial|SavePlan|Recover'
go test ./tests/integration -run 'TestRealCrashAt' -v
dkvd -id n0 -listen 127.0.0.1:7001 -raft -data-dir /tmp/n0 -crash-at fsync:2   # a real node that dies before its 2nd fsync
```

A `crashat` event in a script — `crashat n2 after-save 3` or `crashat n1 write 2 power 16` — arms
the crash for the nth occurrence counting from the event; the trace line `crashpoint n2 after-save
#3` marks where it fired.

## 13. Known untested cases

- **Real power loss**, in every form; modeled only (§4).
- **A torn write inside a real process**: `-crash-at` kills *between* operations; a real `write(2)`
  cut mid-way is produced only by the simulator's short writes and the `raftlog` artifact tests.
- **"After send" on a real process** means after the message was handed to its peer's outbox, not
  after it left the socket; the simulator's after-send is exact.
- **Which `Save` is the Nth** on a live cluster depends on real election timing; the real-process
  tests assert what holds whichever `Save` it was, and the simulator pins the exact windows.
- **The storage engine behind the state-machine seam** (WAL, flush, compaction windows while
  hosted by a node) — the engine is not hosted yet; its standalone crash tests are Phases 2–4.
- **Kernel fsync-error semantics** ("fsyncgate"), as in Phase 10.
