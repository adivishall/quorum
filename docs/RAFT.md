# RAFT — the consensus core (Phase 9)

Status: **implemented, Phase 9** (`internal/raft`, `internal/raftlog`, `internal/raftnode`;
ADR-016). This document specifies what was built and, as precisely, what it does **not** yet
prove. It implements §5.1–5.4 of Ongaro & Ousterhout, "In Search of an Understandable Consensus
Algorithm", against the repository-specific contract in `docs/DESIGN.md` §8.

> **Scope boundary, stated once.** Phase 9 proves Raft's **safety properties in deterministic
> simulation** and makes Raft state **durable across process kill**. It does **not** add
> end-to-end linearizability, the full network fault matrix (Phase 10, `docs/FAULTS.md`), a client/HTTP API or
> request forwarding (Phases 13/15), linearizable-read serving / ReadIndex (later), snapshots
> (Phase 14), or dynamic membership (never, in v1 — ADR-005). Raft working is not the finished
> Quorum consistency model.
>
> **Phase 12 update (ADR-019, `docs/LINEARIZABILITY.md`).** The core gained **ReadIndex**
> (`Raft.ReadIndex`, `Ready.ReadStates`): every AppendEntries carries a heartbeat sequence
> `Message.Seq` that every response echoes; a read registered by a leader at
> `max(commitIndex, own no-op index)` is confirmed once a quorum has answered a request sent after
> it, and dropped on any role change. The driver completes a client write only when its entry is
> applied in its proposal's term (`raftnode.Node.Write`, `Waiters`; otherwise `ErrLost`) and a
> read only once the store has applied through its confirmed read index (`Node.ReadIndex`,
> `Reads`). The core is still pure. End-to-end linearizability of client histories is verified
> there, with its bounds.
>
> **Phase 15 update (ADR-022, ADR-023, `docs/MEMBERSHIP.md`, `docs/MULTI_RAFT.md`).** Membership
> is no longer static: the core implements Raft §6 joint consensus with learners, one member at a
> time, and every quorum asks the current configuration (§13). A node hosts several groups, each
> with its own core and driver. ADR-005's "static membership in v1" is superseded.

---

## 1. Architecture — three layers plus a test harness

Raft is split so the algorithm can be tested without a network, a clock, or a disk (ADR-002,
ADR-016):

```
    inputs: Tick() / Propose(data) / Step(msg)
                    │
        ┌───────────▼─────────────┐
        │  internal/raft.Raft     │   pure, deterministic core (no I/O, no clock, no goroutines)
        │  drives replication.Log │
        └───────────┬─────────────┘
        Ready{HardState, Entries, Messages}  +  NextApply()/AppliedTo()
                    │
        ┌───────────▼─────────────┐   ┌─────────────────────────┐
        │  internal/raftnode      │──▶│  internal/raftlog       │  durable log + HardState
        │  driver: goroutine,     │   │  (record framing)       │
        │  transport, ticks, apply│   └─────────────────────────┘
        └───────────┬─────────────┘
                    │  transport kinds (ADR-013)
        ┌───────────▼─────────────┐
        │  internal/transport     │  generic byte carrier — knows nothing of terms/logs
        └─────────────────────────┘

    tests: internal/raft simulated network — N cores, one goroutine, a message queue we control.
```

- **`internal/raft`** — the pure core. Only `Tick`, `Propose`, `Step` change state; only
  `Ready`/`Advance` and `NextApply`/`AppliedTo` observe/drain it. It owns no sockets, goroutines,
  wall-clock, filesystem, or global randomness. Its log is a `replication.Log` (Phase 8), so Raft
  *drives the Phase 8 primitive* directly.
- **`internal/raftlog`** — the durable Raft log: an append-only `internal/record` stream of
  `Entry` and `HardState` records, with recovery.
- **`internal/raftnode`** — the driver: one goroutine per group, owning the transport adapter,
  the tick source, persistence ordering, and the apply loop.
- **The simulated network** (test-only, in `internal/raft`) runs N cores in one goroutine with a
  message queue the test controls — the deterministic proving ground for the invariants.

## 2. State

Per Raft node (one group):

- **Persistent** (durable before any dependent reply): `currentTerm`, `votedFor`, and the log
  entries. `commitIndex` is persisted **as an optimization only** — recovery clamps it to
  `min(persisted, lastIndex)` and correctness never depends on it (`docs/DESIGN.md` §8.1).
- **Volatile, all servers:** `commitIndex`, `lastApplied` (both held in the `replication.Log`).
- **Volatile, leader only:** `nextIndex[peer]`, `matchIndex[peer]`.
- **Role:** an explicit `Role` type — `Follower`, `Candidate`, `Leader` — never a string.

The log is 1-based and contiguous with 0 as the empty sentinel (`Term(0) == 0`), inherited from
Phase 8; terms are non-decreasing; a committed entry is never overwritten.

## 3. Election timing (tick model)

The core counts logical **ticks**; the driver supplies them from a real `time.Ticker`. Defaults
(`docs/DESIGN.md` §8.3): tick = 50 ms, `electionTicks` = 10, `heartbeatTicks` = 2. The randomized
election timeout is drawn uniformly in `[electionTicks, 2*electionTicks)` from an **injected**
`rand.Source`, re-drawn on each conversion to follower/candidate. The core calls no `time.*` and no
global `math/rand`, so it is replayable from *initial state + seed + event sequence*.

## 4. Roles and transitions (`docs/DESIGN.md` §8.2)

- **Follower election timeout** → become Candidate: `term++`, `votedFor = self`, persist, broadcast
  `RequestVote`.
- **Candidate election timeout** → new election: `term++`, vote self, persist, broadcast again.
- **Candidate with votes ≥ quorum** → become Leader: set `nextIndex[peer] = lastIndex+1`,
  `matchIndex[peer] = 0`, **append a mandatory no-op entry in the current term** (not optional —
  §5.4.2/ReadIndex depend on it), persist, begin replication/heartbeat.
- **Candidate receiving `AppendEntries` at ≥ own term** → become Follower and handle it.
- **Any node seeing `msg.Term > currentTerm`** → `currentTerm = msg.Term`, `votedFor = nil`,
  become Follower, **persist before replying**.
- **Leader seeing a higher term** → step down to Follower; in-flight proposals had already been
  accepted into the log and simply may not commit under this node's leadership.

## 5. RequestVote (§5.2, §5.4.1)

Fields — request: `term, candidateID, lastLogIndex, lastLogTerm`; response: `term, voteGranted`.
A vote is granted iff **all** hold:

1. `msg.Term ≥ currentTerm` (a lower term is rejected with our current term, mutating nothing else —
   INV-R10);
2. `votedFor` is unset or already equals the candidate (in that term);
3. the candidate's log is **at least as up to date**: `(lastLogTerm, lastLogIndex)` compared
   lexicographically ≥ ours.

`currentTerm` and `votedFor` are persisted **before** the granting response is sent, so a node can
never grant two votes in one term across a crash (INV-R1, INV-R6).

## 6. AppendEntries (§5.3)

Fields — request: `term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit`; response:
`term, success, conflictTerm, conflictIndex, matchIndex`.

**Follower handling:**

1. Reject a stale `term` (reply with current term; mutate nothing else — INV-R10).
2. If `msg.Term > currentTerm`: adopt it, clear `votedFor`, become Follower, persist before the
   dependent reply.
3. Recognize the sender as leader for this term; reset the election timeout.
4. If `prevLogIndex > lastIndex`: reject with `conflictIndex = lastIndex+1`, `conflictTerm = 0`
   (we are missing entries).
5. If the term at `prevLogIndex` ≠ `prevLogTerm`: reject with `conflictTerm` = the term actually at
   `prevLogIndex` and `conflictIndex` = the first index of that term (so the leader backs up a whole
   term — §10).
6. Otherwise, for the entries that follow: find the first index where our term disagrees with the
   leader's, and `TruncateAndAppend` from there — replacing only the **uncommitted** conflicting
   suffix, never a committed entry (the Phase 8 log refuses that). Entries we already have identically
   are left in place (so a duplicated/reordered AppendEntries cannot truncate committed history).
7. Advance `commitIndex` to `min(leaderCommit, indexOfLastNewEntry)`.
8. Reply `success` with `matchIndex` = the last index now guaranteed to match the leader.

The follower drives the Phase 8 `Log` (`Term`, `Slice`, `TruncateAndAppend`, `Commit`) rather than
bypassing it.

## 7. Conflict hint and leader back-up (§10)

A rejection carries `(conflictTerm, conflictIndex)`. The leader, on a rejection, backs up
`nextIndex[peer]` by a **whole term**: if it has any entry in `conflictTerm`, it sets `nextIndex` to
the index after its last entry of that term; otherwise it sets `nextIndex = conflictIndex`. A
follower that is many entries behind is caught up in O(#terms) round trips, not O(#entries). The
tests assert the *number and pattern* of AppendEntries attempts, not merely eventual convergence.

## 8. Commit rule (§5.4.2) — the figure-8 defense

The leader advances `commitIndex` to `N` only when **both**:

1. a quorum (including itself) has `matchIndex ≥ N`; and
2. the entry at `N` is from the leader's **current term**.

A leader **never** commits an entry from a previous term by replica-counting alone. The mandatory
no-op appended on election is what lets a new leader commit at all: once the no-op (current term)
commits, everything before it commits with it. This is verified by the mandatory **`TestFigure8`**
regression, which constructs the exact scenario where naive replica-counting would commit an
old-term entry that is later overwritten, and asserts correct Raft does not.

## 9. Apply path

Commit advancement (leader or follower) is recorded in the `replication.Log` via `Commit`. The
driver then pulls `NextApply()` (= the log's `Unapplied()`, i.e. `(appliedIndex, commitIndex]`),
applies each command to the state-machine seam **in order**, and only then calls `AppliedTo` (=
`Apply`). An uncommitted entry is never applied (INV-R7), `appliedIndex ≤ commitIndex` always
(Phase 8 enforces it), and an apply failure does not advance `appliedIndex` — it stops the node
(§17). Phase 9 drives a
minimal deterministic state machine to verify these semantics; it adds no dedup and claims no
exactly-once client application (`docs/CONSISTENCY.md` C4). `appliedIndex` is **volatile**: a
restart re-applies the whole recovered committed prefix from index 1, so application is
at-least-once across restarts and exactly-once within an incarnation — pinned in Phase 11
(`docs/CRASH_RECOVERY.md` §6, INV-CR4). The apply loop is one shared function,
`raftnode.ApplyCommitted`, with the driver's crash points in it.

## 10. Persistence and ordering (`internal/raftlog`)

The durable log is an append-only record stream (§2 framing) of three record kinds:

- **`Entry`** — `index, term, data` (bounded varints + length-prefixed bytes).
- **`HardState`** — `currentTerm, votedFor, commitIndex` (commitIndex an optimization).
- **`Boundary`** (Phase 14) — `index, term`: a durable snapshot covers the log through `index`.
  Replay applies it by the install rule (keep the entries after it only if the log holds `index`
  with that term); a compaction rewrites the file as boundary + HardState + the entries after it,
  atomically (temporary file, fsync, rename, directory fsync) — §15, `docs/SNAPSHOTS.md` §5.
- **Typed `Entry`** (Phase 15, record kind 4) — a type byte, then the `Entry` encoding. Only a
  configuration entry uses it, so a Phase 9–14 log reads unchanged. Recovery refuses a typed record
  whose configuration does not decode or has no voters, an unknown type, and a normal entry written
  as typed — each is corruption, never repaired (`TestVoterlessConfigurationEntryIsCorruption`).
  Compaction's rewrite preserves types.

A conflicting-suffix replacement is **appended**, not rewritten: recovery replays records in file
order and applies each `Entry` at index `i` as "set `i`, drop anything above `i`" (the Phase 8
`TruncateAndAppend` semantics per record), reconstructing the final log from an append-only file;
the last `HardState` wins. Every append that a reply depends on is `f.Sync()`'d **before** the
reply is sent — the `Ready` contract makes this structural: the driver persists all of a `Ready`'s
`HardState` and `Entries` before sending any of its `Messages`.

**Record order inside one Save (Phase 11, `raftlog.SavePlan`).** A crash between two records of a
`Save` must leave a log recovery accepts. A changed term or vote is written **first** — carrying
the previously durable commit — then the entries, then the `HardState` with the new commit if it
changed. The first rule keeps `currentTerm` at or above every entry's term (the core refuses the
reverse, and the pre-Phase-11 entries-first order let a single-node election's crash brick the
node); the second keeps a commit index from ever being durable before the entries it covers,
which on a suffix replacement would mark the old conflicting entries committed. `SavePlan` is one
pure function, shared with the simulator's model of what a crash may leave (`docs/CRASH_RECOVERY.md`
§5).

**Durability is the default, not an opt-in.** `raftnode.Config` is durable at its zero value: the
driver fsyncs every `Save` unless `DisableSync` is explicitly set, and `DisableSync` exists only for
tests/benchmarks where durability is not under test (production leaves it false). The default is
pinned by a test (`TestDefaultConfigIsDurable`) that asserts the effective log options, so the
unsafe zero-value behaviour cannot be reintroduced silently. `f.Sync()` here is a kernel-level
fsync: it makes an acknowledged write survive **process kill**, not proven against power loss (the
same honest bound the WAL carries, `docs/FAILURE_MODEL.md`).

**Crash policy** (distinct from the WAL's; ADR-016): a torn final record truncates to the last good
offset (crash mid-append; the dependent reply was never sent). A checksum mismatch with bytes
following, a zero-filled header with data after it, an unknown kind, a malformed payload, or an
impossible index progression is **fatal** — the log refuses to open rather than silently skip a
record.

**Failure policy (Phase 10, INV-F1; `docs/FAULTS.md` §10).** A failed or short write, or a failed
fsync, poisons the log: every later `Save` returns the original error (`raftlog.ErrFailed`) and
writes nothing, because appending behind a partial record would turn a recoverable torn tail into
mid-log corruption, and after a failed fsync a later successful one vouches for nothing. The
driver fail-stops: none of the failing Ready's messages is sent, the actor never drives the core
again, `Node.Err`/`Node.Done` report it, and `dkvd` exits 1. All file access goes through the
`vfs` seam (`raftlog.Options.FS`, `raftnode.Config.FS`; nil is the real OS), which is where Phase
10 injects persistence faults underneath this unchanged code.

## 11. Recovery (`docs/DESIGN.md` §10, steps 4–5)

On restart the driver reads the durable log → `currentTerm`, `votedFor`, entries, and the persisted
`commitIndex` (clamped) — and, since Phase 10, `raftlog.Open` **fsyncs the file before returning**,
so whatever it recovered is durable before the node acts on it. (A Save whose fsync failed can
leave records in the page cache; without this, a restarted node could acknowledge entries a power
loss would still erase — the Phase 10 simulator found exactly that, `docs/FAULTS.md` §13.) The
startup path is one function, `raftnode.Recover`, which the deterministic simulator uses too. It builds a `MemoryLog` from the entries, sets `commitIndex`, and
constructs the core in Follower state at the recovered term. Recovery verifies indexes are still
contiguous, terms non-decreasing, and `currentTerm` does not move backward; incoherent state is
refused, not repaired (`TestRecoverRefusesATermBelowItsLog`). Since Phase 14 recovery first loads
and validates the published snapshot and reconciles it with the log — the in-memory log starts at
the log's boundary, the state machine is restored from the snapshot and `appliedIndex` set to its
index; an install that crashed after publishing is completed; every contradiction is refused
(`docs/SNAPSHOTS.md` §6). Since Phase 14 `raftlog.Open` also fsyncs the log's directory on every
open, so a compaction's rename that survived a process crash is durable before the node acts.
Phase 11 (`docs/CRASH_RECOVERY.md`) crashes the node at every boundary of this cycle and of
recovery itself and checks what each restart recovers against an independent record of every
`Save` (INV-F2, INV-CR1..4).

Durable HardState recovery is verified against real process death: `TestHardStateSurvivesSIGKILL`
SIGKILLs a live `dkvd -raft` and reads the recovered `currentTerm` and `votedFor` back from the
log file (via `raftlog.Inspect`, a read-only no-truncate reader), and
`TestCurrentTermMonotonicAcrossRestart` shows `currentTerm` climbs across a real restart (reachable
only if it was recovered, not reset). The persist-before-reply ordering is tested on the actual
driver path — not just the core's `Ready` — by `TestPersistBeforeReplyOnDriverPath`, whose hook
transport reads the durable log at the instant a reply is sent and requires the HardState to
already be there. Where the storage engine is eventually
involved, the documented `engine.appliedIndex ≤ raft.lastIndex` invariant is preserved.

## 12. Determinism and the simulated network

The core is fully deterministic: given the same initial state, seed, and event sequence it produces
the same state **and the same messages in the same order**. Message ordering never depends on map
iteration (peers are iterated in a sorted, fixed order). The test-only simulated network runs N
cores in one goroutine with an explicit message queue. It gives a test direct control over message
scheduling, and each mode is actually exercised (`schedule_test.go`):

- **FIFO delivery** — `deliverOne`/`deliverAll`.
- **Duplicate delivery** — `TestDuplicateDelivery` delivers every message twice; the per-node
  apply-once check confirms no entry is applied twice.
- **Reordered delivery** — `TestReorderedDelivery` drains the queue and delivers it in reverse.
- **Delayed delivery** — `TestDelayedDelivery` holds one follower's message back while the cluster
  makes progress, then releases the now-stale message.
- **Dropped delivery** — `TestDroppedDelivery` partitions a follower (messages dropped), then heals
  and relies on retransmission to catch it up.

The continuously-checkable invariants (R1/R3/R5/R7) run after every delivery, so a bad schedule
that induced a safety violation is caught immediately. This is enough deterministic control to
prove the algorithm and is deliberately **not** the Phase 10 fault-injection framework — it is
hand-scheduled correctness testing, not a reusable systematic failure-matrix generator. No test
result depends on wall-clock timing or on `time.Sleep`. The fault framework is
`internal/raftsim` (`docs/FAULTS.md`): it drives the same core, the real durable log on a
crash-modeling disk, and the driver's own persist-then-send and recovery functions under seeded,
replayable schedules of drops, duplicates, delays, partitions, crashes, power losses, pauses and
persistence failures.

## 12a. Mutation testing

The correctness suite is checked for teeth by a mutation runner (`scripts/mutation.sh`, `make
mutation`). For each mutant it applies a real source edit that violates one Raft rule, runs the
test(s) that should catch it, and requires them to **fail** (the mutant is "killed"); every edit is
reverted with `git checkout`, so the tree is unchanged afterward. A mutant that survives fails the
runner. It is not a source-string inspection — it exercises altered behaviour. The eight mutants
and their killers:

| Mutation | Killed by |
|---|---|
| remove the current-term commit restriction | `TestCommitRuleRequiresCurrentTerm` |
| remove the mandatory election no-op | `TestNoOpAppendedOnElection` |
| grant a second vote to a different candidate in a term | `TestVoteGrantedOncePerTerm`, `TestVoteAgainstReferenceModel` |
| overwrite a committed suffix | `TestCannotReplaceCommittedEntry` (Phase 8 log) |
| disable term-based conflict backtracking (naive per-index) | `TestConflictBackupByTerm` |
| let a stale lower-term message mutate state | `TestStaleAppendResponseIgnored`, `TestStaleMessageIsInert` |
| fail to step down on a higher term | `TestHigherTermForcesStepDown`, `TestLeaderCompleteness` |
| send a dependent reply without persisting HardState first | `TestPersistBeforeReplyOnDriverPath` |

A note on the current-term commit rule: because the **mandatory no-op** puts a current-term entry
at the leader's tail, and `matchIndex` only advances by acked prefixes, the dangerous state (a
quorum holding a *prior*-term entry but not a current-term one) is unreachable through the normal
message flow — so removing the guard breaks no end-to-end test. The rule is therefore pinned
directly on `maybeCommit` by `TestCommitRuleRequiresCurrentTerm`, which constructs that exact state
and asserts the guard rejects it. This keeps the §5.4.2 rule tested as defense-in-depth even though
the no-op is its load-bearing partner (whose removal `TestFigure8`/`TestNoOpAppendedOnElection`
catch).

Phase 10 adds 14 mutants for its failure-handling rules and for the fault harness's own fidelity
(`docs/FAULTS.md` §12), plus 2 for the transport's dead-connection handling; Phase 11 adds 9 for
its crash-recovery rules (`docs/CRASH_RECOVERY.md` §11); Phase 12 adds 27 for ReadIndex, write
completion, the client protocol and policy, the state machine, the codecs and the checker
(`docs/LINEARIZABILITY.md` §11); `make mutation` runs all 60.

**What counts as a kill** (since the audit hardening, mutants 173–270 cover its fixes): a target
must be tracked by git and clean, or it is not mutated (`git diff` is blind to an untracked file, so
its mutation went unnoticed and was never reverted). A kill must be a failing test — a `--- FAIL`
line, or the test binary dying in a test (a panic, a timeout); a run that fails otherwise is not
attributable to the mutant and fails the runner, and so does a mutant that does not compile —
including one that breaks only `dkvd`, which a real-process test builds itself and reports as its
own failure. For real-process killers, whose timing is real,
the same tests are re-run on the clean tree and the kill counts only if they pass there, so a flaky
failure cannot pass for one; `CONFIRM=1` does this for every mutant. A pattern matching several
sites is reported, since only the first is mutated. `DRY=1` checks every pattern still applies
without running tests — a whole-suite dry run after this branch's changes found six mutants whose
code had moved, now re-targeted.

## 13. Multi-Raft and membership (Phase 15)

*Phases 9–14:* membership was static (ADR-005) and quorum was `⌊n/2⌋+1` over a fixed peer list.
*Phase 15* replaces both; `docs/MEMBERSHIP.md` is the full specification and this section is what
changed in the core.

- **The configuration is replicated state.** A group's configuration — voters, learners and,
  while joint, the outgoing voters — is the latest configuration entry in the node's log, committed
  or not, or else the base configuration its snapshot (or genesis) gives. The core holds no
  membership of its own; `ConfAt(i)` answers the configuration at index `i` only with evidence and
  says it does not know otherwise (`TestConfAtAnswersOnlyWithEvidence`).
- **One quorum function.** Elections, commit and ReadIndex confirmation all ask `quorumOf` over the
  current configuration: a majority of its voters, and while joint a majority of its outgoing
  voters too. Learners never count. No count of peers remains.
- **Four operations, one at a time.** `AddLearner`, `RemoveLearner` (one entry each), `Promote` and
  `RemoveVoter` (a joint entry, then the final one, which the leader appends as soon as the joint
  one commits). A change is refused while another is under way.
- **Who campaigns, whose votes count.** A voter of the current configuration campaigns; so does a
  leader that lost its leadership while removing itself, under the joint and final configurations
  together, so a removal can always finish. Any node may grant a vote; a candidate counts only the
  voters. Messages from a non-member are dropped, except a vote request whose log is at least as
  up to date as the receiver's.
- **Leaving.** A leader whose final configuration excludes it steps down when that configuration
  commits; a removed node never leads a later term (INV-MB4).
- **Snapshots carry the configuration** at their index, joint included, and an install adopts it.

**Multi-Raft.** One shard is one independent Raft group (ADR-001). A node runs one core and one
driver per group it hosts; groups share only the process, the transport and the data directory
(`docs/MULTI_RAFT.md`). The core never sees a group id.

## 14. What Phase 9 proves, and what it does not

**Proven (deterministic simulation + process-kill crash tests), the INV-R series:** election safety
(R1), leader append-only (R2), log matching (R3), leader completeness (R4), state-machine safety
(R5), durable `currentTerm`/`votedFor` before dependent replies (R6), never applying beyond commit
(R7), `commitIndex` monotonic across restart (R8), leader commits only current-term entries (R9),
stale lower-term messages are inert (R10). See `docs/INVARIANTS.md` for the exact tests behind each.

**Not proven / not present in Phase 9:** behavior under the full network fault matrix
(drop/delay/dup/partition at scale — since established by Phase 10, `docs/FAULTS.md`, with the
qualifiers stated there); end-to-end linearizability (Phase 12); client
retry / dedup / exactly-once application (Phase 13); linearizable-read serving / ReadIndex; an HTTP
API or request forwarding (Phases 13/15); snapshots and log compaction (Phase 14); dynamic
membership (Phase 15, §13). Power-loss durability is not tested (SIGKILL only), as everywhere in this
project (`docs/FAILURE_MODEL.md`). **No end-to-end linearizability guarantee, no client/API
semantics, no snapshots, and no dynamic membership were added.** Raft functioning is a necessary
part of the Quorum consistency model, not the whole of it.

## 15. Snapshots and log compaction (Phase 14, Raft §7)

The in-memory log (`replication.MemoryLog`) has a **boundary** `(index, term)` — the last compacted
entry: `FirstIndex = boundary + 1`, `Term(boundary)` is answerable, anything below is
`ErrCompacted`, and `boundary ≤ applied ≤ commit ≤ last`. `Compact(i)` needs `i ≤ applied`;
`InstallSnapshot(i, t)` needs `i > commit`, keeps the suffix only if the log holds `i` with term
`t`, and sets commit = applied = `i`. The pure core adds two messages and nothing impure:

- **Leader.** When `nextIndex[peer] ≤ boundary` the entries the peer needs are gone: the core emits
  `MsgSnapshot(boundary, term)` once, marks the peer pending, sends it only heartbeats at the
  boundary meanwhile, ignores its rejections (they say nothing new), and withdraws the offer after
  `SnapshotRetryTicks` ticks without an answer so the next heartbeat offers again. A success
  (`MsgSnapshotResponse` or an AppendEntries success) at or beyond the offer ends it and
  replication resumes at once.
- **Follower.** Term rules as for AppendEntries. A snapshot at or below the commit index is already
  covered: answered success at the commit, nothing changed. Otherwise the log is reset to it, and
  the `Ready` carries `Snapshot` — which the driver must make durable (term, publication, boundary
  record, state machine) before the `Ready`'s messages, the response among them.
- **A stale rejection never backs `nextIndex` up to or below `matchIndex`** (the peer's log holds
  the leader's entries through it for the rest of the term). Before Phase 14 such a rejection cost
  a resend; with a compacted prefix it stranded the peer in a covered-snapshot loop — found by the
  200-seed gate, `docs/SNAPSHOTS.md` §17.
- **Boundary-aware helpers.** AppendEntries skips entries below the follower's boundary; the
  conflict back-up (`firstIndexOfTerm`, `lastIndexOfTerm`) stops at the boundary; `TermAt` names a
  snapshot for the driver.

Every FirstIndex == 1 assumption was audited: the log, the core, the durable log, the driver, the
simulator's checks and the integration harness are boundary-aware; helpers that need a whole log
refuse a compacted one. Evidence: `internal/raft` snapshot tests (a lagging follower catching up,
an unanswered offer re-offered, a matching suffix kept, a covered snapshot ignored, AppendEntries
below a boundary, compaction needing the applied index, back-up stopping at the boundary) and 40
randomized schedules with compaction; `internal/replication`'s differential test against a
reference log with `Compact` and `InstallSnapshot`; everything in `docs/SNAPSHOTS.md`.

## 16. The entry-size limit

One constant bounds an entry's bytes everywhere: `replication.MaxEntryDataLen` = 1 MiB, aliased as
`raft.MaxEntryDataLen` and `raftlog.MaxEntryDataLen` and, at the client, `kv.MaxCommandLen`. It is
the bound every on-disk and on-wire decoder has always read an entry by; what was missing was its
enforcement where entries are **created** and **written**. The invariant:

> No code path creates, holds, persists, transmits or accepts a Raft entry whose data exceeds
> `MaxEntryDataLen`.

| Boundary | Enforcement | Test (mutant) |
|---|---|---|
| Client front (`kv.Request.validate`) | the **encoded** command, not the raw key and value, must fit: `INVALID_REQUEST`, before anything is proposed or forwarded | `TestEncodedEntryLimitDecidesWriteAdmission` (173) |
| Command (`kv.Command.Validate`, so `Decode`) | an over-limit command is malformed | same (174) |
| Proposal (`raft.Propose`) | `ErrEntryTooLarge` on any node, checked before the role: definite, nothing appended | `TestProposeOverTheEntryLimitIsRefused` (175) |
| In-memory log (`replication.MemoryLog`) | `Append`/`TruncateAndAppend` refuse it, leaving the log unchanged | `TestEntrySizeLimit` (177) |
| Receipt (`raft.Step`; the AppendEntries codec) | an AppendEntries carrying one is malformed and has no effect, not even its term; the codec never decodes one | `TestStepRefusesAnOversizedAppendEntries` (176), `TestCodecEntryLimit` |
| Persistence (`raftlog.Save`) | checked for every entry before any byte is written; the log fails (it cannot claim durability for what its caller holds) but the file is untouched and reopens | `TestSaveRefusesAnEntryOverTheLimit` (178) |
| Recovery (`raftlog` replay) | refuses a longer entry, as before — now the same bound `Save` writes by | `TestSaveAndReplayAtTheEntryLimit` |
| Driver and server (`raftnode.Node.Write`, `kv.Server`) | `raft.ErrEntryTooLarge` is a definite refusal: `INVALID_REQUEST`, never `UNKNOWN` | `TestEntryTooLargeFromBelowIsInvalid` (179) |

A configuration entry always fits (`replication.MaxEncodedConfiguration` is checked against the
limit at compile time); the election no-op is empty. End to end, `TestEntryLimitEndToEnd` (three
real drivers) and `TestRealEntryLimit` (three `dkvd` processes, a full-cluster SIGKILL) commit an
entry of exactly the limit on every node, refuse one byte more at every node, read every node's
durable log to confirm nothing over the limit reached it, and restart every node from its own log.

**Why the limit was not raised instead.** A 1 MiB value plus its key and framing could have been
made to fit by raising the entry limit by a few KiB. That would have widened what every decoder
accepts on disk and on the wire — a format change — to keep a value size that never worked; the
client contract instead states the limit that decides, the encoded entry (`docs/API.md` §3).

**What bounds a message.** The limit bounds one entry; an AppendEntries carrying many is bounded
by the core's per-message budgets (§18).

## 17. Bounds on a leader's outstanding work, abandoned requests, apply failures

**Bounds (audit M3).** A leader cut off from its quorum never commits, and — with no CheckQuorum —
never steps down while nothing reaches it. Before these bounds, every request it accepted in that
state stayed: an uncommitted, persisted entry resent on every broadcast and a client waiter, or a
read awaiting a confirmation that never came. The core now bounds them (`raft.Config`):

| Bound | Default | Beyond it |
|---|---|---|
| `MaxUncommittedEntries` — entries in the leader's `(commit, last]`, its no-op and inherited tail included | 1024 | `Propose` → `ErrBusy` |
| `MaxUncommittedBytes` — their data bytes | 64 MiB | `Propose` → `ErrBusy`, unless the tail holds no data (so no entry within `MaxEntryDataLen` is refused forever) |
| `MaxPendingReads` — reads registered and not yet confirmed | 1024 | `ReadIndex` → `ErrBusy` |

`ErrBusy` is definite — nothing appended or registered — and the key-value server answers it
`UNAVAILABLE` (`docs/API.md` §5). Healthy operation is far below the bounds: the uncommitted tail
is about the writes in flight, and a read is confirmed within a heartbeat. The leader keeps the
sizes of its uncommitted tail as it appends and commits (computed once from the log when it
becomes leader), so a proposal's check costs nothing; every core test checks that bookkeeping
against the log after every step (`assertUncommittedTail`). Configuration changes are not bounded:
one is in progress at most. Tests: `TestIsolatedLeaderRefusesProposalsBeyondItsBound`,
`TestUncommittedBytesBound`, `TestInheritedTailCountsTowardTheBound`,
`TestIsolatedLeaderRefusesReadsBeyondItsBound` (core); `TestIsolatedLeaderRefusesWorkBeyondItsBounds`
(three real drivers, 1,600 writes and reads at an isolated leader); `TestBusyFromBelowIsUnavailable`.

**Abandoned requests.** A client whose deadline passes after its request was accepted tells the
node's actor, which forgets the write's waiter or the unconfirmed read at once (`Waiters.Cancel`,
`Reads.Cancel`); before, a waiter stayed until its index was applied — for an entry an isolated
leader appended, possibly never, if the log that replaced it never grew that far. The notice is
non-blocking and best-effort (a full buffer drops it, and the waiter is then released when its
index is applied); it decides nothing — the entry, if it commits, is applied as before, and the
outcome stays unknown to that client. A waiter is never completed early as `ErrLost` when its
entry is truncated from this node's log: another leader that holds the entry may still commit it.
`TestAbandonedRequestsLeaveNothingBehind`.

**Apply failures (audit M4).** A state machine that returns an error from `Apply` for a committed
entry refuses it on every replica and on every retry — a committed entry is the same everywhere.
The node now fail-stops on it exactly as on a persistence failure: `event=raft_apply_failed`,
`Node.Err()` wraps `ErrApply`, every waiting client learns the error, and `dkvd` exits 1 (`-raft`)
or stops that group alone (`-cluster`). Before, the error was logged and retried every cycle,
forever: the group stalled behind the entry while its leader went on accepting writes it could
never apply. `appliedIndex` never passes the refused entry, so a restart refuses it again; the
entries applied before it in the failing cycle complete, and the published `Status` covers them
(`TestApplyFailureStopsTheNode`). **The contract for a state machine:** `Apply` either applies
the command completely or returns an error with no effect at all — the key-value store's only
error, an undecodable command, leaves it unchanged — and an error means the entry can never be
applied. Recovering such a group needs an operator: a state machine that accepts the entry (a
fixed binary), or restoring from a snapshot past it.

Mutants 214–226 and 270 (`scripts/mutation.sh`) break each bound, the bookkeeping, each release of an
abandoned request, the fail-stop and the `UNAVAILABLE` mapping; each is killed by its test.

**Not done here.** PreVote and CheckQuorum — an isolated leader that steps down by itself —
remain open (`docs/ENGINEERING_ROADMAP.md`); the bounds limit what it holds meanwhile.

## 18. Replication flow control (audit H4)

Before this section, `sendAppend` sent a follower every entry from its `nextIndex` to the end of
the log, and `nextIndex` moves only on a response — so every broadcast (a heartbeat, a proposal, a
read) resent the whole unacknowledged tail, and a backlog beyond the transport's 16 MiB frame or
the decoder's 65,536 entries could never be sent: the follower stayed behind until a snapshot
moved past it, or for good with snapshots off. Three changes:

- **Budgets.** One AppendEntries carries at most `MaxEntriesPerMsg` entries (default 4096, at most
  `MaxEntriesPerMessage`) and `MaxSizePerMsg` bytes of entry data (default 1 MiB, at most 8 MiB);
  an entry larger than the byte budget travels alone, so no entry is ever unsendable
  (`replication.Log.SliceBounded` copies nothing past the budget). A message therefore always
  fits what its receiver accepts, however far behind the follower is.
- **Streaming a backlog.** When the budget cut a peer's last batch, an acknowledgement that
  advances it sends the next batch at once, so a lagging follower catches up at the speed of round
  trips rather than one batch per heartbeat. A lost batch is resent by the next heartbeat, from
  `nextIndex`, as before. A snapshot offer ends the stream (its acknowledgement resumes
  replication once, `TestASnapshotInstallSendsTheNextBatchOnce`), and a leader that the
  acknowledgement steps down — the commit of its own removal — sends nothing more
  (`TestARemovedLeaderSendsNothingOnceItStepsDown`).
- **Read rounds.** A read no longer broadcasts the tail: it joins a round still unsent, or starts
  one of entry-less heartbeats (`docs/LINEARIZABILITY.md` §5.1). Reads registered in one cycle —
  and the driver takes the reads waiting together, up to 256 per cycle — are confirmed together.
  A leader's configuration change re-checks the reads pending: a leader that becomes its own
  quorum (the change removing its last peer) confirms them, where before no reply would ever come
  to and they filled `MaxPendingReads` (`TestReadsPendingWhenTheLeaderBecomesItsOwnQuorumAreConfirmed`).

Safety is unchanged: what a follower accepts and how it answers are untouched; the leader sends
less per message and more often. Evidence: `TestLaggingFollowerCatchesUpInBudgetedBatches`,
`TestEntryBudgetBindsABacklogOfSmallEntries`, `TestAnEntryLargerThanTheByteBudgetIsSentAlone`,
`TestReadsInOneCycleShareOneRound`, `TestAReadNeverJoinsARoundAlreadySent` (core);
`TestFollowerBehindByMoreThanAFrameCatchesUp` (three real drivers: a follower 18 MB behind, snapshots
off, catches up — before, never), `TestConcurrentReadsShareRounds` (512 concurrent reads cost 8–28
messages); `TestBudgetedReplicationUnderFaults` (every simulator profile over three seeds with
budgets of 2 entries and 32 bytes: every invariant and linearizability hold, with tens of
thousands of batches cut, under every fault the simulator injects); mutants 243–251, 267–269.

**Still open.** `nextIndex` still moves only on an acknowledgement — there is no optimistic
pipelining — so while a batch is unacknowledged, every heartbeat and proposal resends it. That
redundancy is now bounded by the budgets (at most one batch per peer per broadcast) but not removed;
removing it means tracking batches in flight per peer (etcd's probe/replicate states), a larger
change to the core's loss recovery left for a later phase. Snapshots and bulk appends still share
one connection with every group's heartbeats.

