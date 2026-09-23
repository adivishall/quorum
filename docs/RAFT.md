# RAFT — the consensus core (Phase 9)

Status: **implemented, Phase 9** (`internal/raft`, `internal/raftlog`, `internal/raftnode`;
ADR-016). This document specifies what was built and, as precisely, what it does **not** yet
prove. It implements §5.1–5.4 of Ongaro & Ousterhout, "In Search of an Understandable Consensus
Algorithm", against the repository-specific contract in `docs/DESIGN.md` §8.

> **Scope boundary, stated once.** Phase 9 proves Raft's **safety properties in deterministic
> simulation** and makes Raft state **durable across process kill**. It does **not** add
> end-to-end linearizability, the full network fault matrix (Phase 10), a client/HTTP API or
> request forwarding (Phases 13/15), linearizable-read serving / ReadIndex (later), snapshots
> (Phase 14), or dynamic membership (never, in v1 — ADR-005). Raft working is not the finished
> Quorum consistency model.

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
(Phase 8 enforces it), and an apply failure does not advance `appliedIndex`. Phase 9 drives a
minimal deterministic state machine to verify these semantics; it adds no dedup and claims no
exactly-once client application (`docs/CONSISTENCY.md` C4).

## 10. Persistence and ordering (`internal/raftlog`)

The durable log is an append-only record stream (§2 framing) of two record kinds:

- **`Entry`** — `index, term, data` (bounded varints + length-prefixed bytes).
- **`HardState`** — `currentTerm, votedFor, commitIndex` (commitIndex an optimization).

A conflicting-suffix replacement is **appended**, not rewritten: recovery replays records in file
order and applies each `Entry` at index `i` as "set `i`, drop anything above `i`" (the Phase 8
`TruncateAndAppend` semantics per record), reconstructing the final log from an append-only file;
the last `HardState` wins. Every append that a reply depends on is `f.Sync()`'d **before** the
reply is sent — the `Ready` contract makes this structural: the driver persists all of a `Ready`'s
`HardState` and `Entries` before sending any of its `Messages`.

**Crash policy** (distinct from the WAL's; ADR-016): a torn final record truncates to the last good
offset (crash mid-append; the dependent reply was never sent). A checksum mismatch with bytes
following, a zero-filled header with data after it, an unknown kind, a malformed payload, or an
impossible index progression is **fatal** — the log refuses to open rather than silently skip a
record.

## 11. Recovery (`docs/DESIGN.md` §10, steps 4–5)

On restart the driver reads the durable log → `currentTerm`, `votedFor`, entries, and the persisted
`commitIndex` (clamped). It builds a `MemoryLog` from the entries, sets `commitIndex`, and
constructs the core in Follower state at the recovered term. Recovery verifies indexes are still
contiguous, terms non-decreasing, and `currentTerm` does not move backward; incoherent state is
refused, not repaired. Snapshot recovery is **not** Phase 9. Where the storage engine is eventually
involved, the documented `engine.appliedIndex ≤ raft.lastIndex` invariant is preserved.

## 12. Determinism and the simulated network

The core is fully deterministic: given the same initial state, seed, and event sequence it produces
the same state **and the same messages in the same order**. Message ordering never depends on map
iteration (peers are iterated in a sorted, fixed order). The test-only simulated network runs N
cores in one goroutine with an explicit message queue, and lets a test deliver, delay, drop,
reorder, and duplicate individual messages and inspect the full history — enough deterministic
control to prove the algorithm, and deliberately **not** the Phase 10 fault-injection framework.
No test result depends on wall-clock timing or on `time.Sleep`.

## 13. Multi-Raft and membership

One shard = one independent Raft group (ADR-001). The core operates on a single group; the driver
instantiates one per shard in a later phase. Membership is **static** (ADR-005): no join/leave,
promotion, or reconfiguration; quorum is `⌊n/2⌋+1` over the fixed group. A one-node group commits on
its own; 2- and 3-node groups are tested. Phase 9 implements no client routing or API — routing
stays declarative (ADR-012).

## 14. What Phase 9 proves, and what it does not

**Proven (deterministic simulation + process-kill crash tests), the INV-R series:** election safety
(R1), leader append-only (R2), log matching (R3), leader completeness (R4), state-machine safety
(R5), durable `currentTerm`/`votedFor` before dependent replies (R6), never applying beyond commit
(R7), `commitIndex` monotonic across restart (R8), leader commits only current-term entries (R9),
stale lower-term messages are inert (R10). See `docs/INVARIANTS.md` for the exact tests behind each.

**Not proven / not present in Phase 9:** behavior under the full network fault matrix
(drop/delay/dup/partition at scale — Phase 10/11); end-to-end linearizability (Phase 12); client
retry / dedup / exactly-once application (Phase 13); linearizable-read serving / ReadIndex; an HTTP
API or request forwarding (Phases 13/15); snapshots and log compaction (Phase 14); dynamic
membership (v1 never). Power-loss durability is not tested (SIGKILL only), as everywhere in this
project (`docs/FAILURE_MODEL.md`). **No end-to-end linearizability guarantee, no client/API
semantics, no snapshots, and no dynamic membership were added.** Raft functioning is a necessary
part of the Quorum consistency model, not the whole of it.
