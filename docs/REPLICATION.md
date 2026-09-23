# REPLICATION — the local replicated-log model (Phase 8)

Status: **implemented, Phase 8** (`internal/replication`, ADR-015). This document specifies the
model precisely enough to falsify. It is the layer that Phase 9's Raft will *drive*; it is not
Raft, and it makes no distributed claim.

> **The boundary, stated once and repeated because it is the whole point of this phase:**
> Phase 8 models a replicated log and its local semantics. It does **not** implement the
> distributed protocol that keeps replicas equal. A `MemoryLog` on one node knows nothing about
> a `MemoryLog` on another; nothing here replicates a byte across a network, decides when an
> entry may be committed, elects anything, or tolerates a fault. Those are Phase 9 and later.

---

## 1. Why this phase exists

Raft's correctness arguments are stated over a **local log** with a small, exact contract:
entries are indexed contiguously; a follower may have to throw away a conflicting suffix and
copy the leader's; a `commitIndex` advances and a state machine applies committed entries in
order. Phase 9 will implement the *distributed protocol* that decides those things across
nodes. Before that, the local primitive the protocol stands on should exist, be deterministic,
and have every edge case executable — so Phase 9 can be written against a log it can trust
rather than debugging consensus and log mechanics at the same time.

Phase 8 builds exactly that primitive and nothing above it. It reuses Phase 6's routing
metadata for *who* a shard's replicas are, and defines the per-replica log they will each keep.

## 2. Replica-group model

Phase 6 (`internal/routing`, ADR-012) already answers `key → shard` and `shard → ordered
replica group` as **declarative metadata**. Phase 8 does not recompute any of that. It defines
a replication-layer value, `ReplicaGroup`, that *consumes* the routing metadata and gives the
replication layer a validated, immutable handle on one shard's replica set.

A `ReplicaGroup` carries:

- the `ShardID` it is the group for;
- the **ordered** replica node IDs, head first;
- the **replication factor** (RF), which equals the replica count;
- the **primary/head** replica — `Replicas()[0]`, matching the routing layer's ordering
  (`Router.ReplicaGroup`'s head is the shard's primary).

It is **immutable** after construction: fields are unexported and every accessor that returns a
slice returns a copy, so a caller can neither reorder nor mutate a group in place. Two groups
are `Equal` iff they have the same shard and the same replicas in the same order — deterministic
identity with no map iteration involved.

**Membership stays static** (ADR-005). A `ReplicaGroup` has no join, leave, promote, demote,
replace, or rebalance. A "membership change" remains what Phase 6 made it: a *new* configuration
built from a new routing `Config`, compared against the old one — never a mutation of a live
group.

**Validation rejects, it never repairs.** `NewReplicaGroup` refuses, rather than silently
fixing, every one of:

| Invalid input | Error |
|---|---|
| empty replica list | `ErrEmptyGroup` |
| an empty node ID | `ErrEmptyReplicaID` |
| a duplicate node ID | `ErrDuplicateReplica` |
| RF < 1 | `ErrInvalidReplicationFactor` |
| RF ≠ replica count | `ErrInvalidReplicationFactor` |

It does **not** deduplicate or reorder the caller's replicas: a duplicate is a caller bug that
is surfaced, and the order is the routing contract's order (head = primary), preserved and
tested. `ReplicaGroupsFromRouter` builds one validated `ReplicaGroup` per shard directly from a
`*routing.Router`, so the routing metadata flows into the replication model without either layer
re-deriving the other.

## 3. The log model

### 3.1 Entry

A log entry is opaque to this layer:

```
Entry{ Index uint64, Term uint64, Data []byte }
```

`Data` is arbitrary bytes — the replication layer never interprets it. In the finished system it
will be an encoded state-machine command, but Phase 8 treats it as opaque, exactly as the
transport treats a frame payload as opaque (ADR-013).

### 3.2 Index numbering

Indexes are **1-based**, contiguous, and start at 1. Index **0 is the empty sentinel**: it is
the index "before the first entry", its term is 0, and `LastIndex()` returns 0 for an empty log.
This is the Raft convention (`docs/DESIGN.md` §8.4's `prevLogIndex/prevLogTerm`, where 0 means
"before the log begins"), chosen here so Phase 9 inherits it rather than translating.

A non-empty log occupies indexes `[1, LastIndex()]` with no gaps and no duplicates.
`FirstIndex()` is **1** in Phase 8 and documented as such: nothing truncates the front of the
log yet. Log compaction / snapshots (which would raise `FirstIndex`) are Phase 14 and are out of
scope here.

### 3.3 Term semantics

`Term` is a logical, non-decreasing label along the log: for any two live entries `i < j`,
`term(i) ≤ term(j)`. Phase 8 enforces this (an append or suffix replacement whose terms would
decrease relative to the retained prefix, or within the batch, is rejected with
`ErrTermRegression`). It is a generic property of a Raft-style log — terms are a logical clock
that never runs backwards — and enforcing it locally catches a class of caller bug cheaply.
Phase 8 does **not** implement Raft's rule for *which* term a new leader stamps on entries, or
the conflicting-term backup optimization; it only requires that whatever terms a caller supplies
are monotonic.

### 3.4 Ownership and copy semantics

The log follows the project's copy discipline (INV-A1): **no entry byte is ever aliased across
the boundary.**

- On `Append`/`TruncateAndAppend`, the log copies each entry's `Data` into log-owned memory.
  Mutating the caller's slice afterwards cannot change stored bytes.
- `At`, `Slice`, and `Unapplied` return entries whose `Data` is a fresh copy. Mutating a
  returned entry cannot change stored bytes or another reader's result.

## 4. The interface Raft will drive

The local log is a small, explicit interface — deliberately *not* a "Raft interface". It exposes
the local primitive Phase 9 needs and nothing that belongs to consensus.

```go
type Log interface {
    FirstIndex() uint64                       // 1 in Phase 8 (no front truncation)
    LastIndex() uint64                        // 0 when empty

    Term(index uint64) (uint64, error)        // Term(0) == 0; out-of-range errors
    At(index uint64) (Entry, error)           // one entry (copy); out-of-range errors
    Slice(lo, hi uint64) ([]Entry, error)     // half-open [lo,hi) copies; invalid ranges error

    Append(entries ...Entry) error            // strict append at the end
    TruncateAndAppend(entries ...Entry) error // replace a conflicting suffix, then append

    CommitIndex() uint64
    Commit(index uint64) error                // advance commit (monotonic, ≤ last index)

    AppliedIndex() uint64
    Unapplied() ([]Entry, error)              // committed-but-not-applied entries (copies)
    Apply(through uint64) error               // advance applied (monotonic, ≤ commit)
}
```

Ranges are **half-open `[lo, hi)`**, matching Go slice conventions: `Slice(lo, lo)` is empty, and
a valid range satisfies `1 ≤ lo ≤ hi ≤ LastIndex()+1`. Anything outside that is `ErrOutOfRange`
— an invalid range is rejected, never clamped or guessed.

What is deliberately **absent**, because it is Phase 9's: `currentTerm`, `votedFor`, election
timers, leader/follower/candidate state, quorum or majority calculation, heartbeats, and any
decision about *whether* an index may be committed. `Commit` records that an index *is* committed;
it does not decide that a distributed group is *allowed* to commit it.

## 5. Local implementation — `MemoryLog`

`MemoryLog` is the Phase 8 reference implementation of `Log`. It is a small deterministic
in-memory structure: a slice of entries (index `i` at slot `i-1`) plus two watermarks,
`commitIndex` and `appliedIndex`. It exists to exercise the interface, make every edge case
executable, and give Phase 9 something deterministic to drive before a persistent log is built.

It is **not** the final log. It has no persistence, no fsync, no segments — the durable
replicated log (a record stream in the §2 framing, sharing the WAL's machinery) is a later
phase. And it is **not** a distributed replication engine; the name avoids implying Raft exists.

Following ADR-002's discipline, `MemoryLog` holds **no locks and starts no goroutines**: it is a
pure object driven by a single goroutine, exactly as `raft.Raft` will be. Determinism is
structural — the state is a slice and two integers, no map affects any observable order, and no
clock or randomness is consulted. Equivalent operation sequences produce equivalent state.

## 6. Conflict / suffix semantics

`TruncateAndAppend(entries...)` is the primitive Phase 9 will call when Raft's log-matching has
already decided that a suffix must be replaced. It is generic: it does **not** implement the
"conflicting leader term" comparison (that is Phase 9's decision about *when* to replace); it
performs the replacement deterministically once told to.

Let `f = entries[0].Index`. The operation:

1. requires `entries` to be non-empty and internally contiguous (`entries[k].Index == f+k`) with
   non-decreasing terms;
2. requires `f ≥ 1` and `f ≤ LastIndex()+1` — a replacement may start at any existing index or
   exactly at the end, but **never past the end**, so it can never open a gap
   (`ErrNonContiguous` otherwise);
3. requires `f > CommitIndex()` — **committed entries are never replaced** (`ErrTruncateCommitted`
   otherwise);
4. requires `term(f-1) ≤ entries[0].Term` (`ErrTermRegression` otherwise);
5. retains the prefix `[1, f-1]`, discards the existing suffix `[f, LastIndex()]`, and appends
   `entries` — leaving indexes contiguous.

When `f == LastIndex()+1` there is no existing suffix and the call is a pure append, so `Append`
is the special case of this primitive that forbids touching any existing entry;
`Append(entries...)` is provided separately because "extend the log" and "rewrite a suffix" are
different intents and keeping them distinct makes a caller's mistake (an unintended truncation)
an error rather than a silent overwrite.

**Committed suffixes may not be replaced.** This is the rule a correct Raft depends on: an entry
is committed only once it is durable on a quorum and can never be overwritten (Leader
Completeness, INV-R4). Phase 8 forbids it locally so a Phase 9 bug that tries to rewrite history
below `commitIndex` fails loudly here instead of corrupting state.

## 7. Commit / apply model

Phase 8 keeps two watermarks with explicit monotonicity rules. It defines the *bookkeeping*, not
the *consensus* that drives it.

- **Initial state.** `CommitIndex() == 0` and `AppliedIndex() == 0` for a fresh log.
- **`Commit(i)`** requires `CommitIndex() ≤ i ≤ LastIndex()`. A backward commit is
  `ErrCommitRegression`; committing past the last local index is `ErrCommitBeyondLog`.
  `Commit(CommitIndex())` is an idempotent no-op, not a regression.
- **`Apply(through)`** requires `AppliedIndex() ≤ through ≤ CommitIndex()`. A backward apply is
  `ErrAppliedRegression`; applying an uncommitted index is `ErrApplyBeyondCommit`.
- **`Unapplied()`** returns copies of the entries in `(AppliedIndex(), CommitIndex()]`,
  deterministically and in index order.
- **At most once.** Application is a watermark, not a queue: after `Apply(i)`, those entries are
  no longer returned by `Unapplied()` and `Apply(i)` again applies nothing. There is no interface
  path that applies an index a second time.

`Commit` and `Apply` never move an entry, and `Append`/`TruncateAndAppend` never move a
watermark. Because a suffix replacement must start above `commitIndex`, `commitIndex ≤ LastIndex`
survives truncation, and therefore `appliedIndex ≤ commitIndex ≤ LastIndex` holds at all times.

**Phase 8 can record that an index is committed. Phase 8 does not decide whether a distributed
group is allowed to commit it.** That sentence is the entire boundary between this phase and
Raft.

## 8. Storage / state-machine boundary

The intended pipeline, for the record, is:

```
replicated log  →  committed entries  →  state-machine application  →  storage engine
```

Phase 8 defines the **seam** without building the driver. `StateMachine` is a minimal, opaque
interface:

```go
type StateMachine interface {
    Apply(index uint64, command []byte) error
}
```

Phase 8 does **not** wire this to the LSM engine, does not build the loop that pumps a log's
`Unapplied()` entries into a state machine and advances `AppliedIndex()`, and adds no client
request semantics (no request IDs, dedup, transactions, linearizable reads, or forwarding —
those are Phases 13+). A test drives a trivial in-test `StateMachine` from a `MemoryLog` to show
the seam composes; that is the extent of it.

The existing LSM engine (`internal/storage`) is untouched. ADR-006's two-logs-per-shard decision
stands: the replicated log defined here is a separate structure from the engine's WAL.

## 9. Determinism

Everything in `internal/replication` is deterministic. There is no wall-clock dependence, no
randomness, no goroutine scheduling that affects state, and no map iteration that reaches a
serialized or test-visible result. Equivalent inputs produce equivalent outputs, bit for bit,
which is what makes the reference-model and fuzz tests meaningful. Phase 8 introduces **no**
serializer for replication metadata — nothing in this phase needs to put a group or a log on the
wire or on disk (that is Phase 9's persistent log and Raft codec), so none is written.

## 10. Invariants

The replication-model invariants are the **INV-P** series (P for replication), a namespace
distinct from every series in `docs/INVARIANTS.md`. Each is bound to the exact tests that
establish it there.

| ID | Property |
|---|---|
| INV-P1 | A `ReplicaGroup` is valid or it does not exist: empty groups, empty/duplicate replica IDs, and an RF inconsistent with the replica count are refused, never repaired; order and identity are deterministic and the head is the primary. |
| INV-P2 | Log indexes are contiguous and 1-based with no gaps or duplicates, and terms are non-decreasing along the log. |
| INV-P3 | Suffix replacement is deterministic and hole-free: it retains a prefix, drops the existing suffix, appends the batch, cannot start past the end, and cannot replace a committed entry. |
| INV-P4 | Entries are copy-safe in both directions: stored bytes are never aliased to caller memory, and returned bytes never expose internal storage. |
| INV-P5 | `commitIndex` is monotonic — it never moves backward. |
| INV-P6 | `commitIndex` never exceeds the last local log index. |
| INV-P7 | `appliedIndex` is monotonic — it never moves backward. |
| INV-P8 | `appliedIndex` never exceeds `commitIndex`. |
| INV-P9 | No entry is applied twice through the interface. |

## 11. What Phase 8 explicitly does NOT provide

Stated plainly so no reader infers more than was built. Phase 8 provides a **local** replicated-log
model and a replica-group abstraction. It does **not** provide:

- consensus, or any agreement across nodes;
- leader election, voting, or terms-as-elections;
- `AppendEntries`/`RequestVote` semantics or any network replication;
- a decision about *when* an entry may be committed;
- commit quorums, failover, or fault tolerance;
- cross-node consistency, linearizability, or any distributed consistency guarantee;
- request forwarding, client serving, an HTTP API, or a dashboard;
- snapshots, log truncation from the front, or dynamic membership;
- persistence of the log (the Phase 8 log is in-memory).

**No distributed consistency guarantee is added by Phase 8**, anywhere in the repository. The
next phase, **Phase 9 — Raft**, is **not started**.
