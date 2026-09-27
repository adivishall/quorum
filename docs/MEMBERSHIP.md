# MEMBERSHIP — dynamic membership by joint consensus (Phase 15)

Status: **design (Phase 15, in progress).** This document specifies how a Raft group's membership
changes at runtime, what is safe about it, and how it is verified. Sections marked *evidence* are
filled in as the phase's tests land; a claim without a named test is not a claim.

Phase 14 left every group with a fixed member set (ADR-005): a node could fail and rejoin under
its own id, but nothing could be added, removed or replaced. Phase 15 replaces ADR-005 with a
replicated, safe transition protocol — Raft §6 joint consensus — plus a non-voting **learner**
stage for catching a new member up before it may vote.

---

## 1. Identities

| Identity | Type | Meaning |
|---|---|---|
| `NodeID` | `replication.NodeID` (= `routing.NodeID`) | a physical node: one `dkvd` process, one transport identity, one data directory |
| `GroupID` | `replication.GroupID` (`uint32`) | one Raft group: one log, one snapshot, one state machine, one membership, one leader |
| `ShardID` | `routing.ShardID` | one slice of the key space (`docs/ROUTING.md`) |

A node hosts many groups; a group has replicas on many nodes; a shard maps to exactly one
group. In Phase 15 the cluster configuration maps shard `s` to group `GroupID(s)` — the identity
function, recorded explicitly, **not** asserted to be permanent (a later phase may split or merge
groups; the types stay distinct so that change is a configuration change, not a type pun).

## 2. The configuration — replicated state

A group's membership is a `replication.Configuration`:

```
Configuration {
    Voters   []Member   // the current voting members (C_new while a transition is in progress)
    Outgoing []Member   // C_old, non-empty only while the group is in a JOINT configuration
    Learners []Member   // non-voting members: replicated to, never counted, never campaign
}
Member { ID NodeID; Addr string }   // Addr: the transport address, so every member can connect
```

Members are sorted by id; ids are unique across `Voters ∪ Learners`; `Outgoing` is non-empty
**iff** the configuration is joint; a learner is never a voter of either set. The encoding is a
hand-written canonical codec (`replication.EncodeConfiguration`): version, then each list as a
count followed by `(id, addr)` pairs, all canonical uvarints and length-prefixed bytes; decoding is
strict and total, and re-encoding an accepted configuration reproduces its bytes.

**The configuration lives in the Raft log.** A log entry has a type (`replication.EntryType`):
`EntryNormal` (a state-machine command) or `EntryConfig` (an encoded configuration). A node's
**current configuration is the latest `EntryConfig` in its log** — committed or not, exactly as
Raft §6 prescribes — and, when the log holds none after its compaction boundary, the configuration
recorded by its snapshot (or, for a log that was never compacted, the group's **genesis**: the
bootstrap configuration written once, to `<log>.meta`, when the group was created on that node).
Because the configuration is derived from the log, it is **recomputed whenever the log loses
entries** — a conflicting-suffix replacement or a snapshot install that removes a configuration
entry reverts the node to the previous one. Nothing outside the log is authoritative; the node
host caches configurations only to connect transports.

The genesis is the one piece of membership state that is not a log entry. Every bootstrap member
is created with the identical genesis (the same way every member of a group is created with the
same group id), it is immutable, and every later configuration is a replicated entry. A node that
**joins** an existing group is created with an *empty* genesis: no voters, no learners — it
cannot campaign or vote, and learns the configuration from the leader's first snapshot or entry.

## 3. Quorum

```
stable (Outgoing empty):  a set S is a quorum iff |S ∩ Voters| > |Voters| / 2
joint  (Outgoing set):    a set S is a quorum iff it is a majority of Voters AND a majority of Outgoing
```

Every place the core needs a quorum — winning an election, advancing the commit index,
confirming a ReadIndex — asks one function, `Configuration.Quorum(set)`, with the set of nodes
that voted, acknowledged or echoed. Learners are never in the set's denominator or numerator. A
leader counts itself only if it is a voter of the set in question (a leader being removed leads a
group that no longer includes it until `C_new` is committed, without counting itself). A
single-voter group's quorum is that voter, so it elects itself and commits alone, as before.

## 4. The operations

| Operation | Precondition | Entries appended | Quorum change |
|---|---|---|---|
| `AddLearner(m)` | stable; `m` not a member | one: `Learners += m` | none |
| `RemoveLearner(m)` | stable; `m` a learner | one: `Learners −= m` | none |
| `Promote(m)` | stable; `m` a learner | **joint** `{Voters: old ∪ m, Outgoing: old, Learners −= m}`; then, once that entry is committed, **final** `{Voters: old ∪ m}` | two phases |
| `RemoveVoter(m)` | stable; `m` a voter; at least one voter remains | **joint** `{Voters: old − m, Outgoing: old}`; then **final** `{Voters: old − m}` | two phases |

Only a leader accepts an operation, and at most one configuration change is in progress per
group: a request is refused (`ErrConfChangeInProgress`) while the log holds an uncommitted
configuration entry or the configuration is joint. The leader appends the joint entry; when it
commits — which needs both majorities — the leader appends the final entry on its own; when that
commits the change is settled. A **replacement** of a failed node `A` by a new node `D` is the
documented sequence `Join(D)` (start `D` as a joiner) → `AddLearner(D)` → `Promote(D)` →
`RemoveVoter(A)`, each step settled before the next; there is no composite operation, because each
step is individually safe and individually recoverable.

## 5. Every role, every case

- **A learner** never campaigns (its election timer never fires), never grants a vote, is
  replicated to like a voter (heartbeats, entries, snapshots), and is never counted.
- **A new member starts empty.** Its first message from the leader is an AppendEntries whose
  `prevLogIndex` it lacks; it rejects; the leader backs up to the log's compaction boundary and
  offers its snapshot (Phase 14); the snapshot carries the configuration at its index, so the
  joiner learns the group, then continues by entries. It becomes eligible to vote exactly when a
  configuration in which it is a voter is in its log — `Promote`'s joint entry at the earliest.
- **The leader is removed.** It appends the joint entry, keeps leading — replicating to the new
  set, not counting itself in `C_new`'s majority — until the **final** configuration is committed,
  then steps down (becomes a follower of no one). It will not campaign again: it is no longer a
  voter in its own log.
- **A candidate learns it is not a voter** (a configuration entry arrives that excludes it): it
  becomes a follower and stops campaigning.
- **A voter disappears before voting** (crash, partition): the election needs a quorum of the
  configuration the candidate holds; a missing voter simply does not answer.
- **The leader crashes after appending a configuration entry, before it commits.** The entry is in
  the logs of whoever received it. A new leader is elected under the configuration *its* log holds
  (§6: a server uses the latest configuration in its log); if the entry is on a quorum it survives
  and the new leader completes the transition (it appends the final entry when it sees a committed
  joint entry it leads under); if not, it is overwritten and every node that held it reverts to
  the previous configuration by recomputation.
- **Leadership changes between the joint and the final entry.** The new leader — elected by both
  majorities — finds a committed joint configuration in its log and appends the final entry itself.
- **A removed node returns** (from a crash, or from a partition it sat out during its removal):
  - if its log holds a configuration in which it is not a voter, it never campaigns and never
    grants a vote;
  - if its log predates its removal it still believes it is a voter of `C_old` and may campaign in
    a higher term; it cannot win — every majority of `C_old` intersects the majority that holds
    the later configuration entries, and those nodes deny it for having a stale log (§5.4.1);
  - **a `RequestVote` from a node that is not in the receiver's configuration is answered with a
    rejection and its term is not adopted**, and responses from non-members are dropped; so a
    removed node's inflated terms never reach the group and cannot depose its leader. Messages from
    a *leader* (AppendEntries, snapshots) are always processed — a member that has fallen behind
    on configurations must still be able to learn them from a leader it does not yet know.
- **Stale messages from removed members** are inert (INV-R10 and the rule above).

## 6. Snapshots and configuration

A snapshot is a snapshot of *the replicated state at an index*, and the configuration is part of
that state: the snapshot file (format version 2, `docs/SNAPSHOTS.md` §3) carries the group id and
the configuration in effect at its index. A follower that installs one adopts that configuration;
a restart from a snapshot starts from it and replays the configuration entries after it; a
compaction's boundary configuration is the latest configuration entry at or below the compaction
index. The group id is the snapshot's identity — a snapshot of another group is refused whatever
its members are — replacing Phase 14's member-list identity, which a changing membership would
break.

## 7. What the node driver adds

`raftnode.Node.ChangeMembership(ctx, change)` proposes an operation at the leader and returns once
the change is **settled**: the final configuration is committed and applied on this node. If the
node stops leading first the call returns `ErrNotLeader` — the change may still complete under
the next leader (the entries are in the log) and the caller asks the group (`Describe`) rather
than assuming. When a committed configuration no longer includes this node, the driver reports it
(`Status.Voter`, an event) and the host stops serving the group on that node; its files are kept.

## 8. Invariants

Candidates named by the phase plan, each adopted only with a check at the instant it must hold and
a mutant its tests kill (`docs/INVARIANTS.md`, the `M` series):

| ID | Statement |
|---|---|
| INV-M1 | Every committed configuration is reached from the previous committed one by exactly one of the transitions of §4, in order: a joint entry follows a stable one and differs from it by one member; a final entry follows the joint one and equals its `Voters`. |
| INV-M2 | At every committed index, every node that holds the entry interprets the same configuration; a stable configuration has exactly one authoritative voter set. |
| INV-M3 | A leader in a joint configuration advances its commit index only when the entry is on a majority of `Voters` **and** a majority of `Outgoing`; a candidate wins only with both. |
| INV-M4 | Once a configuration excluding node X is committed, X never leads a term at or above that entry's term, and no later commit or read confirmation counts X. |
| INV-M5 | A learner, or a node in no configuration, never becomes a candidate or leader and never grants a vote. |
| INV-M6 | Every Raft message a node host receives is delivered to the group named in its envelope, or dropped — never to another group (`docs/MULTI_RAFT.md`). |
| INV-M7 | A node's crash or restart changes no group's membership; every group recovers its configuration from its own files alone. |
| INV-M8 | Snapshot + suffix reproduces the configuration exactly: the configuration after a restart or an install equals the one full replay yields. |
| INV-M9 | Groups hosted on one node share no consensus state: a group's term, log, commit, applied and configuration are functions of its own log and snapshot. |
| INV-M10 | Client-visible histories stay linearizable through membership changes (INV-X1..X3 hold, per group). |

## 9. Evidence

*Filled in as the phase's tests land.*

## 10. Limitations

*Filled in as the phase's tests land.*
