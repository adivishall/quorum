# MEMBERSHIP — dynamic membership by joint consensus (Phase 15)

Status: **implemented and verified (Phase 15).** This document specifies how a Raft group's
membership changes at runtime, what is safe about it, and how each claim is verified. Every claim
names the test that verifies it; a claim without one is not made (§9, §10).

Phase 14 left every group with a fixed member set (ADR-005): a node could fail and rejoin under its
own id, but nothing could be added, removed or replaced. Phase 15 replaces ADR-005 (ADR-023) with a
replicated, safe transition protocol — Raft §6 **joint consensus** — plus a non-voting **learner**
stage that catches a new member up before it may vote.

---

## 1. Identities

| Identity | Type | Meaning |
|---|---|---|
| `NodeID` | `replication.NodeID` (= `routing.NodeID`) | a physical node: one `dkvd` process, one transport identity, one data directory |
| `GroupID` | `replication.GroupID` (`uint32`) | one Raft group: one log, one snapshot, one state machine, one membership, one leader |
| `ShardID` | `routing.ShardID` | one slice of the key space (`docs/ROUTING.md`) |

A node hosts many groups; a group has replicas on many nodes; a shard maps to exactly one group.
Phase 15 maps shard `s` to group `GroupID(s)` — the identity function, recorded in one place
(`multiraft.Assignment`); the types stay distinct so a later split or merge is a configuration
change, not a type pun (`docs/MULTI_RAFT.md` §2).

## 2. The configuration — replicated state

```
Configuration {
    Voters   []Member   // the voting members — C_new while a transition is in progress
    Outgoing []Member   // C_old: non-empty exactly while the configuration is JOINT
    Learners []Member   // replicated to, never counted, never campaign
}
Member { ID NodeID; Addr string }   // Addr: where the other members reach it
```

**The representation is canonical** — one meaning, one value, one encoding
(`replication.Configuration`, `EncodeConfiguration`):

- each list is sorted strictly ascending by id — no duplicate within a list;
- a *stable* configuration has no `Outgoing` list;
- a *joint* configuration lists `C_new` in `Voters` and `C_old` in `Outgoing`, both non-empty and
  different as sets. A member of both is **listed in both, deliberately**: its vote and its
  acknowledgement count toward each majority. It is one member, so it carries one address in both.
  An `Outgoing` equal to `Voters` would be a second representation of the stable configuration and
  is refused;
- a learner is in neither voter set; at most 64 distinct members.

The encoding is hand-written and canonical (a version, then each list as a count and `(id, addr)`
pairs, all canonical uvarints); decoding is strict and total, and re-encoding an accepted
configuration reproduces its bytes (`TestConfigurationRoundTripIsCanonical`,
`TestJointConfigurationIsCanonical`, `FuzzDecodeConfiguration`). The largest valid configuration —
64 members at the id and address bounds, all voters and all but one again outgoing — fits the bound
every container uses (`replication.MaxEncodedConfiguration`; `TestLargestConfigurationFits`).

**The configuration lives in the Raft log.** An entry has a type: `EntryNormal` (a state-machine
command, or the leader's no-op) or `EntryConfig` (an encoded configuration). A configuration entry
must have voters — no transition yields a voterless configuration, and a log holding one could never
elect a leader again — so the message codec, the durable log (a typed record, kind 4; a normal entry
keeps the Phase 9 record) and core construction all refuse one (`DecodeConfigurationEntry`). A
configuration entry reaches the state machine as an **empty command**, exactly like the no-op:
membership is the core's state, never the application's, and a key-value command can never alter it
(`TestConfigurationEntriesReachTheStateMachineEmpty`).

### Three configurations, kept distinct

| Name | What it is | Where it comes from |
|---|---|---|
| **represented by a snapshot** | the configuration in effect at the snapshot's index | carried in the snapshot file (format v2) |
| **known at a log index** — `ConfAt(i)` | the latest configuration entry at or below `i`; else the base configuration, **only with evidence that the base holds at `i`** | the log and the base |
| **current** — `Conf()` | the latest configuration entry in the log, **committed or not** (Raft §6); else the base | the log and the base |

The **base configuration** is the configuration at a known log index — the snapshot's, at the
snapshot's index; the group's genesis, at 0; nothing (empty) for a joiner that has learned none.
The base's index may lie **above the log's compaction boundary**: a recovered log keeps entries below
its snapshot (Phase 14's retain), and those entries may hold configuration changes the snapshot's
configuration has already absorbed. So:

- `ConfAt(i)` for `i` at or above the base's index, with no configuration entry after the base up to
  `i`, is the base;
- `ConfAt(i)` below the base's index is the base only if the log holds **no** configuration entry
  between `i` and the base's index — the log itself is then the evidence nothing changed;
  otherwise it is **`ErrConfUnknown`**;
- a joiner's empty base is never an answer: `ErrConfUnknown`;
- **unknown is an error, never a guess from the newest state.** A snapshot needs its configuration,
  so a node that does not know it (a joiner not yet reached by its addition) defers the snapshot.

Recovery refuses a base that contradicts the log's configuration entry at or below its index, or
that claims an index past the log (`ErrConfMismatch`); a voterless base, except a joiner's empty
genesis at index 0. The twelve cases of the configuration-history check are one test,
`TestConfAtAnswersOnlyWithEvidence`: index at, above and below the base's index; at the snapshot's
index; a snapshot above and at its configuration's entry; recovery with a suffix; a suffix holding a
change; a joint configuration spanning the snapshot boundary, by recovery and by install; a joiner;
voterless and invalid configurations; conflicting metadata. Because the current configuration is
derived from the log, it is **recomputed whenever the log loses entries** — a conflicting suffix
replaced, a snapshot installed — and reverts to the previous one
(`TestConfigurationRevertsWhenItsEntryIsTruncated`).

### The genesis and the group identity file

The genesis is the one piece of membership state that is not a log entry. At a node's first start
of a group it is recorded, durably and once, in the **group identity file** beside the log
(`<log>.group`: group id + genesis configuration, CRC-framed; temporary file, fsync, rename,
directory fsync — before the log exists). Every restart reads it rather than trusting flags: a
restart naming another genesis, another group, or the empty genesis of a joiner over a bootstrap
member's, is refused; so is a log or snapshot with no identity file (`ErrIdentity`;
`TestIdentityFileRules`, `TestIdentityFileCorruptionIsRefused`, `FuzzDecodeIdentity`). A node that
**joins** an existing group records the *empty* genesis: it knows nothing until the leader that
adds it replicates to it.

## 3. Quorum

```
stable:  S is a quorum iff |S ∩ Voters| > |Voters| / 2
joint:   S is a quorum iff it is a majority of Voters AND a majority of Outgoing
```

Every place the core needs a quorum — winning an election, advancing the commit index, confirming
a ReadIndex — asks one function (`quorumOf`) with the nodes that voted, acknowledged or echoed.
Learners are never in the numerator or the denominator; an empty voter set is never a quorum; a
leader counts itself only if it is a voter of the set in question. No `majority(len(peers))`
remains anywhere. `TestQuorumRules` pins the table — one, two, three and five voters; overlapping
and disjoint joint sets; adding a fourth and removing one of four; the leader removed; the new
member, an old member, both unavailable — and the protocol tests show both majorities are needed
in motion (`TestPromoteNeedsTheNewMajority`, `TestRemoveVoterNeedsTheOldMajority`). A stable
configuration is never held to a joint rule: its absent `Outgoing` list is not consulted.

## 4. The operations

| Operation | Precondition | Entries | Quorum change |
|---|---|---|---|
| `AddLearner(m)` | stable; `m` not a member | `Learners += m` | none |
| `RemoveLearner(m)` | stable; `m` a learner | `Learners −= m` | none |
| `Promote(m)` | stable; `m` a learner | **joint** `{Voters: old ∪ m, Outgoing: old}`, then **final** `{Voters: old ∪ m}` | two phases |
| `RemoveVoter(m)` | stable; `m` a voter; not the last | **joint** `{Voters: old − m, Outgoing: old}`, then **final** `{Voters: old − m}` | two phases |

Only a leader accepts an operation, and at most one change is under way per group: a request is
refused (`ErrConfChangeInProgress`) while an uncommitted configuration entry exists or the
configuration is joint (`TestOneConfigurationChangeAtATime`). The leader appends the joint entry;
when it commits — both majorities — the leader appends the final entry itself, **and tries to commit
it at once** (when the leader alone is the final configuration's quorum no acknowledgement would
ever come — §8, bug 3); when the final entry commits the change is settled
(`TestConfChangeTransitions`). A **replacement** of a failed node `A` by `D` is the documented
sequence: start `D` as a joiner → `AddLearner(D)` → `Promote(D)` → `RemoveVoter(A)`, each settled
before the next. There is no composite operation: each step is individually safe and individually
recoverable. No other protocol exists — in particular no "remove the old member, then add the new
one" without the joint overlap.

## 5. Every role, every case

- **Who may campaign.** A voter of its current configuration. One other case (§8, bug 2): a node
  whose current configuration is the *uncommitted final entry of its own removal*, and which was a
  voter of the joint configuration that entry finalizes, may campaign under that joint
  configuration as well — asking the voters of both, winning only with a quorum of both. A quorum of
  the joint configuration is a majority of the old voters and of the new, so it intersects every
  quorum another candidate of the term could win; election safety is untouched. A learner, a joiner
  and a removed node never campaign (`TestLearnerReplicatesButNeverCampaignsOrCounts`).
- **Who may grant a vote.** Any node, at most once per term, to an up-to-date candidate. A learner
  may already be a voter of a configuration it has not received (§8, bug 1) — whether the vote
  counts is the **candidate's** configuration's decision: a candidate counts only the voters it
  must win. One durable vote per term is what election safety rests on; every node keeps it.
- **A node outside the receiver's configuration.** Its responses are dropped. Its vote request is
  refused, **its term not adopted**, unless its log is at least as up to date as the receiver's —
  then it may be a voter of a configuration the receiver has not learned (the receiver lags, or is
  a joiner) and is heard. A removed node's log lacks the entry that removed it, so a removed node is
  refused and its inflated terms never reach the group
  (`TestNonMemberVoteRequestIsHeardOnlyWithAnUpToDateLog`, `TestRemovedNodeCannotDeposeOrLead`).
  What a *leader* sends — AppendEntries, snapshots — is processed from anyone: a member that fell
  behind must learn from a leader it does not yet know.
- **A new member starts empty.** Its first AppendEntries names a `prevLogIndex` it lacks; it
  rejects; the leader backs up — to the compaction boundary, where it offers its snapshot, which
  carries the configuration at its index. That snapshot may **predate** the member's addition; the
  member then holds a committed configuration without itself, which is not a removal (§8, bug 4) —
  the entries after the snapshot add it. It becomes eligible to vote exactly when a configuration
  naming it a voter is in its log — `Promote`'s joint entry at the earliest.
- **The leader is removed.** It appends the joint entry and keeps leading — replicating to the new
  set, not counting itself in `C_new`'s majority — until the final configuration commits; then it
  steps down and never campaigns again (`TestLeaderRemovedStepsDownOnceTheFinalEntryCommits`). If
  it loses its leadership between appending the final entry and its commit, the campaign rule above
  lets it finish (`TestRemovedLeaderThatLostItsLeadershipFinishesItsRemoval`).
- **A candidate that learns it may not campaign** (an entry arrives that excludes it) becomes a
  follower (`TestCandidateThatLearnsItIsRemovedStopsCampaigning`).
- **The leader crashes after appending a configuration entry, before it commits.** The entry lives
  in the logs that received it. A new leader is elected under the configuration *its* log holds; if
  the entry reached a quorum it survives and the new leader completes the transition (it appends the
  final entry on seeing a committed joint entry it leads under,
  `TestNewLeaderCompletesAnInterruptedTransition`); if not, it is overwritten and every node that
  held it reverts. The proposer's `ChangeMembership` then answers `ErrConfLost` — definitely did not
  happen (`TestChangeMembershipReportsALostChange`).
- **A removed node returns** — from a crash, or from a partition it sat out during its removal. If
  its log names it no voter, it never campaigns. If its log predates its removal, it campaigns on
  `C_old` with ever higher terms and cannot win: the nodes holding the later configuration refuse
  it, its terms not adopted (`TestSimRemovedNodeRestartsAndStaysOut`,
  `TestRealRemoveAPartitionedMember`). It is contained, not stopped: the operator stops it
  (§10).

## 6. Snapshots and configuration

A snapshot is the replicated state at an index, and the configuration is part of it: the snapshot
file (format v2, `docs/SNAPSHOTS.md` §3) carries the group id — the snapshot's identity; the same
members in another group are another group — and the configuration at its index. A follower that
installs one adopts that configuration as its base, at the snapshot's index; a restart from a
snapshot starts from it and replays the configuration entries after it; the configuration of a
snapshot taken during a joint configuration is the joint one
(`TestSimSnapshotDuringJointConfiguration`). No separate configuration metadata is persisted
anywhere: the configuration travels inside the atomically published snapshot file, so no crash can
recover a snapshot with the wrong configuration (`docs/SNAPSHOTS.md` §5). A snapshot never
resurrects an old configuration: a stale snapshot delivered to a removed node installs an old
configuration on that node alone, which the group refuses like any stale state
(`TestSimRemovedNodeReceivesAStaleSnapshot`).

## 7. What the node driver adds

`raftnode.Node.ChangeMembership(ctx, change)` submits one operation to the local node, which must
lead, and waits until the change is **complete**: its entry — for `Promote` and `RemoveVoter` the
joint one, and then the final one — committed, no change under way. Answers: success with the
configuration reached; `ErrNotLeader`, `ErrConfChangeInProgress`, `ErrInvalidConfChange` (nothing
appended); `ErrConfLost` (its entry was overwritten — Log Matching makes that final); `ctx.Err()` or
`ErrStopped` (unknown — read the configuration). A node that loses its leadership meanwhile keeps
waiting, and answers success if the next leader completes the change from the same entry.

The driver reports the configuration in `Status` (`Conf`, `ConfIndex`, `ConfPending`, `Voter`) and
events `raft_conf`; and `raft_removed` once a node **that has been a member** — of its genesis or of
a configuration it held — holds a committed configuration without itself. The multi-Raft host then
retires the group on that node, keeping its files (`docs/MULTI_RAFT.md` §3).

## 8. Bugs found and fixed

1. **The vote deadlock** (membership chaos profile, seed 9 of 200). A single voter promoted a
   learner — the joint entry needs the learner to commit — and stepped down before the entry reached
   it. It could be elected only with the learner's vote, and a learner refused every vote: no leader,
   ever. The same held for a joiner that had received nothing (the candidate is not a member of its
   empty configuration, and its request was dropped). Fix: any node grants its one vote per term; a
   stranger with an up-to-date log is heard. Regression: `TestPromotedLearnerThatMissedItsPromotionStillElects`,
   `TestJoinerWithNoConfigurationVotesForItsPromotion`; mutants 138, 139.
2. **The removal deadlock** (multi-group simulator, seed 83 of 100). Two voters, one removing
   itself: joint committed, final appended, leadership lost before the final entry reached the
   other voter. The remover's configuration excluded it, so it did not campaign; the other voter,
   still joint, needed the remover's vote and was refused for its shorter log. Fix: the campaign
   rule of §5. Regression: `TestRemovedLeaderThatLostItsLeadershipFinishesItsRemoval`; mutant 140.
3. **The final entry that never committed** (bounded membership model: remove-leader,
   crash-follower, remove-follower). When the final configuration's only voter is the leader, no
   acknowledgement retries the commit; the change stayed under way until an unrelated proposal.
   Fix: try to commit on appending the final entry. Regression:
   `TestFinalEntryCommitsAtOnceWhenTheLeaderAloneIsItsQuorum`; mutant 141.
4. **The joiner retired before it was added** (real processes). A joiner installed a snapshot that
   predated its addition — a committed configuration without itself — was reported removed, and its
   host retired the group before the entries that added it arrived. Fix: only a former member is
   removed. Regression: `TestJoinerInstallingASnapshotThatPredatesItIsNotRemoved`; mutants 150, 158.

## 9. Invariants and evidence

The `MB` series (`docs/INVARIANTS.md`; `M` is the manifest's). Each is checked by code independent
of the rule it checks, at the instant it must hold, and has a mutant its tests kill.

| ID | Statement | Checked by | Mutants |
|---|---|---|---|
| INV-MB1 | Every committed configuration follows the previous committed one by exactly one transition of §4: a learner added or removed with the voters unchanged; a joint configuration whose outgoing set is the previous voters and whose voters differ by one member (a promoted learner, or a removed voter); a final configuration equal to the joint one's voters and learners. | the simulator's committed record (`commitConf`, an independent statement of the rules), every event of every run | 129, 130 |
| INV-MB2 | A node's configuration is exactly the one its own log and snapshot give — the core holds no membership of its own — and a configuration is known at an index only with evidence. | the simulator after every event (`derivedConf`); `TestConfAtAnswersOnlyWithEvidence` | 131, 134, 135, 136 |
| INV-MB3 | A leader advances its commit index only when the entry is durable on a majority of its configuration's voters and, when joint, of its outgoing voters; a candidate wins only with the quorum of every configuration its campaign must win. | the simulator at every commit advance, against the nodes' **durable** logs; `TestQuorumRules` | 126, 127 |
| INV-MB4 | A leader is a voter of its configuration or is leading its own removal; once a configuration excluding node X is committed, X never becomes leader of a later term. | the simulator after every event | 137, 142, 157 |
| INV-MB5 | Only a node allowed by the campaign rule campaigns, and only the voters whose votes count are asked; learners, joiners and removed nodes never lead, and a leader replicates only to its configuration's members. (Any node may grant a vote.) | the simulator at every send and every election; `TestSimRemoveAFollower` | 128, 140, 143 |
| INV-MB6 | Every frame a node host receives is delivered to the group its envelope names, or dropped — never to another group. | `TestFramesReachExactlyTheirGroup`, `TestNodeDropsFramesOfOtherGroups` | 152, 153, 154 |
| INV-MB7 | A node's crash or restart changes no group's membership: every group recovers its configuration from its own files alone, and the recovered configuration is the one the node held. | the simulator's derivation after every boot; the membership crash matrix; `TestMembershipSurvivesRestarts`; `TestRealNewMemberCatchesUpBySnapshotAndTheClusterRestarts` | 132, 146–148 |
| INV-MB8 | Snapshot + suffix reproduces the configuration exactly, and a snapshot records the configuration at its index — joint included. | the simulator after every install and boot; `TestSimSnapshotDuringJointConfiguration` | 131, 133 |
| INV-MB9 | Groups on one node share no consensus state: a group's trace is a function of its own inputs alone. | `TestMultiGroupIsolation` (each group's trace reproduced by replaying its own events on a lone cluster); `TestGroupsSnapshotAndCompactIndependently`; `TestRealOneGroupBrokenTheOtherContinues` | 153, 155 |
| INV-MB10 | Client-visible histories stay linearizable through membership changes, per group, and a request's identity survives them. | the kv-membership profiles (200 seeds each); `TestKVSimRetryIsADuplicateAcrossMembershipSnapshotAndFullRestart`; `TestRealReplaceACrashedNode`, `TestRealMembershipChangesUnderASessionWorkload` | 149, 151, 156 |

### Evidence

- **Pure core** (`internal/raft`): the quorum table; every transition and refusal; learner,
  promotion and removal in motion; leader removal and step-down; a candidate removed while
  campaigning; truncation reverting a configuration; snapshot install; compaction; one change at a
  time; a new leader completing an interrupted transition; recovery; the twelve configuration-history
  cases; the three liveness regressions.
- **Durable log and codecs**: typed records survive reopen and compaction; undecodable, voterless
  and unknown-type entries are corruption; the configuration, identity and envelope codecs refuse
  every truncation and bit flip; fuzz targets for each.
- **Driver** (`internal/raftnode`, real TCP): add, promote, remove the leader; the configuration
  survives restarts and snapshots; a joiner catches up by a snapshot that predates it and is not
  reported removed; a lost change answers `ErrConfLost`; frames of other groups are dropped.
- **Simulator**: the INV-MB checks on every event; four membership chaos profiles and two client
  profiles, 200 seeds each; scripted removal of a follower, the leader, a partitioned node; a
  removed node restarted; a failed node replaced and the stale one back; a new member via snapshot; a
  snapshot during a joint configuration; a stale snapshot to a removed node; a retry that stays a
  duplicate across membership changes, snapshots, compaction and a full power-loss restart.
- **Bounded model**: every sequence of four steps over eleven — add, promote, remove the leader,
  remove a follower, crash the leader (power), crash a follower, restart, snapshot, partition the
  leader, heal, propose — 14,641 sequences, every invariant after every event, convergence at the
  end (`TestMembershipBoundedModel -raftsim.model-depth=4`; depth 3 in every run).
- **Crash matrix**: three membership scenarios (add, promote and remove the leader, with snapshots;
  replace a partitioned member; a torn configuration Save and an overwritten change), a crash at
  every driver point and I/O boundary every node reaches, in every mode — 6,660 crashes across every
  transition state (stable, change proposed, joint proposed, joint committed with the final pending,
  a joiner with no configuration), 0 failures; each row records group, transition, configuration
  before and after, and the crashed node's recovered configuration
  (`TestMembershipCrashMatrix`, `-raftsim.membership-matrix.out`).
- **Real processes** (`tests/integration/membership_test.go`, three consecutive passes under
  `-race`): a fourth node added and promoted; a follower removed and restarted; the leader removed;
  a crashed node replaced with the stale one back under a session workload; a promotion held in its
  joint configuration when the leader is SIGKILLed; a new member caught up by snapshot and the whole
  cluster SIGKILLed and restarted; a partitioned member removed; a full membership cycle under a
  session workload.

## 10. Limitations

- **Membership changes are operator-driven.** Nothing decides on its own to add, remove or replace
  a member; there is no placement policy, no rebalancing, no automatic replacement of failed nodes.
- **One change at a time per group, one member per change.** Joint consensus would admit arbitrary
  sets; the operations change one member, which is what the transition checks (INV-MB1) and the
  tests cover.
- **A removed node is contained, not stopped.** A node that does not learn its removal — a removed
  follower, whose leader stops replicating to it once the final entry is appended — keeps
  campaigning with ever higher terms, refused by the group; the operator stops it. A removed
  *leader* learns it (it commits its own removal) and its host retires the group. A node that
  learns its removal keeps its files; re-adding it later means a new join with fresh files.
- **No PreVote, no check-quorum.** A node that still counts a removed node as a member (it lags on
  configurations) can be made to adopt that node's inflated term during a transition, costing an
  election; an isolated leader keeps believing it leads until it hears a higher term.
- **The genesis is fixed per group per node.** It is recorded once; a node cannot rejoin a group
  under the same data directory with a different genesis.
- **Addresses are recorded, not verified.** A member's address is what the operator (or the
  genesis flags) said; a wrong address makes the member unreachable, not the configuration invalid.
