# MULTI_RAFT — hosting many Raft groups on one node (Phase 15)

Status: **implemented and verified (Phase 15).** `docs/MEMBERSHIP.md` is how one group's membership
changes; this document is how a node hosts several groups, how a frame finds its group, how a client
finds a key's group, and what stays separate. Every claim names the test that verifies it.

---

## 1. Topology

```
                 Node (dkvd process, one transport identity, one data directory)
                   │
        ┌──────────┼──────────┐
        │          │          │
      Group 0    Group 1    Group N          one GroupID each
        │          │          │
      raftnode   raftnode   raftnode         one driver and one pure core each
        │          │          │
     log/snap   log/snap   log/snap          groups/<gid>/raft.log, .group, .snap
        │          │          │
     kv.Store   kv.Store   kv.Store          one state machine and one session table each
```

Each group is an independent consensus domain: its own membership, log, snapshot, commit index,
applied index, term, leader and session table. Nothing mutable is shared between groups; what they
share is the process, the transport connections, and the data directory. **There is no cluster-wide
Raft** and no cross-group atomicity: every client operation names one key, one key lives in one
shard, one shard is served by one group. A node crash is a *correlated* failure of every group it
hosts; each recovers on its own.

## 2. Identities and assignment

`ShardID → GroupID` is the identity (`multiraft.Assignment`): shard `s` is group `GroupID(s)`. Which
nodes form each group's **genesis** comes from the Phase 6 routing and nothing else: the cluster's
routing configuration (`-shards`, `-rf`, `-nodes`, identical on every process) gives each shard's
replica group, and that is its group's genesis voter set. A key's group is its shard on the
consistent-hash ring — never a modulo — and no second routing algorithm exists
(`TestAssignmentIsTheRouting`: 2, 4 and 8 groups over five nodes; every key's group is its routing
shard). After genesis a group's membership is its own replicated configuration: the routing
configuration describes where groups **started**; a membership change does not rewrite it.

## 3. The node host (`internal/multiraft`)

The host owns only what is per process:

- **the registry and lifecycle** — `Start` recovers every group found under `DataDir/groups/` in
  ascending id order; a group that fails to recover is reported (`Failed`, `event=group_failed`) and
  left down, and the others start (`TestAGroupThatFailsToRecoverDoesNotBlockTheOthers`). `Create`
  starts a group for the first time as a genesis member or a joiner; `Stop` stops one, keeping its
  files; `Open` starts a stopped one again from its own identity file; a group whose node reports its
  removal (`docs/MEMBERSHIP.md` §7) is **retired** — stopped, files kept (`event=group_retired`,
  logged and counted once, when the stop is certain: `TestRemovedLeaderRetiresItsGroup`,
  `TestARetirementIsLoggedOnce`). Nothing is ever deleted automatically. A group directory is
  created durably: each new directory's parent is fsynced, so a group can never vanish from a
  node that held its state. **Starts and stops of one group never overlap** (audit H3): a start
  reserves the group before it touches its files and holds it until the group is registered and
  announced (`OnGroup`); a stop moves it out of the registry into the reservation and holds it until
  its node is closed. Meanwhile any other `Create`, `Open` or `Stop` of that group — an admin
  operation, or a retirement — is refused with `ErrGroupBusy` (the admin answers it as an error, to
  retry), so a group never has two drivers on its log, and the front's attach and detach of a group
  strictly alternate. `Close` waits for transitions in flight. A first start that fails before
  recording anything removes the empty group directory it made
  (`TestAStartingGroupCannotBeStartedOrStoppedAgain`, `TestAStoppingGroupCannotBeStartedUntilItsNodeIsClosed`,
  `TestConcurrentLifecycleOperations` under `-race`, `TestAFailedFirstStartLeavesNoDirectory`;
  mutants 189–191);
- **the demultiplexer** (§4);
- **the transport's peer set** — the members with addresses of every hosted group's current
  configuration, plus the static peers (`-peers`), kept in step as configurations change
  (`transport.PeerSet`: `AddPeer`, `RemovePeer`; `TestJoinerAddedThroughTheHost`,
  `TestAddPeerConnectsAndRemovePeerDisconnects`). The host reads configurations from the groups —
  replicated state — and never holds one authoritatively.

Each group runs the driver (`raftnode.Node`) over its own files, its own `kv.Store` and its own
`kv.Server`; the host hands it a bounded inbox instead of the transport.

## 4. Message routing — the group envelope

The transport carries bytes tagged with a frame kind, attributed to the connection's handshake
identity (INV-T4). Every payload a group's node sends — its Raft messages, its snapshot chunks, its
forwarded client requests — is prefixed with the **group envelope**, `uvarint(GroupID) ‖ payload`,
exactly once, at the driver's send boundary. The host's single receive loop removes it and hands the
payload to that group's inbox — or drops and counts the frame: an unknown or stopped (or retired)
group, a malformed envelope, a full inbox. It never delivers a frame to another group, and it never
blocks on one group: a full inbox drops the frame (Raft retransmits) rather than stalling every
other group (INV-MB6, INV-MB9). The core never sees a group id; the transport never interprets one.
A node reading its transport itself (no host) drops frames of other groups the same way. A snapshot
carries a second, independent identity — its file's group id — so a transfer misdelivered at any
layer is still refused (`ErrWrongGroup`).

Tested cases (`TestFramesReachExactlyTheirGroup`, `TestNodeDropsFramesOfOtherGroups`,
`TestEnvelopeRoundTripAndRefusals`, `FuzzUnwrapGroup`): a valid group; an unknown group; a stopped
group; a group created after its frames started arriving; a malformed or non-canonical envelope; a
group id over 32 bits; duplicated frames; one group's frame never moving another group's state.

## 5. Persistence layout

```
<data-dir>/                      mode 0700
  LOCK                           flock'd by the process using the directory (internal/nodedir)
  node.identity                  the node identity: node id, cluster id, replica settings,
                                 initialized (written at -init)
  groups/
    <gid>/
      raft.log                   the group's durable Raft log (entries, HardStates, boundaries)
      raft.log.group             the group identity: group id + genesis configuration + node id (written once)
      raft.log.snap              its one published snapshot (format v2: group id + configuration)
      raft.log.tmp, raft.log.group.tmp, raft.log.snap.tmp, raft.log.snap.recv
                                 temporaries (a log rewrite, an identity, a snapshot being
                                 written or received), removed at startup
```

Groups are isolated by directory; a group's recovery reads only its own files. Two groups start,
recover, snapshot, compact and fail independently (`TestGroupsSnapshotAndCompactIndependently`,
`TestHostRestartRecoversEveryGroup`, `TestBreakingOneGroupLeavesTheOtherRunning`).
`dkvd -raft` — the Phase 9–14 single-group deployment — is the host with one group, 0, whose log
keeps its Phase 9–14 path (`<data-dir>/raft-<id>.log`) and its first-start I/O, so every earlier
real-process test runs unchanged.

**A data directory is one node's, and is initialized only on request** (audit H1,
`internal/nodedir`). Raft's safety rests on each node's durable term, vote and log; a node that
runs without them — on a temporary directory, on another node's directory, beside a second process
on the same one, or on an empty directory under the id of a member whose state was lost — can vote
twice in a term or lose a committed entry. So `dkvd -raft|-cluster`:

- requires `-data-dir` (there is no temporary default) and takes an exclusive `flock` on
  `<data-dir>/LOCK` for its lifetime — the kernel releases it when the process dies, SIGKILL
  included;
- reads `<data-dir>/node.identity`, written once, and refuses a directory recorded for another node
  id or another cluster id;
- initializes a directory that holds no node **only with `-init`** (and a `-cluster-id`), and
  refuses one otherwise: an empty directory and a wiped one look the same, and only the operator
  knows which it is. A node whose state was lost is replaced through a membership change, never
  restarted empty under its old id. `-init` on an initialized directory is refused too, so it cannot
  live in a unit file;
- creates genesis and `-join` groups only while initializing. Initialization is crash-safe:
  `node.identity` is written first, marked unfinished, and marked initialized once every such
  group's identity is durable; a start that finds an unfinished initialization resumes it without
  `-init`. In an initialized directory a genesis or `-join` group with no state is reported
  (`event=group_failed`, or exit 2 in `-raft` mode), never created empty
  (`TestALostJoinGroupIsReportedNotRecreated`, mutant 266). A group new to an initialized node is
  created through the admin port (`create-group`, §7), which cannot tell a new group from one whose
  state was lost: the operator must;
- adopts a directory written before node identities (Raft state, no `node.identity`) once a
  `-cluster-id` is given, unless it holds another node's `raft-<id>.log`;
- pins the node's **replica settings** in `node.identity` (audit H5): the settings every replica
  must share — the session limits, and in `-cluster` mode the routing (`-shards`, `-rf`, the
  sorted `-nodes`) — recorded at initialization; a start whose flags give others exits 2 naming
  both, even one resuming an unfinished initialization (its genesis groups may already exist
  under the recorded settings). Peer addresses are not among them. `-shards/-rf/-nodes` outside `-cluster` mode are
  refused;
- opens the directory **before** the transport listens, and the transport handshake carries the
  recorded cluster id and a SHA-256 digest of the pinned settings: a node of another cluster, or
  one whose settings differ, is never connected to, both ways (`docs/TRANSPORT.md` §3,
  `TestRealImpostorsNeverJoinTheGroup`).

Each group's identity file (version 2) also records its node, and `raftnode` refuses a group's
state recorded for another node; a version-1 file, written before, is still read. Evidence:
`internal/nodedir` (every rule, the codec, the lock), `TestIdentityFileNamesItsNode`,
`TestDataDirectoryRules` (dkvd's own raft-mode code) and `TestRealWipedNodeIsRefused` (a member's
directory wiped and restarted with its ordinary flags is refused, and a second process on a running
node's directory too).

## 6. Client routing

```
key ──Route──▶ shard ──identity──▶ group ──▶ that group's Server ──one hop──▶ the group's leader
```

A client computes the key's group with the cluster's routing and names it in the request
(`Request.Group`; client protocol v3, `docs/API.md`). A node's `kv.Front`:

- refuses a keyed request whose key belongs to another group than it names (`INVALID_REQUEST`), so
  a request can never execute in a group its session does not live in
  (`TestFrontRefusesAMisroutedRequest`);
- hands a request for a group it hosts to that group's `kv.Server`, which serves it or forwards it
  one hop to the group's leader (Phase 13 semantics, unchanged — the forward travels in the group
  envelope, with the request's identity intact);
- answers a request for a group it does not host with `NOT_LEADER` and no hint: nothing was
  proposed, and the client tries another node.

`REGISTER` names its group. **A session is group-local**: its ClientID is a log index of that group,
so the same number in two groups names two unrelated sessions, and a request identity
`(ClientID, RequestID)` is deduplicated by its group alone. A client that touches several shards
holds one session per group (`kv.Sharded`, which registers each on first use). Every operation is
for exactly one key, hence exactly one group; there is no multi-key operation and no cross-group
request semantics (`TestShardedClientRoutesEachKeyToItsGroup`).

## 7. Admin operations

A deliberately small protocol on `-admin-listen`, separate from the client port — one JSON line per
request and per answer, acting on this node's groups only:

| Op | Arguments | Effect |
|---|---|---|
| `status` | | every hosted group's role, term, leader, commit, applied, boundary, snapshot, configuration, pending change; the groups that failed to recover |
| `add-learner` | group, id, addr | add a non-voting member (only the group's leader accepts; others name it) |
| `promote` | group, id | a learner becomes a voter, by joint consensus |
| `remove-voter` | group, id | by joint consensus |
| `remove-learner` | group, id | |
| `create-group` | group, `join` or `voters` | host a group for the first time: as a joiner, or as a genesis member |
| `start-group` | group | start a stopped group again from its own files |
| `stop-group` | group | stop hosting a group, keeping its files |
| `snapshot` | group | snapshot the group's state machine now |

A membership operation completes only when the group's log says so (`docs/MEMBERSHIP.md` §7). The
protocol never decides membership (`TestAdminDrivesMembershipThroughTheLog`).

**The port is unauthenticated plaintext**, and anyone who reaches it can remove voters and stop
groups. So `dkvd` refuses an `-admin-listen` address other than loopback unless
`-admin-allow-remote` is given (audit H6, `TestAdminListensOnLoopbackUnlessAllowed`); expose it
only on a network you trust. No token is offered: on plaintext it would only seem to protect.
What a connection may cost is bounded (audit M1): at most 16 at once (one beyond is closed at
once), 30 s to send each request line, 10 s to take each answer, a request's `timeout_ms` clamped
to 60 s, and an `Accept` error retried rather than ending the loop. The client's `op` and `id` are
logged quoted, so a newline cannot forge an event line; and a member id or address holding
whitespace or a control character is refused, since a member enters the replicated configuration
and every node logs it in its own event lines (`TestAdminCapsItsConnections`,
`TestAdminDropsAnIdleConnection`, `TestAdminTimeoutIsClamped`, `TestAdminSurvivesAcceptErrors`,
`TestAdminLogLinesCannotBeForged`, `TestAMemberIDCannotForgeLogLines`).

## 8. Evidence

- **Host** (`internal/multiraft`, real TCP, race detector): two groups on three nodes independent;
  one group's quorum broken while the other commits; a group whose identity file is corrupt fails
  alone; four groups rediscovered at restart; a joiner added to one group and reached by address
  from the configuration alone; a removed leader's group retired; frames reach exactly their group;
  two groups snapshotting and compacting independently.
- **Client routing** (`internal/kv`): keys land in their own group on every replica; a misrouted
  request executes nowhere; the wire's group round-trips and retired request kinds are refused.
- **Multi-group simulator** (`internal/raftsim`): groups over nodes from the routing; node-level
  faults fanned out to every group, a crash point killing the process in every group; seeded
  schedules of 2 groups on 3 nodes, 4 on 5 (with snapshots) and 8 on 5 — every group converges,
  keeps every invariant and, with clients, is linearizable on its own; same seed, same trace; each
  group's trace reproduced exactly by replaying its own events alone (INV-MB9); one group broken by
  persistence failures while the other commits. Its 100-seed runs found the self-removal liveness
  bug (`docs/MEMBERSHIP.md` §8).
- **Real processes** (`tests/integration/membership_test.go`, `-cluster` mode): two groups on three
  nodes under concurrent session workloads (linearizable); a fourth node added to one group; group 0
  stopped on two of three nodes while group 1 commits on the same processes, then restarted with
  `start-group`; a full SIGKILL restart of a two-group cluster with every configuration and key
  intact; the membership scenarios of `docs/MEMBERSHIP.md` §9.

## 9. Performance

Measured on an Apple M4 (10 cores, 16 GiB), macOS 26.5, Go 1.27.1, `internal/multiraft`
`-multiraft.measure` and the routing benchmarks. Timings describe this machine and are not asserted;
the goroutine structure is.

| Measurement | Result |
|---|---|
| goroutines per added group replica | 2 on one node (actor, receive loop); 4 on three nodes (+ one sender per peer) — asserted |
| heap per idle replica | ≈113 KiB (one node, 8–128 groups); ≈215 KiB (three nodes) |
| idle CPU per replica (50 ms tick) | ≈1.0 ms/s at 128 groups on one node; ≈1.1 ms/s on three nodes (heartbeats); higher per replica with few groups (fixed per-node costs) |
| learner change (one entry, fsync) | p50 16.9 ms, p90 22.8 ms, max 186 ms over 40 changes |
| `Promote` (joint + final, fsync) | 26 ms |
| `RemoveVoter` (joint + final, fsync) | 31 ms |
| new-member catch-up, 20,002 entries (100 B values over 2,000 keys) | 158 ms by entries; 23 ms by a snapshot at 20,000 + 2 entries |
| a key's group (`Assignment.GroupOf`) | 175 ns (4 shards), 241 ns (16), 229 ns (64) |
| the front's routing decision for one request | 403 ns |

Largest configuration exercised: 128 groups per node in one process (three nodes × 128 groups =
384 replicas). No claim is made beyond it.

## 10. Limitations

- **The group set is the shard set.** Groups are created from the routing at genesis, or joined
  explicitly; there is no split, merge, shard movement between groups, or rebalancing policy.
- **Bounded group counts.** Measured to 128 groups per node; every group has its own goroutines,
  ticker and heartbeats, so an idle group is not free (§9). Thousands of groups per node are
  neither tested nor claimed.
- **One process-wide failure domain.** A node crash takes every group it hosts down together; a
  group's persistence failure stops that group only (`-cluster`), or the process (`-raft`).
- **No cross-group anything.** No transactions, no atomic multi-key operation, no global ordering, no
  cross-group deduplication; a session lives in one group.
- **A node that does not host a key's group redirects without a hint**; the client tries another
  node. There is no routing directory beyond each client's copy of the routing configuration.
- **A full inbox drops frames.** Under overload a group loses frames rather than stalling its
  neighbours; Raft retransmits, forwarded client requests time out (unknown outcome).
