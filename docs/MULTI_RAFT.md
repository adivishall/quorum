# MULTI_RAFT — hosting many Raft groups on one node (Phase 15)

Status: **design (Phase 15, in progress).** `docs/MEMBERSHIP.md` is how one group's membership
changes; this document is how a node hosts several groups, how a message finds its group, how a
client finds a key's group, and what stays separate. Sections marked *evidence* are filled in as
the tests land.

---

## 1. Topology

```
                 Node (dkvd process, one transport identity, one data directory)
                   │
        ┌──────────┼──────────┐
        │          │          │
      Group 0    Group 1    Group N          one GroupID each
        │          │          │
      raftnode   raftnode   raftnode         one driver, one pure core each
        │          │          │
     log/snap   log/snap   log/snap          groups/<gid>/raft.log, .snap, .meta
        │          │          │
     kv.Store   kv.Store   kv.Store          one state machine, one session table each
```

Each group is an independent consensus domain: its own membership, log, snapshot, commit index,
applied index, leader, term, and session table. Nothing is shared between groups except the
process, the transport connections, and the node's data directory. **There is no cluster-wide
Raft** and no cross-group atomicity: every client operation names one key, one key lives in one
shard, one shard is served by one group (ADR-001, ADR-008). A node crash is a *correlated* failure
of every group it hosts; each recovers on its own.

## 2. Identities and assignment

`ShardID → GroupID` is the identity function in Phase 15 (`docs/MEMBERSHIP.md` §1). Which nodes
form each group's **initial** membership comes from the Phase 6 routing metadata and nothing
else: the cluster bootstrap is a `routing.Config{ShardCount, ReplicationFactor, Nodes}` plus each
node's address; `Router.ReplicaGroup(s)` is group `s`'s genesis voter set. No second assignment
algorithm exists (INV-C1..C3 keep holding for `key → shard`). After bootstrap a group's membership
is its own replicated configuration; the routing metadata is not consulted again for membership.

## 3. The node host (`internal/multiraft`)

The host owns:

- **the group registry**: which groups this node hosts, discovered at start from
  `groups/<gid>/raft.log.meta` (a directory without a valid `.meta` refuses the start — loud, never
  guessed);
- **group lifecycle**: create (write the genesis, then start), start, stop, join (create a group
  with an empty genesis, to be added by its leader), and stop-on-removal (a group whose committed
  configuration excludes this node is stopped; its files are kept — deletion is an operator's
  explicit action, never automatic);
- **inbound routing** (§4) and the transport's peer set (the union of every hosted group's members,
  added as configurations arrive);
- **client routing** (§6) and the admin operations (§7).

Each group runs the unchanged Phase 14 driver (`raftnode.Node`) over its own files, with its own
`kv.Store` and `kv.Server`.

## 4. Message routing — the group envelope

The transport still carries bytes tagged with a frame kind and attributes them to the connection's
handshake identity (INV-T4). Every Raft, snapshot and forwarding payload a node sends is prefixed
with the **group envelope**: `uvarint(GroupID) ‖ payload`. The host's receive loop unwraps it and
hands the inner payload to that group's driver; a frame naming a group this node does not host
is dropped and counted, never delivered to another group (INV-M6). The Raft core never sees a
group id; the transport never interprets one. The envelope is the smallest boundary that lets one
connection carry every group's traffic: no frame-format change, no per-group connections.

Cases the routing must get right, each with a test (*evidence*): an unknown group; a stopped
group; a group being created or removed while a message is in flight; a malformed envelope; a
message meant for another node (the transport's handshake identity already prevents this); a
duplicated frame.

## 5. Persistence layout

```
<data-dir>/
  node.meta                      this node's id and the cluster bootstrap (written once)
  groups/
    <gid>/
      raft.log                   the group's durable Raft log (entries, HardStates, boundaries)
      raft.log.meta              the group's id and genesis configuration (written once)
      raft.log.snap              its one published snapshot (format v2: group id + configuration)
      raft.log.tmp / .snap.tmp / .snap.recv    temporaries, removed at startup
```

Groups are isolated by directory; a group's recovery reads only its own directory
(`docs/SNAPSHOTS.md` §6), so a failure to recover one group is reported for that group and does
not stop the others from starting (INV-M7). Phase 14's single-group layout (`raft-<id>.log`
beside the data directory) is retired; `dkvd -raft` is group 0 of a one-group cluster.

## 6. Client routing

A client operation carries one key. The node that receives it computes `shard = Route(key)`,
`group = GroupID(shard)`, and:

- if it hosts the group, hands the request to that group's `kv.Server` — which serves it, or
  forwards it one hop to the group's leader (Phase 13, unchanged; the forward travels in the group
  envelope);
- otherwise answers `NOT_LEADER` with a hint naming a member of the group it knows, and the client
  goes there itself. There is never a second forwarding hop (INV-X13).

`REGISTER` carries no key, so a request names its group explicitly (`Request.Group`; wire protocol
v3); a **session is group-local** — its ClientID is a log index of that group — and a client that
touches several shards holds one session per group (`kv.ShardedClient` does this for the tests;
the request identity `(ClientID, RequestID)` is scoped by group, and no cross-group deduplication
exists or is claimed).

## 7. Admin operations

A minimal admin protocol on `-admin-listen`, separate from the client API: `Describe(group)`
(configuration, leader, term, indexes), `AddLearner`, `Promote`, `RemoveVoter`, `RemoveLearner`,
`Join(group)` (start hosting a group as a joiner), `Snapshot(group)` (a test snapshot). A
membership operation is accepted only by the group's leader; any other node answers with the
leader it believes in.

## 8. Evidence

*Filled in as the phase's tests land.*

## 9. Limitations

*Filled in as the phase's tests land.*
