# OPERATIONS — dkvctl, health and readiness

Status: **implemented** (`cmd/dkvctl`, `internal/ctl`, `internal/health`; the admin protocol's
status fields in `internal/multiraft/admin.go`).

`dkvctl` is the operator's command line. It is a client of `dkvd`'s admin protocol (`-admin-listen`,
JSON lines over TCP; `docs/MULTI_RAFT.md` §7) and of nothing else. Every answer it prints is
either a node's own status, read now, or a verdict derived from such answers by `internal/health`.
It does not cache, guess or simulate: a node that does not answer is reported **unreachable**.

There is no HTTP API for this. The admin protocol already carries every fact an operator needs,
and `dkvctl`'s exit codes make it usable as a probe.

---

## 1. Commands

```bash
dkvctl -nodes n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 <command>
dkvctl -admin 127.0.0.1:9001 <command>          # one node
```

| Command | Prints |
|---|---|
| `status` | Every group on every node: role, term, leader, commit / applied / last index, snapshot and log boundary, voter, pending writes / reads; groups that failed to recover; unreachable nodes |
| `leader [-group g]` | Each group's leader and term, and how many voters confirm it (of the quorum needed) |
| `groups` | Each group's configuration (voters, learners, a joint configuration's outgoing voters), its index, whether a change is pending, which nodes host it |
| `members -group g` | One group's configuration |
| `lag` | Per group, on its leader: every voter's state (leading, following, lagging, stale, unreachable) and its lag in entries; every node's apply lag |
| `health` | Each group's health and every node's readiness (§3) |
| `ready [node]` | One node's readiness (§2) |
| `snapshot -group g [node]` | Makes a node, by default the group's leader, snapshot the group |

Options:
- `-json`: the same facts, machine-readable.
- `-timeout`: per-node request timeout, default 2 s.
- `-max-apply-lag` and `-max-follower-lag`: the bounds §2 and §3 use, default 64 and 1024 entries.

**Exit codes**, so a script or an orchestrator's probe can branch on them:

| Code | Meaning |
|---|---|
| 0 | ok · `health`: healthy · `ready`: ready |
| 1 | `health`: degraded · or the requested operation itself failed (a snapshot refused) |
| 2 | usage error |
| 3 | `health`, `leader`: some group has no confirmed leader (unavailable) · `ready`: not ready |
| 4 | unreachable: no node named answered |

## 2. Node readiness — may this node be sent traffic?

From the node's own status only (`health.Node`):

| Verdict | When |
|---|---|
| `unreachable` | the admin port did not answer |
| `not-ready` | It answered (it is **live**), but one of these holds: it hosts no group (a group it was removed from does not count); a group failed to recover at startup; in some group it belongs to it knows no leader (a candidate, a follower that timed out); it is a joiner not yet in any configuration; or its applied index trails its commit index by more than `-max-apply-lag`. |
| `ready` | otherwise |

Three different facts, deliberately not merged:
- **Live** means the process answered.
- **Ready** means this node, by its own state, can take requests: it knows a leader to serve or
  forward to, and has applied what it knows to be committed.
- **A healthy cluster** (§3) is a claim about agreement between nodes, which no single node can make.

**What node readiness cannot see.** Raft without CheckQuorum (`docs/LIMITATIONS.md`) lets a leader
cut off from its peers keep believing it leads until it hears a higher term. By its own state it is
ready. Only the group view (§3) shows it, because the majority follows another leader in a higher
term. `TestDkvctlOnARealCluster` checks both halves on real processes.

## 3. Group health — can each group make progress?

From every named node's status (`health.Cluster`). For each group:
1. **The leader** is the node reporting itself leader in the highest term.
2. **The voters** are those of that leader's configuration (both halves while it is joint). With no
   leader, they are the union of the voters every node reports.
3. **The verdict:**

| Verdict | When |
|---|---|
| `healthy` | A majority of the voters, the leader included, confirm the leader: they report following it in its term. Every voter answered, follows it, and trails it by at most `-max-follower-lag` entries, measured on the leader as its last index minus the voter's match index. |
| `degraded` | A majority confirms the leader, but some voter is unreachable, follows another leader or term (**stale**), or lags. The group makes progress without it. |
| `unavailable` | No node reports leading, or fewer than a quorum of voters confirm the one that does. Nothing shows the group can commit a write. |

- **The cluster's verdict** is its worst group's.
- **A named node that does not answer** makes the cluster at least degraded.
- **A voter nobody named** counts as unreachable. Health is never claimed for a node nobody asked.

These are snapshots: a verdict says what the answers showed when they were read. A group in the
middle of an election is `unavailable` for that moment, and `health` called again a second later
may say `healthy`. Probes should allow for that, as Kubernetes' failure thresholds do.

## 4. What the admin status carries

`dkvctl` reads one admin op, `status`. Since this milestone it also carries:
- the answering node's id (`node`);
- on each group's leader, every other member's `follower_match`, which gives its lag;
- each group's `pending_writes` and `pending_reads`.

The fields are additive, and older clients ignore them. `TestAdminStatusNamesItsNodeAndTheLeadersFollowers`
pins them.

## 5. Tests

| Test | Checks |
|---|---|
| `internal/health` | Every verdict on synthesized states: healthy; leader lost; a stale isolated leader (degraded in the group view, ready in its own); a restarting node; a majority down; a lagging follower; each reason a node is not ready; an unnamed voter |
| `internal/ctl` | Every command against fake admin servers speaking the real protocol: both output forms, every exit code, every usage error |
| `TestDkvctlOnARealCluster` | Real `dkvd` processes taken through these states: healthy; the leader isolated (degraded, the old leader stale); healed; a node killed (unreachable, then ready after its restart); two of three killed (unavailable, and the survivor not ready because it lost its leader) |

Mutants 302–304 and 308 are each killed:
- a node ready with no leader;
- a leader accepted without a confirming quorum;
- `dkvctl health` exiting 0 on a degraded cluster;
- a status without the leader's follower match.

## 6. Limits

- **You must name the nodes.** A group's configuration holds transport addresses, not admin
  addresses, so `dkvctl` cannot discover the rest of a cluster from one node.
- **The admin port is unauthenticated** and binds to loopback unless `-admin-allow-remote` is
  given (`docs/LIMITATIONS.md`). `dkvctl` has no credentials to send.
- **Readiness is not a load balancer.** `dkvctl ready` is a probe; nothing in Quorum routes clients
  by it.
