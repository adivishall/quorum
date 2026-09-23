# CONSISTENCY MODEL

Status: **Phase 10 — the consensus core is verified in simulation and under injected faults; the
end-to-end model is not.**
The claims in §2 remain design targets, not yet properties of the running system. This file is
updated after Phase 12 to say which claims are backed by passing tests and which are not.

**Currently verified (Phase 9).** Raft's *safety* properties — election safety, log matching,
leader completeness, state-machine safety, the §5.4.2 commit rule, and durable term/vote before a
dependent reply — hold in a deterministic simulation, and durable Raft state survives process kill
(`docs/RAFT.md`, `docs/INVARIANTS.md` INV-R1..R10). These are the mechanisms §4 relies on
(single vote per term, current-term commit + no-op, fsync before reply). **Phase 10**
(`docs/FAULTS.md`) re-verified them under injected drops, duplicates, delays, reordering,
partitions, crashes, a modeled power loss, restarts and persistence failures — continuously, in a
seed-replayable simulator — and on the real driver and real processes.

**Not yet verified.** End-to-end single-key linearizability
against a real cluster under faults (Phase 12); client retry / dedup semantics (C4 stage two,
Phase 13); the distributed API and `stale`/`linearizable` read serving including ReadIndex (§8.5,
Phases 13/15); crash recovery across every failure window of a node that hosts the storage
engine (Phase 11); and real power-loss durability (untestable here). The claims C1–C5 below
are therefore **still design targets**: Raft working is a necessary part of them, not the whole
proof.

---

## 1. The model in one paragraph

Quorum targets **linearizable single-key operations**. `PUT`, `GET`, and `DELETE` on a single
key behave as if they took effect instantaneously at some point between the client's request
and the client's response, consistent with real time. There are no transactions, no multi-key
atomicity, and no snapshot isolation. Under a network partition, the minority side becomes
**unavailable** for that shard rather than serving stale or divergent data — Quorum is CP in
the CAP sense, per shard.

---

## 2. Precise claims, and the conditions attached to each

**C1. Single-key linearizability.**
Every `PUT`/`GET`/`DELETE` on key *k* is linearizable, provided:
- (a) the write was acknowledged to the client (a timeout is *not* a failure — see C4);
- (b) reads are served in `linearizable` mode (the default), which uses ReadIndex
  (`docs/DESIGN.md` §8.5) — not follower reads;
- (c) no more than *f* of the shard's 2*f*+1 replicas have failed;
- (d) the client's retries are deduplicated (see C4).

**C2. Composition across keys.**
Linearizability is a *local* property (Herlihy & Wing, 1990): a history is linearizable iff
the projection onto each object is linearizable. Each key lives in exactly one shard and
every operation on it goes through that shard's single Raft log, so per-key linearizability
composes into a linearizable history for the whole store.

What this does **not** give you:
- No consistent snapshot across keys. Reading `a` then `b` can observe a state no single
  instant ever had, *if* you reason about them as a pair. That is still a linearizable
  history; it is not a serializable transaction.
- No atomic multi-key write. There is no API for one, precisely so that we never have to
  pretend there is a guarantee.

**C3. Durability.**
An acknowledged write is durable against the simultaneous failure of any *f* replicas in its
shard, because it was committed by a majority and fsynced on each of them before the ack.
"fsync" means what `docs/DESIGN.md` §3 says it means, and it depends on the configured
`wal.sync` mode. In the default `batch` mode a write survives **process kill** but may not
survive **host power loss**. We test the former. We do not test the latter and therefore do
not claim it.

**C4. Client retries — the honest version.**
A client that sends a write and receives a **timeout** does not know whether the write
committed. This is unavoidable; it is not a bug. Two stages:

| Stage | Guarantee | Consequence |
|---|---|---|
| Phases 9–12 | **at-least-once** application | A retried `PUT` may be applied twice. For `PUT`/`DELETE` (idempotent given the same value) the final state is the same, but a retry that lands *after* a newer write from another client will clobber it. C1 does **not** hold under retries in this stage. |
| Phase 13 onward | **exactly-once application** per `(clientID, seqNo)` | The state machine keeps a dedup table. A duplicate proposal is recognized at apply time and returns the original result without re-applying. C1 holds under retries. |

Note the phrase: exactly-once **application**, not exactly-once delivery. Messages are still
delivered any number of times. What is guaranteed is that the state machine transition
happens once.

**C5. Read modes.** The API exposes an explicit mode per read:

| Mode | Route | Guarantee | Cost |
|---|---|---|---|
| `linearizable` (default) | leader, ReadIndex quorum round | C1 | 1 network RTT to a quorum |
| `stale` | any replica, local read | **eventual consistency only.** May return arbitrarily old data, including data from before a write the same client just made. | 0 RTT |

There is no third mode that is "fast and also correct". `stale` is labeled `stale` in the API,
in the CLI flag, and in the dashboard.

---

## 3. What we will not claim

Written down explicitly so that nobody, including future me, quietly upgrades the language:

- **Not serializable / not strictly serializable.** Those terms describe transactions. We have
  no transactions.
- **Not "strong consistency"** as a bare phrase. It is marketing, not a model. The model is
  named in §2.
- **Not exactly-once delivery.** See C4.
- **Not external consistency / TrueTime-style.** We make no clock assumptions.
- **Not linearizable for `stale` reads.** Obviously, but it is worth stating because dashboards
  love to read stale and look fast.
- **Not production-ready.** See `docs/LIMITATIONS.md`.

---

## 4. Where linearizability could actually break

The failure modes we are specifically defending against and testing:

1. **Split brain.** Two leaders in the same term. Prevented by: one vote per node per term,
   persisted before replying. Tested by fault injection with partitions.
2. **Stale leader serving reads.** A partitioned leader that has not noticed. Prevented by the
   ReadIndex quorum round. This is the classic bug in naive Raft implementations that serve
   reads locally on the leader; if we skipped step 2 of §8.5 the system would be fast and
   wrong.
3. **Committing an entry from a previous term by counting replicas.** Raft §5.4.2's figure-8
   scenario. Prevented by: a leader only advances `commitIndex` for entries in its **own**
   term, and appends a no-op on election so that it can.
4. **Applying uncommitted entries.** Prevented by the apply path consuming only
   `entries[appliedIndex+1 : commitIndex]`.
5. **Losing a committed entry on restart.** Prevented by fsync of log + HardState before the
   AppendEntries reply. This is the one most likely to be silently broken by an optimization,
   which is why the crash tests kill with SIGKILL rather than a graceful shutdown.
6. **Duplicate application under retry.** C4 / Phase 13.
7. **Divergent replicas after compaction.** Different replicas compacting at different times
   must still expose identical logical state. Tested by comparing full key-space dumps across
   replicas after chaos.

---

## 5. Availability, stated honestly

Per shard, with replication factor 3:
- 0 failures: available.
- 1 failure: available after a leader election (expected sub-second; measured in Phase 19).
- 2 failures: **unavailable for writes and for linearizable reads.** `stale` reads still work
  against whatever replica you can reach. This is the correct behavior and we do not attempt
  to "degrade gracefully" into serving writes without a quorum.

Because shards are independent, losing a node makes *no* shard unavailable at RF=3 in a
3-node cluster, but it does make every shard elect. Cluster-wide availability is the
conjunction of per-shard availability; the dashboard shows this per shard rather than as a
single green light.

---

## 6. How the claims get verified (Phase 12)

A claim with no test behind it is a comment. The plan:

1. **Deterministic Raft unit tests** — the core is a pure state machine, so every scenario in
   the Raft paper's figures is a table-driven test with no timing.
2. **Simulated-network cluster tests** — n nodes, one goroutine, a message queue we control.
   Drop, delay, duplicate, reorder, partition, crash, restart — all deterministic and
   replayable from a seed.
3. **Linearizability checking** — record a real concurrent history (invocation time, response
   time, operation, argument, result) from a real multi-process cluster under fault injection,
   then check it. Because our objects are single-key registers and histories project per key,
   we can use the Wing–Gong linearizability algorithm with memoization per key. This is sound
   and tractable here precisely *because* the API is restricted to single-key operations.
4. **Reference-model differential testing** — run the same operation stream against an
   in-memory `map[string]string` and against the cluster; any divergence is a bug until
   explained.
5. **Real-process crash tests** — SIGKILL, restart, verify recovered state.

Any discrepancy found by (3) or (4) is treated as a defect in the implementation, never as a
reason to relax the model. If we discover the model is genuinely unachievable, the correct
response is to change §2 *and* say so in the phase report — not to weaken the checker.
