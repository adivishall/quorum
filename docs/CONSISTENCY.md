# CONSISTENCY MODEL

Status: **Phase 13 — client-visible linearizability of single-key operations is verified on
recorded histories, for one Raft group, including under retries of identified writes (C4 stage
two), with the conditions below.** `docs/CLIENT_SEMANTICS.md` is the Phase 13 contract (request
identity, retries, duplicates, statuses); `docs/DEDUP.md` how the server keeps it. `docs/LINEARIZABILITY.md` is
the full statement (object model, write completion, ReadIndex and its safety argument,
incomplete operations, the checker and how it was validated, every scenario, every mutant).

**What is verified now (Phase 12).** For `PUT`/`GET`/`DELETE` on single keys, served by one Raft
group through `internal/kv` — a write acknowledged only once committed and applied on the serving
node in its proposal's term, a read served only through ReadIndex — every client-visible history
recorded from real `dkvd` processes (concurrent clients; leader and follower SIGKILL and restart;
partitions and heals; repeated leader changes; a SIGKILL at every point of a write's life), from the
real driver in-process (message drops, duplicates, reordering), and from 1,400 seeded simulator
runs under every fault family, is **linearizable**, as decided by a checker validated against an
independent oracle on 20,000 arbitrary histories. The mechanisms are argued (write completion:
LINEARIZABILITY §3; ReadIndex: §5.2) and pinned by 27 Phase 12 mutants.

That is C1 below **with its condition (d) replaced**: Phase 12 records every retry as its own
operation (an unknown write is never silently retried), and the guarantee holds for those honest
histories. It did not hold for a caller that hides a retry of an unknown write inside one
operation (LINEARIZABILITY §4.3 shows the exact history).

**What Phase 13 adds (C4 stage two).** A write that carries a request identity — a session's
`(ClientID, RequestID)` — is executed at most once however many times it is sent, to however many
nodes, across crashes, restarts, leader changes and forwarding; every retry of it is answered with
the one execution's index. Its sends are therefore one **logical operation**, and the histories of
logical operations — from real processes (the hardest case at every crash window, forwarders
killed before relaying, concurrent copies through every node, a full-cluster restart with small
limits), the real driver, and 1,200 more seeded simulator runs in which every replica's every
apply-time decision is checked against an independent model — are linearizable (LINEARIZABILITY
§15; INV-X2, X11–X14; 29 more mutants). Anonymous writes keep Phase 12's semantics.

**Still verified from earlier phases.** Raft's safety properties (INV-R1..R10; Phase 9), under
injected faults (Phase 10, `docs/FAULTS.md`), and across crashes at every boundary of the node's
cycle (Phase 11, `docs/CRASH_RECOVERY.md`: application is at-least-once across restarts, exactly-once
within an incarnation — the applied index is volatile and a restart replays the committed prefix).

**Not yet verified.** More than one Raft group and routed keys (the routing layer is not wired to Raft); the Phase 15 API
and `stale` reads (C5 — no such mode exists yet); snapshots and membership change (Phase 14+);
the storage engine hosted by a node; real power-loss durability.

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
- (c) no more than *f* of the shard's 2*f*+1 replicas have failed (for liveness; safety needs no
  bound);
- (d) the client's retries are deduplicated (see C4) — **or**, in Phases 12 and before, every retry
  is recorded as a separate operation (LINEARIZABILITY §4.3).

*Phase 12 status:* verified on recorded histories for one group, with (d) in its Phase 12 form
(LINEARIZABILITY §7–§8). Only the ReadIndex path exists; there is no `stale` mode yet.

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
| Phases 9–12 | **at-least-once** application (verified in Phase 11: a restart re-applies the whole recovered committed prefix; exactly-once holds only within one incarnation) | A retried `PUT` may be applied twice. For `PUT`/`DELETE` (idempotent given the same value) the final state is the same, but a retry that lands *after* a newer write from another client will clobber it. C1 does **not** hold under *hidden* retries in this stage. Phase 12 made this concrete on real processes: an unacknowledged-but-committed `PUT(A)`, a `PUT(B)` by another client, then the retry of `PUT(A)` — linearizable as two operations, rejected by the checker as one (`TestRealIncompleteWriteThenRetry`). The Phase 12 test client therefore never retries an unknown write. |
| Phase 13 onward | **at most one execution per `(ClientID, RequestID)`** for identified writes — exactly one if it executes at all (verified: INV-X2, INV-X11; LINEARIZABILITY §15) | The replicated state machine keeps a bounded session table (`docs/DEDUP.md`). A duplicate entry is recognized at apply time and answered with the original execution's index without re-applying; a different command under a used id is refused (`REQUEST_CONFLICT`). C1 holds under retries of identified writes. Limits: a retry after its session was evicted learns only `SESSION_EXPIRED`; anonymous writes are as in Phases 9–12. |

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
   wrong. *Phase 12:* attacked on real processes (`TestRealStaleLeaderNeverServesARead`: the
   read times out, never returns), with delayed pre-read acknowledgements in the simulator
   (`TestKVStaleLeaderReadIsNeverServed`), and by mutants 36, 37 and 43, each killed.
3. **Committing an entry from a previous term by counting replicas.** Raft §5.4.2's figure-8
   scenario. Prevented by: a leader only advances `commitIndex` for entries in its **own**
   term, and appends a no-op on election so that it can.
4. **Applying uncommitted entries.** Prevented by the apply path consuming only
   `entries[appliedIndex+1 : commitIndex]`.
5. **Losing a committed entry on restart.** Prevented by fsync of log + HardState before the
   AppendEntries reply. This is the one most likely to be silently broken by an optimization,
   which is why the crash tests kill with SIGKILL rather than a graceful shutdown.
6. **Duplicate application under retry.** C4. *Phase 12:* made visible (LINEARIZABILITY §4.3).
   *Phase 13:* prevented for identified writes by deciding at apply, from the replicated session
   table, identically on every replica (`docs/DEDUP.md`); anonymous writes are still exposed.
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

*Phase 12 status:* items 1–5 below are done; LINEARIZABILITY §6–§9 says exactly how, and §12 there
lists what remains untested. Item 3 is implemented as `internal/lincheck` (validated against an
independent oracle before being trusted) over histories from real processes, the real driver and
the simulator; item 4 is the sequential baseline diffed against the model op by op, the store
diffed against the model over the committed log (INV-X8), and the store against the Phase 2
storage contract.

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
