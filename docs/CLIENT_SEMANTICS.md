# CLIENT SEMANTICS — Phase 13

Status: **Phase 13 contract — implemented and verified** (on recorded histories and replayed
logs, for one Raft group; `docs/DEDUP.md` §7, `docs/LINEARIZABILITY.md` §15). This document is the
client-visible contract for request identity,
retries, duplicates, conflicting reuse, forwarding and unknown outcomes. `docs/DEDUP.md` is how the
server keeps it (the replicated session table, its bounds, its recovery); `docs/API.md` is the wire
protocol that carries it; `docs/LINEARIZABILITY.md` is how client-visible histories are checked.
ADR-020 records the decisions and the alternatives rejected.

The problem this phase closes: *a client that does not know whether its request took effect must
be able to ask again without the request taking effect twice.* Phase 12 left it open — a retried
write was a second write, and the only honest option was never to retry one.

---

## 1. Logical requests

A **logical request** is one operation a client intends to happen once. It is identified by

    (ClientID, RequestID)

and carries a **command**: the operation, the key and — for PUT — the value. Every transmission of
it (a first send, a retry after a timeout, a resend to another node, a copy a follower forwards, a
duplicate the network delivers twice) is a **transport attempt** of the same logical request.

Three things that are easy to conflate are kept apart:

| | What it is | Who deals with it |
|---|---|---|
| **transport duplicate** | the same bytes delivered twice, or a resend of an attempt | may produce a second Raft log entry; the state machine recognizes it |
| **retry duplicate** | the client re-sending a request whose outcome it does not know, with the same identity | the same |
| **logical duplicate** | a second log entry carrying an identity the state machine has already executed | answered from the session table: no state change, the original result |
| **physical replay** | a restarted node re-applying its committed log from index 1 into a fresh state machine | rebuilds the identical state, session table included; not a duplicate of anything |

The guarantee is about logical requests: **for an identified write, at most one log entry carrying
its identity changes the key-value state** — the first to be applied — and every other entry
carrying it is answered with that first execution's result.

## 2. ClientID — a session the cluster assigns

A **ClientID** is a session id assigned by the cluster: a client sends `REGISTER`, which is a
replicated command, and receives the Raft log index of the `REGISTER` entry that created its
session. Consequences:

- **Unique by construction** for the lifetime of the log: two committed entries never share an
  index, so two sessions never share an id — no client can collide with another by accident, and
  no random-number assumption is needed.
- **Not tied to a connection.** A client may reconnect to any node, any number of times, and keep
  its session; the id travels in every request.
- **Persistent across a client restart only if the client persists it** (with its next
  RequestID). A client that loses them registers a new session; it can no longer retry the old
  session's unknown requests safely, and must report them as unknown.
- **Never inferred from TCP**, never reused after it is evicted (a later `REGISTER` gets a larger
  index).
- **Not authenticated** (no authentication exists, `docs/LIMITATIONS.md`): a client that presents
  another client's id is indistinguishable from it. The contract assumes each client uses only the
  ids it was given.

A `REGISTER` that times out may or may not have created a session; the client simply registers
again. An abandoned session is harmless and is eventually evicted (§8).

**ClientID 0 is anonymous**: no identity, no deduplication, exactly Phase 12's semantics — a write
whose outcome is unknown must not be retried if it must not happen twice. It is kept so that the
Phase 12 contract and its tests remain meaningful, and it is marked as such everywhere.

## 3. RequestID — unique within a session, chosen by the client

A **RequestID** is a positive integer chosen by the client, **unique within its ClientID** (not
globally): `(7, 1)` and `(9, 1)` are unrelated requests. The client library assigns 1, 2, 3, … and
never reuses one. With each request the client also sends **AckedBelow**: the lowest RequestID for
which it does not yet have a response. It is a promise — "I have every response below this and will
never send those ids again" — and it is what lets the server forget old results (§8). It must not
exceed the request's own RequestID.

## 4. What happens to an identified write

The decision is made when the write's log entry is **applied**, in log order, identically on every
replica. For the entry's `(C, R)`:

| Condition at apply time | Decision | State change | Client status |
|---|---|---|---|
| session C does not exist (never registered, or evicted) | **expired** | none | `SESSION_EXPIRED` |
| R < C's AckedBelow | **stale** | none | `REQUEST_STALE` |
| R already executed, same command | **duplicate** | none | `OK`, with the index where it was executed |
| R already executed, different command | **conflict** | none | `REQUEST_CONFLICT` |
| C already holds its maximum of unacknowledged results | **limit** | none | `SESSION_LIMIT` |
| otherwise | **executed** | the write | `OK`, with this entry's index |

"Same command" means the same operation, key and value, compared by a SHA-256 fingerprint of the
command's canonical encoding that the state machine computes itself (a client supplies no hash, so
there is no hash to forge or get wrong). Whichever of two conflicting commands is applied first owns
the RequestID; the other is refused, whatever order they were sent in.

The answers to the questions a retry raises:

1. **Is RequestID globally unique?** No — within a ClientID. The pair is the identity.
2. **Can a client reconnect and reuse its identity?** Yes; that is the point.
3. **Same RequestID, different command?** The later-applied one is refused with
   `REQUEST_CONFLICT` and has no effect; the first one's result stands.
4. **The same request sent to two nodes, or two leaders?** Each may become a log entry; the first
   applied executes, the other is a duplicate with the same result. An entry that is overwritten
   (its leader was deposed) simply never applies.
5. **Committed, reply lost?** The retry becomes a new entry; applied after the original, it is a
   duplicate: `OK` with the original index, no state change.
6. **The same request reaching the state machine twice?** Only the first changes state (row 3).
7. **After a restart?** The session table is part of the replicated state machine, rebuilt by
   replaying the committed log (`docs/DEDUP.md` §4): the answer is identical.
8. **A client reusing an old RequestID after it restarted?** Same command → answered as the same
   request (a duplicate: the server cannot and need not tell); different command → `REQUEST_CONFLICT`;
   below its AckedBelow → `REQUEST_STALE`. Reusing ids is a client bug; the server's answer is still
   well defined and never executes twice.
9. **Client A using client B's RequestID?** Unrelated: RequestIDs are per ClientID.

## 5. Reads

`GET` is not deduplicated and needs no session: a read has no effect, so re-executing it is always
safe, and a retried read returns the value at the time of the attempt that answered. It may carry a
ClientID and RequestID for tracing; they are not checked. As a logical operation, a read that
needed several attempts returns what its last attempt read, which lies within the read's interval
— so it remains linearizable (`docs/LINEARIZABILITY.md` §15).

## 6. Statuses

Every response carries exactly one status. Each belongs to one outcome class, which is what a client
must act on:

| Status | Class | Meaning | What a client does |
|---|---|---|---|
| `OK` | definite, effect | executed — now, or earlier (a duplicate) | done |
| `NOT_FOUND` | definite | a read found the key absent | done |
| `NOT_LEADER` | definite, no effect | this node did not accept the request; `leader` names the one it believes in, if any | send the **same** request there |
| `UNAVAILABLE` | definite, no effect | nothing was sent onward (the leader it would forward to is unreachable, or the node is not serving) | try elsewhere, same request |
| `INVALID_REQUEST` | definite, no effect | malformed or out-of-contract fields | a client bug |
| `REQUEST_CONFLICT` | definite, no effect | this RequestID is taken by a different command | a client bug |
| `REQUEST_STALE` | definite, no effect | below the session's AckedBelow | a client bug (or a very late network duplicate) |
| `SESSION_EXPIRED` | definite, no effect **for this attempt** | the session is unknown or evicted | register again; any earlier attempt whose outcome was unknown stays unknown forever |
| `SESSION_LIMIT` | definite, no effect | too many unacknowledged results in this session | acknowledge (finish outstanding requests), then retry the same request |
| `LOST` | definite, no effect **for this attempt** | the attempt's log entry was overwritten by a different one | identified: retry the same request; anonymous: it did not happen |
| `UNKNOWN_OUTCOME` | unknown | a deadline passed, a node stopped, a forward was lost | identified: retry the same request; anonymous: do not retry a write |

A client that sees a transport failure *before* its request was sent treats it as `UNAVAILABLE`;
*after* it was sent, as `UNKNOWN_OUTCOME`.

## 7. Unknown outcomes

    client sends R → server commits and applies R → server dies → client times out

The request **happened**. The client does not know that; nothing pretends otherwise. The client
retries R — at any node, after any number of leader changes, restarts or reconnects — and the
retry's entry, applied after the original, is a duplicate: `OK`, with the index of the original
execution, and no state change. The response of a duplicate is the proof the client could not get
the first time.

An unknown outcome stays unknown only if the client stops asking: it gives up (its attempts or
its patience run out — `kv.Session` then reports `Known: false`), or its session is evicted before
it retries (`SESSION_EXPIRED`). Either way the client must report every request whose outcome it
never learned as unknown — the contract never turns an unknown into "did not happen".

## 8. Bounds

Unbounded memory is not an option (`docs/FAILURE_MODEL.md` §5), and the bounds are part of the
state machine's definition — identical on every replica, or the replicas would diverge:

- at most **MaxSessions** sessions; registering one more evicts the session whose last command is
  oldest in the log (least recently used, decided by log index, never by a clock);
- at most **MaxUnacked** remembered results per session; results below the session's AckedBelow
  are forgotten; a new request beyond the limit is refused (`SESSION_LIMIT`) rather than evicting a
  result a retry might still need.

Eviction is never silent: an evicted session's requests are refused (`SESSION_EXPIRED`), never
executed as new. That is the difference from "degrades to at-least-once", which the Phase 0 plan
allowed and this contract does not.

## 9. Forwarding

A node that is not the leader forwards a client request to the leader it believes in — **one hop**,
over the internal transport — and relays the answer. A forwarded request is never forwarded again
(a non-leader that receives one answers `NOT_LEADER`), so there is no loop. The forwarder never
resends a forward: if the leader's answer does not come back in time the client is told
`UNKNOWN_OUTCOME` and retries the same request. If the leader cannot be reached before anything is
sent, the answer is `UNAVAILABLE` (definite). If the node knows no leader, `NOT_LEADER` with no
hint. Forwarding cannot duplicate an identified request's effect, because nothing in the forwarding
path decides whether a request executes — only the state machine's apply does (§4). For an
anonymous request forwarding adds no risk either: a forward is sent at most once.

## 10. What the guarantee is — and is not

**Guaranteed**, under the identity assumptions of §2–§3 (a client uses only its own ClientID and
never reuses a RequestID for a different command) and the fault model of `docs/FAILURE_MODEL.md`:

- an identified write changes the key-value state **at most once**, however many times, to however
  many nodes, across however many crashes, restarts, reconnects and leader changes it is sent;
- every `OK` for it — first or duplicate — reports the one execution (its log index);
- if it executed, every later attempt that reaches the state machine while the session exists,
  and before the client has acknowledged the request (moved its AckedBelow past it), receives
  `OK` with that execution's index; and the client-visible history of logical requests is
  linearizable (`docs/LINEARIZABILITY.md` §15).

This is **exactly-once execution of each identified write that executes at all, and at most once
for every other**. It is **not**:

- exactly-once *delivery* — messages are delivered any number of times;
- "every request eventually succeeds" — liveness needs a leader and a quorum;
- protection against a client that reuses an id for a different command (it gets
  `REQUEST_CONFLICT`, not a second execution), or presents another client's id;
- a guarantee after eviction: once a session is evicted, a retry learns only that it expired;
- deduplication of anonymous requests or of reads (neither is recorded).
