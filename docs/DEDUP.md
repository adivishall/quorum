# DEDUP — how the server keeps the request-identity contract (Phase 13)

Status: **Phase 13, implemented and verified on recorded histories for one Raft group.**
`docs/CLIENT_SEMANTICS.md` is the contract this document implements; `docs/API.md` is the wire
protocol; `docs/LINEARIZABILITY.md` §15 is how the resulting histories are checked; ADR-020
records the decisions and the alternatives rejected.

---

## 1. Where the state lives — and why only there

The deduplication state is a **session table inside the replicated state machine** (`kv.Store`,
`internal/kv/store.go`). It is changed only by applying committed log entries, in log order, on
every replica; it is never written by a request handler, a forwarder, a client connection or a
timer.

That placement is the whole design. A duplicate can arrive as a second log entry at any node —
the same request sent to two leaders in two terms, a retry after a leader change, a forward
re-sent by a client through another follower, a network duplicate — and the only point where all
of those are totally ordered and seen identically by every replica is **apply**. A table kept
beside the log (in the server, in memory, keyed by connection) would be:

- **volatile** — lost on restart, so a retry after a crash executes again;
- **per node** — a retry that reaches a different leader finds no record;
- **racy** — two copies in flight at once both find "not yet executed".

The alternatives were considered and rejected in ADR-020: a leader-local in-memory table
(fails all three), checking at propose time (a duplicate can be proposed before the original
commits — `TestDuplicateSentBeforeTheOriginalCommits` — and a new leader has not applied its
predecessor's tail), a separately persisted table (a second source of truth that must be kept
atomic with the log), and client-generated random ids (collisions, and nothing to evict by).

## 2. The session table

```
Store
  sessions  map[ClientID]*session          at most Limits.MaxSessions entries
  session
    last        uint64                     index of the last entry that touched it (LRU key)
    ackedBelow  uint64                     the client's promise: every id below is finished
    results     map[RequestID]execution    at most Limits.MaxUnacked entries
  execution
    fp     [32]byte                        SHA-256 of the command's canonical anonymous encoding
    index  uint64                          the log index where the request executed
```

A **ClientID** is the log index of the committed `REGISTER` entry that created the session —
unique for the life of the log, assigned by the cluster, never inferred from a connection, never
reused (a later `REGISTER` has a larger index). ClientID 0 is anonymous: Phase 12 semantics, no
record kept.

The table remembers **which request executed where** (the fingerprint and the index), not the
response bytes: every write's response is fully determined by that pair (`OK`, the original
index, `Duplicate`), so there is nothing else to cache. Reads are never recorded (§6).

## 3. The decision, at apply, in this order

`Store.ApplyResult(index, command)` (`decide`), for a committed entry:

| # | Condition | Decision | Effect on the table | Effect on keys |
|---|---|---|---|---|
| 1 | `REGISTER` | **registered** (ClientID = index) | new session; if now more than MaxSessions, evict the one with the smallest `last` | none |
| 2 | anonymous PUT/DELETE | **executed** | none | the write |
| 3 | session unknown (never registered, or evicted) | **expired** | none — **never re-created** | none |
| 4 | — | — | `last = index`; `w = min(AckedBelow, RequestID)`; if `w > ackedBelow`: raise it and forget results below `w` | — |
| 5 | RequestID < ackedBelow | **stale** | (as 4) | none |
| 6 | result for RequestID held, same fingerprint | **duplicate** (original index) | (as 4) | none |
| 7 | result held, different fingerprint | **conflict** | (as 4) | none |
| 8 | MaxUnacked results already held | **limit** | (as 4) | none |
| 9 | otherwise | **executed** | record (fingerprint, index) | the write |

Every decision is a function of the command, the entry's index and the table — no clock, no
randomness, no node identity, no map-iteration order (the LRU victim is the unique smallest
`last`: two sessions never share one, since each entry touches at most one session). So every
replica decides every entry identically; the simulator checks exactly that at every apply
(INV-X11, §7).

The **limits are part of the state machine's definition**. Two replicas with different limits
decide differently and diverge; `dkvd -session-max/-session-max-unacked` must be identical on
every node of a group, restarts included. Nothing detects a mismatch today (a limitation, §10).

## 4. Durability and recovery: the log is the persistence

There is no separate persistence for the table. It is **rebuilt by replay**: a restarted node
starts with an empty `kv.Store`, and the driver re-applies its recovered committed prefix from
index 1 (`docs/CRASH_RECOVERY.md` §6). Because `ApplyResult` is a pure function of (table,
index, command), replaying the same prefix in the same order rebuilds the same table, by
induction on the index — the same argument that makes the key-value map correct after a restart,
applied to one more field of the same state machine.

**Physical replay is not a logical duplicate.** Replay re-applies entry *i* to a state that has
not seen *i* (the store is fresh), so the decision at *i* is the same one made the first time — an
entry that executed executes again *into the fresh store*, which is exactly reconstruction; an
entry that was a duplicate is a duplicate again. A logical duplicate is a *different* entry *j*
carrying an identity that executed at *i < j*. The two are never confused: the table is keyed by
identity and filled in log order, and replay results are delivered to no client (a waiter exists
only for an entry proposed by this incarnation, and completes only for its own (index, term) —
INV-X5/X6).

What each failure does, in the windows of one identified write (`TestRetryAtEveryCrashPointOfAWrite`
in-process, `TestKVSessionRetryAfterCrashAtEveryPoint` simulated, `TestRealSessionRetryAcrossCrashWindows`
on real processes):

| The leader dies… | The entry | The client's retry of the same request |
|---|---|---|
| before the entry is persisted anywhere (`before-save`) | nowhere | the first execution |
| after it is persisted on the leader only (`after-save`, first save) | on the dead leader's disk only; overwritten by the new leader's log | the first execution |
| after it may have reached followers (`after-send`, `before-advance`) | committed iff a quorum persisted it before the next election | either, decided by that; the scripted simulator runs observe a duplicate at both points |
| after the commit is persisted (`after-save` second, `before-apply`) | committed | a **duplicate** of the original index |
| after apply, before or after the applied index is recorded (`after-apply`, `after-applied-to`) — the dedup record is part of the apply | committed and executed | a duplicate |
| before the reply is written (`before-reply`) | committed and executed | a duplicate |
| after the reply (`after-reply`) | committed; the client may know | a duplicate (retrying a known request is allowed) |

In every row the request executes exactly once; which entry executes it is what varies. The real
tests establish each row's premise from the victim's durable log (`raftlog.Inspect`), not by
assumption.

"The dedup state is persisted" is not a separate window: the record is a consequence of applying a
committed entry, and the committed entry is durable before it is applied. Power loss and torn
writes are covered where they are modelled — the simulator's `kv-sessions-crashes`/`-crashpoints`/
`-mixed` profiles lose unsynced data and tear the log tail, and every replica's replay is checked
against the model entry by entry (INV-X11).

A **lagging follower** catching up applies the same entries in the same order and reaches the same
table; a **new leader** has the table of every entry it has applied, and answers a request only
after applying it, in its own term — a request that arrived at the old leader and at the new one
is two entries, and whichever applies second is the duplicate.

## 5. Concurrency

Deduplication needs no locking beyond the store's mutex: decisions happen in the apply goroutine,
one entry at a time. Concurrent copies of one request (two connections, two nodes, a forward and
a direct send) become separate log entries and are ordered by the log; the first applied executes,
every later one is a duplicate carrying the first's index (`TestConcurrentDuplicatesAtTwoNodes`,
20 rounds of three copies on real processes in `TestRealConcurrentDuplicatesThroughEveryNode`).
A client's concurrent requests share one session: each carries `AckedBelow` = the lowest id the
client still has in flight, so the watermark never passes an unanswered request
(`TestConcurrentRequestsFromOneSession`, 16 goroutines; mutant 75).

## 6. Bounds, eviction, memory

| Bound | Default | Enforced | When hit |
|---|---|---|---|
| sessions | `MaxSessions` = 1024 | at `REGISTER` | the least recently used session (smallest `last`) is evicted; its later requests are `SESSION_EXPIRED` — **never executed as new** |
| results per session | `MaxUnacked` = 128 | at a new request | `SESSION_LIMIT` (no effect); a result a retry may still need is never evicted to make room |
| results below the watermark | — | whenever `AckedBelow` rises | forgotten: the client promised never to send those ids again (a later send is `REQUEST_STALE`) |

So the table holds at most MaxSessions × MaxUnacked results: `TestSessionTableStaysBounded` applies
20,000 commands that press on every bound and checks both limits after each. Measured (§8): a
**full table at the default limits holds 13.4 MiB** (≈107 B per remembered result, including the
session and map overhead). Nothing expires by time — eviction is by count and log position, the
only order all replicas share.

Eviction's consequence is the one way an unknown outcome stays unknown: a client whose session
was evicted before it retried learns `SESSION_EXPIRED` and nothing about its earlier attempts
(CLIENT_SEMANTICS §7). The Phase 0 plan allowed "degrades to at-least-once" here; this contract
does not — an evicted session's requests are refused, not re-executed (mutant 65).

## 7. How it is verified

- **Two independent implementations.** `lincheck.SessionModel` (`internal/lincheck/session.go`) is
  the contract written again, with no shared code, as a reference. `TestStoreAgreesWithTheSessionModel`
  diffs every decision, index and session table over 400 random runs built to reach every decision;
  `TestReplayRebuildsTheSessionTable` cuts 200 random runs at a random index, replays the prefix
  into a fresh store and requires the identical table at the cut and identical decisions after it.
- **At every apply, on every replica, in simulation (INV-X11).** The simulator feeds the model the
  committed log in index order and requires each node's decision for each entry — first
  application and every replay after a crash or power loss — to equal the model's; and no identity
  may execute at two indexes (INV-X2). Six session profiles run in `make faults` (200 seeds each:
  crashes, crash points, partitions, message faults, mixed, and eviction with 3 sessions × 2
  results); INV-X8 compares every store's key-value map *and* session table with the model after
  convergence.
- **On real processes.** Every Phase 13 real-process test replays the durable committed logs the
  `dkvd` processes wrote through a fresh `kv.Store` and the model, requiring identical decisions and
  one execution per identity (`dedupEvidence`).
- **Through client histories.** Retries and duplicates are recorded as sends of one logical
  operation and checked for linearizability (LINEARIZABILITY §15).
- **Mutants 61–89** (`scripts/mutation.sh`): each rule above broken on purpose, each killed.

## 8. Cost (measured)

Apple M4, Go 1.27.1, darwin/arm64, `go test ./internal/kv -run '^$' -bench 'Apply|Codec|SessionTable|Forwarding' -benchmem`:

| Operation | Time | Notes |
|---|---|---|
| apply anonymous PUT (100 B) | 69 ns | Phase 12 path, for reference |
| apply identified PUT, executed | 235 ns | + fingerprint (84 ns for 100 B) and the table insert |
| apply identified PUT, duplicate | 153 ns | lookup + fingerprint; no write |
| apply identified PUT, 127 results held, watermark rising | 985 ns | forgetting below the watermark scans the held results: O(MaxUnacked) |
| apply `REGISTER` into a full table (1024) | 10.3 µs | LRU victim search: O(MaxSessions) |
| fingerprint, 64 KiB value | 24 µs (2.7 GB/s) | allocates a copy of the value (encodes, then hashes) |
| request / response codec round trip (100 B) | 101 / 132 ns | |
| full table at default limits | 13.4 MiB | 107 B per result |
| identified PUT end to end, at the leader (3 nodes, TCP loopback, no fsync) | ≈40 µs | anonymous: ≈41–47 µs — identity is in the noise |
| the same, through a forwarding follower | ≈67 µs | forwarding adds ≈26 µs: two internal messages and a goroutine |

End to end, replication dominates by two orders of magnitude; with fsync (the real binary) the
difference shrinks further. The two linear scans (watermark, LRU) are bounded by the limits and
cost at most ~1 µs and ~10 µs; they are the first thing to index if the limits are ever raised by
orders of magnitude.

**Unbounded growth, audited.** Everything Phase 13 added is bounded: the session table by its
limits (§6); a forwarder's table of forwards awaiting an answer by the requests in flight (each
entry is removed when its forward completes or times out); a session client's in-flight set by the
caller's concurrency. The one unbounded structure on the request path is pre-existing: the Raft log
itself, which nothing truncates until Phase 14 — and every request, including a duplicate, a
refusal decided at apply, or a request naming an unknown session, adds an entry to it.

## 9. Mutants

61 dedup lookup skipped · 62 conflict without fingerprint · 63 a refused conflict takes effect ·
64 fingerprint without the value · 65 an evicted session revived · 66 the watermark forgets the
result at it · 67 stale requests executed · 68 unbounded results · 69 LRU evicts the most recent ·
70 a forwarded request forwarded again · 71 an unanswered forward reported OK · 72 the forwarder
strips identity · 73 a new request id per attempt · 74 an unanswered request reported known ·
75 a watermark that passes requests in flight · 76 dkvd ignores the configured limits · 77–81 the
checker's logical merge (identity scope, accepted conflicts, invocation, completion, foreign
commands) · 82 the model without deduplication · 83–86 the same rules killed by client-visible
histories of real processes alone · 87 validation before proposal · 88 a duplicate reports the
original's index · 89 a restart rebuilds the table by replay. `LINEARIZABILITY.md` §15.6 has the
killers.

## 10. Limitations

- **No snapshots (Phase 14).** The table is rebuilt by replaying the whole log. Any snapshot must
  include the session table — a snapshot without it would turn every retry after a restore into a
  second execution. This is a constraint Phase 14 inherits, not something implemented here.
- **Limits are configuration that must agree**, and nothing checks that they do.
- **No authentication.** A client presenting another client's ClientID is that client
  (CLIENT_SEMANTICS §2).
- **No time-based expiry.** An idle session lives until 1024 newer sessions have been used after
  it; a client that registers in a loop evicts everyone else's sessions.
- **One Raft group.** Multi-group routing would need a session per group or a global one; neither
  exists.
- The LRU and watermark scans are linear in the limits (§8).
