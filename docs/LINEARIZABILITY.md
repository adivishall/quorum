# LINEARIZABILITY — Phase 12

Status: **Phase 12 complete.** This document states exactly what the running system guarantees
to a client, what mechanism provides it, how it is checked, and — just as precisely — what it
does not guarantee and what was not tested. It is the reference the code comments point at
(`docs/LINEARIZABILITY.md §N`). ADR-019 records the decisions; `docs/CONSISTENCY.md` is the
model's summary; `docs/INVARIANTS.md` lists the INV-X rows.

The one-sentence claim, with every qualifier attached:

> For the operation set {PUT, GET, DELETE} on single keys, served by one Raft group through
> `internal/kv` — writes acknowledged only when committed and applied on the serving node in the
> term they were proposed in, reads served only through ReadIndex — every finite client-visible
> history we recorded (from real processes, the real driver in-process, and a deterministic
> simulator, under crashes, restarts, partitions and message faults) is linearizable, as decided
> by a checker that was itself validated against an independent oracle; and the mechanisms that
> make it so are argued in §3 and §5 and pinned by mutants (§11). It is **not** a proof that every
> possible history is linearizable, it assumes the fault model of `docs/FAILURE_MODEL.md`, and it
> holds for honestly recorded histories only — a client that retries an unknown write and reports
> the two attempts as one operation is outside it until Phase 13 (§4.3).

---

## 1. The object and its operations

**Operations.** `PUT(k, v)`, `GET(k)`, `DELETE(k)` on one key each. Keys are non-empty byte
strings (≤ `kv.MaxKeyLen`), values are byte strings (≤ `kv.MaxValueLen`) and may be empty. There
are no scans, no multi-key operations, no transactions, no conditional writes.

**Sequential specification** (the Phase 1 storage contract, INV-A1..A5, restated as the pure
function `lincheck.Step`): each key is a register that is either *absent* or holds a value.
`PUT(k,v)` makes it present with `v` (an empty `v` is present); `DELETE(k)` makes it absent and
reports nothing (deleting an absent key succeeds); `GET(k)` returns the value, or *not found*.
The replicated state machine `kv.Store` implements exactly this; the two implementations are
diffed on every converged simulator run (INV-X8) and against the Phase 2 storage contract
(`TestStoreMatchesTheStorageContract`, `TestStoreMatchesTheReferenceModel`).

**The object is the whole key-value map**, and the claim is linearizability of that whole object
— not a weaker "per-key" property. Because every operation touches exactly one key, the map is
the composition of independent per-key registers, and Herlihy & Wing's locality theorem says a
history of the composition is linearizable **iff** its projection onto every key is. The checker
therefore works one key at a time; that step is exact, not an approximation, and it is tested
empirically: on 20,000 arbitrary two-key histories the whole-history oracle (§6.4), which never
projects, agrees with the conjunction of per-key verdicts on every one. The equivalence would
**not** survive a multi-key operation (a scan, a batch); none exists, and adding one would require
a different checker.

**What "one Raft group" means.** Phase 12 runs a single group (`dkvd -raft`, three processes in
the tests). The routing layer's sharding (Phase 6) is not wired to Raft yet; with it, each key
would still live in exactly one group, and per-group linearizability would compose the same way
(C2 in `docs/CONSISTENCY.md`) — but that composition is not what was tested.

---

## 2. Linearizability, as checked

A **history** is a finite set of operations. Each has an invocation position and — if the client
ever received a response — a completion position, both drawn from **one** logical counter
(`lincheck.Recorder`: one mutex, one increment per event). The recorder takes the invocation
position *before* the first request is sent and the completion position *after* the response is
received, so recorded intervals contain the real ones; "A completed before B was invoked" is
exactly `A.Complete < B.Invoke`. No wall clock is read.

A history is **linearizable** iff, after applying the outcome rules of §4 (remove definite
rejections and unanswered reads; for each unanswered write choose whether it took effect), there
is a total order of the remaining operations that

1. **respects real time** — if A completed before B was invoked, A precedes B;
2. **is legal** for the sequential specification of §1; and
3. **reproduces every observed output** (every value and every *not found* a client saw).

That is Herlihy & Wing (1990). The checker decides it for the finite history it is given — no
more (§6.5).

---

## 3. Write completion — exactly when PUT/DELETE returns success

**The completion point.** `raftnode.Node.Write` (and so `kv.Server.Put/Delete`, and so the wire
`statusOK`) returns success only when **the entry the proposal created has been committed and
then applied to this node's state machine, and the entry applied at that index carries the term
the proposal was made in.** Concretely: the actor proposes (the entry is the log's tail at index
*i* in term *t*), registers a waiter `(i, t)` in `raftnode.Waiters`, and `ApplyCommitted` completes
it after `sm.Apply` and `AppliedTo` — never before the application it reports.

| What the node observes | Client result | Meaning |
|---|---|---|
| entry *(i, t)* applied | `OK` (index *i*, term *t*) | committed ⇒ in every later leader's log ⇒ in every later state |
| a *different* entry applied at *i* (term ≠ *t*) | `ErrLost` → `Rejected` | at most one entry is ever committed at an index (Log Matching + State Machine Safety), so this proposal is **definitely not** committed: no effect |
| not leader when proposing | `NotLeaderError` → `Rejected` | nothing was appended: no effect |
| deadline, stop, fail-stop, dead connection | `ErrUnknown` → `Incomplete` | the entry may still commit: **unknown** |

**Why every successful write has a valid linearization point.** Order all committed entries by
index; linearize each committed write at its index. A write that returned `OK` at *i* was
committed before it returned (applied implies committed), and it was proposed after it was
invoked — so the point "the moment *i* was committed" lies inside its interval. Any write that
completed before another write was invoked has a lower index (it was already in the log when the
later one was appended, on whichever leader appended it, by Leader Completeness), so index order
respects real time among writes. Reads fit into the same order in §5.

**Proven where.** `TestWaitersCompleteWritesOnlyInTheirTerm` (the rule itself);
`TestWriteCompletesOnlyAfterApply`, `TestWriteReportsLostWhenItsEntryIsOverwritten` and
`TestIsolatedLeaderCannotServeAReadOrCompleteAWrite` on the real driver over real TCP
(`internal/raftnode/client_test.go`); INV-X5 and INV-X6 checked at the completion instant of every
simulated write (§8); the write crash windows on real processes (§7.3). Mutants 40 and 41 break
the rule and are killed (§11).

---

## 4. Outcome classes and incomplete operations

Every client-visible outcome falls in exactly one class, and the history records which:

| Class | Outcomes | In the checker |
|---|---|---|
| **definite, effect** | `OK` (write or read), `NotFound` (read) | must be linearized, with the observed output |
| **definite, no effect** | not leader (with or without a hint), `ErrLost`, invalid input, node unreachable before the request was sent (`ErrUnavailable`: connection refused) | excluded — `Rejected` |
| **unknown** | deadline passed, connection died mid-request, node stopped or fail-stopped | `Incomplete` — a write is **optional** (it may have taken effect at any moment after its invocation, or never); a read constrains nothing |

### 4.1 The rule for incomplete operations, and why it is right

"No response" is not "did not happen", and "committed on the server" is not "the client knows".
Phase 12 keeps these as separate facts:

- The **history** records only what the client knows: the operation is `Incomplete`.
- The **system** may well have committed it. `TestRealWriteCrashWindows` proves both on a real
  process: at `after-save:2`, `before-apply`, `after-apply`, `after-applied-to` and `before-reply`
  the leader's durable log (inspected after the SIGKILL) holds the entry committed, the client
  heard nothing, and a later read on the new leader **must** return it — which it does
  (§7.3). `TestKVCrashAtEveryPointOfAWrite` proves the same in the simulator.
- The **checker** treats the unanswered write as optional, which is exactly the definition's
  "extend the history with a response or remove the invocation": it neither assumes the write
  happened nor assumes it did not. An unanswered write may be linearized **late** — after writes
  that completed after its invocation — because in Raft an accepted proposal can sit in a log and
  commit later (`good/incomplete-write-takes-effect-late.hist`); it can never be linearized
  **before its invocation** (`bad/incomplete-write-before-its-invocation.hist`).

A definite rejection is excluded rather than optional because the server asserted "no effect".
If that assertion were false, a later read would expose the value (values are unique per
operation) and the history would fail (`bad/rejected-write-observed.hist`) — so a false
rejection cannot hide.

### 4.2 The client policy (part of what a history means)

`internal/kv/workload` (and its scripted `workload.Client`) is the only client, and its policy is
fixed and pinned by `TestClientPolicy`:

- a not-leader answer is followed to the named leader (or the next node); every attempt is
  recorded with the node contacted and what came back — nothing is hidden;
- an **unknown write is never retried** (it ends `Incomplete`): without deduplication a retry
  under the same operation could apply it twice, and the history would lie;
- an unknown read may be retried (a read has no effect); the operation's interval spans all its
  attempts, and the successful attempt's interval lies inside it, so this is sound;
- values are unique per operation, so a read identifies the write it observed.

### 4.3 Retries, and what Phase 13 changes

A client that times out on `PUT(k,A)` and retries it **as a new operation** is recorded honestly as
two operations (the first `Incomplete`), and such histories are linearizable even when the write
is applied twice. `TestRealIncompleteWriteThenRetry` produces the dangerous interleaving on real
processes: c1's `PUT(A)` commits and applies but the leader is SIGKILLed before replying; a reader
sees A; c2 completes `PUT(B)`; the reader sees B; c1 retries `PUT(A)` — applied again; the reader
sees A. As two operations: linearizable. **Collapsed into one** (what a client library that hid
its retries would report): the checker rejects it — one PUT cannot precede the first read and
follow B (`bad/retry-collapsed-into-one-op.hist` is the same shape).

So in Phase 12 the guarantee holds for histories that record every attempt as an operation, and
**not** for "at-most-once from the caller's point of view". Phase 13 adds request ids and a
deduplication table so a retry is recognized at apply time and answered with the original result
without re-applying; then the collapsed view becomes true (INV-X2), and C1 of
`docs/CONSISTENCY.md` holds under retries. Phase 12 deliberately does **not** add deduplication.

---

## 5. Reads — ReadIndex

### 5.1 The mechanism, exactly as implemented

In the pure core (`internal/raft`, DESIGN §8.5, ADR-019):

1. **Registration** (`Raft.ReadIndex`). Only a node whose role is `Leader` in its current term *T*
   accepts a read; any other node returns `ErrNotLeader` and the client is redirected — **a
   follower never serves a read**. The read index is
   `ri = max(commitIndex, termStart)`, where `termStart` is the index of this leader's own
   election no-op.
2. **Confirmation.** Every AppendEntries a leader sends carries a heartbeat sequence
   `Message.Seq`; `broadcastAppend` increments it first. Every AppendEntries response in the same
   term — success or rejection — echoes the `Seq` of the request it answers. The read is pending
   with `seq = hbSeq+1`, and registration immediately broadcasts, so that broadcast carries exactly
   `hbSeq+1`. The read is confirmed when a quorum — the leader plus followers whose highest echoed
   sequence `ackSeq[peer] ≥ seq` — has answered a request **sent after the read was registered**.
   A single-node group is its own quorum and confirms at once. Confirmation is FIFO (sequences are
   non-decreasing), and confirmed reads leave the core as `Ready.ReadStates`.
3. **Leadership change.** Stepping down (`becomeFollower` from leader), campaigning
   (`becomeCandidate`) and winning (`becomeLeader`, which also resets `ackSeq`) all drop every
   unconfirmed read. The driver (`raftnode.Reads.DropStale`, after every cycle) fails every
   unconfirmed read registered in a term the node no longer leads with `ErrNotLeader` — a
   definite no-effect; the client may retry elsewhere.
4. **Hand-off and completion** (driver). `DrainReadyAt` hands each confirmed `ReadState` to
   `Reads.Confirmed` after the Ready's Save and before its messages; the read then waits in
   `Waiters` as a barrier until the state machine's applied index reaches `ri` (immediately if it
   already has). `kv.Server.Get` then reads the node's `kv.Store` and answers. The value returned
   is the store's state at some applied index `a ≥ ri` (the store is mutex-guarded, so the read is
   atomic with respect to `Apply`).

### 5.2 Safety argument (why a served read is linearizable)

Let a read *R* be registered at real time *t₀* on node *L* as leader of term *T*, confirmed, and
answered from state at applied index *a ≥ ri*. Linearize *R* immediately after entry *a* in the
index order of §3.

**(a) Every write that completed before *R* was invoked is at an index ≤ *ri*.** Such a write *W*
at index *i* was committed before *t₀* by the leader of some term *T′* (the first leader to count
*i* committed).
- If *T′ > T*: a quorum *Q′* had stored an entry of term *T′*, and so had durably adopted term
  ≥ *T′*, before *t₀*. *R*'s confirmation needs a quorum *Q* that answered, **in term *T***, an
  AppendEntries *L* sent **after *t₀*** (only acknowledgements with `Seq ≥ hbSeq+1` count, and
  every such request was sent after registration). *Q* ∩ *Q′* ≠ ∅, and a node's term never
  decreases — it is fsynced before any reply that depends on it (INV-R6, INV-CR1) — so that node
  cannot answer in term *T < T′* after *t₀*. Contradiction: *R* would never be confirmed.
- If *T′ < T*: by Leader Completeness *i* is in *L*'s log at its election, so
  *i* < `termStart` ≤ *ri*.
- If *T′ = T*: *L* itself committed *i* before *t₀*, and its commit index is monotonic, so
  *i* ≤ `commitIndex` at *t₀* ≤ *ri*.

**(b) Everything *R* observes is committed and was invoked before *R* completed.** *R* waits for
`applied ≥ ri` and the store only applies committed entries (INV-R7), so the state at *a* is a
prefix of the one committed log. Every entry ≤ *a* existed in a log before *R* returned, so its
write was invoked before *R* completed.

**(c) Reads are ordered among themselves.** If *R₁* completed before *R₂* was invoked, *R₁*'s
index *a₁* was committed before *R₂* registered, so by (a) *a₁* ≤ *ri₂* ≤ *a₂*.

(a)–(c) are the conditions for inserting reads into the index order of writes without violating
real time; reads at the same index are ordered by real time. ∎

**What the argument needs, stated as assumptions:** non-Byzantine nodes; a node's term and vote
durable before any dependent reply (verified: INV-R6, INV-CR1, the Phase 11 record order); a
unique leader per term (INV-R1); Leader Completeness (INV-R4); the store applying only committed
entries in order (INV-R7, INV-P9). **No clock assumption** — leases are not used (§8.5 of DESIGN).

### 5.3 The attacks, and why each fails

| Attack | Why it cannot produce a stale read | Evidence |
|---|---|---|
| Follower serves a local read | `ReadIndex` requires `role == Leader` | `TestReadIndexRequiresLeader`; `TestFollowerLocalReadIsCaughtAsNonLinearizable` (the bypass is caught); `TestRealCompletedWriteIsSeenByEveryLaterRead` (every follower refuses and redirects); mutant 34 |
| Deposed leader that still believes it leads | it can never gather a quorum of term-*T* answers to a request sent after the read: the majority has a higher term | `TestIsolatedLeaderNeverConfirmsARead`; `TestKVStaleLeaderReadIsNeverServed`; `TestRealStaleLeaderNeverServesARead` (real processes: the read times out, never returns); mutants 37, 43, 59 |
| …**and still has a follower** acknowledging its post-read heartbeats in its own term (a 2-of-5 minority) | fresh, same-term acknowledgements from a live follower are still fewer than a quorum | `TestKVMinorityLeaderWithAFollowerNeverServesARead`; `TestRealMinorityLeaderWithAFollowerNeverServesARead` (five real processes split 2 \| 3); the `kv-splits` seeded profile; mutants 37, 54, 55 — without the quorum rule the real minority leader **does** return a stale value |
| Acknowledgements of an **earlier** heartbeat, delayed, arriving after the read | they carry a `Seq` below the read's; they prove leadership only up to when they were sent | `TestAcksFromBeforeTheReadDoNotConfirmIt`; `TestKVStaleLeaderReadIsNeverServed` delivers exactly those acks to the stale leader; mutant 36 |
| Responses from an earlier **term** | dropped (`m.Term < currentTerm`); `ackSeq` is reset on becoming leader; a node leads a term at most once, in one incarnation | `TestStaleAppendResponseIgnored`; INV-R10 |
| New leader whose commit index lags its predecessor's | `ri ≥ termStart`: the read waits for the no-op, which commits every earlier entry | `TestNewLeaderReadIndexIsAtLeastItsNoop`; `TestKVNewLeaderReadWaitsForItsNoop` (confirmation by a *rejection* ack while commit is stale); mutant 35 |
| Leadership lost between registration and confirmation | the core drops pending reads on every role change; the driver fails them `ErrNotLeader` | `TestPendingReadsAreDroppedOnStepDown`, `TestReadsConfirmAndDropStale`; mutants 38, 39 |
| Serving a confirmed read before the state reaches `ri` | the barrier waits for `applied ≥ ri` | `TestReadsConfirmAndDropStale`; INV-X7; mutant 42 |
| A late response on a reused connection answering a *newer* request | the client abandons any connection whose request failed | `TestWireClientNeverMatchesALateResponseToANewRequest`; mutant 47 |

A read confirmed **before** its node steps down is still served after it: confirmation proved
leadership at registration, which is all (a) needs, and (b) holds for any applied index.

### 5.4 Liveness (not safety) caveats

A leader cut off from its peers keeps believing it leads (there is no check-quorum step-down);
reads registered on it wait until the client's deadline and end `Incomplete`, and its pending
reads and write waiters accumulate until it learns a higher term. Reads cost one quorum round
trip each; there is no batching beyond FIFO confirmation. Neither affects safety.

---

## 6. Histories and the checker

### 6.1 Representation

`lincheck.Op`: id (the request's identity in the history — there is no protocol request id until
Phase 13), client, kind, key, value written, invocation and completion positions, outcome
(`OK`, `NotFound`, `Rejected`, `Incomplete`), output, the node/term/index of the completing
response, and **every attempt** (node contacted, its own interval, what came back). The text form
(`Op.String`, `ParseHistory`) round-trips byte-exactly and ignores `#` comment lines, so a saved
artifact carries its seed, options and fault events as a header.

### 6.2 Algorithm

Per key (locality, §1): drop `Rejected` operations and `Incomplete` reads; sort by invocation;
depth-first search over "which operation is linearized next". An operation may go next only if it
was invoked before the earliest completion among the operations not yet linearized (so nothing
that completed before it was invoked is left behind); `Incomplete` writes have infinite completion
(they never block others) and are optional (the search succeeds once every *required* operation is
placed). Failed `(linearized set, register state)` pairs are memoized — the register's state is
absent-or-one-value, which is what makes the memo effective (Wing & Gong; Lowe; as in Porcupine).
A per-key state budget (default 2,000,000) turns an exhausted search into **UNCHECKED** — never a
verdict.

On failure the checker reports the deepest linearization prefix and why every remaining
candidate fails there, and minimizes the counterexample (delta debugging, anchored so it never
invents a different violation: the culprit stays, every observed value keeps a writer, and
removing the culprit alone makes the rest linearizable).

### 6.3 Complexity and measured cost

Worst case exponential in the number of mutually concurrent operations on one key; the memo bounds
it by (subsets of concurrent operations) × (register states). Measured on an Apple-silicon laptop
(`LINCHECK_COST_FULL=1 go test ./internal/lincheck -run TestCheckerCostEnvelope -v`; the default
run omits the 14- and 16-write rows):

| History (one key unless noted) | Ops | Verdict | States | Time | Allocated | Minimize |
|---|---|---|---|---|---|---|
| linearizable by construction, 8 clients, 10% unanswered | 200 | linearizable | 205 | 0.6 ms | 0.2 MiB | – |
| same | 1,000 | linearizable | 981 | 4.4 ms | 1.7 MiB | – |
| same | 5,000 | linearizable | 5,894 | 50 ms | 20 MiB | – |
| same + one injected stale read (search must exhaust) | 100 | violation → 3 ops | 1,407 | 0.6 ms | 0.5 MiB | 1.8 ms |
| same | 600 | violation → 3 ops | 3,371 | 2.7 ms | 2.7 MiB | 7.6 ms |
| same | 2,000 | violation → 3 ops | 43 | 1.1 ms | 5.4 MiB | 11 ms |
| 8 mutually concurrent writes + 8 reads pinning one order | 16 | linearizable | 2,090 | 0.4 ms | 0.3 MiB | – |
| same, reads needing one write twice (must exhaust) | 16 | violation → 5 ops | 6,662 | 1.9 ms | 1.5 MiB | 7.7 ms |
| 12 mutually concurrent writes | 24 | linearizable | 94,296 | 12 ms | 7.9 MiB | – |
| same, impossible | 24 | violation → 5 ops | 253,962 | 57 ms | 35 MiB | 228 ms |
| 14 mutually concurrent writes | 28 | linearizable | 548,981 | 64 ms | 38 MiB | – |
| same, impossible | 28 | violation → 5 ops | 1,409,036 | 320 ms | 181 MiB | 1.4 s |
| 16 mutually concurrent writes (either kind) | 32 | **UNCHECKED** (budget of 2,000,000 states) | 2,000,000 | 230 ms | 125 MiB | – |

So: **practical length** is bounded by concurrency, not length — linearizable histories of thousands
of operations with ≤ 8 concurrent clients per key check in milliseconds, roughly linearly.
**Worst-case backtracking** is exponential in the number of *mutually concurrent writes on one
key*: the memo is bounded by (subsets of concurrent operations) × (register states), ~2ⁿ·n; at 14
the exhaustive case costs 1.4 M states and ~180 MiB; at 16 the default budget is reached and the
answer is UNCHECKED — the budget is what keeps a pathological history from exhausting memory, and
it never turns into a verdict. **Memory** is dominated by the memo (≈ 90–130 bytes per state).
**Minimization** re-runs the search per candidate deletion, so it costs a few times the failing
check (seconds at the adversarial extreme). The system's own histories never approach the limit:
with at most 8 clients, at most 8 operations can be concurrent on a key.

The real-process histories (≤ ~500 operations per run over ≤ 3 keys) check in milliseconds; the
largest simulator histories (~400 operations) in under 1 ms. The budget has never been reached by
a recorded history.

### 6.4 How the checker itself was validated

A broken checker is worse than none, so it is attacked before it is trusted:

- **Independent oracle** (`oracle_test.go`). Written from the definition, sharing no code with the
  checker: its own register semantics, its own outcome rules, the real-time rule in its *backward*
  pairwise form ("nothing placed later completed before something placed earlier was invoked")
  instead of the checker's forward form, naive enumeration, and the **whole multi-key history**
  (no locality). On **20,000 arbitrary histories** — random intervals, 1–7 operations, 1–2 keys,
  values from {"", a, b} so empty and repeated values occur, every outcome class — the checker,
  the oracle and the brute-force enumerator agree on every one: **11,814 linearizable, 8,186 not**,
  and every feature (overlap, incomplete writes, incomplete reads, rejections, deletes, empty
  values, repeated values, concurrent same-key operations, two keys) occurs in ≥ 500 histories.
- **Fuzzing** (`FuzzCheckerMatchesOracle`): the same comparison on fuzzer-steered histories
  (6.2 million executions in a 45 s run; no disagreement), part of `make fuzz`.
- **Brute force** (`TestCheckerAgreesWithBruteForce`): thousands of concurrent histories that are
  linearizable by construction are accepted; perturbed ones are decided like exhaustive
  enumeration.
- **Corpus** (`testdata/corpus/{good,bad}/*.hist`, 17 + 18 commented files): every known-good
  history accepted and every known-bad rejected — by the checker and by the oracle — and every
  minimized counterexample is itself rejected by the oracle. Known-bad includes: stale read after a
  completed write; a later read returning an older value; a completed write disappearing (also
  across a leader change); real-time inversion (a read of a write invoked after it completed); an
  unanswered write taking effect before its invocation; a read of a deleted value; the stale-leader
  read; two observers disagreeing on the order of two writes; two completed writes both "last"; an
  impossible concurrent history; an observed "definitely rejected" write; a value never written;
  the empty value read as absent and absence read as the empty value; a violation on one key among
  many; the collapsed retry.
- **Mutants of the checker** (§11, 49–53): ignoring real time, discarding unanswered writes,
  forcing rejections into the order, memoizing without the register state, and confusing absent
  with empty are each killed by the corpus or the oracle.

### 6.5 What a verdict means

"Linearizable" is a statement about the finite history checked, under the outcome rules of §4. It
says nothing about histories that were not produced. "Not linearizable" is a proof (the
counterexample). "Unchecked" is neither.

---

## 7. Real-process tier (`tests/integration/kv_linearizability_test.go`)

Real `dkvd -raft` processes (durable: fsync on), three per group, node-to-node TCP through
test-owned proxies (for partitions), clients over the real wire protocol on each node's
`-client-listen` port. The checker consumes the recorded client history only.

### 7.1 Scenarios

| Scenario | Test |
|---|---|
| sequential baseline, diffed op by op against the reference model | `TestRealSequentialBaselineMatchesTheModel` |
| 2, 4, 8 concurrent clients, shared keys | `TestRealConcurrentClientsAreLinearizable` |
| concurrent same-key writes, write/read overlap, delete racing put (8 clients, 1 key) | `TestRealSameKeyWritesReadsAndDeletes` |
| completed write → every later read, sent first to each node (followers refuse and redirect) | `TestRealCompletedWriteIsSeenByEveryLaterRead` |
| stale leader: cut off, still believes it leads; its read never returns; after heal it refuses | `TestRealStaleLeaderNeverServesARead` |
| minority leader **with a follower** (5 processes split 2 \| 3): acknowledged, yet never serves | `TestRealMinorityLeaderWithAFollowerNeverServesARead` |
| GET / leader crash / GET and GET / leader partition / GET, twice each | `TestRealReadsAcrossLeaderChanges` |
| leader SIGKILL + restart during a workload | `TestRealLeaderKilledDuringWorkload` |
| follower SIGKILL + restart during a workload (served on a quorum of two; restarted follower refuses reads) | `TestRealFollowerKilledDuringWorkload` |
| leader partition + heal during a workload | `TestRealLeaderPartitionedDuringWorkload` |
| repeated leader changes without crashes (isolate until the others elect, heal) | `TestRealLeaderChangesWithoutCrashes` |
| rolling SIGKILL + restart of every process | `TestRealRollingRestartDuringWorkload` |
| a write to an isolated leader never takes effect | `TestRealWriteToPartitionedLeaderNeverTakesEffect` |
| committed-but-unacknowledged write, then a retry (§4.3) | `TestRealIncompleteWriteThenRetry` |
| SIGKILL at every point of one write's life (§7.3) | `TestRealWriteCrashWindows` |
| malformed and abandoned requests amid a workload | `TestRealMalformedAndAbandonedRequestsDoNotCorruptTheHistory` |

### 7.2 Synchronization and reproducibility

No fault is injected after a guessed sleep. Every step waits for an observable condition: a leader
confirmed in the nodes' own output (and followed by a peer), a client port reported ready, *N*
more operations **served** (OK/NotFound — refusals are not progress), a crash point logged, a
follower reporting the new leader. Timing is real, so a run cannot be replayed from a seed — its
**evidence** can: each run records the workload seed and options (each client's operation
sequence is a function of them), every fault and node event at a position in the history's own
clock, and the history. A failure writes those and every node's output to a directory it names;
`go run ./cmd/lincheck DIR/history.txt` re-checks, minimizes and explains the saved history
offline. (Exact replay of a *schedule* is the simulator's job, §8.)

### 7.3 The write crash windows

`dkvd -crash-at P -crash-armed-by-signal` makes a process SIGKILL itself at point *P*, counting
occurrences only after the test sends SIGUSR1 (`event=crash_armed`). The test arms the current
leader of a quiesced group and issues one `PUT(k,new)` there (one attempt, no redirect). The
premise of each row is **proved from the victim's durable log** after the kill, not assumed.

| Point (1st after arming) | What the process was doing | Client heard | Entry on the victim's disk | Later read on the new leader |
|---|---|---|---|---|
| `before-save` | before persisting the proposal | nothing | absent | `old` — it never left the leader |
| `after-save:1` | persisted, nothing sent | nothing | present, uncommitted | either (observed: `old`, overwritten by the new leader's no-op) |
| `after-save:2` | commit persisted, not yet sent or applied | nothing | present, **committed** | `new` — required |
| `before-apply` | committed, not applied | nothing | committed | `new` — required |
| `after-apply` | applied, not recorded | nothing | committed | `new` — required |
| `after-applied-to` | applied and recorded, before the waiter fired | nothing | committed | `new` — required |
| `before-reply` | about to write the response | nothing | committed | `new` — required |
| `after-reply` | response handed to the kernel | `OK` (observed) or nothing | committed | `new` — required |

In every row the history (setup put, the crashed put, the later read) is also checked. The
simulator repeats the driver points deterministically (`TestKVCrashAtEveryPointOfAWrite`).

---

## 8. Deterministic simulator tier (`internal/raftsim`, `kv.go`)

Every simulated node's state machine is a `kv.Store`; `kvput`/`kvget`/`kvdel`/`kvtimeout` are
script events; completion goes through the driver's own `raftnode.Waiters` and `raftnode.Reads`
and the real `DrainReadyAt`/`ApplyCommitted`. Clients target (mostly) any node that believes it
leads — **including deposed leaders** — and otherwise any node, including followers, paused and
dead nodes. A run is a pure function of (config, script): the same seed gives a byte-identical
history and trace; a saved script replays exactly (`-raftsim.replay`); a non-linearizable run
minimizes to a short script (`MinimizeFunc` with "the history is not linearizable" as the
predicate).

Checked at the instant each operation completes, **independently of the history checker**:

- **INV-X5** an acknowledged write's entry (index, proposal term, bytes) is the committed one;
- **INV-X6** a write reported lost is not committed at its index;
- **INV-X7** a read is served at a read index — and an applied index — at least the highest index
  committed **anywhere** in the cluster when it was registered (ReadIndex freshness as an
  implementation invariant);
- **INV-X8** after convergence, every node's store equals the `lincheck` register model folded
  over the committed log.

`KVProfiles` — `kv-steady`, `kv-partitions`, `kv-splits` (two-sided splits of five nodes, so a
minority keeps a leader *and* a follower), `kv-crashes`, `kv-messages`, `kv-crashpoints`,
`kv-mixed` — at 200 seeds each (`make faults FAULT_SEEDS=200`): every Raft invariant, INV-X5..X8,
and a linearizable, non-vacuous history on all 1,400 runs. Exact scripted attacks:
`TestKVStaleLeaderReadIsNeverServed`, `TestKVMinorityLeaderWithAFollowerNeverServesARead`,
`TestKVNewLeaderReadWaitsForItsNoop`, `TestKVWriteIsNotAcknowledgedBeforeCommit`,
`TestKVCrashAtEveryPointOfAWrite`, `TestKVHistoryOfAScriptIsReplayable`.

**What the seeded search does and does not find.** Random schedules are a net, not a proof. Measured
against mutants: the seeded runs alone kill "writes acknowledged at append" (mutant 58) and "DELETE
does not delete" (via INV-X8) at the default seed count; they did **not** kill "a read confirmed
without a quorum" until the `kv-splits` profile existed — a fully isolated old leader receives no
acknowledgements, so even a broken quorum rule never fires — and with it they catch that mutant in
about 9% of runs (by INV-X7). Mutation testing found this gap; the scripted minority-leader attacks,
in the simulator and on five real processes, are the deterministic killers.

**The tier has teeth.** `TestKVTierCatchesStaleLocalReads` switches reads to a deliberately broken
local path (no ReadIndex): 12 of 12 seeded runs are rejected, and the first minimizes from 1,500
events to a 26-event script (a leader commits a put with two followers; the third never hears of
it; a read served locally on the third returns *not found*).

---

## 9. In-process driver tier (`internal/kv`, `internal/raftnode`)

Real `raftnode` actors over real TCP (optionally through `fault.Network`), `kv.Server` per node:
the sequential baseline; 1/2/4/8 clients; a hot key; held-and-reversed, dropped and duplicated
Raft traffic; leader stop and restart with a fresh store (whose contents then equal the leader's);
leader partition and heal; the wire protocol end to end. Fault schedules wait for the fault to
engage (the network's own counters) and for served operations, never for a duration.

---

## 10. Six things that are easy to conflate

| Layer | Property | Where established |
|---|---|---|
| **Raft safety** | one leader per term, log matching, leader completeness, state-machine safety, current-term commit | Phases 9–11: INV-R1..R10, INV-F*, INV-CR* |
| **ReadIndex freshness** | a served read reflects every entry committed anywhere when it was registered | §5.2 argument; INV-X7 in the simulator; mutants 34–39, 42 |
| **State-machine semantics** | the store implements the register specification (empty ≠ absent; idempotent delete) | `kv` store tests; INV-X8; mutants 45, 46 |
| **Client completion** | OK only when committed and applied in the proposal's term; lost is definite | §3; INV-X5, X6; mutants 40, 41 |
| **Incomplete operations** | unknown is recorded as unknown, never as success or failure | §4; `TestClientPolicy`; mutant 48 |
| **Finite-history checking** | a recorded history is (or is not) linearizable | §6; the oracle; mutants 49–53 |

Raft safety alone does not give linearizable reads (a leader's local read is Raft-safe and stale);
a correct read path does not help if completion is reported early; and a correct system can be
misjudged by a broken checker. Each layer is argued and tested on its own.

---

## 11. Mutation testing

`make mutation` applies each edit, runs its killers, requires them to fail, and reverts. Phase 12
adds:

| # | Mutant | Killed by |
|---|---|---|
| 34 | a follower registers a ReadIndex | `TestReadIndexRequiresLeader`, `TestKVHistoryOfAScriptIsReplayable` |
| 35 | read index = commit index only (no no-op rule) | `TestNewLeaderReadIndexIsAtLeastItsNoop`, `TestKVNewLeaderReadWaitsForItsNoop` |
| 36 | acks of a heartbeat sent before the read confirm it (stale ReadIndex response) | `TestAcksFromBeforeTheReadDoNotConfirmIt`, `TestKVStaleLeaderReadIsNeverServed` |
| 37 | a read confirmed without a quorum | `TestReadIndexIsConfirmedByAQuorumRound`, `TestIsolatedLeaderNeverConfirmsARead`, `TestKVStaleLeaderReadIsNeverServed` |
| 38 | unconfirmed reads survive step-down in the core | `TestPendingReadsAreDroppedOnStepDown` |
| 39 | the driver ignores a leadership change during a read | `TestReadsConfirmAndDropStale`, `TestKVStaleLeaderReadIsNeverServed` |
| 40 | a PUT completes at append, before a quorum commits it | `TestWaitersCompleteWritesOnlyInTheirTerm`, `TestKVWriteIsNotAcknowledgedBeforeCommit` |
| 41 | a write whose index went to a different entry reports success | `TestWaitersCompleteWritesOnlyInTheirTerm`, `TestKVWriteIsNotAcknowledgedBeforeCommit` |
| 42 | a confirmed read is served before apply reaches its read index (pre-commit/pre-apply state) | `TestReadsConfirmAndDropStale`, `TestKVNewLeaderReadWaitsForItsNoop` |
| 43 | `Server.Get` bypasses ReadIndex (local read) | `TestFollowerLocalReadIsCaughtAsNonLinearizable`, `TestRealStaleLeaderNeverServesARead` |
| 44 | dkvd serves clients from a store that is not the replicated state machine (committed state lost from the client-visible view) | `TestRealSequentialBaselineMatchesTheModel` |
| 45 | DELETE does not remove the key | store contract/model tests, `TestKVSeededHistoriesAreLinearizable` |
| 46 | an empty value is stored as absence | store contract/model tests, `TestWireClientAgainstRealNodes` |
| 47 | the client reuses a connection whose request timed out | `TestWireClientNeverMatchesALateResponseToANewRequest` |
| 48 | the test client retries an unknown write under one operation | `TestClientPolicy` |
| 49 | the checker ignores real-time order (linearizes a read before a completed write) | corpus, oracle |
| 50 | the checker discards unanswered writes | corpus, oracle |
| 51 | the checker forces definite rejections into the order | corpus, oracle |
| 52 | the checker memoizes without the register state | oracle |
| 53 | the model reads an absent key as a present empty value | corpus, oracle |
| 54 | (37 again) no-quorum ReadIndex — killed by **real processes alone** | `TestRealMinorityLeaderWithAFollowerNeverServesARead` |
| 55 | (37 again) — killed by a **simulated history alone** | `TestKVMinorityLeaderWithAFollowerNeverServesARead` |
| 56 | (36 again) — simulated history alone | `TestKVStaleLeaderReadIsNeverServed` |
| 57 | (35 again) — simulated history alone | `TestKVNewLeaderReadWaitsForItsNoop` |
| 58 | (40 again) — the **seeded** workloads alone | `TestKVSeededHistoriesAreLinearizable` |
| 59 | (43 again) — real processes alone | `TestRealStaleLeaderNeverServesARead` |

Mutants 34–53 list unit killers next to history-level ones, so their kill alone would not show the
history-level tests have teeth; 54–59 prove it by using only a recorded-history test. Writing them
exposed the one gap described in §8 (the seeded and real-process stale-leader tests did not kill
mutant 37 until the minority-leader attack existed).

MUTATION_RESULT

---

## 12. Guarantees, assumptions, untested cases

**Verified on finite histories** (every recorded history checked, none failed): real processes
under the scenarios of §7.1; the real driver in-process under §9's faults; 1,400 seeded simulator
runs (7 profiles × 200 seeds) under every fault family, plus the scripted attacks.

**Bounded-model results:** the checker agrees with an independent oracle on 20,000 arbitrary
histories of ≤ 7 operations over ≤ 2 keys and on 6.2 M fuzzed ones; the simulator explores
schedules (message order, loss, duplication, delay, partitions, crashes at every driver and I/O
point, power loss) that real timing rarely produces.

**Implementation invariants** checked at every completion in the simulator: INV-X5..X8.

**Argued, with the argument's assumptions named:** §3 (writes) and §5.2 (reads).

**Assumptions:** the fault model of `docs/FAILURE_MODEL.md` — crash-stop/crash-recover,
non-Byzantine, omission/duplication/reordering/delay of messages, fsync means durable. No clock
assumption for safety.

**Not tested / not claimed:**
- histories longer than those recorded (hours-long runs, millions of operations);
- more than one Raft group, or keys routed across groups (Phase 6 routing is not wired to Raft);
- real power loss on real hardware (the simulator's power-loss model only);
- Byzantine faults, clock-based anything (none is used);
- exactly-once / at-most-once semantics under hidden retries (§4.3, Phase 13);
- snapshots and membership changes (Phase 14+) — the argument assumes fixed membership;
- the Phase 15 API and stale-mode reads (none exist yet).

---

## 13. Reproducing

```
go test ./internal/lincheck/ -run 'Oracle|Corpus|BruteForce' -v        # checker validation
go test ./internal/raftsim -run TestKV -raftsim.seeds=200              # simulator tier
go test ./internal/raftsim -run 'TestKVSeededHistoriesAreLinearizable/kv-mixed/seed=17$' -raftsim.profile=kv-mixed -raftsim.seed=17 -v
go test -race -count=1 -run 'TestReal' -v ./tests/integration/          # real processes
go run ./cmd/lincheck internal/lincheck/testdata/corpus/bad/stale-leader-read.hist
make mutation
```
