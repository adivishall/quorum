# INVARIANTS

Every invariant here is (a) stated precisely enough to be falsifiable, (b) assigned an ID, and
(c) bound to the test that checks it. An invariant with no test is marked `UNVERIFIED` and is
not allowed to be cited as a guarantee anywhere else in the docs.

Status column: `PLANNED` (Phase 0), `VERIFIED` (a test exists and passes), `VIOLATED` (a test
exists and fails — the implementation is broken and the phase is not done).

---

## Storage

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-S1 | After any crash and restart, the recovered state equals the state implied by the durable log prefix. No acknowledged write is missing; no unacknowledged write appears. | Phase 2 crash tests (SIGKILL + replay + compare) | PLANNED |
| INV-S2 | WAL replay is deterministic: replaying the same log bytes any number of times, from any starting point, yields identical state. | Phase 2 repeated-recovery test | PLANNED |
| INV-S3 | A deleted key never reappears — at any level, after any number of compactions, across restarts. (Tombstones are only dropped at the bottom-most level.) | Phase 4 compaction tests | PLANNED |
| INV-S4 | Sequence numbers are assigned deterministically in apply order, so two replicas that applied the same log prefix hold byte-identical logical state. | Phase 12 replica-comparison | PLANNED |
| INV-S5 | A reader holding a version never observes a partially-installed SSTable set. Compaction's version swap is atomic. | Phase 4 concurrent read-during-compaction test (`-race`) | PLANNED |
| INV-S6 | The MANIFEST is the sole authority on which files are live. Files on disk but absent from it are orphans and are deleted; files in it but absent from disk are a fatal error. | Phase 4 crash-during-compaction test | PLANNED |
| INV-S7 | A Bloom filter never returns "absent" for a key that is present (zero false negatives). | Phase 4 bloom test over a large corpus | PLANNED |
| INV-S8 | A CRC failure in the middle of a log aborts startup; a CRC failure in the final record truncates and continues. Never the reverse. | Phase 2 corruption tests | PLANNED |

## Raft

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-R1 | **Election Safety.** At most one leader can be elected in a given term. | Phase 9 deterministic simulation; asserted continuously by the test harness after every step | PLANNED |
| INV-R2 | **Leader Append-Only.** A leader never overwrites or deletes entries in its own log; it only appends. | Phase 9 invariant checker | PLANNED |
| INV-R3 | **Log Matching.** If two logs contain an entry with the same index and term, the logs are identical in all entries up through that index. | Phase 9 invariant checker, run across all nodes after every step | PLANNED |
| INV-R4 | **Leader Completeness.** If an entry is committed in term T, it is present in the log of every leader of every term > T. | Phase 9 simulation with election churn | PLANNED |
| INV-R5 | **State Machine Safety.** If a node has applied an entry at index i, no node ever applies a different entry at index i. | Phase 9/12 cross-node apply-log comparison | PLANNED |
| INV-R6 | `currentTerm` and `votedFor` are durable before any vote is granted or any AppendEntries is acknowledged. | Phase 11 crash-during-vote test | PLANNED |
| INV-R7 | A node never applies an entry with index > `commitIndex`. | Phase 9 assertion in the apply path (enabled in tests) | PLANNED |
| INV-R8 | `commitIndex` is monotonically non-decreasing on every node, across restarts. | Phase 11 restart tests | PLANNED |
| INV-R9 | A leader only advances `commitIndex` past entries from its own term. | Phase 9 figure-8 regression test | PLANNED |
| INV-R10 | A stale message (lower term) never mutates state beyond sending a rejection carrying the current term. | Phase 9 stale/duplicate/delayed-message tests | PLANNED |

## Routing / cluster

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-C1 | Key routing is a pure function of (key, membership configuration). Same inputs → same shard, on every node, forever. | Phase 6 determinism + golden-file test | PLANNED |
| INV-C2 | Every shard has exactly one replica group, and every key maps to exactly one shard. No key is unowned; no key is doubly owned. | Phase 6 exhaustive ring coverage test | PLANNED |
| INV-C3 | A membership change of one node moves only the keys it must (≈ 1/N of the space), not a reshuffle. | Phase 6 redistribution test | PLANNED |
| INV-C4 | A client request for key k is never served by a node that does not host k's shard, except as an explicit forward or redirect. | Phase 7 routing tests | PLANNED |

## Client semantics

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-X1 | A write acknowledged to a client is readable by a subsequent linearizable read from *any* client. | Phase 12 linearizability checker | PLANNED |
| INV-X2 | A duplicate request with the same `(clientID, seqNo)` is applied at most once. | Phase 13 dedup tests | PLANNED |
| INV-X3 | A read in `linearizable` mode never returns a value older than any completed write that preceded the read's invocation in real time. | Phase 12 linearizability checker | PLANNED |
| INV-X4 | A read in `stale` mode is never *presented* as linearizable — the API response carries the mode it was served under. | Phase 15 API test | PLANNED |
