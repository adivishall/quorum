# INVARIANTS

Every invariant here is (a) stated precisely enough to be falsifiable, (b) assigned an ID, and
(c) bound to the test that checks it. An invariant with no test is marked `UNVERIFIED` and is
not allowed to be cited as a guarantee anywhere else in the docs.

Status column: `PLANNED` (no test yet), `VERIFIED` (a test exists and passes), `VIOLATED` (a
test exists and fails — the implementation is broken and the phase is not done).

The list grows as subsystems land. A phase that establishes a new guarantee adds its
invariants here rather than asserting the guarantee in prose somewhere else.

---

## API semantics (Phase 1)

These constrain the `storage.Store` contract itself, independently of which implementation
backs it. They are enforced by `internal/storage/store_conformance_test.go`, a suite
parameterised over a `Store` constructor — so the Phase 3 LSM engine inherits every one of
them and cannot quietly change client-visible behaviour.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-A1 | No aliasing in either direction: `Put` copies the caller's key and value, and `Get` returns a fresh copy. A caller cannot mutate stored data except through `Put`/`Delete`, and cannot corrupt another reader's result. | `PutCopiesInput`, `GetReturnsCopy`, `TestConcurrentGetsReturnIndependentCopies` | VERIFIED |
| INV-A2 | `Delete` is idempotent and never reports whether the key existed. | `DeleteMissingKeyIsIdempotent`, `DeleteIsRepeatable` | VERIFIED |
| INV-A3 | Each operation is atomic with respect to every other operation on the same store: no `Get` ever observes a partially-applied `Put`. | `TestConcurrentWritersSameKey` (torn-value detector), `TestConcurrentReadersDuringWrites` | VERIFIED |
| INV-A4 | An empty value is a *present* key. It is distinguishable from an absent key by the error, never by the nil-ness of the returned slice — `Get` never returns `(nil, nil)`. | `EmptyValueIsStorable` | VERIFIED |
| INV-A5 | Keys are opaque byte strings compared bytewise: no normalisation, case-sensitive, and whitespace / newlines / NULs / invalid UTF-8 are all valid and round-trip exactly. | `KeysAreOpaqueBytes`, `KeysAreCaseSensitive`, `FuzzPutGetRoundTrip` | VERIFIED |
| INV-A6 | Validation is uniform across operations: a key rejected by `Put` is rejected identically by `Get` and `Delete`, so an oversized key is an error rather than a silent no-op. | `KeySizeLimit` | VERIFIED |
| INV-A7 | Every failure is an `*OpError` wrapping exactly one sentinel, reachable by `errors.Is`, naming the operation and a safely-escaped key. The CLI's error-to-exit-code mapping is total. | `ErrorsCarryOperationAndKey`, `TestCLIExitCodes`, `TestCLIClosedStoreIsInternalError` | VERIFIED |
| INV-A8 | A rejected operation has no effect: a `Put` that fails validation or a cancelled context leaves the store unchanged. | `ValueSizeLimit`, `CancelledContextRejected` | VERIFIED |
| INV-A9 | A closed store is permanently closed: every operation returns `ErrClosed`, `Close` is idempotent, and closing concurrently with in-flight operations neither panics nor races. | `ClosedStoreRejectsOperations`, `TestConcurrentCloseDuringOperations`, `TestConcurrentCloseIsIdempotent` | VERIFIED |

Note on INV-A3: under the current `sync.RWMutex` a torn value is impossible by construction.
The test is not redundant — it exists so that a later sharded or lock-free memtable cannot
break the guarantee silently. It was confirmed to have teeth by mutation testing (removing
the write lock makes the race detector fire).

---

## Storage

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-S1 | After any crash and restart, the recovered state equals the state produced by applying some **prefix** of the submitted write sequence. Every acknowledged write is inside that prefix, and the prefix has no holes. (Corrected in Phase 2 — see the note below.) | `TestCrashRecoveryAcknowledgedWritesSurvive`, `TestCrashRecoveryMidWriteStormYieldsAPrefix`, `TestReplayReconstructsIdenticalState` | VERIFIED (process kill only) |
| INV-S2 | WAL replay is deterministic: replaying the same log bytes any number of times yields identical state, and replay never alters the log. | `TestReplayIsDeterministic`, `TestRecoveryIsIdempotent` | VERIFIED |
| INV-S3 | A deleted key never reappears — at any level, after any number of compactions, across restarts. (Tombstones are only dropped at the bottom-most level.) | Phase 4 compaction tests | PLANNED |
| INV-S4 | Sequence numbers are assigned deterministically in apply order, so two replicas that applied the same log prefix hold byte-identical logical state. | Phase 12 replica-comparison | PLANNED |
| INV-S5 | A reader holding a version never observes a partially-installed SSTable set. Compaction's version swap is atomic. | Phase 4 concurrent read-during-compaction test (`-race`) | PLANNED |
| INV-S6 | The MANIFEST is the sole authority on which files are live. Files on disk but absent from it are orphans and are deleted; files in it but absent from disk are a fatal error. | Phase 4 crash-during-compaction test | PLANNED |
| INV-S7 | A Bloom filter never returns "absent" for a key that is present (zero false negatives). | Phase 4 bloom test over a large corpus | PLANNED |
| INV-S8 | Damage that a crash cannot explain aborts startup; a torn tail in the newest segment truncates and continues. Never the reverse. A refused open modifies nothing. | `TestBadChecksumInMiddleRecordIsRefused`, `TestCorruptionInAnOlderSegmentIsRefused`, `TestBadChecksumInFinalRecordIsRepaired`, `TestTruncatedFinalPayloadIsRepaired` | VERIFIED |

### Note on INV-S1 — a Phase 0 invariant that was wrong

As written in Phase 0, INV-S1 said "no unacknowledged write appears". **That is not
achievable by any write-ahead log**, and the error was mine in Phase 0 rather than a defect
found in Phase 2.

A crash between the log append and the acknowledgement leaves a record that is durable but
was never acknowledged, and replay will apply it. There is no way to avoid this without
making the log write and the client response atomic, which is impossible across a network or
a process boundary. It is the same phenomenon as `docs/CONSISTENCY.md` C4: a client that
times out genuinely cannot know whether its write took effect.

The invariant has been restated as what is both achievable and actually useful: the recovered
state is a **prefix** of the submitted sequence, every acknowledged write is inside it, and
there are no holes. "No holes" is the load-bearing part — a hole would mean replay skipped a
record, which is exactly the failure the corruption policy exists to prevent.

The VERIFIED marker carries the qualifier **(process kill only)**. The crash tests destroy a
real process with SIGKILL. They do not test OS crash or power loss, and INV-S1 is therefore
not established against those failures in any sync mode.

## Write-ahead log (Phase 2)

Enforced by `internal/record`, `internal/storage/wal` and `internal/storage/walstore.go`.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-W1 | The order mutations reach the log is exactly the order they reach memory, so the state after a restart equals the state before it. | `TestWriteOrderMatchesLogOrder` | VERIFIED |
| INV-W2 | A batch is atomic with respect to a crash: it is one checksummed record, so either every operation in it replays or none does. | `TestBatchRoundTrip`, `TestMultiOperationBatch`, `TestTruncatedFinalPayloadIsRepaired` | VERIFIED |
| INV-W3 | A mutation that the log rejected is never published in memory. In-memory state is always a subset of what the log contains. | `TestFailedWriteIsNotPublished` | VERIFIED |
| INV-W4 | A record is never split across segments, so each segment can be replayed independently. | `TestLargeRecordNearFramingLimit` | VERIFIED |
| INV-W5 | A refused recovery leaves the log byte-for-byte unmodified, so the data remains available to investigate. | `TestBadChecksumInMiddleRecordIsRefused` | VERIFIED |
| INV-W6 | A missing segment is refused rather than replayed around. | `TestSegmentGapIsRefused` | VERIFIED |
| INV-W7 | The framing reader never resynchronises: after any failure it stays failed and returns no further records. | `TestReaderStopsAtFirstProblem`, `TestReaderIsStickyAfterTornTail` | VERIFIED |
| INV-W8 | A malformed payload — unknown record kind, unknown operation kind, bad lengths, trailing bytes — is refused, never guessed at. | `TestDecodeBatchRejectsMalformedPayloads`, `TestUnknownRecordKindIsRefused`, `TestMalformedBatchPayloadIsRefused` | VERIFIED |
| INV-W9 | Every append completes a `write(2)` before returning, in every sync mode, so an acknowledged write survives process death even with fsync disabled. | `TestCrashWithSyncOffStillRecoversFromThePageCache` | VERIFIED |
| INV-W10 | A durability failure latches: once a flush has failed, no further append is acknowledged. | asserted in `wal.append`; no fault-injection test yet | PLANNED |

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
