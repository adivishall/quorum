# INVARIANTS

Every invariant here is (a) stated precisely enough to be falsifiable, (b) assigned an ID, and
(c) bound to the test that checks it. An invariant with no test is marked `UNVERIFIED` and is
not allowed to be cited as a guarantee anywhere else in the docs.

Status column: `PLANNED` (no test yet), `VERIFIED` (a test exists and passes), `PARTIAL` (a
test establishes part of the statement and the rest is named explicitly), `VIOLATED` (a test
exists and fails — the implementation is broken and the phase is not done).

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
| INV-S1 | After any crash and restart, the recovered state equals the state produced by applying some **prefix** of the submitted write sequence. Every acknowledged write is inside that prefix, and the prefix has no holes. (Corrected in Phase 2 — see the note below.) | `TestCrashRecoveryAcknowledgedWritesSurvive`, `TestCrashRecoveryMidWriteStormYieldsAPrefix`, `TestReplayReconstructsIdenticalState`; Phase 3: `TestLSMCrashMidWriteStormYieldsAPrefix`, `TestCrashDuringFlush`, `TestLSMRepeatedCrashes` | VERIFIED (process kill only) |
| INV-S2 | WAL replay is deterministic: replaying the same log bytes any number of times yields identical state, and replay never alters the log. | `TestReplayIsDeterministic`, `TestRecoveryIsIdempotent`; Phase 3: `TestRecoveryIsDeterministic`, `TestCrashWithOverwritesAndDeletesAcrossFiles` | VERIFIED |
| INV-S3 | A deleted key never reappears — at any level, after any number of compactions, across restarts. (Tombstones are only dropped at the bottom-most level; see INV-C4 for what "bottom-most" means in this engine.) | `TestDeletedKeyNeverReappearsAfterCompaction`, `TestTombstoneIsNotDroppedWhileAnOlderFileCouldHoldTheValue`, `TestTombstoneSurvivesManyCompactionGenerations`, `TestDeleteThenRewriteSurvivesCompaction`, `TestCrashDuringCompactionNeverLosesADelete`, `TestTombstoneIsKeptWhenNotBottomMost`, `TestDroppedTombstoneStillSuppressesOlderVersions` | VERIFIED |
| INV-S4 | Sequence numbers are assigned deterministically in apply order, so two replicas that applied the same log prefix hold byte-identical logical state. | Phase 12 replica-comparison | PLANNED |
| INV-S5 | A reader holding a version never observes a partially-installed SSTable set. Compaction's version swap is atomic. | `TestReadsDuringCompactionSeeACoherentFileSet` (`-race`), `TestWritesAndFlushesDuringCompaction`, `TestFlushDuringCompactionIsNotLost`; flush half: INV-L4 | VERIFIED |
| INV-S6 | The MANIFEST is the sole authority on which files are live. Files on disk but absent from it are orphans and are deleted; files in it but absent from disk are a fatal error. | `TestManifestRecordsTheLiveFileSet`, `TestOrphanSSTableIsDeletedAtStartup`, `TestReferencedButMissingSSTableIsFatal`, `TestSSTablesWithoutACurrentFileAreRefused`, `TestCompactionInputsLeftOnDiskAreSweptAsOrphans`, `TestCompactionOutputWithoutAManifestRecordIsIgnored`, `TestCrashDuringCompaction` | VERIFIED |
| INV-S7 | A Bloom filter never returns "absent" for a key that is present (zero false negatives). | `TestZeroFalseNegativesOverALargeCorpus`, `TestZeroFalseNegativesForRandomByteKeys`, `FuzzFilterHasNoFalseNegatives`, `TestFilterNeverSkipsAPresentKey`, `TestTombstonedKeysAreInTheFilter`, `TestFalsePositiveRateIsNearTheoretical` | VERIFIED |
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

## LSM storage engine (Phase 3)

Enforced by `internal/storage/ikey`, `internal/storage/memtable`,
`internal/storage/sstable`, `internal/storage/lsmstore.go` and
`tests/integration/lsm_crash_test.go`.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-L1 | The memtable iterates in internal-key order — user key ascending, then sequence number descending — so the newest version of a key is always the first one a scan reaches, and a flush can write a sorted SSTable in one forward pass with no sort step. | `TestOrderedIteration`, `TestVersionsOfOneKeyAreNewestFirst`, `TestLargeTableOrdering`, `TestOrdering` (ikey) | VERIFIED |
| INV-L2 | Sequence assignment is deterministic: exactly one number per mutation, assigned in log order, and never consumed by a read, a rejected operation, or applied-index metadata. Replaying the same log re-derives the identical numbering, which is what makes "this mutation is already in an SSTable" decidable. | `TestSequenceNumbersAreAssignedOnePerMutation`, `TestSequenceNumbersSurviveRestart`, `TestRecoveryIsDeterministic` | VERIFIED (single node) |
| INV-L3 | An SSTable that opens is structurally sound, and any damage to one is detected rather than acted on: bad magic, bad footer fields, overflowing extents, an index disagreeing with the data region, malformed varints and failed block checksums are all refused. | `TestRandomSingleByteDamageIsAlwaysDetected` (every byte of a real file flipped in turn), `TestCorruptFooterFieldsAreRefused`, `TestFooterOverflowIsRefused`, `TestCorruptIndexBlockIsRefused`, `TestMalformedVarintsAreRefused`, `TestIndexDisagreeingWithDataIsRefused`, `TestBadMagicIsAFormatError` | VERIFIED |
| INV-L4 | A partially written SSTable is never visible. A flush writes to a temporary name, fsyncs, renames, then fsyncs the directory, so the final name either does not exist or names a complete, fsynced file; and publication adds the table and drops the immutable memtable in one critical section, so every mutation is in exactly one source at every instant. | `TestCrashDuringFlush` (real SIGKILL inside the SSTable write, with the window it hit verified rather than assumed), `TestOrphanTempFileIsSwept`, `TestConcurrentReadsAcrossFlushPublication` (`-race`) | VERIFIED (process kill) |
| INV-L5 | A lookup resolves correctly across the memtable, the immutable memtables and every SSTable: the newest version of a key wins, and a newer tombstone hides an older value rather than falling through to it. | `TestTombstoneHidesOlderValueAcrossSSTables`, `TestNewestValueWinsAcrossSSTables`, `TestOverwriteAndDeleteInterleavedAcrossManyFiles`, `TestKeyPresentOnlyInTheOldestFile`, `TestAgainstReferenceModel` | VERIFIED |
| INV-L6 | Recovery composes the SSTables and the WAL without applying a mutation twice and without dropping one: mutations already covered by a table are skipped, the rest are replayed, and the result is the state that existed before the crash. | `TestLSMCrashBeforeAnyFlush`, `TestLSMCrashAfterFlush` (asserts the skipped/replayed counts, not only the values), `TestReopenAfterCleanClose`, `TestReopenWithEverythingFlushed`, `TestLSMReopenAcrossManySessions` | VERIFIED (process kill) |
| INV-L7 | Corruption is never presented as absence. A read that cannot decode the file it needs returns an error; it never returns `ErrNotFound`. | `TestCorruptDataBlockIsRefusedNotReportedAsMissing`, `TestDamageAfterOpenIsReportedNotSwallowed` | VERIFIED |
| INV-L8 | An incoherent file set aborts startup rather than being worked around: a gap in the SSTable numbering, overlapping sequence ranges, an empty table, or tables covering sequence numbers the log never held are all refused. | `TestGapInSSTableNumberingIsRefused`, `TestSSTableAheadOfTheLogIsRefused`, `TestCorruptSSTableRefusesToOpen`, `TestTruncatedSSTableRefusesToOpen` | VERIFIED |
| INV-L9 | Replacing the Phase 2 map with the LSM engine changed no client-visible semantic: INV-A1..A9 hold for `LSMStore`, including in a configuration where every single mutation becomes its own SSTable. | `TestLSMStoreConformance`, `TestLSMStoreConformanceAcrossSSTables`, `TestLSMStoreConcurrency`, `TestLSMStoreConcurrencyAcrossSSTables`, `TestFlushEveryWriteReallyFlushes` | VERIFIED |

### Note on INV-L4 and what the concurrency test can and cannot show

`TestConcurrentReadsAcrossFlushPublication` was confirmed to have teeth by mutation testing:
splitting publication into two critical sections **with a 200 µs gap** makes all twelve
readers fail. Splitting it with no artificial delay does **not** reliably fail, because the
window is nanoseconds wide and readers rarely land in it.

So the honest statement is: atomicity comes from the code — one critical section — and the
test guards against that shape changing, not against an arbitrarily narrow race. It is
recorded here rather than left implied, because a concurrency test that cannot fail is worse
than no test at all.

### Note on INV-S5 becoming fully VERIFIED in Phase 4

Phase 3 established the flush half: a reader never observes a partial SSTable file, and a flush's
version change is atomic (INV-L4). Phase 4 adds the half that was PLANNED — a compaction's swap,
performed while it rewrites and unlinks files under live readers.

What makes it hold is the immutable, reference-counted version
(`internal/storage/lsmversion.go`): a read acquires the current version once and holds it for the
whole operation, publication is a single pointer swap, and a retired file is unlinked only once no
live version holds it. `TestReadsDuringCompactionSeeACoherentFileSet` treats `ErrNotFound` as a hard
failure for keys that exist continuously, which is precisely the symptom a two-step publication
would produce.

The same honesty note as INV-L4 applies: the window is nanoseconds wide, so the test guards the
*shape* of the code — one critical section, one pointer — rather than proving an arbitrarily narrow
race cannot occur.

### Note on INV-L3 and INV-L8 after Phase 4

Two Phase 3 invariants had clauses that the MANIFEST superseded. Neither guarantee was dropped;
both moved, and the moves are recorded here rather than left for someone to discover.

**INV-L3 — when data-block damage is detected.** Phase 3 read every block of every SSTable at
startup, because a file's sequence range had nowhere on disk to live and recovering it meant reading
the file. The MANIFEST records it, so startup now cross-checks each file's footer against the
MANIFEST's record of it (entry count and size, recorded independently) and does not read data blocks.
Damage inside a data block is therefore found at the read that needs that block rather than at open.
It is still found, and still reported as corruption rather than as a missing key — which is INV-L7
and is unchanged. `Options.VerifySSTablesOnOpen` restores the Phase 3 behaviour, and
`TestCorruptSSTableRefusesToOpen` pins both halves so neither can drift silently.

**INV-L8 — SSTable numbering gaps.** Phase 3 refused a gap in the file numbering, because with the
directory as the authority a missing file was indistinguishable from data loss. Phase 4 allows gaps:
a compaction allocates a file number before it knows whether it will produce output, so a
no-output compaction legitimately leaves one unused. The check is superseded by a stronger one —
INV-S6's "a referenced file that is missing is fatal" — which catches the condition the numbering
gap was a proxy for, and catches it precisely.

## Bloom filters, compaction and the MANIFEST (Phase 4)

Enforced by `internal/storage/bloom`, `internal/storage/compaction`,
`internal/storage/manifest`, `internal/storage/lsmcompact.go`,
`internal/storage/lsmversion.go` and `tests/integration/lsm_crash_test.go`.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-B1 | A Bloom filter's "no" is authoritative and its "yes" means nothing: the filter may only ever eliminate a file from a lookup, and a lookup it eliminates performs no block I/O at all. A file with no filter answers "maybe" for every key, so a filterless file is consulted in full rather than skipped. | `TestFilterSkipsAbsentKeysWithoutReadingBlocks` (asserts `skips + blockReads == probes`), `TestZeroValueFilterFailsSafe`, `TestFilterlessFileIsHandledExplicitly`, `TestFilterSkipsFilesForAbsentKeys`, `TestMixedFilterAndFilterlessFilesResolveCorrectly` | VERIFIED |
| INV-B2 | A filter that cannot be decoded, or whose block fails its checksum, is corruption — never silently treated as "this file has no filter", and never as "the key is absent". The filter encoding carries no checksum of its own; the block's `crc32c` is what protects it. | `TestDecodeRejectsMalformedFilters`, `TestCorruptFilterIsRefusedNotIgnored` (every byte of the filter block), `TestMalformedFilterEncodingIsRefused` (checksum recomputed, so only strict decoding catches it), `TestCorruptFilterChecksumIsRefused`, `FuzzDecodeIsTotal` | VERIFIED |
| INV-C1 | Compaction preserves logical state exactly: for every key, the value a lookup returns before a compaction is the value it returns after it, and after any number of compactions and restarts. | `TestAgainstReferenceModelWithCompaction` (3 configurations, compared after every operation and after restarts), `TestCompactedStateSurvivesRestart`, `TestManyCompactionGenerationsWithRestarts`, `TestAgainstReferenceModel` (merge-level, 80 generated file sets) | VERIFIED |
| INV-C2 | The merge is streaming: its memory is one cursor per input plus fixed buffers, never proportional to the data being merged. | Structural — `compaction.Merger` holds a heap of cursors with reused key/value buffers and no accumulation. `TestLargeMergeStaysOrdered` merges 32,000 entries across 8 files. **Not** independently measured; see the note below. | PARTIAL |
| INV-C3 | An input that fails partway through stops the compaction and is reported. A source that stopped because a block failed its checksum is never mistaken for one that finished, so a compaction cannot produce a structurally perfect, silently truncated output. | `TestInputFailureStopsTheMergeAndIsReported`, `TestInputFailureAtPositioningIsReported` | VERIFIED |
| INV-C4 | A tombstone is dropped only when the compaction's input set contains the oldest live data in the database — i.e. when no live file outside the input set holds a lower sequence number. Otherwise it is retained even though it is the newest version of its key. This is `docs/DESIGN.md` §7's "bottom-most level" rule expressed in the ordering this engine actually has. | `TestTombstoneIsKeptWhenNotBottomMost`, `TestTombstoneIsDroppedOnlyWhenBottomMost`, `TestTombstoneIsNotDroppedWhileAnOlderFileCouldHoldTheValue` (asserts the retention count, not just the read), `TestDroppedTombstoneStillSuppressesOlderVersions` | VERIFIED |
| INV-C5 | Live SSTables have pairwise-disjoint sequence ranges, which is what makes "the first source holding any version of the key wins" correct. A flush and a compaction each preserve it, and a file set that violates it aborts startup rather than being read. | `checkCoherentFileSet` at every open, asserted by `TestAutomaticFlushOnMemTableSize`, `TestL0TriggerMergesAllOfLevelZero`; violation refused by `TestOverlappingInputsAreRefused` (merge level) and the Phase 3 overlap tests | VERIFIED |
| INV-C6 | A file that appears while a compaction is running — a concurrent flush's output — is neither deleted by that compaction nor lost from the MANIFEST. A compaction deletes exactly the inputs it named. | `TestFlushDuringCompactionIsNotLost`, `TestWritesAndFlushesDuringCompaction` | VERIFIED |
| INV-C7 | A compaction whose output is empty publishes a deletion rather than an empty SSTable, and the input files cease to exist. | `TestCompactionThatDropsEverythingLeavesNoFile` | VERIFIED |
| INV-M1 | A MANIFEST version edit is atomic with respect to a crash: it is one checksummed record, so either the whole file-set change replays or none of it does. There is no state in which a compaction's output and its inputs are simultaneously live. | `TestTornFinalRecordIsRepaired`, `TestEditRoundTripCarriesEveryField`, `TestCrashDuringCompaction`; ADR-010 | VERIFIED (process kill) |
| INV-M2 | Damage a crash cannot explain aborts startup; a torn final record truncates and continues. A refused recovery leaves the manifest byte-for-byte unmodified. Identical to INV-S8's policy for the WAL. | `TestDamageInTheMiddleIsRefused`, `TestTornFinalRecordIsRepaired`, `TestUnknownRecordKindIsRefused`, `TestMalformedEditPayloadIsRefused`, `TestIncoherentHistoryIsRefused`, `TestEmptyManifestIsRefused`, `TestGarbageInCurrentIsRefused`, `TestCurrentNamingAMissingManifestIsRefused`, `TestCorruptManifestIsRefused` | VERIFIED |
| INV-M3 | Recovery time tracks current state, not total history: a fresh MANIFEST holding a full snapshot is installed on every open and superseded ones are deleted, so replay is one snapshot plus one session's edits. | `TestManifestIsReinstalledOnEveryOpen` (asserts the edit count on the following recovery), `TestRemoveObsoleteKeepsOnlyTheLiveManifest` | VERIFIED |
| INV-M4 | A data directory holding SSTables but no `CURRENT` is refused, not adopted. Nothing on the filesystem distinguishes a Phase 3 database from a Phase 4 one whose `CURRENT` was lost, and adopting silently would reduce INV-S6 to a suggestion. | `TestSSTablesWithoutACurrentFileAreRefused` (both the refusal and the explicit opt-in), `TestEmptyDirectoryBootstrapsAManifest` | VERIFIED |
| INV-M5 | Startup validates each referenced file against the MANIFEST's independent record of its size and entry count, so a disagreement between the two is caught without reading a data block. | `TestManifestDisagreeingWithAFileIsRefused`, `TestStartupDoesNotFullyScanByDefault` (asserts zero data-block reads by default and non-zero with `VerifySSTablesOnOpen`) | VERIFIED |

### Note on INV-C2 being PARTIAL

The merge is streaming by construction: `compaction.Merger` holds a heap of one cursor per input,
each with a key and value buffer it reuses on every advance, and accumulates nothing. Reading the
code establishes it, and `TestLargeMergeStaysOrdered` exercises 32,000 entries across 8 files
without incident.

What no test does is **measure** that memory stays flat as the input grows — an allocation or
peak-RSS assertion across input sizes. Until one exists, the honest status is PARTIAL: the property
is true of the code as written, and nothing would catch a future change that started buffering.
Marking it VERIFIED would be turning "we read the code" into "we proved it".

### Note on INV-M1's qualifier

`(process kill)` carries the same meaning as it does for INV-S1 and INV-L4: the crash tests destroy
a real process with SIGKILL, which establishes that the bytes reached the kernel. Power loss is
untested for the MANIFEST exactly as it is for the WAL, and for the same reason
(`docs/FAILURE_MODEL.md` §4).

Two of the publication protocol's five crash windows are a few microseconds wide — between the
rename and the MANIFEST append, and between that append and the unlink — so a sleep-and-kill reaches
them only by luck and `TestCrashDuringCompaction` does not require it. They are covered
deterministically instead, by constructing the exact on-disk state
(`TestCompactionOutputWithoutAManifestRecordIsIgnored`,
`TestCompactionInputsLeftOnDiskAreSweptAsOrphans`). That is weaker evidence than a real crash and is
labelled as such.

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
