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

## Raft (Phase 9)

Enforced by `internal/raft` (the pure deterministic core), `internal/raftlog` (durable log),
and `internal/raftnode` (driver); specified in `docs/RAFT.md` and introduced by ADR-016. The `R`
series is proven in **deterministic simulation** (`internal/raft`'s single-goroutine network,
which runs the continuously-checkable invariants after every step) and, for durability, by
**process-kill** tests. These are Raft **safety** properties; they are not, on their own, the
end-to-end Quorum consistency model (`docs/CONSISTENCY.md`), which Phase 12 verifies.

**Phase 10 (`docs/FAULTS.md`)** re-verified every one of them **under injected faults**: the
`internal/raftsim` simulator checks R1–R10 continuously — after every event, or at the exact send,
commit, apply or restart the property is about — across scripted fault scenarios and seeded
random schedules of drop, duplication, delay, reordering, partitions, process crashes, a modeled
power loss, restarts, pauses and persistence failures; the driver and real-process fault tests
add what the simulator cannot show. The "Checked by" column lists both.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-R1 | **Election Safety.** At most one leader can be elected in a given term. | `internal/raft`: `assertAtMostOneLeaderPerTerm` (continuous, every step of every sim test), `TestThreeNodeElection`, `TestVoteGrantedOncePerTerm`, `TestVoteDeniedToStaleLog`, `TestSplitVoteResolves`. Phase 10 — `internal/raftsim`: continuous under every fault schedule, by role observation and by AppendEntries sender, plus `TestDuplicatedVoteDoesNotCountTwice`; `internal/raftnode`: a Status monitor in every real-TCP fault test; `tests/integration`: no two `dkvd` processes ever announce one term (sampled every 20 ms, so it can miss a violation, never invent one) | VERIFIED (simulation, incl. under faults) |
| INV-R2 | **Leader Append-Only.** A leader never overwrites or deletes entries in its own log; it only appends. | `internal/raft`: `TestLeaderAppendOnly` (records each index across proposals and fails on any rewrite). Phase 10 — `internal/raftsim`: continuous for every leader under every fault schedule | VERIFIED (simulation, incl. under faults) |
| INV-R3 | **Log Matching.** If two logs contain an entry with the same index and term, the logs are identical in all entries up through that index. | `internal/raft`: `assertLogMatching` (continuous, every step, all node pairs), `TestReplicationAndCommitAndApply`, `TestSuffixReplacement`, `TestConflictBackupByTerm`. Phase 10 — `internal/raftsim`: continuous across all live pairs under every fault schedule; `internal/raftnode` and `tests/integration`: the durable logs on disk compared after partition, crash and flapping runs | VERIFIED (simulation, incl. under faults; durable logs of real processes) |
| INV-R4 | **Leader Completeness.** If an entry is committed in term T, it is present in the log of every leader of every term > T. | `internal/raft`: `TestLeaderCompleteness` (commit, then forced leadership changes), `TestFigure8` (the committed old-term entry survives and blocks a competing candidate). Phase 10 — `internal/raftsim`: continuous (every leader against the global committed record) under every fault schedule, `TestLeaderCrashAndReelection`, `TestAckedEntrySurvivesPowerLoss`, `TestNoAckOfUnsyncedEntriesAfterFailedFsync`; `tests/integration`: every prefix committed before a SIGKILL/partition is at the head of every log afterwards | VERIFIED (simulation, incl. under faults; process kill) |
| INV-R5 | **State Machine Safety.** If a node has applied an entry at index i, no node ever applies a different entry at index i. | `internal/raft`: cross-node apply-history comparison in `applyCommitted` (continuous), `TestReplicationAndCommitAndApply`; `internal/raftnode`: `TestClusterElectsAndReplicates` (real TCP, applied order asserted). Phase 10 — `internal/raftsim`: continuous at every commit and every apply under every fault schedule; `internal/raftnode`: `TestDuplicatedAndReorderedTrafficAppliesOnce` (real TCP) | VERIFIED (simulation, incl. under faults; real cluster) |
| INV-R6 | `currentTerm` and `votedFor` are durable before any vote is granted or any dependent AppendEntries reply is sent. | Structural via the `Ready` contract (persist HardState/Entries before Messages), asserted by `internal/raft`: `TestHardStateAccompaniesVoteGrant`, `TestHardStateAccompaniesTermBump`; on the real driver path by `internal/raftnode`: `TestPersistBeforeReplyOnDriverPath`; across process death by `tests/integration`: `TestRaftLogSurvivesSIGKILL`, `TestHardStateSurvivesSIGKILL`. Phase 10 — the **fsync** form: `internal/raftlog`: `TestSaveIsDurableWhenItReturns`, `TestOpenMakesRecoveredStateDurable`; `internal/raftnode`: `TestReplyOnlyAfterFsync`; `internal/raftsim`: at every send, no un-fsynced byte and durable term/vote/log equal to in-memory, and no node grants one term's vote to two candidates across any number of crashes and modeled power losses | VERIFIED (process kill; fsync ordering against a modeled power loss — not real power loss) |
| INV-R7 | A node never applies an entry with index > `commitIndex`. | `internal/raft`: apply-path guard in `applyCommitted` (continuous), `FuzzRaftEvents` (applied ≤ commit ≤ lastIndex after every step); enforced structurally by the Phase 8 log (INV-P8). Phase 10 — `internal/raftsim`: at every apply under every fault schedule | VERIFIED |
| INV-R8 | `commitIndex` is monotonically non-decreasing on every node, across restarts. | Within a session: Phase 8 log (INV-P5). Across restart: `internal/raftnode`: `TestGracefulRecovery`; `tests/integration`: `TestRaftLogSurvivesSIGKILL` (recovered node reaches a strictly higher commit, never lower), `TestCurrentTermMonotonicAcrossRestart` (the term, too, only climbs); `internal/raftlog` clamps a persisted commit to the recovered log. Phase 10 — `internal/raftsim`: within a run after every event, and at every restart the recovered commit is never below what the node had applied | VERIFIED (process kill; simulated crashes) |
| INV-R9 | A leader only advances `commitIndex` past entries from its own term. | `internal/raft`: the `maybeCommit` current-term check + the mandatory election no-op; `TestCommitRuleRequiresCurrentTerm` (the rule pinned directly on `maybeCommit`), `TestFigure8` (removing the no-op fails it). Phase 10 — `internal/raftsim`: continuous — whenever a leader's commit advances, the entry there has the leader's term | VERIFIED (simulation, incl. under faults) |
| INV-R10 | A stale message (lower term) never mutates state beyond sending a rejection carrying the current term. | `internal/raft`: `TestStaleMessageIsInert`, `TestStaleAppendResponseIgnored`, `TestHigherTermForcesStepDown`. Phase 10 — `internal/raftsim`: at every delivery of a lower-term message (role, term, vote, log and commit unchanged), `TestDelayedOldTermAppendIsInert` | VERIFIED (simulation, incl. under faults) |

### Note on INV-R9 and the no-op

With the **mandatory** election no-op (`TestNoOpAppendedOnElection`), a new leader's first
quorum acknowledgement already covers a current-term entry, so the `maybeCommit` term check never
has to *reject* a commit reachable through the message flow — the no-op is the primary mechanism
and the explicit `term == currentTerm` check is defense-in-depth that matches the paper. Because
that state is unreachable end to end, the rule is pinned directly on `maybeCommit` by
`TestCommitRuleRequiresCurrentTerm`, which is what kills a mutation that drops the check
(`docs/RAFT.md` §12a). `TestFigure8` has teeth against removing the no-op (the term-4 leader then
cannot commit the term-2 entry, failing the `commit >= 3` assertion), and the continuous R3/R5
checks would fire if a committed entry were ever overwritten. Power-loss durability is untested throughout (SIGKILL only), as everywhere in
this project (`docs/FAILURE_MODEL.md`).

## Fault injection (Phase 10)

Enforced by `internal/raftlog`, `internal/raftnode` and `cmd/dkvd`; exercised by `internal/fault`,
`internal/raftsim`, and `tests/integration/raft_fault_test.go`; specified in `docs/FAULTS.md` and
introduced by ADR-017. The `F` series is the fault-behaviour namespace, distinct from every series
above. Each is a property of the system under injected failure, not of the test harness.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-F1 | A durable-log write or fsync failure is **fail-stop**: none of the failing Ready's messages is sent, the node processes no further event, the log accepts no further write, `dkvd` exits 1, and the log remains openable (a torn record is truncated on reopen). | `internal/raftlog`: `TestFailedWriteLatchesAndLogStaysRecoverable`, `TestFailedSyncLatches`; `internal/raftnode`: `TestPersistFailureIsFailStop` (disk full, torn write, fsync failure — the op log shows no write or fsync after the failure); `cmd/dkvd`: `TestRaftModeExitsNonZeroWhenTheLogFails`; `internal/raftsim`: `TestVoteNotSentWhenItCannotBePersisted`, `TestTornWriteIsTruncatedOnRestart`, `TestDiskFullOnLeaderStopsItAndClusterMovesOn`, the `disk` profile | VERIFIED (injected software I/O errors) |
| INV-F2 | A restarted node resumes from exactly its durable state: its recovered term, vote, log and commit equal what it last successfully persisted, extended by at most a prefix of the records of a save that was interrupted (in the order `raftlog.SavePlan` writes them) — after a process crash and after a modeled power loss — and that recovered state is made durable before the node acts on it. | `internal/raftsim`: at every restart, against an independent record of every Save (continuous), `TestFollowerCrashAndCatchUp`, `TestNoAckOfUnsyncedEntriesAfterFailedFsync`, the `crashes`/`disk`/`mixed` profiles; **Phase 11:** at every cell of `TestCrashMatrix` (a crash at every driver and I/O boundary, in three crash modes) and the `crashpoints` profile; `internal/raftlog`: `TestOpenMakesRecoveredStateDurable`; `tests/integration`: committed prefixes survive real SIGKILL/restart, `TestRealCrashAtPoints` | VERIFIED (simulation incl. every crash boundary; process kill) |
| INV-F3 | **Liveness after faults stop**: once every node is up, every partition healed and the schedule fair, one leader is elected, commits an entry of its own term, and every node's log, commit and applied index converge to it within 400 rounds. | `internal/raftsim`: `check-converged` ends every seeded run and fuzz input; `TestRepeatedCrashRestart`, `TestPartitionedNodeRestartsWhileIsolated` | VERIFIED (simulation only) |
| INV-F4 | Faults never fabricate or duplicate a command: every applied command was accepted by a leader and occupies exactly one log index, under any mix of drop, duplicate, reorder, delay and crash. | `internal/raftsim`: at every apply (continuous); `internal/raftnode`: `TestDuplicatedAndReorderedTrafficAppliesOnce` (real TCP) | VERIFIED |
| INV-F5 | The driver never blocks its Raft actor on the network: a peer whose sends block delays only its own messages; heartbeats and replication to the other peers continue, so the wedge by itself triggers no election. (Phase 12 revision of the test: it asserts twenty rounds of accept-and-commit with the healthy follower after the wedge engaged, instead of "no term change for one second", which a starved machine breaks on its own; an election voids that attempt's one-leader premise and the scenario starts over, at most three times — an election in every attempt fails it.) | `internal/raftnode`: `TestWedgedPeerDoesNotStallTheLeader` (real TCP; fails with a synchronous send) | VERIFIED (driver) |

### Note on the F series and what it does not cover

INV-F1 is proven against **injected software I/O errors** at the `vfs` boundary, underneath the
unchanged `raftlog` code; it says nothing about how a real device fails. INV-F2's power-loss half
is proven against `fault.MemFS`, a model that assumes an fsync that returned success is honest and
that lost data is a prefix — never holes or reordered sectors — so real power-loss durability stays
untested, exactly as for INV-S1 and INV-M1. INV-F3 is a liveness property proven only in the
simulator, and only after faults stop (FLP: no claim is possible while they continue). The storage
WAL's own durability latch, INV-W10, is a different layer and stays PLANNED: no node hosts the
engine yet.

## Crash recovery (Phase 11)

Enforced by `internal/raftlog`, `internal/raftnode` and `cmd/dkvd`; exercised by `internal/raftsim`
(the crash matrix, the `crashpoints` profile and the scripted crash windows), `internal/raftnode`
and `tests/integration/raft_crash_test.go`; specified in `docs/CRASH_RECOVERY.md` and introduced by
ADR-018. The `CR` series is the crash-recovery namespace, distinct from every series above (`C` is
already both routing and compaction). Each is checked in the simulator at the instant a node boots,
against an independent record of every Save it completed, at every crash point of every scenario,
matrix cell and chaos seed; INV-F2 remains the umbrella and INV-R1..R10 stay in force throughout.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-CR1 | **Term and vote never regress across a crash.** The recovered `currentTerm` is at least the last durably established one; if equal, a vote durably cast in it is recovered (a vote an interrupted Save was casting may or may not have landed). | `internal/raftsim`: `checkRecovered` at every boot; `TestFollowerCrashAfterAdoptingAHigherTerm`, `TestVoterCrashAroundPersistingItsVote`, `TestSingleNodeCrashInsideItsElectionSave`; `internal/raftlog`: `TestTermChangeIsDurableBeforeEntriesOfThatTerm`, `TestPrePhase11OrderLeftAnUnrecoverableLog` (the old order reproduced deliberately); `tests/integration`: the term a SIGKILL leaves is never below the one the node had established (`TestRealCrashAtPoints`) | VERIFIED (simulation incl. modeled power loss; process kill) |
| INV-CR2 | **The durably committed prefix is recovered bit-identical**, and the recovered commit is never below the durable one. | `internal/raftsim`: `checkRecovered` at every boot, `TestCrashDuringSuffixReplacement`; `internal/raftlog`: `TestCommitNeverCoversEntriesTheSaveHadNotWritten`, `TestCommitBeyondRecoveredLogIsClamped` | VERIFIED (simulation; process kill) |
| INV-CR3 | **Commit is durable before apply.** An entry is applied only after a HardState whose commit covers it is fsynced, so the recovered commit is never below what the dead incarnation had applied. | `internal/raftsim`: `checkRecovered` at every boot, `TestCommitIsDurableBeforeApply`; `internal/raftnode`: `TestCrashAfterApplyReappliesOnRestart` | VERIFIED |
| INV-CR4 | **Replay is exact and at-least-once.** After a restart `appliedIndex` is 0 and the node re-applies exactly its recovered committed prefix, in order, each index once per incarnation, with entries identical to those any earlier incarnation applied there. No exactly-once claim across restarts. | `internal/raftsim`: `checkApply` (INV-P9 per incarnation, INV-R5 across), `TestCrashAroundApply`, `TestRepeatedCrashesAtPoints`; `internal/raftnode`: `TestCrashAfterApplyReappliesOnRestart`, `TestCrashBeforeApplyAppliesOnceOnRestart` | VERIFIED |

### Note on the CR series and what it does not cover

INV-CR1..3 are proven against a **process crash** on real files and processes, and against a
**modeled** power loss (`fault.MemFS`) in the simulator — real power loss is untested, as everywhere
in this project. INV-CR4 states the replay contract the driver actually provides — at-least-once
across restarts — and deliberately claims no more: the applied index is volatile by design.
What Phase 13 adds on top is a different property — at most one execution per client request
identity (INV-X2), decided by the state machine from its replicated session table, which replay
rebuilds exactly (`docs/DEDUP.md` §4) — not a property of the driver. The order of a
Save's records that INV-CR1/CR2 depend on (`raftlog.SavePlan`) was the one Phase 11 bug: the old
entries-first order let a crash between a Save's entry and HardState records leave a log recovery
refused (`docs/CRASH_RECOVERY.md` §5).

## Routing / cluster

The `C` numbers here are the **routing** series and are distinct from the Phase-4
**compaction** `INV-C1..C7` above; both series predate this note and are not renumbered
(the compaction ones are cited across `docs/COMPACTION.md` and the storage tests). Context
disambiguates: these are enforced by `internal/routing`, specified in `docs/ROUTING.md`, and
introduced by ADR-012.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-C1 | Key routing is a pure function of (key, membership configuration). Same inputs → same shard, on every node, forever. | `internal/routing`: `TestGoldenTokenVectors`, `TestGoldenKeyToShard`, `TestGoldenReplicaGroups` (values from an independent Python reference, not self-checked), `TestRouteIsDeterministicAcrossManyCalls`, `TestEquivalentConfigsRouteIdentically`, `TestNodeOrderDoesNotAffectRouting`, `TestConfigRoundTripThroughSerializationRoutesIdentically`, `FuzzRouteIsDeterministicAndValid` | VERIFIED |
| INV-C2 | Every shard has exactly one replica group, and every key maps to exactly one shard. No key is unowned; no key is doubly owned. | `internal/routing`: `TestEveryTokenIntervalHasExactlyOneOwner` (arc lengths sum to 2⁶⁴ — no gap, no overlap — plus per-arc boundary/interior ownership), `TestRingIsSorted`, `TestSuccessorBoundaryAndWrap`, `TestTokenCollisionIsDeterministic`, `TestRouteAlwaysReturnsAValidShard`, `TestEveryShardIsRepresentedExactlyOnce`, `TestEveryShardHasOneReplicaGroup` | VERIFIED |
| INV-C3 | A membership change of one node moves only the keys it must (≈ 1/N of the space), not a reshuffle. | `internal/routing`: `TestKeyToShardIsStableAcrossNodeMembershipChange` (0 key→shard changes), `TestOwnerMovesOnlyWhereItsShardPrimaryMoved` (movement fully attributed to shard reassignment), `TestConsistentHashingBeatsModuloOnRedistribution`, `TestRedistributionGoldenCounts` (fixed 141/141/68 ring vs 383/383/425 modulo shards of 512) | VERIFIED |
| INV-C4 | A client request for key k is never served by a node that does not host k's shard, except as an explicit forward or redirect. | Phase 8+ (request serving) — Phase 7 has no server to violate it | PLANNED |

## Transport / node process (Phase 7)

Enforced by `internal/transport`, `cmd/dkvd`, and `tests/integration/cluster_test.go`;
specified in `docs/TRANSPORT.md` and introduced by ADR-013/ADR-014. The `T` series is the
transport/node namespace, distinct from every series above.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-T1 | Frame parsing is bounded and explicit: a declared length over `MaxFrameSize` (16 MiB) is rejected before any allocation, and a frame is read with `io.ReadFull` discipline regardless of TCP fragmentation. | `internal/transport`: `TestFrameTooLargeIsRejectedBeforeAlloc`, `TestFrameReassembledFromFragments`, `TestConcatenatedFramesDecodeIndividually`, `FuzzFrameDecode` | VERIFIED |
| INV-T2 | Malformed transport input is rejected as a protocol error that closes the connection — never repaired, resynchronised, or interpreted as valid data (a socket is not a WAL). Nothing panics on hostile input. | `internal/transport`: `TestTruncatedFrameIsError`, `TestBadChecksumIsError`, `TestUnknownKindIsError`, `TestHandshakeBadMagicRejected`, `TestHandshakeVersionMismatchRejected`, `TestProbeMalformedRejected`, `FuzzFrameDecode`, `FuzzHandshakeDecode`, `FuzzProbeDecode` | VERIFIED |
| INV-T3 | A successful handshake precedes any application message; a connection that fails the handshake (bad magic, wrong version, self id, unknown peer, timeout) exchanges no frames and is closed. | `internal/transport`: `TestSelfConnectionRejected`, `TestUnknownPeerRejected`, `TestHandshakeTimeoutClosesConnection`, `TestBadMagicClosesConnection`, `TestHandshakeTruncatedRejected` | VERIFIED |
| INV-T4 | Each received message is attributed to the peer identity established by the handshake on its connection, never to a value carried in the payload. | `internal/transport`: `TestTCPHandshakeAndProbe`, `TestPayloadCannotSpoofSender` | VERIFIED |
| INV-T5 | Frames on a single connection are delivered in send order, concurrent senders never interleave a frame's bytes, and a frame is written in full even across short writes (no partial frame on the wire). | `internal/transport`: `TestPerConnectionOrderPreserved`, `TestConcurrentSendersDoNotInterleave`, `TestFrameSurvivesPartialWrites`, `TestWriteErrorAfterPartialWriteIsReturned`, `TestZeroProgressWriterDoesNotLoopForever` | VERIFIED |
| INV-T6 | Node shutdown terminates all transport resources — accept loop, dial loops, reader/writer paths, connections — with no goroutine leak; repeated shutdown is safe; and three real processes exit cleanly. | `internal/transport`: `TestCloseIsIdempotent`, `TestNoGoroutineLeakAfterClose`, `TestSendAfterCloseFails`; `tests/integration`: `TestThreeNodeClusterProbesAndShutsDownCleanly` | VERIFIED |

## Replication model (Phase 8)

Enforced by `internal/replication`; specified in `docs/REPLICATION.md` and introduced by
ADR-015. The `P` series is the replication-model namespace, distinct from every series above
(in particular from the `R` Raft series). These are **local** properties
of a replicated-log model and a replica-group abstraction; none of them is a distributed or
consistency guarantee — Phase 8 adds no such claim anywhere.

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-P1 | A `ReplicaGroup` is valid or it does not exist: an empty group, an empty or duplicate replica id, and an RF that is < 1 or not equal to the replica count are refused (never repaired); replica order is preserved (head = primary), and identity/equality are deterministic and order-sensitive. Groups are consumed from routing metadata rather than recomputed. | `TestReplicaGroupValidConstruction`, `TestReplicaGroupRejectsInvalid`, `TestReplicaGroupIsImmutable`, `TestReplicaGroupEqualityIsDeterministic`, `TestReplicaGroupsFromRouter`, `FuzzReplicaGroup` | VERIFIED |
| INV-P2 | Log indexes are contiguous and 1-based with no gaps or duplicates (0 is the empty sentinel), and terms are non-decreasing along the log; an append that would break either is refused. | `TestSequentialAppendAndRangeReads`, `TestAppendRejectsGapDuplicateAndTermRegression`, `TestAgainstReferenceModel` (invariants asserted after every step), `FuzzLogOperations` | VERIFIED |
| INV-P3 | Suffix replacement is deterministic and hole-free: it retains a prefix, drops the existing suffix, appends the batch, cannot start past the end (no gap), and cannot replace a committed entry. | `TestSuffixReplacementVariants`, `TestSuffixReplacementRejections`, `TestCannotReplaceCommittedEntry`, `TestAgainstReferenceModel`, `FuzzLogOperations` | VERIFIED |
| INV-P4 | Entries are copy-safe in both directions: stored bytes are never aliased to caller memory, and returned bytes never expose internal storage. | `TestAppendCopiesInput`, `TestReadsReturnCopies`, `TestEmptyAndNilDataArePreserved`, `TestGroupIsolation`, `FuzzLogOperations` (aliasing check) | VERIFIED |
| INV-P5 | `commitIndex` is monotonic — a backward commit is rejected and leaves it unchanged. | `TestCommitInitialAndMonotonic`, `TestAgainstReferenceModel` | VERIFIED |
| INV-P6 | `commitIndex` never exceeds the last local log index, including after a suffix replacement. | `TestCommitInitialAndMonotonic`, `TestCannotReplaceCommittedEntry`, `TestAgainstReferenceModel` (asserted after every step), `FuzzLogOperations` | VERIFIED |
| INV-P7 | `appliedIndex` is monotonic — a backward apply is rejected and leaves it unchanged. | `TestApplyInitialAndMonotonic`, `TestAgainstReferenceModel` | VERIFIED |
| INV-P8 | `appliedIndex` never exceeds `commitIndex`: applying an uncommitted index is rejected. | `TestApplyInitialAndMonotonic`, `TestCommittedRangeEnumeration`, `TestAgainstReferenceModel` (asserted after every step), `FuzzLogOperations` | VERIFIED |
| INV-P9 | No entry is applied twice through the interface: application is a watermark, so a re-issued apply applies nothing and a state machine driven from `Unapplied` sees each index exactly once. | `TestNoDoubleApplication`, `TestCommittedRangeEnumeration` | VERIFIED |

## Client semantics (Phase 12: linearizability; Phase 13: identity, dedup, forwarding; Phase 15: API)

Phase 12 rows are enforced by `internal/raft` (ReadIndex), `internal/raftnode` (write and read
completion), `internal/kv` (state machine, server, wire protocol) and `cmd/dkvd`; checked by
`internal/lincheck` over histories from real processes (`tests/integration`), the real driver
(`internal/kv`) and the simulator (`internal/raftsim`), where INV-X5..X8 are also checked directly
at every completion; specified in `docs/LINEARIZABILITY.md` and introduced by ADR-019. "Verified"
here means: every recorded history satisfied it and the mechanism's mutants are killed — a finite,
bounded result, not a proof over all executions (LINEARIZABILITY §12).

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-X1 | A write acknowledged to a client is readable by a subsequent linearizable read from *any* client. | `lincheck` on every recorded history; explicitly: `TestRealCompletedWriteIsSeenByEveryLaterRead` (every node contacted first), `TestRealReadsAcrossLeaderChanges`, `TestRealWriteCrashWindows` (a committed write stays visible across the leader's SIGKILL, acknowledged or not); simulator: INV-X5 + INV-X7; mutants 40, 41, 43, 44 | VERIFIED (recorded histories; one Raft group) |
| INV-X2 | **At most once per identity.** An identified write — `(ClientID, RequestID)` — changes the key-value state at most once, however many log entries carry it; every other entry carrying it is answered with that execution's index (or refused). | `internal/raftsim`: checked at every apply of every replica (no identity executes at two indexes) across the six session profiles × 200 seeds and the scripted cases; every real-process Phase 13 test replays the durable committed logs (`dedupEvidence`); `TestUnknownWriteRetriedAfterLeaderCrashIsOneRequest`, `TestRetryAtEveryCrashPointOfAWrite`, `TestConcurrentDuplicatesAtTwoNodes`, `TestRealSessionRetryAcrossCrashWindows`, `TestRealConcurrentDuplicatesThroughEveryNode`; the logical history is linearizable (LINEARIZABILITY §15); mutants 61–75, 83–86, 88, 89. Anonymous writes (ClientID 0) are outside it — Phase 12's `TestRealIncompleteWriteThenRetry` still shows one applied twice | VERIFIED (recorded histories and replayed logs; one Raft group) |
| INV-X3 | A read in `linearizable` mode never returns a value older than any completed write that preceded the read's invocation in real time. (The only read mode that exists.) | `lincheck` on every recorded history; `TestRealStaleLeaderNeverServesARead`, `TestRealMinorityLeaderWithAFollowerNeverServesARead` (five real processes), `TestKVStaleLeaderReadIsNeverServed`, `TestKVMinorityLeaderWithAFollowerNeverServesARead`, `TestKVNewLeaderReadWaitsForItsNoop`; INV-X7; the ReadIndex argument (LINEARIZABILITY §5.2); mutants 34–39, 42, 43, 54–57, 59 | VERIFIED (recorded histories; one Raft group) |
| INV-X4 | A read in `stale` mode is never *presented* as linearizable — the API response carries the mode it was served under. | Phase 15 API test (no `stale` mode exists yet) | PLANNED |
| INV-X5 | **Acknowledged means committed.** A write reported OK at (index *i*, term *t*) is the entry committed at *i*: its term is *t* and its bytes are the command proposed. | `internal/raftsim`: checked at the completion instant of every simulated write (`kvPoll`); `TestWaitersCompleteWritesOnlyInTheirTerm`, `TestKVWriteIsNotAcknowledgedBeforeCommit`; mutants 40, 41 | VERIFIED (simulation; driver unit) |
| INV-X6 | **Lost means no effect.** A write reported lost (`ErrLost`) is not committed at its index — a different entry is. | `internal/raftsim`: checked at every lost completion; `TestKVWriteIsNotAcknowledgedBeforeCommit`; `TestWaitersCompleteWritesOnlyInTheirTerm` | VERIFIED (simulation; driver unit) |
| INV-X7 | **ReadIndex freshness.** A read is served at a read index — and from a state machine applied through at least — the highest index committed **anywhere** in the cluster when the read was registered. | `internal/raftsim`: checked at every read completion across 1,400 seeded runs and the scripted attacks (it is what catches the no-quorum mutant in the seeded `kv-splits` runs); `TestAcksFromBeforeTheReadDoNotConfirmIt`, `TestNewLeaderReadIndexIsAtLeastItsNoop`, `TestIsolatedLeaderNeverConfirmsARead`; mutants 35–37, 42, 55 | VERIFIED (simulation; core unit) |
| INV-X8 | **The state machine is the specification.** After convergence, every node's `kv.Store` equals the `lincheck` register model folded over the committed log (empty value present; delete idempotent). | `internal/raftsim`: `checkStores` after every seeded client run; `internal/kv`: `TestStoreMatchesTheStorageContract`, `TestStoreMatchesTheReferenceModel`; mutants 45, 46 | VERIFIED |
| INV-X9 | **Unknown is never reported as known.** A request whose response was not received — deadline, dead connection, stopped node, unanswered forward — is recorded `Incomplete`, never OK and never Rejected; an unknown **anonymous** write is never retried under the same operation, an **identified** one only under its own identity; a session that runs out of attempts after an unanswered one reports `Known: false`; a timed-out connection is never reused, so a late response cannot answer a newer request. | `TestClientPolicy`, `TestWireClientNeverMatchesALateResponseToANewRequest`, `TestWireClientReportsUnknownWhenTheConnectionDies`, `TestRealWriteCrashWindows`, `TestDuplicateSentBeforeTheOriginalCommits`, `TestRealSessionRetryAcrossCrashWindows`; mutants 47, 48, 71, 73, 74, 85 | VERIFIED |
| INV-X10 | **The checker is right about the histories it judges.** It agrees with an independent oracle (no shared code, whole multi-key history) and with exhaustive enumeration; accepts every known-good and rejects every known-bad corpus history; and never reports a budget overrun as a verdict. | `internal/lincheck`: `TestCheckerAgreesWithIndependentOracle` (20,000 arbitrary histories), `FuzzCheckerMatchesOracle`, `TestCheckerAgreesWithBruteForce`, `TestKnownGoodAndKnownBadCorpus`, `TestBudgetReportsUncheckedNotLinearizable`; mutants 49–53 | VERIFIED (bounded: ≤ 7 ops × ≤ 2 keys exhaustively, larger by construction and fuzzing) |
| INV-X11 | **Every replica decides every entry identically — and as the contract says.** At the instant any node's `kv.Store` applies a committed entry (first application or any replay after a restart or power loss), its decision (registered, executed, duplicate with the original index, conflict, stale, expired, limit) equals the independent `lincheck.SessionModel`'s decision for that index of the committed log. | `internal/raftsim`: `checkDecision` at every apply of every replica, six session profiles × 200 seeds + scripted cases; `TestStoreAgreesWithTheSessionModel` (400 runs, every decision), `TestReplayRebuildsTheSessionTable`; real processes: every Phase 13 test's durable logs replayed through a fresh store and the model; mutants 61–69, 82, 88, 89 | VERIFIED (simulation; unit differential; replayed real logs) |
| INV-X12 | **The session table is bounded.** At most `MaxSessions` sessions and `MaxUnacked` remembered results per session, whatever is applied; eviction is by log position (LRU), never silent — an evicted session's requests are `SESSION_EXPIRED`, never executed as new; a request past the result bound is `SESSION_LIMIT`, never evicting a result a retry may need. | `TestSessionTableStaysBounded` (20,000 commands, checked after each), `TestEvictedSessionIsRefusedNotReexecuted`, `TestSessionLimitRefusesRatherThanForgets`, `kv-sessions-evict`, `TestRealSessionContractSurvivesFullClusterRestart`; mutants 65, 68, 69, 76, 86 | VERIFIED |
| INV-X13 | **Forwarding is one hop, sent at most once, identity intact.** A forwarded request is never forwarded again; a forward is never resent; not sent → `UNAVAILABLE`, sent and unanswered → `UNKNOWN_OUTCOME`; the request keeps its ClientID and RequestID. | `TestForwardedRequestIsNeverForwardedAgain`, `TestForwardedRequestWhoseAnswerIsLostIsRetriedSafely`, `TestConcurrentDuplicatesAtTwoNodes`, `TestRealForwarderDiesBeforeRelaying`, `TestRealConcurrentDuplicatesThroughEveryNode`; mutants 70–72, 84 | VERIFIED |
| INV-X14 | **Only well-formed requests are proposed, and every proposed command applies.** A request outside the contract is `INVALID_REQUEST` before anything is proposed; one that validates becomes a log command every replica decodes and applies. | `TestRequestValidationRejectsEveryOutOfContractField`, `TestValidatedRequestsAlwaysApply`, the wire and command fuzz targets; mutant 87 | VERIFIED |

### Note on the X series and what it does not cover

"Verified" for INV-X1/X3 is a statement about recorded, finite histories from one Raft group of
three nodes — real processes, the real driver, and seeded simulations — plus an argument
(LINEARIZABILITY §3, §5.2) whose assumptions are named. It is not a proof over every execution,
and it does not cover routed multi-group deployments, snapshots, membership change, or hidden
retries of **anonymous** writes (identified writes: INV-X2, Phase 13). INV-X5..X8 and INV-X11 are
implementation invariants checked at the instant they must hold in the simulator, independently of
the history checker, so a checker bug could not hide a violation of them.
