# COMPACTION

Status: **Phase 4 — implemented and verified.** Every property stated here is bound to a test
named in `docs/INVARIANTS.md`. Where something is *not* established, this document says so.

Strategy: `docs/DESIGN.md` §7 and ADR-007. Merge: `internal/storage/compaction`. Policy,
publication and concurrency: `internal/storage/lsmcompact.go`.

---

## 1. The problem

Phase 3 wrote SSTables and never merged them. Three consequences, all unbounded:

- **read cost** — a lookup consulted every file, measured at ~1.5 µs each;
- **space** — every superseded version of every key stayed on disk forever, as did every tombstone;
- **startup** — the file count grew without limit, and so did the work of opening them all.

Compaction fixes all three by rewriting several SSTables into one, dropping what is no longer
reachable on the way through.

## 2. Size-tiered, not leveled

ADR-007 chose size-tiered for v1. The reasons, restated because they are the kind that get
forgotten: it is simpler, it is much easier to prove the tombstone rule correct for, and choosing
leveled compaction on the strength of a blog post rather than a measurement is exactly the
cargo-culting this project exists to avoid. The cost is worse read amplification, which Bloom
filters mitigate (`docs/BLOOM.md`) and which Phase 5 will measure before the decision is revisited.

## 3. The policy

Deterministic, and deliberately dull enough that a test can predict it:

| Level | Trigger | Action |
|---|---|---|
| 0 | file count ≥ `L0CompactionTrigger` (default **4**) | merge **all** of level 0 into one level-1 file |
| *i* ≥ 1 | total bytes > `L1MaxBytes * 10^(i-1)` (default L1 = **64 MiB**, ratio **10x**) | merge **all** of level *i* into one level *i+1* file |

Level 0 is checked first, because a large level 0 is what makes reads expensive and it is the only
level whose trigger is a file count rather than a size.

**A level holding one file is never compacted**, even when it is over budget. Rewriting a single
file at the next level down merges nothing, drops nothing a later compaction would not also drop,
and would repeat all the way down the hierarchy. `TestCompactionIsNotTriggeredByASingleDeeperFile`
pins that.

This policy makes **no claim to be optimal**. It is a v1 policy chosen to be explicable.

### Why "all of the level" is load-bearing

It is not only DESIGN's rule; it is what keeps the read path correct.

The read path consults sources newest-first and stops at the first one holding any version of the
key (`docs/LSM.md` §9). That is correct only because live files have **pairwise-disjoint sequence
ranges**, so sorting by largest sequence descending orders them from newest data to oldest.

Merging a whole level preserves that. A level's contents form a contiguous run of the global
sequence ordering — deeper levels are strictly older, and level 0's files are strictly newer — so
the output's range `[min(inputs), max(inputs)]` contains no live file that was not an input.
Merging an arbitrary *subset* would produce an output whose range straddled a file that is not in
it, and "first source wins" would start resolving keys to the wrong version.

`checkCoherentFileSet` re-checks disjointness at every startup rather than trusting it, and the
merge itself refuses two inputs holding the same internal key.

## 4. The merge

A streaming k-way merge over a binary heap of one cursor per input
(`internal/storage/compaction`).

Memory is one cursor per input plus a reused per-cursor key/value buffer and the output block
buffer. It never materialises the inputs, because the point of compacting an LSM tree is to
reorganise more data than fits in memory — a merge that built a map of the database would pass
every test and then fall over on the first file larger than RAM.

Entries arrive in internal-key order (user key ascending, then sequence descending), so the first
entry for a user key is its newest version and everything after it for that key is obsolete.

**A failing input is not an end of input.** `Merger.Next` returning false means either "finished"
or "failed", and `sstable.WriteFile` checks `Err()` before writing a footer. Without that, an input
whose block failed its checksum would produce a structurally perfect, silently truncated output —
the precise shape of data loss this engine is built to refuse.

## 5. What gets dropped

### Superseded versions: always safe

For each user key the newest version is kept and older ones are discarded. This is unconditional:

- nothing in this engine exposes a snapshot read — `Get` always asks for the newest version — so no
  reader can ever want a superseded value;
- versions living in *older* files outside this compaction are shadowed by the retained newest
  version, because the output's sequence range covers the inputs' and the read path consults files
  newest-first.

### Tombstones: the rule that resurrects data if you get it wrong

A tombstone is not garbage. It is the only thing hiding an older value that may still exist in a
file this compaction is not reading. Dropping it early makes a deleted key come back (INV-S3).

`docs/DESIGN.md` §7 states the rule as *"a tombstone may only be discarded when compacting into the
bottom-most level"*. This engine has no leveled hierarchy in which to look "below", so the rule is
stated in terms of what actually determines age here — sequence numbers:

> **A tombstone may be dropped only when the compaction's input set contains the oldest live data
> in the database**: that is, when no live file outside the input set holds a sequence number below
> the input set's minimum.

Because live files have pairwise-disjoint sequence ranges, the file holding the global minimum is
unique, so the test is exact and cheap: `inputMin == globalMin`.

When it holds, there is no file anywhere that could hold an older value for any key, so the
tombstone hides nothing. When it does not hold, the tombstone is **retained even though it is the
newest version of its key**.

`BottomMost` defaults to false in the merge's options. That is the safe direction: retaining a
tombstone wastes space, dropping one early loses a delete.

**When a tombstone is dropped, the versions behind it are dropped too.** The merge tracks the last
user key it *saw*, not the last it *emitted*; tracking only emissions would let the next older
version through and resurrect the key with a stale value.
`TestDroppedTombstoneStillSuppressesOlderVersions` covers exactly that.

### An empty output is a real outcome

A bottom-most compaction of nothing but tombstones emits nothing at all. The correct result is that
the input files cease to exist — not an empty SSTable, which the engine refuses to open and which
would carry no information. The compaction publishes a MANIFEST edit that only deletes.

## 6. Publication, and every crash window

The commit protocol is `docs/DESIGN.md` §6's, and the ordering is the crash contract:

```
1. merge the inputs into NNNNNN.sst.tmp        no store lock held
2. fsync the file                             bytes on the device before any name refers to them
3. rename to NNNNNN.sst                        atomic on POSIX
4. fsync the directory                         the rename itself becomes durable
5. append AddFile(output) + DeleteFile(inputs) as ONE record, and fsync it
                                               <-- THIS is the instant the file set changes
6. swap the in-memory version pointer          atomic; readers on the old version keep reading
7. unlink the inputs once no reader holds them
```

| Crash point | On disk | What recovery does |
|---|---|---|
| Before 1 | the inputs | Old file set; nothing happened. |
| During 1–2 | a partial `*.sst.tmp` | Swept. The MANIFEST never named it. |
| Between 3 and 5 | a complete, fsynced, **unreferenced** `*.sst` | Swept as an orphan. The old file set is still authoritative — the output is a perfectly valid file that is not part of the database. |
| Between 5 and 7 | output referenced, inputs still present but unreferenced | New file set is authoritative; the inputs are orphans and are swept. |
| After 7 | output only | Nothing to do. |

Every one of those is explainable by the MANIFEST alone. **At no point does recovery have to
guess**, and that is the entire reason the MANIFEST exists (`docs/MANIFEST.md`).

Step 5 being **one record** is what makes it atomic. DESIGN §6 describes appending "one manifest
record group" of AddFile and DeleteFile records; a group of separate records has no atomicity under
§2's framing, and a crash between them would leave the output and all its inputs simultaneously
live — a state no version of the database was ever in. See ADR-010.

## 7. Concurrency

Compaction runs on a background goroutine and must not make writers wait. Phase 3 kept the flush
synchronous deliberately, to avoid adding a scheduler to a phase whose job was to be obviously
correct; compaction does not have that option, because it rewrites whole levels.

```
pick      under mu (read), against an acquired version
merge     under NO store lock at all        <-- the long part
publish   under writeMu, briefly: one manifest append and one pointer swap
```

Three mechanisms hold it together:

- **An immutable, refcounted version.** A read acquires the current version once and holds it for
  the whole operation, so it observes the file set before the compaction or after it, never a
  mixture (INV-S5). Acquiring is one atomic increment — O(1), independent of the file count.
- **The reference count keeps the merge's inputs open.** The merge runs with no lock, and the
  version it holds is what stops those readers being closed underneath it.
- **Retired files are unlinked only when no live version holds them.** `reap` decrements per-file
  counts when a version dies; a file reaching zero is closed and deleted. A crash before that
  leaves an orphan, which costs space and never correctness.

`writeMu` serialises every MANIFEST append, whether it comes from a flush or a compaction, so the
manifest is written by one goroutine at a time. `compactMu` admits one compaction at a time: two
concurrent compactions could pick overlapping inputs, and the second would publish a version
derived from a file set the first had already replaced.

**A file that appears while the merge runs is not disturbed.** The published edit names the inputs
it is deleting explicitly, so a level-0 file added by a concurrent flush carries over untouched
(`TestFlushDuringCompactionIsNotLost`).

**A failed compaction latches**, like a failed flush. The data is intact and still readable, so
reads and writes keep working; what is no longer happening is file-count control, which an operator
needs to know about, so it is exposed through `CompactionError` rather than only logged.

## 8. Measurements

From `TestCompactionMeasurement`, go1.27.1, darwin/arm64. **Development measurements only** —
nothing about the environment is controlled and no number here may be quoted as a result. Phase 5
owns benchmarking.

12,000 mutations over 3,000 distinct keys, L0 trigger 4:

| | Before | After |
|---|---|---|
| Files | 12 | 1 |
| Entries | 12,000 | 2,471 |
| Bytes | 482,615 | 108,989 |

One compaction, 21 ms: 9,051 superseded versions dropped, 478 tombstones dropped, 0 retained
(the input held the oldest data, so it was bottom-most). Output is 0.23× the input bytes.

Startup over 20 files before compaction versus 1 after: see `docs/MANIFEST.md` §7.

## 9. What the tests establish

**Established.**

- The L0 file-count trigger and the deeper levels' size budgets fire where the policy says, and a
  single-file level is never compacted.
- Superseded versions are dropped; the newest survives; logical state is unchanged.
- **INV-S3**: a deleted key never reappears — after compaction, after a restart, across six
  compaction generations each followed by a restart, and under a real SIGKILL mid-compaction.
- A tombstone is **retained** when an older file outside the input set could still hold a value,
  asserted on the retention count rather than only on the read coming back absent. The regression
  test is built so that dropping tombstones too aggressively makes it fail.
- A merge is a correct k-way merge, differentially tested against an independently computed
  reference over 80 generated file sets plus a fuzz target.
- A compaction whose output is empty leaves no file rather than an empty SSTable.
- Every crash window in §6: three by real SIGKILL (classified by reading the MANIFEST, not by
  asking the engine), and the two microsecond-wide ones deterministically, by building the exact
  on-disk state.
- Readers see a coherent file set across repeated compactions, with `ErrNotFound` treated as a hard
  failure because the keys exist continuously. Writers, readers, flushes and compaction run
  together under `-race`.
- A concurrent flush's file survives a compaction's publication.

**Not established, and therefore not claimed.**

- **Any performance claim.** §8 is development measurement.
- **Write amplification over a realistic workload's lifetime.** One compaction of one synthetic
  dataset is not a write-amplification study; that is Phase 5.
- **That this policy is a good one.** It is deterministic and correct. Whether size-tiered beats
  leveled here is exactly the question ADR-007 declined to answer without measurement.
- **Bounded compaction I/O.** There is no rate limiting or I/O budget, so a compaction competes
  freely with foreground work. Listed in `docs/LIMITATIONS.md`.

## 10. Limitations

| Limitation | Removed in |
|---|---|
| No rate limiting or I/O budget: a compaction competes freely with foreground reads and writes | Phase 5, if measured to matter |
| One compaction at a time; no parallel compaction across levels | deferred |
| A compaction's output is a single file, however large, so one SSTable's size grows with the dataset. The metadata-block bound (`sstable.MaxMetaBlockSize`, 256 MiB) is the practical ceiling | deferred; output splitting would need a target-file-size policy |
| No leveled compaction | ADR-007, revisited with Phase 5 measurements |
| Tombstone dropping is conservative: it uses the whole input set's position in the sequence ordering rather than per-key range checks, so some tombstones outlive their usefulness | deferred; the correct-but-conservative rule is the one that is easy to prove |
| The WAL is still never truncated, so replay scans every mutation ever written even though compaction has long since absorbed most of them | a later phase; `SetLogNumber` is recorded but not acted on |
