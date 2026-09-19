# MANIFEST

Status: **Phase 4 — implemented and verified.** Every property stated here is bound to a test
named in `docs/INVARIANTS.md`. Where something is *not* established, this document says so.

Format: `docs/DESIGN.md` §6. Implementation: `internal/storage/manifest`, used by
`internal/storage/lsmstore.go`.

---

## 1. The problem

Phase 3 answered "which files make up the database?" by listing the directory and reading every
`*.sst` it found. That works exactly as long as nothing ever removes a file.

Compaction removes files. And the moment it can, the directory stops being able to answer the
question. After a crash midway through a compaction, **both the inputs and the output are on
disk**, all of them complete and valid:

```
000001.sst  000002.sst  000003.sst   the inputs
000004.sst                           the output, a complete merge of all three
```

A directory scan sees four files and has to guess. Adopt all four and every superseded version and
every dropped tombstone in the inputs comes back — deleted keys resurrect. Adopt only the newest
and any key not in the output is lost. There is no rule over directory contents that is right in
both directions, because the information needed — *was this compaction committed?* — is simply not
in the directory.

The MANIFEST is where that information lives.

## 2. What it is

An append-only log of **version edits**. The live file set is whatever replaying it produces.

```
<data-dir>/CURRENT            one line: the name of the live manifest
<data-dir>/MANIFEST-000007    an append-only record stream of version edits
<data-dir>/000004.sst         SSTables — live only if the MANIFEST names them
<data-dir>/wal/000001.log     the write-ahead log (docs/WAL.md)
```

**A file on disk that the MANIFEST does not name is not part of the database**, however complete it
looks. That single rule is what makes every compaction crash window explainable (INV-S6).

`CURRENT` is replaced atomically — write `CURRENT.tmp`, fsync it, `rename` over `CURRENT`, fsync the
directory — so a reader sees either the old name or the new one and never a partial one. `rename(2)`
is atomic on POSIX.

## 3. One record is one edit

This is the one place the Phase 0 design was underspecified in a way that mattered, and ADR-010
records the resolution.

`docs/DESIGN.md` §6 lists `AddFile`, `DeleteFile` and the `Set*` operations as record kinds, and
describes the compaction commit as appending *"one manifest record group"*. **A group of separate
records is not atomic.** The framing in §2 has no grouping primitive, so a crash between the
`AddFile` and the `DeleteFile` of one compaction would leave a manifest in which the output *and*
all of its inputs are live — a state no version of the database was ever in, and one that
`Apply` would have no way to recognise as wrong.

So an edit is **exactly one framed record**, and its CRC covers the whole edit. Either the entire
version change replays or none of it does. This is the same reasoning, and the same fix, as the
WAL's write batch (`docs/WAL.md` §2): make the unit of atomicity the unit of checksumming.

DESIGN's operation numbers are preserved as **field tags inside the payload**:

| Tag | Field | Payload |
|---|---|---|
| `0x01` | AddFile | `level`, `fileNum`, `size`, `numEntries`, `smallestKey`, `largestKey`, `smallestSeq`, `largestSeq` |
| `0x02` | DeleteFile | `level`, `fileNum` |
| `0x03` | SetNextFileNum | `u` |
| `0x04` | SetLastSequence | `u` |
| `0x05` | SetLogNumber | `u` |
| `0x06` | SetApplied | `raftIndex`, `raftTerm` |

Integers are uvarints and byte strings are uvarint-length-prefixed, matching the WAL's payload
convention (`docs/DESIGN.md` §3, "all lengths are uvarint") rather than introducing a second style
in the same codebase.

**Decoding is strict**, in the way `wal.DecodeBatch` is strict. An unknown tag is an error, not a
field to skip: a manifest written by a future version is not something this version can safely
half-understand, and a lenient decoder in the log that defines what the database *consists of*
turns damage into a plausible-looking file set.

### Why the sequence range is in there

`AddFile` carries each file's sequence range, and that is what earns the MANIFEST its keep on the
startup path. Phase 3 had to read every byte of every SSTable to recover it, because there was
nowhere on disk to write it down (`docs/LSM.md` §8). Now it is recorded, and startup cross-checks
each file's footer against the MANIFEST's record of it instead — see §6.

## 4. What is not in it

**`SetLogNumber` round-trips but is not acted on.** Retiring WAL segments a flush has superseded is
a later concern; deleting a log on the strength of a field nothing yet tests would be the wrong
order of operations. The WAL is still never truncated (`docs/LIMITATIONS.md`).

**`SetApplied` is recorded, but the WAL remains the authority** for the applied index, as in Phases
2 and 3. The snapshot written at each open records the recovered value so the field is meaningful
and round-trips, but nothing reads it back in preference to the log. Two authorities for one number
would be worse than having the wrong one.

## 5. The publication protocol

The whole point of the MANIFEST is the transition between file sets. For a compaction
(`docs/DESIGN.md` §6, `docs/COMPACTION.md` §6):

```
1. write the output to NNNNNN.sst.tmp
2. fsync it
3. rename to NNNNNN.sst
4. fsync the directory
5. append AddFile(output) + DeleteFile(inputs) as ONE record; fsync the manifest
                                       <-- the file set changes HERE and nowhere else
6. swap the in-memory version pointer
7. unlink the inputs once no reader holds them
```

A flush is the same protocol with no deletions.

Every crash leaves a state that the MANIFEST alone explains:

| Crash point | On disk | Recovery |
|---|---|---|
| During 1–2 | partial `*.sst.tmp` | Swept. Never named, never read. |
| Between 3 and 5 | complete, fsynced, **unreferenced** `*.sst` | Swept as an orphan. The old file set is still the database. |
| Between 5 and 7 | output referenced; inputs present but unreferenced | The new file set is the database. The inputs are orphans and are swept. |
| After 7 | output only | Nothing to do. |

The asymmetry is deliberate: a file becomes live at exactly one instant (the fsync in step 5), and
before that instant it does not exist as far as the database is concerned, no matter how complete it
is on disk.

## 6. Startup

```
1. read CURRENT -> replay the MANIFEST it names
       -> live file set, each file's sequence range and level, nextFileNum, lastSequence
2. open every referenced file; cross-check it against the MANIFEST
3. check the file set is coherent: sequence ranges disjoint and ascending, no empty tables
4. sweep orphans: *.sst.tmp, *.sst the MANIFEST does not name, superseded MANIFESTs, CURRENT.tmp
5. install a fresh MANIFEST holding a full snapshot; point CURRENT at it; delete the old ones
6. replay the WAL from sequence 1, skipping every mutation at or below the highest flushed sequence
```

**Step 2's cross-check is what makes step 6 of Phase 3 unnecessary.** The MANIFEST records each
file's size and entry count, and the file's own footer records them independently. A disagreement
means one of the two is damaged, and it is caught for the price of a 48-byte read
(`TestManifestDisagreeingWithAFileIsRefused`). Reading every data block to rediscover what is
already written down is the duplicated work Phase 4 removes.

The cost of that choice, stated rather than hidden: **damage inside a data block is found at the
read that needs it, not at startup.** It is still found, and still reported as corruption rather
than as a missing key (INV-L7). `Options.VerifySSTablesOnOpen` restores the Phase 3 behaviour for
operators who would rather pay at startup, and `TestCorruptSSTableRefusesToOpen` pins both halves
so neither can drift silently.

**Step 4 is after step 1 for a reason.** Deleting a file because it is absent from a file set we are
not yet sure of would be the one way this could lose data. Orphan sweeping is safe only once the
MANIFEST has been recovered successfully.

**Step 5 is what bounds a manifest's length.** Without it a manifest would accumulate every edit
for the life of the database and recovery time would track total history rather than current state.
Installing a fresh one on every open means recovery replays one snapshot plus one session's edits.
A crash partway through leaves the previous `CURRENT` and manifest untouched and the new manifest
unreferenced — an orphan, swept next time.

The new manifest's number is chosen above every manifest **on disk**, not merely above the one
`CURRENT` named. A leftover manifest — from an interrupted `Install`, or from a directory whose
`CURRENT` was lost — would otherwise collide with the `O_EXCL` create and the store would not open
at all.

## 7. Recovery policy

Identical to the WAL's, because the failure is identical (`docs/WAL.md` §7, INV-S8):

| Situation | Action |
|---|---|
| Torn or bad-checksum record **at the end** of the manifest | A crash during that append. The edit was never fsynced, so it never took effect and nothing that depended on it was acknowledged. Truncate to the last good boundary and continue. |
| Damage **anywhere else** | Not explainable by a crash. Refuse to open. Skipping an edit in the middle would apply a version history that never happened. |
| A refused recovery | Leaves the bytes byte-for-byte unmodified, so the data stays available to investigate. |

Specific refusals, each with a test:

| Condition | Why it is fatal |
|---|---|
| `CURRENT` names a manifest that does not exist | `CURRENT` is only ever pointed at a manifest that was already fsynced. Not a state the protocol can produce. |
| `CURRENT` holds anything but a manifest file name | It is one short line this code writes. Guessing which manifest was meant is the reconstruction the MANIFEST exists to make unnecessary. |
| The manifest holds no usable edits | Every manifest begins with a snapshot, written and fsynced before `CURRENT` could name it. An empty one was replaced or emptied. |
| An unknown record kind or field tag | A format this version does not understand, not something to half-apply. |
| An edit that does not compose — deleting a file that is not live, adding one that already is | The manifest does not describe a history this engine could have produced. |
| A referenced SSTable is missing from disk | The publication protocol never removes a referenced file. |
| SSTables present but **no `CURRENT`** | See §8. |

## 8. A directory with SSTables and no CURRENT

Refused, not adopted.

Nothing on the filesystem distinguishes a Phase 3 database from a Phase 4 database whose `CURRENT`
was lost. Adopting automatically would mean that deleting one small file silently returns the engine
to inferring the file set from the directory — which would make INV-S6 a suggestion rather than an
invariant, and would quietly reintroduce exactly the guessing described in §1.

`Options.AdoptLegacySSTables` is the explicit opt-in: it scans those files the way Phase 3 did,
recovers each one's sequence range by reading it in full, and writes a MANIFEST describing them.
That is a deliberate operator decision, taken once, rather than a silent fallback.

An **empty** directory is a different case and needs no ceremony: it is a database that does not
exist yet, and one is created.

## 9. Measurements

From `TestStartupMeasurement`, go1.27.1, darwin/arm64. **Development measurements only** — nothing
about the environment is controlled and no number may be quoted as a result.

Same 20,000 keys, 20 SSTables:

| Arm | Data blocks read at startup | Elapsed | Files opened |
|---|---|---|---|
| MANIFEST metadata only (default) | **0** | ~20.0 ms | 20 |
| Full verification (Phase 3 behaviour) | 3,340 | ~26.9 ms | 20 |
| After compaction | **0** | ~19.9 ms | 1 |

The block count is the claim; the milliseconds are incidental. Note that the elapsed times are
dominated by WAL replay, which is unchanged: the WAL is never truncated, so every arm still replays
all 20,000 mutations and skips them. That is the remaining startup cost and it is a listed
limitation, not something this phase fixed.

## 10. What the tests establish

**Established.**

- An edit round-trips through encode/decode with every field intact, deterministically, and a fuzz
  target holds the property over generated input.
- Every malformed payload shape is refused with `ErrCorrupt`: unknown tags, truncation at each
  field, overrunning lengths, inverted sequence ranges, absurd levels. `DecodeEdit` never panics on
  arbitrary bytes.
- A torn final record is repaired, the truncated edit does not take effect, the file is truncated on
  disk so the next append cannot follow garbage, and a second recovery is stable.
- Mid-file damage is refused and the file is left unmodified.
- Every §7 refusal, including nine different garbage values in `CURRENT`.
- The live file set, with levels, survives restart; orphan SSTables, temp files, superseded
  manifests and `CURRENT.tmp` are all swept; a referenced-but-missing file is fatal.
- A fresh manifest is installed on every open, asserted through the edit count on the following
  recovery — so a manifest that started accumulating would fail the test.
- The default startup reads zero data blocks, and `VerifySSTablesOnOpen` reads them all.
- Under a real SIGKILL mid-compaction, the MANIFEST is read directly (not via the engine) to
  classify what the crash left, and every attempt recovers every acknowledged write and sweeps
  every orphan.

**Not established, and therefore not claimed.**

- **Power-loss durability**, in any mode. The crash tests destroy a process, which proves the bytes
  reached the kernel, not the platter. `docs/FAILURE_MODEL.md` §4.
- **That a missing oldest WAL segment is detectable.** The MANIFEST has a `SetLogNumber` field and
  Phase 4 does not act on it, so the Phase 2 gap (`docs/WAL.md` §10) is unchanged.
- **Recovery from a MANIFEST written by a different format version.** It is refused, not migrated.
- **Any bound on manifest size for a single long-running session.** It is bounded across restarts by
  the snapshot-on-open, not within one session.

## 11. Limitations

| Limitation | Removed in |
|---|---|
| The WAL is never truncated, so startup replays every mutation ever written even though compaction absorbed most of them long ago. `SetLogNumber` is recorded but not acted on | a later phase |
| Segments missing from the *start* of the WAL remain undetectable | a later phase, using `SetLogNumber` |
| A manifest grows within one session (one record per flush and per compaction) and is only compacted by reopening | deferred; a within-session rotation threshold would be the fix |
| A Phase 3 directory needs an explicit opt-in to open | deliberate, §8 |
| Damage inside a data block is found at read time rather than at startup, unless `VerifySSTablesOnOpen` is set | deliberate trade, §6 |
| Orphan sweeping deletes only files this engine recognises (`*.sst`, `*.sst.tmp`, `MANIFEST-*`, `CURRENT.tmp`); anything else in the directory is left alone | deliberate — the engine does not own the whole directory |
