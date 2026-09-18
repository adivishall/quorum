# WRITE-AHEAD LOG

Status: **Phase 2 — implemented and verified.** Everything stated here is backed by a test
named in `docs/INVARIANTS.md`. Where a property is *not* established, this document says so
rather than leaving it to be assumed.

---

## 1. What problem it solves

Phase 1's store held everything in a Go map. When the process exited, the database ceased to
exist. That is fine for a data structure and useless for a database.

The write-ahead log fixes exactly one thing: **a write that was acknowledged must still be
there after the process dies.** It does that by making the log, not memory, the authority. A
mutation is appended to the log *before* it becomes visible in memory; on restart the log is
replayed to rebuild the state that was durable at the moment of the crash.

"Write-ahead" is the whole idea: the record describing what you are about to do is durable
before you do it. Then any crash leaves the system in a state that the log can explain.

---

## 2. What this is not

Phase 2 is a WAL and nothing else. There is **no memtable, no SSTable, no compaction, no
Bloom filter, no log truncation**. Consequences, stated plainly because they are real:

- The whole live data set is held in memory. A dataset larger than RAM will not fit.
- The whole log is replayed on every open. Startup time grows with total bytes ever written,
  not with the size of the live data set.

Both are fixed by the memtable/SSTable engine in Phase 3 and log truncation in Phase 4. They
are documented in `docs/LIMITATIONS.md` as current limitations, not as design decisions.

---

## 3. Record format

The framing is shared with the Raft log and the MANIFEST and lives in `internal/record`
(`docs/DESIGN.md` §2). Little-endian throughout.

```
offset  size  field
0       4     crc32c(length || kind || payload)
4       4     length   (payload byte count)
8       1     kind
9       N     payload
```

Maximum payload: 64 MiB. The checksum covers the length and kind bytes as well as the
payload, so a corrupted length is detected rather than acted on — but it must still be
**range-checked before use**, because the checksum cannot be verified until the payload has
been read, and a corrupt length would otherwise drive a wild allocation.

### WAL record kinds

| kind | meaning |
|---|---|
| `0x01` | `WriteBatch` |
| `0x02` | `AppliedIndex` |

**WriteBatch payload**

```
count      uvarint
count × {
    kind     u8          0x00 = tombstone (delete), 0x01 = value (put)
    keylen   uvarint
    key      keylen bytes
    vallen   uvarint     ] absent entirely for a tombstone
    value    vallen bytes ]
}
```

The operation kinds are the same values as the internal-key value types in `docs/DESIGN.md`
§1, so the two encodings agree when the LSM engine arrives.

A batch is **one framed record**, so its checksum covers every operation in it. Either the
whole batch replays or none of it does; there is no state in which half a batch survived.
Phase 2 only ever writes single-operation batches, because the `Store` API exposes only
single-key mutations — the multi-operation form exists and is tested because compaction and
Raft apply will use it.

**AppliedIndex payload**: `raftIndex u64`, `raftTerm u64`. Exactly 16 bytes; any other length
is corruption, not a newer format.

### Decoding is strict

Every length is checked against the bytes that actually remain. Unknown operation kinds,
empty keys, zero operation counts, and leftover trailing bytes are all errors. A lenient
decoder in a write-ahead log turns corruption into plausible-looking state, which is worse
than refusing to start: the operator gets a database that came up fine and is quietly wrong.

---

## 4. Segmentation

Segments are `wal/%06d.log`, numbered from 1.

| Property | Value |
|---|---|
| Rotation threshold | 16 MiB (`DefaultSegmentSize`) |
| Record splitting | never — a record always lands whole in one segment |
| Active segment | the highest-numbered one |
| Ordering | by parsed segment number |
| Non-segment files | ignored (`.DS_Store`, editor swap files, anything not exactly six digits + `.log`) |
| Gaps in the sequence | **refused** — `ErrCorrupt` |

**The 16 MiB figure is a choice, not a measurement.** It is large enough that rotation is
rare and small enough that one segment is quick to scan and cheap to reclaim once Phase 4
adds truncation. Phase 5 is where a number earns the right to be called tuned. Because
records are never split, a segment can exceed the threshold by up to one record — a 4 MiB
value produces a 4 MiB segment.

A gap is refused because nothing deletes segments yet, so a missing one means records are
gone; replaying the survivors would produce a state that never existed, with a later write
applied without the earlier write it overwrote.

**Known gap in that check:** segments missing from the *start* of the sequence are
undetectable, because nothing records which segment number the log begins at. That metadata
is the MANIFEST's log number in Phase 4.

---

## 5. Sync modes

The three modes differ **only in when `fsync` happens, never in when `write(2)` happens.**
Every append completes a `write(2)` before it returns, in every mode. Nothing is buffered in
user space — a record sitting in a `bufio.Writer` would be lost on `SIGKILL`, which would
make the batch-mode guarantee below false.

| `wal.sync` | Flush behaviour | Survives process kill (SIGKILL) | Survives OS crash / power loss |
|---|---|---|---|
| `off` | never | **yes** (tested) | no |
| `batch` (default) | every 100 ms or 1 MiB, whichever first | **yes** (tested) | may lose up to that window — *untested* |
| `sync` | before the append returns | **yes** (tested) | claimed, **not tested** — see §9 |

On Darwin, `sync` mode uses `fcntl(F_FULLFSYNC)`, because `fsync(2)` there does *not* flush
the drive's own volatile write cache. Support is probed once at open and reported through
`Stats().FullSyncEnabled`, so a filesystem that lacks it is a fact you can read rather than an
assumption. On Linux and the BSDs `fsync(2)` is the full flush.

A failed background flush **latches**: every subsequent append fails with that error.
Continuing to accept writes after the log can no longer honour its durability promise would
mean acknowledging writes that may not survive, which is the worst possible response.

### The cost, measured

Apple M4, macOS 26.5.2, Go 1.27.1, APFS on internal SSD. 100-byte values, single-operation
batches. Indicative only — Phase 5 does benchmarking properly.

| Mode | ns/op | approx. appends/s |
|---|---|---|
| `off` | 3,879 | 258,000 |
| `batch` | 5,456 | 183,000 |
| `sync` | 3,870,493 | 258 |

`sync` is roughly **700× slower**. That is the real price of a device-level flush per write,
and it is why `batch` is the default and why group commit is an obvious Phase 5 candidate.

---

## 6. The append path

```
Store.Put(key, value)
   │
   ├─ validate                     reject bad input before anything is logged
   ├─ encode as a one-operation batch
   ├─ append to the WAL            write(2) — the bytes reach the kernel
   ├─ flush per the sync mode      now, later, or never
   ├─ publish to the in-memory map
   └─ return nil
```

**In-memory state is updated only after the log write succeeded, never before.** Reversing
those two steps would let a failed or interrupted log write leave a value visible in memory
that the log does not contain, so a restart would silently lose a write the client was told
had succeeded.

The opposite skew is possible and is harmless: a crash between the append and the publish
leaves a record in the log that was never acknowledged, and replay applies it. That is
inherent to write-ahead logging — a client whose request was interrupted genuinely cannot
know whether it took effect — and it is stated in `docs/CONSISTENCY.md` C4.

### Locking

Two locks, always in this order:

- `writeMu` serialises writers and is held across **both** the log append and the in-memory
  publish. Without that span, two concurrent `Put`s to one key could be logged A-then-B and
  applied B-then-A, and the state after a restart would differ from the state before it.
- `mu` guards the map. Writers hold it only for the publish, so a reader never waits on an
  fsync.

The cost is stated rather than hidden: in `sync` mode the flush happens under `writeMu`, so
concurrent writers queue behind it.

---

## 7. Replay

Segments in ascending numeric order; records within a segment in file order. Replay is a pure
function of the bytes on disk: no clock, no map iteration, no goroutines, no randomness.
Replaying the same log any number of times produces the identical sequence (INV-S2).

Replay and the live write path share **one** apply implementation, so recovered state cannot
drift from live state through two subtly different code paths.

Measured replay throughput on the machine above: 20,000 records in ~1.2 ms (~2.1 GB/s).
Startup cost is proportional to the *entire* log, which is the limitation Phase 4 removes.

---

## 8. Corruption policy

This is the part that matters most, and the asymmetry is the whole point:

| Situation | Action |
|---|---|
| Fewer than 9 bytes remain | clean end of file; any stray bytes are truncated if in the newest segment |
| Record extends past EOF | **torn tail** |
| Checksum fails and the record ends exactly at EOF | **torn tail** |
| Checksum fails and bytes follow the record | **corrupt** |
| Declared length > 64 MiB | **corrupt** |
| All-zero record header, zeros to EOF | **torn tail** |
| All-zero record header with real data after it | **corrupt** |
| Unknown record kind | **corrupt** |
| Undecodable batch payload | **corrupt** |
| Missing segment in the sequence | **corrupt** |

and then:

```
torn tail in the NEWEST segment  -> truncate to the last good boundary, continue
anything else, anywhere else     -> ErrCorrupt, refuse to open
```

**Why an older segment is never repaired:** an older segment was completed and closed before
its successor was created, so a crash cannot have been writing to it. Damage there is
something other than a crash, and there is no reason to trust the rest of it.

**Why there is no resynchronisation:** the framing has no block structure, so "skip the bad
record and continue" really means "guess where the next record starts". Guessing wrong
produces a database that opens cleanly and is silently missing committed writes. Refusing is
loud; guessing is not.

**A refused open modifies nothing.** The bytes stay on disk so they can be investigated or
recovered by hand.

Errors carry the file, the byte offset, and a reason — never the payload bytes, because a
corrupt record is arbitrary data and pasting it into a log is how binary garbage ends up in a
terminal.

### What corruption detection does not cover

The tail/corrupt distinction is decided from the record's **extent** — where it ends relative
to EOF. That is sound whenever the length field survived. **If the length field is itself
corrupted in a way that still passes the 64 MiB range check, the classification can be
wrong**, and a mid-log corruption in the newest segment could be truncated as a tail. A
block-fragmented format (LevelDB's 32 KiB blocks with FIRST/MIDDLE/LAST fragment types) gives
a resync point that removes this ambiguity; we deliberately chose the simpler framing, and
this is its cost.

---

## 9. What crash testing actually established

`tests/integration/crash_test.go` starts a **real child process**, has it perform writes that
each return `nil`, then destroys it with `SIGKILL` — no flush, no `Close`, no deferred
functions. The test verifies the child really died by signal, not by exiting. Reopening the
directory must produce every acknowledged write.

Results across the modes: every acknowledged write survived, including with `sync=off`.

That last result is the one to understand. **`sync=off` losing nothing on SIGKILL is not
evidence of durability.** It survived because the bytes had already been handed to the kernel
by `write(2)`, and the kernel outlives the process. Process death and power loss are
different failures, and only the first is being tested.

A second test kills a process mid-write-storm at varying moments and asserts the recovered
state is a **contiguous prefix** of the submitted sequence — some suffix of in-flight writes
may be missing, but there must be no hole. A hole would mean replay skipped a record.

**An observation worth recording:** across 24 real SIGKILL runs, **not one produced a torn
tail.** That is not luck. Each record is a single `write(2)`, and a signal does not abandon a
syscall part-way — the write either completes or never starts. Torn tails come from OS
crashes and power loss, not from process kills. The truncation path is therefore exercised by
deliberately corrupting files in unit tests, and **is not covered by any real-crash test**.
It is correct as far as those tests go, and its real-world trigger is a scenario we cannot
reproduce here.

**Not tested, and therefore not claimed:** power-loss durability in any mode. That needs the
data to have reached the physical device, which no userspace test on a laptop can verify. See
`docs/FAILURE_MODEL.md` §4.

---

## 10. Limitations

| Limitation | Removed in |
|---|---|
| Entire live data set held in memory | Phase 3 (memtable + SSTables) |
| Entire log replayed on every open; startup grows with total bytes ever written | Phase 4 (truncation via the MANIFEST log number) |
| No log truncation, so the WAL grows without bound | Phase 4 |
| Segments missing from the *start* of the sequence are undetectable | Phase 4 (MANIFEST log number) |
| Mis-classification possible if a length field is corrupted within range | inherent to this framing; see §8 |
| `sync` mode serialises writers behind the flush | Phase 5 may add group commit, if measured to matter |
| Power-loss durability untested in every mode | not testable here |
