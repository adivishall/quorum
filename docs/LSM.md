# LSM STORAGE ENGINE

Status: **Phase 3 — implemented and verified.** Every property stated here is bound to a test
named in `docs/INVARIANTS.md`. Where something is *not* established, this document says so
rather than leaving it to be assumed.

This document covers the memtable, the SSTable, the flush, the read path and recovery.
The write-ahead log underneath it all is `docs/WAL.md`; the formats are `docs/DESIGN.md`
§1 and §4.

---

## 1. Why this exists

Phase 2 made writes durable and left one thing untouched: the live data set was still a Go
map. The log was the authority, memory was the cache, and the cache had to hold everything.
Two consequences, both fatal past a certain size:

- a dataset larger than RAM would not fit;
- startup replayed the entire log, so restart time grew with total bytes ever written rather
  than with the size of the data.

An LSM tree fixes both by writing memory out to immutable sorted files. Writes stay
sequential — an append to the log and an insert into an in-memory structure — and the cost
of organising data on disk is paid in the background, in bulk, on immutable files that no
reader is ever holding.

The trade is stated plainly because it is real: **reads get more expensive**. A key may live
in the memtable or in any SSTable, so a lookup may have to ask several places. Phase 4's
Bloom filters and compaction are what make that cheap; Phase 3 pays the full price and
measures it (§10).

---

## 2. Shape

```
        Put / Delete
             │
             ▼
     ┌───────────────┐        every mutation is logged before it is visible
     │      WAL      │        (docs/WAL.md)
     └───────┬───────┘
             ▼
     ┌───────────────┐        ordered skip list, in memory
     │   MemTable    │
     └───────┬───────┘
             │  full → freeze
             ▼
     ┌───────────────┐        readable, no longer written to
     │ immutable MT  │
     └───────┬───────┘
             │  write, fsync, rename
             ▼
     ┌───────────────┐        immutable, sorted, checksummed
     │   SSTable     │
     └───────────────┘

Get: MemTable → immutable MemTable(s) → SSTables, newest first, first hit wins.
```

On disk:

```
<data-dir>/
  wal/000001.log      the write-ahead log (docs/WAL.md)
  000001.sst          SSTables, numbered in flush order
  000002.sst
  000003.sst.tmp      a flush that was interrupted; deleted at startup
```

---

## 3. Internal keys

The engine never stores a bare user key. It stores an **internal key**
(`docs/DESIGN.md` §1, `internal/storage/ikey`):

```
internal_key = user_key || seq(7 bytes, big-endian) || kind(1 byte)
kind: 0x00 = TOMBSTONE, 0x01 = VALUE
```

Ordering:

1. user key ascending
2. for equal user keys, sequence number **descending**
3. for equal sequence numbers, kind descending

Rule 2 is the one that does the work. Because a forward scan reaching a user key yields its
newest version first, every read in the engine is *first match wins* — no version
bookkeeping in the memtable, none in an SSTable, and none in the k-way merge that Phase 4's
compaction will run over overlapping files.

Rule 3 cannot fire in data this engine writes, because a sequence number is consumed by
exactly one mutation. It is specified so that the comparison is a total order over any bytes
of the right shape, including bytes that arrived from a damaged file.

**The comparison is not `bytes.Compare` over the whole key**, and cannot be. The trailer has
to sort descending and a big-endian integer sorts ascending bytewise. Since the trailer is
exactly eight bytes, reading it big-endian gives `(seq<<8)|kind`, and sorting that one number
descending yields rules 2 and 3 together.

A **seek key** is the same encoding with the kind byte set to `0xff`, a value no real entry
carries. It therefore sorts at or before every real version of that user key with a sequence
number at most `seq`, which is what makes "seek, then take the first entry if its user key
matches" correct.

---

## 4. Sequence numbers

A sequence number is a 56-bit counter. Phase 3 defines its behaviour completely, because
Phase 9 has to inherit the same semantics for replicas to agree.

| Question | Answer |
|---|---|
| Initial value | `0`, meaning *no mutation has been assigned*. The first mutation gets `1`. |
| Increment point | Once per mutation, under `writeMu`, immediately before the WAL append. |
| What counts as a mutation | One `Put` or one `Delete`. A delete of an absent key counts: it writes a tombstone like any other. |
| What does not | Reads. Rejected operations (bad key, oversized value, cancelled context, closed store) — they fail before the counter moves. `SetAppliedIndex`, which is metadata, not data. |
| Batches | Each operation in a batch gets its own number, assigned in batch order. Phase 3 only ever writes single-operation batches; the rule is stated because the format allows more. |
| Persistence | **Not stored.** It is re-derived by replay. |
| Recovery | Replay walks the WAL in order and assigns `1, 2, 3, …` to the mutations it finds — which is the identical assignment the original writes received, because both walk the same records in the same order. |
| Exhaustion | A store that reaches `2^56-1` refuses further mutations rather than wrapping. |

That last property is what the whole Phase 3 recovery argument rests on. Because replay
reproduces the original numbering exactly, the predicate

> this mutation's sequence number is at or below the highest sequence in an SSTable

is decidable at startup, and it means exactly *this mutation is already durable in a table*.

**What this is not yet.** There is no Raft, so "identical committed log prefix → identical
sequence assignment" (INV-S4) is only established here for a single node replaying its own
log. The cross-replica claim stays PLANNED until Phase 12 can compare two replicas.

---

## 5. MemTable

`internal/storage/memtable` is a Pugh skip list keyed by internal key: maximum height 12,
branching factor 4, standard search-and-drop-a-level insert.

**Why not a map.** A map serves point lookups perfectly well and cannot serve the operation
the engine actually needs — ordered iteration. A flush emits every entry in internal-key
order, and Phase 4's compaction merges several such streams. With a map, a flush would have
to sort at flush time, with an allocation per key, while writers were blocked.

**Height generation.** A per-memtable xorshift64 seeded from a constructor argument, never
`math/rand`'s global source. Two reasons: the global source is shared mutable state that the
race detector would rightly object to, and a fixed seed makes a failing test reproduce
identically instead of "sometimes". Heights affect only performance — a skip list's iteration
order is determined entirely by its keys — and a test asserts that four different seeds
produce byte-identical ordered content.

**Concurrency.** An `RWMutex`, not a lock-free list. A lock-free skip list is a Phase 5
optimisation that has to be justified by a measurement first. What the structure does give
for free is that nodes are never removed and a node's key never changes after insertion, so
an iterator may release and re-take the read lock between steps and still never observe a
torn entry or an out-of-order key. An iterator over a *frozen* memtable is a stable snapshot;
one over a live memtable may or may not observe concurrent inserts.

**Memory accounting** is approximate: key bytes, value bytes, and a fixed per-node allowance
for the struct and its tower. It deliberately over-counts rather than under — a memtable that
believed itself smaller than it is would grow past its budget, and the claim that memory is
bounded would be false.

**Versions are kept, not collapsed.** Three writes to one key are three entries. Only the
read path collapses them, and it does so by ordering, not by bookkeeping.

---

## 6. SSTable

`internal/storage/sstable`, format from `docs/DESIGN.md` §4:

```
+------------------+
| data block 0     |   4 KiB target, closed on the entry that reaches it
| data block 1     |
| ...              |
+------------------+
| filter block     |   EMPTY in Phase 3 — see below
+------------------+
| index block      |   one entry per data block
+------------------+
| footer (48 B)    |
+------------------+
```

Every block is followed by a four-byte `crc32c` of its bytes. Lengths recorded in the index
and the footer exclude that checksum.

**Data block entry:** `keylen uvarint | internal key | vallen uvarint | value`. No prefix
compression and no restart points: `docs/DESIGN.md` §11 defers both until a benchmark asks
for them. A tombstone is a zero-length value; so is a present key whose value is empty. The
two are distinguished by the kind byte inside the internal key, never by the value.

**Blocks end on entry boundaries.** A block is closed once it *reaches* the target, so it may
exceed it by up to one entry, and an entry larger than the target (a 1 MiB value) is never
split across blocks.

**Index entry:** `keylen uvarint | last_key_of_block | offset uvarint | length uvarint`.
A lookup binary-searches the index for the first block whose last key is at or after the seek
key, then scans that block linearly. At a 4 KiB block that is a few dozen comparisons on one
cache-resident buffer.

**Footer**, fixed 48 bytes at `filesize-48`, six little-endian `uint64`s:
`filter_offset | filter_length | index_offset | index_length | num_entries | magic`.
The magic is `0x444B565353543031`, which read big-endian spells `DKVSST01`; because the field
is little-endian its bytes appear reversed in a hex dump. The constant kept its original
spelling through the rename to Quorum deliberately — churning a format constant for cosmetic
reasons is what format versioning exists to prevent.

### The filter block is empty

The layout reserves a filter block because the on-disk format must be stable before files
exist that later versions have to read. **Phase 3 writes it with length zero.**

A zero-length filter means *this file carries no filter; consult it directly*. It is not a
filter that always answers "maybe", and **no read in Phase 3 is accelerated by it**. Bloom
filters are a Phase 4 deliverable and INV-S7 stays PLANNED until then.

Its checksum is still written and still verified at open. A region of the file that no check
covers is a region where damage goes unnoticed, and "corruption is detected" would then be
true only of the parts that happened to be looked at.

### What the reader validates

At `Open`, without reading a data block:

- the magic — checked first, and a mismatch is `ErrBadMagic`, not `ErrCorrupt`. `DESIGN` §4
  draws that distinction and it is a different diagnosis: *not one of our files, or a format
  we do not know* versus *our file, damaged*.
- the footer's internal consistency, in arithmetic that cannot overflow. Every offset and
  length comes from disk and is about to size a read, so it is range-checked before use, for
  the same reason `record.MaxRecordSize` exists.
- the filter and index blocks' checksums.
- the index's own claims against the file: data blocks contiguous from offset zero, ending
  exactly where the filter block begins, with strictly increasing last keys.

At `Verify` — a full read, which the engine performs on every file at startup (§8):

- every data block's checksum;
- every entry decodable, with lengths that fit inside the block;
- entries strictly increasing;
- the entry count matching the footer;
- the index's last key matching the file's last key.

**A read never degrades corruption into "key not found."** `Get` has three outcomes —
absent, value, tombstone — and an error is none of them. Reporting a damaged file as an
absent key turns corruption into an ordinary answer nobody investigates, and it is the
single failure mode the corruption tests are aimed hardest at.

The corruption tests flip **every byte of a real SSTable in turn** and require that `Open` or
`Verify` rejects each one.

---

## 7. Flush

```
memtable is full (Options.MemTableSize, default 4 MiB)
   │
   ├─ freeze it, install a fresh one     writers continue immediately
   ├─ write entries to NNNNNN.sst.tmp    ordered iteration, no sort step
   ├─ fsync the file                     bytes are on the device
   ├─ rename to NNNNNN.sst               atomic on POSIX
   ├─ fsync the directory                the rename itself becomes durable
   └─ publish                            one critical section: add the table,
                                         drop the immutable memtable
```

Each step is chosen for what a crash immediately after it leaves behind:

| Crash point | On disk | Recovery |
|---|---|---|
| Before the flush starts | WAL only | Replay rebuilds the memtable. |
| During the write | a partial `*.sst.tmp` | Startup deletes it. Its contents are still in the WAL, so nothing is lost. |
| Between rename and directory fsync | either the temp name or the final name | Temp name → swept. Final name → a complete, fsynced file. Both correct. |
| After the directory fsync, before publish | a complete `*.sst` | Startup uses the file and skips the log prefix it covers. |
| After publish | the same | Identical: publication is an in-memory step and nothing on disk records it. |

**A reader never sees a partial SSTable.** The final name either does not exist or names a
file that was completely written and fsynced before the rename. There is no window in
between, because `rename(2)` has none.

**Publication is one critical section**, adding the table and dropping the immutable memtable
together. That is what makes every mutation live in exactly one source at every instant, and
it is why a reader that snapshots all three pointers at once cannot fall through the gap.

**The flush is synchronous.** The writer that triggers it pays for it and other writers wait;
readers do not, which is the entire purpose of keeping the frozen memtable readable. Making
the flush concurrent is a Phase 5 question with a benchmark attached. Doing it now would add
a scheduler to a phase whose job is to be obviously correct.

**A failed flush latches.** The mutation that triggered it genuinely succeeded — it is in the
log and visible in memory — so failing that call would report a write that did happen as one
that did not. Instead the error is latched and the *next* mutation reports it, which also
stops the memtable growing without bound. This mirrors the WAL's `syncErr` (`docs/WAL.md` §6).

**`Close` does not flush the memtable.** Everything in it is already in the WAL, so a clean
close and a crash recover through exactly the same code path — which means every test that
reopens a store is exercising the recovery path, not only the crash tests.

---

## 8. Discovering SSTables at startup — a Phase 3 mechanism

`docs/DESIGN.md` §6 gives the real answer: a MANIFEST that is the single authority on which
files are live, carrying each file's key range and sequence range. **That is Phase 4.**
Phase 3 needs an answer now, and it must not be an unsafe one.

What Phase 3 does:

1. **Delete every `*.sst.tmp`.** A temp file is the signature of a crash during a flush.
   It is never read, so deleting it cannot lose anything the WAL does not still hold.
2. **Open and fully verify every `*.sst`** (§6). Verification is a complete read of the file.
3. **Recover each file's sequence range from its contents** during that same pass.
4. **Check the file set is coherent.** No gap in the numbering; sequence ranges ascending and
   non-overlapping. Both are guaranteed by how flushes are produced, so a violation means a
   file was removed or substituted, and continuing would silently drop what it held.
5. **Replay the WAL**, assigning sequence numbers from 1, skipping every mutation at or below
   the highest flushed sequence and applying the rest to the memtable.
6. **Refuse if the tables are ahead of the log.** Tables covering sequence numbers the log
   never held means the log was truncated or replaced.

### What this costs, stated rather than hidden

- **Startup reads every SSTable in full.** The sequence range has nowhere on disk to live
  until the MANIFEST exists, so it is recomputed. The alternative — encoding it in the file
  name — would mean trusting a name that nothing verifies, which is exactly the "unsafe
  file-selection behaviour" this phase was told not to invent.
- **The WAL is still never truncated**, so it holds every mutation ever written and replay
  still scans all of it. Phase 4's `SetLogNumber` is what retires segments a flush has
  superseded.

Both are removed by Phase 4. Neither is a design; both are the price of not having the
MANIFEST yet, and they are listed in `docs/LIMITATIONS.md` as such.

### The gap this mechanism does not close

Phase 3 can detect tables that are *ahead* of the log. It cannot detect a **missing oldest
WAL segment**, because nothing records which segment number the log begins at — the same
limitation `docs/WAL.md` §10 already carries, and the same MANIFEST field that fixes it.

---

## 9. Read path

```
snapshot (memtable, immutable memtables, SSTables) under one lock, then release
  memtable            → hit? done
  immutable memtables → hit? done          (newest first)
  SSTables            → hit? done          (newest first)
  otherwise ErrNotFound
```

A *hit* means the source holds **any** version of the key, including a tombstone. A tombstone
is an answer, not an absence: the search stops, and `Get` returns `ErrNotFound`. Falling
through a tombstone to an older SSTable is precisely how a deleted key comes back from the
dead, and it is the scenario the multi-file tests are written around.

Stopping at the first source is correct because of two properties held together:

- within one source, the internal-key ordering puts the newest version first;
- across sources, every sequence number in a newer source exceeds every sequence number in an
  older one.

The second is **checked at startup**, not assumed: overlapping sequence ranges are refused.

**Every SSTable is consulted on a miss.** With no Bloom filter and no compaction, a lookup
for an absent key asks every file. That is the Phase 3 cost and it is measured in §10.

---

## 10. Measurements

Development measurements on an Apple M4, 100-byte values, `wal.sync=off` for the read
benchmarks. **They are not benchmarks in the sense Phase 5 will mean**: nothing about the
environment is controlled or recorded, and no number here may be quoted anywhere as a result.

| What | Result |
|---|---|
| MemTable insert | ~450 ns/op, 5 allocs |
| MemTable lookup | ~121 ns/op, 0 allocs |
| SSTable write | ~165 MB/s (10,000-entry files) |
| SSTable point read | ~1.3 µs/op |
| `Put` (batch or off) | ~4.5 µs/op |
| `Get` across 17 SSTables | ~25 µs/op |
| Reopen: verify 17 SSTables + replay 50,000 mutations | ~8.4 ms |

**The one observation worth recording.** A lookup costs roughly 1.5 µs per SSTable consulted,
and the count of SSTables grows without bound because nothing merges them. That is the
expected shape of an LSM tree with no filters and no compaction, it is the bottleneck, and it
is measured rather than assumed. Phase 4's Bloom filters remove most of the per-file cost and
compaction removes most of the files. Nothing is being redesigned around it now.

---

## 11. Limitations

| Limitation | Removed in |
|---|---|
| No Bloom filters: every SSTable is consulted on a miss | Phase 4 |
| No compaction: SSTable count grows without bound, and so does read cost | Phase 4 |
| No MANIFEST: the live file set is inferred from the directory, and each file is fully read at startup to recover its sequence range | Phase 4 |
| The WAL is never truncated, so replay still scans every mutation ever written | Phase 4 |
| Segments missing from the *start* of the WAL are undetectable | Phase 4 (MANIFEST log number) |
| The flush is synchronous: writers wait on it, readers do not | Phase 5, if measured to matter |
| No block cache; the OS page cache is doing that job | deliberate, `docs/DESIGN.md` §11 |
| No prefix compression, no restart points, no compression | deliberate, `docs/DESIGN.md` §11 |
| Corrupted SSTables are detected, never repaired | v1; repair is re-sync from a leader, and there is no leader yet |
| **Power-loss durability is untested in every mode** | not testable here — `docs/WAL.md` §9 |

---

## 12. What Phase 3's testing established, and what it did not

**Established.**

- The Phase 1 conformance suite passes against the LSM engine unchanged, including in a
  configuration where every single mutation becomes its own SSTable — so no conformance
  assertion is being answered out of memory. A separate test asserts that configuration
  really does produce one table per write, so it cannot silently decay into testing a
  memtable.
- A newer tombstone hides an older value across SSTables, and keeps hiding it across a
  restart.
- Recovery is a function of the bytes on disk: repeated recoveries of the same directory
  produce identical state and identical sequence numbers.
- A real process killed with `SIGKILL` *during* an SSTable write recovers every acknowledged
  write. The test classifies each attempt by what the crash actually left on disk and fails
  if the mid-flush window was never hit.
- Flipping any single byte of an SSTable is detected. Corruption is never reported as
  "key not found".
- A differential test against a plain map — deliberately not `WALStore`, which shares this
  engine's WAL code and would cancel out a bug in it.

**Not established, and therefore not claimed.**

- **Power-loss durability, in any mode.** The crash tests destroy a process. That proves the
  bytes reached the kernel, not the platter. `docs/FAILURE_MODEL.md` §4.
- **Tombstone survival through compaction** (INV-S3). There is no compaction to survive.
- **Atomic version installation under compaction** (INV-S5). Phase 3's publication is atomic
  and tested under `-race`, but the hard case is a version swap while a compaction rewrites
  files, which Phase 4 introduces.
- **MANIFEST authority** (INV-S6) and **Bloom filter correctness** (INV-S7). Neither exists.
- **Cross-replica sequence determinism** (INV-S4). Established for one node replaying its own
  log; the replica comparison needs Phase 12.
