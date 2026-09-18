# DESIGN — formats, protocols, state machines

Status: **Phase 0 — specification only.**
Formats defined here are v1 and are versioned on disk so they can change without silent
misinterpretation of old data.

---

## 1. Key encoding (internal keys)

The LSM engine never stores a bare user key. It stores an **internal key**:

```
internal_key = user_key || seq(7 bytes, big-endian) || kind(1 byte)
kind: 0x00 = TOMBSTONE (delete marker)
      0x01 = VALUE
```

Ordering (`storage.Compare`):
1. `bytes.Compare(user_key_a, user_key_b)` — ascending.
2. If equal, **higher sequence number sorts first** (descending).
3. If both equal, `kind` descending (cannot happen; seq is unique per write).

Consequence: a forward scan hitting a user key yields the newest version first, so the read
path is "first match wins" with no version bookkeeping. This is the LevelDB scheme and it is
what makes overlapping SSTables safe during size-tiered compaction.

**Sequence numbers are deterministic.** `seq` is a per-shard monotonic counter incremented
once per mutation, in apply order. Because every replica applies the identical committed log
prefix in identical order, every replica assigns identical sequence numbers to identical
writes. This is an invariant we test (INV-S4), and it is what lets us byte-compare two
replicas' logical state.

Limits: `user_key` ≤ 4 KiB, `value` ≤ 1 MiB, `seq` < 2^56.

---

## 2. Record framing (shared by WAL, Raft log, MANIFEST)

One framing format, three users. Little-endian throughout.

```
offset  size  field
0       4     crc32c(length || kind || payload)
4       4     length  (payload byte count)
8       1     kind    (record-type, namespace is per-file-type)
9       N     payload
```

Reader algorithm:
- Fewer than 9 bytes remain → **clean end of file**.
- `length > MaxRecordSize (64 MiB)` → treat as corruption.
- Fewer than `length` bytes remain → **torn tail**.
- CRC mismatch → **corruption**.

Recovery policy, stated precisely because "handles corruption" is meaningless otherwise:

| Situation | Action |
|---|---|
| Torn tail or CRC failure in the *final* record of the *final* segment | Truncate the file at the last good record boundary. Continue. This is a crash mid-write and is expected. |
| CRC failure anywhere else | Refuse to open. Return `ErrCorrupt` with file and offset. Do **not** skip and continue — skipping a record in the middle of a log silently loses a committed write. |

There is no resynchronization scan. A corrupt record in the middle of a log is data loss,
and the system's job is to say so loudly, not to paper over it.

> **Phase 2 clarifications.** Implementing this exposed three points the Phase 0 text left
> underspecified. None changes the format; each makes an ambiguous rule decidable.
>
> 1. **"The final record" means the record whose extent reaches EOF**, not the last record
>    the reader happened to reach. A checksum failure is classified as a torn tail only when
>    `offset + 9 + length == filesize`; if bytes follow, the record was completed and
>    something later damaged it, and truncating there would discard the valid records after
>    it. This is sound whenever the length field survived — the residual ambiguity is
>    documented in `docs/WAL.md` §8.
> 2. **An all-zero 9-byte header is handled explicitly.** It can never be something we wrote
>    (a legitimate empty record stores `crc32c = 0x45727635`, never `0`). Resolved by
>    scanning the remainder: zeros all the way to EOF is a tail; zeros followed by real data
>    is damage. Without this rule the classification depended on how many zero bytes happened
>    to be present.
> 3. **A failed read latches.** Once the reader reports a problem it returns that same error
>    forever. The underlying stream has already consumed the bad record's bytes, so a
>    subsequent read would return whatever followed as if it were the next record — accidental
>    resynchronization, which this section forbids.

---

## 3. WAL (engine write-ahead log)

Segmented files `wal/%06d.log`. Record kinds:

| kind | payload |
|---|---|
| `0x01 WriteBatch` | `count uvarint`, then `count ×` { `kind u8`, `key len+bytes`, `value len+bytes` (absent for tombstone) }. **Phase 2: all lengths are uvarint**, matching §1; `kind` uses §1's value types (`0x00` tombstone, `0x01` value). |
| `0x02 AppliedIndex` | `raftIndex u64`, `raftTerm u64` |

A write batch is one record, so a batch is atomic with respect to crash: either the whole
CRC'd record replays or none of it does.

**Segment size (Phase 2):** the rotation threshold is **16 MiB**, and records are never split
across segments, so a segment may exceed it by up to one record. Phase 0 did not specify a
value; 16 MiB is a deliberate middle choice, not a measurement, and is documented as such in
`docs/WAL.md` §4.

### What "durable" means here

Three modes, configurable, and the guarantee differs in each:

| `wal.sync` | fsync behavior | Guarantee on power loss / process kill |
|---|---|---|
| `off` | never | Data is lost on process kill and on power loss. Test-only. |
| `batch` (default) | fsync every N ms or M bytes | Survives **process kill** (data is in the page cache). May lose up to the window on **OS/power loss**. |
| `sync` | fsync (macOS: `F_FULLFSYNC`) before the write returns | Survives power loss, assuming the drive honors the barrier. |

On Darwin, `fsync(2)` does **not** flush the drive's write cache; `fcntl(F_FULLFSYNC)` does.
We call `F_FULLFSYNC` on Darwin in `sync` mode and document the cost in `docs/BENCHMARKS.md`.
Our Phase 2 crash test kills the *process* (SIGKILL), which validates `batch` and `sync` but
not the power-loss path — we cannot test power loss on a laptop and we will not claim it.

---

## 4. SSTable format

Immutable. Written once, fsynced, never modified.

```
+------------------+
| data block 0     |   ~4 KiB target, then rounded up to the next entry boundary
| data block 1     |
| ...              |
+------------------+
| filter block     |   bloom filter over all user keys in the file
+------------------+
| index block      |   one entry per data block
+------------------+
| footer (48 B)    |
+------------------+
```

Every block is followed by a 4-byte `crc32c` of the block's bytes; block lengths in the
index/footer exclude the CRC.

**Data block entry** (no prefix compression in v1; see §"Deferred optimizations"):
```
keylen   uvarint      (length of the internal key)
key      keylen bytes (internal key, §1)
vallen   uvarint      (0 for a tombstone)
value    vallen bytes
```
Entries within a block are sorted. Lookup binary-searches the index to find the one candidate
block, then **linearly scans** that block. At 4 KiB blocks this is ~40 comparisons worst case
on a single cache-resident buffer; restart-point binary search is a Phase 5 optimization that
must be justified by a benchmark before it is written.

**Index block entry**: `keylen uvarint | last_key_of_block | offset uvarint | length uvarint`.

**Footer** (fixed 48 bytes, at `filesize-48`):
```
filter_offset u64 | filter_length u64
index_offset  u64 | index_length  u64
num_entries   u64 | magic u64 = 0x444B565353543031  ("DKVSST01")
```
The magic is checked first on open; a wrong magic is a format/version error, not corruption.

---

## 5. Bloom filter

Per-SSTable, covering **user keys** (not internal keys — a point lookup asks "might this file
contain any version of this key?").

- `m = ceil(n * bitsPerKey)` bits, `bitsPerKey = 10` default → ≈1% false positive rate.
- `k = max(1, round(bitsPerKey * ln2)) = 7` hash functions.
- Hashing: one 64-bit xxhash-style hash `h`, split into `h1 = uint32(h)`, `h2 = uint32(h>>32)`,
  then `g_i = h1 + i*h2` (Kirsch–Mitzenmacher). One real hash, k cheap derivations.
- Serialized as `k u8 | m u32 | bits[ceil(m/8)]`.

A Bloom filter may say "maybe" when the answer is no. It may **never** say "no" when the answer
is yes. The unit test asserts zero false negatives over a large random corpus, and asserts the
measured false-positive rate is within tolerance of the theoretical rate — a filter that always
returns "maybe" would pass the first test, so both are required.

---

## 6. MANIFEST and atomic version changes

`MANIFEST-%06d` is an append-only record stream (§2 framing) of **version edits**:

| kind | payload |
|---|---|
| `0x01 AddFile` | `level u8, fileNum u64, size u64, numEntries u64, smallestKey, largestKey, smallestSeq u64, largestSeq u64` |
| `0x02 DeleteFile` | `level u8, fileNum u64` |
| `0x03 SetNextFileNum` | `u64` |
| `0x04 SetLastSequence` | `u64` |
| `0x05 SetLogNumber` | `u64` (WAL segments below this are obsolete) |
| `0x06 SetApplied` | `raftIndex u64, raftTerm u64` |

`CURRENT` holds the name of the live manifest, written as: write `CURRENT.tmp`, fsync it,
`rename()` over `CURRENT`, fsync the directory. `rename` is atomic on POSIX.

**Compaction commit protocol** (the part that is easy to get silently wrong):

1. Write output SSTables to temp names. fsync each file.
2. `rename` each to its final name. fsync the directory.
3. Append one manifest record group: `AddFile(new...) + DeleteFile(old...)`. fsync the manifest.
4. Swap the in-memory version pointer (atomic; readers on the old version keep reading).
5. When the old version's refcount hits zero, unlink the superseded SSTables.

Crash between 2 and 3 → new files exist but are not referenced: **orphans**, deleted at
startup by diffing the directory against the manifest. Crash between 3 and 5 → old files
still exist but are unreferenced: same orphan sweep. The manifest is the single source of
truth about which files constitute the database. At no point is there a window where a
reader could observe a partially-installed file set.

---

## 7. Compaction strategy (v1: size-tiered)

- L0 receives flushed memtables. L0 files **overlap** in key range.
- When L0 file count ≥ 4, merge **all** L0 files into a single L1 file.
- When L1 total size exceeds `L1MaxBytes` (default 64 MiB), merge L1 into L2, and so on with
  a 10x size ratio.
- Merge is a k-way merge over the internal-key ordering (§1), so the newest version of each
  user key is emitted first and subsequent versions are dropped.

**Tombstone dropping** is the subtle part. A tombstone may only be discarded when compacting
into the **bottom-most** level, because an older version of the key may still live in a level
below. Dropping a tombstone early resurrects deleted data. This gets a dedicated test
(INV-S3: "a deleted key never comes back, at any level, across any number of compactions,
across restart").

v1 does not implement leveled compaction, and does not claim its write-amplification profile.

---

## 8. Raft

We implement Raft from the paper (Ongaro & Ousterhout, "In Search of an Understandable
Consensus Algorithm"), sections 5.1–5.4 plus §7 snapshots.

### 8.1 Persistent state (fsynced before any RPC reply that depends on it)

`currentTerm`, `votedFor`, and the log. `commitIndex` is persisted as an optimization only —
it is recoverable, never required. The raft log file is a record stream (§2 framing) holding
both `Entry` records and `HardState` records, so one fsync covers both, and the last
`HardState` record in the file wins on replay.

### 8.2 State transitions

| From | Trigger | To | Actions |
|---|---|---|---|
| Follower | election timeout elapses | Candidate | `term++`, vote for self, persist, broadcast RequestVote |
| Candidate | election timeout elapses | Candidate | new term, new election |
| Candidate | votes ≥ majority | Leader | reset `nextIndex[]=lastIndex+1`, `matchIndex[]=0`, append **no-op entry**, broadcast heartbeat |
| Candidate | AppendEntries from ≥ own term | Follower | adopt leader |
| Any | sees `msg.Term > currentTerm` | Follower | `currentTerm = msg.Term`, `votedFor = nil`, persist **before replying** |
| Leader | sees `msg.Term > currentTerm` | Follower | step down; in-flight proposals fail with `ErrNotLeader` |

The no-op entry on election is not optional. Without it, a new leader cannot know its own
commit index (Raft §5.4.2 forbids committing entries from prior terms by counting replicas),
and ReadIndex reads would be able to return stale data.

### 8.3 Election safety

Vote is granted only if all hold:
- `msg.Term >= currentTerm`
- `votedFor` is nil or equals the candidate (within that term)
- the candidate's log is **at least as up to date**: `(lastTerm, lastIndex)` compared
  lexicographically ≥ ours.

Randomized election timeout: uniform in `[electionTimeout, 2*electionTimeout)` ticks, drawn
from an injected `rand.Source`. Default tick = 50 ms, electionTimeout = 10 ticks (500 ms),
heartbeat = 2 ticks (100 ms).

### 8.4 Log matching and conflict resolution

`AppendEntries` carries `prevLogIndex/prevLogTerm`. A follower rejects if it lacks a matching
entry. Rejection includes a **conflict hint** (`conflictTerm`, `conflictIndex`) so the leader
backs up by a term rather than one index per round trip — the standard optimization from the
paper's §5.3 discussion. Without it, a follower that is 10,000 entries behind needs 10,000
round trips.

### 8.5 Read path — ReadIndex, not leases

A linearizable read does **not** go through the log. It does this:

1. Leader records `readIndex = commitIndex`.
2. Leader confirms it is still leader by exchanging heartbeats with a **quorum**.
3. Leader waits until `appliedIndex >= readIndex`.
4. Read from the state machine.

Step 2 is required because a partitioned old leader still believes it is leader. We do **not**
implement lease-based reads, which would let us skip step 2 at the cost of assuming bounded
clock drift. We make **no clock assumptions for safety** (see `docs/FAILURE_MODEL.md`), so
leases are out.

---

## 9. Wire protocol (internal, node↔node)

Framed over TCP. One connection carries multiplexed request/response pairs.

```
offset size  field
0      4     length   (bytes following this field)
4      2     msgType
6      2     flags    (bit 0: isResponse, bit 1: isError)
8      8     requestID
16     N     payload
```

Handshake on connect: `"DKV1"` magic + 4-byte protocol version + node ID, so a misdirected
connection fails immediately instead of being interpreted as a frame.

Message types: `RequestVote`, `AppendEntries`, `InstallSnapshot`, `Forward` (client request
proxied to a leader), `Probe` (liveness), each with a matching response type.

Payloads use a hand-written binary codec (explicit `Marshal`/`Unmarshal`, varints, no
reflection). Not gob, not JSON, not protobuf. Reasons, in order of weight:
1. Owning the transport is what makes Phase 10 fault injection *real*. We can drop, delay,
   duplicate, and reorder individual logical messages because we can see them. With gRPC the
   framing is opaque and we would be reduced to killing whole connections.
2. No codegen step, no protoc in CI.
3. Round-trip fuzzing over our own codec is a meaningful test; fuzzing generated protobuf
   is a test of protobuf.

`transport.Transport` is an interface, so a gRPC implementation could be added later without
touching `internal/raft`.

Client-facing traffic is plain HTTP/JSON (`docs/API.md`), because clients should be
inspectable with `curl` during a demo.

---

## 10. Startup / recovery sequence (per shard)

```
1.  Read CURRENT → open MANIFEST → replay version edits
        → file set, nextFileNum, lastSequence, logNumber, (raftIndex, raftTerm)
2.  Sweep orphan SSTables (on disk but not in the manifest) — delete.
3.  Replay engine WAL segments >= logNumber into a fresh memtable.
        Torn tail → truncate (§2). Mid-log corruption → abort.
4.  Open the raft log; replay Entry and HardState records.
        → currentTerm, votedFor, entries[]
5.  Reconcile: engine.appliedIndex must be <= raft.lastIndex.
        If engine.appliedIndex > raft.lastIndex → ErrInconsistent, refuse to start.
        (This means the state machine is ahead of its own log: impossible unless the
         raft log was truncated or restored from a stale copy. Refusing is correct;
         "fixing it up" would silently diverge from the other replicas.)
6.  Enter Follower state at currentTerm. Do NOT immediately campaign; wait a full
        randomized election timeout so a restarting node does not disrupt a healthy leader.
7.  Re-apply committed-but-unapplied entries (raft.commitIndex > engine.appliedIndex)
        before serving any read.
```

Step 7 is why apply must be idempotent with respect to the raft index: the engine records
`appliedIndex` and skips entries at or below it.

---

## 11. Deferred optimizations (deliberately not in v1)

Each of these is a real technique we are *choosing* not to implement yet, not one we forgot:

- Block prefix compression and restart points in SSTables.
- Block cache (we rely on the OS page cache).
- Compression (snappy/zstd) of data blocks.
- Batched heartbeats across Raft groups sharing a connection.
- Leader leases for reads (rejected on clock-assumption grounds, §8.5).
- Pipelined/parallel AppendEntries with in-flight windows.
- Leveled compaction.

They belong in `docs/BENCHMARKS.md` as "measured, then decided", not in the code as
speculation.
