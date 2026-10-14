# STORAGE INTEGRATION — the LSM engine as the replicated state machine

Status: **S1, S2, S3 and S4 are implemented (§7, §8, §9, §10); S5–S6 are design.** The design was audited against the code at
PR #10's head (`a8eb9e1`, 2026-10-07), and S1's audit against `main` after it. This document fixes
who owns each invariant, so that no code moves before that is clear. `docs/ENGINEERING_ROADMAP.md`
§0 states the project's thesis, and this is its layer-2 plan.

The rule this document applies everywhere: **Raft decides order. The storage engine makes the
result of applying that order durable. The engine never invents an order of its own.**

---

## 1. What runs today — the before

### 1.1 The apply path

`dkvd -raft` and `dkvd -cluster` run one driver per group (`internal/raftnode`). Its actor
performs one cycle per batch of events (`Node.processReady`):

1. **Persist** (`DrainReadyAt`): the Ready's HardState and new entries go to the Raft log
   (`internal/raftlog`) and are fsynced.
2. **Send:** its messages are handed to the transport.
3. **Advance** the core.
4. **Apply** (`ApplyCommitted`): each committed, unapplied entry, in index order:
   - `kv.Store.ApplyResult` decides it (registered, executed, duplicate, conflict, stale, expired, limit) and
     mutates the **in-memory** map and session table;
   - `AppliedTo` records the index in the core, in memory only.
5. **Snapshot** if the trigger fires (`MaybeSnapshot`): the whole state is encoded in the actor,
   published as a file, and the log compacted to the snapshot's index less `Retain`.
6. **Complete** (`completeApplied`): each applied write's waiter fires with its decision, after
   the cycle's Status is published, and the client is answered.

A write succeeds only when its entry is committed and applied on the leader in the term it was
proposed in (`docs/LINEARIZABILITY.md` §3). The leader fsyncs twice per write: once for the
entry, and once for the HardState whose commit index covers it (INV-CR3, commit durable before
apply). Concurrent writes are never batched into one fsync (`docs/CLUSTER_BENCHMARKS.md` §6.1).

### 1.2 What is durable, and how a node recovers

| State | Where | Durable? |
|---|---|---|
| Entries, term, vote, commit | Raft log (`raft-<id>.log`) | yes, fsynced before any message that depends on it |
| A prefix of the state machine, with its index, term and configuration | the published snapshot file | yes (rename and directory fsync) |
| Key-value data and the session table | `kv.Store`, in memory | **no** |
| Applied index | the core, in memory | **no** |

`raftnode.Recover` restores the published snapshot, sets the applied index to its index, and the
actor then re-applies every committed entry above it. That replay is **at-least-once per
incarnation** (INV-CR4), and it is correct only because the state is rebuilt from a known
starting point: the snapshot, or nothing at all. The session table is rebuilt the same way, which
is why deduplication survives a restart (`docs/DEDUP.md` §4).

### 1.3 The engine

`internal/storage` (`LSMStore`) is a complete single-node engine: a WAL, memtable, SSTables with
Bloom filters, size-tiered compaction and a MANIFEST. `dkvd` links it only for two size
constants. Measured against what hosting needs (`docs/ENGINEERING_ROADMAP.md` §1.6):
- Every mutation is a single-operation WAL batch.
- The applied index is a separate WAL record, so a crash can leave data without its index, or the
  index without its data.
- Sequence numbers are re-derived from WAL position; the MANIFEST's `LastSequence` and `Applied`
  are written but never read back.
- The WAL is never truncated.
- There is no ordered-iterator API, consistent checkpoint, or ingest.
- WAL segments and SSTables bypass `internal/vfs`, so the engine cannot be crash-tested the way
  the node is; only the MANIFEST can.

## 2. Who owns what

| Fact | Owner today | Owner once hosted | Unchanged? |
|---|---|---|---|
| The order of commands | the Raft log | the Raft log | yes |
| Commit (a command will be applied) | Raft: a majority's durable logs | the same | yes |
| A client's success may be sent | committed **and** applied on the leader, in the proposal's term | the same; engine durability is **not** required (§3.3) | yes |
| The result of applying: key-value data | `kv.Store`, memory | the engine | **moves** |
| The result of applying: session table | `kv.Store`, memory | the engine, under a reserved key prefix, written in the same batch as the data | **moves** |
| The applied index | the core, memory; re-derived on restart | the engine, **atomic with the data and sessions it covers** | **moves** |
| Membership configuration | the Raft core, log and snapshot metadata | the same; never the engine's | yes |
| A snapshot's index, term and configuration | `raftnode` (`snapshot.Meta`) | the same | yes |
| A snapshot's state | `kv.Store`'s encoding | an engine checkpoint (§5) | **moves** |
| How far the Raft log may be compacted | the snapshot index less `Retain` | the minimum of that and the engine's **durably synced** applied index | **tightens** |

**One engine per group.** Each Raft group has its own log today, and its own engine once hosted:
data directory, WAL, MANIFEST and applied index. That keeps group isolation (INV-MB) a matter of
separate files, and makes a group's snapshot install a whole-engine ingest rather than a key-range
replacement. Sharing one engine among a node's groups would save fsyncs, but it would need
per-group applied indexes inside one batch and range-scoped ingest. That trade-off is measured in
S5, not assumed.

Two rules follow, and every later step must keep them:
- **R1, apply is atomic.** The engine records `(the data writes, the session writes, applied =
  (i, term))` for a span of entries as one crash-atomic unit. After any crash the engine holds the
  effect of exactly the entries up to its recovered applied index, and nothing beyond it.
- **R2, the log outlives the engine's durability.** The Raft log never discards an entry above the
  engine's durably synced applied index. Otherwise a crash loses the entries the engine had applied
  but not synced, and nothing could replay them.

## 3. One cycle, once hosted

### 3.1 The steps

1. **Persist, send and advance** exactly as today. The Raft log stays authoritative.
2. **Decide:** for the committed entries `a..b`, `ApplyResult` decides each one in index order,
   reading the state as left by every entry before it, **including the earlier entries of the same
   batch, which are not yet in the engine.** A REGISTER at `a` and a write on that session at `a+1`
   must be decided exactly as they would be one cycle apart. The decisions are the same ones
   `kv.Store` makes today (same reference model). The writes go into **one engine batch** for the
   cycle, never one per entry.
3. **Commit the batch:** a single WAL record holding entries `a..b`'s data writes, their session
   writes and `applied = (b, term_b)`, then inserted into the memtable. Whether it is fsynced is a
   policy measured in §4, not a correctness requirement: the Raft log already holds `a..b`.
4. **Advance** the core's applied index to `b`.
5. **Complete:** answer the clients of `a..b`, as today.

### 3.2 Crash windows

Under **R1** the engine recovers either before the cycle's batch (`a-1`) or after it (`b`), never
in between. Each row says what recovery then does, and whether the client's view is allowed by
the contract (`docs/CLIENT_SEMANTICS.md`).

| Crash … | Engine recovers to | Recovery then | The client of `a..b` saw | Outcome | Allowed |
|---|---|---|---|---|---|
| before the batch is written | `a-1` | replays `a..b` from the Raft log once | nothing | unknown → retried under the same request id → the session answers it, executed once | yes |
| during the batch write (torn) | `a-1` (a torn record is discarded whole) | as above | nothing | as above | yes |
| after the write, unsynced; process crash | `b` (the kernel holds it) | nothing to replay | nothing | unknown → retry → **duplicate** | yes |
| after the write, unsynced; power loss | `a-1`, or `b` if the kernel wrote it back | replay from what survived | nothing | unknown → retry → executed once | yes |
| after the batch is synced, before the reply | `b` | nothing | nothing | unknown → retry → duplicate | yes |
| after the reply | `b`, or `a-1` if unsynced at power loss | replay as needed | success | acknowledged and durable, via the Raft log | yes |
| during a flush, compaction or MANIFEST edit | the engine's own recovery (`docs/LSM.md`, `docs/MANIFEST.md`) | as its applied index says | — | not client-visible | needs the engine crash matrix (§7) |

There is no row in which a write the client was told succeeded is lost: success requires commit,
which is a majority's durable Raft log. There is no row in which a write executes twice: the
session table and the applied index are recovered together (**R1**), so replay starts exactly
after the last entry whose effect survived. **Exactly-once across restarts** replaces INV-CR4's
at-least-once. Without **R1** it does not hold:
- a replayed REGISTER resets the session it created, results and all (`kv.Store.decide`);
- a replayed identified write becomes a duplicate of itself, so its retry is answered differently
  than the original would have been.

### 3.3 When the reply becomes legal

The reply rule does not change. A write is durable when it is **committed**, meaning it is in a
majority's fsynced Raft logs. The engine's copy of its effect is a recovery shortcut, not the
durability guarantee. Making the engine's sync a precondition of the reply would add an fsync to
every write and protect nothing that the Raft log does not already protect.

One ordering is required. The engine's applied index must never pass the Raft log's durable
commit. Today INV-CR3 provides this (commit is durable before apply). Any group-commit change that
folds the commit persist into a later append must preserve it, or prove that it is not needed.

## 4. The two-log problem

Hosting the engine gives each write two durable logs: the Raft log and the engine's WAL. ADR-006
accepted that deliberately. Whether to keep it is decided by measurement, not by preference.

| | A. Both logs fsynced | B. Engine WAL written, not fsynced per batch | C. No engine WAL for replicated writes |
|---|---|---|---|
| Engine durability | every batch | at a sync point: before the Raft log is compacted past it, and periodically | each memtable flush: the SSTables and a MANIFEST edit recording the applied index |
| fsyncs per write at the leader, one write per cycle (today 2) | 3 | 2 | 2, plus one per flush |
| Bytes written per write | Raft entry + WAL record | Raft entry + WAL record | Raft entry (+ SSTable at flush) |
| Restart work | WAL replay | WAL replay, plus Raft replay above the engine's durable index | Raft replay above the last flushed index, bounded by the memtable size |
| Log compaction gated by | the engine's applied index | the engine's last **synced** applied index (**R2**) | the engine's last **flushed** applied index (**R2**) |
| Couples the engine to Raft | no | lightly (a sync hook) | yes: the Raft log is the engine's WAL; the standalone engine keeps its own WAL mode |
| Correctness risk | lowest | medium: R2's gate | highest: replay bounds, memtable-sized log retention |

To measure, against the baseline of `docs/CLUSTER_BENCHMARKS.md` §3 and §6:
- persists and fsyncs per write;
- bytes per write to each log;
- write p50/p99 and throughput;
- restart time, and entries replayed, as the state grows;
- snapshot creation and install time;
- compaction frequency.

The node metrics already count Raft log persists; the engine's `WALStats` and `FlushStats` need
exporting. No option is chosen before those numbers exist. A is the correctness baseline the
others are compared against.

## 5. Snapshots from checkpoints

Before S4 a snapshot was the state encoded as one byte slice in the actor, and both ends held the
whole state in memory (up to 512 MiB). Hosting changes where a snapshot's content comes from and
where it goes, and keeps its metadata and Raft's rules. What S4 built (§10), in brief:

- **Creating one:** an engine checkpoint at applied index `i` — the engine flushed and its version
  (immutable SSTables, reference-counted) pinned — streamed in key order into the snapshot file:
  data keys, then the session records, with `applied = (i, term)`. The snapshot's `Meta` (index,
  term, configuration) stays `raftnode`'s and is unchanged. Still on the actor (S6).
- **Transfer:** the existing header-checksummed format, unchanged — written in passes over the
  pinned checkpoint (measure and hash, then write), read and validated as a stream. The planned
  new format with per-chunk checksums and a trailer was not needed (§10.2): every record already
  carries a CRC, and the header hash can be checked at the end of a stream.
- **Installing:** SSTables built from the received stream as unreferenced files in the engine's
  directory, then the engine's whole state replaced by one MANIFEST edit (**ingest**), recording
  the applied index. A crash before that edit leaves the old state; a crash after it leaves the new
  one; unreferenced tables are swept at open. Raft's install ordering is kept: the response never
  precedes the durable install (`DrainReadyAt`).
- **Log truncation** after a checkpoint is bounded by **R2**.

Every snapshot guarantee already proven (INV-SN, the snapshot crash matrix, ErrSuperseded for
writes a snapshot replaced) holds unchanged: they are re-run against the hosted engine, not
re-derived (§10.16).

## 6. Milestones

Each milestone is one PR, with its own tests, mutants, measurements and documentation, and each
leaves `main` green.

| # | Milestone | Touches | Exit criterion |
|---|---|---|---|
| **S1** ✓ | **Atomic apply batches in the engine** (§7) | `internal/storage`, its WAL on `internal/vfs` | **R1**'s storage half proven in isolation by a deterministic crash and power-loss matrix — done, §7.9–§7.11 |
| **S2** ✓ | **The engine behind the state-machine interface** (§8), sessions under a reserved tag; `dkvd -state-machine memory\|lsm` | `internal/kv`, `raftnode` (a cycle hook, the term on restore), `multiraft`, `cmd/dkvd` | every existing linearizability, dedup, snapshot, membership and chaos tier passes on both machines; the engine's applied index is the durable authority at restart — done, §8.9–§8.12 |
| **S3** ✓ | **Durable watermarks, WAL truncation, R2-gated compaction** (§9): the applied index and last sequence authoritative in the MANIFEST, the WAL truncated behind each flush, and the Raft log never compacted past the engine's durable applied index (an installed snapshot covers its own prefix) | `storage`, `raftnode`, `kv` | the crash matrix extended to the truncation protocol, deterministic R2 gating, and the existing tiers on both machines — done, §9.10–§9.13 |
| **S4** ✓ | **Snapshots as engine checkpoints, streamed, installed by ingest** (§10): a checkpoint (a flushed, pinned version) streamed into the unchanged snapshot format; transfer, validation and restore streamed; the follower's engine replaced by one MANIFEST edit; SSTables on the `vfs` seam | `storage`, `snapshot`, `raftnode`, `kv` | the ingest crash matrix (process crash and modeled power loss), the existing snapshot crash matrix and every tier on both machines, a real follower installing and restarting on a snapshot above the S3 limit — done, §10.15–§10.18 |
| S5 | The two-log decision: A, B and C measured | `storage`, `dkvlab` | a published comparison against the baseline, and the chosen option |
| S6 | Performance: group commit, pipelining, batched apply | `raftnode`, `raft` | before-and-after measurements |

Chaos end to end, PreVote and CheckQuorum, and the delivery surface follow, in the order
`docs/ENGINEERING_ROADMAP.md` §0 gives.

## 7. S1 — atomic apply batches in the engine

The smallest step that establishes the storage half of **R1**. It stays inside the engine:
`dkvd`, `raftnode` and `kv` are untouched, and S1 does **not** make `dkvd`'s replicated state
durable.

### 7.1 Audit: the engine before S1

The write path of `LSMStore.Put` or `Delete`:
1. Validate the key and value.
2. Under `writeMu`, append a one-operation `KindWriteBatch` record to the WAL: one `write(2)`,
   then an fsync if the sync mode requires one.
3. Only after the append succeeds, assign the next sequence number (`s.seq++`; `docs/LSM.md` §4
   said "before the append", and the code has always done it after, so a failed append consumes
   nothing).
4. Insert into the memtable.
5. If the memtable is full, flush.

A flush:
1. Freezes the memtable.
2. Writes `NNNNNN.sst.tmp`, fsyncs it, renames it and fsyncs the directory.
3. Syncs the WAL (audit D10).
4. Appends one MANIFEST edit: the new table, `NextFileNum`, and `LastSequence = s.seq`.
5. Swaps the version.

Open:
1. Recovers the MANIFEST.
2. Opens and cross-checks the tables it names.
3. Installs a fresh MANIFEST.
4. Sweeps orphans.
5. Replays every WAL segment. Each mutation gets `seq = 1, 2, …` in log order; one at or below
   the highest flushed sequence is skipped as already in a table.

The questions S1 has to answer, against that code:

| # | Question | Answer before S1 |
|---|---|---|
| 1 | Where is sequence state held? | The last assigned sequence lives only in memory (`LSMStore.seq`). Each table's internal keys carry their sequences, and the MANIFEST records each table's range and a `LastSequence`. That `LastSequence` is written at every flush and open but **never read**. WAL records carry no sequence: replay re-derives it by counting mutations in log order. |
| 2 | Where is the applied index held? | In memory (`LSMStore.applied`), and durably as `KindAppliedIndex` WAL records written by `SetAppliedIndex`: **a record of its own, separate from any data**. The MANIFEST has an `Applied` field that open reads before replay, but nothing ever sets it, so it is always zero and replay overrides it. |
| 3 | Which metadata is durable? | MANIFEST: the live file set, each file's key and sequence range, `NextFileNum`, `LastSequence` (unread), `Applied` (unset). WAL: applied-index records, subject to the sync mode. SSTable footers. Never durable: the current sequence and the current applied index, which are always derived. |
| 4 | What survives a process crash? | Every WAL record whose `write(2)` completed, in every sync mode (INV-W9); every table and MANIFEST edit already written. A record torn by the crash is the newest segment's tail, and open truncates it. |
| 5 | What survives a modeled power loss? | Only fsynced bytes, and a file only once its directory entry is fsynced. **The WAL is not on `internal/vfs`, so none of this is tested** (`docs/WAL.md` §5). Reading the code found two gaps: (a) the WAL directory's own entry is never fsynced into its parent after it is created, so on the first open a power loss can lose every segment, synced or not; (b) open does not fsync the newest segment before a replay flush (`docs/WAL.md` §10). |
| 6 | Which records can be partially written? | The record being appended, at the newest segment's tail. Older segments are fsynced at rotation, except in `SyncOff`. Separately, the legacy pair `Put` + `SetAppliedIndex` is **two** whole records, so a crash between them is a partial logical update that no checksum can see. |
| 7 | What does recovery replay? | Every segment from the first: each mutation in log order, skipping those already in a table, and every applied-index record, the last one winning. A torn tail in the newest segment is truncated; anything else is refused. |
| 8 | When can data become durable without metadata? | (i) Legacy: the `Put` record is durable and the `SetAppliedIndex` record is not yet written, or written but not synced. (ii) Gap (b): a replay flush at open makes tables and a MANIFEST edit durable while the WAL records they came from, and their applied index, are still only in the page cache. A power loss then leaves the MANIFEST ahead of the WAL, and open refuses the store. |
| 9 | When can metadata become durable without data? | Legacy, in the other order: a `SetAppliedIndex` record durable before the `Put` it describes. Nothing else: no record carries an index for data it does not hold. |

The audit corrects the plan in §6 in three places:
- **No MANIFEST `Applied` in S1.** While the WAL is never truncated, it is the only authority
  either fact needs. Adding the MANIFEST as a second authority for the applied index now would
  create exactly the "two authorities for one number" that `SetAppliedIndex`'s comment warns
  against. It moves to S3, together with WAL truncation, which is what first makes it necessary.
- **No durable sequence in S1,** for the same reason: replay reproduces the numbering exactly as
  long as nothing deletes a segment. §7.4 states the gap, and S3 must close it before truncating.
- **Gaps (a) and (b) are fixed in S1.** Both are places where acknowledged or applied state
  could be lost or outrun by its metadata under a power loss, and S1's matrix exists to find
  exactly that.

### 7.2 The contract

```go
// Mutation is one write of an apply batch.
type Mutation struct {
    Kind  MutationKind // MutationPut or MutationDelete
    Key   []byte
    Value []byte       // empty for MutationDelete
}

// Apply records mutations, and the applied index they bring the state
// machine to, as ONE WAL record.
func (s *LSMStore) Apply(ctx context.Context, muts []Mutation, applied AppliedIndex) error
```

**Guaranteed:**
- **Crash atomicity (R1, storage half).** After any process crash or modeled power loss, recovery
  observes a batch **whole**: every one of its mutations together with its applied index. Or it
  observes **none** of it: no mutation, and the applied index as it was before. Recovery never
  observes some mutations without the index, or the index without all its mutations.
- **Prefix.** Recovery observes a prefix of the batches in the order they were applied: never a
  later batch without an earlier one.
- **Durability on return is the sync mode's,** exactly as for `Put` (`docs/WAL.md` §5):
  - every mode survives a process crash;
  - `sync` also survives a power loss;
  - `batch` survives a power loss once a flush has covered it;
  - `Sync()` forces that flush.
- **Monotonic index.** `applied.Index` must exceed the current applied index, `applied.Term` must
  be at least the current term, and both must be at least 1. Otherwise `ErrAppliedIndex` is
  returned and nothing is written.
- **Publication order.** The mutations become visible to `Get` before `AppliedIndex()` reports
  the batch. A reader that sees index `i` sees every mutation of every batch up to `i`.
- **A failed `Apply`** (an I/O error) leaves the in-memory state and `AppliedIndex()` unchanged.
  Like any WAL failure it latches. The batch itself may or may not be recovered after a crash,
  but never partly.
- **Empty batches** (no mutations) are valid. They advance the index alone, as a Raft no-op or
  configuration entry must.

**Not guaranteed:**
- **Isolation from concurrent readers.** A `Get` running while a batch is inserted into the
  memtable may see some of its mutations and not others. Atomicity here is with respect to a
  crash, not a transaction. The publication-order rule above is what a state machine can rely
  on; S2 must decide reader visibility for the hosted engine.
- **Read-modify-write.** There is no compare-and-swap or conditional mutation. The batch is
  computed by the caller, deterministically, from the state it already read.
- **That `SetAppliedIndex` and `Put` are atomic with anything.** They remain the standalone
  API: two separate records. A hosted state machine must use `Apply` only.

### 7.3 The WAL record

A new record kind in the existing framing (`internal/record`). The framing gives one CRC-32C over
length, kind and payload, and the existing torn-tail and mid-log rules. No second persistence
format is introduced.

```
kind 0x03  KindApplyBatch, payload version 1:
  offset  size  field
  0       1     version            = 1
  1       8     applied index      little-endian uint64, >= 1
  9       8     applied term       little-endian uint64, >= 1
  17      var   operation count    uvarint, may be 0
  ...           operations         exactly a WriteBatch's: kind byte (0 delete, 1 put),
                                   uvarint key length, key, and for a put a uvarint value
                                   length and the value
```

- **Size:** the whole payload must fit `record.MaxRecordSize` (64 MiB). A larger batch is refused
  with `ErrInvalidBatch` before anything is written, and latches nothing.
- **Decoding is strict.** Each of these is `ErrCorrupt`:
  - an unknown version;
  - an index or term of 0;
  - a count larger than the bytes left;
  - an unknown operation kind;
  - an empty key;
  - a length running past the payload;
  - trailing bytes.
- **Framing rules are the existing ones:**
  - a torn record at the newest segment's tail is truncated;
  - a bad record with bytes after it, or anywhere in an older segment, is refused;
  - an unknown kind is refused.

  So a binary from before S1 refuses a WAL holding apply records, and never skips them.
- **One more replay rule.** An apply record whose index does not exceed the applied index in
  effect before it, or whose term is lower, is refused as `ErrCorrupt`. The writer never produces
  one, so its presence means the log is not one this engine wrote.

### 7.4 Sequence numbers

- An apply batch of `n` mutations takes sequences `s+1 … s+n`, in batch order, where `s` is the
  last sequence before it. The applied index takes **none**, and an empty batch takes none.
- Sequences are assigned only after the record is appended, so a refused or failed `Apply`
  consumes none.
- Replay counts every mutation of every `KindWriteBatch` and `KindApplyBatch` record in log
  order, so it reproduces the live numbering exactly. It also records, for the last apply
  record, the sequence of its last mutation (`AppliedSequence()`).
- **The gap, stated:** the current sequence is still derived, not stored. That is sound only
  because no WAL segment is ever deleted. The MANIFEST's `LastSequence` stays unread in S1. S3,
  which truncates the WAL, must make it authoritative first, or replay would number the
  surviving records from 1.

### 7.5 Recovery

Replay reconstructs, from the same WAL history:
- the key-value state;
- the last sequence;
- the applied index, together with the sequence it covers.

An apply record is applied as a unit: its mutations, then the index. Two changes close gaps (a)
and (b):
- **(a)** when `wal.Create` makes the WAL directory, it fsyncs the parent;
- **(b)** `wal.Recover` fsyncs the newest segment before replaying it, unless the sync mode is
  `off`, so everything recovery hands to the engine is durable before a replay flush can record
  it. After open, recovered implies durable, which is the fact R2 will need.

### 7.6 The `vfs` seam

`wal.Options.FS` (nil meaning the OS) carries every WAL file operation:
- segment create, append, read, truncate and fsync;
- the directory listing;
- `MkdirAll`;
- directory fsync.

`vfs.FS` gains `ReadDir` and `MkdirAll`, and `fault.MemFS` gains a minimal directory model:
- a directory made with `MkdirAll` is durable only once its parent is fsynced;
- a power loss removes an undurable directory, and every file under it;
- directories never made explicitly behave as before, so existing users are unaffected.

On Darwin a `*os.File` is still flushed with `F_FULLFSYNC`. Nothing else in the engine moves.
SSTables and the MANIFEST's reads stayed on `os` until S4 (§10.11).

### 7.7 The crash matrix

`TestApplyCrashMatrix` scripts a sequence of steps, then crashes at every WAL I/O operation the
script performs. The steps include:
- apply batches of puts, deletes and both;
- an empty batch;
- explicit syncs;
- a flush, whose MANIFEST edit is the metadata it publishes;
- more batches after it.

The script runs in the `sync` and `batch` modes. The `batch` mode runs with its timer disabled,
so every fsync is one the script caused and the run is deterministic.

The crash operations it enumerates:
- every segment create and parent fsync;
- every append, torn halfway as well as whole;
- every fsync.

The fsync before the flush's MANIFEST edit is the "before metadata publication" cell. The first
append after it is the "after publication" cell. One extra set of cells fails the MANIFEST
append itself, before and partway through it.

Each cell is run three ways:
- **process crash:** every byte written survives;
- **power loss:** only fsynced bytes and fsynced directory entries survive;
- **power loss with a torn tail:** a prefix of the unsynced bytes survives.

Every cell asserts:
1. the store opens;
2. the recovered applied index, sequence and full key-value state equal the reference model after
   exactly `m` batches, for one `m`: R1, the prefix and the sequences together;
3. `m` lies within the cell's bounds. The lower bound is every batch whose record was written
   (process crash) or covered by an fsync (power loss); the upper bound is every batch attempted;
4. the recovered WAL is fully synced, and a second power loss right after open changes nothing;
5. the store then accepts the next batch, and recovers it.

The SSTables and the MANIFEST stay on the real filesystem, so their side of a cell is a process
crash. That is sound for R1 because:
- a table becomes part of the database only through its MANIFEST edit;
- the WAL is fsynced before that edit (D10, and the matrix checks it);
- a table that never reached its edit is an orphan, deleted at open.

What it does **not** test is the SSTable and MANIFEST code's own ordering under a power loss:
the MANIFEST has its own matrix (`docs/MANIFEST.md` §10), and SSTables moved onto `vfs` in S4 (§10.11).

### 7.8 Tests, mutants, fuzzing, benchmark

- **Regression tests:**
  - one batch; many mutations; many batches; put and delete of the same key in one batch;
  - an empty batch;
  - index monotonicity, refused live and refused at replay;
  - malformed, duplicate and impossible apply records;
  - torn tails;
  - sequence reconstruction;
  - publication order;
  - a failed `Apply` changing nothing;
  - legacy `Put` and `SetAppliedIndex` interleaved with `Apply` never moving the index a batch
    set, while the legacy pair is shown to split under a crash.
- **Mutants 310–328, one per R1 failure mode, each with its killer named in `scripts/mutation.sh`:**
  - data and index written as two records;
  - the index written without the data;
  - the index published before the append;
  - the index restored late, or not at all, at replay;
  - an empty batch's index skipped;
  - the monotonicity check removed or reversed, live or at replay;
  - the index counted as a sequence;
  - replay numbering apply mutations differently;
  - gaps (a) and (b) reopened.
- **Fuzzing:** `FuzzApplyBatchRoundTrip` and `FuzzDecodeApplyBatchIsTotal` in `internal/storage/wal`.
- **Benchmark:** `dkvbench -suite apply`. Before is today's only way to record an apply: `Put` and
  `SetAppliedIndex`, two records per entry. After is `Apply`, with one entry per batch and with
  many. It reports bytes and records per entry, fsyncs, append and sync latency, and recovery
  time with the recovered index and sequence.

### 7.9 What was built, and what building it found

**Built:**
- `vfs.FS` gains `ReadDir` and `MkdirAll`, and `fault.MemFS` a directory model.
- The WAL runs on `vfs`, with the `ApplyBatch` record (kind `0x03`) and gaps (a) and (b) closed.
- `LSMStore.Apply`, `AppliedSequence`, and replay of apply records as units.
- `WALStore` refuses a log of apply batches.

**Tests:**
- 21 regression tests (7 in the WAL, 12 in the store, 2 for `MemFS`) and the crash matrix;
- two fuzz targets;
- `dkvbench -suite apply`;
- 19 mutants (310–328), every one killed by the test named for it.

One of those kills depends on timing: mutant 313, publication order, is killed by a concurrent
reader. It was measured at 20 kills in 20 runs, and 10 in 10 under the race detector.

**What building it found:**
1. **Gaps (a) and (b)** (§7.1), from reading the code. Both are fixed, and each fix is guarded by
   a mutant (322 and 323) that its test and the matrix kill.
2. **A count or length written in more bytes than it needs was accepted** by the WAL's decoders.
   `0` written as `0x80 0x00` gives one batch two encodings. `FuzzDecodeApplyBatchIsTotal` found it
   within seconds. The `WriteBatch` decoder had always done the same. Both now refuse it, as the
   key-value codecs already did (mutant 328).
3. **`docs/LSM.md` §4 said the sequence number moves before the append.** The code has always
   moved it after, so a failed append consumes nothing. The document was wrong, and is corrected.

The matrix itself passed its first complete run. Every mutant in 310–328 shows it, or the
regression test named beside it, failing when the rule it checks is broken.

### 7.10 Measurements

`dkvbench -suite apply -runs 3` was run on a clean tree at `30b84cd` on an Apple M4, macOS and
APFS; raw results are in `bench/s1-apply.json`. The workload is one 100-byte put per applied
entry: 32,000 entries in `off` and `batch` mode, and 1,600 in `sync` mode. The table gives
medians of three runs. The baseline ("legacy") is the only way to record an application before
S1: a `Put`, then `SetAppliedIndex`, which is two records and not atomic.

| Mode | Arm | Entries/s | Call p50 / p99 (µs) | WAL records per entry | WAL bytes per entry | fsyncs | Recovery (ms) |
|---|---|---|---|---|---|---|---|
| `off` | legacy | 270,389 | 3.2 / 6.5 | 2 | 150.0 | 0 | 28.0 |
| `off` | `Apply`, 1 entry | 391,219 | 1.9 / 5.2 | 1 | 142.0 | 0 | 26.7 |
| `off` | `Apply`, 16 entries | 1,641,717 | 6.0 / 45.2 | 0.0625 | 116.7 | 0 | 24.5 |
| `batch` | legacy | 224,279 | 3.2 / 7.3 | 2 | 150.0 | 5 | 26.9 |
| `batch` | `Apply`, 1 entry | 353,320 | 1.9 / 4.8 | 1 | 142.0 | 4 | 23.5 |
| `batch` | `Apply`, 16 entries | 1,027,161 | 6.1 / 45.8 | 0.0625 | 116.7 | 3 | 22.8 |
| `sync` | legacy | 130 | 7,958 / 11,435 | 2 | 150.0 | 3,200 | 18.9 |
| `sync` | `Apply`, 1 entry | 261 | 3,941 / 6,134 | 1 | 142.0 | 1,600 | 20.1 |
| `sync` | `Apply`, 16 entries | 4,370 | 3,912 / 6,350 | 0.0625 | 116.7 | 100 | 19.9 |

Every reopen recovered exactly what was written:
- index and sequence `(32,000, 32,000)`, or `(2,000, 32,000)` for 16-entry batches;
- `(1,600, 1,600)` in `sync` mode, or `(100, 1,600)` for 16-entry batches.

What the numbers say, and no more:
- **Atomicity costs nothing here; it saves.** The legacy pair writes two records and, in `sync`
  mode, two fsyncs per entry. One apply record writes one, and 8 fewer bytes per entry. On this
  machine an fsync (`F_FULLFSYNC`) is about 3.9 ms, and it dominates `sync` mode.
- **Batching entries into one record** is the caller's choice: one record per Raft cycle, as S2
  will do. It divides records and fsyncs by the batch size: 16 entries per fsync gives 4,370
  entries/s against 261. This is the amortization the cluster measurements asked for
  (`docs/CLUSTER_BENCHMARKS.md` §6.1). It is measured here on the engine alone; the engine has
  not been optimized.
- **Recovery time is not separated by the arm at these sizes.** Replaying 32,000 entries takes
  about 23–28 ms, and the fixed cost of an open (installing a MANIFEST, its fsyncs) is most of
  `sync` mode's 19–20 ms. Recovery growing with the WAL is S3's question, which truncation
  answers.

### 7.11 What S1 proves, and what it does not

**S1 proves, for the standalone engine, in the software model (`fault.MemFS`):**
- **R1, the storage half (INV-W11).** An application's mutations and its applied index are
  recovered together or not at all, as a prefix of the order applied. This holds at every WAL
  operation, under a process crash and under a power loss with or without a torn tail, in `sync`
  and `batch` mode, across a flush and segment rotations.
- **The index, the data and the sequence agree** (INV-L11). The recovered applied index, the
  sequence it covers, and the sequence agree with the reference model in every matrix cell.
- **Durability as each sync mode promises it.** No acknowledged-durable batch is lost, and after
  open everything recovered is durable (INV-W13).
- **The index only advances** (INV-W12). Apply batches are published in order (INV-L10), and
  every accepted payload is canonical (INV-W14).

**S1 does not prove:**
- **That `dkvd` has durable replicated state.** It does not use the engine at all: the
  replicated state machine is still the in-memory `kv.Store`, recovered by replaying the Raft
  log.
- **R2, or exactly-once across restarts of a replica.** Both need the engine behind the state
  machine (S2) and recovery from its applied index (S3).
- **Power loss on a real device.** The model assumes an honest fsync and loses un-synced data
  only as a prefix.
- **The power-loss ordering of the SSTable and MANIFEST code.** Their side of every cell is a
  process crash. §7.7 argues why that is sound for R1. The MANIFEST has its own matrix, and
  SSTables moved onto `vfs` in S4 (§10.11).
- **Crashes during recovery itself.** The matrix crashes the script, then a power loss right
  after open, never inside an open. The WAL's own tests cover a torn tail's repair and
  recovery's idempotence.
- **Isolation of a batch from concurrent readers,** or anything about compaction running under
  a crash: the matrix keeps compaction off.
- **Anything after WAL truncation.** The current sequence and applied index are still derived
  from the whole log, and S3 must make both authoritative in the MANIFEST before deleting a
  segment.

**Next dependency: S2.** S2 puts the engine behind the state-machine interface:
- the session table goes under a reserved key prefix;
- one apply batch is written per Ready cycle;
- the reader-visibility rule §7.2 leaves open is decided;
- recovery still starts from the snapshot.

Every existing linearizability, dedup, snapshot, membership and chaos tier must pass on both
state machines.

## 8. S2 — the engine behind the replicated state machine

Status: **implemented.** The design (§8.1–§8.8) was audited against the code at `5f8ca4b` before
any change; §8.9–§8.12 record what was built, what it is tested against, and what it proves. S2
composes S1's engine with the real system and proves the composition keeps every externally
observable semantic. It does not start S3: no WAL truncation, no MANIFEST applied index, no
engine-backed snapshot format, no optimization.

### 8.1 The path today, traced

One client mutation, from the code:
1. `kv.Front` decodes the request; `Server.handle` → `execute` → `Node.Write(ctx, cmd.Encode())`.
2. The actor: `core.Propose`; the entry is the log's tail at (index, term); `waiters.Add(index, term)`.
3. `processReady`: `DrainReadyAt` saves the Ready (fsync), sends it, advances. Followers persist
   and acknowledge; the commit index moves; the next Ready persists it (INV-CR3).
4. `ApplyCommitted`: for each committed, unapplied entry, `Store.ApplyResult(index, cmd)` decides
   it against the in-memory map and session table, then `core.AppliedTo(index)`; the actor collects
   `(index, term, result)`.
5. The cycle ends: `snapshotStatus`, then `completeApplied` fires every waiter. `Write` returns the
   decision; `execute` turns it into the response. The reply exists only after this point.

What the pieces assume:
- `raftnode` knows a `replication.StateMachine` (`Apply(index, cmd) error`), optionally a
  `ResultStateMachine` (`ApplyResult`) and a `SnapshotStateMachine` (`EncodeSnapshot`,
  `ValidateSnapshot`, `RestoreSnapshot(index, data)`). It never passes an entry's **term** to the
  state machine, and it has no notion of a cycle boundary: entries are applied one at a time.
- `multiraft.Host` makes a group's machine through `Config.NewStateMachine(g)` and hands it to
  `OnGroup`. It **never closes it**: stop and retire call `Node.Close` only.
- `kv.Front.Attach`, `kv.NewServer`, `Server.Store()` and `kv.Metrics.Observe` take the concrete
  `*kv.Store`; `dkvd` asserts `sm.(*kv.Store)`. Tests reach the store through `Server.Store()` for
  `Get`, `Snapshot` and `Stats`.
- `Recover` restores the published snapshot into the machine **unconditionally**, sets the applied
  index to the snapshot's, and the actor then re-applies every committed entry above it: replay is
  at-least-once per incarnation (INV-CR4), which is correct only because the machine starts from the
  snapshot, or from nothing.
- The simulator (`internal/raftsim`) drives `kv.Store` through the same `ApplyCommitted`.

### 8.2 The composition contract

| Question | Answer |
|---|---|
| What is one apply cycle? | One `ApplyCommitted` call: the entries `core.NextApply()` returns in one `processReady`, applied in index order. |
| Which mutations belong to one cycle? | Every effect of every entry in that cycle: the user puts and deletes the executed commands produced, and every session-table change any command caused (a session registered, evicted, marked used, its watermark raised, a result recorded). |
| When may the reply become visible? | Only after the cycle's batch has been recorded by `LSMStore.Apply` (the WAL record is written; fsynced as the engine's sync mode says). `completeApplied` runs after the batch, as it already runs after the applications. A cycle whose batch fails completes **no** waiter. |
| Where is session state? | In the engine, under the session tag (§8.3), as one record per session; and mirrored in memory, loaded from the engine at open, because decisions read it on every identified command. The engine's copy is the durable one. |
| What is persisted atomically with a user mutation? | The session-record rewrite the same command caused, every other mutation of the cycle, and the applied index of the cycle's last entry: one `Apply`, one WAL record. |
| A duplicate request? | No user mutation. The session record is still rewritten: a duplicate marks the session used at this index and may raise its watermark, exactly as `kv.Store.decide` does today. The result is `Duplicate` with the original index. |
| A Raft no-op or configuration entry? | Zero mutations; the batch still advances the applied index. S1 allows an empty batch for exactly this. |
| A committed operation whose reply is lost? | Unchanged: the client retries under the same request id; the session record, recovered with the data it was written with (R1), answers `Duplicate`. |
| The Raft log index and the engine's applied index? | The engine's applied index is the log index of the last entry whose effects the engine holds, with that entry's term. It never exceeds the core's commit index (INV-CR3 makes the commit durable before apply). |
| Who owns session semantics? | `internal/kv`. One decision function, shared by both machines, decides every command from the command and the session table; the engine never interprets a command. |
| Who owns storage atomicity? | The engine: `Apply` is the only write path the LSM machine uses. Never `Put` then `SetAppliedIndex`. |
| `Apply` returns an error? | The cycle fails as a state-machine failure (`ErrApply`): the node fail-stops, as it does today for a deterministic apply error (D6). Its waiters, including this cycle's, fail with the error, and every client of them sees UNKNOWN. The engine's own latch refuses further writes. |
| May the machine continue after an apply failure? | No. The in-memory mirror has advanced past what the engine holds; the only consistent state is the engine's, read back by a restart. |

No second authoritative copy of the applied index: the engine's is the durable one. The core's is
this incarnation's bookkeeping, and the published snapshot's index is the fallback used only when
the engine is behind it (§8.6).

### 8.3 The reserved keyspace

Every key the LSM machine writes to the engine carries a one-byte tag:

| Tag | Key | Value |
|---|---|---|
| `0x01` | the user key, as the client sent it | the value |
| `0x02` | the session id, 8 bytes big-endian | the session record |

A user key cannot collide with a session record because every user key is stored under `0x01`,
whatever its bytes. No user key is forbidden: INV-A5 ("any byte sequence is a valid key") is
untouched, and the engine is opened with `MaxKeySize` one byte above `kv.MaxKeyLen`. The tags are
the engine-side namespace; nothing above the machine sees them.

The session record is exactly the session table's entry for one session, the same fields the
snapshot format carries:

```
version    u8      = 1
last       u64     the log index of the session's last command
ackedBelow u64
n          uvarint
n × { requestID u64 | index u64 | fingerprint 32 bytes }   requestIDs strictly ascending
```

Decoding is strict: an unknown version, a watermark of 0, a count the bytes cannot hold, a
non-ascending request id, a result below the watermark, or a trailing byte is
`ErrSessionRecord`, and the machine refuses to open on it — a malformed record means the engine's
content is not what this machine wrote, and guessing would make replicas diverge.

### 8.4 One batch per cycle

`raftnode` gains one optional interface, and `ApplyCommitted` uses it when the machine has it:

```go
type CycleStateMachine interface {
    StateMachine
    ApplyEntry(index, term uint64, command []byte) (any, error) // decide and stage
    EndCycle() error                                            // record the cycle's effects as one unit
}
```

`ApplyEntry` decides the command against the in-memory session mirror — which it updates at once,
because a later entry of the same cycle may depend on the decision — stages the command's write
and notes the sessions the decision changed, and returns the decision. `EndCycle` calls
`LSMStore.Apply(staged writes + the noted sessions' records + the evicted sessions' deletions,
{index, term of the last entry})`. The mirror is ahead of the engine between the two; a failed
`EndCycle` leaves it so, and the node fail-stops, so the mirror never outlives the engine's state. `ApplyCommitted` calls it once after the loop when it applied at least one
entry, and reports its error as `ErrApply`. The in-memory `kv.Store` and the simulator's machines
do not implement it and are unchanged.

A cycle's mutations exceed one WAL record only if a cycle applies tens of MiB of entries (a
follower catching up). `EndCycle` then splits at entry boundaries, each piece recorded at its last
entry's index — each piece is a true state — and the reply rule holds because `completeApplied`
runs after `EndCycle` returns.

The entry's term reaches the machine through `ApplyEntry`, and `RestoreSnapshot` gains the
snapshot's term: `RestoreSnapshot(index, term, data)`. That is the one change to an existing
interface, and the smallest that lets the engine record a true term for a restored state. The
in-memory store ignores it.

### 8.5 The machine abstraction

`kv.Machine` is what the front, the server, the metrics and the tests need of a state machine:
`Get`, `Applied`, `Sessions`, `Snapshot`, `Stats`, and the observe hooks. `*kv.Store` implements it
unchanged; `*kv.LSMMachine` implements it over an `LSMStore`. `Front.Attach`, `NewServer`,
`Server.Store()` and `Metrics.Observe` take a `Machine`. `dkvd -state-machine memory|lsm` chooses
the constructor in `NewStateMachine`; nothing in `raftnode`, `multiraft` or `kv`'s server branches
on the kind. `multiraft.Host` closes a machine that implements `io.Closer` after it closes the
group's node.

A data directory that holds an engine refuses to start as `memory` (exit 2, the reason named).
Starting an existing `memory` directory as `lsm` is allowed: the engine is empty, its applied index
0, and it is built from the published snapshot and the log like any new replica.

### 8.6 Restart and replay

At open the LSM machine loads its session mirror from the engine's session records and reads the
engine's applied index `e`. Then, in `Recover`:
- a published snapshot at index `i`: if `e >= i`, the machine's state is at least as new and
  `RestoreSnapshot` changes nothing — decided first, before the state is decoded or measured (S3
  review, §9.10); if `e < i`, the state is validated as today and the engine is replaced by the
  snapshot's state (§8.7);
- the core's applied index is set to `i` as today, and the actor re-applies the log above it.
  `ApplyEntry` **skips** an entry whose index is at or below the engine's applied index: its effects
  are already in the engine, and re-applying them would reset a session or turn a request into a
  duplicate of itself (§3.2). It returns no result; no waiter of this incarnation exists for it.

So after any crash the machine is exactly the engine's state at `e` plus the committed entries
above `e`, applied once each. This is what R1 was built for. It is still **not** R2 and not
exactly-once across every compaction scenario: the Raft log is compacted on the snapshot schedule,
not on the engine's durable index, so an engine behind the last snapshot is restored from the
snapshot rather than replayed. That is correct — the snapshot is a true state above `e` — and it is
why S2 needs no R2 yet.

### 8.7 The snapshot boundary

`EncodeSnapshot` produces today's format from the engine: every user key (via the engine's
whole-state iteration, sorted), the session mirror, and the applied index. It reads every table,
as the in-memory store already holds every key; S4 replaces this with a checkpoint.

`RestoreSnapshot(index, term, data)` returns at once if the engine already holds `index`;
otherwise it validates the state as today, then replaces the engine: close it, rename its
directory aside and fsync the parent, delete the old engine, open a fresh one, and record the whole
state — user keys, session records — in **one** `Apply` at `(index, term)`. (Since the S3 review:
deleting in place was not crash-atomic, and any failure after the close now fails the machine
stop rather than leave it serving from an empty engine — §9.10.) The replacement is atomic under R1: either the batch is
recovered whole, or the engine is empty with applied index 0, below the published snapshot, and the
next open restores it again. A snapshot whose state does not fit one WAL record (64 MiB) is refused
by `ValidateSnapshot`, so the core never installs it; today's bound is 512 MiB, and this is an S2
limitation lifted by S4's ingest. (Since S4 the restore is an ingest — §10.6 — and neither the
one-batch restore nor the set-aside exists.)

The existing snapshot tests run against the LSM machine through the same interface; nothing in
`raftnode`'s snapshot machinery changes beyond the term parameter.

### 8.8 What is deliberately deferred

- WAL truncation, the MANIFEST as the authority for the applied index and sequence (S3).
- R2: compaction gated by the engine's durable index (S3).
- Snapshots as checkpoints; ingest; the 512 MiB bound back (S4).
- The two-log measurement (S5) and every optimization (S6).
- Reader isolation across a batch: a `Get` during a batch's publication may see part of it; single-key
  reads over an index-ordered publication stay linearizable (§7.2).

### 8.9 What was built, and what building it found

**Built:**
- `kv.Machine`: what the front, the server, the metrics and the tests need of a state machine.
  `*kv.Store` and `*kv.LSMMachine` implement it; nothing above a machine knows which it has.
  `Lookup` and `Contents` carry an error, so an engine that cannot read never answers "absent".
- `kv.sessionTable`: the session table and its decision function, one type both machines hold.
- `kv.LSMMachine` (`internal/kv/lsm.go`): `ApplyEntry` decides and stages, `EndCycle` records one
  apply batch per cycle; the reserved keyspace and the session record (`namespace.go`); the skip
  rule, the restore rules and the 64 MiB restore bound; `Close`.
- `raftnode.CycleStateMachine`, used by `ApplyCommitted`; a failed cycle completes no waiter;
  `RestoreSnapshot` carries the term; `Durable.Snapshot` never snapshots past the core.
- `multiraft.Config.NewStateMachine` can fail, and the host closes a machine that is an
  `io.Closer` after its node — on `Stop`, on `Close`, and when a start fails.
- `dkvd -state-machine memory|lsm`; a directory holding an engine refuses to start as memory;
  `dkvlab -state-machine`; `QUORUM_STATE_MACHINE=lsm` runs the kv package's and the integration
  suite's every test on the LSM machine.

**What building it found:**
1. **Decision counters are not replicated state**, and two tests used them as if they were. The
   snapshot format omits them; the in-memory store has them after a restart only because it
   replays the log. `TestRetryAtEveryCrashPointOfAWrite` proved "executed exactly once anywhere"
   by every replica's counters, and said restarted replicas "rebuilt theirs by replaying the log".
   A durable machine recovers its state and skips those entries, so its counters cover what it
   applied since. The test now takes the count from the replicas that never restarted and
   requires the restarted one to hold the same contents and session table — stronger than a
   counter. The differential harness compares counters only within one incarnation.
2. **A read released by its barrier mid-cycle** would have read the engine before the cycle's
   batch was written: the core's applied index moves per entry, inside the cycle, and a read
   whose index it has passed is released at once. So a cycle's staged writes are visible to
   `Lookup` before they are recorded, under the staging mutex (§8.4, `TestLSMMachineStagedWrites
   AreVisibleBeforeTheyAreRecorded`, mutant 332). Publication is in index order, so a single-key
   read sees a prefix-closed state.
3. **The engine's batch can fail inside a cycle** after the core has applied the entries and the
   mirror has advanced. The node fail-stops, as for any apply failure, and — new — completes none
   of the cycle's waiters: a client is told nothing definite for a state the engine did not
   record (`TestLSMEngineFailureFailsStopsTheNode`, mutant 337). The write is committed in Raft
   and comes back from the log at the node's restart, once.
4. **Nothing in the engine changed.** S1's `Apply` carried the composition as designed. S2's gate
   did surface a race in one of the engine's own tests, untouched since Phase 5:
   `TestFlushEveryWriteReallyFlushes` counted tables with the background compactor on, and under a
   full parallel run the compactor merged four of them first. The test now keeps the compactor off,
   as an assertion about file counts must; the assertion is unchanged.

### 8.10 Tests and fault coverage

| Test | What it establishes |
|---|---|
| `TestLSMMachineMatchesTheStoreOnAScript`, `…OnSeededScripts` | The executable reference: the same entries through both machines leave the same contents, every key's `Lookup`, the session table, the applied index and (within an incarnation) the counters, after every cycle and across reopens. The script reaches every decision; the seeded scripts mix retries, conflicts, watermarks, anonymous writes, no-ops, evictions and reopens. Each cycle's batch is also sized from the outside: one sequence per executed write, per session changed and per session evicted. |
| `TestLSMMachineSkipsWhatItsEngineHolds` | Replaying the recovered prefix changes nothing; the next entry is applied; a retry is a duplicate of the original. |
| `TestLSMMachineRecoversAPrefixAfterAPowerLoss` | R1 through the machine: after a process crash every recorded cycle is back; after a modeled power loss exactly the fsynced prefix is, contents, sessions and index together; a second restart reads the same; the log replays the rest once. |
| `TestLSMMachineAFailedCyclePublishesNothingDurable` | A failed batch records nothing, latches, and a reopen shows the state before it. |
| `TestLSMMachineRestoresASnapshot`, `…ARestoreBelowItsEngineChangesNothing` | Restore replaces the engine as one batch at (index, term); a restore whose batch failed leaves an empty engine the next restore replaces; at or below the engine's index nothing changes; the machine's own snapshot is the store's, byte for byte; a state too large for one batch is refused before the core could install it. |
| `TestLSMMachineStagedWritesAreVisibleBeforeTheyAreRecorded` | §8.9 (2). |
| `TestLSMMachineRefusesAForeignSessionRecord`, the `namespace_test` tests | A record the machine did not write refuses the open; user keys never collide with records; the record codec is canonical and strict. |
| `TestLSMEngineFailureFailsStopsTheNode` | §8.9 (3), in a real three-node group with the engine's WAL under fault injection. |
| `TestStopAndCloseCloseTheMachine` | The host closes a machine once, after its node. |
| `TestRealStateMachineKindIsAnOperatorsChoice` | Real processes: memory → lsm bootstraps from the snapshot and the log and catches up into the engine; lsm → memory is refused before Raft starts; back on lsm the node recovers and rejoins. |
| The kv package under `QUORUM_STATE_MACHINE=lsm` | Every in-process test — linearizability, sessions and deduplication, retries at every crash point, snapshots, entry limits, forwarding — on the LSM machine, under the race detector. |
| The integration suite under `QUORUM_STATE_MACHINE=lsm` | Every real-process test — crash windows, session retries across crashes, snapshots, membership, chaos, dkvctl — on the LSM machine. |
| Mutants 329–342 | One per way the composition can break (§8.9 and `scripts/mutation.sh`), each killed by the test named for it. |

Fault coverage: the engine's WAL write fails inside a cycle (in-process group); a process crash and
a modeled power loss under the machine (`fault.MemFS`); every crash point of the driver with a
session retrying (`TestRetryAtEveryCrashPointOfAWrite` on the LSM machine); and, on real
processes, every integration fault the suite already injects.

### 8.11 Real-cluster evidence

Real `dkvd` processes on loopback TCP, started by the lab with `-state-machine lsm`, under the
seeded chaos campaign (`docs/CHAOS.md`): clients write, read and delete through the session client
— identified requests, retried under the same identity — while the schedule kills, stops, crashes
at a driver point, pauses, isolates and cuts nodes, takes snapshots and adds a member; every
client-visible operation is checked for linearizability afterwards, and the group must converge.

`dkvlab -scenario chaos -seed 1 -runs 3 -state-machine lsm` at `7597923` (Apple M4, macOS; raw
results in `bench/cluster/chaos-lsm-seeds-1-3.json`, status samples stripped):

| Seed | Faults injected | Leader changes | Operations (kept Incomplete) | Linearizable | Converged |
|---|---|---|---|---|---|
| 1 | 7: add-member, stop, snapshot, stop, crash, crash, pause | 3 | 1804 (0) | yes | yes |
| 2 | 7: crash, crash, isolate, kill, cut, add-member, isolate | 4 | 1645 (4) | yes | yes |
| 3 | 6: stop, cut, add-member, crash, crash, stop | 1 | 2179 (4) | yes | yes |

What identifies the machine: the run's configuration records `state_machine: lsm`, and every
node's log carries `event=state_machine node=… group=0 kind=lsm dir=…/lsm` when its group's
machine is made — once per process, so a node that was crashed and restarted logs it twice. The
same campaign on the in-memory machine is `bench/cluster/chaos-seeds-1-10.json`.

What the runs show on the production path, on the engine:
- leader election and leader replacement (the leader killed, crashed at `after-save`, isolated);
- writes and reads served through forwarding and redirection;
- follower replication, a follower stopped and restarted, a node brought back by a snapshot;
- a member added (`add-member`: learner, caught up, promoted);
- client retries answered by deduplication: unknown outcomes stay Incomplete, and the histories
  are linearizable with every identified request applied once;
- applied indexes coherent: convergence requires every member to have applied the leader's commit
  index, and a restarted node's engine resumes from its own applied index.

The whole real-process suite on the engine (`make integration-lsm`) passed locally at `7597923`:
89 tests in 402 s under the race detector, every `dkvd` race-built, no process left behind. CI runs
it on every change (`integration-lsm`).

### 8.12 What S2 proves, and what it does not

**S2 proves:**
- **The composition keeps the contract.** Every existing linearizability, deduplication, retry,
  snapshot, membership and chaos test passes against the LSM machine unchanged, except the one
  test that measured replicated state by counters (§8.9).
- **One apply batch per cycle holds everything the cycle changed** — the user writes, the
  session records, the applied index and term — and nothing else (the batch is sized from the
  outside in every differential cycle). **R1 holds through the machine:** after a process crash
  or a modeled power loss the machine recovers a prefix of cycles, contents, sessions and index
  together.
- **A durable machine restarts correctly inside Raft:** the entries its engine holds are skipped,
  the rest replayed once, a snapshot below its state leaves it alone, a snapshot above it replaces
  it atomically; a retry after any of that is a duplicate of the original execution.
- **A reply never precedes the batch**, and a failed batch never acknowledges: the node fail-stops
  and its clients learn nothing definite.
- **The two machines decide identically**, from one decision function, on scripted and seeded
  histories.

**S2 does not prove:**
- **R2.** The Raft log is still compacted on the snapshot schedule, not on the engine's durable
  index. An engine behind the last snapshot is restored from the snapshot — correct, but a
  restore of the whole state, not a replay. Compaction gated by the engine's durable index is S3.
- **Exactly-once replay durability across every log-compaction scenario.** What is proven is
  exactly-once across the restarts the tests perform: a restart with the entries still in the
  log, and a restart from a snapshot above the engine.
- **Durability of the engine's own state on a real device.** Power loss is the software model;
  the Raft log, fsynced, is what makes a committed write durable (§3.3).
- **Anything about the WAL growing**, the sequence or the applied index after truncation, or the
  MANIFEST as an authority: S3.
- **Snapshots larger than one WAL record** (64 MiB) on the LSM machine, and snapshots as
  checkpoints rather than a whole-state batch: S4.
- **Reader isolation across a batch,** and the cost of composition beyond what §8.13 measures.
- **The engine's SSTable and MANIFEST code under a power loss** inside the machine: the machine's
  crash tests put the WAL on the model, as S1 did.

### 8.13 The cost of composition

`dkvlab -scenario steady -nodes 3 -clients 4 -read 50 -runs 3`, with and without
`-state-machine lsm`, at `7597923` (Apple M4, macOS, one disk; medians of three 20 s runs; raw
results in `bench/cluster/steady-memory.json` and `bench/cluster/steady-lsm.json`):

| | memory | lsm |
|---|---|---|
| ok/s (median) | 124.5 | 101.6 |
| operations per 20 s run | 2,489 | 2,031 |
| success rate | 100% | 100% |
| GET p50 / p95 / p99 (ms) | 22.2 / 44.1 / 55.0 | 26.9 / 53.5 / 68.2 |
| PUT p50 / p95 / p99 (ms) | 40.2 / 64.9 / 74.9 | 46.2 / 79.0 / 94.4 |
| every-outcome p99 (ms) | 72.0 | 88.0 |
| unknown / refused outcomes | 0 / 0 | 0 / 0 |
| node CPU per second, max RSS | 0.060, 20.2 MiB | 0.060, 20.4 MiB |
| Raft-log persists per node per run | 6,820 | 6,617 |

A second run of the same configuration, on the same machine with uncommitted changes in the
tree, measured 121.7 against 109.0 ok/s: the composition costs **ten to twenty percent** of
throughput here, with run-to-run variance of that order, and ten to twenty-five percent on the
tail latencies.

Nothing was optimized, and no concurrency was added to flatter the numbers. The load is
fsync-bound (`docs/CLUSTER_BENCHMARKS.md` §6.1: one Raft-log fsync per write, two at the leader),
and the engine's WAL runs in its default batch mode, so the engine adds a write per cycle and no
fsync per write. The cost that shows is the engine's write path per entry — the record, the
memtable, the session record rewrite — on the actor goroutine. The replicated apply path is what
S6 measures and optimizes; the figures here are the before.

---

## 9. S3 — durable watermarks, WAL truncation, and R2-gated compaction

S3 makes the engine's applied index and last sequence **authoritative in the MANIFEST**, so the
engine's WAL can be truncated without losing the numbering replay derives; it establishes the
crash-safe ordering between a flush, the MANIFEST commit and WAL-segment deletion; and it enforces
**R2** in the real Raft compaction path. R1 (§7.2) is unchanged and must stay intact.

The design was audited against the merged code at S2 (`main` after PR #16). The rule of §1 still
governs: Raft decides order; the engine makes the result durable; the engine invents no order.

### 9.1 Audit: what is authoritative today, and what truncation would break

| # | Question | Answer at S2 |
|---|---|---|
| 1 | Where is the applied index durable? | The engine's WAL, as `KindApplyBatch`/`KindAppliedIndex` records (S1). On open, `LSMStore.applied` is seeded from the MANIFEST's `Applied` field and then **overridden** by replaying every WAL record. The MANIFEST's `Applied` is written only by the open-time snapshot and is otherwise never advanced, so the WAL is the sole authority. |
| 2 | Where is the sequence durable? | Nowhere. `LSMStore.seq` lives in memory and is **re-derived** on open by counting every mutation in the WAL from 1. The MANIFEST's `LastSequence` is written at each flush but never read back as an authority. |
| 3 | What makes this sound today? | Nothing deletes a WAL segment (`docs/WAL.md` §10, `docs/MANIFEST.md` §4, §11). Replay always reads the whole log from segment 1, so counting reproduces the exact numbering and the last apply record reproduces the applied index. |
| 4 | What would truncation break? | Both derivations. Delete segment 1 and replay starts at segment 2 and numbers its first record `seq = 1` — but that record's true sequence is far higher, so every table's sequence range and the memtable would disagree, and the applied index of the deleted prefix would be lost. Truncation is therefore impossible until the MANIFEST is the authority for both numbers. |
| 5 | Is R2 enforced? | No. `raftnode.Durable.compact` discards the Raft log through `snapshotIndex − Retain`, gated only by the snapshot schedule. Nothing consults the engine's durable applied index. In S2 this is safe only because the published snapshot — fsynced before the log is compacted — is itself a durable copy of the compacted prefix (§9.6 states this precisely). |
| 6 | Does the MANIFEST format need to change? | Minimally. The edit format already carries `SetLogNumber` (tag `0x05`) and `SetApplied` (tag `0x06`), round-tripped and strictly decoded since Phase 4 (`docs/MANIFEST.md` §3); S3 starts **writing** them at each flush and **reading** them at open. Two tags are added — `SetLogStartSeq` (`0x07`), the replay seed (§9.2), because reusing `LastSequence` as the seed is unsafe across a flush during replay (§9.10), and `SetAppliedSeq` (`0x08`), because truncation deletes the apply batch the applied sequence was read from (§9.10). No version bump; a pre-S3 directory still opens unchanged (§9.8). |

### 9.2 The authoritative MANIFEST metadata

Five fields in the MANIFEST become authoritative; three were already in the format and two are added:

- **`Applied` (index, term).** The applied index the flushed tables bring the engine to. Set in
  the same edit that adds a real flush's SSTable, so it advances atomically with the data it covers
  (R1's shape, now in the MANIFEST as well as the WAL). It is the floor replay starts from and the
  durable index R2 clamps to.
- **`LogNumber`.** The WAL segment replay must start from. Every segment below it holds only
  records already in a flushed table, and may be deleted.
- **`LogStartSeq`** (new, tag `0x07`). The sequence before segment `LogNumber`'s first record: the
  value replay **seeds** its counter from. S3 maintains the invariant that **segment `LogNumber`
  begins exactly at sequence `LogStartSeq + 1`.**
- **`AppliedSeq`** (new, tag `0x08`). The sequence of the last mutation the last apply batch held:
  the data `Applied` covers is exactly the mutations through it (`AppliedSequence`). It differs from
  `LogStartSeq` when writes outside apply batches (`Put`, `Delete`) followed the last batch, and the
  batch it would otherwise be read from is among the truncated records.
- **`LastSequence`.** The highest sequence number any flushed table holds, as before. It is *not*
  the replay seed: a flush during replay advances it without moving the log (§9.7), so only
  `LogStartSeq` stays consistent with `LogNumber`.

The three watermark fields — `LogNumber`, `LogStartSeq`, `Applied` — and `AppliedSeq` are written
**together, in one edit, only by a real flush** (one with a WAL to rotate), so they are always
mutually consistent. Recovery decodes every field strictly (an unknown tag or a truncated value is
`ErrCorrupt`) and validates them against the live file set and the WAL: the lowest segment present
must be `LogNumber` (§9.5), and every apply batch replayed must advance the applied index past the
MANIFEST's `Applied` — replay's advance check starts there, as the truncated records would have left
it, so a batch below the durable watermark is `ErrCorrupt`, as it was before S3 when the whole log
was read. An applied-index record written by `SetAppliedIndex` — a pre-S3 API that may lower the
index, and that the LSM machine never calls — is taken as written, as it always was.

`LogNumber == 0` means "no truncation has happened": replay starts at segment 1 and counts from 0,
exactly as before S3. Nothing truncates a log without first recording a log number, so with none
recorded the log must begin at segment 1; a lowest segment above it means the log's start is gone
(an S3 directory whose MANIFEST was lost, say) and is `ErrCorrupt` — counting the rest from zero
would mis-number every record. This is what a pre-S3 directory decodes to, so such a directory
opens and replays unchanged (§9.8).

### 9.3 The watermark commit point

A flush is the only place the watermark advances, and the MANIFEST append is the instant it
becomes durable. The order, with each step chosen for what a crash immediately after it leaves:

1. **Freeze** the memtable at `(seq = L, applied = A)`; install a fresh one. (as today)
2. **Write the table:** `N.sst.tmp`, fsync, rename, **fsync the directory**. The table's bytes and
   name are durable. (as today)
3. **Rotate the WAL:** sync and close the active segment, create a new empty segment `A+1`, and
   **fsync the WAL directory** so the new segment's creation is durable. The new segment will hold
   `seq L+1` onward, so it begins exactly at `LogStartSeq + 1`. (`s.w == nil` during a replay
   flush: no WAL yet, so this step and `LogNumber` are skipped — §9.7.)
4. **Append the MANIFEST edit** — `AddFile(N)`, `SetNextFileNum`, `SetLastSequence(L)`,
   `SetLogNumber(A+1)`, `SetLogStartSeq(L)`, `SetApplied(A)`, `SetAppliedSeq` — as **one record**, fsynced by
   `manifest.Append`. **This is the commit point.** Before it, the flush has not happened; after
   it, the table is live and the watermark is `(L, A)` with the log starting at segment `A+1`.
5. **Swap** the in-memory version to include table `N`. (as today)
6. **Delete** WAL segments below `A+1`, then **fsync the WAL directory**. A failure here is logged,
   not fatal: the segments are redundant (their records are in table `N`) and the next open sweeps
   them. This mirrors obsolete-SSTable removal (`lsmcompact.go`).

**An empty memtable.** An apply batch with no mutation — the election no-op, a configuration
entry — advances the applied index without adding to the memtable. A `Flush` with an empty memtable
whose applied index has moved since the last record therefore still records it: steps 3, 4 and 6
without a table — rotate, append `SetLastSequence(L)`, `SetLogNumber`, `SetLogStartSeq(L)`,
`SetApplied(A)`, `SetAppliedSeq` as one fsynced edit with no `AddFile`, truncate. With the memtable empty every
sequence through `L` is already in a recorded table (a flush failure latches, so no frozen memtable
is left behind), so the edit is exact. With nothing new it records nothing. (Found in the final
review, §9.10.)

The rotation in step 3 is what makes the seed exact: because the new segment is empty at the
freeze, its first record is `L+1`, so `LogNumber`'s segment starts at `LogStartSeq + 1` by
construction. `LogStartSeq` is recorded as its own field rather than read off `LastSequence`, for the
reason §9.7 gives: a flush during replay moves `LastSequence` but not the log.

### 9.4 The WAL truncation eligibility rule

> A WAL segment may be deleted only once a durable MANIFEST edit names a `LogNumber` above it.

Its consequence: every record in a segment below `LogNumber` has `seq ≤ LogStartSeq` and is
therefore in a flushed, MANIFEST-recorded table, and every applied index it records is at or below
the recorded `Applied`. (The converse does not hold: a flush during replay puts records of segment
`LogNumber` into a table without moving `LogNumber`, and that segment is kept until the next real
flush.) Eligibility is never decided by a
filename, an age, or a sequence guess — only by the durable `LogNumber`. Because `LogNumber`
advances only at step 4, and step 4 is preceded by the table (step 2) and the new segment (step 3)
being made durable, a segment is deleted only after its contents provably survive elsewhere.

### 9.5 Crash safety of the ordering

| Crash … | MANIFEST holds | On disk | Recovery | Correct because |
|---|---|---|---|---|
| before step 4 | old watermark | table `N` orphaned; maybe empty segment `A+1`; old segments present | replay from old `LogNumber`, old seed; sweep `N` and any stray segment | the flush did not happen durably |
| step 4 torn | old watermark (torn edit truncated) | as above | as above | `manifest.Recover` truncates the torn final record; the edit never took effect |
| after step 4, before/during step 6 | `(L, A)`, `LogNumber = A+1` | table `N` live; some or all of segments `< A+1` still present | replay from `A+1`, seed `L`; sweep leftover low segments | segment `A+1` was made durable in step 3 **before** the commit, so `LogNumber` never names a non-durable segment; low segments are redundant |
| power loss after step 4, new segment's post-commit writes unsynced | `(L, A)`, `LogNumber = A+1` | `A+1` reverts to empty (its durable creation); old segments gone or present | replay from `A+1` (empty) → engine recovers to `(L, A)`; the entries above `A` come back from the **Raft log** (R2) | R2 keeps the Raft log from discarding entries above the engine's durable applied index `A` |
| partial deletion in step 6 | `(L, A)`, `LogNumber = A+1` | an arbitrary subset of `< A+1` present | replay ignores everything below `LogNumber`; sweep the rest | deletion order is irrelevant; `LogNumber` alone decides what replay reads |

Two refusals make missing segments detectable now that deletion exists: if the lowest segment
present is **above** `LogNumber`, the segment replay needs is gone — `ErrCorrupt`. A gap between
present segments at or above `LogNumber` is `ErrCorrupt`, as before. A segment **below** `LogNumber`
that is still present is a harmless leftover and is swept. This closes the Phase 2 gap that a
missing oldest segment was undetectable (`docs/WAL.md` §10).

### 9.6 R2 in the real Raft compaction path

R2 is enforced where the log is actually discarded. The engine exposes its **durable applied
index** — the applied index it is guaranteed to recover after a power loss without the Raft log,
which is the MANIFEST's flushed `Applied` — up through a small optional interface:

```go
// raftnode: a state machine that bounds log compaction by the applied index it
// can recover after a crash without the Raft log (R2).
type DurableApplier interface { DurableAppliedIndex() uint64 }
```

`Durable.compact` clamps its compaction point `c` to `min(c, sm.DurableAppliedIndex())` when the
state machine implements the interface. The in-memory store does **not** implement it: its durable
copy of the compacted prefix is the published snapshot, so it compacts exactly as before. The LSM
machine does: `DurableAppliedIndex` returns the engine's flushed `Applied`, and the Raft log is
never compacted past it. That is compaction, the only way a node discards its own log. Installing
a snapshot from a leader also moves the boundary — to the installed snapshot's index, above
whatever the engine held — and there the prefix's durable copy is the snapshot file itself,
published before the boundary moves and restored into the engine before anything else is applied
(§8.7, `docs/SNAPSHOTS.md` §8).

The clamp alone would starve compaction: an engine with the default 4 MiB memtable rarely flushes,
its durable index would sit at 0, the log would never compact, and a lagging follower would never be
sent a snapshot (§9.10). So the interface has a second method, **`SyncApplied`**, which snapshot
creation calls **before publishing** the snapshot (and so before compacting): the LSM machine
flushes its engine, advancing the durable index to the snapshot's index — even when the cycles since the last
flush wrote nothing (§9.3's empty-memtable record) — and `compact` then proceeds to
`snapshotIndex − Retain`. The compacted prefix is thereby durable in the engine's **own tables**,
not only in the snapshot file — the stronger property — at the cost of one engine flush per
snapshot, on the actor (measured in S5 with the two-log decision). A failed `SyncApplied` is a
durability failure: the node stops, as on a failed publish. It comes before the publish, not after:
otherwise a power loss between the two leaves a published snapshot above an engine that lost its
unsynced tail, and the restart must replace the whole engine from the file — which the LSM machine
refuses outright for a state larger than one restore batch (§9.10). Synced first, a node never
finds its own snapshot above its engine, and restoring it at a restart is a no-op.

What this does and does not change. In S2's architecture a compaction is always immediately
preceded by a snapshot that is fsynced at an index at or above the compaction point, and that
snapshot file is itself a durable copy of the compacted prefix — so S2 is not losing data, and S3
does not claim to fix a live loss. R2 makes the guarantee hold **by construction** rather than by
that timing argument: the compaction boundary provably never passes the engine's durable applied
index, which is exactly the property the engine-WAL-truncation argument of §9.5 (the power-loss
row) relies on, and the property §4's options B and C and S6's batched apply will need once the
Raft log stops being shadowed by a fresh snapshot at every compaction. The clamp only ever makes a
node **retain more** log, so it cannot break replication, membership or catch-up. If the durable
index cannot be read it reads as 0, which compacts nothing — fail-safe.

The boundary distinctions this rests on, stated explicitly:

- **committed** — in a majority's durable Raft logs; a write may be acknowledged.
- **applied** — the state machine has decided the entry (`core.AppliedIndex()`), in memory.
- **engine-durable (synced)** — the entry's effect survives a power loss in the engine: the
  flushed `Applied`. This, not "applied", is what bounds compaction.

### 9.7 Restart and recovery

At open, after recovering the MANIFEST and before replaying the WAL:

- Seed `applied = Applied` (the MANIFEST floor) and, when `LogNumber > 0`, `seq = LogStartSeq` and
  `appliedSeq = AppliedSeq`.
- Delete any WAL segment below `LogNumber` (a leftover from an interrupted step 6), then verify the
  lowest segment present equals `LogNumber` and the rest are contiguous — before a single record is
  replayed; a higher lowest segment, a gap, or a WAL directory that is gone is `ErrCorrupt`. With
  `LogNumber == 0` the lowest segment must be segment 1.
- Replay from `LogNumber` (or segment 1 when `LogNumber == 0`): each mutation advances `seq`; a
  record at or below the flushed sequence (the live tables' maximum `LargestSeq`) is already in a
  table and is skipped; each apply batch must advance `applied` past the MANIFEST floor (or the
  record before it), or it is `ErrCorrupt`; a `SetAppliedIndex` record is taken as written.

A flush triggered **during** replay (recovering a log larger than memory) has no WAL to rotate. It
records only `AddFile`, `SetNextFileNum` and `SetLastSequence` — the file set and the flushed
sequence — and **none of the watermark trio**: the segments it builds tables from are still being
read, so `LogNumber`, `LogStartSeq` and `Applied` must stay exactly as recovered until the next real
flush after open. This is why the seed is its own field: the replay flush legitimately moves
`LastSequence` (the tables now cover more), and seeding from it on the *next* open would renumber
the surviving records (§9.10). The tables a replay flush writes are still used — their sequences
raise the skip boundary — so nothing is replayed twice. Nothing is deleted during replay.

The result is R1's recovery, now with a truncated log: the engine is exactly its flushed state at
the MANIFEST watermark plus the surviving WAL suffix, applied once. Replay is **idempotent** — a
second crash immediately after open recovers the identical state — and the engine accepts new
writes afterward, numbered above everything durable (no durable sequence is ever reused, §9.8).

### 9.8 Compatibility and sequence continuity

- A pre-S3 directory decodes to `LogNumber == 0` (and no `LogStartSeq`) and replays from segment 1
  counting from 0, with `Applied` as it was — identical to S2. The first S3 flush then rotates and
  sets the watermark trio, enabling truncation from that point. No migration. The format change is
  the two added tags `0x07` and `0x08`; a pre-S3 binary refuses a MANIFEST that carries them, as it
  refuses any unknown tag — but a downgrade was already unsupported, because a pre-S3 binary would replay a
  truncated WAL from segment 1 and renumber it.
- After restart the next sequence is strictly above `LastSequence` and above every surviving
  record's sequence, so a durable sequence is never reused and live tables keep pairwise-disjoint
  ranges (the read path's correctness rests on this). A sequence lost to a power loss (an unsynced
  record above the watermark) is re-consumed deterministically when the Raft log replays the same
  entry — the same mutation, the same number.

### 9.9 What is deliberately deferred

- Snapshots as engine checkpoints and ingest; the 512 MiB snapshot bound (S4).
- The two-log durability measurement — whether to fsync the engine WAL per batch, sync at the
  compaction gate, or drop it — and any tightening of the R2 bound in `sync` mode (S5).
- Group commit, pipelining, batched apply, and any optimization of the per-cycle write path (S6).
- Real-device power-loss certification: the crash matrix uses the software model (`internal/fault`),
  which assumes an honest fsync and models unsynced data as lost or as a torn prefix, never as
  reordering or bit rot (`docs/FAILURE_MODEL.md`).

### 9.10 What was built, and what building it found

- The MANIFEST's `Applied` and `LogNumber` tags were present and strictly decoded since Phase 4 but
  never written during operation. S3 writes both (and keeps writing `LastSequence`) in the one edit
  a real flush appends, adds `LogStartSeq`, and reads them at open. No format version changed; a
  pre-S3 directory decodes `LogNumber == 0` and replays exactly as before.
- The flush rotates the WAL so the log number's segment begins at the frozen sequence plus one. The
  first cut recorded no separate seed and read it off `LastSequence`, which is correct right after
  any real flush. The integration gates broke it: see the two findings below.
- **Found by `make integration`: recovery was not a function of the bytes on disk.** A flush during
  replay advanced `LastSequence` (its tables cover more) without rotating or moving `LogNumber`, so
  on the *next* open the seed was too high and the surviving records were renumbered and
  double-counted — two recoveries of one directory produced sequences 190 and 285
  (`TestCrashWithOverwritesAndDeletesAcrossFiles`). The fix is the dedicated `LogStartSeq` field, set
  only together with `LogNumber` and `Applied` by a real flush and never by a replay flush. Unit tests
  on the storage package alone had passed with the bug present.
- **Found by `make integration-lsm`: the R2 clamp alone starves compaction.** With the default
  memtable the engine never flushed over a test's lifetime, its durable index stayed 0, the log
  never compacted, and eight `TestReal*Snapshot*` tests failed because no follower was ever sent a
  snapshot. The fix is `SyncApplied` (§9.6): snapshot creation flushes the engine through the
  snapshot's index before the log is compacted behind it.
- **Found by the final review: `SyncApplied` did not keep its contract after a cycle that wrote
  nothing.** `Flush` returned early on an empty memtable, so when the entries since the last flush
  were all election no-ops or configuration entries — which advance the applied index without a
  write — the durable index stayed behind the applied index, contrary to
  `raftnode.DurableApplier`'s "advancing DurableAppliedIndex to it". R2 still held (the clamp kept
  the boundary at the older durable index, retaining more log), so this was a liveness and contract
  defect, not a data loss; but it also meant a `Flush` that returned nil had synced nothing. The fix
  records the applied index alone when the memtable is empty (§9.3). Regression tests:
  `TestAFlushRecordsAnAppliedIndexThatMovedWithoutData`,
  `TestLSMMachineSyncAppliedCoversACycleWithoutWrites`, and the crash matrix, whose script now ends
  with a data-less entry and a watermark-only flush; all three fail on the old behaviour.
- **The rest of the final review.** Five independent reviewers (R1, MANIFEST authority, truncation
  order, R2, recovery), every finding checked by two skeptics and the code read rather than the
  docs, then a completeness critic. Besides the defect above it found these, each fixed with a
  regression test that fails on the old code (each test is the intended killer of a mutant that
  reverts its fix):
  - *Adopting a truncated WAL lost writes.* With no log number recorded, replay started at the
    lowest segment present and counted from zero. Before S3 that was always segment 1; S3 deletes
    prefixes, so an S3 directory that lost its MANIFEST, opened with `AdoptLegacySSTables`, replayed
    its surviving records mis-numbered, skipped the first of them as already flushed, and reported
    success. Reachable only by that operator action after damage, never by a crash. With no log
    number the log must now begin at segment 1 (`TestAdoptingATruncatedLogIsRefused`).
  - *Replay's advance check was not anchored after truncation.* It started from zero instead of the
    MANIFEST's `Applied`, so a record below the durable watermark — damage the CRC cannot see, or a
    writer bug; the writer itself refuses one — was accepted and dropped the applied index under the
    watermark R2 compacts against; §9.2 claimed the check existed. `RecoverFrom` now takes the
    MANIFEST's `Applied` as the anchor (`TestReplayIsAnchoredToTheManifestApplied`).
  - *`AppliedSequence` came back wrong after a flush.* Recovery seeded it from `LogStartSeq`,
    crediting `Put`/`Delete` made after the last apply batch to the applied index; the batch that
    held the true value had been truncated. A regression of the S1 contract, with no production
    caller (the LSM machine writes only through apply batches). Tag `0x08` `AppliedSeq` records it
    with the watermark (`TestAppliedSequenceSurvivesAFlushAfterWritesOutsideBatches`).
  - *A node whose state exceeded one restore batch could never restart after its own snapshot.* At
    every start raftnode restores the published snapshot; `RestoreSnapshot` measured the state
    (about 63 MiB, one WAL record) before noticing its engine already held that index, and refused.
    S2's ordering, but on the restart path S3's R2 work makes a no-op. The no-op is now decided
    first (`TestLSMMachineRestartsOnItsOwnSnapshotBeyondOneRestoreBatch`). Installing such a state
    from a leader is still refused — that is S4's limit.
  - *`SyncApplied` ran after the publish* (above, §9.6), leaving the window in which a restart had
    to restore the node's own snapshot whole. It now runs first
    (`TestSnapshotSyncsTheEngineFirstAndCompactsOnlyToItsDurableIndex`, which also covers the next
    item).
  - *A crash while a restore deleted the old engine left a node that could not start.* `RemoveAll`
    is not atomic; a crash part-way left an engine whose MANIFEST named a table or WAL segment that
    was gone, the open refused it (S3 added two such refusals to S2's), and the node never reached
    the restore that would replace it. Fail-stop, not loss. The old engine is now renamed aside,
    the rename fsynced, then deleted; open removes anything left aside
    (`TestLSMMachineACrashWhileReplacingTheEngineLeavesOneThatOpens`, which crashes the deletion
    part-way through a test seam).
  - *Test gaps.* Nothing exercised `compact` reading the machine's durable index — the cluster
    test's clamp premise held without it, and a mutant removing the read survived every test — and
    nothing pinned the refusal of a missing WAL directory, a gap above the log number, or a WAL
    with no segment left. The gap refusal also turned out to be backed by a second check (the WAL
    writer's listing, at the end of open), so the store-level test could not see it go; a WAL-level
    test now requires the refusal before any record reaches the engine
    (`TestRecoverFromRefusesAGapBeforeReplaying`).
  - *Stale comments* that still named `LastSequence` as the seed, said nothing acts on the log
    number, or that the MANIFEST's `Applied` is never read back, are corrected.
- **A second adversarial review, of those fixes** (five reviewers, two skeptics per finding, a
  critic), found no defect in them, and these besides:
  - *A failed restore left the machine open over an empty engine.* After the old engine was closed
    and the new one opened, a refused batch returned an error but left the machine serving: `Lookup`
    answered "absent" for keys that exist (a linearizability hazard for a read already past its
    ReadIndex) and `ApplyEntry` wrote on. From S2's code. Every failure after the close now fails
    the machine stop (`TestLSMMachineAFailedRestoreFailsStop`).
  - *A newly created engine directory was never synced into its parent,* so a power loss could
    take the whole engine — fatal for a state too large to restore from a snapshot. Synced now, as
    the WAL directory always was (`TestANewEngineDirectoryIsSyncedIntoItsParent`).
  - *Two real-process tests' premises.* `TestCrashedFlushLeavesReplayableLog` replayed the raw WAL
    from segment 1, which S3 makes true only before the flush's commit; it now reads its window from
    the MANIFEST. And the full gate on the fixes failed `TestRealFollowerReceivesSnapshot` once
    under integration-lsm: its premise that the leader found before a follower was stopped is still
    leader when it returns. Node logs captured on failure showed every failure was an election —
    the new leader streamed the snapshot and the follower installed it — and the rate is S3's
    (§9.13); the test now asserts the rule (the sender streamed it and was a leader).
  - *Test gaps:* a failed engine sync (now pinned), the crash matrix's watermark-only premise (now
    asserted), and the cluster R2 test's clamp premise, which holds without the clamp (documented;
    the raftnode test pins the wiring).
  - *Doc and comment corrections:* `DurableApplied` can exceed `AppliedIndex` after a lowering
    `SetAppliedIndex`; the replay anchor covers apply batches, not `SetAppliedIndex` records; R2
    bounds compaction, and an install is covered by its snapshot file; SSTable numbers run out
    before segment numbers; §8.6/§8.7's restore order; §9.4's equivalence.

  Reviewed and *not* changed: a duplicated field tag in one edit decodes last-wins (harmless: the
  writer never emits one); under `SyncOff`, a power loss after a flush leaves a directory recovery
  refuses (`SyncOff` is test-only and promises no durability; pre-S3 code refused the same state).
  Recorded as limitations, not fixed here (§9.13): WAL segment and SSTable numbers are six digits
  and never reused, and S3 consumes them faster (a rotation per flush and per snapshot); no test
  cuts power to a whole LSM-backed cluster, nor does the deterministic simulator run a machine with
  a durable index; and the snapshot's engine flush on the actor raises election churn under load.
- Dropping the explicit pre-MANIFEST `WAL.Sync()` was safe only because the rotation already syncs
  and closes the outgoing segment. The S1 mutants that guarded that sync (`flush-syncs-the-wal-first`,
  `replay-flush-has-no-wal-to-sync`, `syncoff-flush-does-not-fsync`) were re-anchored to the
  rotation, which now provides the same ordering.
- The S1 crash matrix had encoded the recovered prefix as the count of surviving WAL batch records.
  Truncation moves the flushed prefix out of the WAL and into the tables, so that count no longer
  described recovery. The matrix now identifies the prefix by its recovered applied index, checks it
  whole, and bounds it by `durable..attempted` — a stronger check that is medium-agnostic.
- Found while building: a process crash at a batch's fsync can still recover that batch (its record
  reached the kernel), so the matrix's upper bound is *attempted*, not *written* — the §7.2 contract
  that a failed `Apply` "may or may not be recovered."
- R2 turned out not to fix a live loss in S2's architecture — a compaction is always preceded by a
  durable snapshot at or above the compaction point, which is itself a durable copy of the prefix.
  S3 makes the guarantee hold by construction instead of by that timing argument, and it is what the
  WAL-truncation-plus-batch-mode power-loss case (§9.5) actually relies on.

### 9.11 Tests, mutants, fuzzing

| Concern | Test |
|---|---|
| WAL segments reclaimed, data intact after reopen | `TestWALTruncationReclaimsSegments` |
| The durable watermark advances only at a flush | `TestDurableAppliedAdvancesOnlyAtFlush` |
| The applied index comes from the MANIFEST when the WAL is empty after a flush | `TestAppliedIndexRecoveredFromTheManifestAfterAFlush` |
| Sequence and applied-index continuity across many reopens; no durable sequence reused | `TestSequenceAndAppliedContinuityAcrossReopen` |
| A leftover low segment is swept; recovery is idempotent | `TestLeftoverLowSegmentsAreSweptOnOpen` |
| A missing required segment is refused | `TestARequiredWALSegmentMissingIsRefused` |
| The replay-flush path records no log number and rotates nothing | `TestFlushWithoutWALRotatesNothingDuringReplay` |
| A truncated engine takes new writes and reopens | `TestTruncatedEngineAcceptsWritesAndReopens` |
| R1 through the extended crash matrix (truncation, rotation, deletion ops; process and power loss; a MANIFEST failure at each of the script's three flushes, the last a watermark-only record) | `TestApplyCrashMatrix` |
| Reopen skip/replay counts under truncation | `TestReopenAfterCleanClose`, `TestReopenWithEverythingFlushed`, `TestApplyBatchesAndRecovery` |
| R2: the compaction clamp arithmetic | `TestCompactPointClampsToDurable` |
| R2: the machine reports the engine's durable index, not the live one | `TestLSMMachineDurableAppliedIsTheEnginesFlushedIndex` |
| A flush with an empty memtable records an applied index that moved without data; with nothing new it records nothing | `TestAFlushRecordsAnAppliedIndexThatMovedWithoutData` |
| R2: `SyncApplied` brings the durable index to the applied index after a cycle that wrote nothing | `TestLSMMachineSyncAppliedCoversACycleWithoutWrites` |
| R2 in the real snapshot path: compaction bounded by a lagging durable index; the engine synced before every publish | `TestSnapshotSyncsTheEngineFirstAndCompactsOnlyToItsDurableIndex` |
| A failed engine sync fail-stops the node, nothing published or compacted | `TestAFailedEngineSyncStopsTheNodeBeforeAnythingIsPublished` |
| A restore that fails after the engine closed fails the machine stop (no absence served) | `TestLSMMachineAFailedRestoreFailsStop` |
| A new engine directory is synced into its parent | `TestANewEngineDirectoryIsSyncedIntoItsParent` |
| A process killed during a flush: the raw log holds every write before the commit, the engine recovers every write in every window | `TestCrashedFlushLeavesReplayableLog` (real process) |
| A lagging follower is sent a snapshot by a leader, whichever node leads when it returns | `TestRealFollowerReceivesSnapshot` (real processes) |
| A WAL directory gone, a gap above the log number, no segment left — all refused | `TestALostWALUnderALogNumberIsRefused`, `TestRecoverFromRefusesAGapBeforeReplaying` |
| No log number: the log must begin at segment 1 (adopting a truncated WAL is refused) | `TestAdoptingATruncatedLogIsRefused`, `TestRecoverWithoutALogNumberRequiresTheFirstSegment` |
| Replay's advance check is anchored to the MANIFEST's `Applied` | `TestReplayIsAnchoredToTheManifestApplied` |
| `AppliedSequence` survives a flush after writes outside apply batches | `TestAppliedSequenceSurvivesAFlushAfterWritesOutsideBatches` |
| A restart restores its own snapshot as a no-op, whatever the state's size | `TestLSMMachineRestartsOnItsOwnSnapshotBeyondOneRestoreBatch` |
| A crash part-way through replacing the engine leaves one that opens | `TestLSMMachineACrashWhileReplacingTheEngineLeavesOneThatOpens` |
| R2 end to end: a real three-node LSM cluster compacts, and every boundary stays within a durable copy (a smoke check — with `SyncApplied` before every publish the clamp is never binding there; its wiring is pinned by the next row but one) | `TestR2CompactionStaysWithinADurableCopy` |

Mutation: thirty correctness-critical mutants for S3, each killed by the intended regression above
and none by an unrelated failure. Eleven from the first cut — the applied index, the log number and
the log start sequence each recorded at a flush; truncation bounded to below the log number; the
sequence seeded from the MANIFEST; a missing required segment refused; a leftover segment swept;
the R2 clamp; the watermark advancing only at a flush; the machine reporting the engine's durable
index rather than the live one; snapshot creation making the engine durable (`SyncApplied`). Three
(353–355) for the empty-memtable record — made at all, made after a rotation, followed by
truncation. Thirteen (356–368) from the final review — the first-segment rule, the replay anchor,
the applied sequence recorded at both kinds of flush and seeded at recovery, the missing-directory,
gap and empty-log refusals, `compact` reading the durable index, the sync before the publish, the
restore's no-op first, the old engine set aside, and open removing it. And three (369–371) from the
review of those fixes — a failed restore failing stop, a new engine directory synced into its
parent, a failed engine sync stopping the snapshot. Three S1 mutants whose
anchor (the explicit pre-MANIFEST `WAL.Sync()`) S3 removed were re-anchored to the rotation that now
provides the same ordering: the rotation before the record, the replay flush skipping the rotation,
and the `SyncOff` fsync skip. The existing WAL, apply-batch and MANIFEST fuzz targets run unchanged
over the new record usage, the MANIFEST one now also decoding tags `0x07` and `0x08` (seeded with
both).

### 9.12 Evidence

Every gate below ran sequentially on `2408d73` (Apple M4, macOS, **go1.27.2**; nothing else
running), after the defects of §9.10 — both reviews' — were fixed. Two commits follow it: this
evidence, and a test-only fix CI exposed — `TestWireClientReportsUnknownWhenTheConnectionDies`
(Phase 13, `internal/kv`) synchronized on the server's listener closing but not on its connection
closing, a race the CI runner lost (the old connection answered UNKNOWN); nothing in that path
changed in S3, and the test now asserts its contract on every attempt. CI runs every gate on the
final commit. Earlier runs, including the ones that *found* defects, are
not reported as evidence; they are §9.10's history.

| Gate | What it covers | Result |
|---|---|---|
| `make check` (fmt, ignore, vet, every unit package under `-race`) | storage, WAL, MANIFEST, kv on both machines, raftnode, raftsim, multiraft | pass, 189 s |
| `make race-lsm` | every `internal/kv` test again with the group's machine in the engine | pass, 67 s |
| staticcheck (`SA*`, the CI pin) and govulncheck v1.8.0 | correctness checks; known vulnerabilities in the code's call graph and imports | pass; **"No vulnerabilities found."** |
| `make integration` | 89 real-process tests on the in-memory machine: linearizability, dedup/retry, snapshots, membership, chaos, the engine crash tests (`lsm_crash_test`, `crash_test`) | 89 pass, 401 s |
| `make integration-lsm` | the same 89 tests with every `dkvd` on the LSM machine, including the `TestReal*Snapshot*` tests that the first cut failed | 89 pass, 390 s |
| `make faults FAULT_SEEDS=200` | the deterministic simulator: 200-seed fault schedules, the crash, snapshot and membership matrices, seeded KV histories checked for linearizability, multi-group isolation, the bounded membership model at depth 4 | pass, 132 s |
| `make fuzz` | 35 of 35 fuzz targets, 10 s each — the WAL and apply-batch decoders, the MANIFEST edit decoder (now with tags `0x07` and `0x08`), the record framing | pass, 389 s |
| `make mutation` | all 373 mutants, including the thirty S3 mutants and the three re-anchored S1 ones (§9.11); each killed by its intended test, none by an unrelated failure | 373 / 373 killed, 966 s |

**Toolchain.** `go.mod` pins `go 1.27.2`, which CI installs (`setup-go` reads `go-version-file`).
On go1.27.1 the check job's govulncheck found nine reachable standard-library vulnerabilities
(`net/http`, its HTTP/2 implementation, `net/textproto`, `crypto/tls`: GO-2026-6603, 6605, 6607,
6608, 6610, 6611, 6612, 6613, 6617) and four more imported or required but not called (GO-2026-6599,
6600, 6604, 6609), all fixed in go1.27.2; under it govulncheck finds none. go1.27.2 writes export
data version 5, which the latest staticcheck release (v0.8.1) cannot read, so CI pins staticcheck to
the master commit that moved to `golang.org/x/tools` v0.51.0 (same `SA*` checks; it flags a planted
bug and reports nothing on this tree). No application code changed for the toolchain.

Real multi-node evidence specific to S3: `TestR2CompactionStaysWithinADurableCopy` (three LSM
nodes, snapshots every 8 entries, the default memtable so only `SyncApplied` flushes) shows every
node's compaction boundary at or below its published snapshot and its engine's durable applied
index — a smoke check, since with the sync before every publish the clamp is not binding there;
`TestSnapshotSyncsTheEngineFirstAndCompactsOnlyToItsDurableIndex` pins the clamp's wiring with a
lagging durable index; `integration-lsm` shows real `dkvd` processes compacting their logs and
sending followers snapshots on the engine, which the first cut could not.

Power-loss model and its boundary (unchanged from S1, §7.6): `fault.MemFS` keeps only fsynced bytes
and only names whose directory was fsynced, optionally with a torn prefix of unsynced appends; it
assumes an honest fsync and never models reordering, holes or bit rot. The WAL and MANIFEST are on
the model; SSTable writes still use `os` directly. Modeled power loss is not device certification.

**Benchmark (micro, not a cluster measurement).** `BenchmarkLSMPut` (the write path, `off` and
`batch` modes) and `BenchmarkLSMRecovery` (restart after 50,000 puts into 512 KiB memtables), on
`main` at S2 (`09e421c`) and on S3 (`137b28b`), each built in its own worktree, `-count=5`,
medians. Not re-run on the final revision: the fixes after `137b28b` add one varint to a flush's
edit and a record when a flush finds the memtable empty, which neither benchmark exercises; the
toolchain moved from go1.27.1 to go1.27.2.

| Benchmark | S2 (`main`) | S3 | Change |
|---|---|---|---|
| `LSMPut/off` ns/op | 4,117 (3,672–4,651) | 3,867 (3,828–4,906) | within noise |
| `LSMPut/batch` ns/op | 4,187 (4,101–4,355) | 4,609 (4,332–5,425) | **+5 to +10 %** |
| `LSMRecovery` ns/op (reopen, 50k mutations, 5 tables) | 21,418,639 | 15,259,071 | **−29 %** |
| `LSMRecovery` allocations / bytes per reopen | 254,007 / 11.37 MB | 4,442 / 0.59 MB | **−98 % / −95 %** |

Reading it. The write path in `off` mode is unchanged: S3 adds work only per flush, and in `off`
mode none of it fsyncs. In `batch` mode a flush now costs **two more directory fsyncs** — the new
segment's creation and the old segments' deletion — on top of the segment sync the pre-S3 flush
already did; amortized over the ~80 flushes of this run that is the five-to-ten-percent change
(the S3 spread also carries one slow outlier). It is a real cost, reported here and not optimized:
whether to batch those fsyncs, or defer deletion off the flush path, is S6's question. Recovery is
the expected win: replay reads only the records after the last flush instead of all 50,000, so the
reopen does 29 % less work and allocates two orders of magnitude less. These are single-machine
micro-benchmarks; the cluster-level cost of S3 is not measured here (the S2 steady-state numbers in
§8.13 remain the latest cluster measurement).

### 9.13 What S3 proves, and what it does not

**PROVEN** (by the tests and mutants of §9.11, on the final revision, §9.12):
- **The WAL is truncated safely.** A segment is deleted only after a durable MANIFEST edit records a
  higher log number, which requires the segment's records to be in a flushed table and that table
  recorded. Recovery starts at the log number, its sequence seeded from the MANIFEST's
  `LogStartSeq`, and the crash matrix confirms R1 holds at every rotation, MANIFEST and deletion
  boundary — of a data flush and of a watermark-only record — under a process crash and the modeled
  power loss.
- **The MANIFEST is authoritative** for the applied index, the log number, the replay seed and the
  applied sequence: an empty WAL after a flush still recovers the applied index, including one
  moved by entries that wrote nothing; the sequence continues upward across reopens with no durable
  sequence reused; `AppliedSequence` survives truncation. Recovery refuses — before replaying
  anything — a missing required segment, a gap, a WAL directory that is gone, and a log without a
  log number that does not begin at segment 1; it refuses a replayed apply batch that does not
  advance past the MANIFEST's `Applied`. A leftover segment is swept; recovery is idempotent.
- **R2 holds in the real compaction path.** A self-compacting node never advances its log-compaction
  boundary past the engine's durable applied index — the clamp's arithmetic, its wiring in the real
  snapshot path with a lagging durable index, and a real three-node LSM cluster. Snapshot creation
  brings the durable index to the snapshot's (`SyncApplied`), also after cycles that wrote nothing,
  and does so before publishing, so a node's own snapshot is never above its engine. The in-memory
  store is unchanged, covered by its published snapshot.
- **R1 is intact.** Every applied transition is still recorded atomically with its applied index; the
  extended matrix checks state, applied index, term and both sequences together.
- **A node restarts on its own snapshot whatever its size, and a crash while a restore replaces the
  engine leaves one that opens** (the crash injected part-way through the deletion).

**SUPPORTED BUT NOT FULLY PROVEN:**
- **Real-device power-loss durability.** The matrix uses the software fault model (`internal/fault`),
  which assumes an honest fsync and models unsynced data as lost or as a torn prefix, never as
  reordering or bit rot (`docs/FAILURE_MODEL.md`). **Modeled power loss is not real-device
  certification.**
- **The engine's SSTable and compaction code under power loss inside the hosted machine.** The crash
  tests put the WAL and MANIFEST on the model; SSTable file writes still use `os` directly (as in
  S1/S2). Likewise the restore's set-aside rename and its directory fsync run on the real filesystem:
  tested against a process crash, not a modeled power loss.
- **R2 under cluster-wide crashes.** R2 is tested by unit, single-node and real-cluster tests, but no
  test cuts power to a whole LSM-backed cluster, and the deterministic simulator's state machine
  does not report a durable index, so its crash schedules do not exercise the clamp.
- **The tightest R2 bound.** The durable watermark is the flushed applied index; in `sync` WAL mode
  the synced-but-unflushed suffix is also recoverable, so the log could be retained less
  conservatively. That optimization is the two-log decision (S5).

**DEFERRED** (known, not fixed in S3):
- Snapshots as engine checkpoints and streamed ingest; the effective limit that a snapshot the LSM
  machine *installs from a leader* must fit one WAL record (about 63 MiB), and the 512 MiB snapshot
  bound (S4).
- The two-log durability measurement and any WAL-mode-dependent R2 tightening (S5).
- Group commit, batched apply, per-cycle write-path optimization, and taking the snapshot's engine
  flush off the actor (S6). That flush is not only latency: under fsync-heavy load it can cost a
  leader its term. Measured with `TestRealFollowerReceivesSnapshot` (three LSM nodes, a snapshot
  every 16 entries, a 60-write window, node logs captured on failure, the same background load): a
  leader change inside the window in 10/60 and 7/60 runs on S3 revisions, 0/60 on `main`. The
  elections are correct Raft behaviour — the new leader streamed the snapshot, the follower
  installed it — but they are a liveness cost S3 introduced, larger at small snapshot intervals.
- **File-number exhaustion.** WAL segment and SSTable numbers are six digits and never reused. S3
  rotates the WAL at every flush and flushes the engine at every snapshot, so it consumes them
  several times faster than S2 did. Under a sustained 2,000 small writes per second with the default
  `-snapshot-every 10000` — a snapshot every 5 s and a memtable flush about every 20 s, some 21,600
  rotations a day — the 999,999 segment numbers last about 46 days, and the SSTable numbers, which
  compaction outputs also use, run out sooner; then every flush fails and the node fail-stops (no
  loss; a restart does not help). A wider name format is the fix; it is a
  format change of its own, and should precede any long-running deployment.

## 10. S4 — engine checkpoints and streamed snapshot installation

S4 replaces "the whole state, encoded in memory, installed as one WAL record" with a consistent
engine checkpoint streamed into the snapshot file, and an installation that streams the file into
new SSTables and switches the engine to them with one MANIFEST edit. Its exit criterion: a follower
installs, and restarts on, a snapshot larger than the ~63 MiB S3 left, with every S1–S3
guarantee intact. R1 and R2 are unchanged and must stay intact.

The design below was written against `main` at `a3c886f` (S3 merged) before any S4 code; §10.15
onward record what was built, what building it found, and what it proves.

### 10.1 Audit: the snapshot path at S3, traced

| Step | Code | What it holds in memory | What bounds it |
|---|---|---|---|
| Create (leader, on the actor) | `Durable.Snapshot` → `LSMMachine.EncodeSnapshot` (`db.Snapshot()`: every key into a map; `encodeState`: the bytes) → `snapshot.Encode` (the file) → `SyncApplied` → `Files.Publish` | the state ~3–4 times | `snapshot.MaxData` = 512 MiB |
| Send | `Snapshots.SendFile` loads, validates and **caches** the whole file for the node's life; `snapshot.Split` into 1 MiB chunks; `transfer` sends them on its own goroutine | the file | `MaxFile` |
| Receive | `Receiver.Accept` appends each in-order chunk to `.snap.recv`; when complete: fsync, `readFile` (all of it), `Decode`, `ValidateSnapshot` → `decodeState` (a map) and, for the LSM machine, `restoreBatchBytes ≤ maxRestoreBytes` | the file, then the state ~2 times | **`maxRestoreBytes` = `record.MaxRecordSize` − 1 MiB ≈ 63 MiB** — the effective install limit |
| Install | `Durable.InstallSnapshot`: term/vote, `PublishReceived` (rename, dir fsync), `Log.Install` (boundary), `RestoreSnapshot(data)`: close the engine, rename it aside, fsync the parent, delete it, open a fresh engine, **one `Apply` batch** holding every key and session record, fsync the parent | the state ~2 times | one WAL record (64 MiB) |
| Restart | `Recover`: `Files.Load` (the whole file), reconcile, `RestoreSnapshot` (a no-op when the engine is at or above the snapshot, decided first since S3) | the file | — |
| Open the machine | `loadSessions` reads `db.Snapshot()` — **every key** — to find the session records | the state | — |

So the ~63 MiB limit is one constant, but behind it every stage holds the whole state at least
once. Lifting the constant alone (or raising the WAL record limit) would leave a 500 MiB snapshot
needing gigabytes of memory at both ends; S4 removes the whole-state buffers on the LSM path.

### 10.2 The decision: what a snapshot is, and what is streamed

**A logical, key-ordered stream in the existing snapshot format.** The snapshot file stays exactly
what it is today — QSNP v2 around the key-value state encoding v1 (`docs/SNAPSHOTS.md` §3): a
header binding group, configuration, index, term, the state's length and its SHA-256; data
records of at most 1 MiB, each with its own CRC-32C; a footer repeating index and term. The kv
state encoding is already key-ordered: every user key ascending, then every session ascending by
id — and that is exactly the engine's key order under the machine's tags (`0x01` user keys sort
before `0x02` session records, and a session key is its id big-endian). So the stream the leader
produces from its engine, and the stream the follower builds SSTables from, are the same ordered
sequence the format already defines. **No on-disk or wire format changes in S4.**

Why this representation is safe: every byte is covered by a record CRC; the whole state by the
SHA-256 in a header that also names the group, index and term, with the footer repeating index and
term; the state encoding is canonical, so a duplicated, reordered or missing key or session is
undecodable, and the session rules (`docs/SNAPSHOTS.md` §3) reject any session table no sequence
of commands could produce; the chunk rules (`docs/SNAPSHOTS.md` §9) never assemble a file from
duplicated, reordered, stale or missing chunks. The receiver builds its own SSTables from
validated logical content; it never adopts a file a peer produced.

**Rejected: a physical checkpoint** — the engine's live SSTables hard-linked into a checkpoint
directory with their own MANIFEST, streamed as a file set. It would add a second durable artifact
beside the published snapshot (two records of "the snapshot at `i`", two commit protocols to
prove); couple the snapshot to the SSTable format and to the sender's sequence numbering, so the
receiver would have to trust and renumber a peer's tables; and the in-memory reference machine,
the simulator and the Phase 14 snapshot crash matrix could no longer install or exercise it.
`fault.MemFS` does not model hard links either.

### 10.3 The engine checkpoint

**What it is.** `LSMStore.Checkpoint()` makes the engine durable through its applied index and pins
it: under the write lock it flushes (S3's protocol; an empty memtable records the applied index
alone) and acquires the current **version** — the immutable set of tables the MANIFEST names, with
an empty memtable. The checkpoint is that pinned version: the engine's state at applied index
`(i, t)`, exactly the MANIFEST's durable `Applied`. It is read in key order through the
compaction merge (`internal/storage/compaction`, `BottomMost`: the version holds the whole
database, so a tombstone hides nothing), which yields each live key's newest value once.

**Consistency model.** The version's reference count keeps every table it names open and on disk
until the checkpoint is closed, as it does for any read (INV-S5): a compaction that runs meanwhile
publishes a new version and its inputs are deleted only when the checkpoint releases them. Writes
after the pin go to the new memtable and are not in the checkpoint. So the checkpoint is one point
in the apply order — never a mixture — whatever the engine does meanwhile. Iterating it any
number of times yields the same bytes.

**Index and term.** The checkpoint carries the `Applied` it was pinned at. It is never above the
durable watermark: it *is* the durable watermark (`DurableApplied()` at the pin, checked). The
machine requires it to equal its own applied index (no cycle staged) and raftnode requires that to
equal the core's applied index, whose term and configuration (`core.TermAt`, `core.ConfAt`) go in
the snapshot's `Meta`, as today.

**Durable form and publication.** The checkpoint's durable, transferable form is the published
snapshot file: the checkpoint streamed into `.snap.tmp` (fsync), renamed over `.snap`, the
directory fsynced (`Files.Publish`'s protocol, unchanged). That rename-and-directory-fsync is the
publication commit point; at restart the snapshot is discovered by its name, and a `.snap.tmp` is
an orphan that `Recover` removes — an abandoned checkpoint is never mistaken for a published one.
A failed creation leaves the engine untouched (the checkpoint only reads) and the previous snapshot
published; the pin is released either way.

### 10.4 Creation

`Durable.Snapshot`, at applied index `i`, on the actor (moving it off is S6):
1. `OpenSnapshot` — the checkpoint (flush + pin) — and the existing guards: the machine's index must
   equal the core's applied index, be above the published snapshot, and have a known configuration.
2. `SyncApplied` (S3; a no-op now, the checkpoint having flushed), before anything is published.
3. Stream into `.snap.tmp`. The format's header needs the state's length and SHA-256 before the
   data, and the state's header needs the key count, so the file is written in passes over the same
   pinned checkpoint: count (the machine's source, first), measure and hash (`snapshot` package),
   write. The writer hashes what it writes and refuses to publish if it differs from the measured
   pass — a source that does not reproduce its bytes is a bug, never a published snapshot. A state
   over `MaxData` is refused at the measuring pass, before anything is written (unchanged
   behaviour: skipped until `Every` more entries).
4. fsync, rename, directory fsync — publication; release the checkpoint; compact (R2's clamp,
   unchanged).

The in-memory store's source is its encoded bytes, so its snapshot is byte-for-byte what it was.
The LSM machine's source must produce **the same bytes the reference encoder produces for the same
state** — tested differentially, and the reason no format changes.

### 10.5 Transfer and receipt

- **Sender.** The leader opens the published file and reads its header (the snapshot it names must
  be the published one); the transfer goroutine then verifies the whole file (a streaming pass:
  every record, the length, the hash, the footer) and sends it in 1 MiB chunks read from that open
  handle — never the whole file in memory, and nothing cached. A later publish renames a new file
  over the name; the open handle still reads the snapshot the transfer started with.
- **Receiver.** The chunk rules are unchanged (offset 0 starts a transfer and abandons any other;
  every other chunk must continue it exactly, or is ignored). `Total ≤ MaxFile` bounds the disk a
  transfer can take; memory is one chunk. When the last chunk arrives: fsync `.snap.recv`, then
  validate **by streaming it**: the framing (each record's CRC, kinds, sizes, the length, the hash,
  the footer, nothing after) and the announced index and term; then the state machine validates the
  state by streaming it — every rule `decodeState` applies, without building the state. Only then is
  `MsgSnapshot` stepped into the core. A failed validation abandons the transfer; the leader offers
  again; the partial or rejected file is never active state and is truncated by the next transfer
  or removed at startup. A retry starts from offset 0 (no resume): simple, and correct by the rules
  above.
- **A repeated offer (closeout, §10.15).** "Memory is one chunk" is the receiver's own; frames
  queue ahead of it — the transport's receive queue (256 frames) and the driver's chunk channel
  (16) — and fill whenever the actor is busy. The leader re-offers a snapshot its follower has not
  answered within `SnapshotRetryTicks`, and proving and ingesting ~85 MiB outlasts that, so the
  whole repeat used to wait there: measured, a second copy of the state in the follower's heap
  during its install. The node's frame pump, off the actor, now drops a chunk of a snapshot no
  newer than the one being proven or installed (`Snapshots.Installing`: from the chunk that
  completes a transfer until it is installed, refused or unstaged; outside that window nothing is
  dropped, and a refused proof clears it, so the next offer is received). A transfer of a *newer*
  snapshot arriving during an install still queues, bounded by those 272 frames.

### 10.6 Installation by ingest — the engine's commit point

`LSMStore.Ingest(src, (i, t))` replaces the engine's entire contents with a key-ordered stream,
atomically, at applied index `(i, t)`. The LSM machine's restore feeds it the published snapshot's
state as engine entries: each user key under its tag, then each session record re-encoded, building
the new session mirror as they pass. Steps, holding the close, compaction and write locks (no
writer and no compaction runs during an ingest):

1. **Stage.** Write SSTables from the stream: entries strictly ascending (checked), each within the
   engine's key and value limits, numbered with fresh sequence numbers above every one the engine
   has used, cut at a target size; each table is written to `N.sst.tmp`, fsynced, renamed to
   `N.sst`. A staged table is named by no MANIFEST edit, so by INV-S6 it is not part of the
   engine: a crash leaves it an orphan, swept at the next open.
2. **End of stream.** The source must end cleanly: for a snapshot this means the reader reached the
   footer with the length and the SHA-256 verified, and the state decoder found nothing after the
   last session. Any failure here — or in step 1 — deletes the staged tables and returns with the
   engine **unchanged and usable**.
3. **Make the staged names durable:** fsync the engine directory.
4. **Rotate the WAL** to a new, empty, durable segment `n` (S3's rotation: sync and close the old
   segment, create `n`, fsync the WAL directory).
5. **Commit:** append ONE MANIFEST edit, fsynced — `DeleteFile` every live table, `AddFile` every
   staged table, `SetNextFileNum`, `SetLastSequence(S)`, `SetLogNumber(n)`, `SetLogStartSeq(S)`,
   `SetApplied(i, t)`, `SetAppliedSeq(S)`, where `S` is the last staged sequence. **This is the
   commit point.** Before it the MANIFEST names the old tables and the old log; after it, only the
   new tables and an empty log that starts at `S + 1`, at applied index `(i, t)`. The old WAL
   records are below the new log number, so nothing of the old state is ever replayed onto the new.
6. **Switch:** install a version with a fresh memtable and only the new tables; the applied index,
   the durable watermark (R2's `DurableApplied`) and both sequences become the edit's.
7. **Retire:** the old tables are deleted once no reader holds them (the existing reference
   counting); the WAL segments below `n` are deleted. A failure here costs space only: the next open
   sweeps unreferenced tables and segments below the log number.

A failure in step 4 or 5 is **ambiguous** (the edit may be on disk): the engine latches it, as a
failed flush does, refuses every later write, and the machine fails stop; the next open reads
whichever edit is durable — old or new, never both. Nothing advances the durable watermark before
the append has returned success.

Ingested tables are placed at the shallowest level ≥ 1 whose size budget holds them, so the level-0
file-count trigger does not immediately rewrite them; they have disjoint key ranges and disjoint,
ascending sequence ranges, the property the read path and the open-time coherence check rely on.

### 10.7 The installation state machine (follower)

| # | State | Durable on disk | Restart selects |
|---|---|---|---|
| 0 | no installation | the old engine; the old published snapshot (if any) | the old engine |
| 1 | partial transfer | `.snap.recv` partly written | removed by `Recover`; the old engine |
| 2 | complete, validated, staged (in memory) | `.snap.recv` fsynced | removed; the old engine — the leader offers again |
| 3 | **published** — the Raft-level commit point | `.snap` = the new snapshot (rename + dir fsync); the log's boundary may or may not be recorded | `Recover` completes the boundary if missing (Phase 14), then restores: the engine (old, index `< i`) is replaced by ingest from `.snap` |
| 4 | ingest staging | new `.sst` files no MANIFEST edit names | open sweeps them; the engine is the old one; `Recover` ingests again |
| 5 | **ingest committed** — the engine's commit point | the MANIFEST names only the new tables at `(i, t)`; old tables and segments may remain | open sweeps the old files; the engine is the new one; `Recover`'s restore is a no-op (`e ≥ i`) |
| 6 | old engine retired | the new engine only | the new engine |

Two commit points, ordered: the snapshot file is published before the engine's ingest commits, so
whenever the engine holds the new state the published snapshot covers it, and whenever the engine
still holds the old state the published snapshot is there to install it again. "Old or new, never
a mixture" is the MANIFEST's: it names one table set. "Never silently stale" is `Recover`'s: an
engine below the published snapshot is restored before anything is applied, exactly as at S3.

### 10.8 Recovery

At open the engine recovers the MANIFEST, refuses a referenced table that is missing, sweeps
unreferenced tables (staged or retired), and replays the WAL from the log number — after a
committed ingest, from the empty segment the ingest rotated to. Then `raftnode.Recover` verifies
the published snapshot by streaming it (bounded memory, the same checks as at receipt) and calls
the machine's restore with a stream over it; the machine decides the no-op first, before reading
anything (S3). A crash at any point of a restore leaves state 3, 4, 5 or 6 above, each of which
`Recover` handles; a second crash right after recovery finds one of the same states, so recovery
is idempotent. Every ambiguity found at open — a referenced table missing, a corrupt published
snapshot, a MANIFEST that does not decode — is refused (the node does not start), never guessed.

### 10.9 R2 and consistency with Raft

- **Creation:** unchanged — `SyncApplied` before publication, the checkpoint's index is the durable
  index, and compaction stays clamped to `min(snapshot − Retain, DurableAppliedIndex)`.
- **Installation:** the log's boundary moves to `i` at step 3 of `InstallSnapshot`, covered by the
  published file (S3's statement: R2 bounds compaction; an install is covered by its snapshot file).
  The engine's durable index jumps to `i` at the ingest commit, so after it the node's own later
  compactions are clamped at or above `i`. A failed or ambiguous ingest never advances it.
- **Sessions and the applied index:** the session records travel inside the snapshot and are
  ingested with the data in the same commit; the machine's mirror is rebuilt from them; the applied
  index and term are the snapshot's. A retry of a request the snapshot covers is answered
  `Duplicate`, as after any restore.

### 10.10 Compatibility

- **Formats:** none change. A snapshot published before S4 loads, sends and installs; a snapshot an
  S4 leader publishes is byte-identical to what the S3 encoder produced for the same state, so an
  S3 follower can install it (within its own 63 MiB limit) and an S4 follower can install an S3
  leader's.
- **Engine directories:** an S3 directory opens unchanged; ingest uses only existing MANIFEST tags.
  S3's restore set the old engine aside as `<dir>.restore-aside`; open still removes one.
- **The in-memory machine and the simulator** keep their byte-slice encodings behind the streaming
  interface; the Phase 14 matrices exercise the new plumbing unchanged.

### 10.11 The `vfs` seam reaches the SSTables and the MANIFEST's reads

The ingest's genuine new persistence operations are SSTable writes, fsyncs, renames, directory
fsyncs and deletions. Testing them under the power-loss model needs them on the model, so
`storage.Options.FS` (nil: the OS) now carries every SSTable and engine-directory operation —
creation, write, fsync, rename, removal, listing, reads — and the MANIFEST's reads join its writes
on `manifest.FS`. `vfs.File` gains `ReadAt`, which an SSTable reader needs (`*os.File` has it;
`fault.MemFS` implements it). Production behaviour is unchanged: everything is the OS.

### 10.12 Residual limits (by design, stated)

- `snapshot.MaxData` stays 512 MiB: S4 lifts the ~63 MiB install limit to the format's bound. On the
  LSM path nothing holds the state in memory any more, so the bound is a policy constant; raising
  it is a separate change (an S3 node would refuse such a snapshot with `ErrTooLarge`, safely).
- Disk during an install: the received file, the staged tables and the old engine coexist until the
  commit — about three times the state.
- Creation is three passes over the checkpoint, on the actor. Taking it off the actor is S6.
- A transfer interrupted part-way restarts from offset 0.
- Frames queue ahead of the receiver (§10.5): a newer snapshot's transfer arriving during an
  install, or any traffic while the actor is busy, can hold up to 272 frames (256 in the
  transport, 16 in the driver) — up to 272 MiB of 1 MiB chunks. A leader's repeat of the same
  snapshot is dropped; the leader still reads and sends it.

### 10.13 Test plan

- **Checkpoint:** empty and large engines; writes after the pin invisible; a compaction during the
  read does not disturb it; its index is the durable index; repeated creation releases every pin.
- **Ingest:** replace, reopen, empty, larger than the old limit; unordered or over-limit input
  refused with the engine unchanged; a failure at every step; the crash matrix below.
- **Crash matrix (`TestIngestCrashMatrix`):** every filesystem operation of an ingest — table
  writes (whole and torn), fsyncs, renames, directory fsyncs, the rotation, the MANIFEST append, the
  deletions — under a process crash and a power loss (with and without a torn tail); each cell
  records the operation and the fault, and checks that the engine opens as exactly the old state or
  exactly the new, matching a reference model, that nothing committed was lost and nothing
  uncommitted is visible, and that a second crash right after recovery changes nothing.
- **Snapshot stream:** the streaming writer is byte-identical to `Encode`; the streaming reader
  agrees with `Decode` on every input (a differential fuzz target); the receiver validates by
  streaming; a transfer reads the snapshot it started with across a later publish.
- **kv:** the LSM source is byte-identical to the reference encoding of the same state; the
  streaming validator agrees with `decodeState` on the known-good and known-bad corpus and under
  fuzzing; a restore above the old limit; sessions and deduplication across a restore; a failed
  restore fails stop.
- **raftnode:** the existing snapshot, install and crash-point tests on the streaming interface;
  the Phase 14 snapshot crash matrix and the simulator unchanged.
- **Real processes:** a follower installs a snapshot larger than 63 MiB from an LSM leader,
  restarts, converges, serves reads and writes, and answers a retried request as a duplicate; a
  follower crashed during installation recovers.
- **Mutants** for every faulty variant listed in the S4 brief that applies to this design.

### 10.14 Deliberately deferred

- S5 (the two-log measurement) and S6 (group commit, pipelining, taking snapshot creation and the
  snapshot flush off the actor).
- Raising `snapshot.MaxData`; resuming an interrupted transfer; real-device power loss.
- The separately tracked follow-ups, each its own issue and not part of S4: file-number exhaustion
  (#20), the S2-era intermittent read rejection (#21), the snapshot flush's election churn (#22, S6),
  and `SyncOff`'s power-loss refusal (#23); opened at the closeout: the leader's repeated sends and
  count-bounded queues (#24), the staticcheck pin (#25), and a Phase 12 test's premise (#26).

### 10.15 What was built, and what building it found

**Built**, in the order of the branch's commits:
- **The `vfs` seam reaches the whole engine** (§10.11): `storage.Options.FS`; `sstable.OpenFS` /
  `WriteFileFS`; the MANIFEST's `*FS` functions, reads included; `vfs.File` gains `ReadAt`, which
  `fault.MemFS` implements. With `Options.FS` nil, production is unchanged.
- **Streamed snapshots** (`internal/snapshot`): `Measure`, `Write` / `WriteMeasured` (byte-identical
  to `Encode`; a source that does not reproduce what was measured is `ErrSourceChanged`), the
  streaming `Reader` (`Decode`'s checks in `Decode`'s order, a record at a time, `io.EOF` only once
  the whole file is proven; a failing disk reported as itself), `Files.PublishFrom` /
  `PublishMeasured` / `Open` / `OpenReceived` / `Verify`, a receiver that proves a completed
  transfer by streaming it, and `Sending` — a published snapshot opened for a transfer, proven,
  and sent from that handle.
- **A streamed state-machine interface** (`raftnode`): `OpenSnapshot` (a `SnapshotSource`),
  `ValidateSnapshotFrom`, `RestoreSnapshotFrom`; creation (cheap guards, open, measure, sync,
  publish), sending, receipt, install and recovery all stream; `event=raft_snapshot_installed`.
- **Engine checkpoints and ingest** (`internal/storage`): `LSMStore.Checkpoint` (flush, then pin the
  version: its index is the durable watermark), `Scan`, `Latched`, and `Ingest` (§10.6).
- **The LSM machine on them** (`internal/kv`): one state decoder (`stateReader`) that `decodeState`
  delegates to; a snapshot source over a checkpoint, byte-identical to the store's encoding; a
  restore that ingests, keeping the old engine until its commit; the session table loaded by a scan.

**What building it found:**
1. **The ~63 MiB constant hid three whole-state buffers.** The leader cached the published file for
   the node's life (`SendFile`), the receiver read the whole completed file back to validate it,
   and the LSM machine read its whole engine into a map at every open to find its session records.
   All three now stream (§10.1).
2. **The S3 restore's fail-stop rule was a consequence of closing the old engine first.** With an
   ingest the old engine survives every failure before the commit, so a refused restore is now
   harmless and the machine keeps serving the old state; only a commit in doubt fails it stop. The
   S2/S3 tests that encoded the old shape were rewritten to the rule that now holds: the oversized
   state validates; the restart test proves the no-op reads nothing; the set-aside crash test is a
   crash during the ingest; the fail-stop test is split into a refused restore and a commit in doubt.
3. **No format had to change.** The plan of §5 called for a new streamed format with a trailer; the
   existing header-checksummed format streams if the writer measures first, and its per-record
   CRCs make it checkable a record at a time. Keeping it made the in-memory machine's snapshots
   byte-for-byte unchanged and every existing corpus, matrix and simulator test an oracle.
4. **The install order test gained a step**: the restore now reads the *published* file, after the
   boundary — pinned by `TestInstallOrderIsTermPublishBoundary`.
5. **The real-process tests' whole-run check took minutes** at these sizes: it compared node states
   by formatting ~85 MiB maps under `-race`. It now compares the SHA-256 of their canonical
   encodings (~15 s per test).
6. **One mutant was equivalent** (385, §10.16).

**An adversarial review** — three independent reviewers, one each on the engine, the snapshot and
driver plumbing, and the machine, reading the code rather than the docs — found **no high- or
medium-confidence defect**. It found these, each fixed with a test that fails on the old code and a
mutant that reverts the fix (399–405):
- *An oversized state was found only while publishing*: after `SyncApplied`, after the
  before-snapshot-publish crash point, after creating `.snap.tmp` — Phase 14 refused it before any
  of these, and nothing tested the skip path. The measuring pass is now its own step, taken first
  (`TestAStateTooLargeToSnapshotIsSkippedBeforeAnySideEffect`).
- *The state was opened before the cheap guards*: for the LSM machine a flush and a pass over the
  engine, at every applied entry while a joiner does not yet know its configuration. The index,
  term and configuration checks now come first.
- *A failing disk read as a corrupt snapshot*: the framing reports any short read as damage. The
  reader now reports the stream's own error (`TestAReadFailureIsReportedAsItselfNotAsCorruption`).
- *A session the engine cannot store* (a group run with `-session-max-unacked` above ~21,800, at the
  1 MiB value limit) was validated by the LSM machine and then failed to restore at every start —
  since S2. It is now refused at validation (`TestLSMMachineRefusesASessionItsEngineCannotStore`).
- *The decoder's oracle was circular*: `decodeState` delegates to the new decoder. The S3 decoder is
  frozen in a test and the new one held to it (`TestTheStreamDecoderIsTheS3Decoder`,
  `FuzzStreamDecoderMatchesS3`).
- Not reachable by the current callers, made impossible anyway: a checkpoint closed concurrently
  released its version twice (could delete live tables); `SetAppliedIndex` ignored the latch after
  an ingest in doubt; a transfer's verification ignored shutdown.
- Coverage: the ingest matrix ran only the `sync` WAL mode; it now runs `batch`, the production
  default, too.
- Recorded, not changed: `Scan` is coherent only when nothing writes (the machine scans at open);
  a checkpoint read after its store closed fails, never answers wrongly; a MANIFEST edit too large
  to encode would need some 8,000 ingested tables (about 128 at the default memtable for 512 MiB);
  in the simulator a publish is now several write operations and `SendFile` re-reads a file after
  each publish (seeds replay differently; no invariant changed); open scans every key to find the
  session records (memory bounded, I/O proportional to the state).

**The closeout** — a final verification of the review-fix revision (`6d044cc`): every gate run on
it, and what the real-process tests had not measured, measured. It found:
- *A survivor in the full mutation run* — 344c (S3: creating a snapshot does not make the engine
  durable) passed its S3 killer, the kv cluster test. Investigated: since S4 the LSM machine's
  `OpenSnapshot` takes a checkpoint, which flushes, so its `SyncApplied` is a second flush that
  finds nothing to do — an empty memtable at the durable index: no rotation, no edit. The rule is
  the driver's, for any `DurableApplier`, whose contract does not require `OpenSnapshot` to flush;
  the mutant is now killed by the raftnode tests in which an engine becomes durable only through
  `SyncApplied`. Retargeted, not weakened; the call stays.
- *A repeated offer held a second copy of the state.* `TestRealFollowerInstallsASnapshotAboveTheS3Limit`
  now samples the installing follower's heap from its `/metrics`. Alone, the follower held 6–8 MiB
  while receiving and proving ~85 MiB and 9–11 MiB while installing it (87–94 MiB resident). But
  whenever the leader had re-offered meanwhile — it does after `SnapshotRetryTicks`, 1 s at the
  tests' tick, and the transfer, proof and ingest took 1.0–1.9 s — the follower held 23–149 MiB
  during the install and 136–547 MiB resident: the repeat, received whole into its transport queue
  behind the busy actor (§10.5). That is Phase 14's retry meeting Phase 9's queue, not an S4 regression — but
  S4's sizes made it the common case (3 of 4 runs under moderate load), and S4's claim "memory is
  one chunk" held only for the receiver itself. Fixed in the driver, with no change to the core,
  the transport or the protocol: the frame pump drops a repeat as it arrives (§10.5). Under the same
  CPU contention: 13–18 MiB, 103–121 MiB resident; the test now asserts the bound on the LSM machine
  and fails at 97 and 120 MiB with the drop disabled. What remains — the leader still sends the
  repeat; a *newer* snapshot arriving during an install still queues — is #24.
- *Two evidence gaps.* No real-process test killed a follower while a large snapshot was still
  streaming in: one does now (nothing published, the engine still the old state, read offline; the
  restart is sent the snapshot again and converges). And the engine-fit check sized a session by a
  formula the boundary test could not hold exactly (it sits 4 bytes over the limit and passes a
  1-byte undercount): an exact-length test and mutant 406 now do.
- *Recorded precisely, not changed:* an oversized state is refused before any snapshot side effect
  — no `SyncApplied`, no crash point, no file — but on the LSM machine after `OpenSnapshot`'s
  checkpoint has flushed the engine: an engine durability step, not a snapshot one.
- *An unrelated failure in the first gate run:* `TestRealWriteToPartitionedLeaderNeverTakesEffect`
  (Phase 12, the in-memory machine, no snapshot on its path; its code unchanged since `main`),
  after a long host sleep — its premise requires the deposed leader to follow one *specific* new
  leader, and an election after the heal (here n1 at term 7) legitimately breaks it. It passed on the rerun (93/93 on
  `ecf046d`, §10.17) and is tracked as #26, with a second question it raises (the healed link
  carried no Raft traffic for 20 s).

### 10.16 Tests, mutants, fuzzing

| Concern | Test |
|---|---|
| The whole engine runs, compacts and recovers on the model filesystem, never touching the real disk; a new engine directory outlives a power loss only because its parent is synced | `TestTheEngineRunsOnTheModelFilesystem`, `TestANewEngineDirectoryOutlivesAPowerLoss` |
| A checkpoint is the durable state at its applied index (the index is the durable watermark), unchanged by later writes, flushes and a compaction; its tables stay on disk until it is closed, then are reaped | `TestACheckpointIsTheDurableStateAtItsAppliedIndex` |
| An empty engine's checkpoint; repeated checkpoints release every pin; a failed checkpoint flush latches, keeps the durable index and pins nothing | `TestACheckpointOfAnEmptyEngineIsEmpty`, `TestRepeatedCheckpointsReleaseEveryPin`, `TestAFailedCheckpointFlushLatchesAndPinsNothing` |
| An ingest replaces everything (old keys gone, empty values present, sequences above the old, old tables and WAL segments retired), at the ingest's applied index and term, durable, across a reopen; an empty ingest | `TestIngestReplacesTheWholeEngine`, `TestIngestOfNothingEmptiesTheEngine` |
| Every refusal — order, repeats, key and value limits, an index or term that does not advance, a source failing at its end — leaves the engine unchanged and usable, nothing staged on disk | `TestARefusedIngestLeavesTheEngineUnchangedAndUsable` |
| 80 MiB ingested, past the 64 MiB WAL record; ingested tables are not compacted away | `TestAnIngestLargerThanOneWALRecord`, `TestIngestedTablesAreNotCompactedAway` |
| A commit in doubt latches, never moves the durable watermark, and reopens old or new; a latched store refuses `SetAppliedIndex` | `TestAFailedIngestCommitLatchesAndReopensOldOrNew`, `TestALatchedStoreRefusesSetAppliedIndex` |
| A checkpoint closed from many goroutines releases its version once | `TestACheckpointClosedFromManyGoroutinesReleasesOnce` |
| Crash safety of the ingest at every filesystem operation, with the whole engine on the model, in `sync` and `batch` WAL modes (§10.17) | `TestIngestCrashMatrix` |
| The streamed writer equals `Encode` at every data-record boundary; refuses an oversized state before writing; refuses a source that does not reproduce itself | `TestWriteIsByteIdenticalToEncode`, `TestWriteRefusesAStateOverTheBoundBeforeWritingAnything`, `TestWriteRefusesASourceThatDoesNotReproduceItself` |
| The streaming reader agrees with `Decode` on the corpus, every truncation and bit flip, and under fuzzing; reports the end only once the file is proven; reports a failing disk as itself, not as corruption | `TestReaderAgreesWithDecodeOnTheCorpus`, `FuzzReaderAgreesWithDecode`, `TestReaderReportsEndOnlyOnceTheFileIsProven`, `TestAReadFailureIsReportedAsItselfNotAsCorruption` |
| A streamed publish is atomic under every crash (process, power loss); a refused one publishes nothing | `TestPublishFromIsByteIdenticalAndAtomicUnderEveryCrash`, `TestARefusedStreamedPublishPublishesNothing` |
| A send reads the snapshot it opened across a later publish (MemFS and OS); chunks reassemble; a retried transfer starts clean; a transfer's verification stops with the node | `TestASendReadsTheSnapshotItOpenedAcrossALaterPublish`, `TestARetriedTransferStartsClean`, `TestASendsVerificationStopsWhenItsContextIsDone` |
| A state too large to snapshot is refused by the measuring pass before any side effect — no engine sync, no publish crash point, no file — and not tried again for `Every` entries | `TestAStateTooLargeToSnapshotIsSkippedBeforeAnySideEffect` |
| Bounded memory: a 96 MiB snapshot published, sent, received and read back grows the live heap by ~3 MiB | `TestStreamingHoldsOneRecordAtATime` |
| The restore reads only the snapshot it was asked for; the install streams its restore from the published file after the boundary | `TestARestoreReadsOnlyTheSnapshotItWasAskedFor`, `TestInstallOrderIsTermPublishBoundary` |
| One decoder for both machines: every corpus state judged alike, with the same reason; restores equal; the decoder is the frozen S3 decoder's equal on the corpus, every truncation and bit flip, and under fuzzing | `TestBothMachinesJudgeTheSnapshotCorpusAlike`, the Phase 14 corpus tests (`TestRestoreRefusesImpossibleState`, …) now through the stream decoder, `TestTheStreamDecoderIsTheS3Decoder`, `FuzzStreamDecoderMatchesS3` |
| The LSM machine refuses at validation a session whose record its engine cannot store, sizing it exactly as `encodeSession` writes it | `TestLSMMachineRefusesASessionItsEngineCannotStore`, `TestSessionRecordLenIsTheEncodedLength` |
| A leader's repeated offer, arriving while the follower proves or installs the first copy, is dropped where it arrives — one proof, not one per offer — and the follower still catches up; "installing" spans exactly the proof and the install (never a transfer still arriving, never a refused proof) | `TestARepeatedOfferIsDroppedWhileTheFirstIsProven`, `TestInstallingCoversExactlyTheProofAndTheInstall` |
| The LSM machine's streamed snapshot is the store's encoding byte for byte, with flushes and compactions underneath, and restores into a fresh machine that snapshots to the same bytes | `TestLSMMachineSnapshotIsTheStoresEncodingOnSeededScripts` |
| A 67 MiB state (above the S3 limit) published and restored from a stream: the store's state at the snapshot's index and term, sessions answering retries as duplicates, across a reopen, ~13 MiB peak live-heap growth | `TestLSMMachineInstallsAStateAboveTheS3Limit` |
| The source refuses an engine that is not the machine's state, and a staged cycle | `TestLSMMachineSnapshotIsRefusedWhenTheEngineIsNotTheMachine`, `TestLSMMachineSourceRefusesAStagedCycle` |
| The restart's restore of the node's own snapshot reads nothing | `TestLSMMachineRestartsOnItsOwnSnapshotWithoutReadingIt` |
| A crash while staging leaves the old state, after the commit the new, each opening; a refused restore changes nothing; a commit in doubt fails stop and recovers; open removes an S3 set-aside engine | `TestLSMMachineACrashDuringARestoreLeavesTheOldStateOrTheNew`, `TestLSMMachineARefusedRestoreChangesNothing`, `TestLSMMachineARestoreWhoseCommitIsInDoubtFailsStop`, `TestLSMMachineOpenRemovesAnS3SetAsideEngine` |
| A real follower installs a snapshot above the S3 limit from whichever node leads — on the LSM machine with its sampled heap below half the snapshot in both phases — restarts on it, converges, and a retried request is a duplicate; killed after the install's boundary it re-ingests; SIGKILLed during the install it recovers; SIGKILLed mid-transfer nothing is published, its engine is the old state, and it is sent the snapshot again — each ending with every node's engine read back offline | `TestRealFollowerInstallsASnapshotAboveTheS3Limit`, `TestRealFollowerDiesBetweenALargeInstallsBoundaryAndItsRestore`, `TestRealFollowerKilledDuringALargeInstallRecovers`, `TestRealFollowerKilledMidTransferOfALargeSnapshotRecovers` |

The S2/S3 tests whose premise was the one-batch restore were rewritten, not deleted, each to the
rule that now holds (§10.15): the oversized state validates; the restart test proves the no-op
reads nothing; the set-aside crash test is a crash during the ingest; the fail-stop test is split
into a refused restore (harmless) and a commit in doubt (still fail-stop).

**Mutation.** Thirty-nine S4 mutants (372–411; 412 in the script), each killed by its intended
test and none by an unrelated failure. Twenty-six (372–398) are the faulty variants of the S4 brief
that apply to this design: publishing a
snapshot before its bytes are durable, a source that changes, the streamed reader's checksum,
length, footer and trailing-bytes checks, a receiver that does not prove the staged file, a retried
transfer reusing a partial one, a restore of another snapshot than the one named, a checkpoint not
flushed or not pinned, an ingest committing before its source ends cleanly, an ingested table not
fsynced, the old log kept, old tables kept (a mixture), the watermark advanced before the commit,
a commit in doubt deleting its tables, the old log retired before the commit, the wrong term, keys
out of order, a state at the wrong index, the session table dropped from a snapshot, a checkpoint
that is not the machine's state, a refused restore failing stop, trailing state bytes, and a session
scan that stops early. Seven (399–405) revert the review's fixes and six (406–411) the
closeout's (§10.15). One candidate (385: ignoring a failed rename of a staged table) survived and
was investigated: it is equivalent — the reader open that follows opens the table by its final name,
so a rename that did not happen fails the ingest there, and no other file can hold that name (numbers
are never reused; open sweeps unreferenced tables) — and is recorded rather than kept. Ten older
mutants whose code S4 moved were re-anchored to the code that now implements the same rule; 367,
which guarded S3's set-aside (deleted by S4), now guards the property it protected — a crash during
a restore leaves an engine that opens — on the ingest's directory fsync. And 344c (S3), which the
first full run after S4 found surviving its kv-level killer, is equivalent there (the checkpoint
already flushed) and is killed where its rule lives, in the driver's tests (§10.15).

**Fuzzing.** Two new targets, `FuzzReaderAgreesWithDecode` (the streaming reader against `Decode`)
and `FuzzStreamDecoderMatchesS3` (the state decoder against the frozen S3 decoder);
`FuzzRestoreSnapshot` and the other kv snapshot targets now exercise the stream decoder.

### 10.17 Evidence

Every gate below ran sequentially on `ecf046d` (Apple M4, macOS, **go1.27.2**, the machine kept
awake, no other test running), the last commit that changes code or tests. The commit after it is
this evidence — documentation only (`git diff ecf046d HEAD` names only files under `docs/`). CI
runs every gate again on the final commit. Earlier runs are not reported as evidence; one of
them is history worth stating — see *An unrelated failure* below.

| Gate | What it covers | Result |
|---|---|---|
| `make check` (fmt, ignore, vet, every unit package under `-race`) | storage — including the ingest crash matrix in both WAL modes — WAL, MANIFEST, snapshot, kv on the in-memory machine, raftnode, raftsim, multiraft; 37 packages | pass, 203 s |
| `make race-lsm` | every `internal/kv` test again with the group's machine in the engine | pass, 76 s |
| staticcheck (`SA*`, the CI pin, #25) and govulncheck v1.8.0 | correctness checks; known vulnerabilities in the code's call graph and imports | pass (no findings); **"No vulnerabilities found."** |
| `make integration` | 93 real-process tests on the in-memory machine: linearizability, dedup/retry, snapshots — the four large-snapshot tests included — membership, chaos, the engine crash tests | 93 pass, 448 s |
| `make integration-lsm` | the same 93 tests with every `dkvd` on the LSM machine | 93 pass, 459 s |
| `make faults FAULT_SEEDS=200` | the deterministic simulator: 200-seed fault schedules, the crash, snapshot and membership matrices, seeded KV histories checked for linearizability, multi-group isolation, the bounded membership model at depth 4, the pinned trace hash | pass, 151 s |
| `make fuzz` | 37 of 37 fuzz targets, 10 s each — `FuzzReaderAgreesWithDecode` and `FuzzStreamDecoderMatchesS3` (S4) among them | pass, 407 s |
| `make mutation` | all 412 mutants — the thirty-nine S4 mutants (372–411) and 344c retargeted (§10.15) among them; each killed by its intended test, none by an unrelated failure | **412 / 412 killed**, 1048 s |
| `TestIngestCrashMatrix -v` (under `-race`; it also runs inside `make check`) | the ingest's crash matrix, verbose, for the per-mode record below | **1192 / 1192 cells** recover exactly one whole state, 10 s |

**The S4 exit criterion, on real processes.** Real `dkvd` processes
(`tests/integration/snapshot_large_test.go`), in both suites above, each test ending with every
node's durable state compared by SHA-256 and every LSM node's engine opened offline and compared
with it:

| Test | In-memory machine | LSM machine |
|---|---|---|
| `TestRealFollowerInstallsASnapshotAboveTheS3Limit` | 84 MiB installed from n3, 1.2 s; restart, writes, duplicate retry: pass | 84 MiB installed from n3, 1.0 s; heap ≤ 7 MiB receiving and proving, ≤ 9 MiB installing (sampled), 92 MiB peak resident; one transfer; restart, writes, duplicate retry: pass; engines at 105, durable through 96–97 |
| `TestRealFollowerDiesBetweenALargeInstallsBoundaryAndItsRestore` | pass | pass; the restart ingests the published snapshot; engines at 104 |
| `TestRealFollowerKilledDuringALargeInstallRecovers` | killed with snapshot 97 published, install incomplete: pass | killed with snapshot 96 published, install incomplete: pass |
| `TestRealFollowerKilledMidTransferOfALargeSnapshotRecovers` | killed with 4 MiB of an 87 MiB snapshot received; restart installs 86 MiB: pass | killed 4 MiB in; nothing published, engine ≤ 7 (offline); restart installs 84 MiB: pass |

The in-memory machine's follower held 350 MiB of heap (1252 MiB resident) installing 84 MiB, with
three transfers sent to it: it decodes and holds the state by design; nothing is asserted there.
On the LSM machine the heap bound is asserted; the closeout's repeated measurements, with and
without CPU contention and with and without a repeated offer, are in §10.15.

**The ingest crash matrix.** 1192 cells, 596 per WAL mode (`sync` and the production default
`batch`): a crash before each filesystem operation of the ingest and of the writes after it (132
over both modes), under a process crash and three modeled power losses — unsynced data lost, kept
as a 7-byte torn prefix, or kept whole — plus torn writes, a crash after the ingest returns, and an
injected I/O error at each of the ingest's operations (100 over both modes). Every cell recovers
exactly one whole state — the old one, the new one, or the new one with the batch written after it
— durably across a second power loss, and the engine goes on. Per mode and fault (old / new /
new-and-after): `sync` 64–65 / 12 / 72–73; `batch` 64–65 / 12–30 / 54–73. In both modes a crash
before the MANIFEST record is written recovers the old state; between its write and its fsync,
either (a power loss may drop the unsynced record); after the fsync, the new: the commit point is
that one fsync.

**An unrelated failure.** The first run of these gates, on `6d044cc`, had one failure that is
not S4's: `TestRealWriteToPartitionedLeaderNeverTakesEffect` (Phase 12, in-memory machine, no snapshot on its
path, its code unchanged since `main`) waited for the deposed leader to follow one specific new
leader, and an election after the heal broke that premise. It passed in this run; it is #26.
That run also lost the network for staticcheck and govulncheck after a host sleep, and its mutation
run found 344c surviving (§10.15). None of it is reported as evidence here.

Power-loss model and its boundary: `fault.MemFS` keeps only fsynced bytes and only names whose
directory was fsynced, optionally with a torn prefix of unsynced appends; it assumes an honest
fsync and never models reordering, holes or bit rot. Since S4 the whole engine — WAL, MANIFEST
and SSTables — is on the model. Modeled power loss is not device certification.

### 10.18 What S4 proves, and what it does not

**PROVEN WITHIN THE TEST MODEL** (the tests and mutants of §10.16, passing on `ecf046d`, §10.17):
- **A follower installs, and restarts on, a snapshot larger than the S3 limit.** Real `dkvd`
  processes on both machines: a follower behind every other node's compaction boundary is sent an
  83–86 MiB snapshot (above the ~63 MiB one-WAL-record limit S3 left) by whichever node leads,
  installs it, restarts on its own copy, converges, takes writes, and answers a retried request as
  a duplicate of its original index; every node's engine, read back offline, holds exactly its
  durable state at its own applied index, durable at or above its published snapshot. In process,
  a 67 MiB state installs from a stream and an 80 MiB ingest commits.
- **Interruption never exposes partial state.** On real processes: SIGKILL while the snapshot is
  still streaming in (nothing published, the engine still the old state, read offline), at the
  install's boundary before the ingest (the restart ingests the published snapshot), and during
  the install wherever the kill lands — each recovers and converges. In process, a crash at every
  filesystem operation of an ingest (below) and of a streamed publish.
- **The engine's commit point for an install is one MANIFEST edit, and a crash anywhere recovers
  exactly the old state or exactly the new one.** `TestIngestCrashMatrix`: 1192 cells, in the
  `sync` and the production `batch` WAL modes — a process crash and a modeled power loss (with and
  without a torn tail) at every filesystem operation from the ingest on, an injected I/O error at
  every ingest operation — every cell recovers old, new, or new plus the next batch, whole, never a
  mixture, nothing acknowledged lost, no unreferenced table left, durably across a second power
  loss, and the engine goes on. Before the MANIFEST write: old; after its fsync: new; at the fsync
  itself, either.
- **Partial and invalid input never becomes state.** A partial or abandoned transfer is a staging
  file never read; a complete one is proven — every record's CRC, its length, its SHA-256, its
  footer, nothing after it, its index and term — and its state validated by the decoder both
  machines share (held to a frozen copy of the S3 decoder on a corpus, every truncation and bit
  flip, and under fuzzing) before the core sees it; the restore commits only after the stream
  reports a proven end. A failing disk is reported as itself, not as corruption.
- **The checkpoint is consistent and never above the durable watermark**: the flushed, pinned
  version whose index is the MANIFEST's durable `Applied`, unchanged by later writes and
  compaction, released exactly once; its streamed encoding is the reference encoding of the same
  state, byte for byte.
- **Sessions and deduplication survive an install**, with the applied index and term (unit,
  machine and real-process evidence), and a session the LSM engine cannot store is refused at
  validation, sized exactly.
- **R1, R2 and the S3 rules are intact.** The install's restore never advances the engine's durable
  index before its commit; a commit in doubt latches the engine (refusing even `SetAppliedIndex`)
  and fails the machine stop; the log's boundary moves at install only to a published snapshot;
  compaction is clamped to the engine's durable index in the real path; the S1–S3 crash matrices,
  R2 tests and every existing tier pass on both machines.
- **Ordering of creation**: the cheap guards before the checkpoint; an oversized state refused
  before `SyncApplied`, the publish crash point and any file; the engine synced before the publish.
- **Bounded memory on the LSM path** — the snapshot code: streaming a 96 MiB snapshot through
  publication, transfer, receipt and read-back grows the live heap by ~3 MiB; restoring a 67 MiB
  state by ~13 MiB; and on real processes, the installing follower's sampled heap stays below half
  the snapshot in both phases — asserted; 6–18 MiB measured for ~85 MiB, including under CPU
  contention and with a leader's repeated offer, which is dropped as it arrives.
- **No format changed**; the in-memory machine and the simulator run through the same streamed
  plumbing; the Phase 14 snapshot crash matrix and the 200-seed simulator pass unchanged.

**SUPPORTED BUT NOT FULLY PROVEN:**
- **Real-device durability.** The ingest matrix runs on the software model (`internal/fault`): an
  honest fsync, unsynced data lost or kept as a torn prefix — never reordered sectors, a lying
  device cache, partial-sector writes, holes or bit rot, and no particular filesystem's semantics.
  **Simulated power loss is not real-hardware certification.** The real-process tests kill
  processes (SIGKILL), which proves only that data reached the kernel.
- **Process-level memory in general.** The bound above is the snapshot path's, measured by sampling
  (5 ms) on one machine under `-race`; frames queued ahead of a busy actor are bounded by count
  (272), not bytes, so a newer snapshot's transfer arriving during an install can still wait in
  memory (#24); the in-memory machine holds and decodes the state by design (84 MiB measured at
  ~350 MiB of heap).
- **The leader's verification before sending.** A transfer proves the published file before
  streaming it; no test corrupts a leader's published snapshot to observe it (the receiver's proof
  is tested, so a corrupt file would still never be installed).
- **Crash timing in the real-process kill tests.** The kill lands wherever the install or transfer
  is when the trigger is seen; each run reports where. The deterministic in-process tests and the
  matrix cover each window exactly.
- **Sizes near the format bound.** The largest snapshots tested are ~87 MiB (real processes) and
  96 MiB (the stream alone); nothing near 512 MiB was run.
- **The adversarial review.** Three independent reviewers found no high- or medium-confidence
  defect; their findings and the closeout's are fixed or recorded (§10.15). A review is evidence,
  not proof.

**DEFERRED / KNOWN LIMITATIONS** (not done in S4):
- Snapshot work on the actor (S6, #22): creation's three passes, and a follower's proof and ingest,
  pause that node's Raft processing — 1–2 s for ~85 MiB under `-race` here; the leader still sends
  a repeated offer the follower drops, and count-bounded queues remain (#24).
- Raising `snapshot.MaxData`; resuming an interrupted transfer; incremental snapshots; seeking the
  session records at open instead of scanning every key; disk for about three times the state
  during an install.
- The two-log measurement (S5).
- Separately tracked, not touched by S4: six-digit file-number exhaustion (#20) — a reliability
  blocker before any long-running deployment, which S4's ingest makes come sooner (a table number
  per ~memtable of state); the S2-era intermittent read rejection (#21); the snapshot flush's
  election churn on the leader's actor (#22, S6); `SyncOff`'s power-loss refusal (#23); the
  staticcheck pin (#25); and a Phase 12 test whose premise is too strong (#26).
