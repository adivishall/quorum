# STORAGE INTEGRATION — the LSM engine as the replicated state machine

Status: **design and audit; nothing here is implemented.** This document is audited against the
code at PR #10's head (`a8eb9e1`, 2026-10-07). It fixes who owns each invariant, so that no code
moves before that is clear. `docs/ENGINEERING_ROADMAP.md` §0 states the project's thesis, and this
is its first layer-2 deliverable.

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

Today a snapshot is the state encoded as one byte slice in the actor. The format puts the length
and SHA-256 in its header, so it cannot be streamed, and both ends hold the whole state in memory
(up to 512 MiB). Hosting changes the content of a snapshot and keeps its metadata and Raft's rules.

- **Creating one:** take a consistent engine checkpoint at applied index `i`: a pinned version
  (immutable SSTables, reference-counted) plus the memtable at `i`. Iterate it in key order **off
  the actor**: data keys, then the session prefix, with `applied = (i, term)`. The snapshot's
  `Meta` (index, term, configuration) stays `raftnode`'s and is unchanged.
- **Transfer:** a streamed format with per-chunk checksums and a trailer, replacing the
  header-checksummed blob. The chunked transfer protocol already exists (`docs/SNAPSHOTS.md` §9).
- **Installing:** build SSTables from the received stream into a staging directory, then replace
  the engine's state with one MANIFEST edit (**ingest**), recording the applied index. A crash
  before that edit leaves the old state; a crash after it leaves the new one; staging files are
  swept at open. Raft's install ordering is kept: the response never precedes the durable install
  (`DrainReadyAt`).
- **Log truncation** after a checkpoint is bounded by **R2**.

Every snapshot guarantee already proven (INV-SN, the snapshot crash matrix, ErrSuperseded for
writes a snapshot replaced) must hold unchanged. They are re-run against the hosted engine, not
re-derived.

## 6. Milestones

Each milestone is one PR, with its own tests, mutants, measurements and documentation, and each
leaves `main` green.

| # | Milestone | Touches | Exit criterion |
|---|---|---|---|
| **S1** | **Atomic apply batches in the engine** (§7) | `internal/storage` only | **R1** proven in isolation by a deterministic crash and power-loss matrix |
| S2 | The engine behind the state-machine interface, with sessions under a reserved prefix; `dkvd -state-machine=memory\|lsm` | `internal/kv`, `cmd/dkvd` | every existing linearizability, dedup, snapshot, membership and chaos tier passes on both state machines; recovery still replays from the snapshot (engine state discarded at start) |
| S3 | Recovery from the engine's applied index (exactly-once across restarts); log compaction gated by **R2**; the engine's WAL truncated, with its applied index and last sequence made authoritative in the MANIFEST first (§7.1, §7.4) | `raftnode`, `kv`, `storage` | the hosted crash matrix (§3.2, and `docs/ENGINEERING_ROADMAP.md` §3 item 4) and a replaced INV-CR4 |
| S4 | Snapshots as checkpoints, streamed, installed by ingest | `storage`, `snapshot`, `raftnode` | the snapshot crash matrix on the hosted engine |
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
SSTables and the MANIFEST's reads stay on `os` until S4.

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
the MANIFEST has its own matrix (`docs/MANIFEST.md` §10), and SSTables move onto `vfs` in S4.

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
- **Mutants, one per R1 failure mode, each with its killer named in `scripts/mutation.sh`:**
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
