# STORAGE INTEGRATION — the LSM engine as the replicated state machine

Status: **S1 and S2 are implemented (§7, §8); S3–S6 are design.** The design was audited against the code at
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
| **S1** ✓ | **Atomic apply batches in the engine** (§7) | `internal/storage`, its WAL on `internal/vfs` | **R1**'s storage half proven in isolation by a deterministic crash and power-loss matrix — done, §7.9–§7.11 |
| **S2** ✓ | **The engine behind the state-machine interface** (§8), sessions under a reserved tag; `dkvd -state-machine memory\|lsm` | `internal/kv`, `raftnode` (a cycle hook, the term on restore), `multiraft`, `cmd/dkvd` | every existing linearizability, dedup, snapshot, membership and chaos tier passes on both machines; the engine's applied index is the durable authority at restart — done, §8.9–§8.12 |
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
  SSTables move onto `vfs` in S4.
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
- a published snapshot at index `i` is validated as today; if `e >= i`, the machine's state is at
  least as new and `RestoreSnapshot` changes nothing; if `e < i`, the engine is replaced by the
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

`RestoreSnapshot(index, term, data)` validates the state as today, then replaces the engine: close
it, remove its directory, open it fresh, and record the whole state — user keys, session records
— in **one** `Apply` at `(index, term)`. The replacement is atomic under R1: either the batch is
recovered whole, or the engine is empty with applied index 0, below the published snapshot, and the
next open restores it again. A snapshot whose state does not fit one WAL record (64 MiB) is refused
by `ValidateSnapshot`, so the core never installs it; today's bound is 512 MiB, and this is an S2
limitation lifted by S4's ingest.

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
