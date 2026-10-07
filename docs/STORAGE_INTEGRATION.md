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
| S3 | Recovery from the engine's applied index (exactly-once across restarts); log compaction gated by **R2** | `raftnode`, `kv` | the hosted crash matrix (§3.2, and `docs/ENGINEERING_ROADMAP.md` §3 item 4) and a replaced INV-CR4 |
| S4 | Snapshots as checkpoints, streamed, installed by ingest | `storage`, `snapshot`, `raftnode` | the snapshot crash matrix on the hosted engine |
| S5 | The two-log decision: A, B and C measured | `storage`, `dkvlab` | a published comparison against the baseline, and the chosen option |
| S6 | Performance: group commit, pipelining, batched apply | `raftnode`, `raft` | before-and-after measurements |

Chaos end to end, PreVote and CheckQuorum, and the delivery surface follow, in the order
`docs/ENGINEERING_ROADMAP.md` §0 gives.

## 7. S1 — atomic apply batches in the engine

The smallest step that establishes the central invariant. It stays inside the engine, so the
replicated system is not touched.

**Builds:**
1. `LSMStore.Apply(batch, applied)`: ordered puts and deletes plus an applied `(index, term)`,
   written as **one** WAL record, a new record kind, decoded strictly. It is inserted into the
   memtable only after the append succeeds. The applied index advances only forward; a batch
   whose index does not exceed the current one is refused.
2. **Recovery:** the engine returns the state and the applied index of the longest prefix of whole
   apply records. A torn tail is discarded whole, as today. The data and the index are never
   recovered apart.
3. **The MANIFEST's `Applied`** is set at each flush to the applied index of the last batch the
   flushed memtable holds, and read back at open. The WAL's value wins only when it is newer. This
   makes the index survive the WAL truncation that S3 and S5 need.
4. **The WAL on `internal/vfs`:** segment create, write, sync, rename and directory sync, so the
   crash matrix can run on `fault.MemFS`. SSTables follow in S4. Until then the matrix covers WAL
   and MANIFEST I/O and treats SSTable writes as atomic, and says so.

**Tests:**
- A deterministic crash and power-loss matrix over every I/O operation of a scripted sequence of
  apply batches, flushes and reopens: write, torn write, fsync, rename, directory fsync. At each
  cell the recovered `(state, applied)` must equal the reference model after exactly `applied`
  batches, and `applied` must be at least the last batch whose sync returned.
- Fuzzing the new record decoder.
- The existing conformance suite, unchanged.

**Mutants:**
- data and index written as two records;
- memtable insertion before the append succeeds;
- MANIFEST `Applied` taken from the wrong memtable;
- the applied index allowed to go backwards;
- a torn record half-applied.

**Measures:** `dkvbench` before and after, comparing one batch per cycle with N single-operation
writes: records, bytes and fsyncs per entry.

**Out of scope:**
- any change to `dkvd`, `raftnode` or `kv`;
- WAL truncation;
- checkpoints and ingest;
- SSTables on `vfs`.

**Risk:** contained. The engine is still standalone, and its standalone API and recovery are
unchanged for callers that never use `Apply`.
