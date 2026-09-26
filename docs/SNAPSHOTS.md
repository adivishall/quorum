# SNAPSHOTS — state snapshots and log compaction (Phase 14)

Status: **design — Phase 14 in progress.** This document fixes the architecture before code is
written; each section is re-stated with its evidence (tests, crash windows, mutants,
measurements) when the phase completes.

The problem: the Raft log grows without bound (`docs/LIMITATIONS.md`), and every restart replays
all of it. A snapshot captures the replicated state at an applied index so the log prefix it
covers can be discarded — but the snapshot is then the **only** record of that prefix. It is part
of the replicated state and of Raft's recovery model, not a serialized map:

```
committed prefix  →  snapshot at index A  →  discard the prefix  →  later restart / follower catch-up  →  identical state
```

---

## 1. What owns what

| Responsibility | Owner | Why there |
|---|---|---|
| the log's **boundary** `(index, term)` of the last compacted entry | `replication.Log` (`MemoryLog`) | the core reads terms and slices through it; the boundary is where `Term(i)` stops being answerable |
| deciding a follower needs a snapshot (`nextIndex ≤ boundary`), the retry of an unanswered one, the follower's accept/ignore decision, resetting the log to a snapshot | `internal/raft` (pure) | these are Raft rules (§7 of the paper); they change `commitIndex`, the log and replication progress |
| the snapshot **file format**, publication, loading, validation, the chunk codec, reassembly | `internal/snapshot` (new) | a format and a file protocol, like `raftlog`'s — no Raft decisions |
| the **state** inside a snapshot (key-value map + session table) | `internal/kv` (`Store.Snapshot`, `Store.Restore`) | only the state machine knows its state; Phase 13's session table is part of it |
| the boundary record and the compaction rewrite of the durable log | `internal/raftlog` | the durable log's format and recovery |
| when to snapshot, the ordering of publication / compaction / installation, recovery that reconciles snapshot + log, streaming | `internal/raftnode` (driver) | impure, I/O-ordering work — shared with the simulator as single functions (ADR-017) |

The core stays pure: it never sees snapshot bytes. It knows a snapshot as metadata `(index,
term)`; the driver moves the bytes.

## 2. Snapshot state — inclusion and exclusion

**Included** (everything a later command's decision can depend on): the key-value map; the
applied index the snapshot represents; the snapshot's term (the term of the entry at that index);
the session table — every session's id, `last` (the LRU key), `ackedBelow`, and every remembered
result `(requestID, fingerprint, index)`; the session limits the table was built under (they
change decisions, so a snapshot built under other limits is refused); the group identity.

**Excluded**, each with its reason: `currentTerm`/`votedFor` (the durable log's HardState owns
them); leader identity, `nextIndex`/`matchIndex`, election timers, votes received (volatile Raft
state, rebuilt by the protocol); in-flight requests, waiters, transport connections (volatile
driver state); the store's decision counters (observability, not replicated state — they restart
at zero after a restore and are documented as "since this incarnation's base").

## 3. Format (`internal/snapshot`)

A snapshot file is a sequence of records in the shared §2 framing (`internal/record`,
CRC-32C per record), in a snapshot-file kind namespace:

```
Header  (kind 1): magic "QSNP" | version=1 | group | index | term | dataLen | SHA-256(data)
Data    (kind 2): up to 1 MiB of state bytes each, in order        (repeated)
Footer  (kind 3): index | term                                       (end marker, cross-checked)
EOF
```

Integers are canonical uvarints; byte strings are length-prefixed and bounded. A file is valid
only if it is exactly: one header, data records whose concatenation is `dataLen` bytes hashing to
the header's SHA-256, one footer repeating the header's index and term, then end of file. There is
no torn-tail repair: a published snapshot was fsynced before it was published, so any damage is
corruption and the node refuses to start (§7).

The state bytes are the state machine's canonical encoding (`kv.Store.Snapshot`): version, applied
index, limits, the keys in strictly ascending order with their values, the sessions in strictly
ascending id order, each session's results in strictly ascending request-id order. Strict
ordering makes the encoding canonical (one state, one byte string) and makes duplicates
undecodable. Decoding validates every bound and every cross-field rule (§7).

No timestamp, random id or host name appears anywhere: the same state produces the same bytes.

## 4. Trigger

A node snapshots when `appliedIndex − snapshotIndex ≥ SnapshotEvery` (entries; a configuration
value, 0 = never), checked after each apply cycle; tests can also request one explicitly. The
trigger reads only indexes — never a clock — so it is deterministic in the simulator. Every node
snapshots independently (Raft §7).

## 5. Creation, publication and compaction (the order, and why)

At applied index `A` (the actor applies entries, so the store is exactly the state at `A`):

1. serialize the state (memory only);
2. write `<log>.snap.tmp`, fsync it;
3. rename it over `<log>.snap`, fsync the directory — **publication**: from here the snapshot is
   the durable record of `[1, A]`;
4. rewrite the log without the prefix: write `<log>.tmp` = boundary record `(A, term(A))` +
   the current HardState + entries `A+1..last`, fsync it, rename it over the log, fsync the
   directory — **durable compaction**;
5. discard the prefix in memory (`core.Compact(A)`).

The log is never compacted before the snapshot covering it is durable: after every step recovery
finds either the old state or the new one. Step 4 cannot run before step 3 finishes (a mutant
pins it). Crashing between 3 and 4 leaves the new snapshot with the old, longer log — coherent
(§6). One snapshot is retained: publication replaces the previous one atomically, and temporary
files are removed at startup.

## 6. Recovery

Remove temporary files; load and fully validate the snapshot (if any); open the log (which now
reports its boundary `B`); reconcile with the snapshot index `S`:

| Durable state | Decision |
|---|---|
| no snapshot, `B = 0` | as before Phase 14 (full log) |
| snapshot `S`, `B ≤ S`, the log holds `S` with the snapshot's term | state = snapshot; keep log entries `> S` |
| snapshot `S`, the log does not reach `S` | state = snapshot; log empty after `S` (an install that crashed before its log record) |
| snapshot `S`, the log holds `S` with another term, persisted commit `< S` | state = snapshot; discard the log (the install rule) |
| snapshot `S`, the log holds `S` with another term, persisted commit `≥ S` | **refuse** — two committed entries at one index |
| `B > S`, or `B > 0` with no snapshot | **refuse** — the log was compacted past the only snapshot |
| `B = S` with a different term | **refuse** |
| snapshot invalid (torn, checksum, version, group, index/term, malformed state) | **refuse** |

Then: the in-memory log starts after `S`; `commit = max(clamped persisted commit, S)`;
`applied = S`; the state machine is restored from the snapshot before any entry is applied. The
core validates that `currentTerm ≥` the last log term, which after a snapshot is the snapshot's
term — so installation must make the new term durable before the snapshot (§8).

## 7. Corruption policy

Loud and total, as for the log (`docs/CRASH_RECOVERY.md` §9): an invalid published snapshot, or a
snapshot that contradicts the log, refuses to start — never a silent fallback. A snapshot being
received is validated completely before the core ever sees it; an invalid one is discarded and
the leader retries.

## 8. Follower installation (Raft §7)

- **Leader:** when `nextIndex[peer] ≤ boundary`, the core emits `MsgSnapshot(index, term)` (metadata)
  and marks the peer *snapshot-pending*; while pending it sends the peer only heartbeats
  (`prevLogIndex = boundary`), ignores its rejections, and re-emits the snapshot after a bounded
  number of heartbeat rounds without an answer. A success at or beyond the snapshot index ends the
  pending state and replication resumes.
- **Transfer:** the driver streams the published file over transport kind 20 (`InstallSnapshot`)
  in chunks of at most 1 MiB (frames are capped at 16 MiB — `docs/TRANSPORT.md` §2): `term, index,
  snapTerm, total, offset, data`. At most one transfer per peer; chunks are accepted only in order;
  the receiver writes `<log>.snap.recv`, and only a complete file that validates is handed to the
  core. A partial snapshot is never active state.
- **Follower core:** term rules as for AppendEntries. A snapshot at or below the commit index is
  already covered: answer success at the commit index, change nothing. Otherwise reset the log to
  the snapshot — keeping entries after it if the log holds the snapshot's index with its term,
  discarding the log otherwise — set commit and applied to the index, and ask the driver (a
  `Ready.Snapshot`) to make it durable before the response leaves: `MsgSnapshotResponse` on kind 21.
- **Follower driver, in order:** persist a changed term/vote first (so the durable term is never
  below the snapshot's); publish the received file; append the boundary record and the new commit;
  restore the state machine; then send the response.

## 9. Invariants (proposed; each is adopted only with its tests)

- **INV-SN1** A published snapshot is exactly the replicated state at its index: equal to the
  reference model folded over the committed prefix through that index, session table included.
- **INV-SN2** Nothing partial or invalid becomes active: a node's state machine is only ever
  restored from a complete snapshot that validated.
- **INV-SN3** Compaction never discards the only record of committed state: a log's boundary is
  never above its node's durable snapshot index.
- **INV-SN4** Snapshot + suffix is equivalent to the full log: the state after any restore equals
  the state replaying the uncompacted committed prefix would produce.
- **INV-SN5** Deduplication survives snapshots: every request's decision after a restore is the
  decision the reference session model makes without snapshots.
- **INV-SN6** A follower that installed a snapshot resumes replication: its log matches the
  leader's at the snapshot index and converges.

## 10. Scope

In: all of the above, simulated and on real processes, under faults and crashes. Out: dynamic
membership, multiple groups, snapshots of anything but the one group's state machine, incremental
or copy-on-write snapshots, a snapshot size beyond the documented bound, and any claim about real
power loss.
