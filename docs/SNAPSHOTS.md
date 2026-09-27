# SNAPSHOTS — state snapshots and log compaction (Phase 14)

Status: **implemented and verified** (Phase 14). Every claim below names its evidence; §15 states
exactly what is guaranteed and §16 what is not.

The problem: the Raft log grew without bound, and every restart replayed all of it. A snapshot
captures the replicated state at an applied index so the log prefix it covers can be discarded —
but the snapshot is then the **only** record of that prefix. It is part of the replicated state and
of Raft's recovery model, not a serialized map:

```
committed prefix  →  snapshot at index S  →  discard the prefix  →  later restart / follower catch-up  →  identical state
```

The measured effect (§14): 100,000 writes over 1,000 keys leave a 14 MB log that a restart replays
entry by entry (100,001 entries); with a snapshot every 500 entries, the log is 6.4 KB, the
snapshot 110 KB, and a restart restores the snapshot and replays 1 entry.

---

## 1. What owns what

| Responsibility | Owner | Why there |
|---|---|---|
| the log's **boundary** `(index, term)` of the last compacted entry; `Compact`, `InstallSnapshot` on the in-memory log | `internal/replication` (`MemoryLog`) | the core reads terms and slices through it; `Term(boundary)` is answerable, `Term(i < boundary)` is `ErrCompacted` |
| deciding a follower needs a snapshot (`nextIndex ≤ boundary`), the retry of an unanswered offer, the follower's accept/ignore decision, resetting the log to a snapshot | `internal/raft` (pure) | Raft rules (§7 of the paper) that change `commitIndex`, the log and replication progress; the core never sees snapshot bytes, only `(index, term)` |
| the snapshot **file format**, publication, loading, validation, the chunk codec, reassembly | `internal/snapshot` | a format and a file protocol — no Raft decision |
| the **state** inside a snapshot (key-value map + session table) and its validation | `internal/kv` (`EncodeSnapshot`, `ValidateSnapshot`, `RestoreSnapshot`) | only the state machine knows its state; Phase 13's session table is part of it |
| the boundary record, `Install` and the compaction rewrite of the durable log | `internal/raftlog` | the durable log's format and recovery |
| the trigger, the orderings of creation / publication / compaction / installation, recovery's reconciliation, streaming | `internal/raftnode` (`Durable`, `Snapshots`, `Recover`) | impure, I/O-ordering work — single functions shared by the node's actor and the simulator (ADR-017, ADR-021) |

## 2. Snapshot state — inclusion and exclusion

**Included** — everything a later command's decision can depend on: the key-value map (a present
empty value stays present); the applied index the snapshot represents; the snapshot's term (of the
entry at that index, in the file's metadata); the session table — every session's id, `last` (the
LRU key), `ackedBelow` (the watermark), and every remembered result `(requestID, SHA-256
fingerprint, index)`; the session limits the table was built under (they change decisions, so a
state built under other limits is refused); the group identity (the sorted member ids).

**Excluded**, each with its reason: `currentTerm` / `votedFor` (the durable log's HardState owns
them); leader identity, `nextIndex` / `matchIndex`, election timers, votes received (volatile Raft
state, rebuilt by the protocol); in-flight requests, waiters, transport connections (volatile
driver state); the store's decision counters (observability, not replicated state — they restart
at zero after a restore).

## 3. Format (`internal/snapshot`) and metadata

A snapshot file is a sequence of records in the shared framing (`internal/record`, CRC-32C per
record), in the snapshot file's own kind namespace:

```
Header (kind 1): magic "QSNP" | version=1 | members | index | term | dataLen | SHA-256(data)
Data   (kind 2): up to 1 MiB of state bytes each, in order        (zero or more)
Footer (kind 3): index | term                                       (repeated, cross-checked)
EOF
```

Integers are canonical uvarints; members are length-prefixed, non-empty, strictly ascending
(`snapshot.Group`). A file is valid only if it is exactly: one header, data records whose
concatenation is `dataLen` bytes hashing to the header's SHA-256, one footer repeating the header's
index and term, then end of file — nothing after (a single stray byte is refused; FuzzDecode found
the framing's clean-EOF rule accepting fewer than 9 trailing bytes, §17). **Metadata**
(`snapshot.Meta`): the members, the index (> 0) and the term (> 0). Bounds: `MaxData` = 512 MiB of
state, `MaxDataRecord` = 1 MiB, 64 members of ≤ 256 bytes; a header declaring more is `ErrTooLarge`.

The **state bytes** are the state machine's canonical encoding (`kv.Store.EncodeSnapshot`, version
1): applied index, limits, `nKeys` keys strictly ascending with their values, `nSessions` sessions
strictly ascending by id, each session's results strictly ascending by request id. Strict ordering
makes the encoding canonical (one state, one byte string) and makes a duplicate undecodable.
`ValidateSnapshot` / `RestoreSnapshot` check every bound and every relation the session table's
construction guarantees: a non-empty key; ids in `[1, applied]`; `last` in `[id, applied]` and
distinct across sessions; watermark ≥ 1; at most `MaxSessions` sessions and `MaxUnacked` results;
results at or above the watermark, executed in `(id, last]`; no trailing bytes; the snapshot's index
equal to the state's applied index. A refused restore changes nothing.

**Determinism.** No timestamp, random id or host name appears anywhere: the same state and
metadata produce the same bytes (`TestSnapshotRoundTripIsCanonical`, `TestRoundTripAndDeterminism`);
the simulator checks that every snapshot at one index — on any node, in any incarnation — has
byte-identical state (INV-SN1).

## 4. Trigger

A node snapshots when `appliedIndex − publishedSnapshotIndex ≥ SnapshotEvery` (entries), checked
after each cycle's applications; `SnapshotEvery = 0` never snapshots. It reads only indexes — never a
clock — so it is deterministic in the simulator. Every node snapshots independently (Raft §7).
`dkvd -snapshot-every` defaults to 10,000; tests and the simulator use 4–100 (and an explicit
`snapshot` event). A state larger than `MaxData` is not snapshotted: `snapshot.ErrTooLarge` is
logged (`event=raft_snapshot_skipped`), nothing is written, the next attempt waits `SnapshotEvery`
more entries — and the log then keeps growing (§16).

## 5. Creation, publication and compaction (the order, and why)

`raftnode.Durable.Snapshot`, at applied index `S`:

1. encode the state (memory only) — crash point `before-snapshot-publish` follows;
2. write `<log>.snap.tmp`, fsync it;
3. rename it over `<log>.snap`, fsync the directory — **publication**: the snapshot is now the
   durable record of `[1, S]` — crash point `after-snapshot-publish`;
4. rewrite the durable log without the prefix (keeping `SnapshotRetain` entries below `S`, so a
   follower slightly behind catches up by entries): `<log>.tmp` = boundary record `(c, term(c))` +
   the durable HardState + the entries after `c`, fsync, rename over the log, fsync the directory
   (`raftlog.Log.Compact`) — crash point `after-log-compact`;
5. discard the prefix in memory (`core.Compact(c)`).

`c = S − SnapshotRetain` never exceeds the durable commit (`Compact` refuses otherwise) nor the
applied index (`MemoryLog.Compact` refuses otherwise). The log is never compacted before the
snapshot covering it is durable: after every step recovery finds either the old state or the new
one (§7). A failed write, fsync, rename or directory fsync fail-stops the node, as a failed Save
does (INV-F1): after a failed directory fsync the log's own name is no longer known to be durable.

## 6. Recovery

`raftnode.Recover`: remove the temporaries (`.snap.tmp`, `.snap.recv`, `log.tmp`); load and fully
validate the published snapshot `S` (if any) — its format, its group; open the log (boundary `B`),
fsyncing the directory; reconcile:

| Durable state | Decision |
|---|---|
| no snapshot, `B = 0` | recover: the full log, as before Phase 14 |
| snapshot `S`, `B = S` with the snapshot's term | recover: state = snapshot, the log's entries after `S` |
| snapshot `S`, `B < S`, the log holds `S` with the snapshot's term | recover: state = snapshot; the in-memory log keeps `B+1..` (the retained entries), applied = `S` |
| snapshot `S`, the log does not reach `S` | **repair**: an install that crashed after publishing — append the boundary record `(S, T)` recovery would have, re-read the log |
| snapshot `S`, the log holds `S` with another term, persisted commit `< S` | **repair**: the same (the install rule discards the log) |
| snapshot `S`, the log holds `S` with another term, persisted commit `≥ S` | **reject** — two committed entries at one index |
| `B > S`, or `B = S` with another term, or `B > 0` with no snapshot | **reject** — the log was compacted past its only snapshot |
| snapshot invalid (torn, checksum, version, group, index/term, malformed or impossible state) | **reject** |

Then the in-memory log starts at `B`; `commit = max(clamped persisted commit, S)`; the state
machine is restored from the snapshot and `applied = S`; the core validates `currentTerm ≥` the last
log term (which after a snapshot may be the snapshot's term — so an install makes the new term
durable before the snapshot, §8).

The spec's recovery cases, each defined and tested:

| Case | Decision | Evidence |
|---|---|---|
| A no snapshot + complete log | recover | every pre-Phase-14 test |
| B snapshot + suffix log | recover | `TestRecoverFromSnapshotAndSuffix`; `TestSimRestartFromSnapshotAfterPowerLoss`; real test 6 |
| C snapshot only (the log gone) | **reject**: the durable term and vote went with the log; a node that forgot its vote must not rejoin | `TestRecoverRefusesASnapshotWithoutItsLogOrWithAStateItCannotRestore` |
| D snapshot + empty suffix | recover | `TestInstallAppliesTheInstallRule` ("does not reach it"); every install |
| E snapshot + partially written suffix | recover (torn tail truncated) | the snapshot crash matrix's torn-power-loss rows; `TestInstallSurvivesEveryCrash` |
| F old snapshot + newer log | recover (the log holds `S`) | `after-snapshot-publish` rows; real test 4 |
| G new snapshot + old log remnants | recover or repair (rows 3–5) | `TestInstallCrashPointsRecover`; real test 5 (`repaired=true`) |
| H torn snapshot | **reject** (published only after its fsync, so a torn published file is damage); a torn temporary is removed | `TestPublishIsAtomicUnderEveryCrash`; the corpus's `truncated-*` |
| I corrupted snapshot | **reject** | `TestLoadRefusesACorruptPublishedSnapshot`; `TestSimCorruptPublishedSnapshotRefusesToStart`; `TestEveryTruncationAndEveryBitFlipIsCorruption` |
| J stale snapshot (behind the log's boundary) | **reject**; a stale *install* is ignored by the core | `TestReconcileTable`; `TestSimStaleSnapshotAfterANewerInstallIsIgnored` |
| K snapshot beyond the local log | **repair** | `TestReconcileTable`; `after-install-publish` crash points |
| L snapshot term/index mismatch | **repair** if uncommitted, **reject** if committed or at the boundary | `TestReconcileTable` |
| M invalid session state | **reject** | the state corpus (22 known-bad); case M of the test above; `FuzzRestoreSnapshot` |

## 7. Crash windows

Every step of creation and installation is a crash point (driver points, `raftnode.Point`) or an
I/O boundary (every write, fsync, truncate, rename and directory fsync of every file of the node):

| Window (the spec's list) | Where it is crashed |
|---|---|
| before creation; during serialization | `before-snapshot-publish` (serialization is memory only) |
| during the snapshot's write / fsync | I/O points on `.snap.tmp` |
| before / after publication | the `rename` / `syncdir` I/O points; `after-snapshot-publish` |
| before / during / after compaction | `after-snapshot-publish`; I/O points on `log.tmp` and its rename; `after-log-compact` |
| during installation | `before-install-publish`, I/O points on `.snap.recv`, `after-install-publish` |
| after installation, before suffix recovery | `after-install-boundary` (the state machine not yet restored, the retained entries and commit not yet saved) |
| after suffix recovery; before / after the InstallSnapshot response | `before-save` / `after-save` / `after-send` of the installing Ready |

Evidence: the **snapshot crash matrix** (`TestSnapshotCrashMatrix`) crashes at every one of the 992
points its scenario reaches × {process crash, power loss, power loss with a 13-byte torn tail} —
2,976 crashes, **0 failures**, every point covered; the `snapshot-crashpoints` and
`kv-snapshots-crashpoints` seeded profiles; `internal/raftnode` in-process crash tests at every
creation and install point (`TestSnapshotCrashPointsRecover`, `TestInstallCrashPointsRecover`);
publication (`TestPublishIsAtomicUnderEveryCrash`), compaction (`TestCompactIsAtomicUnderEveryCrash`)
and the boundary record (`TestInstallSurvivesEveryCrash`) at every I/O operation; real processes
killed at `after-snapshot-publish` and `after-install-publish` (real tests 4 and 5).

## 8. Follower installation (Raft §7)

- **Leader (core):** when `nextIndex[peer] ≤ boundary`, the core emits `MsgSnapshot(boundary)` and
  marks the peer pending; while pending it sends it only heartbeats at the boundary, ignores its
  rejections, and re-offers after `SnapshotRetryTicks` (default 4 × ElectionTicks) without an
  answer. A success at or beyond the offered index ends the offer.
- **Leader (driver):** the offer is realized as a transfer of the **published** snapshot `S ≥
  boundary` (compaction never passes publication), streamed on its own goroutine per peer — at most
  one at a time; an offer while one runs is ignored — so heartbeats keep flowing meanwhile (a chunk
  does not reset a follower's election timer; a heartbeat does).
- **Follower (driver):** chunks are reassembled strictly in order into `<log>.snap.recv`; a
  complete file is validated — its format, its group, **and its state** (`ValidateSnapshot`) —
  before the core ever sees it (a snapshot the state machine refuses would, once installed, be
  durable and unstartable). Only then is `MsgSnapshot` stepped into the core.
- **Follower (core):** term rules as for AppendEntries; a snapshot at or below the commit index is
  already covered — answered success at the commit, nothing changed; otherwise the log is reset to
  the snapshot (keeping the entries after it only if the log holds its index with its term),
  commit = applied = `S`, and the Ready carries the snapshot.
- **Follower (driver, `Durable.InstallSnapshot`), in order:** a changed term and vote, alone,
  carrying the previous commit (the durable term must never be below the snapshot's); publication
  of the staged file; the log's boundary record (`raftlog.Log.Install`); the state machine's
  restore; then the Ready's HardState and retained entries are saved and only then its response
  (`MsgSnapshotResponse`, transport kind 21) leaves. The order is pinned by
  `TestInstallOrderIsTermPublishBoundary` and two mutants.
- **Waiters:** an install completes every waiter at or below its index — a read barrier
  successfully (the state machine is at the index), a write with `ErrSuperseded` (§10).

## 9. Chunking

Frames are capped at 16 MiB (`docs/TRANSPORT.md` §2), so a snapshot travels on transport kind 20
(`InstallSnapshot`) in chunks of at most 1 MiB: `term | index | snapTerm | total | offset | data`,
strictly decoded (`UnmarshalChunk`: canonical integers, `offset + len ≤ total ≤ MaxFile`, non-empty
data). A chunk at offset 0 starts a transfer (abandoning any other); any other chunk must continue
the current transfer exactly — same sender, term, snapshot, total and offset — or it is ignored, so
duplicate, reordered, missing and stale chunks can never assemble a file; the completed file is
fsynced and validated before anything uses it. A partial snapshot is never active state
(`TestReceiverAcceptsOnlyAnInOrderCompleteSnapshot`; the simulator sends 64–256-byte chunks that its
network drops, duplicates, delays, reorders and corrupts). The whole snapshot is held in memory
while it is written, sent and received — which is why `MaxData` is 512 MiB and not "unbounded".

## 10. Dedup / session preservation

The session table is part of the snapshot (§2) and is restored with the map, so a request's
identity survives snapshot, compaction and restart exactly as it survived replay in Phase 13:

- `TestSnapshotPlusSuffixEqualsFullReplay` (kv): 120 random session runs cut at **every** index —
  snapshot, restore, apply the rest — make exactly the decisions of an uncut replay (executed,
  duplicate with the original index, conflict, stale, expired, limit, registered — each occurs
  after a restore);
- `TestRestoredStateMatchesTheModel`: a restored table equals the independent session model's;
- `TestSimDedupSurvivesSnapshotCompactionAndRestart`: R executes and its client never hears; every
  node snapshots and compacts R's entry away; every node loses power and restarts from its
  snapshot; the retry of R is answered *duplicate of index R*, and R executed once;
- real test 7 (`TestRealRetryAfterSnapshotIsADuplicate`): the same on real processes, R's entry
  compacted away on every node, every process killed and restarted; real test 8: a different
  command under R's identity is `CONFLICT`;
- eviction, watermarks and LRU under snapshots: the `kv-snapshots-evict` profile (tiny limits,
  a snapshot every 5 entries) and the state corpus's evicted-session and watermark cases;
- the linearizability checker over logical operations runs on every `kv-snapshots-*` history.

A write whose index an install replaced before its node applied it is **unknown**, not lost:
`ErrSuperseded` (status `UNKNOWN_OUTCOME`), and a session client retries it under the same
identity — answered by the table the snapshot carried (`docs/CLIENT_SEMANTICS.md`).

## 11. Corruption policy

Loud and total, as for the log (`docs/CRASH_RECOVERY.md` §9): an invalid published snapshot, or a
snapshot that contradicts the log, refuses to start (`ErrSnapshot`), never a silent fallback. A
received snapshot that does not validate is discarded (`event=raft_snapshot_refused`) and the
leader offers again. Evidence: the file-format corpus (`internal/snapshot/testdata/corpus`: 5
known-good — empty state, multi-record, large index/terms, single member, a state — and 28
known-bad — truncations, a corrupted CRC, a checksum mismatch, a future version, wrong magic, zero
index/term, unsorted or duplicate members, missing or repeated records, bytes after the footer,
oversized lengths, non-canonical integers); the state corpus (`internal/kv/testdata/snapshots`: 9
known-good — empty store, keys, deleted keys, empty values, sessions, evicted sessions, a session
at its limit, an advanced watermark, a >1 MiB state — and 22 known-bad, each refused by the rule it
breaks); every truncation and every bit flip of a file refused; `FuzzDecode`,
`FuzzUnmarshalChunk`, `FuzzRestoreSnapshot`, `FuzzDecodeBoundary`; a corrupted chunk in the
simulator (`TestSimCorruptedChunkIsRefusedAndRetried`); a corrupted published snapshot on a down
node refusing its restart (`TestSimCorruptPublishedSnapshotRefusesToStart`).

## 12. Retention

**Exactly one** published snapshot per node, at `<log>.snap`. A new one replaces it only by an
atomic rename after its own fsync, and the log is compacted only after that, so the previous
snapshot is never needed again once the rename is durable (INV-SN3). Nothing else is ever deleted
except the temporaries, at startup (`<log>.snap.tmp`, `<log>.snap.recv`, `<log>.tmp`): each is
either a publication that never happened or a transfer never installed. A node's published
snapshot only moves forward (the simulator checks it).

## 13. Simulator vs real-process evidence

| Property | Deterministic simulator (`internal/raftsim`) | Real driver, in-process (`internal/raftnode`) | Real processes (`tests/integration`) |
|---|---|---|---|
| crash at every snapshot window | 992-point matrix × 3 modes; seeded crash-point profiles | every creation and install point | SIGKILL at `after-snapshot-publish`, `after-install-publish` |
| power loss | modeled (`fault.MemFS`), incl. torn tails | modeled | **not tested** (§16) |
| lagging follower / install | scripted + every profile | over TCP | real tests 2, 3, 9, 10 |
| duplicated / reordered / delayed / corrupted / stale transfers | scripted + profiles | — | — |
| leader change around snapshots | profiles | — | real test 9 |
| partition | profiles | — | real test 10 |
| dedup across snapshots | scripted + every `kv-snapshots-*` history | — | real tests 7, 8 |
| disk failure / stall during creation | `snapshot-disk` profile (failed writes, fsyncs, renames, directory fsyncs) | failed and stalled fsync | — |

The simulator drives the real core, log, snapshot files, state machine and driver functions; its
network, disk and clock are models. The real-process tests exercise real files, TCP and SIGKILL
with real timing.

## 14. Performance (measured)

Apple M4, macOS 26.5, Go 1.27.1, APFS; 100-byte values; `go test ./internal/raftnode -bench
'Snapshot|LogCompact' -benchtime 5x` and `-run TestSnapshotsBoundRecoveryWork
-raftnode.measure=100000`. Order-of-magnitude figures on one machine, not a benchmark suite.

| Operation | 1k keys (112 KB file) | 10k keys (1.1 MB) | 100k keys (11.2 MB) |
|---|---|---|---|
| encode (state + file + SHA-256) | 0.37 ms | 2.8 ms | 25 ms (446 MB/s) |
| publish (write, fsync, rename, dir fsync) | 22 ms | 25 ms | 28 ms (≈ fsync-bound) |
| load + validate + restore | 0.37 ms | 2.4 ms | 17 ms |
| install (reassemble, validate, publish, boundary, restore) | 11 ms | 18 ms | 73 ms |

Log compaction (rewrite of N 100-byte entries, fsync): 7.8 ms (1k), 8.4 ms (10k), 18 ms (100k).

**Replay reduction** (the point): 100,000 writes over 1,000 keys. Without snapshots the log is
14,067,024 bytes and a restart replays 100,001 entries (37.7 ms). With a snapshot every 500 entries
(retain 50) the log is 6,420 bytes, the snapshot 110,090 bytes, and a restart restores index
100,000 and replays **1** entry (3.6 ms). The bounds — at most `every + retain` entries held, at
most `every` replayed, independent of how long the node has run — are asserted on every test run
(`TestSnapshotsBoundRecoveryWork`); the times are only reported.

## 15. Exact guarantees (proven, within the scope of §16)

- **Bounded log.** With `SnapshotEvery = k > 0` and a state below `MaxData`, a node's durable log
  holds fewer than `k + SnapshotRetain` *applied* entries (entries not yet committed are not the
  snapshot policy's to bound), and a restart replays fewer than `k` (asserted, for a node whose
  entries are all applied, by `TestSnapshotsBoundRecoveryWork`).
- **INV-SN1–SN6** (`docs/INVARIANTS.md`): a snapshot is exactly the replicated state at its index
  (checked against an independent model, byte-identical across nodes); nothing partial or invalid
  becomes active; compaction never discards the only record of committed state; snapshot + suffix
  equals full replay; deduplication survives; an installed follower resumes replication.
- Every Phase 9–13 invariant (INV-R, F, CR, X) continues to hold with compaction — re-checked by
  every simulator run, boundary-aware.
- Recovery after a **process crash** at any window recovers a coherent state (real processes and
  simulator); after a **modeled** power loss likewise (simulator).

## 16. Limitations

- **Size.** A snapshot is held in memory while it is created, sent and received; `MaxData` is 512
  MiB of state. Beyond it the node does not snapshot (logged) and its log grows. No incremental,
  streaming-from-disk or copy-on-write snapshots.
- **Creation blocks the actor.** The state is encoded and published on the node's actor goroutine
  (≈ 50 ms at 100k keys on the machine above); Raft processing pauses meanwhile.
- **A transfer can be retried while running.** If a transfer outlasts `SnapshotRetryTicks`, the
  leader re-offers; the second transfer waits for the first (one per peer) and then repeats it.
- **Power loss is modeled, not tested** — as everywhere in this project (`docs/FAILURE_MODEL.md`).
- **One group, fixed membership.** The group identity is the member list; no membership change, no
  multiple groups (Phase 15+ scope was not started).
- **Configuration consistency is not checked across nodes.** A follower refuses a snapshot built
  under other session limits (and cannot then catch up); nothing prevents the misconfiguration.
- **No snapshot of the LSM engine.** The replicated state machine is the in-memory `kv.Store`; the
  storage engine is not hosted by the Raft node yet.
- **A wiped node is not a safe replacement.** A node started on an empty data directory catches up
  by snapshot, but it has forgotten its term and vote; rejoining under the same id is not proven
  safe without membership change (`docs/FAILURE_MODEL.md`). Recovery refuses a snapshot whose log
  is gone (case C) for this reason; an empty directory has neither, and starts fresh.

## 17. Bugs found by this phase's tests

1. `snapshot.Decode` accepted up to 8 stray bytes after the footer (the framing reports fewer than a
   record header as a clean end of file) — found by `FuzzDecode`; fixed by comparing offsets; corpus
   cases and a mutant keep it fixed. (The first commit of the format was made with that fuzz
   failure masked by a pipe; fixed forward, never rewritten.)
2. `raftlog.Open` fsynced the directory only when it created the log. Once compaction renames a new
   file over the log, a process crash after the rename and before its directory fsync, followed by
   a restart, left appends that a later power loss would lose with the log's name — found by
   `TestCompactIsAtomicUnderEveryCrash`; fixed (Open always fsyncs the directory); a mutant keeps it
   fixed.
3. **A follower stranded behind a stale rejection** (a liveness bug in the core, latent since
   Phase 9, made reachable by compaction). A rejection delayed past the success that followed it
   backed `nextIndex` up below the peer's `matchIndex`; that used to cost only a resend, but with
   the prefix compacted the leader then offered a snapshot the peer's commit already covered, the
   covered answer raised nothing, and the peer never caught up. Found by the 200-seed gate
   (`kv-snapshots-partitions` seed 100, INV-F3); fixed — `nextIndex` never falls below
   `matchIndex + 1` — with `TestStaleRejectionNeverBacksUpBelowTheMatch`, a replay of the seed, and
   mutant 122.
4. **A simulator bug:** the `Duplicate` event copied a snapshot chunk's flight without its
   payload, so it was delivered as a raw `MsgSnapshot` and its node fail-stopped (seed 153). The
   real node cannot reach that path; the simulator now copies the payload, treats a chunk-less
   `MsgSnapshot` in flight as a harness violation, and mutant 123 keeps it so.
5. The mutation runner counted a mutant that did not compile as killed, and four older mutants no
   longer matched after this phase's edits; a dry-run mode (`DRY=1`) now checks every pattern, and
   a build failure is reported as a failure of the runner.
6. **CI found a timing premise and a latent checker race** (neither a consistency failure). A Phase
   13 test's leader partition did not always leave a session write unanswered — a write replicated
   before the cut committed in the new term and was answered after a short partition — so its
   non-vacuity check failed on CI; the schedule now holds the partition until the recorded history
   shows an unanswered write (a void premise restarts the scenario). Reading the history mid-run
   for that exposed a data race in `lincheck.Recorder.History`, which shared the attempts of ops in
   flight; it now copies them (`TestHistoryIsASnapshotWhileClientsRecord`, mutant 124).

## 18. Mutation testing

`scripts/mutation.sh` mutants 93–123 break each snapshot rule in turn and require a real test to
fail; mutant 124 pins the recorder fix of §17 (`make mutation`: 124/124 killed). Orderings — compaction before the snapshot is durable,
publication before the new term is durable, the boundary record before publication, the response
before the install is durable. Bounds — compaction past the applied index, past the durable
commit. Validation — the SHA-256 skipped, bytes after the footer accepted, an incomplete or
out-of-order transfer accepted, another group's snapshot or a state the state machine refuses
accepted, a corrupt published snapshot silently ignored. Recovery — the wrong boundary index or
term, the snapshot not restored, a compacted log without a snapshot started anyway, the published
snapshot deleted at startup, an interrupted install not completed. Protocol — no snapshot offered
for compacted entries, a covered snapshot refused, a stale install resetting the log, a matching
suffix discarded (in memory, on disk). Dedup — the session table left out of the snapshot, a
replaced write reported as applied. And `Open`'s directory fsync, the stale-rejection clamp, and
the simulator's duplication of a chunk with its payload. Two are killed by real processes
alone (the session table; the interrupted install). `DRY=1 scripts/mutation.sh` checks that every
mutant's pattern still applies; a mutant that does not compile is reported as a failure of the
runner, never as a kill.
