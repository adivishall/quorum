# ENGINEERING ROADMAP — the audit after the first wave, and the work that follows

Status: **audit of `b717347` (main, 2026-10-01) and the cluster-lab branch (#3).** The previous
version of this document audited `b401edf`, after Phase 15, and planned a first wave: observability
(#1), a load generator (#2) and cluster experiments (#3). That wave is done (§2). This audit checked
every claim against the code, not the documents. Where a defect is called *reproduced*, a test or
probe showed it; where it is called *from the code*, it was read and traced but not yet run.
`docs/ROADMAP.md` is the phase table.

---

## 1. Audit

### 1.1 What `dkvd` runs (the production path)

- **Transport** (`internal/transport`): framed TCP, one connection per peer pair (the smaller id
  dials), a version handshake, a group envelope on every frame.
- **Node host** (`internal/multiraft`): a driver (`internal/raftnode`) per Raft group, each with a
  pure core (`internal/raft`), a durable log (`internal/raftlog`), a snapshot file
  (`internal/snapshot`) and an **in-memory** state machine (`kv.Store`).
- **Client front** (`internal/kv`): the binary client protocol v3, routing key → shard → group,
  sessions and deduplication at apply, one-hop forwarding.
- **Admin and metrics:** a JSON-lines admin port (membership, snapshot, status) and `/metrics`
  (`internal/metrics`).
- **Modes:** `-raft` (group 0) or `-cluster` (one group per shard of the routing ring); the default
  is the Phase 7 probe demo.

### 1.2 What is test-only, simulation-only, or a tool

- Test- and simulation-only:
  - `internal/raftsim`: the deterministic simulator, running the real core, log and driver order;
  - `internal/fault`: a crash-consistent disk model and fault injection;
  - `internal/lincheck`: the linearizability checker;
  - `internal/kv/workload`, `internal/testport`.
- Measurement tools: `internal/load` with `cmd/dkvload`, `internal/lab` with `cmd/dkvlab`,
  `internal/bench` with `cmd/dkvbench`.
- Test seams that ship in the production binary:
  - `dkvd -crash-at` and `-crash-armed-by-signal`, which link `internal/fault` into `dkvd`;
  - the exported crash `Hook` and `DisableSync` in `raftnode.Config`.

### 1.3 What is disconnected or partial

- **The LSM engine is standalone.** `dkvd`, `raftnode` and `multiraft` never import
  `internal/storage`; `kv` imports it only for two size constants. Every number in
  `docs/BENCHMARKS.md` measures a component the replicated system does not use. §1.6 re-checks what
  hosting it needs.
- **The `dkv` CLI is an in-memory toy**: a fresh `MemStore` per invocation, not a network client.
- **Empty directories:** `internal/api`, `internal/cluster`, `internal/config`, `pkg/client`,
  `docker`, `dashboard` and `tests/chaos` hold no tracked files. The first five exist in
  ARCHITECTURE's layer map.
- **No operator tool** beyond raw JSON lines to the admin port; no health or readiness endpoint.

### 1.4 Verified defects

Ranked by what a single input can do.

| # | Defect | Evidence | How verified | Severity |
|---|---|---|---|---|
| D1 | **Fixed** (one entry-size limit enforced at every boundary; `docs/RAFT.md` §16, `TestRealEntryLimit`, mutants 173–179). **A PUT near the 1 MiB value limit breaks the group.** The client protocol accepts a 4 KiB key and a 1 MiB value; the Raft decoders accept an entry of at most 1 MiB (`raft.MaxEntryDataLen`, `raftlog.MaxEntryDataLen`); nothing checks at `raft.Propose` or `raftlog.Save`. The leader persists the entry; every follower drops the AppendEntries carrying it; an election replaces the leader; the write is `LOST`; and the old leader can never restart (`raftlog: corrupt log: length 1048582 out of range`). | `kv/api.go` validation; `raft/message.go:20`; `raftlog/raftlog.go:654` | **Reproduced** (in-process three-node group, a 1 MiB value) | Critical: one valid request permanently disables a node; three in turn, a group |
| D2 | **AppendEntries has no byte or count budget, and every broadcast resends the unacknowledged tail.** `sendAppend` sends `[nextIndex, last]`; `nextIndex` moves only on a response. The receiver refuses a frame over 16 MiB (`transport.MaxFrameSize`), which the sender does not check, so a backlog above it can never be sent; past 65,536 entries the decoder refuses the message (`MaxEntriesPerMessage`). | `raft/raft.go` `sendAppend`; `transport/wire.go:147` | Resend **measured** (bytes per entry ×10.9 from 1 to 16 clients, `docs/CLUSTER_BENCHMARKS.md` §6.3); the 16 MiB stall **from the code** | High: liveness of a lagging follower; bandwidth grows with the square of the writes in flight |
| D3 | **Fixed** (per-group lifecycle reservations; `docs/MULTI_RAFT.md` §3, mutants 189–191). **Concurrent `create-group`/`start-group` admin calls can start two drivers on one log.** `Host.start` checks `groups[g]` under the lock, releases it, starts the node, and re-takes it to store the result. | `multiraft/host.go` `start` | From the code | High (durability), operator-triggered |
| D4 | **Fixed** (`-data-dir` required and locked; node and cluster identity recorded; a directory with no node initialized only with `-init`; `docs/MULTI_RAFT.md` §5). **`dkvd -data-dir` defaults to a fresh temporary directory on every start**, so a restarted node forgets its term, vote and log, which Raft forbids. Nothing locks a data directory, so two processes can open one log. | `cmd/dkvd/main.go:80, 394`; no flock anywhere | From the code | High (safety), configuration-triggered |
| D5 | **Fixed** (each data directory pins its replica settings — the session limits, and in `-cluster` mode the routing — and refuses a start with others; the transport handshake carries their digest and nodes whose settings differ never connect; `-shards/-rf/-nodes` are refused outside `-cluster` mode; `docs/MULTI_RAFT.md` §5, `docs/TRANSPORT.md` §3, `TestRealImpostorsNeverJoinTheGroup`). **Replicated configuration is not validated across nodes.** Session limits must be identical on every replica (they decide `SESSION_LIMIT`/`EXPIRED` at apply), but nothing checks. Mismatched nodes decide differently before a snapshot, and refuse each other's snapshots after one. `-shards/-rf/-nodes` are unchecked, and silently ignored under `-raft`; `-id` need not be in `-nodes`. | `cmd/dkvd/main.go:85-86, 181-192`; `kv/snapshot.go:142` | From the code | High (replica divergence), configuration-triggered |
| D6 | **Fixed** (the node fail-stops on an apply error; `docs/RAFT.md` §17). **A deterministic Apply error is logged and retried every cycle, forever,** instead of failing stop. | `raftnode/node.go` processReady; `crashpoint.go` ApplyCommitted | From the code | Medium |
| D7 | **Fixed** (every accept loop logs and retries an error; the transport bounds pending handshakes, the client and admin ports their connections and deadlines; `docs/TRANSPORT.md` §8, `docs/API.md` §1, `docs/MULTI_RAFT.md` §7). **Accept loops (transport, client, admin) exit permanently on any error, EMFILE included, without logging. Client and admin connections have no deadlines and no cap.** | `transport.go:272`, `kv/wire.go:312, 326`, `multiraft/admin.go:131` | From the code | Medium |
| D8 | **Fixed** (a non-positive tick is a startup error in `dkvd`, `raftnode` and `multiraft`). **`-tick-interval` is unvalidated.** 0 disables the idle-connection timeout while the driver silently uses 50 ms; a negative value panics the actor, and with it every group (there is no `recover`). | `cmd/dkvd/main.go:81, 137`; `raftnode/node.go` | From the code | Medium |
| D9 | **Fixed** (handshake version 2: answered, with the cluster id and settings digest; `docs/TRANSPORT.md` §3, mutants 197–213). **The transport's handshake version stayed 1 when Phase 15 added the group envelope,** so a pre-envelope peer passes the handshake and its frames are dropped as malformed. | `transport/handshake.go:16` | From the code (`git log -S`) | Low (no mixed deployments exist) |
| D10 | **Storage engine (standalone):** the WAL and MANIFEST writers do not latch a failed write, so the next append follows a partial record and recovery then refuses the store; a flush does not sync the WAL first, so a power loss in `batch` mode can leave tables ahead of the durable WAL, which open refuses. | `wal/wal.go:277`, `manifest.go:694`, `lsmstore.go:833` | From the code | Medium (engine only; blocks hosting it) |

### 1.5 What is verified, argued, measured — and what is not

- **Strongly verified:**
  - Raft safety (INV-R1..R10) under every fault family, in seeded and replayable simulation;
  - three bounded crash matrices over the node (1,440 + 2,976 + 6,660 cells, 0 failures);
  - linearizability and at-most-once execution, on recorded histories from real processes, the
    driver and the simulator;
  - snapshots (INV-SN), membership and group isolation (INV-MB);
  - metrics against ground truth;
  - 172 mutants, all killed (168–172, added with this audit, cover the lab's statistics and usage).
- **Only argued, or modeled:**
  - power-loss durability (a software disk model; real power loss is untested);
  - histories are finite and small: at most five nodes and eight clients;
  - real-process partitions are connection resets, never silent loss.
- **Measured:** the storage engine on one machine (`docs/BENCHMARKS.md`), and since #3 the
  cluster (`docs/CLUSTER_BENCHMARKS.md`):
  - throughput and latency for 1, 3 and 5 nodes, 1 to 16 groups and three mixes;
  - the election, outage and catch-up after a leader kill;
  - rolling restart, membership change and snapshot costs under load;
  - persistence and replication traffic per write.
- **Not measured:**
  - partitions under load;
  - values larger than 100 bytes (D1 blocks values near the limit), and key skew;
  - more than 16 groups or 16 clients under load;
  - multi-host deployments;
  - restart time as the state grows.
- **Not operationally visible:**
  - no storage-engine metrics (no engine on the path);
  - no health or readiness endpoint;
  - the transport logs a disconnect but not its reason (a frame too large, a bad checksum);
  - no aggregation; `/metrics` is per node.

### 1.6 The storage engine as a state machine — the prerequisites, re-checked

The previous audit named four prerequisites. Each was checked against the current code, and the
code adds four more that the earlier list missed.

| Prerequisite | State | Evidence |
|---|---|---|
| Atomic write batch carrying the applied index | **Partial.** `wal.Batch` is crash-atomic for many operations, but both stores append single-operation batches, and the applied index is a separate record kind. | `lsmstore.go:718`, `wal/batch.go:24-27` |
| WAL truncation | **Missing.** Segments rotate at 16 MiB and are never deleted; every open replays them all. | `wal/segment.go`, `recover.go` |
| — *new:* durable sequence numbers | **Missing.** Sequence numbers are re-derived from WAL position on replay; the MANIFEST's `LastSequence` is written but never read, and `Applied`/`LogNumber` are always zero. So no WAL prefix can be deleted, and nothing can be ingested, without changing recovery. | `lsmstore.go:608-648, 889` |
| Ordered iterator | **Missing as an API.** The parts exist: a memtable iterator with Seek, an SSTable iterator, and a k-way merger that drops tombstones. | `compaction.go`, `memtable.go:325` |
| Consistent checkpoint | **Partial.** Versions are reference-counted and files immutable, but a version and its sequence cannot be captured together, and `Snapshot()` iterates the live memtable. | `lsmversion.go`, `lsmstore.go:1248` |
| Bulk ingest | **Missing.** There is no API, no "replace everything", and recovery would refuse an ingested file's sequences. | `lsmstore.go:641-648` |
| — *new:* failure latching | **Missing** for the WAL and MANIFEST writers (D10). | — |
| — *new:* a sync before flush | **Missing** (D10). | — |
| — *new:* crash and power-loss testing | **Partial.** Real SIGKILL tests exist for flush and compaction; the engine does not use `internal/vfs`, so it has no injected crash-point matrix and no power-loss model, unlike the Raft node. | `tests/integration/lsm_crash_test.go` |

**Why the applied index must be atomic with the data and the sessions:** replaying an entry the
engine already holds is not idempotent for this state machine. A replayed REGISTER overwrites its
session (`store.go:178`), and a replayed identified write becomes a duplicate of itself, so its
client is answered differently.

**The snapshot protocol does not fit an engine checkpoint.** Encoding and restoring work on whole
byte slices in the actor; the file format puts the state's length and SHA-256 in its header, so it
cannot be written as a stream; and both ends of a transfer hold the whole state in memory, up to
512 MiB.

### 1.7 Documentation that contradicts the code

The docs-vs-code audit found 60 stale or contradicting statements. The five most misleading:

1. **DESIGN §5 says `wal.sync=off` loses data on a process kill.** The opposite is tested (INV-W9).
2. **CONSISTENCY C3 ties write durability to the engine's `wal.sync` mode.** Replicated writes go
   through the Raft log, which fsyncs every Save.
3. **ADR-004 and CONSISTENCY C5 describe a `stale` read mode.** None exists (INV-X4 is PLANNED).
4. **README, LIMITATIONS, LSM, COMPACTION and MANIFEST say `SetLogNumber` is recorded.** Nothing
   sets it.
5. **The layer map is wrong.** ARCHITECTURE §2 shows HTTP/JSON, `internal/api`, `internal/cluster`
   and the LSM engine on the request path, and DESIGN §10's startup sequence opens an engine that
   `dkvd` never opens.

The rest are stale phase references, mutant counts quoted as current, components described as
future that exist, and package comments that predate their package's growth
(`internal/kv/doc.go`, `internal/raft/doc.go`, `internal/transport/doc.go`, `cmd/dkv`). Task 9
removes them. This change already corrects README's status line and request path, ROADMAP's
Phase 19 row, and LIMITATIONS' status, metrics and measurement statements.

### 1.8 Verification infrastructure

- **CI:** five jobs on linux/amd64:
  - check: gofmt, the gitignore guard, vet, the unit packages under `-race`;
  - integration: real processes under `-race`;
  - faults: 200 seeds per profile and the crash matrices;
  - mutation: all mutants;
  - fuzz: every target for 5 s.
- **CI was red on `main` after PRs #5 and #6.** The cause was four real-timing premises, plus the
  real-process suite running twice; issue #7 and PR #8 fixed both. The remaining gaps:
  - `dkvd` under the integration tests is built without `-race`, so the server under fault is
    never race-checked;
  - macOS (`F_FULLFSYNC`) is never exercised in CI;
  - no job has a timeout;
  - the mutation runner has no baseline run of the killer tests, any non-build failure counts as a
    kill, three mutants match more than one site, and the storage engine and routing have no
    mutants;
  - harness logic is duplicated: four ways to launch `dkvd`, four leader waits, three percentile
    definitions.

### 1.9 Out of scope, deliberately

Shard rebalancing and moving data between groups, cross-group transactions, TLS, authentication,
authorization, encryption at rest, and Byzantine faults. Membership changes move replicas of a
group, never shards between groups. The admin and metrics ports are unauthenticated.

---

## 2. What the first wave delivered

- **#1 Observability (Phase 16):** 61 metric families from every layer, verified against ground
  truth on in-process and real clusters. The instrumented request path is within noise of the bare
  one.
- **#2 Load generator:**
  - closed and open loop, with coordinated omission accounted for;
  - exact percentiles, outcome classes and an outage timeline;
  - throughput counted as completions inside the window;
  - the generator's own ceiling measured.
- **#3 Cluster experiments and the first performance report:** `internal/lab`, `dkvlab` and
  `docs/CLUSTER_BENCHMARKS.md`, with raw results. The diagnosis:
  - persistence is the limit: one fsync per write, never batched, and two at the leader;
  - all nodes contend for one SSD;
  - reads queue behind the fsyncs;
  - the leader resends unacknowledged entries (D2).

  Building it found a scenario that measured nothing (a snapshot interval too large) and a restart
  catch-up gated by the 500 ms redial. It also left rolling-restart unknown outcomes unexplained.

---

## 3. The work, in dependency order

The order differs from the one proposed before this audit in one way: D1–D8 come before the
storage engine. D1 lets one valid request disable a node, and D3–D5 let a configuration mistake
break safety or make replicas diverge. The engine integration adds a durable apply path, a second
log and more configuration on top of exactly these paths, so they must hold first.

### 1. Input and replication bounds (D1, D2) — next

- **Status:** D1 done — the entry budget is enforced at the front, at `raft.Propose`, in the
  in-memory log, at `Step` and at `raftlog.Save` (`docs/RAFT.md` §16). D2 is open.

- **Problem:** a request within the documented limits can disable a node; a replication message
  has no size bound.
- **Architecture impact:**
  - one entry budget, `kv.MaxCommand ≤ raft.MaxEntryDataLen`, enforced at the front (`INVALID`),
    at `raft.Propose` (refused), and at `raftlog.Save` (it refuses to persist what replay would
    refuse);
  - AppendEntries capped in bytes and entries below the transport's frame limit;
  - the transport refuses to send a frame its peer would refuse.
- **Difficulty:** low–medium. **Risk:** medium; the commit rule and nextIndex handling are
  touched.
- **Testing:**
  - a real-process regression for the 1 MiB PUT (refused, not lost; every node restarts);
  - a lagging follower with a backlog above the frame limit catches up;
  - simulator profiles with large entries;
  - a mutant for each bound.
- **Benchmark:** the §6.3 concurrency series before and after.
- **Docs:** API, DESIGN §1, RAFT, LIMITATIONS.

### 2. `dkvd` configuration and lifecycle safety (D3–D8)

- **Problem:** the operator can make a node forget its votes, run two drivers on one log, or run
  replicas that decide differently.
- **Architecture impact:**
  - `-data-dir` required in raft and cluster modes, and locked;
  - the group-start race closed;
  - the configuration a group's state machine depends on (session limits) recorded in its
    identity file at bootstrap and checked at every start and against every snapshot;
  - routing configuration validated against `-id`;
  - tick validated;
  - a deterministic apply failure fails stop;
  - accept loops that log and back off; client and admin deadlines and a connection cap.
- **Difficulty:** medium. **Risk:** low–medium.
- **Testing:** real-process tests per rule (each refused start names its reason); a race test for
  the start path; mutants.
- **Docs:** MULTI_RAFT, DEDUP, LIMITATIONS, a configuration reference.

### 3. Storage-engine prerequisites (§1.6)

- **Problem:** the engine cannot host the replicated state machine, and cannot be tested under the
  faults the node is.
- **Architecture impact**, inside `internal/storage`:
  - the engine on `internal/vfs`, which gains ReadDir, MkdirAll and Link;
  - failure latching in the WAL and MANIFEST writers;
  - sync before flush;
  - durable `LastSequence` and `Applied`, recovered from the MANIFEST;
  - WAL truncation behind a flush;
  - an atomic multi-operation batch carrying a reserved applied-index record;
  - a consistent, sequence-bounded iterator;
  - a checkpoint;
  - ingest that replaces the whole state.
- **Difficulty:** high. **Risk:** high, but contained: the engine is still standalone.
- **Testing:**
  - an exhaustive crash-point matrix over the engine on `fault.MemFS`, covering process crash,
    power loss and torn tail, the way the node has one;
  - the conformance suite;
  - fuzzing of the new records;
  - storage mutants (there are none today).
- **Benchmark:** `dkvbench` before and after; restart time as data grows, now bounded by
  truncation.
- **Docs:** LSM, WAL, MANIFEST, DECISIONS.

### 4. The LSM-backed replicated state machine, and its hosted crash matrix

- **Problem:** the replicated state lives in memory and is at-least-once across restarts.
- **Design constraints:**
  - Raft stays authoritative for order;
  - apply is deterministic, and writes one engine batch per Ready (key-value data, session table,
    applied index), never per entry;
  - recovery replays the log from the engine's durable applied index: exactly-once across
    restarts, and INV-CR4 is replaced;
  - log compaction is capped at the engine's durable applied index;
  - the engine's WAL and the Raft log are two logs (ADR-006): keeping, disabling or unifying them
    is decided by measurement against `docs/CLUSTER_BENCHMARKS.md`.
- **Testing:**
  - crash at every window: before apply, after the engine write, after the engine sync, during
    flush and compaction, before and after the reply;
  - the reference model;
  - the existing linearizability, snapshot and membership tiers unchanged.
- **Difficulty:** very high. **Risk:** very high.
- **Docs:** DESIGN §10, CRASH_RECOVERY, DEDUP, CONSISTENCY C3.

### 5. Snapshots as engine checkpoints

- **Problem:** the snapshot protocol is bytes in memory (§1.6).
- **Architecture impact:**
  - a streamed, chunk-checksummed snapshot format, with the checksum in a trailer;
  - creation off the actor from a checkpoint;
  - install by ingest, crash-safe;
  - the session table and applied index carried;
  - Raft's snapshot metadata stays authoritative.
- **Testing:** the snapshot crash matrix rerun with the engine; write, snapshot, compact, restart,
  retry, read against the reference model.

### 6. Group commit and replication pipelining (measured: §2, D2)

- **Problem:** one fsync per write, never batched; two at the leader; every broadcast resends the
  unacknowledged tail.
- **Levers:**
  - drain the actor's queued proposals into one Ready;
  - fold the commit-index persist into the next append, with INV-CR3 kept, or replaced once the
    engine holds the applied index;
  - optimistic `nextIndex` with an in-flight window.
- **Benchmark:** `docs/CLUSTER_BENCHMARKS.md` §3 and §6.3 before and after, with variance.
- **Risk:** medium; the crash matrices and fault schedules must stay green.

### 7. Chaos under load, and the rest of Phase 19

- Simulator campaigns that combine clients, snapshots, membership, partitions and persistence
  faults.
- A real-process runner that replays a recorded fault schedule against `dkvd` under `dkvload`,
  recording client history, metrics, faults and node events, and checking linearizability.
- Partitions in `dkvlab`.
- The open measurements: value sizes, key skew, more groups and clients, more runs.
- The rolling-restart unknown outcomes explained, with an attempt-level trace.

### 8. PreVote, then CheckQuorum

- **Evidence to capture first:**
  - a late-starting node can force an election (`TestSettledStartWaitsForEveryLink` logs it when it
    happens);
  - a partitioned node rejoining raises the term;
  - an isolated leader keeps accepting writes it cannot commit.
- **The rule:** each is measured with the lab before and after, in the pure core, with simulator
  profiles and mutants.
- **CheckQuorum** comes after PreVote. Its interaction with ReadIndex, pending writes, forwarding
  and membership is specified before code.

### 9. Documentation: remove the contradictions (§1.7)

- **Scope:** the 60 statements; ARCHITECTURE's layer map rewritten as built; DESIGN §10 split into
  "as built" and "planned"; invariants for Phase 16 and #2.
- **Timing:** best done after task 1, so the size rules are written once.

### 10. Operator and client surface

- **Problem:** there is no operator tool beyond raw JSON lines, and the `dkv` CLI is not a network
  client.
- **Architecture impact:**
  - `dkvctl` over the admin protocol: status, leader, terms, indexes, lag, membership, snapshot,
    health;
  - health and readiness beside `/metrics`;
  - `dkv` as a networked session client over the existing binary protocol.
- **HTTP:** an HTTP gateway only if a consumer needs one; the binary protocol already carries
  identity, retries and routing.

### 11. Docker, then the dashboard

- **Docker:** a compose file for 3 and 5 nodes from the real `dkvd`, with health checks and a CI
  smoke job running `dkvload`. Containers on one host share one disk, so this demonstrates
  topology, not hardware independence.
- **Dashboard:** reads only `/metrics` and the admin port.

### 12. Verification infrastructure

- `dkvd` built with `-race` under integration;
- a macOS CI job;
- timeouts on every job;
- a baseline run in the mutation runner, and anchored test patterns;
- one launcher and one leader wait shared by the integration tests and the lab.

---

## 4. Highest risk, highest value

- **Highest risk:** the LSM-backed state machine (task 4). Three pieces of state must become
  durable atomically, a second log appears, and the snapshot protocol changes under it.
- **Highest value:** the same task. It puts the project's storage engine behind its consensus, and
  makes apply exactly-once across restarts.
- **Most urgent:** task 1. It is small, and today one valid request disables a node.
