# ENGINEERING ROADMAP — the next layer after Phase 15

Status: **audit of `b401edf` (2026-09-29), and the plan that follows from it.** `docs/ROADMAP.md` is
the phase table; this document is the audit behind the next phases and the ranked list of
engineering tasks they are made of. Each audit claim was checked against the code, not the
documents, and names where it was found.

---

## 1. Audit

### What the database does

- **Serves linearizable single-key PUT/GET/DELETE** through real `dkvd` processes: each key's shard
  is a Raft group (`-cluster`), or everything is group 0 (`-raft`); writes complete at
  commit-and-apply in their term, reads through ReadIndex (`docs/LINEARIZABILITY.md`).
- **Deduplicates identified requests** at apply from a replicated, bounded session table
  (`docs/DEDUP.md`); a request is forwarded one hop to its group's leader.
- **Survives crashes** at every boundary of the node's cycle, with durable Raft logs, snapshots and
  log compaction (`docs/CRASH_RECOVERY.md`, `docs/SNAPSHOTS.md`).
- **Changes membership** one member at a time by joint consensus, and hosts many groups per process
  with isolated state (`docs/MEMBERSHIP.md`, `docs/MULTI_RAFT.md`).
- **Has a durable single-node LSM engine** (WAL, memtable, SSTables, Bloom filters, size-tiered
  compaction, MANIFEST) with its own crash tests and benchmarks (`docs/LSM.md`,
  `docs/BENCHMARKS.md`).

### What is simulated or modeled

- Power loss: a crash-consistent disk model (`internal/fault`), never real hardware.
- Most partition, reordering and duplication schedules: the deterministic simulator
  (`internal/raftsim`); real processes are partitioned only by resetting TCP connections.

### What is disconnected

- **The replicated state machine is in memory** (`kv.Store`, `internal/kv/store.go`), rebuilt from
  the latest snapshot plus the log. `dkvd` never opens the LSM engine; `internal/raftnode` does not
  import `internal/storage` (`cmd/dkvd/main.go:403`).
- The engine is not ready to be a Raft state machine:
  - it has no atomic multi-key write: `wal.Batch` is crash-atomic, but both stores append
    single-operation batches only (`internal/storage/lsmstore.go:718`);
  - its applied index is a separate WAL record, not atomic with the data it describes
    (`internal/storage/wal/batch.go:26`);
  - its MANIFEST `Applied`/`LogNumber` fields are always zero;
  - it has no iterator or checkpoint and no way to restore from a snapshot stream: `Snapshot()`
    materialises the whole state as a map (`lsmstore.go:1248`);
  - its WAL is never truncated and is replayed in full on every open (`docs/LIMITATIONS.md`).
- The client CLI (`cmd/dkv`) is in-memory and not a network client; there is no HTTP API; the
  `internal/api`, `internal/cluster`, `internal/config`, `internal/metrics`, `pkg/client`,
  `docker/`, `dashboard/` and `tests/chaos` directories are empty.

### What is strongly verified

- Raft safety (INV-R1..R10) under every fault family in seeded, replayable simulation; crash
  recovery at every driver and I/O boundary (three exhaustive crash matrices); client-visible
  linearizability and at-most-once execution on recorded histories from real processes, the
  in-process driver and the simulator; snapshots (INV-SN); membership and group isolation
  (INV-MB); 159 mutants killed. See `docs/INVARIANTS.md`.

### What is only partially verified

- Durability against real power loss (modeled only).
- Histories are finite and small: at most five nodes, eight clients, four groups with clients.
- Real-process partitions are connection resets, never silent packet loss; one-way partitions exist
  only in the simulator.

### What is not measured

- Anything distributed under load: cluster throughput, request latency distributions, election
  time, client-visible outage on leader failure, replication lag, the cost of snapshots, restarts
  and membership changes under load (`docs/BENCHMARKS.md`, `docs/LIMITATIONS.md`). The only
  cluster-level numbers are idle-group costs and membership-change latency from in-process tests
  (`docs/MULTI_RAFT.md` §9).

### What is not operationally visible

- A running node exposes no metrics: its only signals are `event=` log lines (a 20 ms status poll
  that can miss short leaderships) and an admin `status` call. Nothing in the non-test code
  measures a duration. Frame-drop counters exist but are read only by tests
  (`internal/multiraft/host.go:349`); the store's decision counters reset on every snapshot
  restore (`internal/kv/snapshot.go:99`). Most `raftnode` log events carry no group.

### What is missing for realistic use

- Operational visibility, a deployment recipe, an operator tool, a networked client, state larger
  than memory, bounded restart work for the state machine, and protection against the liveness
  hazards the membership phase documented (no PreVote, no CheckQuorum).

### What is missing for a convincing systems project

- Measured behaviour. Every correctness claim is backed; no performance or availability claim about
  the cluster is, and the storage engine's value to the distributed system is unrealised.

---

## 2. Ranked engineering tasks

Ranked by engineering value: what each unblocks, and what it would cost to be wrong without it.

### 1. Observability — metrics from real code (#1)

- **Problem:** no metrics, no durations, nothing exported.
- **Evidence:** §1, "not operationally visible".
- **Why it matters:** every later task — load testing, the LSM state machine, performance work,
  chaos diagnosis — needs to see what the system did; without it, numbers are guesses.
- **Architecture impact:** a leaf `internal/metrics` package; optional hooks in raftnode, kv,
  multiraft and transport; counters in the pure core that nothing decides on; a `/metrics` port.
- **Difficulty:** medium. **Correctness risk:** low, as long as instrumentation never changes
  behaviour and never reaches the simulator's shared code with a clock.
- **Testing:** ground-truth tests on real in-process clusters and real processes; the full suite,
  200-seed faults and the mutation run unchanged.
- **Benchmark:** request-path overhead with and without metrics.
- **Docs:** `docs/OBSERVABILITY.md`, ARCHITECTURE, LIMITATIONS.

### 2. Load generator for real clusters (#2)

- **Problem:** nothing puts load on a cluster; `dkvbench` is storage-only and closed-loop.
- **Evidence:** §1, "not measured".
- **Why it matters:** the instrument every performance claim is measured with.
- **Architecture impact:** new `internal/load`, `cmd/dkvload`; a client of the existing protocol.
- **Difficulty:** medium. **Correctness risk:** none to the database; the risk is a harness that
  measures itself (coordinated omission, client-side bottlenecks), which the tests must rule out.
- **Testing:** determinism for a seed, distributions, open-loop accounting against injected stalls.
- **Benchmark:** the generator's own ceiling against a no-op server.
- **Docs:** `docs/LOAD_TESTING.md`.

### 3. Cluster experiments and the first performance report (#3)

- **Problem:** no reproducible multi-process experiment exists outside the test suite.
- **Evidence:** the launcher and proxies live only in `tests/integration/*_test.go`.
- **Why it matters:** turns the two tasks above into published, reproducible numbers — the baseline
  every later change is judged against.
- **Architecture impact:** new `internal/lab`, `cmd/dkvlab`.
- **Difficulty:** medium. **Correctness risk:** none to the database.
- **Testing:** a short scenario in `make integration`; statistics unit-tested.
- **Benchmark:** 1, 3 and 5 nodes; 1 and several groups; read-heavy, mixed, write-heavy; leader
  kill; rolling restart; membership change; snapshots; repeated runs with medians and spread.
- **Docs:** `docs/CLUSTER_BENCHMARKS.md`.

### 4. Storage-engine prerequisites for state-machine use

- **Problem:** the engine cannot serve as a Raft state machine (§1, "disconnected").
- **Why it matters:** the database's durable engine is unused by the distributed system; state is
  bounded by memory and the 512 MiB snapshot bound.
- **Architecture impact:** inside `internal/storage` only:
  - an atomic write batch that carries the applied index with the data;
  - WAL truncation once a flush makes a prefix redundant;
  - an ordered iterator and a checkpoint;
  - bulk ingest for installing a snapshot.
- **Difficulty:** high. **Correctness risk:** high, but contained: the engine is still standalone
  and its crash harness applies.
- **Testing:** the existing crash tests extended to batches, truncation and ingest; the
  conformance suite; fuzzing of the new records.
- **Benchmark:** `dkvbench` before and after; restart time as data grows.
- **Docs:** LSM, WAL, MANIFEST, DECISIONS.

### 5. The LSM-backed replicated state machine

- **Problem:** see task 4. Wiring it in is where Raft and storage semantics meet.
- **Design constraints:**
  - Raft stays authoritative for order; apply stays deterministic.
  - The key-value data, the session table and the applied index are written in one atomic batch.
  - A snapshot is a consistent iteration, and installing one is an ingest.
  - Recovery replays the Raft log from the engine's durable applied index, which makes apply
    exactly-once across restarts. Today it is at-least-once, INV-CR4.
  - The engine's WAL and the Raft log are two logs (ADR-006); keeping, disabling or unifying them
    is decided with measurements from task 3.
- **Difficulty:** very high. **Correctness risk:** very high.
- **Testing:** the snapshot and membership crash matrices with the engine behind the node; a
  crash matrix over apply-and-flush windows; tests that separate consensus persistence (the Raft
  log) from application persistence (the engine); the linearizability tiers unchanged.
- **Benchmark:** throughput, latency and restart time against the task 3 baseline.
- **Docs:** DESIGN §10, CRASH_RECOVERY, SNAPSHOTS, DEDUP, CONSISTENCY (C3's durability meaning).

### 6. Chaos campaigns with load

- **Problem:** fault families are exercised per profile; real-process chaos is scenario tests only
  (`tests/chaos` is empty).
- **Why it matters:** combinations — load during a membership change during a partition during a
  snapshot — are where the next bugs are.
- **Architecture impact:**
  - simulator campaign profiles combining every family with clients;
  - a real-process runner that replays a recorded fault schedule against `dkvd`, with metrics and
    linearizability checking.
- **Difficulty:** medium-high. **Correctness risk:** none; it finds bugs.
- **Testing:** the campaigns are the tests; every failure replays from its seed or schedule.
- **Benchmark:** availability under chaos, from task 3's measurements.
- **Docs:** FAULTS, FAILURE_MODEL.

### 7. PreVote and CheckQuorum

- **Problem:** documented liveness hazards. A rejoining node forces an election; an isolated leader
  accepts requests it can never serve; a removed node can disturb a lagging member
  (`docs/MEMBERSHIP.md` §10).
- **Why it matters:** election churn and client timeouts under partitions, measurable with tasks 1
  to 3.
- **Architecture impact:** the pure core's election path.
- **Difficulty:** medium. **Correctness risk:** medium-high (election safety).
- **Testing:** simulator profiles with the INV-R and INV-MB checks; new mutants; real-process
  partition tests.
- **Benchmark:** elections and outage under partitions, before and after.
- **Docs:** RAFT, MEMBERSHIP, LIMITATIONS, DECISIONS.

### 8. Measurement-driven performance work

- **Problem:** unknown until task 3 measures it.
- **Likely candidates:**
  - many proposals per Save (group commit);
  - pipelined AppendEntries with in-flight windows;
  - heartbeat coalescing across groups, which is ADR-001's stated cost.
- **Why it matters:** only after a baseline; without one, optimisation is guesswork.
- **Difficulty:** medium each. **Correctness risk:** medium; the crash and fault suites must stay
  green.
- **Benchmark:** before and after on the task 3 harness, with variance.
- **Docs:** BENCHMARKS, CLUSTER_BENCHMARKS, DECISIONS.

### 9. A reproducible container cluster

- **Problem:** `docker/` is empty; `docker compose up` does nothing (Phase 18).
- **Architecture impact:** a multi-stage Dockerfile building the real `dkvd`, a compose file for 3
  and 5 nodes in `-cluster` mode with metrics ports, and a CI job that brings it up and runs a
  `dkvload` smoke test.
- **Difficulty:** low-medium. **Correctness risk:** none.
- **Testing:** the CI smoke job.
- **Benchmark:** none; containers share one disk (`docs/LIMITATIONS.md`).
- **Docs:** README, a deployment page.

### 10. Operator and client surface

- **Problem:** no operator tool beyond raw JSON lines; no networked client; `dkvd`'s package comment
  is stale; most `raftnode` events carry no group.
- **Architecture impact:**
  - `dkvctl` over the admin protocol;
  - health and readiness endpoints beside `/metrics`;
  - `group=` on every event;
  - the displaced API + CLI phase (an HTTP gateway or a networked `dkv`).
- **Difficulty:** medium. **Correctness risk:** low.
- **Testing:** real-process tests of each command.
- **Docs:** CLI, API, an operator guide.

---

## 3. The first wave: tasks 1 → 2 → 3

Observability, then the load generator, then cluster experiments with the first performance report.

- **They build on one another.** The load generator needs the metrics to explain what it measures:
  server-side latency, elections, persistence cost. The experiments need both. The report is their
  output.
- **They come before the LSM state machine.** The integration changes when an acknowledged write
  becomes durable in the state machine and adds work to the apply path. Its cost and its
  regressions can only be judged against a measured baseline, and its crash tests are far easier
  to diagnose with metrics.
- **They carry the least correctness risk,** so the system stays shippable while they land. The
  rules that keep it that way: instrumentation must not change behaviour, and the simulator's
  shared code must not see a clock.
- **Chaos, PreVote/CheckQuorum and performance work** all need these measurements to show their
  effect.

The phase table maps them to Phase 16 (observability) and Phase 19 (load testing). The dashboard
(Phase 17) and the Docker demo (Phase 18, task 9) follow once the metrics exist.
