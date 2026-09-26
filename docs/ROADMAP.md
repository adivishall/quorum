# ROADMAP

The build order is not arbitrary. Each phase exists because the next one cannot be tested
honestly without it.

Rules that apply to every phase:
- No phase is complete while a core-correctness test is failing.
- Every feature ships with: implementation, tests, defined failure behavior, documentation.
- `go test ./... -race` and `go vet ./...` must pass before the phase commit.
- Each phase ends with a report: what was built, why, tests run, failures found, fixes, current
  guarantees, known limitations.

Legend: ☐ not started · ◐ in progress · ☑ complete and verified

---

| # | Phase | Deliverable | Exit criterion | Status |
|---|---|---|---|---|
| 0 | Architecture & spec | `docs/*` for architecture, design, consistency, failure model, invariants | Formats, protocols, and invariants are specified precisely enough to be implemented against and falsified | ☑ |
| 1 | Single-node KV | `internal/storage` in-memory engine, `cmd/dkv` CLI | Concurrency-safe PUT/GET/DELETE, `-race` clean, error taxonomy defined | ☑ INV-A1..A9 |
| 2 | Write-ahead log | segmented WAL, CRC, replay, torn-tail repair | SIGKILL-and-recover test passes; corruption tests pass | ☑ INV-S1 (process kill), S2, S8, W1–W9 |
| 3 | MemTable + SSTable | skip list, SST writer/reader, flush, multi-level read path | Reads correct across memtable + N SSTables; restart-safe | ☑ INV-L1..L9; INV-S5 partial (flush only) |
| 4 | Bloom + compaction | bloom filters, size-tiered compaction, MANIFEST | INV-S3, S5, S6, S7 verified; crash-during-compaction test passes | ☑ INV-S3, S5, S6, S7; INV-B1–B2, C1–C7, M1–M5 |
| 5 | Storage benchmarks | `internal/bench`, `cmd/dkvbench`, `bench/`, `docs/BENCHMARKS.md` | Reproducible numbers with recorded hardware/config; no fabricated figures | ☑ harness + measurements; see `docs/BENCHMARKS.md` |
| 6 | Sharding | consistent hash ring, shard metadata, routing | INV-C1, C2, C3 verified; ring visualization | ☑ INV-C1, C2, C3 (routing); `internal/routing`, `cmd/dkvring`, `docs/ROUTING.md` |
| 7 | Node process + networking | `cmd/dkvd`, `internal/transport`, framing, heartbeats | Real 3-process cluster; nodes communicate over TCP; clean shutdown | ☑ INV-T1..T6; `internal/transport`, `cmd/dkvd`, `docs/TRANSPORT.md`; real 3-process TCP test |
| 8 | Replication model | replica groups, replicated-log interface | Interfaces Raft will drive exist and are exercised; **no consistency claim yet** | ☑ INV-P1..P9; `internal/replication`, `docs/REPLICATION.md`, ADR-015 |
| 9 | Raft | deterministic core + node driver + persistence | INV-R1..R10 verified in deterministic simulation; paper figures reproduced as tests | ☑ INV-R1..R10; `internal/raft`, `internal/raftlog`, `internal/raftnode`, `docs/RAFT.md`, ADR-016; real 3-process election + SIGKILL recovery |
| 10 | Fault injection | `internal/fault`, drop/delay/dup/partition/crash | Full matrix in `docs/FAILURE_MODEL.md` §7 runs from a seed in CI | ☑ INV-F1..F5; INV-R1..R10 re-verified under faults; `internal/fault`, `internal/vfs`, `internal/raftsim`, `docs/FAULTS.md`, ADR-017. Simulator rows replay from a seed; real-driver and real-process rows run in CI with real timing; "leader crash mid-write" is PARTIAL (no client acks until Phases 12–13) and the engine crash-window rows remain the standalone Phase 3–4 tests (hosted-engine windows: the phase that hosts the engine) |
| 11 | Crash recovery | crash-window model + harness: `raftnode` crash points, `raftsim` crash matrix, `dkvd -crash-at` | Every crash window of the Raft node characterised and tested; recovery never produces a state that violates Phases 9–10's guarantees | ☑ INV-CR1..CR4; INV-F2 at every crash point; `docs/CRASH_RECOVERY.md`, ADR-018. A 1,440-cell bounded exhaustive matrix (every driver and I/O boundary × process crash / power loss / torn power loss) with 0 failures; seeded `crashpoints` schedules; real SIGKILL at exact points; found and fixed a window that bricked a node (a Save's record order). Scoped to the node as it exists — no client, no dedup: application is proven at-least-once, exactly-once per incarnation. The engine's hosted crash windows (WAL/flush/compaction under a node) wait for the phase that hosts the engine; its standalone crash tests are Phases 2–4 |
| 12 | Consistency testing | linearizability checker, reference-model diff | Real histories from a real cluster under faults check out (INV-X1, X3) | ☑ INV-X1, X3, X5–X10; `docs/LINEARIZABILITY.md`, ADR-019. ReadIndex in the pure core (heartbeat sequence, no-op rule), writes completed at commit-and-apply in their term (ErrLost otherwise), a minimal `-client-listen` protocol, and `internal/lincheck` — validated against an independent oracle (20,000 arbitrary histories), a 35-file known-good/known-bad corpus and fuzzing — checking client histories from real processes (concurrency, leader/follower SIGKILL, partitions, a 5-node minority leader with a follower, a SIGKILL at every point of a write's life), the real driver, and 1,400 seeded simulator runs; 27 mutants killed. Mutation found and closed one hole in the history tiers (no test had a stale leader that still had a follower); fuzzing found the key-value codecs accepting non-canonical varints (no consistency impact; fixed). No execution produced a non-linearizable history. Scoped: single-key ops, one Raft group, finite recorded histories, honest recording of retries — hidden retries need Phase 13's dedup |
| 13 | Idempotency & client semantics | request IDs, dedup table, redirection, retries | INV-X2 verified; exactly-once *application* becomes claimable | ☑ INV-X2, X11–X14 (X9 extended); `docs/CLIENT_SEMANTICS.md`, `docs/DEDUP.md`, `docs/API.md`, LINEARIZABILITY §15, ADR-020. Cluster-assigned sessions (ClientID = the REGISTER entry's index) and client RequestIDs with an acknowledgement watermark; the decision (executed / duplicate with the original index / conflict / stale / expired / limit) made at apply from a bounded session table inside the replicated state machine, rebuilt by replay; eleven statuses in three outcome classes; one-hop forwarding over transport kinds 32/33 (or redirect-only); wire protocol v2; a retrying session client. Verified: the logical history (a request's sends as one operation) is linearizable on real processes — the hardest retry case at all eight crash windows, forwarders killed before relaying, concurrent copies through every node, a full-cluster restart with small limits, session workloads under kill/partition/rolling restart — in-process, and in 1,200 more seeded simulator runs where every replica's every apply-time decision matches an independent model (INV-X11); the checker's logical merge validated against an independent reading of the contract (20,000 histories) and a 48-file corpus; 31 mutants killed; fuzzing found the wire decoder non-canonical for durations past `time.Duration`, and the race suite a data race in the driver's start-up log line (since Phase 9) — neither with a consistency impact; both fixed. Claimed: at most one execution per identity (exactly one if it executes) — not exactly-once delivery, nothing new for anonymous writes, nothing after eviction beyond `SESSION_EXPIRED` |
| 14 | Snapshots | snapshot create/install, log truncation | Follower catches up from snapshot; restart from snapshot works | ☐ |
| 15 | API + CLI | HTTP endpoints, `dkv` CLI | `docs/API.md` matches behavior; error messages are actionable | ☐ |
| 16 | Observability | real metrics, structured logs | Every dashboard number traces to a counter incremented by real code | ☐ |
| 17 | Dashboard | cluster / raft / storage / perf views + fault controls | Fault buttons trigger real faults; no simulated state | ☐ |
| 18 | Docker demo | `docker compose up` → 3 or 5 nodes + dashboard | Separate containers; demo workflow reproducible from a clean clone | ☐ |
| 19 | Load testing | workload generator, 1/3/5-node runs, failure workloads | Reproducible throughput/latency/election-time numbers | ☐ |
| 20 | Correctness & chaos suite | `make test` / `make integration` / `make chaos` | The whole matrix green, from a clean clone | ☐ |
| 21 | Hardening | validation, limits, timeouts, graceful shutdown | Fuzz/limit tests pass; no secrets; safe logging | ☐ |
| 22 | Documentation | full `docs/` set | Docs match implementation; every guarantee traces to an invariant with a passing test | ☐ |
| 23 | Demo script | `docs/DEMO_SCRIPT.md` | A 5-minute run executes end to end without improvisation | ☐ |
| 24 | Interview prep | `docs/INTERVIEW.md` | Answers derived from this implementation, not from generic theory | ☐ |
| 25 | Resume material | `docs/RESUME.md` | Bullets cite measured results that exist in `docs/BENCHMARKS.md` | ☐ |

---

## Dependency reasoning (why this order)

- **Storage before networking.** A distributed system built on an unreliable local engine
  produces bugs that look like consensus bugs. Making the single-node engine crash-correct
  first means that later, when a cluster diverges, we can trust that the divergence came from
  the distributed layer.
- **Benchmarks (5) before sharding.** We need a single-node performance baseline to make any
  later statement about what distribution costs. Without it, "the cluster does X ops/sec" has
  no denominator.
- **Sharding (6) before nodes (7).** Routing is a pure function and can be fully tested with no
  network at all. Debugging a hash ring over TCP is self-inflicted.
- **Networking (7) before replication (8) before Raft (9).** Raft's correctness arguments assume
  a message-passing model. Having a real, faulty transport underneath before implementing Raft
  prevents the classic mistake of writing Raft against reliable in-process channels and
  discovering later that it assumed reliability.
- **Fault injection (10) immediately after Raft.** Happy-path Raft is roughly 40% of the work and
  100% of the bugs are in the other 60%. Faults come before snapshots, before the API, before
  anything cosmetic.
- **Snapshots (14) after consistency testing (12).** Snapshotting a consensus implementation
  that is not yet known to be correct just makes the bugs durable.
- **Dashboard (17) after observability (16).** The dashboard must render metrics that already
  exist for the system's own sake. Building the dashboard first is how fake counters get born.

## Deliberately deferred past v1

Dynamic membership changes and shard rebalancing, cross-shard transactions, TLS/auth,
compression, block cache, leveled compaction, leader leases. Tracked in
`docs/LIMITATIONS.md` with the reason each was cut.
