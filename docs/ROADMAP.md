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
| 4 | Bloom + compaction | bloom filters, size-tiered compaction, MANIFEST | INV-S3, S5, S6, S7 verified; crash-during-compaction test passes | ☐ |
| 5 | Storage benchmarks | `bench/`, `docs/BENCHMARKS.md` | Reproducible numbers with recorded hardware/config; no fabricated figures | ☐ |
| 6 | Sharding | consistent hash ring, shard metadata, routing | INV-C1, C2, C3 verified; ring visualization | ☐ |
| 7 | Node process + networking | `cmd/dkvd`, `internal/transport`, framing, heartbeats | Real 3-process cluster; nodes communicate over TCP; clean shutdown | ☐ |
| 8 | Replication model | replica groups, replicated-log interface | Interfaces Raft will drive exist and are exercised; **no consistency claim yet** | ☐ |
| 9 | Raft | deterministic core + node driver + persistence | INV-R1..R10 verified in deterministic simulation; paper figures reproduced as tests | ☐ |
| 10 | Fault injection | `internal/fault`, drop/delay/dup/partition/crash | Full matrix in `docs/FAILURE_MODEL.md` §7 runs from a seed in CI | ☐ |
| 11 | Crash recovery | real-process kill/restart harness | Leader crash, follower crash, crash during compaction/WAL/flush all recover | ☐ |
| 12 | Consistency testing | linearizability checker, reference-model diff | Real histories from a real cluster under faults check out (INV-X1, X3) | ☐ |
| 13 | Idempotency & client semantics | request IDs, dedup table, redirection, retries | INV-X2 verified; exactly-once *application* becomes claimable | ☐ |
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
