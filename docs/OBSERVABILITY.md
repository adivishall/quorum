# OBSERVABILITY — what a running node reports (Phase 16)

Status: **implemented and verified (#1).** Every number a node exports is produced by the code path
it describes: a counter is incremented where the event happens, a histogram observes a duration
measured around the operation, a gauge reads live state at the moment of the scrape. Nothing is
sampled, estimated or reconstructed. This document lists every metric, where it comes from, what
it means and — as carefully — what it does not mean.

---

## 1. The endpoint

`dkvd -metrics-listen HOST:PORT` serves `GET /metrics` in the Prometheus text format (0.0.4), on
its own port, separate from the client (`-client-listen`) and admin (`-admin-listen`) ports. The
node logs `event=metrics_ready node=... addr=...` once the listener is bound; a bad address is a
startup error. One registry per process holds every layer's families, so a node hosting many groups
reports each group as its own series (label `group`).

```bash
dkvd -id n1 -listen 127.0.0.1:7001 -peers n2=...,n3=... -raft -data-dir d1 \
     -client-listen 127.0.0.1:8001 -metrics-listen 127.0.0.1:9101
curl -s http://127.0.0.1:9101/metrics
```

Families are sorted by name and series by label values (numerically where the values are
numbers), so two scrapes of the same state are byte-identical. `internal/metrics.Parse` reads the
format back; the tests and the load tools use it.

## 2. Rules the instrumentation follows

- **No behaviour change.** Every metric method is a no-op on a nil receiver, and a node started
  without a registry wraps nothing and follows no write. The existing suites run with no
  registry; the gate (`make faults FAULT_SEEDS=200`, `make mutation`) is unchanged by this layer.
- **No clock in shared code.** The Raft core gained only plain counters it never reads
  (`raft.Counters`); the deterministic simulator's shared functions (`DrainReadyAt`,
  `ApplyCommitted`, `Waiters`) are untouched. Timing lives in the node's actor and in wrappers
  around its storage.
- **Counted where it happens.** Role transitions are counted by the core at the transition,
  so a campaign won inside one step, or a deposition and re-election between two observations,
  is still counted. Apply decisions are counted by an observer outside the replicated state, because
  `kv.Store.Stats` resets on every snapshot restore and would lie as a metric.
- **Hot paths only increment.** Series for a fixed label set are resolved once and cached by a
  struct key; a request adds no allocation (§5).

## 3. Reference

Types: C counter, G gauge, H histogram (seconds; buckets 50 µs to about 26 s, doubling). Every
`dkv_raft_*` series carries `group`.

### Client API (`internal/kv`)

| Metric | Type | Labels | Meaning | Produced by |
|---|---|---|---|---|
| `dkv_kv_requests_total` | C | group, op, status | Client requests this node's front answered, by the status it returned. A request relayed from a forward is counted here with the leader's status. | `Front.Do` |
| `dkv_kv_request_seconds` | H | op | Server-side duration: the front receiving the decoded request to its response being ready. Network time excluded. | `Front.Do` |
| `dkv_kv_duplicate_responses_total` | C | group | OK responses for a retry answered from the session table. | `Front.Do` |
| `dkv_kv_inflight_requests` | G | | Requests the front is working on. | `Front.Do` |
| `dkv_kv_forwards_total` | C | group, result | Forwards to the group's leader, by outcome: `answered`, `not_sent` (no connection: definite no effect), `send_unknown`, `timeout`. | `Server.forward` |
| `dkv_kv_forwarded_requests_total` | C | group, op, status | Requests forwarded to this node by a peer and served here. | `Server.onApp` |
| `dkv_kv_apply_decisions_total` | C | group, decision | State-machine decisions this replica made applying committed commands: `registered`, `executed`, `duplicate`, `conflict`, `stale`, `expired`, `limit`, and `evicted` sessions. | `Store.ApplyResult` via `Metrics.Observe` |

### Raft driver (`internal/raftnode`), per group

| Metric | Type | Labels | Meaning | Produced by |
|---|---|---|---|---|
| `dkv_raft_role` | G | role | 1 for the current role (`follower`, `candidate`, `leader`), 0 for the others. | Status |
| `dkv_raft_term`, `_commit_index`, `_applied_index`, `_last_index`, `_snapshot_index`, `_boundary_index` | G | | The node's term and indexes. | Status |
| `dkv_raft_has_leader` | G | | 1 if the node knows a leader of its term. | Status |
| `dkv_raft_log_bytes` | G | | The durable log file's length (`raftlog.Log.Size`). | Status |
| `dkv_raft_pending_writes`, `_pending_reads` | G | | Writes waiting for their apply; reads waiting for confirmation or apply. | Status |
| `dkv_raft_voters`, `_learners`, `_configuration_joint`, `_configuration_pending`, `_removed` | G | | The current configuration. | Status |
| `dkv_raft_follower_lag_entries` | G | peer | On a leader only: its last index minus each other member's match index. | Status (`raft.Raft.Progress`) |
| `dkv_raft_campaigns_total` | C | | Elections this node started. | the core, `becomeCandidate` |
| `dkv_raft_elections_won_total` | C | | Terms this node became leader of. | the core, `becomeLeader` |
| `dkv_raft_leader_stepdowns_total` | C | | Times this node stopped leading. | the core, `becomeFollower` |
| `dkv_raft_leader_changes_total` | C | | Times this node observed a (leader, term) it had not followed or been, checked after every Ready cycle. | `Node.observeStatus` |
| `dkv_raft_proposals_total` | C | result | Client writes offered to the core: `accepted` or `refused` (not leader). | actor, `writeCh` |
| `dkv_raft_commit_seconds` | H | | For each client write this node accepted as leader: its append to this node seeing it committed. | `trackCommitted` |
| `dkv_raft_apply_seconds` | H | | For those writes: from being seen committed to being applied. | `trackApplied` |
| `dkv_raft_persist_seconds` | H | | Each durable Save of the log: records written and fsynced. | `timedLog.Save` |
| `dkv_raft_persisted_entries_total` | C | | Entries written by Saves. | `timedLog.Save` |
| `dkv_raft_snapshots_created_total` | C | trigger | Snapshots published: `periodic` or `request`. | actor, `processReady`, `snapCh` |
| `dkv_raft_snapshot_create_seconds` | H | | Encode, publish by rename, compact the log behind it. | same |
| `dkv_raft_snapshots_installed_total`, `_snapshot_install_seconds` | C, H | | Snapshots from a leader made durable and active. | `timedStorage.InstallSnapshot` |
| `dkv_raft_snapshot_transfers_total` | C | result | Snapshot transfers streamed to a follower: `sent` or `failed`. | `startTransfer`, `transfer` |
| `dkv_raft_messages_dropped_total` | C | reason | `outbox_full`, `send_failed`, `decode`, `wrong_group`. Raft retransmits. | `enqueue`, `sendMessage`, `receiveLoop` |
| `dkv_raft_configurations_total` | C | kind | Configuration entries adopted after start: `joint` or `stable`. | `noteConf` |
| `dkv_raft_membership_changes_total` | C | type, result | Changes proposed through this node: `refused`, `completed`, `lost`. | actor `confCh`, `settleChanges` |

### Node host (`internal/multiraft`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `dkv_host_groups` | G | state | Groups `running`, or `failed` (on disk, did not recover). |
| `dkv_host_frames_dropped_total` | C | reason | Frames delivered to no group: `malformed`, `unknown_group`, `inbox_full`. |
| `dkv_host_groups_retired_total` | C | | Groups stopped because their node saw a configuration without it. |
| `dkv_host_group_failures_total` | C | | Groups that failed to recover at start. |

### Transport (`internal/transport`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `dkv_transport_frames_sent_total`, `_bytes_sent_total` | C | kind | Frames written in full to a connection, and their bytes including the record header. |
| `dkv_transport_frames_received_total`, `_bytes_received_total` | C | kind | Frames read whole. |
| `dkv_transport_send_failures_total` | C | reason | `not_connected`, `closed`, `write`. |
| `dkv_transport_connections_total` | C | dir | Connections established, `inbound` or `outbound`. |
| `dkv_transport_dial_failures_total` | C | | Failed dials or handshake sends. |
| `dkv_transport_peers` | G | state | Peers `known` and `connected`. |

### Process

`go_goroutines`, `go_heap_objects_bytes`, `go_heap_inuse_bytes`, `go_memory_total_bytes` and
`go_gc_cycles_total` are read through `runtime/metrics`, which does not stop the world.
`process_cpu_seconds_total` and `process_max_rss_bytes` come from `getrusage`; RSS is in bytes on
both Darwin and Linux. `process_start_time_seconds` is also reported.

## 4. What the metrics do not mean

- **They are one node's view.** Cluster-wide values are sums over nodes. A client request is
  counted once at the front that answered it; a forward also appears as `forwards_total` at the
  follower and `forwarded_requests_total` at the node that served it. Summing
  `dkv_kv_requests_total` over nodes therefore counts each attempt once.
- **Latencies are server-side and partial.** `dkv_kv_request_seconds` excludes the network.
  `dkv_raft_commit_seconds` covers only writes this node accepted as leader, and only those that
  committed with its term. A write whose index was overwritten is not observed, so a leader change
  can hide slow writes from this histogram. Client-observed latency is the load generator's to
  measure (#2).
- **Leader changes are observed, not reconstructed.** `dkv_raft_leader_changes_total` compares
  statuses between Ready cycles. Two leaders elected inside one cycle count once. Elections won and
  campaigns are exact: the core counts them.
- **Counters restart with the process,** and with a group stopped and restarted inside it:
  scrapers must treat a decrease as a reset. `dkv_kv_apply_decisions_total` includes
  re-applications after a restart; it counts work done, not requests.
- **Gauges are sampled at the scrape.** They read each node's latest published Status, which the
  actor publishes after every cycle, so they are at most one cycle old.
- **No storage-engine metrics.** The LSM engine is not behind the node yet
  (`docs/ENGINEERING_ROADMAP.md` task 5); `dkv_raft_log_bytes` is the Raft log, not an engine WAL.
- **The endpoint is unauthenticated plaintext HTTP.** It reveals node ids, peer counts, group
  layout and traffic volume. Bind `-metrics-listen` to a private interface; the project has no
  security model (`docs/LIMITATIONS.md`).

## 5. Cost (measured)

Apple M4 (10 cores), macOS 26.5, Go 1.27.1.

| Measurement | Result |
|---|---|
| counter increment / cached labelled series | 1.7 ns / 29 ns (`BenchmarkCounterInc`, `BenchmarkCounterVecWith`) |
| histogram observation / with `time.Now` | 4.7 ns / 58 ns |
| one scrape of a 16-group registry | 96 µs (`BenchmarkScrape`; 705 µs before the runtime figures moved to `runtime/metrics` and the label escaper stopped being rebuilt per call) |
| identified PUT at the leader, 3 nodes, loopback, no fsync, 10 interleaved runs × 3,000 ops | median 44.5 µs bare, 45.2 µs instrumented; ranges 43.9–61.8 and 44.5–62.8 |
| same, via a follower that forwards | median 74.5 µs bare, 75.1 µs instrumented |
| allocations per request | 113 (leader) and 150 (follower), bare and instrumented alike |

The instrumented request path is within the run-to-run noise of the uninstrumented one
(`BenchmarkInstrumentationOverhead`).

## 6. How it is verified

- **Format and primitives:** `internal/metrics` tests the exact exposition, escaping, cumulative
  inclusive buckets, the parse round trip, nil safety, conflicting registration and exact totals
  under concurrent updates and scrapes (race detector).
- **Ground truth on real TCP clusters:**
  - `TestMetricsMatchWhatTheClusterDid` checks the Raft metrics against the test's own count of
    accepted and refused writes, every node's Status, the log file's size on disk, snapshots
    published, a stopped follower's lag, a leader stopped and replaced, and an install by snapshot.
  - `TestRoleGaugesShowOneLeader` checks that exactly one node reports leader, under a no-election
    premise.
  - `TestMembershipMetrics` covers membership changes and configurations: the joint configuration
    is counted adopted by the quorum that committed it (a member outside that quorum can receive the
    joint and the final entry in one batch, and adopts only the final), and every member ends in
    the final one.
  - `TestKVMetricsMatchTheResponses` checks every front's counts against the test's own tally of
    responses; forwards answered equal forwards served, and each replica's decisions match what it
    applied.
  - `TestDecisionCountsSurviveARestore` checks that the decision counter survives a snapshot
    restore mid-count.
  - `TestHostMetrics` and `TestTransportMetrics` check dropped frames, groups, frames and bytes by
    kind, and connections.
- **Mutants 160–165** break the series key, the front's count of every answer, the decision
  observer, the counting of role transitions by difference, the log's size and the order of a write's
  completion after its Status; each is killed by
  the test above written for it. `FuzzParse` fuzzes the parser, and this package's own output must
  always parse back.
- **Real processes:** `TestRealProcessesServeTruthfulMetrics` runs three `dkvd` processes and
  scrapes `/metrics` over HTTP. Each write sent through a follower is counted once at the answering
  front, forwarded by the follower, served by the leader and executed on every replica. It also
  checks CPU and heap, that the connected-peers gauge matches the connections the process's own log
  shows, and that a survivor counts the election it wins after a SIGKILL. Its premises are read from
  the processes' logs and metrics, and a run that violates one starts over on a fresh cluster: the
  cluster has settled before the writes (every node follows one leader, every link is up — a leader
  exists as soon as a majority is connected, while the last link may still be waiting on the
  transport's 500 ms redial), no election ran during the writes, and no link changed around the
  scrape the gauge is compared with (`TestSettledStartWaitsForEveryLink` checks the settling itself,
  on a cluster whose third node starts after the other two elected a leader).

## 7. Found and fixed while building it

1. **Two label sets could be one series** (found by `FuzzParse`, minimized to one line). A series
   was keyed by its label values joined with a `\xff` byte, and the exposition split the key back
   apart. A value holding that byte panicked the scrape, and `("a\xff", "b")` and `("a", "\xffb")`
   collided. Each series now keeps its own values, and the key length-prefixes each one
   (`TestLabelValuesAreNeverAmbiguous`, mutant 160). No label this system produces holds that
   byte, but a tool parsing arbitrary scrapes would have hit it.
2. **A scrape cost 705 µs.** Two causes: five stop-the-world `runtime.ReadMemStats` calls, and a
   `strings.Replacer` rebuilt for every label value. After moving to `runtime/metrics` and a
   package-level escaper it costs 96 µs.
3. **The instrumented front allocated four times per request,** building label strings. Its series
   are now cached by a struct key, and it allocates exactly as the bare front does.
4. **A metric that would have lied:** the store's own decision counters reset on every snapshot
   restore. The exported counts come from an observer outside the replicated state instead
   (`TestDecisionCountsSurviveARestore`, mutant 162).
5. **A write could complete before the node's Status covered it** (found by this branch's race
   gate: `TestWriteCompletesOnlyAfterApply`, a Phase 12 test). The waiter completed inside the
   apply loop, and the Status that covers the write was published at the end of the same cycle, so
   a client that saw its write complete could read an older applied index. The race existed on
   `main`, failing 1 run in 60 under `-race`. The instrumentation's extra end-of-cycle work widened
   it to 3 in 60. The node now completes a cycle's applied writes when the cycle ends, after its
   Status is published: 0 in 60. `TestWriteCompletionFollowsItsStatus` holds the actor between the
   apply and the Status and makes the old order fail every write (mutant 165). No client-visible
   response changed: a completion was never early relative to the apply, only relative to
   `Status`. One cost: in a cycle that also creates a snapshot, the writes applied in it now wait
   for the snapshot to be published before they complete. That is once every `-snapshot-every`
   entries, and it adds that one creation's duration, about 50 ms at 100,000 keys
   (`docs/SNAPSHOTS.md` §14). The actor was already blocked for that time.

