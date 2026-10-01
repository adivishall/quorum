# CLUSTER BENCHMARKS — real clusters, measured (#3)

Status: **first baseline (#3), 2026-09-29.** Every number below is a measurement of real `dkvd`
processes under the stated configuration, on one machine. None of them is a capacity: a closed-loop
throughput belongs to its concurrency, every run shares one disk and one CPU with its load
generator, and loopback has no network latency. The raw output of every run is committed under
`bench/cluster/`; §8 reproduces it.

How the load is generated and what each number means: `docs/LOAD_TESTING.md`. Where each server-side
number comes from: `docs/OBSERVABILITY.md`.

---

## 1. Environment

| | |
|---|---|
| Machine | Apple M4, 10 cores, 16 GiB, internal SSD |
| OS | macOS 26.5.2 |
| Go | go1.27.1 |
| Commit | `b251527` (§3, §4, §5.1, §5.2), `415c61c` (§5.3 snapshots), `86c8518` (§6.3), `f1b6e8f` (§5.3 rolling restart and membership); the `dkvd` source is identical at all four — they differ only in `internal/lab` |
| Tree | clean at each (`git_dirty: false` in every file) |
| Network | loopback; every node and the load generator on the one machine |
| Persistence | every Raft persist is `os.File.Sync`, which on macOS issues `fcntl(F_FULLFSYNC)` (a drive-cache flush); all nodes share the one SSD |
| Raft timing | `dkvd` defaults: tick 50 ms, election timeout uniform in [10, 20) ticks (500–1000 ms), heartbeat every 2 ticks (100 ms) |

## 2. Method

`dkvlab` (`internal/lab`, `cmd/dkvlab`) builds `dkvd` from the repository, launches real processes on
loopback ports, drives them with `internal/load` through the ordinary client protocol (the session
client: identified requests, retried under their identity), acts on them while the load runs, and
scrapes every node's `/metrics` before and after the measured window. A run's numbers are:

- **from the clients:** throughput (operations completed with a definite answer inside the window,
  per second), exact latency percentiles per operation, outcomes by class (`ok`, `not_found`,
  `refused`, `unknown`), and the longest run of 100 ms buckets with no success (the outage);
- **from the nodes:** CPU per second, peak RSS, persists (fsyncs) and their mean duration, mean
  commit latency of a write, AppendEntries traffic and commit progress (§6.3 only), snapshots taken;
- **from the lab:** each action's settle time — a kill to the next leader seen by the admin
  protocol; a restart or an addition to the node having applied the commit index the leader had
  when the wait began.

The steady matrix (§3, §4) is **closed loop**: 16 clients, each issuing its next operation when the
previous completes. The fault scenarios (§5) are **open loop at 50 ops/s** — below every
configuration's closed-loop throughput (§3), so an outage shows as latency and missing successes
rather than as a queue that was already growing. Common workload: 10,000 keys, uniform; 100-byte
values; seed 1; a 3 s warmup, then a 20 s window (the leader-kill series and §6.3: 2 s and 10 s);
an attempt timeout of 2 s, up to 8 attempts; `dkvd`'s default snapshot interval of 10,000 entries,
which no steady run reached.

Each configuration ran **3 times** (leader kill: 10 times at each of two attempt timeouts). Tables
give the **median** and, in brackets, the **range** over the runs; p95 across runs is given only
where there are 10. A closed-loop run reports 16 "late" operations: the ones in flight when the
window closed, not delayed ones.

## 3. Cluster size and read/write mix — one group, closed loop, 16 clients

| Nodes | Reads | ok/s | GET p50 / p99 | PUT p50 / p99 | Persist (mean) | Commit (mean) | CPU per node | Peak RSS |
|---|---|---|---|---|---|---|---|---|
| 1 | 95% | **4,895** [4,853–4,900] | 0.5 / 16.0 ms | 4.2 / 16.1 ms | 3.9 ms | 3.9 ms | 0.29 | 26 MiB |
| 1 | 50% | **496** [487–499] | 23.8 / 71.9 ms | 40.0 / 64.0 ms | 3.9 ms | 3.9 ms | 0.11 | 22 MiB |
| 1 | 5% | **268** [268–272] | 6.0 / 28.0 ms | 64.0 / 68.0 ms | 3.9 ms | 3.9 ms | 0.09 | 21 MiB |
| 3 | 95% | **1,046** [1,039–1,050] | 11.5 / 61.1 ms | 28.1 / 75.1 ms | 7.5 ms | 28.3 ms | 0.13 | 21 MiB |
| 3 | 50% | **119** [112–119] | 125.7 / 195.9 ms | 143.5 / 219.8 ms | 9.5 ms | 125.9 ms | 0.05 | 21 MiB |
| 3 | 5% | **65** [63–69] | 226.0 / 271.8 ms | 247.9 / 296.0 ms | 9.5 ms | 227.9 ms | 0.05 | 20 MiB |
| 5 | 95% | **778** [716–848] | 16.3 / 69.1 ms | 36.0 / 89.0 ms | 9.9 ms | 37.0 ms | 0.08 | 21 MiB |
| 5 | 50% | **106** [105–109] | 138.9 / 209.9 ms | 158.1 / 237.4 ms | 10.7 ms | 143.8 ms | 0.04 | 22 MiB |
| 5 | 5% | **64** [64–65] | 231.4 / 266.9 ms | 251.0 / 282.0 ms | 10.4 ms | 238.4 ms | 0.04 | 21 MiB |

Persist and commit are the mean over the run's nodes of each node's mean (commit: the leader's).
The load generator used 0.02–0.24 of a core (the most at one node, 95% reads): it was never the
bottleneck. The largest spreads are 5 nodes at 95% reads (0.17) and 4 groups (0.16, §4); every
other configuration's is at most 0.09.

## 4. Groups per cluster — 3 nodes, RF 3, 50% reads, closed loop, 16 clients

`dkvd -cluster -shards G -rf 3`: G groups on the same three processes, keys routed by shard.

| Groups | ok/s | GET p50 / p99 | PUT p50 / p99 | Persist (mean) | Commit (mean) | CPU per node | Peak RSS |
|---|---|---|---|---|---|---|---|
| 1 | **115** [110–117] | 128.3 / 208.0 ms | 146.9 / 231.6 ms | 9.7 ms | 129.3 ms | 0.05 | 21 MiB |
| 4 | **186** [172–201] | 55.9 / 220.0 ms | 90.2 / 242.2 ms | 16.7 ms | 86.5 ms | 0.06 | 23 MiB |
| 16 | **214** [210–215] | 24.2 / 174.9 ms | 100.6 / 254.1 ms | 29.3 ms | 98.0 ms | 0.11 | 30 MiB |

One group in `-cluster` mode matches `-raft` (115 vs 119 ok/s). More groups write more logs
concurrently, so more writes are in flight at once — throughput rises 1.9× from 1 to 16 groups —
while each fsync takes longer (9.7 → 29.3 ms): the groups share one disk.

## 5. Faults — 3 nodes, 50% reads, open loop at 50 ops/s

### 5.1 Baseline: no fault

| | ok/s | GET p50 / p99 | PUT p50 / p99 | Persist | Commit | Outage |
|---|---|---|---|---|---|---|
| steady | 50 [50–50] | 2.9 / 6.1 ms | 14.1 / 18.6 ms | 4.8 ms | 15.8 ms | none |

### 5.2 Leader killed (SIGKILL), restarted 3 s after the new leader — 10 runs at each attempt timeout

Window 10 s after a 2 s warmup; the kill at 40% of the window.

| Attempt timeout | Election (kill → new leader) | Outage | PUT p99 | GET p99 | Unknown outcomes |
|---|---|---|---|---|---|
| 2 s | **521 ms** (p95 617) [453–617] | **450 ms** (p95 600) [400–600] | 681 ms [643–756] | 652 ms [341–710] | 0 in every run |
| 500 ms | **559 ms** (p95 685) [458–685] | **500 ms** (p95 600) [400–600] | 726 ms [644–746] | 673 ms [315–692] | 0 in 9 runs, 3 in 1 |

Every run kept the offered rate (500 operations issued, 254 OK and 246 NOT_FOUND in all but the one
run with 3 unknown). Every election settled inside the window the configuration allows: the first
of the followers' randomized timeouts, [500, 1000) ms after the last heartbeat, which was at most
100 ms before the kill. The attempt timeout made no measurable difference — the outage is the
election, not a client waiting on a dead node. The p99 latencies are the operations due during the
election (they wait for it).

**The restarted node's catch-up is bimodal: 45–84 ms in 9 runs, 346–465 ms in 11.** The cause,
established from the runs and a separate probe:

- The node with the smaller id of a pair dials it (ADR-014). The killed leader was n3 in all 20
  runs and the new leader n1 or n2, so the new leader had to redial n3 after its restart.
- A transport redials a dead peer every `DialRetryInterval` (500 ms), from when the connection died —
  the kill. The restart came 3 s after the election, so the wait for the leader's next redial is
  fixed by the election's duration: `500 − ((election + 3 s) mod 500)` ms.
- That formula, plus 16–46 ms, predicts all 20 catch-up times (`bench/cluster/README.md`).
- A supplementary probe (ten kill-and-restart cycles without load, not part of the lab) separated the
  two cases:
  - the three restarted nodes with a smaller id than the leader dialled it themselves, and caught up
    in 77–95 ms;
  - the seven the leader had to redial took 87–491 ms, by the same phase rule.

So a restarted node waits up to 500 ms for its peers' next redial before it can catch up — a fixed
interval with no backoff and no reset on an inbound attempt (a roadmap item, not a correctness issue).

### 5.3 Rolling restart, membership change, snapshots — 3 runs each

| Scenario | ok/s | GET p50 / p99 | PUT p50 / p99 | Outage | Settle | Unknown outcomes |
|---|---|---|---|---|---|---|
| rolling restart (SIGTERM, restart, catch up — each node in turn) | 49 [49–50] | 2.5 / 208.5 ms | 15.4 / 280.7 ms | 500 ms [400–500] | followers: 24–37 ms; the leader: 529–594 ms | 10, 14, 14 |
| membership (a 4th node added as a learner, caught up, promoted) | 50 [50–50] | 3.2 / 16.1 ms | 16.3 / 33.2 ms | none | addition: 148 ms [126–150] | 0 |
| snapshots (one every 100 entries; 5 per node per run) | 50 [50–50] | 3.0 / 7.3 ms | 15.7 / 27.1 ms | none | — | 0 |

- **Rolling restart** is the one scenario that lost outcomes: 10–14 operations per run ended
  `unknown` (11–15 in the first suite run), against 0 for a leader SIGKILL at the same rate. Of the
  three restarts in a run, the two followers' caught up in 24–37 ms: each had applied index 232–234
  and had missed nothing while it was down. The leader's took 529–594 ms, because its stop forces
  an election, and the 400–500 ms outage is of that size. Why some operations stayed unknown after
  every retry, rather than being settled by a retry under the same identity, is **not yet
  explained**. An unknown outcome is allowed by the contract (`docs/CLIENT_SEMANTICS.md` §6), but
  this many is a finding, and §10 lists it.
- **A membership change** under load caused no outage. The learner was added, caught up and
  promoted in 148 ms (median; 126–150 ms). The change does move write latency: PUT p99 was 33.2 ms
  in this series and 21.7 ms in the first suite run, against 18.6 ms with no change. Three runs
  cannot say by how much.
- **Snapshots** every 100 entries — five per node per run — raised PUT p99 from 18.6 to 27.1 ms
  (creation runs on the node's actor and pauses it, `docs/SNAPSHOTS.md`); nothing else moved.
  The first suite run of this scenario used an interval of 1,000 entries and took no snapshot at
  all (a run appends about 600); it measured nothing and is not reported. The lab now reports the
  snapshots each node created and installed, so a snapshot scenario that takes none is visible.
- **The rolling-restart and membership rows are a rerun.** Reviewing the lab found that its
  catch-up wait read the leader's commit index as its target and, if that read failed, kept 0. Any
  started node has applied 0, so the wait would have ended at once and reported the process start
  as the catch-up. The first suite run was taken with that code, so these rows come from a rerun
  with the fix (`f1b6e8f`), in which every catch-up records the index it reached. The two series
  agree: followers took 26–47 ms then and 24–37 ms now. The leader-kill catch-ups (§5.2) were
  checked separately: all 20 fit the redial model, which a target of 0 could not produce.

## 6. What limits throughput — the evidence

### 6.1 One fsync per write, never shared

The persist count per completed write, from each node's `dkv_raft_persist_seconds_count`,
normalised for the warmup, during which writes are not counted. These are approximate; §6.3 measures
the leader's ratio exactly against the commit index: 2.00.

| Configuration | Leader | Each follower |
|---|---|---|
| 1 node, any mix, 16 clients | 1.0 | — |
| 3 or 5 nodes, 95% reads | 2.0 | 1.8–1.9 |
| 3 or 5 nodes, 50% reads | 2.0 | 1.4–1.5 |
| 3 or 5 nodes, 5% reads | 2.0 | 1.1–1.2 |

- **With 16 clients writing concurrently, a single node still fsyncs once per write.** The driver's
  actor takes one event per iteration — one proposal, one message — and persists the Ready it
  produced, fsync included, before it takes the next (`raftnode` `actorLoop`, `processReady`):
  concurrent writes are never batched into one fsync. A write therefore costs at least one
  F_FULLFSYNC (3.9 ms here), which caps one group near 1 / 3.9 ms ≈ 255 writes/s whatever the
  concurrency. The 1-node, 5%-reads row measured 268 ops/s, 95% of them writes.
- **The leader fsyncs twice per write.** Once to append the entry and once when its commit index
  advances: a Ready whose HardState changed only in `Commit` is persisted and fsynced
  (`raft.Ready.HardState`, "persisted as an optimization"), and INV-CR3 requires the commit that
  covers an entry to be durable before the entry is applied. A follower's second fsync is shared
  with the next append when one arrives in the same message, which is why followers do fewer as the
  write share grows.
- **All nodes share one SSD.** The mean persist rises from 3.9 ms (1 node) to 7.5–10.7 ms (3 and 5
  nodes) and 29.3 ms (16 groups): F_FULLFSYNCs from different processes contend for one device.
  On separate machines each node would flush its own disk; the per-node numbers here are not what a
  multi-host deployment would see.
- **Commit latency is queueing.** At 3 nodes and 50% reads a commit took 125.9 ms on average,
  ≈ 13 persists: each write waits behind the fsyncs of the writes ahead of it.

### 6.2 Reads wait behind writes

A ReadIndex read is confirmed and answered in the same actor cycle that persists, so it queues
behind the cycle's fsyncs: GET p50 is 2.9 ms at 50 ops/s (§5.1) and 125.7 ms at 16 closed-loop
clients (§3), on the same cluster. At 95% reads the single node answers half its GETs within
0.5 ms, but its GET p99 (16.0 ms) is its PUT p99 (16.1 ms): the slowest reads waited as long as the
slowest writes.

### 6.3 Concurrency, persists and replication traffic per committed entry

3 nodes, 50% reads, closed loop at 1, 4 and 16 clients; 10 s window after a 2 s warmup; 3 runs
each (commit `86c8518`). The denominator is the commit index's advance over the window — every
committed entry, the election no-op included — and the numerators are the leader's own counters over
the same window.

| Clients | ok/s | Commit (mean) | Leader persists per entry | Leader AppendEntries frames per entry | Leader AppendEntries bytes per entry |
|---|---|---|---|---|---|
| 1 | **116** [111–117] | 15.0 ms | 2.00 | 4.3 | **358** [357–361] |
| 4 | **124** [118–125] | 33.8 ms | 2.00 | 4.4 | **1,014** [1,009–1,017] |
| 16 | **122** [120–122] | 124.9 ms | 2.00 | 4.4 | **3,884** [3,883–3,903] |

- **Concurrency buys no throughput.** Sixteen times the clients gave 5% more operations per
  second, while a write's commit latency grew 8×: the writes queue for one fsync each (§6.1).
- **The leader persists exactly twice per committed entry** in all nine runs, at every
  concurrency (§6.1).
- **The leader resends every unacknowledged entry with every AppendEntries.** Frames per entry stay
  constant, about 2.2 per follower. But bytes per entry
  grow 10.9× from 1 to 16 clients, for identical 100-byte entries. An AppendEntries carries every
  entry from the follower's `nextIndex` to the end of the log. `nextIndex` advances only when a
  response arrives (`raft.sendAppend`), and every proposal broadcasts. So each new write resends
  every write still in flight to every follower: the bytes per entry grow in proportion to the
  writes in flight, and the traffic of a burst with its square.
- On loopback, with 100-byte values, this costs CPU and bandwidth that are not the bottleneck: at
  most 0.13 of a core per node. On a real network, or with large values, the same multiplication
  applies to bandwidth that may be; that was not measured. The code also puts no
  byte budget on an AppendEntries, so a backlog larger than the transport's 16 MiB frame limit would
  be refused by the receiver on every send. This comes from reading the code and was not measured
  here; `docs/ENGINEERING_ROADMAP.md` lists it.

## 7. Resource use

Per-node CPU stayed at or below 0.29 of a core, and peak RSS between 20 and 30 MiB, in every
configuration (§3, §4): the cluster is bound by persistence, not CPU or memory. The largest RSS is
16 groups (30 MiB): each group holds a driver, a log and an in-memory state machine.

## 8. Reproducing

```bash
go build -o bin/dkvlab ./cmd/dkvlab
bin/dkvlab -suite report -runs 3 -out report.json                     # §3, §4, §5.1, §5.3
bin/dkvlab -scenario snapshots -nodes 3 -runs 3 -rate 50 -out snap.json # §5.3, snapshots
for sc in rolling-restart membership; do
  bin/dkvlab -scenario $sc -nodes 3 -runs 3 -rate 50 -out $sc.json      # §5.3
done
for at in 2s 500ms; do
  bin/dkvlab -scenario leader-kill -nodes 3 -runs 10 -duration 10s -warmup 2s -rate 50 \
             -attempt-timeout $at -out lk_$at.json                    # §5.2
done
for c in 1 4 16; do
  bin/dkvlab -scenario steady -nodes 3 -runs 3 -duration 10s -warmup 2s -clients $c -read 50 \
             -out amp_c$c.json                                        # §6.3
done
```

Each file records the environment (CPU, memory, OS, Go, commit, dirty flag), every run's full
result — the load generator's summary and timeline, every action, every node's usage — and the
per-configuration summaries. Timing differs between machines and between runs; the structure of the
results (where the time goes) is what should reproduce.

The lab's own arithmetic is tested, and has teeth: `internal/lab`'s tests check the medians, the
p95, the per-node deltas (a restarted node's counters from zero, the commit index as a difference,
AppendEntries frames by kind), and that every scenario acts inside the measured window and the
snapshot scenario takes snapshots. Mutants 168–172 break each of these and are killed;
`TestLabLeaderKillOnRealProcesses` runs a leader kill on real processes in `make integration`.

## 9. What these numbers do not show

- **One machine.** Nodes and clients share CPU and one SSD; a multi-host cluster has independent
  disks (higher throughput per node) and network round trips (higher latency per commit).
- **Closed-loop throughput is a property of the concurrency** it was measured at (16 clients).
- **Three runs** per configuration give a median and a range, not a distribution; only the
  leader-kill series has enough runs for a p95.
- **Partitions are not in this baseline.** The lab kills, stops and restarts processes and changes
  membership; it does not partition (the integration tests do, through TCP proxies).
- **Small values only** (100 bytes), uniform keys. Large values hit a defect found by the audit that
  accompanied this baseline (`docs/ENGINEERING_ROADMAP.md` §1): a PUT of a value at the documented
  1 MiB maximum forces an election, is lost, and leaves the leader that accepted it unable to
  restart.

## 10. Findings this baseline produced

1. **Group commit is the first performance task** (§6.1, §6.3). Today there is one fsync per write,
   two at the leader, and 16 concurrent clients gain 5% over one. The measured levers:
   - batch the proposals queued at the actor into one Ready, so one persist covers them all;
   - fold the commit-index persist into the next append.

   INV-CR3 must keep holding.
2. **The leader resends every unacknowledged entry on every broadcast** (§6.3). Replication traffic
   per entry grows with the writes in flight, and no message has a byte budget.
3. **A restarted node waits up to 500 ms for its peers' next redial** (§5.2).
4. **A rolling restart leaves 11–15 operations per run unknown** (§5.3) — not yet explained.
5. **The lab itself had two defects:**
   - the snapshot scenario's interval was too large, so it took no snapshot (found by checking the
     scenario's premise in its result);
   - a catch-up wait could silently measure a process start (found in review).

   Both are fixed, and both now leave evidence in every result: snapshots per node, and the index
   each catch-up reached. Mutants 168–172 guard the lab's arithmetic.
