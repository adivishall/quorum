# LOAD TESTING — how a real cluster is put under load and measured (#2)

Status: **implemented (#2).** `internal/load` and `cmd/dkvload` drive a running cluster through its
ordinary client protocol and record what the clients observe. `docs/CLUSTER_BENCHMARKS.md` holds
the numbers measured with it; this document is how they are measured and what they mean.

---

## 1. What is measured

For each run, from the clients' side:

- **Throughput (achieved):** successes (OK, or NOT_FOUND for a GET) that *completed inside* the
  measured window, per second of it. An open-loop rate the cluster cannot sustain therefore shows
  as its real throughput, not the offered rate.
- **Late operations:** those due in the window that completed after it. Any at all means the
  offered rate exceeded what the cluster sustained, and the latencies include the backlog. The
  summary prints a warning.
- **Latency per operation:** exact nearest-rank percentiles (p50, p90, p95, p99, p99.9, max). No
  histogram, no sampling, no interpolation. Two populations, both over the operations due in the
  window:
  - `latency` — the **successes** (`ok`, `not_found`) only;
  - `latency_all_outcomes` — **every** operation, each measured to its definite answer or to the
    client giving up, with `excluded_from_latency` counting what `latency` leaves out.

  The refused and unknown operations are usually the slowest — they waited out their retries — so
  in a failure scenario the success-only percentiles understate the tail; the summary prints the
  every-outcome rows (`put*`) whenever anything was excluded (`TestFailedOperationsAreNotHiddenFromLatency`).
  Before this distinction (audit), only the successes' durations were recorded, though this
  section said every completed operation counted.
- **Outcomes, by the class a client must act on** (`docs/CLIENT_SEMANTICS.md` §6):
  - `ok`;
  - `not_found`;
  - `refused`, a definite answer that the operation did not take effect;
  - `unknown`, meaning it may or may not have taken effect.
- **A timeline:** successes and failures completed in each 100 ms. The **longest outage** is the
  longest run of whole buckets inside the window in which nothing succeeded.
- **The generator's own CPU time,** so a run whose load generator saturates a core is visible as
  one that measured the generator.

The server's view — elections, persistence and commit latency, lag, CPU per node — is read from
each node's `/metrics` (`docs/OBSERVABILITY.md`) by `dkvlab` (#3).

## 2. The load models

- **Closed loop** (`-rate 0`): each of `-clients` simulated clients issues its next operation when
  the previous one completes. Throughput is what the cluster sustains at that concurrency. Latency
  is measured from each operation's issue. A stall lowers throughput rather than raising latency,
  because no operation is issued during it.
- **Open loop** (`-rate R`): operation *k* is due at `start + k/R`, fixed in advance. A free client
  takes it when it falls due. If none is free, the operation waits, and **its latency is measured
  from when it was due**, so the wait counts. A cluster that stalls for 400 ms shows 400 ms
  latencies instead of a gap in the sample (coordinated omission), and no scheduled operation is
  skipped. `TestOpenLoopCountsAStallAsLatency` checks both against a server that stalls
  mid-run.

## 3. Clients

Each simulated client has its own connection to every endpoint (`kv.Client` answers one request
at a time per connection) and its own session in every group (`kv.Sharded`), so no client
serializes behind another. Every client registers its sessions before the clock starts; no
registration falls inside a measurement. Identified requests are retried under their identity
(up to `-max-attempts`, each `-attempt-timeout`), exactly as the session client does for any
application. `-anonymous` sends one attempt per operation to one endpoint (round-robin) and never
retries, because an unknown anonymous write must not be retried.

Keys are `key%09d` over `-keys` distinct keys, chosen uniformly or by a Zipf law (`-dist zipf
-zipf-s S`). Values are `-value` bytes. The mix is `-read`% GETs, `-delete`% DELETEs, the rest PUTs.
Every random choice comes from `-seed`: open loop draws one sequence, closed loop one per client.
The same configuration and seed issue the same operations; the timing is real and is not
reproducible.

## 4. Reproducing a run

```bash
dkvload -endpoints n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003 \
        -clients 16 -duration 30s -warmup 5s -read 50 -keys 10000 -value 100 -seed 1 -out run.json
```

For a `-cluster` deployment add the cluster's routing (`-shards -rf -nodes`, exactly as every
`dkvd` was started); without it every key is group 0. The JSON records the configuration, the
environment (CPU model and count, memory, OS and version, Go version, git commit and dirty flag)
and every number in the summary.

## 4a. Evidence, not only statistics: histories and traces

Two options make a run leave a record of each operation, not only aggregates. Neither is on the
default path, so a run without them measures exactly what it did before.

**`Config.History`** is a `lincheck.Recorder`. Every operation the run issues, the warmup's
included, is recorded for the linearizability checker:
- the client and the kind, key and value;
- the session identity `(group << 48 | session id, request id)`, since session ids are local to a
  group (`docs/MULTI_RAFT.md` §6);
- every attempt, with the node it went to and the answer it got;
- the outcome. An unknown one is recorded **Incomplete**, never dropped.

Each PUT's value begins `c<client>.<seq>.`, padded to `-value`, so every write is distinguishable.
The chaos runner (`docs/CHAOS.md`) records its workload this way and checks the history.
`TestHistoryRecordsEveryOperation` pins three things against an in-process linearizable server
with sessions:
- every issued operation is in the history;
- writes whose first answer was lost are settled by a retry and recorded once, with both attempts;
- the history checks.

**`Config.Trace`** (`dkvload`/`dkvlab -trace`) keeps, for every operation that ended refused or
unknown, its attempt-by-attempt trace: node, offset, duration, status or error, the leader it
named. At most 5,000 traces are kept per run; the result counts the rest.
`TestTracesKeepEveryUnknownOperation` pins that every unknown outcome is traced and recorded
Incomplete. `lab.ClassifyUnknown` reads a trace's cause:

| Cause | Meaning |
|---|---|
| `unanswered-then-refused` | an attempt went unanswered (the request may have taken effect), and every attempt after it was a definite refusal that reached no leader, until the attempts ran out |
| `unanswered-at-give-up` | the last attempt itself was unanswered |
| `cancelled` | the run's context ended |
| `other` | none of these |

Applied to the rolling restart, this explained its unknown outcomes (`docs/CLUSTER_BENCHMARKS.md`
§11).

## 4b. Experiment matrices (`dkvlab`)

`dkvlab`'s cluster and workload flags take comma lists, and it runs every combination, `-runs`
times each:
- cluster: `-nodes`, `-shards` (groups, in `-mode cluster`);
- workload: `-clients`, `-read`, `-value`, `-max-attempts`.

```bash
dkvlab -scenario steady -nodes 1,3,5 -read 5,50,95 -runs 3 -out matrix.json
dkvlab -scenario steady -mode cluster -shards 1,4,16 -clients 4,16 -value 100,1024 -out groups.json
```

Also: `-delete`, `-keys`, `-dist uniform|zipf`, `-seed`, `-duration`, `-warmup`, `-rate`. The fault
schedule is the scenario: `leader-kill`, `rolling-restart`, `membership`, `snapshots`, or `chaos`
with its seed. A dimension that varies is named in each configuration's name (`/c16`, `/v1024`,
`/a30`).

**A configuration's summary is never success-only.** For each configuration, over its runs
(median, range and spread; p95 at 10 or more runs):
- operations, success rate, and refused and unknown counts;
- every-outcome p50, p95 and p99: each operation measured to its definite answer or to its client
  giving up;
- the success-only percentiles beside them, with how many operations they exclude;
- throughput, the longest outage, each action's settle time (election or catch-up);
- with `-trace`, the unknown outcomes by cause.

The printed table leads with the every-outcome percentiles. The JSON holds every run in full.

## 5. What a result does not mean

- **One machine.** Clients and nodes share one host's CPU and loopback network, so the generator
  competes with the cluster. The generator's CPU is reported. A run in which it approaches a full
  core per client is measuring the machine, not the cluster.
- **Loopback has no network latency.** Replication costs here are fsync and CPU, not round trips.
  A multi-host deployment adds its RTT to every commit.
- **Closed-loop numbers depend on the concurrency** they were measured at, and are not a capacity.
- **Timing varies** from run to run and machine to machine. Reports give the median over repeated
  runs with the spread, never a single run.

## 6. The generator's ceiling (measured)

The generator is measured closed loop against a protocol server that answers at once, over
loopback. Both run in one process on an Apple M4 (10 cores), macOS 26.5.2, Go 1.27.1:
`go test ./internal/load -run TestMeasureGeneratorCeiling -load.measure -v`, 3 s after a 0.5 s
warmup, 50% GETs.

| Clients | ops/s | p50 | p99 | CPU (generator + server) |
|---|---|---|---|---|
| 1 | 44,491 | 19 µs | 71 µs | 1.35 cores |
| 4 | 54,977 | 64 µs | 208 µs | 3.32 cores |
| 16 | 51,077 | 292 µs | 925 µs | 3.95 cores |
| 64 | 43,847 | 1,030 µs | 6,136 µs | 4.39 cores |

That is about 45,000 to 55,000 operations per second with sub-millisecond medians. A cluster run
reaching this range, or a sub-100 µs latency, is measuring the generator and the machine. A
durable cluster fsyncs every commit and runs far below this ceiling (`docs/CLUSTER_BENCHMARKS.md`).
The ceiling itself is one run on one machine: a reference, not a claim.
