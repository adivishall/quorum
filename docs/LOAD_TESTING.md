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
