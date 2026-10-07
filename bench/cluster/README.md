# bench/cluster — raw results of the first cluster baseline (#3)

The numbers in [`docs/CLUSTER_BENCHMARKS.md`](../../docs/CLUSTER_BENCHMARKS.md) were read from these files.
Each file is one `dkvlab` invocation, written with `-out`. It records:

- the environment: CPU, memory, OS, Go, git commit, dirty flag;
- every run: the experiment, the load generator's full result with its 100 ms timeline, every action and its settle time, and every node's usage;
- one summary per configuration.

| File | Command (from the repository root, after `go build -o bin/dkvlab ./cmd/dkvlab`) | Commit |
|---|---|---|
| `report.json` | `bin/dkvlab -suite report -runs 3` | `b251527` |
| `snapshots.json` | `bin/dkvlab -scenario snapshots -nodes 3 -runs 3 -rate 50` | `415c61c` |
| `leader-kill-2s.json` | `bin/dkvlab -scenario leader-kill -nodes 3 -runs 10 -duration 10s -warmup 2s -rate 50 -attempt-timeout 2s` | `b251527` |
| `leader-kill-500ms.json` | the same with `-attempt-timeout 500ms` | `b251527` |
| `concurrency-c{1,4,16}.json` | `bin/dkvlab -scenario steady -nodes 3 -runs 3 -duration 10s -warmup 2s -read 50 -clients C` | `86c8518` |
| `rolling-restart.json`, `membership.json` | `bin/dkvlab -scenario rolling-restart -nodes 3 -runs 3 -rate 50` (and `membership`) | `f1b6e8f` |
| `rolling-restart-attempts.json` | `bin/dkvlab -scenario rolling-restart -nodes 3 -runs 3 -rate 50 -read 50 -trace -max-attempts 8,30` — the unknown outcomes' attempt traces (`docs/CLUSTER_BENCHMARKS.md` §11) | `3b5654a` |
| `chaos-seeds-1-10.json` | `bin/dkvlab -scenario chaos -seed 1 -runs 10` — ten chaos runs, every fault kind (`docs/CHAOS.md` §7); the per-run status samples are dropped to keep the file small, everything else is as written | `00a136b` |

The `dkvd` source is identical at the four commits; they differ only in `internal/lab`. Any configuration not named here is the default: 16 clients, 20 s window, 3 s warmup, 50% reads, 10,000 keys, 100-byte values, seed 1, tick 50 ms.

**Not valid:** `report.json`'s `snapshots/raft-3n/r50-rate50`. It used a snapshot interval of 1,000 entries, and a run appends about 600, so it took no snapshot. `snapshots.json` is its replacement, at 100 entries; each node created 5 snapshots per run.

**Superseded:** `report.json`'s `rolling-restart` and `membership` configurations. Their catch-up times were measured by a wait that would have reported the process start had the leader's status read failed (fixed in `f1b6e8f`). `rolling-restart.json` and `membership.json` replace them, and each of their actions records the index it caught up to.

## The restarted leader's catch-up, predicted from the redial interval

After a leader is SIGKILLed, the new leader (n1 or n2 here) is the side of the link to n3 that dials, since the smaller id dials (ADR-014). Its transport retries every 500 ms (`DialRetryInterval`), counting from the moment the connection died, i.e. the kill. n3 restarts 3 s after the election, so the leader's next redial comes `500 − (restart offset mod 500)` ms after the restart.

The table lists every run of both files, sorted by that prediction. The measured catch-up exceeds the prediction by 16–46 ms in every run. That gap is the process start, the handshake and a heartbeat: catch-up is complete when n3 has applied the leader's commit index.

| File | Run | Restarted | New leader | Election (ms) | Restart after kill (ms) | Predicted wait for the leader's redial (ms) | Measured catch-up (ms) | Difference (ms) |
|---|---|---|---|---|---|---|---|---|
| leader-kill-2s | 10 | n3 | n2 | 482 | 3482 | 18 | 51 | +33 |
| leader-kill-2s | 2 | n3 | n2 | 477 | 3478 | 22 | 46 | +24 |
| leader-kill-500ms | 7 | n3 | n2 | 475 | 3475 | 25 | 48 | +23 |
| leader-kill-500ms | 10 | n3 | n2 | 475 | 3475 | 25 | 45 | +20 |
| leader-kill-500ms | 4 | n3 | n2 | 472 | 3472 | 28 | 70 | +42 |
| leader-kill-2s | 5 | n3 | n2 | 466 | 3466 | 34 | 80 | +46 |
| leader-kill-2s | 3 | n3 | n2 | 462 | 3462 | 38 | 84 | +46 |
| leader-kill-500ms | 5 | n3 | n2 | 458 | 3460 | 40 | 66 | +26 |
| leader-kill-2s | 9 | n3 | n2 | 453 | 3454 | 46 | 69 | +23 |
| leader-kill-500ms | 3 | n3 | n1 | 685 | 3687 | 313 | 346 | +33 |
| leader-kill-2s | 6 | n3 | n2 | 617 | 3618 | 382 | 419 | +37 |
| leader-kill-2s | 4 | n3 | n2 | 601 | 3601 | 399 | 425 | +26 |
| leader-kill-2s | 1 | n3 | n2 | 591 | 3592 | 408 | 437 | +29 |
| leader-kill-2s | 8 | n3 | n2 | 574 | 3575 | 425 | 454 | +29 |
| leader-kill-500ms | 8 | n3 | n2 | 570 | 3572 | 428 | 462 | +34 |
| leader-kill-500ms | 9 | n3 | n2 | 569 | 3571 | 429 | 457 | +28 |
| leader-kill-500ms | 2 | n3 | n2 | 566 | 3568 | 432 | 458 | +26 |
| leader-kill-2s | 7 | n3 | n2 | 560 | 3561 | 439 | 465 | +26 |
| leader-kill-500ms | 6 | n3 | n2 | 560 | 3561 | 439 | 455 | +16 |
| leader-kill-500ms | 1 | n3 | n2 | 558 | 3559 | 441 | 463 | +22 |

**`chaos-seeds-1-10.json` predates `22673c0`**, which stopped counting a group's first leader as a leader change. In it, the first `leader` event of each run is that first sighting: a run's leader changes are its `leader` events minus one.

