# CHAOS — real processes, faults under load, every history checked

Status: **implemented** (`internal/lab/chaos.go`, `dkvlab -scenario chaos`,
`tests/integration/chaos_test.go`).

The deterministic simulator (`docs/FAULTS.md`) checks every fault family against the real Raft core,
log and driver, step by step, from a seed. This is the other half: the same kinds of fault, on real
`dkvd` processes talking TCP on loopback. Clients write, read and delete throughout, and every
client-visible operation is checked for linearizability afterwards. Nothing here is simulated: a
killed node is a `SIGKILL`ed process, and a partition is a TCP link whose connections are reset.

---

## 1. A run

`lab.RunChaos` (or `dkvlab -scenario chaos`):

1. **Starts a cluster** of real `dkvd` processes: `-raft` or `-cluster`, three or more genesis nodes,
   optionally spares to add. Every link between genesis nodes runs through a proxy
   (`internal/netproxy`, §4).
2. **Starts a recorded workload** (`internal/load` with `History` and `Trace`). Clients use the
   session client: identified requests, retried under the same identity, deduplicated at apply.
   The history records, for every operation:
   - its identity, `(group << 48 | session, request id)`;
   - every attempt, with the node it went to and the answer it got;
   - its outcome.

   An operation whose outcome the client cannot know is recorded **Incomplete**, never dropped. Each
   PUT writes a value that names its writer and sequence, so the checker can tell every write apart.
3. **Injects the schedule's faults, one at a time** (§2), each at its planned offset, held, then
   undone.
4. **Records the run** throughout:
   - every node's status, every 250 ms;
   - each group's leader changes;
   - every fault injected, undone, skipped or failed, with the node it hit;
   - each event's position in the history, so the history file shows exactly which operations a fault
     fell between.
5. **Recovers everything** once the load ends: heals every link, resumes every paused process,
   restarts every node that should be running.
6. **Requires convergence:** every group has one leader that every running member follows in the same
   term, every member has applied the leader's commit index, and every node ever started is running.
7. **Checks the history** with `internal/lincheck`. Writes that share an identity are merged into one
   logical operation (`docs/LINEARIZABILITY.md` §15), and a violation is minimized to a
   counterexample.

A run **passes** if:
- the schedule was carried out with no error event (a fault the runner could not inject or undo,
  such as a restart that failed);
- the history is linearizable and was checked, not left over the checker's budget;
- the cluster converged;
- clients completed operations.

Anything else fails the run, and its evidence is written to disk (§5).

## 2. Faults

| Kind | What happens | Undone after its hold by |
|---|---|---|
| `kill` | `SIGKILL`: no shutdown path runs | restart on its data directory |
| `stop` | `SIGTERM`: a clean shutdown | restart |
| `crash` | The node kills itself at the next crash point it reaches. The point is a driver point such as `after-save`: persisted, nothing sent yet. This is dkvd's `-crash-at` seam (`docs/CRASH_RECOVERY.md`), armed by `SIGUSR1` at the fault's start; if the point is not reached during the hold, the node is killed then. | restart |
| `pause` | `SIGSTOP`: the process holds its connections and state but answers nothing | `SIGCONT` |
| `isolate` | every link between the node and the other genesis nodes cut | healing those links |
| `cut` | one link cut | healing it |
| `snapshot` | the group's leader takes a snapshot (admin `snapshot`) | — |
| `add-member` | a spare joins the group: started with `-join`, added as a learner, caught up, promoted | — |

A node fault targets a node drawn from the seed or, half the time, the leader of the fault's group at
the moment the fault starts. The node it actually hit is recorded.

**One impairment at a time.** No fault starts before the previous one has been undone, and none
touches more than one node or link. In a group of three or more voters an impairment always leaves
a quorum, so the run measures recovery and checks safety under faults the group can survive. It
does not measure losing a quorum; `TestDkvctlOnARealCluster` and the partition tests do that
deliberately.

## 3. Reproducibility — what a seed fixes, and what it cannot

`PlanChaos` draws the schedule as a pure function of four things: the seed, the configuration
(window, warmup, quiet tail, mean gap and hold, the kinds allowed), the node ids, and the groups. The
same seed and configuration produce the same schedule, byte for byte. `TestPlanChaosIsAFunctionOfItsSeed`
pins it, and two runs of seed 1 wrote identical `schedule.json` files. A recorded schedule replays
exactly: `dkvlab -scenario chaos -schedule <run>/schedule.json`.

What a seed fixes:
- **the fault schedule:** which faults, when, how long, against which node or role;
- **the workload:** each closed-loop client's sequence of operations, keys and values, drawn from
  `Load.Seed` (which defaults to the chaos seed).

What a seed cannot fix:
- **real time.** Which leader a "leader" fault hits, when an election ends, how the clients'
  operations interleave with each other and with the faults, which operations end unknown. These
  depend on the machine and the scheduler.

So a failing run is diagnosed from its **artifacts**, which record what actually happened. It is
not re-run in the hope of the same interleaving. Replaying its schedule re-creates the same
circumstances: the same faults, at the same offsets, against the same targets or roles, under the
same workload.

## 4. Partitions

With `ClusterConfig.Links`, the lab puts a `netproxy.Proxy` on every link between genesis nodes, on
the dialing side. The transport's smaller id dials (ADR-014), so the dialer's peer address is the
proxy, and the accepter's entry for the dialer is a real address it never dials. Operations:
- `Cluster.Cut(a, b)` / `Heal(a, b)`: one link.
- `Isolate(id)`: every link of `id`.
- `HealAll()`.
- `Cuts()`: which links are cut now.
- `Pause` / `Resume`: freeze and continue a process.
- `Close`: kills every process, then closes every proxy.

The integration tests partition through the same proxy package. Its behaviour is pinned by
`internal/netproxy`'s tests. `TestLinksFollowTheDialer` pins the lab's link bookkeeping without
processes, and `TestLabPartitionsAndPausesARealCluster` partitions and freezes a real leader.

What a partition here is **not**:
- **Not a black hole.** A cut resets the connections and refuses new ones until healed; it never
  silently drops packets. That would need kernel-level packet filtering.
- **Not one-way.** A link is one bidirectional connection.
- **Not between client and node.** Client ports are never proxied, so an isolated node still
  serves its clients. It cannot answer them definitely: as a follower it cannot forward, and as an
  isolated old leader it cannot commit. Those clients see unknown outcomes, and retry elsewhere.
- **Not for spares.** A spare's links are direct: its address enters the group's configuration,
  which every dialer shares, so it cannot be given one proxy per link.

## 5. Evidence

Every failing run writes its artifacts, and `-artifacts DIR` (or `ChaosConfig.Dir`) writes every
run's:

```
chaos.json         configuration, schedule, every event and status sample, the load's result (with
                   every unknown operation's attempt trace), the verdict, final status of every node,
                   each node's resource use
history.txt        the client history; its header names the seed, the schedule and every event at its
                   position in the history. Re-check: go run ./cmd/lincheck history.txt
schedule.json      the schedule alone. Replay: dkvlab -scenario chaos -schedule schedule.json
nodes/<id>.log     every node's output, all its processes in order
links/<a>-<b>.log  each link proxy's activity, connection by connection
```

## 6. Running it

```bash
go run ./cmd/dkvlab -scenario chaos -seed 1 -runs 10 -artifacts chaos/       # seeds 1..10
go run ./cmd/dkvlab -scenario chaos -faults kill,isolate -fault-every 1s -seed 7
go run ./cmd/dkvlab -scenario chaos -schedule chaos/seed-7/schedule.json     # replay seed 7's schedule
make chaos CHAOS_SEEDS=10
```

Chaos defaults apply where a flag is not given:
- 4 clients on 16 keys, so operations contend and the history says something;
- 10% deletes;
- a snapshot every 200 entries, so restarted nodes may catch up by a snapshot;
- one spare, for `add-member`;
- a 20 s window.

`-fault-every`, `-fault-hold` and `-fault-quiet` shape the schedule. `-crash-point` chooses the
point a `crash` fault dies at. The command exits 1 if any run failed.

## 7. What has been run

| Test or run | Setup | Result |
|---|---|---|
| `TestChaosLeaderLossUnderLoadIsLinearizable` | an explicit schedule: the leader killed, later the next leader isolated, both under load | Premises checked: each fault was followed by another node leading, and operations succeeded after the last recovery. Unknown outcomes are kept as Incomplete. |
| `TestChaosSeedsAreLinearizable` | seeds 1–3, every fault kind, 12 s windows, race-built `dkvd` | every run passes |
| `dkvlab -scenario chaos -seed 1 -runs 10` (macOS, Apple M4, default 20 s windows; commit `00a136b`, `bench/cluster/chaos-seeds-1-10.json`) | 4–6 faults per run, every kind drawn at least once across the ten | 10 of 10 pass: 2,032–2,603 operations per run, 1–5 leader changes, 0–4 unknown outcomes (kept as Incomplete), all linearizable and converged |

Mutants 298–301, 305–307 (`scripts/mutation.sh`) break the rules this rests on, and each is killed:
- a Cut that leaves live connections up;
- the proxy on the wrong side of a link;
- faults scheduled to overlap;
- a run that passes despite an error event;
- an unknown outcome recorded as a refusal;
- recorded writes that share one value;
- UNAVAILABLE not counted a refusal.

The first chaos run found a bug in the lab, not in Quorum. A spare added by `add-member` and
restarted after a crash was restarted without `-join`. `dkvd` rightly refused it, since its data
directory says it joined, and the run passed only because convergence then ignored the dead node.
Since then the lab restarts a spare as the joiner it is, convergence requires every started node
to run, and an error event fails the run.

## 8. Limits

- **One machine.** Every process shares a CPU and a disk, and real power loss is not injected (it
  is modeled only in the simulator, `docs/FAULTS.md`).
- **Persistence faults are crashes at driver points.** A real process is never given a failed
  `write` or `fsync`; that fail-stop path is tested in-process (`docs/FAULTS.md` §10). Each run uses
  a single crash point, since every node is started with it.
- **One impairment at a time** (§2). A run never loses its quorum; quorum loss is tested elsewhere,
  deliberately.
- **Histories are bounded by the checker:** a few thousand operations over a handful of keys per
  run. Everything recorded is checked, but a run proves only the histories it produced.
