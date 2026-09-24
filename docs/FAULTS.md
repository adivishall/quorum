# FAULTS — deterministic fault injection (Phase 10)

Status: **implemented and verified (Phase 10)** — `internal/vfs`, `internal/fault`,
`internal/raftsim`, the `internal/raftnode`/`internal/raftlog`/`cmd/dkvd` hardening, and
`tests/integration/raft_fault_test.go`; ADR-017. This document specifies the fault model, the
architecture that injects it, what was proven — and, as carefully, what was not.

> **Scope boundary, stated once.** Phase 10 proves how the existing Raft core, durable log, driver
> and transport behave under **controlled, injected** failures. It does **not** add end-to-end
> linearizability (Phase 12), a client API, dedup or forwarding (Phases 13/15), snapshots (14),
> dynamic membership (never, ADR-005), or a systematic crash-window harness for the storage engine
> (Phase 11). It proves nothing about real power loss or real hardware.

---

## 1. Why fault injection is its own phase

Happy-path Raft is a small part of the work; the bugs live in the interleavings of failures — a
message delayed past a leadership change, a crash between a write and its fsync, a disk that fails
exactly when a vote must be persisted. Phase 9 proved the safety invariants on schedules a human
wrote. Phase 10 builds the machinery to generate, replay and shrink failure schedules, and injects
failures at the system's **real** boundaries rather than around test doubles. §13 lists what it
found: three real safety/liveness bugs, smaller API and observability bugs, and three gaps in
the Phase 9 test suite.

## 2. The fault model

Every fault is injected at a real boundary, in one or more of three tiers. The deterministic
simulator is replayable from a seed; the other two tiers run real goroutines, real timers and (for
processes) the real OS, so they are not — they assert properties that must hold under any timing.

| Fault | Deterministic simulator (`internal/raftsim`) | Real driver, in-process (`internal/raftnode`) | Real processes (`tests/integration`) |
|---|---|---|---|
| Message drop — by link, type, count, or at random | `drop` event; `TestMessageLoss`; all chaos profiles | `fault.Network` Drop rule | TCP proxy cut |
| Message delay, controlled release | `delay` event (logical steps); `TestDelayedOldTermAppendIsInert` | Hold rule + `Release` | — |
| Message duplication | `dup` event; `TestDuplicatedVoteDoesNotCountTwice`, `TestDuplicatedReplicationTraffic` | Duplicate rule; `TestDuplicatedAndReorderedTrafficAppliesOnce` | — |
| Message reordering | random delivery order (profiles); `TestStaleSuccessDoesNotRegressReplication` | Hold + reversed release | — |
| Partition — one-way, symmetric, isolate, split, heal | `block`/`cut`/`isolate`/`healall`; `TestLeaderIsolatedFromMajority` | `fault.Network` links; `TestIsolatedLeaderCannotCommitAndRejoins` | proxy cut/heal; `TestRealIsolatedLeaderRejoinsAfterPartition` |
| Process crash (SIGKILL) | `crash n process` — the disk keeps every written byte | — | `SIGKILL`; `TestRealLeaderCrashAndReelection`, `TestRealFollowerCrashAndCatchUp` |
| Power loss (software model) | `crash n power T` — fsynced bytes + ≤ T-byte torn prefix survive | `fault.MemFS` durable view at send time; `TestReplyOnlyAfterFsync` | **not testable** |
| Restart from durable state | `restart` through `raftnode.Recover` | `raftnode.Start` on the same log | new process on the same data dir |
| Repeated crash/restart | `TestRepeatedCrashRestart`; `crashes` profile | — | `TestRealRepeatedCrashRestart` |
| Failed write / disk full / torn write | `failpersist write`/`short N`; `disk` profile | `fault.InjectFS`; `TestPersistFailureIsFailStop` | — (no portable way to fail a real process's disk) |
| Failed fsync | `failpersist sync`; `TestVoteNotSentWhenItCannotBePersisted` | `fault.InjectFS`; `TestPersistFailureIsFailStop` | — |
| Slow disk (stalled fsync) | — | gated stall; `TestProposeHonoursContextDuringSlowFsync` | — |
| Paused / frozen node | `pause`/`resume`; `TestPausedLeaderStepsDownOnResume` | — | `SIGSTOP`/`SIGCONT`; `TestRealFrozenLeaderStepsDown` |
| Wedged peer (a send that blocks) | — | Block rule; `TestWedgedPeerDoesNotStallTheLeader` | — |
| Connection loss, redial, flapping | — | — | `TestRealConnectionFlapping` |
| Partition + restart while isolated | `TestPartitionedNodeRestartsWhileIsolated` | — | `TestRealRestartWhileIsolated` |
| Tick control — delayed ticks, election timeouts, heartbeat suppression | explicit per-node `tick` events; a down or paused node's clock does not move | — | — |

Deliberately **not** in the model: Byzantine behaviour; message corruption (the transport's
checksum turns it into a failed connection — Phase 7's INV-T2); clock skew (no correctness
decision reads a clock); and hardware that lies about fsync.

## 3. Architecture — faults live at the boundaries, never in the core

```
                 ┌──────────────────────── internal/raftsim ─────────────────────────┐
  seed/script ──▶│ scheduler · event script · network queue · logical clock · trace   │
                 │ continuous invariant checker · stabilizer · replay · minimizer     │
                 └───────┬──────────────────────┬────────────────────────┬───────────┘
                         │ Tick/Propose/Step     │ raftnode.DrainReady    │ raftnode.Recover
                ┌────────▼────────┐   ┌─────────▼─────────┐   ┌──────────▼─────────┐
                │ internal/raft   │   │ internal/raftnode │   │ internal/raftlog   │
                │ (pure core,     │   │ persist → send →  │   │ durable log        │
                │  unchanged)     │   │ advance ordering  │   │ (format unchanged) │
                └─────────────────┘   └───────────────────┘   └──────────┬─────────┘
                                                                         │ vfs.FS
                                                     ┌───────────────────▼──────────────┐
                                                     │ internal/fault: InjectFS ▸ MemFS │
                                                     └──────────────────────────────────┘
  real driver:   raftnode ─▶ fault.Network (decorates transport.Transport) ─▶ TCP transport
  real process:  dkvd ─▶ TCP ─▶ test-owned TCP proxy ─▶ dkvd      (+ SIGKILL / SIGSTOP)
```

- **`internal/raft` is untouched** — no Phase 10 code. It stays pure: no sockets, filesystem,
  goroutines, clock, or global randomness.
- **`internal/vfs`** is the filesystem seam: `raftlog` does all file I/O through `vfs.FS`;
  production passes nil, which is the real OS.
- **`internal/fault`** holds Raft-agnostic decorators and models: `MemFS`, `InjectFS`, `Links`,
  `Network`. It imports neither `raft` nor `raftlog`.
- **`internal/raftsim`** composes them with the real core, the real durable log, and the driver's
  own `raftnode.DrainReady` (persist, then send, then advance) and `raftnode.Recover` (the
  startup path) — the same two functions the node's actor loop calls, so the simulator exercises
  the production ordering and recovery, not a copy.

## 4. The deterministic simulator

A `Cluster` runs N nodes (n1..nN) in one goroutine. Each node is a real `raft.Raft` over a real
`raftlog.Log` on its own disk (`fault.InjectFS` over `fault.MemFS`). Only three things are
simulated: the **clock** (explicit per-node `tick` events), the **network** (an in-memory queue in
which a message moves only by an explicit event), and the **disk's crash behaviour** (`MemFS`).

**Events** form a script, one per line — `tick n1`, `deliver n1 n2 0`, `drop n1 n2 1`,
`dup n2 n1 0`, `delay n1 n3 0 40`, `block n1 n2`, `unblock n1 n2`, `cut n1 n2`, `isolate n3`,
`healall`, `crash n2 process`, `crash n2 power 13`, `restart n2`, `pause n4`, `resume n4`,
`failpersist n1 sync|write|short 5`, `disarm`, `release`, `propose n1 cmd`, `check-converged`.
A message is addressed by its link and its position among the messages in flight on that link,
so a script stays meaningful when the minimizer deletes events; an event that no longer applies is
skipped (and traced), never an error.

**Impossible states cannot be generated.** A down node has no core: it cannot tick, receive, send
or propose. A paused node neither ticks nor receives (messages to it stay in flight). A message to
a down node, or on a blocked link, is lost when delivered. Messages a node sent before crashing
stay in flight — they were already on the wire. A crash kills every file handle of the dead
process (`fault.ErrCrashed`).

**Seeded chaos.** `Run(profile, seed)` draws 3,000 events from a weighted generator — profiles
`mixed`, `partitions`, `crashes`, `disk`, `messages` — then **stabilizes**: heals every partition,
disarms faults, releases delays, resumes and restarts every node, and runs fair rounds (every node
ticks once, every message is delivered in order) proposing a sentinel until it commits everywhere,
ending with `check-converged`. Every stabilization step is itself an event in the script. Fault
weights are kept low relative to ticks and deliveries so faults interleave with real replication:
a run commits roughly 100–225 commands through 12–57 crashes (40–50% of them power losses),
12–56 persistence failures, or 130–180 each of dropped, duplicated and delayed messages,
depending on the profile (measured over the default seed set). Each run must *prove* it exercised its profile — a commit, an election, and every
fault the profile weights — or it fails; no run passes vacuously.

## 5. Invariants

Phase 10 builds on INV-R1..R10 rather than replacing them. In the simulator they run
**continuously**, after every event or at the exact instant the property is defined:

| Check | When | Invariant |
|---|---|---|
| at most one leader per term — by role observation and by AppendEntries senders | every event, every send | INV-R1 |
| a leader never rewrites or shrinks its own log | every event | INV-R2 |
| log matching across every pair of live nodes | every event | INV-R3 |
| every leader holds every entry committed in an earlier term | every event | INV-R4 |
| every node agrees on each committed index; applied entries agree | every commit, every apply | INV-R5 |
| **fsync form:** at the instant a message is sent, the sender's log holds no un-fsynced byte and its durable term/vote/log equal its in-memory ones; a node never grants its vote in one term to two candidates, across any number of crashes | every send | INV-R6 |
| apply never exceeds commit; apply is in order, once per incarnation | every apply | INV-R7 |
| commit never decreases within a run; recovery never drops below what was applied | every event, every restart | INV-R8 |
| a leader only advances commit to an entry of its own term | every event | INV-R9 |
| a lower-term message changes nothing | every stale delivery | INV-R10 |

New invariants — each a property of the system, not of the harness:

| ID | Invariant | Checked by | Status |
|---|---|---|---|
| INV-F1 | A durable-log write or fsync failure is **fail-stop**: none of the failing Ready's messages is sent, the node processes no further event, the log accepts no further write, `dkvd` exits 1, and the log remains openable (a torn record is truncated on reopen). | `internal/raftlog`: `TestFailedWriteLatchesAndLogStaysRecoverable`, `TestFailedSyncLatches`; `internal/raftnode`: `TestPersistFailureIsFailStop` (disk full, torn write, fsync failure; op log shows no write/fsync after the failure); `cmd/dkvd`: `TestRaftModeExitsNonZeroWhenTheLogFails`; `internal/raftsim`: `TestVoteNotSentWhenItCannotBePersisted`, `TestTornWriteIsTruncatedOnRestart`, `TestDiskFullOnLeaderStopsItAndClusterMovesOn`, the `disk` profile | VERIFIED |
| INV-F2 | A restarted node resumes from exactly its durable state: its recovered term, vote, log and commit equal what it last successfully persisted, extended by at most a prefix of the records of a save that was interrupted — after a process crash and after a modeled power loss — and that recovered state is itself made durable before the node acts on it. | `internal/raftsim`: checked at every restart against an independent record of every Save (continuous), `TestFollowerCrashAndCatchUp`, `TestNoAckOfUnsyncedEntriesAfterFailedFsync`; `internal/raftlog`: `TestOpenMakesRecoveredStateDurable`; the `crashes`/`disk`/`mixed` profiles | VERIFIED (simulation; process kill for real processes) |
| INV-F3 | **Liveness after faults stop** (simulation): once every node is up, every partition healed and the schedule fair, one leader is elected, commits an entry of its own term, and every node's log, commit and applied index converge to it within 400 rounds. | `internal/raftsim`: `check-converged` at the end of every chaos run and fuzz input, `TestRepeatedCrashRestart`, `TestPartitionedNodeRestartsWhileIsolated` | VERIFIED (simulation) |
| INV-F4 | Faults never fabricate or duplicate a command: every applied command was accepted by a leader and occupies exactly one log index, under any mix of drop, duplicate, reorder, delay and crash. | `internal/raftsim`: checked at every apply (continuous); `internal/raftnode`: `TestDuplicatedAndReorderedTrafficAppliesOnce` (real TCP) | VERIFIED |
| INV-F5 | The driver never blocks its Raft actor on the network: a peer whose sends block delays only its own messages; heartbeats and replication to the other peers continue and no election is triggered. | `internal/raftnode`: `TestWedgedPeerDoesNotStallTheLeader` (real TCP) | VERIFIED (driver) |

## 6. Scripted scenarios

Each drives an exact fault sequence and asserts its specific expected behaviour, on top of the
continuous checks (all in `internal/raftsim/scenario_test.go`):

| | Scenario | Test |
|---|---|---|
| A | leader isolated from the majority cannot commit; majority elects a higher-term leader; old leader steps down and its uncommitted entry is replaced | `TestLeaderIsolatedFromMajority` |
| B | follower crash; leader commits with the majority; follower restarts from exactly its durable state and catches up | `TestFollowerCrashAndCatchUp` |
| C | leader crash with power loss after committing; new leader holds the committed entry; old leader rejoins | `TestLeaderCrashAndReelection` |
| D | loss of AppendEntries, RequestVote, and responses; progress stops only while the loss lasts | `TestMessageLoss` |
| E | one vote duplicated six times cannot elect a 5-node candidate; duplicated replication traffic keeps logs identical | `TestDuplicatedVoteDoesNotCountTwice`, `TestDuplicatedReplicationTraffic` |
| F | an older success delivered after a newer one does not regress replication; a delayed old-term AppendEntries is inert | `TestStaleSuccessDoesNotRegressReplication`, `TestDelayedOldTermAppendIsInert` |
| G | a vote that cannot be persisted is never sent; an acknowledged entry survives power loss; no acknowledgement of entries recovered from un-fsynced cache; torn write; disk-full leader | `TestVoteNotSentWhenItCannotBePersisted`, `TestAckedEntrySurvivesPowerLoss`, `TestNoAckOfUnsyncedEntriesAfterFailedFsync`, `TestTornWriteIsTruncatedOnRestart`, `TestDiskFullOnLeaderStopsItAndClusterMovesOn` |
| H | every node crashed in turn (process and power loss), progress between crashes, every committed command present exactly once | `TestRepeatedCrashRestart` |
| I | a node isolated, crashed, restarted while isolated; higher term disrupts but cannot win with a stale log | `TestPartitionedNodeRestartsWhileIsolated` |
| — | a frozen leader still believes it leads until its first exchange, then steps down | `TestPausedLeaderStepsDownOnResume` |
| J | long seeded schedules mixing every fault, every invariant continuous, convergence at the end | `TestRandomizedFaultSchedules` |

## 7. Seeds, traces, replay, minimization

Every simulated run is a pure function of its inputs: `Run` of (profile, seed), `Replay` of
(config, script). There are no goroutines, no clock reads, no map-order dependence; all randomness
comes from seeds — the scheduler's, and each node incarnation's election seed derived from the
config seed. A run records a **trace** (one `s=<step> …` line per event and per consequence:
sends, drops and their reasons, role and term changes, commits, applies, crashes, power losses,
recoveries with the recovered state, persistence failures), whose SHA-256 is the run's
fingerprint, and its full **script**.

- **Same seed, same run** — `TestSameSeedSameTrace`: same (profile, seed) → same script and trace
  hash; replaying the script, or its text form, reproduces the identical trace; different seeds
  differ.
- **Same trace on every platform** — `TestTraceIsPlatformIndependent` pins the hash of
  `mixed/seed=1`; CI (linux/amd64) must reproduce the hash recorded on darwin/arm64. A deliberate
  change to the simulator, the trace format, or Raft's behaviour changes it; re-record it and say
  why in the commit.
- **A failure is reproducible from the report alone.** It prints the seed, the invariant, the
  trace tail, the exact command, and a **minimized** script (greedy delta debugging over the
  recorded events; `TestMinimizeShrinksWhileKeepingTheFailure` checks the minimizer gives a >10×
  shorter, 1-minimal script that still fails).

```bash
go test ./internal/raftsim                                                    # scenarios + default seed set
make faults FAULT_SEEDS=500                                                   # 500 seeds per profile
go test ./internal/raftsim -run 'TestRandomizedFaultSchedules/disk/seed=42$' -raftsim.profile=disk -raftsim.seed=42 -v
go test ./internal/raftsim -run TestReplayScript -raftsim.replay=failure.txt -raftsim.nodes=3 -raftsim.seed=42 -raftsim.verbose -v
go test ./internal/raftsim -run '^$' -fuzz FuzzFaultSchedule -fuzztime 60s -fuzzminimizetime 1s
```

`FuzzFaultSchedule` turns fuzzer bytes into state-valid events and checks every invariant plus
convergence. (Use a short `-fuzzminimizetime`: with Go's default of 60 s the engine spends long
stretches minimizing each newly interesting input and reports 0 execs/sec meanwhile — measured,
not a hang.)

## 8. The real driver under faults (`internal/raftnode`)

These run the real actor loop, the real `raftlog` (on real files via `fault.InjectFS`, or on
`fault.MemFS`), and — where used — the real TCP transport wrapped in `fault.Network`:

- `TestPersistFailureIsFailStop` — disk full, torn write, fsync failure at the instant a vote must
  be persisted: no message sent, `Done`/`Err`, `ErrStopped` proposals, no write or fsync after the
  failure (op log), and a clean restart that answers normally.
- `TestReplyOnlyAfterFsync` — the vote reply leaves only once the vote is in the log's fsynced
  view (INV-R6 in its fsync form on the real driver; Phase 9's ordering test read through the page
  cache and would have passed without the fsync).
- `TestProposeHonoursContextDuringSlowFsync` — a disk stall does not hold `Propose` past its
  deadline; the outcome is then unknown, like any client timeout (`docs/CONSISTENCY.md` C4).
- `TestWedgedPeerDoesNotStallTheLeader` — INV-F5.
- `TestIsolatedLeaderCannotCommitAndRejoins` — scenario A over real TCP; durable logs compared on
  disk afterwards.
- `TestDuplicatedAndReorderedTrafficAppliesOnce` — every message duplicated, AppendEntries held and
  released in reverse; every node applies the same commands, each once.

A monitor polls every node's consistent `Status()` and fails a test on two leaders in one term.

## 9. Real processes (`tests/integration`)

Real `dkvd -raft` processes over real TCP. Faults: `SIGKILL`; restart on the same data directory
and address; `SIGSTOP`/`SIGCONT`; and partitions made by **test-owned TCP proxies** on each
node-to-node link (each pair has exactly one connection, dialed by the lower id, so pointing the
dialer at a proxy puts the whole link under the test's control — no root, no firewall rules, no
change to `dkvd`). `Cut` resets live connections and refuses new ones; a partition that silently
black-holes packets is not modelled.

Every run checks, from outside the processes: no two processes ever announced leadership of one
term (their `raft_leader` events — sound because each comes from one status snapshot, but
leadership shorter than dkvd's 20 ms status poll can be missed, so this monitor can miss a
violation, never invent one); log matching across the durable logs on disk; and that every prefix
known committed before a fault is still at the head of every log afterwards.

Scenarios: `TestRealLeaderCrashAndReelection`, `TestRealFollowerCrashAndCatchUp`,
`TestRealFrozenLeaderStepsDown`, `TestRealIsolatedLeaderRejoinsAfterPartition`,
`TestRealConnectionFlapping`, `TestRealRestartWhileIsolated`, `TestRealRepeatedCrashRestart`,
plus `TestProxyForwardsCutsAndHeals` pinning the proxy. `dkvd` proposes no client commands, so the
committed entries in these runs are election no-ops, each carrying its term — enough structure to
check matching and survival across crashes, not a workload.

## 10. Persistence-failure semantics

- A failed or short write, or a failed fsync, **poisons** the `raftlog.Log`: every later `Save`
  returns the original error (wrapping `raftlog.ErrFailed`) and writes nothing — appending behind
  a partial record would turn a recoverable torn tail into mid-log corruption, and after a failed
  fsync a later "successful" one vouches for nothing.
- The driver **fail-stops**: `DrainReady` returns the error before sending any of that Ready's
  messages; the actor exits and never drives the core again; `Err()` reports the cause and
  `Done()` closes; `Propose` answers with the failure (or `ErrStopped`). `dkvd` logs
  `event=raft_fatal` and exits 1.
- **Recovery is an operator restart.** `raftlog.Open` truncates a torn tail and then **fsyncs the
  file before returning**: whatever the restarted node recovered — including records a failed
  Save wrote into the page cache — is durable before the node acts on it (§13, bug 3).
- What is injected is a **software** I/O error at the `vfs` boundary. It proves the code's
  reaction to an error return; it is not evidence about what a real device does when it fails.
  The design assumes a successful fsync is honest: a kernel that drops dirty pages after a
  write-back error ("fsyncgate") can still lose records the restarted process read back from cache.

## 11. Crash and restart semantics

- **Process crash** (simulator `crash … process`, real `SIGKILL`): every byte a `write` accepted
  survives; nothing volatile does (role, leader, commit beyond the persisted value, match/next
  indexes, votes received, un-sent messages, un-applied entries). Messages already sent stay in
  flight.
- **Power loss** (simulator only): only fsynced bytes survive, plus at most a torn *prefix* of the
  un-synced tail; a file whose creation was never directory-synced disappears.
- **Restart** always goes through the real startup path (`raftnode.Recover` / a new `dkvd`
  process): open the log (truncate a torn tail, fsync), rebuild the in-memory log and the
  (clamped) commit index, and start as a Follower at the recovered term and vote. The applied
  index is volatile and re-applied from the log.
- **Rejoin** is Raft's normal protocol: the node's term is corrected by the first exchange, its
  missing or conflicting entries by AppendEntries. Without PreVote/CheckQuorum a node that
  inflated its term while isolated forces one extra election when it rejoins (it cannot win with a
  stale log — §5.4.1); `TestPartitionedNodeRestartsWhileIsolated` and `TestRealRestartWhileIsolated`
  exercise exactly that.

## 12. Mutation testing

`make mutation` applies each rule-violating edit, runs the named tests, requires them to fail, and
reverts. Phase 10 adds 14 mutants to Phase 9's 8; all 22 are killed.

| Mutant (Phase 10) | Killed by |
|---|---|
| `Save` returns before its fsync | `TestSaveIsDurableWhenItReturns`, `TestReplyOnlyAfterFsync`, `TestAckedEntrySurvivesPowerLoss` |
| `Open` does not make the recovered state durable | `TestOpenMakesRecoveredStateDurable`, `TestNoAckOfUnsyncedEntriesAfterFailedFsync` |
| a durability failure does not latch | `TestFailedWriteLatchesAndLogStaysRecoverable`, `TestFailedSyncLatches` |
| the driver ignores a persistence failure | `TestPersistFailureIsFailStop`, `TestVoteNotSentWhenItCannotBePersisted` |
| the actor sends synchronously (a wedged peer stalls it) | `TestWedgedPeerDoesNotStallTheLeader` |
| `Propose` ignores its deadline | `TestProposeHonoursContextDuringSlowFsync` |
| `dkvd` keeps running after its node fail-stops | `TestRaftModeExitsNonZeroWhenTheLogFails` |
| duplicated vote responses counted as separate votes | `TestDuplicatedVoteDoesNotCountTwice` |
| an older success moves a follower's progress backward | `TestStaleSuccessDoesNotRegressReplication` |
| **harness:** the simulator delivers across a partition | `TestLeaderIsolatedFromMajority` |
| **harness:** the simulator delivers a delayed message early | `TestDelayedOldTermAppendIsInert` |
| **harness:** a simulated restart ignores the node's disk | `TestFollowerCrashAndCatchUp` |
| **harness:** the power-loss model keeps un-fsynced bytes | `TestPowerLossKeepsOnlySyncedBytes` |
| **harness:** the transport decorator lets a dropped message through | `TestDropRuleByKindAndCount` |

The harness mutants exist because a fault injector that silently stopped injecting would make
every test above pass for the wrong reason.

## 13. What Phase 10 found

Bugs, each fixed with a test that fails without the fix:

1. **A persistence failure did not stop the node deterministically.** The driver cancelled its
   context and returned to its `select`, where Go's random choice among ready cases could still
   run another tick, message or proposal — calling `Save` again after a failed (possibly torn)
   write, which appends behind a partial record and makes the log unopenable, or sending after a
   retried fsync. Fixed: the actor exits at once, and `raftlog` latches the failure.
2. **One wedged peer stalled the leader.** Sends ran synchronously in the actor, so a TCP write
   blocked on one peer stopped heartbeats to every peer and cost the leader its term — the
   FAILURE_MODEL §7 "slow node" row. Fixed with per-peer bounded outboxes.
3. **Found by the simulator: a restarted node could act on state that was never durable.** A Save
   writes a record and its fsync fails; the node stops; the page cache still holds the record, so
   the restarted node recovers it — and, holding an entry it already has, acknowledges it without
   a new Save. The leader commits an entry that a power loss on the follower then erases. The
   continuous INV-R6 check caught the acknowledgement leaving with un-fsynced bytes behind it.
   Fixed: `raftlog.Open` fsyncs what it recovered.
4. `Propose` ignored the caller's deadline once the actor had taken the proposal (a slow fsync
   could hang the caller); and it reported acceptance before the entry was persisted. Fixed.
5. `dkvd` built its `raft_leader` events from four separately locked reads (a torn read could
   announce leadership of a term the node never led), kept running as a zombie after its node
   failed, and did not report a follower re-following the same leader in a new term. Fixed.
6. **Found by a flaky real-process restart test: a silent connection was never detected, so a peer
   was never reconnected.** `TestRealRestartWhileIsolated` failed roughly 1 run in 15: after the
   old leader was killed and restarted, a survivor sometimes never re-established a usable link to
   it, so no leader was elected within the timeout. The transport tore a connection down only on a
   read or write *failure*; but a connection established through the test's TCP proxy during the
   restart window could end up delivering nothing while never erroring — writes into it succeeded
   (into a dead socket buffer) and the reader blocked in `Read` forever. Because a registered
   connection also suppresses the dial loop (`hasConn`), the survivor believed it held a live peer
   it could never reach, and its votes vanished. The goroutine dump was decisive: the dial loop was
   parked in `readLoop`→`Read` on a registered connection to the restarted node, and that node's
   Raft log showed sends failing only to the *dead* third node, never to the peer it was wedged
   against. Two fixes: the dialer now waits its retry interval after a connection *dies* (not only
   after a failed dial), so a peer that accepts-and-resets cannot become a reconnect storm; and the
   Raft deployment (`cmd/dkvd`) enables `transport.Config.ReadIdleTimeout` (~120 ticks) so a
   connection that delivers no frame for that long is torn down and redialled. The read-idle-timeout
   is the fix for the root cause — proven by re-running the exact reproduction (the dialer spinning,
   which reliably reproduced the hang) with the timeout enabled: 0 failures where the hang had been
   reliable. TCP keepalive was rejected: a connection stuck in a listener's accept backlog is
   kernel-established, so its probes are answered and it is never seen as dead; only an
   application-level idle deadline catches it. Regression tests:
   `internal/transport`: `TestReaderIdleTimeoutReconnectsASilentConnection`,
   `TestDialerBacksOffWhenPeerKeepsClosingConnections` (both shown to fail without their fix). The
   three sibling restart-through-proxy tests (`TestRealLeaderCrashAndReelection`,
   `TestRealFollowerCrashAndCatchUp`, `TestRealRepeatedCrashRestart`) shared the same exposure and
   are covered by the same fix. A *second*, independent flake in the same test — a wait pinned to a
   leader that a legitimate rejoin-forced re-election had replaced — was also fixed, by waiting for
   the group to converge on *some* leader everyone follows (`waitStable`) rather than for a specific
   leader to keep its term.

Test gaps the Phase 9 suite had, now closed: removing the fsync from `raftlog.Save` passed every
existing test (the ordering test read through the page cache); counting duplicated vote responses
as separate votes, and letting an older AppendEntries success regress a follower's progress, both
passed the entire `internal/raft` suite.

## 14. What is modeled, and what remains untested

- **Real power loss is untested.** `MemFS` is a software model: it assumes a successful fsync is
  honest and models lost un-synced data as a prefix — never as holes, reordered sectors, or bit
  rot. It proves the code *issues* its writes and fsyncs in the right order; it cannot prove what
  a device does.
- **Kernel fsync-error semantics** ("fsyncgate": pages dropped after a write-back error) are not
  modelled and are not defended against beyond fail-stop.
- **Silent network black holes** are not produced in the real-process tier (the proxy resets
  connections). One-way partitions exist only in the simulator and the in-process decorator — TCP
  is bidirectional, so a real one-way cut degenerates into a symmetric one.
- **Goroutine scheduling and TCP** are not covered by the simulator; the driver and process tiers
  cover them without seed-replayability.
- **Disk faults in a real process** (a real `ENOSPC`, a real EIO) are not injected; the fail-stop
  path is proven in-process on the real driver and on `dkvd`'s real raft-mode code.
- **Liveness is only claimed after faults stop** (INV-F3, bounded rounds, simulation). There is no
  PreVote or CheckQuorum, so a rejoining node with an inflated term costs one extra election.
- **The storage engine** (WAL, LSM, compaction) is not hosted by a node yet; its crash windows are
  covered by the Phase 2–4 SIGKILL tests and belong to Phase 11's harness.
- The real-process election-safety monitor samples every 20 ms and can miss a violation that
  lasts less; the simulator's check sees every event.
