# FAULTS — deterministic fault injection (Phase 10)

Status: **specification — Phase 10 in progress** (ADR-017). This document defines the fault
model, the architecture that injects it, and the invariants Phase 10 must establish. The INV-F
series below is `PLANNED` until a test binds it; the phase's final commit records what was
actually verified, and nothing here may be cited as a guarantee before then.

> **Scope boundary, stated once.** Phase 10 proves how the existing Raft core, durable log, driver
> and transport behave under **controlled, injected** failures. It does **not** add end-to-end
> linearizability (Phase 12), a client API, dedup or forwarding (Phases 13/15), snapshots (14),
> dynamic membership (never, ADR-005), or a systematic crash-window harness for the storage engine
> (Phase 11). It does not prove anything about real power loss or real hardware.

---

## 1. Why fault injection is its own phase

Happy-path Raft is a small part of the work; the bugs live in the interleavings of failures —
a message delayed past a leadership change, a crash between a write and its fsync, a disk that
fails exactly when a vote must be persisted. Phase 9 proved the safety invariants on schedules a
human wrote. Phase 10 builds the machinery to generate, replay and shrink failure schedules, and
to inject failures at the system's **real** boundaries rather than around test doubles.

## 2. The fault model

Every fault is injected at a real boundary, in one or more of three tiers:

| Fault | Deterministic simulator | Real driver (in-process) | Real processes |
|---|---|---|---|
| Message drop (by link, kind, count, or at random) | `Drop` event | `fault.Network` Drop rule | TCP proxy cut |
| Message delay / controlled release | `Delay` event (logical steps) | `fault.Network` Hold + `Release` | — |
| Message duplication | `Duplicate` event | `fault.Network` Duplicate rule | — |
| Message reordering | random delivery order; `Release(reverse)` | Hold + reversed release | — |
| Partition: one-way, symmetric, isolate, split, heal | `Block`/`Cut`/`Isolate`/`HealAll` | `fault.Network` links | TCP proxy cut/heal |
| Process crash (SIGKILL) | `Crash` (disk keeps every written byte) | stop + restart on the same disk | `SIGKILL` + restart |
| Power loss (software model) | `Crash … power N` (only fsynced bytes + torn prefix survive) | `fault.MemFS.CrashPowerLoss` | **not testable** |
| Restart from durable state | `Restart` via the real startup path | `raftnode.Start` on the same log | new process, same data dir |
| Failed / torn write, disk full | `FailPersist write`/`short N` | `fault.InjectFS` | — |
| Failed fsync | `FailPersist sync` | `fault.InjectFS` | — |
| Slow disk (stalled fsync) | — | `fault.InjectFS` gated stall | — |
| Paused / frozen node (GC pause) | `Pause`/`Resume` | — | `SIGSTOP`/`SIGCONT` |
| Wedged peer (a send that blocks) | — | `fault.Network` Block rule | — |
| Connection loss / reconnect / flapping | — | — | TCP proxy |
| Tick control (delayed ticks, election timeouts, heartbeat suppression) | explicit `Tick` events per node | — | — |

What the model deliberately does **not** include: Byzantine behaviour, message corruption (the
transport's checksum turns it into a dropped connection — covered by the Phase 7 transport tests),
clock skew (no correctness decision reads a clock), and hardware that lies about fsync.

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
                │  unchanged)     │   │ advance ordering  │   │ (unchanged format) │
                └─────────────────┘   └───────────────────┘   └──────────┬─────────┘
                                                                         │ vfs.FS
                                                     ┌───────────────────▼──────────────┐
                                                     │ internal/fault: InjectFS ▸ MemFS │
                                                     └──────────────────────────────────┘
  real driver:   raftnode ─▶ fault.Network (decorates transport.Transport) ─▶ TCP transport
  real process:  dkvd ─▶ TCP ─▶ test-owned TCP proxy ─▶ dkvd      (+ SIGKILL / SIGSTOP)
```

- **`internal/raft` is untouched.** It stays pure: no sockets, filesystem, goroutines, clock, or
  global randomness, and it contains no fault-injection code.
- **`internal/vfs`** is the filesystem seam: `internal/raftlog` does all file I/O through a small
  `vfs.FS`; production passes nil, which is the real OS.
- **`internal/fault`** holds Raft-agnostic decorators and models: `MemFS` (a crash-consistent
  in-memory filesystem), `InjectFS` (armed one-shot I/O faults over any FS, with an op log),
  `Links` (the partition model), and `Network` (a `transport.Transport` decorator).
- **`internal/raftsim`** composes them with the real core, the real durable log, and the real
  driver ordering and startup path — the two driver functions it needs, `DrainReady` and
  `Recover`, are exported from `internal/raftnode` and used by the node's own actor loop, so the
  simulator exercises the production code path, not a copy of it.

## 4. The deterministic simulator (`internal/raftsim`)

A `Cluster` runs N nodes in one goroutine. Each node is a real `raft.Raft` over a real
`raftlog.Log` on its own disk (`fault.InjectFS` over `fault.MemFS`). Only three things are
simulated: the **clock** (explicit `Tick` events per node — a node's clock does not move unless
it is ticked), the **network** (an in-memory queue; a message is delivered, dropped, duplicated
or delayed only by an explicit event), and the **disk's crash behaviour** (MemFS).

**Events** (`event.go`) form a script, one per line: `tick n1`, `deliver n1 n2 0`,
`drop n1 n2 1`, `dup n2 n1 0`, `delay n1 n3 0 40`, `block n1 n2`, `cut n1 n2`, `isolate n3`,
`healall`, `crash n2 process`, `crash n2 power 13`, `restart n2`, `pause n4`, `resume n4`,
`failpersist n1 sync|write|short 5`, `disarm`, `release`, `propose n1 cmd`, `check-converged`.
A message is addressed by its link and its position among the messages in flight on that link,
so a script stays meaningful when a minimizer deletes events; an event that no longer applies is
skipped (and traced), never an error.

**Impossible states cannot be generated.** A down node has no core: it cannot tick, receive,
send or propose. A paused node neither ticks nor receives (its messages stay in flight). A
message to a down node is lost on delivery; one on a blocked link is lost on delivery. Messages a
node sent before crashing stay in flight (they were already on the wire). A crash invalidates
every file handle of the dead process.

**Crash semantics.** `crash n process` models SIGKILL: every byte a `write` accepted survives
(the kernel outlives the process). `crash n power T` additionally models a power loss: only the
fsynced bytes survive, plus at most `T` bytes of each file's un-synced tail (a torn, prefix-only
append), and a file whose creation was never directory-synced disappears. Restart goes through
`raftnode.Recover` — the node's real startup path — on the surviving disk.

**Persistence faults.** `failpersist` arms a one-shot fault on the node's disk: its next write
fails (disk full), is torn after `N` bytes (a short write), or its next fsync fails. When it fires
inside `DrainReady`, the node fail-stops exactly as the real driver does.

**Seeded chaos.** `Run(profile, seed)` draws events from a weighted generator (profiles: `mixed`,
`partitions`, `crashes`, `disk`, `messages`) seeded by `seed`, then **stabilizes** — heals every
partition, disarms faults, releases delays, resumes and restarts every node, and runs fair rounds
(every node ticks once, every message is delivered in order) until a sentinel command commits
everywhere — and ends with `check-converged`. Every stabilization step is itself an event.

## 5. Invariants

Phase 10 builds on INV-R1..R10 rather than replacing them. In the simulator they are checked
**continuously** — after every event, or at the exact instant the property is defined:

| Check | When | Invariant |
|---|---|---|
| at most one leader per term (role observation and AppendEntries senders) | every event, every send | INV-R1 |
| a leader never rewrites or shrinks its own log | every event | INV-R2 |
| log matching across every pair of live nodes | every event | INV-R3 |
| every leader holds every entry committed in an earlier term | every event | INV-R4 |
| every node agrees on each committed index; applied entries agree | every commit, every apply | INV-R5 |
| **fsync form:** at the instant a message is sent, the sender's log has no un-fsynced byte and its durable term/vote/log equal its in-memory ones; a node never grants its vote in one term to two candidates, across any number of crashes | every send | INV-R6 |
| apply never exceeds commit; apply is in order, once per incarnation | every apply | INV-R7 |
| commit never decreases within a run, and recovery never drops below what was applied | every event, every restart | INV-R8 |
| a leader only advances commit to an entry of its own term | every event | INV-R9 |
| a lower-term message changes nothing | every stale delivery | INV-R10 |

New invariants — each a property of the system, not of the test harness:

| ID | Invariant | Status |
|---|---|---|
| INV-F1 | A durable-log write or fsync failure is **fail-stop**: none of the failing Ready's messages is sent, the node processes no further event, the log accepts no further write, and the log remains openable (a torn record is truncated on reopen). | PLANNED |
| INV-F2 | A restarted node resumes from exactly its durable state: its recovered term, vote, log and commit equal what it last successfully persisted, extended by at most a prefix of the records of a save that was interrupted — after a process crash and after a modeled power loss — and that recovered state is itself made durable before the node acts on it. | PLANNED |
| INV-F3 | **Liveness after faults stop** (simulation): once every node is up, every partition healed and the schedule fair, one leader is elected, commits an entry of its own term, and every node's log, commit and applied index converge to it within a bounded number of rounds. | PLANNED |
| INV-F4 | Faults never fabricate or duplicate a command: every applied command was accepted by a leader, and occupies exactly one log index, under any mix of drop, duplicate, reorder, delay and crash. | PLANNED |
| INV-F5 | The driver never blocks its Raft actor on the network: a peer whose sends block delays only its own messages; heartbeats and replication to the other peers continue. | PLANNED |

## 6. Scripted scenarios

Each scenario drives an exact fault sequence and asserts its specific expected behaviour, on top
of the continuous checks: (A) leader isolated from the majority; (B) follower crash and catch-up;
(C) leader crash and re-election; (D) loss of AppendEntries, RequestVote and responses; (E)
duplicated votes and replication traffic; (F) stale successes and delayed old-term messages; (G)
vote/append persistence failures, torn writes, disk full, acknowledged entries across power loss;
(H) repeated crash/restart; (I) partition + restart while isolated; plus a frozen leader.

## 7. Seeds, traces, replay, minimization

Every simulated run is a pure function of its inputs: `Run` of (profile, seed), `Replay` of
(config, script). There are no goroutines, no clock reads, no map-order dependence, and all
randomness comes from seeds (the scheduler's, and each node incarnation's election seed derived
from the config seed). A run records a **trace** — one `s=<step> …` line per event and per
consequence (sends, drops, role changes, commits, applies, crashes, recoveries, persistence
failures) — whose SHA-256 is the run's fingerprint, and its full **script**. A failure prints the
seed, the invariant, the trace tail, the exact reproduction command, and a **minimized** script
(greedy delta debugging over the recorded events).

## 8. What is modeled and what is not

The simulator models the network, the clock and the disk's crash behaviour; it does not model
goroutine scheduling, TCP, or hardware. `MemFS` assumes a successful fsync is honest and models
lost un-synced data as a prefix — never as holes, reordered sectors, or bit rot — so it can
prove the code *issues* its writes and fsyncs in the right order, but it cannot prove what real
hardware does on power loss. Real power-loss durability remains **untested**, as everywhere in
this project (`docs/FAILURE_MODEL.md` §4).
