// Package raftsim is Quorum's deterministic fault-injection simulator for Raft
// (Phase 10, docs/FAULTS.md, ADR-017).
//
// A Cluster runs N nodes in one goroutine. Each node is the real internal/raft
// core over the real internal/raftlog durable log, stored on its own
// crash-modeling disk (fault.MemFS behind fault.InjectFS); it is driven through
// the real driver ordering (raftnode.DrainReady: persist, then send, then
// advance) and started and restarted through the real startup path
// (raftnode.Recover). Only the clock, the network and the scheduler are
// simulated: ticks are explicit events, the network is an in-memory queue, and
// every scheduling decision is an Event in a script.
//
// Faults are events: drop, duplicate, delay and reorder individual messages;
// partition links (one-way, symmetric, isolate a node); crash a process (the disk
// keeps every written byte) or lose power (only fsynced bytes survive, plus a torn
// prefix); restart through recovery; pause and resume a node; fail or tear a
// durable-log write or fsync; and (Phase 11, docs/CRASH_RECOVERY.md) crash a node
// at an exact crash point — a boundary of the driver's persist → send → advance →
// apply cycle (raftnode.Point) or an I/O boundary of the durable log — with
// RunCrashMatrix doing so at every point a scenario reaches. After every event
// the continuous invariant checks (check.go) run — INV-R1..R10, the Phase 10
// INV-F series, and at every restart the Phase 11 INV-CR series — and the first
// violation stops the run.
//
// Everything is deterministic: no goroutines, no clock, no map-order dependence,
// and all randomness comes from seeds. A run is a pure function of (Config,
// script); a seeded chaos run (Run) is a pure function of (Profile, seed); and
// every run records its script, so any failure is replayable exactly (Replay) and
// can be shrunk (Minimize). The trace's hash is the run's fingerprint.
//
// This is a model of the network, the clock and the disk — not of goroutine
// scheduling, TCP, or real hardware. Those are covered by the driver-level
// (internal/raftnode) and real-process (tests/integration) fault tests.
package raftsim
