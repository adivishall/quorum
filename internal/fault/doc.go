// Package fault holds Quorum's reusable fault-injection primitives (Phase 10,
// docs/FAULTS.md, ADR-017). It is a set of decorators and models placed at the
// system's real failure boundaries; it knows nothing about Raft.
//
//   - MemFS (memfs.go) is a deterministic in-memory filesystem that models what a
//     crash leaves on disk: a process crash keeps every written byte, a modeled
//     power loss keeps only fsynced bytes (plus, optionally, a torn prefix of the
//     rest), and a file's creation is durable only after its directory is synced.
//   - InjectFS (inject.go) decorates any filesystem — MemFS or the real OS — with
//     armed, one-shot I/O faults (a failed or short write, a failed fsync, ...)
//     and records every operation, so a test can prove nothing was written or
//     synced after a failure.
//   - Links (links.go) is the partition model — a set of blocked directed links —
//     shared by the deterministic simulator and the transport decorator.
//   - Network (transport.go) decorates real transport.Transports with partitions
//     and per-message rules (drop, duplicate, hold/release, block).
//
// The deterministic, seed-replayable cluster simulator that composes these with
// the real Raft core, durable log and driver ordering is internal/raftsim.
//
// Every primitive here is deterministic (no clock, no randomness, no map-order
// dependence). The filesystem model is a model: it assumes a successful fsync is
// honest and models lost un-synced data as a prefix, never as holes or reordered
// sectors. It says nothing about real hardware, and real power-loss durability
// remains untested.
//
// Phase 11 (docs/CRASH_RECOVERY.md) adds Injection.At: an observation point at an
// I/O boundary — "before the Nth write/fsync/truncate of this file" — through which
// the simulator crashes a process between two record writes and a real dkvd
// process kills itself there.
package fault
