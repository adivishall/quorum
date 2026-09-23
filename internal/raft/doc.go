// Package raft implements the deterministic core of Raft consensus (Phase 9,
// docs/RAFT.md, ADR-016), following §5.1–5.4 of Ongaro & Ousterhout, "In Search
// of an Understandable Consensus Algorithm", against docs/DESIGN.md §8.
//
// The core is a pure state machine (ADR-002): it owns no sockets, goroutines,
// wall-clock time, filesystem, or global randomness. The only inputs that change
// state are Tick, Propose, and Step; the only outputs are Ready/Advance (persist
// effects and messages to send) and NextApply/AppliedTo (the apply path).
// Election-timeout randomness comes from an injected rand.Source. Given the same
// initial state, seed, and event sequence, the core produces the same state and
// the same messages in the same order — which is what makes a whole simulated
// cluster replayable from one integer.
//
// The core's log is a replication.Log (Phase 8): Raft drives that primitive
// directly (TruncateAndAppend for AppendEntries, Append for proposals, Commit for
// the commit rule, Unapplied/Apply for the apply path) rather than reimplementing
// a log. Durability (internal/raftlog) and networking (internal/raftnode) live
// outside this package; the core must not depend on either.
//
// What lives here: role/term/vote state, RequestVote and AppendEntries handling,
// log matching with a conflict hint, leader replication, the §5.4.2 commit rule,
// the mandatory election no-op, and the hand-written message codec. What does not:
// I/O, snapshots (Phase 14), dynamic membership (ADR-005), client/API semantics.
package raft
