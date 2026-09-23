// Package replication defines Quorum's local replicated-log model (Phase 8,
// docs/REPLICATION.md, ADR-015).
//
// It is the primitive that Phase 9's Raft will drive. It is NOT Raft, and it
// makes no distributed claim: a Log on one node knows nothing about a Log on
// another, nothing here replicates a byte across a network, decides when an
// entry may be committed, elects anything, or tolerates a fault. Those are
// Phase 9 and later.
//
// The package owns two things and nothing above them:
//
//   - ReplicaGroup — an immutable, validated handle on one shard's ordered
//     replica set, consumed from internal/routing's declarative metadata
//     (ReplicaGroupsFromRouter) rather than recomputed. Membership is static
//     (ADR-005); validation refuses invalid groups rather than repairing them.
//
//   - Log — a small explicit interface for a local replicated log, with an
//     in-memory reference implementation, MemoryLog. Indexes are 1-based and
//     contiguous (0 is the empty sentinel), terms are non-decreasing, a
//     conflicting suffix can be replaced but a committed entry cannot, and
//     commitIndex/appliedIndex are monotonic watermarks with
//     applied <= commit <= lastIndex enforced.
//
// The boundary, stated once: Phase 8 records that an index IS committed; it does
// not decide that a distributed group is ALLOWED to commit it. See
// docs/REPLICATION.md §11 for the full list of what this phase does not provide.
//
// Following ADR-002, the implementations here hold no locks, start no
// goroutines, and read no clock or randomness: they are pure objects a single
// goroutine drives, exactly like the coming raft.Raft. Entry bytes are copied in
// and out (INV-A1 discipline); the invariants are the INV-P series.
package replication
