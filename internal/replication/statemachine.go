package replication

// StateMachine is the seam between a committed log and the thing that applies its
// commands (docs/REPLICATION.md §8). The intended pipeline is:
//
//	replicated log -> committed entries -> state-machine application -> storage
//
// Phase 8 defines this seam but does NOT build the driver that pumps a log's
// Unapplied entries into a StateMachine and advances AppliedIndex, and it wires
// nothing here to the LSM engine (internal/storage). The interface is minimal and
// opaque on purpose: index is the log index being applied, command is the entry's
// opaque bytes. Client request semantics — request IDs, dedup, transactions,
// linearizable reads, forwarding — are Phases 13+, not here.
//
// A correct driver applies committed entries exactly once, in index order, and
// advances the log's AppliedIndex only after Apply returns nil. Phase 8's tests
// drive a trivial in-test StateMachine to show the seam composes; that is the
// extent of what this phase builds on top of it.
type StateMachine interface {
	Apply(index uint64, command []byte) error
}
