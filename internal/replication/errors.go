package replication

import "errors"

// Sentinel errors for the replication model. Like the routing and storage
// layers, these are the complete, closed set of failure classes; callers branch
// on them with errors.Is, never on error strings. The replication layer never
// repairs an invalid input — an invalid ReplicaGroup or an illegal log operation
// is refused, not silently fixed (docs/REPLICATION.md §2, §6).
var (
	// --- ReplicaGroup construction (INV-P1) ---

	// ErrEmptyGroup means a replica group was constructed with no replicas. A
	// shard with no node to hold it is not a state this package represents.
	ErrEmptyGroup = errors.New("empty replica group")

	// ErrEmptyReplicaID means a replica node id was the empty string. Node ids
	// are opaque but must be non-empty (matches routing.ErrEmptyNodeID).
	ErrEmptyReplicaID = errors.New("empty replica id")

	// ErrDuplicateReplica means the same node id appeared twice in a group. It is
	// never silently collapsed: a caller that meant two replicas and typed one
	// twice has a bug we surface rather than hide.
	ErrDuplicateReplica = errors.New("duplicate replica id")

	// ErrInvalidReplicationFactor means the replication factor was < 1 or did not
	// equal the number of replicas. A group's RF is its replica count; a
	// mismatch is a construction error.
	ErrInvalidReplicationFactor = errors.New("invalid replication factor")

	// --- Log operations (INV-P2..P9) ---

	// ErrOutOfRange means an index or a half-open [lo,hi) range fell outside the
	// log. An invalid range is rejected, never clamped or guessed.
	ErrOutOfRange = errors.New("index or range out of bounds")

	// ErrNonContiguous means an append or suffix replacement would leave a gap or
	// a duplicate index: appended entries must extend the log by exactly one
	// index each, and a replacement may not start past the end of the log.
	ErrNonContiguous = errors.New("non-contiguous log operation")

	// ErrTermRegression means an operation would make terms decrease along the
	// log. Terms are a non-decreasing logical clock (docs/REPLICATION.md §3.3).
	ErrTermRegression = errors.New("term regression")

	// ErrTruncateCommitted means a suffix replacement tried to overwrite a
	// committed entry. Committed entries are never replaced (INV-P3); this is the
	// rule a correct Raft depends on.
	ErrTruncateCommitted = errors.New("cannot replace a committed entry")

	// ErrEmptyBatch means Append or TruncateAndAppend was called with no entries.
	// TruncateAndAppend is a replace-and-append primitive, not a bare truncate,
	// so it requires at least one entry.
	ErrEmptyBatch = errors.New("empty entry batch")

	// ErrCommitRegression means Commit was asked to move commitIndex backward
	// (INV-P5).
	ErrCommitRegression = errors.New("commit index regression")

	// ErrCommitBeyondLog means Commit was asked to advance commitIndex past the
	// last local log index (INV-P6). Phase 8 cannot commit an entry it does not
	// have.
	ErrCommitBeyondLog = errors.New("commit index beyond last log index")

	// ErrAppliedRegression means Apply was asked to move appliedIndex backward
	// (INV-P7).
	ErrAppliedRegression = errors.New("applied index regression")

	// ErrApplyBeyondCommit means Apply was asked to advance appliedIndex past
	// commitIndex — applying an uncommitted entry (INV-P8, INV-P9).
	ErrApplyBeyondCommit = errors.New("applied index beyond commit index")
)
