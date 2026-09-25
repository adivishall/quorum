package raft

import "github.com/adivishall/quorum/internal/replication"

// NodeID and Entry are the identity and log-entry types shared with the routing
// and replication layers, re-exported so callers speak one set of types.
type (
	NodeID = replication.NodeID
	Entry  = replication.Entry
)

// Role is a node's Raft role. It is an explicit type, never a string, so an
// invalid role cannot be constructed and a switch is exhaustive.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// String renders a role for logs and test failures.
func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Role(?)"
	}
}

// HardState is the durable Raft state that must survive a restart: the current
// term, who this node voted for in that term (empty = nobody), and the commit
// index. commitIndex is a persisted optimization only — recovery never trusts it
// beyond min(persisted, lastIndex) and correctness does not depend on it
// (docs/DESIGN.md §8.1, ADR-016).
type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

// MessageType identifies a Raft RPC. The driver maps these to the transport's
// reserved frame kinds (ADR-013); the core never imports the transport.
type MessageType uint8

const (
	MsgVoteRequest MessageType = iota + 1
	MsgVoteResponse
	MsgAppendRequest
	MsgAppendResponse
)

// String renders a message type.
func (t MessageType) String() string {
	switch t {
	case MsgVoteRequest:
		return "VoteRequest"
	case MsgVoteResponse:
		return "VoteResponse"
	case MsgAppendRequest:
		return "AppendRequest"
	case MsgAppendResponse:
		return "AppendResponse"
	default:
		return "MessageType(?)"
	}
}

// Message is a single Raft RPC or its response. One struct carries the union of
// fields for all four types; a decoder only reads the fields its type defines
// (message.go). From/To/Term are common to all. Using one struct keeps the codec
// and the Step switch simple and keeps message ordering deterministic.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term uint64

	// RequestVote (candidate log position).
	LastLogIndex uint64
	LastLogTerm  uint64

	// VoteResponse.
	VoteGranted bool

	// AppendEntries.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// AppendResponse.
	Success       bool
	ConflictTerm  uint64
	ConflictIndex uint64
	MatchIndex    uint64 // on success, the last index the follower now matches

	// Seq is the leader's heartbeat sequence (Phase 12, ReadIndex): every
	// AppendEntries a leader sends carries the sequence current at that moment,
	// and every AppendEntriesResponse echoes the sequence of the request it
	// answers. A response with Seq >= s therefore proves the follower still
	// accepted this leader AFTER the leader issued sequence s — the "confirm
	// leadership with a quorum round after the read arrived" step of
	// docs/DESIGN.md §8.5, without a separate heartbeat message type.
	Seq uint64
}

// ReadState is a confirmed ReadIndex request (Phase 12, docs/DESIGN.md §8.5): once
// the driver has applied entries through Index it may serve the read that
// registered as ID from the state machine, and that read is linearizable with
// respect to every write acknowledged before it was registered.
type ReadState struct {
	ID    uint64
	Index uint64
}

// quorum returns the majority size for a group of n members: floor(n/2)+1. For
// n==1 this is 1, so a single-node group commits on its own (ADR-016).
func quorum(n int) int { return n/2 + 1 }
