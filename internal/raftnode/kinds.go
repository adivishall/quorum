package raftnode

import (
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/transport"
)

// kindForType maps a Raft message type to the transport's reserved frame kind
// (ADR-013). This mapping is the only place the two vocabularies meet: the core
// never imports the transport, and the transport never learns what a term means
// (docs/RAFT.md §25). Only the four Phase 9 Raft kinds are activated; snapshot and
// forward kinds stay reserved.
func kindForType(t raft.MessageType) (transport.MsgKind, bool) {
	switch t {
	case raft.MsgVoteRequest:
		return transport.MsgRequestVote, true
	case raft.MsgVoteResponse:
		return transport.MsgRequestVoteResponse, true
	case raft.MsgAppendRequest:
		return transport.MsgAppendEntries, true
	case raft.MsgAppendResponse:
		return transport.MsgAppendEntriesResponse, true
	default:
		return 0, false
	}
}

// typeForKind is the inverse of kindForType. A non-Raft kind (e.g. Probe) returns
// false and is ignored by the receive loop.
func typeForKind(k transport.MsgKind) (raft.MessageType, bool) {
	switch k {
	case transport.MsgRequestVote:
		return raft.MsgVoteRequest, true
	case transport.MsgRequestVoteResponse:
		return raft.MsgVoteResponse, true
	case transport.MsgAppendEntries:
		return raft.MsgAppendRequest, true
	case transport.MsgAppendEntriesResponse:
		return raft.MsgAppendResponse, true
	default:
		return 0, false
	}
}
