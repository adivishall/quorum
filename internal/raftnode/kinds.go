package raftnode

import (
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/transport"
)

// kindForType maps a Raft message type to the transport's reserved frame kind
// (ADR-013). This mapping is the only place the two vocabularies meet: the core
// never imports the transport, and the transport never learns what a term means
// (docs/RAFT.md §25). The four Phase 9 Raft kinds and, since Phase 14,
// InstallSnapshotResponse are Raft messages; the Phase 13 forward kinds are
// application messages (SetAppHandler, SendApp). InstallSnapshot carries a
// snapshot's chunks, not a Raft message: the core's MsgSnapshot is realized as
// a transfer of the published file (Node.startTransfer), and a complete one
// becomes a MsgSnapshot on the receiving side (Snapshots.Receive).
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
	case raft.MsgSnapshotResponse:
		return transport.MsgInstallSnapshotResponse, true
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
	case transport.MsgInstallSnapshotResponse:
		return raft.MsgSnapshotResponse, true
	default:
		return 0, false
	}
}
