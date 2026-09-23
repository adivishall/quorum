package raft

import "errors"

// Sentinel errors. Callers branch with errors.Is, never on strings — the same
// discipline as internal/storage and internal/routing.
var (
	// ErrNotLeader means Propose was called on a node that is not the leader. The
	// caller must find the current leader (LeaderID) and retry there.
	ErrNotLeader = errors.New("raft: not leader")

	// ErrStopped means the core has been closed and rejects further input. (The
	// core itself has no lifecycle; the driver uses this when appropriate.)
	ErrStopped = errors.New("raft: stopped")

	// --- configuration ---

	// ErrNoID means Config.ID was empty.
	ErrNoID = errors.New("raft: empty node id")

	// ErrIDNotInPeers means Config.ID was not one of Config.Peers. The peer set is
	// the whole group membership, including this node (ADR-005).
	ErrIDNotInPeers = errors.New("raft: node id not in peers")

	// ErrDuplicatePeer means Config.Peers listed the same id twice.
	ErrDuplicatePeer = errors.New("raft: duplicate peer id")

	// ErrEmptyPeer means Config.Peers contained an empty id.
	ErrEmptyPeer = errors.New("raft: empty peer id")

	// ErrInvalidTicks means ElectionTicks/HeartbeatTicks were not positive, or the
	// election timeout was not strictly greater than the heartbeat (an election
	// timeout at or below the heartbeat interval prevents stable leadership).
	ErrInvalidTicks = errors.New("raft: invalid tick configuration")

	// ErrNoRand means Config.Rand was nil. Randomness is injected so elections are
	// deterministic under a seed (ADR-002).
	ErrNoRand = errors.New("raft: nil rand source")

	// ErrNoLog means Config.Log was nil.
	ErrNoLog = errors.New("raft: nil log")

	// ErrTermRegression means a recovered/injected currentTerm was below a term
	// already present in the log, which no correct history produces.
	ErrTermRegression = errors.New("raft: current term below log term")

	// --- message decoding (see message.go) ---

	// ErrMalformedMessage means a message payload could not be decoded: a bad
	// length, a truncated field, a bad varint, an oversized count, or trailing
	// bytes. Decoding never panics on hostile input.
	ErrMalformedMessage = errors.New("raft: malformed message")

	// ErrUnknownMessageType means a message payload carried a type byte that is
	// not a defined Raft message type.
	ErrUnknownMessageType = errors.New("raft: unknown message type")
)
