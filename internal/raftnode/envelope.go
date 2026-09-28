package raftnode

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/replication"
)

// Phase 15: the group envelope (docs/MULTI_RAFT.md §4). Every frame a node sends
// — its Raft messages, its snapshot chunks, its application messages
// (forwarding) — carries the id of the group it belongs to ahead of the payload,
// so one transport connection between two processes serves every group they
// share, and a frame is delivered to its group and no other:
//
//	payload on the wire = uvarint(group) | the group's payload
//
// The integer is canonical; a frame whose envelope does not decode, or names a
// group this process does not run, is dropped by the receiver (logged), never
// guessed at. The transport itself stays ignorant of groups: it carries bytes.

// ErrEnvelope means a frame's group envelope does not decode.
var ErrEnvelope = errors.New("raftnode: malformed group envelope")

// WrapGroup returns payload prefixed with group's envelope.
func WrapGroup(group replication.GroupID, payload []byte) []byte {
	out := make([]byte, 0, binary.MaxVarintLen32+len(payload))
	out = binary.AppendUvarint(out, uint64(group))
	return append(out, payload...)
}

// UnwrapGroup splits a frame payload into its group and the group's payload
// (which aliases b).
func UnwrapGroup(b []byte) (replication.GroupID, []byte, error) {
	g, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrEnvelope, len(b))
	}
	if n != len(binary.AppendUvarint(nil, g)) || g > uint64(^uint32(0)) {
		return 0, nil, fmt.Errorf("%w: non-canonical or oversized group id", ErrEnvelope)
	}
	return replication.GroupID(g), b[n:], nil
}
