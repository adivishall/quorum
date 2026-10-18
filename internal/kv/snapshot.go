package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/adivishall/quorum/internal/raftnode"
)

// The key-value store's snapshot state (Phase 14, docs/SNAPSHOTS.md §2–§3): the
// state bytes a snapshot file carries. Everything a later command's decision
// can depend on is here — the key-value map and the whole session table (every
// session's id, recency, watermark and remembered results) — together with the
// applied index it represents and the limits it was built under. The decision
// counters (Stats) are observability, not replicated state, and are not.
//
//	version | applied | maxSessions | maxUnacked
//	nKeys     | (key, value)*                               keys strictly ascending
//	nSessions | (id, last, ackedBelow, nResults, (requestID, index, fingerprint[32])*)*
//	                                                        ids strictly ascending, requestIDs too
//
// Integers are canonical uvarints; keys and values are length-prefixed. Strict
// ordering makes the encoding canonical — one state, one byte string — and
// makes a duplicate key, session or request undecodable.

// SnapshotVersion is the state encoding version.
const SnapshotVersion = 1

// ErrSnapshotState is an invalid snapshot state: malformed, or describing a
// session table no sequence of commands could have produced.
var ErrSnapshotState = errors.New("kv: invalid snapshot state")

// EncodeSnapshot returns the store's state at its applied index, encoded (Phase
// 14). It is called by the driver between applications, so the state is exactly
// the state at that index. (Snapshot, the older method, is a map copy for tests.)
func (s *Store) EncodeSnapshot() (index uint64, data []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied, encodeState(s.applied, s.sessions.limits, s.m, s.sessions.sessions), nil
}

var _ raftnode.SnapshotStateMachine = (*Store)(nil)

// OpenSnapshot returns the store's state at its applied index as a snapshot
// source (raftnode.SnapshotStateMachine, S4): its encoding, in memory — the
// in-memory store holds its state there anyway.
func (s *Store) OpenSnapshot() (raftnode.SnapshotSource, error) {
	index, data, err := s.EncodeSnapshot()
	if err != nil {
		return nil, err
	}
	return raftnode.BytesSource(index, data), nil
}

// ValidateSnapshotFrom reads a snapshot's whole state and validates it as
// ValidateSnapshot does (raftnode.SnapshotStateMachine).
func (s *Store) ValidateSnapshotFrom(index uint64, r io.Reader) error {
	data, err := raftnode.ReadState(r)
	if err != nil {
		return err
	}
	return s.ValidateSnapshot(index, data)
}

// RestoreSnapshotFrom reads a snapshot's whole state and restores it as
// RestoreSnapshot does (raftnode.SnapshotStateMachine).
func (s *Store) RestoreSnapshotFrom(index, term uint64, r io.Reader) error {
	data, err := raftnode.ReadState(r)
	if err != nil {
		return err
	}
	return s.RestoreSnapshot(index, term, data)
}

// encodeState renders a state — the map, the session table, the applied index
// and the limits — in the snapshot format. Both machines encode through it.
func encodeState(applied uint64, l Limits, m map[string][]byte, sessions map[uint64]*session) []byte {
	b := binary.AppendUvarint(nil, SnapshotVersion)
	b = binary.AppendUvarint(b, applied)
	b = binary.AppendUvarint(b, uint64(l.MaxSessions))
	b = binary.AppendUvarint(b, uint64(l.MaxUnacked))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = appendBytes(b, []byte(k))
		b = appendBytes(b, m[k])
	}
	return appendSessions(b, sessions)
}

// appendSessions encodes the session table as the snapshot format carries it:
// ids strictly ascending, each session's record fields, request ids ascending.
func appendSessions(b []byte, sessions map[uint64]*session) []byte {
	ids := make([]uint64, 0, len(sessions))
	for id := range sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		b = appendSession(b, id, sessions[id])
	}
	return b
}

func appendBytes(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

// RestoreSnapshot replaces the store's whole state with a snapshot's, taken at
// index (Phase 14). The state is validated completely first — encoding, bounds,
// ordering, the limits it was built under (they must be this store's: they
// change decisions), and every relation the session table's construction
// guarantees — and the store is unchanged if any fails. The decision counters
// restart at zero. The snapshot's term is not part of the state: the in-memory
// store ignores it, and the LSM machine records it beside the index (S2).
func (s *Store) RestoreSnapshot(index, _ uint64, data []byte) error {
	st, err := s.checkSnapshot(index, data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m, s.sessions.sessions, s.applied, s.sessions.stats = st.m, st.sessions, st.applied, ApplyStats{}
	return nil
}

// ValidateSnapshot reports whether RestoreSnapshot would accept data as the
// state at index, without changing the store. The driver checks a snapshot a
// leader sent before the Raft core ever sees it (docs/SNAPSHOTS.md §7): once
// installed, a snapshot is durable, and one the store then refused would leave
// the node unable to start.
func (s *Store) ValidateSnapshot(index uint64, data []byte) error {
	_, err := s.checkSnapshot(index, data)
	return err
}

func (s *Store) checkSnapshot(index uint64, data []byte) (*state, error) {
	st, err := decodeState(data, s.sessions.limits)
	if err != nil {
		return nil, err
	}
	if st.applied != index {
		return nil, fmt.Errorf("%w: state at index %d, snapshot at %d", ErrSnapshotState, st.applied, index)
	}
	return st, nil
}

type state struct {
	applied  uint64
	m        map[string][]byte
	sessions map[uint64]*session
}

// decodeState decodes and validates snapshot state built under limits l. It is
// the streaming decoder (stateReader, S4) over the bytes: one set of rules,
// whichever way a state arrives.
func decodeState(data []byte, l Limits) (*state, error) {
	return readState(bytes.NewReader(data), uint64(len(data)), l)
}
