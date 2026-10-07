package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
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
	b := binary.AppendUvarint(nil, SnapshotVersion)
	b = binary.AppendUvarint(b, s.applied)
	b = binary.AppendUvarint(b, uint64(s.limits.MaxSessions))
	b = binary.AppendUvarint(b, uint64(s.limits.MaxUnacked))
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = appendBytes(b, []byte(k))
		b = appendBytes(b, s.m[k])
	}
	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		ss := s.sessions[id]
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, ss.last)
		b = binary.AppendUvarint(b, ss.ackedBelow)
		rids := make([]uint64, 0, len(ss.results))
		for rid := range ss.results {
			rids = append(rids, rid)
		}
		sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
		b = binary.AppendUvarint(b, uint64(len(rids)))
		for _, rid := range rids {
			e := ss.results[rid]
			b = binary.AppendUvarint(b, rid)
			b = binary.AppendUvarint(b, e.index)
			b = append(b, e.fp[:]...)
		}
	}
	return s.applied, b, nil
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
	s.m, s.sessions, s.applied, s.stats = st.m, st.sessions, st.applied, ApplyStats{}
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
	st, err := decodeState(data, s.limits)
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

// decodeState decodes and validates snapshot state built under limits l.
func decodeState(data []byte, l Limits) (*state, error) {
	d := decoder{b: data}
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrSnapshotState, fmt.Sprintf(format, args...))
	}
	if v := d.uint(); d.err == nil && v != SnapshotVersion {
		return nil, bad("version %d (this build reads %d)", v, SnapshotVersion)
	}
	st := &state{applied: d.uint(), m: map[string][]byte{}, sessions: map[uint64]*session{}}
	maxS, maxU := d.uint(), d.uint()
	if d.err == nil && (maxS != uint64(l.MaxSessions) || maxU != uint64(l.MaxUnacked)) {
		return nil, bad("built under limits %d sessions x %d results; this store runs %d x %d", maxS, maxU, l.MaxSessions, l.MaxUnacked)
	}
	nKeys := d.uint()
	if d.err == nil && nKeys > uint64(len(data)) {
		return nil, bad("%d keys in %d bytes", nKeys, len(data))
	}
	var prev []byte
	for i := uint64(0); i < nKeys && d.err == nil; i++ {
		k := d.bytes(MaxKeyLen)
		v := d.bytes(MaxValueLen)
		if d.err != nil {
			break
		}
		if len(k) == 0 {
			return nil, bad("an empty key")
		}
		if i > 0 && bytes.Compare(prev, k) >= 0 {
			return nil, bad("keys not strictly ascending at %q", k)
		}
		prev = k
		st.m[string(k)] = append([]byte{}, v...) // a present empty value stays present
	}
	nSessions := d.uint()
	if d.err == nil && nSessions > uint64(l.MaxSessions) {
		return nil, bad("%d sessions, limit %d", nSessions, l.MaxSessions)
	}
	lasts := map[uint64]bool{}
	var prevID uint64
	for i := uint64(0); i < nSessions && d.err == nil; i++ {
		id, last, acked, nRes := d.uint(), d.uint(), d.uint(), d.uint()
		if d.err != nil {
			break
		}
		switch {
		case id == 0 || id > st.applied:
			return nil, bad("session id %d outside [1, %d]", id, st.applied)
		case i > 0 && id <= prevID:
			return nil, bad("session ids not strictly ascending at %d", id)
		case last < id || last > st.applied:
			return nil, bad("session %d last command %d outside [%d, %d]", id, last, id, st.applied)
		case lasts[last]:
			return nil, bad("two sessions last touched at index %d", last)
		case acked == 0:
			return nil, bad("session %d has watermark 0", id)
		case nRes > uint64(l.MaxUnacked):
			return nil, bad("session %d holds %d results, limit %d", id, nRes, l.MaxUnacked)
		}
		prevID = id
		lasts[last] = true
		ss := &session{last: last, ackedBelow: acked, results: map[uint64]execution{}}
		var prevRID uint64
		for j := uint64(0); j < nRes && d.err == nil; j++ {
			rid, idx := d.uint(), d.uint()
			fp := d.fixed(32)
			if d.err != nil {
				break
			}
			switch {
			case rid < acked:
				return nil, bad("session %d remembers request %d below its watermark %d", id, rid, acked)
			case j > 0 && rid <= prevRID:
				return nil, bad("session %d request ids not strictly ascending at %d", id, rid)
			case idx <= id || idx > last:
				return nil, bad("session %d request %d executed at %d, outside (%d, %d]", id, rid, idx, id, last)
			}
			prevRID = rid
			var e execution
			copy(e.fp[:], fp)
			e.index = idx
			ss.results[rid] = e
		}
		st.sessions[id] = ss
	}
	if err := d.done(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSnapshotState, err)
	}
	return st, nil
}

// fixed reads exactly n bytes.
func (d *decoder) fixed(n int) []byte {
	if d.err != nil {
		return nil
	}
	if len(d.b)-d.i < n {
		d.err = fmt.Errorf("%w: truncated", ErrProtocol)
		return nil
	}
	out := d.b[d.i : d.i+n]
	d.i += n
	return out
}
