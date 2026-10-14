package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
)

// The snapshot state, streamed (S4, docs/STORAGE_INTEGRATION.md §10.2): the
// encoding of snapshot.go read one key or session at a time. It is THE
// decoder — decodeState is built on it — so the in-memory store and the LSM
// machine refuse exactly the same states, by the same rules, in the same
// order, whether a state arrives as bytes or as a stream too large to hold.

// stateReader decodes a snapshot state from a stream: the header at
// construction, then the keys (nextKey), then the sessions (nextSession), then
// end, which requires nothing to follow. The first failure latches: a rule the
// state breaks is ErrSnapshotState; a stream that ends early is ErrSnapshotState
// (truncated); a failure of the stream itself — a snapshot file whose checksum
// does not match, say — is returned as the stream reported it.
type stateReader struct {
	r    *bufio.Reader
	size uint64 // the state's length (math.MaxUint64: unknown)
	n    uint64 // bytes consumed
	l    Limits
	err  error

	applied   uint64
	nKeys     uint64
	keysRead  uint64
	prev      []byte
	sessions  bool // the session count has been read
	nSessions uint64
	sessRead  uint64
	lasts     map[uint64]bool
	prevID    uint64
}

func invalidState(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrSnapshotState, fmt.Sprintf(format, args...))
}

// stateSize is the length of a state stream when the reader knows it: a
// snapshot file's (snapshot.Reader.DataLen) or a byte slice's. Unknown, the
// per-item bounds still hold; only the cheap key-count sanity check is lost.
func stateSize(r io.Reader) uint64 {
	switch x := r.(type) {
	case interface{ DataLen() uint64 }:
		return x.DataLen()
	case interface{ Len() int }:
		return uint64(x.Len())
	}
	return math.MaxUint64
}

// newStateReader reads and checks a state's header: the version, the applied
// index, the limits it was built under (they must be l: they change decisions)
// and the key count.
func newStateReader(r io.Reader, size uint64, l Limits) (*stateReader, error) {
	s := &stateReader{r: bufio.NewReaderSize(r, 64<<10), size: size, l: l, lasts: map[uint64]bool{}}
	if v := s.uint(); s.err == nil && v != SnapshotVersion {
		return nil, invalidState("version %d (this build reads %d)", v, SnapshotVersion)
	}
	s.applied = s.uint()
	maxS, maxU := s.uint(), s.uint()
	if s.err == nil && (maxS != uint64(l.MaxSessions) || maxU != uint64(l.MaxUnacked)) {
		return nil, invalidState("built under limits %d sessions x %d results; this store runs %d x %d", maxS, maxU, l.MaxSessions, l.MaxUnacked)
	}
	s.nKeys = s.uint()
	if s.err == nil && s.nKeys > s.size {
		return nil, invalidState("%d keys in %d bytes", s.nKeys, s.size)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s, nil
}

// fail latches a decode failure: the stream's own error as it is; running out
// of bytes, or a malformed field, as an invalid state.
func (s *stateReader) fail(err error) {
	if s.err != nil {
		return
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		s.err = invalidState("%v: truncated", ErrProtocol)
		return
	}
	if errors.Is(err, ErrProtocol) || errors.Is(err, ErrMalformedCommand) {
		s.err = fmt.Errorf("%w: %v", ErrSnapshotState, err)
		return
	}
	s.err = err
}

// uint reads a canonical uvarint.
func (s *stateReader) uint() uint64 {
	if s.err != nil {
		return 0
	}
	var buf [binary.MaxVarintLen64]byte
	for i := 0; i < len(buf); i++ {
		b, err := s.r.ReadByte()
		if err != nil {
			s.fail(err)
			return 0
		}
		s.n++
		buf[i] = b
		if b < 0x80 {
			v, k := uvarint(buf[:i+1])
			if k <= 0 {
				s.fail(fmt.Errorf("%w: bad integer", ErrProtocol))
				return 0
			}
			return v
		}
	}
	s.fail(fmt.Errorf("%w: bad integer", ErrProtocol))
	return 0
}

// bytes reads a length-prefixed byte string of at most max bytes, which the
// stream must still hold.
func (s *stateReader) bytes(max int) []byte {
	l := s.uint()
	if s.err != nil {
		return nil
	}
	if l > uint64(max) || l > s.size-s.n {
		s.fail(fmt.Errorf("%w: %w: length %d out of range", ErrProtocol, ErrMalformedCommand, l))
		return nil
	}
	return s.fixed(int(l))
}

// fixed reads exactly n bytes.
func (s *stateReader) fixed(n int) []byte {
	if s.err != nil {
		return nil
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(s.r, out); err != nil {
		s.fail(err)
		return nil
	}
	s.n += uint64(n)
	return out
}

// nextKey returns the next key and its value, both fresh. ok is false once the
// keys are done — or on a failure (check err).
func (s *stateReader) nextKey() (k, v []byte, ok bool) {
	if s.err != nil || s.keysRead >= s.nKeys {
		return nil, nil, false
	}
	k = s.bytes(MaxKeyLen)
	v = s.bytes(MaxValueLen)
	if s.err != nil {
		return nil, nil, false
	}
	if len(k) == 0 {
		s.err = invalidState("an empty key")
		return nil, nil, false
	}
	if s.keysRead > 0 && bytes.Compare(s.prev, k) >= 0 {
		s.err = invalidState("keys not strictly ascending at %q", k)
		return nil, nil, false
	}
	s.keysRead++
	s.prev = append(s.prev[:0], k...)
	if v == nil {
		v = []byte{} // a present empty value stays present
	}
	return k, v, true
}

// nextSession returns the next session. ok is false once the sessions are
// done — or on a failure (check err). It is called after the keys.
func (s *stateReader) nextSession() (id uint64, ss *session, ok bool) {
	if s.err != nil {
		return 0, nil, false
	}
	if s.keysRead < s.nKeys {
		s.err = fmt.Errorf("kv: the state's sessions read before its %d keys", s.nKeys)
		return 0, nil, false
	}
	if !s.sessions {
		s.sessions = true
		s.nSessions = s.uint()
		if s.err == nil && s.nSessions > uint64(s.l.MaxSessions) {
			s.err = invalidState("%d sessions, limit %d", s.nSessions, s.l.MaxSessions)
		}
	}
	if s.err != nil || s.sessRead >= s.nSessions {
		return 0, nil, false
	}
	id, last, acked, nRes := s.uint(), s.uint(), s.uint(), s.uint()
	if s.err != nil {
		return 0, nil, false
	}
	switch {
	case id == 0 || id > s.applied:
		s.err = invalidState("session id %d outside [1, %d]", id, s.applied)
	case s.sessRead > 0 && id <= s.prevID:
		s.err = invalidState("session ids not strictly ascending at %d", id)
	case last < id || last > s.applied:
		s.err = invalidState("session %d last command %d outside [%d, %d]", id, last, id, s.applied)
	case s.lasts[last]:
		s.err = invalidState("two sessions last touched at index %d", last)
	case acked == 0:
		s.err = invalidState("session %d has watermark 0", id)
	case nRes > uint64(s.l.MaxUnacked):
		s.err = invalidState("session %d holds %d results, limit %d", id, nRes, s.l.MaxUnacked)
	}
	if s.err != nil {
		return 0, nil, false
	}
	s.prevID = id
	s.lasts[last] = true
	ss = &session{last: last, ackedBelow: acked, results: map[uint64]execution{}}
	var prevRID uint64
	for j := uint64(0); j < nRes; j++ {
		rid, idx := s.uint(), s.uint()
		fp := s.fixed(32)
		if s.err != nil {
			return 0, nil, false
		}
		switch {
		case rid < acked:
			s.err = invalidState("session %d remembers request %d below its watermark %d", id, rid, acked)
		case j > 0 && rid <= prevRID:
			s.err = invalidState("session %d request ids not strictly ascending at %d", id, rid)
		case idx <= id || idx > last:
			s.err = invalidState("session %d request %d executed at %d, outside (%d, %d]", id, rid, idx, id, last)
		}
		if s.err != nil {
			return 0, nil, false
		}
		prevRID = rid
		var e execution
		copy(e.fp[:], fp)
		e.index = idx
		ss.results[rid] = e
	}
	s.sessRead++
	return id, ss, true
}

// end reports whether the state was read whole and nothing follows it: the
// stream must report its end — for a snapshot file, only once the file's
// checksum, length and footer are proven.
func (s *stateReader) end() error {
	if s.err != nil {
		return s.err
	}
	if s.keysRead < s.nKeys || !s.sessions || s.sessRead < s.nSessions {
		return fmt.Errorf("kv: the state was not read to its end")
	}
	_, err := s.r.ReadByte()
	switch {
	case err == nil:
		s.err = invalidState("%v: trailing bytes", ErrProtocol)
	case errors.Is(err, io.EOF):
		return nil
	default:
		s.err = err
	}
	return s.err
}

// readState decodes a whole state from a stream into memory (the in-memory
// store's restore, and every byte-slice caller through decodeState).
func readState(r io.Reader, size uint64, l Limits) (*state, error) {
	sr, err := newStateReader(r, size, l)
	if err != nil {
		return nil, err
	}
	st := &state{applied: sr.applied, m: map[string][]byte{}, sessions: map[uint64]*session{}}
	for {
		k, v, ok := sr.nextKey()
		if !ok {
			break
		}
		st.m[string(k)] = v
	}
	for {
		id, ss, ok := sr.nextSession()
		if !ok {
			break
		}
		st.sessions[id] = ss
	}
	if err := sr.end(); err != nil {
		return nil, err
	}
	return st, nil
}

// appendSession encodes one session as the snapshot format carries it: its
// id, last command, watermark and results, request ids ascending.
func appendSession(b []byte, id uint64, ss *session) []byte {
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
	return b
}
