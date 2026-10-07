package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// The LSM machine's engine keyspace (S2, docs/STORAGE_INTEGRATION.md §8.3).
//
// Every key the machine writes to the engine carries a one-byte tag, so the
// user's keys and the machine's own records can never collide: a user key is
// stored under tagUser whatever its bytes, and a session record under
// tagSession. No user key is forbidden — INV-A5, any byte sequence is a valid
// key, is untouched — and nothing above the machine sees a tag. The engine is
// opened with a key limit one byte above MaxKeyLen for it.
const (
	tagUser    byte = 0x01
	tagSession byte = 0x02
)

// userKey is the engine key of a user key.
func userKey(key []byte) []byte {
	return append(append(make([]byte, 0, 1+len(key)), tagUser), key...)
}

// sessionKeyLen is tagSession and the id, big-endian so the records sort by id.
const sessionKeyLen = 9

// sessionKey is the engine key of a session's record.
func sessionKey(id uint64) []byte {
	b := make([]byte, sessionKeyLen)
	b[0] = tagSession
	binary.BigEndian.PutUint64(b[1:], id)
	return b
}

// parseSessionKey reports whether an engine key is a session record's, and
// whose. A user key never is: its tag differs.
func parseSessionKey(k []byte) (uint64, bool) {
	if len(k) != sessionKeyLen || k[0] != tagSession {
		return 0, false
	}
	return binary.BigEndian.Uint64(k[1:]), true
}

// ErrSessionRecord is a session record the machine did not write: malformed,
// or describing a session no sequence of commands could have produced. The
// machine refuses to open on one rather than guess; guessing would make
// replicas diverge.
var ErrSessionRecord = errors.New("kv: invalid session record")

// sessionRecordVersion is the record encoding this build writes and reads.
const sessionRecordVersion = 1

// sessionResultLen is one remembered result: request id, index, fingerprint.
const sessionResultLen = 8 + 8 + 32

// encodeSession renders a session's record: exactly the session-table entry
// the snapshot format carries for it.
//
//	version    u8      = 1
//	last       u64     little-endian: the log index of the session's last command
//	ackedBelow u64
//	n          uvarint
//	n × { requestID u64 | index u64 | fingerprint 32 bytes }   requestIDs strictly ascending
//
// Fixed-width little-endian integers, as the engine's own records use; the
// key is big-endian only so that records sort by id. The encoding is
// canonical: one session, one byte string.
func encodeSession(ss *session) []byte {
	b := make([]byte, 0, 1+16+binary.MaxVarintLen64+len(ss.results)*sessionResultLen)
	b = append(b, sessionRecordVersion)
	b = binary.LittleEndian.AppendUint64(b, ss.last)
	b = binary.LittleEndian.AppendUint64(b, ss.ackedBelow)
	rids := make([]uint64, 0, len(ss.results))
	for rid := range ss.results {
		rids = append(rids, rid)
	}
	sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
	b = binary.AppendUvarint(b, uint64(len(rids)))
	for _, rid := range rids {
		e := ss.results[rid]
		b = binary.LittleEndian.AppendUint64(b, rid)
		b = binary.LittleEndian.AppendUint64(b, e.index)
		b = append(b, e.fp[:]...)
	}
	return b
}

// decodeSession decodes the record of session id, built under limits l,
// strictly: it accepts exactly what encodeSession produces for a session the
// table could hold — the rules the snapshot decoder applies to a session
// (docs/SNAPSHOTS.md §3) — and nothing else.
func decodeSession(id uint64, b []byte, l Limits) (*session, error) {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: session %d: %s", ErrSessionRecord, id, fmt.Sprintf(format, args...))
	}
	if len(b) < 1+16 {
		return nil, bad("%d bytes, shorter than the header", len(b))
	}
	if b[0] != sessionRecordVersion {
		return nil, bad("version %d (this build reads %d)", b[0], sessionRecordVersion)
	}
	last := binary.LittleEndian.Uint64(b[1:9])
	acked := binary.LittleEndian.Uint64(b[9:17])
	p := b[17:]
	n, k := uvarint(p)
	if k <= 0 {
		return nil, bad("unreadable result count")
	}
	p = p[k:]
	switch {
	case id == 0:
		return nil, bad("id 0")
	case last < id:
		return nil, bad("last command %d before its registration at %d", last, id)
	case acked == 0:
		return nil, bad("watermark 0")
	case n > uint64(l.MaxUnacked):
		return nil, bad("%d results, limit %d", n, l.MaxUnacked)
	case n*sessionResultLen != uint64(len(p)):
		return nil, bad("%d results in %d bytes", n, len(p))
	}
	ss := &session{last: last, ackedBelow: acked, results: make(map[uint64]execution, n)}
	var prev uint64
	for i := uint64(0); i < n; i++ {
		rid := binary.LittleEndian.Uint64(p[0:8])
		idx := binary.LittleEndian.Uint64(p[8:16])
		var e execution
		copy(e.fp[:], p[16:sessionResultLen])
		p = p[sessionResultLen:]
		switch {
		case rid < acked:
			return nil, bad("remembers request %d below its watermark %d", rid, acked)
		case i > 0 && rid <= prev:
			return nil, bad("request ids not strictly ascending at %d", rid)
		case idx <= id || idx > last:
			return nil, bad("request %d executed at %d, outside (%d, %d]", rid, idx, id, last)
		}
		prev = rid
		e.index = idx
		ss.results[rid] = e
	}
	return ss, nil
}
