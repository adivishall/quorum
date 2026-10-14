package kv

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// S4 review: decodeState now delegates to the stream decoder, so a test that
// "the stream decoder agrees with decodeState" proves nothing. The decoder S3
// shipped is frozen here, and the stream decoder is held to it.

// decodeStateS3 is decodeState exactly as it was at S3 (a3c886f), frozen: the
// oracle the S4 stream decoder is held to.
func decodeStateS3(data []byte, l Limits) (*state, error) {
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
			fp := fixedS3(&d, 32)
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

// fixedS3 is the S3 decoder's fixed: exactly n bytes.
func fixedS3(d *decoder, n int) []byte {
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

// sameAsS3 fails unless the stream decoder and the S3 decoder accept data
// alike — refusing it both with ErrSnapshotState, or accepting it both as the
// same state.
func sameAsS3(t *testing.T, what string, data []byte, l Limits) {
	t.Helper()
	old, e1 := decodeStateS3(data, l)
	got, e2 := readState(bytes.NewReader(data), uint64(len(data)), l)
	switch {
	case (e1 == nil) != (e2 == nil):
		t.Fatalf("%s: the S3 decoder says %v, the stream decoder %v", what, e1, e2)
	case e1 != nil && (!errors.Is(e1, ErrSnapshotState) || !errors.Is(e2, ErrSnapshotState)):
		t.Fatalf("%s: refusals not both ErrSnapshotState: %v / %v", what, e1, e2)
	case e1 == nil && (old.applied != got.applied || !reflect.DeepEqual(old.m, got.m) || !reflect.DeepEqual(old.sessions, got.sessions)):
		t.Fatalf("%s: both accept, as different states", what)
	}
}

// TestTheStreamDecoderIsTheS3Decoder: on every corpus state — known-good and
// known-bad — and every truncation and bit flip of a valid one, the stream
// decoder accepts exactly what the S3 decoder accepted, as the same state.
func TestTheStreamDecoderIsTheS3Decoder(t *testing.T) {
	good := validSpec().encode()
	sameAsS3(t, "the valid base state", good, corpusLimits)
	for name, bad := range invalidStates() {
		sameAsS3(t, name, bad.spec.encode(), corpusLimits)
	}
	for n := 0; n <= len(good); n++ {
		sameAsS3(t, fmt.Sprintf("truncated to %d", n), good[:n], corpusLimits)
	}
	for i := range good {
		for bit := 0; bit < 8; bit++ {
			b := append([]byte(nil), good...)
			b[i] ^= 1 << bit
			sameAsS3(t, fmt.Sprintf("bit %d of byte %d", bit, i), b, corpusLimits)
		}
	}
	sameAsS3(t, "a trailing byte", append(append([]byte(nil), good...), 0), corpusLimits)
}

// FuzzStreamDecoderMatchesS3: on any input, the stream decoder and the S3
// decoder agree.
func FuzzStreamDecoderMatchesS3(f *testing.F) {
	f.Add(validSpec().encode())
	for _, bad := range invalidStates() {
		f.Add(bad.spec.encode())
	}
	f.Fuzz(func(t *testing.T, data []byte) { sameAsS3(t, "fuzz input", data, corpusLimits) })
}
