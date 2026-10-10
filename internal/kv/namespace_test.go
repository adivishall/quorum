package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

// The reserved keyspace and the session record (S2, docs/STORAGE_INTEGRATION.md §8.3).

func fpOf(s string) [32]byte {
	return Command{Op: OpPut, Key: []byte("k"), Value: []byte(s)}.Fingerprint()
}

func sampleSession(id, last, acked uint64, rids ...uint64) *session {
	ss := &session{last: last, ackedBelow: acked, results: map[uint64]execution{}}
	for i, rid := range rids {
		ss.results[rid] = execution{fp: fpOf(fmt.Sprint(rid)), index: id + 1 + uint64(i)}
	}
	return ss
}

func sameSession(a, b *session) bool {
	if a.last != b.last || a.ackedBelow != b.ackedBelow || len(a.results) != len(b.results) {
		return false
	}
	for rid, e := range a.results {
		if b.results[rid] != e {
			return false
		}
	}
	return true
}

func TestSessionRecordRoundTrip(t *testing.T) {
	l := Limits{MaxSessions: 4, MaxUnacked: 8}
	for _, tc := range []struct {
		id uint64
		ss *session
	}{
		{1, sampleSession(1, 1, 1)},
		{7, sampleSession(7, 12, 3, 3, 4, 5, 6, 7)},
		{1 << 40, sampleSession(1<<40, 1<<40+9, 1<<20, 1<<20, 1<<21, 1<<62)},
	} {
		b := encodeSession(tc.ss)
		got, err := decodeSession(tc.id, b, l)
		if err != nil {
			t.Fatalf("session %d: %v", tc.id, err)
		}
		if !sameSession(got, tc.ss) {
			t.Fatalf("session %d changed: %+v, want %+v", tc.id, got, tc.ss)
		}
		if again := encodeSession(got); !bytes.Equal(again, b) {
			t.Fatalf("session %d: the encoding is not canonical", tc.id)
		}
	}
	// The record carries the same fields the snapshot format does: a table
	// restored from a snapshot and one rebuilt from its records agree.
	ss := sampleSession(7, 12, 3, 3, 4, 5)
	snap := appendSessions(nil, map[uint64]*session{7: ss})
	d := decoder{b: snap}
	if n := d.uint(); n != 1 {
		t.Fatalf("snapshot holds %d sessions", n)
	}
	if id, last, acked, nres := d.uint(), d.uint(), d.uint(), d.uint(); id != 7 || last != 12 || acked != 3 || nres != 3 {
		t.Fatalf("snapshot session (%d, %d, %d, %d)", id, last, acked, nres)
	}
}

// TestSessionRecordRefusesMalformedRecords: every way a record can fail to be
// one this machine wrote is ErrSessionRecord, never a guess.
func TestSessionRecordRefusesMalformedRecords(t *testing.T) {
	l := Limits{MaxSessions: 4, MaxUnacked: 2}
	good := encodeSession(sampleSession(7, 12, 3, 3, 4))
	header := func(version byte, last, acked uint64, n byte) []byte {
		b := []byte{version}
		b = binary.LittleEndian.AppendUint64(b, last)
		b = binary.LittleEndian.AppendUint64(b, acked)
		return append(b, n)
	}
	result := func(rid, idx uint64) []byte {
		b := binary.LittleEndian.AppendUint64(nil, rid)
		b = binary.LittleEndian.AppendUint64(b, idx)
		return append(b, make([]byte, 32)...)
	}
	cases := map[string][]byte{
		"empty":                    {},
		"short header":             good[:10],
		"no count":                 good[:17],
		"unknown version":          append([]byte{2}, good[1:]...),
		"watermark 0":              header(1, 12, 0, 0),
		"last before id":           header(1, 6, 1, 0),
		"too many results":         append(append(append(header(1, 12, 1, 3), result(1, 8)...), result(2, 9)...), result(3, 10)...),
		"count beyond the bytes":   header(1, 12, 1, 1),
		"trailing byte":            append(append([]byte(nil), good...), 0),
		"truncated result":         good[:len(good)-1],
		"result below watermark":   append(header(1, 12, 3, 1), result(2, 8)...),
		"ids not ascending":        append(append(header(1, 12, 1, 2), result(2, 8)...), result(1, 9)...),
		"index at registration":    append(header(1, 12, 1, 1), result(1, 7)...),
		"index after last":         append(header(1, 12, 1, 1), result(1, 13)...),
		"overlong count":           append(header(1, 12, 1, 0x80), 0x00),
		"count 0 with bytes after": append(header(1, 12, 1, 0), 0x01),
	}
	for name, b := range cases {
		if _, err := decodeSession(7, b, l); !errors.Is(err, ErrSessionRecord) {
			t.Errorf("%s: err = %v, want ErrSessionRecord", name, err)
		}
	}
	if _, err := decodeSession(0, good, l); !errors.Is(err, ErrSessionRecord) {
		t.Errorf("session id 0: err = %v", err)
	}
	if _, err := decodeSession(7, good, l); err != nil {
		t.Fatalf("the good record: %v", err)
	}
}

// TestUserKeysNeverCollideWithSessionRecords: a user key is stored under its
// own tag whatever its bytes — including the exact bytes of a session key, or
// a key beginning with the session tag — so no user key is forbidden and no
// user key resolves as a session record.
func TestUserKeysNeverCollideWithSessionRecords(t *testing.T) {
	sk := sessionKey(7)
	for _, k := range [][]byte{sk, sk[:1], append([]byte{tagSession}, []byte("x")...), {tagUser}, []byte("plain"), make([]byte, MaxKeyLen)} {
		uk := userKey(k)
		if uk[0] != tagUser || !bytes.Equal(uk[1:], k) || len(uk) != len(k)+1 {
			t.Fatalf("userKey(%x) = %x", k, uk)
		}
		if _, isSession := parseSessionKey(uk); isSession {
			t.Fatalf("the user key %x resolves as a session record", k)
		}
	}
	if id, ok := parseSessionKey(sk); !ok || id != 7 {
		t.Fatalf("parseSessionKey(sessionKey(7)) = %d, %v", id, ok)
	}
	for _, k := range [][]byte{sk[:8], append(append([]byte(nil), sk...), 0), {}, nil} {
		if _, ok := parseSessionKey(k); ok {
			t.Fatalf("%x parsed as a session key", k)
		}
	}
	// Records sort by id: the key order is the numeric order.
	if bytes.Compare(sessionKey(255), sessionKey(256)) >= 0 || bytes.Compare(sessionKey(1), sessionKey(1<<40)) >= 0 {
		t.Fatal("session keys do not sort by id")
	}
}
