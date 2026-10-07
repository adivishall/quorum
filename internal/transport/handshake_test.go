package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func helloOf(id NodeID) hello { return hello{id: id, cluster: "c1", digest: []byte{1, 2, 3}} }

func TestHandshakeRoundTrip(t *testing.T) {
	for _, h := range []hello{
		helloOf("n0"),
		{id: "node-with-a-longer-name"},
		{id: NodeID(bytes.Repeat([]byte("z"), MaxNodeIDLen)), cluster: string(bytes.Repeat([]byte("c"), MaxClusterIDLen)), digest: bytes.Repeat([]byte{7}, MaxDigestLen)},
	} {
		var buf bytes.Buffer
		if err := writeHandshake(&buf, h); err != nil {
			t.Fatalf("writeHandshake(%q): %v", h.id, err)
		}
		got, err := readHandshake(&buf)
		if err != nil {
			t.Fatalf("readHandshake: %v", err)
		}
		if got.id != h.id || got.cluster != h.cluster || !bytes.Equal(got.digest, h.digest) {
			t.Errorf("round trip = %+v, want %+v", got, h)
		}
	}
	for status := statusAccepted; status <= statusWrongDirection; status++ {
		var buf bytes.Buffer
		if err := writeReply(&buf, status, helloOf("n1")); err != nil {
			t.Fatal(err)
		}
		s, got, err := readReply(&buf)
		if err != nil || s != status || got.id != "n1" || got.cluster != "c1" {
			t.Fatalf("reply %d round trip: %d %+v %v", status, s, got, err)
		}
	}
}

func TestWriteHandshakeRejectsBadLocalID(t *testing.T) {
	var buf bytes.Buffer
	if err := writeHandshake(&buf, hello{}); !errors.Is(err, ErrEmptyNodeID) {
		t.Errorf("empty id: err = %v, want ErrEmptyNodeID", err)
	}
	for _, h := range []hello{
		{id: NodeID(bytes.Repeat([]byte("z"), MaxNodeIDLen+1))},
		{id: "n", cluster: string(bytes.Repeat([]byte("c"), MaxClusterIDLen+1))},
		{id: "n", digest: bytes.Repeat([]byte{1}, MaxDigestLen+1)},
	} {
		if err := writeHandshake(&buf, h); !errors.Is(err, ErrHandshakeTooLarge) {
			t.Errorf("oversized %+v: err = %v, want ErrHandshakeTooLarge", h, err)
		}
	}
}

func TestHandshakeBadMagicRejected(t *testing.T) {
	buf := bytes.NewReader([]byte("XXXX\x02\x00\x00\x00\x02\x00nX\x00\x00\x00"))
	if _, err := readHandshake(buf); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("bad magic: err = %v, want ErrBadMagic", err)
	}
}

// header writes magic and a version.
func header(version uint32) *bytes.Buffer {
	var buf bytes.Buffer
	buf.WriteString(handshakeMagic)
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], version)
	buf.Write(v[:])
	return &buf
}

// TestHandshakeVersionMismatchRejected: a version-1 peer (one-way handshake,
// no cluster) and a future version are refused, never misparsed.
func TestHandshakeVersionMismatchRejected(t *testing.T) {
	for _, v := range []uint32{1, ProtocolVersion + 1} {
		buf := header(v)
		buf.Write([]byte{2, 0, 'n', '1'})
		if _, err := readHandshake(buf); !errors.Is(err, ErrVersionMismatch) {
			t.Fatalf("version %d: err = %v, want ErrVersionMismatch", v, err)
		}
	}
}

func TestHandshakeEmptyIDRejected(t *testing.T) {
	buf := header(ProtocolVersion)
	buf.Write([]byte{0, 0, 0, 0, 0}) // idLen = 0, empty cluster, empty digest
	if _, err := readHandshake(buf); !errors.Is(err, ErrEmptyNodeID) {
		t.Fatalf("empty id: err = %v, want ErrEmptyNodeID", err)
	}
}

func TestHandshakeOversizedFieldsRejected(t *testing.T) {
	var l [2]byte
	binary.LittleEndian.PutUint16(l[:], uint16(MaxNodeIDLen+1))
	buf := header(ProtocolVersion)
	buf.Write(l[:]) // no bytes supplied: the length check must fire first
	if _, err := readHandshake(buf); !errors.Is(err, ErrHandshakeTooLarge) {
		t.Fatalf("oversized id: err = %v, want ErrHandshakeTooLarge", err)
	}
	binary.LittleEndian.PutUint16(l[:], uint16(MaxClusterIDLen+1))
	buf = header(ProtocolVersion)
	buf.Write([]byte{1, 0, 'n'})
	buf.Write(l[:])
	if _, err := readHandshake(buf); !errors.Is(err, ErrHandshakeTooLarge) {
		t.Fatalf("oversized cluster id: err = %v, want ErrHandshakeTooLarge", err)
	}
	buf = header(ProtocolVersion)
	buf.Write([]byte{1, 0, 'n', 0, 0, MaxDigestLen + 1})
	if _, err := readHandshake(buf); !errors.Is(err, ErrHandshakeTooLarge) {
		t.Fatalf("oversized digest: err = %v, want ErrHandshakeTooLarge", err)
	}
	var r bytes.Buffer
	_ = writeReply(&r, statusAccepted, helloOf("n"))
	b := r.Bytes()
	b[8] = statusWrongDirection + 1 // an unknown status
	if _, _, err := readReply(bytes.NewReader(b)); !errors.Is(err, ErrMalformedHandshake) {
		t.Fatalf("unknown reply status: err = %v, want ErrMalformedHandshake", err)
	}
}

func TestHandshakeTruncatedRejected(t *testing.T) {
	var full bytes.Buffer
	_ = writeHandshake(&full, helloOf("some-node"))
	b := full.Bytes()
	for cut := 0; cut < len(b); cut++ {
		if _, err := readHandshake(bytes.NewReader(b[:cut])); !errors.Is(err, ErrTruncatedHandshake) && !errors.Is(err, ErrBadMagic) {
			t.Errorf("cut=%d: err = %v, want ErrTruncatedHandshake (or ErrBadMagic for a partial magic)", cut, err)
		}
	}
}
