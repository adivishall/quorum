package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestHandshakeRoundTrip(t *testing.T) {
	for _, id := range []NodeID{"n0", "node-with-a-longer-name", NodeID(bytes.Repeat([]byte("z"), MaxNodeIDLen))} {
		var buf bytes.Buffer
		if err := writeHandshake(&buf, id); err != nil {
			t.Fatalf("writeHandshake(%q): %v", id, err)
		}
		got, err := readHandshake(&buf)
		if err != nil {
			t.Fatalf("readHandshake: %v", err)
		}
		if got != id {
			t.Errorf("round trip = %q, want %q", got, id)
		}
	}
}

func TestWriteHandshakeRejectsBadLocalID(t *testing.T) {
	var buf bytes.Buffer
	if err := writeHandshake(&buf, ""); !errors.Is(err, ErrEmptyNodeID) {
		t.Errorf("empty id: err = %v, want ErrEmptyNodeID", err)
	}
	big := NodeID(bytes.Repeat([]byte("z"), MaxNodeIDLen+1))
	if err := writeHandshake(&buf, big); !errors.Is(err, ErrHandshakeTooLarge) {
		t.Errorf("oversized id: err = %v, want ErrHandshakeTooLarge", err)
	}
}

func TestHandshakeBadMagicRejected(t *testing.T) {
	buf := bytes.NewReader([]byte("XXXX\x01\x00\x00\x00\x02\x00nX"))
	if _, err := readHandshake(buf); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("bad magic: err = %v, want ErrBadMagic", err)
	}
}

func TestHandshakeVersionMismatchRejected(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(handshakeMagic)
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], ProtocolVersion+1)
	buf.Write(v[:])
	var l [2]byte
	binary.LittleEndian.PutUint16(l[:], 2)
	buf.Write(l[:])
	buf.WriteString("n1")
	if _, err := readHandshake(&buf); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("version mismatch: err = %v, want ErrVersionMismatch", err)
	}
}

func TestHandshakeEmptyIDRejected(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(handshakeMagic)
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], ProtocolVersion)
	buf.Write(v[:])
	buf.Write([]byte{0, 0}) // idLen = 0
	if _, err := readHandshake(&buf); !errors.Is(err, ErrEmptyNodeID) {
		t.Fatalf("empty id: err = %v, want ErrEmptyNodeID", err)
	}
}

func TestHandshakeOversizedIDRejected(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(handshakeMagic)
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], ProtocolVersion)
	buf.Write(v[:])
	var l [2]byte
	binary.LittleEndian.PutUint16(l[:], uint16(MaxNodeIDLen+1))
	buf.Write(l[:])
	// No need to supply the bytes: the length check must fire first.
	if _, err := readHandshake(&buf); !errors.Is(err, ErrHandshakeTooLarge) {
		t.Fatalf("oversized id: err = %v, want ErrHandshakeTooLarge", err)
	}
}

func TestHandshakeTruncatedRejected(t *testing.T) {
	var full bytes.Buffer
	_ = writeHandshake(&full, "some-node")
	b := full.Bytes()
	for _, cut := range []int{0, 2, 4, 7, 9, 11} {
		if cut >= len(b) {
			continue
		}
		if _, err := readHandshake(bytes.NewReader(b[:cut])); !errors.Is(err, ErrTruncatedHandshake) && !errors.Is(err, ErrBadMagic) {
			t.Errorf("cut=%d: err = %v, want ErrTruncatedHandshake (or ErrBadMagic for a partial magic)", cut, err)
		}
	}
}
