package transport

import (
	"encoding/binary"
	"io"
)

// Handshake constants (docs/TRANSPORT.md §3).
const (
	// handshakeMagic gates a wrong service: a connection that does not begin
	// with these four bytes is refused before anything is interpreted as a frame.
	handshakeMagic = "DKV1"

	// ProtocolVersion is the current wire version. A peer announcing a different
	// version is refused (ErrVersionMismatch) rather than misparsed.
	ProtocolVersion uint32 = 1

	// MaxNodeIDLen bounds the node id in a handshake, so a declared length can
	// never size an unbounded allocation.
	MaxNodeIDLen = 256

	handshakeMagicLen = 4
	// maxHandshakeSize is the largest a valid handshake can be, for callers that
	// want to bound reads: magic + version + idLen + id.
	maxHandshakeSize = handshakeMagicLen + 4 + 2 + MaxNodeIDLen
)

// writeHandshake sends the one-way handshake announcing id. The caller sets a
// write deadline first (handshake timeout).
func writeHandshake(w io.Writer, id NodeID) error {
	if len(id) == 0 {
		return ErrEmptyNodeID
	}
	if len(id) > MaxNodeIDLen {
		return ErrHandshakeTooLarge
	}
	buf := make([]byte, 0, handshakeMagicLen+4+2+len(id))
	buf = append(buf, handshakeMagic...)
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], ProtocolVersion)
	buf = append(buf, v[:]...)
	var l [2]byte
	binary.LittleEndian.PutUint16(l[:], uint16(len(id)))
	buf = append(buf, l[:]...)
	buf = append(buf, id...)
	_, err := w.Write(buf)
	return err
}

// readHandshake reads and validates the peer's handshake, returning the peer's
// announced node id. It never allocates on an unchecked length. The caller sets
// a read deadline first (handshake timeout); a deadline expiry surfaces as a
// timeout error the caller maps to ErrHandshakeTimeout.
func readHandshake(r io.Reader) (NodeID, error) {
	magic := make([]byte, handshakeMagicLen)
	if _, err := io.ReadFull(r, magic); err != nil {
		return "", truncatedHandshake(err)
	}
	if string(magic) != handshakeMagic {
		return "", ErrBadMagic
	}

	var vb [4]byte
	if _, err := io.ReadFull(r, vb[:]); err != nil {
		return "", truncatedHandshake(err)
	}
	if binary.LittleEndian.Uint32(vb[:]) != ProtocolVersion {
		return "", ErrVersionMismatch
	}

	var lb [2]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return "", truncatedHandshake(err)
	}
	n := binary.LittleEndian.Uint16(lb[:])
	if n == 0 {
		return "", ErrEmptyNodeID
	}
	if int(n) > MaxNodeIDLen {
		return "", ErrHandshakeTooLarge
	}

	id := make([]byte, n)
	if _, err := io.ReadFull(r, id); err != nil {
		return "", truncatedHandshake(err)
	}
	return NodeID(id), nil
}

// truncatedHandshake maps a premature end of stream to ErrTruncatedHandshake and
// passes everything else (e.g. a deadline timeout) through unchanged.
func truncatedHandshake(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return ErrTruncatedHandshake
	}
	return err
}
