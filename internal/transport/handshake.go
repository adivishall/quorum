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
	// version is refused (ErrVersionMismatch) rather than misparsed. Version 2
	// (audit H2, D9): the handshake carries the cluster id and settings digest
	// and is answered, and every frame carries a group envelope (Phase 15).
	ProtocolVersion uint32 = 2

	// MaxNodeIDLen bounds the node id in a handshake, so a declared length can
	// never size an unbounded allocation.
	MaxNodeIDLen = 256
	// MaxClusterIDLen and MaxDigestLen bound the cluster id and the settings
	// digest a handshake carries.
	MaxClusterIDLen = 256
	MaxDigestLen    = 64

	handshakeMagicLen = 4
	// maxHandshakeSize is the largest a valid handshake can be: magic, version,
	// node id, cluster id and digest with their lengths, and a reply's status.
	maxHandshakeSize = handshakeMagicLen + 4 + 1 + 2 + MaxNodeIDLen + 2 + MaxClusterIDLen + 1 + MaxDigestLen
)

// hello is what each side of a connection announces (docs/TRANSPORT.md §3):
// its node id, the cluster it belongs to and the digest of the settings every
// node of that cluster must share. A node refuses a connection whose cluster
// or settings differ from its own: a node of another cluster — a wrong
// address, a recycled IP — must never be heard as a peer, its Raft traffic
// taken for its namesake's (audit H2), and nodes with different replica
// settings must never form a group (audit H5). Both are opaque here; the empty
// cluster id and digest are values like any other, equal only to themselves.
type hello struct {
	id      NodeID
	cluster string
	digest  []byte
}

// Statuses of an accepter's reply.
const (
	statusAccepted       byte = 0
	statusUnknownPeer    byte = 1
	statusWrongCluster   byte = 2
	statusWrongSettings  byte = 3
	statusSelfConnection byte = 4
	statusWrongDirection byte = 5
)

func statusError(s byte) error {
	switch s {
	case statusAccepted:
		return nil
	case statusUnknownPeer:
		return ErrUnknownPeer
	case statusWrongCluster:
		return ErrClusterMismatch
	case statusWrongSettings:
		return ErrSettingsMismatch
	case statusSelfConnection:
		return ErrSelfConnection
	case statusWrongDirection:
		return ErrWrongDirection
	}
	return ErrMalformedHandshake
}

// encodeHello appends a hello: magic | version u32 | [status u8, reply only] |
// idLen u16 | id | clusterLen u16 | cluster | digestLen u8 | digest.
func encodeHello(h hello, reply bool, status byte) ([]byte, error) {
	switch {
	case len(h.id) == 0:
		return nil, ErrEmptyNodeID
	case len(h.id) > MaxNodeIDLen, len(h.cluster) > MaxClusterIDLen, len(h.digest) > MaxDigestLen:
		return nil, ErrHandshakeTooLarge
	}
	b := make([]byte, 0, maxHandshakeSize)
	b = append(b, handshakeMagic...)
	b = binary.LittleEndian.AppendUint32(b, ProtocolVersion)
	if reply {
		b = append(b, status)
	}
	b = binary.LittleEndian.AppendUint16(b, uint16(len(h.id)))
	b = append(b, h.id...)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(h.cluster)))
	b = append(b, h.cluster...)
	b = append(b, byte(len(h.digest)))
	return append(b, h.digest...), nil
}

// writeHandshake sends the dialer's hello. The caller sets a write deadline
// first (handshake timeout).
func writeHandshake(w io.Writer, h hello) error {
	b, err := encodeHello(h, false, 0)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// writeReply sends the accepter's answer: a status and its own hello.
func writeReply(w io.Writer, status byte, h hello) error {
	b, err := encodeHello(h, true, status)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// readHandshake reads and validates the dialer's hello. It never allocates on
// an unchecked length. The caller sets a read deadline first (handshake
// timeout); a deadline expiry surfaces as a timeout error the caller maps to
// ErrHandshakeTimeout.
func readHandshake(r io.Reader) (hello, error) {
	h, _, err := readHello(r, false)
	return h, err
}

// readReply reads the accepter's answer: its status and its hello.
func readReply(r io.Reader) (byte, hello, error) {
	h, status, err := readHello(r, true)
	return status, h, err
}

func readHello(r io.Reader, reply bool) (hello, byte, error) {
	var h hello
	magic := make([]byte, handshakeMagicLen)
	if _, err := io.ReadFull(r, magic); err != nil {
		return h, 0, truncatedHandshake(err)
	}
	if string(magic) != handshakeMagic {
		return h, 0, ErrBadMagic
	}
	var vb [4]byte
	if _, err := io.ReadFull(r, vb[:]); err != nil {
		return h, 0, truncatedHandshake(err)
	}
	if binary.LittleEndian.Uint32(vb[:]) != ProtocolVersion {
		return h, 0, ErrVersionMismatch
	}
	var status byte
	if reply {
		var sb [1]byte
		if _, err := io.ReadFull(r, sb[:]); err != nil {
			return h, 0, truncatedHandshake(err)
		}
		status = sb[0]
		if status > statusWrongDirection {
			return h, 0, ErrMalformedHandshake
		}
	}
	id, err := readString(r, 2, MaxNodeIDLen)
	if err != nil {
		return h, 0, err
	}
	if id == "" {
		return h, 0, ErrEmptyNodeID
	}
	cluster, err := readString(r, 2, MaxClusterIDLen)
	if err != nil {
		return h, 0, err
	}
	digest, err := readString(r, 1, MaxDigestLen)
	if err != nil {
		return h, 0, err
	}
	h = hello{id: NodeID(id), cluster: cluster}
	if digest != "" {
		h.digest = []byte(digest)
	}
	return h, status, nil
}

// readString reads a length of width bytes (1 or 2, little-endian) and that
// many bytes, refusing a length above max before allocating.
func readString(r io.Reader, width, max int) (string, error) {
	var lb [2]byte
	if _, err := io.ReadFull(r, lb[:width]); err != nil {
		return "", truncatedHandshake(err)
	}
	n := int(lb[0])
	if width == 2 {
		n = int(binary.LittleEndian.Uint16(lb[:]))
	}
	if n > max {
		return "", ErrHandshakeTooLarge
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", truncatedHandshake(err)
	}
	return string(b), nil
}

// truncatedHandshake maps a premature end of stream to ErrTruncatedHandshake and
// passes everything else (e.g. a deadline timeout) through unchanged.
func truncatedHandshake(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return ErrTruncatedHandshake
	}
	return err
}
