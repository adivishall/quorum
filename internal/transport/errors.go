package transport

import "errors"

// Sentinel errors for the transport. Callers branch with errors.Is, never on
// error strings. They fall into three groups: framing/codec errors (a peer sent
// bytes we refuse), handshake errors (a connection we refuse), and lifecycle
// errors (the local transport's own state). None of them is ever "repaired" —
// unlike the WAL, a malformed network frame closes the connection (ADR-013,
// docs/TRANSPORT.md §2).
var (
	// --- framing / codec ---

	// ErrFrameTooLarge means a frame declared a payload larger than
	// MaxFrameSize. It is detected before any buffer is allocated, so a hostile
	// peer cannot induce a large allocation with a lie about the length.
	ErrFrameTooLarge = errors.New("transport: frame exceeds maximum size")

	// ErrTruncatedFrame means the connection ended in the middle of a frame's
	// header or payload. On a socket this is a failed message, not a recoverable
	// tail — the connection is closed.
	ErrTruncatedFrame = errors.New("transport: truncated frame")

	// ErrBadChecksum means a frame's CRC-32C did not match its bytes.
	ErrBadChecksum = errors.New("transport: frame checksum mismatch")

	// ErrUnknownKind means the frame's kind byte is not a defined message kind.
	ErrUnknownKind = errors.New("transport: unknown message kind")

	// ErrUnimplementedKind means the kind is a reserved identifier whose codec
	// and semantics belong to a later phase (docs/TRANSPORT.md §5).
	ErrUnimplementedKind = errors.New("transport: message kind not implemented in this phase")

	// ErrMalformedPayload means a message payload could not be decoded: a bad
	// varint, a length larger than the bytes available, or trailing bytes.
	ErrMalformedPayload = errors.New("transport: malformed payload")

	// ErrTrailingBytes means a payload decoded successfully but left bytes over.
	// A codec must consume exactly its frame payload.
	ErrTrailingBytes = errors.New("transport: trailing bytes after payload")

	// --- handshake ---

	// ErrBadMagic means the connection did not begin with the "DKV1" magic.
	ErrBadMagic = errors.New("transport: bad handshake magic")

	// ErrVersionMismatch means the peer announced a protocol version this build
	// does not speak.
	ErrVersionMismatch = errors.New("transport: protocol version mismatch")

	// ErrEmptyNodeID means the handshake carried a zero-length node id.
	ErrEmptyNodeID = errors.New("transport: empty node id")

	// ErrHandshakeTooLarge means the handshake declared a node id longer than
	// MaxNodeIDLen.
	ErrHandshakeTooLarge = errors.New("transport: handshake node id too large")

	// ErrTruncatedHandshake means the connection ended mid-handshake.
	ErrTruncatedHandshake = errors.New("transport: truncated handshake")

	// ErrHandshakeTimeout means the handshake did not complete within the
	// handshake deadline.
	ErrHandshakeTimeout = errors.New("transport: handshake timed out")

	// ErrSelfConnection means a handshake announced our own node id.
	ErrSelfConnection = errors.New("transport: self connection rejected")

	// ErrUnknownPeer means a handshake announced a node id that is not a
	// configured peer.
	ErrUnknownPeer = errors.New("transport: unknown peer")

	// --- lifecycle ---

	// ErrPeerNotConnected means Send was called for a peer with no live
	// connection.
	ErrPeerNotConnected = errors.New("transport: peer not connected")

	// ErrClosed means the transport has been closed.
	ErrClosed = errors.New("transport: closed")

	// ErrInvalidConfig means the transport Config failed validation.
	ErrInvalidConfig = errors.New("transport: invalid config")
)
