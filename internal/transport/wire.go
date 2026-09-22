package transport

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/adivishall/quorum/internal/record"
)

// MaxFrameSize bounds a single frame's payload. It is smaller than the on-disk
// record maximum (record.MaxRecordSize, 64 MiB) because a network frame comes
// from a possibly-hostile peer: the declared length is checked against this
// bound before any buffer is allocated (ADR-013, docs/TRANSPORT.md §2). Snapshot
// streaming (Phase 14) will chunk rather than raise this.
const MaxFrameSize = 16 << 20 // 16 MiB

// MsgKind is the 1-byte frame kind, which is the message type. The reserved
// kinds are identifiers only: Phase 7 defines no codec or semantics for them
// (docs/TRANSPORT.md §5).
type MsgKind uint8

const (
	// Implemented in Phase 7.
	MsgProbe         MsgKind = 1
	MsgProbeResponse MsgKind = 2

	// Reserved for later phases — identifiers only, no codec, no semantics.
	MsgRequestVote             MsgKind = 16
	MsgRequestVoteResponse     MsgKind = 17
	MsgAppendEntries           MsgKind = 18
	MsgAppendEntriesResponse   MsgKind = 19
	MsgInstallSnapshot         MsgKind = 20
	MsgInstallSnapshotResponse MsgKind = 21
	MsgForward                 MsgKind = 32
	MsgForwardResponse         MsgKind = 33
)

// String renders a kind for logs and errors.
func (k MsgKind) String() string {
	switch k {
	case MsgProbe:
		return "Probe"
	case MsgProbeResponse:
		return "ProbeResponse"
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResponse:
		return "RequestVoteResponse"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResponse:
		return "AppendEntriesResponse"
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotResponse:
		return "InstallSnapshotResponse"
	case MsgForward:
		return "Forward"
	case MsgForwardResponse:
		return "ForwardResponse"
	default:
		return "Unknown"
	}
}

// defined reports whether k is a defined kind (implemented or reserved). A frame
// carrying an undefined kind is ErrUnknownKind and closes the connection.
func (k MsgKind) defined() bool { return k.String() != "Unknown" }

// implemented reports whether Phase 7 has a codec and handler for k.
func (k MsgKind) implemented() bool { return k == MsgProbe || k == MsgProbeResponse }

// castagnoli is CRC-32C, matching internal/record so the transport and the
// on-disk logs share one checksum.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// writeFrame encodes one frame (record framing, §2) into buf and writes it in
// full to w. buf is a caller-provided scratch slice reused across calls to avoid
// per-frame allocation; the returned slice is buf grown as needed. The caller
// holds the connection's writer lock, so w receives one frame's bytes without
// interleaving from another goroutine.
func writeFrame(w io.Writer, buf []byte, kind MsgKind, payload []byte) ([]byte, error) {
	enc, err := record.Encode(buf[:0], record.Kind(kind), payload)
	if err != nil {
		return enc, err // payload exceeds record.MaxRecordSize
	}
	if _, err := w.Write(enc); err != nil {
		return enc, err
	}
	return enc, nil
}

// readFrame reads exactly one frame from r with io.ReadFull discipline, so it is
// correct regardless of how TCP fragments the bytes. It returns:
//
//   - io.EOF only when the stream ends cleanly at a frame boundary (zero bytes of
//     a new header were available);
//   - ErrTruncatedFrame when the stream ends mid-header or mid-payload;
//   - ErrFrameTooLarge when the declared length exceeds MaxFrameSize (checked
//     before allocating the payload buffer);
//   - ErrBadChecksum when the CRC does not match;
//   - ErrUnknownKind when the kind byte is not a defined kind.
//
// hdr is a caller-provided 9-byte (record.HeaderSize) scratch array.
func readFrame(r io.Reader, hdr []byte) (MsgKind, []byte, error) {
	if _, err := io.ReadFull(r, hdr[:record.HeaderSize]); err != nil {
		if err == io.EOF {
			return 0, nil, io.EOF // clean end at a frame boundary
		}
		if err == io.ErrUnexpectedEOF {
			return 0, nil, ErrTruncatedFrame
		}
		return 0, nil, err
	}
	crc := binary.LittleEndian.Uint32(hdr[0:4])
	length := binary.LittleEndian.Uint32(hdr[4:8])
	kind := MsgKind(hdr[8])

	if length > MaxFrameSize {
		return 0, nil, ErrFrameTooLarge
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, ErrTruncatedFrame
		}
		return 0, nil, err
	}

	// The checksum covers length, kind and payload — everything the encoder
	// checksummed (record.Encode) except the checksum field itself.
	h := crc32.New(castagnoli)
	_, _ = h.Write(hdr[4:record.HeaderSize]) // length(4) + kind(1)
	_, _ = h.Write(payload)
	if h.Sum32() != crc {
		return 0, nil, ErrBadChecksum
	}
	if !kind.defined() {
		return 0, nil, ErrUnknownKind
	}
	return kind, payload, nil
}
