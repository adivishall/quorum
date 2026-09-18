package record

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

// Reader reads framed records from a stream of known length.
//
// The length is required, not optional. Distinguishing "the last record was cut
// short by a crash" from "a record in the middle of the log is corrupt" is the
// single most important thing this package does, and it cannot be decided
// without knowing where the file ends.
//
// Reader never resynchronises. There is no scan for the next plausible header:
// a format without block boundaries offers no sound place to resume, and
// guessing would mean silently skipping a committed record. When a record
// cannot be trusted, Reader stops and says exactly where.
type Reader struct {
	r    *bufio.Reader
	name string
	size int64

	off  int64 // offset at which the next record starts
	last int64 // offset of the record most recently returned by Next

	// failed latches the first failure. Once Next has reported a problem the
	// Reader is finished: the underlying stream has already consumed the bytes
	// of the bad record, so r.off and the stream position no longer agree, and
	// a subsequent read would return whatever followed as though it were the
	// next record. That is resynchronisation by accident, and it is exactly
	// the silent record-skipping this package exists to prevent.
	failed error

	hdr []byte
}

// NewReader reads records from r, which must contain exactly size bytes.
// name is used only for diagnostics.
func NewReader(r io.Reader, name string, size int64) *Reader {
	return &Reader{
		r:    bufio.NewReaderSize(r, 64<<10),
		name: name,
		size: size,
		hdr:  make([]byte, HeaderSize),
	}
}

// Offset returns the byte offset of the record most recently returned by Next.
func (r *Reader) Offset() int64 { return r.last }

// NextOffset returns the offset immediately after the last successfully read
// record — that is, the end of the last known-good record boundary.
//
// This is the truncation point during recovery. Note that it can be less than
// the file size even when Next has returned a clean io.EOF: a crash can leave
// fewer than HeaderSize trailing bytes, which the framing treats as a clean end
// of file but which must still be truncated away before appending, or the next
// record would be written after garbage.
func (r *Reader) NextOffset() int64 { return r.off }

// Next returns the next record.
//
// It returns io.EOF at a clean end of file. Any other error is an *Error
// wrapping either ErrTornTail or ErrCorrupt; see the package documentation for
// the distinction and docs/WAL.md for the policy built on it.
//
// The returned payload is only valid until the next call to Next.
func (r *Reader) Next() (Kind, []byte, error) {
	if r.failed != nil {
		return 0, nil, r.failed
	}
	kind, payload, err := r.next()
	if err != nil && err != io.EOF {
		r.failed = err
	}
	return kind, payload, err
}

func (r *Reader) next() (Kind, []byte, error) {
	start := r.off
	remaining := r.size - start

	// Fewer than a header's worth of bytes left: a clean end of file. Any
	// stray bytes are reported through NextOffset rather than as an error,
	// because a partially-written header is indistinguishable from a file
	// that simply ends, and the caller's truncation policy handles both.
	if remaining < HeaderSize {
		return 0, nil, io.EOF
	}

	if _, err := io.ReadFull(r.r, r.hdr); err != nil {
		// The size we were given disagrees with what the stream actually
		// holds. That is not a torn record; it is a broken caller or a file
		// that changed underneath us.
		return 0, nil, corruptAt(r.name, start, "short read of record header: %v", err)
	}

	// An all-zero header is never something this package wrote: a legitimately
	// encoded empty record of kind 0 stores crc32c(0,0,0,0,0) = 0x45727635, not
	// zero. So a zero header means "no record was written here" — a file that
	// was extended without being filled, or a region that was zeroed.
	//
	// Which of those it is decides whether recovery may truncate, so it is
	// worth the one scan it takes to find out rather than guessing. If every
	// remaining byte is zero, nothing follows and this is the end of the log.
	// If real data follows, the zeros are damage in the middle of the log and
	// truncating here would discard valid records.
	if isAllZero(r.hdr) {
		rest, err := r.remainderAllZero()
		switch {
		case err != nil:
			return 0, nil, corruptAt(r.name, start, "reading past zero-filled header: %v", err)
		case rest:
			return 0, nil, tornAt(r.name, start,
				"zero-filled from this offset to end of file (%d bytes); no record was written here", remaining)
		default:
			return 0, nil, corruptAt(r.name, start,
				"zero-filled record header with non-zero data following it")
		}
	}

	storedCRC := binary.LittleEndian.Uint32(r.hdr[offCRC:])
	length := binary.LittleEndian.Uint32(r.hdr[offLength:])
	kind := Kind(r.hdr[offKind])

	// The length must be range-checked before it is used to size a read: the
	// checksum cannot be verified until the payload is read, so a corrupted
	// length would otherwise drive a wild allocation.
	if int64(length) > MaxRecordSize {
		return 0, nil, corruptAt(r.name, start,
			"declared payload length %d exceeds maximum %d", length, MaxRecordSize)
	}

	total := int64(HeaderSize) + int64(length)
	if remaining < total {
		// The record claims more bytes than the file contains. Nothing can
		// follow it, so this is unambiguously the tail.
		return 0, nil, tornAt(r.name, start,
			"record declares %d bytes but only %d remain", total, remaining)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return 0, nil, corruptAt(r.name, start, "short read of record payload: %v", err)
	}

	sum := crc32.Update(0, castagnoli, r.hdr[offLength:])
	sum = crc32.Update(sum, castagnoli, payload)
	if sum != storedCRC {
		// A checksum failure alone does not say whether this was an
		// interrupted append or damage to an established record. The extent
		// does: if the record ends exactly at EOF, nothing was written after
		// it, which is what an interrupted append looks like. If bytes follow,
		// this record was completed and something later damaged it — and
		// truncating here would discard the valid records that follow.
		//
		// This is sound whenever the length field survived. If the length was
		// itself corrupted the arithmetic misleads, which is a documented
		// limitation of a framing format with no block structure to resync on
		// (docs/WAL.md, "What corruption detection does not cover").
		if start+total == r.size {
			return 0, nil, tornAt(r.name, start,
				"checksum mismatch in the final record (have %#08x, want %#08x)", sum, storedCRC)
		}
		return 0, nil, corruptAt(r.name, start,
			"checksum mismatch (have %#08x, want %#08x); %d bytes follow this record",
			sum, storedCRC, r.size-(start+total))
	}

	r.last = start
	r.off = start + total
	return kind, payload, nil
}

// isAllZero reports whether every byte of b is zero.
func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// remainderAllZero reports whether every remaining byte in the stream is zero.
// It consumes the stream, which is acceptable because it is only called on a
// path that is about to return an error.
func (r *Reader) remainderAllZero() (bool, error) {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.r.Read(buf)
		if !isAllZero(buf[:n]) {
			return false, nil
		}
		if err == io.EOF {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
}
