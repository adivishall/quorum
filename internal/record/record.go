// Package record implements dkv's on-disk record framing.
//
// One framing format serves three different logs — the storage engine's
// write-ahead log, the Raft log, and the MANIFEST (docs/DESIGN.md §2). They are
// deliberately not three subtly different implementations: a checksum or
// truncation bug fixed in one place would otherwise survive in the other two,
// and each of those logs is a place where silently losing a record means
// silently losing committed data.
//
// Layout, little-endian throughout:
//
//	offset  size  field
//	0       4     crc32c(length || kind || payload)
//	4       4     length  (payload byte count)
//	8       1     kind    (record type; the namespace is per-log-type)
//	9       N     payload
//
// The checksum covers the length and kind bytes as well as the payload, so a
// corrupted length field is detected rather than acted upon — with one caveat
// that is handled explicitly: a corrupt length must be range-checked *before*
// it is used to size a read, because the checksum cannot be verified until the
// payload has been read. That is what MaxRecordSize is for.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// HeaderSize is the fixed per-record overhead in bytes.
	HeaderSize = 9

	// MaxRecordSize bounds a single record's payload (docs/DESIGN.md §2).
	// It exists to stop a corrupted length field from being used to attempt a
	// multi-gigabyte allocation before any checksum can be verified.
	MaxRecordSize = 64 << 20 // 64 MiB

	offCRC    = 0
	offLength = 4
	offKind   = 8
)

// Kind identifies a record's type. The value namespace belongs to the log that
// uses the framing: kind 0x01 means WriteBatch in a WAL and something else
// entirely in a Raft log.
type Kind uint8

// castagnoli is CRC-32C, which has hardware support on both amd64 and arm64.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Sentinel errors. Callers must branch with errors.Is, never on error strings.
var (
	// ErrCorrupt means a record failed validation in a way that cannot be
	// explained by an interrupted write. Nothing after it can be trusted, and
	// there is no resynchronisation: skipping a record in the middle of a log
	// silently loses a committed write, so the only correct response is to
	// refuse.
	ErrCorrupt = errors.New("corrupt record")

	// ErrTornTail means the final record in the file is incomplete or fails
	// its checksum, which is exactly what a crash during an append looks like.
	// Whether that is recoverable is a policy decision for the caller — a torn
	// tail in the newest WAL segment is expected, whereas the same thing in an
	// older segment is not.
	ErrTornTail = errors.New("torn record at end of file")
)

// Error locates a framing failure precisely enough to act on.
//
// It carries the file, the byte offset of the offending record, and a
// human-readable reason. It never carries the payload bytes: a corrupt record
// is arbitrary data, and pasting it into an error string or a log line is how
// binary garbage ends up in a terminal.
type Error struct {
	File   string // file being read, for diagnostics
	Offset int64  // byte offset at which the bad record starts
	Reason string // what specifically was wrong
	Err    error  // ErrCorrupt or ErrTornTail
}

func (e *Error) Error() string {
	return fmt.Sprintf("record: %s at %s offset %d: %s", e.Err, e.File, e.Offset, e.Reason)
}

func (e *Error) Unwrap() error { return e.Err }

func corruptAt(file string, off int64, format string, args ...any) *Error {
	return &Error{File: file, Offset: off, Reason: fmt.Sprintf(format, args...), Err: ErrCorrupt}
}

func tornAt(file string, off int64, format string, args ...any) *Error {
	return &Error{File: file, Offset: off, Reason: fmt.Sprintf(format, args...), Err: ErrTornTail}
}

// EncodedLen returns the total on-disk size of a record with this payload.
func EncodedLen(payloadLen int) int { return HeaderSize + payloadLen }

// Encode appends a framed record to dst and returns the extended slice.
//
// It returns an error only when the payload exceeds MaxRecordSize; that check
// lives here so a record that could never be read back cannot be written.
func Encode(dst []byte, kind Kind, payload []byte) ([]byte, error) {
	if len(payload) > MaxRecordSize {
		return dst, fmt.Errorf("record: payload of %d bytes exceeds maximum %d: %w",
			len(payload), MaxRecordSize, ErrCorrupt)
	}

	start := len(dst)
	dst = append(dst, make([]byte, HeaderSize)...)
	binary.LittleEndian.PutUint32(dst[start+offLength:], uint32(len(payload)))
	dst[start+offKind] = byte(kind)
	dst = append(dst, payload...)

	// The checksum covers length, kind and payload — everything but the
	// checksum field itself.
	sum := crc32.Checksum(dst[start+offLength:], castagnoli)
	binary.LittleEndian.PutUint32(dst[start+offCRC:], sum)

	return dst, nil
}
