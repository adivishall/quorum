// Package ikey implements Quorum's internal-key encoding.
//
// The LSM engine never stores a bare user key. It stores an internal key
// (docs/DESIGN.md §1):
//
//	internal_key = user_key || seq(7 bytes, big-endian) || kind(1 byte)
//	kind: 0x00 = TOMBSTONE, 0x01 = VALUE
//
// The point of the encoding is the ordering it induces:
//
//  1. user key ascending
//  2. for equal user keys, sequence number DESCENDING
//  3. for equal sequence numbers, kind descending
//
// Rule 2 is what makes the read path trivial. A forward scan that reaches a
// user key yields that key's newest version first, so a lookup is "first match
// wins" with no version bookkeeping anywhere — not in the memtable, not in an
// SSTable, and not in the merge that Phase 4's compaction will perform over
// overlapping files.
//
// Rule 3 can never fire in practice: a sequence number is consumed by exactly
// one mutation, so two entries cannot share a user key and a sequence number.
// It is specified anyway so that Compare is a total order on all byte strings
// of the right shape, including ones this package never produced.
//
// # Why not compare the whole thing bytewise
//
// Because the trailer must sort descending and a big-endian integer sorts
// ascending under bytes.Compare. Splitting the comparison is not an
// optimisation to be removed later; it is the definition.
package ikey

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Kind distinguishes a live value from a delete marker. The values match
// wal.OpDelete and wal.OpPut so that a WAL record's operation kind is the
// internal key's kind with no translation table in between.
type Kind uint8

const (
	// KindTombstone marks a key as deleted. It carries no value.
	KindTombstone Kind = 0x00
	// KindValue marks a key as present. Its value may be empty.
	KindValue Kind = 0x01
)

func (k Kind) String() string {
	switch k {
	case KindTombstone:
		return "tombstone"
	case KindValue:
		return "value"
	default:
		return fmt.Sprintf("Kind(%#x)", uint8(k))
	}
}

// Valid reports whether k is a kind this package can have written.
func (k Kind) Valid() bool { return k == KindTombstone || k == KindValue }

const (
	// SeqBytes is the width of the sequence number in the trailer.
	SeqBytes = 7
	// KindBytes is the width of the kind in the trailer.
	KindBytes = 1
	// TrailerSize is the fixed suffix appended to every user key.
	TrailerSize = SeqBytes + KindBytes

	// MaxSeq is the largest representable sequence number, 2^56-1
	// (docs/DESIGN.md §1).
	MaxSeq = uint64(1)<<(8*SeqBytes) - 1

	// MinLen is the shortest possible internal key: a one-byte user key plus
	// the trailer. A zero-length user key is rejected by the Store contract,
	// so it can never have been encoded.
	MinLen = 1 + TrailerSize

	// maxKindByte is the largest byte that can appear in the kind position. It
	// is used to build seek keys, never to encode a real entry.
	maxKindByte = 0xff
)

// Encode appends the internal key for (userKey, seq, kind) to dst and returns
// the extended slice.
//
// It panics on a sequence number that does not fit in seven bytes. That is a
// programming error in the engine — sequence numbers are generated here, not
// received from a client — and silently truncating one would corrupt the
// ordering for every key that followed.
func Encode(dst, userKey []byte, seq uint64, kind Kind) []byte {
	if seq > MaxSeq {
		panic(fmt.Sprintf("ikey: sequence number %d exceeds the %d-byte maximum %d", seq, SeqBytes, MaxSeq))
	}
	dst = append(dst, userKey...)
	return appendTrailer(dst, seq, byte(kind))
}

// Seek returns the internal key to seek to when looking for the newest version
// of userKey that is visible at seq.
//
// It is a synthetic key: the kind byte is 0xff, which no real entry carries.
// That makes it sort at or before every real entry for the same user key with
// a sequence number of at most seq, which is exactly what a lookup wants —
// seek, then take the first entry if its user key matches.
func Seek(userKey []byte, seq uint64) []byte {
	if seq > MaxSeq {
		seq = MaxSeq
	}
	out := make([]byte, 0, len(userKey)+TrailerSize)
	out = append(out, userKey...)
	return appendTrailer(out, seq, maxKindByte)
}

func appendTrailer(dst []byte, seq uint64, kindByte byte) []byte {
	var t [TrailerSize]byte
	// Seven-byte big-endian sequence number: write the eight-byte form and
	// drop the (always zero) most significant byte.
	var full [8]byte
	binary.BigEndian.PutUint64(full[:], seq)
	copy(t[:SeqBytes], full[1:])
	t[SeqBytes] = kindByte
	return append(dst, t[:]...)
}

// UserKey returns the user key portion of an internal key. It aliases ik.
func UserKey(ik []byte) []byte {
	if len(ik) < TrailerSize {
		return nil
	}
	return ik[:len(ik)-TrailerSize]
}

// Seq returns the sequence number encoded in an internal key.
func Seq(ik []byte) uint64 {
	if len(ik) < TrailerSize {
		return 0
	}
	t := ik[len(ik)-TrailerSize:]
	var full [8]byte
	copy(full[1:], t[:SeqBytes])
	return binary.BigEndian.Uint64(full[:])
}

// KindOf returns the kind byte of an internal key as a Kind. The caller must
// check Kind.Valid if the bytes came from disk.
func KindOf(ik []byte) Kind {
	if len(ik) < TrailerSize {
		return Kind(0xff)
	}
	return Kind(ik[len(ik)-1])
}

// trailer returns the eight-byte trailer as (seq<<8)|kind, which is the number
// that has to sort descending.
func trailer(ik []byte) uint64 {
	// TrailerSize is exactly eight bytes, so the trailer read big-endian IS
	// (seq<<8)|kind. Sorting that number descending gives seq descending and,
	// for equal sequence numbers, kind descending — rules 2 and 3 in one
	// comparison.
	return binary.BigEndian.Uint64(ik[len(ik)-TrailerSize:])
}

// Compare orders two internal keys: user key ascending, then trailer
// descending. It is the ordering specified in docs/DESIGN.md §1 and is the one
// ordering used by the memtable, the SSTable writer and every reader.
//
// Keys shorter than TrailerSize cannot be compared meaningfully; they sort
// before every well-formed key so that Compare stays a total order rather than
// panicking on data that arrived from a damaged file.
func Compare(a, b []byte) int {
	shortA, shortB := len(a) < TrailerSize, len(b) < TrailerSize
	switch {
	case shortA && shortB:
		return bytes.Compare(a, b)
	case shortA:
		return -1
	case shortB:
		return 1
	}

	if c := bytes.Compare(UserKey(a), UserKey(b)); c != 0 {
		return c
	}
	ta, tb := trailer(a), trailer(b)
	switch {
	case ta > tb:
		return -1 // higher sequence number sorts FIRST
	case ta < tb:
		return 1
	default:
		return 0
	}
}

// Valid reports whether ik is shaped like something this package encoded: long
// enough to hold a non-empty user key plus a trailer, and carrying a kind this
// version understands.
func Valid(ik []byte) bool {
	return len(ik) >= MinLen && KindOf(ik).Valid()
}

// String renders an internal key for diagnostics. It is not a format anything
// parses.
func String(ik []byte) string {
	if len(ik) < TrailerSize {
		return fmt.Sprintf("<malformed internal key, %d bytes>", len(ik))
	}
	return fmt.Sprintf("%q@%d:%s", UserKey(ik), Seq(ik), KindOf(ik))
}
