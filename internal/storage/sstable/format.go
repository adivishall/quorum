// Package sstable implements Quorum's immutable on-disk sorted table.
//
// An SSTable is written once, fsynced, and never modified. It is the durable
// form of a flushed memtable: entries in internal-key order (package ikey),
// grouped into blocks, with an index that names the last key of each block.
//
// # Layout (docs/DESIGN.md §4)
//
//	+------------------+
//	| data block 0     |   ~4 KiB target, rounded up to the next entry boundary
//	| data block 1     |
//	| ...              |
//	+------------------+
//	| filter block     |   Phase 4: a Bloom filter. Phase 3 writes it EMPTY.
//	+------------------+
//	| index block      |   one entry per data block
//	+------------------+
//	| footer (48 B)    |
//	+------------------+
//
// Every block is followed by a four-byte crc32c of the block's bytes. The
// lengths recorded in the index and the footer exclude that checksum.
//
// # The filter block in Phase 3
//
// The format reserves a filter block because the final on-disk format has to
// be stable before files exist that later versions must read. Phase 3 writes
// it with length zero. A zero-length filter means "this file carries no
// filter, consult it directly" — it is not a filter that always says "maybe",
// and no read is accelerated by it. Bloom filters are Phase 4 (INV-S7), and
// nothing here should be read as claiming otherwise.
//
// # Endianness
//
// Little-endian, matching the record framing in docs/DESIGN.md §2 and the rest
// of the on-disk formats. One consequence worth knowing before reaching for a
// hex dump: the magic constant 0x444B565353543031 spells "DKVSST01" when read
// big-endian, so on disk its bytes appear reversed. The constant is the
// project's original spelling and was deliberately not changed by the rename
// to Quorum — churning a format constant for cosmetic reasons is exactly what
// format versioning exists to prevent.
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/adivishall/quorum/internal/record"
)

const (
	// FooterSize is the fixed footer width in bytes (docs/DESIGN.md §4).
	FooterSize = 48

	// BlockTrailerSize is the per-block checksum width.
	BlockTrailerSize = 4

	// Magic identifies the file and its format version. Read big-endian it is
	// the ASCII "DKVSST01".
	Magic = uint64(0x444B565353543031)

	// DefaultBlockSize is the data block target from docs/DESIGN.md §4. A
	// block is closed once it reaches this size, so a block always ends on an
	// entry boundary and may exceed the target by up to one entry. An entry
	// larger than the target becomes a block of its own.
	DefaultBlockSize = 4 << 10

	// MaxBlockSize bounds a DATA block length read from disk before it is used
	// to size an allocation. A data block holds at most one maximum-size key
	// plus one maximum-size value plus framing, so 2 MiB is generous; the point
	// is that a corrupted length cannot drive a wild allocation, which is the
	// same reasoning as record.MaxRecordSize.
	MaxBlockSize = 2 << 20

	// MaxMetaBlockSize is the same bound for the two blocks whose size scales
	// with the file rather than with one entry: the filter block and the index
	// block.
	//
	// Phase 3 bounded all three by MaxBlockSize, which was correct while the
	// only producer of an SSTable was a memtable flush — a 4 MiB memtable
	// cannot produce a 2 MiB index. Phase 4's compaction can: the index holds
	// one entry per data block, so a 1 GiB output file indexes ~262,000 blocks
	// and needs several MiB, and a filter at 10 bits per key needs ~1.2 MiB per
	// million keys. Keeping the 2 MiB bound would have made compaction fail on
	// large files with a corruption error, which is a silent cliff rather than
	// a limit.
	//
	// 256 MiB is chosen to be far above anything this engine produces while
	// still bounding the allocation a corrupted length can request. The
	// practical ceiling it implies on a single SSTable is recorded in
	// docs/LIMITATIONS.md rather than left to be discovered.
	MaxMetaBlockSize = 256 << 20
)

// Errors. Callers must branch with errors.Is.
var (
	// ErrCorrupt means a structural check or a checksum failed. It is
	// deliberately the same value as record.ErrCorrupt, so that one sentinel
	// covers damage wherever it is detected — WAL framing, WAL payloads, or
	// an SSTable.
	ErrCorrupt = record.ErrCorrupt

	// ErrBadMagic means the file's trailing magic is not this format's.
	//
	// docs/DESIGN.md §4 draws the distinction: a wrong magic is a format or
	// version error, not corruption. The file may be perfectly intact and
	// simply not be one of ours, or be from a future format version. It is a
	// separate sentinel so a diagnosis can tell those apart; the store still
	// refuses to open either way, because the operator's next step is the
	// same.
	ErrBadMagic = errors.New("sstable: not a Quorum SSTable (bad magic)")

	// ErrNotSorted means Add was called with a key that does not sort strictly
	// after the previous one. It is a programming error in the engine, not a
	// data condition.
	ErrNotSorted = errors.New("sstable: keys must be added in strictly increasing order")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockChecksum returns the crc32c of a block's bytes.
func blockChecksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Footer is the parsed fixed-size trailer.
type Footer struct {
	FilterOffset uint64
	FilterLength uint64
	IndexOffset  uint64
	IndexLength  uint64
	NumEntries   uint64
}

// AppendTo encodes the footer onto dst and returns the extended slice.
func (f Footer) AppendTo(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, f.FilterOffset)
	dst = binary.LittleEndian.AppendUint64(dst, f.FilterLength)
	dst = binary.LittleEndian.AppendUint64(dst, f.IndexOffset)
	dst = binary.LittleEndian.AppendUint64(dst, f.IndexLength)
	dst = binary.LittleEndian.AppendUint64(dst, f.NumEntries)
	return binary.LittleEndian.AppendUint64(dst, Magic)
}

// DecodeFooter parses and range-checks a footer read from a file of fileSize
// bytes.
//
// The magic is checked first, so a file that is not an SSTable at all is
// reported as such rather than as corruption. Everything after that is a
// bounds check, performed in uint64 with explicit overflow tests: these values
// come from disk and are about to be used as file offsets and allocation
// sizes, so a value that has been damaged must be rejected before it is acted
// on, not after.
func DecodeFooter(buf []byte, fileSize int64, name string) (Footer, error) {
	if len(buf) != FooterSize {
		return Footer{}, fmt.Errorf("sstable %s: footer is %d bytes, want %d: %w",
			name, len(buf), FooterSize, ErrCorrupt)
	}
	if fileSize < FooterSize {
		return Footer{}, fmt.Errorf("sstable %s: file is %d bytes, too short to hold a %d-byte footer: %w",
			name, fileSize, FooterSize, ErrCorrupt)
	}
	if got := binary.LittleEndian.Uint64(buf[40:]); got != Magic {
		return Footer{}, fmt.Errorf("sstable %s: magic is %#016x, want %#016x: %w",
			name, got, Magic, ErrBadMagic)
	}

	f := Footer{
		FilterOffset: binary.LittleEndian.Uint64(buf[0:]),
		FilterLength: binary.LittleEndian.Uint64(buf[8:]),
		IndexOffset:  binary.LittleEndian.Uint64(buf[16:]),
		IndexLength:  binary.LittleEndian.Uint64(buf[24:]),
		NumEntries:   binary.LittleEndian.Uint64(buf[32:]),
	}

	size := uint64(fileSize)
	footerStart := size - FooterSize

	filterEnd, ok := addChecked(f.FilterOffset, f.FilterLength, BlockTrailerSize)
	if !ok || filterEnd > f.IndexOffset {
		return Footer{}, fmt.Errorf(
			"sstable %s: filter block [%d,+%d) does not fit before the index block at %d: %w",
			name, f.FilterOffset, f.FilterLength, f.IndexOffset, ErrCorrupt)
	}
	indexEnd, ok := addChecked(f.IndexOffset, f.IndexLength, BlockTrailerSize)
	if !ok || indexEnd != footerStart {
		return Footer{}, fmt.Errorf(
			"sstable %s: index block [%d,+%d) plus its checksum ends at %d, want exactly %d "+
				"(the index block is the last thing before the footer): %w",
			name, f.IndexOffset, f.IndexLength, indexEnd, footerStart, ErrCorrupt)
	}
	if f.IndexLength > MaxMetaBlockSize || f.FilterLength > MaxMetaBlockSize {
		return Footer{}, fmt.Errorf(
			"sstable %s: metadata block length (filter %d, index %d) exceeds the %d-byte maximum: %w",
			name, f.FilterLength, f.IndexLength, uint64(MaxMetaBlockSize), ErrCorrupt)
	}
	// An index block of zero length is only legal for a file with no entries;
	// a data block always produces an index entry.
	if (f.IndexLength == 0) != (f.NumEntries == 0) {
		return Footer{}, fmt.Errorf(
			"sstable %s: index block is %d bytes but the footer declares %d entries: %w",
			name, f.IndexLength, f.NumEntries, ErrCorrupt)
	}
	return f, nil
}

// addChecked returns a+b+c, reporting false on overflow.
func addChecked(a, b, c uint64) (uint64, bool) {
	s := a + b
	if s < a {
		return 0, false
	}
	t := s + c
	if t < s {
		return 0, false
	}
	return t, true
}

// appendEntry encodes one data-block entry: keylen | key | vallen | value.
func appendEntry(dst, key, value []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = append(dst, key...)
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

// appendIndexEntry encodes one index entry: keylen | last_key | offset | length.
func appendIndexEntry(dst, lastKey []byte, offset, length uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(lastKey)))
	dst = append(dst, lastKey...)
	dst = binary.AppendUvarint(dst, offset)
	return binary.AppendUvarint(dst, length)
}

// takeBytes reads a uvarint-prefixed byte string from p.
//
// Every failure mode is named rather than collapsed into one message, because
// "corrupt block" in a log tells an operator nothing about which field lied.
func takeBytes(p []byte, what, where string) (value, rest []byte, err error) {
	n, read := binary.Uvarint(p)
	if read <= 0 {
		return nil, nil, fmt.Errorf("sstable %s: unreadable %s length: %w", where, what, ErrCorrupt)
	}
	p = p[read:]
	if n > uint64(len(p)) {
		return nil, nil, fmt.Errorf("sstable %s: declares a %d-byte %s but only %d bytes remain: %w",
			where, n, what, len(p), ErrCorrupt)
	}
	return p[:n], p[n:], nil
}

// takeUvarint reads a uvarint from p.
func takeUvarint(p []byte, what, where string) (v uint64, rest []byte, err error) {
	n, read := binary.Uvarint(p)
	if read <= 0 {
		return 0, nil, fmt.Errorf("sstable %s: unreadable %s: %w", where, what, ErrCorrupt)
	}
	return n, p[read:], nil
}
