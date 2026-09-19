package sstable

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/adivishall/quorum/internal/storage/bloom"
	"github.com/adivishall/quorum/internal/storage/ikey"
)

// indexEntry locates one data block. LastKey is the greatest internal key in
// the block, which is what makes a binary search over the index possible.
type indexEntry struct {
	LastKey []byte
	Offset  uint64
	Length  uint64 // excludes the block's checksum
}

// Reader reads an immutable SSTable.
//
// The index is parsed once at Open and held in memory; data blocks are read on
// demand with ReadAt. There is no block cache — the OS page cache is doing
// that job, and adding another one is a Phase 5 decision that has to be
// justified by a measurement (docs/DESIGN.md §11).
//
// A Reader is safe for concurrent use: everything it holds is immutable after
// Open, and ReadAt does not share a file offset. Close is the exception and
// must not race with in-flight reads; storage.LSMStore guarantees that by
// draining its operations before it closes anything.
type Reader struct {
	f    *os.File
	name string
	size int64

	footer Footer
	index  []indexEntry

	// filter is the decoded Bloom filter, or the zero Filter when the file
	// carries none (a Phase 3 file, or one written with DisableFilter). The
	// zero Filter answers "maybe" to everything, so a missing filter degrades
	// to consulting the file rather than skipping it.
	filter bloom.Filter

	// Counters. They are atomic because a Reader is shared by concurrent
	// readers, and they are counters rather than state: nothing about the
	// answers a Reader gives depends on them. They exist so that the Bloom
	// measurement in docs/BLOOM.md can report work avoided rather than only
	// wall-clock time, which is the part of the result that generalises.
	blockReads  atomic.Uint64
	filterSkips atomic.Uint64
}

// Open opens and structurally validates an SSTable.
//
// Validation here is everything that can be checked without reading a data
// block: the magic, the footer's internal consistency, the filter and index
// blocks' checksums, and the index's own claims — that the data blocks it names are
// contiguous from offset zero, end exactly where the filter block begins, and
// have strictly increasing last keys.
//
// What Open does NOT do is verify the data blocks' checksums; that is Verify,
// which costs a full read of the file. The split is deliberate: Open is on the
// read path for every file at startup, Verify is a decision the engine makes
// about how much it wants to pay to trust a file before serving from it.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: opening %s: %w", path, err)
	}
	r, err := newReader(f, path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return r, nil
}

func newReader(f *os.File, path string) (*Reader, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	name := info.Name()
	size := info.Size()

	if size < FooterSize {
		return nil, fmt.Errorf("sstable %s: file is %d bytes, too short to be an SSTable (%d-byte footer): %w",
			name, size, FooterSize, ErrCorrupt)
	}

	buf := make([]byte, FooterSize)
	if _, err := f.ReadAt(buf, size-FooterSize); err != nil {
		return nil, fmt.Errorf("sstable %s: reading footer: %w", name, err)
	}
	footer, err := DecodeFooter(buf, size, name)
	if err != nil {
		return nil, err
	}

	r := &Reader{f: f, name: name, size: size, footer: footer}

	// The filter block's checksum is verified whether or not it holds a filter.
	// A region of the file that no check covers is a region where damage goes
	// unnoticed, and it matters doubly here: the filter encoding carries no
	// checksum of its own, so this block checksum is the only thing that stops
	// a flipped bit from turning into a filter that answers "definitely absent"
	// for a key that is present.
	filterBlock, err := r.readBlock(footer.FilterOffset, footer.FilterLength,
		MaxMetaBlockSize, "filter block")
	if err != nil {
		return nil, err
	}
	// A zero-length filter block means "no filter; consult the file directly",
	// which is what Phase 3 wrote and what DisableFilter still writes. Anything
	// else must decode, and a filter that does not decode is corruption — never
	// silently downgraded to "no filter", because that would mean damage
	// quietly costs performance instead of being reported.
	if footer.FilterLength > 0 {
		flt, ferr := bloom.Decode(filterBlock)
		if ferr != nil {
			return nil, fmt.Errorf("sstable %s: filter block: %v: %w", name, ferr, ErrCorrupt)
		}
		r.filter = flt
	}

	indexBlock, err := r.readBlock(footer.IndexOffset, footer.IndexLength,
		MaxMetaBlockSize, "index block")
	if err != nil {
		return nil, err
	}
	if err := r.parseIndex(indexBlock); err != nil {
		return nil, err
	}
	return r, nil
}

// parseIndex decodes the index block and checks every claim it makes.
func (r *Reader) parseIndex(block []byte) error {
	var (
		entries  []indexEntry
		expected uint64 // where the next data block must start
		p        = block
	)
	for len(p) > 0 {
		where := fmt.Sprintf("%s index entry %d", r.name, len(entries))

		key, rest, err := takeBytes(p, "key", where)
		if err != nil {
			return err
		}
		offset, rest, err := takeUvarint(rest, "block offset", where)
		if err != nil {
			return err
		}
		length, rest, err := takeUvarint(rest, "block length", where)
		if err != nil {
			return err
		}
		p = rest

		if !ikey.Valid(key) {
			return fmt.Errorf("sstable %s: index entry %d names a malformed key %s: %w",
				r.name, len(entries), ikey.String(key), ErrCorrupt)
		}
		if n := len(entries); n > 0 && ikey.Compare(entries[n-1].LastKey, key) >= 0 {
			return fmt.Errorf("sstable %s: index keys are not increasing: %s then %s: %w",
				r.name, ikey.String(entries[n-1].LastKey), ikey.String(key), ErrCorrupt)
		}
		if offset != expected {
			return fmt.Errorf(
				"sstable %s: index entry %d puts its data block at %d, but the previous block ends at %d "+
					"(data blocks are contiguous from offset 0): %w",
				r.name, len(entries), offset, expected, ErrCorrupt)
		}
		if length == 0 || length > MaxBlockSize {
			return fmt.Errorf("sstable %s: index entry %d declares a %d-byte data block: %w",
				r.name, len(entries), length, ErrCorrupt)
		}
		end, ok := addChecked(offset, length, BlockTrailerSize)
		if !ok || end > r.footer.FilterOffset {
			return fmt.Errorf(
				"sstable %s: index entry %d names bytes [%d,+%d), which runs past the filter block at %d: %w",
				r.name, len(entries), offset, length, r.footer.FilterOffset, ErrCorrupt)
		}
		expected = end

		entries = append(entries, indexEntry{LastKey: key, Offset: offset, Length: length})
	}

	if expected != r.footer.FilterOffset {
		return fmt.Errorf(
			"sstable %s: the indexed data blocks end at %d but the filter block starts at %d; "+
				"%d bytes are unaccounted for: %w",
			r.name, expected, r.footer.FilterOffset, r.footer.FilterOffset-expected, ErrCorrupt)
	}
	if len(entries) == 0 && r.footer.NumEntries != 0 {
		return fmt.Errorf("sstable %s: the index is empty but the footer declares %d entries: %w",
			r.name, r.footer.NumEntries, ErrCorrupt)
	}
	r.index = entries
	return nil
}

// readBlock reads a block and verifies its checksum.
//
// maxLen is the caller's bound on the declared length: MaxBlockSize for a data
// block, MaxMetaBlockSize for the filter and index blocks, whose size scales
// with the file rather than with one entry. The bound is applied before the
// length is used to size an allocation, because it came off disk.
func (r *Reader) readBlock(offset, length, maxLen uint64, what string) ([]byte, error) {
	if length > maxLen {
		return nil, fmt.Errorf("sstable %s: %s declares %d bytes, over the %d-byte maximum: %w",
			r.name, what, length, maxLen, ErrCorrupt)
	}
	end, ok := addChecked(offset, length, BlockTrailerSize)
	if !ok || end > uint64(r.size) {
		return nil, fmt.Errorf("sstable %s: %s at [%d,+%d) runs past the %d-byte file: %w",
			r.name, what, offset, length, r.size, ErrCorrupt)
	}

	buf := make([]byte, length+BlockTrailerSize)
	if _, err := r.f.ReadAt(buf, int64(offset)); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("sstable %s: %s at [%d,+%d) is truncated: %w",
				r.name, what, offset, length, ErrCorrupt)
		}
		return nil, fmt.Errorf("sstable %s: reading %s at %d: %w", r.name, what, offset, err)
	}

	block := buf[:length]
	want := uint32(buf[length]) | uint32(buf[length+1])<<8 | uint32(buf[length+2])<<16 | uint32(buf[length+3])<<24
	if got := blockChecksum(block); got != want {
		return nil, fmt.Errorf("sstable %s: %s at offset %d fails its checksum (%#08x, want %#08x): %w",
			r.name, what, offset, got, want, ErrCorrupt)
	}
	return block, nil
}

// Get returns the newest version of userKey visible at seq.
//
// The three outcomes are distinct and the caller must treat them as such:
//
//	found=false            this file holds no version of the key; keep looking
//	found=true, KindValue  the newest version is this value; stop
//	found=true, Tombstone  the key was deleted; stop, and do NOT fall through
//	                       to an older file, which would resurrect it
//
// An error is never one of those outcomes. A damaged file reports an error;
// it must never be reported as "not found", because that turns corruption into
// a plausible-looking answer and the operator never learns anything is wrong.
//
// The returned value is freshly allocated and is the caller's to keep.
func (r *Reader) Get(userKey []byte, seq uint64) (value []byte, kind ikey.Kind, found bool, err error) {
	// The Bloom filter is consulted before the index, because a negative answer
	// means the file can be skipped without reading anything at all.
	//
	// The asymmetry is the whole point and it is deliberate in the code as well
	// as the comment: the filter may only ever eliminate a file. It cannot
	// confirm one, it cannot supply a value, and it cannot turn a damaged file
	// into an absent key — a filter that fails to decode was already rejected at
	// Open, and a file with no filter answers "maybe" and is consulted in full.
	if !r.filter.MayContain(userKey) {
		r.filterSkips.Add(1)
		return nil, 0, false, nil
	}

	target := ikey.Seek(userKey, seq)

	// The candidate block is the first whose last key is >= the seek key.
	// Because the seek key sorts at or before every version of userKey, the
	// newest visible version — if the file has one — is the first entry >=
	// target, and that entry is in this block or the key is absent.
	i := sort.Search(len(r.index), func(i int) bool {
		return ikey.Compare(r.index[i].LastKey, target) >= 0
	})
	if i == len(r.index) {
		return nil, 0, false, nil // greater than everything in the file
	}

	r.blockReads.Add(1)
	block, err := r.readBlock(r.index[i].Offset, r.index[i].Length, MaxBlockSize,
		fmt.Sprintf("data block %d", i))
	if err != nil {
		return nil, 0, false, err
	}

	where := fmt.Sprintf("%s data block %d", r.name, i)
	p := block
	for n := 0; len(p) > 0; n++ {
		key, rest, err := takeBytes(p, "key", where)
		if err != nil {
			return nil, 0, false, err
		}
		val, rest, err := takeBytes(rest, "value", where)
		if err != nil {
			return nil, 0, false, err
		}
		p = rest

		if !ikey.Valid(key) {
			return nil, 0, false, fmt.Errorf("sstable %s: entry %d has a malformed key %s: %w",
				where, n, ikey.String(key), ErrCorrupt)
		}
		if ikey.Compare(key, target) < 0 {
			continue
		}
		// First entry at or after the seek key.
		if !bytesEqual(ikey.UserKey(key), userKey) {
			return nil, 0, false, nil
		}
		out := make([]byte, len(val))
		copy(out, val)
		return out, ikey.KindOf(key), true, nil
	}

	// The index said this block's last key is >= target, so the scan must
	// have found an entry >= target. Reaching here means the index and the
	// block disagree, which is damage the index's own checksum did not catch.
	return nil, 0, false, fmt.Errorf(
		"sstable %s: index says data block %d ends at %s but the block holds no key >= %s: %w",
		r.name, i, ikey.String(r.index[i].LastKey), ikey.String(target), ErrCorrupt)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Stats summarises a verified file.
type Stats struct {
	NumEntries  uint64
	NumBlocks   int
	FileSize    int64
	SmallestKey []byte
	LargestKey  []byte
	SmallestSeq uint64
	LargestSeq  uint64
}

// Verify reads every block, checks every checksum, and confirms that the file's
// contents match what its footer and index claim.
//
// The engine runs this on every SSTable at startup. It is not cheap — it is a
// full read of the file — and it is what Phase 3 pays instead of having a
// MANIFEST. Two things come out of it: damage is found at startup rather than
// at some later read, and the sequence-number range is recovered, which Phase
// 3 has nowhere else to get (docs/LSM.md §"Discovering SSTables").
func (r *Reader) Verify() (Stats, error) {
	st := Stats{NumBlocks: len(r.index), FileSize: r.size}

	var (
		prev  []byte
		count uint64
	)
	it := r.NewIterator()
	for it.Next() {
		key := it.Key()
		if prev != nil && ikey.Compare(prev, key) >= 0 {
			return Stats{}, fmt.Errorf("sstable %s: entries are not in increasing order: %s then %s: %w",
				r.name, ikey.String(prev), ikey.String(key), ErrCorrupt)
		}
		if count == 0 {
			st.SmallestKey = append([]byte(nil), key...)
			st.SmallestSeq = ikey.Seq(key)
			st.LargestSeq = ikey.Seq(key)
		}
		if seq := ikey.Seq(key); seq < st.SmallestSeq {
			st.SmallestSeq = seq
		} else if seq > st.LargestSeq {
			st.LargestSeq = seq
		}
		prev = append(prev[:0], key...)
		count++
	}
	if err := it.Err(); err != nil {
		return Stats{}, err
	}

	if count != r.footer.NumEntries {
		return Stats{}, fmt.Errorf("sstable %s: holds %d entries but its footer declares %d: %w",
			r.name, count, r.footer.NumEntries, ErrCorrupt)
	}
	st.NumEntries = count
	if prev != nil {
		st.LargestKey = append([]byte(nil), prev...)
	}
	// The last index entry names the file's greatest key; disagreeing with
	// the data is damage that neither block's checksum would catch on its own.
	if n := len(r.index); n > 0 && ikey.Compare(r.index[n-1].LastKey, st.LargestKey) != 0 {
		return Stats{}, fmt.Errorf("sstable %s: the index's last key is %s but the data ends at %s: %w",
			r.name, ikey.String(r.index[n-1].LastKey), ikey.String(st.LargestKey), ErrCorrupt)
	}
	return st, nil
}

// HasFilter reports whether this file carries a Bloom filter. A file written by
// Phase 3, or with WriterOptions.DisableFilter, does not.
func (r *Reader) HasFilter() bool { return r.filter.Present() }

// Filter returns the decoded filter. The zero Filter means the file has none,
// and answers "maybe" to every key.
func (r *Reader) Filter() bloom.Filter { return r.filter }

// MayContain reports whether the file might hold any version of userKey.
//
// False means definitely absent. True means nothing on its own. A file with no
// filter answers true for every key.
func (r *Reader) MayContain(userKey []byte) bool { return r.filter.MayContain(userKey) }

// BlockReads returns the number of data blocks this Reader has read since Open.
func (r *Reader) BlockReads() uint64 { return r.blockReads.Load() }

// FilterSkips returns the number of Gets the filter answered without touching
// the file.
func (r *Reader) FilterSkips() uint64 { return r.filterSkips.Load() }

// NumEntries returns the entry count the footer declares.
func (r *Reader) NumEntries() uint64 { return r.footer.NumEntries }

// NumBlocks returns the number of data blocks.
func (r *Reader) NumBlocks() int { return len(r.index) }

// Size returns the file size in bytes.
func (r *Reader) Size() int64 { return r.size }

// Name returns the file's base name, for diagnostics.
func (r *Reader) Name() string { return r.name }

// Close releases the file. It must not race with an in-flight read.
func (r *Reader) Close() error { return r.f.Close() }

// Iterator walks every entry in the file in internal-key order, across block
// boundaries. It is used by Verify and by the tests; Phase 4's compaction is
// what will use it in anger.
type Iterator struct {
	r     *Reader
	blk   int    // index of the block currently loaded; -1 before the first
	buf   []byte // remaining bytes of the loaded block
	key   []byte
	value []byte
	err   error
	done  bool
}

// NewIterator returns an iterator positioned before the first entry.
func (r *Reader) NewIterator() *Iterator { return &Iterator{r: r, blk: -1} }

// Next advances to the next entry and reports whether one exists. When it
// returns false, check Err: a false with a non-nil error means the file is
// damaged, not that the iteration finished.
func (it *Iterator) Next() bool {
	if it.err != nil || it.done {
		return false
	}
	for len(it.buf) == 0 {
		it.blk++
		if it.blk >= len(it.r.index) {
			it.done = true
			return false
		}
		e := it.r.index[it.blk]
		it.r.blockReads.Add(1)
		block, err := it.r.readBlock(e.Offset, e.Length, MaxBlockSize,
			fmt.Sprintf("data block %d", it.blk))
		if err != nil {
			it.err = err
			return false
		}
		it.buf = block
	}

	where := fmt.Sprintf("%s data block %d", it.r.name, it.blk)
	key, rest, err := takeBytes(it.buf, "key", where)
	if err != nil {
		it.err = err
		return false
	}
	value, rest, err := takeBytes(rest, "value", where)
	if err != nil {
		it.err = err
		return false
	}
	if !ikey.Valid(key) {
		it.err = fmt.Errorf("sstable %s: malformed internal key %s: %w", where, ikey.String(key), ErrCorrupt)
		return false
	}
	it.buf = rest
	it.key = key
	it.value = value
	return true
}

// Key returns the internal key at the current position. It aliases the block
// buffer and stays valid until the next call to Next.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the value at the current position, with the same lifetime as
// Key. A tombstone's value is zero length; the kind lives in the key.
func (it *Iterator) Value() []byte { return it.value }

// Err returns the failure that stopped the iteration, if any.
func (it *Iterator) Err() error { return it.err }
