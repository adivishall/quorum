package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/adivishall/quorum/internal/storage/ikey"
)

// Metadata describes a finished SSTable.
//
// SmallestSeq and LargestSeq are the sequence-number range the file covers.
// In Phase 4 they live in the MANIFEST (docs/DESIGN.md §6 AddFile); in Phase 3
// there is nowhere on disk to record them, so the engine recomputes them by
// scanning the file at startup. See docs/LSM.md §"Discovering SSTables".
type Metadata struct {
	NumEntries   uint64
	NumBlocks    int
	FileSize     int64
	SmallestKey  []byte // internal key
	LargestKey   []byte // internal key
	SmallestSeq  uint64
	LargestSeq   uint64
	DataBytes    int64
	IndexBytes   int64
	FilterBytes  int64
	LargestBlock int
}

// WriterOptions configures a Writer. The zero value is the documented default.
type WriterOptions struct {
	// BlockSize is the data-block target in bytes. A block is closed once it
	// reaches this size, so it always ends on an entry boundary. Zero selects
	// DefaultBlockSize.
	BlockSize int
}

func (o *WriterOptions) applyDefaults() {
	if o.BlockSize <= 0 {
		o.BlockSize = DefaultBlockSize
	}
}

// Writer builds an SSTable onto an io.Writer.
//
// Entries must be added in strictly increasing internal-key order. The writer
// does not sort: sorting here would mean buffering the whole file in memory,
// which is precisely what the ordered memtable exists to avoid. Feeding it an
// out-of-order key is a bug in the caller and is reported as one.
type Writer struct {
	w    io.Writer
	opts WriterOptions

	block []byte // the data block being accumulated
	index []byte // the index block being accumulated

	offset   int64 // bytes written to w so far
	lastKey  []byte
	blockOff int64 // offset of the block being accumulated

	meta Metadata
	err  error // latched: once the writer has failed it stays failed
}

// NewWriter returns a Writer appending an SSTable to w.
func NewWriter(w io.Writer, opts WriterOptions) *Writer {
	opts.applyDefaults()
	return &Writer{w: w, opts: opts}
}

// Add appends one entry. internalKey must sort strictly after the previous one.
//
// A tombstone is added with an empty value and a key whose kind is
// ikey.KindTombstone; the kind lives in the key, so a tombstone and a present
// key with an empty value are encoded identically on the value side and are
// still distinguishable.
func (w *Writer) Add(internalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if !ikey.Valid(internalKey) {
		return w.fail(fmt.Errorf("sstable: refusing to write malformed internal key %s",
			ikey.String(internalKey)))
	}
	if w.lastKey != nil && ikey.Compare(w.lastKey, internalKey) >= 0 {
		return w.fail(fmt.Errorf("%w: %s then %s",
			ErrNotSorted, ikey.String(w.lastKey), ikey.String(internalKey)))
	}

	if w.meta.NumEntries == 0 {
		w.meta.SmallestKey = append([]byte(nil), internalKey...)
		w.meta.SmallestSeq = ikey.Seq(internalKey)
		w.meta.LargestSeq = ikey.Seq(internalKey)
	}
	if seq := ikey.Seq(internalKey); seq < w.meta.SmallestSeq {
		w.meta.SmallestSeq = seq
	} else if seq > w.meta.LargestSeq {
		w.meta.LargestSeq = seq
	}

	w.block = appendEntry(w.block, internalKey, value)
	w.lastKey = append(w.lastKey[:0], internalKey...)
	w.meta.NumEntries++

	// Close the block once it reaches the target. The check is after the
	// append, which is what "rounded up to the next entry boundary" means:
	// an entry is never split, so a single entry larger than the target
	// becomes a block of its own.
	if len(w.block) >= w.opts.BlockSize {
		return w.flushBlock()
	}
	return nil
}

// flushBlock writes the accumulated data block and records its index entry.
func (w *Writer) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	if len(w.block) > w.meta.LargestBlock {
		w.meta.LargestBlock = len(w.block)
	}
	length := int64(len(w.block))
	if err := w.writeBlock(w.block); err != nil {
		return err
	}
	w.index = appendIndexEntry(w.index, w.lastKey, uint64(w.blockOff), uint64(length))

	w.meta.NumBlocks++
	w.meta.DataBytes += length + BlockTrailerSize
	w.block = w.block[:0]
	w.blockOff = w.offset
	return nil
}

// writeBlock writes a block followed by its checksum.
func (w *Writer) writeBlock(b []byte) error {
	if err := w.write(b); err != nil {
		return err
	}
	var sum [BlockTrailerSize]byte
	binary.LittleEndian.PutUint32(sum[:], blockChecksum(b))
	return w.write(sum[:])
}

func (w *Writer) write(b []byte) error {
	n, err := w.w.Write(b)
	w.offset += int64(n)
	if err != nil {
		return w.fail(fmt.Errorf("sstable: writing at offset %d: %w", w.offset, err))
	}
	if n != len(b) {
		return w.fail(fmt.Errorf("sstable: short write at offset %d: %w", w.offset, io.ErrShortWrite))
	}
	return nil
}

func (w *Writer) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return w.err
}

// Finish writes the pending data block, the filter block, the index block and
// the footer, and returns the file's metadata.
//
// After Finish the Writer is spent; a second call returns an error.
func (w *Writer) Finish() (Metadata, error) {
	if w.err != nil {
		return Metadata{}, w.err
	}
	if err := w.flushBlock(); err != nil {
		return Metadata{}, err
	}

	// Filter block. Phase 3 writes it empty; see the package comment. It
	// still carries a checksum so that every block in the file has the same
	// shape and the reader has one code path.
	filterOffset := w.offset
	if err := w.writeBlock(nil); err != nil {
		return Metadata{}, err
	}
	w.meta.FilterBytes = w.offset - filterOffset

	indexOffset := w.offset
	indexLength := int64(len(w.index))
	if err := w.writeBlock(w.index); err != nil {
		return Metadata{}, err
	}
	w.meta.IndexBytes = w.offset - indexOffset

	footer := Footer{
		FilterOffset: uint64(filterOffset),
		FilterLength: 0,
		IndexOffset:  uint64(indexOffset),
		IndexLength:  uint64(indexLength),
		NumEntries:   w.meta.NumEntries,
	}
	if err := w.write(footer.AppendTo(nil)); err != nil {
		return Metadata{}, err
	}

	w.meta.FileSize = w.offset
	w.meta.LargestKey = append([]byte(nil), w.lastKey...)
	if w.meta.NumEntries == 0 {
		w.meta.LargestKey = nil
	}

	meta := w.meta
	w.err = fmt.Errorf("sstable: writer already finished")
	return meta, nil
}

// Source supplies entries to WriteFile in internal-key order.
//
// It is the memtable iterator's shape, deliberately: a flush is "hand the
// frozen memtable's iterator to the SSTable writer", with no intermediate
// buffer and no sort.
type Source interface {
	Next() bool
	Key() []byte
	Value() []byte
}

// WriteFile creates path, writes every entry from src into it, and fsyncs it.
//
// It does NOT rename or publish the file, and it does not fsync the directory.
// Deciding when a file becomes visible is the engine's business, because that
// decision is the crash window (see docs/LSM.md §"Flush"); a writer that
// published its own output would take that decision away from the layer that
// has to reason about it.
//
// On any failure the partial file is removed, so a failed flush does not leave
// something that a later startup has to classify.
func WriteFile(path string, src Source, opts WriterOptions) (Metadata, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return Metadata{}, fmt.Errorf("sstable: creating %s: %w", path, err)
	}

	cleanup := func(cause error) (Metadata, error) {
		_ = f.Close()
		_ = os.Remove(path)
		return Metadata{}, cause
	}

	w := NewWriter(f, opts)
	for src.Next() {
		if err := w.Add(src.Key(), src.Value()); err != nil {
			return cleanup(err)
		}
	}
	meta, err := w.Finish()
	if err != nil {
		return cleanup(err)
	}
	if err := f.Sync(); err != nil {
		return cleanup(fmt.Errorf("sstable: syncing %s: %w", path, err))
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return Metadata{}, fmt.Errorf("sstable: closing %s: %w", path, err)
	}
	return meta, nil
}

// SyncDir fsyncs a directory so that a rename within it is durable.
//
// A rename is atomic but not durable: after a crash the directory entry can
// still be the old one. For a freshly published SSTable that would mean a file
// whose contents are on disk but whose name is not, which startup would sweep
// as an orphan — correct, but only because the WAL still holds everything the
// file contained. Flushing the directory is what makes the publication itself
// survive.
func SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
