package sstable_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/storage/ikey"
	"github.com/adivishall/quorum/internal/storage/sstable"
)

// ---------------------------------------------------------------- helpers

// entry is one (key, seq, kind, value) tuple in a test table.
type entry struct {
	key   string
	seq   uint64
	kind  ikey.Kind
	value string
}

func put(key string, seq uint64, value string) entry {
	return entry{key: key, seq: seq, kind: ikey.KindValue, value: value}
}

func del(key string, seq uint64) entry {
	return entry{key: key, seq: seq, kind: ikey.KindTombstone}
}

// src feeds entries to the writer, in the order given.
type src struct {
	entries []entry
	i       int
	key     []byte
	value   []byte
}

func (s *src) Next() bool {
	if s.i >= len(s.entries) {
		return false
	}
	e := s.entries[s.i]
	s.i++
	s.key = ikey.Encode(nil, []byte(e.key), e.seq, e.kind)
	s.value = []byte(e.value)
	return true
}
func (s *src) Key() []byte   { return s.key }
func (s *src) Value() []byte { return s.value }

// build writes an SSTable containing entries (which must already be sorted by
// the internal-key ordering) and returns its path.
func build(t *testing.T, entries []entry, blockSize int) (string, sstable.Metadata) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	meta, err := sstable.WriteFile(path, &src{entries: entries},
		sstable.WriterOptions{BlockSize: blockSize})
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path, meta
}

func openReader(t *testing.T, path string) *sstable.Reader {
	t.Helper()
	r, err := sstable.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", filepath.Base(path), err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// mustGet asserts a present value.
func mustGet(t *testing.T, r *sstable.Reader, key, want string) {
	t.Helper()
	val, kind, found, err := r.Get([]byte(key), ikey.MaxSeq)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !found {
		t.Fatalf("Get(%q): not found, want %q", key, want)
	}
	if kind != ikey.KindValue {
		t.Fatalf("Get(%q): kind = %v, want value", key, kind)
	}
	if string(val) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, val, want)
	}
}

func mustTombstone(t *testing.T, r *sstable.Reader, key string) {
	t.Helper()
	_, kind, found, err := r.Get([]byte(key), ikey.MaxSeq)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !found {
		t.Fatalf("Get(%q): not found, want a tombstone (a deleted key must be FOUND, "+
			"or an older file would resurrect it)", key)
	}
	if kind != ikey.KindTombstone {
		t.Fatalf("Get(%q): kind = %v, want tombstone", key, kind)
	}
}

func mustAbsent(t *testing.T, r *sstable.Reader, key string) {
	t.Helper()
	_, _, found, err := r.Get([]byte(key), ikey.MaxSeq)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if found {
		t.Fatalf("Get(%q): found, want absent", key)
	}
}

// corrupt flips every bit of one byte at off. Negative offsets count from the
// end of the file.
func corrupt(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if off < 0 {
		off = info.Size() + off
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

// writeU64At overwrites an eight-byte little-endian field.
func writeU64At(t *testing.T, path string, off int64, v uint64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if off < 0 {
		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		off = info.Size() + off
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

// footer field offsets, counted back from the end of the file.
const (
	offFilterOffset = -sstable.FooterSize + 0
	offFilterLength = -sstable.FooterSize + 8
	offIndexOffset  = -sstable.FooterSize + 16
	offIndexLength  = -sstable.FooterSize + 24
	offNumEntries   = -sstable.FooterSize + 32
	offMagic        = -sstable.FooterSize + 40
)

// assertRefused requires that opening or verifying the file fails with a
// corruption-class error, and never with a silent "key not found".
func assertRefused(t *testing.T, path, what string) error {
	t.Helper()
	r, err := sstable.Open(path)
	if err == nil {
		_, err = r.Verify()
		if err == nil {
			// The structure survived; a read must still not invent an answer.
			_, _, _, gerr := r.Get([]byte("key0000"), ikey.MaxSeq)
			err = gerr
		}
		_ = r.Close()
	}
	if err == nil {
		t.Fatalf("%s was accepted; corruption must never be silently tolerated", what)
	}
	if !errors.Is(err, sstable.ErrCorrupt) && !errors.Is(err, sstable.ErrBadMagic) {
		t.Fatalf("%s gave %v, want ErrCorrupt or ErrBadMagic", what, err)
	}
	return err
}

// ---------------------------------------------------------------- writer

func TestEmptySSTable(t *testing.T) {
	path, meta := build(t, nil, 0)

	if meta.NumEntries != 0 || meta.NumBlocks != 0 {
		t.Fatalf("empty table metadata = %+v, want 0 entries and 0 blocks", meta)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != meta.FileSize {
		t.Fatalf("Metadata.FileSize = %d, on-disk size = %d", meta.FileSize, info.Size())
	}
	// Empty filter block + its checksum, empty index block + its checksum,
	// then the footer. Nothing else is legal.
	if want := int64(2*sstable.BlockTrailerSize + sstable.FooterSize); info.Size() != want {
		t.Fatalf("empty table is %d bytes, want exactly %d", info.Size(), want)
	}

	r := openReader(t, path)
	if r.NumEntries() != 0 || r.NumBlocks() != 0 {
		t.Fatalf("reader sees %d entries in %d blocks, want 0 and 0", r.NumEntries(), r.NumBlocks())
	}
	mustAbsent(t, r, "anything")
	if st, err := r.Verify(); err != nil || st.NumEntries != 0 {
		t.Fatalf("Verify = (%+v, %v), want a clean empty table", st, err)
	}
	it := r.NewIterator()
	if it.Next() {
		t.Fatal("an empty table iterated an entry")
	}
	if it.Err() != nil {
		t.Fatalf("iterator error on an empty table: %v", it.Err())
	}
}

func TestSingleEntry(t *testing.T) {
	path, meta := build(t, []entry{put("k", 1, "v")}, 0)
	if meta.NumEntries != 1 || meta.NumBlocks != 1 {
		t.Fatalf("metadata = %+v, want 1 entry in 1 block", meta)
	}
	if meta.SmallestSeq != 1 || meta.LargestSeq != 1 {
		t.Fatalf("sequence range = [%d,%d], want [1,1]", meta.SmallestSeq, meta.LargestSeq)
	}

	r := openReader(t, path)
	mustGet(t, r, "k", "v")
	mustAbsent(t, r, "j")
	mustAbsent(t, r, "l")
	if _, err := r.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestManyEntriesAcrossBlocks(t *testing.T) {
	const n = 2000
	entries := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, put(fmt.Sprintf("key%06d", i), uint64(i+1), fmt.Sprintf("value-%06d", i)))
	}
	path, meta := build(t, entries, 0)

	if meta.NumEntries != n {
		t.Fatalf("NumEntries = %d, want %d", meta.NumEntries, n)
	}
	if meta.NumBlocks < 2 {
		t.Fatalf("NumBlocks = %d; %d entries must not fit in one %d-byte block",
			meta.NumBlocks, n, sstable.DefaultBlockSize)
	}
	if meta.SmallestSeq != 1 || meta.LargestSeq != n {
		t.Fatalf("sequence range = [%d,%d], want [1,%d]", meta.SmallestSeq, meta.LargestSeq, n)
	}

	r := openReader(t, path)
	for i := 0; i < n; i++ {
		mustGet(t, r, fmt.Sprintf("key%06d", i), fmt.Sprintf("value-%06d", i))
	}
	mustAbsent(t, r, "key000000x")
	mustAbsent(t, r, "aaa")
	mustAbsent(t, r, "zzz")

	st, err := r.Verify()
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if st.NumEntries != n || st.NumBlocks != meta.NumBlocks {
		t.Fatalf("Verify = %+v, want %d entries in %d blocks", st, n, meta.NumBlocks)
	}
}

// TestBlockBoundaries checks the rule from docs/DESIGN.md §4: a block is closed
// once it reaches the target, so it ends on an entry boundary and may exceed
// the target by up to one entry.
func TestBlockBoundaries(t *testing.T) {
	const blockSize = 256
	entries := make([]entry, 0, 100)
	for i := 0; i < 100; i++ {
		entries = append(entries, put(fmt.Sprintf("key%04d", i), uint64(i+1), "0123456789"))
	}
	path, meta := build(t, entries, blockSize)

	if meta.NumBlocks < 5 {
		t.Fatalf("NumBlocks = %d, want several at a %d-byte target", meta.NumBlocks, blockSize)
	}
	// One entry is about 4+8+1+10+1 = 24 bytes, so no block should overshoot
	// the target by more than one entry's worth.
	if meta.LargestBlock > blockSize+64 {
		t.Fatalf("largest block is %d bytes, more than one entry past the %d-byte target",
			meta.LargestBlock, blockSize)
	}

	r := openReader(t, path)
	for i := 0; i < 100; i++ {
		mustGet(t, r, fmt.Sprintf("key%04d", i), "0123456789")
	}
	if _, err := r.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// TestOversizedEntryIsNeverSplit is the other half of the boundary rule. A
// block is closed once it REACHES the target, so an entry larger than the
// target makes the block it lands in oversized rather than being split across
// two. The block that follows it starts clean.
func TestOversizedEntryIsNeverSplit(t *testing.T) {
	big := string(bytes.Repeat([]byte("x"), 1<<20))

	// The big entry first: it alone takes block 0 past the target.
	path, meta := build(t, []entry{
		put("a", 1, big),
		put("b", 2, "small"),
		put("c", 3, "small"),
	}, 4096)
	if meta.NumBlocks != 2 {
		t.Fatalf("NumBlocks = %d, want 2 (the oversized entry closes block 0; b and c share block 1)",
			meta.NumBlocks)
	}
	if meta.LargestBlock < 1<<20 {
		t.Fatalf("largest block is %d bytes; the 1 MiB entry was split", meta.LargestBlock)
	}
	r := openReader(t, path)
	mustGet(t, r, "a", big)
	mustGet(t, r, "b", "small")
	mustGet(t, r, "c", "small")
	if _, err := r.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	// The big entry in the middle: it joins the block already in progress and
	// closes it, which is the documented "rounded up to the next entry
	// boundary" behaviour rather than a third block.
	path2, meta2 := build(t, []entry{
		put("a", 1, "small"),
		put("b", 2, big),
		put("c", 3, "small"),
	}, 4096)
	if meta2.NumBlocks != 2 {
		t.Fatalf("NumBlocks = %d, want 2", meta2.NumBlocks)
	}
	if meta2.LargestBlock < 1<<20 {
		t.Fatalf("largest block is %d bytes; the 1 MiB entry was split", meta2.LargestBlock)
	}
	r2 := openReader(t, path2)
	mustGet(t, r2, "a", "small")
	mustGet(t, r2, "b", big)
	mustGet(t, r2, "c", "small")
	if _, err := r2.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestTombstonesRoundTrip(t *testing.T) {
	entries := []entry{
		put("a", 1, "value-a"),
		del("b", 2),
		put("c", 3, ""), // an empty value is NOT a tombstone
		del("d", 4),
	}
	path, _ := build(t, entries, 0)
	r := openReader(t, path)

	mustGet(t, r, "a", "value-a")
	mustTombstone(t, r, "b")
	mustGet(t, r, "c", "")
	mustTombstone(t, r, "d")

	// The empty value and the tombstone are encoded with the same zero-length
	// value; only the kind in the internal key tells them apart.
	val, kind, _, err := r.Get([]byte("c"), ikey.MaxSeq)
	if err != nil {
		t.Fatal(err)
	}
	if kind != ikey.KindValue || val == nil || len(val) != 0 {
		t.Fatalf("empty value came back as (%v, %v); it must be a present key", val, kind)
	}
}

func TestMultipleVersionsNewestFirst(t *testing.T) {
	// Sorted by the internal-key ordering: same user key, sequence descending.
	entries := []entry{
		put("k", 9, "newest"),
		del("k", 5),
		put("k", 1, "oldest"),
		put("z", 2, "other"),
	}
	path, meta := build(t, entries, 0)
	if meta.SmallestSeq != 1 || meta.LargestSeq != 9 {
		t.Fatalf("sequence range = [%d,%d], want [1,9]", meta.SmallestSeq, meta.LargestSeq)
	}

	r := openReader(t, path)
	mustGet(t, r, "k", "newest")

	// A bounded read sees the version that was current at that sequence.
	if _, kind, found, err := r.Get([]byte("k"), 5); err != nil || !found || kind != ikey.KindTombstone {
		t.Fatalf("Get(k, seq=5) = (%v, %v, %v), want a tombstone", kind, found, err)
	}
	val, _, _, err := r.Get([]byte("k"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "oldest" {
		t.Fatalf("Get(k, seq=4) = %q, want %q", val, "oldest")
	}
}

func TestWriterRejectsUnsortedKeys(t *testing.T) {
	var buf bytes.Buffer
	w := sstable.NewWriter(&buf, sstable.WriterOptions{})

	if err := w.Add(ikey.Encode(nil, []byte("b"), 1, ikey.KindValue), []byte("v")); err != nil {
		t.Fatal(err)
	}
	err := w.Add(ikey.Encode(nil, []byte("a"), 2, ikey.KindValue), []byte("v"))
	if !errors.Is(err, sstable.ErrNotSorted) {
		t.Fatalf("Add(out of order) = %v, want ErrNotSorted", err)
	}
	// The failure latches: an SSTable built from a partially-rejected stream
	// must not be finishable.
	if _, err := w.Finish(); !errors.Is(err, sstable.ErrNotSorted) {
		t.Fatalf("Finish after a rejected Add = %v, want the latched ErrNotSorted", err)
	}
}

func TestWriterRejectsDuplicateKey(t *testing.T) {
	var buf bytes.Buffer
	w := sstable.NewWriter(&buf, sstable.WriterOptions{})
	k := ikey.Encode(nil, []byte("a"), 1, ikey.KindValue)
	if err := w.Add(k, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(k, []byte("v")); !errors.Is(err, sstable.ErrNotSorted) {
		t.Fatalf("Add(duplicate) = %v, want ErrNotSorted (strictly increasing)", err)
	}
}

func TestWriterRejectsMalformedKey(t *testing.T) {
	var buf bytes.Buffer
	w := sstable.NewWriter(&buf, sstable.WriterOptions{})
	if err := w.Add([]byte{0x01, 0x02}, []byte("v")); err == nil {
		t.Fatal("Add accepted a key too short to be an internal key")
	}
}

func TestWriteFileRemovesItsOutputOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	// A source that yields keys in the wrong order fails mid-write.
	bad := &src{entries: []entry{put("b", 1, "v"), put("a", 2, "v")}}
	if _, err := sstable.WriteFile(path, bad, sstable.WriterOptions{}); !errors.Is(err, sstable.ErrNotSorted) {
		t.Fatalf("WriteFile = %v, want ErrNotSorted", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a failed WriteFile left %s behind; startup would have to classify it", path)
	}
}

func TestWriteFileRefusesToOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")
	if _, err := sstable.WriteFile(path, &src{entries: []entry{put("a", 1, "v")}}, sstable.WriterOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := sstable.WriteFile(path, &src{entries: []entry{put("a", 1, "v")}}, sstable.WriterOptions{}); err == nil {
		t.Fatal("WriteFile overwrote an existing SSTable; an SSTable is written once and never modified")
	}
}

// ---------------------------------------------------------------- reader

func TestIteratorCoversEveryEntryInOrder(t *testing.T) {
	const n = 500
	entries := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		e := put(fmt.Sprintf("key%05d", i), uint64(i+1), fmt.Sprintf("v%05d", i))
		if i%5 == 0 {
			e = del(fmt.Sprintf("key%05d", i), uint64(i+1))
		}
		entries = append(entries, e)
	}
	path, _ := build(t, entries, 512)
	r := openReader(t, path)

	it := r.NewIterator()
	var (
		prev  []byte
		count int
	)
	for it.Next() {
		if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("iteration out of order at %d", count)
		}
		want := entries[count]
		if got := string(ikey.UserKey(it.Key())); got != want.key {
			t.Fatalf("entry %d key = %q, want %q", count, got, want.key)
		}
		if got := ikey.KindOf(it.Key()); got != want.kind {
			t.Fatalf("entry %d kind = %v, want %v", count, got, want.kind)
		}
		if got := string(it.Value()); got != want.value {
			t.Fatalf("entry %d value = %q, want %q", count, got, want.value)
		}
		prev = append(prev[:0], it.Key()...)
		count++
	}
	if it.Err() != nil {
		t.Fatalf("iterator error: %v", it.Err())
	}
	if count != n {
		t.Fatalf("iterated %d entries, want %d", count, n)
	}
}

func TestLookupsAroundBlockBoundaries(t *testing.T) {
	const n = 300
	entries := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		// Only even keys exist, so every odd key probes a gap.
		entries = append(entries, put(fmt.Sprintf("key%04d", i*2), uint64(i+1), "v"))
	}
	path, meta := build(t, entries, 128)
	if meta.NumBlocks < 10 {
		t.Fatalf("NumBlocks = %d, want many small blocks for this test to mean anything", meta.NumBlocks)
	}
	r := openReader(t, path)

	for i := 0; i < n; i++ {
		mustGet(t, r, fmt.Sprintf("key%04d", i*2), "v")
		mustAbsent(t, r, fmt.Sprintf("key%04d", i*2+1))
	}
}

func TestOpaqueKeysRoundTrip(t *testing.T) {
	raw := [][]byte{
		{0x00}, {0x00, 0x01}, {0x01}, []byte(" "), []byte("a\nb"), []byte("a\x00b"),
		[]byte("ключ"), {0xc3, 0x28}, {0xff}, {0xff, 0xff},
	}
	// Sort by user key so the writer accepts them.
	sorted := append([][]byte(nil), raw...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && bytes.Compare(sorted[j-1], sorted[j]) > 0; j-- {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
		}
	}
	entries := make([]entry, 0, len(sorted))
	for i, k := range sorted {
		entries = append(entries, put(string(k), uint64(i+1), fmt.Sprintf("v%d", i)))
	}

	path, _ := build(t, entries, 0)
	r := openReader(t, path)
	for i, k := range sorted {
		mustGet(t, r, string(k), fmt.Sprintf("v%d", i))
	}
}

// ---------------------------------------------------------------- corruption

func TestBadMagicIsAFormatError(t *testing.T) {
	path, _ := build(t, []entry{put("k", 1, "v")}, 0)
	writeU64At(t, path, offMagic, 0xdeadbeefdeadbeef)

	_, err := sstable.Open(path)
	if !errors.Is(err, sstable.ErrBadMagic) {
		t.Fatalf("Open with a wrong magic = %v, want ErrBadMagic", err)
	}
	// docs/DESIGN.md §4: a wrong magic is a format/version error, not
	// corruption. The two are separate sentinels so a diagnosis can tell "this
	// is not one of our files" from "this file is damaged".
	if errors.Is(err, sstable.ErrCorrupt) {
		t.Fatal("a wrong magic was reported as corruption; the two are different diagnoses")
	}
}

func TestTruncatedFileIsRefused(t *testing.T) {
	for _, keep := range []int64{0, 1, sstable.FooterSize - 1, sstable.FooterSize + 1} {
		t.Run(fmt.Sprint(keep), func(t *testing.T) {
			path, _ := build(t, []entry{put("k", 1, "v")}, 0)
			if err := os.Truncate(path, keep); err != nil {
				t.Fatal(err)
			}
			if _, err := sstable.Open(path); err == nil {
				t.Fatalf("a %d-byte file was accepted as an SSTable", keep)
			}
		})
	}
}

func TestCorruptFooterFieldsAreRefused(t *testing.T) {
	cases := []struct {
		name  string
		off   int64
		value uint64
	}{
		{"index offset past the file", offIndexOffset, 1 << 40},
		{"index offset inside the data", offIndexOffset, 4},
		{"index length absurd", offIndexLength, 1 << 40},
		{"index length short by one", offIndexLength, 1},
		{"filter offset past the index", offFilterOffset, 1 << 40},
		{"filter length overlapping the index", offFilterLength, 1 << 20},
		{"entry count inflated", offNumEntries, 999999},
		{"entry count zeroed", offNumEntries, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := build(t, []entry{put("key0000", 1, "v"), put("key0001", 2, "v")}, 0)
			writeU64At(t, path, tc.off, tc.value)
			assertRefused(t, path, tc.name)
		})
	}
}

func TestFooterOverflowIsRefused(t *testing.T) {
	path, _ := build(t, []entry{put("key0000", 1, "v")}, 0)
	// Offsets chosen so that offset+length would wrap around uint64. The
	// bounds check has to happen in arithmetic that cannot overflow, or the
	// wrapped value looks like a small, plausible offset.
	writeU64At(t, path, offIndexOffset, ^uint64(0)-2)
	writeU64At(t, path, offIndexLength, 8)
	assertRefused(t, path, "overflowing index extent")
}

func TestCorruptIndexBlockIsRefused(t *testing.T) {
	path, meta := build(t, []entry{put("key0000", 1, "v"), put("key0001", 2, "v")}, 0)
	// The index block sits immediately before the footer.
	indexStart := meta.FileSize - sstable.FooterSize - meta.IndexBytes
	corrupt(t, path, indexStart+2)
	err := assertRefused(t, path, "a corrupted index block")
	if !errors.Is(err, sstable.ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestCorruptIndexChecksumIsRefused(t *testing.T) {
	path, _ := build(t, []entry{put("key0000", 1, "v")}, 0)
	// The index block's checksum is the four bytes just before the footer.
	corrupt(t, path, -sstable.FooterSize-1)
	assertRefused(t, path, "a corrupted index checksum")
}

func TestCorruptFilterChecksumIsRefused(t *testing.T) {
	path, meta := build(t, []entry{put("key0000", 1, "v")}, 0)
	// Phase 3's filter block is empty, so its four checksum bytes sit
	// immediately before the index block. Damaging them must still be caught:
	// a block that carries no data still carries a checksum, and a reader
	// that skipped verifying it would be skipping a check it advertises.
	filterStart := meta.FileSize - sstable.FooterSize - meta.IndexBytes - meta.FilterBytes
	corrupt(t, path, filterStart)

	_, err := sstable.Open(path)
	if !errors.Is(err, sstable.ErrCorrupt) {
		t.Fatalf("Open with a damaged filter checksum = %v, want ErrCorrupt. "+
			"The filter block is empty in Phase 3 and nothing reads it, but leaving its "+
			"checksum unverified would leave a region of the file where damage is invisible.", err)
	}
}

func TestCorruptDataBlockIsRefusedNotReportedAsMissing(t *testing.T) {
	const n = 200
	entries := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, put(fmt.Sprintf("key%04d", i), uint64(i+1), "value"))
	}
	path, _ := build(t, entries, 0)

	// Damage a byte well inside the first data block.
	corrupt(t, path, 40)

	r := openReader(t, path)

	// Verify must catch it.
	if _, err := r.Verify(); !errors.Is(err, sstable.ErrCorrupt) {
		t.Fatalf("Verify on a damaged data block = %v, want ErrCorrupt", err)
	}

	// And so must a read that lands on that block. The specific failure this
	// guards against is the worst one available: returning "not found" for a
	// key that is present but unreadable.
	var sawError bool
	for i := 0; i < n; i++ {
		_, _, found, err := r.Get([]byte(fmt.Sprintf("key%04d", i)), ikey.MaxSeq)
		if err != nil {
			if !errors.Is(err, sstable.ErrCorrupt) {
				t.Fatalf("Get(key%04d) = %v, want ErrCorrupt", i, err)
			}
			sawError = true
			continue
		}
		if !found {
			t.Fatalf("Get(key%04d) reported the key absent from a damaged file; "+
				"corruption must never be presented as 'key not found'", i)
		}
	}
	if !sawError {
		t.Fatal("no read reached the damaged block; the test is not exercising what it claims")
	}
}

func TestCorruptDataBlockChecksumIsRefused(t *testing.T) {
	entries := []entry{put("key0000", 1, "value"), put("key0001", 2, "value")}
	path, meta := build(t, entries, 0)

	// The first (and only) data block's checksum sits at the end of the data
	// region, just before the filter block.
	sumOff := meta.DataBytes - sstable.BlockTrailerSize
	corrupt(t, path, sumOff)

	r := openReader(t, path)
	if _, err := r.Verify(); !errors.Is(err, sstable.ErrCorrupt) {
		t.Fatalf("Verify = %v, want ErrCorrupt", err)
	}
	if _, _, _, err := r.Get([]byte("key0000"), ikey.MaxSeq); !errors.Is(err, sstable.ErrCorrupt) {
		t.Fatalf("Get = %v, want ErrCorrupt", err)
	}
}

// TestMalformedVarintsAreRefused rewrites an entry's key-length varint to
// claim more bytes than the block holds. The checksum is recomputed so the
// damage is invisible to it — this is the case where only strict decoding
// catches the problem.
func TestMalformedVarintsAreRefused(t *testing.T) {
	entries := []entry{put("key0000", 1, "value0"), put("key0001", 2, "value1")}
	path, meta := build(t, entries, 0)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blockLen := int(meta.DataBytes) - sstable.BlockTrailerSize
	block := append([]byte(nil), raw[:blockLen]...)

	// The first byte is the first entry's key-length uvarint. A length of 0x7f
	// is a single-byte varint claiming 127 bytes, far more than remains.
	block[0] = 0x7f

	out := append([]byte(nil), block...)
	var sum [4]byte
	binary.LittleEndian.PutUint32(sum[:], crc32cOf(block))
	out = append(out, sum[:]...)
	out = append(out, raw[blockLen+sstable.BlockTrailerSize:]...)
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}

	err = assertRefused(t, path, "a block whose key length overruns the block")
	if !errors.Is(err, sstable.ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

// TestIndexDisagreeingWithDataIsRefused rewrites an index entry's block length
// so that the index and the data no longer describe the same file, then fixes
// the index's own checksum. Each block is individually intact; only the
// cross-check catches it.
func TestIndexDisagreeingWithDataIsRefused(t *testing.T) {
	entries := []entry{put("key0000", 1, "value0"), put("key0001", 2, "value1")}
	path, meta := build(t, entries, 0)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	indexStart := int(meta.FileSize - sstable.FooterSize - meta.IndexBytes)
	indexLen := int(meta.IndexBytes) - sstable.BlockTrailerSize
	index := append([]byte(nil), raw[indexStart:indexStart+indexLen]...)

	// The last byte of the single index entry is the block-length uvarint (the
	// data block is well under 128 bytes, so it is one byte).
	index[len(index)-1]--

	out := append([]byte(nil), raw[:indexStart]...)
	out = append(out, index...)
	var sum [4]byte
	binary.LittleEndian.PutUint32(sum[:], crc32cOf(index))
	out = append(out, sum[:]...)
	out = append(out, raw[indexStart+indexLen+sstable.BlockTrailerSize:]...)
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}

	assertRefused(t, path, "an index that disagrees with the data region")
}

func TestMissingFileIsAnError(t *testing.T) {
	if _, err := sstable.Open(filepath.Join(t.TempDir(), "nope.sst")); err == nil {
		t.Fatal("Open succeeded on a file that does not exist")
	}
}

func TestRandomSingleByteDamageIsAlwaysDetected(t *testing.T) {
	const n = 60
	entries := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, put(fmt.Sprintf("key%04d", i), uint64(i+1), fmt.Sprintf("value%04d", i)))
	}
	reference, _ := build(t, entries, 256)
	raw, err := os.ReadFile(reference)
	if err != nil {
		t.Fatal(err)
	}

	// Every byte, not a sample: the claim being checked is that the file has
	// no region a check does not cover, and a sample cannot establish that.
	path := filepath.Join(t.TempDir(), "damaged.sst")
	for off := 0; off < len(raw); off++ {
		damaged := append([]byte(nil), raw...)
		damaged[off] ^= 0xff
		if err := os.WriteFile(path, damaged, 0o644); err != nil {
			t.Fatal(err)
		}

		r, err := sstable.Open(path)
		if err != nil {
			continue // caught at open, which is the best case
		}
		_, verr := r.Verify()
		if verr == nil {
			// Verify reads every byte of every block and checks every
			// checksum. If it passes, the flipped byte must have been in a
			// region nothing covers — there is no such region, so this is a
			// real failure.
			_ = r.Close()
			t.Fatalf("flipping byte %d of %d produced a file that Open and Verify both accepted",
				off, len(raw))
		}
		if !errors.Is(verr, sstable.ErrCorrupt) && !errors.Is(verr, sstable.ErrBadMagic) {
			_ = r.Close()
			t.Fatalf("flipping byte %d gave %v, want ErrCorrupt or ErrBadMagic", off, verr)
		}
		_ = r.Close()
	}
}

// crc32cOf mirrors the package's block checksum so a test can recompute one
// after deliberately editing a block.
func crc32cOf(b []byte) uint32 { return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)) }
