package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/sstable"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// ------------------------------------------------- conformance and concurrency

// newLSMStore adapts LSMStore to the shared suites with the documented
// defaults: a 4 MiB memtable, so most of the suite never reaches an SSTable.
func newLSMStore(tb testing.TB, opts storage.Options) storage.Store {
	tb.Helper()
	s, err := storage.OpenLSMStore(tb.TempDir(), opts)
	if err != nil {
		tb.Fatalf("OpenLSMStore: %v", err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

// The "across SSTables" variants of the shared suites run the same assertions
// against a store that is constantly flushing. Two different thresholds are
// used, for a reason worth stating:
//
//   - The conformance suite is small — a few dozen operations per subtest — so
//     it can afford flushEveryWrite, where every single mutation becomes its
//     own SSTable. That is the strongest available check: no conformance
//     assertion is answered from memory.
//   - The concurrency suite performs thousands of operations. With no Bloom
//     filter and no compaction until Phase 4, a lookup consults every table,
//     so one table per write would make it quadratic — thousands of files and
//     millions of block reads. That cost is a real Phase 3 limitation and is
//     documented as one; it is not something to rediscover as a test timeout.
//     It uses a 16 KiB memtable, which still produces dozens of tables.
const (
	flushEveryWrite = 1
	smallMemTable   = 16 << 10
	smallBlock      = 256
)

func newLSMStoreWith(tb testing.TB, opts storage.Options, memTableSize int64, blockSize int) storage.Store {
	tb.Helper()
	opts.MemTableSize = memTableSize
	opts.BlockSize = blockSize
	s, err := storage.OpenLSMStore(tb.TempDir(), opts)
	if err != nil {
		tb.Fatalf("OpenLSMStore: %v", err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

// newFlushingLSMStore turns every mutation into its own SSTable, so that the
// Phase 1 conformance assertions are answered by the memtable, the immutable
// memtable, the flush, the SSTable writer, the SSTable reader and the
// multi-file merge rather than by a Go map.
func newFlushingLSMStore(tb testing.TB, opts storage.Options) storage.Store {
	return newLSMStoreWith(tb, opts, flushEveryWrite, 64)
}

// newSmallLSMStore flushes often enough to exercise the same paths under the
// concurrency suite's much larger operation counts.
func newSmallLSMStore(tb testing.TB, opts storage.Options) storage.Store {
	return newLSMStoreWith(tb, opts, smallMemTable, smallBlock)
}

func TestLSMStoreConformance(t *testing.T) {
	runConformance(t, newLSMStore)
}

// TestLSMStoreConformanceAcrossSSTables is the same suite with every write
// forced onto disk. If this diverges from the one above, the engine changed a
// client-visible semantic by going to disk, which is the thing Phase 3 is not
// allowed to do.
func TestLSMStoreConformanceAcrossSSTables(t *testing.T) {
	runConformance(t, newFlushingLSMStore)
}

func TestLSMStoreConcurrency(t *testing.T) {
	runConcurrency(t, newLSMStore)
}

func TestLSMStoreConcurrencyAcrossSSTables(t *testing.T) {
	runConcurrency(t, newSmallLSMStore)
}

// ---------------------------------------------------------------- helpers

func openLSM(t *testing.T, dir string, opts storage.Options) *storage.LSMStore {
	t.Helper()
	s, err := storage.OpenLSMStore(dir, opts)
	if err != nil {
		t.Fatalf("OpenLSMStore(%s): %v", dir, err)
	}
	return s
}

// lsmOpts returns options with a memtable small enough to force flushes at a
// predictable rate, and a sync mode that survives SIGKILL.
func lsmOpts(memTableSize int64) storage.Options {
	o := storage.DefaultOptions()
	o.MemTableSize = memTableSize
	o.BlockSize = 256
	o.WAL.SyncMode = wal.SyncBatch
	return o
}

func mustFlush(t *testing.T, s *storage.LSMStore) {
	t.Helper()
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func sstFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------- flush

func TestFlushProducesAnSSTable(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	if got := len(sstFiles(t, dir)); got != 0 {
		t.Fatalf("%d SSTables exist before any flush", got)
	}

	mustFlush(t, s)

	files := sstFiles(t, dir)
	if len(files) != 1 || files[0] != "000001.sst" {
		t.Fatalf("after one flush the directory holds %v, want [000001.sst]", files)
	}
	info := s.SSTables()
	if len(info) != 1 || info[0].Entries != 2 {
		t.Fatalf("SSTables() = %+v, want one table with 2 entries", info)
	}
	if info[0].SmallestSeq != 1 || info[0].LargestSeq != 2 {
		t.Fatalf("sequence range = [%d,%d], want [1,2]", info[0].SmallestSeq, info[0].LargestSeq)
	}

	// Reads keep working, now served from the file.
	assertValue(t, s, "a", "1")
	assertValue(t, s, "b", "2")
	if s.MemTableSize() != 0 {
		t.Fatalf("the memtable is %d bytes after a flush, want empty", s.MemTableSize())
	}
}

func TestFlushOfAnEmptyMemTableWritesNothing(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	mustFlush(t, s)
	mustFlush(t, s)
	if got := sstFiles(t, dir); len(got) != 0 {
		t.Fatalf("flushing an empty memtable produced %v; an empty SSTable is never written", got)
	}
}

func TestAutomaticFlushOnMemTableSize(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(4096))
	defer func() { _ = s.Close() }()

	const n = 400
	for i := 0; i < n; i++ {
		mustPut(t, s, fmt.Sprintf("key%04d", i), fmt.Sprintf("value-%04d-padding-padding", i))
	}

	tables := s.SSTables()
	if len(tables) < 2 {
		t.Fatalf("%d writes at a 4 KiB memtable produced %d SSTables, want several", n, len(tables))
	}
	// Sequence ranges must ascend and never overlap: that is what makes
	// newest-file-first lookup correct.
	for i := 1; i < len(tables); i++ {
		if tables[i-1].LargestSeq >= tables[i].SmallestSeq {
			t.Fatalf("tables %d and %d have overlapping sequence ranges [%d,%d] and [%d,%d]",
				i-1, i, tables[i-1].SmallestSeq, tables[i-1].LargestSeq,
				tables[i].SmallestSeq, tables[i].LargestSeq)
		}
		if tables[i].Number != tables[i-1].Number+1 {
			t.Fatalf("file numbers are not consecutive: %d then %d", tables[i-1].Number, tables[i].Number)
		}
	}
	for i := 0; i < n; i++ {
		assertValue(t, s, fmt.Sprintf("key%04d", i), fmt.Sprintf("value-%04d-padding-padding", i))
	}
}

// ---------------------------------------------------------------- multi-SSTable reads

func TestNewestValueWinsAcrossSSTables(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	mustPut(t, s, "k", "one")
	mustFlush(t, s)
	mustPut(t, s, "k", "two")
	mustFlush(t, s)
	mustPut(t, s, "k", "three")

	if got := len(s.SSTables()); got != 2 {
		t.Fatalf("%d SSTables, want 2", got)
	}
	assertValue(t, s, "k", "three") // from the memtable

	mustFlush(t, s)
	if got := len(s.SSTables()); got != 3 {
		t.Fatalf("%d SSTables, want 3", got)
	}
	assertValue(t, s, "k", "three") // now from the newest file
}

// TestTombstoneHidesOlderValueAcrossSSTables is the specific scenario the
// phase brief names: a value in one file, a tombstone in a newer one, and a
// Get that must report the key absent rather than resurrecting it.
func TestTombstoneHidesOlderValueAcrossSSTables(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	mustPut(t, s, "k", "A")
	mustFlush(t, s) // 000001.sst: k -> A
	mustDelete(t, s, "k")
	mustFlush(t, s) // 000002.sst: k -> tombstone

	if got := len(s.SSTables()); got != 2 {
		t.Fatalf("%d SSTables, want 2", got)
	}
	assertAbsent(t, s, "k")

	// And it stays absent across a restart, which is where a naive recovery
	// that replayed the WAL over the tables would bring it back.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = reopened.Close() }()
	assertAbsent(t, reopened, "k")
}

func TestOverwriteAndDeleteInterleavedAcrossManyFiles(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	// Each round writes the key, then flushes, so every round lives in its own
	// file and the read path has to pick the newest of many.
	for round := 0; round < 8; round++ {
		switch round % 3 {
		case 0:
			mustPut(t, s, "k", fmt.Sprintf("round-%d", round))
		case 1:
			mustDelete(t, s, "k")
		case 2:
			mustPut(t, s, "k", fmt.Sprintf("round-%d", round))
		}
		mustFlush(t, s)

		if round%3 == 1 {
			assertAbsent(t, s, "k")
		} else {
			assertValue(t, s, "k", fmt.Sprintf("round-%d", round))
		}
	}
	if got := len(s.SSTables()); got != 8 {
		t.Fatalf("%d SSTables, want 8", got)
	}
}

func TestKeyPresentOnlyInTheOldestFile(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	mustPut(t, s, "ancient", "still here")
	mustFlush(t, s)
	for i := 0; i < 5; i++ {
		mustPut(t, s, fmt.Sprintf("later%d", i), "v")
		mustFlush(t, s)
	}
	if got := len(s.SSTables()); got != 6 {
		t.Fatalf("%d SSTables, want 6", got)
	}
	assertValue(t, s, "ancient", "still here")
	assertAbsent(t, s, "never-written")
}

func TestMissingKeyIsNotFoundNotAnError(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(1024))
	defer func() { _ = s.Close() }()

	for i := 0; i < 200; i++ {
		mustPut(t, s, fmt.Sprintf("key%04d", i), "v")
	}
	mustFlush(t, s)

	for _, k := range []string{"", "a", "key", "key9999", "zzz", "key0000x"} {
		_, err := s.Get(context.Background(), []byte(k))
		if k == "" {
			if !errors.Is(err, storage.ErrKeyEmpty) {
				t.Errorf("Get(%q) = %v, want ErrKeyEmpty", k, err)
			}
			continue
		}
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("Get(%q) = %v, want ErrNotFound", k, err)
		}
	}
}

// ---------------------------------------------------------------- sequences

func TestSequenceNumbersAreAssignedOnePerMutation(t *testing.T) {
	dir := t.TempDir()
	s := openLSM(t, dir, lsmOpts(storage.DefaultMemTableSize))
	defer func() { _ = s.Close() }()

	if got := s.Sequence(); got != 0 {
		t.Fatalf("a fresh store starts at sequence %d, want 0", got)
	}
	mustPut(t, s, "a", "1")
	if got := s.Sequence(); got != 1 {
		t.Fatalf("after one Put the sequence is %d, want 1", got)
	}
	mustDelete(t, s, "a") // a delete is a mutation and consumes a number
	if got := s.Sequence(); got != 2 {
		t.Fatalf("after a Delete the sequence is %d, want 2", got)
	}
	mustDelete(t, s, "never-existed") // so is a delete of an absent key
	if got := s.Sequence(); got != 3 {
		t.Fatalf("after an idempotent Delete the sequence is %d, want 3", got)
	}

	// A rejected mutation must not consume a sequence number.
	if err := s.Put(context.Background(), nil, []byte("v")); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Fatalf("Put(empty key) = %v", err)
	}
	if got := s.Sequence(); got != 3 {
		t.Fatalf("a rejected Put consumed a sequence number: %d, want 3", got)
	}

	// Reads do not.
	assertAbsent(t, s, "a")
	if got := s.Sequence(); got != 3 {
		t.Fatalf("a Get consumed a sequence number: %d, want 3", got)
	}

	// SetAppliedIndex is metadata, not a mutation.
	if err := s.SetAppliedIndex(context.Background(), storage.AppliedIndex{Index: 9, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if got := s.Sequence(); got != 3 {
		t.Fatalf("SetAppliedIndex consumed a sequence number: %d, want 3", got)
	}
}

// TestSequenceNumbersSurviveRestart is what makes Phase 3's recovery work at
// all: replay must re-derive exactly the numbers the original writes were
// given, or "this mutation is already in an SSTable" is not decidable.
func TestSequenceNumbersSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 25; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), "v")
	}
	mustFlush(t, s)
	for i := 0; i < 10; i++ {
		mustDelete(t, s, fmt.Sprintf("k%02d", i))
	}
	want := s.Sequence()
	if want != 35 {
		t.Fatalf("sequence = %d, want 35", want)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for gen := 0; gen < 3; gen++ {
		reopened := openLSM(t, dir, opts)
		if got := reopened.Sequence(); got != want {
			t.Fatalf("generation %d recovered sequence %d, want %d", gen, got, want)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------- restart

func TestReopenAfterCleanClose(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "user:1", "Adi")
	mustPut(t, s, "user:2", "Bo")
	mustFlush(t, s) // user:1, user:2 land in 000001.sst
	mustPut(t, s, "user:1", "Adi Vishal")
	mustDelete(t, s, "user:2")
	mustPut(t, s, "empty", "")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()

	assertValue(t, reopened, "user:1", "Adi Vishal")
	assertAbsent(t, reopened, "user:2")
	assertValue(t, reopened, "empty", "")

	rec := reopened.Recovery()
	if rec.SSTablesLoaded != 1 {
		t.Errorf("loaded %d SSTables, want 1", rec.SSTablesLoaded)
	}
	// The two mutations already in the table are skipped; the three that came
	// after it are replayed.
	if rec.OpsSkipped != 2 || rec.OpsReplayed != 3 {
		t.Errorf("replay skipped %d and applied %d, want 2 and 3", rec.OpsSkipped, rec.OpsReplayed)
	}
	if rec.MaxFlushedSeq != 2 {
		t.Errorf("MaxFlushedSeq = %d, want 2", rec.MaxFlushedSeq)
	}
	if got := reopened.Len(); got != 2 {
		t.Errorf("recovered %d live keys, want 2", got)
	}
}

func TestReopenWithNoSSTables(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	assertValue(t, reopened, "k", "v")
	if rec := reopened.Recovery(); rec.SSTablesLoaded != 0 || rec.OpsReplayed != 1 || rec.OpsSkipped != 0 {
		t.Fatalf("recovery = %+v, want 0 tables and 1 replayed op", rec)
	}
}

func TestReopenWithEverythingFlushed(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 30; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()

	rec := reopened.Recovery()
	if rec.OpsReplayed != 0 || rec.OpsSkipped != 30 {
		t.Fatalf("replay applied %d and skipped %d, want 0 and 30: "+
			"everything was already in an SSTable", rec.OpsReplayed, rec.OpsSkipped)
	}
	if reopened.MemTableSize() != 0 {
		t.Fatalf("the memtable is %d bytes after recovering a fully flushed store",
			reopened.MemTableSize())
	}
	for i := 0; i < 30; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
}

func TestLSMReopenAcrossManySessions(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(2048)
	const sessions, perSession = 6, 40

	reference := map[string]string{}
	for session := 0; session < sessions; session++ {
		s := openLSM(t, dir, opts)
		for i := 0; i < perSession; i++ {
			k := fmt.Sprintf("key%03d", (session*perSession+i)%75)
			if i%5 == 4 {
				mustDelete(t, s, k)
				delete(reference, k)
			} else {
				v := fmt.Sprintf("s%d-i%d", session, i)
				mustPut(t, s, k, v)
				reference[k] = v
			}
		}
		if session%2 == 1 {
			mustFlush(t, s)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("session %d: Close: %v", session, err)
		}
	}

	final := openLSM(t, dir, opts)
	defer func() { _ = final.Close() }()

	got, err := final.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	assertSameState(t, reference, got)
}

// TestRecoveryIsDeterministic reopens the same directory repeatedly and
// requires byte-identical state each time.
func TestRecoveryIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(1024)

	s := openLSM(t, dir, opts)
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 400; i++ {
		k := fmt.Sprintf("key%03d", rng.Intn(90))
		if rng.Intn(4) == 0 {
			mustDelete(t, s, k)
		} else {
			mustPut(t, s, k, fmt.Sprintf("value-%d", i))
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	var first map[string][]byte
	var firstSeq uint64
	for gen := 0; gen < 4; gen++ {
		r := openLSM(t, dir, opts)
		snap, err := r.Snapshot()
		if err != nil {
			t.Fatalf("generation %d: Snapshot: %v", gen, err)
		}
		seq := r.Sequence()
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}

		if gen == 0 {
			first, firstSeq = snap, seq
			continue
		}
		if seq != firstSeq {
			t.Fatalf("generation %d recovered sequence %d, want %d", gen, seq, firstSeq)
		}
		assertSameBytes(t, first, snap, fmt.Sprintf("generation %d", gen))
	}
	if len(first) == 0 {
		t.Fatal("the test recovered an empty store; it is not exercising anything")
	}
}

// ---------------------------------------------------------------- file-set integrity

func TestOrphanTempFileIsSwept(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Exactly what a crash during a flush leaves: a partial file under a
	// temporary name. It must be deleted, not classified, not read.
	tmp := filepath.Join(dir, "000001.sst.tmp")
	if err := os.WriteFile(tmp, []byte("half an sstable"), 0o644); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()

	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s survived recovery", tmp)
	}
	if rec := reopened.Recovery(); rec.TempFilesRemoved != 1 {
		t.Fatalf("TempFilesRemoved = %d, want 1", rec.TempFilesRemoved)
	}
	assertValue(t, reopened, "k", "v")
}

func TestGapInSSTableNumberingIsRefused(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 3; i++ {
		mustPut(t, s, fmt.Sprintf("k%d", i), "v")
		mustFlush(t, s)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(dir, "000002.sst")); err != nil {
		t.Fatal(err)
	}

	_, err := storage.OpenLSMStore(dir, opts)
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("opening with a missing SSTable = %v, want ErrCorrupt. "+
			"Replay skips every mutation below the newest table's sequence, so the "+
			"removed file's keys would vanish silently.", err)
	}
}

func TestCorruptSSTableRefusesToOpen(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 40; i++ {
		mustPut(t, s, fmt.Sprintf("key%03d", i), "value")
	}
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "000001.sst")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		off  int
	}{
		{"a data block", 30},
		{"the index block", len(raw) - sstable.FooterSize - 6},
		{"the footer", len(raw) - 20},
		{"the magic", len(raw) - 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			damaged := append([]byte(nil), raw...)
			damaged[tc.off] ^= 0xff
			if err := os.WriteFile(path, damaged, 0o644); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.WriteFile(path, raw, 0o644) })

			got, err := storage.OpenLSMStore(dir, opts)
			if err == nil {
				_ = got.Close()
				t.Fatalf("damage to %s was accepted", tc.name)
			}
			if !errors.Is(err, storage.ErrCorrupt) {
				t.Fatalf("damage to %s gave %v, want ErrCorrupt", tc.name, err)
			}
			var opErr *storage.OpError
			if !errors.As(err, &opErr) {
				t.Fatalf("error %v is not an *OpError", err)
			}
		})
	}
}

func TestTruncatedSSTableRefusesToOpen(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 40; i++ {
		mustPut(t, s, fmt.Sprintf("key%03d", i), "value")
	}
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "000001.sst")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Half a file is what a crash during a flush would leave under the FINAL
	// name if publication were not a rename. It cannot happen, and it is
	// refused if it somehow does.
	if err := os.Truncate(path, info.Size()/2); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenLSMStore(dir, opts); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("a truncated SSTable opened with %v, want ErrCorrupt", err)
	}
}

// TestSSTableAheadOfTheLogIsRefused covers the one inconsistency Phase 3 can
// detect without a MANIFEST: tables that cover sequence numbers the log never
// held, which means the log was truncated or replaced.
func TestSSTableAheadOfTheLogIsRefused(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 20; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), "v")
	}
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Throw the log away, keeping the table built from it.
	if err := os.Truncate(walPath(dir, 1), 0); err != nil {
		t.Fatal(err)
	}

	_, err := storage.OpenLSMStore(dir, opts)
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("opening with a log shorter than its tables = %v, want ErrCorrupt", err)
	}
}

func TestStrayFilesAreIgnored(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{".DS_Store", "notes.txt", "1.sst", "0000001.sst", "000001.sst.bak", "README"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("junk"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()

	if got := reopened.Recovery().SSTablesLoaded; got != 1 {
		t.Fatalf("loaded %d SSTables, want 1: a stray file was mistaken for table data", got)
	}
	assertValue(t, reopened, "k", "v")
}

// ---------------------------------------------------------------- WAL interaction

func TestWALCorruptionStillRefusesWithSSTablesPresent(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	for i := 0; i < 20; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), "v")
	}
	mustFlush(t, s)
	for i := 0; i < 20; i++ {
		mustPut(t, s, fmt.Sprintf("later%02d", i), "v")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Damage a record in the middle of the log. Having valid SSTables on disk
	// must not make the engine any more willing to skip past it.
	f, err := os.OpenFile(walPath(dir, 1), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 40); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], 40); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := storage.OpenLSMStore(dir, opts); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("mid-log corruption with SSTables present = %v, want ErrCorrupt", err)
	}
}

func TestLSMAppliedIndexSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize)

	s := openLSM(t, dir, opts)
	mustPut(t, s, "k", "v")
	if err := s.SetAppliedIndex(context.Background(), storage.AppliedIndex{Index: 42, Term: 7}); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer func() { _ = reopened.Close() }()
	if got, want := reopened.AppliedIndex(), (storage.AppliedIndex{Index: 42, Term: 7}); got != want {
		t.Fatalf("AppliedIndex = %+v, want %+v", got, want)
	}
}

func TestLSMAllSyncModesRecover(t *testing.T) {
	for _, mode := range []wal.SyncMode{wal.SyncOff, wal.SyncBatch, wal.SyncAlways} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			opts := lsmOpts(1024)
			opts.WAL.SyncMode = mode

			s := openLSM(t, dir, opts)
			for i := 0; i < 60; i++ {
				mustPut(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			reopened := openLSM(t, dir, opts)
			defer func() { _ = reopened.Close() }()
			for i := 0; i < 60; i++ {
				assertValue(t, reopened, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
			}
		})
	}
}

// ---------------------------------------------------------------- options

func TestLSMOpenRejectsInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts storage.Options
	}{
		{"zero max key size", storage.Options{MaxKeySize: 0, MaxValueSize: 1}},
		{"negative max value size", storage.Options{MaxKeySize: 1, MaxValueSize: -1}},
		{"negative memtable size", storage.Options{MaxKeySize: 1, MaxValueSize: 1, MemTableSize: -1}},
		{"negative block size", storage.Options{MaxKeySize: 1, MaxValueSize: 1, BlockSize: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := storage.OpenLSMStore(t.TempDir(), tc.opts); !errors.Is(err, storage.ErrInvalidOptions) {
				t.Fatalf("OpenLSMStore = %v, want ErrInvalidOptions", err)
			}
		})
	}
}

func TestLSMOpenRejectsEmptyDirectoryArgument(t *testing.T) {
	if _, err := storage.OpenLSMStore("", storage.DefaultOptions()); !errors.Is(err, storage.ErrInvalidOptions) {
		t.Fatalf("OpenLSMStore(\"\") = %v, want ErrInvalidOptions", err)
	}
}

// ---------------------------------------------------------------- comparison helpers

func assertSameState(t *testing.T, want map[string]string, got map[string][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("store holds %d live keys, reference holds %d", len(got), len(want))
	}
	for k, wantVal := range want {
		gotVal, ok := got[k]
		if !ok {
			t.Errorf("key %q is in the reference but missing from the store", k)
			continue
		}
		if string(gotVal) != wantVal {
			t.Errorf("key %q = %q, want %q", k, gotVal, wantVal)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("key %q is in the store but not the reference", k)
		}
	}
}

func assertSameBytes(t *testing.T, want, got map[string][]byte, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d keys, want %d", what, len(got), len(want))
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("%s: key %q is missing", what, k)
		}
		if !bytes.Equal(gv, want[k]) {
			t.Fatalf("%s: key %q = %q, want %q", what, k, gv, want[k])
		}
	}
}

// TestFlushEveryWriteReallyFlushes keeps the "conformance across SSTables"
// claim honest. If the threshold ever stopped forcing a flush per mutation,
// that suite would quietly go back to testing a memtable and would still pass,
// which is the most comfortable kind of false confidence.
func TestFlushEveryWriteReallyFlushes(t *testing.T) {
	dir := t.TempDir()
	opts := storage.DefaultOptions()
	opts.MemTableSize = flushEveryWrite
	opts.BlockSize = 64

	s := openLSM(t, dir, opts)
	defer func() { _ = s.Close() }()

	for i := 0; i < 5; i++ {
		mustPut(t, s, fmt.Sprintf("k%d", i), "v")
	}
	mustDelete(t, s, "k0")

	tables := s.SSTables()
	if len(tables) != 6 {
		t.Fatalf("%d SSTables after 6 mutations, want 6 (one per mutation)", len(tables))
	}
	for i, tb := range tables {
		if tb.Entries != 1 {
			t.Errorf("table %d holds %d entries, want 1", i, tb.Entries)
		}
	}
	if s.MemTableSize() != 0 {
		t.Errorf("the memtable holds %d bytes; nothing should be left in memory", s.MemTableSize())
	}

	// And the answers are still right, now with every read served from a file.
	assertAbsent(t, s, "k0")
	for i := 1; i < 5; i++ {
		assertValue(t, s, fmt.Sprintf("k%d", i), "v")
	}
}

// ------------------------------------------------------- reference model

// reference is the simplest possible correct implementation of the same
// contract: a Go map. It is deliberately not WALStore — WALStore shares this
// engine's WAL code, so a bug in the log would cancel out and the comparison
// would prove nothing.
type reference map[string]string

func (r reference) put(k, v string) { r[k] = v }
func (r reference) delete(k string) { delete(r, k) }
func (r reference) get(k string) (string, bool) {
	v, ok := r[k]
	return v, ok
}

// TestAgainstReferenceModel is the differential test the phase calls for: a
// generated operation sequence applied to both a map and the real engine, with
// the two compared after every operation and again after a restart.
//
// The generator is seeded, so a failure reproduces exactly rather than
// "sometimes on CI".
func TestAgainstReferenceModel(t *testing.T) {
	for _, tc := range []struct {
		name         string
		memTableSize int64
		ops          int
		keyspace     int
	}{
		{"flush every write", flushEveryWrite, 300, 40},
		{"small memtable", 2048, 3000, 120},
		{"one memtable, no flush", storage.DefaultMemTableSize, 1500, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := lsmOpts(tc.memTableSize)
			s := openLSM(t, dir, opts)

			ref := reference{}
			rng := rand.New(rand.NewSource(int64(len(tc.name))))
			ctx := context.Background()

			for i := 0; i < tc.ops; i++ {
				key := fmt.Sprintf("key%04d", rng.Intn(tc.keyspace))
				switch n := rng.Intn(10); {
				case n < 6: // put
					value := fmt.Sprintf("v%d-%d", i, rng.Intn(1000))
					if rng.Intn(20) == 0 {
						value = "" // empty values are present keys
					}
					if err := s.Put(ctx, []byte(key), []byte(value)); err != nil {
						t.Fatalf("op %d: Put(%q): %v", i, key, err)
					}
					ref.put(key, value)
				case n < 8: // delete
					if err := s.Delete(ctx, []byte(key)); err != nil {
						t.Fatalf("op %d: Delete(%q): %v", i, key, err)
					}
					ref.delete(key)
				case n < 9: // read back a key, compared below
				default: // an explicit flush, to move the memtable/SSTable boundary around
					if err := s.Flush(); err != nil {
						t.Fatalf("op %d: Flush: %v", i, err)
					}
				}

				// Compare the key just touched on every operation, and the
				// whole keyspace periodically. Comparing everything every time
				// would make the test quadratic and slower than it is useful.
				assertMatchesReference(t, s, ref, key, i)
				if i%200 == 0 {
					for k := 0; k < tc.keyspace; k++ {
						assertMatchesReference(t, s, ref, fmt.Sprintf("key%04d", k), i)
					}
				}
			}

			for k := 0; k < tc.keyspace; k++ {
				assertMatchesReference(t, s, ref, fmt.Sprintf("key%04d", k), -1)
			}
			if got := s.Len(); got != len(ref) {
				t.Fatalf("store holds %d live keys, reference holds %d", got, len(ref))
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			// And again after a restart: recovery must land on the same state.
			reopened := openLSM(t, dir, opts)
			defer func() { _ = reopened.Close() }()
			for k := 0; k < tc.keyspace; k++ {
				assertMatchesReference(t, reopened, ref, fmt.Sprintf("key%04d", k), -1)
			}
			snap, err := reopened.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			assertSameState(t, ref, snap)
		})
	}
}

func assertMatchesReference(t *testing.T, s storage.Store, ref reference, key string, op int) {
	t.Helper()
	where := "final state"
	if op >= 0 {
		where = fmt.Sprintf("after op %d", op)
	}

	got, err := s.Get(context.Background(), []byte(key))
	want, present := ref.get(key)

	if !present {
		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("%s: Get(%q) = (%q, %v), want ErrNotFound", where, key, got, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("%s: Get(%q) = %v, want %q", where, key, err, want)
	}
	if string(got) != want {
		t.Fatalf("%s: Get(%q) = %q, want %q", where, key, got, want)
	}
}
