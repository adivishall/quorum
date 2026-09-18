package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/distributed-kv/internal/record"
	"github.com/adivishall/distributed-kv/internal/storage"
	"github.com/adivishall/distributed-kv/internal/storage/wal"
)

// newWALStore adapts WALStore to the shared suites.
//
// This four-line function is the entire cost of proving that adding durability
// changed no client-visible semantic: WALStore runs the Phase 1 conformance and
// concurrency suites verbatim, with no exemptions and no modifications to them.
func newWALStore(tb testing.TB, opts storage.Options) storage.Store {
	tb.Helper()
	s, err := storage.OpenWALStore(tb.TempDir(), opts)
	if err != nil {
		tb.Fatalf("OpenWALStore: %v", err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

func TestWALStoreConformance(t *testing.T) {
	runConformance(t, newWALStore)
}

func TestWALStoreConcurrency(t *testing.T) {
	runConcurrency(t, newWALStore)
}

// ---------------------------------------------------------------- helpers

// openAt opens a store in a fixed directory, so a test can close and reopen it.
func openAt(t *testing.T, dir string, mode wal.SyncMode) *storage.WALStore {
	t.Helper()
	opts := storage.DefaultOptions()
	opts.WAL.SyncMode = mode
	s, err := storage.OpenWALStore(dir, opts)
	if err != nil {
		t.Fatalf("OpenWALStore(%s): %v", dir, err)
	}
	return s
}

func mustPut(t *testing.T, s storage.Store, k, v string) {
	t.Helper()
	if err := s.Put(context.Background(), []byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func mustDelete(t *testing.T, s storage.Store, k string) {
	t.Helper()
	if err := s.Delete(context.Background(), []byte(k)); err != nil {
		t.Fatalf("Delete(%q): %v", k, err)
	}
}

func assertValue(t *testing.T, s storage.Store, k, want string) {
	t.Helper()
	got, err := s.Get(context.Background(), []byte(k))
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", k, got, want)
	}
}

func assertAbsent(t *testing.T, s storage.Store, k string) {
	t.Helper()
	if _, err := s.Get(context.Background(), []byte(k)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get(%q) = %v, want ErrNotFound", k, err)
	}
}

func walPath(dir string, seg int) string {
	return filepath.Join(dir, "wal", fmt.Sprintf("%06d.log", seg))
}

// ---------------------------------------------------------------- durability

// TestStateSurvivesReopen is the headline difference from Phase 1.
func TestStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	s := openAt(t, dir, wal.SyncBatch)
	mustPut(t, s, "user:1", "Adi")
	mustPut(t, s, "user:2", "Bo")
	mustPut(t, s, "user:1", "Adi Vishal") // overwrite
	mustDelete(t, s, "user:2")
	mustPut(t, s, "empty", "")
	mustDelete(t, s, "never-existed") // idempotent delete of an absent key
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = reopened.Close() }()

	assertValue(t, reopened, "user:1", "Adi Vishal")
	assertAbsent(t, reopened, "user:2")
	assertValue(t, reopened, "empty", "")
	assertAbsent(t, reopened, "never-existed")

	if got := reopened.Len(); got != 2 {
		t.Errorf("recovered %d keys, want 2", got)
	}
	rec := reopened.Recovery()
	if rec.OpsApplied != 6 {
		t.Errorf("Recovery.OpsApplied = %d, want 6", rec.OpsApplied)
	}
	if rec.Truncated {
		t.Errorf("a cleanly closed store was truncated on reopen: %+v", rec)
	}
}

// TestStateSurvivesAbandonedStore reopens a store that was never closed.
//
// Because every Append completes a write(2) before it returns, the records are
// already in the kernel's page cache and therefore in the file, with no
// graceful shutdown involved. This is the in-process analogue of the real
// SIGKILL test in tests/integration; both are needed, because this one is fast
// enough to run on every commit.
func TestStateSurvivesAbandonedStore(t *testing.T) {
	dir := t.TempDir()

	func() {
		s := openAt(t, dir, wal.SyncBatch)
		for i := 0; i < 50; i++ {
			mustPut(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
		}
		// Deliberately no Close: nothing flushes, nothing tidies up.
	}()

	reopened := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = reopened.Close() }()

	for i := 0; i < 50; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
}

func TestReopenAcrossManySessions(t *testing.T) {
	dir := t.TempDir()
	const sessions = 5

	for session := 0; session < sessions; session++ {
		s := openAt(t, dir, wal.SyncBatch)
		for i := 0; i < 10; i++ {
			mustPut(t, s, fmt.Sprintf("s%d-k%d", session, i), fmt.Sprintf("v%d", i))
		}
		// Every session must see everything written by every earlier session.
		for earlier := 0; earlier <= session; earlier++ {
			assertValue(t, s, fmt.Sprintf("s%d-k3", earlier), "v3")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	final := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = final.Close() }()
	if got, want := final.Len(), sessions*10; got != want {
		t.Fatalf("recovered %d keys, want %d", got, want)
	}
}

// TestReplayReconstructsIdenticalState compares the live map against the
// recovered one key by key, which is the property INV-S1 actually needs.
func TestReplayReconstructsIdenticalState(t *testing.T) {
	dir := t.TempDir()
	opts := storage.DefaultOptions()
	opts.WAL.SegmentSize = 4 << 10 // force several segments

	s, err := storage.OpenWALStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]string{}
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key%03d", i%97)
		switch i % 3 {
		case 0, 1:
			value := fmt.Sprintf("value-%d", i)
			mustPut(t, s, key, value)
			want[key] = value
		case 2:
			mustDelete(t, s, key)
			delete(want, key)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := storage.OpenWALStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()

	if got := reopened.Len(); got != len(want) {
		t.Fatalf("recovered %d keys, want %d", got, len(want))
	}
	for k, v := range want {
		assertValue(t, reopened, k, v)
	}
	if reopened.Recovery().SegmentsScanned < 2 {
		t.Errorf("only %d segments were scanned; the test did not exercise rotation",
			reopened.Recovery().SegmentsScanned)
	}
}

func TestAppliedIndexSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := openAt(t, dir, wal.SyncBatch)
	if got := s.AppliedIndex(); got != (storage.AppliedIndex{}) {
		t.Fatalf("a fresh store reports AppliedIndex %+v, want the zero value", got)
	}
	mustPut(t, s, "k", "v")
	if err := s.SetAppliedIndex(ctx, storage.AppliedIndex{Index: 41, Term: 2}); err != nil {
		t.Fatalf("SetAppliedIndex: %v", err)
	}
	mustPut(t, s, "k2", "v2")
	if err := s.SetAppliedIndex(ctx, storage.AppliedIndex{Index: 42, Term: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = reopened.Close() }()
	if got, want := reopened.AppliedIndex(), (storage.AppliedIndex{Index: 42, Term: 2}); got != want {
		t.Fatalf("AppliedIndex after restart = %+v, want %+v", got, want)
	}
	assertValue(t, reopened, "k2", "v2")
}

func TestAllSyncModesRecover(t *testing.T) {
	for _, mode := range []wal.SyncMode{wal.SyncOff, wal.SyncBatch, wal.SyncAlways} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			s := openAt(t, dir, mode)
			for i := 0; i < 20; i++ {
				mustPut(t, s, fmt.Sprintf("k%02d", i), "v")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			reopened := openAt(t, dir, mode)
			defer func() { _ = reopened.Close() }()
			if got := reopened.Len(); got != 20 {
				t.Fatalf("mode %v recovered %d keys, want 20", mode, got)
			}
		})
	}
}

// ---------------------------------------------------------------- ordering

// TestFailedWriteIsNotPublished checks the write-path ordering: a mutation that
// the log rejected must not be visible in memory, or a restart would silently
// lose a write the client was told had succeeded.
func TestFailedWriteIsNotPublished(t *testing.T) {
	dir := t.TempDir()
	s := openAt(t, dir, wal.SyncBatch)

	mustPut(t, s, "k", "original")

	// Oversized value: rejected by validation, before anything is logged.
	huge := bytes.Repeat([]byte("x"), storage.DefaultMaxValueSize+1)
	if err := s.Put(context.Background(), []byte("k"), huge); !errors.Is(err, storage.ErrValueTooLarge) {
		t.Fatalf("Put(oversized) = %v, want ErrValueTooLarge", err)
	}
	assertValue(t, s, "k", "original")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = reopened.Close() }()
	assertValue(t, reopened, "k", "original")
	if got := reopened.Recovery().OpsApplied; got != 1 {
		t.Errorf("the log holds %d operations, want 1: a rejected write reached the log", got)
	}
}

// TestWriteOrderMatchesLogOrder is the reason writeMu spans both the append and
// the publish. If the two orders could differ, the state after a restart would
// differ from the state before it.
func TestWriteOrderMatchesLogOrder(t *testing.T) {
	const rounds = 300
	dir := t.TempDir()
	s := openAt(t, dir, wal.SyncBatch)

	// Many writers hammering one key. Whichever value wins in memory must be
	// the same value that replay produces, because both derive from the same
	// serialisation order.
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < rounds; i++ {
				if err := s.Put(context.Background(), []byte("contended"),
					[]byte(fmt.Sprintf("w%d-i%d", w, i))); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}
	for w := 0; w < 8; w++ {
		<-done
	}

	live, err := s.Get(context.Background(), []byte("contended"))
	if err != nil {
		t.Fatal(err)
	}
	liveValue := string(live)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = reopened.Close() }()
	assertValue(t, reopened, "contended", liveValue)
}

// ---------------------------------------------------------------- corruption

func TestOpenRepairsTornTail(t *testing.T) {
	dir := t.TempDir()
	s := openAt(t, dir, wal.SyncBatch)
	for i := 0; i < 10; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Cut the final record in half, as a crash mid-append would.
	path := walPath(dir, 1)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-6); err != nil {
		t.Fatal(err)
	}

	reopened := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = reopened.Close() }()

	rec := reopened.Recovery()
	if !rec.Truncated {
		t.Fatal("the torn tail was not repaired")
	}
	if rec.TruncatedBytes <= 0 {
		t.Errorf("TruncatedBytes = %d, want a positive count", rec.TruncatedBytes)
	}
	// Nine writes survive; the tenth was torn away.
	for i := 0; i < 9; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	assertAbsent(t, reopened, "k09")

	// The repaired store must be usable and must stay consistent across
	// another restart.
	mustPut(t, reopened, "after", "repair")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	again := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = again.Close() }()
	if again.Recovery().Truncated {
		t.Error("the repaired log needed repair a second time")
	}
	assertValue(t, again, "after", "repair")
	assertValue(t, again, "k08", "v08")
}

// TestOpenRefusesCorruptLog: damage that a crash cannot explain must stop the
// store from opening, rather than producing a database that looks healthy and
// is missing writes.
func TestOpenRefusesCorruptLog(t *testing.T) {
	dir := t.TempDir()
	s := openAt(t, dir, wal.SyncBatch)
	for i := 0; i < 20; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a record well before the end.
	path := walPath(dir, 1)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	off := int64(3*record.HeaderSize + 20)
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xff
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	opts := storage.DefaultOptions()
	reopened, err := storage.OpenWALStore(dir, opts)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("OpenWALStore succeeded on a corrupt log")
	}
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("error = %v, want errors.Is(..., storage.ErrCorrupt)", err)
	}
	// The error must locate the damage well enough to act on.
	var re *record.Error
	if !errors.As(err, &re) {
		t.Fatalf("error %v does not carry a *record.Error with the file and offset", err)
	}
	if re.File == "" || re.Offset < 0 {
		t.Errorf("record.Error = %+v, want a file and offset", re)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("000001.log")) {
		t.Errorf("error %q does not name the segment", err)
	}
}

func TestOpenRejectsEmptyDirectoryArgument(t *testing.T) {
	if _, err := storage.OpenWALStore("", storage.DefaultOptions()); !errors.Is(err, storage.ErrInvalidOptions) {
		t.Fatalf("OpenWALStore(\"\") = %v, want ErrInvalidOptions", err)
	}
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	if _, err := storage.OpenWALStore(t.TempDir(), storage.Options{MaxKeySize: 0}); !errors.Is(err, storage.ErrInvalidOptions) {
		t.Fatalf("OpenWALStore with bad options = %v, want ErrInvalidOptions", err)
	}
}

func TestSyncAndCloseAreIdempotentAndOrdered(t *testing.T) {
	dir := t.TempDir()
	s := openAt(t, dir, wal.SyncBatch)
	mustPut(t, s, "k", "v")

	if err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	if err := s.Sync(); !errors.Is(err, storage.ErrClosed) {
		t.Errorf("Sync after Close = %v, want ErrClosed", err)
	}
	if err := s.SetAppliedIndex(context.Background(), storage.AppliedIndex{Index: 1}); !errors.Is(err, storage.ErrClosed) {
		t.Errorf("SetAppliedIndex after Close = %v, want ErrClosed", err)
	}
}

func TestWALStatsReflectWrites(t *testing.T) {
	dir := t.TempDir()
	s := openAt(t, dir, wal.SyncBatch)
	defer func() { _ = s.Close() }()

	before := s.WALStats()
	if before.ActiveSegment != 1 {
		t.Errorf("ActiveSegment = %d, want 1", before.ActiveSegment)
	}
	mustPut(t, s, "k", bytes.NewBuffer(make([]byte, 0)).String()+"value")
	after := s.WALStats()
	if after.ActiveBytes <= before.ActiveBytes {
		t.Errorf("ActiveBytes did not grow after a write: %d -> %d", before.ActiveBytes, after.ActiveBytes)
	}
	if s.Dir() != dir {
		t.Errorf("Dir() = %q, want %q", s.Dir(), dir)
	}
}
