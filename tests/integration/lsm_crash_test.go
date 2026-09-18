package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// The Phase 3 crash tests. Same harness as the Phase 2 ones — a real child
// process, destroyed with SIGKILL, verified to have died by signal — but the
// engine under test now has an on-disk component that can be caught halfway
// through being written.
//
// The flush is the new crash window, and it is the one this file exists for:
//
//	before the flush   everything is in the WAL; replay rebuilds it
//	during the flush   a partial *.sst.tmp exists under a name no reader
//	                   consults; startup deletes it and replay rebuilds
//	                   everything it held
//	after the rename   the SSTable is complete and fsynced; replay skips the
//	                   prefix it covers
//
// Every one of those must recover to the same state. A test that only ever
// lands in the first case would pass while proving nothing about the other
// two, so TestCrashDuringFlush classifies each attempt by what it actually
// found on disk and requires that the mid-flush case really occurred.

// flushValueSize is the value size used by the kill-during-flush mode.
//
// The window this test aims at is the duration of sstable.WriteFile plus its
// fsync. With small values that is well under a millisecond and hitting it is
// luck. Four-kilobyte values make a few thousand keys into a memtable of
// several megabytes, so the write takes long enough that a kill a millisecond
// later reliably lands inside it. The test still verifies which window it
// actually hit rather than assuming.
const flushValueSize = 4 << 10

// paddedValueFor is valueFor(i) padded to flushValueSize, with the index kept
// at the front so a truncated or swapped value is still recognisable.
func paddedValueFor(i int) string {
	v := valueFor(i)
	if len(v) >= flushValueSize {
		return v
	}
	return v + strings.Repeat("x", flushValueSize-len(v))
}

// lsmEnv returns the child environment for the LSM engine.
func lsmEnv(memTableSize int64) []string {
	return []string{
		envEngine + "=lsm",
		envMemTbl + "=" + strconv.FormatInt(memTableSize, 10),
	}
}

// runLSMChild is the child process for the Phase 3 tests.
func runLSMChild() {
	dir := os.Getenv(envDir)
	opts := childOptions()

	s, err := storage.OpenLSMStore(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: OpenLSMStore: %v\n", err)
		os.Exit(2)
	}
	// Deliberately no defer Close. This process is going to be destroyed.

	ctx := context.Background()
	n := childCount()

	put := func(i int) {
		if err := s.Put(ctx, []byte(keyFor(i)), []byte(valueFor(i))); err != nil {
			fmt.Fprintf(os.Stderr, "child: Put %d: %v\n", i, err)
			os.Exit(2)
		}
	}

	switch os.Getenv(envMode) {
	case "burst":
		// Write, acknowledge, then wait to be killed. Nothing is flushed
		// unless the memtable threshold forced it.
		for i := 0; i < n; i++ {
			put(i)
		}
		announceReady()

	case "flush-then-write":
		// A published SSTable plus a WAL tail that is not in it: the state a
		// crash "after a flush" has to recover from.
		for i := 0; i < n; i++ {
			put(i)
		}
		if err := s.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "child: Flush: %v\n", err)
			os.Exit(2)
		}
		for i := n; i < 2*n; i++ {
			put(i)
		}
		announceReady()

	case "kill-during-flush":
		// Fill the memtable with enough data that writing it out takes long
		// enough to be interrupted, announce readiness, and start the flush
		// immediately. The parent kills a moment later, so the SIGKILL lands
		// inside sstable.WriteFile.
		for i := 0; i < n; i++ {
			if err := s.Put(ctx, []byte(keyFor(i)), []byte(paddedValueFor(i))); err != nil {
				fmt.Fprintf(os.Stderr, "child: Put %d: %v\n", i, err)
				os.Exit(2)
			}
		}
		announceReady()
		if err := s.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "child: Flush: %v\n", err)
			os.Exit(2)
		}

	case "mutate-then-flush-loop":
		// Overwrites and deletes spread across many files, with a kill
		// somewhere in the middle.
		announceReady()
		for i := 0; ; i++ {
			k := keyFor(i % n)
			if i%4 == 3 {
				if err := s.Delete(ctx, []byte(k)); err != nil {
					fmt.Fprintf(os.Stderr, "child: Delete: %v\n", err)
					os.Exit(2)
				}
				continue
			}
			if err := s.Put(ctx, []byte(k), []byte(fmt.Sprintf("gen-%08d", i))); err != nil {
				fmt.Fprintf(os.Stderr, "child: Put: %v\n", err)
				os.Exit(2)
			}
		}

	case "stream":
		announceReady()
		for i := 0; ; i++ {
			put(i)
		}

	default:
		fmt.Fprintf(os.Stderr, "child: unknown lsm mode %q\n", os.Getenv(envMode))
		os.Exit(2)
	}

	select {} // wait to be killed
}

// reopenLSM reopens a crashed directory.
func reopenLSM(t *testing.T, dir string, memTableSize int64) *storage.LSMStore {
	t.Helper()
	opts := storage.DefaultOptions()
	opts.MemTableSize = memTableSize
	s, err := storage.OpenLSMStore(dir, opts)
	if err != nil {
		t.Fatalf("reopening %s after the crash: %v", dir, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// dirState describes what a crash left behind on disk.
type dirState struct {
	sstables []string
	temps    []string
}

func inspectDir(t *testing.T, dir string) dirState {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	var st dirState
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".sst.tmp"):
			st.temps = append(st.temps, e.Name())
		case strings.HasSuffix(e.Name(), ".sst"):
			st.sstables = append(st.sstables, e.Name())
		}
	}
	return st
}

// assertPrefixIntact checks that keys 0..n-1 are all present with the values
// want(i) says they should hold — the "recovered state is a prefix with no
// holes" property from INV-S1, now spanning the memtable and the SSTables.
func assertPrefixIntact(t *testing.T, s *storage.LSMStore, n int, want func(int) string) {
	t.Helper()
	for i := 0; i < n; i++ {
		got, err := s.Get(context.Background(), []byte(keyFor(i)))
		if err != nil {
			t.Fatalf("Get(%s) after the crash: %v", keyFor(i), err)
		}
		if string(got) != want(i) {
			t.Fatalf("Get(%s) = %.40q..., want %.40q...", keyFor(i), got, want(i))
		}
	}
}

// ============================================================ the tests

// TestLSMCrashBeforeAnyFlush is the baseline: nothing reached an SSTable, so
// recovery is pure WAL replay into a fresh memtable.
func TestLSMCrashBeforeAnyFlush(t *testing.T) {
	const n = 500
	dir := t.TempDir()

	// A memtable far larger than the data, so no flush can be triggered.
	c := startChildEnv(t, dir, "burst", wal.SyncBatch, n, lsmEnv(64<<20))
	c.kill()

	if st := inspectDir(t, dir); len(st.sstables) != 0 || len(st.temps) != 0 {
		t.Fatalf("expected no SSTables before the first flush, found %+v", st)
	}

	s := reopenLSM(t, dir, 64<<20)
	assertPrefixIntact(t, s, n, valueFor)
	if got := s.Len(); got != n {
		t.Fatalf("recovered %d keys, want %d", got, n)
	}
	rec := s.Recovery()
	if rec.OpsReplayed != int64(n) || rec.OpsSkipped != 0 {
		t.Fatalf("replay applied %d and skipped %d, want %d and 0", rec.OpsReplayed, rec.OpsSkipped, n)
	}
}

// TestLSMCrashAfterFlush covers the other end: an SSTable was published, and
// the WAL holds both what is in it and what came after.
func TestLSMCrashAfterFlush(t *testing.T) {
	const n = 300
	dir := t.TempDir()

	c := startChildEnv(t, dir, "flush-then-write", wal.SyncBatch, n, lsmEnv(64<<20))
	c.kill()

	st := inspectDir(t, dir)
	if len(st.sstables) != 1 {
		t.Fatalf("expected exactly one published SSTable, found %+v", st)
	}
	if len(st.temps) != 0 {
		t.Fatalf("a temporary file survived a completed flush: %v", st.temps)
	}

	s := reopenLSM(t, dir, 64<<20)
	assertPrefixIntact(t, s, 2*n, valueFor)
	if got := s.Len(); got != 2*n {
		t.Fatalf("recovered %d keys, want %d", got, 2*n)
	}

	rec := s.Recovery()
	if rec.SSTablesLoaded != 1 {
		t.Fatalf("loaded %d SSTables, want 1", rec.SSTablesLoaded)
	}
	// The first n mutations are in the table and must NOT be replayed; the
	// rest must be. Replaying the flushed prefix would still give the right
	// answers, but it would mean the engine had learned nothing from the file
	// and memory use would not be bounded by the memtable.
	if rec.OpsSkipped != int64(n) || rec.OpsReplayed != int64(n) {
		t.Fatalf("replay skipped %d and applied %d, want %d and %d",
			rec.OpsSkipped, rec.OpsReplayed, n, n)
	}
	t.Logf("recovered from %d-entry SSTable + %d replayed WAL ops", rec.SSTableEntries, rec.OpsReplayed)
}

// TestCrashDuringFlush is the test this file exists for.
//
// The child fills a large memtable, announces that every write was
// acknowledged, and immediately starts writing it out. The parent kills it a
// moment later, so the SIGKILL lands inside the SSTable write.
//
// Each attempt is classified by what the crash actually left on disk, because
// a test that assumed it hit the window would pass just as happily if it never
// did. The run requires that the mid-flush case occurred at least once, and
// that every attempt — whichever window it landed in — recovers every
// acknowledged write.
func TestCrashDuringFlush(t *testing.T) {
	const (
		n        = 3000 // ~12 MiB of memtable at flushValueSize
		attempts = 6
	)

	var (
		hitMidFlush int
		hitPostSwap int
		hitPreFlush int
	)

	for attempt := 0; attempt < attempts; attempt++ {
		dir := t.TempDir()

		c := startChildEnv(t, dir, "kill-during-flush", wal.SyncBatch, n, lsmEnv(256<<20))
		// Long enough for the flush to be well under way, short enough that
		// it is unlikely to have finished: the memtable holds several MiB and
		// the write ends with an fsync.
		time.Sleep(time.Duration(attempt*attempt*3) * time.Millisecond)
		c.kill()

		st := inspectDir(t, dir)
		switch {
		case len(st.temps) > 0:
			hitMidFlush++
			if len(st.sstables) != 0 {
				t.Fatalf("attempt %d: a temp file and a published table exist at once: %+v", attempt, st)
			}
		case len(st.sstables) > 0:
			hitPostSwap++
		default:
			hitPreFlush++
		}

		s := reopenLSM(t, dir, 256<<20)

		// Whatever the crash caught, every acknowledged write is here.
		assertPrefixIntact(t, s, n, paddedValueFor)
		if got := s.Len(); got != n {
			t.Fatalf("attempt %d: recovered %d keys, want %d", attempt, got, n)
		}
		// And the partial file is gone, not carried forward.
		if after := inspectDir(t, dir); len(after.temps) != 0 {
			t.Fatalf("attempt %d: %v survived recovery", attempt, after.temps)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("attempt %d: Close: %v", attempt, err)
		}
	}

	t.Logf("windows hit: %d during the flush, %d after the rename, %d before the flush started",
		hitMidFlush, hitPostSwap, hitPreFlush)
	if hitMidFlush == 0 {
		t.Fatalf("no attempt was killed during the SSTable write, so the crash window this "+
			"test exists for was never exercised (during=%d after=%d before=%d)",
			hitMidFlush, hitPostSwap, hitPreFlush)
	}
}

// TestLSMCrashMidWriteStormYieldsAPrefix is INV-S1 again, now with the
// memtable small enough that the storm produces many SSTables while it runs.
func TestLSMCrashMidWriteStormYieldsAPrefix(t *testing.T) {
	const memTable = 32 << 10

	for attempt := 0; attempt < 3; attempt++ {
		dir := t.TempDir()

		c := startChildEnv(t, dir, "stream", wal.SyncBatch, 0, lsmEnv(memTable))
		time.Sleep(time.Duration(20+attempt*25) * time.Millisecond)
		c.kill()

		s := reopenLSM(t, dir, memTable)

		// Find the contiguous prefix, then prove there is no hole after it.
		n := 0
		for {
			if _, err := s.Get(context.Background(), []byte(keyFor(n))); err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					break
				}
				t.Fatalf("attempt %d: Get(%s): %v", attempt, keyFor(n), err)
			}
			n++
		}
		if n == 0 {
			t.Fatalf("attempt %d: nothing survived the crash at all", attempt)
		}
		if got := s.Len(); got != n {
			t.Fatalf("attempt %d: the store holds %d keys but the contiguous prefix is %d; "+
				"recovery left a hole", attempt, got, n)
		}
		assertPrefixIntact(t, s, n, valueFor)

		tables := s.SSTables()
		t.Logf("attempt %d: %d writes survived across %d SSTables, contiguous, no holes",
			attempt, n, len(tables))
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestLSMRepeatedCrashes: crash, recover, write more, crash again, with
// flushes happening throughout. Each generation inherits the previous one's
// SSTables and its repaired WAL tail.
func TestLSMRepeatedCrashes(t *testing.T) {
	const memTable = 16 << 10
	dir := t.TempDir()

	prev := 0
	for round := 0; round < 4; round++ {
		c := startChildEnv(t, dir, "stream", wal.SyncBatch, 0, lsmEnv(memTable))
		time.Sleep(30 * time.Millisecond)
		c.kill()

		s := reopenLSM(t, dir, memTable)
		n := s.Len()
		if n < prev {
			t.Fatalf("round %d: the store shrank from %d keys to %d", round, prev, n)
		}
		assertPrefixIntact(t, s, n, valueFor)

		rec := s.Recovery()
		t.Logf("round %d: %d keys, %d SSTables, %d ops skipped, %d replayed, truncated=%v",
			round, n, rec.SSTablesLoaded, rec.OpsSkipped, rec.OpsReplayed, rec.Truncated)
		prev = n
		if err := s.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
	}
	if prev == 0 {
		t.Fatal("nothing ever survived; the test is not exercising anything")
	}
}

// TestCrashWithOverwritesAndDeletesAcrossFiles is the multi-file resolution
// property under a real crash: after recovery, a key's value is whatever the
// last surviving mutation said, and a deleted key stays deleted no matter how
// many older files still hold a value for it.
func TestCrashWithOverwritesAndDeletesAcrossFiles(t *testing.T) {
	const (
		keys     = 40
		memTable = 8 << 10
	)
	dir := t.TempDir()

	c := startChildEnv(t, dir, "mutate-then-flush-loop", wal.SyncBatch, keys, lsmEnv(memTable))
	time.Sleep(60 * time.Millisecond)
	c.kill()

	st := inspectDir(t, dir)
	if len(st.sstables) < 2 {
		t.Fatalf("the storm produced %d SSTables; the test needs several to mean anything", len(st.sstables))
	}

	// Recover twice and require identical state. The point is not only that
	// recovery works, but that it is a function of the bytes on disk.
	first := reopenLSM(t, dir, memTable)
	snapA, err := first.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	seqA := first.Sequence()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := reopenLSM(t, dir, memTable)
	snapB, err := second.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if seqB := second.Sequence(); seqB != seqA {
		t.Fatalf("two recoveries of the same directory produced sequences %d and %d", seqA, seqB)
	}
	if len(snapA) != len(snapB) {
		t.Fatalf("two recoveries produced %d and %d live keys", len(snapA), len(snapB))
	}
	for k, v := range snapA {
		w, ok := snapB[k]
		if !ok {
			t.Fatalf("key %q recovered once and not the second time", k)
		}
		if string(v) != string(w) {
			t.Fatalf("key %q recovered as %q then %q", k, v, w)
		}
	}

	// Every surviving key must hold a value the child actually wrote, not a
	// stale one resurrected from an older file.
	for k, v := range snapA {
		if !strings.HasPrefix(string(v), "gen-") {
			t.Fatalf("key %q holds %q, which the child never wrote", k, v)
		}
	}
	t.Logf("%d SSTables, %d live keys of %d, sequence %d", len(st.sstables), len(snapA), keys, seqA)
}

// TestCrashedFlushLeavesReplayableLog inspects the directory directly rather
// than trusting the store's view of itself, and replays the WAL independently.
func TestCrashedFlushLeavesReplayableLog(t *testing.T) {
	const n = 1500
	dir := t.TempDir()

	c := startChildEnv(t, dir, "kill-during-flush", wal.SyncBatch, n, lsmEnv(256<<20))
	time.Sleep(2 * time.Millisecond)
	c.kill()

	st := inspectDir(t, dir)
	t.Logf("after the crash: sstables=%v temps=%v", st.sstables, st.temps)

	// Replay the log with no engine involved at all. Whatever happened to the
	// flush, the log still holds every acknowledged mutation.
	var ops int
	rec, err := wal.Recover(filepath.Join(dir, "wal"), wal.Handler{
		Batch: func(b wal.Batch) error { ops += len(b); return nil },
	})
	if err != nil {
		t.Fatalf("replaying the crashed log: %v", err)
	}
	if ops != n {
		t.Fatalf("the log holds %d operations, want %d", ops, n)
	}
	t.Logf("wal recovery: %+v", rec)
}
