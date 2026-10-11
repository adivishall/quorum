package storage_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/storage"
)

// S3: durable watermarks and WAL truncation (docs/STORAGE_INTEGRATION.md §9).

// countWALSegments returns how many WAL segment files dir's engine holds.
func countWALSegments(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			n++
		}
	}
	return n
}

// TestWALTruncationReclaimsSegments: a flush rotates the WAL and deletes the
// segments below the new one, so the log does not grow without bound as a
// long-running engine flushes again and again — and every flushed write is still
// recoverable, from the tables, after a reopen.
func TestWALTruncationReclaimsSegments(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(4 << 10)   // flush often
	opts.WAL.SegmentSize = 512 // and rotate often, so there are segments to reclaim
	s := openLSM(t, dir, opts)

	const n = 400
	for i := 0; i < n; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%04d", i), "v")}, ai(uint64(i+1), 1)})
	}
	// Many writes have flushed; the WAL has been truncated, not accumulated.
	if segs := countWALSegments(t, dir); segs > 8 {
		t.Fatalf("the WAL holds %d segments after %d writes; truncation is not reclaiming them", segs, n)
	}
	if ts := s.WALTruncationStats(); ts.SegmentsRemoved == 0 {
		t.Fatal("no WAL segments were truncated after many flushes")
	}
	if ts := s.WALTruncationStats(); ts.Failures != 0 {
		t.Fatalf("%d WAL truncation failures", ts.Failures)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Every flushed write survives the reopen, recovered from the tables with a
	// truncated log.
	reopened := openLSM(t, dir, opts)
	defer reopened.Close()
	for i := 0; i < n; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%04d", i), "v")
	}
	if got := reopened.AppliedIndex(); got != ai(n, 1) {
		t.Fatalf("recovered applied index %+v, want %+v", got, ai(n, 1))
	}
}

// TestAppliedIndexRecoveredFromTheManifestAfterAFlush: when the last thing before
// close is a flush, the surviving WAL holds no apply records, so the applied index
// can only come from the MANIFEST's Applied. The reopen must recover it, which
// proves the flush recorded the applied index durably (not just the data).
func TestAppliedIndexRecoveredFromTheManifestAfterAFlush(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize) // large: only the explicit flush flushes
	s := openLSM(t, dir, opts)
	for i := 1; i <= 10; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%02d", i), "v")}, ai(uint64(i), 3)})
	}
	mustFlush(t, s) // covers index 10 and truncates the WAL; nothing is written after
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	defer reopened.Close()
	if got := reopened.Recovery().ApplyBatches; got != 0 {
		t.Fatalf("replay read %d apply batches; the WAL should be empty after the final flush", got)
	}
	if got := reopened.AppliedIndex(); got != ai(10, 3) {
		t.Fatalf("recovered applied index %+v, want (10,3): with an empty WAL it must come from the MANIFEST", got)
	}
	for i := 1; i <= 10; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%02d", i), "v")
	}
}

// TestDurableAppliedAdvancesOnlyAtFlush: the engine's durable applied index —
// what Raft log compaction must not pass (R2) — advances only when a flush makes
// its MANIFEST edit durable, never merely because an entry was applied into the
// (unsynced) WAL.
func TestDurableAppliedAdvancesOnlyAtFlush(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize) // large: no automatic flush
	s := openLSM(t, dir, opts)
	defer s.Close()

	if got := s.DurableApplied(); got != (ai(0, 0)) {
		t.Fatalf("durable applied %+v before any flush, want zero", got)
	}
	for i := 1; i <= 5; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%d", i), "v")}, ai(uint64(i), 1)})
	}
	// Applied into the WAL, but not flushed: the durable watermark has not moved.
	if got := s.DurableApplied(); got != (ai(0, 0)) {
		t.Fatalf("durable applied %+v after applies without a flush, want zero", got)
	}
	if got := s.AppliedIndex(); got != ai(5, 1) {
		t.Fatalf("applied index %+v, want (5,1)", got)
	}
	mustFlush(t, s)
	// The flush recorded its edit: the watermark is now the applied index it covers.
	if got := s.DurableApplied(); got != ai(5, 1) {
		t.Fatalf("durable applied %+v after the flush, want (5,1)", got)
	}
	// Further applies do not move it until the next flush.
	mustApply(t, s, applyStep{[]storage.Mutation{mput("k6", "v")}, ai(6, 2)})
	if got := s.DurableApplied(); got != ai(5, 1) {
		t.Fatalf("durable applied %+v after an apply past the flush, want still (5,1)", got)
	}
	if got := s.AppliedIndex(); got != ai(6, 2) {
		t.Fatalf("applied index %+v, want (6,2)", got)
	}
}

// TestAFlushRecordsAnAppliedIndexThatMovedWithoutData: an apply batch with no
// mutation (the election no-op, a configuration entry) and SetAppliedIndex
// advance the applied index without adding to the memtable. A Flush must still
// make that index the durable watermark and record it in the MANIFEST — the LSM
// machine's SyncApplied relies on it to let the Raft log compact behind a
// snapshot (R2) — so a reopen with nothing left in the WAL recovers it. A Flush
// with nothing new records and rotates nothing. Found by the final S3 review:
// an empty memtable used to make Flush a no-op that left the watermark behind.
func TestAFlushRecordsAnAppliedIndexThatMovedWithoutData(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(storage.DefaultMemTableSize) // large: only the explicit flushes flush
	s := openLSM(t, dir, opts)
	for i := 1; i <= 5; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%d", i), "v")}, ai(uint64(i), 1)})
	}
	mustFlush(t, s)
	if got := s.DurableApplied(); got != ai(5, 1) {
		t.Fatalf("premise: durable applied %+v after the first flush, want (5,1)", got)
	}

	// Entries that write nothing: the memtable stays empty.
	mustApply(t, s, applyStep{nil, ai(6, 2)})
	mustApply(t, s, applyStep{nil, ai(7, 2)})
	if got := s.DurableApplied(); got != ai(5, 1) {
		t.Fatalf("durable applied %+v before the flush, want still (5,1)", got)
	}
	seg := s.WALStats().ActiveSegment
	mustFlush(t, s)
	if got := s.DurableApplied(); got != ai(7, 2) {
		t.Fatalf("durable applied %+v after a flush, want the applied index (7,2): an empty memtable left the watermark behind", got)
	}
	if got := s.WALStats().ActiveSegment; got <= seg {
		t.Fatalf("the flush recorded the index without rotating the WAL (segment %d, was %d)", got, seg)
	}
	if segs := countWALSegments(t, dir); segs != 1 {
		t.Fatalf("%d WAL segments after the flush, want 1: those below the new log number are truncated", segs)
	}

	// Nothing new: a second flush records nothing and rotates nothing.
	seg = s.WALStats().ActiveSegment
	mustFlush(t, s)
	if got := s.WALStats().ActiveSegment; got != seg {
		t.Fatalf("a flush with nothing to record rotated the WAL to segment %d (was %d)", got, seg)
	}

	// SetAppliedIndex moves the index without data too.
	if err := s.SetAppliedIndex(context.Background(), ai(9, 3)); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	if got := s.DurableApplied(); got != ai(9, 3) {
		t.Fatalf("durable applied %+v after SetAppliedIndex and a flush, want (9,3)", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The MANIFEST alone carries the index: the WAL holds nothing to replay.
	reopened := openLSM(t, dir, opts)
	defer reopened.Close()
	if got := reopened.Recovery().ApplyBatches; got != 0 {
		t.Fatalf("replay read %d apply batches; the WAL should be empty after the final flush", got)
	}
	if got := reopened.AppliedIndex(); got != ai(9, 3) {
		t.Fatalf("recovered applied index %+v, want (9,3) from the MANIFEST", got)
	}
	if got := reopened.DurableApplied(); got != ai(9, 3) {
		t.Fatalf("recovered durable applied %+v, want (9,3)", got)
	}
	for i := 1; i <= 5; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%d", i), "v")
	}
	// And the engine goes on from there.
	mustApply(t, reopened, applyStep{[]storage.Mutation{mput("k6", "v")}, ai(10, 3)})
	assertValue(t, reopened, "k6", "v")
}

// TestSequenceAndAppliedContinuityAcrossReopen: after truncation the next
// sequence is seeded from the MANIFEST and continues strictly upward — no
// durable sequence is reused — and the applied index is monotonic across many
// reopens.
func TestSequenceAndAppliedContinuityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(4 << 10)
	opts.WAL.SegmentSize = 512

	var idx uint64
	put := func(s *storage.LSMStore, key string) {
		idx++
		mustApply(t, s, applyStep{[]storage.Mutation{mput(key, "v")}, ai(idx, 1)})
	}

	s := openLSM(t, dir, opts)
	for i := 0; i < 50; i++ {
		put(s, fmt.Sprintf("k%03d", i))
	}
	lastSeq := s.Sequence()
	lastApplied := s.AppliedIndex()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for session := 0; session < 4; session++ {
		s = openLSM(t, dir, opts)
		// The recovered sequence is exactly what the previous session reached:
		// seeded from the MANIFEST and advanced by the surviving log, never reset.
		if got := s.Sequence(); got != lastSeq {
			t.Fatalf("session %d: recovered sequence %d, want %d", session, got, lastSeq)
		}
		if got := s.AppliedIndex(); got != lastApplied {
			t.Fatalf("session %d: recovered applied index %+v, want %+v", session, got, lastApplied)
		}
		// New writes take sequences strictly above everything durable.
		for i := 0; i < 20; i++ {
			put(s, fmt.Sprintf("s%d-%03d", session, i))
		}
		if got := s.Sequence(); got <= lastSeq {
			t.Fatalf("session %d: sequence %d did not advance past %d", session, got, lastSeq)
		}
		lastSeq = s.Sequence()
		lastApplied = s.AppliedIndex()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	s = openLSM(t, dir, opts)
	defer s.Close()
	for i := 0; i < 50; i++ {
		assertValue(t, s, fmt.Sprintf("k%03d", i), "v")
	}
	for session := 0; session < 4; session++ {
		for i := 0; i < 20; i++ {
			assertValue(t, s, fmt.Sprintf("s%d-%03d", session, i), "v")
		}
	}
}

// TestLeftoverLowSegmentsAreSweptOnOpen: a crash that interrupts a truncation
// can leave a segment below the log number on disk. The next open removes it —
// its records are in a table — and recovers the exact state, and a reopen right
// after is clean.
func TestLeftoverLowSegmentsAreSweptOnOpen(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(4 << 10)
	opts.WAL.SegmentSize = 512
	s := openLSM(t, dir, opts)
	for i := 0; i < 100; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%03d", i), "v")}, ai(uint64(i+1), 1)})
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate an interrupted truncation: drop a stray low-numbered segment into
	// the WAL directory, below the live log number. (A real crash leaves one the
	// deletion did not reach; this plants an obvious one.)
	walDir := filepath.Join(dir, "wal")
	stray := filepath.Join(walDir, "000001.log")
	if _, err := os.Stat(stray); err == nil {
		t.Fatal("premise: segment 000001 should already have been truncated away")
	}
	if err := os.WriteFile(stray, []byte("leftover from an interrupted truncation"), 0o644); err != nil {
		t.Fatal(err)
	}

	reopened := openLSM(t, dir, opts)
	if rec := reopened.Recovery(); rec.SegmentsRemovedOnOpen < 1 {
		t.Fatalf("the leftover low segment was not swept on open (SegmentsRemovedOnOpen=%d)", rec.SegmentsRemovedOnOpen)
	}
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatalf("the leftover segment is still present after open: %v", err)
	}
	for i := 0; i < 100; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%03d", i), "v")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	// A reopen right after recovery is clean and finds nothing more to sweep.
	again := openLSM(t, dir, opts)
	defer again.Close()
	if rec := again.Recovery(); rec.SegmentsRemovedOnOpen != 0 {
		t.Fatalf("a second open swept %d segments; recovery was not idempotent", rec.SegmentsRemovedOnOpen)
	}
	for i := 0; i < 100; i++ {
		assertValue(t, again, fmt.Sprintf("k%03d", i), "v")
	}
}

// TestTruncatedEngineAcceptsWritesAndReopens: after a reopen on a truncated log
// the engine serves reads, takes new writes, and those writes survive their own
// reopen — recovery is not a one-way door.
func TestTruncatedEngineAcceptsWritesAndReopens(t *testing.T) {
	dir := t.TempDir()
	opts := lsmOpts(4 << 10)
	opts.WAL.SegmentSize = 512
	s := openLSM(t, dir, opts)
	for i := 0; i < 80; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%03d", i), "v")}, ai(uint64(i+1), 1)})
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openLSM(t, dir, opts)
	if _, err := s.Get(context.Background(), []byte("k000")); err != nil {
		t.Fatalf("reading after recovery: %v", err)
	}
	for i := 80; i < 120; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%03d", i), "v")}, ai(uint64(i+1), 1)})
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openLSM(t, dir, opts)
	defer s.Close()
	for i := 0; i < 120; i++ {
		assertValue(t, s, fmt.Sprintf("k%03d", i), "v")
	}
	if got := s.AppliedIndex(); got != ai(120, 1) {
		t.Fatalf("recovered applied index %+v, want (120,1)", got)
	}
}

// TestFlushWithoutWALRotatesNothingDuringReplay is a guard on the replay-flush
// path: a flush that runs while the WAL is not yet open (recovering a log larger
// than memory) advances the watermark but records no log number, so the segments
// it builds tables from stay readable. Session one uses a large memtable so
// nothing flushes and the whole log is unflushed; the reopen uses a small one so
// replay must flush mid-stream.
func TestFlushWithoutWALRotatesNothingDuringReplay(t *testing.T) {
	dir := t.TempDir()
	big := lsmOpts(storage.DefaultMemTableSize) // no flush in session one
	s := openLSM(t, dir, big)
	const n = 300
	for i := 0; i < n; i++ {
		mustApply(t, s, applyStep{[]storage.Mutation{mput(fmt.Sprintf("k%04d", i), "vvvvvvvvvvvvvvvv")}, ai(uint64(i+1), 1)})
	}
	if r := s.Recovery(); r.SegmentsRemovedOnOpen != 0 {
		t.Fatal("premise: session one should not have truncated anything")
	}
	// Close without flushing the memtable (Close never flushes): the whole log is
	// on disk and the next open must replay it, flushing mid-replay.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	small := lsmOpts(2 << 10) // force replay to flush
	reopened := openLSM(t, dir, small)
	defer reopened.Close()
	if rec := reopened.Recovery(); rec.FlushesOnReplay == 0 {
		t.Fatalf("premise: replay did not flush; raise n or lower the memtable (recovery %+v)", rec)
	}
	for i := 0; i < n; i++ {
		assertValue(t, reopened, fmt.Sprintf("k%04d", i), "vvvvvvvvvvvvvvvv")
	}
	if got := reopened.AppliedIndex(); got != ai(n, 1) {
		t.Fatalf("recovered applied index %+v, want %+v", got, ai(n, 1))
	}
}
