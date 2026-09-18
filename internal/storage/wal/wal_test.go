package wal_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// ---------------------------------------------------------------- helpers

// collector records everything replay hands it, so a test can assert on the
// exact sequence rather than on a summary.
type collector struct {
	batches []wal.Batch
	applied []wal.AppliedIndex
}

func (c *collector) handler() wal.Handler {
	return wal.Handler{
		Batch: func(b wal.Batch) error {
			// Copy: the decoder's buffers are not retained past the call.
			cp := make(wal.Batch, len(b))
			for i, op := range b {
				cp[i] = wal.Op{
					Kind:  op.Kind,
					Key:   append([]byte(nil), op.Key...),
					Value: append([]byte(nil), op.Value...),
				}
			}
			c.batches = append(c.batches, cp)
			return nil
		},
		Applied: func(a wal.AppliedIndex) error {
			c.applied = append(c.applied, a)
			return nil
		},
	}
}

// flat renders the replayed batches as a comparable string, so two replays can
// be checked for exact equality including order.
func (c *collector) flat() string {
	var b bytes.Buffer
	for i, batch := range c.batches {
		fmt.Fprintf(&b, "batch %d:\n", i)
		for _, op := range batch {
			fmt.Fprintf(&b, "  %s %q %q\n", op.Kind, op.Key, op.Value)
		}
	}
	for _, a := range c.applied {
		fmt.Fprintf(&b, "applied %d/%d\n", a.Index, a.Term)
	}
	return b.String()
}

func mustRecover(t *testing.T, dir string) (*collector, wal.Recovery) {
	t.Helper()
	c := &collector{}
	rec, err := wal.Recover(dir, c.handler())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	return c, rec
}

func put(k, v string) wal.Op  { return wal.Op{Kind: wal.OpPut, Key: []byte(k), Value: []byte(v)} }
func del(k string) wal.Op     { return wal.Op{Kind: wal.OpDelete, Key: []byte(k)} }
func one(op wal.Op) wal.Batch { return wal.Batch{op} }
func testDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal")
}

// segments lists the segment files present, sorted.
func segments(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var names []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat %s: %v", path, err)
	}
	return info.Size()
}

// flipByte corrupts one byte in a file.
func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatalf("ReadAt %s %d: %v", path, off, err)
	}
	buf[0] ^= 0xff
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("WriteAt %s %d: %v", path, off, err)
	}
}

func truncateFile(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("Truncate %s to %d: %v", path, size, err)
	}
}

// ---------------------------------------------------------------- basics

func TestRecoverMissingDirectoryIsEmptyLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	c, rec := mustRecover(t, dir)
	if len(c.batches) != 0 {
		t.Fatalf("replayed %d batches from a nonexistent directory", len(c.batches))
	}
	if rec.SegmentsScanned != 0 || rec.Truncated {
		t.Fatalf("Recovery = %+v, want an empty result", rec)
	}
}

func TestRecoverEmptyDirectory(t *testing.T) {
	dir := testDir(t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	c, rec := mustRecover(t, dir)
	if len(c.batches) != 0 || rec.SegmentsScanned != 0 {
		t.Fatalf("empty directory replayed %d batches across %d segments", len(c.batches), rec.SegmentsScanned)
	}
}

func TestRecoverEmptySegmentFile(t *testing.T) {
	dir := testDir(t)
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := segments(t, dir); len(got) != 1 || got[0] != "000001.log" {
		t.Fatalf("segments = %v, want [000001.log]", got)
	}
	c, rec := mustRecover(t, dir)
	if len(c.batches) != 0 {
		t.Fatalf("replayed %d batches from an empty segment", len(c.batches))
	}
	if rec.SegmentsScanned != 1 || rec.Truncated {
		t.Fatalf("Recovery = %+v", rec)
	}
}

func TestAppendAndRecover(t *testing.T) {
	dir := testDir(t)
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	writes := []wal.Op{
		put("user:1", "Adi"),
		put("user:2", "Bo"),
		put("user:1", "Adi Vishal"), // overwrite
		del("user:2"),
		put("user:3", ""), // empty value
		del("never-existed"),
	}
	for i, op := range writes {
		if err := w.AppendBatch(one(op)); err != nil {
			t.Fatalf("AppendBatch #%d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c, rec := mustRecover(t, dir)
	if len(c.batches) != len(writes) {
		t.Fatalf("replayed %d batches, want %d", len(c.batches), len(writes))
	}
	// Order is the whole point: replay must reproduce the append order exactly,
	// or an overwrite lands before the value it replaced.
	for i, op := range writes {
		got := c.batches[i]
		if len(got) != 1 {
			t.Fatalf("batch %d has %d operations, want 1", i, len(got))
		}
		if got[0].Kind != op.Kind || !bytes.Equal(got[0].Key, op.Key) {
			t.Fatalf("batch %d = %s %q, want %s %q", i, got[0].Kind, got[0].Key, op.Kind, op.Key)
		}
		if op.Kind == wal.OpPut && !bytes.Equal(got[0].Value, op.Value) {
			t.Fatalf("batch %d value = %q, want %q", i, got[0].Value, op.Value)
		}
	}
	if rec.OpsApplied != int64(len(writes)) {
		t.Errorf("Recovery.OpsApplied = %d, want %d", rec.OpsApplied, len(writes))
	}
	if rec.Truncated {
		t.Errorf("a cleanly closed WAL was truncated: %+v", rec)
	}
}

func TestMultiOperationBatch(t *testing.T) {
	dir := testDir(t)
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	batch := wal.Batch{put("a", "1"), del("b"), put("c", "3")}
	if err := w.AppendBatch(batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	c, _ := mustRecover(t, dir)
	if len(c.batches) != 1 {
		t.Fatalf("replayed %d batches, want 1", len(c.batches))
	}
	if len(c.batches[0]) != 3 {
		t.Fatalf("batch has %d operations, want 3", len(c.batches[0]))
	}
}

func TestAppendedAppliedIndexSurvivesRecovery(t *testing.T) {
	dir := testDir(t)
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendBatch(one(put("k", "v"))); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendAppliedIndex(wal.AppliedIndex{Index: 17, Term: 3}); err != nil {
		t.Fatalf("AppendAppliedIndex: %v", err)
	}
	if err := w.AppendBatch(one(put("k2", "v2"))); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendAppliedIndex(wal.AppliedIndex{Index: 18, Term: 3}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	c, rec := mustRecover(t, dir)
	if len(c.applied) != 2 {
		t.Fatalf("replayed %d applied-index records, want 2", len(c.applied))
	}
	// The last one wins, which is what the Phase 9 reconciliation will need.
	if want := (wal.AppliedIndex{Index: 18, Term: 3}); rec.AppliedIndex != want {
		t.Fatalf("Recovery.AppliedIndex = %+v, want %+v", rec.AppliedIndex, want)
	}
	if !rec.SawAppliedIndex {
		t.Error("SawAppliedIndex is false despite two records")
	}
	if len(c.batches) != 2 {
		t.Fatalf("replayed %d batches, want 2", len(c.batches))
	}
}

func TestReopenAppendsToExistingSegment(t *testing.T) {
	dir := testDir(t)

	for round := 0; round < 3; round++ {
		if _, err := wal.Recover(dir, wal.Handler{}); err != nil {
			t.Fatalf("round %d: Recover: %v", round, err)
		}
		w, err := wal.Create(dir, wal.DefaultOptions())
		if err != nil {
			t.Fatalf("round %d: Create: %v", round, err)
		}
		if err := w.AppendBatch(one(put(fmt.Sprintf("round%d", round), "v"))); err != nil {
			t.Fatalf("round %d: AppendBatch: %v", round, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
	}

	if got := segments(t, dir); len(got) != 1 {
		t.Fatalf("segments = %v, want a single reused segment", got)
	}
	c, _ := mustRecover(t, dir)
	if len(c.batches) != 3 {
		t.Fatalf("replayed %d batches across three sessions, want 3", len(c.batches))
	}
	for i, b := range c.batches {
		want := fmt.Sprintf("round%d", i)
		if string(b[0].Key) != want {
			t.Errorf("batch %d key = %q, want %q", i, b[0].Key, want)
		}
	}
}

// ---------------------------------------------------------------- segments

func TestSegmentRotation(t *testing.T) {
	dir := testDir(t)
	opts := wal.DefaultOptions()
	opts.SegmentSize = 1 << 10 // 1 KiB, so rotation happens quickly

	w, err := wal.Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	const n = 200
	value := bytes.Repeat([]byte("v"), 64)
	for i := 0; i < n; i++ {
		op := wal.Op{Kind: wal.OpPut, Key: []byte(fmt.Sprintf("key%04d", i)), Value: value}
		if err := w.AppendBatch(one(op)); err != nil {
			t.Fatalf("AppendBatch #%d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	names := segments(t, dir)
	if len(names) < 5 {
		t.Fatalf("segments = %v, want several after %d records at a 1 KiB threshold", names, n)
	}
	// Names must be the documented format, contiguous from 1.
	for i, name := range names {
		if want := fmt.Sprintf("%06d.log", i+1); name != want {
			t.Fatalf("segment %d is named %q, want %q", i, name, want)
		}
	}
	// A record is never split across segments, so a segment may exceed the
	// threshold by up to one record but must never be empty mid-log.
	for _, name := range names[:len(names)-1] {
		if size := fileSize(t, filepath.Join(dir, name)); size < opts.SegmentSize {
			t.Errorf("segment %s is %d bytes, below the %d threshold despite not being last",
				name, size, opts.SegmentSize)
		}
	}

	c, rec := mustRecover(t, dir)
	if len(c.batches) != n {
		t.Fatalf("replayed %d batches across %d segments, want %d", len(c.batches), rec.SegmentsScanned, n)
	}
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("key%04d", i); string(c.batches[i][0].Key) != want {
			t.Fatalf("batch %d key = %q, want %q (segments replayed out of order)", i, c.batches[i][0].Key, want)
		}
	}
}

func TestRotationContinuesAfterReopen(t *testing.T) {
	dir := testDir(t)
	opts := wal.DefaultOptions()
	opts.SegmentSize = 512

	for session := 0; session < 3; session++ {
		if _, err := wal.Recover(dir, wal.Handler{}); err != nil {
			t.Fatalf("session %d: Recover: %v", session, err)
		}
		w, err := wal.Create(dir, opts)
		if err != nil {
			t.Fatalf("session %d: Create: %v", session, err)
		}
		for i := 0; i < 20; i++ {
			op := wal.Op{
				Kind:  wal.OpPut,
				Key:   []byte(fmt.Sprintf("s%d-k%02d", session, i)),
				Value: bytes.Repeat([]byte("v"), 64),
			}
			if err := w.AppendBatch(one(op)); err != nil {
				t.Fatalf("session %d: append: %v", session, err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	c, _ := mustRecover(t, dir)
	if len(c.batches) != 60 {
		t.Fatalf("replayed %d batches, want 60", len(c.batches))
	}
	for i, b := range c.batches {
		want := fmt.Sprintf("s%d-k%02d", i/20, i%20)
		if string(b[0].Key) != want {
			t.Fatalf("batch %d key = %q, want %q", i, b[0].Key, want)
		}
	}
}

func TestSegmentGapIsRefused(t *testing.T) {
	dir := testDir(t)
	opts := wal.DefaultOptions()
	opts.SegmentSize = 256

	w, err := wal.Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		op := wal.Op{Kind: wal.OpPut, Key: []byte(fmt.Sprintf("k%03d", i)), Value: bytes.Repeat([]byte("v"), 32)}
		if err := w.AppendBatch(one(op)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	names := segments(t, dir)
	if len(names) < 3 {
		t.Fatalf("need at least three segments, got %v", names)
	}
	// Delete a middle segment: every record it held is gone, and replaying the
	// survivors would produce a state that never existed.
	if err := os.Remove(filepath.Join(dir, names[1])); err != nil {
		t.Fatal(err)
	}

	_, err = wal.Recover(dir, wal.Handler{})
	if !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("Recover with a missing middle segment = %v, want ErrCorrupt", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("gap")) {
		t.Errorf("error = %q, want it to name the gap", err)
	}
}

func TestNonSegmentFilesAreIgnored(t *testing.T) {
	dir := testDir(t)
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendBatch(one(put("k", "v"))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// macOS and editors leave files around. They must not stop the database
	// from opening, and must not be mistaken for log data.
	for _, name := range []string{".DS_Store", "000001.log.swp", "1.log", "0000001.log", "notes.txt", "00000a.log"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("not a segment"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	c, rec := mustRecover(t, dir)
	if len(c.batches) != 1 {
		t.Fatalf("replayed %d batches, want 1", len(c.batches))
	}
	if rec.SegmentsScanned != 1 {
		t.Errorf("SegmentsScanned = %d, want 1", rec.SegmentsScanned)
	}
	if rec.IgnoredEntries != 7 {
		t.Errorf("IgnoredEntries = %d, want 7", rec.IgnoredEntries)
	}
}

// ---------------------------------------------------------------- determinism

// TestReplayIsDeterministic establishes INV-S2: replaying the same bytes any
// number of times yields the identical sequence, and replay does not consume or
// alter the log.
func TestReplayIsDeterministic(t *testing.T) {
	dir := testDir(t)
	opts := wal.DefaultOptions()
	opts.SegmentSize = 2 << 10

	w, err := wal.Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		var b wal.Batch
		switch i % 4 {
		case 0:
			b = one(put(fmt.Sprintf("k%03d", i%37), fmt.Sprintf("v%03d", i)))
		case 1:
			b = one(del(fmt.Sprintf("k%03d", i%37)))
		case 2:
			b = wal.Batch{put("multi-a", "1"), del("multi-b"), put("multi-c", fmt.Sprintf("%d", i))}
		case 3:
			if err := w.AppendAppliedIndex(wal.AppliedIndex{Index: uint64(i), Term: uint64(i / 10)}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := w.AppendBatch(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	first, firstRec := mustRecover(t, dir)
	want := first.flat()
	if want == "" {
		t.Fatal("replay produced nothing")
	}

	for attempt := 0; attempt < 5; attempt++ {
		got, rec := mustRecover(t, dir)
		if got.flat() != want {
			t.Fatalf("replay %d differs from the first replay", attempt)
		}
		if rec.RecordsApplied != firstRec.RecordsApplied ||
			rec.OpsApplied != firstRec.OpsApplied ||
			rec.BytesScanned != firstRec.BytesScanned {
			t.Fatalf("replay %d summary = %+v, want %+v", attempt, rec, firstRec)
		}
		if rec.Truncated {
			t.Fatalf("replay %d truncated an intact log: %+v", attempt, rec)
		}
	}
}

// ---------------------------------------------------------------- sync modes

func TestSyncModesAllRoundTrip(t *testing.T) {
	for _, mode := range []wal.SyncMode{wal.SyncOff, wal.SyncBatch, wal.SyncAlways} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := testDir(t)
			opts := wal.DefaultOptions()
			opts.SyncMode = mode
			opts.SyncInterval = 5 * time.Millisecond

			w, err := wal.Create(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 50; i++ {
				if err := w.AppendBatch(one(put(fmt.Sprintf("k%02d", i), "v"))); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			if got := w.Stats().SyncMode; got != mode {
				t.Errorf("Stats().SyncMode = %v, want %v", got, mode)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			c, _ := mustRecover(t, dir)
			if len(c.batches) != 50 {
				t.Fatalf("mode %v: replayed %d batches, want 50", mode, len(c.batches))
			}
		})
	}
}

func TestParseSyncMode(t *testing.T) {
	for in, want := range map[string]wal.SyncMode{"off": wal.SyncOff, "batch": wal.SyncBatch, "sync": wal.SyncAlways} {
		got, err := wal.ParseSyncMode(in)
		if err != nil {
			t.Errorf("ParseSyncMode(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseSyncMode(%q) = %v, want %v", in, got, want)
		}
		if got.String() != in {
			t.Errorf("%v.String() = %q, want %q", got, got.String(), in)
		}
	}
	if _, err := wal.ParseSyncMode("fsync"); err == nil {
		t.Error("ParseSyncMode(\"fsync\") succeeded, want an error")
	}
}

func TestSyncAlwaysFlushesEveryAppend(t *testing.T) {
	dir := testDir(t)
	opts := wal.DefaultOptions()
	opts.SyncMode = wal.SyncAlways

	w, err := wal.Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	for i := 0; i < 5; i++ {
		if err := w.AppendBatch(one(put("k", "v"))); err != nil {
			t.Fatal(err)
		}
		if got := w.Stats().UnsyncedBytes; got != 0 {
			t.Fatalf("after append %d there are %d unsynced bytes; sync mode must leave none", i, got)
		}
	}
}

func TestBatchModeFlushesInBackground(t *testing.T) {
	dir := testDir(t)
	opts := wal.DefaultOptions()
	opts.SyncMode = wal.SyncBatch
	opts.SyncInterval = 10 * time.Millisecond
	opts.SyncBytes = 1 << 30 // force the time-based path, not the byte-based one

	w, err := wal.Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if err := w.AppendBatch(one(put("k", "v"))); err != nil {
		t.Fatal(err)
	}
	if w.Stats().UnsyncedBytes == 0 {
		t.Skip("the append was flushed before it could be observed as unsynced")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.Stats().UnsyncedBytes == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("batch mode left %d bytes unsynced after 2s with a 10ms interval", w.Stats().UnsyncedBytes)
}

func TestClosedWALRejectsAppends(t *testing.T) {
	dir := testDir(t)
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close = %v, want nil (Close is idempotent)", err)
	}
	if err := w.AppendBatch(one(put("k", "v"))); !errors.Is(err, wal.ErrClosed) {
		t.Errorf("AppendBatch after Close = %v, want ErrClosed", err)
	}
	if err := w.AppendAppliedIndex(wal.AppliedIndex{}); !errors.Is(err, wal.ErrClosed) {
		t.Errorf("AppendAppliedIndex after Close = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, wal.ErrClosed) {
		t.Errorf("Sync after Close = %v, want ErrClosed", err)
	}
}

func TestAppendEmptyBatchIsRejected(t *testing.T) {
	dir := testDir(t)
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.AppendBatch(nil); err == nil {
		t.Error("AppendBatch(nil) succeeded; an empty batch must never reach the log")
	}
}

func TestLargeRecordNearFramingLimit(t *testing.T) {
	dir := testDir(t)
	opts := wal.DefaultOptions()
	opts.SegmentSize = 1 << 10 // deliberately smaller than the record

	w, err := wal.Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("x"), 4<<20) // 4 MiB, far above the segment threshold
	if err := w.AppendBatch(wal.Batch{{Kind: wal.OpPut, Key: []byte("big"), Value: big}}); err != nil {
		t.Fatalf("AppendBatch(4 MiB): %v", err)
	}
	if err := w.AppendBatch(one(put("after", "v"))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	c, _ := mustRecover(t, dir)
	if len(c.batches) != 2 {
		t.Fatalf("replayed %d batches, want 2", len(c.batches))
	}
	if !bytes.Equal(c.batches[0][0].Value, big) {
		t.Fatal("the oversized record did not round-trip")
	}
	// A record is never split, so it lands whole in one segment even though
	// that segment then exceeds the rotation threshold.
	if got := segments(t, dir); len(got) != 2 {
		t.Fatalf("segments = %v, want 2 (the big record's own, then the next)", got)
	}
	if size := fileSize(t, filepath.Join(dir, "000001.log")); size < 4<<20 {
		t.Errorf("segment 1 is %d bytes; the 4 MiB record was split across segments", size)
	}
}

func TestRecordKindsAreDistinct(t *testing.T) {
	if wal.KindWriteBatch == wal.KindAppliedIndex {
		t.Fatal("record kinds collide")
	}
	if wal.KindWriteBatch != record.Kind(0x01) || wal.KindAppliedIndex != record.Kind(0x02) {
		t.Fatalf("record kinds are %#x/%#x, want 0x01/0x02 per docs/DESIGN.md §3",
			wal.KindWriteBatch, wal.KindAppliedIndex)
	}
}
