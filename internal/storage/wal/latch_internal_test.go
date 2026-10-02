package wal

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// A WAL that failed a write or a flush never writes again (audit D10, M11).

// tearNth tears the nth write through it: only short bytes of it reach the
// file, then it fails.
type tearNth struct {
	w     io.Writer
	n     *int
	nth   int
	short int
}

func (t tearNth) Write(p []byte) (int, error) {
	*t.n++
	if *t.n == t.nth {
		k, _ := t.w.Write(p[:min(t.short, len(p)-1)])
		return k, syscall.EIO
	}
	return t.w.Write(p)
}

func batchOf(k string) Batch { return Batch{{Kind: OpPut, Key: []byte(k), Value: []byte("v")}} }

// TestATornAppendLatches: an append whose write tears leaves a partial record
// — recoverable, as a torn TAIL. The failure latches, so nothing is written
// after it and the log recovers to the records before it. Before, the next
// append wrote after the partial record, turning the torn tail into damage
// mid-segment that recovery refuses.
func TestATornAppendLatches(t *testing.T) {
	writes := 0
	defer func(prev func(*os.File) io.Writer) { segmentOut = prev }(segmentOut)
	segmentOut = func(f *os.File) io.Writer { return tearNth{w: f, n: &writes, nth: 2, short: 5} }

	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SyncMode = SyncOff
	w, err := Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendBatch(batchOf("a")); err != nil {
		t.Fatal(err)
	}
	first := w.AppendBatch(batchOf("b"))
	if first == nil {
		t.Fatal("the torn append succeeded")
	}
	before := w.Stats().ActiveBytes
	if err := w.AppendBatch(batchOf("c")); err == nil || err.Error() != first.Error() {
		t.Fatalf("an append after a torn one: %v, want the latched %v", err, first)
	}
	if got := w.Stats().ActiveBytes; got != before {
		t.Fatalf("an append after a torn one wrote %d bytes", got-before)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close of a failed WAL reported success")
	}
	var keys []string
	rec, err := Recover(dir, Handler{Batch: func(b Batch) error {
		keys = append(keys, string(b[0].Key))
		return nil
	}})
	if err != nil {
		t.Fatalf("recovery after a torn append: %v; the torn record must stay a tail", err)
	}
	if len(keys) != 1 || keys[0] != "a" || !rec.Truncated {
		t.Fatalf("recovered %v (truncated %v), want exactly the append before the torn one", keys, rec.Truncated)
	}
}

// TestCloseAfterAFailedFlushReportsItAndDoesNotFlushAgain: once a flush has
// failed, Close returns that failure without flushing again. A second fsync
// can succeed after the kernel dropped the pages the first failed to write,
// and Close used to report that as durability (the fsyncgate error).
func TestCloseAfterAFailedFlushReportsItAndDoesNotFlushAgain(t *testing.T) {
	calls := 0
	defer func(prev func(*os.File) error) { syncFile = prev }(syncFile)
	syncFile = func(f *os.File) error {
		calls++
		if calls == 1 {
			return syscall.EIO
		}
		return f.Sync()
	}
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SyncMode = SyncAlways
	w, err := Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendBatch(batchOf("a")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the append whose flush failed: %v, want EIO", err)
	}
	if err := w.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Close after a failed flush: %v, want the latched failure", err)
	}
	if calls != 1 {
		t.Fatalf("Close flushed again after a failed flush (%d flushes)", calls)
	}
}

// TestCloseAfterAFailedWriteStillFlushes: a failed write latches, but the
// records acknowledged before it were never flushed in batch mode, and Close
// still owes them that flush — only a failed FLUSH forbids another. Close
// used to skip it after any failure.
func TestCloseAfterAFailedWriteStillFlushes(t *testing.T) {
	writes, syncs := 0, 0
	defer func(prev func(*os.File) io.Writer) { segmentOut = prev }(segmentOut)
	segmentOut = func(f *os.File) io.Writer { return tearNth{w: f, n: &writes, nth: 2, short: 5} }
	defer func(prev func(*os.File) error) { syncFile = prev }(syncFile)
	syncFile = func(f *os.File) error {
		syncs++
		return f.Sync()
	}
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SyncMode = SyncBatch
	opts.SyncInterval = time.Hour // only Close flushes
	opts.SyncBytes = 1 << 40
	w, err := Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendBatch(batchOf("a")); err != nil {
		t.Fatal(err)
	}
	failed := w.AppendBatch(batchOf("b"))
	if failed == nil {
		t.Fatal("the torn append succeeded")
	}
	before := syncs
	if err := w.Close(); err == nil || err.Error() != failed.Error() {
		t.Fatalf("Close after a failed write: %v, want the latched %v", err, failed)
	}
	if syncs != before+1 {
		t.Fatalf("Close after a failed write flushed %d times, want once: the acknowledged record is never made durable", syncs-before)
	}
}

// TestAnOversizedRecordLatchesNothing: an append the framing cannot hold is
// refused before anything is written, and the log goes on taking appends. It
// latched: one refused record — nothing on disk — failed every later append.
func TestAnOversizedRecordLatchesNothing(t *testing.T) {
	opts := DefaultOptions()
	opts.SyncMode = SyncOff
	w, err := Create(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	big := Batch{{Kind: OpPut, Key: []byte("k"), Value: make([]byte, 65<<20)}}
	if err := w.AppendBatch(big); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("an oversized append: %v, want ErrRecordTooLarge", err)
	}
	if got := w.Stats().ActiveBytes; got != 0 {
		t.Fatalf("the refused append wrote %d bytes", got)
	}
	if err := w.AppendBatch(batchOf("a")); err != nil {
		t.Fatalf("an append after a refused oversized one: %v", err)
	}
}

// TestAFailedWriteDoesNotStopTheBatchFlush: after a failed write, batch mode
// still flushes the records acknowledged before it within its interval, and
// Sync flushes them too — reporting the latched failure, since the log takes
// no more appends. Only a failed flush forbids another. Both refused after
// any failure, so those records stayed unflushed until Close.
func TestAFailedWriteDoesNotStopTheBatchFlush(t *testing.T) {
	writes := 0
	var mu sync.Mutex
	syncs := 0
	defer func(prev func(*os.File) io.Writer) { segmentOut = prev }(segmentOut)
	segmentOut = func(f *os.File) io.Writer { return tearNth{w: f, n: &writes, nth: 2, short: 5} }
	defer func(prev func(*os.File) error) { syncFile = prev }(syncFile)
	syncFile = func(f *os.File) error {
		mu.Lock()
		syncs++
		mu.Unlock()
		return f.Sync()
	}
	opts := DefaultOptions()
	opts.SyncMode = SyncBatch
	opts.SyncInterval = 20 * time.Millisecond
	opts.SyncBytes = 1 << 40
	w, err := Create(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.AppendBatch(batchOf("a")); err != nil { // acknowledged, unflushed
		t.Fatal(err)
	}
	failed := w.AppendBatch(batchOf("b"))
	if failed == nil {
		t.Fatal("premise: the torn append succeeded")
	}
	deadline := time.Now().Add(5 * time.Second)
	for w.Stats().UnsyncedBytes != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the batch syncer never flushed after a failed write (%d bytes unsynced)", w.Stats().UnsyncedBytes)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := w.Sync(); err == nil || err.Error() != failed.Error() {
		t.Fatalf("Sync after a failed write: %v, want the latched %v", err, failed)
	}
	mu.Lock()
	defer mu.Unlock()
	if syncs == 0 {
		t.Fatal("no flush happened")
	}
}

// TestSyncAfterAFailedWriteFlushes: an explicit Sync after a failed write
// flushes the records acknowledged before it, and reports the latched
// failure. It refused without flushing.
func TestSyncAfterAFailedWriteFlushes(t *testing.T) {
	writes, syncs := 0, 0
	defer func(prev func(*os.File) io.Writer) { segmentOut = prev }(segmentOut)
	segmentOut = func(f *os.File) io.Writer { return tearNth{w: f, n: &writes, nth: 2, short: 5} }
	defer func(prev func(*os.File) error) { syncFile = prev }(syncFile)
	syncFile = func(f *os.File) error {
		syncs++
		return f.Sync()
	}
	opts := DefaultOptions()
	opts.SyncMode = SyncBatch
	opts.SyncInterval = time.Hour // only explicit flushes
	opts.SyncBytes = 1 << 40
	w, err := Create(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.AppendBatch(batchOf("a")); err != nil {
		t.Fatal(err)
	}
	failed := w.AppendBatch(batchOf("b"))
	if failed == nil {
		t.Fatal("premise: the torn append succeeded")
	}
	if err := w.Sync(); err == nil || err.Error() != failed.Error() {
		t.Fatalf("Sync after a failed write: %v, want the latched %v", err, failed)
	}
	if syncs != 1 || w.Stats().UnsyncedBytes != 0 {
		t.Fatalf("Sync after a failed write flushed %d times, %d bytes still unsynced", syncs, w.Stats().UnsyncedBytes)
	}
}
