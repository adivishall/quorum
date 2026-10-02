package wal

import (
	"errors"
	"io"
	"os"
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
