package fault

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestInjectFailsTheNthMatchingOperation proves an armed fault fires on exactly
// the Nth matching operation, once, and that the error is recognisable both as
// injected and as the requested errno (e.g. ENOSPC for a full disk).
func TestInjectFailsTheNthMatchingOperation(t *testing.T) {
	inj := NewInjectFS(NewMemFS())
	f, err := inj.OpenFile("/d/log", os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	inj.Arm(Injection{Op: OpWrite, Nth: 3, Err: syscall.ENOSPC})

	for i := 1; i <= 4; i++ {
		_, err := f.Write([]byte("x"))
		switch {
		case i == 3 && err == nil:
			t.Fatal("third write succeeded; the fault did not fire")
		case i == 3 && (!errors.Is(err, ErrInjected) || !errors.Is(err, syscall.ENOSPC)):
			t.Fatalf("injected error %v does not wrap ErrInjected and ENOSPC", err)
		case i != 3 && err != nil:
			t.Fatalf("write %d failed: %v (the fault must fire exactly once)", i, err)
		}
	}
	if inj.Armed() != 0 {
		t.Fatalf("fault still armed after firing")
	}
}

// TestInjectShortWriteLeavesATornRecord proves a short write really leaves the
// partial bytes in the file (what a torn append looks like) and reports them.
func TestInjectShortWriteLeavesATornRecord(t *testing.T) {
	mem := NewMemFS()
	inj := NewInjectFS(mem)
	f, _ := inj.OpenFile("/d/log", os.O_RDWR|os.O_CREATE, 0o644)
	inj.Arm(Injection{Op: OpWrite, Short: 4})
	n, err := f.Write([]byte("abcdefgh"))
	if err == nil || n != 4 {
		t.Fatalf("short write: n=%d err=%v, want 4 bytes and an error", n, err)
	}
	if b, _ := mem.Cached("/d/log"); string(b) != "abcd" {
		t.Fatalf("file holds %q, want the torn prefix abcd", b)
	}
	ops := inj.Ops()
	last := ops[len(ops)-1]
	if last.Op != OpWrite || !last.Injected || last.Bytes != 4 {
		t.Fatalf("op log did not record the torn write: %+v", last)
	}
}

// TestInjectFailedSyncDoesNotMakeDataDurable proves a failed fsync leaves the
// written bytes cached but NOT durable — a power loss then loses them — which is
// the only thing a failed fsync can be assumed to mean.
func TestInjectFailedSyncDoesNotMakeDataDurable(t *testing.T) {
	mem := NewMemFS()
	inj := NewInjectFS(mem)
	f, _ := inj.OpenFile("/d/log", os.O_RDWR|os.O_CREATE, 0o644)
	_ = inj.SyncDir("/d")
	_, _ = f.Write([]byte("v1"))
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("v2"))
	inj.Arm(Injection{Op: OpSync})
	if err := f.Sync(); !errors.Is(err, ErrInjected) {
		t.Fatalf("sync err = %v, want injected", err)
	}
	if mem.FullySynced("/d/log") {
		t.Fatal("data became durable although the fsync failed")
	}
	mem.CrashPowerLoss(0)
	if b, _ := mem.Cached("/d/log"); string(b) != "v1" {
		t.Fatalf("after power loss = %q, want only the synced v1", b)
	}
}

// TestInjectPathFilterAndOpLog proves a fault restricted to one file does not fire
// on another, and the op log records every mutating operation in order.
func TestInjectPathFilterAndOpLog(t *testing.T) {
	inj := NewInjectFS(NewMemFS())
	a, _ := inj.OpenFile("/d/a", os.O_RDWR|os.O_CREATE, 0o644)
	b, _ := inj.OpenFile("/d/b", os.O_RDWR|os.O_CREATE, 0o644)
	inj.Arm(Injection{Op: OpSync, Path: "/d/b"})
	if err := a.Sync(); err != nil {
		t.Fatalf("sync of a hit a fault armed for b: %v", err)
	}
	if err := b.Sync(); !errors.Is(err, ErrInjected) {
		t.Fatalf("sync of b: %v, want injected", err)
	}
	var got []string
	for _, r := range inj.Ops() {
		got = append(got, r.Op.String()+":"+filepath.Base(r.Path))
	}
	want := []string{"open:a", "open:b", "fsync:a", "fsync:b"}
	if len(got) != len(want) {
		t.Fatalf("op log = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("op log = %v, want %v", got, want)
		}
	}
}

// TestInjectGateStallsThenProceeds proves a gated injection is a stall, not a
// failure: the fsync blocks until the gate opens and then succeeds and makes the
// data durable.
func TestInjectGateStallsThenProceeds(t *testing.T) {
	mem := NewMemFS()
	inj := NewInjectFS(mem)
	f, _ := inj.OpenFile("/d/log", os.O_RDWR|os.O_CREATE, 0o644)
	_ = inj.SyncDir("/d")
	_, _ = f.Write([]byte("slow"))
	gate := make(chan struct{})
	inj.Arm(Injection{Op: OpSync, Gate: gate})
	done := make(chan error, 1)
	go func() { done <- f.Sync() }()
	select {
	case err := <-done:
		t.Fatalf("stalled fsync returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("fsync after the stall: %v, want success", err)
	}
	if !mem.FullySynced("/d/log") {
		t.Fatal("a stalled-then-completed fsync did not make the data durable")
	}
}

// TestInjectOverRealFiles proves the decorator works on the real OS filesystem
// too: an injected fsync failure on a real file, and a real write afterwards.
func TestInjectOverRealFiles(t *testing.T) {
	dir := t.TempDir()
	inj := NewInjectFS(nil) // nil base = the real OS
	path := filepath.Join(dir, "real.log")
	f, err := inj.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	inj.Arm(Injection{Op: OpSync, Err: syscall.EIO})
	_, _ = f.Write([]byte("hello"))
	if err := f.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("sync: %v, want injected EIO", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("second sync (fault spent): %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "hello" {
		t.Fatalf("real file holds %q, %v", b, err)
	}
}
