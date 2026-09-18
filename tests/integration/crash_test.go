package integration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// The child process is this same test binary, re-executed with these variables
// set. TestMain notices them and runs the child instead of the test suite,
// which avoids shipping a helper command just for testing.
const (
	envDir   = "DKV_CRASH_DIR"
	envMode  = "DKV_CRASH_MODE"  // "burst" or "stream"
	envSync  = "DKV_CRASH_SYNC"  // "off", "batch", "sync"
	envCount = "DKV_CRASH_COUNT" // burst mode: how many keys to write

	readyLine   = "READY"
	childTimout = 30 * time.Second
)

func TestMain(m *testing.M) {
	if os.Getenv(envDir) != "" {
		runChild() // never returns
	}
	os.Exit(m.Run())
}

// keyFor and valueFor define the child's write pattern, shared with the parent
// so the parent can verify exactly what should be present.
func keyFor(i int) string   { return fmt.Sprintf("key%08d", i) }
func valueFor(i int) string { return fmt.Sprintf("value-%08d", i) }

// runChild performs writes and then waits to be killed. It never exits on its
// own: if it did, the test would be measuring a graceful shutdown.
func runChild() {
	dir := os.Getenv(envDir)

	mode, err := wal.ParseSyncMode(os.Getenv(envSync))
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: %v\n", err)
		os.Exit(2)
	}
	opts := storage.DefaultOptions()
	opts.WAL.SyncMode = mode

	s, err := storage.OpenWALStore(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: OpenWALStore: %v\n", err)
		os.Exit(2)
	}
	// Deliberately no defer Close. This process is going to be destroyed.

	ctx := context.Background()

	switch os.Getenv(envMode) {
	case "burst":
		n, err := strconv.Atoi(os.Getenv(envCount))
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: bad count: %v\n", err)
			os.Exit(2)
		}
		for i := 0; i < n; i++ {
			if err := s.Put(ctx, []byte(keyFor(i)), []byte(valueFor(i))); err != nil {
				fmt.Fprintf(os.Stderr, "child: Put %d: %v\n", i, err)
				os.Exit(2)
			}
		}
		if err := s.SetAppliedIndex(ctx, storage.AppliedIndex{Index: uint64(n), Term: 1}); err != nil {
			fmt.Fprintf(os.Stderr, "child: SetAppliedIndex: %v\n", err)
			os.Exit(2)
		}
		// Every Put above returned nil. Announcing readiness only now is what
		// makes the parent's assertion meaningful: it is checking that
		// ACKNOWLEDGED writes survived, not that some writes happened to.
		fmt.Println(readyLine)
		_ = os.Stdout.Sync()

	case "stream":
		fmt.Println(readyLine)
		_ = os.Stdout.Sync()
		for i := 0; ; i++ {
			if err := s.Put(ctx, []byte(keyFor(i)), []byte(valueFor(i))); err != nil {
				fmt.Fprintf(os.Stderr, "child: Put %d: %v\n", i, err)
				os.Exit(2)
			}
		}

	default:
		fmt.Fprintf(os.Stderr, "child: unknown mode %q\n", os.Getenv(envMode))
		os.Exit(2)
	}

	select {} // wait to be killed
}

// child launches and returns a running child process, already past READY.
type child struct {
	cmd *exec.Cmd
	t   *testing.T
}

func startChild(t *testing.T, dir, mode string, sync wal.SyncMode, count int) *child {
	t.Helper()

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		envDir+"="+dir,
		envMode+"="+mode,
		envSync+"="+sync.String(),
		envCount+"="+strconv.Itoa(count),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("starting child: %v", err)
	}
	c := &child{cmd: cmd, t: t}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	ready := make(chan error, 1)
	go func() {
		r := bufio.NewReader(stdout)
		line, err := r.ReadString('\n')
		if err != nil {
			ready <- fmt.Errorf("reading child output: %w", err)
			return
		}
		if strings.TrimSpace(line) != readyLine {
			ready <- fmt.Errorf("child said %q, want %q", strings.TrimSpace(line), readyLine)
			return
		}
		ready <- nil
		_, _ = io.Copy(io.Discard, r) // keep the pipe drained
	}()

	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("child never became ready: %v", err)
		}
	case <-time.After(childTimout):
		t.Fatal("timed out waiting for the child to become ready")
	}
	return c
}

// kill destroys the child with SIGKILL and verifies it really died that way.
//
// The verification matters: if the child had exited on its own, the test would
// be measuring a graceful shutdown while claiming to measure a crash.
func (c *child) kill() {
	c.t.Helper()

	if err := c.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		c.t.Fatalf("SIGKILL: %v", err)
	}
	err := c.cmd.Wait()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		c.t.Fatalf("child Wait returned %v; it was not killed", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		c.t.Fatalf("cannot inspect the child's wait status")
	}
	if !status.Signaled() {
		c.t.Fatalf("child exited with status %d rather than being signalled; "+
			"this test only means something if the process was destroyed", status.ExitStatus())
	}
	if status.Signal() != syscall.SIGKILL {
		c.t.Fatalf("child died from %v, want SIGKILL", status.Signal())
	}
	c.cmd.Process = nil
}

func reopen(t *testing.T, dir string, mode wal.SyncMode) *storage.WALStore {
	t.Helper()
	opts := storage.DefaultOptions()
	opts.WAL.SyncMode = mode
	s, err := storage.OpenWALStore(dir, opts)
	if err != nil {
		t.Fatalf("reopening %s after the crash: %v", dir, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// ============================================================ the tests

// TestCrashRecoveryAcknowledgedWritesSurvive is the phase's central claim.
//
// A separate process performs N writes, each of which returns nil. The process
// is then destroyed with SIGKILL — no flush, no Close, no deferred functions.
// Reopening the same directory must produce every one of those writes.
//
// What this proves: an acknowledged write survives the writing PROCESS dying,
// because Append completes a write(2) before it returns, so the bytes are in
// the kernel's page cache and the kernel outlives the process.
//
// What this does not prove: survival of an OS crash or power loss. Nothing here
// tests that, and nothing in the docs claims it for batch mode.
func TestCrashRecoveryAcknowledgedWritesSurvive(t *testing.T) {
	for _, mode := range []wal.SyncMode{wal.SyncBatch, wal.SyncAlways} {
		t.Run(mode.String(), func(t *testing.T) {
			const n = 500
			dir := t.TempDir()

			c := startChild(t, dir, "burst", mode, n)
			c.kill()

			s := reopen(t, dir, mode)

			if got := s.Len(); got != n {
				t.Fatalf("recovered %d keys, want %d", got, n)
			}
			for i := 0; i < n; i++ {
				got, err := s.Get(context.Background(), []byte(keyFor(i)))
				if err != nil {
					t.Fatalf("Get(%s) after crash: %v", keyFor(i), err)
				}
				if string(got) != valueFor(i) {
					t.Fatalf("Get(%s) = %q, want %q", keyFor(i), got, valueFor(i))
				}
			}
			if want := (storage.AppliedIndex{Index: n, Term: 1}); s.AppliedIndex() != want {
				t.Errorf("AppliedIndex after crash = %+v, want %+v", s.AppliedIndex(), want)
			}
		})
	}
}

// TestCrashRecoveryMidWriteStormYieldsAPrefix kills a process in the middle of
// a continuous write storm, so the kill lands at an arbitrary point — often
// partway through appending a record.
//
// The property under test is the one that is actually achievable, and it is
// stronger and more honest than "nothing is lost": the recovered state is a
// PREFIX of the submitted sequence. Some suffix of in-flight writes may be
// missing, because a write interrupted before it was acknowledged genuinely may
// or may not have happened. But there must be no hole — key 400 present while
// key 399 is missing would mean replay skipped a record, which is the failure
// the corruption policy exists to prevent.
func TestCrashRecoveryMidWriteStormYieldsAPrefix(t *testing.T) {
	for _, mode := range []wal.SyncMode{wal.SyncBatch, wal.SyncAlways} {
		t.Run(mode.String(), func(t *testing.T) {
			for attempt := 0; attempt < 3; attempt++ {
				dir := t.TempDir()

				c := startChild(t, dir, "stream", mode, 0)
				// Let the storm run for a varying interval so the kill lands
				// at a different point each time, including mid-record.
				time.Sleep(time.Duration(15+attempt*20) * time.Millisecond)
				c.kill()

				s := reopen(t, dir, mode)

				// Find the prefix length, then prove there is no hole after it.
				n := 0
				for {
					if _, err := s.Get(context.Background(), []byte(keyFor(n))); err != nil {
						if errors.Is(err, storage.ErrNotFound) {
							break
						}
						t.Fatalf("Get(%s): %v", keyFor(n), err)
					}
					n++
				}
				if n == 0 {
					t.Fatalf("attempt %d: nothing survived the crash at all", attempt)
				}
				if got := s.Len(); got != n {
					t.Fatalf("attempt %d: the store holds %d keys but the contiguous prefix is %d; "+
						"replay skipped a record and left a hole", attempt, got, n)
				}
				for i := 0; i < n; i++ {
					got, err := s.Get(context.Background(), []byte(keyFor(i)))
					if err != nil {
						t.Fatalf("attempt %d: Get(%s): %v", attempt, keyFor(i), err)
					}
					if string(got) != valueFor(i) {
						t.Fatalf("attempt %d: Get(%s) = %q, want %q", attempt, keyFor(i), got, valueFor(i))
					}
				}
				t.Logf("attempt %d (%v): %d writes survived, contiguous, no holes", attempt, mode, n)
			}
		})
	}
}

// TestRepeatedCrashes: crash, recover, write more, crash again. Recovery must
// stay correct across generations, including when an earlier crash left a torn
// tail that was repaired.
func TestRepeatedCrashes(t *testing.T) {
	dir := t.TempDir()
	const perRound = 100
	const rounds = 4

	for round := 0; round < rounds; round++ {
		c := startChild(t, dir, "stream", wal.SyncBatch, 0)
		time.Sleep(25 * time.Millisecond)
		c.kill()

		s := reopen(t, dir, wal.SyncBatch)
		n := s.Len()
		if n == 0 {
			t.Fatalf("round %d: nothing survived", round)
		}
		// Whatever survived must still be a contiguous prefix.
		for i := 0; i < n; i++ {
			if _, err := s.Get(context.Background(), []byte(keyFor(i))); err != nil {
				t.Fatalf("round %d: key %d missing from a %d-key prefix: %v", round, i, n, err)
			}
		}
		t.Logf("round %d: %d keys, truncated=%v", round, n, s.Recovery().Truncated)
		if err := s.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
	}
	_ = perRound
}

// TestCrashWithSyncOffStillRecoversFromThePageCache documents an easily
// misread result. Even with fsync disabled entirely, a SIGKILL loses nothing,
// because the data was already handed to the kernel and the kernel survives.
//
// This is exactly why "we call fsync" and "we survive process death" are
// different claims, and why the crash tests here cannot be cited as evidence of
// power-loss durability for any mode.
func TestCrashWithSyncOffStillRecoversFromThePageCache(t *testing.T) {
	const n = 200
	dir := t.TempDir()

	c := startChild(t, dir, "burst", wal.SyncOff, n)
	c.kill()

	s := reopen(t, dir, wal.SyncOff)
	if got := s.Len(); got != n {
		t.Fatalf("recovered %d keys, want %d", got, n)
	}
	t.Logf("sync=off survived SIGKILL with all %d writes: process death is not power loss", n)
}

// TestCrashLeavesRecoverableLogOnDisk inspects the directory directly, so the
// test does not rely solely on the store's own view of itself.
func TestCrashLeavesRecoverableLogOnDisk(t *testing.T) {
	const n = 50
	dir := t.TempDir()

	c := startChild(t, dir, "burst", wal.SyncBatch, n)
	c.kill()

	entries, err := os.ReadDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("reading the WAL directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("the WAL directory is empty after a crash")
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("  %s: %d bytes", e.Name(), info.Size())
	}

	// Replay the log directly, independently of WALStore, and count.
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
	t.Logf("recovery: %+v", rec)
}
