package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/transport"
)

func TestParsePeers(t *testing.T) {
	got, err := parsePeers("b=127.0.0.1:7002,c=127.0.0.1:7003")
	if err != nil {
		t.Fatal(err)
	}
	if got["b"] != "127.0.0.1:7002" || got["c"] != "127.0.0.1:7003" || len(got) != 2 {
		t.Fatalf("parsed %v", got)
	}
	if empty, err := parsePeers(""); err != nil || len(empty) != 0 {
		t.Fatalf("empty peers: %v %v", empty, err)
	}
}

func TestParsePeersRejectsBadInput(t *testing.T) {
	for _, in := range []string{
		"no-equals-sign",
		"=127.0.0.1:1",            // empty id
		"b=",                      // empty addr
		"b=1.1.1.1:1,b=2.2.2.2:2", // duplicate id
	} {
		if _, err := parsePeers(in); err == nil {
			t.Errorf("parsePeers(%q) = nil error, want error", in)
		}
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	var out, errb bytes.Buffer
	// Missing -id: transport config validation fails, exit code 2.
	if code := run(context.Background(), []string{"-listen", "127.0.0.1:0"}, &out, &errb); code != 2 {
		t.Fatalf("bad config exit code = %d, want 2", code)
	}
}

// TestRunRejectsBadClientAndCrashSeamFlags: the Phase 12 client port and the
// crash seam refuse combinations they cannot honour, before anything starts.
func TestRunRejectsBadClientAndCrashSeamFlags(t *testing.T) {
	base := []string{"-id", "a", "-listen", "127.0.0.1:0"}
	for _, extra := range [][]string{
		{"-client-listen", "127.0.0.1:0"},                                                     // without -raft
		{"-raft", "-crash-armed-by-signal"},                                                   // arming nothing
		{"-raft", "-crash-at", "fsync:2", "-crash-armed-by-signal"},                           // I/O points count from startup
		{"-raft", "-crash-at", "before-reply:1"},                                              // a reply point with no client port
		{"-raft", "-crash-at", "after-reply:0", "-client-listen", "127.0.0.1:0"},              // occurrence must be positive
		{"-raft", "-crash-at", "during-reply:1", "-client-listen", "127.0.0.1:0"},             // unknown point
		{"-raft", "-crash-at", "before-reply:x", "-client-listen", "127.0.0.1:0", "-id", "b"}, // bad occurrence
		{"-raft", "-client-listen", "127.0.0.1:0", "-session-max", "0"},                       // no session could exist
		{"-raft", "-client-listen", "127.0.0.1:0", "-session-max-unacked", "-1"},              // no request could complete
		{"-raft", "-session-max", "x"},                                                        // not a number
	} {
		var out, errb bytes.Buffer
		if code := run(context.Background(), append(append([]string(nil), base...), extra...), &out, &errb); code != 2 {
			t.Fatalf("%v: exit code %d, want 2 (stderr %q)", extra, code, errb.String())
		}
	}
}

// TestRunRejectsNonPositiveProbeInterval proves a zero or negative
// -probe-interval is rejected as configuration (exit 2) with a stderr message,
// and — crucially — never reaches time.NewTicker (a panic would fail the test
// rather than return 2). run() returns synchronously here, so no goroutine or
// ticker is started.
func TestRunRejectsNonPositiveProbeInterval(t *testing.T) {
	for _, ivl := range []string{"0", "0s", "-5s", "-1ns"} {
		var out, errb bytes.Buffer
		code := run(context.Background(),
			[]string{"-id", "a", "-listen", "127.0.0.1:0", "-probe-interval", ivl},
			&out, &errb)
		if code != 2 {
			t.Fatalf("-probe-interval %q: exit code = %d, want 2", ivl, code)
		}
		if !strings.Contains(errb.String(), "probe-interval") {
			t.Fatalf("-probe-interval %q: stderr = %q, want it to mention probe-interval", ivl, errb.String())
		}
		if out.String() != "" {
			t.Fatalf("-probe-interval %q: node emitted output %q before rejecting the config", ivl, out.String())
		}
	}
}

// TestRunAcceptsValidProbeInterval proves an explicit positive interval still
// lets the node start and shut down cleanly with exit 0.
func TestRunAcceptsValidProbeInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-id", "solo", "-listen", "127.0.0.1:0", "-probe-interval", "50ms"}, out, io.Discard)
	}()
	waitFor(t, out, "event=ready", 3*time.Second)
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node did not exit after cancellation")
	}
}

// syncBuffer is a concurrency-safe buffer for reading a node's event stream
// while it runs.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitFor(t *testing.T, buf *syncBuffer, substr string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not see %q within %s; output:\n%s", substr, d, buf.String())
}

// TestRunStartsAndShutsDownCleanly runs a single node (no peers), waits for it to
// report ready, cancels the context (as a signal would), and asserts a clean
// exit code 0 and a shutdown_done line.
func TestRunStartsAndShutsDownCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-id", "solo", "-listen", "127.0.0.1:0"}, out, io.Discard)
	}()

	waitFor(t, out, "event=ready", 3*time.Second)
	waitFor(t, out, "event=all_peers_acked", 3*time.Second) // no peers -> immediate
	cancel()

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node did not exit after cancellation")
	}
	if !strings.Contains(out.String(), "event=shutdown_done") {
		t.Fatalf("no shutdown_done line; output:\n%s", out.String())
	}
}

// TestRaftModeExitsNonZeroWhenTheLogFails drives the whole fail-stop path through
// the real dkvd raft-mode code (INV-F1): the fsync that must make the node's
// self-vote and election no-op durable fails. (fsync #1 is raftlog.Open making the
// recovered log durable; #2 is the election's Save.) The process must report
// event=raft_fatal and exit 1, and must never announce leadership it could not
// persist.
func TestRaftModeExitsNonZeroWhenTheLogFails(t *testing.T) {
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "solo", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	inj := fault.NewInjectFS(nil)
	inj.Arm(fault.Injection{Op: fault.OpSync, Nth: 2})
	out := &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- runRaft(context.Background(), raftRun{
			id: "solo", tr: tr, dataDir: t.TempDir(), tick: 5 * time.Millisecond,
			lg: &logger{w: out}, stderr: io.Discard, fs: inj,
		})
	}()
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("exit code = %d, want 1 after a durable-log failure; output:\n%s", code, out.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("dkvd kept running after its durable log failed; output:\n%s", out.String())
	}
	s := out.String()
	if !strings.Contains(s, "event=raft_fatal node=solo") || !strings.Contains(s, "injected") {
		t.Fatalf("no raft_fatal event naming the injected failure; output:\n%s", s)
	}
	if strings.Contains(s, "event=raft_leader") {
		t.Fatalf("announced leadership it never made durable; output:\n%s", s)
	}
}

// TestRaftModeCleanShutdownExitsZero proves the healthy path is unchanged: a
// single raft-mode node elects itself, commits, and exits 0 on cancellation.
func TestRaftModeCleanShutdownExitsZero(t *testing.T) {
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "solo", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- runRaft(ctx, raftRun{id: "solo", tr: tr, dataDir: t.TempDir(), tick: 5 * time.Millisecond, lg: &logger{w: out}, stderr: io.Discard})
	}()
	waitFor(t, out, "event=raft_commit node=solo index=1", 5*time.Second)
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0; output:\n%s", code, out.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not shut down")
	}
}

// compile-time guard that NodeID conversion stays valid.
var _ transport.NodeID = transport.NodeID("x")
