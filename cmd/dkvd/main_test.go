package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

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

// compile-time guard that NodeID conversion stays valid.
var _ transport.NodeID = transport.NodeID("x")
