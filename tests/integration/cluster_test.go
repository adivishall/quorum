package integration

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// This is the Phase 7 exit criterion: three real, independent OS processes
// running cmd/dkvd, establishing TCP connections, handshaking, exchanging
// Probe/ProbeResponse in both directions, and shutting down cleanly on SIGTERM.
// It builds the actual dkvd binary and inspects real process exit status and the
// processes' event streams; nothing here passes on a timeout expiring.

// safeBuf is a concurrency-safe buffer for a child's live stdout+stderr.
type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// freeTCPAddr returns a currently-free 127.0.0.1 address. There is a small race
// between closing the probe listener and the child re-binding it, but on
// localhost in a test that window is negligible; a bind failure would surface as
// the child failing to start, which the test detects.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

type dkvNode struct {
	id   string
	addr string
	cmd  *exec.Cmd
	out  *safeBuf
}

func TestThreeNodeClusterProbesAndShutsDownCleanly(t *testing.T) {
	// Build the real cmd/dkvd binary.
	bin := filepath.Join(t.TempDir(), "dkvd")
	build := exec.Command("go", "build", "-o", bin, "github.com/adivishall/quorum/cmd/dkvd")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build dkvd: %v\n%s", err, out)
	}

	ids := []string{"node-1", "node-2", "node-3"}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = freeTCPAddr(t)
	}

	// Launch each node with the other two as peers.
	nodes := make([]*dkvNode, 0, len(ids))
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other+"="+addrs[other])
			}
		}
		buf := &safeBuf{}
		cmd := exec.Command(bin,
			"-id", id,
			"-listen", addrs[id],
			"-peers", strings.Join(peers, ","),
			"-probe-interval", "100ms",
		)
		cmd.Stdout = buf
		cmd.Stderr = buf
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		nodes = append(nodes, &dkvNode{id: id, addr: addrs[id], cmd: cmd, out: buf})
	}

	// Ensure every child is reaped even if the test fails midway.
	defer func() {
		for _, n := range nodes {
			_ = n.cmd.Process.Kill()
			_ = n.cmd.Wait()
		}
	}()

	// Every node must report ready on its real address...
	for _, n := range nodes {
		waitForLine(t, n, "event=ready", 10*time.Second)
		if !strings.Contains(n.out.String(), "addr="+n.addr) {
			t.Fatalf("%s did not report its real listen addr %s; output:\n%s", n.id, n.addr, n.out.String())
		}
	}

	// ...and every node must round-trip Probes with BOTH of its peers. Each node
	// having acked all peers means all six directed probe exchanges crossed real
	// TCP (a Probe out, a ProbeResponse back), and confirms handshakes succeeded.
	for _, n := range nodes {
		waitForLine(t, n, "event=all_peers_acked", 20*time.Second)
	}

	// Cross-check the concrete evidence: each node connected to and acked the
	// other two by name.
	for _, n := range nodes {
		out := n.out.String()
		for _, other := range ids {
			if other == n.id {
				continue
			}
			if !strings.Contains(out, "event=peer_connected peer="+other) {
				t.Errorf("%s never logged a connection to %s; output:\n%s", n.id, other, out)
			}
			if !strings.Contains(out, "event=probe_ack node="+n.id+" peer="+other) {
				t.Errorf("%s never acked a probe from %s; output:\n%s", n.id, other, out)
			}
		}
	}

	// Clean shutdown: SIGTERM each node and require exit status 0.
	for _, n := range nodes {
		if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("signal %s: %v", n.id, err)
		}
	}
	for _, n := range nodes {
		if err := waitExit(n, 10*time.Second); err != nil {
			t.Fatalf("%s did not exit cleanly: %v\noutput:\n%s", n.id, err, n.out.String())
		}
		if !strings.Contains(n.out.String(), "event=shutdown_done") {
			t.Errorf("%s did not log shutdown_done; output:\n%s", n.id, n.out.String())
		}
	}
}

func waitForLine(t *testing.T, n *dkvNode, substr string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if strings.Contains(n.out.String(), substr) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s did not emit %q within %s; output:\n%s", n.id, substr, d, n.out.String())
}

// waitExit waits for the process to exit and returns an error unless it exited
// with status 0 within d.
func waitExit(n *dkvNode, d time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case err := <-done:
		return err // nil means exit code 0
	case <-time.After(d):
		return fmt.Errorf("process did not exit within %s", d)
	}
}
