package integration

import (
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These tests are the Phase 9 real-process smoke checks (docs/RAFT.md §26): actual
// cmd/dkvd processes running Raft over real TCP, electing a leader, replicating the
// mandatory no-op, and — after a SIGKILL — recovering their durable log. The
// algorithmic correctness proof lives in the deterministic simulation
// (internal/raft); these prove the wiring is real, not mocked.

func buildDkvd(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "dkvd")
	build := exec.Command("go", "build", "-o", bin, "github.com/adivishall/quorum/cmd/dkvd")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build dkvd: %v\n%s", err, out)
	}
	return bin
}

// anyNodeHas polls every node's output for substr until the deadline.
func anyNodeHas(nodes []*dkvNode, substr string, d time.Duration) *dkvNode {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if strings.Contains(n.out.String(), substr) {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// TestThreeNodeRaftElectsLeaderOverTCP launches three real dkvd -raft processes and
// verifies an election happens over TCP: one leader emerges, the no-op commits on a
// quorum (raft_commit), followers acknowledge the leader (raft_follower), and the
// cluster shuts down cleanly.
func TestThreeNodeRaftElectsLeaderOverTCP(t *testing.T) {
	bin := buildDkvd(t)
	ids := []string{"node-1", "node-2", "node-3"}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = freeTCPAddr(t)
	}
	dir := t.TempDir()

	var nodes []*dkvNode
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other+"="+addrs[other])
			}
		}
		buf := &safeBuf{}
		cmd := exec.Command(bin,
			"-id", id, "-listen", addrs[id], "-peers", strings.Join(peers, ","),
			"-raft", "-data-dir", filepath.Join(dir, id), "-tick-interval", "25ms",
		)
		cmd.Stdout = buf
		cmd.Stderr = buf
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		nodes = append(nodes, &dkvNode{id: id, addr: addrs[id], cmd: cmd, out: buf})
	}
	defer func() {
		for _, n := range nodes {
			_ = n.cmd.Process.Kill()
			_ = n.cmd.Wait()
		}
	}()

	// A leader is elected over real TCP.
	if anyNodeHas(nodes, "event=raft_leader", 20*time.Second) == nil {
		t.Fatalf("no leader elected; outputs:\n%s", allOutputs(nodes))
	}
	// The mandatory no-op commits on a quorum (replication happened).
	if anyNodeHas(nodes, "event=raft_commit node=", 20*time.Second) == nil {
		t.Fatalf("no commit observed; outputs:\n%s", allOutputs(nodes))
	}
	// At least one follower recognized the leader (received AppendEntries).
	if anyNodeHas(nodes, "event=raft_follower node=", 20*time.Second) == nil {
		t.Fatalf("no follower acknowledged a leader; outputs:\n%s", allOutputs(nodes))
	}

	// Clean shutdown on SIGTERM, exit 0.
	for _, n := range nodes {
		if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("signal %s: %v", n.id, err)
		}
	}
	for _, n := range nodes {
		if err := waitExit(n, 10*time.Second); err != nil {
			t.Fatalf("%s did not exit cleanly: %v\noutput:\n%s", n.id, err, n.out.String())
		}
	}
}

// TestRaftLogSurvivesSIGKILL proves durability across real process death: a
// single-node Raft process commits its no-op (index 1), is SIGKILLed, and on
// restart from the same data directory recovers the entry — shown by the restarted
// process reaching commit index 2 (a fresh, non-recovered log would only ever reach
// index 1). This is the process-kill durability check (§32), not power loss.
func TestRaftLogSurvivesSIGKILL(t *testing.T) {
	bin := buildDkvd(t)
	addr := freeTCPAddr(t)
	dir := filepath.Join(t.TempDir(), "n0")

	launch := func() *dkvNode {
		buf := &safeBuf{}
		cmd := exec.Command(bin, "-id", "n0", "-listen", addr, "-raft", "-data-dir", dir, "-tick-interval", "25ms")
		cmd.Stdout = buf
		cmd.Stderr = buf
		if err := cmd.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		return &dkvNode{id: "n0", addr: addr, cmd: cmd, out: buf}
	}

	n1 := launch()
	// Wait until it has committed and fsynced the no-op (index 1).
	if anyNodeHas([]*dkvNode{n1}, "event=raft_commit node=n0 index=1", 15*time.Second) == nil {
		t.Fatalf("node did not commit the no-op; output:\n%s", n1.out.String())
	}
	// Kill it hard — no graceful shutdown, no extra fsync.
	if err := n1.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = n1.cmd.Wait()

	// Restart from the same durable log.
	n2 := launch()
	defer func() { _ = n2.cmd.Process.Kill(); _ = n2.cmd.Wait() }()
	// The recovered node re-elects and reaches commit index 2 — index 1 (the
	// recovered entry) plus its new-term no-op at index 2. Reaching 2 is only
	// possible if index 1 survived the kill.
	if anyNodeHas([]*dkvNode{n2}, "event=raft_commit node=n0 index=2", 15*time.Second) == nil {
		t.Fatalf("recovered node did not reach commit index 2; the durable entry did not survive SIGKILL.\noutput:\n%s", n2.out.String())
	}
}

func allOutputs(nodes []*dkvNode) string {
	var b strings.Builder
	for _, n := range nodes {
		b.WriteString("--- " + n.id + " ---\n")
		b.WriteString(n.out.String())
		b.WriteString("\n")
	}
	return b.String()
}
