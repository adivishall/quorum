package integration

import (
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raftlog"
)

// These tests prove that Raft HardState (currentTerm and votedFor) — not just log
// entries — survives real process death, by SIGKILLing a live dkvd -raft process
// and inspecting the recovered HardState directly from the durable log file. They
// are the process-kill durability checks for INV-R6 (not power loss).

// launchRaftNode starts a single-node dkvd -raft process on its own data dir.
func launchRaftNode(t *testing.T, bin, addr, dir string) *dkvNode {
	t.Helper()
	buf := &safeBuf{}
	cmd := exec.Command(bin, "-id", "n0", "-listen", addr, "-raft", "-data-dir", dir, "-tick-interval", "25ms")
	cmd.Stdout = buf
	cmd.Stderr = buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	return &dkvNode{id: "n0", addr: addr, cmd: cmd, out: buf}
}

// TestHardStateSurvivesSIGKILL proves currentTerm and votedFor are recovered
// exactly after a SIGKILL. A single node votes for itself in term 1 on election;
// after the kill, the durable log's HardState must report term 1 and vote n0.
func TestHardStateSurvivesSIGKILL(t *testing.T) {
	bin := buildDkvd(t)
	addr := freeTCPAddr(t)
	dir := filepath.Join(t.TempDir(), "n0")
	logPath := filepath.Join(dir, "raft-n0.log")

	n := launchRaftNode(t, bin, addr, dir)
	defer func() { _ = n.cmd.Process.Kill(); _ = n.cmd.Wait() }()

	// Wait until it is leader in term 1 — by then it has durably voted for itself
	// and appended (and fsynced) its no-op.
	waitForLine(t, n, "event=raft_leader node=n0 term=1", 15*time.Second)

	if err := n.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = n.cmd.Wait()

	// Inspect the durable log directly (read-only, no truncation).
	rec, err := raftlog.Inspect(logPath)
	if err != nil {
		t.Fatalf("inspect durable log after kill: %v", err)
	}
	if rec.HardState.Term != 1 {
		t.Fatalf("recovered term = %d, want 1 (currentTerm did not survive SIGKILL)", rec.HardState.Term)
	}
	if rec.HardState.Vote != "n0" {
		t.Fatalf("recovered vote = %q, want n0 (votedFor did not survive SIGKILL)", rec.HardState.Vote)
	}
	if len(rec.Entries) < 1 || rec.Entries[0].Term != 1 {
		t.Fatalf("recovered entries = %+v, want the term-1 no-op at index 1", rec.Entries)
	}
}

// TestCurrentTermMonotonicAcrossRestart proves currentTerm is recovered (not reset)
// across a real restart: a single node reaches term 1, is killed, and on restart
// from the same data dir campaigns to term 2 — reachable only if the recovered
// currentTerm was 1. A node that lost its term would elect at term 1 again forever.
func TestCurrentTermMonotonicAcrossRestart(t *testing.T) {
	bin := buildDkvd(t)
	addr := freeTCPAddr(t)
	dir := filepath.Join(t.TempDir(), "n0")
	logPath := filepath.Join(dir, "raft-n0.log")

	n1 := launchRaftNode(t, bin, addr, dir)
	waitForLine(t, n1, "event=raft_leader node=n0 term=1", 15*time.Second)
	if err := n1.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill1: %v", err)
	}
	_ = n1.cmd.Wait()

	rec, err := raftlog.Inspect(logPath)
	if err != nil || rec.HardState.Term != 1 {
		t.Fatalf("after run 1: inspect term = %d err=%v, want 1", rec.HardState.Term, err)
	}

	// Restart from the same durable state.
	n2 := launchRaftNode(t, bin, addr, dir)
	defer func() { _ = n2.cmd.Process.Kill(); _ = n2.cmd.Wait() }()
	// It must climb to term 2 — recovered term 1 plus one new election.
	waitForLine(t, n2, "event=raft_leader node=n0 term=2", 15*time.Second)

	if err := n2.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill2: %v", err)
	}
	_ = n2.cmd.Wait()
	rec, err = raftlog.Inspect(logPath)
	if err != nil || rec.HardState.Term != 2 {
		t.Fatalf("after run 2: recovered term = %d err=%v, want 2 (currentTerm did not advance across restart)", rec.HardState.Term, err)
	}
	if rec.HardState.Vote != "n0" {
		t.Fatalf("after run 2: recovered vote = %q, want n0", rec.HardState.Vote)
	}
}
