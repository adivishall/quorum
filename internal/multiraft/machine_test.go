package multiraft

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/adivishall/quorum/internal/raftnode"
)

// closableSM is a state machine that holds a resource, as the LSM-backed one
// holds its engine (S2): the host must close it once its node has stopped.
type closableSM struct {
	recSM
	closed atomic.Int32
}

func (s *closableSM) Close() error {
	s.closed.Add(1)
	return nil
}

// TestStopAndCloseCloseTheMachine: a group's machine is closed exactly once
// when the group is stopped, and when the host is closed with the group
// still running — after its node, never before it stops applying.
func TestStopAndCloseCloseTheMachine(t *testing.T) {
	c := newHostCluster(t, "n1")
	var made []*closableSM
	h := lifecycleHost(t, c, "n1", func(GroupID) (raftnode.StateMachine, error) {
		sm := &closableSM{}
		made = append(made, sm)
		return sm, nil
	}, nil)
	grp, err := h.Create(3, c.genesis("n1"))
	if err != nil {
		t.Fatal(err)
	}
	waitLeads(t, grp.Node)
	if n := made[0].closed.Load(); n != 0 {
		t.Fatalf("the machine was closed %d times while its group ran", n)
	}
	if err := h.Stop(3); err != nil {
		t.Fatal(err)
	}
	select {
	case <-grp.Node.Done():
	default:
		t.Fatal("the group's node is still running after Stop")
	}
	if n := made[0].closed.Load(); n != 1 {
		t.Fatalf("Stop closed the machine %d times, want once", n)
	}
	grp, err = h.Open(3)
	if err != nil {
		t.Fatal(err)
	}
	waitLeads(t, grp.Node)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if n := made[1].closed.Load(); n != 1 {
		t.Fatalf("Close closed the running group's machine %d times, want once", n)
	}
	if n := made[0].closed.Load(); n != 1 {
		t.Fatalf("Close closed the stopped group's old machine again (%d)", n)
	}
}

// TestAFailedMachineLeavesNoDirectory: a first start whose state machine
// cannot be made — the engine would not open — fails the group and leaves no
// directory behind, as a start that fails in Raft does, so the group is not
// reported as failed at every later start.
func TestAFailedMachineLeavesNoDirectory(t *testing.T) {
	c := newHostCluster(t, "n1")
	h := lifecycleHost(t, c, "n1", func(GroupID) (raftnode.StateMachine, error) {
		return nil, errors.New("the engine would not open")
	}, nil)
	if _, err := h.Create(7, c.genesis("n1")); err == nil || !strings.Contains(err.Error(), "its state machine") {
		t.Fatalf("Create with a machine that cannot be made: %v", err)
	}
	if _, err := os.Stat(GroupDir(c.dirs["n1"], 7)); !os.IsNotExist(err) {
		t.Fatalf("the failed start left its directory: %v", err)
	}
	if _, err := h.Open(7); err == nil {
		t.Fatal("Open of a group that was never started succeeded")
	}
}
