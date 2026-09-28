package raftnode

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/transport"
)

// TestSnapshotOnRequest: Node.Snapshot snapshots at the applied index now and
// compacts behind it; with nothing applied since, it does nothing; a node
// whose state machine cannot snapshot refuses the request and keeps running.
func TestSnapshotOnRequest(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "n0.log")
	sm := &snapSM{}
	n, tr := startSnapSingle(t, ctx, path, sm, 0, 0, nil, nil)
	defer tr.Close()
	defer n.Close()
	waitLeads(t, n)
	for i := 0; i < 5; i++ {
		if _, _, err := writeWithin(n, []byte("c"), 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	applied := n.Status().Applied
	if err := n.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	st := n.Status()
	if st.Snapshot != applied || st.Boundary != applied {
		t.Fatalf("after the request: %+v (applied %d)", st, applied)
	}
	if err := n.Snapshot(ctx); err != nil || n.Status().Snapshot != applied {
		t.Fatalf("a second request with nothing new: %v %+v", err, n.Status())
	}

	plain, tr2 := startPlainSingle(t, ctx, filepath.Join(t.TempDir(), "p.log"))
	defer tr2.Close()
	defer plain.Close()
	waitLeads(t, plain)
	if err := plain.Snapshot(ctx); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("a state machine that cannot snapshot: %v", err)
	}
	if _, _, err := writeWithin(plain, []byte("still-running"), 5*time.Second); err != nil {
		t.Fatalf("the node stopped: %v", err)
	}
}

// startPlainSingle starts a one-node group whose state machine cannot
// snapshot.
func startPlainSingle(t *testing.T, ctx context.Context, logPath string) (*Node, *transport.TCPTransport) {
	t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "n0", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := Start(ctx, Config{ID: "n0", Peers: []NodeID{"n0"}, Transport: tr, LogPath: logPath,
		StateMachine: &recSM{}, TickInterval: 10 * time.Millisecond, DisableSync: true})
	if err != nil {
		tr.Close()
		t.Fatal(err)
	}
	return n, tr
}

// waitLeads waits until a one-node group's node has elected itself.
func waitLeads(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for n.Role() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatalf("never led: %+v", n.Status())
		}
		time.Sleep(2 * time.Millisecond)
	}
}
