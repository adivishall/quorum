package raftnode

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/transport"
)

// recSM is a recording state machine: it appends every applied command, so a test
// can assert what was applied and in what order.
type recSM struct {
	mu      sync.Mutex
	applied []string
}

func (s *recSM) Apply(index uint64, command []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, string(command))
	return nil
}
func (s *recSM) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.applied...)
}

// TestDefaultConfigIsDurable pins the durability default: a zero-value Config (no
// DisableSync set) fsyncs the Raft log, and only an explicit DisableSync turns it
// off. Start uses raftlogOptions() verbatim, so this fails if anyone reintroduces
// the old unsafe zero-value behaviour where durability was off by default.
func TestDefaultConfigIsDurable(t *testing.T) {
	if got := (Config{}).raftlogOptions(); !got.Sync {
		t.Fatalf("default Config is not durable: raftlogOptions().Sync = %v, want true", got.Sync)
	}
	if got := (Config{ID: "n0", LogPath: "x"}).raftlogOptions(); !got.Sync {
		t.Fatalf("a populated Config without DisableSync is not durable: Sync = %v, want true", got.Sync)
	}
	if got := (Config{DisableSync: true}).raftlogOptions(); got.Sync {
		t.Fatalf("DisableSync did not turn off fsync: Sync = %v, want false", got.Sync)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := ln.Addr().String()
	_ = ln.Close()
	return a
}

type harness struct {
	t     *testing.T
	dir   string
	ids   []NodeID
	trs   map[NodeID]*transport.TCPTransport
	nodes map[NodeID]*Node
	sms   map[NodeID]*recSM
}

// startCluster builds an n-node cluster over real TCP transports.
func startCluster(t *testing.T, ctx context.Context, n int) *harness {
	t.Helper()
	var ids []NodeID
	addrs := map[NodeID]string{}
	for i := 0; i < n; i++ {
		id := NodeID(fmt.Sprintf("n%d", i))
		ids = append(ids, id)
		addrs[id] = freeAddr(t)
	}
	h := &harness{t: t, dir: t.TempDir(), ids: ids, trs: map[NodeID]*transport.TCPTransport{}, nodes: map[NodeID]*Node{}, sms: map[NodeID]*recSM{}}
	for _, id := range ids {
		peers := map[transport.NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[transport.NodeID(other)] = addrs[other]
			}
		}
		tr, err := transport.NewTCPTransport(transport.Config{
			NodeID: transport.NodeID(id), ListenAddr: addrs[id], Peers: peers,
		})
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		h.trs[id] = tr
		sm := &recSM{}
		h.sms[id] = sm
		node, err := Start(ctx, Config{
			ID: id, Peers: ids, Transport: tr,
			LogPath:      filepath.Join(h.dir, string(id)+".log"),
			StateMachine: sm, TickInterval: 15 * time.Millisecond, DisableSync: true,
		})
		if err != nil {
			t.Fatalf("node %s: %v", id, err)
		}
		h.nodes[id] = node
	}
	return h
}

func (h *harness) stop() {
	for _, n := range h.nodes {
		_ = n.Close()
	}
	for _, tr := range h.trs {
		_ = tr.Close()
	}
}

// waitLeader polls until exactly one node reports Leader, and returns it.
func (h *harness) waitLeader(timeout time.Duration) NodeID {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []NodeID
		for _, id := range h.ids {
			if h.nodes[id].Role() == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("no single leader within %s", timeout)
	return ""
}

// waitCommit polls until every node's commit index reaches at least idx.
func (h *harness) waitAllCommit(idx uint64, timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range h.ids {
			if h.nodes[id].CommitIndex() < idx {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("not all nodes committed through %d within %s", idx, timeout)
}

// TestClusterElectsAndReplicates proves a real 3-node TCP cluster elects one
// leader, and a proposal replicates, commits, and applies on every node in order.
func TestClusterElectsAndReplicates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := startCluster(t, ctx, 3)
	defer h.stop()

	leader := h.waitLeader(5 * time.Second)
	// Propose three commands on the leader.
	for _, cmd := range []string{"x", "y", "z"} {
		if err := h.nodes[leader].Propose(ctx, []byte(cmd)); err != nil {
			t.Fatalf("propose %q on %s: %v", cmd, leader, err)
		}
	}
	// no-op(1) + 3 proposals = commit index 4 on every node.
	h.waitAllCommit(4, 5*time.Second)

	// Wait for the leader's state machine to apply the three commands in order.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.sms[leader].snapshot()) >= 4 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	applied := h.sms[leader].snapshot()
	// index 1 is the no-op (empty), then x, y, z.
	want := []string{"", "x", "y", "z"}
	if len(applied) < 4 {
		t.Fatalf("leader applied %v, want at least %v", applied, want)
	}
	for i, w := range want {
		if applied[i] != w {
			t.Fatalf("applied[%d] = %q, want %q (full: %v)", i, applied[i], w, applied)
		}
	}
}

// TestSingleNodeClusterCommits proves a one-node group elects itself and commits
// proposals over the driver (no peers required).
func TestSingleNodeClusterCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := startCluster(t, ctx, 1)
	defer h.stop()

	leader := h.waitLeader(3 * time.Second)
	if err := h.nodes[leader].Propose(ctx, []byte("solo")); err != nil {
		t.Fatalf("propose: %v", err)
	}
	h.waitAllCommit(2, 3*time.Second) // no-op(1) + proposal(2)
}

// TestGracefulRecovery proves a node restarted from its durable log comes back
// with its recovered term and entries, then re-elects and continues.
func TestGracefulRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := NodeID("n0")
	addr := freeAddr(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "n0.log")

	start := func() (*Node, *transport.TCPTransport) {
		tr, err := transport.NewTCPTransport(transport.Config{NodeID: transport.NodeID(id), ListenAddr: addr})
		if err != nil {
			t.Fatalf("transport: %v", err)
		}
		node, err := Start(ctx, Config{
			ID: id, Peers: []NodeID{id}, Transport: tr,
			LogPath: logPath, TickInterval: 15 * time.Millisecond, // durable by default
		})
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		return node, tr
	}

	n1, tr1 := start()
	// Elect and commit a couple of proposals.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && n1.Role() != raft.Leader {
		time.Sleep(5 * time.Millisecond)
	}
	if n1.Role() != raft.Leader {
		t.Fatal("did not become leader")
	}
	_ = n1.Propose(ctx, []byte("a"))
	_ = n1.Propose(ctx, []byte("b"))
	for time.Now().Before(deadline) && n1.CommitIndex() < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	termBefore := n1.Term()
	commitBefore := n1.CommitIndex()
	if commitBefore < 3 {
		t.Fatalf("setup commit = %d, want >= 3", commitBefore)
	}
	// Graceful shutdown (closes and fsyncs the durable log).
	_ = n1.Close()
	_ = tr1.Close()

	// Restart from the same durable log.
	n2, tr2 := start()
	defer func() { _ = n2.Close(); _ = tr2.Close() }()
	if n2.Term() < termBefore {
		t.Fatalf("recovered term %d < %d (term went backward)", n2.Term(), termBefore)
	}
	// The recovered node re-elects and its commit index does not go backward.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && n2.CommitIndex() < commitBefore {
		time.Sleep(5 * time.Millisecond)
	}
	if n2.CommitIndex() < commitBefore {
		t.Fatalf("recovered commit %d < %d (INV-R8: commit moved backward)", n2.CommitIndex(), commitBefore)
	}
}
