package kv_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/kv/workload"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/transport"
)

// The Phase 12 real-driver tier: real raftnode actors over real TCP transports
// (optionally wrapped in fault.Network), a kv.Store as each node's state machine,
// a kv.Server per node, and concurrent workload clients recording a
// lincheck.History that is then checked. Timing is real, so these are not
// seed-replayable; what they assert must hold under any timing: every recorded
// history is linearizable.

func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// endpoint routes to a node's current Server, and reports the node unavailable
// while it is down (between a crash and its restart).
type endpoint struct {
	name string
	mu   sync.RWMutex
	srv  *kv.Server
}

func (e *endpoint) Name() string { return e.name }
func (e *endpoint) current() (*kv.Server, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.srv == nil {
		return nil, fmt.Errorf("%w: %s is down", kv.ErrUnavailable, e.name)
	}
	return e.srv, nil
}
func (e *endpoint) Put(ctx context.Context, key, value []byte) (kv.Meta, error) {
	s, err := e.current()
	if err != nil {
		return kv.Meta{Node: e.name}, err
	}
	return s.Put(ctx, key, value)
}
func (e *endpoint) Get(ctx context.Context, key []byte) ([]byte, kv.Meta, error) {
	s, err := e.current()
	if err != nil {
		return nil, kv.Meta{Node: e.name}, err
	}
	return s.Get(ctx, key)
}

// Do is the Phase 13 API (kv.Doer): a node that is down is unreachable before
// anything was sent — a definite ErrUnavailable.
func (e *endpoint) Do(ctx context.Context, req kv.Request) (kv.Response, error) {
	s, err := e.current()
	if err != nil {
		return kv.Response{}, err
	}
	return s.Do(ctx, req)
}

func (e *endpoint) Delete(ctx context.Context, key []byte) (kv.Meta, error) {
	s, err := e.current()
	if err != nil {
		return kv.Meta{Node: e.name}, err
	}
	return s.Delete(ctx, key)
}

type cluster struct {
	t     testing.TB
	ctx   context.Context
	dir   string
	ids   []raftnode.NodeID
	addrs map[raftnode.NodeID]string
	net   *fault.Network
	trs   map[raftnode.NodeID]*transport.TCPTransport
	nodes map[raftnode.NodeID]*raftnode.Node
	eps   map[raftnode.NodeID]*endpoint
	mu    sync.Mutex

	// limits are the session-table limits every node's store uses (zero:
	// kv.DefaultLimits); they must be the same on every node.
	limits kv.Limits
	// hook, if set, is consulted at every driver crash point of every node: it
	// lets a test abort one node's cycle at an exact point (Phase 11's seam).
	hook atomic.Pointer[func(id raftnode.NodeID, p raftnode.Point, arg uint64) error]
}

// setHook installs (or with nil removes) the crash-point hook.
func (c *cluster) setHook(h func(id raftnode.NodeID, p raftnode.Point, arg uint64) error) {
	if h == nil {
		c.hook.Store(nil)
		return
	}
	c.hook.Store(&h)
}

func startCluster(t testing.TB, ctx context.Context, n int, faults bool) *cluster {
	t.Helper()
	c := &cluster{t: t, ctx: ctx, dir: t.TempDir(), addrs: map[raftnode.NodeID]string{},
		trs: map[raftnode.NodeID]*transport.TCPTransport{}, nodes: map[raftnode.NodeID]*raftnode.Node{}, eps: map[raftnode.NodeID]*endpoint{}}
	if faults {
		c.net = fault.NewNetwork()
	}
	for i := 0; i < n; i++ {
		id := raftnode.NodeID(fmt.Sprintf("n%d", i+1))
		c.ids = append(c.ids, id)
		c.addrs[id] = freeAddr(t)
		c.eps[id] = &endpoint{name: string(id)}
	}
	for _, id := range c.ids {
		c.startNode(id)
	}
	t.Cleanup(c.stop)
	return c
}

// startNode starts (or restarts) a node on its log path with a FRESH store — a
// restart rebuilds the state machine by replaying the recovered committed prefix.
func (c *cluster) startNode(id raftnode.NodeID) {
	c.t.Helper()
	peers := map[transport.NodeID]string{}
	for _, other := range c.ids {
		if other != id {
			peers[transport.NodeID(other)] = c.addrs[other]
		}
	}
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: transport.NodeID(id), ListenAddr: c.addrs[id], Peers: peers,
		ReadIdleTimeout: 120 * 15 * time.Millisecond})
	if err != nil {
		c.t.Fatalf("transport %s: %v", id, err)
	}
	var wrapped transport.Transport = tr
	if c.net != nil {
		wrapped = c.net.Wrap(tr)
	}
	store := kv.NewStore()
	if c.limits != (kv.Limits{}) {
		store = kv.NewStoreWithLimits(c.limits)
	}
	node, err := raftnode.Start(c.ctx, raftnode.Config{
		ID: id, Peers: c.ids, Transport: wrapped,
		LogPath:      filepath.Join(c.dir, string(id)+".log"),
		StateMachine: store, TickInterval: 15 * time.Millisecond, DisableSync: true,
		Hook: func(p raftnode.Point, arg uint64) error {
			if h := c.hook.Load(); h != nil {
				return (*h)(id, p, arg)
			}
			return nil
		},
	})
	if err != nil {
		c.t.Fatalf("node %s: %v", id, err)
	}
	c.mu.Lock()
	c.trs[id], c.nodes[id] = tr, node
	c.mu.Unlock()
	c.eps[id].mu.Lock()
	c.eps[id].srv = kv.NewServer(string(id), node, store)
	c.eps[id].mu.Unlock()
}

// crash stops a node abruptly from the clients' point of view (its actor exits;
// the transport closes) — the in-process stand-in for a kill. Its durable log
// stays; restart brings it back on that log.
func (c *cluster) crash(id raftnode.NodeID) {
	c.eps[id].mu.Lock()
	c.eps[id].srv = nil
	c.eps[id].mu.Unlock()
	c.mu.Lock()
	n, tr := c.nodes[id], c.trs[id]
	delete(c.nodes, id)
	delete(c.trs, id)
	c.mu.Unlock()
	_ = n.Close()
	_ = tr.Close()
}

func (c *cluster) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		_ = n.Close()
	}
	for _, tr := range c.trs {
		_ = tr.Close()
	}
}

func (c *cluster) endpoints() []workload.Endpoint {
	var out []workload.Endpoint
	for _, id := range c.ids {
		out = append(out, c.eps[id])
	}
	return out
}

func (c *cluster) node(id raftnode.NodeID) *raftnode.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// waitLeader waits for a node that reports leading in a term above minTerm.
func (c *cluster) waitLeader(minTerm uint64, d time.Duration) raftnode.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			if n := c.node(id); n != nil {
				if st := n.Status(); st.Role == raft.Leader && st.Term > minTerm {
					return id
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader above term %d within %s", minTerm, d)
	return ""
}

func (c *cluster) members() []string {
	var out []string
	for _, id := range c.ids {
		out = append(out, string(id))
	}
	return out
}

// check runs the checker on the recorded history and fails the test with the
// minimized counterexample on a violation. It also requires the history to be
// non-vacuous: some operations completed successfully.
func check(t *testing.T, rec *lincheck.Recorder, st workload.Stats) lincheck.Result {
	t.Helper()
	h := rec.History()
	if err := h.Validate(); err != nil {
		t.Fatalf("recorded history is malformed: %v", err)
	}
	r := lincheck.Check(h, lincheck.Options{Minimize: true})
	if r.Unchecked {
		t.Fatalf("history exceeded the checker's budget: %s", r.Reason)
	}
	if !r.OK {
		t.Fatalf("NOT LINEARIZABLE (%s)\n%s\n--- minimized counterexample ---\n%s\n--- full history (%d ops) ---\n%s",
			st, r.Reason, lincheck.Format(r.Counterexample), len(h.Ops), h)
	}
	if st.OK == 0 {
		t.Fatalf("no operation succeeded; the history proves nothing: %s", st)
	}
	t.Logf("linearizable: %s; checker: %d states, %d keys, %s", st, r.Stats.States, r.Stats.Keys, r.Stats.Duration)
	return r
}
