package kv_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/transport"
	"github.com/adivishall/quorum/internal/vfs"
)

// The LSM machine inside a real Raft group (S2, docs/STORAGE_INTEGRATION.md
// §8.2, apply failure): the engine's batch fails inside a cycle, and the node
// must fail-stop with the write's client told UNKNOWN — never OK for a state
// the engine did not record — while the group goes on without it and the
// node, restarted on its engine, applies the entry from the log once.

// TestLSMEngineFailureFailsStopsTheNode: the leader's engine refuses the
// cycle's WAL write. The write is committed in Raft (a majority persisted it),
// so it is not lost; but the leader cannot record it, so the leader stops and
// the client learns nothing definite. A new leader serves the value; the old
// leader restarted on its engine — the failed batch absent — applies the
// entry at replay and agrees with the group.
func TestLSMEngineFailureFailsStopsTheNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &cluster{t: t, ctx: ctx, dir: t.TempDir(), addrs: map[raftnode.NodeID]string{}, lsm: true,
		trs: map[raftnode.NodeID]*transport.TCPTransport{}, nodes: map[raftnode.NodeID]*raftnode.Node{}, eps: map[raftnode.NodeID]*endpoint{}}
	c.engineFS = map[raftnode.NodeID]vfs.FS{}
	injectors := map[raftnode.NodeID]*fault.InjectFS{}
	for i := 0; i < 3; i++ {
		id := raftnode.NodeID("n" + string(rune('1'+i)))
		c.ids = append(c.ids, id)
		c.addrs[id] = freeAddr(t)
		c.eps[id] = &endpoint{name: string(id)}
		injectors[id] = fault.NewInjectFS(nil)
		c.engineFS[id] = injectors[id]
	}
	for _, id := range c.ids {
		c.startNode(id)
	}
	t.Cleanup(c.stop)
	l, term := c.waitSettled(10 * time.Second)
	srv, _ := c.eps[l].current()
	if _, err := srv.Put(ctx, []byte("k"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	// The next cycle's batch fails on the leader.
	injectors[l].Arm(fault.Injection{Op: fault.OpWrite})
	_, err := srv.Put(ctx, []byte("k"), []byte("2"))
	if err == nil {
		t.Fatal("a write whose engine batch failed was acknowledged")
	}
	if errors.Is(err, kv.ErrNotFound) || !errors.Is(err, kv.ErrUnknown) {
		t.Fatalf("the client of the failed cycle got %v, want an unknown outcome", err)
	}
	// The leader stopped on the failure: it takes no more writes, and the
	// write's own result named the apply failure.
	if _, _, _, err := c.node(l).Write(ctx, kv.Command{Op: kv.OpPut, Key: []byte("x"), Value: []byte("y")}.Encode()); !errors.Is(err, raft.ErrStopped) && !errors.Is(err, raftnode.ErrApply) {
		t.Fatalf("the failed node answered a write with %v, want stopped or apply failed", err)
	}

	// The write is committed: the others elect a leader in a later term and
	// serve it.
	l2 := c.waitLeader(term, 20*time.Second)
	if l2 == l {
		t.Fatalf("%s leads again after failing", l)
	}
	srv2, _ := c.eps[l2].current()
	v, _, err := srv2.Get(ctx, []byte("k"))
	if err != nil || string(v) != "2" {
		t.Fatalf("the committed write at the new leader: %q, %v", v, err)
	}

	// The old leader, restarted on its engine, applies the entry from the log.
	c.crash(l)
	c.engineFS[l] = nil
	c.startNode(l)
	c.waitApplied(l, c.node(l2).Status().Applied)
	m := c.eps[l].srv.Store()
	v, ok, err := m.Lookup([]byte("k"))
	if err != nil || !ok || string(v) != "2" {
		t.Fatalf("after its restart the failed node holds %q, %v, %v; want \"2\"", v, ok, err)
	}
	if lm, ok := m.(*kv.LSMMachine); !ok || lm.EngineApplied().Index != c.node(l).Status().Applied {
		t.Fatalf("the restarted node's engine is at %+v, its core applied %d", lm.EngineApplied(), c.node(l).Status().Applied)
	}
}

// TestR2CompactionStaysWithinADurableCopy: with the LSM machine the Raft log is
// compacted on the snapshot schedule but never past a durable copy of the
// compacted prefix (R2, docs/STORAGE_INTEGRATION.md §9.6). A node that creates
// its own snapshots clamps its compaction boundary to the engine's durable
// (flushed) applied index, so a power loss that reverts the engine to its last
// flush still finds every entry above it in the log; a node that installs a
// leader's snapshot is covered instead by that snapshot's durable file. The test
// drives enough writes that the engine flushes and the log compacts, then checks
// the invariant on every node and that the clamp was actually in force.
func TestR2CompactionStaysWithinADurableCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The default (large) memtable: the engine does not flush on its own over
	// this short run, so the ONLY thing that advances its durable index — and so
	// lets the log compact at all (R2) — is SyncApplied, which snapshot creation
	// calls. If that step were skipped the durable index would stay 0, the log
	// would never compact, and the `compacted` premise below would fail.
	c := &cluster{t: t, ctx: ctx, dir: t.TempDir(), addrs: map[raftnode.NodeID]string{}, lsm: true,
		snapEvery: 8, snapRetain: 2,
		trs: map[raftnode.NodeID]*transport.TCPTransport{}, nodes: map[raftnode.NodeID]*raftnode.Node{}, eps: map[raftnode.NodeID]*endpoint{}}
	for i := 0; i < 3; i++ {
		id := raftnode.NodeID("n" + string(rune('1'+i)))
		c.ids = append(c.ids, id)
		c.addrs[id] = freeAddr(t)
		c.eps[id] = &endpoint{name: string(id)}
	}
	for _, id := range c.ids {
		c.startNode(id)
	}
	t.Cleanup(c.stop)
	l, _ := c.waitSettled(10 * time.Second)

	srv, err := c.eps[l].current()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 150; i++ {
		if _, err := srv.Put(ctx, []byte(fmt.Sprintf("k%03d", i)), []byte("a value with some padding")); err != nil {
			l, _ = c.waitSettled(10 * time.Second) // a leader change; re-resolve and retry
			srv, _ = c.eps[l].current()
			i--
			continue
		}
	}
	applied := c.node(l).Status().Applied
	for _, id := range c.ids {
		c.waitApplied(id, applied)
	}

	flushed, compacted, clampHeld := false, false, false
	for _, id := range c.ids {
		st := c.node(id).Status()
		m, err := c.eps[id].current()
		if err != nil {
			t.Fatal(err)
		}
		lm, ok := m.Store().(*kv.LSMMachine)
		if !ok {
			t.Fatalf("%s is not on the LSM machine", id)
		}
		durable := lm.DurableAppliedIndex()
		// R2: the boundary is covered by a durable copy — the engine's flushed
		// state (a self-compactor's clamp) or the published snapshot file (an
		// installed snapshot). It is never past both.
		if st.Boundary > durable && st.Boundary > st.Snapshot {
			t.Fatalf("%s: the log is compacted through %d, past both the engine's durable applied index %d and the published snapshot %d (R2)",
				id, st.Boundary, durable, st.Snapshot)
		}
		if durable > 0 {
			flushed = true
		}
		if st.Boundary > 0 {
			compacted = true
			if st.Boundary <= durable {
				clampHeld = true // a boundary held at or below the engine watermark
			}
		}
	}
	if !flushed {
		t.Fatal("premise: no node's engine flushed; raise the write count or lower memTableSize")
	}
	if !compacted {
		t.Fatal("premise: no node compacted its log; the snapshot/compaction path was not exercised")
	}
	if !clampHeld {
		t.Fatal("no node's compaction boundary was held at or below its engine's durable applied index; the R2 clamp was not demonstrated")
	}
}

// sameState fails the test unless two machines hold the same contents and
// session table: the replicated state, whichever machine holds it and however
// it was rebuilt.
func sameState(t testing.TB, what string, ref, got kv.Machine) {
	t.Helper()
	a, err := ref.Contents()
	if err != nil {
		t.Fatal(err)
	}
	b, err := got.Contents()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("%s: contents differ\n ref %v\n got %v", what, a, b)
	}
	if rs, gs := ref.Sessions(), got.Sessions(); !reflect.DeepEqual(rs, gs) {
		t.Fatalf("%s: sessions differ\n ref %v\n got %v", what, rs, gs)
	}
}
