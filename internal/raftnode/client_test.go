package raftnode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/transport"
)

// Phase 12 completion semantics on the real driver, over real TCP.

func writeWithin(n *Node, data []byte, d time.Duration) (uint64, uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return n.Write(ctx, data)
}

func readWithin(n *Node, d time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return n.ReadIndex(ctx)
}

// TestWriteCompletesOnlyAfterApply: when Write returns, the leader's state machine
// has applied the entry and the node's applied index covers it.
func TestWriteCompletesOnlyAfterApply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := startCluster(t, ctx, 3)
	defer h.stop()
	l := h.waitLeader(5 * time.Second)
	for i := 0; i < 5; i++ {
		idx, term, err := writeWithin(h.nodes[l], []byte{byte('a' + i)}, 5*time.Second)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		st := h.nodes[l].Status()
		if st.Applied < idx || st.Commit < idx {
			t.Fatalf("Write returned at index %d but the node has applied %d / committed %d", idx, st.Applied, st.Commit)
		}
		if term != st.Term {
			t.Fatalf("write term %d, node term %d", term, st.Term)
		}
		got := h.sms[l].snapshot()
		if len(got) < int(idx) || got[idx-1] != string([]byte{byte('a' + i)}) {
			t.Fatalf("state machine had not applied the write when Write returned: %q at %d", got, idx)
		}
	}
	// A follower refuses, definitely, and nothing was appended anywhere.
	for _, id := range h.ids {
		if id == l {
			continue
		}
		if _, _, err := writeWithin(h.nodes[id], []byte("x"), time.Second); !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("follower Write = %v, want ErrNotLeader", err)
		}
	}
}

// TestReadIndexOnLeaderWaitsForApply: ReadIndex returns an index at least the
// commit at registration, and the node has applied through it when it returns;
// a follower is refused.
func TestReadIndexOnLeaderWaitsForApply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := startCluster(t, ctx, 3)
	defer h.stop()
	l := h.waitLeader(5 * time.Second)
	idx, _, err := writeWithin(h.nodes[l], []byte("w"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ri, err := readWithin(h.nodes[l], 5*time.Second)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if ri < idx {
		t.Fatalf("read index %d below the completed write at %d", ri, idx)
	}
	if st := h.nodes[l].Status(); st.Applied < ri {
		t.Fatalf("ReadIndex returned %d but applied is %d", ri, st.Applied)
	}
	for _, id := range h.ids {
		if id != l {
			if _, err := readWithin(h.nodes[id], time.Second); !errors.Is(err, raft.ErrNotLeader) {
				t.Fatalf("follower ReadIndex = %v, want ErrNotLeader", err)
			}
		}
	}
}

// TestIsolatedLeaderCannotServeAReadOrCompleteAWrite: the stale-leader case. The
// leader is cut off from both followers at the network; it still believes it
// leads. A ReadIndex must not return (no quorum can confirm it) and a Write must
// not complete (nothing commits) — both end only with the client's deadline,
// i.e. UNKNOWN, never a stale success.
func TestIsolatedLeaderCannotServeAReadOrCompleteAWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	net := fault.NewNetwork()
	h := startClusterWith(t, ctx, 3, func(tr transport.Transport) transport.Transport { return net.Wrap(tr) })
	defer h.stop()
	l := h.waitLeader(5 * time.Second)
	if _, _, err := writeWithin(h.nodes[l], []byte("before"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	var members []string
	for _, id := range h.ids {
		members = append(members, string(id))
	}
	net.Isolate(string(l), members)
	// Give the isolation a moment to take effect for in-flight acks, then ask.
	time.Sleep(100 * time.Millisecond)
	if h.nodes[l].Status().Role != raft.Leader {
		t.Skip("the isolated node already stepped down (timing); nothing to prove here")
	}
	if ri, err := readWithin(h.nodes[l], 600*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("an isolated leader served a read at %d (err=%v): a stale read", ri, err)
	}
	_, _, err := writeWithin(h.nodes[l], []byte("during"), 600*time.Millisecond)
	if err == nil {
		t.Fatal("an isolated leader completed a write with no quorum")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, raft.ErrNotLeader) && !errors.Is(err, ErrLost) {
		t.Fatalf("isolated write: %v", err)
	}
	net.HealAll()
	// After healing the group converges; a write there completes again.
	deadline := time.Now().Add(10 * time.Second)
	for {
		l2 := h.waitLeader(5 * time.Second)
		if _, _, err := writeWithin(h.nodes[l2], []byte("after"), 2*time.Second); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no write completed after the partition healed")
		}
	}
}

// TestWriteReportsLostWhenItsEntryIsOverwritten: a leader accepts a write while
// cut off from its followers; the others elect a new leader and commit; when the
// old leader rejoins, its entry is replaced and the client learns ErrLost — a
// definite no-effect, never a silent success.
func TestWriteReportsLostWhenItsEntryIsOverwritten(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	net := fault.NewNetwork()
	h := startClusterWith(t, ctx, 3, func(tr transport.Transport) transport.Transport { return net.Wrap(tr) })
	defer h.stop()
	l := h.waitLeader(5 * time.Second)
	if _, _, err := writeWithin(h.nodes[l], []byte("base"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	var members []string
	for _, id := range h.ids {
		members = append(members, string(id))
	}
	net.Isolate(string(l), members)
	time.Sleep(50 * time.Millisecond)
	if h.nodes[l].Status().Role != raft.Leader {
		t.Skip("the leader stepped down before the isolated write (timing)")
	}
	// The isolated leader accepts a write it can never commit; the client waits
	// on it with a long deadline while the rest of the group moves on.
	type res struct {
		idx, term uint64
		err       error
	}
	done := make(chan res, 1)
	go func() {
		idx, term, err := writeWithin(h.nodes[l], []byte("orphan"), 20*time.Second)
		done <- res{idx, term, err}
	}()
	others := without(h.ids, l)
	var l2 NodeID
	deadline := time.Now().Add(10 * time.Second)
	for l2 == "" {
		for _, id := range others {
			if st := h.nodes[id].Status(); st.Role == raft.Leader && st.Term > h.nodes[l].Status().Term-1 {
				l2 = id
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the majority elected no new leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, _, err := writeWithin(h.nodes[l2], []byte("winner"), 5*time.Second); err != nil {
		t.Fatalf("write on the new leader: %v", err)
	}
	net.HealAll()
	r := <-done
	if !errors.Is(r.err, ErrLost) {
		t.Fatalf("the orphaned write ended with %v (index %d term %d), want ErrLost", r.err, r.idx, r.term)
	}
	// And the lost value is on no state machine.
	h.waitAllCommit(h.nodes[l2].Status().Commit, 5*time.Second)
	for _, id := range h.ids {
		for _, cmd := range h.sms[id].snapshot() {
			if cmd == "orphan" {
				t.Fatalf("%s applied the write that was reported lost", id)
			}
		}
	}
}
