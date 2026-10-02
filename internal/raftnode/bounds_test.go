package raftnode

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/transport"
)

// A node's failure modes and its bounds (audit M3, M4): a state machine that
// refuses a committed entry fail-stops the node; requests whose clients gave
// up leave nothing behind; and a leader cut off from its quorum refuses work
// beyond its bounds instead of accumulating it.

var errPoison = errors.New("poison: this state machine refuses the command")

// poisonSM applies every command except "poison", which it refuses with no
// effect — a command every replica refuses alike, as a bug or a version skew
// in a state machine would.
type poisonSM struct{ recSM }

func (s *poisonSM) Apply(index uint64, command []byte) error {
	if string(command) == "poison" {
		return errPoison
	}
	return s.recSM.Apply(index, command)
}

type logBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuf) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.b, f+"\n", a...)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// startSolo starts a one-node group on logPath.
func startSolo(t *testing.T, logPath string, sm StateMachine, lg *logBuf) *Node {
	t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "solo", ListenAddr: freeAddr(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	n, err := Start(context.Background(), Config{
		ID: "solo", Peers: []NodeID{"solo"}, Transport: tr, LogPath: logPath,
		StateMachine: sm, TickInterval: 5 * time.Millisecond, DisableSync: true, Logf: lg.logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// waitSoloLeads waits until a one-node group has elected itself, or stopped.
func waitSoloLeads(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for n.Status().Role != raft.Leader {
		select {
		case <-n.Done():
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the one-node group did not elect itself")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestApplyFailureStopsTheNode (audit M4): a committed entry the state machine
// refuses stops the node — Err is ErrApply, Done is closed, the event names it,
// the write that proposed it learns ErrApply — and the applied index never
// passes it: a restart refuses it again. Before, the failure was logged and
// retried every cycle forever; the group stalled behind it, and the write's
// client waited out its deadline.
func TestApplyFailureStopsTheNode(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "solo.log")
	sm, lg := &poisonSM{}, &logBuf{}
	n := startSolo(t, logPath, sm, lg)
	waitSoloLeads(t, n)
	if _, _, err := writeWithin(n, []byte("before"), 5*time.Second); err != nil {
		t.Fatalf("a write before the poison: %v", err)
	}
	if _, _, err := writeWithin(n, []byte("poison"), 5*time.Second); !errors.Is(err, ErrApply) {
		t.Fatalf("the poisoned write: %v, want ErrApply", err)
	}
	select {
	case <-n.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the node kept running past a refused entry")
	}
	if !errors.Is(n.Err(), ErrApply) || !errors.Is(n.Err(), errPoison) {
		t.Fatalf("Err() = %v, want ErrApply wrapping the state machine's error", n.Err())
	}
	if !strings.Contains(lg.String(), "event=raft_apply_failed") {
		t.Fatalf("no event=raft_apply_failed:\n%s", lg.String())
	}
	if got := sm.snapshot(); len(got) != 2 || got[1] != "before" {
		t.Fatalf("applied %q, want the no-op and \"before\" only", got)
	}
	_ = n.Close()

	// Restarted, it reaches the same entry and stops again: nothing applied past it.
	sm2 := &poisonSM{}
	n2 := startSolo(t, logPath, sm2, &logBuf{})
	select {
	case <-n2.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the restarted node applied past the refused entry")
	}
	if !errors.Is(n2.Err(), ErrApply) {
		t.Fatalf("restarted: Err() = %v, want ErrApply", n2.Err())
	}
	for _, c := range sm2.snapshot() {
		if c == "poison" {
			t.Fatal("the refused entry was applied")
		}
	}
}

// isolatedLeader starts a three-node group, elects a leader, commits a write,
// and cuts the leader off from both followers.
func isolatedLeader(t *testing.T) (*harness, *Node, *fault.Network) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	net := fault.NewNetwork()
	h := startClusterWith(t, ctx, 3, func(tr transport.Transport) transport.Transport { return net.Wrap(tr) })
	t.Cleanup(h.stop)
	l0 := h.waitLeader(5 * time.Second)
	if _, _, err := writeWithin(h.nodes[l0], []byte("before"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	l := isolateTheLeader(t, h, net)
	return h, h.nodes[l], net
}

// waitStatus polls n's Status until cond holds.
func waitStatus(t *testing.T, n *Node, d time.Duration, what string, cond func(Status) bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond(n.Status()) {
		if time.Now().After(deadline) {
			st := n.Status()
			t.Fatalf("%s: never held (pending writes %d, reads %d)", what, st.PendingWrites, st.PendingReads)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAbandonedRequestsLeaveNothingBehind (audit M3): clients of an isolated
// leader give up — their deadlines pass — and the leader forgets their waiters
// and unconfirmed reads at once. Before, each stayed until its index was
// applied or its read confirmed: on a leader cut off from its quorum, never.
func TestAbandonedRequestsLeaveNothingBehind(t *testing.T) {
	_, l, _ := isolatedLeader(t)
	var wg sync.WaitGroup
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				_, _, _ = writeWithin(l, []byte(fmt.Sprintf("w%d-%d", c, i)), 3*time.Millisecond)
				_, _ = readWithin(l, 3*time.Millisecond)
			}
		}(c)
	}
	wg.Wait()
	if l.Status().Role != raft.Leader {
		t.Fatal("the isolated leader stepped down: nothing reaches it, and it has no CheckQuorum")
	}
	waitStatus(t, l, 2*time.Second, "every abandoned request forgotten", func(st Status) bool {
		return st.PendingWrites == 0 && st.PendingReads == 0
	})
}

// TestIsolatedLeaderRefusesWorkBeyondItsBounds (audit M3): an isolated leader
// accepts writes and reads up to its bounds, then refuses them with
// raft.ErrBusy — definite, immediate — while nothing it holds exceeds them.
// Healed, the group serves writes again.
func TestIsolatedLeaderRefusesWorkBeyondItsBounds(t *testing.T) {
	h, l, net := isolatedLeader(t)
	var busyW, busyR atomic.Int64
	var maxW, maxR atomic.Int64
	var wg sync.WaitGroup
	for c := 0; c < 16; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// Clients that keep waiting: nothing is forgotten, so only the
				// bounds hold the leader's work down.
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				go func() {
					defer cancel()
					if _, _, _, err := l.Write(ctx, []byte(fmt.Sprintf("w%d-%d", c, i))); errors.Is(err, raft.ErrBusy) {
						busyW.Add(1)
					}
				}()
				ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
				go func() {
					defer cancel2()
					if _, err := l.ReadIndex(ctx2); errors.Is(err, raft.ErrBusy) {
						busyR.Add(1)
					}
				}()
				st := l.Status()
				maxW.Store(max(maxW.Load(), int64(st.PendingWrites)))
				maxR.Store(max(maxR.Load(), int64(st.PendingReads)))
			}
		}(c)
	}
	wg.Wait()
	if l.Status().Role != raft.Leader {
		t.Fatal("the isolated leader stepped down: nothing reaches it, and it has no CheckQuorum")
	}
	waitStatus(t, l, 20*time.Second, "1600 writes and reads answered or held", func(st Status) bool {
		return busyW.Load()+int64(st.PendingWrites) >= 1600 && busyR.Load()+int64(st.PendingReads) >= 1600
	})
	st := l.Status()
	if st.PendingWrites > raft.DefaultMaxUncommittedEntries || st.PendingReads > raft.DefaultMaxPendingReads ||
		maxW.Load() > raft.DefaultMaxUncommittedEntries || maxR.Load() > raft.DefaultMaxPendingReads {
		t.Fatalf("the isolated leader holds %d writes and %d reads (peaks %d, %d), beyond its bounds %d and %d",
			st.PendingWrites, st.PendingReads, maxW.Load(), maxR.Load(), raft.DefaultMaxUncommittedEntries, raft.DefaultMaxPendingReads)
	}
	if busyW.Load() == 0 || busyR.Load() == 0 {
		t.Fatalf("no request was refused (%d writes, %d reads): the bounds were never reached", busyW.Load(), busyR.Load())
	}
	net.HealAll()
	deadline := time.Now().Add(15 * time.Second)
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

// TestForgetReleasesWhatTheActorHolds: each kind of abandon notice releases
// exactly what the actor holds for its request — a write's waiter, an
// unconfirmed read — including for a client that gave up before it read the
// acceptance the actor had already sent; a notice for a request already
// completed, or refused, changes nothing.
func TestForgetReleasesWhatTheActorHolds(t *testing.T) {
	n := &Node{waiters: NewWaiters(), reads: NewReads()}
	other := n.waiters.Add(7, 1, 0) // another client's write at the same index, still waiting

	w := n.waiters.Add(7, 1, 0)
	n.forget(abandoned{index: 7, ch: w})
	if n.waiters.Len() != 1 {
		t.Fatalf("an abandoned write: %d waiters left, want 1 (the other client's)", n.waiters.Len())
	}

	acc := make(chan writeAccepted, 1)
	acc <- writeAccepted{index: 8, term: 1, done: n.waiters.Add(8, 1, 0)}
	n.forget(abandoned{write: acc})
	if n.waiters.Len() != 1 {
		t.Fatalf("a write abandoned before its acceptance was read: %d waiters left, want 1", n.waiters.Len())
	}

	n.reads.Add(3, 1)
	n.forget(abandoned{readID: 3})
	racc := make(chan readAccepted, 1)
	racc <- readAccepted{id: 4, done: n.reads.Add(4, 1)}
	n.forget(abandoned{read: racc})
	if n.reads.Len() != 0 {
		t.Fatalf("abandoned reads: %d left, want 0", n.reads.Len())
	}

	refused := make(chan writeAccepted, 1)
	refused <- writeAccepted{err: raft.ErrNotLeader}
	n.forget(abandoned{write: refused})
	n.forget(abandoned{index: 7, ch: w}) // already forgotten
	n.waiters.Applied(7, 1, nil)
	if n.waiters.Len() != 0 {
		t.Fatalf("%d waiters left", n.waiters.Len())
	}
	if out := <-other; out.Err != nil || out.Index != 7 {
		t.Fatalf("the other client's write at the same index: %+v, want success", out)
	}
}
