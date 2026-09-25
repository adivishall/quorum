package raftnode

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/transport"
)

// Phase 11 on the real driver: a Node's actor is stopped at an exact crash point
// (Config.Hook aborts the cycle there and the node fail-stops, as it would for a
// persistence failure) and a new Node is started on the same durable log — the
// process is alive, so the kernel-held bytes are exactly what a SIGKILL would
// leave, and the state machine object outlives the "crash", which is what lets a
// test see re-application. The precise durable-state contract is proven in the
// simulator (internal/raftsim); these prove the real actor, real files and real
// recovery path honour it.

// crashHook returns a Hook that aborts the nth time point p is reached.
func crashHook(p Point, nth int) (Hook, chan struct{}) {
	fired := make(chan struct{})
	n := 0
	return func(q Point, _ uint64) error {
		if q != p {
			return nil
		}
		n++
		if n < nth {
			return nil
		}
		select {
		case <-fired:
		default:
			close(fired)
		}
		return errors.New("crash point")
	}, fired
}

// startSingle starts a single-node group on logPath with sm and hook.
func startSingle(t *testing.T, ctx context.Context, logPath string, sm StateMachine, hook Hook) (*Node, *transport.TCPTransport) {
	t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "n0", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := Start(ctx, Config{
		ID: "n0", Peers: []NodeID{"n0"}, Transport: tr, LogPath: logPath,
		StateMachine: sm, TickInterval: 10 * time.Millisecond, Hook: hook, // durable by default
	})
	if err != nil {
		t.Fatal(err)
	}
	return n, tr
}

func waitLeaderNode(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && n.Role() != raft.Leader {
		time.Sleep(2 * time.Millisecond)
	}
	if n.Role() != raft.Leader {
		t.Fatal("single node did not elect itself")
	}
}

// TestCrashAfterApplyReappliesOnRestart pins the driver's replay semantics
// (docs/CRASH_RECOVERY.md §6): appliedIndex is not persisted, so a restarted node
// re-applies its whole recovered committed prefix. A crash after the state
// machine applied index k but before AppliedTo(k) therefore applies k twice —
// once per incarnation — and so is every committed index below it. Within one
// incarnation each index is applied exactly once, in order. The state machine
// must tolerate this (at-least-once); nothing here deduplicates.
func TestCrashAfterApplyReappliesOnRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logPath := filepath.Join(t.TempDir(), "n0.log")
	sm := &recSM{}

	// Incarnation 1: elect, commit two commands, then die right after applying
	// the third (index 4) but before recording it.
	hook, fired := crashHook(AfterApply, 4) // apply points: no-op (1), a (2), b (3), c (4)
	n1, tr1 := startSingle(t, ctx, logPath, sm, hook)
	waitLeaderNode(t, n1)
	for _, cmd := range []string{"a", "b"} {
		if err := n1.Propose(ctx, []byte(cmd)); err != nil {
			t.Fatal(err)
		}
	}
	_ = n1.Propose(ctx, []byte("c")) // the actor dies while applying this one
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("the crash point after apply never fired")
	}
	<-n1.Done()
	if n1.Err() == nil {
		t.Fatal("a crash point abort must stop the node with an error, like a persistence failure")
	}
	_ = tr1.Close()
	first := sm.snapshot()
	if got := len(first); got != 4 {
		t.Fatalf("incarnation 1 applied %d commands %v, want 4 (through the one it died after)", got, first)
	}

	// The durable log holds every entry and a commit covering them (INV-CR3): the
	// entry the node died applying was committed and durable before it was applied.
	rec, err := raftlog.Inspect(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Entries) != 4 || rec.HardState.Commit < 4 {
		t.Fatalf("durable log after the crash: %d entries, commit %d; want 4 entries and commit >= 4", len(rec.Entries), rec.HardState.Commit)
	}

	// Incarnation 2 on the same log and the SAME state machine.
	n2, tr2 := startSingle(t, ctx, logPath, sm, nil)
	defer func() { _ = n2.Close(); _ = tr2.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && n2.Status().Applied < 4 {
		time.Sleep(2 * time.Millisecond)
	}
	if n2.Status().Applied < 4 {
		t.Fatalf("restarted node applied only through %d", n2.Status().Applied)
	}
	all := sm.snapshot()
	second := all[len(first):]
	// The new incarnation re-applied the entire committed prefix — including the
	// one the old incarnation had already applied — in the same order.
	if len(second) < 4 {
		t.Fatalf("incarnation 2 applied %v, want the whole prefix %v again", second, first)
	}
	for i := 0; i < 4; i++ {
		if second[i] != first[i] {
			t.Fatalf("incarnation 2 applied %q at position %d, incarnation 1 applied %q: replay must be identical", second[i], i, first[i])
		}
	}
	// A restarted single node also elects itself again, appending a new no-op; it
	// is applied once, after the replayed prefix, never interleaved.
	if len(second) > 4 && second[4] != "" {
		t.Fatalf("after the replayed prefix incarnation 2 applied %q, want the new no-op", second[4])
	}
}

// TestCrashBeforeApplyAppliesOnceOnRestart: dying before Apply(k) means
// incarnation 1 never applied k; incarnation 2 applies it exactly once (after
// replaying the prefix below it).
func TestCrashBeforeApplyAppliesOnceOnRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logPath := filepath.Join(t.TempDir(), "n0.log")
	sm := &recSM{}
	hook, fired := crashHook(BeforeApply, 3) // no-op (1), a (2), then die before b (3)
	n1, tr1 := startSingle(t, ctx, logPath, sm, hook)
	waitLeaderNode(t, n1)
	_ = n1.Propose(ctx, []byte("a"))
	_ = n1.Propose(ctx, []byte("b"))
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("the crash point before apply never fired")
	}
	<-n1.Done()
	_ = tr1.Close()
	first := sm.snapshot()
	if len(first) != 2 || first[1] != "a" {
		t.Fatalf("incarnation 1 applied %v, want the no-op and a only", first)
	}
	n2, tr2 := startSingle(t, ctx, logPath, sm, nil)
	defer func() { _ = n2.Close(); _ = tr2.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && n2.Status().Applied < 3 {
		time.Sleep(2 * time.Millisecond)
	}
	all := sm.snapshot()
	second := all[len(first):]
	count := func(s []string, cmd string) int {
		n := 0
		for _, x := range s {
			if x == cmd {
				n++
			}
		}
		return n
	}
	if count(second, "b") != 1 || count(all, "b") != 1 {
		t.Fatalf("b applied %d times in incarnation 2 and %d overall, want exactly once, by incarnation 2: %v", count(second, "b"), count(all, "b"), all)
	}
	if count(second, "a") != 1 {
		t.Fatalf("incarnation 2 re-applied a %d times, want once (its whole committed prefix): %v", count(second, "a"), second)
	}
}

// TestCrashAfterSaveBeforeAdvanceRestartsFromDurableState: the actor dies after
// the Save of a proposal but before the cycle completes. The entry is durable
// (fsynced); the restarted node recovers it, re-elects, and commits it.
func TestCrashAfterSaveBeforeAdvanceRestartsFromDurableState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logPath := filepath.Join(t.TempDir(), "n0.log")
	sm := &recSM{}
	hook, fired := crashHook(AfterSave, 2) // election Save (1), then the proposal's Save (2)
	n1, tr1 := startSingle(t, ctx, logPath, sm, hook)
	waitLeaderNode(t, n1)
	err := n1.Propose(ctx, []byte("x"))
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("the crash point after the Save never fired")
	}
	<-n1.Done()
	_ = tr1.Close()
	if err == nil {
		t.Fatal("a proposal whose cycle died before completing must not be acknowledged")
	}
	rec, err := raftlog.Inspect(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Entries) != 2 || string(rec.Entries[1].Data) != "x" {
		t.Fatalf("durable log after the crash: %+v, want the no-op and x (the Save had completed)", rec.Entries)
	}
	n2, tr2 := startSingle(t, ctx, logPath, sm, nil)
	defer func() { _ = n2.Close(); _ = tr2.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && n2.Status().Applied < 2 {
		time.Sleep(2 * time.Millisecond)
	}
	// Incarnation 1 applied only its no-op before dying. Incarnation 2 replays
	// that no-op, elects itself (a new no-op), and — with a current-term entry to
	// commit behind — commits and applies the recovered x exactly once.
	all := sm.snapshot()
	xs := 0
	for i, cmd := range all {
		if cmd == "x" {
			xs++
			if i < 1 {
				t.Fatalf("x applied before the replayed no-op: %v", all)
			}
		}
	}
	if xs != 1 {
		t.Fatalf("restarted node applied x %d times in %v, want exactly once (recovered from the durable Save, never applied by the dead incarnation)", xs, all)
	}
}

// TestRecoverRefusesATermBelowItsLog pins the recovery guard: a durable log whose
// last entry's term exceeds its HardState's term is incoherent (the Save order
// never produces it), and Recover must refuse it rather than invent a term.
func TestRecoverRefusesATermBelowItsLog(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "bad.log")
	l, _, err := raftlog.Open(logPath, raftlog.Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	// Write the entry first, then a HardState of a lower term — bypassing the
	// driver, which never asks for this.
	if err := l.Save(nil, []raftlog.Entry{{Index: 1, Term: 5}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Save(&raftlog.HardState{Term: 2}, nil); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	_, err = Recover(Config{ID: "n0", Peers: []NodeID{"n0"}, LogPath: logPath, Rand: rand.New(rand.NewSource(1))})
	if !errors.Is(err, raft.ErrTermRegression) {
		t.Fatalf("Recover = %v, want ErrTermRegression", err)
	}
}
