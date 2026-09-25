package raftnode

import (
	"errors"
	"testing"

	"github.com/adivishall/quorum/internal/raft"
)

// TestWaitersCompleteWritesOnlyInTheirTerm: a write succeeds when the entry
// applied at its index carries its term, and is ErrLost when a different
// entry (another term) was committed there instead.
func TestWaitersCompleteWritesOnlyInTheirTerm(t *testing.T) {
	w := NewWaiters()
	ok := w.Add(5, 2, 3)
	lost := w.Add(6, 2, 3)
	if w.Len() != 2 {
		t.Fatalf("len %d", w.Len())
	}
	select {
	case <-ok:
		t.Fatal("a write completed before its index was applied")
	default:
	}
	w.Applied(5, 2)
	w.Applied(6, 3) // a different term's entry at 6: the term-2 proposal was overwritten
	if out := <-ok; out.Err != nil || out.Index != 5 || out.Term != 2 {
		t.Fatalf("matching term: %+v", out)
	}
	if out := <-lost; !errors.Is(out.Err, ErrLost) || out.Term != 3 {
		t.Fatalf("overwritten entry: %+v, want ErrLost", out)
	}
	if w.Len() != 0 {
		t.Fatalf("len %d after completion", w.Len())
	}
}

// TestWaitersBarrierIsImmediateWhenAlreadyApplied: a read barrier at or below
// the applied index completes at once; above it, when that index applies.
func TestWaitersBarrierIsImmediateWhenAlreadyApplied(t *testing.T) {
	w := NewWaiters()
	now := w.Add(4, 0, 4)
	if out := <-now; out.Err != nil || out.Index != 4 {
		t.Fatalf("immediate barrier: %+v", out)
	}
	later := w.Add(7, 0, 4)
	w.Applied(5, 1)
	w.Applied(6, 1)
	select {
	case <-later:
		t.Fatal("barrier at 7 completed at index 6")
	default:
	}
	w.Applied(7, 9)
	if out := <-later; out.Err != nil || out.Index != 7 {
		t.Fatalf("barrier: %+v", out)
	}
	// A write is never immediate, even if the caller's applied index is stale.
	wr := w.Add(2, 1, 9)
	select {
	case <-wr:
		t.Fatal("a write completed without an apply")
	default:
	}
}

// TestWaitersFailAllReportsUnknown.
func TestWaitersFailAllReportsUnknown(t *testing.T) {
	w := NewWaiters()
	a, b := w.Add(1, 1, 0), w.Add(9, 0, 0)
	w.FailAll(raft.ErrStopped)
	for _, ch := range []<-chan Outcome{a, b} {
		if out := <-ch; !errors.Is(out.Err, raft.ErrStopped) {
			t.Fatalf("%+v", out)
		}
	}
	if w.Len() != 0 {
		t.Fatal("waiters remain after FailAll")
	}
}

// TestReadsConfirmAndDropStale: a confirmed read becomes a barrier (or completes
// at once if already applied); an unconfirmed read from an older term, or on a
// node that no longer leads, fails with ErrNotLeader.
func TestReadsConfirmAndDropStale(t *testing.T) {
	r, w := NewReads(), NewWaiters()
	c1 := r.Add(1, 3)
	c2 := r.Add(2, 3)
	c3 := r.Add(3, 3)
	r.Confirmed(raft.ReadState{ID: 1, Index: 10}, w, 12)
	if out := <-c1; out.Err != nil || out.Index != 10 {
		t.Fatalf("already-applied read: %+v", out)
	}
	r.Confirmed(raft.ReadState{ID: 2, Index: 15}, w, 12)
	select {
	case <-c2:
		t.Fatal("read at 15 served at applied 12")
	default:
	}
	w.Applied(15, 3)
	if out := <-c2; out.Err != nil || out.Index != 15 {
		t.Fatalf("barrier read: %+v", out)
	}
	r.DropStale(3, true) // still leading in term 3: nothing dropped
	if r.Len() != 1 {
		t.Fatalf("len %d", r.Len())
	}
	r.DropStale(4, true) // a new term: the term-3 read is gone
	if out := <-c3; !errors.Is(out.Err, raft.ErrNotLeader) {
		t.Fatalf("stale read: %+v, want ErrNotLeader", out)
	}
	c4 := r.Add(4, 4)
	r.DropStale(4, false) // same term, but no longer leader
	if out := <-c4; !errors.Is(out.Err, raft.ErrNotLeader) {
		t.Fatalf("read on a deposed leader: %+v", out)
	}
	r.Confirmed(raft.ReadState{ID: 99, Index: 1}, w, 0) // unknown id: ignored
	if r.Len() != 0 || w.Len() != 0 {
		t.Fatalf("stray state: reads %d waiters %d", r.Len(), w.Len())
	}
}
