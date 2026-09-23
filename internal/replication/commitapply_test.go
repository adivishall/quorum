package replication

import (
	"errors"
	"fmt"
	"testing"
)

func filled(t *testing.T, n uint64) *MemoryLog {
	t.Helper()
	l := NewMemoryLog()
	for i := uint64(1); i <= n; i++ {
		mustAppend(t, l, ent(i, 1))
	}
	return l
}

// TestCommitInitialAndMonotonic covers the initial commit state, forward
// advancement, idempotent no-op, backward rejection, and the beyond-last-index
// rejection (INV-P5, INV-P6).
func TestCommitInitialAndMonotonic(t *testing.T) {
	l := filled(t, 5)
	if l.CommitIndex() != 0 {
		t.Fatalf("initial commit = %d, want 0", l.CommitIndex())
	}
	if err := l.Commit(3); err != nil {
		t.Fatalf("Commit(3): %v", err)
	}
	if l.CommitIndex() != 3 {
		t.Fatalf("commit = %d, want 3", l.CommitIndex())
	}
	// Idempotent no-op is allowed, not a regression.
	if err := l.Commit(3); err != nil {
		t.Fatalf("Commit(3) again: %v", err)
	}
	// Forward again.
	if err := l.Commit(5); err != nil {
		t.Fatalf("Commit(5): %v", err)
	}
	// Backward is rejected and does not change state.
	if err := l.Commit(4); !errors.Is(err, ErrCommitRegression) {
		t.Fatalf("Commit(4) after 5 = %v, want ErrCommitRegression", err)
	}
	if l.CommitIndex() != 5 {
		t.Fatalf("commit = %d after rejected backward commit, want 5", l.CommitIndex())
	}
	// Beyond the last index is rejected.
	if err := l.Commit(6); !errors.Is(err, ErrCommitBeyondLog) {
		t.Fatalf("Commit(6) with lastIndex 5 = %v, want ErrCommitBeyondLog", err)
	}
}

// TestCommittedRangeEnumeration proves Unapplied enumerates exactly the
// committed-but-not-applied entries, in order.
func TestCommittedRangeEnumeration(t *testing.T) {
	l := filled(t, 5)
	if err := l.Commit(3); err != nil {
		t.Fatal(err)
	}
	es, err := l.Unapplied()
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 3 || es[0].Index != 1 || es[2].Index != 3 {
		t.Fatalf("Unapplied() = %v, want indexes [1,2,3]", es)
	}
	// After applying some, only the remainder is unapplied.
	if err := l.Apply(2); err != nil {
		t.Fatal(err)
	}
	es, _ = l.Unapplied()
	if len(es) != 1 || es[0].Index != 3 {
		t.Fatalf("Unapplied() after Apply(2) = %v, want [3]", es)
	}
}

// TestApplyInitialAndMonotonic covers initial applied state, forward apply,
// backward rejection, applying past commit rejection, and applied <= commit
// (INV-P7, INV-P8).
func TestApplyInitialAndMonotonic(t *testing.T) {
	l := filled(t, 5)
	if l.AppliedIndex() != 0 {
		t.Fatalf("initial applied = %d, want 0", l.AppliedIndex())
	}
	// Applying an uncommitted index is rejected (nothing is committed yet).
	if err := l.Apply(1); !errors.Is(err, ErrApplyBeyondCommit) {
		t.Fatalf("Apply(1) with commit 0 = %v, want ErrApplyBeyondCommit", err)
	}
	if err := l.Commit(4); err != nil {
		t.Fatal(err)
	}
	if err := l.Apply(2); err != nil {
		t.Fatalf("Apply(2): %v", err)
	}
	if l.AppliedIndex() != 2 {
		t.Fatalf("applied = %d, want 2", l.AppliedIndex())
	}
	// Forward within commit.
	if err := l.Apply(4); err != nil {
		t.Fatalf("Apply(4): %v", err)
	}
	// Backward is rejected.
	if err := l.Apply(3); !errors.Is(err, ErrAppliedRegression) {
		t.Fatalf("Apply(3) after 4 = %v, want ErrAppliedRegression", err)
	}
	// Past commit is rejected.
	if err := l.Apply(5); !errors.Is(err, ErrApplyBeyondCommit) {
		t.Fatalf("Apply(5) with commit 4 = %v, want ErrApplyBeyondCommit", err)
	}
	if l.AppliedIndex() != 4 {
		t.Fatalf("applied = %d after rejected applies, want 4", l.AppliedIndex())
	}
}

// TestNoDoubleApplication proves an entry is applied at most once through the
// interface (INV-P9): a re-issued Apply of an already-applied index applies
// nothing, and a StateMachine driven from Unapplied sees each index exactly once.
func TestNoDoubleApplication(t *testing.T) {
	l := filled(t, 4)
	if err := l.Commit(4); err != nil {
		t.Fatal(err)
	}

	sm := &countingSM{seen: map[uint64]int{}}
	// Drive the state-machine seam the way a Phase 9 driver will: pull the
	// unapplied entries, apply each, then advance the applied watermark.
	drain := func() {
		es, err := l.Unapplied()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range es {
			if err := sm.Apply(e.Index, e.Data); err != nil {
				t.Fatal(err)
			}
		}
		if len(es) > 0 {
			if err := l.Apply(es[len(es)-1].Index); err != nil {
				t.Fatal(err)
			}
		}
	}
	drain()
	drain() // a second drain must find nothing new
	// Re-issuing an already-applied index is a no-op, not a re-application.
	if err := l.Apply(4); err != nil {
		t.Fatalf("Apply(4) again: %v", err)
	}
	drain()

	if sm.applied != 4 {
		t.Fatalf("state machine applied %d commands, want 4", sm.applied)
	}
	for idx, n := range sm.seen {
		if n != 1 {
			t.Fatalf("index %d applied %d times, want exactly once", idx, n)
		}
	}
}

// countingSM is a trivial in-test StateMachine that records how many times each
// index was applied. It exercises the Phase 8 state-machine seam.
type countingSM struct {
	applied int
	seen    map[uint64]int
}

func (s *countingSM) Apply(index uint64, command []byte) error {
	want := []byte(fmt.Sprintf("cmd@%d.1", index))
	if string(command) != string(want) {
		return fmt.Errorf("index %d: command %q, want %q", index, command, want)
	}
	s.applied++
	s.seen[index]++
	return nil
}

var _ StateMachine = (*countingSM)(nil)
