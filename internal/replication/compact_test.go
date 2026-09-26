package replication

import (
	"errors"
	"testing"
)

// logWith returns a log of n entries, entry i in term termOf(i), committed and
// applied through applied.
func logWith(t *testing.T, n int, termOf func(i uint64) uint64, applied uint64) *MemoryLog {
	t.Helper()
	l := NewMemoryLog()
	for i := uint64(1); i <= uint64(n); i++ {
		mustAppend(t, l, Entry{Index: i, Term: termOf(i), Data: []byte{byte(i)}})
	}
	if err := l.Commit(uint64(n)); err != nil {
		t.Fatal(err)
	}
	if err := l.Apply(applied); err != nil {
		t.Fatal(err)
	}
	return l
}

// TestCompactDiscardsOnlyTheAppliedPrefix: a compaction may discard only entries
// the state machine has applied — a snapshot holds only applied state — and
// never moves the boundary back; the boundary's term stays answerable (Raft's
// consistency check needs it) while the entries at and below it are gone.
func TestCompactDiscardsOnlyTheAppliedPrefix(t *testing.T) {
	l := logWith(t, 10, func(i uint64) uint64 { return 1 + i/4 }, 6)
	if err := l.Compact(7); !errors.Is(err, ErrCompactBeyondApplied) {
		t.Fatalf("compacting past the applied index: %v", err)
	}
	if err := l.Compact(6); err != nil {
		t.Fatal(err)
	}
	if b, bt := l.Boundary(); b != 6 || bt != 2 || l.FirstIndex() != 7 || l.LastIndex() != 10 {
		t.Fatalf("after Compact(6): boundary (%d,%d) first %d last %d", b, bt, l.FirstIndex(), l.LastIndex())
	}
	if tm, err := l.Term(6); err != nil || tm != 2 {
		t.Fatalf("Term(boundary) = %d, %v; want 2", tm, err)
	}
	for _, i := range []uint64{0, 1, 5} {
		if _, err := l.Term(i); !errors.Is(err, ErrCompacted) {
			t.Fatalf("Term(%d) below the boundary: %v", i, err)
		}
	}
	if _, err := l.At(6); !errors.Is(err, ErrCompacted) {
		t.Fatalf("At(boundary): %v", err)
	}
	if _, err := l.Slice(6, 8); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Slice reaching the boundary: %v", err)
	}
	if es, err := l.Slice(7, 11); err != nil || len(es) != 4 || es[0].Index != 7 {
		t.Fatalf("Slice(7,11): %v %v", es, err)
	}
	if err := l.Compact(5); !errors.Is(err, ErrCompacted) {
		t.Fatalf("moving the boundary back: %v", err)
	}
	if err := l.Compact(6); err != nil {
		t.Fatalf("compacting to the boundary is a no-op: %v", err)
	}
	if err := l.TruncateAndAppend(Entry{Index: 6, Term: 9}); !errors.Is(err, ErrTruncateCommitted) {
		t.Fatalf("rewriting at the boundary: %v", err)
	}
	// The log goes on: the next entry follows the last one, as ever.
	mustAppend(t, l, Entry{Index: 11, Term: 3})
	if err := l.Commit(11); err != nil {
		t.Fatal(err)
	}
	if es, err := l.Unapplied(); err != nil || len(es) != 5 || es[0].Index != 7 {
		t.Fatalf("Unapplied after compaction: %v %v", es, err)
	}
}

// TestInstallSnapshotKeepsOnlyAMatchingSuffix is Raft §7's rule: a snapshot
// whose last entry the log holds with the same term keeps the entries after it;
// any other snapshot empties the log. Commit and applied become the snapshot's
// index; a snapshot at or below the commit is stale and changes nothing.
func TestInstallSnapshotKeepsOnlyAMatchingSuffix(t *testing.T) {
	uncommitted := func() *MemoryLog {
		l := NewMemoryLog()
		for i := uint64(1); i <= 8; i++ {
			mustAppend(t, l, Entry{Index: i, Term: 1 + i/5, Data: []byte{byte(i)}})
		}
		if err := l.Commit(3); err != nil {
			t.Fatal(err)
		}
		return l
	}
	// Matching: the log holds (6, term 2).
	l := uncommitted()
	if err := l.InstallSnapshot(6, 2); err != nil {
		t.Fatal(err)
	}
	if b, bt := l.Boundary(); b != 6 || bt != 2 || l.LastIndex() != 8 || l.CommitIndex() != 6 || l.AppliedIndex() != 6 {
		t.Fatalf("matching install: boundary (%d,%d) last %d commit %d applied %d", b, bt, l.LastIndex(), l.CommitIndex(), l.AppliedIndex())
	}
	// Conflicting: the log holds index 6 in term 2, the snapshot says term 3.
	l = uncommitted()
	if err := l.InstallSnapshot(6, 3); err != nil {
		t.Fatal(err)
	}
	if l.LastIndex() != 6 || l.FirstIndex() != 7 {
		t.Fatalf("conflicting install kept entries: last %d", l.LastIndex())
	}
	// Beyond the log.
	l = uncommitted()
	if err := l.InstallSnapshot(20, 5); err != nil {
		t.Fatal(err)
	}
	if b, _ := l.Boundary(); b != 20 || l.LastIndex() != 20 || l.CommitIndex() != 20 {
		t.Fatalf("install beyond the log: boundary %d last %d", b, l.LastIndex())
	}
	// A later entry must not fall below the snapshot's term.
	if err := l.Append(Entry{Index: 21, Term: 4}); !errors.Is(err, ErrTermRegression) {
		t.Fatalf("an entry below the snapshot's term was appended: %v", err)
	}
	// Stale: at or below the commit.
	l = uncommitted()
	for _, idx := range []uint64{0, 2, 3} {
		if err := l.InstallSnapshot(idx, 1); !errors.Is(err, ErrStaleSnapshot) {
			t.Fatalf("stale install at %d: %v", idx, err)
		}
	}
	if b, _ := l.Boundary(); b != 0 || l.LastIndex() != 8 || l.CommitIndex() != 3 {
		t.Fatal("a refused install changed the log")
	}
}

// TestEmptyLogBoundaryIsTheSentinel: a never-compacted log's boundary is the
// index-0 sentinel of term 0, so every pre-Phase-14 caller sees no change.
func TestEmptyLogBoundaryIsTheSentinel(t *testing.T) {
	l := NewMemoryLog()
	if b, bt := l.Boundary(); b != 0 || bt != 0 || l.FirstIndex() != 1 {
		t.Fatalf("boundary (%d,%d), first %d", b, bt, l.FirstIndex())
	}
	if tm, err := l.Term(0); err != nil || tm != 0 {
		t.Fatalf("Term(0) = %d, %v", tm, err)
	}
	if _, err := l.At(0); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("At(0) = %v, want ErrOutOfRange (not compacted)", err)
	}
}
