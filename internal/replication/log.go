package replication

// Entry is one opaque log entry. Data is arbitrary bytes the replication layer
// never interprets — in the finished system it is an encoded state-machine
// command, but Phase 8 treats it as opaque (docs/REPLICATION.md §3.1).
//
// Indexes are 1-based and contiguous; index 0 is the empty sentinel with term 0
// (docs/REPLICATION.md §3.2). Terms are a non-decreasing logical clock (§3.3).
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Log is the small, explicit interface for a local replicated log — the
// primitive Phase 9's Raft will drive. It exposes the local operations Raft
// needs and nothing that belongs to consensus: there is no currentTerm, votedFor,
// leader/follower state, quorum, or decision about WHETHER an index may be
// committed. Commit records that an index IS committed; it does not decide that a
// distributed group is ALLOWED to (docs/REPLICATION.md §4).
//
// Ranges are half-open [lo,hi); a valid range satisfies 1 <= lo <= hi <=
// LastIndex()+1. Implementations copy Entry.Data in and out (INV-P4) and are
// deterministic (INV-P2, §9).
type Log interface {
	// FirstIndex is the index of the first entry the log could hold. It is 1 in
	// Phase 8 (nothing truncates the front; snapshots are Phase 14).
	FirstIndex() uint64
	// LastIndex is the highest live index, or 0 when the log is empty.
	LastIndex() uint64

	// Term returns the term at index. Term(0) == 0; an index past LastIndex is
	// ErrOutOfRange.
	Term(index uint64) (uint64, error)
	// At returns a copy of the entry at index; out of range is ErrOutOfRange.
	At(index uint64) (Entry, error)
	// Slice returns copies of the entries with indexes in [lo,hi). An invalid
	// range is ErrOutOfRange, never clamped.
	Slice(lo, hi uint64) ([]Entry, error)

	// Append extends the log at the end only. Entries must be contiguous starting
	// at LastIndex()+1 with non-decreasing terms; a gap, a duplicate index, or a
	// term regression is refused (ErrNonContiguous / ErrTermRegression).
	Append(entries ...Entry) error
	// TruncateAndAppend replaces a conflicting suffix, then appends. Entries begin
	// at some index f: it retains [1,f-1], drops the existing suffix [f,LastIndex],
	// and appends the batch. f may not exceed LastIndex()+1 (no gap) and must be
	// greater than CommitIndex() (committed entries are never replaced).
	TruncateAndAppend(entries ...Entry) error

	// CommitIndex is the highest index recorded as committed.
	CommitIndex() uint64
	// Commit advances commitIndex to index. It is monotonic (never backward,
	// ErrCommitRegression) and never exceeds LastIndex (ErrCommitBeyondLog).
	Commit(index uint64) error

	// AppliedIndex is the highest index recorded as applied.
	AppliedIndex() uint64
	// Unapplied returns copies of the committed-but-not-applied entries, in index
	// order — those in (AppliedIndex(), CommitIndex()].
	Unapplied() ([]Entry, error)
	// Apply advances appliedIndex to through. It is monotonic (never backward,
	// ErrAppliedRegression) and never exceeds CommitIndex (ErrApplyBeyondCommit),
	// so an entry is applied at most once through this interface.
	Apply(through uint64) error
}

// MemoryLog is the Phase 8 in-memory reference implementation of Log. It exists
// to exercise the interface, make every edge case executable, and give Phase 9
// something deterministic to drive before a persistent replicated log is built.
// It is NOT the final log (no persistence, no fsync, no segments) and NOT a
// distributed replication engine.
//
// Following ADR-002, it holds no locks, starts no goroutine, and reads no clock
// or randomness: it is a pure object a single goroutine drives, exactly like the
// coming raft.Raft. It is therefore NOT safe for concurrent use.
type MemoryLog struct {
	entries []Entry // index i is stored at slot i-1
	commit  uint64
	applied uint64
}

// NewMemoryLog returns an empty log: LastIndex 0, commitIndex 0, appliedIndex 0.
func NewMemoryLog() *MemoryLog { return &MemoryLog{} }

// compile-time assertion that MemoryLog satisfies Log.
var _ Log = (*MemoryLog)(nil)

// FirstIndex is always 1 in Phase 8 (docs/REPLICATION.md §3.2).
func (l *MemoryLog) FirstIndex() uint64 { return 1 }

// LastIndex returns the highest live index, or 0 when empty.
func (l *MemoryLog) LastIndex() uint64 { return uint64(len(l.entries)) }

// CommitIndex returns the highest committed index.
func (l *MemoryLog) CommitIndex() uint64 { return l.commit }

// AppliedIndex returns the highest applied index.
func (l *MemoryLog) AppliedIndex() uint64 { return l.applied }

// Term returns the term at index. Term(0) is 0; index > LastIndex is out of range.
func (l *MemoryLog) Term(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	if index > l.LastIndex() {
		return 0, ErrOutOfRange
	}
	return l.entries[index-1].Term, nil
}

// At returns a copy of the entry at index (1 <= index <= LastIndex).
func (l *MemoryLog) At(index uint64) (Entry, error) {
	if index < 1 || index > l.LastIndex() {
		return Entry{}, ErrOutOfRange
	}
	return copyEntry(l.entries[index-1]), nil
}

// Slice returns copies of the entries with indexes in the half-open range
// [lo,hi). A valid range satisfies 1 <= lo <= hi <= LastIndex()+1; anything else
// is ErrOutOfRange. Slice(lo,lo) is empty.
func (l *MemoryLog) Slice(lo, hi uint64) ([]Entry, error) {
	if lo < 1 || hi < lo || hi > l.LastIndex()+1 {
		return nil, ErrOutOfRange
	}
	out := make([]Entry, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, copyEntry(l.entries[i-1]))
	}
	return out, nil
}

// Append extends the log at the end. See the Log interface for the contract.
func (l *MemoryLog) Append(entries ...Entry) error {
	if len(entries) == 0 {
		return ErrEmptyBatch
	}
	if entries[0].Index != l.LastIndex()+1 {
		return ErrNonContiguous
	}
	return l.appendFrom(l.LastIndex()+1, entries)
}

// TruncateAndAppend replaces a conflicting suffix, then appends. See the Log
// interface and docs/REPLICATION.md §6 for the exact contract.
func (l *MemoryLog) TruncateAndAppend(entries ...Entry) error {
	if len(entries) == 0 {
		return ErrEmptyBatch
	}
	f := entries[0].Index
	if f < 1 || f > l.LastIndex()+1 {
		// f == LastIndex+1 is a pure append; beyond that would open a gap.
		return ErrNonContiguous
	}
	if f <= l.commit {
		return ErrTruncateCommitted
	}
	return l.appendFrom(f, entries)
}

// appendFrom validates the batch is contiguous from f with non-decreasing terms
// (against the retained prefix and within the batch), then installs it: it drops
// any existing suffix at or after f and appends copies of the batch. It mutates
// nothing until all validation passes, so a rejected operation leaves the log
// unchanged (matches INV-A8's "a rejected operation has no effect").
func (l *MemoryLog) appendFrom(f uint64, entries []Entry) error {
	prevTerm, err := l.Term(f - 1)
	if err != nil {
		return err
	}
	for k, e := range entries {
		if e.Index != f+uint64(k) {
			return ErrNonContiguous
		}
		if e.Term < prevTerm {
			return ErrTermRegression
		}
		prevTerm = e.Term
	}
	// All checks passed; install.
	l.entries = l.entries[:f-1]
	for _, e := range entries {
		l.entries = append(l.entries, copyEntry(e))
	}
	return nil
}

// Commit advances commitIndex to index (monotonic, never past LastIndex).
func (l *MemoryLog) Commit(index uint64) error {
	if index < l.commit {
		return ErrCommitRegression
	}
	if index > l.LastIndex() {
		return ErrCommitBeyondLog
	}
	l.commit = index
	return nil
}

// Unapplied returns copies of the entries in (appliedIndex, commitIndex].
func (l *MemoryLog) Unapplied() ([]Entry, error) {
	if l.applied >= l.commit {
		return nil, nil
	}
	return l.Slice(l.applied+1, l.commit+1)
}

// Apply advances appliedIndex to through (monotonic, never past commitIndex).
func (l *MemoryLog) Apply(through uint64) error {
	if through < l.applied {
		return ErrAppliedRegression
	}
	if through > l.commit {
		return ErrApplyBeyondCommit
	}
	l.applied = through
	return nil
}

// copyEntry returns an entry whose Data is a fresh copy, so no stored byte is
// ever aliased to caller memory and no returned byte exposes internal storage
// (INV-P4). A nil Data stays nil; an empty non-nil Data is preserved as empty.
func copyEntry(e Entry) Entry {
	if e.Data == nil {
		return Entry{Index: e.Index, Term: e.Term, Data: nil}
	}
	d := make([]byte, len(e.Data))
	copy(d, e.Data)
	return Entry{Index: e.Index, Term: e.Term, Data: d}
}
