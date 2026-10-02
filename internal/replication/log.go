package replication

// MaxEntryDataLen is the largest Entry.Data, in bytes, anywhere in the system:
// the one authoritative entry-size limit. Every layer an entry passes through
// enforces this same constant — the key-value front validates the ENCODED
// command against it, raft.Propose refuses a larger proposal, the log refuses
// to hold one, the AppendEntries codec and the core refuse to accept one,
// raftlog.Save refuses to persist one and raftlog replay refuses to read one.
// So no code path creates, persists, transmits or accepts an entry that some
// other path would refuse to read back (docs/RAFT.md §16). It is 1 MiB, the
// size every on-disk and on-wire decoder has always bounded an entry by.
const MaxEntryDataLen = 1 << 20

// Entry is one opaque log entry. Data is arbitrary bytes the replication layer
// never interprets — in the finished system it is an encoded state-machine
// command, but Phase 8 treats it as opaque (docs/REPLICATION.md §3.1).
//
// Indexes are 1-based and contiguous; index 0 is the empty sentinel with term 0
// (docs/REPLICATION.md §3.2). Terms are a non-decreasing logical clock (§3.3).
// Since Phase 14 a log may begin after a compaction boundary: the entries at
// and below it were discarded into a snapshot (docs/SNAPSHOTS.md).
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
	// Type says what Data is (Phase 15): a state-machine command (EntryNormal,
	// the zero value — every entry before Phase 15) or an encoded Configuration
	// (EntryConfig), which the state machine never sees.
	Type EntryType
}

// Log is the small, explicit interface for a local replicated log — the
// primitive Phase 9's Raft will drive. It exposes the local operations Raft
// needs and nothing that belongs to consensus: there is no currentTerm, votedFor,
// leader/follower state, quorum, or decision about WHETHER an index may be
// committed. Commit records that an index IS committed; it does not decide that a
// distributed group is ALLOWED to (docs/REPLICATION.md §4).
//
// Ranges are half-open [lo,hi); a valid range satisfies FirstIndex() <= lo <=
// hi <= LastIndex()+1. Implementations copy Entry.Data in and out (INV-P4) and
// are deterministic (INV-P2, §9).
//
// Phase 14 adds a compaction boundary (Boundary): the index and term of the last
// entry discarded into a snapshot. Entries at or below it are gone — asking for
// one is ErrCompacted — but the boundary's own term stays answerable, because
// Raft's consistency check needs the term of the entry just before the first
// one it sends. boundary <= applied <= commit <= LastIndex always holds.
type Log interface {
	// FirstIndex is the index of the first entry the log could hold:
	// Boundary()+1 (1 when nothing was compacted).
	FirstIndex() uint64
	// Boundary is the index and term of the last compacted entry, (0, 0) when
	// the log was never compacted.
	Boundary() (index, term uint64)
	// LastIndex is the highest live index, or 0 when the log is empty.
	LastIndex() uint64

	// Term returns the term at index. Term(Boundary index) is the boundary term
	// (Term(0) == 0 on an uncompacted log); below the boundary is ErrCompacted,
	// past LastIndex ErrOutOfRange.
	Term(index uint64) (uint64, error)
	// At returns a copy of the entry at index; at or below the boundary is
	// ErrCompacted, past LastIndex ErrOutOfRange.
	At(index uint64) (Entry, error)
	// Slice returns copies of the entries with indexes in [lo,hi). A range
	// reaching at or below the boundary is ErrCompacted; any other invalid range
	// is ErrOutOfRange, never clamped.
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

	// Compact discards the entries at or below index into a snapshot the caller
	// already made durable (Phase 14). index must be applied
	// (ErrCompactBeyondApplied otherwise) — a snapshot contains only applied
	// state — and not below the current boundary (ErrCompacted); compacting to
	// the boundary itself changes nothing. The term of the entry at index becomes
	// the boundary term.
	Compact(index uint64) error
	// InstallSnapshot resets the log to a snapshot whose last entry is (index,
	// term), received from a leader (Raft §7). index must be above the commit
	// index (ErrStaleSnapshot otherwise). If the log holds index with that term,
	// the entries after it are kept; otherwise every entry is discarded. The
	// boundary becomes (index, term) and commit and applied become index: the
	// caller replaces the state machine's state with the snapshot's.
	InstallSnapshot(index, term uint64) error
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
	entries  []Entry // index i is stored at slot i-base-1
	base     uint64  // compaction boundary: the last discarded index (0: none)
	baseTerm uint64  // the term of the entry at base
	commit   uint64
	applied  uint64
}

// NewMemoryLog returns an empty log: LastIndex 0, commitIndex 0, appliedIndex 0.
func NewMemoryLog() *MemoryLog { return &MemoryLog{} }

// compile-time assertion that MemoryLog satisfies Log.
var _ Log = (*MemoryLog)(nil)

// FirstIndex is the first index the log can hold: the boundary plus one.
func (l *MemoryLog) FirstIndex() uint64 { return l.base + 1 }

// Boundary returns the index and term of the last compacted entry.
func (l *MemoryLog) Boundary() (uint64, uint64) { return l.base, l.baseTerm }

// LastIndex returns the highest live index: the boundary when nothing follows
// it, 0 for an empty, never-compacted log.
func (l *MemoryLog) LastIndex() uint64 { return l.base + uint64(len(l.entries)) }

// CommitIndex returns the highest committed index.
func (l *MemoryLog) CommitIndex() uint64 { return l.commit }

// AppliedIndex returns the highest applied index.
func (l *MemoryLog) AppliedIndex() uint64 { return l.applied }

// Term returns the term at index: the boundary term at the boundary (0 at index
// 0 of a never-compacted log), ErrCompacted below it, ErrOutOfRange past the end.
func (l *MemoryLog) Term(index uint64) (uint64, error) {
	switch {
	case index < l.base:
		return 0, ErrCompacted
	case index == l.base:
		return l.baseTerm, nil
	case index > l.LastIndex():
		return 0, ErrOutOfRange
	}
	return l.entries[index-l.base-1].Term, nil
}

// At returns a copy of the entry at index (FirstIndex <= index <= LastIndex).
func (l *MemoryLog) At(index uint64) (Entry, error) {
	switch {
	case index <= l.base && l.base > 0:
		return Entry{}, ErrCompacted
	case index < 1 || index > l.LastIndex():
		return Entry{}, ErrOutOfRange
	}
	return copyEntry(l.entries[index-l.base-1]), nil
}

// Slice returns copies of the entries with indexes in the half-open range
// [lo,hi). A valid range satisfies FirstIndex <= lo <= hi <= LastIndex()+1; a
// range starting at or below the boundary is ErrCompacted, anything else invalid
// is ErrOutOfRange. Slice(lo,lo) is empty.
func (l *MemoryLog) Slice(lo, hi uint64) ([]Entry, error) {
	if lo <= l.base && l.base > 0 {
		return nil, ErrCompacted
	}
	if lo < 1 || hi < lo || hi > l.LastIndex()+1 {
		return nil, ErrOutOfRange
	}
	out := make([]Entry, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, copyEntry(l.entries[i-l.base-1]))
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
		// Also covers f <= base: compacted entries are committed.
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
		if len(e.Data) > MaxEntryDataLen {
			return ErrEntryTooLarge
		}
		prevTerm = e.Term
	}
	// All checks passed; install.
	l.entries = l.entries[:f-l.base-1]
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

// Compact discards the entries at or below index (see the Log interface).
func (l *MemoryLog) Compact(index uint64) error {
	switch {
	case index < l.base:
		return ErrCompacted
	case index == l.base:
		return nil
	case index > l.applied:
		return ErrCompactBeyondApplied
	}
	term, err := l.Term(index)
	if err != nil {
		return err
	}
	keep := l.entries[index-l.base:]
	l.entries = append([]Entry(nil), keep...) // release the discarded prefix
	l.base, l.baseTerm = index, term
	return nil
}

// InstallSnapshot resets the log to a snapshot ending at (index, term) (see the
// Log interface).
func (l *MemoryLog) InstallSnapshot(index, term uint64) error {
	if index <= l.commit {
		return ErrStaleSnapshot
	}
	var keep []Entry
	if t, err := l.Term(index); err == nil && t == term && index <= l.LastIndex() {
		keep = append([]Entry(nil), l.entries[index-l.base:]...)
	}
	l.entries = keep
	l.base, l.baseTerm = index, term
	l.commit, l.applied = index, index
	return nil
}

// copyEntry returns an entry whose Data is a fresh copy, so no stored byte is
// ever aliased to caller memory and no returned byte exposes internal storage
// (INV-P4). A nil Data stays nil; an empty non-nil Data is preserved as empty.
func copyEntry(e Entry) Entry {
	if e.Data == nil {
		return Entry{Index: e.Index, Term: e.Term, Data: nil, Type: e.Type}
	}
	d := make([]byte, len(e.Data))
	copy(d, e.Data)
	return Entry{Index: e.Index, Term: e.Term, Data: d, Type: e.Type}
}
