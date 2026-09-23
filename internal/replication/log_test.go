package replication

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// ent builds an entry with data derived from its index/term, so a test can assert
// it read back the entry it expected.
func ent(index, term uint64) Entry {
	return Entry{Index: index, Term: term, Data: []byte(fmt.Sprintf("cmd@%d.%d", index, term))}
}

// mustAppend appends and fails the test on error.
func mustAppend(t *testing.T, l *MemoryLog, entries ...Entry) {
	t.Helper()
	if err := l.Append(entries...); err != nil {
		t.Fatalf("Append(%v): %v", entries, err)
	}
}

func TestEmptyLog(t *testing.T) {
	l := NewMemoryLog()
	if l.FirstIndex() != 1 {
		t.Errorf("FirstIndex() = %d, want 1", l.FirstIndex())
	}
	if l.LastIndex() != 0 {
		t.Errorf("LastIndex() = %d, want 0", l.LastIndex())
	}
	if l.CommitIndex() != 0 || l.AppliedIndex() != 0 {
		t.Errorf("commit=%d applied=%d, want 0/0", l.CommitIndex(), l.AppliedIndex())
	}
	if tm, err := l.Term(0); err != nil || tm != 0 {
		t.Errorf("Term(0) = (%d,%v), want (0,nil)", tm, err)
	}
	if _, err := l.Term(1); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("Term(1) on empty = %v, want ErrOutOfRange", err)
	}
	if _, err := l.At(1); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("At(1) on empty = %v, want ErrOutOfRange", err)
	}
	// The empty half-open range [1,1) is valid and yields nothing.
	es, err := l.Slice(1, 1)
	if err != nil || len(es) != 0 {
		t.Errorf("Slice(1,1) = (%v,%v), want empty, nil", es, err)
	}
	if es, err := l.Unapplied(); err != nil || len(es) != 0 {
		t.Errorf("Unapplied() on empty = (%v,%v), want empty, nil", es, err)
	}
}

func TestSingleAppend(t *testing.T) {
	l := NewMemoryLog()
	mustAppend(t, l, ent(1, 1))
	if l.LastIndex() != 1 {
		t.Fatalf("LastIndex() = %d, want 1", l.LastIndex())
	}
	if tm, err := l.Term(1); err != nil || tm != 1 {
		t.Fatalf("Term(1) = (%d,%v), want (1,nil)", tm, err)
	}
	got, err := l.At(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != 1 || got.Term != 1 || !bytes.Equal(got.Data, ent(1, 1).Data) {
		t.Fatalf("At(1) = %+v, want the appended entry", got)
	}
}

func TestSequentialAppendAndRangeReads(t *testing.T) {
	l := NewMemoryLog()
	// Append across two terms, one batch then singles.
	mustAppend(t, l, ent(1, 1), ent(2, 1))
	mustAppend(t, l, ent(3, 2))
	mustAppend(t, l, ent(4, 2), ent(5, 3))
	if l.LastIndex() != 5 {
		t.Fatalf("LastIndex() = %d, want 5", l.LastIndex())
	}
	// Term lookups across the whole log, including the 0 sentinel.
	for idx, want := range map[uint64]uint64{0: 0, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3} {
		if tm, err := l.Term(idx); err != nil || tm != want {
			t.Errorf("Term(%d) = (%d,%v), want %d", idx, tm, err, want)
		}
	}
	// A mid-log half-open slice returns exactly [2,4): indexes 2 and 3.
	es, err := l.Slice(2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 2 || es[0].Index != 2 || es[1].Index != 3 {
		t.Fatalf("Slice(2,4) = %v, want indexes [2,3]", es)
	}
	// The full log is [1, LastIndex+1).
	all, err := l.Slice(1, l.LastIndex()+1)
	if err != nil || len(all) != 5 {
		t.Fatalf("Slice(1,6) = (%v,%v), want 5 entries", all, err)
	}
}

func TestInvalidIndexAndRanges(t *testing.T) {
	l := NewMemoryLog()
	mustAppend(t, l, ent(1, 1), ent(2, 1), ent(3, 1))

	for _, idx := range []uint64{4, 5, 100} {
		if _, err := l.At(idx); !errors.Is(err, ErrOutOfRange) {
			t.Errorf("At(%d) = %v, want ErrOutOfRange", idx, err)
		}
		if _, err := l.Term(idx); !errors.Is(err, ErrOutOfRange) {
			t.Errorf("Term(%d) = %v, want ErrOutOfRange", idx, err)
		}
	}
	if _, err := l.At(0); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("At(0) = %v, want ErrOutOfRange (0 is the sentinel, not an entry)", err)
	}
	badRanges := [][2]uint64{
		{0, 2},   // lo below 1
		{3, 2},   // hi < lo
		{1, 5},   // hi past LastIndex+1 (4)
		{2, 100}, // hi way past the end
	}
	for _, r := range badRanges {
		if _, err := l.Slice(r[0], r[1]); !errors.Is(err, ErrOutOfRange) {
			t.Errorf("Slice(%d,%d) = %v, want ErrOutOfRange", r[0], r[1], err)
		}
	}
	// [LastIndex+1, LastIndex+1) is the valid empty range at the end.
	if es, err := l.Slice(4, 4); err != nil || len(es) != 0 {
		t.Errorf("Slice(4,4) = (%v,%v), want empty, nil", es, err)
	}
}

func TestAppendRejectsGapDuplicateAndTermRegression(t *testing.T) {
	l := NewMemoryLog()
	mustAppend(t, l, ent(1, 5), ent(2, 5))

	// A gap: next index must be 3.
	if err := l.Append(ent(4, 5)); !errors.Is(err, ErrNonContiguous) {
		t.Errorf("Append(gap) = %v, want ErrNonContiguous", err)
	}
	// A duplicate/backward index.
	if err := l.Append(ent(2, 5)); !errors.Is(err, ErrNonContiguous) {
		t.Errorf("Append(duplicate) = %v, want ErrNonContiguous", err)
	}
	// A non-contiguous batch (1,3 skips 2 relative to start).
	if err := l.Append(ent(3, 5), ent(5, 5)); !errors.Is(err, ErrNonContiguous) {
		t.Errorf("Append(non-contiguous batch) = %v, want ErrNonContiguous", err)
	}
	// A term regression: index 3 with a lower term than index 2.
	if err := l.Append(ent(3, 4)); !errors.Is(err, ErrTermRegression) {
		t.Errorf("Append(term regression) = %v, want ErrTermRegression", err)
	}
	// An empty batch.
	if err := l.Append(); !errors.Is(err, ErrEmptyBatch) {
		t.Errorf("Append() = %v, want ErrEmptyBatch", err)
	}
	// After all those rejections the log is unchanged (rejected op has no effect).
	if l.LastIndex() != 2 {
		t.Fatalf("LastIndex() = %d after rejected appends, want 2", l.LastIndex())
	}
	if tm, _ := l.Term(2); tm != 5 {
		t.Fatalf("Term(2) = %d, want 5 (log mutated by a rejected op)", tm)
	}
}

func TestSuffixReplacementVariants(t *testing.T) {
	build := func() *MemoryLog {
		l := NewMemoryLog()
		mustAppend(t, l, ent(1, 1), ent(2, 1), ent(3, 2), ent(4, 2))
		return l
	}

	t.Run("replace full suffix from index 1", func(t *testing.T) {
		l := build()
		if err := l.TruncateAndAppend(ent(1, 3), ent(2, 3)); err != nil {
			t.Fatal(err)
		}
		if l.LastIndex() != 2 {
			t.Fatalf("LastIndex() = %d, want 2", l.LastIndex())
		}
		if tm, _ := l.Term(1); tm != 3 {
			t.Fatalf("Term(1) = %d, want 3", tm)
		}
	})

	t.Run("replace one-entry suffix", func(t *testing.T) {
		l := build()
		if err := l.TruncateAndAppend(ent(4, 3)); err != nil {
			t.Fatal(err)
		}
		if l.LastIndex() != 4 {
			t.Fatalf("LastIndex() = %d, want 4", l.LastIndex())
		}
		if tm, _ := l.Term(4); tm != 3 {
			t.Fatalf("Term(4) = %d, want 3", tm)
		}
	})

	t.Run("replace suffix after a retained prefix", func(t *testing.T) {
		l := build()
		// Retain [1,2], drop [3,4], append a longer replacement.
		if err := l.TruncateAndAppend(ent(3, 4), ent(4, 4), ent(5, 4)); err != nil {
			t.Fatal(err)
		}
		if l.LastIndex() != 5 {
			t.Fatalf("LastIndex() = %d, want 5", l.LastIndex())
		}
		if tm, _ := l.Term(2); tm != 1 {
			t.Fatalf("prefix Term(2) = %d, want 1 (prefix must be retained)", tm)
		}
		if tm, _ := l.Term(5); tm != 4 {
			t.Fatalf("Term(5) = %d, want 4", tm)
		}
	})

	t.Run("at the end is a pure append", func(t *testing.T) {
		l := build()
		if err := l.TruncateAndAppend(ent(5, 2)); err != nil {
			t.Fatal(err)
		}
		if l.LastIndex() != 5 {
			t.Fatalf("LastIndex() = %d, want 5", l.LastIndex())
		}
	})
}

func TestSuffixReplacementRejections(t *testing.T) {
	l := NewMemoryLog()
	mustAppend(t, l, ent(1, 1), ent(2, 1), ent(3, 2), ent(4, 2))

	// A gap: starting past LastIndex+1 (5) would leave a hole.
	if err := l.TruncateAndAppend(ent(6, 3)); !errors.Is(err, ErrNonContiguous) {
		t.Errorf("TruncateAndAppend(gap) = %v, want ErrNonContiguous", err)
	}
	// Non-contiguous within the batch.
	if err := l.TruncateAndAppend(ent(3, 3), ent(5, 3)); !errors.Is(err, ErrNonContiguous) {
		t.Errorf("TruncateAndAppend(non-contiguous batch) = %v, want ErrNonContiguous", err)
	}
	// A term regression against the retained prefix (index 2 has term 1; can't
	// replace [3..] with a lower term than the prefix's last term — here term 0).
	if err := l.TruncateAndAppend(ent(3, 0)); !errors.Is(err, ErrTermRegression) {
		t.Errorf("TruncateAndAppend(term regression) = %v, want ErrTermRegression", err)
	}
	// Empty batch.
	if err := l.TruncateAndAppend(); !errors.Is(err, ErrEmptyBatch) {
		t.Errorf("TruncateAndAppend() = %v, want ErrEmptyBatch", err)
	}
	// The log is untouched by every rejection.
	if l.LastIndex() != 4 {
		t.Fatalf("LastIndex() = %d after rejected replacements, want 4", l.LastIndex())
	}
}

// TestCannotReplaceCommittedEntry proves a committed entry is never overwritten,
// while an uncommitted suffix above commitIndex still can be (INV-P3).
func TestCannotReplaceCommittedEntry(t *testing.T) {
	l := NewMemoryLog()
	mustAppend(t, l, ent(1, 1), ent(2, 1), ent(3, 1), ent(4, 1))
	if err := l.Commit(2); err != nil {
		t.Fatal(err)
	}
	// f == commitIndex is forbidden.
	if err := l.TruncateAndAppend(ent(2, 2)); !errors.Is(err, ErrTruncateCommitted) {
		t.Errorf("replace at commitIndex = %v, want ErrTruncateCommitted", err)
	}
	// f below commitIndex is forbidden.
	if err := l.TruncateAndAppend(ent(1, 2)); !errors.Is(err, ErrTruncateCommitted) {
		t.Errorf("replace below commitIndex = %v, want ErrTruncateCommitted", err)
	}
	// f == commitIndex+1 is allowed: the uncommitted suffix may be rewritten.
	if err := l.TruncateAndAppend(ent(3, 2)); err != nil {
		t.Fatalf("replace above commitIndex: %v", err)
	}
	if l.LastIndex() != 3 {
		t.Fatalf("LastIndex() = %d, want 3", l.LastIndex())
	}
	// commit <= lastIndex still holds after truncation (INV-P6).
	if l.CommitIndex() > l.LastIndex() {
		t.Fatalf("commit %d > lastIndex %d after truncation", l.CommitIndex(), l.LastIndex())
	}
}

// TestAppendCopiesInput proves stored bytes are not aliased to caller memory
// (INV-P4): mutating the caller's Data after Append cannot change the log.
func TestAppendCopiesInput(t *testing.T) {
	l := NewMemoryLog()
	data := []byte("original")
	if err := l.Append(Entry{Index: 1, Term: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] = 'X' // scribble over the caller's buffer
	}
	got, err := l.At(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != "original" {
		t.Fatalf("At(1).Data = %q, log aliased the caller's slice", got.Data)
	}
}

// TestReadsReturnCopies proves returned bytes never expose internal storage
// (INV-P4): mutating a returned entry cannot change the log or another read.
func TestReadsReturnCopies(t *testing.T) {
	l := NewMemoryLog()
	mustAppend(t, l, Entry{Index: 1, Term: 1, Data: []byte("stored")})

	got, err := l.At(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range got.Data {
		got.Data[i] = 'Z'
	}
	again, _ := l.At(1)
	if string(again.Data) != "stored" {
		t.Fatalf("At(1).Data = %q after mutating a prior read; storage was exposed", again.Data)
	}
	// Slice returns copies too.
	es, _ := l.Slice(1, 2)
	for i := range es[0].Data {
		es[0].Data[i] = 'Q'
	}
	final, _ := l.At(1)
	if string(final.Data) != "stored" {
		t.Fatalf("Slice exposed internal storage; At(1).Data = %q", final.Data)
	}
}

// TestEmptyAndNilDataArePreserved checks the copy path keeps nil nil and empty
// empty rather than collapsing them.
func TestEmptyAndNilDataArePreserved(t *testing.T) {
	l := NewMemoryLog()
	mustAppend(t, l, Entry{Index: 1, Term: 1, Data: nil})
	mustAppend(t, l, Entry{Index: 2, Term: 1, Data: []byte{}})
	a, _ := l.At(1)
	if a.Data != nil {
		t.Errorf("nil Data became %v", a.Data)
	}
	b, _ := l.At(2)
	if b.Data == nil || len(b.Data) != 0 {
		t.Errorf("empty Data became %v", b.Data)
	}
}
