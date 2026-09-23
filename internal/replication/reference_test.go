package replication

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// refLog is an independent, deliberately naive reference implementation of the
// log semantics (docs/REPLICATION.md §4–§7). The differential test drives the
// same operation sequence through it and through MemoryLog and asserts they never
// diverge — a cross-check that the real implementation matches the specified
// contract rather than only its own idea of it.
type refLog struct {
	entries []Entry // slot i-1 holds index i
	commit  uint64
	applied uint64
}

func (r *refLog) last() uint64 { return uint64(len(r.entries)) }

func (r *refLog) term(i uint64) uint64 {
	if i == 0 {
		return 0
	}
	return r.entries[i-1].Term
}

// contiguousNonDecreasing reports whether a batch starting at f is contiguous and
// its terms do not fall below the retained prefix's last term.
func (r *refLog) contiguousNonDecreasing(f uint64, es []Entry) bool {
	prev := r.term(f - 1)
	for k, e := range es {
		if e.Index != f+uint64(k) || e.Term < prev {
			return false
		}
		prev = e.Term
	}
	return true
}

func (r *refLog) append(es []Entry) bool {
	if len(es) == 0 || es[0].Index != r.last()+1 || !r.contiguousNonDecreasing(r.last()+1, es) {
		return false
	}
	r.install(r.last()+1, es)
	return true
}

func (r *refLog) truncateAndAppend(es []Entry) bool {
	if len(es) == 0 {
		return false
	}
	f := es[0].Index
	if f < 1 || f > r.last()+1 || f <= r.commit || !r.contiguousNonDecreasing(f, es) {
		return false
	}
	r.install(f, es)
	return true
}

func (r *refLog) install(f uint64, es []Entry) {
	kept := append([]Entry(nil), r.entries[:f-1]...)
	for _, e := range es {
		kept = append(kept, copyEntry(e))
	}
	r.entries = kept
}

func (r *refLog) doCommit(i uint64) bool {
	if i < r.commit || i > r.last() {
		return false
	}
	r.commit = i
	return true
}

func (r *refLog) doApply(through uint64) bool {
	if through < r.applied || through > r.commit {
		return false
	}
	r.applied = through
	return true
}

// TestAgainstReferenceModel drives a large deterministic sequence of valid and
// invalid operations through MemoryLog and refLog and compares them after every
// step: the two must agree on whether each op is legal, and on the full
// observable state after it. It also asserts the core invariants hold after
// every step (INV-P2, P6, P8).
func TestAgainstReferenceModel(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			impl := NewMemoryLog()
			ref := &refLog{}
			term := uint64(1)

			for step := 0; step < 400; step++ {
				switch rng.Intn(5) {
				case 0, 1: // append at the end (mostly valid)
					if rng.Intn(6) == 0 {
						term++ // occasionally bump the term
					}
					n := 1 + rng.Intn(3)
					es := makeBatch(impl.LastIndex()+1, term, n)
					assertSameOutcome(t, step, "append",
						impl.Append(es...), ref.append(es), impl, ref)
				case 2: // truncateAndAppend, f possibly illegal
					f := 1 + uint64(rng.Intn(int(impl.LastIndex())+3)) // may exceed last+1 or hit committed
					bt := term
					if rng.Intn(4) == 0 {
						bt++
					}
					if rng.Intn(8) == 0 && bt > 0 {
						bt-- // sometimes force a term regression
					}
					n := 1 + rng.Intn(3)
					es := makeBatch(f, bt, n)
					if term < bt {
						term = bt
					}
					assertSameOutcome(t, step, "truncateAndAppend",
						impl.TruncateAndAppend(es...), ref.truncateAndAppend(es), impl, ref)
				case 3: // commit, target possibly illegal
					target := uint64(rng.Intn(int(impl.LastIndex()) + 2))
					assertSameOutcome(t, step, "commit",
						impl.Commit(target), ref.doCommit(target), impl, ref)
				case 4: // apply, target possibly illegal
					target := uint64(rng.Intn(int(impl.LastIndex()) + 2))
					assertSameOutcome(t, step, "apply",
						impl.Apply(target), ref.doApply(target), impl, ref)
				}
				assertInvariants(t, step, impl)
			}
		})
	}
}

func makeBatch(start, term uint64, n int) []Entry {
	es := make([]Entry, n)
	for k := range es {
		idx := start + uint64(k)
		es[k] = Entry{Index: idx, Term: term, Data: []byte(fmt.Sprintf("s%d.t%d", idx, term))}
	}
	return es
}

// assertSameOutcome asserts impl and ref agreed on legality, and if the op
// succeeded, that their full observable state matches.
func assertSameOutcome(t *testing.T, step int, op string, implErr error, refOK bool, impl *MemoryLog, ref *refLog) {
	t.Helper()
	if (implErr == nil) != refOK {
		t.Fatalf("step %d %s: impl err=%v but ref legal=%v", step, op, implErr, refOK)
	}
	if impl.LastIndex() != ref.last() {
		t.Fatalf("step %d %s: lastIndex impl=%d ref=%d", step, op, impl.LastIndex(), ref.last())
	}
	if impl.CommitIndex() != ref.commit {
		t.Fatalf("step %d %s: commit impl=%d ref=%d", step, op, impl.CommitIndex(), ref.commit)
	}
	if impl.AppliedIndex() != ref.applied {
		t.Fatalf("step %d %s: applied impl=%d ref=%d", step, op, impl.AppliedIndex(), ref.applied)
	}
	for i := uint64(1); i <= impl.LastIndex(); i++ {
		got, err := impl.At(i)
		if err != nil {
			t.Fatalf("step %d %s: At(%d): %v", step, op, i, err)
		}
		want := ref.entries[i-1]
		if got.Index != want.Index || got.Term != want.Term || !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("step %d %s: entry %d impl=%+v ref=%+v", step, op, i, got, want)
		}
	}
}

// assertInvariants pins the structural invariants that must hold at every step.
func assertInvariants(t *testing.T, step int, l *MemoryLog) {
	t.Helper()
	if l.CommitIndex() > l.LastIndex() {
		t.Fatalf("step %d: commit %d > lastIndex %d (INV-P6)", step, l.CommitIndex(), l.LastIndex())
	}
	if l.AppliedIndex() > l.CommitIndex() {
		t.Fatalf("step %d: applied %d > commit %d (INV-P8)", step, l.AppliedIndex(), l.CommitIndex())
	}
	// Contiguity and non-decreasing terms (INV-P2).
	var prevTerm uint64
	for i := uint64(1); i <= l.LastIndex(); i++ {
		e, err := l.At(i)
		if err != nil {
			t.Fatalf("step %d: At(%d): %v", step, i, err)
		}
		if e.Index != i {
			t.Fatalf("step %d: entry at slot %d has index %d (INV-P2)", step, i, e.Index)
		}
		if e.Term < prevTerm {
			t.Fatalf("step %d: term regression at index %d (%d < %d) (INV-P2)", step, i, e.Term, prevTerm)
		}
		prevTerm = e.Term
	}
}

// TestGroupIsolation constructs two independent replication models and proves a
// mutation of one cannot reach the other's log or bookkeeping. This guards
// against any accidental shared backing state between instances.
func TestGroupIsolation(t *testing.T) {
	a := NewMemoryLog()
	b := NewMemoryLog()

	mustAppend(t, a, ent(1, 1), ent(2, 1), ent(3, 1))
	mustAppend(t, b, ent(1, 1))
	if err := a.Commit(2); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(2); err != nil {
		t.Fatal(err)
	}

	// b is entirely unaffected by everything done to a.
	if b.LastIndex() != 1 || b.CommitIndex() != 0 || b.AppliedIndex() != 0 {
		t.Fatalf("b = (last %d, commit %d, applied %d), want (1,0,0) — a leaked into b",
			b.LastIndex(), b.CommitIndex(), b.AppliedIndex())
	}
	// Mutating an entry read out of a cannot change b, and truncating a leaves b.
	got, _ := a.At(1)
	for i := range got.Data {
		got.Data[i] = 'X'
	}
	if err := a.TruncateAndAppend(ent(3, 2), ent(4, 2)); err != nil {
		t.Fatal(err)
	}
	if b.LastIndex() != 1 {
		t.Fatalf("b.LastIndex() = %d after truncating a, want 1", b.LastIndex())
	}
	bEntry, _ := b.At(1)
	if string(bEntry.Data) != string(ent(1, 1).Data) {
		t.Fatalf("b's entry changed to %q; a leaked into b", bEntry.Data)
	}

	// Two replica groups built from overlapping node lists are independent values.
	ga, _ := NewReplicaGroup(0, []NodeID{"n0", "n1", "n2"}, 3)
	gb, _ := NewReplicaGroup(1, []NodeID{"n0", "n1", "n2"}, 3)
	ra := ga.Replicas()
	ra[0] = "MUTATED"
	if gb.Primary() != "n0" || ga.Primary() != "n0" {
		t.Fatalf("mutating one group's Replicas() copy affected a group (ga=%q gb=%q)", ga.Primary(), gb.Primary())
	}
}
