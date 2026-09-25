package lincheck

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
)

// mustParse parses a history literal or fails the test.
func mustParse(t *testing.T, text string) History {
	t.Helper()
	h, err := ParseHistory(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := h.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return h
}

func requireOK(t *testing.T, h History) {
	t.Helper()
	if r := Check(h, Options{}); !r.OK {
		t.Fatalf("history should be linearizable:\n%s\n%s", h, r.Reason)
	}
}

func requireBad(t *testing.T, h History) Result {
	t.Helper()
	r := Check(h, Options{Minimize: true})
	if r.OK || r.Unchecked {
		t.Fatalf("history should NOT be linearizable (ok=%v unchecked=%v):\n%s", r.OK, r.Unchecked, h)
	}
	if r.Reason == "" || len(r.Counterexample) == 0 {
		t.Fatalf("a failing check must explain itself: %+v", r)
	}
	return r
}

// --- known-good histories ---

// TestSequentialBaseline is scenario A: one client, put/get/put/get/delete/get.
func TestSequentialBaseline(t *testing.T) {
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c1 get "k" 3 4 ok output="A"
3 c1 put "k" 5 6 ok value="B"
4 c1 get "k" 7 8 ok output="B"
5 c1 delete "k" 9 10 ok
6 c1 get "k" 11 12 notfound
`))
}

// TestConcurrentIndependentKeys is scenario B: locality — keys check separately.
func TestConcurrentIndependentKeys(t *testing.T) {
	requireOK(t, mustParse(t, `
1 c1 put "a" 1 5 ok value="A1"
2 c2 put "b" 2 6 ok value="B1"
3 c1 get "a" 7 9 ok output="A1"
4 c2 get "b" 8 10 ok output="B1"
`))
}

// TestOverlappingWriteAndReadMayOrderEitherWay is scenario D: a read overlapping a
// write may see the old or the new value.
func TestOverlappingWriteAndReadMayOrderEitherWay(t *testing.T) {
	old := `
1 c1 put "k" 1 2 ok value="old"
2 c1 put "k" 3 8 ok value="new"
3 c2 get "k" 4 6 ok output="old"
`
	new := `
1 c1 put "k" 1 2 ok value="old"
2 c1 put "k" 3 8 ok value="new"
3 c2 get "k" 4 6 ok output="new"
`
	requireOK(t, mustParse(t, old))
	requireOK(t, mustParse(t, new))
}

// TestConcurrentSameKeyWrites is scenario C: either overwrite order is legal, and
// the reads afterwards pin which one happened.
func TestConcurrentSameKeyWrites(t *testing.T) {
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 5 ok value="X"
2 c2 put "k" 2 6 ok value="Y"
3 c3 get "k" 7 8 ok output="X"
4 c3 get "k" 9 10 ok output="X"
`))
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 5 ok value="X"
2 c2 put "k" 2 6 ok value="Y"
3 c3 get "k" 7 8 ok output="Y"
`))
}

// TestDeleteWriteOverlap is scenario E.
func TestDeleteWriteOverlap(t *testing.T) {
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c1 delete "k" 3 7 ok
3 c2 put "k" 4 8 ok value="B"
4 c3 get "k" 9 10 notfound
`))
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c1 delete "k" 3 7 ok
3 c2 put "k" 4 8 ok value="B"
4 c3 get "k" 9 10 ok output="B"
`))
}

// TestIncompleteWriteMayOrMayNotHaveHappened: an unanswered put may be observed
// (it took effect) or never observed (it did not) — both histories are legal.
func TestIncompleteWriteMayOrMayNotHaveHappened(t *testing.T) {
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 put "k" 3 0 incomplete value="B"
3 c3 get "k" 4 5 ok output="B"
4 c3 get "k" 6 7 ok output="B"
`))
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 put "k" 3 0 incomplete value="B"
3 c3 get "k" 4 5 ok output="A"
4 c3 get "k" 6 7 ok output="A"
`))
	// It may even take effect late — after reads that did not see it.
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 put "k" 3 0 incomplete value="B"
3 c3 get "k" 4 5 ok output="A"
4 c3 get "k" 6 7 ok output="B"
`))
}

// TestRejectedOpsAreExcluded: a definite rejection has no effect on the register.
func TestRejectedOpsAreExcluded(t *testing.T) {
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 put "k" 3 4 rejected value="B"
3 c3 get "k" 5 6 ok output="A"
`))
}

// TestIncompleteReadConstrainsNothing.
func TestIncompleteReadConstrainsNothing(t *testing.T) {
	requireOK(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 get "k" 3 0 incomplete
3 c1 put "k" 4 5 ok value="B"
4 c3 get "k" 6 7 ok output="B"
`))
}

// --- known-bad histories ---

// TestReadAfterCompletedWriteMustSeeIt is scenario F, tested directly: the write
// completed at 2, the read was invoked at 3, so the read cannot linearize before
// the write and must not return the old value.
func TestReadAfterCompletedWriteMustSeeIt(t *testing.T) {
	r := requireBad(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="old"
2 c1 put "k" 3 4 ok value="new"
3 c2 get "k" 5 6 ok output="old"
`))
	if !strings.Contains(r.Reason, `returned "old"`) {
		t.Fatalf("reason should name the stale read:\n%s", r.Reason)
	}
}

// TestReadOfNeverWrittenValue.
func TestReadOfNeverWrittenValue(t *testing.T) {
	requireBad(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 get "k" 3 4 ok output="Z"
`))
}

// TestStaleReadAfterCompletedDelete.
func TestStaleReadAfterCompletedDelete(t *testing.T) {
	requireBad(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c1 delete "k" 3 4 ok
3 c2 get "k" 5 6 ok output="A"
`))
}

// TestReadsCannotGoBackwards: two reads in real-time order that observe two
// completed writes in the reverse of any order consistent with a third read.
func TestReadsCannotGoBackwards(t *testing.T) {
	requireBad(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c1 put "k" 3 4 ok value="B"
3 c2 get "k" 5 6 ok output="B"
4 c2 get "k" 7 8 ok output="A"
`))
}

// TestRejectedWriteObservedIsAViolation: the server said the write had no
// effect; a read that sees its value proves the claim false.
func TestRejectedWriteObservedIsAViolation(t *testing.T) {
	requireBad(t, mustParse(t, `
1 c1 put "k" 1 2 rejected value="A"
2 c2 get "k" 3 4 ok output="A"
`))
}

// TestNotFoundAfterCompletedWriteIsAViolation.
func TestNotFoundAfterCompletedWriteIsAViolation(t *testing.T) {
	requireBad(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 get "k" 3 4 notfound
`))
}

// TestConcurrentWritesThenReadsMustAgreeOnOneOrder is scenario G: after two
// concurrent writes complete, reads may see either winner, but not both.
func TestConcurrentWritesThenReadsMustAgreeOnOneOrder(t *testing.T) {
	requireBad(t, mustParse(t, `
1 c1 put "k" 1 5 ok value="X"
2 c2 put "k" 2 6 ok value="Y"
3 c3 get "k" 7 8 ok output="X"
4 c3 get "k" 9 10 ok output="Y"
`))
}

// --- the checker against the brute-force oracle ---

// genHistory generates a history that is linearizable BY CONSTRUCTION, with
// real concurrency: every op is invoked at one position, takes effect at a
// hidden point some time later, and completes at a later position still — so
// ops from different clients overlap, and reads return what the register held
// at their point. With dropPct > 0 some ops never complete (Incomplete): half
// of those still took effect (a client that never hears back can still have
// written), half never did.
func genHistory(rng *rand.Rand, clients, keys, ops int, dropPct int) History {
	type pend struct {
		op                Op
		point, completeAt int64
		done              bool
	}
	var seq int64
	var h History
	state := map[string]State{}
	var inflight []*pend
	busy := map[string]bool{}
	id := 0
	spread := 1 + rng.Intn(12)
	for id < ops || len(inflight) > 0 {
		if id < ops && rng.Intn(100) < 70 {
			c := fmt.Sprintf("c%d", rng.Intn(clients))
			if !busy[c] {
				seq++
				id++
				op := Op{ID: id, Client: c, Key: fmt.Sprintf("k%d", rng.Intn(keys)), Invoke: seq}
				switch rng.Intn(3) {
				case 0:
					op.Kind, op.Value = Put, []byte(fmt.Sprintf("v%d", id))
				case 1:
					op.Kind = Get
				default:
					op.Kind = Delete
				}
				busy[c] = true
				p := &pend{op: op, point: seq + 1 + int64(rng.Intn(spread)), completeAt: 0}
				p.completeAt = p.point + int64(rng.Intn(spread))
				if rng.Intn(100) < dropPct {
					p.completeAt = -1 // never completes
					if rng.Intn(2) == 0 {
						p.point = -1 // and never took effect
					}
				}
				inflight = append(inflight, p)
			}
		}
		seq++
		// linearize every op whose point has come, in point order
		for {
			best := -1
			for i, p := range inflight {
				if !p.done && p.point >= 0 && p.point <= seq && (best < 0 || p.point < inflight[best].point) {
					best = i
				}
			}
			if best < 0 {
				break
			}
			p := inflight[best]
			p.done = true
			s := state[p.op.Key]
			switch p.op.Kind {
			case Put:
				state[p.op.Key] = State{Present: true, Value: string(p.op.Value)}
				p.op.Outcome = OK
			case Delete:
				state[p.op.Key] = State{}
				p.op.Outcome = OK
			case Get:
				if s.Present {
					p.op.Outcome, p.op.Output = OK, []byte(s.Value)
				} else {
					p.op.Outcome = NotFound
				}
			}
		}
		// complete (or abandon) what is due
		rest := inflight[:0]
		for _, p := range inflight {
			switch {
			case p.completeAt < 0 && (p.done || p.point < 0):
				p.op.Outcome, p.op.Complete, p.op.Output = Incomplete, 0, nil
				h.Ops = append(h.Ops, p.op)
				busy[p.op.Client] = false
			case p.done && p.completeAt >= 0 && p.completeAt <= seq:
				seq++
				p.op.Complete = seq
				h.Ops = append(h.Ops, p.op)
				busy[p.op.Client] = false
			default:
				rest = append(rest, p)
			}
		}
		inflight = rest
	}
	return h
}

// perturb changes one observation, producing a history that may or may not be
// linearizable — the oracle decides.
func perturb(rng *rand.Rand, h History) History {
	out := History{Ops: append([]Op(nil), h.Ops...)}
	var gets []int
	for i, op := range out.Ops {
		if op.Kind == Get && op.Outcome != Incomplete {
			gets = append(gets, i)
		}
	}
	if len(gets) == 0 {
		return out
	}
	i := gets[rng.Intn(len(gets))]
	var values []string
	for _, op := range out.Ops {
		if op.Kind == Put && op.Key == out.Ops[i].Key {
			values = append(values, string(op.Value))
		}
	}
	if len(values) == 0 || rng.Intn(3) == 0 {
		out.Ops[i].Outcome, out.Ops[i].Output = NotFound, nil
		return out
	}
	out.Ops[i].Outcome, out.Ops[i].Output = OK, []byte(values[rng.Intn(len(values))])
	return out
}

// TestCheckerAgreesWithBruteForce: on thousands of small generated histories —
// linearizable by construction, and perturbed ones of either kind — the search
// checker returns exactly what exhaustive enumeration returns.
func TestCheckerAgreesWithBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(12))
	var good, bad int
	for i := 0; i < 3000; i++ {
		h := genHistory(rng, 1+rng.Intn(3), 1+rng.Intn(2), 3+rng.Intn(6), rng.Intn(30))
		if err := h.Validate(); err != nil {
			t.Fatalf("generated history invalid: %v\n%s", err, h)
		}
		if r := Check(h, Options{}); !r.OK {
			t.Fatalf("a history linearizable by construction was rejected:\n%s\n%s", h, r.Reason)
		}
		p := perturb(rng, h)
		for _, key := range p.Keys() {
			ops := p.ForKey(key)
			want := BruteForce(ops)
			got := checkKey(ops, DefaultMaxStates)
			if got.budget || got.ok != want {
				t.Fatalf("checker=%v brute=%v (budget=%v) on key %s:\n%s", got.ok, want, got.budget, key, Format(ops))
			}
			if want {
				good++
			} else {
				bad++
			}
		}
	}
	if bad < 100 || good < 100 {
		t.Fatalf("the corpus is not balanced enough to mean anything: good=%d bad=%d", good, bad)
	}
	t.Logf("perturbed keys: %d linearizable, %d not", good, bad)
}

// TestGeneratedHistoriesAtScaleAreAccepted: larger linearizable-by-construction
// histories (8 clients, hundreds of ops per key, incomplete ops included) are
// accepted within the default budget, and the cost is reported.
func TestGeneratedHistoriesAtScaleAreAccepted(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, n := range []int{50, 200, 800} {
		h := genHistory(rng, 8, 2, n, 10)
		r := Check(h, Options{})
		if !r.OK {
			t.Fatalf("%d ops: rejected:\n%s", n, r.Reason)
		}
		t.Logf("%d ops over %d keys (%d complete, %d optional): %d states, %d memo hits, %s",
			r.Stats.Ops, r.Stats.Keys, r.Stats.Complete, r.Stats.Optional, r.Stats.States, r.Stats.MemoHits, r.Stats.Duration)
	}
}

// --- minimization ---

// TestMinimizeShrinksToTheViolation: a 200-op history with one stale read
// minimizes to a few ops that still fail, still contain the stale read, and
// still contain the writes whose values the remaining reads return.
func TestMinimizeShrinksToTheViolation(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	h := genHistory(rng, 4, 1, 200, 0)
	// Inject: the last completed read returns the value of the first put, which
	// many later completed writes have long since overwritten.
	var firstPut, lastGet = -1, -1
	for i, op := range h.Ops {
		if op.Kind == Put && firstPut < 0 {
			firstPut = i
		}
		if op.Kind == Get && op.Outcome != Incomplete {
			lastGet = i
		}
	}
	h.Ops[lastGet].Outcome, h.Ops[lastGet].Output = OK, h.Ops[firstPut].Value
	r := Check(h, Options{Minimize: true})
	if r.OK {
		t.Fatal("the injected stale read was not detected")
	}
	if len(r.Counterexample) >= 20 {
		t.Fatalf("counterexample not minimized: %d ops of %d", len(r.Counterexample), len(h.Ops))
	}
	if kr := checkKey(r.Counterexample, DefaultMaxStates); kr.ok {
		t.Fatal("the minimized counterexample is linearizable")
	}
	found := false
	for _, op := range r.Counterexample {
		if op.ID == h.Ops[lastGet].ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("the minimized counterexample lost the stale read:\n%s", Format(r.Counterexample))
	}
	if !producersPresent(r.Counterexample) {
		t.Fatalf("the minimized counterexample reads a value nothing in it wrote:\n%s", Format(r.Counterexample))
	}
	// Anchored: without the stale read, what remains is linearizable.
	var rest []Op
	for _, op := range r.Counterexample {
		if op.ID != h.Ops[lastGet].ID {
			rest = append(rest, op)
		}
	}
	if kr := checkKey(rest, DefaultMaxStates); !kr.ok {
		t.Fatalf("the counterexample fails for a reason other than the stale read:\n%s", Format(rest))
	}
	t.Logf("minimized %d -> %d ops\n%s\n%s", len(h.Ops), len(r.Counterexample), Format(r.Counterexample), r.Reason)
}

// TestMinimizeNeverInventsAViolation: a Delete that a NotFound relies on is kept,
// because dropping it would manufacture a failure the system never produced.
func TestMinimizeNeverInventsAViolation(t *testing.T) {
	h := mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c1 delete "k" 3 4 ok
3 c2 get "k" 5 6 notfound
4 c1 put "k" 7 8 ok value="B"
5 c2 get "k" 9 10 ok output="A"
`)
	r := requireBad(t, h)
	ids := map[int]bool{}
	for _, op := range r.Counterexample {
		ids[op.ID] = true
	}
	// The stale read and the write whose value it saw must stay; the write it
	// missed may be the Delete or the Put(B) — either is the same violation.
	if !ids[5] || !ids[1] || !(ids[2] || ids[4]) || len(r.Counterexample) != 3 {
		t.Fatalf("want the stale read, the write it saw and one write it missed:\n%s", Format(r.Counterexample))
	}
	if ids[3] && !ids[2] {
		t.Fatalf("the counterexample keeps a NotFound but dropped the Delete that explains it:\n%s", Format(r.Counterexample))
	}
	var rest []Op
	for _, op := range r.Counterexample {
		if op.ID != 5 {
			rest = append(rest, op)
		}
	}
	if kr := checkKey(rest, DefaultMaxStates); !kr.ok {
		t.Fatalf("without the stale read the counterexample must be linearizable:\n%s", Format(rest))
	}
	// And the rejected-write case keeps the rejected write as evidence.
	r = requireBad(t, mustParse(t, `
1 c1 put "k" 1 2 ok value="A"
2 c2 put "k" 3 4 rejected value="B"
3 c1 put "k" 5 6 ok value="C"
4 c3 get "k" 7 8 ok output="B"
`))
	ids = map[int]bool{}
	for _, op := range r.Counterexample {
		ids[op.ID] = true
	}
	if !ids[2] || !ids[4] || len(r.Counterexample) != 2 {
		t.Fatalf("want exactly the rejected write and the read that saw it:\n%s", Format(r.Counterexample))
	}
}

// TestNonLinearizableAtScaleIsBounded measures the expensive case: proving that
// NO linearization exists needs the search to exhaust, so this is where the
// memoization has to earn its keep. Eight clients, one hot key, hundreds of
// overlapping ops, one stale read injected near the end.
func TestNonLinearizableAtScaleIsBounded(t *testing.T) {
	rng := rand.New(rand.NewSource(21))
	for _, n := range []int{100, 300, 600} {
		h := genHistory(rng, 8, 1, n, 15)
		// W is the earliest-completing put; P a put invoked after W completed (so
		// P follows W in every linearization); G a read invoked after P completed.
		// G returning W's unique value is then impossible: some write after W
		// (P at least) precedes G.
		var puts []int
		for i, op := range h.Ops {
			if op.Kind == Put && op.Outcome == OK {
				puts = append(puts, i)
			}
		}
		sort.Slice(puts, func(a, b int) bool { return h.Ops[puts[a]].Complete < h.Ops[puts[b]].Complete })
		if len(puts) < 2 {
			t.Fatalf("%d ops: too few puts", n)
		}
		w := puts[0]
		p, g := -1, -1
		for _, i := range puts {
			if h.Ops[i].Invoke > h.Ops[w].Complete {
				p = i
				break
			}
		}
		if p < 0 {
			t.Fatalf("%d ops: no put after the first one completed", n)
		}
		for i, op := range h.Ops {
			if op.Kind == Get && op.Outcome != Incomplete && op.Invoke > h.Ops[p].Complete && (g < 0 || op.Invoke < h.Ops[g].Invoke) {
				g = i
			}
		}
		if g < 0 {
			t.Fatalf("%d ops: no read after the second put completed", n)
		}
		h.Ops[g].Outcome, h.Ops[g].Output = OK, h.Ops[w].Value
		r := Check(h, Options{Minimize: true})
		if r.OK {
			t.Fatalf("%d ops: the injected stale read was not detected", n)
		}
		if r.Unchecked {
			t.Fatalf("%d ops: exceeded the default budget (%d states)", n, r.Stats.States)
		}
		t.Logf("%d ops (%d complete, %d optional): %d states, %d memo hits, %s; counterexample %d ops",
			r.Stats.Ops, r.Stats.Complete, r.Stats.Optional, r.Stats.States, r.Stats.MemoHits, r.Stats.Duration, len(r.Counterexample))
	}
}

// --- budget, validation, serialization, recorder ---

// TestBudgetReportsUncheckedNotLinearizable: an exhausted search is never an OK.
func TestBudgetReportsUncheckedNotLinearizable(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	h := genHistory(rng, 8, 1, 400, 20)
	r := Check(h, Options{MaxStates: 50})
	if r.OK || !r.Unchecked {
		t.Fatalf("want unchecked with a 50-state budget, got ok=%v unchecked=%v (%d states)", r.OK, r.Unchecked, r.Stats.States)
	}
	if !strings.Contains(r.Reason, "UNCHECKED") {
		t.Fatalf("reason must say the history is unchecked: %s", r.Reason)
	}
}

func TestValidateRejectsMalformedHistories(t *testing.T) {
	bad := []string{
		`1 c1 put "k" 5 3 ok value="A"`,                                  // completed before invoked
		`1 c1 put "k" 1 0 ok value="A"`,                                  // ok without completion
		`1 c1 put "k" 1 2 notfound value="A"`,                            // notfound on a put
		`1 c1 put "k" 1 2 ok value="A" output="x"`,                       // output on a put
		"1 c1 put \"k\" 1 2 ok value=\"A\"\n2 c2 get \"k\" 2 3 notfound", // shared position
	}
	for _, text := range bad {
		h, err := ParseHistory(text)
		if err == nil {
			err = h.Validate()
		}
		if err == nil {
			t.Fatalf("accepted malformed history:\n%s", text)
		}
	}
	h, _ := ParseHistory(`1 c1 put "k" 5 3 ok value="A"`)
	if r := Check(h, Options{}); r.OK {
		t.Fatal("an invalid history must never check as linearizable")
	}
}

func TestParseFormatRoundTrip(t *testing.T) {
	text := `1 c1 put "a b" 1 2 ok value="x\ny" node=n1 term=3 index=7
2 c2 get "a b" 3 4 ok output="x\ny" node=n1 term=3 index=7
3 c3 delete "a b" 5 6 ok
4 c1 get "a b" 7 8 notfound
5 c2 put "a b" 9 0 incomplete value=""
6 c2 put "a b" 10 11 rejected value="z"
`
	h := mustParse(t, text)
	if got := h.String(); got != text {
		t.Fatalf("round trip differs:\n%s\n---\n%s", got, text)
	}
	if h.Ops[4].Value == nil || len(h.Ops[4].Value) != 0 {
		t.Fatalf("an empty value must round-trip as an empty, present value: %#v", h.Ops[4].Value)
	}
}

// TestRecorderOrdersConcurrentClients: positions are unique and monotone in
// call order, and an op that never Ended is Incomplete in the history.
func TestRecorderOrdersConcurrentClients(t *testing.T) {
	r := NewRecorder()
	var wg sync.WaitGroup
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			name := fmt.Sprintf("c%d", c)
			for i := 0; i < 50; i++ {
				id := r.Begin(name, Put, "k", []byte("v"))
				a := r.Attempt(id, "n1")
				r.AttemptDone(id, a, true, "ok", 1)
				r.End(id, OK, nil, "n1", 1, uint64(i))
			}
		}(c)
	}
	wg.Wait()
	hanging := r.Begin("c9", Get, "k", nil)
	h := r.History()
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := h.Summary(); got.Total != 401 || got.Incomplete != 1 || got.Attempts != 400 {
		t.Fatalf("summary %+v", got)
	}
	for _, op := range h.Ops {
		if op.ID == hanging && op.Outcome != Incomplete {
			t.Fatal("an op that never Ended must be Incomplete")
		}
	}
	if r := Check(h, Options{}); !r.OK {
		t.Fatalf("a history of blind writes must be linearizable: %s", r.Reason)
	}
}

// TestSequentialReferenceModel pins the model's own semantics.
func TestSequentialReferenceModel(t *testing.T) {
	h := mustParse(t, `
1 c put "k" 1 2 ok value=""
2 c get "k" 3 4 ok output=""
3 c delete "k" 5 6 ok
4 c delete "k" 7 8 ok
5 c get "k" 9 10 notfound
6 c put "k" 11 12 ok value="v"
7 c put "k" 13 14 ok value="w"
8 c get "k" 15 16 ok output="w"
`)
	if _, bad := Sequential(h.Ops); bad != -1 {
		t.Fatalf("model contradicts op %d", h.Ops[bad].ID)
	}
	h.Ops[7].Output = []byte("v")
	if _, bad := Sequential(h.Ops); bad != 7 {
		t.Fatalf("model should contradict op 8, got %d", bad)
	}
}

func BenchmarkCheck200OpsPerKey(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	h := genHistory(rng, 8, 1, 200, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if r := Check(h, Options{}); !r.OK {
			b.Fatal(r.Reason)
		}
	}
}
