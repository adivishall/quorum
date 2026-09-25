package lincheck

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The checker's independent oracle (docs/LINEARIZABILITY.md §6). BruteForce
// (brute.go) is exhaustive but shares the checker's building blocks — Step,
// effective, Optional — so a bug in any of those would fool both. This oracle
// shares NOTHING with the checker: it re-states the definition from scratch on
// the WHOLE multi-key history (so it also tests the checker's use of locality),
// with its own register semantics, its own treatment of outcomes and its own
// form of the real-time rule, and enumerates orders naively. It is written from
// the definition, not from the implementation:
//
//	H is linearizable iff, after removing every op with a definite no-effect
//	(Rejected) and every unanswered read, and choosing for each unanswered
//	write whether it took effect (then its response is appended at the end of
//	the history) or not (then it is removed), there is a total order S of what
//	remains such that
//	  (1) for every pair a before b in S, b did NOT complete before a was
//	      invoked (b.complete < a.invoke is forbidden: that would put b's
//	      whole execution before a's, so b must come first); and
//	  (2) running S sequentially against a map of registers — put sets,
//	      delete clears (idempotently), get returns the value or absence, an
//	      empty value is a present value — reproduces every observed output.
//
// The checker instead walks forward ("which op may go next": invoked before
// every pending op's completion), memoizes, works per key, sorts, and stops
// when every required op is placed. The two formulations agreeing on thousands
// of arbitrary histories is the evidence that the checker is right.

type oracleOp struct {
	put, get, del bool
	key, val      string // val: the value written, or (get) the value observed
	absent        bool   // get observed absence
	inv, res      int64  // res = +inf for an unanswered write that took effect
}

// oracleLinearizable decides h by the definition above.
func oracleLinearizable(h History) bool {
	var required, pending []oracleOp
	for _, op := range h.Ops {
		o := oracleOp{key: op.Key, inv: op.Invoke, res: op.Complete}
		switch op.Kind {
		case Put:
			o.put, o.val = true, string(op.Value)
		case Get:
			o.get = true
		case Delete:
			o.del = true
		}
		switch {
		case op.Outcome == Rejected:
			continue // the system said it had no effect
		case op.Outcome == Incomplete && o.get:
			continue // an unanswered read can be given whatever answer fits
		case op.Outcome == Incomplete:
			o.res = math.MaxInt64 // took effect: answered at the end of time
			pending = append(pending, o)
			continue
		case o.get && op.Outcome == NotFound:
			o.absent = true
		case o.get:
			o.val = string(op.Output)
		}
		required = append(required, o)
	}
	for mask := 0; mask < 1<<len(pending); mask++ {
		ops := append([]oracleOp(nil), required...)
		for i, p := range pending {
			if mask&(1<<i) != 0 {
				ops = append(ops, p)
			}
		}
		if oracleOrder(ops, make([]bool, len(ops)), nil, map[string]string{}) {
			return true
		}
	}
	return false
}

// oracleOrder extends the partial order `placed` one op at a time, checking
// condition (1) against every op already placed and condition (2) on a register
// map, and backtracks. It returns true if some complete order satisfies both.
func oracleOrder(ops []oracleOp, used []bool, placed []int, regs map[string]string) bool {
	if len(placed) == len(ops) {
		return true
	}
	for i := range ops {
		if used[i] {
			continue
		}
		b := ops[i]
		ok := true
		for _, j := range placed {
			if b.res < ops[j].inv { // b finished before an earlier-placed op began
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		v, present := regs[b.key]
		var undo func()
		switch {
		case b.get:
			if b.absent == present || (present && v != b.val) {
				continue
			}
			undo = func() {}
		case b.put:
			regs[b.key] = b.val
			undo = func() { restore(regs, b.key, v, present) }
		case b.del:
			delete(regs, b.key)
			undo = func() { restore(regs, b.key, v, present) }
		}
		used[i] = true
		if oracleOrder(ops, used, append(placed, i), regs) {
			return true
		}
		used[i] = false
		undo()
	}
	return false
}

func restore(regs map[string]string, key, v string, present bool) {
	if present {
		regs[key] = v
	} else {
		delete(regs, key)
	}
}

// randomHistory builds an ARBITRARY small history — not linearizable by
// construction — so the corpus contains every kind of violation as well as
// every kind of legal concurrency: random intervals (overlapping, nested,
// disjoint), puts, gets and deletes over one or two keys, a tiny value domain
// including the empty value (so values repeat), and every outcome class:
// ok, notfound, incomplete (no completion), rejected.
func randomHistory(rng *rand.Rand, n, keys int) History {
	values := []string{"", "a", "b"}
	slots := rng.Perm(2 * n)
	var h History
	for i := 0; i < n; i++ {
		x, y := int64(slots[2*i]+1), int64(slots[2*i+1]+1)
		if x > y {
			x, y = y, x
		}
		op := Op{ID: i + 1, Client: fmt.Sprintf("c%d", i+1), Key: fmt.Sprintf("k%d", rng.Intn(keys)), Invoke: x, Complete: y}
		switch rng.Intn(3) {
		case 0:
			op.Kind, op.Value = Put, []byte(values[rng.Intn(len(values))])
		case 1:
			op.Kind = Get
		default:
			op.Kind = Delete
		}
		switch r := rng.Intn(100); {
		case r < 12:
			op.Outcome, op.Complete = Incomplete, 0
		case r < 20:
			op.Outcome = Rejected
		case op.Kind == Get && r < 50:
			op.Outcome = NotFound
		case op.Kind == Get:
			op.Outcome, op.Output = OK, []byte(values[rng.Intn(len(values))])
		default:
			op.Outcome = OK
		}
		h.Ops = append(h.Ops, op)
	}
	return h
}

// TestCheckerAgreesWithIndependentOracle is the checker's cross-validation:
// thousands of arbitrary histories of 1–7 ops over 1–2 keys, each decided by
// the checker (whole history, via locality), the independent oracle (whole
// history, from the definition) and BruteForce (per key). All three must agree
// on every one, and the corpus must be rich in BOTH verdicts and in every
// feature the spec names — or the agreement would mean nothing.
func TestCheckerAgreesWithIndependentOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20260926))
	var good, bad int
	features := map[string]int{}
	for i := 0; i < 20000; i++ {
		h := randomHistory(rng, 1+rng.Intn(7), 1+rng.Intn(2))
		if err := h.Validate(); err != nil {
			t.Fatalf("generator produced an invalid history: %v\n%s", err, h)
		}
		want := oracleLinearizable(h)
		got := Check(h, Options{})
		if got.Unchecked {
			t.Fatalf("budget exceeded on a %d-op history:\n%s", len(h.Ops), h)
		}
		if got.OK != want {
			t.Fatalf("checker=%v oracle=%v on:\n%s\nchecker reason: %s", got.OK, want, h, got.Reason)
		}
		perKey := true
		for _, key := range h.Keys() {
			bf := BruteForce(h.ForKey(key))
			if bf != oracleLinearizable(History{Ops: h.ForKey(key)}) {
				t.Fatalf("BruteForce and the oracle disagree on key %s:\n%s", key, Format(h.ForKey(key)))
			}
			perKey = perKey && bf
		}
		if perKey != want {
			t.Fatalf("locality fails: per-key verdicts %v, whole-history oracle %v:\n%s", perKey, want, h)
		}
		if want {
			good++
		} else {
			bad++
		}
		countFeatures(h, features)
	}
	t.Logf("%d linearizable, %d not; features: %v", good, bad, features)
	if good < 5000 || bad < 5000 {
		t.Fatalf("corpus not balanced: %d linearizable, %d not", good, bad)
	}
	for _, f := range []string{"overlap", "incomplete-write", "incomplete-read", "rejected", "delete", "empty-value", "repeated-value", "concurrent-same-key", "two-keys"} {
		if features[f] < 500 {
			t.Fatalf("feature %q appears in only %d histories", f, features[f])
		}
	}
}

func countFeatures(h History, f map[string]int) {
	seen := map[string]bool{}
	values := map[string]int{}
	for i, a := range h.Ops {
		switch {
		case a.Outcome == Incomplete && a.Kind == Get:
			seen["incomplete-read"] = true
		case a.Outcome == Incomplete:
			seen["incomplete-write"] = true
		case a.Outcome == Rejected:
			seen["rejected"] = true
		}
		if a.Kind == Delete {
			seen["delete"] = true
		}
		if a.Kind == Put {
			if len(a.Value) == 0 {
				seen["empty-value"] = true
			}
			values[string(a.Value)]++
		}
		for _, b := range h.Ops[i+1:] {
			ae, be := a.Complete, b.Complete
			if ae == 0 {
				ae = math.MaxInt64
			}
			if be == 0 {
				be = math.MaxInt64
			}
			if a.Invoke < be && b.Invoke < ae {
				seen["overlap"] = true
				if a.Key == b.Key {
					seen["concurrent-same-key"] = true
				}
			}
		}
	}
	for _, n := range values {
		if n > 1 {
			seen["repeated-value"] = true
		}
	}
	if len(h.Keys()) > 1 {
		seen["two-keys"] = true
	}
	for k := range seen {
		f[k]++
	}
}

// TestOracleOnHandPickedCases pins the oracle itself on the textbook cases, so
// the cross-validation is not two wrong implementations agreeing.
func TestOracleOnHandPickedCases(t *testing.T) {
	cases := []struct {
		name string
		want bool
		text string
	}{
		{"sequential", true, "1 c put \"k\" 1 2 ok value=\"A\"\n2 c get \"k\" 3 4 ok output=\"A\""},
		{"stale read", false, "1 c put \"k\" 1 2 ok value=\"A\"\n2 c put \"k\" 3 4 ok value=\"B\"\n3 d get \"k\" 5 6 ok output=\"A\""},
		{"overlap may see old", true, "1 c put \"k\" 1 2 ok value=\"A\"\n2 c put \"k\" 3 6 ok value=\"B\"\n3 d get \"k\" 4 5 ok output=\"A\""},
		{"future read", false, "1 d get \"k\" 1 2 ok output=\"A\"\n2 c put \"k\" 3 4 ok value=\"A\""},
		{"incomplete write seen", true, "1 c put \"k\" 1 0 incomplete value=\"A\"\n2 d get \"k\" 2 3 ok output=\"A\""},
		{"incomplete write before its invocation", false, "1 d get \"k\" 1 2 ok output=\"A\"\n2 c put \"k\" 3 0 incomplete value=\"A\""},
		{"rejected write seen", false, "1 c put \"k\" 1 2 rejected value=\"A\"\n2 d get \"k\" 3 4 ok output=\"A\""},
		{"empty value is present", false, "1 c put \"k\" 1 2 ok value=\"\"\n2 d get \"k\" 3 4 notfound"},
		{"empty value read", true, "1 c put \"k\" 1 2 ok value=\"\"\n2 d get \"k\" 3 4 ok output=\"\""},
		{"idempotent delete", true, "1 c delete \"k\" 1 2 ok\n2 c delete \"k\" 3 4 ok\n3 c get \"k\" 5 6 notfound"},
		{"cross-key independent", true, "1 c put \"a\" 1 4 ok value=\"A\"\n2 d put \"b\" 2 3 ok value=\"B\"\n3 d get \"a\" 5 6 ok output=\"A\""},
	}
	for _, c := range cases {
		h, err := ParseHistory(c.text)
		if err != nil {
			t.Fatal(err)
		}
		if got := oracleLinearizable(h); got != c.want {
			t.Fatalf("%s: oracle says %v, want %v", c.name, got, c.want)
		}
	}
}

// --- the file corpus ---

// TestKnownGoodAndKnownBadCorpus runs every history under testdata/corpus: each
// file in good/ must be accepted and each in bad/ rejected — by the checker AND
// by the independent oracle — and every rejection must come with a minimized
// counterexample that is itself rejected by the oracle (a minimizer that
// "explained" a failure with a linearizable remnant would be lying). The files
// document what each case means; new cases are added as files, not code.
func TestKnownGoodAndKnownBadCorpus(t *testing.T) {
	for _, dir := range []string{"good", "bad"} {
		files, err := filepath.Glob(filepath.Join("testdata", "corpus", dir, "*.hist"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) < 12 {
			t.Fatalf("corpus %s has only %d histories", dir, len(files))
		}
		sort.Strings(files)
		for _, f := range files {
			name := dir + "/" + filepath.Base(f)
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(string(b), "#") {
				t.Fatalf("%s: every corpus file starts with a comment saying what it is", name)
			}
			h, err := ParseHistory(string(b))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if err := h.Validate(); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			want := dir == "good"
			r := Check(h, Options{Minimize: true})
			if r.Unchecked {
				t.Fatalf("%s: unchecked", name)
			}
			if r.OK != want {
				t.Fatalf("%s: checker says linearizable=%v\n%s", name, r.OK, r.Reason)
			}
			if o := oracleLinearizable(h); o != want {
				t.Fatalf("%s: oracle says linearizable=%v", name, o)
			}
			if !want {
				if len(r.Counterexample) == 0 || oracleLinearizable(History{Ops: r.Counterexample}) {
					t.Fatalf("%s: the minimized counterexample is not itself a violation:\n%s", name, Format(r.Counterexample))
				}
				t.Logf("%s: rejected; counterexample %d of %d ops", name, len(r.Counterexample), len(h.Ops))
			}
		}
	}
}

// FuzzCheckerMatchesOracle feeds arbitrary bytes, decoded into a small history,
// to the checker and the oracle; any disagreement is a checker (or oracle) bug.
func FuzzCheckerMatchesOracle(f *testing.F) {
	f.Add([]byte{3, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{5, 9, 9, 9, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		n := 1 + int(data[0])%7
		rng := rand.New(rand.NewSource(int64(data[1]) | int64(len(data))<<8))
		h := randomHistory(rng, n, 1+int(data[0]/7)%2)
		// Let the input steer outcomes and outputs directly, too.
		for i := range h.Ops {
			if 2+i < len(data) {
				b := data[2+i]
				switch b % 5 {
				case 0:
					if h.Ops[i].Kind == Get {
						h.Ops[i].Outcome, h.Ops[i].Output = OK, []byte{"ab"[b/5%2]}
						if h.Ops[i].Complete == 0 {
							h.Ops[i].Complete = int64(2*n + 1 + i)
						}
					}
				case 1:
					h.Ops[i].Outcome, h.Ops[i].Complete, h.Ops[i].Output = Incomplete, 0, nil
				}
			}
		}
		if h.Validate() != nil {
			return
		}
		got := Check(h, Options{})
		if !got.Unchecked && got.OK != oracleLinearizable(h) {
			t.Fatalf("checker=%v oracle=%v:\n%s", got.OK, !got.OK, h)
		}
	})
}
