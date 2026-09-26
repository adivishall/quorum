package lincheck

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// DefaultMaxStates bounds the search per key: the number of (linearized set,
// state) nodes the checker may visit before giving up on that key. At that
// point the result is Unchecked, never a verdict.
const DefaultMaxStates = 2_000_000

// Options tune a Check.
type Options struct {
	// MaxStates is the per-key search budget (0 = DefaultMaxStates).
	MaxStates int
	// Minimize shrinks the counterexample of a failing key (Minimize).
	Minimize bool
}

// Stats is what a Check cost.
type Stats struct {
	Keys, Ops, Complete, Optional, Excluded int
	States, MemoHits                        int
	MaxDepth                                int
	Duration                                time.Duration
}

// Result is the verdict for a whole history.
type Result struct {
	// OK: every key's projection is linearizable. False means either a real
	// violation (Unchecked false) or that some key exceeded the budget
	// (Unchecked true), never a guess.
	OK        bool
	Unchecked bool
	Key       string // the key that failed (or exceeded the budget)
	// Counterexample is the failing key's history — minimized if asked — and
	// Reason explains where every linearization gets stuck.
	Counterexample []Op
	Reason         string
	Stats          Stats
}

// Check decides whether h is linearizable under the register model. It
// validates the history first (a malformed history is an error in the harness,
// reported as a non-OK result with a reason, not silently "linearizable"), then
// checks its LOGICAL operations (History.Logical): the sends of one identified
// write are one operation. A history in which the server acknowledged two
// different commands under one request identity is not linearizable under the
// request-identity contract, and is reported as such.
func Check(h History, opts Options) Result {
	start := time.Now()
	res := Result{OK: true}
	if err := h.Validate(); err != nil {
		res.OK = false
		res.Reason = "invalid history: " + err.Error()
		return res
	}
	lh, err := h.Logical()
	if err != nil {
		res.OK = false
		res.Reason = "request identity violated: " + err.Error()
		var ie *IdentityError
		if errors.As(err, &ie) {
			res.Counterexample = ie.Ops
			if len(ie.Ops) > 0 {
				res.Key = ie.Ops[0].Key
			}
		}
		return res
	}
	h = lh
	max := opts.MaxStates
	if max <= 0 {
		max = DefaultMaxStates
	}
	for _, key := range h.Keys() {
		ops := h.ForKey(key)
		res.Stats.Keys++
		kr := checkKey(ops, max)
		res.Stats.Ops += kr.ops
		res.Stats.Complete += kr.complete
		res.Stats.Optional += kr.optional
		res.Stats.Excluded += kr.excluded
		res.Stats.States += kr.states
		res.Stats.MemoHits += kr.memoHits
		if kr.maxDepth > res.Stats.MaxDepth {
			res.Stats.MaxDepth = kr.maxDepth
		}
		if kr.ok {
			continue
		}
		res.OK = false
		res.Key = key
		if kr.budget {
			res.Unchecked = true
			res.Counterexample = ops
			res.Reason = fmt.Sprintf("key %q: search budget of %d states exceeded; the history is UNCHECKED, not known to be linearizable", key, max)
			break
		}
		ce := ops
		if opts.Minimize {
			ce = Minimize(ops, max)
		}
		res.Counterexample = ce
		res.Reason = explain(key, ce, max)
		break
	}
	res.Stats.Duration = time.Since(start)
	return res
}

// keyResult is the outcome for one key.
type keyResult struct {
	ok, budget                       bool
	ops, complete, optional, exclude int
	excluded                         int
	states, memoHits, maxDepth       int
	stuck                            *stuckPoint
}

// stuckPoint is the deepest point any linearization reached: the ops linearized
// so far (in order), the state there, and why each remaining candidate fails.
type stuckPoint struct {
	depth    int
	order    []int
	state    State
	failures []string
	culprits []int // IDs of the ops whose observation failed there, in candidate order
}

// searcher runs the per-key search.
type searcher struct {
	ops       []Op
	inv, res  []int64
	optional  []bool
	nComplete int
	max       int
	states    int
	memoHits  int
	budget    bool
	memo      map[string]struct{}
	order     []int
	best      *stuckPoint
}

// checkKey searches one key's projection. Rejected ops are excluded; Incomplete
// ops are optional; the rest must all be linearized.
func checkKey(all []Op, max int) keyResult {
	var kr keyResult
	var ops []Op
	for _, op := range all {
		kr.ops++
		if !op.Effective() {
			kr.excluded++
			continue
		}
		ops = append(ops, op)
	}
	// Incomplete reads constrain nothing: drop them so they cannot inflate the
	// search. Incomplete writes stay, as optional.
	filtered := ops[:0]
	for _, op := range ops {
		if op.Optional() && op.Kind == Get {
			kr.excluded++
			continue
		}
		filtered = append(filtered, op)
	}
	ops = filtered
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Invoke < ops[j].Invoke })
	s := &searcher{ops: ops, max: max, memo: map[string]struct{}{}}
	s.inv = make([]int64, len(ops))
	s.res = make([]int64, len(ops))
	s.optional = make([]bool, len(ops))
	for i, op := range ops {
		s.inv[i] = op.Invoke
		if op.Optional() {
			s.optional[i] = true
			s.res[i] = math.MaxInt64
			kr.optional++
		} else {
			s.res[i] = op.Complete
			s.nComplete++
			kr.complete++
		}
	}
	kr.ok = s.search(newBits(len(ops)), Initial, 0)
	kr.budget = s.budget
	kr.states, kr.memoHits = s.states, s.memoHits
	if s.best != nil {
		kr.maxDepth = s.best.depth
	}
	kr.stuck = s.best
	if kr.budget {
		kr.ok = false
	}
	return kr
}

// search is the depth-first walk: lin is the set of ops already linearized, st
// the register state after them, done the number of complete ops in lin.
func (s *searcher) search(lin bits, st State, done int) bool {
	if done == s.nComplete {
		return true
	}
	if s.states >= s.max {
		s.budget = true
		return false
	}
	s.states++
	key := lin.key(st)
	if _, seen := s.memo[key]; seen {
		s.memoHits++
		return false
	}
	// An op may be linearized next only if no unlinearized op completed before
	// it was invoked: inv < min completion over unlinearized ops.
	minRes := int64(math.MaxInt64)
	for i := range s.ops {
		if !lin.has(i) && s.res[i] < minRes {
			minRes = s.res[i]
		}
	}
	var failures []string
	var culprits []int
	for i := range s.ops {
		if lin.has(i) || s.inv[i] >= minRes {
			continue
		}
		st2, ok := Step(st, s.ops[i])
		if !ok {
			failures = append(failures, fmt.Sprintf("%s returned %s but the state was %s", describe(s.ops[i]), Observed(s.ops[i]), Expected(st)))
			culprits = append(culprits, s.ops[i].ID)
			continue
		}
		lin.set(i)
		s.order = append(s.order, i)
		d := done
		if !s.optional[i] {
			d++
		}
		if s.search(lin, st2, d) {
			return true
		}
		s.order = s.order[:len(s.order)-1]
		lin.clear(i)
		if s.budget {
			return false
		}
	}
	if s.best == nil || done > s.best.depth {
		s.best = &stuckPoint{depth: done, order: append([]int(nil), s.order...), state: st, failures: failures, culprits: culprits}
	}
	s.memo[key] = struct{}{}
	return false
}

func describe(op Op) string {
	switch op.Kind {
	case Put:
		return fmt.Sprintf("op %d %s put(%q,%q) [%d,%d]", op.ID, op.Client, op.Key, op.Value, op.Invoke, op.Complete)
	case Get:
		return fmt.Sprintf("op %d %s get(%q) [%d,%d]", op.ID, op.Client, op.Key, op.Invoke, op.Complete)
	default:
		return fmt.Sprintf("op %d %s delete(%q) [%d,%d]", op.ID, op.Client, op.Key, op.Invoke, op.Complete)
	}
}

// explain runs the search once more on a (possibly minimized) counterexample and
// renders where every linearization gets stuck.
func explain(key string, ops []Op, max int) string {
	kr := checkKey(ops, max)
	var b strings.Builder
	fmt.Fprintf(&b, "key %q is not linearizable (%d ops, %d must linearize, %d optional, %d excluded; %d states searched)\n",
		key, kr.ops, kr.complete, kr.optional, kr.excluded, kr.states)
	if kr.stuck == nil {
		return b.String()
	}
	eff := effective(ops)
	sort.SliceStable(eff, func(i, j int) bool { return eff[i].Invoke < eff[j].Invoke })
	fmt.Fprintf(&b, "the longest linearization prefix explains %d of %d required ops:\n", kr.stuck.depth, kr.complete)
	for _, i := range kr.stuck.order {
		fmt.Fprintf(&b, "  %s\n", describe(eff[i]))
	}
	fmt.Fprintf(&b, "state there: present=%v value=%q; no remaining op can go next:\n", kr.stuck.state.Present, kr.stuck.state.Value)
	for _, f := range kr.stuck.failures {
		fmt.Fprintf(&b, "  %s\n", f)
	}
	if len(kr.stuck.failures) == 0 {
		b.WriteString("  (every candidate leads to a state the search already proved a dead end)\n")
	}
	return b.String()
}

// effective returns the ops the search actually considers (Rejected ops and
// Incomplete reads removed), in the order checkKey uses.
func effective(ops []Op) []Op {
	var out []Op
	for _, op := range ops {
		if !op.Effective() || (op.Optional() && op.Kind == Get) {
			continue
		}
		out = append(out, op)
	}
	return out
}

// --- bitset ---

type bits []uint64

func newBits(n int) bits      { return make(bits, (n+63)/64) }
func (b bits) has(i int) bool { return b[i/64]&(1<<(uint(i)%64)) != 0 }
func (b bits) set(i int)      { b[i/64] |= 1 << (uint(i) % 64) }
func (b bits) clear(i int)    { b[i/64] &^= 1 << (uint(i) % 64) }
func (b bits) key(st State) string {
	buf := make([]byte, 0, 8*len(b)+2+len(st.Value))
	for _, w := range b {
		for k := 0; k < 8; k++ {
			buf = append(buf, byte(w>>(8*k)))
		}
	}
	if st.Present {
		buf = append(buf, 1)
		buf = append(buf, st.Value...)
	} else {
		buf = append(buf, 0)
	}
	return string(buf)
}
