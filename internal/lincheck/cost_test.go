package lincheck

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"
)

// TestCheckerCostEnvelope measures — it does not optimize — what the checker
// costs, so docs/LINEARIZABILITY.md §6.3 can state the tested bounds honestly:
// practical history length (linearizable, one hot key, eight clients), the
// expensive direction (proving NO linearization exists), the adversarial
// worst case (many mutually concurrent writes that every read must be checked
// against), memory, and minimization. Every row is logged; the assertions are
// only that each case finished inside the default budget with the verdict it
// must have. The two heaviest rows run only with LINCHECK_COST_FULL=1:
//
//	LINCHECK_COST_FULL=1 go test ./internal/lincheck -run TestCheckerCostEnvelope -v
func TestCheckerCostEnvelope(t *testing.T) {
	type row struct {
		name     string
		ops      int
		verdict  string
		states   int
		dur, min time.Duration
		alloc    uint64
	}
	var rows []row
	// measure checks h; mayExceed marks the adversarial cases whose point is to
	// find where the default budget runs out (then the row says UNCHECKED — the
	// checker's answer, which is never a verdict).
	measure := func(name string, h History, wantOK, mayExceed bool) {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		r := Check(h, Options{})
		dur := time.Since(start)
		runtime.ReadMemStats(&after)
		if r.Unchecked {
			if !mayExceed {
				t.Fatalf("%s: exceeded the default budget", name)
			}
			rows = append(rows, row{name: name, ops: len(h.Ops), verdict: "UNCHECKED (budget)", states: r.Stats.States, dur: dur, alloc: after.TotalAlloc - before.TotalAlloc})
			return
		}
		if r.OK != wantOK {
			t.Fatalf("%s: linearizable=%v, want %v", name, r.OK, wantOK)
		}
		rw := row{name: name, ops: len(h.Ops), states: r.Stats.States, dur: dur, alloc: after.TotalAlloc - before.TotalAlloc}
		rw.verdict = "linearizable"
		if !r.OK {
			rw.verdict = "violation"
			start = time.Now()
			ce := Minimize(h.ForKey(r.Key), DefaultMaxStates)
			rw.min = time.Since(start)
			rw.verdict = fmt.Sprintf("violation → %d ops", len(ce))
		}
		rows = append(rows, rw)
	}
	rng := rand.New(rand.NewSource(99))
	for _, n := range []int{200, 1000, 5000} {
		measure("by construction, 8 clients, 1 key", genHistory(rng, 8, 1, n, 10), true, false)
	}
	for _, n := range []int{100, 600, 2000} {
		h := genHistory(rng, 8, 1, n, 15)
		injectStaleRead(t, h)
		measure("stale read injected (must exhaust)", h, false, false)
	}
	widths := []int{8, 12}
	if os.Getenv("LINCHECK_COST_FULL") != "" {
		widths = append(widths, 14, 16) // ~2 s and ~200 MiB more; see the doc
	}
	for _, w := range widths {
		measure(fmt.Sprintf("%d mutually concurrent writes", w), concurrentWrites(w, true), true, w > 12)
		measure(fmt.Sprintf("%d concurrent writes, impossible reads", w), concurrentWrites(w, false), false, w > 12)
	}
	for _, r := range rows {
		min := "-"
		if r.min > 0 {
			min = r.min.Round(time.Microsecond).String()
		}
		t.Logf("| %-40s | %5d | %-22s | %8d | %10s | %8.1f KiB | %10s |", r.name, r.ops, r.verdict, r.states, r.dur.Round(time.Microsecond), float64(r.alloc)/1024, min)
	}
}

// injectStaleRead makes the earliest read after two sequential completed puts
// return the first put's value: impossible, since the second put precedes it.
func injectStaleRead(t *testing.T, h History) {
	t.Helper()
	var puts []int
	for i, op := range h.Ops {
		if op.Kind == Put && op.Outcome == OK {
			puts = append(puts, i)
		}
	}
	sort.Slice(puts, func(a, b int) bool { return h.Ops[puts[a]].Complete < h.Ops[puts[b]].Complete })
	w := puts[0]
	p, g := -1, -1
	for _, i := range puts {
		if h.Ops[i].Invoke > h.Ops[w].Complete {
			p = i
			break
		}
	}
	for i, op := range h.Ops {
		if p >= 0 && op.Kind == Get && op.Outcome != Incomplete && op.Invoke > h.Ops[p].Complete && (g < 0 || op.Invoke < h.Ops[g].Invoke) {
			g = i
		}
	}
	if p < 0 || g < 0 {
		t.Fatal("cannot inject a stale read")
	}
	h.Ops[g].Outcome, h.Ops[g].Output = OK, h.Ops[w].Value
}

// concurrentWrites builds the adversarial shape: n writes of distinct values
// that all overlap each other, then reads, each after the writes completed,
// that observe the writes in one order — the checker must find that order among
// n! (the memo collapses it to subsets). With consistent=false the reads observe
// an order that also requires one write to happen twice: every order fails, so
// the search must exhaust.
func concurrentWrites(n int, consistent bool) History {
	var h History
	pos := int64(0)
	for i := 0; i < n; i++ {
		pos++
		h.Ops = append(h.Ops, Op{ID: i + 1, Client: fmt.Sprintf("w%d", i), Kind: Put, Key: "k", Value: []byte(fmt.Sprintf("v%d", i)), Invoke: pos, Outcome: OK})
	}
	for i := 0; i < n; i++ {
		pos++
		h.Ops[i].Complete = pos
	}
	// Reads, each completing before the next is invoked. Only the last write
	// can be observed after all completed; to force search, interleave reads
	// during the writes instead: read i overlaps every write and returns v_i.
	for i := range h.Ops {
		h.Ops[i].Complete += int64(2 * n)
	}
	pos = int64(n)
	for i := 0; i < n; i++ {
		pos++
		out := fmt.Sprintf("v%d", i)
		if !consistent && i == n-1 {
			out = "v0" // v0 again after v1..v{n-2}: needs v0 twice
		}
		h.Ops = append(h.Ops, Op{ID: n + i + 1, Client: "r", Kind: Get, Key: "k", Invoke: pos, Complete: pos + 1, Outcome: OK, Output: []byte(out)})
		pos++
	}
	return h
}
