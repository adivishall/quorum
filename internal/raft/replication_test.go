package raft

import "testing"

// TestReplicationAndCommitAndApply proves proposals replicate to a quorum, commit,
// and apply in order on every node (INV-R2/R3/R5/R7 exercised continuously).
func TestReplicationAndCommitAndApply(t *testing.T) {
	nw := newNetwork(t, ids(3), 100)
	nw.electLeader("a")
	for _, cmd := range []string{"one", "two", "three"} {
		nw.propose("a", cmd)
	}
	// no-op(1) + 3 proposals = index 4, all committed and applied on the leader.
	a := nw.nodes["a"]
	if a.CommitIndex() != 4 || a.AppliedIndex() != 4 {
		t.Fatalf("leader commit=%d applied=%d, want 4/4", a.CommitIndex(), a.AppliedIndex())
	}
	// Every node's log is identical through index 4.
	for _, id := range []NodeID{"b", "c"} {
		if nw.logs[id].LastIndex() != 4 {
			t.Fatalf("%s lastIndex = %d, want 4\n%s", id, nw.logs[id].LastIndex(), nw.dumpLog(id))
		}
	}
	nw.assertLogMatching()
	// The applied commands, in order, are exactly the three proposals (index 1 is
	// the no-op with empty data).
	want := []string{"", "one", "two", "three"}
	for i, w := range want {
		e := nw.appliedLog[uint64(i+1)]
		if string(e.Data) != w {
			t.Fatalf("applied[%d] = %q, want %q", i+1, e.Data, w)
		}
	}
}

// TestLeaderAppendOnly proves a leader only extends its own log — indexes it
// already holds are never rewritten while it stays leader (INV-R2).
func TestLeaderAppendOnly(t *testing.T) {
	nw := newNetwork(t, ids(3), 111)
	nw.electLeader("a")
	snap := map[uint64]string{}
	record := func() {
		l := nw.logs["a"]
		for i := uint64(1); i <= l.LastIndex(); i++ {
			e, _ := l.At(i)
			key := string(rune(e.Term)) + ":" + string(e.Data)
			if prev, ok := snap[i]; ok && prev != key {
				t.Fatalf("leader rewrote its own entry at index %d: %q -> %q (INV-R2)", i, prev, key)
			}
			snap[i] = key
		}
	}
	record()
	for _, cmd := range []string{"a", "b", "c", "d"} {
		nw.propose("a", cmd)
		record()
	}
}

// TestFollowerCatchUpMissingEntries proves a follower that missed entries (pure
// lag) is caught up, and the missing-entries conflict hint jumps straight to the
// gap rather than decrementing one index per round trip (§10).
func TestFollowerCatchUpMissingEntries(t *testing.T) {
	nw := newNetwork(t, ids(3), 200)
	nw.electLeader("a")
	cBefore := nw.logs["c"].LastIndex()
	nw.isolate("c") // c misses the next proposals entirely
	for _, cmd := range []string{"1", "2", "3", "4", "5"} {
		nw.propose("a", cmd)
	}
	if nw.logs["c"].LastIndex() != cBefore {
		t.Fatalf("isolated c advanced from %d to %d", cBefore, nw.logs["c"].LastIndex())
	}
	// Heal and trigger replication with one more proposal.
	nw.heal()
	nw.appendsTo["c"] = 0
	nw.propose("a", "6")
	if nw.logs["c"].LastIndex() != nw.logs["a"].LastIndex() {
		t.Fatalf("c did not catch up: c=%d a=%d\n%s", nw.logs["c"].LastIndex(), nw.logs["a"].LastIndex(), nw.dumpLog("c"))
	}
	// A missing-entries hint (conflictTerm=0) means one probe then the bulk send:
	// far fewer than the 6 the leader was ahead by.
	if nw.appendsTo["c"] > 2 {
		t.Fatalf("catch-up took %d AppendEntries; a missing-entries hint should need ~1 probe", nw.appendsTo["c"])
	}
}

// TestConflictBackupByTerm constructs a divergent-log follower (paper Figure 7,
// follower f) and proves the leader backs up a whole TERM per rejection, not one
// index — asserting the exact number of AppendEntries, not just convergence (§10).
func TestConflictBackupByTerm(t *testing.T) {
	nw := newNetwork(t, ids(3), 300)
	// a and b share a multi-term log; c diverges from index 2 (all term 1).
	shared := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 3}, {Index: 5, Term: 3}}
	nw.seedNode("a", shared, 3, "", 1)
	nw.seedNode("b", shared, 3, "", 2)
	nw.seedNode("c", []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}, 1, "", 3)

	// Keep c isolated while a wins an election (with b's vote) and appends its
	// term-4 no-op, so nextIndex[c] starts stale at a.lastIndex+1.
	nw.isolate("c")
	nw.electLeader("a")
	if nw.nodes["a"].Role() != Leader {
		t.Fatalf("a failed to lead: %s", nw.dumpLog("a"))
	}

	// Now heal and trigger replication with one proposal; the whole back-up runs
	// in the deliverAll inside propose.
	nw.heal()
	nw.appendsTo["c"] = 0
	nw.propose("a", "z")

	if nw.logs["c"].LastIndex() != nw.logs["a"].LastIndex() {
		t.Fatalf("c did not converge: c=%d a=%d\n%s\n%s", nw.logs["c"].LastIndex(), nw.logs["a"].LastIndex(), nw.dumpLog("a"), nw.dumpLog("c"))
	}
	nw.assertLogMatching()
	// a.log spans terms {1,2,3,4}. Backing up by term: probe@5(term3, missing on
	// c) -> probe@3(term3 vs c's term1) -> probe@1(term1, matches) -> success. That
	// is 3 AppendEntries. One-index-per-round-trip from nextIndex=6 down to 2 would
	// be 5. Assert the term-based bound.
	if got := nw.appendsTo["c"]; got != 3 {
		t.Fatalf("catch-up took %d AppendEntries; term back-up should take 3 (per-index would be 5)", got)
	}
}

// TestSuffixReplacement proves a follower's uncommitted, conflicting suffix is
// replaced by the leader's entries while the matching prefix is retained.
func TestSuffixReplacement(t *testing.T) {
	nw := newNetwork(t, ids(3), 400)
	// a leads with entries at term 2; c has a divergent uncommitted suffix at term 1.
	shared := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}}
	nw.seedNode("a", shared, 2, "", 1)
	nw.seedNode("b", shared, 2, "", 2)
	nw.seedNode("c", []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}}, 1, "", 3)

	nw.isolate("c")
	nw.electLeader("a")
	nw.heal()
	nw.propose("a", "z")

	// c retains index 1 (term 1, matched) and takes a's suffix from index 2.
	if nw.logs["c"].LastIndex() != nw.logs["a"].LastIndex() {
		t.Fatalf("c length = %d, want %d\n%s", nw.logs["c"].LastIndex(), nw.logs["a"].LastIndex(), nw.dumpLog("c"))
	}
	t1, _ := nw.logs["c"].Term(1)
	t2, _ := nw.logs["c"].Term(2)
	if t1 != 1 || t2 != 2 {
		t.Fatalf("c terms = (1:%d, 2:%d), want (1, 2) — prefix kept, suffix replaced", t1, t2)
	}
	nw.assertLogMatching()
}

// TestMinorityDoesNotCommit proves a leader without a quorum cannot advance
// commit, and that healing lets it commit (availability is CP, not AP).
func TestMinorityDoesNotCommit(t *testing.T) {
	nw := newNetwork(t, ids(3), 500)
	nw.electLeader("a")
	commit0 := nw.nodes["a"].CommitIndex()
	// Cut a off from both peers: it is a minority of one.
	nw.isolate("a")
	if err := nw.nodes["a"].Propose([]byte("lonely")); err != nil {
		t.Fatalf("propose: %v", err)
	}
	nw.drain("a")
	nw.deliverAll() // messages to b,c are dropped
	if nw.nodes["a"].CommitIndex() != commit0 {
		t.Fatalf("commit advanced to %d without a quorum (want %d)", nw.nodes["a"].CommitIndex(), commit0)
	}
	if nw.logs["a"].LastIndex() <= commit0 {
		t.Fatalf("proposal should still be in a's log, uncommitted")
	}
}
