package raft

import "testing"

// TestNoOpAppendedOnElection proves the mandatory no-op: on becoming leader a node
// appends exactly one entry in its current term (§8.2). Without it a new leader
// cannot commit entries from prior terms (the figure-8 defense).
func TestNoOpAppendedOnElection(t *testing.T) {
	nw := newNetwork(t, ids(3), 600)
	nw.electLeader("a")
	a := nw.nodes["a"]
	if a.LastIndex() != 1 {
		t.Fatalf("lastIndex = %d, want 1 (the no-op)", a.LastIndex())
	}
	e, _ := nw.logs["a"].At(1)
	if e.Term != a.Term() || len(e.Data) != 0 {
		t.Fatalf("index 1 = {term %d, data %q}, want an empty entry in the current term %d", e.Term, e.Data, a.Term())
	}
}

// TestFigure8 reproduces the Raft paper's Figure 8: an entry created in an earlier
// term is replicated to a majority by a later-term leader. A naive implementation
// that commits it by counting replicas would lose it when a different leader
// overwrites it. Correct Raft (the mandatory no-op + the current-term commit rule,
// §5.4.2) commits the old entry only together with a current-term entry, after
// which no other node can win an election and overwrite it.
//
// This test has teeth against removing the no-op: without it the term-4 leader
// cannot commit the old term-2 entry at all, so the commit==3 assertion fails; and
// the network's continuous log-matching / state-machine-safety checks would fire if
// a committed entry were ever overwritten.
func TestFigure8(t *testing.T) {
	nw := newNetwork(t, ids(5), 700)
	// Post-crash state (Figure 8, panel c precursor): a holds an old term-2 entry
	// at index 2; b,c are behind; d,e hold a competing term-3 entry at index 2.
	nw.seedNode("a", []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 3, "", 1)
	nw.seedNode("b", []Entry{{Index: 1, Term: 1}}, 3, "", 2)
	nw.seedNode("c", []Entry{{Index: 1, Term: 1}}, 3, "", 3)
	nw.seedNode("d", []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}}, 3, "", 4)
	nw.seedNode("e", []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}}, 3, "", 5)

	// a wins term 4 with votes from a,b,c (b,c logs are less up to date than a's;
	// d,e have a higher last term and deny). d,e are then partitioned off.
	nw.isolate("d")
	nw.isolate("e")
	nw.electLeader("a")
	if nw.nodes["a"].Term() != 4 {
		t.Fatalf("a term = %d, want 4", nw.nodes["a"].Term())
	}
	// a replicates to b,c. The commit advances to 3 — the term-4 no-op — which
	// carries the old term-2 entry at index 2 with it. The old entry is committed
	// ONLY because a current-term entry committed (the figure-8 rule): remove the
	// no-op and the term-4 leader can never commit the term-2 entry, so this
	// assertion fails.
	nw.propose("a", "cur") // a current-term (4) command, forces replication
	a := nw.nodes["a"]
	if a.CommitIndex() < 3 {
		t.Fatalf("commit = %d, want >= 3 (the no-op enables committing the prior-term entry)\n%s", a.CommitIndex(), nw.dumpLog("a"))
	}
	// index 2 is the old term-2 entry, now committed as part of the prefix.
	e2, _ := nw.logs["a"].At(2)
	if e2.Term != 2 {
		t.Fatalf("index 2 term = %d, want 2 (the preserved old entry)", e2.Term)
	}

	// Safety: the old entry is now durable. d (with its competing term-3 entry)
	// cannot win an election, because a,b,c hold a term-4 entry (more up to date).
	nw.heal()
	nw.campaign("d")
	nw.deliverAll()
	if nw.nodes["d"].Role() == Leader {
		t.Fatalf("d became leader and could overwrite a committed entry (figure-8 violation)")
	}
	// a,b,c still hold the committed term-2 entry at index 2.
	for _, id := range []NodeID{"a", "b", "c"} {
		e, err := nw.logs[id].At(2)
		if err != nil || e.Term != 2 {
			t.Fatalf("%s lost the committed entry at index 2: %v %+v", id, err, e)
		}
	}
}

// TestLeaderCompleteness proves INV-R4: a committed entry is present in the log of
// every leader of every later term. It commits an entry, then forces two
// leadership changes and checks the entry survives on each new leader.
func TestLeaderCompleteness(t *testing.T) {
	nw := newNetwork(t, ids(3), 800)
	nw.electLeader("a")
	nw.propose("a", "committed") // index 2, committed on a quorum
	committedIdx := nw.nodes["a"].CommitIndex()
	if committedIdx < 2 {
		t.Fatalf("setup: commit = %d, want >= 2", committedIdx)
	}

	entryAt := func(id NodeID, idx uint64) Entry {
		e, err := nw.logs[id].At(idx)
		if err != nil {
			t.Fatalf("%s missing index %d", id, idx)
		}
		return e
	}
	want := entryAt("a", 2)

	// Force a new leader (b), then another (c); each must still hold the entry.
	// Isolate only the node just deposed (a stale isolated ex-leader steps down on
	// the next election's RequestVote, so it must stay reachable).
	leader := NodeID("a")
	for _, next := range []NodeID{"b", "c"} {
		nw.isolate(leader) // depose the current leader so a new election is needed
		nw.electLeader(next)
		got := entryAt(next, 2)
		if got.Term != want.Term || string(got.Data) != string(want.Data) {
			t.Fatalf("new leader %s lost the committed entry at index 2: got %+v want %+v (INV-R4)", next, got, want)
		}
		nw.heal()
		leader = next
	}
}
