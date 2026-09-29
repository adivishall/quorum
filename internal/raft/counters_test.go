package raft

import "testing"

// TestCountersRecordEveryRoleTransition (Phase 16, docs/OBSERVABILITY.md): the
// core counts each campaign, each term won and each time it stops leading, at
// the transition itself — so a node that campaigns and wins inside one step, or
// is deposed and re-elected between two observations, is still counted.
func TestCountersRecordEveryRoleTransition(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 11)
	nw.electLeader("n1")
	if c := nw.nodes["n1"].Counters(); c.Campaigns != 1 || c.ElectionsWon != 1 || c.StepDowns != 0 {
		t.Fatalf("n1 after winning its first election: %+v", c)
	}
	for _, id := range []NodeID{"n2", "n3"} {
		if c := nw.nodes[id].Counters(); c != (Counters{}) {
			t.Fatalf("%s voted and never campaigned, yet counts %+v", id, c)
		}
	}
	nw.electLeader("n2")
	if c := nw.nodes["n1"].Counters(); c.StepDowns != 1 || c.ElectionsWon != 1 {
		t.Fatalf("n1 deposed by n2: %+v", c)
	}
	if c := nw.nodes["n2"].Counters(); c.Campaigns != 1 || c.ElectionsWon != 1 {
		t.Fatalf("n2 after winning: %+v", c)
	}
	nw.electLeader("n1")
	if c := nw.nodes["n1"].Counters(); c.Campaigns != 2 || c.ElectionsWon != 2 || c.StepDowns != 1 {
		t.Fatalf("n1 re-elected: %+v", c)
	}

	// A single-voter group campaigns and wins inside one Tick: both counted.
	solo := newNetwork(t, []NodeID{"s"}, 3)
	solo.campaign("s")
	if c := solo.nodes["s"].Counters(); c.Campaigns != 1 || c.ElectionsWon != 1 {
		t.Fatalf("a single voter's immediate win: %+v", c)
	}
}

// TestProgressIsTheLeadersViewOfItsFollowers: a leader reports each other
// member's match index — the entries it knows that member holds — and nothing
// for itself; any other role reports nothing.
func TestProgressIsTheLeadersViewOfItsFollowers(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 5)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	last := nw.nodes["n1"].LastIndex()
	p := nw.nodes["n1"].Progress()
	if len(p) != 2 || p["n2"] != last || p["n3"] != last {
		t.Fatalf("leader progress %v, want n2 and n3 at %d", p, last)
	}
	if _, self := p["n1"]; self {
		t.Fatal("the leader reports itself")
	}
	nw.isolate("n3")
	nw.propose("n1", "b")
	p = nw.nodes["n1"].Progress()
	if p["n2"] != last+1 || p["n3"] != last {
		t.Fatalf("with n3 cut off: %v (want n2 %d, n3 %d)", p, last+1, last)
	}
	p["n2"] = 0
	if nw.nodes["n1"].Progress()["n2"] != last+1 {
		t.Fatal("Progress returned the core's own map")
	}
	if nw.nodes["n2"].Progress() != nil {
		t.Fatal("a follower reported progress")
	}
}
