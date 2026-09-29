package raft

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

// Phase 15 in the pure core (docs/MEMBERSHIP.md): joint consensus with a
// learner stage, in the deterministic harness. Every scenario the design
// enumerates — quorum rules, learners, both majorities, the leader and a
// candidate being removed, a removed node returning, a truncated configuration
// entry, snapshots and compaction carrying the configuration, one change at a
// time, recovery — is a table or a script here; the continuous invariants
// (R1, R3, R5, R7) run after every delivery.

func members(ids ...NodeID) []Member {
	out := make([]Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, Member{ID: id})
	}
	return out
}

func voters(ids ...NodeID) Configuration { return replication.VotersOf(ids) }

// TestQuorumRules pins quorumOf: a majority of the voters; in a joint
// configuration a majority of BOTH sets — old-only and new-only fail;
// overlapping and disjoint sets; a single voter; learners never count; an
// empty voter set never has a quorum.
func TestQuorumRules(t *testing.T) {
	set := func(ids ...NodeID) func(NodeID) bool {
		m := map[NodeID]bool{}
		for _, id := range ids {
			m[id] = true
		}
		return func(id NodeID) bool { return m[id] }
	}
	abc := voters("a", "b", "c")
	joint := func(old, nw Configuration) Configuration {
		c := nw.Clone()
		c.Outgoing = old.Clone().Voters
		return c
	}
	withLearner := abc.Clone()
	withLearner.Learners = members("l")
	cases := []struct {
		name string
		conf Configuration
		has  func(NodeID) bool
		want bool
	}{
		{"stable 3: two of three", abc, set("a", "b"), true},
		{"stable 3: one of three", abc, set("c"), false},
		{"stable 3: outsiders do not count", abc, set("a", "x", "y"), false},
		{"stable 5: three of five", voters("a", "b", "c", "d", "e"), set("a", "c", "e"), true},
		{"stable 5: two of five", voters("a", "b", "c", "d", "e"), set("a", "c"), false},
		{"single voter: itself", voters("a"), set("a"), true},
		{"single voter: nobody", voters("a"), set("b"), false},
		{"empty configuration: never", Configuration{}, set("a", "b"), false},
		{"learners never count", withLearner, set("a", "l"), false},
		{"learners never needed", withLearner, set("a", "b"), true},

		// Overlapping sets {a,b,c} -> {a,b,d}.
		{"joint overlapping: both majorities", joint(abc, voters("a", "b", "d")), set("a", "b"), true},
		{"joint overlapping: new-only majority", joint(abc, voters("a", "b", "d")), set("a", "d"), false},
		{"joint overlapping: old-only majority", joint(abc, voters("a", "b", "d")), set("b", "c"), false},
		{"joint overlapping: everyone", joint(abc, voters("a", "b", "d")), set("a", "b", "c", "d"), true},

		// Disjoint sets {a,b,c} -> {d,e,f}.
		{"joint disjoint: old-only", joint(abc, voters("d", "e", "f")), set("a", "b", "c"), false},
		{"joint disjoint: new-only", joint(abc, voters("d", "e", "f")), set("d", "e", "f"), false},
		{"joint disjoint: two of each", joint(abc, voters("d", "e", "f")), set("a", "b", "d", "e"), true},
		{"joint disjoint: two and one", joint(abc, voters("d", "e", "f")), set("a", "b", "d"), false},

		// Removing one of four: the old set needs three, the new set two.
		{"remove one of four: two acks satisfy new only", joint(voters("a", "b", "c", "d"), abc), set("a", "b"), false},
		{"remove one of four: three acks", joint(voters("a", "b", "c", "d"), abc), set("a", "b", "c"), true},
		// Adding a fourth: the new set needs three, the old two.
		{"add a fourth: two acks satisfy old only", joint(abc, voters("a", "b", "c", "d")), set("a", "b"), false},
		{"add a fourth: the newcomer with one old", joint(abc, voters("a", "b", "c", "d")), set("a", "d"), false},
		{"add a fourth: three acks", joint(abc, voters("a", "b", "c", "d")), set("a", "b", "d"), true},
		{"add a fourth: the new member unavailable", joint(abc, voters("a", "b", "c", "d")), set("a", "b", "c"), true},
		{"add a fourth: an old member and the new one unavailable", joint(abc, voters("a", "b", "c", "d")), set("a", "b"), false},

		// Two voters: both are needed.
		{"stable 2: both", voters("a", "b"), set("a", "b"), true},
		{"stable 2: one", voters("a", "b"), set("a"), false},
		// The leader a removes itself from {a,b}: the new set needs b, the old
		// set needs both — a alone never suffices, b alone neither.
		{"leader removed: the leader alone", joint(voters("a", "b"), voters("b")), set("a"), false},
		{"leader removed: the remaining voter alone", joint(voters("a", "b"), voters("b")), set("b"), false},
		{"leader removed: both", joint(voters("a", "b"), voters("b")), set("a", "b"), true},
		{"leader removed: the final configuration, the remaining voter", voters("b"), set("b"), true},
		// Removing c of {a,b,c}: the old member c unavailable is fine.
		{"remove c: the removed member unavailable", joint(abc, voters("a", "b")), set("a", "b"), true},
		{"remove c: an old member unavailable", joint(abc, voters("a", "b")), set("a", "c"), false},
		// A stable configuration is never held to a joint rule: its (absent)
		// Outgoing list is not consulted.
		{"stable: no outgoing rule", voters("a", "b", "c"), set("a", "b"), true},
	}
	for _, tc := range cases {
		if got := quorumOf(tc.conf, tc.has); got != tc.want {
			t.Errorf("%s: quorum=%v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestConfChangeTransitions pins ConfChange.Apply: what each operation leads to,
// and every refusal (an existing member added, a non-learner promoted, a
// non-member removed, the last voter removed, a change on a joint
// configuration).
func TestConfChangeTransitions(t *testing.T) {
	abc := voters("a", "b", "c")
	next, err := ConfChange{Type: AddLearner, Member: Member{ID: "d", Addr: "d:1"}}.Apply(abc)
	if err != nil || !next.IsLearner("d") || next.Joint() || len(next.Voters) != 3 {
		t.Fatalf("add learner: %v %v", next, err)
	}
	if addr, _ := next.Addr("d"); addr != "d:1" {
		t.Fatalf("the learner's address was not kept: %q", addr)
	}
	joint, err := ConfChange{Type: Promote, Member: Member{ID: "d"}}.Apply(next)
	if err != nil || !joint.Joint() || !joint.IsVoter("d") || joint.IsLearner("d") || len(joint.Voters) != 4 || len(joint.Outgoing) != 3 {
		t.Fatalf("promote: %v %v", joint, err)
	}
	if addr, _ := joint.Addr("d"); addr != "d:1" {
		t.Fatalf("promotion lost the address: %q", addr)
	}
	final := Final(joint)
	if final.Joint() || len(final.Voters) != 4 || !final.IsVoter("d") {
		t.Fatalf("final: %v", final)
	}
	rm, err := ConfChange{Type: RemoveVoter, Member: Member{ID: "a"}}.Apply(final)
	if err != nil || !rm.Joint() || rm.IsVoter("a") != true || len(rm.Voters) != 3 || len(rm.Outgoing) != 4 {
		t.Fatalf("remove voter: %v %v (a stays a voter of the outgoing set while joint)", rm, err)
	}
	if f := Final(rm); f.IsVoter("a") || len(f.Voters) != 3 {
		t.Fatalf("final after removal: %v", f)
	}
	rl, err := ConfChange{Type: RemoveLearner, Member: Member{ID: "d"}}.Apply(next)
	if err != nil || rl.IsMember("d") || rl.Joint() {
		t.Fatalf("remove learner: %v %v", rl, err)
	}
	for name, cc := range map[string]struct {
		cc   ConfChange
		conf Configuration
	}{
		"add an existing voter":     {ConfChange{Type: AddLearner, Member: Member{ID: "a"}}, abc},
		"add an existing learner":   {ConfChange{Type: AddLearner, Member: Member{ID: "d"}}, next},
		"promote a non-learner":     {ConfChange{Type: Promote, Member: Member{ID: "a"}}, abc},
		"promote a stranger":        {ConfChange{Type: Promote, Member: Member{ID: "z"}}, abc},
		"remove a non-member":       {ConfChange{Type: RemoveVoter, Member: Member{ID: "z"}}, abc},
		"remove a learner as voter": {ConfChange{Type: RemoveVoter, Member: Member{ID: "d"}}, next},
		"remove the last voter":     {ConfChange{Type: RemoveVoter, Member: Member{ID: "a"}}, voters("a")},
		"remove a voter as learner": {ConfChange{Type: RemoveLearner, Member: Member{ID: "a"}}, abc},
		"empty id":                  {ConfChange{Type: AddLearner}, abc},
		"unknown operation":         {ConfChange{Type: 9, Member: Member{ID: "d"}}, abc},
	} {
		if _, err := cc.cc.Apply(cc.conf); !errors.Is(err, ErrInvalidConfChange) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := (ConfChange{Type: AddLearner, Member: Member{ID: "e"}}).Apply(joint); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("a change on a joint configuration: %v", err)
	}
}

// change proposes a membership change on the leader and runs fair rounds
// until it is settled (no change pending) or the bound is reached.
func (nw *network) change(leader NodeID, cc ConfChange) {
	nw.t.Helper()
	if err := nw.nodes[leader].ProposeConfChange(cc); err != nil {
		nw.t.Fatalf("%s: %v", cc, err)
	}
	nw.drain(leader)
	nw.settleChange(leader)
}

// settleChange runs fair rounds until the leader has no change pending.
func (nw *network) settleChange(leader NodeID) {
	nw.t.Helper()
	for i := 0; i < 60; i++ {
		nw.deliverAll()
		if !nw.nodes[leader].ConfPending() {
			return
		}
		nw.tickAll()
	}
	c, idx := nw.nodes[leader].Conf()
	nw.t.Fatalf("the change did not settle on %s: %s at %d, commit %d", leader, c, idx, nw.nodes[leader].CommitIndex())
}

// confEntries returns the configuration entries in a node's log, in order.
func (nw *network) confEntries(id NodeID) []Configuration {
	var out []Configuration
	l := nw.logs[id]
	for i := l.FirstIndex(); i <= l.LastIndex(); i++ {
		e, _ := l.At(i)
		if e.Type == replication.EntryConfig {
			c, err := replication.DecodeConfiguration(e.Data)
			if err != nil {
				nw.t.Fatalf("%s: configuration entry %d: %v", id, i, err)
			}
			out = append(out, c)
		}
	}
	return out
}

// TestLearnerReplicatesButNeverCampaignsOrCounts (INV-MB5): a joiner added as a
// learner receives the log, never campaigns however long it waits for a
// leader, is not asked for votes and has its vote ignored by a candidate, and
// never counts toward a commit. Asked anyway, it grants a vote to an
// up-to-date candidate: it may be a voter of a configuration it has not yet
// received (TestPromotedLearnerThatMissedItsPromotionStillElects), and only
// the candidate's configuration decides whether a vote counts.
func TestLearnerReplicatesButNeverCampaignsOrCounts(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 1)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	if c, _ := nw.nodes["n4"].Conf(); !c.Empty() {
		t.Fatalf("a joiner starts with an empty configuration, has %s", c)
	}
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n4"}})
	nw.heartbeatRounds()
	if c, _ := nw.nodes["n4"].Conf(); !c.IsLearner("n4") {
		t.Fatalf("n4 did not learn it is a learner: %s", c)
	}
	nw.requireConverged("n1")

	// It never campaigns: many election timeouts with no leader heartbeat.
	nw.isolate("n4")
	for i := 0; i < 20*DefaultElectionTicks; i++ {
		nw.tick("n4")
	}
	if r := nw.nodes["n4"]; r.Role() != Follower || r.Term() != nw.nodes["n1"].Term() {
		t.Fatalf("a learner campaigned: role %s term %d", r.Role(), r.Term())
	}
	nw.heal()

	// Asked, it grants an up-to-date candidate its one vote of the term; a
	// candidate never asks it, and never counts its vote.
	n4 := nw.nodes["n4"]
	if err := n4.Step(Message{Type: MsgVoteRequest, From: "n2", To: "n4", Term: n4.Term() + 1, LastLogIndex: 99, LastLogTerm: 99}); err != nil {
		t.Fatal(err)
	}
	rd := n4.Ready()
	n4.Advance()
	if len(rd.Messages) != 1 || !rd.Messages[0].VoteGranted || rd.HardState == nil || rd.HardState.Vote != "n2" {
		t.Fatalf("a learner asked by an up-to-date candidate: %+v", rd)
	}
	nw.isolate("n1") // n1 steps down eventually; make n2 campaign
	nw.campaign("n2")
	for _, m := range nw.queue {
		if m.Type == MsgVoteRequest && m.To == "n4" {
			t.Fatal("a candidate asked a learner for its vote")
		}
	}
	n2 := nw.nodes["n2"]
	if err := n2.Step(Message{Type: MsgVoteResponse, From: "n4", To: "n2", Term: n2.Term(), VoteGranted: true}); err != nil {
		t.Fatal(err)
	}
	if n2.Role() != Candidate {
		t.Fatalf("a learner's vote was counted: %s is %s", "n2", n2.Role())
	}
	nw.heal()
	nw.deliverAll()

	// It never counts toward a commit: with n2 and n3 cut off, the leader and
	// the learner alone commit nothing.
	L := nw.leaders()[0]
	nw.isolate("n2")
	nw.isolate("n3")
	before := nw.nodes[L].CommitIndex()
	nw.proposeNoDeliver(L, "uncommittable")
	for i := 0; i < 10; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if nw.nodes[L].CommitIndex() != before {
		t.Fatalf("a learner's acknowledgement advanced the commit from %d to %d", before, nw.nodes[L].CommitIndex())
	}
	nw.heal()
	nw.heartbeatRounds()
	nw.requireConverged(L)
}

// TestPromoteNeedsTheNewMajority (INV-MB3): promoting the learner n4 appends a
// joint entry {n1,n2,n3,n4}/{n1,n2,n3}; with n3 and n4 cut off, n1 and n2 are a
// majority of the OLD set but not of the new — nothing commits; with n3 back
// (three of four) the joint entry commits, the leader appends the final entry,
// and the change settles with n4 a voter.
func TestPromoteNeedsTheNewMajority(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 2)
	nw.electLeader("n1")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n4"}})
	nw.heartbeatRounds()
	nw.isolate("n3")
	nw.isolate("n4")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: Promote, Member: Member{ID: "n4"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	joint, jointIdx := nw.nodes["n1"].Conf()
	if !joint.Joint() || !joint.IsVoter("n4") || len(joint.Outgoing) != 3 {
		t.Fatalf("the leader did not adopt the joint configuration: %s", joint)
	}
	for i := 0; i < 10; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if c := nw.nodes["n1"].CommitIndex(); c >= jointIdx {
		t.Fatalf("the joint entry committed with only n1 and n2 (commit %d, entry %d): the new majority was not required", c, jointIdx)
	}
	// n3 rejoins: n1, n2, n3 are three of the four new voters and all of the old.
	nw.blocked = map[[2]NodeID]bool{}
	nw.isolate("n4")
	nw.settleChange("n1")
	final, _ := nw.nodes["n1"].Conf()
	if final.Joint() || !final.IsVoter("n4") || len(final.Voters) != 4 {
		t.Fatalf("after the change: %s", final)
	}
	es := nw.confEntries("n1")
	if len(es) != 3 || !es[1].Joint() || es[2].Joint() || !es[2].Equal(Final(es[1])) {
		t.Fatalf("configuration entries: %v", es)
	}
	nw.heal()
	nw.heartbeatRounds()
	nw.requireConverged("n1")
	if c, _ := nw.nodes["n4"].Conf(); !c.IsVoter("n4") {
		t.Fatalf("n4 did not learn it is a voter: %s", c)
	}
}

// TestRemoveVoterNeedsTheOldMajority (INV-MB3): removing n4 from {n1..n4} makes
// the joint entry need three of the old four, not only two of the new three:
// with n3 and n4 cut off, n1 and n2 are a majority of the NEW set and nothing
// commits; with n3 back it does, and n4 — still a voter of the outgoing set
// while joint — is not needed.
func TestRemoveVoterNeedsTheOldMajority(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3", "n4"}, 3)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	nw.isolate("n3")
	nw.isolate("n4")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: RemoveVoter, Member: Member{ID: "n4"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	joint, jointIdx := nw.nodes["n1"].Conf()
	if !joint.Joint() || joint.IsVoter("n4") != true || len(joint.Voters) != 3 {
		t.Fatalf("joint: %s", joint)
	}
	for i := 0; i < 10; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if c := nw.nodes["n1"].CommitIndex(); c >= jointIdx {
		t.Fatalf("the joint entry committed with two of the four old voters (commit %d, entry %d)", c, jointIdx)
	}
	nw.blocked = map[[2]NodeID]bool{}
	nw.isolate("n4")
	nw.settleChange("n1")
	final, _ := nw.nodes["n1"].Conf()
	if final.Joint() || final.IsMember("n4") || len(final.Voters) != 3 {
		t.Fatalf("after the removal: %s", final)
	}
	// The leader no longer replicates to n4.
	nw.heal()
	nw.propose("n1", "b")
	nw.heartbeatRounds()
	for _, m := range nw.queue {
		if m.To == "n4" {
			t.Fatalf("the leader still sends to the removed n4: %s", m.Type)
		}
	}
}

// TestLeaderRemovedStepsDownOnceTheFinalEntryCommits (docs/MEMBERSHIP.md §5):
// the leader removes itself: it keeps leading through the joint phase without
// counting itself in the new set, appends the final entry when the joint one
// commits, steps down once the final entry commits, and never campaigns again;
// the remaining voters elect a leader and the group continues without it.
func TestLeaderRemovedStepsDownOnceTheFinalEntryCommits(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 4)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: RemoveVoter, Member: Member{ID: "n1"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	// Joint: the new set is {n2,n3}; the leader alone acknowledging commits
	// nothing (it is not in the new set), and n2 alone is one of two.
	jointIdx := nw.nodes["n1"].LastIndex()
	nw.isolate("n3")
	for i := 0; i < 6; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if c := nw.nodes["n1"].CommitIndex(); c >= jointIdx {
		t.Fatalf("a removed leader counted itself toward the new majority (commit %d)", c)
	}
	nw.heal()
	for i := 0; i < 60 && nw.nodes["n1"].Role() == Leader; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if nw.nodes["n1"].Role() != Follower {
		t.Fatalf("the removed leader did not step down: %s", nw.nodes["n1"].Role())
	}
	if c, _ := nw.nodes["n1"].Conf(); c.IsMember("n1") || c.Joint() {
		t.Fatalf("n1's configuration after its removal: %s", c)
	}
	es := nw.confEntries("n2")
	if len(es) != 2 || !es[0].Joint() || es[1].Joint() || es[1].IsMember("n1") {
		t.Fatalf("n2's configuration entries: %v", es)
	}
	// It never campaigns again.
	term := nw.nodes["n1"].Term()
	for i := 0; i < 20*DefaultElectionTicks; i++ {
		nw.tick("n1")
	}
	if r := nw.nodes["n1"]; r.Role() != Follower || r.Term() != term {
		t.Fatalf("a removed node campaigned: role %s term %d", r.Role(), r.Term())
	}
	// The group continues without it.
	var L NodeID
	for i := 0; i < 100 && L == ""; i++ {
		nw.tickAll()
		nw.deliverAll()
		if ls := nw.leaders(); len(ls) == 1 {
			L = ls[0]
		}
	}
	if L == "" || L == "n1" {
		t.Fatalf("leader after the removal: %q", L)
	}
	nw.propose(L, "b")
	nw.heartbeatRounds() // followers learn the commit from the next AppendEntries
	if nw.nodes["n2"].CommitIndex() != nw.nodes["n3"].CommitIndex() || nw.nodes["n2"].CommitIndex() <= jointIdx+1 {
		t.Fatalf("the group did not commit without n1: %d %d", nw.nodes["n2"].CommitIndex(), nw.nodes["n3"].CommitIndex())
	}
	if nw.nodes["n1"].CommitIndex() > jointIdx+1 {
		t.Fatalf("the removed node kept receiving the log: commit %d", nw.nodes["n1"].CommitIndex())
	}
}

// TestCandidateThatLearnsItIsRemovedStopsCampaigning: n3 is cut off and
// campaigning when the leader removes it. Its stale vote requests move nobody
// (§5). A leader's AppendEntries at n3's own term — the entries it missed,
// the configuration entries among them — makes it a follower that knows it is
// not a voter and never campaigns again. (A removed node whose term outran the
// group's never receives such a message — leaders do not send to non-members
// — and keeps campaigning, refused and contained, until it is stopped:
// docs/MEMBERSHIP.md §10.)
func TestCandidateThatLearnsItIsRemovedStopsCampaigning(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 5)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	nw.isolate("n3")
	nw.campaign("n3")
	if nw.nodes["n3"].Role() != Candidate {
		t.Fatalf("n3 is %s, want Candidate", nw.nodes["n3"].Role())
	}
	nw.change("n1", ConfChange{Type: RemoveVoter, Member: Member{ID: "n3"}})
	nw.heal()
	// n3's stale vote requests reach n1 and n2: refused, terms untouched (§5).
	term1 := nw.nodes["n1"].Term()
	nw.deliverAll()
	if nw.nodes["n1"].Term() != term1 || nw.nodes["n1"].Role() != Leader {
		t.Fatalf("a removed candidate's vote request moved the leader: term %d role %s", nw.nodes["n1"].Term(), nw.nodes["n1"].Role())
	}
	// The leader does not send to n3 any more; hand n3 the entries it missed
	// the way a leader of its own term would (a late AppendEntries).
	n1, n3 := nw.nodes["n1"], nw.nodes["n3"]
	es, _ := nw.logs["n1"].Slice(2, n1.LastIndex()+1)
	if err := n3.Step(Message{Type: MsgAppendRequest, From: "n1", To: "n3", Term: n3.Term(), PrevLogIndex: 1, PrevLogTerm: 1, Entries: es, LeaderCommit: n1.CommitIndex()}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n3")
	if c, _ := n3.Conf(); n3.Role() != Follower || c.IsMember("n3") {
		t.Fatalf("n3 after learning its removal: role %s conf %s", n3.Role(), c)
	}
	term3 := n3.Term()
	for i := 0; i < 20*DefaultElectionTicks; i++ {
		nw.tick("n3")
	}
	if n3.Role() != Follower || n3.Term() != term3 {
		t.Fatalf("a removed node campaigned: role %s term %d", n3.Role(), n3.Term())
	}
}

// TestRemovedNodeCannotDeposeOrLead (INV-MB4): n3 sits out its removal in a
// partition, believing itself a voter of the old set; it campaigns in ever
// higher terms. When it returns its vote requests are refused WITHOUT the
// members adopting its term — the leader keeps its term and its seat, keeps
// committing, and n3 never leads.
func TestRemovedNodeCannotDeposeOrLead(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 6)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	nw.isolate("n3")
	nw.change("n1", ConfChange{Type: RemoveVoter, Member: Member{ID: "n3"}})
	nw.propose("n1", "b")
	// n3 inflates its term while isolated.
	for i := 0; i < 6*DefaultElectionTicks; i++ {
		nw.tick("n3")
	}
	if nw.nodes["n3"].Term() <= nw.nodes["n1"].Term() {
		t.Fatalf("premise: n3's term %d should exceed the leader's %d", nw.nodes["n3"].Term(), nw.nodes["n1"].Term())
	}
	term1 := nw.nodes["n1"].Term()
	nw.heal()
	for i := 0; i < 40; i++ {
		nw.tickAll()
		nw.deliverAll()
		if nw.nodes["n3"].Role() == Leader {
			t.Fatal("a removed node became leader")
		}
	}
	if r := nw.nodes["n1"]; r.Role() != Leader || r.Term() != term1 {
		t.Fatalf("the removed node's inflated term reached the leader: role %s term %d (was %d)", r.Role(), r.Term(), term1)
	}
	if nw.nodes["n2"].Term() != term1 {
		t.Fatalf("n2 adopted the removed node's term: %d", nw.nodes["n2"].Term())
	}
	nw.propose("n1", "c")
	nw.heartbeatRounds()
	if nw.nodes["n2"].CommitIndex() != nw.nodes["n1"].CommitIndex() || nw.nodes["n1"].CommitIndex() != nw.nodes["n1"].LastIndex() {
		t.Fatalf("the group stopped committing after the removed node returned: %d %d", nw.nodes["n1"].CommitIndex(), nw.nodes["n2"].CommitIndex())
	}
}

// TestConfigurationRevertsWhenItsEntryIsTruncated: a leader appends a
// configuration entry that reaches nobody, is cut off, and is deposed; when it
// rejoins the new leader's log replaces the entry and the node's configuration
// reverts to the one before it (docs/MEMBERSHIP.md §2).
func TestConfigurationRevertsWhenItsEntryIsTruncated(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 7)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	nw.isolate("n1")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: AddLearner, Member: Member{ID: "n4"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	nw.queue = nil // the entry left n1 for nobody
	if c, idx := nw.nodes["n1"].Conf(); !c.IsLearner("n4") || idx == 0 {
		t.Fatalf("the leader did not adopt its own entry: %s at %d", c, idx)
	}
	nw.electLeader("n2")
	nw.propose("n2", "b")
	nw.heal()
	nw.heartbeatRounds()
	nw.requireConverged("n2")
	if c, idx := nw.nodes["n1"].Conf(); c.IsMember("n4") || idx != 0 || !c.Equal(voters("n1", "n2", "n3")) {
		t.Fatalf("n1's configuration did not revert with its truncated entry: %s at %d", c, idx)
	}
	if len(nw.confEntries("n1")) != 0 {
		t.Fatal("a truncated configuration entry survived in n1's log")
	}
}

// TestSnapshotInstallAdoptsTheConfiguration (INV-MB8): a joiner added as a
// learner while the leader compacts its log catches up by a snapshot and
// adopts the configuration the snapshot carries, in which it is a learner;
// promoted afterwards, its vote counts.
func TestSnapshotInstallAdoptsTheConfiguration(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 8)
	nw.electLeader("n1")
	nw.isolate("n4")
	nw.proposeN("n1", "a", 6)
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n4"}})
	nw.proposeN("n1", "b", 3)
	nw.compact("n1", nw.nodes["n1"].AppliedIndex())
	nw.heal()
	for i := 0; i < 6; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if len(nw.snapshotsInstalled["n4"]) == 0 {
		t.Fatal("the joiner did not install a snapshot")
	}
	if c, idx := nw.nodes["n4"].Conf(); !c.IsLearner("n4") || idx != 0 {
		t.Fatalf("n4's configuration after the install: %s at %d", c, idx)
	}
	if got, err := nw.nodes["n4"].ConfAt(nw.nodes["n4"].AppliedIndex()); err != nil || !got.IsLearner("n4") {
		t.Fatalf("ConfAt the snapshot's index: %s %v", got, err)
	}
	nw.change("n1", ConfChange{Type: Promote, Member: Member{ID: "n4"}})
	nw.heartbeatRounds()
	nw.requireConverged("n1")
	// n4's acknowledgement now counts: with n2 and n3 cut off, n1 and n4 are
	// two of the four... not a majority; with n2 back they are three of four.
	nw.isolate("n3")
	nw.propose("n1", "c")
	if nw.nodes["n1"].CommitIndex() != nw.nodes["n1"].LastIndex() {
		t.Fatal("three of four voters (n4 among them) did not commit")
	}
	nw.heal()
}

// TestCompactRecordsTheBaseConfiguration: after a configuration entry is
// applied and compacted away, the configuration is the base one — the same
// membership, from the boundary — and a snapshot at the boundary names it.
func TestCompactRecordsTheBaseConfiguration(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 9)
	nw.electLeader("n1")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n4"}})
	nw.proposeN("n1", "a", 2)
	nw.heartbeatRounds()
	before, idx := nw.nodes["n1"].Conf()
	if idx == 0 {
		t.Fatal("premise: the configuration comes from a log entry")
	}
	nw.compact("n1", nw.nodes["n1"].AppliedIndex())
	after, idx2 := nw.nodes["n1"].Conf()
	if idx2 != 0 || !after.Equal(before) {
		t.Fatalf("after compaction: %s at %d, want %s at 0", after, idx2, before)
	}
	base, _ := nw.nodes["n1"].Boundary()
	if c, err := nw.nodes["n1"].ConfAt(base); err != nil || !c.Equal(before) {
		t.Fatalf("ConfAt(boundary): %s %v", c, err)
	}
	if _, err := nw.nodes["n1"].ConfAt(base - 1); err == nil {
		t.Fatal("ConfAt below the boundary answered")
	}
}

// TestOneConfigurationChangeAtATime: a second change while one is pending — an
// uncommitted entry, or a joint configuration — is refused, and a follower
// refuses every change.
func TestOneConfigurationChangeAtATime(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4", "n5"}, voters("n1", "n2", "n3"), 10)
	nw.electLeader("n1")
	if err := nw.nodes["n2"].ProposeConfChange(ConfChange{Type: AddLearner, Member: Member{ID: "n4"}}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("a follower accepted a change: %v", err)
	}
	nw.isolate("n3")
	nw.isolate("n2")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: AddLearner, Member: Member{ID: "n4"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: AddLearner, Member: Member{ID: "n5"}}); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("a second change while the first is uncommitted: %v", err)
	}
	nw.heal()
	nw.settleChange("n1")
	// A joint entry, while it is uncommitted — n1 and n2 are a majority of the
	// old set but not of the new — is pending too.
	nw.isolate("n3")
	nw.isolate("n4")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: Promote, Member: Member{ID: "n4"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	nw.deliverAll()
	if c, _ := nw.nodes["n1"].Conf(); !nw.nodes["n1"].ConfPending() || !c.Joint() {
		t.Fatalf("premise: the promotion should be pending and joint: %s", c)
	}
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: AddLearner, Member: Member{ID: "n5"}}); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("a change during a joint configuration: %v", err)
	}
	nw.heal()
	nw.settleChange("n1")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n5"}})
}

// TestNewLeaderCompletesAnInterruptedTransition: the leader commits the joint
// entry and appends the final one, then is cut off before the final entry
// reaches anyone. The new leader — elected under the joint configuration, by
// both majorities — finds the committed joint entry in its log and appends the
// final entry itself; the group settles with n4 a voter.
func TestNewLeaderCompletesAnInterruptedTransition(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 11)
	nw.electLeader("n1")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n4"}})
	nw.heartbeatRounds()
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: Promote, Member: Member{ID: "n4"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	// Deliver one message at a time: the joint entry reaches everyone, their
	// acknowledgements commit it on n1, and n1 appends the final entry — whose
	// AppendEntries are then in the queue, and never leave.
	for i := 0; i < 1000 && len(nw.confEntries("n1")) < 3; i++ {
		if !nw.deliverOne() {
			nw.tickAll()
		}
	}
	if len(nw.confEntries("n1")) != 3 {
		t.Fatal("premise: the leader did not append the final entry")
	}
	nw.queue = nil
	nw.isolate("n1")
	for _, id := range []NodeID{"n2", "n3", "n4"} {
		if es := nw.confEntries(id); len(es) != 2 || !es[1].Joint() {
			t.Fatalf("%s should hold the joint entry only: %v", id, es)
		}
	}
	nw.electLeader("n2")
	nw.settleChange("n2")
	final, idx := nw.nodes["n2"].Conf()
	if final.Joint() || !final.IsVoter("n4") {
		t.Fatalf("the new leader did not complete the transition: %s", final)
	}
	if es := nw.confEntries("n2"); len(es) != 3 || !es[2].Equal(Final(es[1])) {
		t.Fatalf("n2's configuration entries: %v", es)
	}
	// The final entry is n2's own: appended in its term, not inherited.
	if e, _ := nw.logs["n2"].At(idx); e.Term != nw.nodes["n2"].Term() {
		t.Fatalf("the final entry at %d has term %d; the new leader's term is %d", idx, e.Term, nw.nodes["n2"].Term())
	}
	nw.heal()
	nw.heartbeatRounds()
	nw.requireConverged("n2")
	if es := nw.confEntries("n1"); len(es) != 3 || !es[2].Equal(Final(es[1])) {
		t.Fatalf("n1's own final entry was replaced by the new leader's: %v", es)
	}
}

// TestChangeCompletesWithAVoterUnavailable: a follower is down throughout; a
// learner is added and promoted with the remaining majority of both sets.
func TestChangeCompletesWithAVoterUnavailable(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 12)
	nw.electLeader("n1")
	nw.isolate("n3")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n4"}})
	nw.heartbeatRounds()
	nw.change("n1", ConfChange{Type: Promote, Member: Member{ID: "n4"}})
	final, _ := nw.nodes["n1"].Conf()
	if final.Joint() || !final.IsVoter("n4") || len(final.Voters) != 4 {
		t.Fatalf("after the change: %s", final)
	}
	nw.change("n1", ConfChange{Type: RemoveVoter, Member: Member{ID: "n2"}})
	final, _ = nw.nodes["n1"].Conf()
	if final.IsMember("n2") || len(final.Voters) != 3 {
		t.Fatalf("after the removal: %s", final)
	}
	nw.heal()
	nw.heartbeatRounds()
	nw.requireConverged("n1")
}

// TestRecoveryUsesTheLatestConfigurationInTheLog: a core built from a recovered
// log takes its configuration from the latest configuration entry, whether or
// not it is committed; a core whose log holds none takes the base
// configuration; an undecodable configuration entry is refused.
func TestRecoveryUsesTheLatestConfigurationInTheLog(t *testing.T) {
	base := voters("n1", "n2", "n3")
	c1, _ := ConfChange{Type: AddLearner, Member: Member{ID: "n4"}}.Apply(base)
	c2, _ := ConfChange{Type: Promote, Member: Member{ID: "n4"}}.Apply(c1)
	lg := replication.NewMemoryLog()
	if err := lg.Append(
		Entry{Index: 1, Term: 1},
		Entry{Index: 2, Term: 1, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(c1)},
		Entry{Index: 3, Term: 1, Data: []byte("x")},
		Entry{Index: 4, Term: 2, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(c2)},
	); err != nil {
		t.Fatal(err)
	}
	_ = lg.Commit(3)
	r, err := New(Config{ID: "n1", Conf: &base, Log: lg, Rand: rand.New(rand.NewSource(1)), Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	if c, idx := r.Conf(); !c.Equal(c2) || idx != 4 || !r.ConfPending() {
		t.Fatalf("recovered configuration: %s at %d (pending %v)", c, idx, r.ConfPending())
	}
	if c, err := r.ConfAt(3); err != nil || !c.Equal(c1) {
		t.Fatalf("ConfAt(3): %s %v", c, err)
	}
	if c, err := r.ConfAt(1); err != nil || !c.Equal(base) {
		t.Fatalf("ConfAt(1): %s %v", c, err)
	}
	plain := replication.NewMemoryLog()
	_ = plain.Append(Entry{Index: 1, Term: 1})
	r2, err := New(Config{ID: "n1", Conf: &base, Log: plain, Rand: rand.New(rand.NewSource(1)), Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if c, idx := r2.Conf(); !c.Equal(base) || idx != 0 {
		t.Fatalf("base configuration: %s at %d", c, idx)
	}
	bad := replication.NewMemoryLog()
	_ = bad.Append(Entry{Index: 1, Term: 1, Type: replication.EntryConfig, Data: []byte{0xff}})
	if _, err := New(Config{ID: "n1", Conf: &base, Log: bad, Rand: rand.New(rand.NewSource(1)), Term: 1}); !errors.Is(err, replication.ErrInvalidConfiguration) {
		t.Fatalf("an undecodable configuration entry was accepted: %v", err)
	}
	// A joiner: no configuration, never a candidate.
	empty := Configuration{}
	j, err := New(Config{ID: "n9", Conf: &empty, Log: replication.NewMemoryLog(), Rand: rand.New(rand.NewSource(1))})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10*DefaultElectionTicks; i++ {
		j.Tick()
	}
	if j.Role() != Follower || j.Term() != 0 || j.IsVoter() {
		t.Fatalf("a joiner campaigned: %s term %d", j.Role(), j.Term())
	}
}

// TestNonMemberVoteRequestDoesNotBumpTheTerm pins the containment rule
// directly on one core: a vote request from a node outside the configuration
// is refused and the term stays; from a member it is honoured.
func TestNonMemberVoteRequestIsHeardOnlyWithAnUpToDateLog(t *testing.T) {
	lg := replication.NewMemoryLog()
	if err := lg.Append(Entry{Index: 1, Term: 2}, Entry{Index: 2, Term: 2}); err != nil {
		t.Fatal(err)
	}
	r, err := New(Config{ID: "a", Peers: ids(3), Rand: rand.New(rand.NewSource(1)), Log: lg, Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	// A stranger whose log is behind ours — a removed node's lacks the entry
	// that removed it — is refused without its term being adopted.
	_ = r.Step(Message{Type: MsgVoteRequest, From: "z", To: "a", Term: 50, LastLogIndex: 1, LastLogTerm: 2})
	rd := r.Ready()
	r.Advance()
	if r.Term() != 2 || len(rd.Messages) != 1 || rd.Messages[0].VoteGranted || rd.Messages[0].Term != 2 || rd.HardState != nil {
		t.Fatalf("a stranger with a stale log: term %d, %+v", r.Term(), rd)
	}
	_ = r.Step(Message{Type: MsgVoteResponse, From: "z", To: "a", Term: 50, VoteGranted: true})
	if r.Term() != 2 {
		t.Fatalf("a stranger's response bumped the term to %d", r.Term())
	}
	// A stranger whose log is at least as up to date may be a voter of a
	// configuration this node has not learned: it is heard.
	_ = r.Step(Message{Type: MsgVoteRequest, From: "z", To: "a", Term: 60, LastLogIndex: 2, LastLogTerm: 2})
	rd = r.Ready()
	r.Advance()
	if r.Term() != 60 || len(rd.Messages) != 1 || !rd.Messages[0].VoteGranted {
		t.Fatalf("a stranger with an up-to-date log: term %d, %+v", r.Term(), rd)
	}
	_ = r.Step(Message{Type: MsgVoteRequest, From: "b", To: "a", Term: 61, LastLogIndex: 2, LastLogTerm: 2})
	rd = r.Ready()
	r.Advance()
	if r.Term() != 61 || !rd.Messages[0].VoteGranted {
		t.Fatalf("a member's vote request: term %d granted %v", r.Term(), rd.Messages[0].VoteGranted)
	}
	// A leader's entries from a non-member are processed: a joiner learns the
	// group from them.
	empty := Configuration{}
	j, _ := New(Config{ID: "j", Conf: &empty, Log: replication.NewMemoryLog(), Rand: rand.New(rand.NewSource(1))})
	conf := voters("a", "b")
	conf.Learners = members("j")
	if err := j.Step(Message{Type: MsgAppendRequest, From: "a", To: "j", Term: 3, Entries: []Entry{{Index: 1, Term: 3, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(conf)}}}); err != nil {
		t.Fatal(err)
	}
	if c, _ := j.Conf(); j.Term() != 3 || !c.IsLearner("j") {
		t.Fatalf("a joiner did not learn from the leader: term %d conf %s", j.Term(), c)
	}
}

// TestConfigurationEntriesTravelInMessages: the codec carries an entry's type
// and a configuration entry's bytes, refuses a configuration entry that does
// not decode and an unknown entry type, and a snapshot offer carries its
// configuration.
func TestConfigurationEntriesTravelInMessages(t *testing.T) {
	conf := voters("a", "b", "c")
	conf.Learners = members("d")
	m := Message{Type: MsgAppendRequest, Term: 3, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1,
		Entries: []Entry{{Index: 2, Term: 3, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(conf)}, {Index: 3, Term: 3, Data: []byte("x")}}}
	got, err := Unmarshal(m.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if got.Entries[0].Type != replication.EntryConfig || got.Entries[1].Type != replication.EntryNormal {
		t.Fatalf("entry types: %v %v", got.Entries[0].Type, got.Entries[1].Type)
	}
	if c, err := replication.DecodeConfiguration(got.Entries[0].Data); err != nil || !c.Equal(conf) {
		t.Fatalf("configuration entry: %s %v", c, err)
	}
	bad := Message{Type: MsgAppendRequest, Term: 3, Entries: []Entry{{Index: 2, Term: 3, Type: replication.EntryConfig, Data: []byte{0xff, 0xff}}}}
	if _, err := Unmarshal(bad.Marshal()); !errors.Is(err, ErrMalformedMessage) {
		t.Fatalf("an undecodable configuration entry was accepted: %v", err)
	}
	unknown := Message{Type: MsgAppendRequest, Term: 3, Entries: []Entry{{Index: 2, Term: 3, Type: 7, Data: []byte("x")}}}
	if _, err := Unmarshal(unknown.Marshal()); !errors.Is(err, ErrMalformedMessage) {
		t.Fatalf("an unknown entry type was accepted: %v", err)
	}
	snap := Message{Type: MsgSnapshot, Term: 3, SnapshotIndex: 9, SnapshotTerm: 2, Seq: 1, Conf: &conf}
	got, err = Unmarshal(snap.Marshal())
	if err != nil || got.Conf == nil || !got.Conf.Equal(conf) {
		t.Fatalf("a snapshot offer's configuration: %v %v", got.Conf, err)
	}
	if got, err := Unmarshal(Message{Type: MsgSnapshot, Term: 3, SnapshotIndex: 9, SnapshotTerm: 2}.Marshal()); err != nil || got.Conf != nil {
		t.Fatalf("a snapshot offer without a configuration: %v %v", got.Conf, err)
	}
}

// TestSnapshotWithoutAConfigurationIsRefused: the core refuses to install a
// snapshot that carries no configuration — it would not know its group.
func TestSnapshotWithoutAConfigurationIsRefused(t *testing.T) {
	r, lg := followerWith(t, 3, 3, 2)
	if err := r.Step(Message{Type: MsgSnapshot, From: "n1", To: "n2", Term: 2, SnapshotIndex: 15, SnapshotTerm: 1}); err != nil {
		t.Fatal(err)
	}
	rd := drainReady(r)
	if rd.Snapshot != nil || len(rd.Messages) != 1 || rd.Messages[0].Success {
		t.Fatalf("a snapshot without its configuration was installed: %+v", rd)
	}
	if b, _ := lg.Boundary(); b != 0 {
		t.Fatal("the log was reset")
	}
	// Nor one whose configuration has no voters: no group applies without one.
	learnersOnly := Configuration{Learners: members("n2")}
	if err := r.Step(Message{Type: MsgSnapshot, From: "n1", To: "n2", Term: 2, SnapshotIndex: 15, SnapshotTerm: 1, Conf: &learnersOnly}); err != nil {
		t.Fatal(err)
	}
	if rd := drainReady(r); rd.Snapshot != nil || len(rd.Messages) != 1 || rd.Messages[0].Success {
		t.Fatalf("a snapshot whose configuration has no voters was installed: %+v", rd)
	}
}

// TestBaseConfigurationHoldsOnlyAtItsIndex: a recovered core whose log keeps
// entries below its snapshot (Phase 14's retain) knows its base configuration
// at the snapshot's index, not at the log's boundary. Below a configuration
// entry that precedes the snapshot's index, ConfAt answers ErrConfUnknown — it
// never passes the snapshot's configuration off as an earlier one. A base that
// contradicts the log's configuration entry at or below its index, or that
// claims an index past the log, is refused. A compaction below the base's index
// keeps it.
func TestBaseConfigurationHoldsOnlyAtItsIndex(t *testing.T) {
	base := voters("n1", "n2", "n3")
	c1, _ := ConfChange{Type: AddLearner, Member: Member{ID: "n4"}}.Apply(base)
	build := func() *replication.MemoryLog {
		lg := replication.NewMemoryLog()
		if err := lg.InstallSnapshot(2, 1); err != nil {
			t.Fatal(err)
		}
		if err := lg.Append(
			Entry{Index: 3, Term: 1, Data: []byte("a")},
			Entry{Index: 4, Term: 1, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(c1)},
			Entry{Index: 5, Term: 1, Data: []byte("b")},
			Entry{Index: 6, Term: 1, Data: []byte("c")},
		); err != nil {
			t.Fatal(err)
		}
		if err := lg.Commit(6); err != nil {
			t.Fatal(err)
		}
		return lg
	}
	lg := build()
	r, err := New(Config{ID: "n1", Conf: &c1, ConfIndex: 5, Log: lg, Rand: rand.New(rand.NewSource(1)), Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if c, idx := r.Conf(); !c.Equal(c1) || idx != 4 {
		t.Fatalf("recovered configuration %s at %d", c, idx)
	}
	for _, i := range []uint64{2, 3} {
		if c, err := r.ConfAt(i); !errors.Is(err, ErrConfUnknown) {
			t.Fatalf("ConfAt(%d) below the entry at 4 answered %s %v", i, c, err)
		}
	}
	for _, i := range []uint64{4, 5, 6} {
		if c, err := r.ConfAt(i); err != nil || !c.Equal(c1) {
			t.Fatalf("ConfAt(%d): %s %v", i, c, err)
		}
	}
	// A compaction below the base's index keeps the base where it holds.
	if err := lg.Apply(6); err != nil {
		t.Fatal(err)
	}
	if err := r.Compact(3); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConfAt(3); !errors.Is(err, ErrConfUnknown) {
		t.Fatalf("ConfAt(3) after compacting through 3: %v", err)
	}
	if err := r.Compact(5); err != nil {
		t.Fatal(err)
	}
	if c, err := r.ConfAt(5); err != nil || !c.Equal(c1) {
		t.Fatalf("ConfAt(5) after compacting through 5: %s %v", c, err)
	}
	if c, idx := r.Conf(); !c.Equal(c1) || idx != 0 {
		t.Fatalf("after compacting the entry away: %s at %d", c, idx)
	}

	// With no configuration entry between the boundary and the base's index,
	// the base is the boundary's too.
	plain := replication.NewMemoryLog()
	_ = plain.InstallSnapshot(2, 1)
	_ = plain.Append(Entry{Index: 3, Term: 1}, Entry{Index: 4, Term: 1}, Entry{Index: 5, Term: 1})
	r2, err := New(Config{ID: "n1", Conf: &base, ConfIndex: 5, Log: plain, Rand: rand.New(rand.NewSource(1)), Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if c, err := r2.ConfAt(2); err != nil || !c.Equal(base) {
		t.Fatalf("ConfAt(boundary) with no entry in between: %s %v", c, err)
	}

	// A base contradicting the log's configuration at its index is refused.
	if _, err := New(Config{ID: "n1", Conf: &base, ConfIndex: 5, Log: build(), Rand: rand.New(rand.NewSource(1)), Term: 1}); !errors.Is(err, ErrConfMismatch) {
		t.Fatalf("a base contradicting the log: %v", err)
	}
	// A base claimed past the log's last index is refused.
	if _, err := New(Config{ID: "n1", Conf: &c1, ConfIndex: 7, Log: build(), Rand: rand.New(rand.NewSource(1)), Term: 1}); !errors.Is(err, ErrConfMismatch) {
		t.Fatalf("a base past the log: %v", err)
	}
	// An entry above the base's index may differ: it is a later change.
	if _, err := New(Config{ID: "n1", Conf: &base, ConfIndex: 3, Log: build(), Rand: rand.New(rand.NewSource(1)), Term: 1}); err != nil {
		t.Fatalf("a base below a later configuration entry: %v", err)
	}
}

// TestJoinerKnowsNoConfigurationUntilItLearnsOne: a joiner starts with an empty
// configuration — it never campaigns, and ConfAt answers ErrConfUnknown rather
// than an empty configuration (which no snapshot may carry). Entries replicated
// before the first configuration entry it receives stay unknown; from that
// entry on it knows. A compaction below that entry moves its base up to it,
// since it is committed.
func TestJoinerKnowsNoConfigurationUntilItLearnsOne(t *testing.T) {
	base := voters("n1", "n2", "n3")
	c1, _ := ConfChange{Type: AddLearner, Member: Member{ID: "n4"}}.Apply(base)
	lg := replication.NewMemoryLog()
	empty := Configuration{}
	r, err := New(Config{ID: "n4", Conf: &empty, Log: lg, Rand: rand.New(rand.NewSource(1))})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		r.Tick()
	}
	if rd := drainReady(r); len(rd.Messages) != 0 || r.Role() != Follower {
		t.Fatalf("a joiner campaigned: %+v", rd.Messages)
	}
	if _, err := r.ConfAt(0); !errors.Is(err, ErrConfUnknown) {
		t.Fatalf("a joiner's ConfAt(0): %v", err)
	}
	if err := r.Step(Message{Type: MsgAppendRequest, From: "n1", To: "n4", Term: 1, LeaderCommit: 3, Entries: []Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Type: replication.EntryConfig, Data: replication.EncodeConfiguration(c1)},
		{Index: 3, Term: 1, Data: []byte("b")},
	}}); err != nil {
		t.Fatal(err)
	}
	drainReady(r)
	if c, idx := r.Conf(); !c.Equal(c1) || idx != 2 {
		t.Fatalf("the joiner's configuration after the entries: %s at %d", c, idx)
	}
	if _, err := r.ConfAt(1); !errors.Is(err, ErrConfUnknown) {
		t.Fatalf("ConfAt(1), before the first configuration entry: %v", err)
	}
	if c, err := r.ConfAt(3); err != nil || !c.Equal(c1) {
		t.Fatalf("ConfAt(3): %s %v", c, err)
	}
	if err := lg.Apply(3); err != nil {
		t.Fatal(err)
	}
	if err := r.Compact(1); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConfAt(1); !errors.Is(err, ErrConfUnknown) {
		t.Fatalf("ConfAt(1) after compacting through it: %v", err)
	}
	if c, err := r.ConfAt(2); err != nil || !c.Equal(c1) {
		t.Fatalf("ConfAt(2) after compacting through 1: %s %v", c, err)
	}
	if err := r.Compact(3); err != nil {
		t.Fatal(err)
	}
	if c, err := r.ConfAt(3); err != nil || !c.Equal(c1) {
		t.Fatalf("ConfAt(3) after compacting through 3: %s %v", c, err)
	}
}

// stepDown makes the leader id adopt a higher term from its member from — as a
// removed or restarted node's campaign would — so it must be elected again.
func (nw *network) stepDown(id, from NodeID) {
	nw.t.Helper()
	r := nw.nodes[id]
	if err := r.Step(Message{Type: MsgVoteRequest, From: from, To: id, Term: r.Term() + 5}); err != nil {
		nw.t.Fatal(err)
	}
	nw.drain(id)
	nw.queue = nil
	if r.Role() != Follower {
		nw.t.Fatalf("%s did not step down: %s", id, r.Role())
	}
}

// TestPromotedLearnerThatMissedItsPromotionStillElects is the regression for a
// liveness bug the membership chaos profile found (seed 9): a single voter n1
// promotes the learner n2 — the joint entry {n1,n2}/{n1} needs n2 to commit —
// and steps down before the entry reaches n2. n1 can be elected only with
// n2's vote, and n2 still believes it is a learner. Were a learner to refuse
// every vote, the group would have no leader for ever; it grants, n1 is
// elected, and the promotion completes.
func TestPromotedLearnerThatMissedItsPromotionStillElects(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2"}, voters("n1"), 3)
	nw.electLeader("n1")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n2"}})
	nw.heartbeatRounds()
	if c, _ := nw.nodes["n2"].Conf(); !c.IsLearner("n2") {
		t.Fatalf("n2: %s", c)
	}
	nw.isolate("n2")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: Promote, Member: Member{ID: "n2"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	nw.queue = nil // the joint entry never reaches n2
	nw.stepDown("n1", "n2")
	nw.heal()
	nw.electLeader("n1")
	nw.settleChange("n1")
	if c, _ := nw.nodes["n1"].Conf(); c.Joint() || !c.IsVoter("n2") {
		t.Fatalf("after the election: %s", c)
	}
	nw.heartbeatRounds()
	nw.requireConverged("n1")
}

// TestJoinerWithNoConfigurationVotesForItsPromotion: the same with a joiner
// that received nothing at all — its configuration is empty, so the candidate
// is not a member of it. The candidate's log is up to date, so the joiner
// hears it, grants, and the promotion completes.
func TestJoinerWithNoConfigurationVotesForItsPromotion(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2"}, voters("n1"), 4)
	nw.electLeader("n1")
	nw.isolate("n2")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n2"}}) // commits with n1 alone
	if c, _ := nw.nodes["n2"].Conf(); !c.Empty() {
		t.Fatalf("n2 learned something while isolated: %s", c)
	}
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: Promote, Member: Member{ID: "n2"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	nw.queue = nil
	nw.stepDown("n1", "n2")
	nw.heal()
	nw.electLeader("n1")
	nw.settleChange("n1")
	nw.heartbeatRounds()
	if c, _ := nw.nodes["n2"].Conf(); c.Joint() || !c.IsVoter("n2") {
		t.Fatalf("n2 after the election: %s", c)
	}
	nw.requireConverged("n1")
}

// TestRemovedLeaderThatLostItsLeadershipFinishesItsRemoval is the regression
// for the second liveness bug the simulator found (multi-2x3, seed 83): in a
// group of two voters, n1 removes itself. The joint entry {n2}/{n1,n2} commits,
// n1 appends the final entry {n2} — and loses its leadership before that entry
// reaches n2. n1's latest configuration excludes it, so it may not campaign as
// a voter of it; n2, still in the joint configuration, needs n1's vote, and n1
// refuses it — its log is longer. Nobody could ever be elected. n1 may
// campaign under the configuration its final entry replaced, winning a quorum
// of both; it is elected, commits its removal and steps down, and n2 leads.
func TestRemovedLeaderThatLostItsLeadershipFinishesItsRemoval(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2"}, voters("n1", "n2"), 5)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: RemoveVoter, Member: Member{ID: "n1"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	n1 := nw.nodes["n1"]
	for i := 0; i < 100; i++ {
		if c, _ := n1.Conf(); !c.Joint() && n1.ConfPending() {
			break
		}
		if !nw.deliverOne() {
			nw.tick("n1")
		}
	}
	if c, _ := n1.Conf(); c.Joint() || !n1.ConfPending() || c.IsMember("n1") {
		t.Fatalf("premise: n1 is not between its joint and final configurations: %s", c)
	}
	nw.queue = nil // the final entry never reaches n2
	if c, _ := nw.nodes["n2"].Conf(); !c.Joint() {
		t.Fatalf("premise: n2 holds %s, not the joint configuration", c)
	}
	nw.stepDown("n1", "n2")
	var leader NodeID
	for i := 0; i < 400 && leader != "n2"; i++ {
		nw.tickAll()
		nw.deliverAll()
		if ls := nw.leaders(); len(ls) == 1 {
			leader = ls[0]
		}
	}
	if leader != "n2" {
		c1, _ := n1.Conf()
		c2, _ := nw.nodes["n2"].Conf()
		t.Fatalf("no leader of the group after the removal: leaders %v; n1 %s %s; n2 %s %s", nw.leaders(), n1.Role(), c1, nw.nodes["n2"].Role(), c2)
	}
	if c, _ := nw.nodes["n2"].Conf(); c.Joint() || nw.nodes["n2"].ConfPending() || c.IsMember("n1") {
		t.Fatalf("n2 leads under %s (pending %v)", c, nw.nodes["n2"].ConfPending())
	}
	if n1.Role() == Leader || n1.IsVoter() {
		t.Fatalf("n1 after its removal: %s voter=%v", n1.Role(), n1.IsVoter())
	}
}

// TestFinalEntryCommitsAtOnceWhenTheLeaderAloneIsItsQuorum is the regression
// for a liveness bug the bounded membership model found (sequence
// remove-leader, crash-follower, remove-follower): the leader of {n1,n2}
// removes n2. The joint entry {n1}/{n1,n2} commits with n2's acknowledgement;
// the leader appends the final entry {n1} — whose quorum is the leader alone.
// No voter will ever acknowledge it, and nothing retried the commit, so the
// change stayed under way until some unrelated proposal came. The leader now
// tries to commit as soon as it appends the final entry.
func TestFinalEntryCommitsAtOnceWhenTheLeaderAloneIsItsQuorum(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2"}, voters("n1", "n2"), 6)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: RemoveVoter, Member: Member{ID: "n2"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	nw.deliverAll() // n2 acknowledges the joint entry; it commits; the final one follows
	n1 := nw.nodes["n1"]
	if c, _ := n1.Conf(); c.Joint() || c.IsMember("n2") {
		t.Fatalf("premise: the final entry was not appended: %s", c)
	}
	if n1.ConfPending() {
		c, idx := n1.Conf()
		t.Fatalf("the final entry %s at %d is not committed (commit %d): nothing will ever commit it", c, idx, n1.CommitIndex())
	}
}

// TestFinalEntryWaitsForTheJointCommit (INV-MB3): the leader appends the final
// configuration only once the joint entry is committed. An ordinary entry
// proposed just before a promotion commits first — its acknowledgements arrive
// while the joint entry is still in flight — and the leader stays joint
// through that commit; the final entry follows the joint entry's own commit.
// Appending it earlier would switch the leader to the final configuration's
// quorum before the joint one was ever satisfied.
func TestFinalEntryWaitsForTheJointCommit(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2", "n3", "n4"}, voters("n1", "n2", "n3"), 2)
	nw.electLeader("n1")
	nw.change("n1", ConfChange{Type: AddLearner, Member: Member{ID: "n4"}})
	nw.heartbeatRounds()
	nw.proposeNoDeliver("n1", "before")
	before := nw.logs["n1"].LastIndex()
	if err := nw.nodes["n1"].ProposeConfChange(ConfChange{Type: Promote, Member: Member{ID: "n4"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	_, jointIdx := nw.nodes["n1"].Conf()
	earlier := false
	for i := 0; i < 10000 && nw.deliverOne(); i++ {
		c, idx := nw.nodes["n1"].Conf()
		commit := nw.nodes["n1"].CommitIndex()
		if commit >= before && commit < jointIdx {
			earlier = true
		}
		if !c.Joint() && idx > jointIdx && commit < jointIdx {
			t.Fatalf("the final configuration %s was appended at %d while the joint entry at %d was uncommitted (commit %d)", c, idx, jointIdx, commit)
		}
	}
	if !earlier {
		t.Fatalf("premise: the entry at %d never committed before the joint entry at %d", before, jointIdx)
	}
	if c, _ := nw.nodes["n1"].Conf(); c.Joint() || !c.IsVoter("n4") || nw.nodes["n1"].ConfPending() {
		t.Fatalf("the promotion did not complete: %s", c)
	}
}
