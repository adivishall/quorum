package raft

import (
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

func ids(n int) []NodeID {
	out := make([]NodeID, n)
	for i := 0; i < n; i++ {
		out[i] = NodeID(string(rune('a' + i)))
	}
	return out
}

// TestSingleNodeElectsAndCommits proves a one-node group elects itself, appends
// the mandatory no-op, and commits it without any peer response (§29, ADR-016).
func TestSingleNodeElectsAndCommits(t *testing.T) {
	nw := newNetwork(t, ids(1), 1)
	nw.electLeader("a")
	r := nw.nodes["a"]
	if r.Role() != Leader {
		t.Fatalf("role = %s, want Leader", r.Role())
	}
	if r.LastIndex() != 1 {
		t.Fatalf("lastIndex = %d, want 1 (the no-op)", r.LastIndex())
	}
	if r.CommitIndex() != 1 {
		t.Fatalf("commit = %d, want 1 (single node commits its no-op alone)", r.CommitIndex())
	}
	// A proposal commits immediately too.
	nw.propose("a", "x")
	if r.CommitIndex() != 2 || r.AppliedIndex() != 2 {
		t.Fatalf("after propose: commit=%d applied=%d, want 2/2", r.CommitIndex(), r.AppliedIndex())
	}
}

// TestThreeNodeElection proves a clean election in a 3-node group yields exactly
// one leader, and the no-op commits once a quorum has it.
func TestThreeNodeElection(t *testing.T) {
	nw := newNetwork(t, ids(3), 10)
	nw.electLeader("a")
	if got := nw.leaders(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("leaders = %v, want [a]", got)
	}
	if nw.nodes["a"].Term() != 1 {
		t.Fatalf("term = %d, want 1", nw.nodes["a"].Term())
	}
	// The no-op (index 1, term 1) commits on the leader once a quorum acked.
	if nw.nodes["a"].CommitIndex() != 1 {
		t.Fatalf("commit = %d, want 1", nw.nodes["a"].CommitIndex())
	}
	// Followers are at term 1 and recognize the leader.
	for _, f := range []NodeID{"b", "c"} {
		if nw.nodes[f].Role() != Follower || nw.nodes[f].Term() != 1 {
			t.Fatalf("%s: role=%s term=%d, want Follower/1", f, nw.nodes[f].Role(), nw.nodes[f].Term())
		}
		if nw.nodes[f].LeaderID() != "a" {
			t.Fatalf("%s leader = %q, want a", f, nw.nodes[f].LeaderID())
		}
	}
}

// TestVoteGrantedOncePerTerm proves a node does not grant a second vote in a term
// to a different candidate (INV-R1 building block).
func TestVoteGrantedOncePerTerm(t *testing.T) {
	lg := replication.NewMemoryLog()
	r, err := New(Config{ID: "a", Peers: ids(3), Rand: rand.New(rand.NewSource(1)), Log: lg})
	if err != nil {
		t.Fatal(err)
	}
	// b requests a vote at term 1; a grants.
	_ = r.Step(Message{Type: MsgVoteRequest, From: "b", To: "a", Term: 1})
	rd := r.Ready()
	if rd.HardState == nil || rd.HardState.Vote != "b" {
		t.Fatalf("after voting for b, hardstate = %+v, want vote=b", rd.HardState)
	}
	if len(rd.Messages) != 1 || !rd.Messages[0].VoteGranted {
		t.Fatalf("did not grant vote to b: %+v", rd.Messages)
	}
	r.Advance()
	// c requests a vote at the same term 1; a must refuse (already voted for b).
	_ = r.Step(Message{Type: MsgVoteRequest, From: "c", To: "a", Term: 1})
	rd = r.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].VoteGranted {
		t.Fatalf("granted a second vote in term 1: %+v", rd.Messages)
	}
}

// TestVoteDeniedToStaleLog proves the up-to-date check: a candidate whose log is
// behind is denied even at an equal/higher term (§5.4.1).
func TestVoteDeniedToStaleLog(t *testing.T) {
	lg := replication.NewMemoryLog()
	// a has two entries at term 2.
	_ = lg.Append(Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 2})
	r, err := New(Config{ID: "a", Peers: ids(3), Rand: rand.New(rand.NewSource(1)), Log: lg, Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	// Candidate b at term 3 but with a shorter/older log (lastTerm 1) — denied.
	_ = r.Step(Message{Type: MsgVoteRequest, From: "b", To: "a", Term: 3, LastLogIndex: 1, LastLogTerm: 1})
	rd := r.Ready()
	last := rd.Messages[len(rd.Messages)-1]
	if last.VoteGranted {
		t.Fatalf("granted vote to a candidate with a less up-to-date log")
	}
	// Candidate c at term 3 with an equal-or-better log (lastTerm 2, index 2) — granted.
	r.Advance()
	_ = r.Step(Message{Type: MsgVoteRequest, From: "c", To: "a", Term: 3, LastLogIndex: 2, LastLogTerm: 2})
	rd = r.Ready()
	last = rd.Messages[len(rd.Messages)-1]
	if !last.VoteGranted {
		t.Fatalf("denied vote to an up-to-date candidate")
	}
}

// TestHigherTermForcesStepDown proves a leader that sees a higher term steps down
// to follower and clears its vote (§8.2, INV-R10 inverse).
func TestHigherTermForcesStepDown(t *testing.T) {
	nw := newNetwork(t, ids(3), 20)
	nw.electLeader("a")
	// A message from a higher term forces a to step down.
	_ = nw.nodes["a"].Step(Message{Type: MsgAppendRequest, From: "b", To: "a", Term: 5, PrevLogIndex: 0, PrevLogTerm: 0})
	if nw.nodes["a"].Role() != Follower {
		t.Fatalf("role = %s, want Follower after higher term", nw.nodes["a"].Role())
	}
	if nw.nodes["a"].Term() != 5 {
		t.Fatalf("term = %d, want 5", nw.nodes["a"].Term())
	}
	if nw.nodes["a"].LeaderID() != "b" {
		t.Fatalf("leader = %q, want b", nw.nodes["a"].LeaderID())
	}
}

// TestStaleMessageIsInert proves a lower-term message does not mutate protected
// state beyond a rejection carrying the current term (INV-R10).
func TestStaleMessageIsInert(t *testing.T) {
	nw := newNetwork(t, ids(3), 30)
	nw.electLeader("a")
	termBefore := nw.nodes["a"].Term()
	commitBefore := nw.nodes["a"].CommitIndex()
	lastBefore := nw.nodes["a"].LastIndex()

	// A stale AppendEntries (term 0) and stale RequestVote (term 0) to the leader.
	_ = nw.nodes["a"].Step(Message{Type: MsgAppendRequest, From: "b", To: "a", Term: 0})
	_ = nw.nodes["a"].Step(Message{Type: MsgVoteRequest, From: "c", To: "a", Term: 0})
	rd := nw.nodes["a"].Ready()

	if nw.nodes["a"].Term() != termBefore || nw.nodes["a"].Role() != Leader {
		t.Fatalf("stale message changed term/role: term=%d role=%s", nw.nodes["a"].Term(), nw.nodes["a"].Role())
	}
	if nw.nodes["a"].CommitIndex() != commitBefore || nw.nodes["a"].LastIndex() != lastBefore {
		t.Fatalf("stale message changed commit/last: commit=%d last=%d", nw.nodes["a"].CommitIndex(), nw.nodes["a"].LastIndex())
	}
	// The replies (if any) carry the current term and are rejections.
	for _, m := range rd.Messages {
		if m.Term != termBefore {
			t.Fatalf("reply term = %d, want current %d", m.Term, termBefore)
		}
		if m.Type == MsgVoteResponse && m.VoteGranted {
			t.Fatalf("stale vote request was granted")
		}
		if m.Type == MsgAppendResponse && m.Success {
			t.Fatalf("stale append request succeeded")
		}
	}
}

// TestSplitVoteResolves proves that if two candidates split a vote, a later
// election (different randomized timeouts) still produces a single leader.
func TestSplitVoteResolves(t *testing.T) {
	nw := newNetwork(t, ids(3), 7)
	// Force a and b to both campaign at term 1 before any delivery.
	nw.campaign("a")
	nw.campaign("b")
	nw.deliverAll()
	// Possibly no leader yet (split). Keep ticking all nodes until one wins.
	for i := 0; i < 200 && len(nw.leaders()) == 0; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if got := nw.leaders(); len(got) != 1 {
		t.Fatalf("leaders = %v, want exactly one after split-vote resolution", got)
	}
}
