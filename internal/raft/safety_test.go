package raft

import (
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

// TestHardStateAccompaniesVoteGrant proves the persist-before-reply dependency at
// the source (INV-R6): whenever the core grants a vote, the same Ready carries the
// updated HardState, so the driver (which persists a Ready's HardState before
// sending its Messages) can never send a vote reply before the vote is durable.
func TestHardStateAccompaniesVoteGrant(t *testing.T) {
	r, _ := newCore(t, "a", ids(3), 1)
	_ = r.Step(Message{Type: MsgVoteRequest, From: "b", To: "a", Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	rd := r.Ready()

	var granted bool
	for _, m := range rd.Messages {
		if m.Type == MsgVoteResponse && m.VoteGranted {
			granted = true
		}
	}
	if !granted {
		t.Fatal("expected a granted vote to test")
	}
	if rd.HardState == nil || rd.HardState.Vote != "b" || rd.HardState.Term != 1 {
		t.Fatalf("a granted vote without an accompanying HardState to persist first: %+v", rd.HardState)
	}
}

// TestHardStateAccompaniesTermBump proves the same for a higher-term step-down: the
// new term and cleared vote are in the Ready before any reply is sent (INV-R6).
func TestHardStateAccompaniesTermBump(t *testing.T) {
	r, _ := newCore(t, "a", ids(3), 1)
	_ = r.Step(Message{Type: MsgAppendRequest, From: "b", To: "a", Term: 4})
	rd := r.Ready()
	if rd.HardState == nil || rd.HardState.Term != 4 || rd.HardState.Vote != "" {
		t.Fatalf("higher-term step-down did not surface a HardState to persist: %+v", rd.HardState)
	}
}

// TestStaleAppendResponseIgnored proves a lower-term AppendResponse does not mutate
// leader state (INV-R10): matchIndex/nextIndex are untouched by a stale reply.
func TestStaleAppendResponseIgnored(t *testing.T) {
	nw := newNetwork(t, ids(3), 900)
	nw.electLeader("a")
	nw.propose("a", "x")
	a := nw.nodes["a"]
	matchBefore := a.matchIndex["b"]
	nextBefore := a.nextIndex["b"]
	termBefore := a.Term()

	// A stale success (term 0) claiming a huge match must be dropped, not applied.
	_ = a.Step(Message{Type: MsgAppendResponse, From: "b", To: "a", Term: 0, Success: true, MatchIndex: 9999})
	if a.matchIndex["b"] != matchBefore || a.nextIndex["b"] != nextBefore || a.Term() != termBefore {
		t.Fatalf("stale AppendResponse mutated leader state: match %d->%d next %d->%d term %d->%d",
			matchBefore, a.matchIndex["b"], nextBefore, a.nextIndex["b"], termBefore, a.Term())
	}
	if a.CommitIndex() > a.LastIndex() {
		t.Fatal("commit advanced past last index from a stale response")
	}
}

// TestRecoveredTermCannotRegress proves construction refuses a currentTerm below a
// term already in the log (a corrupt/rolled-back HardState), rather than silently
// accepting it (recovery coherence, docs/RAFT.md §11).
func TestRecoveredTermCannotRegress(t *testing.T) {
	lg := replication.NewMemoryLog()
	_ = lg.Append(Entry{Index: 1, Term: 5})
	_, err := New(Config{ID: "a", Peers: ids(3), Rand: rand.New(rand.NewSource(1)), Log: lg, Term: 3})
	if err != ErrTermRegression {
		t.Fatalf("New with term 3 below log term 5 = %v, want ErrTermRegression", err)
	}
}
