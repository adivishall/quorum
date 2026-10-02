package raft

import (
	"bytes"
	"errors"
	"testing"
)

// Bounds on a leader's outstanding work (audit M3): a leader that cannot reach
// a quorum refuses proposals and reads beyond its bounds — definitely, with
// nothing appended or registered — instead of accumulating them without
// limit; once it reaches a quorum again it serves as before.

// TestIsolatedLeaderRefusesProposalsBeyondItsBound: an isolated leader accepts
// proposals up to MaxUncommittedEntries, then refuses with ErrBusy: no entry,
// no Ready. Healed, the tail commits and proposals are accepted again.
func TestIsolatedLeaderRefusesProposalsBeyondItsBound(t *testing.T) {
	nw := newNetwork(t, ids(3), 800)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxUncommittedEntries = 8
	nw.isolate("a")
	accepted := 0
	for i := 0; i < 50; i++ {
		last := a.LastIndex()
		err := a.Propose([]byte("w"))
		if errors.Is(err, ErrBusy) {
			if a.LastIndex() != last {
				t.Fatalf("a refused proposal appended: last %d -> %d", last, a.LastIndex())
			}
			continue
		}
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		accepted++
		nw.drain("a")
	}
	if accepted != 8 {
		t.Fatalf("the isolated leader accepted %d proposals, want its bound of 8", accepted)
	}
	if a.HasReady() {
		t.Fatalf("refused proposals produced a Ready: %+v", a.Ready())
	}
	nw.heal()
	for i := 0; i < a.heartbeatTicks; i++ {
		nw.tick("a")
	}
	nw.deliverAll()
	if a.Role() != Leader || a.CommitIndex() != a.LastIndex() {
		t.Fatalf("healed: role %s, commit %d of %d", a.Role(), a.CommitIndex(), a.LastIndex())
	}
	nw.propose("a", "after")
}

// TestUncommittedBytesBound: proposals are refused once their bytes would take
// the tail past MaxUncommittedBytes — but one is always admitted when the tail
// holds no data, so no entry within MaxEntryDataLen is refused forever.
func TestUncommittedBytesBound(t *testing.T) {
	nw := newNetwork(t, ids(3), 801)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxUncommittedBytes = 100
	nw.isolate("a")
	big := bytes.Repeat([]byte{'x'}, 150) // over the bound on its own
	if err := a.Propose(big); err != nil {
		t.Fatalf("a proposal larger than the bound, on a tail with no data: %v; it must be admitted", err)
	}
	nw.drain("a")
	if err := a.Propose([]byte("y")); !errors.Is(err, ErrBusy) {
		t.Fatalf("a proposal past the byte bound: %v, want ErrBusy", err)
	}
	nw.heal()
	for i := 0; i < a.heartbeatTicks; i++ {
		nw.tick("a")
	}
	nw.deliverAll()
	for _, d := range []string{"0123456789012345678901234567890123456789", "0123456789012345678901234567890123456789"} {
		if err := a.Propose([]byte(d)); err != nil {
			t.Fatalf("Propose within the byte bound: %v", err)
		}
		nw.drain("a")
	}
	if err := a.Propose(bytes.Repeat([]byte{'z'}, 30)); !errors.Is(err, ErrBusy) {
		t.Fatalf("80 bytes uncommitted, 30 more: %v, want ErrBusy", err)
	}
	nw.deliverAll()
	nw.propose("a", "after")
}

// TestInheritedTailCountsTowardTheBound: a new leader's uncommitted tail
// includes the entries it inherited, not only its own.
func TestInheritedTailCountsTowardTheBound(t *testing.T) {
	nw := newNetwork(t, ids(3), 802)
	nw.electLeader("a")
	// a replicates three entries to b only, then is cut off before they commit.
	nw.blocked[linkKey("a", "c")] = true
	nw.blocked[linkKey("b", "a")] = true // b's acknowledgements never return
	for _, d := range []string{"x1", "x2", "x3"} {
		if err := nw.nodes["a"].Propose([]byte(d)); err != nil {
			t.Fatal(err)
		}
		nw.drain("a")
		nw.deliverAll()
	}
	nw.isolate("a")
	b := nw.nodes["b"]
	if b.LastIndex() != nw.nodes["a"].LastIndex() || b.CommitIndex() == b.LastIndex() {
		t.Fatalf("premise: b holds %d entries, %d committed; want a's uncommitted tail", b.LastIndex(), b.CommitIndex())
	}
	inherited := int(b.LastIndex() - b.CommitIndex())
	b.maxUncommittedEntries = inherited + 1 // the inherited tail and b's no-op
	nw.campaign("b")
	for b.Role() == Candidate {
		nw.drain("b")
		if !nw.deliverOne() {
			break
		}
	}
	// b's no-op has not been acknowledged yet: only Ready drained, nothing delivered.
	if b.Role() != Leader {
		t.Fatalf("b did not win: %s", b.Role())
	}
	if got := int(b.LastIndex() - b.CommitIndex()); got != inherited+1 {
		t.Fatalf("premise: b leads with %d uncommitted entries, want the %d inherited and its no-op", got, inherited)
	}
	if err := b.Propose([]byte("w")); !errors.Is(err, ErrBusy) {
		t.Fatalf("b leads with %d inherited uncommitted entries and its no-op at a bound of %d: Propose = %v, want ErrBusy", inherited, inherited+1, err)
	}
	nw.deliverAll()
	if b.CommitIndex() != b.LastIndex() {
		t.Fatalf("b committed %d of %d", b.CommitIndex(), b.LastIndex())
	}
	nw.propose("b", "after")
}

// TestIsolatedLeaderRefusesReadsBeyondItsBound: reads awaiting confirmation
// are bounded by MaxPendingReads; beyond it ReadIndex refuses with ErrBusy and
// registers nothing. Healed, the pending reads are confirmed and new ones are
// accepted.
func TestIsolatedLeaderRefusesReadsBeyondItsBound(t *testing.T) {
	nw := newNetwork(t, ids(3), 803)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxPendingReads = 4
	nw.isolate("a")
	registered := 0
	for i := 0; i < 20; i++ {
		_, err := a.ReadIndex()
		if errors.Is(err, ErrBusy) {
			continue
		}
		if err != nil {
			t.Fatalf("ReadIndex: %v", err)
		}
		registered++
		nw.drain("a")
	}
	if registered != 4 || len(a.pending) != 4 {
		t.Fatalf("the isolated leader registered %d reads (%d pending), want its bound of 4", registered, len(a.pending))
	}
	nw.heal()
	for i := 0; i < a.heartbeatTicks; i++ {
		nw.tick("a")
	}
	nw.deliverAll()
	if len(a.pending) != 0 || len(nw.reads["a"]) != 4 {
		t.Fatalf("healed: %d still pending, %d confirmed; want 0 and 4", len(a.pending), len(nw.reads["a"]))
	}
	if _, err := a.ReadIndex(); err != nil {
		t.Fatalf("ReadIndex after healing: %v", err)
	}
}

// TestReadsPendingWhenTheLeaderBecomesItsOwnQuorumAreConfirmed: reads that
// wait for a member's acknowledgement when a change removes that member are
// confirmed once the leader alone is the quorum. Confirmation ran only on a
// reply, and none comes once no other member is left: the reads stayed
// pending forever, and, as their bound fills, the leader refused every read.
func TestReadsPendingWhenTheLeaderBecomesItsOwnQuorumAreConfirmed(t *testing.T) {
	nw := newNetworkWith(t, []NodeID{"n1", "n2"}, voters("n1", "n2"), 7)
	nw.electLeader("n1")
	nw.propose("n1", "a")
	n1 := nw.nodes["n1"]
	if err := n1.ProposeConfChange(ConfChange{Type: RemoveVoter, Member: Member{ID: "n2"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	rs := nw.readIndex("n1") // a round after the joint entry's: n2's ack of the entry does not confirm it
	if len(n1.pending) != 1 {
		t.Fatalf("premise: the read is not pending (%d)", len(n1.pending))
	}
	nw.deliverAll() // n2 acknowledges the joint entry; it commits; the final {n1} follows
	if c, _ := n1.Conf(); c.Joint() || c.IsMember("n2") {
		t.Fatalf("premise: the final entry was not appended: %s", c)
	}
	got := nw.readStates("n1")
	if len(n1.pending) != 0 || len(got) != 1 || got[0].ID != rs.ID {
		t.Fatalf("the leader is its own quorum, yet %d read(s) still pending, %v confirmed", len(n1.pending), got)
	}
}
