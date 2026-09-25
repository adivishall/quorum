package raft

import (
	"errors"
	"testing"
)

// Phase 12: ReadIndex in the pure core (docs/DESIGN.md §8.5, ADR-019). These run
// on the deterministic simulated network, so every message the confirmation
// depends on is delivered by hand.

// readStates returns (and clears) the reads a node has confirmed so far — those
// drained by the harness on every step plus any still pending in its Ready.
func (nw *network) readStates(id NodeID) []ReadState {
	r := nw.nodes[id]
	for r.HasReady() {
		rd := r.Ready()
		nw.queue = append(nw.queue, rd.Messages...)
		nw.reads[id] = append(nw.reads[id], rd.ReadStates...)
		r.Advance()
	}
	out := nw.reads[id]
	nw.reads[id] = nil
	return out
}

// readIndex registers a read on id and drains the registration's broadcast into
// the queue (the core's effects sit in its Ready until drained).
func (nw *network) readIndex(id NodeID) ReadState {
	nw.t.Helper()
	rs, err := nw.nodes[id].ReadIndex()
	if err != nil {
		nw.t.Fatalf("ReadIndex on %s: %v", id, err)
	}
	nw.drain(id)
	return rs
}

// TestReadIndexRequiresLeader: a follower cannot serve a linearizable read.
func TestReadIndexRequiresLeader(t *testing.T) {
	nw := newNetwork(t, ids(3), 2100)
	nw.electLeader("a")
	if _, err := nw.nodes["b"].ReadIndex(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower ReadIndex = %v, want ErrNotLeader", err)
	}
	if _, err := nw.nodes["a"].ReadIndex(); err != nil {
		t.Fatalf("leader ReadIndex: %v", err)
	}
}

// TestReadIndexIsConfirmedByAQuorumRound: the read is not confirmed until a
// follower's response to the post-registration heartbeat arrives; then its index
// is the leader's commit index.
func TestReadIndexIsConfirmedByAQuorumRound(t *testing.T) {
	nw := newNetwork(t, ids(3), 2200)
	nw.electLeader("a")
	nw.propose("a", "x")
	a := nw.nodes["a"]
	rs, err := a.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	if rs.Index != a.CommitIndex() {
		t.Fatalf("read index %d, want the commit index %d", rs.Index, a.CommitIndex())
	}
	if got := nw.readStates("a"); len(got) != 0 {
		t.Fatalf("confirmed before any peer answered: %+v", got)
	}
	// The registration broadcast one heartbeat per peer; deliver a's heartbeats
	// (the followers reply) and then the replies.
	q := nw.takeQueue()
	if len(q) != 2 {
		t.Fatalf("want 2 heartbeats after ReadIndex, have %d", len(q))
	}
	for _, m := range q {
		if m.Type != MsgAppendRequest || m.Seq == 0 {
			t.Fatalf("registration should broadcast AppendEntries carrying a sequence, got %+v", m)
		}
		nw.deliver(m)
	}
	replies := nw.takeQueue()
	if len(replies) != 2 {
		t.Fatalf("want 2 replies, have %d", len(replies))
	}
	nw.deliver(replies[0]) // one follower's ack + self = quorum of 3
	got := nw.readStates("a")
	if len(got) != 1 || got[0].ID != rs.ID || got[0].Index != rs.Index {
		t.Fatalf("after a quorum acked: read states %+v, want %+v", got, rs)
	}
	nw.deliver(replies[1])
	if got := nw.readStates("a"); len(got) != 0 {
		t.Fatalf("a read confirmed twice: %+v", got)
	}
}

// TestAcksFromBeforeTheReadDoNotConfirmIt is the property that makes ReadIndex
// safe against a deposed leader: responses that were in flight BEFORE the read was
// registered prove nothing about leadership after it, and must not count. Only a
// response echoing the post-registration sequence does.
func TestAcksFromBeforeTheReadDoNotConfirmIt(t *testing.T) {
	nw := newNetwork(t, ids(3), 2300)
	nw.electLeader("a")
	a := nw.nodes["a"]
	// A heartbeat round whose replies we hold back.
	for i := 0; i < a.heartbeatTicks; i++ {
		a.Tick()
	}
	nw.drain("a")
	for _, m := range nw.takeQueue() {
		nw.deliver(m)
	}
	oldReplies := nw.takeQueue()
	if len(oldReplies) != 2 {
		t.Fatalf("want 2 held replies, have %d", len(oldReplies))
	}
	// Now the read is registered, and only THEN the old replies arrive.
	rs := nw.readIndex("a")
	newHeartbeats := nw.takeQueue()
	for _, m := range oldReplies {
		nw.deliver(m)
	}
	if got := nw.readStates("a"); len(got) != 0 {
		t.Fatalf("stale acknowledgements confirmed the read: %+v", got)
	}
	// The post-registration heartbeats and their replies do confirm it.
	for _, m := range newHeartbeats {
		nw.deliver(m)
	}
	for _, m := range nw.takeQueue() {
		nw.deliver(m)
	}
	if got := nw.readStates("a"); len(got) != 1 || got[0].ID != rs.ID {
		t.Fatalf("post-registration acks did not confirm the read: %+v", got)
	}
}

// TestNewLeaderReadIndexIsAtLeastItsNoop: a fresh leader whose no-op has not
// committed yet must not read at its (stale) commit index; the read index is the
// no-op's index, so the read waits until everything before it is committed.
func TestNewLeaderReadIndexIsAtLeastItsNoop(t *testing.T) {
	nw := newNetwork(t, ids(3), 2400)
	nw.electLeader("a")
	nw.propose("a", "one")
	nw.propose("a", "two")
	// b campaigns and wins with a's log replicated; deliver only the election,
	// not b's no-op replication.
	nw.campaign("b")
	for _, m := range nw.takeQueue() {
		if m.Type == MsgVoteRequest || m.Type == MsgVoteResponse {
			nw.deliver(m)
		}
	}
	for _, m := range nw.takeQueue() {
		if m.Type == MsgVoteRequest || m.Type == MsgVoteResponse {
			nw.deliver(m)
		}
	}
	b := nw.nodes["b"]
	if b.Role() != Leader {
		t.Fatalf("b did not win: %s", b.Role())
	}
	noop := b.LastIndex()
	if b.CommitIndex() >= noop {
		t.Fatalf("setup: b's no-op %d already committed (commit %d)", noop, b.CommitIndex())
	}
	rs, err := b.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	if rs.Index != noop {
		t.Fatalf("new leader's read index %d, want its no-op index %d (commit was %d)", rs.Index, noop, b.CommitIndex())
	}
}

// TestPendingReadsAreDroppedOnStepDown: a leader deposed by a higher term never
// confirms a read it had registered.
func TestPendingReadsAreDroppedOnStepDown(t *testing.T) {
	nw := newNetwork(t, ids(3), 2500)
	nw.electLeader("a")
	a := nw.nodes["a"]
	nw.readIndex("a")
	nw.takeQueue() // a's heartbeats never arrive
	// Meanwhile b wins a higher term; a learns of it.
	nw.isolate("a")
	nw.campaign("b")
	nw.deliverAll()
	nw.heal()
	for i := 0; i < nw.nodes["b"].heartbeatTicks; i++ {
		nw.tick("b")
	}
	nw.deliverAll()
	if a.Role() == Leader {
		t.Fatalf("a still leads in term %d", a.Term())
	}
	if got := nw.readStates("a"); len(got) != 0 {
		t.Fatalf("a deposed leader confirmed a read: %+v", got)
	}
	if len(a.pending) != 0 {
		t.Fatalf("pending reads survived the step-down: %+v", a.pending)
	}
}

// TestIsolatedLeaderNeverConfirmsARead: the stale-leader case of
// docs/CONSISTENCY.md §4(2). A leader cut off from every peer keeps believing it
// leads; its reads must never be confirmed, however many heartbeats it sends.
func TestIsolatedLeaderNeverConfirmsARead(t *testing.T) {
	nw := newNetwork(t, ids(3), 2600)
	nw.electLeader("a")
	nw.propose("a", "x")
	nw.isolate("a")
	a := nw.nodes["a"]
	nw.readIndex("a")
	for i := 0; i < 50; i++ {
		nw.tick("a")
		nw.deliverAll() // everything a sends is dropped by the partition
	}
	if a.Role() != Leader {
		t.Fatalf("setup: the isolated leader stepped down (%s)", a.Role())
	}
	if got := nw.readStates("a"); len(got) != 0 {
		t.Fatalf("an isolated leader confirmed a read: %+v", got)
	}
}

// TestSingleNodeReadIndexConfirmsImmediately: a group of one is its own quorum.
func TestSingleNodeReadIndexConfirmsImmediately(t *testing.T) {
	nw := newNetwork(t, ids(1), 2700)
	nw.electLeader("a")
	nw.propose("a", "x")
	a := nw.nodes["a"]
	rs, err := a.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	if got := nw.readStates("a"); len(got) != 1 || got[0] != rs || rs.Index != a.CommitIndex() {
		t.Fatalf("single node: read states %+v, want %+v at commit %d", got, rs, a.CommitIndex())
	}
}

// TestReadsConfirmInOrderAndOnlyWithTheirOwnRound: two reads registered in a row
// need acks of the second round to confirm both; acks of the first round confirm
// only the first.
func TestReadsConfirmInOrderAndOnlyWithTheirOwnRound(t *testing.T) {
	nw := newNetwork(t, ids(3), 2800)
	nw.electLeader("a")
	r1 := nw.readIndex("a")
	round1 := nw.takeQueue()
	r2 := nw.readIndex("a")
	round2 := nw.takeQueue()
	for _, m := range round1 {
		nw.deliver(m)
	}
	for _, m := range nw.takeQueue() {
		nw.deliver(m)
	}
	got := nw.readStates("a")
	if len(got) != 1 || got[0].ID != r1.ID {
		t.Fatalf("after round 1: %+v, want only read %d", got, r1.ID)
	}
	for _, m := range round2 {
		nw.deliver(m)
	}
	for _, m := range nw.takeQueue() {
		nw.deliver(m)
	}
	got = nw.readStates("a")
	if len(got) != 1 || got[0].ID != r2.ID {
		t.Fatalf("after round 2: %+v, want read %d", got, r2.ID)
	}
}

// TestHeartbeatSequenceIsEchoed: every AppendEntriesResponse — success or
// rejection — carries the sequence of the request it answers.
func TestHeartbeatSequenceIsEchoed(t *testing.T) {
	nw := newNetwork(t, ids(3), 2900)
	nw.electLeader("a")
	nw.propose("a", "x")
	a := nw.nodes["a"]
	for i := 0; i < a.heartbeatTicks; i++ {
		a.Tick()
	}
	nw.drain("a")
	reqs := nw.takeQueue()
	for _, m := range reqs {
		nw.deliver(m)
	}
	replies := nw.takeQueue()
	if len(reqs) != 2 || len(replies) != 2 {
		t.Fatalf("want 2 requests and 2 replies, have %d/%d", len(reqs), len(replies))
	}
	for _, m := range replies {
		if m.Seq != reqs[0].Seq {
			t.Fatalf("reply seq %d, request seq %d", m.Seq, reqs[0].Seq)
		}
	}
	// A rejection echoes too: b gets an AppendEntries whose prevLog it lacks.
	stale := Message{Type: MsgAppendRequest, From: "a", To: "b", Term: a.Term(), PrevLogIndex: 99, PrevLogTerm: a.Term(), Seq: 12345}
	nw.nodes["b"].Step(stale)
	var rej []Message
	for nw.nodes["b"].HasReady() {
		rd := nw.nodes["b"].Ready()
		rej = append(rej, rd.Messages...)
		nw.nodes["b"].Advance()
	}
	if len(rej) != 1 || rej[0].Success || rej[0].Seq != 12345 {
		t.Fatalf("rejection did not echo the sequence: %+v", rej)
	}
}
