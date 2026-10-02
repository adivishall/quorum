package raft

import (
	"bytes"
	"fmt"
	"testing"
)

// Replication flow control (audit H4): a backlog travels in budgeted
// AppendEntries, a lagging follower catches up batch after acknowledged
// batch, and reads share confirmation rounds of entry-less heartbeats instead
// of each resending the unacknowledged tail.

// catchUp heals the network, lets the leader's heartbeat reach the lagging
// peer, then delivers messages — and only messages, no ticks — until the
// network is quiet, checking every AppendEntries against the budgets.
func catchUp(t *testing.T, nw *network, leader NodeID, maxEntries, maxBytes int) (appends int) {
	t.Helper()
	nw.heal()
	for i := 0; i < nw.nodes[leader].heartbeatTicks; i++ {
		nw.tick(leader)
	}
	for len(nw.queue) > 0 {
		m := nw.queue[0]
		nw.queue = nw.queue[1:]
		if m.Type == MsgAppendRequest && len(m.Entries) > 0 {
			appends++
			size := 0
			for _, e := range m.Entries {
				size += len(e.Data)
			}
			if len(m.Entries) > maxEntries || (size > maxBytes && len(m.Entries) > 1) {
				t.Fatalf("an AppendEntries of %d entries and %d bytes, beyond the budgets of %d and %d", len(m.Entries), size, maxEntries, maxBytes)
			}
		}
		nw.deliver(m)
	}
	return appends
}

// TestLaggingFollowerCatchesUpInBudgetedBatches: a follower cut off while the
// leader appended 3 MB catches up — in messages within the budgets, each
// acknowledged batch sending the next, with no heartbeat needed between them.
// Before, every message carried the whole backlog: one beyond the transport's
// frame or the decoder's entry count could never be sent.
func TestLaggingFollowerCatchesUpInBudgetedBatches(t *testing.T) {
	nw := newNetwork(t, ids(3), 900)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxEntriesPerMsg, a.maxSizePerMsg = 16, 64<<10
	nw.isolate("c")
	nw.blocked[linkKey("a", "b")], nw.blocked[linkKey("b", "a")] = false, false
	for i := 0; i < 300; i++ {
		nw.propose("a", string(bytes.Repeat([]byte{byte('A' + i%26)}, 10<<10)))
	}
	if nw.nodes["c"].LastIndex() >= a.LastIndex() {
		t.Fatal("premise: c is not behind")
	}
	appends := catchUp(t, nw, "a", 16, 64<<10)
	if c := nw.nodes["c"]; c.LastIndex() != a.LastIndex() || c.CommitIndex() != a.CommitIndex() {
		t.Fatalf("c reached %d (commit %d) of the leader's %d (commit %d) without a heartbeat between batches", c.LastIndex(), c.CommitIndex(), a.LastIndex(), a.CommitIndex())
	}
	if appends < 300/6 {
		t.Fatalf("the backlog took %d messages; at 6 entries of 10 KiB per 64 KiB it needs at least %d", appends, 300/6)
	}
	nw.assertLogMatching()
}

// TestAnEntryLargerThanTheByteBudgetIsSentAlone: the byte budget never blocks
// an entry larger than itself — it travels alone.
func TestAnEntryLargerThanTheByteBudgetIsSentAlone(t *testing.T) {
	nw := newNetwork(t, ids(3), 901)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxSizePerMsg = 100
	nw.isolate("c")
	nw.blocked[linkKey("a", "b")], nw.blocked[linkKey("b", "a")] = false, false
	for i := 0; i < 3; i++ {
		nw.propose("a", string(bytes.Repeat([]byte{'x'}, 1000)))
	}
	catchUp(t, nw, "a", DefaultMaxEntriesPerMsg, 100)
	if c := nw.nodes["c"]; c.LastIndex() != a.LastIndex() {
		t.Fatalf("c reached %d of %d", c.LastIndex(), a.LastIndex())
	}
	nw.assertLogMatching()
}

// TestReadsInOneCycleShareOneRound: a thousand reads registered between two
// Readies are confirmed by one round — one entry-less heartbeat per peer —
// where each used to broadcast the whole unacknowledged tail.
func TestReadsInOneCycleShareOneRound(t *testing.T) {
	nw := newNetwork(t, ids(3), 902)
	nw.electLeader("a")
	a := nw.nodes["a"]
	// An unacknowledged tail, which a read's round must not resend.
	nw.blocked[linkKey("b", "a")], nw.blocked[linkKey("c", "a")] = true, true
	nw.propose("a", "tail")
	nw.heal()
	for i := 0; i < 1000; i++ {
		if _, err := a.ReadIndex(); err != nil {
			t.Fatal(err)
		}
	}
	rd := a.Ready()
	if len(rd.Messages) != 2 {
		t.Fatalf("1000 reads produced %d messages, want one round: 2", len(rd.Messages))
	}
	for _, m := range rd.Messages {
		if m.Type != MsgAppendRequest || len(m.Entries) != 0 {
			t.Fatalf("a read round sent %s with %d entries, want an entry-less heartbeat", m.Type, len(m.Entries))
		}
	}
	nw.drain("a")
	nw.deliverAll()
	if got := len(nw.reads["a"]); got != 1000 {
		t.Fatalf("%d reads confirmed by the round, want 1000", got)
	}
	// A proposal's broadcast in the same cycle is a round a read can join.
	nw.reads["a"] = nil
	if err := a.Propose([]byte("p")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	if n := len(a.Ready().Messages); n != 2 {
		t.Fatalf("a proposal and a read in one cycle: %d messages, want the proposal's 2", n)
	}
	nw.drain("a")
	nw.deliverAll()
	if len(nw.reads["a"]) != 1 {
		t.Fatalf("the read that joined the proposal's round: %d confirmed, want 1", len(nw.reads["a"]))
	}
}

// TestAReadNeverJoinsARoundAlreadySent: the safety of sharing rounds. A read
// registered after a round's messages were sent must not be confirmed by that
// round's acknowledgements — they may predate it, and a deposed leader would
// serve a stale read. It starts a round of its own.
func TestAReadNeverJoinsARoundAlreadySent(t *testing.T) {
	nw := newNetwork(t, ids(3), 903)
	nw.electLeader("a")
	a := nw.nodes["a"]
	if _, err := a.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	nw.drain("a") // the first read's round is sent (Advance)
	first := nw.takeQueue()
	if _, err := a.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	nw.drain("a")
	second := nw.takeQueue()
	if len(second) != 2 {
		t.Fatalf("a read after the first round was sent produced %d messages, want a round of its own: 2", len(second))
	}
	for _, m := range first { // only the first round is answered
		nw.deliver(m)
	}
	for len(nw.queue) > 0 {
		m := nw.queue[0]
		nw.queue = nw.queue[1:]
		if m.To == "a" {
			nw.deliver(m)
		}
	}
	if got := len(nw.reads["a"]); got != 1 {
		t.Fatalf("acknowledgements of the first round confirmed %d reads, want only the first", got)
	}
	for _, m := range second {
		nw.deliver(m)
	}
	nw.deliverAll()
	if got := len(nw.reads["a"]); got != 2 {
		t.Fatalf("after its own round: %d reads confirmed, want 2", got)
	}
}

// TestEntryBudgetBindsABacklogOfSmallEntries: when entries are small the
// count budget, not the byte budget, cuts the backlog — a message never
// carries more than MaxEntriesPerMsg entries (the decoder refuses more than
// MaxEntriesPerMessage, whatever their size).
func TestEntryBudgetBindsABacklogOfSmallEntries(t *testing.T) {
	nw := newNetwork(t, ids(3), 904)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxEntriesPerMsg = 8 // the byte budget stays 1 MiB: never reached here
	nw.isolate("c")
	nw.blocked[linkKey("a", "b")], nw.blocked[linkKey("b", "a")] = false, false
	for i := 0; i < 200; i++ {
		nw.propose("a", "small")
	}
	appends := catchUp(t, nw, "a", 8, DefaultMaxSizePerMsg)
	if c := nw.nodes["c"]; c.LastIndex() != a.LastIndex() {
		t.Fatalf("c reached %d of %d", c.LastIndex(), a.LastIndex())
	}
	if appends < 200/8 {
		t.Fatalf("200 entries took %d messages; at 8 per message at least %d", appends, 200/8)
	}
	nw.assertLogMatching()
}

// TestARemovedLeaderSendsNothingOnceItStepsDown: the acknowledgement that
// commits the final entry of the leader's own removal steps it down. The
// same acknowledgement used to continue a budget-cut backlog afterwards: the
// ex-leader sent entries in its old term, and the receiver took it for its
// leader again, delaying the election of the next.
func TestARemovedLeaderSendsNothingOnceItStepsDown(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 11)
	nw.electLeader("n1")
	n1 := nw.nodes["n1"]
	n1.maxEntriesPerMsg = 1
	if err := n1.ProposeConfChange(ConfChange{Type: RemoveVoter, Member: Member{ID: "n1"}}); err != nil {
		t.Fatal(err)
	}
	nw.drain("n1")
	for i := 0; i < 10000; i++ {
		if c, _ := n1.Conf(); !c.Joint() && !c.IsMember("n1") {
			break
		}
		if !nw.deliverOne() {
			t.Fatal("premise: the final entry was never appended")
		}
	}
	for i := 0; i < 3; i++ {
		nw.proposeNoDeliver("n1", fmt.Sprintf("after-final-%d", i)) // a backlog the budget cuts
	}
	stepped := false
	for len(nw.queue) > 0 {
		m := nw.queue[0]
		nw.queue = nw.queue[1:]
		before, was := len(nw.queue), n1.Role() == Leader
		nw.deliver(m)
		if was && n1.Role() != Leader {
			stepped = true
			for _, out := range nw.queue[before:] {
				if out.From == "n1" && out.Type == MsgAppendRequest {
					t.Fatalf("n1 stepped down and in the same step sent %s an AppendEntries of %d entries in term %d", out.To, len(out.Entries), out.Term)
				}
			}
		}
	}
	if !stepped {
		t.Fatal("premise: the removed leader never stepped down")
	}
}

// TestASnapshotInstallSendsTheNextBatchOnce: a follower that was streaming a
// budget-cut backlog and then fell behind the boundary is offered the
// snapshot; its install is answered with the next batch once. The cut flag
// survived the snapshot offer, so the reply sent the batch twice — once to
// continue the cut backlog, once to resume after the snapshot.
func TestASnapshotInstallSendsTheNextBatchOnce(t *testing.T) {
	nw := newNetwork(t, ids(3), 913)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxEntriesPerMsg = 2
	nw.isolate("c")
	for i := 0; i < 12; i++ {
		nw.propose("a", fmt.Sprintf("v%d", i))
	}
	// c streams one cut batch, then a compacts past it.
	nw.heal()
	for i := 0; i < a.heartbeatTicks; i++ {
		nw.tick("a")
	}
	if !a.cut["c"] {
		t.Fatal("premise: the heartbeat to c is not a cut batch")
	}
	nw.queue = nil // c never sees it
	if err := a.Compact(a.CommitIndex() - 2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < a.heartbeatTicks; i++ {
		nw.tick("a")
	}
	var offer *Message
	for _, m := range nw.takeQueue() {
		if m.To == "c" && m.Type == MsgSnapshot {
			offer = &m
		}
	}
	if offer == nil {
		t.Fatal("premise: c was not offered the snapshot")
	}
	nw.deliver(*offer) // c installs it and answers
	var reply *Message
	for _, m := range nw.takeQueue() {
		if m.From == "c" && m.Type == MsgSnapshotResponse {
			reply = &m
		}
	}
	if reply == nil || !reply.Success {
		t.Fatalf("premise: c did not install the snapshot: %+v", reply)
	}
	nw.deliver(*reply)
	batches := map[uint64]int{}
	for _, m := range nw.queue {
		if m.To == "c" && m.Type == MsgAppendRequest && len(m.Entries) > 0 {
			batches[m.PrevLogIndex]++
		}
	}
	for prev, n := range batches {
		if n > 1 {
			t.Fatalf("the snapshot's acknowledgement sent the batch after %d to c %d times", prev, n)
		}
	}
	if len(batches) == 0 {
		t.Fatal("premise: the snapshot's acknowledgement sent c nothing")
	}
}

// TestABatchInFlightIsNotResentWithEveryProposal: while a cut batch is in
// flight to a lagging peer, a proposal sends that peer a heartbeat, not the
// batch again; the heartbeat tick still resends it, so a lost batch is not
// stranded; and a follower catches up while clients keep writing, in about as
// many batches as its backlog needs. Every proposal used to resend the batch
// in flight — 50 writes queued 50 copies of it — and a follower behind under
// load caught up 20 times slower than idle (a review of the budgets).
func TestABatchInFlightIsNotResentWithEveryProposal(t *testing.T) {
	nw := newNetwork(t, ids(3), 4242)
	nw.electLeader("a")
	a := nw.nodes["a"]
	a.maxSizePerMsg = 64 << 10
	nw.isolate("c")
	v := string(bytes.Repeat([]byte{'x'}, 4<<10))
	for i := 0; i < 200; i++ {
		nw.propose("a", v)
	}
	nw.heal()
	for i := 0; i < a.heartbeatTicks; i++ {
		nw.tick("a")
	}
	if !a.cut["c"] {
		t.Fatal("premise: the batch to c is not cut")
	}
	inFlight := nw.takeQueue() // its acknowledgement is not back yet
	entriesTo := func(q []Message) (msgs int) {
		for _, m := range q {
			if m.To == "c" && m.Type == MsgAppendRequest && len(m.Entries) > 0 {
				msgs++
			}
		}
		return msgs
	}
	for i := 0; i < 50; i++ {
		nw.proposeNoDeliver("a", "w")
	}
	if n := entriesTo(nw.queue); n != 0 {
		t.Fatalf("50 proposals with a batch in flight sent c %d more batches", n)
	}
	nw.queue = nil
	for i := 0; i < a.heartbeatTicks; i++ {
		nw.tick("a")
	}
	if n := entriesTo(nw.queue); n != 1 {
		t.Fatalf("the heartbeat tick resent %d batches to c, want the one in flight", n)
	}
	// Catch up from here while a client keeps writing: one write every few
	// deliveries, as fast as the simulated network drains.
	nw.queue = append(inFlight, nw.queue...)
	target := a.LastIndex()
	sent := 0
	for i := 0; a.matchIndex["c"] < target; i++ {
		if i > 20000 || len(nw.queue) == 0 {
			t.Fatalf("c reached %d of %d", a.matchIndex["c"], target)
		}
		m := nw.queue[0]
		nw.queue = nw.queue[1:]
		sent += entriesTo([]Message{m})
		nw.deliver(m)
		if i%6 == 0 {
			nw.proposeNoDeliver("a", "w")
		}
	}
	need := int(target)*4<<10/(64<<10) + 1
	if sent > 2*need+4 {
		t.Fatalf("c caught up through %d with %d batches sent; its backlog needs about %d", target, sent, need)
	}
	t.Logf("caught up through %d with %d batches (about %d needed)", target, sent, need)
}

// TestAReadDoesNotMoveNextIndexBack: a read round's heartbeat goes at the
// match index or the boundary, and its acknowledgement reports no more. A new
// leader on a compacted log, whose no-op append to a peer was lost, took that
// as the peer's progress and lowered nextIndex to the boundary: the next
// heartbeat re-sent every entry after it, all of which the peer held.
func TestAReadDoesNotMoveNextIndexBack(t *testing.T) {
	nw := newNetwork(t, ids(3), 4343)
	nw.electLeader("a")
	for i := 0; i < 60; i++ {
		nw.propose("a", fmt.Sprintf("v%d", i))
	}
	nw.heartbeatRounds()
	for _, id := range []NodeID{"a", "b", "c"} {
		if err := nw.nodes[id].Compact(10); err != nil {
			t.Fatalf("compact %s: %v", id, err)
		}
	}
	nw.isolate("a")
	nw.campaign("c")
	c := nw.nodes["c"]
	for i := 0; i < 1000 && c.Role() != Leader && nw.deliverOne(); i++ {
	}
	if c.Role() != Leader {
		t.Fatal("premise: c did not win")
	}
	var kept []Message
	for _, m := range nw.queue {
		if !(m.From == "c" && m.To == "b" && m.Type == MsgAppendRequest) {
			kept = append(kept, m) // c's no-op append to b is lost
		}
	}
	nw.queue = kept
	if _, err := c.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	nw.drain("c")
	nw.deliverAll()
	bLast := nw.nodes["b"].LastIndex()
	if c.nextIndex["b"] <= bLast {
		t.Fatalf("after the read, nextIndex[b] = %d although b holds through %d", c.nextIndex["b"], bLast)
	}
	nw.queue = nil
	for i := 0; i < c.heartbeatTicks; i++ {
		nw.tick("c")
	}
	for _, m := range nw.queue {
		if m.To == "b" && m.Type == MsgAppendRequest {
			for _, e := range m.Entries {
				if e.Index <= bLast {
					t.Fatalf("the heartbeat re-sent b entry %d, which it holds (through %d)", e.Index, bLast)
				}
			}
		}
	}
}
