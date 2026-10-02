package raft

import (
	"bytes"
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
