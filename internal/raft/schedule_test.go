package raft

import "testing"

// These tests exercise the deterministic simulator's explicit message scheduling:
// duplicate, reordered, delayed, and dropped delivery. Every test is single
// goroutine, uses no wall clock, and is replayable from its seed. The harness's
// continuous checks (R1 election safety, R3 log matching, R5 state-machine safety,
// R7 no-apply-beyond-commit) run after every delivery, so any safety violation a
// bad schedule induced is caught immediately. This is Phase 9 correctness-oriented
// scheduling — NOT the Phase 10 fault-injection framework.

// assertConverged requires every node's log to equal the leader's, entry for entry.
func (nw *network) assertConverged(leader NodeID) {
	nw.t.Helper()
	ll := nw.logs[leader]
	for _, id := range nw.ids {
		if id == leader {
			continue
		}
		l := nw.logs[id]
		if l.LastIndex() != ll.LastIndex() {
			nw.t.Fatalf("%s lastIndex %d != leader %s %d\n%s\n%s",
				id, l.LastIndex(), leader, ll.LastIndex(), nw.dumpLog(id), nw.dumpLog(leader))
		}
		for i := uint64(1); i <= ll.LastIndex(); i++ {
			a, _ := l.At(i)
			b, _ := ll.At(i)
			if a.Term != b.Term || string(a.Data) != string(b.Data) {
				nw.t.Fatalf("%s diverges from leader at index %d", id, i)
			}
		}
	}
}

// drainDuplicating delivers every queued message twice, repeatedly, until quiescent.
func (nw *network) drainDuplicating() {
	for i := 0; i < 1000 && len(nw.queue) > 0; i++ {
		for _, m := range nw.takeQueue() {
			nw.deliver(m)
			nw.deliver(m) // exact duplicate
		}
	}
}

// TestDuplicateDelivery delivers every Raft message exactly twice and requires
// safety, convergence, and — via the harness's per-node apply-once check — that no
// entry is applied twice despite the duplication.
func TestDuplicateDelivery(t *testing.T) {
	nw := newNetwork(t, ids(3), 1300)
	nw.electLeader("a")
	nw.proposeNoDeliver("a", "dup")
	nw.drainDuplicating()

	if nw.nodes["a"].CommitIndex() < 2 {
		t.Fatalf("commit = %d, want >= 2 after a duplicated proposal", nw.nodes["a"].CommitIndex())
	}
	nw.assertLogMatching()
	nw.assertConverged("a")
	// The applied command "dup" was applied exactly once per node (the harness's
	// double-apply guard would have failed otherwise); confirm it is in the log.
	e, _ := nw.logs["a"].At(2)
	if string(e.Data) != "dup" {
		t.Fatalf("index 2 = %q, want dup", e.Data)
	}
}

// TestReorderedDelivery delivers a batch of queued messages in reverse order, then
// lets the rest settle, and requires safe convergence.
func TestReorderedDelivery(t *testing.T) {
	nw := newNetwork(t, ids(3), 1400)
	nw.electLeader("a")
	// Two proposals with no delivery: the leader queues several AppendEntries.
	nw.proposeNoDeliver("a", "one")
	nw.proposeNoDeliver("a", "two")
	q := nw.takeQueue()
	if len(q) < 2 {
		t.Fatalf("expected several queued messages to reorder, got %d", len(q))
	}
	// Deliver them newest-queued first (reverse of FIFO).
	for i := len(q) - 1; i >= 0; i-- {
		nw.deliver(q[i])
	}
	nw.deliverAll() // let responses and any follow-up settle

	if nw.nodes["a"].CommitIndex() < 3 {
		t.Fatalf("commit = %d, want >= 3 after two proposals", nw.nodes["a"].CommitIndex())
	}
	nw.assertLogMatching()
	nw.assertConverged("a")
}

// TestDelayedDelivery holds one follower's AppendEntries back while the rest of the
// cluster makes progress, then releases the now-stale message and requires that it
// is handled safely and the follower still converges.
func TestDelayedDelivery(t *testing.T) {
	nw := newNetwork(t, ids(3), 1500)
	nw.electLeader("a")

	// Propose "one"; hold back the message destined for b, deliver the rest.
	nw.proposeNoDeliver("a", "one")
	var delayed []Message
	for _, m := range nw.takeQueue() {
		if m.To == "b" {
			delayed = append(delayed, m) // hold b's copy
			continue
		}
		nw.deliver(m)
	}
	nw.deliverAll() // c acks; with a+c that is a quorum, so "one" commits

	// Make more progress while b's message is still delayed.
	nw.proposeNoDeliver("a", "two")
	nw.deliverAll()

	if len(delayed) == 0 {
		t.Fatal("expected a delayed message to b")
	}
	// Now release the delayed (stale) message. It must not corrupt b or the cluster.
	for _, m := range delayed {
		nw.deliver(m)
	}
	nw.deliverAll()

	nw.assertLogMatching()
	nw.assertConverged("a")
	if nw.logs["b"].LastIndex() != nw.logs["a"].LastIndex() {
		t.Fatalf("b did not converge after the delayed message: b=%d a=%d", nw.logs["b"].LastIndex(), nw.logs["a"].LastIndex())
	}
}

// TestDroppedDelivery drops every message to a follower for a while (a partition),
// then heals and requires that retransmission catches it up — and that messages
// were genuinely dropped.
func TestDroppedDelivery(t *testing.T) {
	nw := newNetwork(t, ids(3), 1600)
	nw.electLeader("a")
	before := nw.dropped

	nw.isolate("c")
	for _, cmd := range []string{"1", "2", "3"} {
		nw.proposeNoDeliver("a", cmd)
		nw.deliverAll() // messages to c are dropped; a+b still commit
	}
	if nw.dropped <= before {
		t.Fatalf("expected messages to c to be dropped; dropped counter unchanged (%d)", nw.dropped)
	}
	if nw.logs["c"].LastIndex() == nw.logs["a"].LastIndex() {
		t.Fatal("c should be behind while partitioned")
	}

	// Heal and retransmit via one more proposal.
	nw.heal()
	nw.proposeNoDeliver("a", "4")
	nw.deliverAll()

	nw.assertLogMatching()
	nw.assertConverged("a")
}
