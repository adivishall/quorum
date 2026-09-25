package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

// network is the deterministic Raft simulation harness (docs/RAFT.md §12). It runs
// N cores in one goroutine with an explicit message queue the test controls: no
// sockets, no clock, no goroutines, replayable from a seed.
//
// It gives tests explicit control over message scheduling, exercised by
// schedule_test.go: FIFO delivery (deliverOne/deliverAll), single-message delivery
// (deliver), draining the queue for reordering/duplication (takeQueue), enqueuing
// without delivering (proposeNoDeliver), and directional partition/drop
// (isolate/heal). Duplicate, reorder, delayed, and dropped delivery are all tested.
// The continuously-checkable invariants (R1 election safety, R3 log matching, R5
// state-machine safety, R7 no-apply-beyond-commit) run after every step. This is
// deliberately NOT the Phase 10 fault-injection framework: it is hand-scheduled
// correctness testing, not a reusable systematic failure-matrix generator.
type network struct {
	t     *testing.T
	ids   []NodeID
	nodes map[NodeID]*Raft
	logs  map[NodeID]*replication.MemoryLog

	queue []Message // in-flight messages, FIFO by default

	// Invariant bookkeeping.
	termLeader map[uint64]NodeID // term -> the one leader ever seen for it (R1)
	appliedLog map[uint64]Entry  // index -> the entry applied there, by any node (R5)
	applyCount map[NodeID]uint64 // per-node highest applied index, for double-apply checks

	blocked   map[[2]NodeID]bool // directional From->To links that drop
	appendsTo map[NodeID]int     // AppendEntries requests actually delivered to a node
	dropped   int

	reads map[NodeID][]ReadState // confirmed ReadIndex requests drained from each node (Phase 12)
}

// key builds the directional-link key.
func linkKey(from, to NodeID) [2]NodeID { return [2]NodeID{from, to} }

// isolate blocks all links in and out of id (a total partition of one node).
func (nw *network) isolate(id NodeID) {
	for _, other := range nw.ids {
		if other == id {
			continue
		}
		nw.blocked[linkKey(id, other)] = true
		nw.blocked[linkKey(other, id)] = true
	}
}

// heal removes every partition.
func (nw *network) heal() { nw.blocked = map[[2]NodeID]bool{} }

func (nw *network) reachable(from, to NodeID) bool { return !nw.blocked[linkKey(from, to)] }

// seedNode replaces a node with one whose log holds the given entries at the given
// term/vote — for constructing exact divergent-log scenarios (paper Figure 7/8).
// It must be called before the scenario runs.
func (nw *network) seedNode(id NodeID, entries []Entry, term uint64, vote NodeID, seed int64) {
	nw.t.Helper()
	lg := replication.NewMemoryLog()
	if len(entries) > 0 {
		if err := lg.Append(entries...); err != nil {
			nw.t.Fatalf("seed %s: %v", id, err)
		}
	}
	r, err := New(Config{
		ID: id, Peers: nw.ids, Rand: rand.New(rand.NewSource(seed)),
		Log: lg, Term: term, Vote: vote,
	})
	if err != nil {
		nw.t.Fatalf("seed %s: %v", id, err)
	}
	nw.nodes[id] = r
	nw.logs[id] = lg
}

// newNetwork builds an n-node group. Each node gets a distinct rand seed so
// election timeouts differ and a split vote resolves, which is exactly what
// randomized timeouts are for.
func newNetwork(t *testing.T, ids []NodeID, seedBase int64) *network {
	t.Helper()
	nw := &network{
		t:          t,
		ids:        append([]NodeID(nil), ids...),
		nodes:      map[NodeID]*Raft{},
		logs:       map[NodeID]*replication.MemoryLog{},
		termLeader: map[uint64]NodeID{},
		appliedLog: map[uint64]Entry{},
		applyCount: map[NodeID]uint64{},
		blocked:    map[[2]NodeID]bool{},
		appendsTo:  map[NodeID]int{},
		reads:      map[NodeID][]ReadState{},
	}
	sort.Slice(nw.ids, func(i, j int) bool { return nw.ids[i] < nw.ids[j] })
	for i, id := range nw.ids {
		lg := replication.NewMemoryLog()
		r, err := New(Config{
			ID:    id,
			Peers: ids,
			Rand:  rand.New(rand.NewSource(seedBase + int64(i)*1000 + 1)),
			Log:   lg,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		nw.nodes[id] = r
		nw.logs[id] = lg
	}
	return nw
}

// drain collects a node's pending effects: it enqueues its outbound messages,
// advances it, and applies its committed entries. It respects the Ready contract
// (persist would precede send on a real driver; here both are synchronous).
func (nw *network) drain(id NodeID) {
	r := nw.nodes[id]
	for r.HasReady() {
		rd := r.Ready()
		nw.queue = append(nw.queue, rd.Messages...)
		nw.reads[id] = append(nw.reads[id], rd.ReadStates...)
		r.Advance()
	}
	nw.applyCommitted(id)
	nw.checkContinuousInvariants()
}

// applyCommitted applies a node's committed-but-unapplied entries, recording the
// apply history for the state-machine-safety check.
func (nw *network) applyCommitted(id NodeID) {
	r := nw.nodes[id]
	for _, e := range r.NextApply() {
		// INV-R7: never apply beyond commit (the log enforces it, asserted here).
		if e.Index > r.CommitIndex() {
			nw.t.Fatalf("%s applied index %d beyond commit %d (INV-R7)", id, e.Index, r.CommitIndex())
		}
		// INV-R5: no two nodes apply a different entry at the same index.
		if prev, ok := nw.appliedLog[e.Index]; ok {
			if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
				nw.t.Fatalf("state machine safety violated at index %d: %s applied {t=%d,d=%q} but earlier {t=%d,d=%q} (INV-R5)",
					e.Index, id, e.Term, e.Data, prev.Term, prev.Data)
			}
		} else {
			nw.appliedLog[e.Index] = e
		}
		// Application is once-per-index per node: applied indexes strictly increase.
		if e.Index <= nw.applyCount[id] {
			nw.t.Fatalf("%s applied index %d but had already applied through %d (double apply)", id, e.Index, nw.applyCount[id])
		}
		nw.applyCount[id] = e.Index
		if err := r.AppliedTo(e.Index); err != nil {
			nw.t.Fatalf("%s AppliedTo(%d): %v", id, e.Index, err)
		}
	}
}

// tick advances one node and drains it.
func (nw *network) tick(id NodeID) {
	nw.nodes[id].Tick()
	nw.drain(id)
}

// tickAll ticks every node once, in id order.
func (nw *network) tickAll() {
	for _, id := range nw.ids {
		nw.tick(id)
	}
}

// deliver hands one specific message to its recipient and drains it. A message on
// a partitioned link is dropped. This is the single primitive the scheduling
// controls (FIFO, reorder, duplicate, delay) are all built from.
func (nw *network) deliver(m Message) {
	if !nw.reachable(m.From, m.To) {
		nw.dropped++ // partitioned link: the message is lost
		return
	}
	if m.Type == MsgAppendRequest {
		nw.appendsTo[m.To]++
	}
	if err := nw.nodes[m.To].Step(m); err != nil {
		nw.t.Fatalf("Step(%s <- %s %s): %v", m.To, m.From, m.Type, err)
	}
	nw.drain(m.To)
}

// deliverOne pops and delivers the oldest queued message (FIFO). It returns false
// if the queue was empty.
func (nw *network) deliverOne() bool {
	if len(nw.queue) == 0 {
		return false
	}
	m := nw.queue[0]
	nw.queue = nw.queue[1:]
	nw.deliver(m)
	return true
}

// takeQueue removes and returns all currently-queued messages, so a test can
// reorder, duplicate, or delay them explicitly before delivering.
func (nw *network) takeQueue() []Message {
	q := nw.queue
	nw.queue = nil
	return q
}

// proposeNoDeliver appends a proposal on the leader and enqueues its outbound
// messages WITHOUT delivering them, so the test controls their scheduling.
func (nw *network) proposeNoDeliver(id NodeID, data string) {
	nw.t.Helper()
	if err := nw.nodes[id].Propose([]byte(data)); err != nil {
		nw.t.Fatalf("Propose on %s: %v", id, err)
	}
	nw.drain(id)
}

// deliverAll delivers messages until the network is quiescent (no messages, no
// pending effects). It bounds iterations so a bug cannot hang the test.
func (nw *network) deliverAll() {
	for i := 0; i < 100000; i++ {
		if !nw.deliverOne() {
			return
		}
	}
	nw.t.Fatal("deliverAll did not quiesce (possible message storm)")
}

// campaign ticks id until it becomes a candidate (its randomized timeout fires),
// leaving its vote requests queued.
func (nw *network) campaign(id NodeID) {
	nw.t.Helper()
	for i := 0; i < 4*nw.nodes[id].electionTicks; i++ {
		nw.tick(id)
		if nw.nodes[id].Role() == Candidate || nw.nodes[id].Role() == Leader {
			return
		}
	}
	nw.t.Fatalf("%s did not become candidate after ticking", id)
}

// electLeader forces id to win an election and settle, then asserts it leads.
func (nw *network) electLeader(id NodeID) {
	nw.t.Helper()
	nw.campaign(id)
	nw.deliverAll()
	if nw.nodes[id].Role() != Leader {
		nw.t.Fatalf("%s did not become leader; role=%s term=%d", id, nw.nodes[id].Role(), nw.nodes[id].Term())
	}
	nw.assertAtMostOneLeaderPerTerm()
}

// propose submits a command on the leader and settles the network.
func (nw *network) propose(id NodeID, data string) {
	nw.t.Helper()
	if err := nw.nodes[id].Propose([]byte(data)); err != nil {
		nw.t.Fatalf("Propose on %s: %v", id, err)
	}
	nw.drain(id)
	nw.deliverAll()
}

// leaders returns the ids currently in the Leader role.
func (nw *network) leaders() []NodeID {
	var out []NodeID
	for _, id := range nw.ids {
		if nw.nodes[id].Role() == Leader {
			out = append(out, id)
		}
	}
	return out
}

// --- continuous invariant checks ---

func (nw *network) checkContinuousInvariants() {
	nw.assertAtMostOneLeaderPerTerm()
	nw.assertLogMatching()
}

// assertAtMostOneLeaderPerTerm enforces INV-R1: at most one leader in any term,
// ever (across the whole run, not just simultaneously).
func (nw *network) assertAtMostOneLeaderPerTerm() {
	for _, id := range nw.ids {
		r := nw.nodes[id]
		if r.Role() != Leader {
			continue
		}
		if prev, ok := nw.termLeader[r.Term()]; ok && prev != id {
			nw.t.Fatalf("two leaders in term %d: %s and %s (INV-R1)", r.Term(), prev, id)
		}
		nw.termLeader[r.Term()] = id
	}
}

// assertLogMatching enforces INV-R3: if two logs share an entry at the same
// (index, term), they are identical through that index.
func (nw *network) assertLogMatching() {
	for a := 0; a < len(nw.ids); a++ {
		for b := a + 1; b < len(nw.ids); b++ {
			la, lb := nw.logs[nw.ids[a]], nw.logs[nw.ids[b]]
			hi := la.LastIndex()
			if lb.LastIndex() < hi {
				hi = lb.LastIndex()
			}
			for i := hi; i >= 1; i-- {
				ta, _ := la.Term(i)
				tb, _ := lb.Term(i)
				if ta != tb {
					continue
				}
				// Same (index, term): every entry through i must match.
				nw.assertPrefixEqual(nw.ids[a], nw.ids[b], i)
				break // once they match at i, lower indexes are covered
			}
		}
	}
}

func (nw *network) assertPrefixEqual(a, b NodeID, through uint64) {
	la, lb := nw.logs[a], nw.logs[b]
	for i := uint64(1); i <= through; i++ {
		ea, _ := la.At(i)
		eb, _ := lb.At(i)
		if ea.Term != eb.Term || string(ea.Data) != string(eb.Data) {
			nw.t.Fatalf("log matching violated: %s and %s share (index %d, term %d) but differ at index %d (INV-R3)",
				a, b, through, ea.Term, i)
		}
	}
}

// dumpLog renders a node's log for failure messages.
func (nw *network) dumpLog(id NodeID) string {
	l := nw.logs[id]
	s := fmt.Sprintf("%s[role=%s term=%d commit=%d]:", id, nw.nodes[id].Role(), nw.nodes[id].Term(), l.CommitIndex())
	for i := uint64(1); i <= l.LastIndex(); i++ {
		e, _ := l.At(i)
		s += fmt.Sprintf(" (%d,t%d,%q)", i, e.Term, e.Data)
	}
	return s
}
