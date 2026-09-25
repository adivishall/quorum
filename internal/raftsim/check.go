package raftsim

import (
	"fmt"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
)

// checker is the cross-node history the continuous invariants are checked
// against. Every check runs after every event (or at the exact moment the
// property is defined — a send, an apply, a restart), and the first violation
// stops the cluster.
type checker struct {
	termLeader   map[uint64]NodeID // INV-R1: the one node that led each term
	appendLeader map[uint64]NodeID // INV-R1: the one node that sent AppendEntries in each term
	votes        map[voteKey]NodeID
	committed    []committedEntry // INV-R4/R5: the globally committed prefix
	applied      map[uint64]raft.Entry
	proposals    map[string]int    // INV-F4: accepted commands -> step
	appliedCmd   map[string]uint64 // INV-F4: command -> the one index it was applied at
	// kvCommands are the accepted key-value commands (Phase 12). They carry no
	// request identity until Phase 13, so two clients deleting one key — or one
	// client writing the same value twice — propose byte-identical commands
	// that are rightly applied at two indexes: INV-F4's "exactly one index"
	// half does not apply to them; its "really proposed" half does.
	kvCommands map[string]bool
}

type voteKey struct {
	voter NodeID
	term  uint64
}

type committedEntry struct {
	e          raft.Entry
	commitTerm uint64 // the term of the node that first reported it committed
}

func (k *checker) init() {
	k.termLeader = map[uint64]NodeID{}
	k.appendLeader = map[uint64]NodeID{}
	k.votes = map[voteKey]NodeID{}
	k.applied = map[uint64]raft.Entry{}
	k.proposals = map[string]int{}
	k.appliedCmd = map[string]uint64{}
	k.kvCommands = map[string]bool{}
}

func (k *checker) proposed(data string, step int) { k.proposals[data] = step }

// violate records the first violation and stops the cluster.
func (c *Cluster) violate(inv, format string, args ...any) {
	if c.viol != nil {
		return
	}
	c.viol = &Violation{Invariant: inv, Step: c.step, Detail: fmt.Sprintf(format, args...)}
	c.trace.add(c.step, "VIOLATION %s: %s", inv, c.viol.Detail)
	if c.OnViolation != nil {
		c.OnViolation(c.viol)
	}
}

// checkRecovered runs at the instant a node boots, against the shadow's
// independent record of every Save it completed (docs/CRASH_RECOVERY.md).
//
// INV-F2: the recovered state is the last completed Save's state, extended by at
// most a prefix of an interrupted Save's records. The Phase 11 checks sharpen it:
// INV-CR1, term and vote never regress across a crash — the recovered term is at
// least the durably established one, and a vote cast in that term is never
// forgotten (though a vote the interrupted Save was casting may or may not have
// landed); INV-CR2, the durably committed prefix is recovered bit-identical and
// the recovered commit is no lower than the durable one; INV-CR3, an entry is
// applied only after the commit covering it is durable, so the recovered commit
// is never below what the previous incarnation had applied.
func (c *Cluster) checkRecovered(n *node, rec *raftlog.Recovered) {
	if !n.shadow.matches(rec) {
		c.violate("INV-F2", "%s recovered term=%d vote=%q commit=%d entries=%d, which is neither its last persisted state nor that plus a prefix of its interrupted save (%s)",
			n.id, rec.HardState.Term, rec.HardState.Vote, rec.HardState.Commit, len(rec.Entries), n.shadow.describe())
		return
	}
	p := n.shadow.persisted
	if rec.HardState.Term < p.hs.Term {
		c.violate("INV-CR1", "%s recovered term %d below its durable term %d", n.id, rec.HardState.Term, p.hs.Term)
		return
	}
	if rec.HardState.Term == p.hs.Term && p.hs.Vote != "" && rec.HardState.Vote != p.hs.Vote {
		c.violate("INV-CR1", "%s recovered vote %q in term %d but had durably voted for %q", n.id, rec.HardState.Vote, p.hs.Term, p.hs.Vote)
		return
	}
	durableCommit := p.hs.Commit
	if durableCommit > uint64(len(p.entries)) {
		durableCommit = uint64(len(p.entries))
	}
	if rec.HardState.Commit < durableCommit {
		c.violate("INV-CR2", "%s recovered commit %d below its durable commit %d", n.id, rec.HardState.Commit, durableCommit)
		return
	}
	for i := uint64(0); i < durableCommit; i++ {
		if i >= uint64(len(rec.Entries)) || !sameEntry(rec.Entries[i], p.entries[i]) {
			c.violate("INV-CR2", "%s recovered a different entry at committed index %d (durable (t%d,%q))", n.id, i+1, p.entries[i].Term, p.entries[i].Data)
			return
		}
	}
	if rec.HardState.Commit < n.prevApplied {
		c.violate("INV-CR3", "%s recovered commit %d below the %d it had already applied before crashing", n.id, rec.HardState.Commit, n.prevApplied)
	}
}

// checkSend runs at the instant DrainReady hands a message to the network.
//
// INV-R6 (fsync form): the sender's log file holds no un-fsynced byte, and the
// durable state the driver persisted equals the node's in-memory term, vote and
// log — so a power loss at this instant would recover exactly the state the
// message was computed from. INV-R6's observable consequence is checked too: a
// node never grants its vote in one term to two candidates, across any number of
// crashes and restarts. INV-R1 at the message level: only one node ever sends
// AppendEntries in a given term.
func (c *Cluster) checkSend(n *node, m raft.Message) {
	if !n.disk.FullySynced(logPath) {
		c.violate("INV-R6", "%s sent %s while its log held bytes that were not fsynced", n.id, describe(m))
		return
	}
	p := n.shadow.persisted
	if p.hs.Term != n.core.Term() || p.hs.Vote != n.core.VotedFor() {
		c.violate("INV-R6", "%s sent %s with durable term/vote (%d,%q) but in-memory (%d,%q)",
			n.id, describe(m), p.hs.Term, p.hs.Vote, n.core.Term(), n.core.VotedFor())
		return
	}
	if uint64(len(p.entries)) != n.mem.LastIndex() {
		c.violate("INV-R6", "%s sent %s with %d durable entries but %d in memory", n.id, describe(m), len(p.entries), n.mem.LastIndex())
		return
	}
	for i, e := range p.entries {
		if t, _ := n.mem.Term(uint64(i + 1)); t != e.Term {
			c.violate("INV-R6", "%s sent %s while durable entry %d has term %d but in-memory term %d", n.id, describe(m), i+1, e.Term, t)
			return
		}
	}
	switch m.Type {
	case raft.MsgVoteResponse:
		if m.VoteGranted {
			k := voteKey{m.From, m.Term}
			if prev, ok := c.chk.votes[k]; ok && prev != m.To {
				c.violate("INV-R6", "%s granted its term-%d vote to both %s and %s", m.From, m.Term, prev, m.To)
				return
			}
			c.chk.votes[k] = m.To
		}
	case raft.MsgAppendRequest:
		if prev, ok := c.chk.appendLeader[m.Term]; ok && prev != m.From {
			c.violate("INV-R1", "both %s and %s sent AppendEntries as leader of term %d", prev, m.From, m.Term)
			return
		}
		c.chk.appendLeader[m.Term] = m.From
	}
}

// checkApply runs for each entry a node applies: INV-R7 (never beyond commit),
// in-order exactly-once application within an incarnation (INV-P9), INV-R5 (no
// two nodes apply different entries at one index) and INV-F4 (every applied
// command was really proposed and occupies exactly one index).
func (c *Cluster) checkApply(n *node, e raft.Entry) {
	if e.Index > n.core.CommitIndex() {
		c.violate("INV-R7", "%s applied index %d beyond its commit %d", n.id, e.Index, n.core.CommitIndex())
		return
	}
	if e.Index != n.applied+1 {
		c.violate("INV-P9", "%s applied index %d after %d (skipped or repeated)", n.id, e.Index, n.applied)
		return
	}
	if prev, ok := c.chk.applied[e.Index]; ok && !sameEntry(prev, e) {
		c.violate("INV-R5", "%s applied (t%d,%q) at index %d but (t%d,%q) was applied there before",
			n.id, e.Term, e.Data, e.Index, prev.Term, prev.Data)
		return
	}
	c.chk.applied[e.Index] = e
	if len(e.Data) > 0 {
		cmd := string(e.Data)
		if _, ok := c.chk.proposals[cmd]; !ok {
			c.violate("INV-F4", "%s applied command %q at index %d that no leader ever accepted", n.id, cmd, e.Index)
			return
		}
		if c.chk.kvCommands[cmd] {
			return
		}
		if idx, ok := c.chk.appliedCmd[cmd]; ok && idx != e.Index {
			c.violate("INV-F4", "command %q applied at index %d and at index %d", cmd, idx, e.Index)
			return
		}
		c.chk.appliedCmd[cmd] = e.Index
	}
}

// afterEvent runs the continuous checks on the node the event touched, and the
// cross-node checks that involve it.
func (c *Cluster) afterEvent(n *node) {
	if n == nil || !n.up || c.viol != nil {
		return
	}
	n.refresh()
	role, term, commit := n.core.Role(), n.core.Term(), n.core.CommitIndex()
	if role != n.role || term != n.term {
		c.trace.add(c.step, "role %s %s->%s t=%d", n.id, n.role, role, term)
		if role == raft.Leader {
			c.stats.LeaderElections++
		}
	}
	if commit != n.commit {
		c.trace.add(c.step, "commit %s %d->%d", n.id, n.commit, commit)
	}
	if commit < n.commit {
		c.violate("INV-R8", "%s commit moved backward %d -> %d", n.id, n.commit, commit)
		return
	}
	if role == raft.Leader && commit > n.commit {
		if t, _ := n.mem.Term(commit); t != term {
			c.violate("INV-R9", "leader %s of term %d advanced commit to index %d whose entry has term %d", n.id, term, commit, t)
			return
		}
	}
	if role == raft.Leader {
		if prev, ok := c.chk.termLeader[term]; ok && prev != n.id {
			c.violate("INV-R1", "two leaders in term %d: %s and %s", term, prev, n.id)
			return
		}
		c.chk.termLeader[term] = n.id
	}
	n.role, n.term, n.commit = role, term, commit
	if term > c.stats.MaxTerm {
		c.stats.MaxTerm = term
	}
	if commit > c.stats.MaxCommit {
		c.stats.MaxCommit = commit
	}

	c.recordCommits(n)
	c.checkLeaders(n)
	for _, id := range c.ids {
		if o := c.nodes[id]; o != n && o.up && c.viol == nil {
			c.checkMatching(n, o)
		}
	}
}

// recordCommits extends the global committed record with the node's newly
// committed entries, and checks that every node agrees on each committed index
// (INV-R5 at the point of commitment).
func (c *Cluster) recordCommits(n *node) {
	for i := n.verified + 1; i <= n.commit; i++ {
		e := n.entries[i-1]
		if int(i) <= len(c.chk.committed) {
			if ce := c.chk.committed[i-1]; !sameEntry(ce.e, e) {
				c.violate("INV-R5", "index %d is committed as (t%d,%q) on %s but was committed as (t%d,%q) before",
					i, e.Term, e.Data, n.id, ce.e.Term, ce.e.Data)
				return
			}
			continue
		}
		c.chk.committed = append(c.chk.committed, committedEntry{e: e, commitTerm: n.term})
	}
	n.verified = n.commit
}

// checkLeaders checks INV-R2 (a leader never rewrites an entry it holds, for the
// node just touched) and INV-R4 (every leader holds every entry committed in an
// earlier term, for every current leader).
func (c *Cluster) checkLeaders(touched *node) {
	for _, id := range c.ids {
		L := c.nodes[id]
		if !L.up || L.core.Role() != raft.Leader {
			if L.up {
				L.lv = nil
			}
			continue
		}
		term := L.core.Term()
		lv := L.lv
		if lv == nil || lv.inc != L.inc || lv.term != term {
			lv = &leaderView{inc: L.inc, term: term, entries: L.entries}
			L.lv = lv
		} else if L == touched {
			if len(L.entries) < len(lv.entries) {
				c.violate("INV-R2", "leader %s of term %d shrank its log from %d to %d", L.id, term, len(lv.entries), len(L.entries))
				return
			}
			for i := range lv.entries {
				if !sameEntry(lv.entries[i], L.entries[i]) {
					c.violate("INV-R2", "leader %s of term %d rewrote its own entry at index %d", L.id, term, i+1)
					return
				}
			}
			lv.entries = L.entries
		}
		for i := lv.r4Through; i < len(c.chk.committed); i++ {
			ce := c.chk.committed[i]
			if ce.commitTerm >= term {
				continue
			}
			if i >= len(L.entries) || !sameEntry(L.entries[i], ce.e) {
				c.violate("INV-R4", "leader %s of term %d lacks entry %d (t%d,%q) committed in term %d",
					L.id, term, i+1, ce.e.Term, ce.e.Data, ce.commitTerm)
				return
			}
		}
		lv.r4Through = len(c.chk.committed)
	}
}

// checkMatching checks INV-R3 (Log Matching) between two nodes: if their logs hold
// an entry with the same index and term, they are identical through that index.
func (c *Cluster) checkMatching(a, b *node) {
	la, lb := a.entries, b.entries
	hi := len(la)
	if len(lb) < hi {
		hi = len(lb)
	}
	for i := hi; i >= 1; i-- {
		if la[i-1].Term != lb[i-1].Term {
			continue
		}
		for j := 0; j < i; j++ {
			if !sameEntry(la[j], lb[j]) {
				c.violate("INV-R3", "%s and %s share (index %d, term %d) but differ at index %d", a.id, b.id, i, la[i-1].Term, j+1)
				return
			}
		}
		return
	}
}

// r10Snap is the protected state a stale message must not change.
type r10Snap struct {
	role      raft.Role
	term      uint64
	vote      NodeID
	lastIndex uint64
	lastTerm  uint64
	commit    uint64
}

func snapR10(n *node) r10Snap {
	last := n.core.LastIndex()
	lt, _ := n.mem.Term(last)
	return r10Snap{n.core.Role(), n.core.Term(), n.core.VotedFor(), last, lt, n.core.CommitIndex()}
}

// checkR10 checks INV-R10: a message from a lower term changed nothing.
func (c *Cluster) checkR10(n *node, before r10Snap, m raft.Message) {
	if after := snapR10(n); after != before {
		c.violate("INV-R10", "stale %s (term %d) changed %s from %+v to %+v", describe(m), m.Term, n.id, before, after)
	}
}

// checkConverged checks INV-F3 (liveness after faults stop): every node is up,
// exactly one leads, every log is identical to the leader's, the leader has
// committed an entry of its own term, and every node has committed and applied
// the whole log.
func (c *Cluster) checkConverged() {
	c.trace.add(c.step, "check-converged")
	var down []NodeID
	for _, id := range c.ids {
		if !c.nodes[id].up {
			down = append(down, id)
		}
	}
	if len(down) > 0 {
		c.violate("INV-F3", "nodes %v are still down after stabilization", down)
		return
	}
	leaders := c.Leaders()
	if len(leaders) != 1 {
		c.violate("INV-F3", "want exactly one leader after stabilization, have %v", leaders)
		return
	}
	L := c.nodes[leaders[0]]
	lterm := L.core.Term()
	last := L.core.LastIndex()
	if t, _ := L.mem.Term(last); last == 0 || t != lterm || L.core.CommitIndex() != last {
		c.violate("INV-F3", "leader %s (term %d) has not committed an entry of its own term: last=%d (t%d) commit=%d",
			L.id, lterm, last, t, L.core.CommitIndex())
		return
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		n.refresh()
		if n.core.Term() != lterm || len(n.entries) != len(L.entries) {
			c.violate("INV-F3", "%s (term %d, %d entries) has not converged to leader %s (term %d, %d entries)",
				id, n.core.Term(), len(n.entries), L.id, lterm, len(L.entries))
			return
		}
		for i := range n.entries {
			if !sameEntry(n.entries[i], L.entries[i]) {
				c.violate("INV-F3", "%s differs from leader %s at index %d after stabilization", id, L.id, i+1)
				return
			}
		}
		if n.core.CommitIndex() != last || n.core.AppliedIndex() != last {
			c.violate("INV-F3", "%s commit=%d applied=%d, want both %d", id, n.core.CommitIndex(), n.core.AppliedIndex(), last)
			return
		}
	}
	c.trace.add(c.step, "converged leader=%s term=%d index=%d", L.id, lterm, last)
}
