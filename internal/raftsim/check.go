package raftsim

import (
	"fmt"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
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
	// snapStates is the SHA-256 of the state of every snapshot seen at each
	// index (Phase 14): one index, one state.
	snapStates map[uint64][32]byte
	// Phase 15: the latest committed configuration of the global record (the
	// genesis before any), and the nodes a committed stable configuration
	// excludes, with the removing entry's term (INV-MB1, INV-MB4).
	conf      replication.Configuration
	removedAt map[NodeID]uint64
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
	k.snapStates = map[uint64][32]byte{}
	k.removedAt = map[NodeID]uint64{}
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
//
// Phase 14: the log's boundary is part of the durable state compared; a
// recovery that completed an interrupted install (appending the boundary
// record the install did not write) matches a candidate plus that record; the
// commit INV-CR3 compares is the recovered node's, which a restored snapshot
// raises to its index; and INV-SN3 — the recovered log is never compacted past
// the recovered snapshot, nor compacted at all without one.
func (c *Cluster) checkRecovered(n *node, rc *raftnode.Recovered) {
	rec := rc.State
	ok := n.shadow.matches(rec)
	if !ok && rc.Repaired && rc.Snapshot != nil {
		ok = n.shadow.matchesAfter(rec, raftlogBoundary(rc.Snapshot.Index, rc.Snapshot.Term))
	}
	if !ok {
		c.violate("INV-F2", "%s recovered boundary=%d term=%d vote=%q commit=%d entries=%d, which is neither its last persisted state nor that plus a prefix of its interrupted write (%s)",
			n.id, rec.Boundary.Index, rec.HardState.Term, rec.HardState.Vote, rec.HardState.Commit, len(rec.Entries), n.shadow.describe())
		return
	}
	switch {
	case rc.Snapshot == nil && rec.Boundary.Index > 0:
		c.violate("INV-SN3", "%s recovered a log compacted through %d with no snapshot", n.id, rec.Boundary.Index)
		return
	case rc.Snapshot != nil && rec.Boundary.Index > rc.Snapshot.Index:
		c.violate("INV-SN3", "%s recovered a log compacted through %d past its snapshot at %d", n.id, rec.Boundary.Index, rc.Snapshot.Index)
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
	durableCommit := p.commit()
	if rec.HardState.Commit < durableCommit {
		c.violate("INV-CR2", "%s recovered commit %d below its durable commit %d", n.id, rec.HardState.Commit, durableCommit)
		return
	}
	for i := max(p.b.Index, rec.Boundary.Index) + 1; i <= durableCommit; i++ {
		want := p.entries[i-p.b.Index-1]
		if i > rec.LastIndex() || !sameEntry(rec.Entries[i-rec.Boundary.Index-1], want) {
			c.violate("INV-CR2", "%s recovered a different entry at committed index %d (durable (t%d,%q))", n.id, i, want.Term, want.Data)
			return
		}
	}
	if commit := rc.Core.CommitIndex(); commit < n.prevApplied {
		c.violate("INV-CR3", "%s recovered commit %d below the %d it had already applied before crashing", n.id, commit, n.prevApplied)
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
	if p.last() != n.mem.LastIndex() {
		c.violate("INV-R6", "%s sent %s with a durable log ending at %d but %d in memory", n.id, describe(m), p.last(), n.mem.LastIndex())
		return
	}
	if mb, mt := n.mem.Boundary(); p.b.Index != mb || p.b.Term != mt {
		c.violate("INV-R6", "%s sent %s with durable boundary (%d,%d) but in-memory (%d,%d)", n.id, describe(m), p.b.Index, p.b.Term, mb, mt)
		return
	}
	for i, e := range p.entries {
		idx := p.b.Index + uint64(i) + 1
		if t, _ := n.mem.Term(idx); t != e.Term {
			c.violate("INV-R6", "%s sent %s while durable entry %d has term %d but in-memory term %d", n.id, describe(m), idx, e.Term, t)
			return
		}
	}
	// INV-MB5: only a voter of its own configuration campaigns, and only the
	// voters whose votes count are asked. (Any node may GRANT a vote: it may be
	// a voter of a configuration it has not received, and whether the vote
	// counts is the candidate's configuration's decision.) The one other
	// campaign: a node whose own uncommitted removal is its configuration,
	// under the joint configuration that removal finalizes.
	if m.Type == raft.MsgVoteRequest {
		conf, prev := c.campaignConfs(n)
		ok := conf.IsVoter(n.id) && conf.IsVoter(m.To)
		if prev != nil {
			ok = prev.IsVoter(n.id) && (conf.IsVoter(m.To) || prev.IsVoter(m.To))
		}
		if !ok {
			c.violate("INV-MB5", "%s sent %s under its configuration %s", n.id, describe(m), conf)
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
	if e.Type != replication.EntryNormal {
		return // a configuration entry is no command (Phase 15; INV-MB1 checks it)
	}
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
	lastBefore := n.last() // the log as of the previous event (Phase 15, INV-MB3)
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
	if c.checkMembership(n, role, lastBefore); c.viol != nil {
		return
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
// (INV-R5 at the point of commitment). Entries below the node's boundary are
// not in its log (Phase 14): a snapshot covers them (checked by INV-SN1), and
// the boundary's own term must be the committed entry's. A node records its
// commits before it compacts, so the record never has a gap.
func (c *Cluster) recordCommits(n *node) {
	commit := n.core.CommitIndex()
	base, baseTerm := n.mem.Boundary()
	for i := n.verified + 1; i <= commit; i++ {
		if i <= base {
			if i == base && int(i) <= len(c.chk.committed) && c.chk.committed[i-1].e.Term != baseTerm {
				c.violate("INV-R5", "%s's boundary (%d, term %d) contradicts the entry committed there (term %d)", n.id, i, baseTerm, c.chk.committed[i-1].e.Term)
				return
			}
			continue
		}
		if int(i) > len(c.chk.committed)+1 {
			c.violate("harness", "%s committed index %d but the global record ends at %d", n.id, i, len(c.chk.committed))
			return
		}
		e, err := n.mem.At(i)
		if err != nil {
			c.violate("harness", "%s committed index %d it does not hold: %v", n.id, i, err)
			return
		}
		if int(i) <= len(c.chk.committed) {
			if ce := c.chk.committed[i-1]; !sameEntry(ce.e, e) {
				c.violate("INV-R5", "index %d is committed as (t%d,%q) on %s but was committed as (t%d,%q) before",
					i, e.Term, e.Data, n.id, ce.e.Term, ce.e.Data)
				return
			}
			continue
		}
		c.chk.committed = append(c.chk.committed, committedEntry{e: e, commitTerm: n.core.Term()})
		if e.Type == replication.EntryConfig {
			if c.commitConf(e); c.viol != nil {
				return
			}
		}
	}
	n.verified = max(n.verified, commit)
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
			lv = &leaderView{inc: L.inc, term: term, base: L.base, entries: L.entries}
			L.lv = lv
		} else if L == touched {
			// Over the indexes both views hold (a compaction moves the base).
			lvLast := lv.base + uint64(len(lv.entries))
			if L.last() < lvLast {
				c.violate("INV-R2", "leader %s of term %d shrank its log from %d to %d", L.id, term, lvLast, L.last())
				return
			}
			for i := max(lv.base, L.base) + 1; i <= lvLast; i++ {
				if !sameEntry(lv.entries[i-lv.base-1], L.entries[i-L.base-1]) {
					c.violate("INV-R2", "leader %s of term %d rewrote its own entry at index %d", L.id, term, i)
					return
				}
			}
			lv.base, lv.entries = L.base, L.entries
		}
		for i := lv.r4Through; i < len(c.chk.committed); i++ {
			ce := c.chk.committed[i]
			idx := uint64(i + 1)
			if ce.commitTerm >= term {
				continue
			}
			if idx <= L.base {
				// Compacted: the leader holds it in its snapshot (INV-SN1); the
				// boundary's term is still the committed entry's.
				if idx == L.base && L.baseTerm != ce.e.Term {
					c.violate("INV-R4", "leader %s of term %d has boundary (%d, term %d) but term %d is committed there", L.id, term, idx, L.baseTerm, ce.e.Term)
					return
				}
				continue
			}
			if idx > L.last() || !sameEntry(L.entries[idx-L.base-1], ce.e) {
				c.violate("INV-R4", "leader %s of term %d lacks entry %d (t%d,%q) committed in term %d",
					L.id, term, idx, ce.e.Term, ce.e.Data, ce.commitTerm)
				return
			}
		}
		lv.r4Through = len(c.chk.committed)
	}
}

// checkMatching checks INV-R3 (Log Matching) between two nodes: if their logs hold
// an entry with the same index and term, they are identical through that index —
// over the indexes both still hold (Phase 14: below a boundary, a snapshot).
func (c *Cluster) checkMatching(a, b *node) {
	lo := max(a.base, b.base)
	hi := min(a.last(), b.last())
	for i := hi; i > lo; i-- {
		if a.termAt(i) != b.termAt(i) {
			continue
		}
		for j := lo + 1; j <= i; j++ {
			if !sameEntry(a.entries[j-a.base-1], b.entries[j-b.base-1]) {
				c.violate("INV-R3", "%s and %s share (index %d, term %d) but differ at index %d", a.id, b.id, i, a.termAt(i), j)
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
// the whole log. Phase 15: "every node" is every member of the leader's
// configuration — a removed node or a spare that was never added has nothing
// to converge to — and no membership change may still be under way (a joint
// configuration is always completed).
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
	conf, _ := L.core.Conf()
	if L.core.ConfPending() {
		c.violate("INV-F3", "leader %s's membership change is still under way after stabilization: %s", L.id, conf)
		return
	}
	L.refresh()
	for _, id := range conf.Members() {
		n := c.nodes[id]
		n.refresh()
		if n.core.Term() != lterm || n.last() != L.last() {
			c.violate("INV-F3", "%s (term %d, last index %d) has not converged to leader %s (term %d, last index %d)",
				id, n.core.Term(), n.last(), L.id, lterm, L.last())
			return
		}
		for i := max(n.base, L.base) + 1; i <= last; i++ {
			if !sameEntry(n.entries[i-n.base-1], L.entries[i-L.base-1]) {
				c.violate("INV-F3", "%s differs from leader %s at index %d after stabilization", id, L.id, i)
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

// --- Phase 15: membership (docs/MEMBERSHIP.md §8) ---

// commitConf records a newly committed configuration entry of the global
// record. INV-MB1: it is reached from the previous committed configuration by
// exactly one transition of docs/MEMBERSHIP.md §4 — an independent statement
// of the rules, not the core's own function. A committed stable configuration
// excludes the nodes outside it from ever leading a later term (INV-MB4).
func (c *Cluster) commitConf(e raft.Entry) {
	next, err := replication.DecodeConfiguration(e.Data)
	if err != nil {
		c.violate("INV-MB1", "committed configuration entry %d does not decode: %v", e.Index, err)
		return
	}
	prev := c.chk.conf
	if why := invalidTransition(prev, next); why != "" {
		c.violate("INV-MB1", "committed configuration %s at index %d does not follow %s: %s", next, e.Index, prev, why)
		return
	}
	c.chk.conf = next
	c.stats.ConfCommits++
	switch {
	case next.Joint():
		c.stats.JointCommits++
	case prev.Joint():
		c.stats.FinalCommits++
	default:
		c.stats.LearnerCommits++
	}
	c.trace.add(c.step, "conf-committed index=%d term=%d %s", e.Index, e.Term, next)
	if next.Joint() {
		return
	}
	for _, id := range c.ids {
		switch {
		case next.IsMember(id):
			delete(c.chk.removedAt, id) // (re-)added
		case prev.IsMember(id):
			c.chk.removedAt[id] = e.Term
		}
	}
}

// invalidTransition says why next is not one transition from prev, or "".
func invalidTransition(prev, next replication.Configuration) string {
	ids := func(ms []replication.Member) map[NodeID]bool {
		out := map[NodeID]bool{}
		for _, m := range ms {
			out[m.ID] = true
		}
		return out
	}
	same := func(a, b map[NodeID]bool) bool {
		if len(a) != len(b) {
			return false
		}
		for id := range a {
			if !b[id] {
				return false
			}
		}
		return true
	}
	// diff returns the ids in a not in b.
	diff := func(a, b map[NodeID]bool) []NodeID {
		var out []NodeID
		for id := range a {
			if !b[id] {
				out = append(out, id)
			}
		}
		return out
	}
	pv, pl := ids(prev.Voters), ids(prev.Learners)
	nv, no, nl := ids(next.Voters), ids(next.Outgoing), ids(next.Learners)
	if len(nv) == 0 {
		return "no voters"
	}
	if prev.Joint() {
		if next.Joint() {
			return "a joint configuration follows a joint one"
		}
		if !same(nv, pv) || !same(nl, pl) {
			return "the final configuration is not the joint one's voters and learners"
		}
		return ""
	}
	if !next.Joint() {
		// A learner added or removed; the voters unchanged.
		if !same(nv, pv) {
			return "the voters changed without a joint configuration"
		}
		if len(diff(nl, pl))+len(diff(pl, nl)) != 1 {
			return "not exactly one learner added or removed"
		}
		for _, id := range diff(nl, pl) {
			if pv[id] {
				return "a voter became a learner"
			}
		}
		return ""
	}
	// A joint configuration: the outgoing voters are the previous voters, and
	// the incoming differ by one — a promoted learner, or a removed voter.
	if !same(no, pv) {
		return "the outgoing voters are not the previous voters"
	}
	added, removed := diff(nv, pv), diff(pv, nv)
	switch {
	case len(added) == 1 && len(removed) == 0:
		if !pl[added[0]] {
			return "a voter was added that was not a learner"
		}
		if !same(nl, func() map[NodeID]bool {
			m := ids(prev.Learners)
			delete(m, added[0])
			return m
		}()) {
			return "the learners are not the previous learners less the promoted one"
		}
	case len(added) == 0 && len(removed) == 1:
		if !same(nl, pl) {
			return "the learners changed with a voter's removal"
		}
	default:
		return fmt.Sprintf("the voters changed by +%d/-%d", len(added), len(removed))
	}
	return ""
}

// derivedConf is the configuration a node's log and snapshot give — computed
// independently of the core: the latest configuration entry in its log, else
// its published snapshot's, else its genesis (a joiner's is empty).
func (c *Cluster) derivedConf(n *node) replication.Configuration {
	for i := len(n.entries) - 1; i >= 0; i-- {
		if e := n.entries[i]; e.Type == replication.EntryConfig {
			conf, _ := replication.DecodeConfiguration(e.Data)
			return conf
		}
	}
	if p := n.dur.Snap.Published(); p.Index > 0 {
		return p.Conf
	}
	if c.isGenesis(n.id) {
		return replication.VotersOf(c.cfg.genesis(c.ids))
	}
	return replication.Configuration{}
}

// confThrough is the configuration in n's log at index bound (>= its
// boundary): the latest configuration entry at or below it, else the base.
func (c *Cluster) confThrough(n *node, bound uint64) replication.Configuration {
	conf, _, _ := c.confEntryThrough(n, bound)
	return conf
}

// confEntryThrough is confThrough with where the configuration comes from:
// the index of its entry when it is in n's log (inLog), else the index its
// base holds at — the published snapshot's, or 0 for the genesis.
func (c *Cluster) confEntryThrough(n *node, bound uint64) (conf replication.Configuration, index uint64, inLog bool) {
	for i := len(n.entries) - 1; i >= 0; i-- {
		if e := n.entries[i]; e.Index <= bound && e.Type == replication.EntryConfig {
			conf, _ := replication.DecodeConfiguration(e.Data)
			return conf, e.Index, true
		}
	}
	if p := n.dur.Snap.Published(); p.Index > 0 {
		return p.Conf, p.Index, false
	}
	if c.isGenesis(n.id) {
		return replication.VotersOf(c.cfg.genesis(c.ids)), 0, false
	}
	return replication.Configuration{}, 0, false
}

// campaignConfs derives, from n's log alone, the configuration it campaigns
// under and — when its configuration is the uncommitted final entry of its
// own removal and it was a voter of the joint configuration before it — that
// joint configuration, which it must also win (raft.campaignRule's case).
func (c *Cluster) campaignConfs(n *node) (raft.Configuration, *raft.Configuration) {
	conf := c.derivedConf(n)
	if conf.IsVoter(n.id) {
		return conf, nil
	}
	at := -1
	for i := len(n.entries) - 1; i >= 0; i-- {
		if n.entries[i].Type == replication.EntryConfig {
			at = i
			break
		}
	}
	if at < 0 || n.entries[at].Index <= n.core.CommitIndex() {
		return conf, nil
	}
	prev := c.confThrough(n, n.entries[at].Index-1)
	if !prev.Joint() || !prev.IsVoter(n.id) || !raft.Final(prev).Equal(conf) {
		return conf, nil
	}
	return conf, &prev
}

// holdsDurably reports whether node x's durable state holds the entry (i, t):
// its persisted log has it, or its persisted boundary covers it.
func holdsDurably(x *node, i, t uint64) bool {
	p := x.shadow.persisted
	switch {
	case i < p.b.Index:
		return true
	case i == p.b.Index:
		return p.b.Term == t
	case i <= p.last():
		return p.entries[i-p.b.Index-1].Term == t
	}
	return false
}

// checkMembership runs after every event on the node it touched:
//
//   - INV-MB2 (and INV-MB8, after an install or a boot): the node's
//     configuration is exactly the one its log
//     and snapshot give — the core holds no membership of its own.
//   - INV-MB3: a leader that advanced its commit index did so with the entry
//     durable on a majority of its configuration's voters AND, when joint, of
//     its outgoing voters — its configuration being its current one after the
//     event: a configuration takes effect when it is appended (Raft §6), a
//     final entry the leader appended in this very event included. A final
//     entry is appended only once the joint configuration it ends is
//     committed under the joint rule: when a leader appends one, its commit
//     covers the joint entry, and that entry is durable on a majority of both
//     of the joint configuration's voter sets. A leader that removed itself
//     in this event (its final configuration committed, it stepped down) is
//     checked the same way.
//   - INV-MB5: a node that became leader is a voter of its configuration.
//   - INV-MB4: a leader whose committed configuration excludes it has stepped
//     down, and a node a committed configuration removed never becomes leader
//     of a later term.
func (c *Cluster) checkMembership(n *node, role raft.Role, lastBefore uint64) {
	conf, _ := n.core.Conf()
	if want := c.derivedConf(n); !conf.Equal(want) {
		c.violate("INV-MB2", "%s holds configuration %s; its log and snapshot give %s", n.id, conf, want)
		return
	}
	term, commit := n.core.Term(), n.core.CommitIndex()
	if role == raft.Leader {
		if n.role != raft.Leader || n.term != term {
			if _, prev := c.campaignConfs(n); !conf.IsVoter(n.id) && prev == nil {
				c.violate("INV-MB5", "%s became leader of term %d while no voter of its configuration %s", n.id, term, conf)
				return
			}
			if at, ok := c.chk.removedAt[n.id]; ok && term > at {
				c.violate("INV-MB4", "%s became leader of term %d after a configuration committed in term %d removed it", n.id, term, at)
				return
			}
		}
		if !conf.IsVoter(n.id) && !n.core.ConfPending() {
			c.violate("INV-MB4", "%s still leads term %d under a committed configuration %s without it", n.id, term, conf)
			return
		}
	} else if n.role != raft.Leader || n.term != term {
		return // not a leader's decision: a follower learns its commit
	}
	if c.checkFinalAppends(n, term, commit, lastBefore); c.viol != nil {
		return
	}
	if commit <= n.commit {
		return // no commit decided in this event
	}
	decided := c.derivedConf(n)
	t := n.termAt(commit)
	if v := decided.VoterIDs(); countDurable(c, v, commit, t) <= len(v)/2 {
		c.violate("INV-MB3", "leader %s committed index %d (term %d) held durably by %d of the voters %v", n.id, commit, t, countDurable(c, v, commit, t), v)
		return
	}
	if decided.Joint() {
		if o := decided.OutgoingIDs(); countDurable(c, o, commit, t) <= len(o)/2 {
			c.violate("INV-MB3", "leader %s committed index %d (term %d) in joint configuration %s held durably by only %d of the outgoing voters %v", n.id, commit, t, decided, countDurable(c, o, commit, t), o)
		}
	}
}

// checkFinalAppends is INV-MB3's precondition for the final configuration:
// every final entry the leader appended in this event (its term, above what
// its log held before) follows a joint configuration the leader's commit
// covers, and that joint entry is durable on a majority of the joint voters
// and a majority of the outgoing voters — it was committed under the joint
// rule before the leader switched to the final one. (A joint configuration
// that a snapshot carries is committed: the snapshot is of applied state.)
func (c *Cluster) checkFinalAppends(n *node, term, commit, lastBefore uint64) {
	for i := len(n.entries) - 1; i >= 0 && n.entries[i].Index > lastBefore; i-- {
		e := n.entries[i]
		if e.Type != replication.EntryConfig || e.Term != term {
			continue
		}
		final, _ := replication.DecodeConfiguration(e.Data)
		joint, j, inLog := c.confEntryThrough(n, e.Index-1)
		if !joint.Joint() || !raft.Final(joint).Equal(final) {
			continue
		}
		if commit < j {
			c.violate("INV-MB3", "leader %s appended the final configuration %s at %d while the joint entry at %d was uncommitted (commit %d)", n.id, final, e.Index, j, commit)
			return
		}
		if !inLog {
			continue
		}
		jt := n.termAt(j)
		for _, ids := range [][]NodeID{joint.VoterIDs(), joint.OutgoingIDs()} {
			if k := countDurable(c, ids, j, jt); k <= len(ids)/2 {
				c.violate("INV-MB3", "leader %s appended the final configuration %s at %d though the joint entry at %d (term %d) is durable on only %d of %v", n.id, final, e.Index, j, jt, k, ids)
				return
			}
		}
	}
}

// countDurable counts the nodes among ids whose durable state holds (i, t).
func countDurable(c *Cluster, ids []NodeID, i, t uint64) int {
	k := 0
	for _, id := range ids {
		if x := c.nodes[id]; x != nil && holdsDurably(x, i, t) {
			k++
		}
	}
	return k
}
