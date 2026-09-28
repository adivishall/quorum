package raft

import (
	"fmt"
	"math/rand"

	"github.com/adivishall/quorum/internal/replication"
)

// Raft is the deterministic Raft core for one group. It is a pure object: no
// goroutines, no locks, no clock, no sockets, no filesystem (ADR-002, ADR-016).
// It is NOT safe for concurrent use — a single goroutine (the driver) owns it.
type Raft struct {
	id  NodeID
	log replication.Log
	rng *rand.Rand

	// Membership (Phase 15, docs/MEMBERSHIP.md; membership.go). conf is the
	// current configuration — the latest configuration entry in the log, at
	// confIndex — or, with none after the boundary, baseConf. baseConf is the
	// configuration at log index baseConfIndex (>= the boundary, <= the commit
	// index): the snapshot's at its index, or the genesis at 0; empty for a
	// joiner that has learned none. peers is conf's members, sorted, for
	// deterministic iteration; it need not contain this node (a leader being
	// removed, a joiner, a removed node).
	conf          Configuration
	confIndex     uint64
	baseConf      Configuration
	baseConfIndex uint64
	peers         []NodeID

	electionTicks      int
	heartbeatTicks     int
	snapshotRetryTicks int

	// Persistent (durable) state.
	role        Role
	currentTerm uint64
	votedFor    NodeID

	// Volatile state.
	leaderID         NodeID
	electionElapsed  int
	heartbeatElapsed int
	randElectionTO   int

	// Candidate state.
	votesGranted map[NodeID]bool

	// Leader state.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	// Snapshot transfers (Phase 14, docs/SNAPSHOTS.md §8): the index of the
	// snapshot offered to each peer still being answered, and the ticks since.
	snapPending map[NodeID]uint64
	snapWait    map[NodeID]int

	// installed is the snapshot this follower's log was reset to since the last
	// Advance: the driver must make it durable before the Ready's messages.
	installed *SnapshotMeta

	// Effects accumulated since the last Advance.
	msgs                []Message
	hsDirty             bool   // currentTerm/votedFor changed
	unstable            uint64 // lowest log index written since Advance (0 = none)
	lastPersistedCommit uint64 // to detect a commit change worth persisting

	// ReadIndex state (Phase 12, docs/DESIGN.md §8.5), leader only.
	hbSeq      uint64            // sequence carried by the next AppendEntries this leader sends
	ackSeq     map[NodeID]uint64 // highest sequence each peer has echoed in the current term
	termStart  uint64            // index of this leader's election no-op
	nextReadID uint64
	pending    []pendingRead // registered reads awaiting a quorum of post-registration acks, FIFO
	readStates []ReadState   // confirmed reads, drained by Ready/Advance
}

// pendingRead is a ReadIndex request waiting for confirmation: it is confirmed
// once a quorum (the leader included) has acknowledged a sequence >= seq.
type pendingRead struct {
	id, index, seq uint64
}

// New constructs a Raft core from cfg. The core starts as a Follower at the
// recovered term/vote (zero for a fresh node). The log's already-present entries
// and commit index are treated as durable; the latest configuration entry among
// them is the configuration (Raft §6), else cfg's base configuration.
func New(cfg Config) (*Raft, error) {
	cfg.withDefaults()
	base, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	r := &Raft{
		id:                 cfg.ID,
		log:                cfg.Log,
		rng:                cfg.Rand,
		electionTicks:      cfg.ElectionTicks,
		heartbeatTicks:     cfg.HeartbeatTicks,
		snapshotRetryTicks: cfg.SnapshotRetryTicks,
		role:               Follower,
		currentTerm:        cfg.Term,
		votedFor:           cfg.Vote,
		baseConf:           base.Clone(),
		nextIndex:          map[NodeID]uint64{},
		matchIndex:         map[NodeID]uint64{},
	}
	boundary, _ := r.log.Boundary()
	if cfg.ConfIndex > r.log.LastIndex() {
		return nil, fmt.Errorf("%w: base configuration at %d, past the log's last index %d", ErrConfMismatch, cfg.ConfIndex, r.log.LastIndex())
	}
	r.baseConfIndex = max(cfg.ConfIndex, boundary)
	r.setConf(base, 0)
	if err := r.checkLogConfs(); err != nil {
		return nil, err
	}
	r.reconcileConf()
	r.lastPersistedCommit = r.log.CommitIndex()
	r.resetElectionTimer()
	return r, nil
}

// checkLogConfs verifies every configuration entry the recovered log holds
// decodes — recovery refuses an undecodable one rather than guessing — and that
// the latest one at or below the base configuration's index agrees with it (a
// snapshot and a log that disagree are refused, never reconciled).
func (r *Raft) checkLogConfs() error {
	base, _ := r.log.Boundary()
	var atBase *Configuration
	for i := base + 1; i <= r.log.LastIndex(); i++ {
		e, err := r.log.At(i)
		if err != nil {
			return err
		}
		if e.Type == replication.EntryConfig {
			c, err := replication.DecodeConfiguration(e.Data)
			if err != nil {
				return err
			}
			if i <= r.baseConfIndex {
				atBase = &c
			}
		} else if e.Type != replication.EntryNormal {
			return ErrMalformedMessage
		}
	}
	if atBase != nil && !atBase.Equal(r.baseConf) {
		return fmt.Errorf("%w: the log's configuration at %d is %s, the base is %s", ErrConfMismatch, r.baseConfIndex, atBase, r.baseConf)
	}
	return nil
}

// --- observability (read-only) ---

func (r *Raft) ID() NodeID          { return r.id }
func (r *Raft) Role() Role          { return r.role }
func (r *Raft) Term() uint64        { return r.currentTerm }
func (r *Raft) VotedFor() NodeID    { return r.votedFor }
func (r *Raft) LeaderID() NodeID    { return r.leaderID }
func (r *Raft) CommitIndex() uint64 { return r.log.CommitIndex() }
func (r *Raft) LastIndex() uint64   { return r.log.LastIndex() }

// Boundary is the index and term of the last entry compacted away (0, 0 if
// none). A follower that needs an entry at or below it can only be brought up
// to date by a snapshot at or beyond it; the driver sends its latest published
// snapshot, which a compaction never passes (docs/SNAPSHOTS.md §5).
func (r *Raft) Boundary() (index, term uint64) { return r.log.Boundary() }

// TermAt returns the term of the entry at index, which must be in the log or
// be its boundary (Phase 14: the driver names a snapshot by (index, term)).
func (r *Raft) TermAt(index uint64) (uint64, error) { return r.log.Term(index) }

// --- inputs ---

// Tick advances the core's logical clock by one tick. A leader heartbeats every
// heartbeatTicks; a follower/candidate that is a voter starts an election after
// its randomized election timeout — a learner or a node outside its
// configuration never does (docs/MEMBERSHIP.md §5).
func (r *Raft) Tick() {
	if r.role == Leader {
		// A snapshot offer nobody answered in time is withdrawn: the next
		// replication to that peer offers it again (the transfer may have been
		// lost — a dropped chunk, a follower restart).
		for _, p := range r.peers {
			if r.snapPending[p] == 0 {
				continue
			}
			r.snapWait[p]++
			if r.snapWait[p] >= r.snapshotRetryTicks {
				delete(r.snapPending, p)
				delete(r.snapWait, p)
			}
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastAppend()
		}
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.randElectionTO {
		if r.conf.IsVoter(r.id) {
			r.becomeCandidate()
		} else {
			r.resetElectionTimer()
		}
	}
}

// Propose appends a client command to the leader's log and replicates it. It
// returns ErrNotLeader on a non-leader.
func (r *Raft) Propose(data []byte) error {
	if r.role != Leader {
		return ErrNotLeader
	}
	r.appendEntry(data)
	r.broadcastAppend()
	r.maybeCommit() // a single-node leader (self is the quorum) commits at once
	return nil
}

// ReadIndex registers a linearizable read (Phase 12, docs/DESIGN.md §8.5,
// ADR-019) and returns its id and read index. Only a leader may serve one
// (ErrNotLeader otherwise; the caller redirects). The read index is the higher
// of the current commit index and the index of this leader's election no-op —
// a new leader's commit index can lag entries committed by its predecessors
// until its own no-op commits, and every earlier entry is committed by then.
//
// The read is NOT yet safe to serve. The leader must first confirm it is still
// the leader: it advances its heartbeat sequence and broadcasts AppendEntries
// carrying it, and the read is confirmed only when a quorum (itself included,
// if it is a voter) has echoed a sequence at least that high — acknowledgements
// that were in flight before the read was registered do not count, because they
// prove leadership only up to the time they were sent. A confirmed read appears
// in Ready.ReadStates; the driver serves it once it has applied through its
// index. A configuration whose only voter is this node is its own quorum and
// confirms immediately. Stepping down drops every unconfirmed read (the driver
// reports them as not-leader).
func (r *Raft) ReadIndex() (ReadState, error) {
	if r.role != Leader {
		return ReadState{}, ErrNotLeader
	}
	r.nextReadID++
	rs := ReadState{ID: r.nextReadID, Index: r.log.CommitIndex()}
	if r.termStart > rs.Index {
		rs.Index = r.termStart
	}
	if r.hasQuorum(func(id NodeID) bool { return id == r.id }) {
		r.readStates = append(r.readStates, rs)
		return rs, nil
	}
	// The broadcast below carries hbSeq+1; only acks of that or a later sequence
	// confirm this read.
	r.pending = append(r.pending, pendingRead{id: rs.ID, index: rs.Index, seq: r.hbSeq + 1})
	r.broadcastAppend()
	return rs, nil
}

// confirmReads moves every pending read whose sequence a quorum has echoed into
// readStates. Pending reads are FIFO with non-decreasing sequences, so
// confirmation is a prefix.
func (r *Raft) confirmReads() {
	for len(r.pending) > 0 {
		p := r.pending[0]
		if !r.hasQuorum(func(id NodeID) bool { return id == r.id || r.ackSeq[id] >= p.seq }) {
			return
		}
		r.readStates = append(r.readStates, ReadState{ID: p.id, Index: p.index})
		r.pending = r.pending[1:]
	}
}

// Compact discards the log through index into a snapshot the driver has made
// durable (Phase 14, docs/SNAPSHOTS.md §5): index must be applied. From then on a
// follower that needs an entry at or below index is offered the snapshot. The
// base configuration moves up with the boundary where it is known (Phase 15,
// baseAfter).
func (r *Raft) Compact(index uint64) error {
	base, at := r.baseAfter(index)
	if err := r.log.Compact(index); err != nil {
		return err
	}
	r.baseConf, r.baseConfIndex = base, at
	if r.confIndex != 0 && r.confIndex <= index {
		r.confIndex = 0 // the entry is compacted: the configuration is the base now
	}
	return nil
}

// Step handles one inbound message. It is the only entry point for peer traffic.
func (r *Raft) Step(m Message) error {
	// A node outside this node's configuration (docs/MEMBERSHIP.md §5): its
	// responses are dropped, and its vote request is refused — its term never
	// adopted — unless its log is at least as up to date as this node's. A
	// removed node's log lacks the entry that removed it, so its inflated terms
	// cannot depose the group's leader; but a candidate with an up-to-date log
	// may be a voter of a configuration this node has not learned yet (this
	// node lags, or is a joiner that has learned nothing), whose election may
	// need this node's vote — refusing it could leave the group without a
	// leader for ever (found by the membership chaos profiles, Phase 15). What a
	// LEADER sends (AppendEntries, a snapshot) is processed from anyone: a
	// member that has fallen behind must be able to learn from a leader it does
	// not yet know.
	if !r.conf.IsMember(m.From) && m.Type != MsgAppendRequest && m.Type != MsgSnapshot {
		if m.Type != MsgVoteRequest || !r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {
			if m.Type == MsgVoteRequest {
				r.send(Message{Type: MsgVoteResponse, To: m.From, Term: r.currentTerm, VoteGranted: false})
			}
			return nil
		}
	}
	// Higher term: step down and adopt it before doing anything else. For an
	// AppendEntries or a snapshot the sender is the new leader; otherwise we do
	// not yet know one.
	if m.Term > r.currentTerm {
		leader := NodeID("")
		if m.Type == MsgAppendRequest || m.Type == MsgSnapshot {
			leader = m.From
		}
		r.becomeFollower(m.Term, leader)
	}
	// Lower term: reject a request with our current term; drop a response. A stale
	// message never mutates protected state (INV-R10).
	if m.Term < r.currentTerm {
		switch m.Type {
		case MsgVoteRequest:
			r.send(Message{Type: MsgVoteResponse, To: m.From, Term: r.currentTerm, VoteGranted: false})
		case MsgAppendRequest:
			r.send(Message{Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: false})
		case MsgSnapshot:
			r.send(Message{Type: MsgSnapshotResponse, To: m.From, Term: r.currentTerm, Success: false})
		}
		return nil
	}
	// m.Term == r.currentTerm.
	switch m.Type {
	case MsgVoteRequest:
		r.handleVoteRequest(m)
	case MsgVoteResponse:
		r.handleVoteResponse(m)
	case MsgAppendRequest:
		r.handleAppendRequest(m)
	case MsgAppendResponse:
		r.handleAppendResponse(m)
	case MsgSnapshot:
		r.handleSnapshot(m)
	case MsgSnapshotResponse:
		r.handleSnapshotResponse(m)
	default:
		return ErrUnknownMessageType
	}
	return nil
}

// --- role transitions ---

func (r *Raft) becomeFollower(term uint64, leader NodeID) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
		r.hsDirty = true
	}
	if r.role == Leader {
		r.pending = nil // unconfirmed reads die with the leadership
	}
	r.role = Follower
	r.leaderID = leader
	r.resetElectionTimer()
}

// becomeCandidate starts an election: only a voter reaches here (Tick), and it
// asks only the voters — a learner cannot vote, and is not asked.
func (r *Raft) becomeCandidate() {
	r.currentTerm++
	r.votedFor = r.id
	r.hsDirty = true
	r.pending = nil
	r.role = Candidate
	r.leaderID = ""
	r.votesGranted = map[NodeID]bool{r.id: true}
	r.resetElectionTimer()

	lastIdx := r.log.LastIndex()
	lastTerm, _ := r.log.Term(lastIdx)
	for _, p := range r.peers {
		if p == r.id || !r.conf.IsVoter(p) {
			continue
		}
		r.send(Message{
			Type: MsgVoteRequest, To: p, Term: r.currentTerm,
			LastLogIndex: lastIdx, LastLogTerm: lastTerm,
		})
	}
	r.maybeBecomeLeader() // a single-voter group wins its own vote immediately
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leaderID = r.id
	r.nextIndex = map[NodeID]uint64{}
	r.matchIndex = map[NodeID]uint64{}
	r.snapPending = map[NodeID]uint64{}
	r.snapWait = map[NodeID]int{}
	r.ackSeq = map[NodeID]uint64{}
	r.syncProgress() // nextIndex = last+1, matchIndex = 0 for every member
	// The no-op entry in the current term is mandatory (docs/DESIGN.md §8.2,
	// §5.4.2): without it a new leader cannot commit entries from prior terms —
	// and a ReadIndex may not be served below it.
	r.termStart = r.log.LastIndex() + 1
	r.pending = nil
	r.appendEntry(nil)
	r.heartbeatElapsed = 0
	r.broadcastAppend()
	r.maybeCommit() // a single-node leader commits the no-op at once
}

func (r *Raft) maybeBecomeLeader() {
	if r.role != Candidate {
		return
	}
	if r.hasQuorum(func(id NodeID) bool { return r.votesGranted[id] }) {
		r.becomeLeader()
	}
}

// --- message handlers (all called with m.Term == r.currentTerm) ---

// handleVoteRequest grants a vote by the §5.2/§5.4.1 rules — at most one per
// term, to a candidate whose log is at least as up to date. It grants whether
// or not this node believes it is
// a voter (Phase 15): a learner, or a joiner, may already be a voter of a
// configuration it has not received — a promotion that commits without it,
// or whose joint entry needs its vote to be elected at all — and only the
// candidate's configuration decides whether the vote counts
// (handleVoteResponse). One vote per term, durable before the answer, is what
// election safety rests on; that holds for every node.
func (r *Raft) handleVoteRequest(m Message) {
	grant := false
	if (r.votedFor == "" || r.votedFor == m.From) && r.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {
		grant = true
		r.votedFor = m.From
		r.hsDirty = true
		r.resetElectionTimer() // granting a vote defers our own election
	}
	r.send(Message{Type: MsgVoteResponse, To: m.From, Term: r.currentTerm, VoteGranted: grant})
}

// handleVoteResponse counts a vote from a voter of the configuration; a vote
// from anyone else is never counted.
func (r *Raft) handleVoteResponse(m Message) {
	if r.role != Candidate || !r.conf.IsVoter(m.From) {
		return
	}
	if m.VoteGranted {
		r.votesGranted[m.From] = true
		r.maybeBecomeLeader()
	}
}

func (r *Raft) handleAppendRequest(m Message) {
	// A message at our own term from a leader means we are (or become) a follower
	// of it; reset the election timer so we do not campaign against a live leader.
	r.becomeFollower(m.Term, m.From)

	last := r.log.LastIndex()
	// Entries at or below our boundary are compacted into our snapshot: they are
	// committed, so they match the leader's (Leader Completeness). Skip them and
	// check consistency at the boundary instead (Phase 14). This happens when a
	// leader's nextIndex for us lags a snapshot we installed, or a delayed
	// AppendEntries arrives after one.
	if base, baseTerm := r.log.Boundary(); m.PrevLogIndex < base {
		skip := base - m.PrevLogIndex
		lastNew := m.PrevLogIndex + uint64(len(m.Entries))
		if uint64(len(m.Entries)) <= skip {
			// Nothing beyond our snapshot: what it carried we already hold.
			r.send(Message{Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: true, MatchIndex: lastNew, Seq: m.Seq})
			return
		}
		m.Entries = m.Entries[skip:]
		m.PrevLogIndex, m.PrevLogTerm = base, baseTerm
	}
	// The entry before the new ones must exist.
	if m.PrevLogIndex > last {
		r.send(Message{
			Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: false,
			ConflictTerm: 0, ConflictIndex: last + 1, Seq: m.Seq,
		})
		return
	}
	// ...and must match the leader's term there.
	prevTerm, _ := r.log.Term(m.PrevLogIndex)
	if prevTerm != m.PrevLogTerm {
		r.send(Message{
			Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: false,
			ConflictTerm: prevTerm, ConflictIndex: r.firstIndexOfTerm(prevTerm, m.PrevLogIndex), Seq: m.Seq,
		})
		return
	}
	// prevLog matches. Reconcile the entries, replacing only an uncommitted
	// conflicting suffix (the Phase 8 log refuses to overwrite a committed entry).
	if !r.appendFollowerEntries(m.PrevLogIndex, m.Entries) {
		r.send(Message{Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: false,
			ConflictTerm: 0, ConflictIndex: r.log.LastIndex() + 1, Seq: m.Seq})
		return
	}
	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if m.LeaderCommit > r.log.CommitIndex() {
		r.commitTo(minU64(m.LeaderCommit, lastNew))
	}
	r.send(Message{Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: true, MatchIndex: lastNew, Seq: m.Seq})
}

func (r *Raft) handleAppendResponse(m Message) {
	if r.role != Leader {
		return
	}
	peer := m.From
	// Any response in our term — success or rejection — is the peer's
	// acknowledgement that we were its leader when it answered the request
	// carrying m.Seq (ReadIndex confirmation).
	if m.Seq > r.ackSeq[peer] {
		r.ackSeq[peer] = m.Seq
		r.confirmReads()
	}
	if m.Success {
		r.progress(peer, m.MatchIndex)
		return
	}
	// A peer we are sending a snapshot rejects our heartbeats until it has
	// installed it; that says nothing new (Phase 14).
	if r.snapPending[peer] != 0 {
		return
	}
	// Rejection: back up nextIndex by a whole conflicting term (§10). Only act if
	// it actually moves us backward, so a stale rejection cannot disturb progress
	// — and never to or below matchIndex: the peer's log holds our entries
	// through matchIndex, durably, for the rest of our term, so a rejection that
	// would back up past it answers an earlier request and is stale. Acting on it
	// once only wasted a resend; since Phase 14 it can strand the peer: with the
	// prefix compacted, the leader offers a snapshot the peer's commit already
	// covers, whose success says nothing new, and the peer is never caught up
	// (found by the kv-snapshots-partitions schedules, seed 100).
	back := max(r.backupNextIndex(m.ConflictTerm, m.ConflictIndex), r.matchIndex[peer]+1)
	if back < r.nextIndex[peer] {
		r.nextIndex[peer] = back
		r.sendAppend(peer)
	}
}

// progress records that peer's log matches ours through match — a success,
// ignored if stale (it would not advance matchIndex). Reaching an offered
// snapshot's index ends that offer.
func (r *Raft) progress(peer NodeID, match uint64) {
	if match <= r.matchIndex[peer] {
		return
	}
	r.matchIndex[peer] = match
	r.nextIndex[peer] = match + 1
	if s := r.snapPending[peer]; s != 0 && match >= s {
		delete(r.snapPending, peer)
		delete(r.snapWait, peer)
	}
	r.maybeCommit()
}

// handleSnapshot is a follower offered the leader's snapshot (Raft §7). The
// driver hands it over only once the whole snapshot arrived and validated,
// together with the configuration the snapshot carries (Phase 15): the node
// adopts it as its base configuration.
func (r *Raft) handleSnapshot(m Message) {
	r.becomeFollower(m.Term, m.From)
	commit := r.log.CommitIndex()
	if m.SnapshotIndex <= commit {
		// Already covered by what we have committed (a duplicate, a delayed
		// offer, or one we installed before): nothing to install. Our committed
		// prefix matches the leader's.
		r.send(Message{Type: MsgSnapshotResponse, To: m.From, Term: r.currentTerm, Success: true, MatchIndex: commit, Seq: m.Seq})
		return
	}
	if m.Conf == nil || len(m.Conf.Voters) == 0 {
		// A snapshot without its configuration cannot be installed: the node would
		// not know the group it belongs to. Refuse rather than guess. (A snapshot
		// is of applied state, and only a configuration with voters applies.)
		r.send(Message{Type: MsgSnapshotResponse, To: m.From, Term: r.currentTerm, Success: false, Seq: m.Seq})
		return
	}
	if err := r.log.InstallSnapshot(m.SnapshotIndex, m.SnapshotTerm); err != nil {
		// Cannot happen above the commit index; refuse rather than guess.
		r.send(Message{Type: MsgSnapshotResponse, To: m.From, Term: r.currentTerm, Success: false, Seq: m.Seq})
		return
	}
	// Entries we had not persisted yet at or below the snapshot are gone; those
	// kept after it are still to be persisted.
	if r.unstable != 0 && r.unstable <= m.SnapshotIndex {
		r.unstable = 0
		if r.log.LastIndex() > m.SnapshotIndex {
			r.unstable = m.SnapshotIndex + 1
		}
	}
	r.baseConf, r.baseConfIndex = m.Conf.Clone(), m.SnapshotIndex
	r.confIndex = 0
	r.setConf(r.baseConf, 0)
	r.reconcileConf() // the kept suffix, if any, may hold a later configuration
	r.installed = &SnapshotMeta{Index: m.SnapshotIndex, Term: m.SnapshotTerm}
	r.send(Message{Type: MsgSnapshotResponse, To: m.From, Term: r.currentTerm, Success: true, MatchIndex: m.SnapshotIndex, Seq: m.Seq})
}

// handleSnapshotResponse is the leader learning a follower's snapshot outcome.
func (r *Raft) handleSnapshotResponse(m Message) {
	if r.role != Leader {
		return
	}
	peer := m.From
	if m.Seq > r.ackSeq[peer] {
		r.ackSeq[peer] = m.Seq
		r.confirmReads()
	}
	if !m.Success {
		return
	}
	r.progress(peer, m.MatchIndex)
	if r.role == Leader && r.matchIndex[peer] < r.log.LastIndex() {
		r.sendAppend(peer) // resume replication after the snapshot at once
	}
}

// --- log / replication helpers ---

func (r *Raft) appendEntry(data []byte) {
	idx := r.log.LastIndex() + 1
	if err := r.log.Append(Entry{Index: idx, Term: r.currentTerm, Data: data}); err != nil {
		// The core only appends contiguous, current-term entries, which the Phase
		// 8 log always accepts; a failure here is a programming error.
		panic("raft: leader append rejected by log: " + err.Error())
	}
	r.markUnstable(idx)
}

// appendFollowerEntries installs the leader's entries after prevIndex, keeping any
// identical prefix and replacing only the first divergent (uncommitted) suffix. It
// returns false only if the log refuses the write (a committed-entry conflict),
// which a correct leader never causes. A batch that adds a configuration entry,
// or a replacement that may have removed one, makes the node recompute its
// configuration from the log (Phase 15).
func (r *Raft) appendFollowerEntries(prevIndex uint64, entries []Entry) bool {
	i := 0
	for ; i < len(entries); i++ {
		idx := prevIndex + 1 + uint64(i)
		if idx > r.log.LastIndex() {
			break // we are missing this entry; append from here
		}
		t, _ := r.log.Term(idx)
		if t != entries[i].Term {
			break // conflict; truncate and replace from here
		}
	}
	if i == len(entries) {
		return true // everything already present and matching
	}
	batch := entries[i:]
	appendIdx := prevIndex + 1 + uint64(i)
	var err error
	truncates := appendIdx <= r.log.LastIndex()
	if !truncates {
		err = r.log.Append(batch...)
	} else {
		err = r.log.TruncateAndAppend(batch...)
	}
	if err != nil {
		return false
	}
	r.markUnstable(appendIdx)
	hasConf := false
	for _, e := range batch {
		if e.Type == replication.EntryConfig {
			hasConf = true
		}
	}
	if hasConf || (truncates && r.confIndex >= appendIdx) {
		r.reconcileConf()
	}
	return true
}

func (r *Raft) candidateUpToDate(lastIdx, lastTerm uint64) bool {
	myIdx := r.log.LastIndex()
	myTerm, _ := r.log.Term(myIdx)
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= myIdx
}

// firstIndexOfTerm returns the first index whose term equals term, searching down
// from upto. Terms are non-decreasing, so a term occupies a contiguous block.
func (r *Raft) firstIndexOfTerm(term, upto uint64) uint64 {
	ci := upto
	for ci > r.log.FirstIndex() { // never below what the log still holds
		t, _ := r.log.Term(ci - 1)
		if t != term {
			break
		}
		ci--
	}
	return ci
}

// lastIndexOfTerm returns the highest index whose term equals term, or 0 if none
// is known. The compaction boundary counts: its term is still known.
func (r *Raft) lastIndexOfTerm(term uint64) uint64 {
	base, baseTerm := r.log.Boundary()
	for i := r.log.LastIndex(); i > base; i-- {
		t, _ := r.log.Term(i)
		if t == term {
			return i
		}
		if t < term {
			return 0 // terms are non-decreasing; no entry of this term below here
		}
	}
	if base > 0 && baseTerm == term {
		return base
	}
	return 0
}

func (r *Raft) backupNextIndex(conflictTerm, conflictIndex uint64) uint64 {
	if conflictTerm != 0 {
		if last := r.lastIndexOfTerm(conflictTerm); last > 0 {
			return last + 1 // leader has this term; resume just past it
		}
	}
	if conflictIndex < 1 {
		return 1
	}
	return conflictIndex
}

func (r *Raft) sendAppend(peer NodeID) {
	next := r.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	// The entries this peer needs next are compacted: only the snapshot can
	// bring it up to date (Phase 14, Raft §7). Offer it once; until the offer is
	// answered or withdrawn (Tick), only heartbeat the peer — at the boundary, so
	// the heartbeat succeeds as soon as the peer has installed the snapshot.
	if base, baseTerm := r.log.Boundary(); next <= base {
		if r.snapPending[peer] == 0 {
			r.snapPending[peer], r.snapWait[peer] = base, 0
			r.send(Message{Type: MsgSnapshot, To: peer, Term: r.currentTerm,
				SnapshotIndex: base, SnapshotTerm: baseTerm, Seq: r.hbSeq})
			return
		}
		r.send(Message{Type: MsgAppendRequest, To: peer, Term: r.currentTerm,
			PrevLogIndex: base, PrevLogTerm: baseTerm, LeaderCommit: r.log.CommitIndex(), Seq: r.hbSeq})
		return
	}
	prevIndex := next - 1
	prevTerm, _ := r.log.Term(prevIndex)
	entries, _ := r.log.Slice(next, r.log.LastIndex()+1)
	r.send(Message{
		Type: MsgAppendRequest, To: peer, Term: r.currentTerm,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: r.log.CommitIndex(), Seq: r.hbSeq,
	})
}

// broadcastAppend sends AppendEntries (a heartbeat when there is nothing to
// replicate) to every member — voters of both sets and learners — under a fresh
// heartbeat sequence.
func (r *Raft) broadcastAppend() {
	r.hbSeq++
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.sendAppend(p)
	}
}

// maybeCommit advances commitIndex to the highest N replicated on a quorum of the
// current configuration (docs/MEMBERSHIP.md §3: a majority of the voters and, in
// a joint configuration, of the outgoing voters) whose entry is from the current
// term (§5.4.2 — the figure-8 rule). The leader counts itself only if it is a
// voter of the set in question.
func (r *Raft) maybeCommit() {
	matchOf := func(id NodeID) uint64 {
		if id == r.id {
			return r.log.LastIndex()
		}
		return r.matchIndex[id]
	}
	// Candidate indexes: every voter's match, tried highest first.
	var cands []uint64
	for _, list := range [][]NodeID{r.conf.VoterIDs(), r.conf.OutgoingIDs()} {
		for _, id := range list {
			cands = append(cands, matchOf(id))
		}
	}
	sortDescU64(cands)
	var n uint64
	for i, c := range cands {
		if i > 0 && c == cands[i-1] {
			continue
		}
		if r.hasQuorum(func(id NodeID) bool { return matchOf(id) >= c }) {
			n = c
			break
		}
	}
	if n <= r.log.CommitIndex() {
		return
	}
	t, _ := r.log.Term(n)
	if t == r.currentTerm {
		r.commitTo(n)
		r.afterCommit()
	}
}

func (r *Raft) commitTo(idx uint64) {
	if last := r.log.LastIndex(); idx > last {
		idx = last
	}
	if idx > r.log.CommitIndex() {
		_ = r.log.Commit(idx) // monotonic; guarded above, cannot error
	}
}

// --- apply path (driver-controlled) ---

// NextApply returns the committed-but-not-applied entries in index order (copies).
// The driver applies them to the state machine, then calls AppliedTo.
func (r *Raft) NextApply() []Entry {
	es, _ := r.log.Unapplied()
	return es
}

// AppliedTo records that the state machine has applied through index. It never
// advances past commitIndex (the Phase 8 log enforces it).
func (r *Raft) AppliedTo(index uint64) error {
	return r.log.Apply(index)
}

// AppliedIndex returns the highest applied index.
func (r *Raft) AppliedIndex() uint64 { return r.log.AppliedIndex() }

// --- timers ---

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	// Uniform in [electionTicks, 2*electionTicks) from the injected source.
	r.randElectionTO = r.electionTicks + r.rng.Intn(r.electionTicks)
}

// --- effects ---

func (r *Raft) send(m Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func (r *Raft) markUnstable(from uint64) {
	if r.unstable == 0 || from < r.unstable {
		r.unstable = from
	}
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// sortDescU64 sorts a small slice descending (insertion sort — deterministic and
// allocation-free for the tiny group sizes here).
func sortDescU64(s []uint64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] > s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
