package raft

import (
	"math/rand"

	"github.com/adivishall/quorum/internal/replication"
)

// Raft is the deterministic Raft core for one group. It is a pure object: no
// goroutines, no locks, no clock, no sockets, no filesystem (ADR-002, ADR-016).
// It is NOT safe for concurrent use — a single goroutine (the driver) owns it.
type Raft struct {
	id    NodeID
	peers []NodeID // sorted, includes self; fixed membership (ADR-005)
	log   replication.Log
	rng   *rand.Rand

	electionTicks  int
	heartbeatTicks int

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

	// Effects accumulated since the last Advance.
	msgs                []Message
	hsDirty             bool   // currentTerm/votedFor changed
	unstable            uint64 // lowest log index written since Advance (0 = none)
	lastPersistedCommit uint64 // to detect a commit change worth persisting
}

// New constructs a Raft core from cfg. The core starts as a Follower at the
// recovered term/vote (zero for a fresh node). The log's already-present entries
// and commit index are treated as durable.
func New(cfg Config) (*Raft, error) {
	cfg.withDefaults()
	peers, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	r := &Raft{
		id:             cfg.ID,
		peers:          peers,
		log:            cfg.Log,
		rng:            cfg.Rand,
		electionTicks:  cfg.ElectionTicks,
		heartbeatTicks: cfg.HeartbeatTicks,
		role:           Follower,
		currentTerm:    cfg.Term,
		votedFor:       cfg.Vote,
	}
	r.lastPersistedCommit = r.log.CommitIndex()
	r.resetElectionTimer()
	return r, nil
}

// --- observability (read-only) ---

func (r *Raft) ID() NodeID          { return r.id }
func (r *Raft) Role() Role          { return r.role }
func (r *Raft) Term() uint64        { return r.currentTerm }
func (r *Raft) VotedFor() NodeID    { return r.votedFor }
func (r *Raft) LeaderID() NodeID    { return r.leaderID }
func (r *Raft) CommitIndex() uint64 { return r.log.CommitIndex() }
func (r *Raft) LastIndex() uint64   { return r.log.LastIndex() }

// --- inputs ---

// Tick advances the core's logical clock by one tick. A leader heartbeats every
// heartbeatTicks; a follower/candidate starts an election after its randomized
// election timeout.
func (r *Raft) Tick() {
	if r.role == Leader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastAppend()
		}
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.randElectionTO {
		r.becomeCandidate()
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

// Step handles one inbound message. It is the only entry point for peer traffic.
func (r *Raft) Step(m Message) error {
	// Higher term: step down and adopt it before doing anything else. For an
	// AppendEntries the sender is the new leader; otherwise we do not yet know one.
	if m.Term > r.currentTerm {
		leader := NodeID("")
		if m.Type == MsgAppendRequest {
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
	r.role = Follower
	r.leaderID = leader
	r.resetElectionTimer()
}

func (r *Raft) becomeCandidate() {
	r.currentTerm++
	r.votedFor = r.id
	r.hsDirty = true
	r.role = Candidate
	r.leaderID = ""
	r.votesGranted = map[NodeID]bool{r.id: true}
	r.resetElectionTimer()

	lastIdx := r.log.LastIndex()
	lastTerm, _ := r.log.Term(lastIdx)
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.send(Message{
			Type: MsgVoteRequest, To: p, Term: r.currentTerm,
			LastLogIndex: lastIdx, LastLogTerm: lastTerm,
		})
	}
	r.maybeBecomeLeader() // a single-node group wins its own vote immediately
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leaderID = r.id
	last := r.log.LastIndex()
	r.nextIndex = make(map[NodeID]uint64, len(r.peers))
	r.matchIndex = make(map[NodeID]uint64, len(r.peers))
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.nextIndex[p] = last + 1
		r.matchIndex[p] = 0
	}
	// The no-op entry in the current term is mandatory (docs/DESIGN.md §8.2,
	// §5.4.2): without it a new leader cannot commit entries from prior terms.
	r.appendEntry(nil)
	r.heartbeatElapsed = 0
	r.broadcastAppend()
	r.maybeCommit() // a single-node leader commits the no-op at once
}

func (r *Raft) maybeBecomeLeader() {
	if r.role != Candidate {
		return
	}
	granted := 0
	for _, ok := range r.votesGranted {
		if ok {
			granted++
		}
	}
	if granted >= quorum(len(r.peers)) {
		r.becomeLeader()
	}
}

// --- message handlers (all called with m.Term == r.currentTerm) ---

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

func (r *Raft) handleVoteResponse(m Message) {
	if r.role != Candidate {
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
	// The entry before the new ones must exist.
	if m.PrevLogIndex > last {
		r.send(Message{
			Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: false,
			ConflictTerm: 0, ConflictIndex: last + 1,
		})
		return
	}
	// ...and must match the leader's term there.
	prevTerm, _ := r.log.Term(m.PrevLogIndex)
	if prevTerm != m.PrevLogTerm {
		r.send(Message{
			Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: false,
			ConflictTerm: prevTerm, ConflictIndex: r.firstIndexOfTerm(prevTerm, m.PrevLogIndex),
		})
		return
	}
	// prevLog matches. Reconcile the entries, replacing only an uncommitted
	// conflicting suffix (the Phase 8 log refuses to overwrite a committed entry).
	if !r.appendFollowerEntries(m.PrevLogIndex, m.Entries) {
		r.send(Message{Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: false,
			ConflictTerm: 0, ConflictIndex: r.log.LastIndex() + 1})
		return
	}
	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if m.LeaderCommit > r.log.CommitIndex() {
		r.commitTo(minU64(m.LeaderCommit, lastNew))
	}
	r.send(Message{Type: MsgAppendResponse, To: m.From, Term: r.currentTerm, Success: true, MatchIndex: lastNew})
}

func (r *Raft) handleAppendResponse(m Message) {
	if r.role != Leader {
		return
	}
	peer := m.From
	if m.Success {
		// Ignore a stale success that would not advance matchIndex.
		if m.MatchIndex > r.matchIndex[peer] {
			r.matchIndex[peer] = m.MatchIndex
			r.nextIndex[peer] = m.MatchIndex + 1
			r.maybeCommit()
		}
		return
	}
	// Rejection: back up nextIndex by a whole conflicting term (§10). Only act if
	// it actually moves us backward, so a stale rejection cannot disturb progress.
	back := r.backupNextIndex(m.ConflictTerm, m.ConflictIndex)
	if back < r.nextIndex[peer] {
		r.nextIndex[peer] = back
		r.sendAppend(peer)
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
// which a correct leader never causes.
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
	if appendIdx == r.log.LastIndex()+1 {
		err = r.log.Append(batch...)
	} else {
		err = r.log.TruncateAndAppend(batch...)
	}
	if err != nil {
		return false
	}
	r.markUnstable(appendIdx)
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
	for ci > 1 {
		t, _ := r.log.Term(ci - 1)
		if t != term {
			break
		}
		ci--
	}
	return ci
}

// lastIndexOfTerm returns the highest index whose term equals term, or 0 if none.
func (r *Raft) lastIndexOfTerm(term uint64) uint64 {
	for i := r.log.LastIndex(); i >= 1; i-- {
		t, _ := r.log.Term(i)
		if t == term {
			return i
		}
		if t < term {
			break // terms are non-decreasing; no entry of this term below here
		}
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
	prevIndex := next - 1
	prevTerm, _ := r.log.Term(prevIndex)
	entries, _ := r.log.Slice(next, r.log.LastIndex()+1)
	r.send(Message{
		Type: MsgAppendRequest, To: peer, Term: r.currentTerm,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: r.log.CommitIndex(),
	})
}

func (r *Raft) broadcastAppend() {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.sendAppend(p)
	}
}

// maybeCommit advances commitIndex to the highest N replicated on a quorum whose
// entry is from the current term (§5.4.2 — the figure-8 rule).
func (r *Raft) maybeCommit() {
	matches := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		if p == r.id {
			matches = append(matches, r.log.LastIndex())
		} else {
			matches = append(matches, r.matchIndex[p])
		}
	}
	// Descending sort; the value at rank quorum-1 is matched by a majority.
	sortDescU64(matches)
	n := matches[quorum(len(r.peers))-1]
	if n <= r.log.CommitIndex() {
		return
	}
	t, _ := r.log.Term(n)
	if t == r.currentTerm {
		r.commitTo(n)
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
