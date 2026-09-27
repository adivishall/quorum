package raft

import (
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/replication"
)

// Membership in the pure core (Phase 15, docs/MEMBERSHIP.md). The core knows a
// group's membership as a replication.Configuration and nothing else: no
// addresses it would dial, no registry, no clock. The configuration is the
// latest EntryConfig in the log — committed or not (Raft §6) — or, below the
// first one, the configuration the log's boundary represents (the snapshot's,
// or the group's genesis); a truncation that removes a configuration entry
// reverts to the previous one by recomputation. Quorum is a majority of the
// voters and, in a joint configuration, a majority of the outgoing voters too.

// Configuration and Member are re-exported so callers speak one set of types.
type (
	Configuration = replication.Configuration
	Member        = replication.Member
)

// ConfChangeType is one of the four membership operations (docs/MEMBERSHIP.md
// §4).
type ConfChangeType uint8

const (
	// AddLearner adds a non-voting member: one configuration entry, no quorum
	// change.
	AddLearner ConfChangeType = iota + 1
	// RemoveLearner removes a learner: one entry, no quorum change.
	RemoveLearner
	// Promote makes a learner a voter by joint consensus: a joint entry (the old
	// voters as Outgoing, the new set as Voters), then the final entry once the
	// joint one is committed.
	Promote
	// RemoveVoter removes a voter by joint consensus, the same two phases.
	RemoveVoter
)

func (t ConfChangeType) String() string {
	switch t {
	case AddLearner:
		return "add-learner"
	case RemoveLearner:
		return "remove-learner"
	case Promote:
		return "promote"
	case RemoveVoter:
		return "remove-voter"
	}
	return fmt.Sprintf("confchange(%d)", t)
}

// ConfChange is one requested membership operation on one member.
type ConfChange struct {
	Type   ConfChangeType
	Member Member
}

func (cc ConfChange) String() string { return fmt.Sprintf("%s %s", cc.Type, cc.Member.ID) }

// Errors of membership changes.
var (
	// ErrConfChangeInProgress means the group already has a configuration change
	// under way — an uncommitted configuration entry, or a joint configuration
	// awaiting its final entry — and refuses a second (docs/MEMBERSHIP.md §4).
	ErrConfChangeInProgress = errors.New("raft: a configuration change is in progress")
	// ErrInvalidConfChange means the operation does not apply to the current
	// configuration: adding a member that exists, promoting a non-learner,
	// removing a non-member, or removing the last voter.
	ErrInvalidConfChange = errors.New("raft: invalid configuration change")
)

// Apply computes the configuration the change leads to from cur: for
// AddLearner and RemoveLearner the next stable configuration; for Promote and
// RemoveVoter the JOINT one (its final configuration is Final). cur must be
// stable. It is a pure function shared by the leader (which appends the result)
// and by anything that wants to predict it.
func (cc ConfChange) Apply(cur Configuration) (Configuration, error) {
	if cur.Joint() {
		return Configuration{}, ErrConfChangeInProgress
	}
	if cc.Member.ID == "" {
		return Configuration{}, fmt.Errorf("%w: empty member id", ErrInvalidConfChange)
	}
	id := cc.Member.ID
	switch cc.Type {
	case AddLearner:
		if cur.IsMember(id) {
			return Configuration{}, fmt.Errorf("%w: %s is already a member", ErrInvalidConfChange, id)
		}
		return replication.NewConfiguration(cur.Voters, append(cur.Clone().Learners, cc.Member))
	case RemoveLearner:
		if !cur.IsLearner(id) {
			return Configuration{}, fmt.Errorf("%w: %s is not a learner", ErrInvalidConfChange, id)
		}
		return replication.NewConfiguration(cur.Voters, without(cur.Learners, id))
	case Promote:
		if !cur.IsLearner(id) {
			return Configuration{}, fmt.Errorf("%w: %s is not a learner", ErrInvalidConfChange, id)
		}
		var m Member
		for _, l := range cur.Learners {
			if l.ID == id {
				m = l // the address the learner was added with
			}
		}
		next, err := replication.NewConfiguration(append(cur.Clone().Voters, m), without(cur.Learners, id))
		if err != nil {
			return Configuration{}, err
		}
		next.Outgoing = cur.Clone().Voters
		return next, next.Validate()
	case RemoveVoter:
		if !cur.IsVoter(id) {
			return Configuration{}, fmt.Errorf("%w: %s is not a voter", ErrInvalidConfChange, id)
		}
		if len(cur.Voters) == 1 {
			return Configuration{}, fmt.Errorf("%w: %s is the last voter", ErrInvalidConfChange, id)
		}
		next, err := replication.NewConfiguration(without(cur.Voters, id), cur.Learners)
		if err != nil {
			return Configuration{}, err
		}
		next.Outgoing = cur.Clone().Voters
		return next, next.Validate()
	}
	return Configuration{}, fmt.Errorf("%w: unknown operation %d", ErrInvalidConfChange, cc.Type)
}

// Final is the stable configuration a joint one leads to: its Voters and
// Learners, no Outgoing.
func Final(joint Configuration) Configuration {
	return Configuration{Voters: joint.Clone().Voters, Learners: joint.Clone().Learners}
}

func without(list []Member, id NodeID) []Member {
	var out []Member
	for _, m := range list {
		if m.ID != id {
			out = append(out, m)
		}
	}
	return out
}

// majority reports whether has holds for more than half of ids. An empty set
// has no majority: a configuration with no voters can decide nothing.
func majority(ids []NodeID, has func(NodeID) bool) bool {
	if len(ids) == 0 {
		return false
	}
	n := 0
	for _, id := range ids {
		if has(id) {
			n++
		}
	}
	return n > len(ids)/2
}

// quorumOf reports whether the nodes for which has holds form a quorum of conf
// (docs/MEMBERSHIP.md §3): a majority of the voters, and in a joint
// configuration a majority of the outgoing voters as well. Learners are never
// consulted.
func quorumOf(conf Configuration, has func(NodeID) bool) bool {
	if !majority(conf.VoterIDs(), has) {
		return false
	}
	if conf.Joint() && !majority(conf.OutgoingIDs(), has) {
		return false
	}
	return true
}

// hasQuorum is quorumOf on the current configuration.
func (r *Raft) hasQuorum(has func(NodeID) bool) bool { return quorumOf(r.conf, has) }

// Conf returns the current configuration and the log index it came from (0:
// the base configuration — the snapshot's or the genesis).
func (r *Raft) Conf() (Configuration, uint64) { return r.conf.Clone(), r.confIndex }

// ConfPending reports whether a configuration change is under way: the current
// configuration's entry is not committed yet, or it is joint (its final entry
// is still to come).
func (r *Raft) ConfPending() bool {
	return r.confIndex > r.log.CommitIndex() || r.conf.Joint()
}

// IsVoter reports whether this node votes in its current configuration.
func (r *Raft) IsVoter() bool { return r.conf.IsVoter(r.id) }

// ConfAt returns the configuration in effect at index: the latest configuration
// entry at or below it in the log, else the base configuration. index must be
// held by the log or be its boundary (the driver names a snapshot's
// configuration with it).
func (r *Raft) ConfAt(index uint64) (Configuration, error) {
	base, _ := r.log.Boundary()
	if index < base || index > r.log.LastIndex() {
		return Configuration{}, replication.ErrOutOfRange
	}
	for i := index; i > base; i-- {
		e, err := r.log.At(i)
		if err != nil {
			return Configuration{}, err
		}
		if e.Type == replication.EntryConfig {
			return replication.DecodeConfiguration(e.Data)
		}
	}
	return r.baseConf.Clone(), nil
}

// ProposeConfChange starts a membership change on the leader (docs/MEMBERSHIP.md
// §4): it appends the configuration entry the change leads to — a joint one for
// Promote and RemoveVoter — and replicates it. From that entry on the leader
// uses the new configuration. When a joint entry commits the leader appends the
// final entry on its own (afterCommit); when a stable entry that excludes the
// leader commits, it steps down. ErrNotLeader on a non-leader;
// ErrConfChangeInProgress while one is under way; ErrInvalidConfChange when the
// operation does not apply.
func (r *Raft) ProposeConfChange(cc ConfChange) error {
	if r.role != Leader {
		return ErrNotLeader
	}
	if r.ConfPending() {
		return ErrConfChangeInProgress
	}
	next, err := cc.Apply(r.conf)
	if err != nil {
		return err
	}
	r.appendConf(next)
	r.broadcastAppend()
	r.maybeCommit()
	return nil
}

// appendConf appends a configuration entry (the leader's own) and adopts it.
func (r *Raft) appendConf(c Configuration) {
	idx := r.log.LastIndex() + 1
	if err := r.log.Append(Entry{Index: idx, Term: r.currentTerm, Data: replication.EncodeConfiguration(c), Type: replication.EntryConfig}); err != nil {
		panic("raft: leader append rejected by log: " + err.Error())
	}
	r.markUnstable(idx)
	r.setConf(c, idx)
}

// setConf makes c the current configuration, from log index idx (0: the base),
// and brings the leader's replication state in line with it: progress for every
// member, none for anyone else.
func (r *Raft) setConf(c Configuration, idx uint64) {
	r.conf = c.Clone()
	r.confIndex = idx
	r.peers = r.conf.Members()
	if r.role == Leader {
		r.syncProgress()
	}
	if r.role == Candidate && !r.conf.IsVoter(r.id) {
		// A candidate that learns it is no longer a voter stops campaigning.
		r.role = Follower
		r.votesGranted = nil
		r.resetElectionTimer()
	}
}

// syncProgress gives every member the leader replicates to a nextIndex and
// matchIndex (a new member starts at the leader's tail, as a fresh leader's
// followers do — its rejections back the leader up, to a snapshot if need be),
// and forgets members that are gone.
func (r *Raft) syncProgress() {
	last := r.log.LastIndex()
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		if _, ok := r.nextIndex[p]; !ok {
			r.nextIndex[p] = last + 1
			r.matchIndex[p] = 0
		}
	}
	for p := range r.nextIndex {
		if !r.conf.IsMember(p) {
			delete(r.nextIndex, p)
			delete(r.matchIndex, p)
			delete(r.snapPending, p)
			delete(r.snapWait, p)
			delete(r.ackSeq, p)
		}
	}
}

// reconcileConf recomputes the current configuration from the log: the latest
// configuration entry after the boundary, else the base configuration. It is
// called after any change to the log that may have added or removed a
// configuration entry (a follower's append or truncation, a snapshot install,
// recovery).
func (r *Raft) reconcileConf() {
	base, _ := r.log.Boundary()
	for i := r.log.LastIndex(); i > base; i-- {
		e, err := r.log.At(i)
		if err != nil {
			break
		}
		if e.Type != replication.EntryConfig {
			continue
		}
		if i != r.confIndex {
			c, err := replication.DecodeConfiguration(e.Data)
			if err != nil {
				// The codec refused it on the wire and recovery refuses it on
				// disk; a configuration entry in the log is always decodable.
				panic("raft: undecodable configuration entry in the log: " + err.Error())
			}
			r.setConf(c, i)
		}
		return
	}
	if r.confIndex != 0 || !r.conf.Equal(r.baseConf) {
		r.setConf(r.baseConf, 0)
	}
}

// afterCommit is the leader's reaction to a commit advance (docs/MEMBERSHIP.md
// §4, §5): a committed joint configuration gets its final entry; a committed
// stable configuration that excludes the leader makes it step down. Followers
// do neither — the next leader does, from the same log.
func (r *Raft) afterCommit() {
	if r.role != Leader || r.confIndex > r.log.CommitIndex() {
		return
	}
	if r.conf.Joint() {
		r.appendConf(Final(r.conf))
		r.broadcastAppend()
		// The final entry cannot be committed by this call: it was just appended
		// and no peer has acknowledged it. maybeCommit runs on the next response.
		return
	}
	if !r.conf.IsVoter(r.id) {
		r.becomeFollower(r.currentTerm, "")
	}
}
