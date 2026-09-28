package raftsim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raft"
)

// Phase 15 scripted membership scenarios (docs/MEMBERSHIP.md): each drives the
// real cores, durable logs and driver orderings through one membership story
// while every continuous invariant — the INV-M series included — is checked
// after every event.

func memberCfg(nodes, genesis int, seed int64) Config {
	return Config{Nodes: nodes, Genesis: genesis, Seed: seed}
}

// member submits a membership change to leader and requires the core to
// accept it.
func (s *sim) member(leader NodeID, op string, target NodeID) {
	s.t.Helper()
	before := s.Stats().ConfChangesAccepted
	s.Apply(Event{Kind: Member, Node: leader, Data: op, To: target})
	if s.Stats().ConfChangesAccepted != before+1 {
		s.t.Fatalf("%s refused %s %s:\n%s", leader, op, target, strings.Join(s.Trace().Tail(10), "\n"))
	}
}

// settleConf heartbeats leader until its configuration change is complete and
// every member of its configuration that is up — but those cut off by the
// scenario, listed in cutOff — has committed its log.
func (s *sim) settleConf(leader NodeID, cutOff ...NodeID) raft.Configuration {
	s.t.Helper()
	skip := map[NodeID]bool{}
	for _, id := range cutOff {
		skip[id] = true
	}
	for i := 0; i < 200; i++ {
		st := s.State(leader)
		if st.Role != raft.Leader {
			s.t.Fatalf("%s lost its leadership: %+v", leader, st)
		}
		done := st.Commit == st.LastIndex && !s.Cluster.nodes[leader].core.ConfPending()
		for _, id := range st.Conf.Members() {
			if o := s.State(id); o.Up && !skip[id] && o.Commit < st.LastIndex {
				done = false
			}
		}
		if done {
			return st.Conf
		}
		s.heartbeat(leader)
	}
	s.t.Fatalf("the change did not settle on %s: %+v\n%s", leader, s.State(leader), strings.Join(s.Trace().Tail(30), "\n"))
	return raft.Configuration{}
}

// electAmong ticks every node of ids, delivering, until one of them leads.
func (s *sim) electAmong(ids ...NodeID) NodeID {
	s.t.Helper()
	for i := 0; i < 60*raft.DefaultElectionTicks; i++ {
		for _, id := range ids {
			if s.Up(id) && s.State(id).Role == raft.Leader {
				return id
			}
		}
		for _, id := range ids {
			if s.Up(id) {
				s.tick(id, 1)
			}
		}
		s.DeliverAll()
	}
	s.t.Fatalf("no leader among %v", ids)
	return ""
}

// requireStable ticks every node — the stale ones campaign — and requires
// that leader keeps its leadership and term throughout.
func (s *sim) requireStable(leader NodeID, rounds int) {
	s.t.Helper()
	term := s.State(leader).Term
	for i := 0; i < rounds; i++ {
		for _, id := range s.IDs() {
			if s.Up(id) {
				s.tick(id, 1)
			}
		}
		s.DeliverAll()
		if st := s.State(leader); st.Role != raft.Leader || st.Term != term {
			s.t.Fatalf("the group's leader %s (term %d) was disturbed: %+v\n%s", leader, term, st, strings.Join(s.Trace().Tail(30), "\n"))
		}
	}
}

// TestSimRemoveAFollower (docs/MEMBERSHIP.md §5, case A): a follower removed
// by joint consensus stops participating — the leader sends it nothing, and
// entries commit without it.
func TestSimRemoveAFollower(t *testing.T) {
	s := newSimWith(t, memberCfg(3, 3, 1))
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.member("n1", "removevoter", "n3")
	conf := s.settleConf("n1")
	if conf.IsMember("n3") || len(conf.Voters) != 2 {
		t.Fatalf("after the removal: %s", conf)
	}
	last3 := s.State("n3").LastIndex
	s.commit("n1", "b", "n1", "n2")
	s.heartbeat("n1")
	if n := s.inFlight(func(m raft.Message) bool { return m.To == "n3" }); n != 0 {
		t.Fatalf("the leader still sends to the removed n3: %d messages", n)
	}
	if s.State("n3").LastIndex != last3 {
		t.Fatalf("the removed n3 kept receiving entries: %d -> %d", last3, s.State("n3").LastIndex)
	}
}

// TestSimRemoveTheLeader (case B): the leader removes itself: it leads the
// change to its end, steps down once the final configuration commits, and the
// remaining voters elect a leader among themselves — never it again.
func TestSimRemoveTheLeader(t *testing.T) {
	s := newSimWith(t, memberCfg(3, 3, 2))
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.member("n1", "removevoter", "n1")
	for i := 0; i < 50 && s.State("n1").Role == raft.Leader; i++ {
		s.heartbeat("n1")
	}
	st := s.State("n1")
	if st.Role == raft.Leader || st.Conf.IsMember("n1") || st.Conf.Joint() {
		t.Fatalf("the removed leader: %+v", st)
	}
	l := s.electAmong("n2", "n3")
	s.commit(l, "b", "n2", "n3")
	s.requireStable(l, 3*raft.DefaultElectionTicks)
}

// TestSimRemoveAPartitionedNode (case C): n3 is cut off before its removal is
// proposed, so it never learns of it; healed, it campaigns with a stale log and
// ever higher terms — and is refused without its term being adopted: the
// group's leader is never disturbed.
func TestSimRemoveAPartitionedNode(t *testing.T) {
	s := newSimWith(t, memberCfg(3, 3, 3))
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Isolate, Node: "n3"})
	s.member("n1", "removevoter", "n3")
	s.settleConf("n1", "n3")
	s.tick("n3", 5*raft.DefaultElectionTicks) // it campaigns, alone
	if s.State("n3").Term <= s.State("n1").Term {
		t.Fatalf("premise: the cut-off n3 did not campaign past the group's term (%d vs %d)", s.State("n3").Term, s.State("n1").Term)
	}
	s.do(Event{Kind: HealAll})
	s.requireStable("n1", 5*raft.DefaultElectionTicks)
}

// TestSimRemovedNodeRestartsAndStaysOut (case D): a removed node restarted
// from its own durable state — which still names it a member — cannot regain
// authority: it never leads, and the group's leader is never disturbed.
func TestSimRemovedNodeRestartsAndStaysOut(t *testing.T) {
	s := newSimWith(t, memberCfg(3, 3, 4))
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.member("n1", "removevoter", "n3")
	s.settleConf("n1")
	s.do(Event{Kind: Crash, Node: "n3", Power: true})
	s.restart("n3")
	if st := s.State("n3"); !st.Conf.IsVoter("n3") {
		t.Logf("n3 restarted knowing its removal (%s)", st.Conf)
	}
	s.requireStable("n1", 5*raft.DefaultElectionTicks)
	s.commit("n1", "b", "n1", "n2")
}

// TestSimReplaceAFailedNode is the replacement procedure (docs/MEMBERSHIP.md
// §4): n3 fails permanently; the spare n4 joins as a learner, catches up, is
// promoted, and n3 is removed — each by joint consensus, with n3 down
// throughout. Then n3's stale process returns with its old durable state: it
// cannot regain authority, and the group of n1, n2 and n4 carries on and
// converges.
func TestSimReplaceAFailedNode(t *testing.T) {
	s := newSimWith(t, memberCfg(4, 3, 5))
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Crash, Node: "n3", Power: true})
	s.member("n1", "addlearner", "n4")
	s.settleConf("n1")
	s.commit("n1", "b", "n1", "n2", "n4")
	s.member("n1", "promote", "n4")
	s.settleConf("n1")
	s.member("n1", "removevoter", "n3")
	conf := s.settleConf("n1")
	if conf.IsMember("n3") || !conf.IsVoter("n4") || len(conf.Voters) != 3 {
		t.Fatalf("after the replacement: %s", conf)
	}
	s.restart("n3")
	s.requireStable("n1", 5*raft.DefaultElectionTicks)
	s.commit("n1", "c", "n1", "n2", "n4")
	s.Stabilize(400)
}

// TestSimNewMemberCatchesUpBySnapshot is the canonical path of
// docs/MEMBERSHIP.md §6: the group has compacted its log past everything a
// new node could be sent as entries; the new node, added as a learner, is
// offered a snapshot, installs it — adopting the configuration it carries,
// which names it a learner — catches up by the suffix, and is promoted.
func TestSimNewMemberCatchesUpBySnapshot(t *testing.T) {
	cfg := memberCfg(4, 3, 6)
	cfg.SnapshotEvery, cfg.SnapshotRetain = 4, 0
	s := newSimWith(t, cfg)
	s.electLeader("n1")
	for i := 0; i < 12; i++ {
		s.commit("n1", fmt.Sprintf("a%d", i), "n1", "n2", "n3")
	}
	if b := s.State("n1").Boundary; b < 5 {
		t.Fatalf("premise: the leader's log is compacted only through %d", b)
	}
	s.member("n1", "addlearner", "n4")
	s.settleConf("n1")
	s.commit("n1", "b", "n1", "n2", "n3", "n4")
	st := s.State("n4")
	if s.Stats().Installs == 0 || st.Snapshot == 0 || !st.Conf.IsLearner("n4") {
		t.Fatalf("n4 did not catch up by a snapshot naming it: installs=%d %+v", s.Stats().Installs, st)
	}
	s.member("n1", "promote", "n4")
	if conf := s.settleConf("n1"); !conf.IsVoter("n4") {
		t.Fatalf("after the promotion: %s", conf)
	}
	s.Stabilize(400)
}

// TestSimSnapshotDuringJointConfiguration: the leader snapshots while its
// joint configuration is committed and its final one is not yet — the
// snapshot carries the joint configuration — then loses power and restarts
// from that snapshot and its log: it recovers the final configuration from the
// log's suffix (INV-MB8 checks the derivation at every step), and the change
// completes.
func TestSimSnapshotDuringJointConfiguration(t *testing.T) {
	cfg := memberCfg(4, 3, 7)
	s := newSimWith(t, cfg)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.member("n1", "addlearner", "n4")
	s.settleConf("n1")
	s.member("n1", "promote", "n4")
	// Step one message at a time until the joint entry has committed on n1 and
	// n1 has appended the final one, not yet committed.
	n1 := s.Cluster.nodes["n1"].core
	for i := 0; i < 2000; i++ {
		if c, _ := n1.Conf(); !c.Joint() && n1.ConfPending() {
			break
		}
		if !s.deliverOldest() {
			s.tick("n1", 1)
		}
	}
	if c, _ := n1.Conf(); c.Joint() || !n1.ConfPending() {
		t.Fatalf("premise: n1 is not between its joint and final configurations: %s pending=%v", c, n1.ConfPending())
	}
	s.dropAll(func(raft.Message) bool { return true }) // the final cannot commit yet
	s.do(Event{Kind: SnapshotNow, Node: "n1"})
	if snap := s.Cluster.nodes["n1"].dur.Snap.Published(); snap.Index == 0 || !snap.Conf.Joint() {
		t.Fatalf("the snapshot does not carry the joint configuration: %+v", snap)
	}
	s.do(Event{Kind: Crash, Node: "n1", Power: true})
	s.restart("n1")
	l := s.electAmong("n1", "n2", "n3", "n4")
	s.settleConf(l)
	s.Stabilize(400)
}

// TestSimRemovedNodeReceivesAStaleSnapshot: a snapshot transfer to n3 is held
// in the network while n3 is removed; delivered afterwards, n3 installs a
// snapshot naming it a member — stale — and campaigns on it: it is refused,
// and the group is never disturbed.
func TestSimRemovedNodeReceivesAStaleSnapshot(t *testing.T) {
	cfg := memberCfg(3, 3, 8)
	cfg.SnapshotEvery, cfg.SnapshotRetain = 3, 0
	s := newSimWith(t, cfg)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Isolate, Node: "n3"})
	for i := 0; i < 8; i++ {
		s.commit("n1", fmt.Sprintf("b%d", i), "n1", "n2")
	}
	s.do(Event{Kind: HealAll})
	// Hold the snapshot the leader now offers n3 (it re-offers one lost to the
	// partition after its retry interval, 4 election timeouts).
	for i := 0; i < 10*raft.DefaultElectionTicks && s.chunkFlights("n3") == 0; i++ {
		s.tick("n1", 1)
		s.deliverWhereNot(func(f *flight) bool { return f.chunk != nil })
	}
	if s.chunkFlights("n3") == 0 {
		t.Fatal("premise: the leader never offered n3 a snapshot")
	}
	s.holdChunks("n3")
	s.member("n1", "removevoter", "n3")
	s.settleConf("n1")
	s.do(Event{Kind: Release})
	s.DeliverAll()
	if s.Stats().Installs == 0 {
		t.Fatal("premise: n3 never installed the stale snapshot")
	}
	s.requireStable("n1", 5*raft.DefaultElectionTicks)
}

// deliverOldest delivers the oldest deliverable message; false if none.
func (s *sim) deliverOldest() bool {
	before := s.Stats().Delivered + s.Stats().DroppedPartition + s.Stats().DroppedDown
	s.deliverWhereNot(func(*flight) bool { return false }, 1)
	return s.Stats().Delivered+s.Stats().DroppedPartition+s.Stats().DroppedDown != before
}

// chunkFlights counts the snapshot chunks in flight to id.
func (s *sim) chunkFlights(to NodeID) int {
	n := 0
	for _, f := range s.Cluster.flights {
		if f.chunk != nil && f.msg.To == to {
			n++
		}
	}
	return n
}

// holdChunks delays every snapshot chunk in flight to id far into the future.
func (s *sim) holdChunks(to NodeID) {
	for _, f := range s.Cluster.flights {
		if f.chunk != nil && f.msg.To == to {
			f.holdUntil = s.Step() + 1_000_000
		}
	}
}

// deliverWhereNot delivers in-flight messages, oldest first, except those skip
// selects — at most limit of them (none: every one).
func (s *sim) deliverWhereNot(skip func(*flight) bool, limit ...int) {
	max := 10000
	if len(limit) > 0 {
		max = limit[0]
	}
	for i := 0; i < max; i++ {
		var next *flight
		for _, f := range s.Cluster.flights {
			if !skip(f) && f.holdUntil <= s.Step() && !s.Paused(f.msg.To) {
				next = f
				break
			}
		}
		if next == nil {
			return
		}
		pos := 0
		for _, g := range s.Cluster.flights {
			if g == next {
				break
			}
			if g.msg.From == next.msg.From && g.msg.To == next.msg.To {
				pos++
			}
		}
		s.Apply(Event{Kind: Deliver, From: next.msg.From, To: next.msg.To, Pos: pos})
	}
}

// TestMembershipRegressionSeeds replays the seeds of the 200-seed membership
// runs that found the vote deadlock (a learner that missed its promotion, and
// a joiner with no configuration, refused the vote their election needed):
// every one now converges.
func TestMembershipRegressionSeeds(t *testing.T) {
	for name, seeds := range map[string][]int64{
		"membership":            {9, 18, 41},
		"membership-partitions": {36, 45, 71},
		"membership-snapshots":  {1, 25, 37},
	} {
		p, _ := ProfileByName(name)
		for _, seed := range seeds {
			if r := Run(p, seed); r.Violation != nil {
				t.Fatalf("%s seed %d: %s", name, seed, r.Report(reproCommand(p, seed)))
			}
		}
	}
}

// TestMembershipEventsRoundTrip: the Member event's script form parses back,
// an unknown operation is refused, and a change the core cannot apply is
// skipped as refused, never an error.
func TestMembershipEventsRoundTrip(t *testing.T) {
	e := Event{Kind: Member, Node: "n1", Data: "promote", To: "n4"}
	got, err := ParseEvent(e.String())
	if err != nil || got != e {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	if _, err := ParseEvent("member n1 rebalance n4"); err == nil {
		t.Fatal("an unknown operation parsed")
	}
	s := newSimWith(t, memberCfg(3, 3, 9))
	s.electLeader("n1")
	before := s.Stats().ConfChangesRejected
	s.do(Event{Kind: Member, Node: "n1", Data: "promote", To: "n2"}) // a voter, not a learner
	s.do(Event{Kind: Member, Node: "n2", Data: "removevoter", To: "n3"})
	if s.Stats().ConfChangesRejected != before+2 {
		t.Fatalf("refusals: %d", s.Stats().ConfChangesRejected-before)
	}
}

// TestKVSimRetryIsADuplicateAcrossMembershipSnapshotAndFullRestart: the
// session table is replicated state, so request identity survives everything
// Phase 15 can do to a group. c1's PUT executes and is applied, and its leader
// n1 dies before the reply; a new leader adds n4, promotes it and removes n1;
// every member snapshots and compacts past the write; every node loses power
// and restarts from its snapshot. c1's retry of the SAME request, at the new
// leader, is answered as a duplicate of the original execution — never
// executed again — and the history is linearizable.
func TestKVSimRetryIsADuplicateAcrossMembershipSnapshotAndFullRestart(t *testing.T) {
	cfg := memberCfg(4, 3, 21)
	cfg.SnapshotEvery, cfg.SnapshotRetain = 4, 0
	s := &kvSim{newSimWith(t, cfg)}
	s.electLeader("n1")
	s.register("n1", "c1")
	s.register("n1", "c2")
	s.heartbeat("n1")
	s.do(Event{Kind: CrashAt, Node: "n1", Point: "after-applied-to", Nth: 1})
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	s.heartbeat("n1")
	if s.Up("n1") || !s.Busy("c1") {
		t.Fatalf("premise: n1 died after applying c1's write, unanswered (n1 up %v, c1 busy %v)", s.Up("n1"), s.Busy("c1"))
	}
	s.do(Event{Kind: KVTimeout, Client: "c1"}) // that send is given up; the request stays open
	l := s.electAmong("n2", "n3")
	s.restart("n1")
	s.member(l, "addlearner", "n4")
	s.settleConf(l)
	s.member(l, "promote", "n4")
	s.settleConf(l)
	s.member(l, "removevoter", "n1")
	conf := s.settleConf(l)
	if conf.IsMember("n1") || !conf.IsVoter("n4") {
		t.Fatalf("after the membership changes: %s", conf)
	}
	for i := 0; i < 4; i++ {
		s.put(l, "c2", fmt.Sprintf("other%d", i), "x")
		s.DeliverAll()
		s.heartbeat(l)
	}
	for _, id := range conf.Members() {
		s.do(Event{Kind: SnapshotNow, Node: id})
	}
	s.settleConf(l)
	for _, id := range conf.Members() {
		if st := s.State(id); st.Boundary < 5 {
			t.Fatalf("premise: %s did not compact past the write: %+v", id, st)
		}
	}
	for _, id := range s.IDs() {
		s.do(Event{Kind: Crash, Node: id, Power: true})
	}
	for _, id := range s.IDs() {
		s.restart(id)
	}
	l = s.electAmong(conf.VoterIDs()...)
	s.Apply(Event{Kind: KVRetry, Node: l, Client: "c1"})
	for i := 0; i < 20 && s.Busy("c1"); i++ {
		s.DeliverAll()
		s.heartbeat(l)
	}
	op := s.last("c1")
	last := op.Attempts[len(op.Attempts)-1]
	if op.Outcome != lincheck.OK || !strings.HasPrefix(last.Result, "duplicate of index") {
		t.Fatalf("the retry must be answered as a duplicate of the original: %s %+v", op, op.Attempts)
	}
	s.get(l, "c3", "k")
	for i := 0; i < 20 && s.Busy("c3"); i++ {
		s.DeliverAll()
		s.heartbeat(l)
	}
	if op := s.last("c3"); op.Outcome != lincheck.OK || string(op.Output) != "A" {
		t.Fatalf("k after the retry: %s", op)
	}
	s.requireLinearizable()
}
