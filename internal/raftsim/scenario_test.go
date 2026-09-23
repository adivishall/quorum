package raftsim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/raft"
)

// These are the Phase 10 scripted fault scenarios (docs/FAULTS.md §6). Each one
// drives a deterministic cluster through an exact fault sequence and asserts the
// specific behaviour it expects. Throughout, the continuous invariants (check.go:
// INV-R1..R10, INV-F2, INV-F4 and the fsync form of INV-R6) run after every event;
// any violation fails the test immediately with the trace and the replay script.

// sim wraps a Cluster with scenario helpers. Every helper is a sequence of Events,
// so a scenario is itself a replayable script.
type sim struct {
	*Cluster
	t *testing.T
}

func newSim(t *testing.T, nodes int, seed int64) *sim {
	t.Helper()
	c, err := New(Config{Nodes: nodes, Seed: seed})
	if err != nil {
		t.Fatal(err)
	}
	s := &sim{Cluster: c, t: t}
	c.OnViolation = func(v *Violation) {
		t.Helper()
		t.Fatalf("%v\n--- trace ---\n%s\n--- script (%d events) ---\n%s", v,
			strings.Join(c.Trace().Tail(80), "\n"), len(c.Script()), FormatScript(c.Script()))
	}
	return s
}

func (s *sim) do(es ...Event) {
	for _, e := range es {
		s.Apply(e)
	}
}

func (s *sim) tick(id NodeID, n int) {
	for i := 0; i < n; i++ {
		s.Apply(Event{Kind: Tick, Node: id})
	}
}

// campaign ticks id until it stands for election, leaving its RequestVotes in
// flight (only id's clock moves, so no one else campaigns).
func (s *sim) campaign(id NodeID) {
	s.t.Helper()
	for i := 0; i < 4*raft.DefaultElectionTicks; i++ {
		if r := s.State(id).Role; r == raft.Candidate || r == raft.Leader {
			return
		}
		s.tick(id, 1)
	}
	s.t.Fatalf("%s never campaigned", id)
}

// electLeader makes id campaign, delivers everything, and requires it to lead.
func (s *sim) electLeader(id NodeID) {
	s.t.Helper()
	s.campaign(id)
	s.DeliverAll()
	if st := s.State(id); st.Role != raft.Leader {
		s.t.Fatalf("%s did not win: role=%s term=%d", id, st.Role, st.Term)
	}
}

// propose submits data on id and requires the leader to accept it.
func (s *sim) propose(id NodeID, data string) uint64 {
	s.t.Helper()
	before := s.Stats().ProposalsAccepted
	s.Apply(Event{Kind: Propose, Node: id, Data: data})
	if s.Stats().ProposalsAccepted != before+1 {
		s.t.Fatalf("%s rejected proposal %q (role %s)", id, data, s.State(id).Role)
	}
	return s.State(id).LastIndex
}

// heartbeat ticks the leader through one heartbeat interval and delivers.
func (s *sim) heartbeat(id NodeID) {
	s.tick(id, raft.DefaultHeartbeatTicks)
	s.DeliverAll()
}

// commit proposes data on the leader and runs heartbeats until every listed node
// has committed it.
func (s *sim) commit(leader NodeID, data string, on ...NodeID) uint64 {
	s.t.Helper()
	idx := s.propose(leader, data)
	s.DeliverAll()
	for i := 0; i < 20; i++ {
		done := true
		for _, id := range on {
			if s.State(id).Commit < idx {
				done = false
			}
		}
		if done {
			return idx
		}
		s.heartbeat(leader)
	}
	s.t.Fatalf("%q (index %d) did not commit on %v", data, idx, on)
	return 0
}

// logHas reports whether id's log holds a command.
func (s *sim) logHas(id NodeID, data string) bool {
	for _, e := range s.State(id).Log {
		if string(e.Data) == data {
			return true
		}
	}
	return false
}

func (s *sim) requireSameLog(a, b NodeID) {
	s.t.Helper()
	la, lb := s.State(a).Log, s.State(b).Log
	if len(la) != len(lb) {
		s.t.Fatalf("%s has %d entries, %s has %d", a, len(la), b, len(lb))
	}
	for i := range la {
		if !sameEntry(la[i], lb[i]) {
			s.t.Fatalf("%s and %s differ at index %d", a, b, i+1)
		}
	}
}

// inFlight counts in-flight messages matching a predicate.
func (s *sim) inFlight(match func(raft.Message) bool) int {
	n := 0
	for _, m := range s.InFlight() {
		if match(m) {
			n++
		}
	}
	return n
}

// dropAll drops every in-flight message matching a predicate.
func (s *sim) dropAll(match func(raft.Message) bool) int {
	dropped := 0
	for {
		var pick *raft.Message
		pos := map[[2]NodeID]int{}
		var at int
		for _, m := range s.InFlight() {
			k := [2]NodeID{m.From, m.To}
			if match(m) {
				mm := m
				pick, at = &mm, pos[k]
				break
			}
			pos[k]++
		}
		if pick == nil {
			return dropped
		}
		s.Apply(Event{Kind: Drop, From: pick.From, To: pick.To, Pos: at})
		dropped++
	}
}

func typeIs(t raft.MessageType) func(raft.Message) bool {
	return func(m raft.Message) bool { return m.Type == t }
}

// --- A. leader isolated from the majority ---

// TestLeaderIsolatedFromMajority: the leader is cut off; an entry it accepts can
// never commit; the majority elects a new leader in a higher term and commits;
// after healing, the old leader steps down and its uncommitted entry is replaced.
func TestLeaderIsolatedFromMajority(t *testing.T) {
	s := newSim(t, 5, 1)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3", "n4", "n5")

	s.do(Event{Kind: Isolate, Node: "n1"})
	stranded := s.propose("n1", "stranded")
	s.heartbeat("n1")
	if c := s.State("n1").Commit; c >= stranded {
		t.Fatalf("isolated leader committed index %d without a quorum", c)
	}

	s.electLeader("n2")
	if s.State("n2").Term <= s.State("n1").Term {
		t.Fatalf("new leader's term %d is not above the isolated leader's %d", s.State("n2").Term, s.State("n1").Term)
	}
	fresh := s.commit("n2", "fresh", "n2", "n3", "n4", "n5")
	if st := s.State("n1"); st.Role != raft.Leader || st.Term != 1 {
		t.Fatalf("the isolated leader should still believe it leads term 1: %+v", st)
	}

	s.do(Event{Kind: HealAll})
	s.heartbeat("n2")
	s.heartbeat("n2")
	if st := s.State("n1"); st.Role != raft.Follower || st.Term != s.State("n2").Term {
		t.Fatalf("old leader did not step down after healing: %+v", st)
	}
	for _, id := range []NodeID{"n1", "n3", "n4", "n5"} {
		s.requireSameLog("n2", id)
		if s.logHas(id, "stranded") {
			t.Fatalf("%s still holds the isolated leader's uncommitted entry", id)
		}
		if s.State(id).Commit < fresh {
			t.Fatalf("%s did not commit %d", id, fresh)
		}
	}
}

// --- B. follower crash and restart ---

// TestFollowerCrashAndCatchUp: a follower crashes; the leader keeps committing with
// the remaining majority; the follower restarts from exactly its durable state
// (INV-F2, checked by the harness on every restart) and catches up.
func TestFollowerCrashAndCatchUp(t *testing.T) {
	s := newSim(t, 3, 2)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	before := s.State("n3").LastIndex

	s.do(Event{Kind: Crash, Node: "n3"})
	for _, cmd := range []string{"b", "c", "d"} {
		s.commit("n1", cmd, "n1", "n2")
	}
	s.do(Event{Kind: Restart, Node: "n3"})
	if got := s.State("n3").LastIndex; got != before {
		t.Fatalf("restarted follower recovered %d entries, want the %d it had durably", got, before)
	}
	s.heartbeat("n1")
	s.heartbeat("n1")
	s.requireSameLog("n1", "n3")
	if st := s.State("n3"); st.Commit != s.State("n1").Commit || st.Applied != st.Commit {
		t.Fatalf("follower did not catch up: %+v", st)
	}
}

// --- C. leader crash and re-election ---

// TestLeaderCrashAndReelection: the leader crashes with a power loss after
// committing; a new leader is elected and must hold the committed entry (INV-R4);
// it commits more; the old leader restarts from its fsynced state and rejoins.
func TestLeaderCrashAndReelection(t *testing.T) {
	s := newSim(t, 3, 3)
	s.electLeader("n1")
	x := s.commit("n1", "x", "n1", "n2", "n3")

	s.do(Event{Kind: Crash, Node: "n1", Power: true})
	s.electLeader("n2")
	if !s.logHas("n2", "x") {
		t.Fatal("new leader lacks the committed entry")
	}
	s.commit("n2", "y", "n2", "n3")

	s.do(Event{Kind: Restart, Node: "n1"})
	if st := s.State("n1"); st.Role != raft.Follower || st.LastIndex < x {
		t.Fatalf("old leader restarted without its durable log: %+v", st)
	}
	s.heartbeat("n2")
	s.heartbeat("n2")
	s.requireSameLog("n2", "n1")
	if !s.logHas("n1", "y") || s.State("n1").Commit != s.State("n2").Commit {
		t.Fatalf("old leader did not rejoin: %+v", s.State("n1"))
	}
}

// --- D. message loss ---

// TestMessageLoss drops each message type in turn and proves progress stops only
// while the loss lasts, then resumes through Raft's own retransmission.
func TestMessageLoss(t *testing.T) {
	t.Run("AppendEntries", func(t *testing.T) {
		s := newSim(t, 3, 4)
		s.electLeader("n1")
		idx := s.propose("n1", "x")
		if s.dropAll(typeIs(raft.MsgAppendRequest)) == 0 {
			t.Fatal("nothing to drop")
		}
		s.DeliverAll()
		if s.State("n1").Commit >= idx {
			t.Fatal("committed although every AppendEntries was lost")
		}
		s.heartbeat("n1") // retransmits from nextIndex
		s.heartbeat("n1")
		if s.State("n1").Commit < idx {
			t.Fatal("did not recover after AppendEntries loss")
		}
	})
	t.Run("RequestVote", func(t *testing.T) {
		s := newSim(t, 3, 5)
		s.campaign("n1")
		first := s.State("n1").Term
		s.dropAll(typeIs(raft.MsgVoteRequest))
		s.DeliverAll()
		if s.State("n1").Role == raft.Leader {
			t.Fatal("elected with every vote request lost")
		}
		// Its election timer expires again; the retried election succeeds.
		for i := 0; i < 4*raft.DefaultElectionTicks && s.State("n1").Role != raft.Leader; i++ {
			s.tick("n1", 1)
			s.DeliverAll()
		}
		if st := s.State("n1"); st.Role != raft.Leader || st.Term <= first {
			t.Fatalf("no leader after the retried election: %+v", st)
		}
	})
	t.Run("responses", func(t *testing.T) {
		s := newSim(t, 3, 6)
		s.electLeader("n1")
		idx := s.propose("n1", "x")
		for i := 0; i < 3; i++ {
			s.deliverFrom("n1") // followers receive and append x ...
			if s.dropAll(typeIs(raft.MsgAppendResponse)) == 0 {
				t.Fatal("no acknowledgement to drop")
			}
			s.tick("n1", raft.DefaultHeartbeatTicks) // ... and every acknowledgement is lost
		}
		if n := s.State("n2").LastIndex; n < idx {
			t.Fatalf("follower never received x (last %d)", n)
		}
		if s.State("n1").Commit >= idx {
			t.Fatal("leader committed without a single acknowledgement")
		}
		s.DeliverAll()
		s.heartbeat("n1")
		if s.State("n1").Commit < idx {
			t.Fatal("did not commit once acknowledgements got through")
		}
	})
}

// --- E. duplicated messages ---

// TestDuplicatedVoteDoesNotCountTwice: a 5-node candidate receives ONE real vote,
// duplicated many times. Two distinct votes (itself + one) are not a majority of
// five; a tally that counted vote messages rather than voters would crown it.
func TestDuplicatedVoteDoesNotCountTwice(t *testing.T) {
	s := newSim(t, 5, 7)
	s.campaign("n1")
	s.dropAll(func(m raft.Message) bool { return m.Type == raft.MsgVoteRequest && m.To != "n2" })
	s.do(Event{Kind: Deliver, From: "n1", To: "n2", Pos: 0}) // n2 grants
	if s.inFlight(func(m raft.Message) bool { return m.From == "n2" && m.VoteGranted }) != 1 {
		t.Fatal("n2 did not grant")
	}
	for i := 0; i < 5; i++ {
		s.do(Event{Kind: Duplicate, From: "n2", To: "n1", Pos: 0})
	}
	s.deliverFrom("n2")
	if st := s.State("n1"); st.Role == raft.Leader {
		t.Fatalf("six copies of one vote elected n1 in a 5-node group: %+v", st)
	}
	if s.Stats().Duplicated != 5 {
		t.Fatalf("duplicated %d, want 5", s.Stats().Duplicated)
	}
}

// deliverFrom delivers, oldest first, every in-flight message sent by from.
func (s *sim) deliverFrom(from NodeID) {
	for {
		var to NodeID
		for _, m := range s.InFlight() {
			if m.From == from {
				to = m.To
				break
			}
		}
		if to == "" {
			return
		}
		s.do(Event{Kind: Deliver, From: from, To: to, Pos: 0})
	}
}

// dupAllInFlight duplicates every message currently in flight, times times.
// Copies are appended behind everything else, so the originals keep their link
// positions while the copies are also reordered.
func (s *sim) dupAllInFlight(times int) {
	type addr struct {
		from, to NodeID
		pos      int
	}
	var as []addr
	pos := map[[2]NodeID]int{}
	for _, m := range s.InFlight() {
		k := [2]NodeID{m.From, m.To}
		as = append(as, addr{m.From, m.To, pos[k]})
		pos[k]++
	}
	for _, a := range as {
		for i := 0; i < times; i++ {
			s.do(Event{Kind: Duplicate, From: a.from, To: a.to, Pos: a.pos})
		}
	}
}

// TestDuplicatedReplicationTraffic duplicates every AppendEntries and response
// several times: logs stay identical, each command appears exactly once, and the
// commit index is exactly what a quorum really holds.
func TestDuplicatedReplicationTraffic(t *testing.T) {
	s := newSim(t, 3, 8)
	s.electLeader("n1")
	for r := 0; r < 4; r++ {
		idx := s.propose("n1", fmt.Sprintf("c%d", r))
		for round := 0; round < 3; round++ {
			s.dupAllInFlight(2)
			s.DeliverAll()
			s.tick("n1", raft.DefaultHeartbeatTicks)
		}
		s.DeliverAll()
		if s.State("n1").Commit < idx {
			t.Fatalf("c%d did not commit under duplication", r)
		}
	}
	s.heartbeat("n1")
	for _, id := range []NodeID{"n2", "n3"} {
		s.requireSameLog("n1", id)
	}
	if s.Stats().Duplicated == 0 {
		t.Fatal("no duplicates were delivered")
	}
}

// --- F. reordered and stale messages ---

// TestStaleSuccessDoesNotRegressReplication: an older AppendEntries success arrives
// after a newer one. The leader must not move that follower's progress backward —
// observable as the prevLogIndex of the next AppendEntries it sends there.
func TestStaleSuccessDoesNotRegressReplication(t *testing.T) {
	s := newSim(t, 3, 9)
	s.electLeader("n1")
	s.dropAll(func(m raft.Message) bool { return m.To == "n3" || m.From == "n3" }) // keep n3 out of it
	s.do(Event{Kind: Isolate, Node: "n3"})

	s.propose("n1", "a") // index 2
	s.do(Event{Kind: Deliver, From: "n1", To: "n2", Pos: 0})
	// n2's success for index 2 is now in flight; hold it back.
	s.propose("n1", "b") // index 3
	s.do(Event{Kind: Deliver, From: "n1", To: "n2", Pos: 0})
	// Two successes from n2 in flight: match=2 (older) then match=3 (newer).
	// Deliver the NEWER first, then the stale one.
	s.do(Event{Kind: Deliver, From: "n2", To: "n1", Pos: 1})
	s.do(Event{Kind: Deliver, From: "n2", To: "n1", Pos: 0})
	s.dropAll(func(m raft.Message) bool { return m.To == "n2" })
	s.tick("n1", raft.DefaultHeartbeatTicks)

	var next *raft.Message
	for _, m := range s.InFlight() {
		if m.To == "n2" && m.Type == raft.MsgAppendRequest {
			mm := m
			next = &mm
		}
	}
	if next == nil {
		t.Fatal("leader sent no heartbeat to n2")
	}
	if next.PrevLogIndex != 3 {
		t.Fatalf("after a stale ack the leader probes n2 at prevLogIndex %d, want 3 (progress regressed)", next.PrevLogIndex)
	}
}

// TestDelayedOldTermAppendIsInert: an AppendEntries from a deposed leader is
// delayed until after a new leader has committed. When it finally arrives it must
// change nothing (INV-R10, checked by the harness at delivery) — in particular it
// must not truncate the follower's newer log.
func TestDelayedOldTermAppendIsInert(t *testing.T) {
	s := newSim(t, 3, 10)
	s.electLeader("n1")
	s.propose("n1", "old")
	// Hold n1's AppendEntries to n3 for a long time.
	s.do(Event{Kind: Delay, From: "n1", To: "n3", Pos: 0, N: 10000})
	s.do(Event{Kind: Isolate, Node: "n1"})
	s.electLeader("n2")
	s.commit("n2", "new", "n2", "n3")
	before := s.State("n3")

	s.do(Event{Kind: HealAll}, Event{Kind: Release})
	if s.inFlight(func(m raft.Message) bool { return m.From == "n1" && m.To == "n3" && m.Term == 1 }) != 1 {
		t.Fatal("the delayed term-1 AppendEntries is not in flight; the delay was not honoured")
	}
	delivered := s.Stats().Delivered
	s.do(Event{Kind: Deliver, From: "n1", To: "n3", Pos: 0})
	if s.Stats().Delivered != delivered+1 {
		t.Fatal("the delayed message was not delivered; the test would prove nothing")
	}
	after := s.State("n3")
	if after.Term != before.Term || after.LastIndex != before.LastIndex || after.Commit != before.Commit {
		t.Fatalf("stale AppendEntries changed n3: before %+v after %+v", before, after)
	}
	if !s.logHas("n3", "new") {
		t.Fatal("the stale AppendEntries truncated n3's newer entry")
	}
}

// --- G. persistent-storage failures ---

// TestVoteNotSentWhenItCannotBePersisted: n2's fsync fails exactly when it must
// persist a vote. The vote reply must not leave (INV-R6/INV-F1), n2 fail-stops,
// and on restart it recovers the (now fsynced, see raftlog.Open) vote rather than
// forgetting it.
func TestVoteNotSentWhenItCannotBePersisted(t *testing.T) {
	s := newSim(t, 3, 11)
	s.do(Event{Kind: FailPersist, Node: "n2", Op: FailSync})
	s.campaign("n1")
	s.do(Event{Kind: Deliver, From: "n1", To: "n2", Pos: 0})
	if s.Up("n2") {
		t.Fatal("n2 kept running after its durable log failed")
	}
	if s.inFlight(func(m raft.Message) bool { return m.From == "n2" }) != 0 {
		t.Fatal("n2 sent a message although its vote could not be persisted")
	}
	s.DeliverAll() // n3 grants: n1 wins with n1+n3
	if s.State("n1").Role != raft.Leader {
		t.Fatal("n1 should win with n3's vote")
	}
	s.do(Event{Kind: Restart, Node: "n2"})
	if st := s.State("n2"); st.Term != 1 || st.Vote != "n1" {
		t.Fatalf("n2 recovered %+v; the cached vote should be recovered (and is now durable)", st)
	}
}

// TestNoAckOfUnsyncedEntriesAfterFailedFsync is the regression for the bug this
// simulator found. A follower appends x, its fsync fails, it fail-stops without
// acknowledging. Its page cache still holds x, so on restart it recovers x. When
// the leader retransmits, the follower already has x — nothing new to persist —
// and acknowledges it, and the leader commits x. If raftlog.Open had not fsynced
// what it recovered, that acknowledgement would vouch for bytes a power loss could
// still erase: the leader dies, the follower loses power, and a leader without the
// committed x is elected (INV-R4) — or, earlier, the send-time check catches the
// ack leaving with un-fsynced bytes (INV-R6).
func TestNoAckOfUnsyncedEntriesAfterFailedFsync(t *testing.T) {
	s := newSim(t, 3, 12)
	s.electLeader("n1")
	s.commit("n1", "base", "n1", "n2", "n3")
	s.do(Event{Kind: Isolate, Node: "n3"})
	s.do(Event{Kind: FailPersist, Node: "n2", Op: FailSync})
	x := s.propose("n1", "x")
	s.deliverFrom("n1") // n2 appends x; the fsync fails; n2 fail-stops without acking
	if s.Up("n2") || s.State("n1").Commit >= x {
		t.Fatalf("setup: n2 up=%v, n1 commit=%d", s.Up("n2"), s.State("n1").Commit)
	}
	s.do(Event{Kind: Restart, Node: "n2"})
	if !s.logHas("n2", "x") {
		t.Fatal("setup: n2 should recover x from its page cache")
	}
	s.heartbeat("n1") // retransmit; n2 acks x without a new Save
	if s.State("n1").Commit < x {
		t.Fatal("setup: x should commit on n1+n2")
	}
	s.do(Event{Kind: Crash, Node: "n1"})
	s.do(Event{Kind: Crash, Node: "n2", Power: true})
	s.do(Event{Kind: Restart, Node: "n2"})
	s.do(Event{Kind: HealAll})
	leader := s.anyLeaderAmong([]NodeID{"n2", "n3"})
	if !s.logHas(leader, "x") {
		t.Fatalf("new leader %s lacks committed entry x", leader)
	}
}

// TestAckedEntrySurvivesPowerLoss: a follower acknowledges an entry, which lets
// the leader commit it; then the leader dies and the follower loses power. The
// follower's fsync (INV-R6) is what keeps the committed entry alive: it must still
// have it, win the election, and keep it committed. Without the fsync in
// raftlog.Save the entry is lost and the harness reports INV-R4/INV-R6.
func TestAckedEntrySurvivesPowerLoss(t *testing.T) {
	s := newSim(t, 3, 13)
	s.electLeader("n1")
	s.commit("n1", "base", "n1", "n2", "n3")
	s.do(Event{Kind: Isolate, Node: "n3"})
	x := s.propose("n1", "x")
	s.DeliverAll()
	s.heartbeat("n1")
	if s.State("n1").Commit < x {
		t.Fatal("x did not commit on n1+n2")
	}
	s.do(Event{Kind: Crash, Node: "n1"})
	s.do(Event{Kind: Crash, Node: "n2", Power: true})
	s.do(Event{Kind: Restart, Node: "n2"})
	if !s.logHas("n2", "x") {
		t.Fatal("n2 lost an entry it had acknowledged (and the leader had committed) to a power loss")
	}
	s.do(Event{Kind: HealAll})
	s.electLeader("n2")
	s.commit("n2", "after", "n2", "n3")
	if !s.logHas("n3", "x") {
		t.Fatal("committed entry x did not survive")
	}
}

// TestTornWriteIsTruncatedOnRestart: a leader's append is torn mid-record (a short
// write); it fail-stops before sending that entry anywhere; on restart the torn
// record is gone and the log is otherwise intact.
func TestTornWriteIsTruncatedOnRestart(t *testing.T) {
	s := newSim(t, 3, 14)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	intact := s.State("n1").LastIndex

	s.do(Event{Kind: FailPersist, Node: "n1", Op: ShortWrite, N: 5})
	s.Apply(Event{Kind: Propose, Node: "n1", Data: "torn"})
	if s.Up("n1") {
		t.Fatal("leader kept running after a torn write")
	}
	if s.inFlight(func(m raft.Message) bool {
		return m.From == "n1" && m.Type == raft.MsgAppendRequest && len(m.Entries) > 0
	}) != 0 {
		t.Fatal("the entry whose write tore was replicated anyway")
	}
	s.do(Event{Kind: Restart, Node: "n1"})
	if st := s.State("n1"); st.LastIndex != intact || s.logHas("n1", "torn") {
		t.Fatalf("restart did not truncate the torn record: %+v", st)
	}
}

// TestDiskFullOnLeaderStopsItAndClusterMovesOn: the leader's disk is full when it
// appends a proposal. It must stop without replicating that entry; the others
// elect a new leader; the old one rejoins after restart.
func TestDiskFullOnLeaderStopsItAndClusterMovesOn(t *testing.T) {
	s := newSim(t, 3, 15)
	s.electLeader("n1")
	s.do(Event{Kind: FailPersist, Node: "n1", Op: FailWrite})
	s.Apply(Event{Kind: Propose, Node: "n1", Data: "nospace"})
	if s.Up("n1") {
		t.Fatal("leader kept running with a full disk")
	}
	s.DeliverAll()
	s.electLeader("n2")
	s.commit("n2", "ok", "n2", "n3")
	s.do(Event{Kind: Restart, Node: "n1"})
	s.heartbeat("n2")
	s.heartbeat("n2")
	s.requireSameLog("n2", "n1")
	for _, id := range s.IDs() {
		if s.logHas(id, "nospace") {
			t.Fatalf("%s holds the entry that could never be persisted", id)
		}
	}
}

// --- H. repeated crash / restart ---

// TestRepeatedCrashRestart crashes every node in turn — alternating process death
// and power loss — making progress between crashes, then requires convergence and
// that every command committed along the way is still committed exactly once.
func TestRepeatedCrashRestart(t *testing.T) {
	s := newSim(t, 3, 16)
	var committed []string
	for round := 0; round < 12; round++ {
		victim := s.IDs()[round%3]
		s.do(Event{Kind: Crash, Node: victim, Power: round%2 == 1, N: round * 7})
		leader := s.anyLeaderAmong(without(s.IDs(), victim))
		cmd := fmt.Sprintf("r%d", round)
		s.commit(leader, cmd, without(s.IDs(), victim)...)
		committed = append(committed, cmd)
		s.do(Event{Kind: Restart, Node: victim})
	}
	s.Stabilize(400)
	for _, id := range s.IDs() {
		for _, cmd := range committed {
			n := 0
			for _, e := range s.State(id).Log {
				if string(e.Data) == cmd {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("%s holds committed command %s %d times, want 1", id, cmd, n)
			}
		}
	}
}

// anyLeaderAmong ticks the given nodes (round-robin) and delivers until one of
// them leads, and returns it.
func (s *sim) anyLeaderAmong(ids []NodeID) NodeID {
	s.t.Helper()
	for i := 0; i < 400; i++ {
		for _, id := range ids {
			if s.State(id).Role == raft.Leader {
				return id
			}
		}
		s.tick(ids[i%len(ids)], 1)
		s.DeliverAll()
	}
	s.t.Fatalf("no leader among %v", ids)
	return ""
}

func without(ids []NodeID, x NodeID) []NodeID {
	var out []NodeID
	for _, id := range ids {
		if id != x {
			out = append(out, id)
		}
	}
	return out
}

// --- I. partition + restart ---

// TestPartitionedNodeRestartsWhileIsolated: a node is isolated, crashes, restarts
// while still isolated (campaigning uselessly and inflating its term), then the
// partition heals. Its higher term disrupts the leader, but it cannot win with a
// stale log (§5.4.1), a node with the committed entries does, and everything
// committed survives.
func TestPartitionedNodeRestartsWhileIsolated(t *testing.T) {
	s := newSim(t, 3, 17)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Isolate, Node: "n3"}, Event{Kind: Crash, Node: "n3"})
	b := s.commit("n1", "b", "n1", "n2")
	s.do(Event{Kind: Restart, Node: "n3"})
	s.tick("n3", 6*raft.DefaultElectionTicks) // several useless elections
	s.DeliverAll()
	if s.State("n3").Term <= s.State("n1").Term {
		t.Fatalf("isolated node's term did not inflate: %d vs %d", s.State("n3").Term, s.State("n1").Term)
	}
	s.do(Event{Kind: HealAll})
	s.Stabilize(400) // ends with CheckConverged (INV-F3)
	for _, id := range s.IDs() {
		if !s.logHas(id, "b") || s.State(id).Commit < b {
			t.Fatalf("%s lost committed entry b", id)
		}
	}
}

// --- a frozen (paused) leader ---

// TestPausedLeaderStepsDownOnResume: the leader freezes (a GC pause, SIGSTOP); the
// others elect a new leader; when it thaws it still believes it leads its old term
// until the first exchange, then steps down — never two leaders in one term.
func TestPausedLeaderStepsDownOnResume(t *testing.T) {
	s := newSim(t, 3, 18)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Pause, Node: "n1"})
	leader := s.anyLeaderAmong([]NodeID{"n2", "n3"})
	s.commit(leader, "b", "n2", "n3")
	s.do(Event{Kind: Resume, Node: "n1"})
	if st := s.State("n1"); st.Role != raft.Leader || st.Term != 1 {
		t.Fatalf("a thawed leader should not yet know it was deposed: %+v", st)
	}
	s.heartbeat("n1") // its stale heartbeats are rejected with the new term
	if st := s.State("n1"); st.Role == raft.Leader {
		t.Fatalf("thawed leader did not step down: %+v", st)
	}
	s.heartbeat(leader)
	s.heartbeat(leader)
	s.requireSameLog(leader, "n1")
}
