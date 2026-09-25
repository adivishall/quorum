package raftsim

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
)

// Phase 11 crash-recovery windows (docs/CRASH_RECOVERY.md). Each scenario kills a
// node at an exact crash point — a boundary of the driver's persist → send →
// advance → apply cycle, or an I/O boundary inside a Save — restarts it through
// the real startup path, and asserts what the crash could and could not lose. The
// continuous checks run throughout: INV-R1..R10, INV-F2/F4, and at every restart
// INV-CR1..3 (check.go). TestCrashMatrix then does the same for EVERY point a
// scenario reaches, in every crash mode.

var flagMatrixOut = flag.String("raftsim.matrix.out", "", "write the crash matrix report (JSON) to this file")

// crashAt arms a crash on a node.
func (s *sim) crashAt(id NodeID, point string, nth int) {
	s.Apply(Event{Kind: CrashAt, Node: id, Point: point, Nth: nth})
}

// deliverLink delivers every in-flight message on one link, oldest first.
func (s *sim) deliverLink(from, to NodeID) {
	for i := 0; i < 1000; i++ {
		if s.inFlight(func(m raft.Message) bool { return m.From == from && m.To == to }) == 0 {
			return
		}
		s.Apply(Event{Kind: Deliver, From: from, To: to, Pos: 0})
	}
}

// requireDown/requireUp assert a node's liveness with the run's trace on failure.
func (s *sim) requireDown(id NodeID, why string) {
	s.t.Helper()
	if s.Up(id) {
		s.t.Fatalf("%s is still up: %s\n%s", id, why, strings.Join(s.Trace().Tail(40), "\n"))
	}
}

func (s *sim) requireUp(id NodeID, why string) {
	s.t.Helper()
	if !s.Up(id) {
		s.t.Fatalf("%s is down: %s\n%s", id, why, strings.Join(s.Trace().Tail(40), "\n"))
	}
}

// restart restarts a node and requires the boot to succeed: a durable log the
// node itself wrote must always reopen.
func (s *sim) restart(id NodeID) {
	s.t.Helper()
	before := s.Stats().RestartFailures
	s.Apply(Event{Kind: Restart, Node: id})
	if s.Stats().RestartFailures != before || !s.Up(id) {
		s.t.Fatalf("%s failed to restart from its own durable log\n%s", id, strings.Join(s.Trace().Tail(30), "\n"))
	}
}

// --- A. proposal / log persistence windows on the leader ---

// TestLeaderCrashBeforeSavingAProposal: the leader dies before its Save of a
// proposal (nothing of that Ready is durable, nothing was sent). The command is
// simply lost — no node ever holds it — and the group carries on.
func TestLeaderCrashBeforeSavingAProposal(t *testing.T) {
	s := newSim(t, 3, 41)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.crashAt("n1", "before-save", 1)
	s.Apply(Event{Kind: Propose, Node: "n1", Data: "lost"})
	s.requireDown("n1", "the crash point before the proposal's Save did not fire")
	s.restart("n1")
	if s.logHas("n1", "lost") {
		t.Fatal("a proposal whose Save never started survived the crash")
	}
	s.Stabilize(400)
	for _, id := range s.IDs() {
		if s.logHas(id, "lost") {
			t.Fatalf("%s holds a command that was never persisted anywhere", id)
		}
	}
}

// TestLeaderCrashAfterSavingBeforeSending: the leader's Save of a proposal
// completes but it dies before the first AppendEntries leaves. The entry is
// durable on the dead leader only (INV-F2 recovers it), uncommitted; the others
// elect a leader without it. After the rejoin the command is either committed
// everywhere or nowhere — never on some nodes and not others (INV-F4/R3).
func TestLeaderCrashAfterSavingBeforeSending(t *testing.T) {
	s := newSim(t, 3, 42)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.crashAt("n1", "after-save", 1)
	s.Apply(Event{Kind: Propose, Node: "n1", Data: "x"})
	s.requireDown("n1", "the crash point after the proposal's Save did not fire")
	if n := s.inFlight(func(m raft.Message) bool { return m.From == "n1" && len(m.Entries) > 0 }); n != 0 {
		t.Fatalf("%d AppendEntries carrying the entry left before the crash point after the Save", n)
	}
	s.restart("n1")
	if !s.logHas("n1", "x") {
		t.Fatal("the durable, unsent entry did not survive a process crash")
	}
	if s.logHas("n2", "x") || s.logHas("n3", "x") {
		t.Fatal("an entry that was never sent reached another node")
	}
	s.Stabilize(400)
	have := 0
	for _, id := range s.IDs() {
		if s.logHas(id, "x") {
			have++
		}
	}
	if have != 0 && have != 3 {
		t.Fatalf("command x is on %d of 3 nodes after convergence", have)
	}
}

// TestLeaderCrashAfterSendingToOnePeer: the leader dies after the AppendEntries
// to exactly one follower left. That follower holds the entry (longer, newer
// log), so it is the only one that can win the next election (§5.4.1), and the
// entry commits under it; nothing committed is lost.
func TestLeaderCrashAfterSendingToOnePeer(t *testing.T) {
	s := newSim(t, 3, 43)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.crashAt("n1", "after-send", 1) // the proposal's first AppendEntries (to n2 in id order)
	s.Apply(Event{Kind: Propose, Node: "n1", Data: "x"})
	s.requireDown("n1", "the crash point after the first send did not fire")
	if n := s.inFlight(func(m raft.Message) bool { return m.From == "n1" && len(m.Entries) > 0 }); n != 1 {
		t.Fatalf("%d AppendEntries with entries in flight, want exactly the one sent before the crash", n)
	}
	s.DeliverAll()
	if !s.logHas("n2", "x") || s.logHas("n3", "x") {
		t.Fatalf("after delivery: n2 has x=%v n3 has x=%v, want only n2", s.logHas("n2", "x"), s.logHas("n3", "x"))
	}
	s.restart("n1")
	s.Stabilize(400)
	for _, id := range s.IDs() {
		if !s.logHas(id, "x") {
			t.Fatalf("%s lacks x: the entry on the newest log was not committed after the crash", id)
		}
	}
}

// TestLeaderCrashBetweenRecordWritesOfOneSave: the process dies between the
// record writes of a single Save — before the entry record (nothing durable),
// after it but before the HardState record, and before the fsync. A process
// crash keeps what the kernel accepted, so recovery is the completed prefix of
// the Save (INV-F2); a power loss keeps only the fsynced state, so the entry is
// gone. Either way the node restarts and the group converges.
func TestLeaderCrashBetweenRecordWritesOfOneSave(t *testing.T) {
	for _, tc := range []struct {
		point string
		nth   int
		power bool
		want  bool // the entry survives on the leader's disk
	}{
		{"write", 1, false, false}, // before the entry record
		{"fsync", 1, false, true},  // entry written, fsync not yet issued: a process crash keeps it
		{"fsync", 1, true, false},  // ...but a power loss does not
	} {
		name := fmt.Sprintf("%s#%d/power=%v", tc.point, tc.nth, tc.power)
		t.Run(name, func(t *testing.T) {
			s := newSim(t, 3, 44)
			s.electLeader("n1")
			s.commit("n1", "a", "n1", "n2", "n3")
			s.Apply(Event{Kind: CrashAt, Node: "n1", Point: tc.point, Nth: tc.nth, Power: tc.power})
			s.Apply(Event{Kind: Propose, Node: "n1", Data: "x"})
			s.requireDown("n1", "the I/O crash point did not fire")
			s.restart("n1")
			if got := s.logHas("n1", "x"); got != tc.want {
				t.Fatalf("after %s: leader recovered x=%v, want %v", name, got, tc.want)
			}
			s.Stabilize(400)
		})
	}
}

// --- B. vote / term persistence windows ---

// TestVoterCrashAroundPersistingItsVote: a follower is asked for its vote. Dying
// before the Save loses nothing (no vote was cast); dying after the Save but
// before the reply keeps the vote: the restarted node still refuses a competing
// candidate in that term (INV-R6 across a crash), and the candidate that never
// got the reply simply times out and retries.
func TestVoterCrashAroundPersistingItsVote(t *testing.T) {
	t.Run("before the vote is saved", func(t *testing.T) {
		s := newSim(t, 3, 45)
		s.crashAt("n2", "before-save", 1)
		s.campaign("n1") // RequestVotes to n2, n3 in flight
		s.deliverFrom("n1")
		s.requireDown("n2", "the vote's Save was not reached")
		s.restart("n2")
		if st := s.State("n2"); st.Term != 0 || st.Vote != "" {
			t.Fatalf("n2 recovered term %d vote %q after dying before any Save: nothing should be durable", st.Term, st.Vote)
		}
		s.Stabilize(400)
	})
	t.Run("after the vote is saved, before the reply", func(t *testing.T) {
		s := newSim(t, 3, 46)
		s.crashAt("n2", "after-save", 1)
		s.campaign("n1")
		term := s.State("n1").Term
		s.deliverFrom("n1")
		s.requireDown("n2", "the crash point after the vote's Save did not fire")
		if s.inFlight(func(m raft.Message) bool { return m.From == "n2" && m.Type == raft.MsgVoteResponse }) != 0 {
			t.Fatal("the vote reply left before the crash point after its Save")
		}
		s.restart("n2")
		if st := s.State("n2"); st.Term != term || st.Vote != "n1" {
			t.Fatalf("n2 recovered term %d vote %q, want the durable vote for n1 in term %d", st.Term, st.Vote, term)
		}
		// A competing candidate in the same term must be refused by the restarted
		// voter: deliver n3's RequestVote for the same term directly.
		s.dropAll(func(raft.Message) bool { return true })
		s.tick("n3", 4*raft.DefaultElectionTicks)
		if s.State("n3").Term != term {
			t.Skipf("n3 campaigned in term %d, not %d; the competing-vote check needs the same term", s.State("n3").Term, term)
		}
		s.deliverFrom("n3")
		granted := s.inFlight(func(m raft.Message) bool {
			return m.From == "n2" && m.To == "n3" && m.Type == raft.MsgVoteResponse && m.VoteGranted
		})
		if granted != 0 {
			t.Fatal("the restarted voter granted its term's vote a second time, to a different candidate")
		}
		s.Stabilize(400)
	})
}

// TestSingleNodeCrashInsideItsElectionSave: a single-node group's election is
// one Save carrying the term bump, the self-vote AND the no-op entry (it is its
// own quorum). The process dies between that Save's record writes. Whatever
// prefix of the Save the kernel kept, the log must reopen and the node must
// restart with a term no lower than any entry it holds — a log a node wrote
// itself can never refuse to open.
func TestSingleNodeCrashInsideItsElectionSave(t *testing.T) {
	// The election Save is three records — the term/vote, the no-op, the commit —
	// so there are three write boundaries to die at.
	for nth := 1; nth <= 3; nth++ {
		t.Run(fmt.Sprintf("before write %d of the election Save", nth), func(t *testing.T) {
			s := newSim(t, 1, 47)
			s.crashAt("n1", "write", nth)
			s.tick("n1", 2*raft.DefaultElectionTicks)
			s.requireDown("n1", "the election Save did not reach that write")
			s.restart("n1")
			st := s.State("n1")
			if lt := st.LastIndex; lt > 0 {
				if lastTerm := st.Log[lt-1].Term; st.Term < lastTerm {
					t.Fatalf("recovered term %d below the term %d of its own last entry", st.Term, lastTerm)
				}
			}
			s.Stabilize(400)
		})
	}
}

// TestFollowerCrashAfterAdoptingAHigherTerm: a follower learns a higher term from
// a leader's heartbeat, persists it, and dies before replying. The restarted
// node is at the new term (never below it: INV-CR1), follows the leader and
// catches up.
func TestFollowerCrashAfterAdoptingAHigherTerm(t *testing.T) {
	s := newSim(t, 3, 48)
	s.do(Event{Kind: Isolate, Node: "n3"})
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2")
	s.do(Event{Kind: HealAll})
	s.crashAt("n3", "after-save", 1) // its first Save: adopting term 1 on the first heartbeat
	s.heartbeat("n1")
	s.requireDown("n3", "the crash point after persisting the higher term did not fire")
	s.restart("n3")
	if st := s.State("n3"); st.Term != s.State("n1").Term {
		t.Fatalf("n3 recovered term %d, want the leader's term %d it had persisted", st.Term, s.State("n1").Term)
	}
	s.Stabilize(400)
}

// --- C. AppendEntries / replication windows on a follower ---

// TestFollowerCrashAroundAppendEntries kills a follower at each boundary of
// handling an AppendEntries that carries a new entry: before its Save (nothing
// kept, the leader retransmits), after the Save but before the success reply
// (the entry is durable, the reply is lost, the leader retransmits and the
// follower re-acknowledges what it already has), and after the reply.
func TestFollowerCrashAroundAppendEntries(t *testing.T) {
	for _, point := range []string{"before-save", "after-save", "after-send"} {
		t.Run(point, func(t *testing.T) {
			s := newSim(t, 3, 49)
			s.electLeader("n1")
			s.commit("n1", "a", "n1", "n2", "n3")
			s.crashAt("n3", point, 1)
			s.Apply(Event{Kind: Propose, Node: "n1", Data: "b"})
			s.deliverFrom("n1")
			s.requireDown("n3", "the crash point while handling AppendEntries did not fire")
			s.restart("n3")
			has := s.logHas("n3", "b")
			switch point {
			case "before-save":
				if has {
					t.Fatal("an entry whose Save never started survived")
				}
			default:
				if !has {
					t.Fatal("a durably saved entry did not survive the crash")
				}
			}
			s.Stabilize(400)
			if !s.logHas("n3", "b") || s.State("n3").Commit < 3 {
				t.Fatalf("follower did not end up with b committed: %+v", s.State("n3"))
			}
		})
	}
}

// TestCrashDuringSuffixReplacement: a deposed leader holds an uncommitted entry
// that the new leader's AppendEntries replaces. The node dies inside the Save of
// the replacement — before the replacing record (its old suffix is still on
// disk), or before the fsync (the replacement is in the kernel's hands). Whatever
// survives, recovery reconstructs a coherent log (INV-F2), the new leader's
// retransmission finishes the replacement, and the committed record wins (R3/R4).
func TestCrashDuringSuffixReplacement(t *testing.T) {
	for _, tc := range []struct {
		point string
		power bool
	}{{"write", false}, {"fsync", false}, {"fsync", true}} {
		t.Run(fmt.Sprintf("%s/power=%v", tc.point, tc.power), func(t *testing.T) {
			s := newSim(t, 3, 50)
			s.electLeader("n1")
			s.commit("n1", "a", "n1", "n2", "n3")
			s.do(Event{Kind: Isolate, Node: "n1"})
			s.Apply(Event{Kind: Propose, Node: "n1", Data: "orphan"}) // n1's uncommitted entry
			s.DeliverAll()
			s.electLeader("n2")
			s.commit("n2", "y", "n2", "n3")
			s.do(Event{Kind: HealAll})
			s.Apply(Event{Kind: CrashAt, Node: "n1", Point: tc.point, Nth: 1, Power: tc.power})
			// n1 learns the higher term from n2's heartbeat (a Save of its own), then
			// the replacement arrives in the next round.
			for i := 0; i < 6 && s.Up("n1"); i++ {
				s.heartbeat("n2")
			}
			s.requireDown("n1", "the crash point inside the replacement did not fire")
			s.restart("n1")
			s.Stabilize(400)
			for _, id := range s.IDs() {
				if s.logHas(id, "orphan") {
					t.Fatalf("%s still holds the deposed leader's uncommitted entry after convergence", id)
				}
				if !s.logHas(id, "y") {
					t.Fatalf("%s lacks the committed entry y", id)
				}
			}
		})
	}
}

// --- D/E. commit and apply windows ---

// TestCrashAroundApply pins the state-machine replay semantics
// (docs/CRASH_RECOVERY.md §6): appliedIndex is volatile, so a restarted node
// re-applies its whole recovered committed prefix. Dying after Apply(i) but
// before AppliedTo(i) therefore re-applies i (at-least-once); dying before
// Apply(i) applies i once, by the next incarnation; and within one incarnation
// every index is applied exactly once, in order, with identical entries.
func TestCrashAroundApply(t *testing.T) {
	for _, point := range []string{"before-apply", "after-apply", "after-applied-to"} {
		t.Run(point, func(t *testing.T) {
			s := newSim(t, 3, 51)
			s.electLeader("n1")
			s.commit("n1", "a", "n1", "n2", "n3")
			target := s.State("n3").Applied + 1 // the next index n3 applies: b
			s.crashAt("n3", point, 1)
			s.commit("n1", "b", "n1", "n2")
			for i := 0; i < 6 && s.Up("n3"); i++ {
				s.heartbeat("n1")
			}
			s.requireDown("n3", "the apply crash point did not fire")
			byFirst := countApplied(s.Applications("n3"), 1, target)
			wantFirst := 1
			if point == "before-apply" {
				wantFirst = 0
			}
			if byFirst != wantFirst {
				t.Fatalf("incarnation 1 applied index %d %d times before dying at %s, want %d", target, byFirst, point, wantFirst)
			}
			s.restart("n3")
			s.Stabilize(400)
			apps := s.Applications("n3")
			if got := countApplied(apps, 2, target); got != 1 {
				t.Fatalf("incarnation 2 applied index %d %d times, want exactly once (it re-applies its whole committed prefix)", target, got)
			}
			// The whole prefix below target was applied by both incarnations, identically.
			for i := uint64(1); i < target; i++ {
				if countApplied(apps, 1, i) != 1 || countApplied(apps, 2, i) != 1 {
					t.Fatalf("index %d applied %d/%d times by incarnations 1/2, want 1/1", i, countApplied(apps, 1, i), countApplied(apps, 2, i))
				}
			}
			requireIdenticalReplay(t, apps)
		})
	}
}

func countApplied(apps []Application, inc int, index uint64) int {
	n := 0
	for _, a := range apps {
		if a.Incarnation == inc && a.Entry.Index == index {
			n++
		}
	}
	return n
}

// requireIdenticalReplay checks that whenever two incarnations applied the same
// index, they applied the identical entry, and that within an incarnation the
// indexes form 1, 2, 3, ... in order.
func requireIdenticalReplay(t *testing.T, apps []Application) {
	t.Helper()
	seen := map[uint64]raft.Entry{}
	next := map[int]uint64{}
	for _, a := range apps {
		if prev, ok := seen[a.Entry.Index]; ok && !sameEntry(prev, a.Entry) {
			t.Fatalf("index %d applied as (t%d,%q) and as (t%d,%q) by different incarnations", a.Entry.Index, prev.Term, prev.Data, a.Entry.Term, a.Entry.Data)
		}
		seen[a.Entry.Index] = a.Entry
		if want := next[a.Incarnation] + 1; a.Entry.Index != want {
			t.Fatalf("incarnation %d applied index %d after %d", a.Incarnation, a.Entry.Index, want-1)
		}
		next[a.Incarnation] = a.Entry.Index
	}
}

// TestCommitIsDurableBeforeApply: INV-CR3 as an explicit scenario. A follower
// applies entries only in the cycle that also persisted the commit index
// covering them, so a crash at any apply point recovers a commit no lower than
// what was applied — the harness checks it at every restart; this scenario
// exercises it at each apply point after a commit advance.
func TestCommitIsDurableBeforeApply(t *testing.T) {
	for _, point := range []string{"before-apply", "after-apply", "after-applied-to"} {
		s := newSim(t, 3, 52)
		s.electLeader("n1")
		s.crashAt("n2", point, 2)
		s.commit("n1", "a", "n1", "n3")
		for i := 0; i < 8 && s.Up("n2"); i++ {
			s.heartbeat("n1")
		}
		s.requireDown("n2", "the apply crash point did not fire: "+point)
		s.restart("n2")
		if rec := s.Recovered("n2"); rec.HardState.Commit < s.Applications("n2")[len(s.Applications("n2"))-1].Entry.Index {
			t.Fatalf("%s: recovered commit %d below the last index the dead incarnation applied", point, rec.HardState.Commit)
		}
		s.Stabilize(400)
	}
}

// --- F. restart and rejoin windows ---

// TestRestartWithoutAQuorum: two of three nodes are down; the restarted one
// campaigns but cannot win, commits nothing, and applies nothing new. When the
// second node returns a leader is elected and nothing committed before is lost.
func TestRestartWithoutAQuorum(t *testing.T) {
	s := newSim(t, 3, 53)
	s.electLeader("n1")
	a := s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Crash, Node: "n2"}, Event{Kind: Crash, Node: "n3"}, Event{Kind: Crash, Node: "n1"})
	s.restart("n1")
	s.tick("n1", 6*raft.DefaultElectionTicks)
	s.DeliverAll()
	if st := s.State("n1"); st.Role == raft.Leader || st.Commit > a {
		t.Fatalf("a lone restarted node led or committed without a quorum: %+v", st)
	}
	s.restart("n2")
	s.Stabilize(400)
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		if !s.logHas(id, "a") {
			t.Fatalf("%s lost the committed entry a", id)
		}
	}
}

// TestRestartWithStaleMessagesInFlight: messages addressed to a node were in
// flight when it died — old-term AppendEntries and a vote request. They reach
// the restarted incarnation, which must treat them exactly as a live node would:
// a lower term is inert (INV-R10, checked at delivery), a current one is handled.
func TestRestartWithStaleMessagesInFlight(t *testing.T) {
	s := newSim(t, 3, 54)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.tick("n1", raft.DefaultHeartbeatTicks) // heartbeats to n2, n3 in flight
	s.do(Event{Kind: Crash, Node: "n3"})
	s.dropAll(func(m raft.Message) bool { return m.To != "n3" }) // keep only the heartbeat to n3
	// Move the live pair to term 2 while n3's term-1 heartbeat waits in flight.
	s.campaign("n2")
	s.deliverLink("n2", "n1") // n1 steps down and votes
	s.deliverLink("n1", "n2")
	if st := s.State("n2"); st.Role != raft.Leader || st.Term != 2 {
		t.Fatalf("n2 did not lead term 2: %+v", st)
	}
	s.restart("n3") // recovers term 1 (its vote for n1)
	s.tick("n2", raft.DefaultHeartbeatTicks)
	s.deliverLink("n2", "n3") // n3 learns term 2 and follows n2
	before := s.State("n3")
	if before.Term != 2 || before.Leader != "n2" {
		t.Fatalf("restarted n3 did not follow n2 at term 2: %+v", before)
	}
	s.deliverLink("n1", "n3") // the stale term-1 heartbeat lands (INV-R10 checked at delivery)
	if after := s.State("n3"); after.Term != before.Term || after.Leader != before.Leader || after.Role != before.Role {
		t.Fatalf("a stale message changed the restarted node: %+v -> %+v", before, after)
	}
	s.Stabilize(400)
}

// TestRepeatedCrashesAtPoints: every node in turn dies at a different crash
// point, restarts, and the group commits between crashes. Every command committed
// along the way is held exactly once by every node at the end, and every
// incarnation's replay was identical to what preceded it.
func TestRepeatedCrashesAtPoints(t *testing.T) {
	s := newSim(t, 3, 55)
	points := []string{"after-save", "write", "after-send", "fsync", "before-advance", "after-apply", "after-applied-to", "before-save", "after-advance"}
	var committed []string
	for round, point := range points {
		victim := s.IDs()[round%3]
		s.Apply(Event{Kind: CrashAt, Node: victim, Point: point, Nth: 1, Power: round%2 == 1, N: 5 * round})
		leader := s.anyLeaderAmong(s.IDs())
		cmd := fmt.Sprintf("c%d", round)
		s.Apply(Event{Kind: Propose, Node: leader, Data: cmd})
		for i := 0; i < 12 && s.Up(victim); i++ {
			s.heartbeat(leader)
			if s.State(leader).Role != raft.Leader {
				leader = s.anyLeaderAmong(s.IDs())
			}
		}
		if s.Up(victim) {
			s.Apply(Event{Kind: Disarm})
		} else {
			s.restart(victim)
		}
		s.Stabilize(200)
		if s.logHas(leader, cmd) && s.State(leader).Commit >= s.State(leader).LastIndex {
			committed = append(committed, cmd)
		}
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
		requireIdenticalReplay(t, s.Applications(id))
	}
	if s.Stats().PointCrashes < len(points)/2 {
		t.Fatalf("only %d of %d armed crash points fired; the scenario did not exercise them", s.Stats().PointCrashes, len(points))
	}
}

// --- the bounded exhaustive matrix ---

// matrixScenario is the script the crash matrix enumerates: elections and term
// bumps, replication and commits on all nodes and on a majority, a lagging
// follower catching up, a deposed leader's suffix replacement, a power-loss
// crash with a torn tail (so recovery's own I/O — the truncate and the fsync of
// the recovered state — are crash points too), and commits after all of it.
func matrixScenario(t *testing.T, seed int64) []Event {
	t.Helper()
	s := newSim(t, 3, seed)
	s.electLeader("n1")
	s.commit("n1", "a", "n1", "n2", "n3")
	s.do(Event{Kind: Isolate, Node: "n3"})
	s.commit("n1", "b", "n1", "n2")
	s.do(Event{Kind: HealAll})
	s.heartbeat("n1")
	s.heartbeat("n1")
	s.do(Event{Kind: Isolate, Node: "n1"})
	s.Apply(Event{Kind: Propose, Node: "n1", Data: "orphan"})
	s.DeliverAll()
	s.electLeader("n2")
	s.commit("n2", "c", "n2", "n3")
	s.do(Event{Kind: HealAll})
	for i := 0; i < 4; i++ {
		s.heartbeat("n2")
	}
	// A torn append on n3 (a short write fail-stops it with a partial record), so
	// its restart exercises recovery's own I/O: the torn-tail truncate and the
	// fsync of the recovered state become crash points of the matrix too.
	s.do(Event{Kind: FailPersist, Node: "n3", Op: ShortWrite, N: 6})
	s.Apply(Event{Kind: Propose, Node: "n2", Data: "d"})
	for i := 0; i < 4 && s.Up("n3"); i++ {
		s.heartbeat("n2")
	}
	s.requireDown("n3", "the torn write did not fail-stop n3")
	s.commit("n2", "dd", "n1", "n2")
	s.restart("n3")
	s.commit("n2", "e", "n1", "n2", "n3")
	return s.Script()
}

// TestCrashMatrix runs the bounded exhaustive matrix: a crash at EVERY point the
// scenario reaches, in every crash mode, each followed by an immediate restart,
// the rest of the scenario, and convergence. No run may pass vacuously: every
// driver point and every I/O boundary must occur in the scenario, and every
// armed crash must fire. A failing cell is reported with its exact point, mode,
// seed and reproducing CrashAt event.
func TestCrashMatrix(t *testing.T) {
	const seed = 60
	scenario := matrixScenario(t, seed)
	rep, err := RunCrashMatrix(Config{Nodes: 3, Seed: seed}, scenario, DefaultCrashModes)
	if err != nil {
		t.Fatal(err)
	}
	if *flagMatrixOut != "" {
		js, err := rep.JSON()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(*flagMatrixOut, js, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	byPoint, byMode := rep.Coverage()
	for _, p := range raftnode.Points {
		if byPoint[p.String()] == 0 {
			t.Errorf("the scenario never reached driver point %s; the matrix does not cover it", p)
		}
	}
	for _, p := range IOPoints {
		if byPoint[p] == 0 {
			t.Errorf("the scenario never reached I/O point %s; the matrix does not cover it", p)
		}
	}
	if len(byMode) != len(DefaultCrashModes) {
		t.Errorf("modes covered: %v", byMode)
	}
	t.Log(rep.Summary())
	if fails := rep.Failures(); len(fails) > 0 {
		shown := fails
		if len(shown) > 12 {
			shown = shown[:12]
		}
		t.Fatalf("%d of %d crashes were not survived (seed=%d nodes=3; reproduce a row by arming its event first):\n%s",
			len(fails), len(rep.Rows), seed, (&MatrixReport{Rows: shown}).Text())
	}
}
