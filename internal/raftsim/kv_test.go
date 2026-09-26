package raftsim

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raft"
)

// Phase 12's deterministic client tier (kv.go, docs/LINEARIZABILITY.md §8):
// seeded client workloads under every fault family, checked for
// linearizability by lincheck on top of every Raft invariant and the
// completion invariants INV-X5..X8; exact message-level attacks on ReadIndex
// and on write completion; and proof that the tier can see a stale read at all.
//
//	go test ./internal/raftsim -run TestKVSeededHistoriesAreLinearizable -raftsim.seeds=200
//	go test ./internal/raftsim -run 'TestKVSeededHistoriesAreLinearizable/kv-mixed/seed=17$' -raftsim.profile=kv-mixed -raftsim.seed=17 -v

func selectedKVProfiles(t *testing.T) []Profile {
	if *flagProfile == "" {
		return KVProfiles
	}
	p, ok := KVProfileByName(*flagProfile)
	if !ok {
		t.Skipf("-raftsim.profile=%s is not a client profile", *flagProfile)
	}
	return []Profile{p}
}

func kvReproCommand(p Profile, seed int64) string {
	return fmt.Sprintf("go test ./internal/raftsim -run 'TestKVSeededHistoriesAreLinearizable/%s/seed=%d$' -raftsim.profile=%s -raftsim.seed=%d -raftsim.steps=%d -count=1 -v",
		p.Name, seed, p.Name, seed, p.Steps)
}

// linearizable checks a history; unchecked (budget) is a failure too.
func linearizable(h lincheck.History) (bool, lincheck.Result) {
	r := lincheck.Check(h, lincheck.Options{Minimize: true})
	return r.OK && !r.Unchecked, r
}

// TestKVSeededHistoriesAreLinearizable runs every client profile over the seed
// set. Each run must hold every Raft invariant and INV-X5..X8, its recorded
// client history must be linearizable, and it must not be vacuous: writes and
// reads were acknowledged, and the profile's faults happened. A failure prints
// the replay command, a minimized script that still produces a
// non-linearizable history, and the minimized counterexample.
func TestKVSeededHistoriesAreLinearizable(t *testing.T) {
	for _, p := range selectedKVProfiles(t) {
		if *flagSteps > 0 {
			p.Steps = *flagSteps
		}
		var tally Stats
		var kvTally KVStats
		var ops, checked int
		for _, seed := range selectedSeeds() {
			p, seed := p, seed
			t.Run(fmt.Sprintf("%s/seed=%d", p.Name, seed), func(t *testing.T) {
				r := RunKV(p, seed)
				cfg := Config{Nodes: p.Nodes, Seed: seed, KVLimits: p.KVLimits}
				if r.Violation != nil {
					min := Minimize(cfg, r.Script, 300)
					t.Fatalf("%s--- minimized script (%d of %d events) ---\n%s\n--- history ---\n%s",
						r.Report(kvReproCommand(p, seed)), len(min), len(r.Script), FormatScript(min), r.History)
				}
				if err := r.History.Validate(); err != nil {
					t.Fatalf("malformed history: %v", err)
				}
				ok, lr := linearizable(r.History)
				if !ok {
					min := MinimizeFunc(cfg, r.Script, 400, func(s []Event) bool {
						rr := Replay(cfg, s)
						good, _ := linearizable(rr.History)
						return rr.Violation == nil && !good
					})
					_, mr := linearizable(Replay(cfg, min).History)
					t.Fatalf("NOT LINEARIZABLE: %s\n%s\n--- counterexample ---\n%s--- reproduce ---\n%s\n--- minimized script (%d of %d events) ---\n%s\n--- minimized counterexample ---\n%s",
						lr.Reason, r.Report(kvReproCommand(p, seed)), lincheck.Format(lr.Counterexample), kvReproCommand(p, seed), len(min), len(r.Script), FormatScript(min), lincheck.Format(mr.Counterexample))
				}
				if r.KV.WritesAcked == 0 || r.KV.ReadsServed == 0 {
					t.Fatalf("vacuous run: %+v", r.KV)
				}
				tally = addStats(tally, r.Stats)
				kvTally = addKV(kvTally, r.KV)
				ops += len(r.History.Ops)
				checked += lr.Stats.States
				if testing.Verbose() {
					t.Logf("events=%d ops=%d %+v checker=%d states %s", len(r.Script), len(r.History.Ops), r.KV, lr.Stats.States, lr.Stats.Duration)
				}
			})
		}
		if len(selectedSeeds()) >= 5 {
			requireEveryFaultOccurred(t, p, tally)
			// The client-visible outcomes the profile's faults must produce.
			// An anonymous client's unanswered write ends Incomplete; a session
			// client's is retried until it is answered — and some of those
			// answers must be duplicates of an unanswered send that executed.
			faulty := p.Crash > 0 || p.CrashAt > 0 || p.KVTimeout > 0
			if faulty && !p.KVSessions && kvTally.Incomplete == 0 {
				t.Fatalf("profile %s: no operation ever ended incomplete: %+v", p.Name, kvTally)
			}
			if faulty && p.KVSessions && (kvTally.Resolved == 0 || kvTally.ResolvedDup == 0) {
				t.Fatalf("profile %s: no unanswered session write was ever resolved by a deduplicated retry: %+v", p.Name, kvTally)
			}
			if kvTally.Redirects == 0 && p.Name != "kv-steady" {
				t.Fatalf("profile %s: no request ever met a non-leader: %+v", p.Name, kvTally)
			}
			// Session profiles must actually retry, duplicate and deduplicate.
			if p.KVSessions && (kvTally.Registered == 0 || kvTally.Retries == 0 || kvTally.DupSends == 0 || kvTally.Duplicates == 0) {
				t.Fatalf("profile %s: sessions were not exercised: %+v", p.Name, kvTally)
			}
			if p.KVLimits.MaxSessions > 0 && kvTally.Expired == 0 {
				t.Fatalf("profile %s: no retry ever met an evicted session: %+v", p.Name, kvTally)
			}
		}
		t.Logf("%s: %d ops over %d seeds %+v; %d checker states", p.Name, ops, len(selectedSeeds()), kvTally, checked)
	}
}

func addKV(a, b KVStats) KVStats {
	a.Ops += b.Ops
	a.OK += b.OK
	a.NotFound += b.NotFound
	a.Rejected += b.Rejected
	a.Incomplete += b.Incomplete
	a.Redirects += b.Redirects
	a.Lost += b.Lost
	a.Unavailable += b.Unavailable
	a.Stalled += b.Stalled
	a.ReadsServed += b.ReadsServed
	a.WritesAcked += b.WritesAcked
	a.Registered += b.Registered
	a.Retries += b.Retries
	a.DupSends += b.DupSends
	a.Duplicates += b.Duplicates
	a.Expired += b.Expired
	a.Resolved += b.Resolved
	a.ResolvedDup += b.ResolvedDup
	return a
}

// TestKVSameSeedSameHistory: a client run is a pure function of (profile,
// seed): the same trace and the byte-identical history twice, and replaying
// the recorded script — also through its text form — reproduces both.
func TestKVSameSeedSameHistory(t *testing.T) {
	for _, p := range KVProfiles {
		a, b := RunKV(p, 11), RunKV(p, 11)
		if a.TraceHash != b.TraceHash || a.History.String() != b.History.String() {
			t.Fatalf("%s: same seed, different runs", p.Name)
		}
		cfg := Config{Nodes: p.Nodes, Seed: 11, KVLimits: p.KVLimits}
		parsed, err := ParseScript(FormatScript(a.Script))
		if err != nil {
			t.Fatalf("%s: script does not parse back: %v", p.Name, err)
		}
		re := Replay(cfg, parsed)
		if re.TraceHash != a.TraceHash || re.History.String() != a.History.String() {
			t.Fatalf("%s: replaying the text script diverged (trace %s vs %s)", p.Name, re.TraceHash, a.TraceHash)
		}
		if c := RunKV(p, 12); c.History.String() == a.History.String() {
			t.Fatalf("%s: seeds 11 and 12 produced the same history", p.Name)
		}
	}
}

// TestKVTierCatchesStaleLocalReads proves the tier has teeth: with the read path
// deliberately broken (a Get served from the contacted node's store, no
// ReadIndex), seeded runs under partitions produce histories the checker
// rejects, and the failure minimizes to a short replayable script. If this ever
// stops failing, the tier has stopped being able to see stale reads.
func TestKVTierCatchesStaleLocalReads(t *testing.T) {
	p, _ := KVProfileByName("kv-partitions")
	p.Steps = 1500
	caught := 0
	for seed := int64(1); seed <= 12; seed++ {
		c, err := New(Config{Nodes: p.Nodes, Seed: seed})
		if err != nil {
			t.Fatal(err)
		}
		c.UnsafeLocalReads()
		rng := newRand(seed)
		for i := 0; i < p.Steps && c.Violation() == nil; i++ {
			c.Apply(c.generate(rng, p))
		}
		if c.Violation() != nil {
			t.Fatalf("seed %d: %v", seed, c.Violation())
		}
		if ok, r := linearizable(c.History()); !ok {
			caught++
			if caught == 1 {
				cfg := Config{Nodes: p.Nodes, Seed: seed}
				replay := func(s []Event) lincheck.History {
					rc, _ := New(cfg)
					rc.UnsafeLocalReads()
					for _, e := range s {
						rc.Apply(e)
					}
					return rc.History()
				}
				min := MinimizeFunc(cfg, c.Script(), 600, func(s []Event) bool {
					good, _ := linearizable(replay(s))
					return !good
				})
				if good, _ := linearizable(replay(min)); good {
					t.Fatal("the minimized script no longer reproduces the stale read")
				}
				t.Logf("seed %d: stale local read caught: %s\nminimized to %d of %d events:\n%s", seed, r.Reason, len(min), len(c.Script()), FormatScript(min))
			}
		}
	}
	if caught == 0 {
		t.Fatal("no run with local reads was caught: the tier cannot see stale reads")
	}
	t.Logf("%d of 12 runs with local reads were rejected", caught)
}

func newRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

// --- exact attacks ---

// kvSim adds client helpers to the scenario harness.
type kvSim struct{ *sim }

func newKVSim(t *testing.T, nodes int) *kvSim { return &kvSim{newSim(t, nodes, 1)} }

func (s *kvSim) put(node NodeID, client, key, value string) {
	s.Apply(Event{Kind: KVPut, Node: node, Client: client, Key: key, Data: value})
}
func (s *kvSim) get(node NodeID, client, key string) {
	s.Apply(Event{Kind: KVGet, Node: node, Client: client, Key: key})
}

// deliverOne delivers the oldest in-flight message on one link.
func (s *kvSim) deliverOne(from, to NodeID) {
	s.t.Helper()
	before := s.Stats().Delivered + s.Stats().DroppedPartition
	s.Apply(Event{Kind: Deliver, From: from, To: to, Pos: 0})
	if s.Stats().Delivered+s.Stats().DroppedPartition == before {
		s.t.Fatalf("no message in flight %s -> %s", from, to)
	}
}

// deliverWhere delivers, oldest first, every in-flight message matching match.
func (s *kvSim) deliverWhere(match func(raft.Message) bool) {
	for {
		pos := map[[2]NodeID]int{}
		found := false
		for _, m := range s.InFlight() {
			k := [2]NodeID{m.From, m.To}
			if match(m) {
				s.Apply(Event{Kind: Deliver, From: m.From, To: m.To, Pos: pos[k]})
				found = true
				break
			}
			pos[k]++
		}
		if !found {
			return
		}
	}
}

func link(from, to NodeID) func(raft.Message) bool {
	return func(m raft.Message) bool { return m.From == from && m.To == to }
}

// last returns the client's most recent operation.
func (s *kvSim) last(client string) lincheck.Op {
	s.t.Helper()
	var op lincheck.Op
	for _, o := range s.History().Ops {
		if o.Client == client && o.ID > op.ID {
			op = o
		}
	}
	if op.ID == 0 {
		s.t.Fatalf("client %s has no operation", client)
	}
	return op
}

func (s *kvSim) requireLinearizable() {
	s.t.Helper()
	h := s.History()
	if ok, r := linearizable(h); !ok {
		s.t.Fatalf("history not linearizable: %s\n%s\n--- script ---\n%s", r.Reason, h, FormatScript(s.Script()))
	}
}

// TestKVStaleLeaderReadIsNeverServed is the stale-leader attack: n1 leads term 1
// and completes put(A); it is cut off from the others (its messages no longer
// reach them), which elect n2 and complete put(B). n1 still believes it leads.
// A read sent to n1 registers there — and must NEVER be served: n1 cannot
// collect a quorum of acknowledgements for a heartbeat it sends after the
// read, because n2 and n3 never receive it. The acknowledgements n2 and n3
// sent to n1 for its LAST heartbeat before the cut — still in flight, from
// n1's own term — are then delivered: they prove only that n1 led before the
// read, and must not confirm it (the stale-ack attack; with a confirmation
// rule of "any ack in my term" the read would be served A, after B completed).
// Finally n2's election traffic reaches n1: it steps down, and the read fails
// definitely (not leader), never with a value.
func TestKVStaleLeaderReadIsNeverServed(t *testing.T) {
	s := newKVSim(t, 3)
	s.electLeader("n1")
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	if op := s.last("c1"); op.Outcome != lincheck.OK {
		t.Fatalf("put(A) did not complete: %s", op)
	}
	// One more heartbeat round: n2 and n3 answer it, and those answers stay in
	// flight to n1.
	s.tick("n1", raft.DefaultHeartbeatTicks)
	s.deliverWhere(link("n1", "n2"))
	s.deliverWhere(link("n1", "n3"))
	oldAcks := s.inFlight(func(m raft.Message) bool { return m.To == "n1" && m.Type == raft.MsgAppendResponse })
	if oldAcks != 2 {
		t.Fatalf("want the two acknowledgements of n1's last heartbeat in flight, have %d", oldAcks)
	}
	// Cut n1's outbound links: nothing it sends from now on arrives.
	s.do(Event{Kind: Block, From: "n1", To: "n2"}, Event{Kind: Block, From: "n1", To: "n3"})
	// n2 and n3 elect n2 in term 2 without involving n1.
	s.campaign("n2")
	s.deliverWhere(link("n2", "n3"))
	s.deliverWhere(link("n3", "n2"))
	if st := s.State("n2"); st.Role != raft.Leader || st.Term != 2 {
		t.Fatalf("n2 should lead term 2: %+v", st)
	}
	s.put("n2", "c2", "k", "B")
	for i := 0; i < 4 && s.Busy("c2"); i++ {
		s.deliverWhere(link("n2", "n3"))
		s.deliverWhere(link("n3", "n2"))
	}
	if op := s.last("c2"); op.Outcome != lincheck.OK {
		t.Fatalf("put(B) on the new leader did not complete: %s", op)
	}
	if st := s.State("n1"); st.Role != raft.Leader || st.Term != 1 {
		t.Fatalf("premise: n1 must still believe it leads term 1: %+v", st)
	}
	// The read on the stale leader.
	s.get("n1", "c3", "k")
	if !s.Busy("c3") {
		t.Fatalf("the read on the stale leader completed at once: %s", s.last("c3"))
	}
	// The stale acknowledgements (term 1, sent before the read) arrive.
	s.deliverWhere(func(m raft.Message) bool {
		return m.To == "n1" && m.Type == raft.MsgAppendResponse && m.Term == 1
	})
	s.tick("n1", 3*raft.DefaultHeartbeatTicks) // more heartbeats: all blocked
	s.DeliverAll()                             // everything except n1's traffic, which is blocked
	if !s.Busy("c3") && s.last("c3").Outcome != lincheck.Rejected {
		t.Fatalf("the stale leader served a read: %s", s.last("c3"))
	}
	// n2's traffic reaches n1 (it is n1's OUTBOUND links that are cut): the
	// higher term deposes it and the pending read fails definitely.
	s.heartbeat("n2")
	s.DeliverAll()
	if s.Busy("c3") {
		t.Fatalf("n1 stepped down (%+v) but the read is still pending", s.State("n1"))
	}
	if op := s.last("c3"); op.Outcome != lincheck.Rejected {
		t.Fatalf("the stale leader's read must end as a definite rejection, got %s", op)
	}
	s.requireLinearizable()
	// And the same history, had n1 served its local state, is caught.
	h := s.History()
	for i, op := range h.Ops {
		if op.Client == "c3" {
			h.Ops[i].Outcome, h.Ops[i].Output, h.Ops[i].Complete = lincheck.OK, []byte("A"), h.Ops[len(h.Ops)-1].Complete+1
		}
	}
	if ok, _ := linearizable(h); ok {
		t.Fatal("a stale read of A after put(B) completed was accepted")
	}
}

// TestKVMinorityLeaderWithAFollowerNeverServesARead is the stale-leader attack
// at its sharpest: the group of five splits {n1, n2} | {n3, n4, n5} while n1
// leads. n2 never hears of the new term, so it keeps acknowledging n1's
// heartbeats — n1 gets FRESH acknowledgements, sent after the read, in its own
// term, from a live follower. It still must not serve: two of five is not a
// quorum. (A rule that counted any post-read acknowledgement, or the leader
// alone, would serve A after the majority completed B.)
func TestKVMinorityLeaderWithAFollowerNeverServesARead(t *testing.T) {
	s := newKVSim(t, 5)
	s.electLeader("n1")
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	s.heartbeat("n1")
	if op := s.last("c1"); op.Outcome != lincheck.OK {
		t.Fatalf("put(A): %s", op)
	}
	s.do(Event{Kind: Split, Data: "n1,n2|n3,n4,n5"})
	s.electLeader("n3")
	s.put("n3", "c2", "k", "B")
	s.DeliverAll()
	s.heartbeat("n3")
	if op := s.last("c2"); op.Outcome != lincheck.OK {
		t.Fatalf("put(B) on the majority: %s", op)
	}
	if st := s.State("n1"); st.Role != raft.Leader || st.Term != 1 {
		t.Fatalf("premise: n1 still believes it leads term 1: %+v", st)
	}
	s.get("n1", "c3", "k")
	acked := s.Stats().Delivered
	for i := 0; i < 5; i++ {
		s.heartbeat("n1") // n2 answers every one of these, in term 1
	}
	if s.Stats().Delivered-acked < 10 {
		t.Fatalf("premise: n1 and n2 must keep exchanging heartbeats (%d deliveries)", s.Stats().Delivered-acked)
	}
	if !s.Busy("c3") {
		t.Fatalf("the minority leader served a read with a follower's acknowledgements: %s", s.last("c3"))
	}
	s.do(Event{Kind: HealAll})
	s.heartbeat("n3")
	s.DeliverAll()
	if op := s.last("c3"); op.Outcome != lincheck.Rejected {
		t.Fatalf("after the heal the stale read must fail definitely: %s", op)
	}
	s.get("n3", "c4", "k")
	s.DeliverAll()
	s.heartbeat("n3")
	if op := s.last("c4"); op.Outcome != lincheck.OK || string(op.Output) != "B" {
		t.Fatalf("read after heal: %s", op)
	}
	s.requireLinearizable()
}

// TestKVNewLeaderReadWaitsForItsNoop is the no-op rule: n1 commits put(A) at
// index 2 with n2's acknowledgement and dies before telling anyone; n2 wins
// term 2 holding A but with commit index 1 — it does not know A committed. A
// read registered on n2 at once, and confirmed by n3's acknowledgement of n2's
// first AppendEntries (a REJECTION — n3 lacks A — so n2's commit index does
// not move), must still not be served from index 1: its read index is n2's
// no-op, and it waits for that to commit. A read index of "the commit index"
// alone would serve NotFound after put(A) completed.
func TestKVNewLeaderReadWaitsForItsNoop(t *testing.T) {
	s := newKVSim(t, 3)
	s.electLeader("n1")
	s.put("n1", "c1", "k", "A")
	// Replicate A to n2 only; n3 never hears of it.
	s.dropAll(link("n1", "n3"))
	s.deliverWhere(link("n1", "n2"))
	s.deliverWhere(link("n2", "n1"))
	if op := s.last("c1"); op.Outcome != lincheck.OK {
		t.Fatalf("put(A) did not complete with n2's ack: %s", op)
	}
	if c := s.State("n2").Commit; c != 1 {
		t.Fatalf("premise: n2 must not yet know A committed (commit %d)", c)
	}
	s.do(Event{Kind: Crash, Node: "n1"})
	s.campaign("n2")
	s.deliverWhere(link("n2", "n3"))
	s.deliverWhere(link("n3", "n2"))
	st := s.State("n2")
	if st.Role != raft.Leader || st.Commit != 1 {
		t.Fatalf("premise: n2 leads with commit 1: %+v", st)
	}
	// n2's first AppendEntries to n3 (prev = A's index, which n3 lacks) is in
	// flight. Register the read now, then let n3 reject that AppendEntries.
	s.get("n2", "c2", "k")
	s.deliverOne("n2", "n3")
	s.deliverOne("n3", "n2")
	if !s.Busy("c2") {
		op := s.last("c2")
		t.Fatalf("the read was served before n2's no-op committed: %s (commit %d)", op, s.State("n2").Commit)
	}
	s.DeliverAll()
	s.heartbeat("n2")
	op := s.last("c2")
	if op.Outcome != lincheck.OK || string(op.Output) != "A" {
		t.Fatalf("the read must see A once served: %s", op)
	}
	s.requireLinearizable()
}

// TestKVWriteIsNotAcknowledgedBeforeCommit: a put accepted by a leader that
// then loses every follower is never acknowledged — not at append, not at
// persistence — and, when a new leader overwrites it, the waiting client
// learns it was LOST (a definite no-effect) and later reads never see it.
func TestKVWriteIsNotAcknowledgedBeforeCommit(t *testing.T) {
	s := newKVSim(t, 3)
	s.electLeader("n1")
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	s.do(Event{Kind: Isolate, Node: "n1"})
	s.put("n1", "c2", "k", "stranded")
	s.tick("n1", 3*raft.DefaultHeartbeatTicks)
	s.DeliverAll()
	if !s.Busy("c2") {
		t.Fatalf("a write that cannot commit was answered: %s", s.last("c2"))
	}
	s.electLeader("n2")
	s.put("n2", "c3", "k", "B")
	s.DeliverAll()
	if op := s.last("c3"); op.Outcome != lincheck.OK {
		t.Fatalf("put(B): %s", op)
	}
	s.do(Event{Kind: HealAll})
	s.heartbeat("n2")
	s.heartbeat("n2")
	op := s.last("c2")
	if op.Outcome != lincheck.Rejected || s.KVStats().Lost != 1 {
		t.Fatalf("the overwritten write must be reported lost: %s %+v", op, s.KVStats())
	}
	s.get("n2", "c4", "k")
	s.DeliverAll()
	if op := s.last("c4"); op.Outcome != lincheck.OK || string(op.Output) != "B" {
		t.Fatalf("read after heal: %s", op)
	}
	s.requireLinearizable()
}

// TestKVCrashAtEveryPointOfAWrite crashes the leader at every driver crash point
// of the cycles that carry one client write — before and after its Save, after
// sending, before and after applying — and requires: the client never hears
// success from a process that died before replying (its op is Incomplete, not
// OK and not Rejected); a write whose entry was committed when the leader died
// is visible to every later read (server-committed but client-unknown is
// still committed); a write that never left the leader's memory is never
// visible; and the history is linearizable.
func TestKVCrashAtEveryPointOfAWrite(t *testing.T) {
	cases := []struct {
		point   string
		visible string // "yes", "no", or "either" (depends on who wins next)
	}{
		{"before-save", "no"},        // nothing persisted, nothing sent
		{"after-save", "either"},     // durable on the leader only
		{"after-send", "either"},     // one AppendEntries may be out
		{"before-advance", "either"}, // every AppendEntries handed off, none answered
		{"before-apply", "yes"},      // committed (the commit Save precedes apply)
		{"after-apply", "yes"},
		{"after-applied-to", "yes"},
	}
	for _, tc := range cases {
		t.Run(tc.point, func(t *testing.T) {
			s := newKVSim(t, 3)
			s.electLeader("n1")
			s.put("n1", "c1", "k", "old")
			s.DeliverAll()
			if op := s.last("c1"); op.Outcome != lincheck.OK {
				t.Fatalf("setup put: %s", op)
			}
			s.do(Event{Kind: CrashAt, Node: "n1", Point: tc.point, Nth: 1})
			s.put("n1", "c2", "k", "new")
			s.DeliverAll()
			if s.Up("n1") {
				s.heartbeat("n1") // the apply points fire once a quorum answers
			}
			if s.Up("n1") {
				t.Fatalf("n1 never reached %s", tc.point)
			}
			if op := s.last("c2"); op.Outcome != lincheck.Incomplete {
				t.Fatalf("the writer must hear nothing from a leader that died at %s: %s", tc.point, op)
			}
			// A new leader, the old one restarted, and a read.
			s.electLeader("n2")
			s.do(Event{Kind: Restart, Node: "n1"})
			s.heartbeat("n2")
			s.heartbeat("n2")
			s.get("n2", "c3", "k")
			s.DeliverAll()
			s.heartbeat("n2")
			op := s.last("c3")
			if op.Outcome != lincheck.OK {
				t.Fatalf("read after recovery: %s", op)
			}
			saw := string(op.Output)
			switch {
			case tc.visible == "yes" && saw != "new":
				t.Fatalf("the write was committed before the leader died at %s, but a later read returned %q", tc.point, saw)
			case tc.visible == "no" && saw != "old":
				t.Fatalf("the write never left the leader's memory at %s, but a later read returned %q", tc.point, saw)
			}
			t.Logf("crash at %s: client heard nothing; the write is %s (later read %q)", tc.point, map[bool]string{true: "visible", false: "not visible"}[saw == "new"], saw)
			s.requireLinearizable()
		})
	}
}

// TestKVHistoryOfAScriptIsReplayable: the scripted attacks above are ordinary
// scripts — replaying one reproduces the identical history.
func TestKVHistoryOfAScriptIsReplayable(t *testing.T) {
	s := newKVSim(t, 3)
	s.electLeader("n1")
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	s.get("n2", "c2", "k") // a follower: redirected to n1
	s.DeliverAll()
	s.heartbeat("n1")
	op := s.last("c2")
	if op.Outcome != lincheck.OK || len(op.Attempts) != 2 || op.Attempts[0].Node != "n2" || op.Attempts[1].Node != "n1" {
		t.Fatalf("a read sent to a follower must be redirected to the leader, both attempts recorded: %s %+v", op, op.Attempts)
	}
	re := Replay(Config{Nodes: 3, Seed: 1}, s.Script())
	if re.History.String() != s.History().String() || re.TraceHash != s.Trace().Hash() {
		t.Fatalf("replay diverged:\n%s\nvs\n%s", re.History, s.History())
	}
	if !strings.Contains(FormatScript(s.Script()), `kvget n2 c2 "k"`) {
		t.Fatalf("client events must appear in the script:\n%s", FormatScript(s.Script()))
	}
}

// --- Phase 13: request identity in the deterministic tier ---

// register makes a client register a session at node and waits for it.
func (s *kvSim) register(node NodeID, client string) uint64 {
	s.t.Helper()
	s.Apply(Event{Kind: KVRegister, Node: node, Client: client})
	for i := 0; i < 20 && s.Busy(client); i++ {
		s.DeliverAll()
		s.heartbeat(node)
	}
	id := s.Session(client)
	if id == 0 {
		s.t.Fatalf("%s did not register at %s", client, node)
	}
	return id
}

// TestKVSimCommittedRequestRetriedAfterLeaderCrash is the hardest case, exact
// and replayable: a session's PUT(A) is committed and applied on the leader,
// which dies (at after-applied-to) before the reply; a new leader is elected;
// another client writes B; the first client's retry of the SAME request is a
// duplicate — no second execution, the key keeps B — and the history of logical
// requests is linearizable. Every replica's decision for every entry matches
// the reference model at the instant it applies it (INV-X11), including the
// replays of the restarted node.
func TestKVSimCommittedRequestRetriedAfterLeaderCrash(t *testing.T) {
	s := newKVSim(t, 3)
	s.electLeader("n1")
	s.register("n1", "c1")
	s.register("n1", "c2")
	s.heartbeat("n1")
	s.do(Event{Kind: CrashAt, Node: "n1", Point: "after-applied-to", Nth: 1})
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	s.heartbeat("n1")
	if s.Up("n1") {
		t.Fatal("premise: n1 must have died after applying the write")
	}
	if !s.Busy("c1") {
		t.Fatalf("the client of a dead leader must not have an answer: %s", s.last("c1"))
	}
	s.do(Event{Kind: KVTimeout, Client: "c1"}) // gives up on that send; the request stays open
	s.electLeader("n2")
	s.do(Event{Kind: Restart, Node: "n1"})
	s.heartbeat("n2")
	s.put("n2", "c2", "k", "B")
	s.DeliverAll()
	s.heartbeat("n2")
	if op := s.last("c2"); op.Outcome != lincheck.OK {
		t.Fatalf("put(B): %s", op)
	}
	s.Apply(Event{Kind: KVRetry, Node: "n2", Client: "c1"})
	s.DeliverAll()
	s.heartbeat("n2")
	op := s.last("c1")
	if op.Outcome != lincheck.OK || len(op.Attempts) != 2 || !strings.HasPrefix(op.Attempts[1].Result, "duplicate of index") {
		t.Fatalf("the retry must be answered as a duplicate of the original: %s %+v", op, op.Attempts)
	}
	s.get("n2", "c3", "k")
	s.DeliverAll()
	s.heartbeat("n2")
	if op := s.last("c3"); op.Outcome != lincheck.OK || string(op.Output) != "B" {
		t.Fatalf("the retry executed again: %s", op)
	}
	s.requireLinearizable()
	if st := s.KVStats(); st.Duplicates != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

// TestKVSimConcurrentDuplicateExecutesOnce: while a session write is in flight
// at the leader, a duplicate is sent to a follower, redirected to the leader,
// and appended as a second entry: the first applied executes, the second is its
// duplicate; the client's request completes once.
func TestKVSimConcurrentDuplicateExecutesOnce(t *testing.T) {
	s := newKVSim(t, 3)
	s.electLeader("n1")
	s.register("n1", "c1")
	s.heartbeat("n1")
	s.put("n1", "c1", "k", "A")
	s.Apply(Event{Kind: KVDup, Node: "n2", Client: "c1"})
	s.DeliverAll()
	s.heartbeat("n1")
	s.heartbeat("n1")
	op := s.last("c1")
	if op.Outcome != lincheck.OK {
		t.Fatalf("%s", op)
	}
	st := s.Store("n1").Stats()
	if st.Executed != 1 || st.Duplicate != 1 {
		t.Fatalf("want one execution and one duplicate on the leader: %+v", st)
	}
	s.requireLinearizable()
}

// TestKVSimSessionsSurviveARestartOfEveryNode: after every node crashes and
// restarts, the session table is rebuilt by replay — every replayed decision
// is checked against the model as it is made — and a retry of an executed
// request is still a duplicate.
func TestKVSimSessionsSurviveARestartOfEveryNode(t *testing.T) {
	s := newKVSim(t, 3)
	s.electLeader("n1")
	s.register("n1", "c1")
	s.heartbeat("n1")
	s.do(Event{Kind: CrashAt, Node: "n1", Point: "after-applied-to", Nth: 1})
	s.put("n1", "c1", "k", "A")
	s.DeliverAll()
	s.heartbeat("n1")
	s.do(Event{Kind: KVTimeout, Client: "c1"})
	for _, id := range []NodeID{"n2", "n3"} {
		s.do(Event{Kind: Crash, Node: id, Power: true})
	}
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		s.do(Event{Kind: Restart, Node: id})
	}
	s.electLeader("n3")
	s.heartbeat("n3")
	s.Apply(Event{Kind: KVRetry, Node: "n3", Client: "c1"})
	s.DeliverAll()
	s.heartbeat("n3")
	op := s.last("c1")
	if op.Outcome != lincheck.OK || !strings.HasPrefix(op.Attempts[len(op.Attempts)-1].Result, "duplicate of index") {
		t.Fatalf("after every node restarted, the retry must still be a duplicate: %s %+v", op, op.Attempts)
	}
	s.requireLinearizable()
}

// TestKVTierCatchesRetriesThatAreNotDeduplicated proves the session tier has
// teeth at the history level: with a deliberately broken client that retries
// under a fresh request id — so an unanswered send that executed is executed
// again by its retry — the store, every replica and the session model all
// agree (every id is new, INV-X11 and INV-X2 are silent), yet seeded runs
// produce histories of logical requests the checker rejects: a write that took
// effect twice. If this ever stops failing, the tier cannot see a lost
// deduplication.
func TestKVTierCatchesRetriesThatAreNotDeduplicated(t *testing.T) {
	p, _ := KVProfileByName("kv-sessions-crashpoints")
	p.Keys = 1 // every write and read on one key: a second execution is observable
	caught := 0
	for seed := int64(1); seed <= 12; seed++ {
		cfg := Config{Nodes: p.Nodes, Seed: seed, KVLimits: p.KVLimits}
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.UnsafeFreshRetryIDs()
		rng := newRand(seed)
		for i := 0; i < p.Steps && c.Violation() == nil; i++ {
			c.Apply(c.generate(rng, p))
		}
		if c.Violation() != nil {
			t.Fatalf("seed %d: the broken client must be invisible to the invariants: %v", seed, c.Violation())
		}
		if ok, r := linearizable(c.History()); !ok {
			caught++
			if caught == 1 {
				replay := func(s []Event) lincheck.History {
					rc, _ := New(cfg)
					rc.UnsafeFreshRetryIDs()
					for _, e := range s {
						rc.Apply(e)
					}
					return rc.History()
				}
				min := MinimizeFunc(cfg, c.Script(), 600, func(s []Event) bool {
					good, _ := linearizable(replay(s))
					return !good
				})
				good, mr := linearizable(replay(min))
				if good {
					t.Fatal("the minimized script no longer reproduces the double execution")
				}
				t.Logf("seed %d: retry without deduplication caught: %s\nminimized to %d of %d events:\n%s\n--- counterexample ---\n%s",
					seed, r.Reason, len(min), len(c.Script()), FormatScript(min), lincheck.Format(mr.Counterexample))
			}
		}
	}
	t.Logf("%d of 12 runs caught", caught)
	if caught == 0 {
		t.Fatal("no run with fresh-id retries was caught: the tier cannot see a lost deduplication")
	}
}
