package raftnode

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/transport"
)

// These are the Phase 10 driver-level fault tests (docs/FAULTS.md): the REAL
// raftnode actor, raftlog, and (where used) TCP transport, with faults injected at
// the real persistence boundary (internal/fault's InjectFS/MemFS underneath
// raftlog) or the real network boundary (fault.Network around the transport).
// They run real goroutines and timers, so they assert properties that must hold
// under any interleaving; the seed-replayable proofs are in internal/raftsim.

// sendLog is a hook transport's thread-safe record of every Send it received.
type sendLog struct {
	mu   sync.Mutex
	msgs []transport.MsgKind
}

func (s *sendLog) add(k transport.MsgKind) { s.mu.Lock(); s.msgs = append(s.msgs, k); s.mu.Unlock() }
func (s *sendLog) count() int              { s.mu.Lock(); defer s.mu.Unlock(); return len(s.msgs) }

// proposeWithin proposes with a bounded wait, so a driver whose actor is stalled
// fails the test in seconds instead of hanging it until the suite timeout.
func proposeWithin(n *Node, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return n.Propose(ctx, data)
}

func voteRequest(term uint64) transport.Envelope {
	return transport.Envelope{
		Peer: "z", Kind: transport.MsgRequestVote,
		Payload: raft.Message{Type: raft.MsgVoteRequest, Term: term}.Marshal(),
	}
}

// TestPersistFailureIsFailStop proves INV-F1 on the real driver path. A durable-log
// write or fsync fails exactly when the node must persist a vote it is about to
// grant. The node must then: send NOTHING (in particular not the vote reply), stop
// (Done) with the injected error (Err), perform no further write or fsync, reject
// proposals, ignore further input — and leave a log that reopens cleanly, after
// which a restarted node resumes and replies normally.
func TestPersistFailureIsFailStop(t *testing.T) {
	cases := []struct {
		name string
		inj  fault.Injection
		// wantVoteAfter is the vote recovered by the restart: an fsync failure
		// leaves the written HardState record in the (surviving) file; a failed or
		// torn write leaves none (the torn record is truncated on reopen).
		wantVoteAfter NodeID
		errno         error
	}{
		{"disk full on write", fault.Injection{Op: fault.OpWrite, Err: syscall.ENOSPC}, "", syscall.ENOSPC},
		{"torn short write", fault.Injection{Op: fault.OpWrite, Short: 6}, "", syscall.EIO},
		{"fsync failure", fault.Injection{Op: fault.OpSync}, "z", syscall.EIO},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "a.log")
			inj := fault.NewInjectFS(nil) // the real OS filesystem underneath
			ht := newHookTransport("a")
			sent := &sendLog{}
			ht.onSend = func(k transport.MsgKind, _ []byte) { sent.add(k) }

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			n, err := Start(ctx, Config{
				ID: "a", Peers: []NodeID{"a", "z", "c"}, Transport: ht,
				LogPath: logPath, TickInterval: time.Hour, FS: inj,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()

			inj.Arm(tc.inj)
			ht.recv <- voteRequest(5)

			select {
			case <-n.Done():
			case <-time.After(3 * time.Second):
				t.Fatal("node did not stop after its durable log failed")
			}
			if err := n.Err(); !errors.Is(err, raftlog.ErrFailed) || !errors.Is(err, fault.ErrInjected) || !errors.Is(err, tc.errno) {
				t.Fatalf("Err() = %v, want ErrFailed wrapping the injected %v", err, tc.errno)
			}

			// Keep feeding it input; a fail-stopped node must ignore all of it.
			for i := 0; i < 5; i++ {
				select {
				case ht.recv <- voteRequest(6 + uint64(i)):
				default:
				}
			}
			if err := proposeWithin(n, []byte("x")); !errors.Is(err, raft.ErrStopped) {
				t.Fatalf("Propose after fail-stop = %v, want ErrStopped", err)
			}
			time.Sleep(50 * time.Millisecond) // any stray sender would have run by now
			if c := sent.count(); c != 0 {
				t.Fatalf("a node whose log failed sent %d message(s); the vote reply escaped before it was durable", c)
			}
			var failedAt = -1
			ops := inj.Ops()
			for i, op := range ops {
				if op.Injected {
					failedAt = i
					break
				}
			}
			if failedAt < 0 {
				t.Fatal("the injected fault never fired")
			}
			for _, op := range ops[failedAt+1:] {
				if op.Op == fault.OpWrite || op.Op == fault.OpSync || op.Op == fault.OpTruncate {
					t.Fatalf("the failed node still performed %s after the failure", op.Op)
				}
			}
			if err := n.Close(); err != nil {
				t.Fatalf("Close after fail-stop: %v", err)
			}

			// Recovery is explicit: the log reopens, any torn record is gone, and a
			// restarted node answers the same request (the reply now follows a
			// successful persist).
			rec, err := raftlog.Inspect(logPath)
			if err != nil {
				t.Fatalf("log unreadable after the failure: %v", err)
			}
			if rec.HardState.Vote != tc.wantVoteAfter {
				t.Fatalf("recovered vote %q, want %q", rec.HardState.Vote, tc.wantVoteAfter)
			}
			ht2 := newHookTransport("a")
			granted := make(chan bool, 1)
			ht2.onSend = func(k transport.MsgKind, p []byte) {
				if k == transport.MsgRequestVoteResponse {
					m, _ := raft.Unmarshal(p)
					select {
					case granted <- m.VoteGranted:
					default:
					}
				}
			}
			n2, err := Start(ctx, Config{ID: "a", Peers: []NodeID{"a", "z", "c"}, Transport: ht2, LogPath: logPath, TickInterval: time.Hour})
			if err != nil {
				t.Fatalf("restart after a persistence failure: %v", err)
			}
			defer n2.Close()
			ht2.recv <- voteRequest(5)
			select {
			case g := <-granted:
				if !g {
					t.Fatal("restarted node refused the vote it had never promised elsewhere")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("restarted node did not reply")
			}
		})
	}
}

// TestReplyOnlyAfterFsync strengthens the Phase 9 ordering test from "written" to
// "fsynced" (INV-R6): at the instant the vote reply is handed to the network, the
// granted vote must already be in the log's DURABLE view — what a power loss at
// that instant would leave — not merely in the cache. Removing the fsync from
// raftlog.Save, or sending before Save, fails this test.
func TestReplyOnlyAfterFsync(t *testing.T) {
	mem := fault.NewMemFS()
	const logPath = "/node/a.log"
	ht := newHookTransport("a")
	checkCh := make(chan error, 1)
	ht.onSend = func(k transport.MsgKind, _ []byte) {
		if k != transport.MsgRequestVoteResponse {
			return
		}
		rec, err := raftlog.InspectFS(mem.DurableCopy(), logPath)
		select {
		case checkCh <- verifyHardState(err, rec, 5, "z"):
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, err := Start(ctx, Config{ID: "a", Peers: []NodeID{"a", "z", "c"}, Transport: ht, LogPath: logPath, TickInterval: time.Hour, FS: mem})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ht.recv <- voteRequest(5)
	awaitCheck(t, checkCh)
}

// TestProposeHonoursContextDuringSlowFsync proves a caller's deadline is honoured
// while the node is stuck persisting (a disk stall, not a failure): Propose
// returns context.DeadlineExceeded promptly — an UNKNOWN outcome, since the entry
// may already be appended — instead of waiting for the disk. When the stall ends
// the node carries on normally: the entry commits and later proposals succeed.
func TestProposeHonoursContextDuringSlowFsync(t *testing.T) {
	inj := fault.NewInjectFS(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, err := Start(ctx, Config{
		ID: "a", Peers: []NodeID{"a"}, Transport: newHookTransport("a"),
		LogPath: filepath.Join(t.TempDir(), "a.log"), TickInterval: 5 * time.Millisecond, FS: inj,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	deadline := time.Now().Add(3 * time.Second)
	for n.Status().Role != raft.Leader || n.Status().Commit < 1 {
		if time.Now().After(deadline) {
			t.Fatal("single node did not elect itself")
		}
		time.Sleep(time.Millisecond)
	}

	gate := make(chan struct{})
	inj.Arm(fault.Injection{Op: fault.OpSync, Gate: gate})
	safety := time.AfterFunc(3*time.Second, func() { close(gate) }) // never hang the test
	defer safety.Stop()

	pctx, pcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer pcancel()
	start := time.Now()
	err = n.Propose(pctx, []byte("slow"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Propose during a disk stall = %v, want DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("Propose ignored its deadline and waited %s for the disk", waited)
	}

	if safety.Stop() {
		close(gate) // end the stall now
	}
	deadline = time.Now().Add(3 * time.Second)
	for n.Status().Commit < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("entry did not commit after the stall ended: %+v", n.Status())
		}
		time.Sleep(time.Millisecond)
	}
	if err := proposeWithin(n, []byte("after")); err != nil {
		t.Fatalf("Propose after the stall: %v", err)
	}
}

// --- real TCP clusters under network faults ---

// faultCluster is an n-node cluster over real TCP transports, each wrapped by one
// shared fault.Network, with a monitor that records every leader it observes per
// term (from consistent Status snapshots) and fails the test on two leaders in one
// term (INV-R1 on the real driver).
type faultCluster struct {
	*harness
	net *fault.Network

	monMu    sync.Mutex
	leaders  map[uint64]NodeID
	monErr   error
	monStop  chan struct{}
	monDone  chan struct{}
	memberID []string
	stopOnce sync.Once
}

func startFaultCluster(t *testing.T, ctx context.Context, n int) *faultCluster {
	t.Helper()
	net := fault.NewNetwork()
	h := startClusterWith(t, ctx, n, func(tr transport.Transport) transport.Transport { return net.Wrap(tr) })
	fc := &faultCluster{harness: h, net: net, leaders: map[uint64]NodeID{}, monStop: make(chan struct{}), monDone: make(chan struct{})}
	for _, id := range h.ids {
		fc.memberID = append(fc.memberID, string(id))
	}
	go fc.monitor()
	return fc
}

func (fc *faultCluster) monitor() {
	defer close(fc.monDone)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-fc.monStop:
			return
		case <-tick.C:
		}
		for _, id := range fc.ids {
			st := fc.nodes[id].Status()
			if st.Role != raft.Leader {
				continue
			}
			fc.monMu.Lock()
			if prev, ok := fc.leaders[st.Term]; ok && prev != id && fc.monErr == nil {
				fc.monErr = errors.New("two leaders in term: " + string(prev) + " and " + string(id))
			}
			fc.leaders[st.Term] = id
			fc.monMu.Unlock()
		}
	}
}

// stop halts the monitor and the cluster (idempotent, so it can be deferred and
// also called early) and fails the test if the monitor saw two leaders in a term.
func (fc *faultCluster) stop() {
	fc.stopOnce.Do(func() {
		close(fc.monStop)
		<-fc.monDone
		fc.harness.stop()
	})
	fc.monMu.Lock()
	defer fc.monMu.Unlock()
	if fc.monErr != nil {
		fc.t.Fatalf("INV-R1 violated on the real driver: %v", fc.monErr)
	}
}

// waitLeaderAmong waits for exactly one of ids to lead in a term > minTerm.
func (fc *faultCluster) waitLeaderAmong(ids []NodeID, minTerm uint64, timeout time.Duration) NodeID {
	fc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var got []NodeID
		for _, id := range ids {
			if st := fc.nodes[id].Status(); st.Role == raft.Leader && st.Term > minTerm {
				got = append(got, id)
			}
		}
		if len(got) == 1 {
			return got[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	fc.t.Fatalf("no single leader among %v above term %d within %s", ids, minTerm, timeout)
	return ""
}

// waitUntil polls cond until it holds or timeout passes, then fails the test.
func (fc *faultCluster) waitUntil(timeout time.Duration, cond func() bool, format string, args ...any) {
	fc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	fc.t.Fatalf("not within %s: %s", timeout, fmt.Sprintf(format, args...))
}

func (fc *faultCluster) waitCommit(ids []NodeID, idx uint64, timeout time.Duration) {
	fc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if fc.nodes[id].Status().Commit < idx {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	var got []uint64
	for _, id := range ids {
		got = append(got, fc.nodes[id].Status().Commit)
	}
	fc.t.Fatalf("nodes %v did not commit through %d within %s (commits %v)", ids, idx, timeout, got)
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

// TestWedgedPeerDoesNotStallTheLeader proves the driver never blocks its Raft actor
// on the network (FAILURE_MODEL §7 "slow node": the leader does not block on the
// slowest follower; INV-F5). One follower is frozen, as a stopped process would be:
// every send to it hangs, as a TCP write to a peer that stopped reading does, and it
// sends nothing (so its own election attempts cannot disrupt anyone — that is a
// different fault). Once the wedge has demonstrably engaged (a Send is blocked on
// it), the leader must keep accepting proposals and committing them with the other
// follower — twenty rounds of it. Once the peer thaws it catches up. With a
// synchronous send in the actor, the first stuck write freezes the leader: it
// accepts no further proposal (Propose hangs to its deadline) — this test fails.
//
// The assertion needs no timing assumption. An earlier version slept for a second
// and required the term not to move, which a starved machine can break on its own:
// the healthy follower's election timer fires, the term moves, and that says
// nothing about the wedge (found in Phase 12 by running the race suite under
// deliberate CPU starvation). An election voids this scenario's premise — one
// leader throughout — without violating its property, and the leader's actor
// proves it is alive by answering "not leader"; the scenario then starts over on
// a fresh cluster, at most three times.
func TestWedgedPeerDoesNotStallTheLeader(t *testing.T) {
	for attempt := 1; attempt <= 3; attempt++ {
		why := wedgedPeerScenario(t)
		if why == "" {
			return
		}
		t.Logf("attempt %d: premise not met (%s); starting over on a fresh cluster", attempt, why)
	}
	t.Fatal("the scenario's premise (one leader throughout) was not met in 3 attempts")
}

// wedgedPeerScenario is one attempt; it returns why its premise failed, or "".
func wedgedPeerScenario(t *testing.T) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := startFaultCluster(t, ctx, 3)
	defer fc.stop()

	leader := fc.waitLeaderAmong(fc.ids, 0, 5*time.Second)
	others := without(fc.ids, leader)
	wedged, healthy := others[0], others[1]

	rule := fc.net.AddRule(fault.Rule{From: string(leader), To: string(wedged), Action: fault.Block})
	silenced := fc.net.AddRule(fault.Rule{From: string(wedged), Action: fault.Drop})
	var last uint64
	for i := 0; i < 20; i++ {
		err := proposeWithin(fc.nodes[leader], []byte{byte('a' + i)})
		if errors.Is(err, raft.ErrNotLeader) {
			return fmt.Sprintf("round %d: the leader answered not-leader (alive, but an election intervened)", i)
		}
		if err != nil {
			t.Fatalf("round %d: the leader stopped accepting proposals while one peer is wedged: %v", i, err)
		}
		last = fc.nodes[leader].Status().LastIndex
		deadline := time.Now().Add(10 * time.Second)
		for fc.nodes[leader].Status().Commit < last || fc.nodes[healthy].Status().Commit < last {
			if st := fc.nodes[leader].Status(); st.Role != raft.Leader {
				return fmt.Sprintf("round %d: leadership moved (%+v)", i, st)
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: index %d did not commit on the leader and the healthy follower while one peer is wedged", i, last)
			}
			time.Sleep(2 * time.Millisecond)
		}
		if i == 0 {
			// Synchronize on the wedge engaging before the remaining rounds.
			fc.waitUntil(5*time.Second, func() bool { return fc.net.Stats().Blocked > 0 }, "a send never blocked on the wedged peer")
		}
	}

	fc.net.RemoveRule(silenced)
	fc.net.RemoveRule(rule)
	fc.waitCommit([]NodeID{wedged}, last, 5*time.Second)
	return ""
}

// TestIsolatedLeaderCannotCommitAndRejoins is scenario A on the real driver: the
// leader is partitioned from both followers; a proposal it accepts can never
// commit; the majority elects a new leader in a higher term and commits; after
// healing, the old leader steps down and every durable log converges, with the
// old leader's uncommitted entry replaced.
func TestIsolatedLeaderCannotCommitAndRejoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := startFaultCluster(t, ctx, 3)
	defer fc.stop()

	old := fc.waitLeaderAmong(fc.ids, 0, 5*time.Second)
	oldTerm := fc.nodes[old].Status().Term
	fc.waitCommit(fc.ids, 1, 3*time.Second) // the election no-op

	fc.net.Isolate(string(old), fc.memberID)
	if err := proposeWithin(fc.nodes[old], []byte("stranded")); err != nil {
		t.Fatalf("propose on isolated leader: %v", err)
	}
	stranded := fc.nodes[old].Status().LastIndex

	majority := without(fc.ids, old)
	next := fc.waitLeaderAmong(majority, oldTerm, 5*time.Second)
	if err := proposeWithin(fc.nodes[next], []byte("fresh")); err != nil {
		t.Fatalf("propose on new leader: %v", err)
	}
	fresh := fc.nodes[next].Status().LastIndex
	fc.waitCommit(majority, fresh, 3*time.Second)
	if c := fc.nodes[old].Status().Commit; c >= stranded {
		t.Fatalf("isolated leader committed index %d (stranded entry at %d) without a quorum", c, stranded)
	}

	fc.net.HealAll()
	fc.waitCommit(fc.ids, fresh, 5*time.Second)
	if st := fc.nodes[old].Status(); st.Role == raft.Leader && st.Term == oldTerm {
		t.Fatalf("old leader still leads its stale term after healing: %+v", st)
	}
	fc.stop()

	logs := map[NodeID]*raftlog.Recovered{}
	for _, id := range fc.ids {
		rec, err := raftlog.Inspect(filepath.Join(fc.dir, string(id)+".log"))
		if err != nil {
			t.Fatal(err)
		}
		logs[id] = rec
	}
	assertLogsIdenticalThrough(t, logs, fresh)
	for i := uint64(0); i < fresh; i++ {
		if string(logs[next].Entries[i].Data) == "stranded" {
			t.Fatalf("the isolated leader's uncommitted entry survived at index %d", i+1)
		}
	}
	if string(logs[next].Entries[fresh-1].Data) != "fresh" {
		t.Fatalf("index %d = %q, want fresh", fresh, logs[next].Entries[fresh-1].Data)
	}
}

// assertLogsIdenticalThrough requires every durable log to hold the same entries
// through idx (Log Matching + convergence, checked on the bytes on disk).
func assertLogsIdenticalThrough(t *testing.T, logs map[NodeID]*raftlog.Recovered, idx uint64) {
	t.Helper()
	var ref *raftlog.Recovered
	var refID NodeID
	for id, rec := range logs {
		if uint64(len(rec.Entries)) < idx {
			t.Fatalf("%s holds %d entries, want at least %d", id, len(rec.Entries), idx)
		}
		if ref == nil {
			ref, refID = rec, id
			continue
		}
		for i := uint64(0); i < idx; i++ {
			a, b := ref.Entries[i], rec.Entries[i]
			if a.Term != b.Term || string(a.Data) != string(b.Data) {
				t.Fatalf("durable logs diverge at index %d: %s has (t%d,%q), %s has (t%d,%q)", i+1, refID, a.Term, a.Data, id, b.Term, b.Data)
			}
		}
	}
}

// TestDuplicatedAndReorderedTrafficAppliesOnce runs the real driver over a network
// that duplicates every message and, in bursts, holds AppendEntries and releases
// them in reverse order. Every node's state machine must apply the same commands,
// each exactly once, in log order.
func TestDuplicatedAndReorderedTrafficAppliesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := startFaultCluster(t, ctx, 3)
	defer fc.stop()

	fc.waitLeaderAmong(fc.ids, 0, 5*time.Second)
	fc.net.AddRule(fault.Rule{Action: fault.Duplicate, Copies: 2})
	for burst := 0; burst < 4; burst++ {
		hold := fc.net.AddRule(fault.Rule{Kinds: []transport.MsgKind{transport.MsgAppendEntries}, Action: fault.Hold})
		heldBefore := fc.net.Stats().Held
		accepted := 0
		for i := 0; i < 5; i++ {
			// The leader may change under reordering; propose wherever leads now.
			l := fc.waitLeaderAmong(fc.ids, 0, 5*time.Second)
			if proposeWithin(fc.nodes[l], []byte{byte('A' + burst), byte('0' + i)}) == nil {
				accepted++
			}
		}
		if accepted == 0 {
			t.Fatalf("burst %d: no proposal was accepted, so the burst reorders nothing", burst)
		}
		// A burst reorders nothing unless the hold caught an AppendEntries, and
		// Propose only enqueues: the leader's actor emits the message on its next
		// cycle, within microseconds when the machine is idle — but a loaded
		// machine can starve the actor past this whole burst, after which the
		// anti-vacuity check below reports Held:0. Wait for the hold to engage
		// (a heartbeat bounds the wait) instead of racing the scheduler.
		fc.waitUntil(5*time.Second, func() bool { return fc.net.Stats().Held > heldBefore },
			"burst %d: the hold caught no AppendEntries", burst)
		fc.net.RemoveRule(hold)
		fc.net.Release(true) // deliver the held AppendEntries newest-first
	}
	l := fc.waitLeaderAmong(fc.ids, 0, 5*time.Second)
	target := fc.nodes[l].Status().LastIndex
	fc.waitCommit(fc.ids, target, 5*time.Second)
	if s := fc.net.Stats(); s.Duplicated == 0 || s.Held == 0 || s.Released == 0 {
		t.Fatalf("faults did not engage: %+v", s)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, id := range fc.ids {
			if uint64(len(fc.sms[id].snapshot())) < target {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	ref := fc.sms[fc.ids[0]].snapshot()
	for _, id := range fc.ids {
		got := fc.sms[id].snapshot()
		if uint64(len(got)) < target {
			t.Fatalf("%s applied %d commands, want %d", id, len(got), target)
		}
		for i := uint64(0); i < target; i++ {
			if got[i] != ref[i] {
				t.Fatalf("%s applied %q at position %d, %s applied %q", id, got[i], i+1, fc.ids[0], ref[i])
			}
		}
		seen := map[string]bool{}
		for _, c := range got {
			if c == "" {
				continue // election no-ops
			}
			if seen[c] {
				t.Fatalf("%s applied command %q twice under duplicated delivery", id, c)
			}
			seen[c] = true
		}
	}
}
