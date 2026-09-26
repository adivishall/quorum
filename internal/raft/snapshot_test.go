package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/replication"
)

// Phase 14 (docs/SNAPSHOTS.md §8): the core's half of snapshot installation. The
// harness delivers MsgSnapshot straight to the follower's core — the driver's
// streaming of the snapshot's bytes is tested in raftnode; here the snapshot is
// its metadata, and a follower that installs one takes its index as applied.

// compact compacts id's log through index (which must be applied).
func (nw *network) compact(id NodeID, index uint64) {
	nw.t.Helper()
	if err := nw.nodes[id].Compact(index); err != nil {
		nw.t.Fatalf("Compact(%d) on %s: %v", index, id, err)
	}
}

// proposeN proposes n commands on the leader, settling after each.
func (nw *network) proposeN(id NodeID, prefix string, n int) {
	nw.t.Helper()
	for i := 0; i < n; i++ {
		nw.propose(id, fmt.Sprintf("%s%d", prefix, i))
	}
}

// heartbeatRounds runs enough fair rounds for every follower to learn the
// leader's latest commit (followers learn it from the next AppendEntries).
func (nw *network) heartbeatRounds() {
	for i := 0; i < 3*DefaultHeartbeatTicks; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
}

// converged reports whether every node committed and applied the leader's
// whole log.
func (nw *network) converged(leader NodeID) bool {
	L := nw.logs[leader]
	if L.CommitIndex() != L.LastIndex() {
		return false
	}
	for _, id := range nw.ids {
		if l := nw.logs[id]; l.CommitIndex() != L.LastIndex() || l.LastIndex() != L.LastIndex() || nw.applyCount[id] != L.LastIndex() {
			return false
		}
	}
	return true
}

// requireConverged checks every node holds the same entries from the highest
// boundary among them through the leader's commit, and has committed and
// applied through it.
func (nw *network) requireConverged(leader NodeID) {
	nw.t.Helper()
	L := nw.logs[leader]
	for _, id := range nw.ids {
		l := nw.logs[id]
		if l.CommitIndex() != L.CommitIndex() || l.LastIndex() != L.LastIndex() || nw.applyCount[id] != L.CommitIndex() {
			nw.t.Fatalf("%s has not converged to %s:\n%s\n%s (applied %d)", id, leader, nw.dumpLog(id), nw.dumpLog(leader), nw.applyCount[id])
		}
		lo := l.FirstIndex()
		if L.FirstIndex() > lo {
			lo = L.FirstIndex()
		}
		for i := lo; i <= L.LastIndex(); i++ {
			a, _ := l.At(i)
			b, _ := L.At(i)
			if a.Term != b.Term || string(a.Data) != string(b.Data) {
				nw.t.Fatalf("%s differs from %s at %d", id, leader, i)
			}
		}
	}
}

// TestLaggingFollowerCatchesUpBySnapshot is the critical scenario at the core:
// a follower falls behind, the leader compacts the prefix it needs, AppendEntries
// can no longer bring it up to date — the leader offers its snapshot instead,
// exactly once while the offer is outstanding; the follower installs it and
// normal replication resumes from the snapshot's index to full convergence.
func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 1)
	nw.electLeader("n1")
	nw.isolate("n3")
	nw.proposeN("n1", "a", 20)
	applied := nw.nodes["n1"].AppliedIndex()
	nw.compact("n1", applied-2)
	base, _ := nw.nodes["n1"].Boundary()
	if last3 := nw.logs["n3"].LastIndex(); last3 >= base {
		t.Fatalf("premise: n3 (last %d) must lack entries below the leader's boundary %d", last3, base)
	}
	nw.heal()
	for i := 0; i < 6; i++ {
		nw.tickAll()
		nw.deliverAll()
	}
	if nw.snapshotsSent["n3"] != 1 || len(nw.snapshotsInstalled["n3"]) != 1 || nw.snapshotsInstalled["n3"][0].Index != base {
		t.Fatalf("want one snapshot offered and installed at %d: sent %d, installed %v", base, nw.snapshotsSent["n3"], nw.snapshotsInstalled["n3"])
	}
	if nw.snapshotsSent["n2"] != 0 {
		t.Fatal("an up-to-date follower was offered a snapshot")
	}
	nw.proposeN("n1", "b", 3)
	nw.heartbeatRounds()
	nw.requireConverged("n1")
	if b3, _ := nw.logs["n3"].Boundary(); b3 != base {
		t.Fatalf("n3's boundary %d, want the snapshot's %d", b3, base)
	}
}

// TestUnansweredSnapshotIsOfferedAgain: while an offer is outstanding the
// leader only heartbeats the follower (at the boundary); if the offer is lost,
// it is made again after SnapshotRetryTicks, never before.
func TestUnansweredSnapshotIsOfferedAgain(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 7)
	nw.electLeader("n1")
	nw.isolate("n3")
	nw.proposeN("n1", "a", 10)
	nw.compact("n1", nw.nodes["n1"].AppliedIndex())
	nw.heal()
	offers := func() int {
		n := 0
		for _, m := range nw.queue {
			if m.Type == MsgSnapshot && m.To == "n3" {
				n++
			}
		}
		return n
	}
	// First heartbeat round: the offer.
	for nw.nodes["n1"].heartbeatElapsed != 0 || offers() == 0 {
		nw.tick("n1")
	}
	// Lose it.
	var kept []Message
	for _, m := range nw.takeQueue() {
		if m.Type != MsgSnapshot {
			kept = append(kept, m)
		}
	}
	nw.queue = kept
	retry := nw.nodes["n1"].snapshotRetryTicks
	for i := 0; i < retry-1; i++ {
		nw.tick("n1")
		if offers() != 0 {
			t.Fatalf("the snapshot was offered again after %d ticks, before the retry interval %d", i+1, retry)
		}
		// n3 answers the boundary heartbeats with rejections; they change nothing.
		nw.deliverAll()
	}
	for i := 0; i < 2*nw.nodes["n1"].heartbeatTicks && offers() == 0; i++ {
		nw.tick("n1")
	}
	if offers() != 1 {
		t.Fatal("an unanswered offer was not made again after the retry interval")
	}
	nw.deliverAll()
	if len(nw.snapshotsInstalled["n3"]) != 1 {
		t.Fatalf("installed %v", nw.snapshotsInstalled["n3"])
	}
}

// followerWith returns a single follower core (of a 3-node group) whose log
// holds n entries of term 1, committed through commit, at currentTerm term.
func followerWith(t *testing.T, n int, commit, term uint64) (*Raft, *replication.MemoryLog) {
	t.Helper()
	lg := replication.NewMemoryLog()
	for i := 1; i <= n; i++ {
		if err := lg.Append(Entry{Index: uint64(i), Term: 1, Data: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := lg.Commit(commit); err != nil {
		t.Fatal(err)
	}
	r, err := New(Config{ID: "n2", Peers: []NodeID{"n1", "n2", "n3"}, Rand: rand.New(rand.NewSource(1)), Log: lg, Term: term})
	if err != nil {
		t.Fatal(err)
	}
	return r, lg
}

func drainReady(r *Raft) Ready {
	rd := r.Ready()
	r.Advance()
	return rd
}

// TestFollowerInstallKeepsAMatchingSuffix: a follower that holds the snapshot's
// last entry with the snapshot's term keeps what follows it; commit and applied
// jump to the snapshot, and the Ready asks the driver to make it durable before
// the response.
func TestFollowerInstallKeepsAMatchingSuffix(t *testing.T) {
	r, lg := followerWith(t, 17, 5, 2)
	if err := r.Step(Message{Type: MsgSnapshot, From: "n1", To: "n2", Term: 2, SnapshotIndex: 15, SnapshotTerm: 1, Seq: 4}); err != nil {
		t.Fatal(err)
	}
	rd := drainReady(r)
	if rd.Snapshot == nil || *rd.Snapshot != (SnapshotMeta{15, 1}) {
		t.Fatalf("Ready.Snapshot = %v", rd.Snapshot)
	}
	if rd.HardState == nil || rd.HardState.Commit != 15 {
		t.Fatalf("the new commit must be persisted with it: %+v", rd.HardState)
	}
	if b, _ := lg.Boundary(); b != 15 || lg.LastIndex() != 17 || lg.CommitIndex() != 15 || lg.AppliedIndex() != 15 {
		t.Fatalf("log after install: boundary %d last %d commit %d applied %d", b, lg.LastIndex(), lg.CommitIndex(), lg.AppliedIndex())
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgSnapshotResponse || !rd.Messages[0].Success || rd.Messages[0].MatchIndex != 15 || rd.Messages[0].Seq != 4 {
		t.Fatalf("response %+v", rd.Messages)
	}
}

// TestFollowerIgnoresASnapshotItAlreadyCovers: a duplicate, delayed or stale
// offer at or below the commit index installs nothing and answers with the
// commit index; a lower-term offer is refused and changes nothing.
func TestFollowerIgnoresASnapshotItAlreadyCovers(t *testing.T) {
	r, lg := followerWith(t, 20, 20, 3)
	for _, idx := range []uint64{5, 20} {
		if err := r.Step(Message{Type: MsgSnapshot, From: "n1", To: "n2", Term: 3, SnapshotIndex: idx, SnapshotTerm: 1}); err != nil {
			t.Fatal(err)
		}
		rd := drainReady(r)
		if rd.Snapshot != nil {
			t.Fatalf("a covered snapshot (%d) was installed", idx)
		}
		if m := rd.Messages[0]; !m.Success || m.MatchIndex != 20 {
			t.Fatalf("answer to a covered snapshot: %+v", m)
		}
	}
	if b, _ := lg.Boundary(); b != 0 || lg.LastIndex() != 20 {
		t.Fatal("a covered snapshot changed the log")
	}
	if err := r.Step(Message{Type: MsgSnapshot, From: "n1", To: "n2", Term: 2, SnapshotIndex: 30, SnapshotTerm: 2}); err != nil {
		t.Fatal(err)
	}
	rd := drainReady(r)
	if rd.Snapshot != nil || rd.Messages[0].Success || rd.Messages[0].Term != 3 {
		t.Fatalf("a lower-term offer was not refused: %+v", rd)
	}
}

// TestAppendEntriesBelowAFollowersSnapshot: a follower that installed a snapshot
// receives an AppendEntries that starts below it (a delayed message, or a leader
// whose nextIndex lagged). The part its snapshot covers is committed and
// matches; the rest is appended.
func TestAppendEntriesBelowAFollowersSnapshot(t *testing.T) {
	r, lg := followerWith(t, 3, 3, 2)
	if err := r.Step(Message{Type: MsgSnapshot, From: "n1", To: "n2", Term: 2, SnapshotIndex: 15, SnapshotTerm: 1}); err != nil {
		t.Fatal(err)
	}
	drainReady(r)
	var es []Entry
	for i := uint64(11); i <= 18; i++ {
		es = append(es, Entry{Index: i, Term: 1, Data: []byte{byte(i)}})
	}
	if err := r.Step(Message{Type: MsgAppendRequest, From: "n1", To: "n2", Term: 2, PrevLogIndex: 10, PrevLogTerm: 1, Entries: es, LeaderCommit: 18}); err != nil {
		t.Fatal(err)
	}
	rd := drainReady(r)
	if m := rd.Messages[0]; !m.Success || m.MatchIndex != 18 {
		t.Fatalf("response %+v", m)
	}
	if lg.LastIndex() != 18 || lg.CommitIndex() != 18 || len(rd.Entries) != 3 || rd.Entries[0].Index != 16 {
		t.Fatalf("after the overlapping append: last %d commit %d, persisted %v", lg.LastIndex(), lg.CommitIndex(), rd.Entries)
	}
	// Entirely below the boundary (entries 11..12): nothing to do, still a success.
	if err := r.Step(Message{Type: MsgAppendRequest, From: "n1", To: "n2", Term: 2, PrevLogIndex: 10, PrevLogTerm: 1, Entries: es[:2]}); err != nil {
		t.Fatal(err)
	}
	if m := drainReady(r).Messages[0]; !m.Success || m.MatchIndex != 12 {
		t.Fatalf("an append the snapshot covers: %+v", m)
	}
}

// TestCompactNeedsTheAppliedIndex: the core refuses to compact what has not
// been applied (a snapshot contains only applied state).
func TestCompactNeedsTheAppliedIndex(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 3)
	nw.electLeader("n1")
	nw.proposeN("n1", "a", 5)
	applied := nw.nodes["n1"].AppliedIndex()
	if err := nw.nodes["n1"].Compact(applied + 1); !errors.Is(err, replication.ErrCompactBeyondApplied) && !errors.Is(err, replication.ErrOutOfRange) {
		t.Fatalf("compacting past the applied index: %v", err)
	}
	nw.compact("n1", applied)
}

// TestConflictBackupStopsAtTheBoundary: a leader whose log begins after a
// boundary backs a conflicting follower up to the boundary at most — then
// offers the snapshot — rather than reading terms it no longer has.
func TestConflictBackupStopsAtTheBoundary(t *testing.T) {
	nw := newNetwork(t, []NodeID{"n1", "n2", "n3"}, 11)
	nw.electLeader("n1")
	nw.proposeN("n1", "a", 6)
	nw.compact("n1", nw.nodes["n1"].AppliedIndex())
	base, baseTerm := nw.nodes["n1"].Boundary()
	if got := nw.nodes["n1"].lastIndexOfTerm(baseTerm); got < base {
		t.Fatalf("lastIndexOfTerm(%d) = %d below the boundary %d", baseTerm, got, base)
	}
	if got := nw.nodes["n1"].firstIndexOfTerm(baseTerm, nw.nodes["n1"].LastIndex()); got < base {
		t.Fatalf("firstIndexOfTerm walked below the boundary %d: %d", base, got)
	}
}

// TestRandomizedSchedulesWithCompaction: seeded schedules of proposals, drops,
// partitions, reorders and compactions on every node (whenever it has applied
// something new); the continuous invariants hold throughout, and after healing
// every node converges — by AppendEntries or by snapshot.
func TestRandomizedSchedulesWithCompaction(t *testing.T) {
	installed := 0
	defer func() {
		// Non-vacuity: the schedules must reach the snapshot path, not only
		// compact logs every follower has already caught up with.
		if !t.Failed() && installed == 0 {
			t.Fatal("no snapshot was installed in any seed")
		}
		t.Logf("snapshots installed across seeds: %d", installed)
	}()
	for seed := int64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			ids := []NodeID{"n1", "n2", "n3", "n4", "n5"}
			nw := newNetwork(t, ids, seed*97)
			nw.electLeader("n1")
			snaps := 0
			for step := 0; step < 600; step++ {
				switch p := rng.Intn(100); {
				case p < 35:
					nw.tick(ids[rng.Intn(len(ids))])
				case p < 60:
					if len(nw.queue) > 0 {
						i := rng.Intn(len(nw.queue))
						m := nw.queue[i]
						nw.queue = append(nw.queue[:i], nw.queue[i+1:]...)
						if rng.Intn(10) > 0 {
							nw.deliver(m) // out of order; 10% dropped
						} else {
							nw.dropped++
						}
					}
				case p < 75:
					if ls := nw.leaders(); len(ls) > 0 {
						_ = nw.nodes[ls[0]].Propose([]byte(fmt.Sprintf("s%d.%d", seed, step)))
						nw.drain(ls[0])
					}
				case p < 85:
					id := ids[rng.Intn(len(ids))]
					if a := nw.nodes[id].AppliedIndex(); a > 0 {
						if b, _ := nw.nodes[id].Boundary(); a > b {
							nw.compact(id, a)
							snaps++
						}
					}
				case p < 90:
					nw.isolate(ids[rng.Intn(len(ids))])
				default:
					nw.heal()
				}
			}
			nw.heal()
			// Fair rounds until one leader accepts a final entry of its term, then
			// until every node has committed and applied the leader's whole log.
			var leader NodeID
			for round := 0; round < 400 && leader == ""; round++ {
				nw.tickAll()
				nw.deliverAll()
				if ls := nw.leaders(); len(ls) == 1 && round > 30 {
					if err := nw.nodes[ls[0]].Propose([]byte("final")); err == nil {
						nw.drain(ls[0])
						leader = ls[0]
					}
				}
			}
			for round := 0; round < 400 && leader != ""; round++ {
				nw.tickAll()
				nw.deliverAll()
				if nw.nodes[leader].Role() != Leader {
					break
				}
				if nw.converged(leader) {
					nw.requireConverged(leader)
					for _, id := range ids {
						installed += len(nw.snapshotsInstalled[id])
					}
					if snaps == 0 {
						t.Fatal("no compaction happened")
					}
					return
				}
			}
			t.Fatalf("did not converge after healing:\n%s", func() string {
				s := ""
				for _, id := range ids {
					s += nw.dumpLog(id) + "\n"
				}
				return s
			}())
		})
	}
}
