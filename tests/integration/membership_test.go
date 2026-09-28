package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/multiraft"
)

// Phase 15 real-process tests (docs/MEMBERSHIP.md, docs/MULTI_RAFT.md): dkvd
// processes in -cluster mode, driven through the admin protocol and the
// client protocol, over real TCP with per-link proxies for partitions.

// TestRealTwoGroupsOnThreeNodes (1, 2, 10): two groups on the same three
// processes, each electing its own leader, under concurrent session
// workloads against both; the client-visible history is linearizable, and
// both groups committed.
func TestRealTwoGroupsOnThreeNodes(t *testing.T) {
	c := newMCluster(t, 3, 0, 2)
	for g := 0; g < 2; g++ {
		c.waitLeaderOf(multiraft.GroupID(g))
	}
	w := c.startWorkload(4, 2, 1)
	w.waitProgress(80)
	w.finish()
	for g := 0; g < 2; g++ {
		l, _ := c.waitLeaderOf(multiraft.GroupID(g))
		if st := c.status(l)[multiraft.GroupID(g)]; st.Commit < 5 {
			t.Fatalf("group %d committed only %d entries\n%s", g, st.Commit, c.statusDump())
		}
	}
	for _, id := range c.genesis {
		if sts := c.status(id); len(sts) != 2 {
			t.Fatalf("%s hosts %d groups\n%s", id, len(sts), c.statusDump())
		}
	}
}

// TestRealAddAFourthNodeToOneGroup (3, 4): a fourth process joins group 0 only:
// started as a joiner, added as a learner — it catches up from the leader —
// then promoted by joint consensus; it never hosts group 1. Every key written
// before, and after, is readable through the cluster.
func TestRealAddAFourthNodeToOneGroup(t *testing.T) {
	c := newMCluster(t, 3, 1, 2)
	cl := c.sharded()
	for i := 0; i < 6; i++ {
		c.write(cl, c.keyIn(0, i), fmt.Sprintf("a%d", i))
		c.write(cl, c.keyIn(1, i), fmt.Sprintf("b%d", i))
	}
	c.startReady("n4", "-join", "0")
	resp := c.change(0, "add-learner", "n4", c.addrs["n4"])
	if resp.Conf == nil || len(resp.Conf.Learners) != 1 || resp.Conf.Learners[0].ID != "n4" {
		t.Fatalf("add-learner: %+v", resp)
	}
	c.waitCaughtUp(0, "n4")
	if st := c.status("n4")[0]; st.Voter || st.Role == "Leader" {
		t.Fatalf("a learner votes or leads: %+v", st)
	}
	c.change(0, "promote", "n4", "")
	c.waitConf(0, c.ids, isVoter("n4"))
	if _, hosts := c.status("n4")[1]; hosts {
		t.Fatalf("n4 hosts group 1\n%s", c.statusDump())
	}
	for i := 0; i < 6; i++ {
		c.write(cl, c.keyIn(0, 10+i), "after")
	}
	for i := 0; i < 6; i++ {
		if v, ok := c.read(cl, c.keyIn(0, i)); !ok || v != fmt.Sprintf("a%d", i) {
			t.Fatalf("%s = %q %v", c.keyIn(0, i), v, ok)
		}
		if v, ok := c.read(cl, c.keyIn(1, i)); !ok || v != fmt.Sprintf("b%d", i) {
			t.Fatalf("%s = %q %v", c.keyIn(1, i), v, ok)
		}
	}
}

// requireStable observes group g for a window of many election timeouts and
// requires its leader and term never to change, while every node in quiet —
// a removed or stale node — does not lead it; campaigners, if set, must have
// campaigned past the leader's term meanwhile (the premise that they tried).
func (c *mcluster) requireStable(g multiraft.GroupID, leader string, term uint64, campaigners ...string) {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second) // 6–12 election timeouts at 25ms ticks
	for time.Now().Before(deadline) {
		l, tm, ok := c.leaderOf(g)
		if ok && (l != leader || tm != term) {
			c.t.Fatalf("group %d's leader %s (term %d) was replaced by %s (term %d)\n%s", g, leader, term, l, tm, c.statusDump())
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, id := range campaigners {
		st, ok := c.status(id)[g]
		if ok && st.Term <= term {
			c.t.Fatalf("premise: %s never campaigned past term %d (it is at %d)\n%s", id, term, st.Term, c.statusDump())
		}
	}
}

// TestRealRemoveAFollowerAndRestartIt (A, D; 4, 9): a follower removed from
// group 0 stops participating — the group commits without it, and it receives
// nothing more; restarted from its own durable state, it may campaign on its
// stale configuration, and the group's leader is never disturbed.
func TestRealRemoveAFollowerAndRestartIt(t *testing.T) {
	c := newMCluster(t, 3, 0, 1)
	cl := c.sharded()
	c.write(cl, c.keyIn(0, 0), "x")
	l, _ := c.waitLeaderOf(0)
	var f string
	for _, id := range c.genesis {
		if id != l {
			f = id
			break
		}
	}
	c.change(0, "remove-voter", f, "")
	var rest []string
	for _, id := range c.genesis {
		if id != f {
			rest = append(rest, id)
		}
	}
	c.waitConf(0, rest, notMember(f))
	before := c.status(f)[0].Commit
	for i := 0; i < 10; i++ {
		c.write(cl, c.keyIn(0, 1+i), "y")
	}
	if st, ok := c.status(f)[0]; ok && st.Commit > before+1 {
		t.Fatalf("the removed %s kept committing: %d -> %d", f, before, st.Commit)
	}
	c.kill(f)
	c.startReady(f)
	l, tm := c.waitLeaderOf(0)
	c.requireStable(0, l, tm)
	c.write(cl, c.keyIn(0, 20), "z")
}

// TestRealRemoveTheLeader (B; 8): the leader removes itself: it leads the
// change to its end, and its host retires the group (event=group_retired);
// the remaining voters elect a leader and the group serves.
func TestRealRemoveTheLeader(t *testing.T) {
	c := newMCluster(t, 3, 0, 1)
	cl := c.sharded()
	c.write(cl, c.keyIn(0, 0), "x")
	l, _ := c.waitLeaderOf(0)
	c.change(0, "remove-voter", l, "")
	waitForLine(t, c.procs[l], "event=group_retired node="+l+" group=0 reason=removed", 30*time.Second)
	var rest []string
	for _, id := range c.genesis {
		if id != l {
			rest = append(rest, id)
		}
	}
	c.waitConf(0, rest, notMember(l))
	nl, _ := c.waitLeaderOf(0)
	if nl == l {
		t.Fatalf("the removed node leads again")
	}
	c.write(cl, c.keyIn(0, 1), "y")
	if v, ok := c.read(cl, c.keyIn(0, 0)); !ok || v != "x" {
		t.Fatalf("read after the removal: %q %v", v, ok)
	}
}

// TestRealReplaceACrashedNode (5, 9, 11): n3 dies for good; the spare n4
// joins both groups, is added and promoted, and n3 is removed — each step by
// joint consensus — while session clients keep working; then n3's stale
// process returns with its old durable state: it may campaign, and neither
// group's leader is ever disturbed. The history stays linearizable.
func TestRealReplaceACrashedNode(t *testing.T) {
	c := newMCluster(t, 3, 1, 2)
	w := c.startWorkload(3, 2, 7)
	w.waitProgress(30)
	c.kill("n3")
	c.startReady("n4", "-join", "0,1")
	for g := multiraft.GroupID(0); g < 2; g++ {
		c.change(g, "add-learner", "n4", c.addrs["n4"])
		c.waitCaughtUp(g, "n4")
		c.change(g, "promote", "n4", "")
		c.change(g, "remove-voter", "n3", "")
		c.waitConf(g, []string{"n1", "n2", "n4"}, func(cs multiraft.ConfStatus) bool {
			return isVoter("n4")(cs) && notMember("n3")(cs)
		})
	}
	w.waitProgress(30)
	c.startReady("n3")
	for g := multiraft.GroupID(0); g < 2; g++ {
		l, tm := c.waitLeaderOf(g)
		c.requireStable(g, l, tm)
		if l == "n3" {
			t.Fatalf("the replaced n3 leads group %d", g)
		}
	}
	w.waitProgress(30)
	w.finish()
}

// TestRealJointConfigurationSurvivesALeaderCrash (7): the promotion of n4 is
// held in its joint configuration — n4 and one old follower cut off, so the
// joint entry cannot reach a majority of the new voters — when the leader is
// SIGKILLed. Healed and restarted, the group elects a leader from the nodes
// that hold the joint entry, completes the transition by itself (its final
// entry), and serves.
func TestRealJointConfigurationSurvivesALeaderCrash(t *testing.T) {
	c := newMCluster(t, 3, 1, 1)
	cl := c.sharded()
	c.write(cl, c.keyIn(0, 0), "x")
	c.startReady("n4", "-join", "0")
	c.change(0, "add-learner", "n4", c.addrs["n4"])
	c.waitCaughtUp(0, "n4")
	l, _ := c.waitLeaderOf(0)
	var cut string
	for _, id := range c.genesis {
		if id != l {
			cut = id
			break
		}
	}
	c.isolate("n4")
	c.isolate(cut)
	promoted := make(chan multiraft.AdminResponse, 1)
	go func() {
		resp, _ := c.adminCall(l, multiraft.AdminRequest{Op: "promote", Group: 0, ID: "n4", Timeout: 20000})
		promoted <- resp
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if st, ok := c.status(l)[0]; ok && len(st.Conf.Outgoing) > 0 {
			break // the leader is in the joint configuration
		}
		if time.Now().After(deadline) {
			t.Fatalf("premise: the leader never entered the joint configuration\n%s", c.statusDump())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := c.status(l)[0]; !st.ConfPending || len(st.Conf.Outgoing) == 0 {
		t.Fatalf("premise: the joint configuration committed while half the new voters were cut off: %+v", st)
	}
	c.kill(l)
	c.healAll()
	c.startReady(l)
	c.waitConf(0, c.ids, isVoter("n4"))
	<-promoted
	c.write(cl, c.keyIn(0, 1), "y")
	if v, ok := c.read(cl, c.keyIn(0, 0)); !ok || v != "x" {
		t.Fatalf("read after the transition: %q %v", v, ok)
	}
}

// TestRealNewMemberCatchesUpBySnapshotAndTheClusterRestarts (6, 12): the
// group snapshots and compacts every few entries; a new member added after
// that can only catch up by installing a snapshot, which names it a learner;
// promoted, it votes. Then every process is SIGKILLed and restarted: every
// group recovers its configuration from its own files — the same everywhere —
// and every key is still there.
func TestRealNewMemberCatchesUpBySnapshotAndTheClusterRestarts(t *testing.T) {
	c := newMCluster(t, 3, 1, 2, "-snapshot-every", "12", "-snapshot-retain", "0")
	cl := c.sharded()
	for i := 0; i < 30; i++ {
		c.write(cl, c.keyIn(0, i%10), fmt.Sprintf("v%d", i))
	}
	c.write(cl, c.keyIn(1, 0), "one")
	l, _ := c.waitLeaderOf(0)
	if st := c.status(l)[0]; st.Snapshot == 0 || st.Boundary == 0 {
		t.Fatalf("premise: group 0's leader has not compacted: %+v", st)
	}
	c.startReady("n4", "-join", "0")
	c.change(0, "add-learner", "n4", c.addrs["n4"])
	func() {
		defer func() {
			if t.Failed() {
				t.Logf("n4's output:\n%s", c.procs["n4"].out.String())
			}
		}()
		c.waitCaughtUp(0, "n4")
	}()
	waitForLine(t, c.procs["n4"], "event=raft_snapshot_received node=n4", 10*time.Second)
	c.change(0, "promote", "n4", "")
	c.waitConf(0, c.ids, isVoter("n4"))
	c.write(cl, c.keyIn(0, 0), "final")
	want := map[string]multiraft.ConfStatus{}
	for _, id := range c.ids {
		for g, st := range c.status(id) {
			want[fmt.Sprintf("%s/%d", id, g)] = st.Conf
		}
	}
	for _, id := range c.ids {
		c.kill(id)
	}
	for _, id := range c.ids {
		c.startReady(id)
	}
	for g := multiraft.GroupID(0); g < 2; g++ {
		c.waitLeaderOf(g)
	}
	for _, id := range c.ids {
		for g, st := range c.status(id) {
			if got, was := st.Conf, want[fmt.Sprintf("%s/%d", id, g)]; fmt.Sprint(got) != fmt.Sprint(was) {
				t.Fatalf("%s group %d recovered configuration %+v, had %+v", id, g, got, was)
			}
		}
	}
	if v, ok := c.read(cl, c.keyIn(0, 0)); !ok || v != "final" {
		t.Fatalf("after the restart: %q %v", v, ok)
	}
	if v, ok := c.read(cl, c.keyIn(1, 0)); !ok || v != "one" {
		t.Fatalf("after the restart: %q %v", v, ok)
	}
}

// TestRealRemoveAPartitionedMember (C; 9): n3 is cut off from everyone and
// removed from both groups by the other two; healed, it campaigns on its stale
// configuration with inflated terms, and is refused without its terms being
// adopted — neither group's leader is disturbed.
func TestRealRemoveAPartitionedMember(t *testing.T) {
	c := newMCluster(t, 3, 0, 2)
	cl := c.sharded()
	c.write(cl, c.keyIn(0, 0), "x")
	c.write(cl, c.keyIn(1, 0), "y")
	// The node to cut off leads neither group: an isolated LEADER keeps
	// believing it leads (there is no check-quorum, docs/LIMITATIONS.md) and
	// never campaigns, so it could not show what this test is about.
	l0, _ := c.waitLeaderOf(0)
	l1, _ := c.waitLeaderOf(1)
	var x string
	var rest []string
	for _, id := range c.genesis {
		if x == "" && id != l0 && id != l1 {
			x = id
		} else {
			rest = append(rest, id)
		}
	}
	c.isolate(x)
	for g := multiraft.GroupID(0); g < 2; g++ {
		c.change(g, "remove-voter", x, "")
		c.waitConf(g, rest, notMember(x))
	}
	// x, alone, campaigns meanwhile, on its stale configuration.
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, tm := c.waitLeaderOf(0)
		if st, ok := c.status(x)[0]; ok && st.Term > tm {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("premise: the cut-off %s never campaigned\n%s", x, c.statusDump())
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.healAll()
	for g := multiraft.GroupID(0); g < 2; g++ {
		l, tm := c.waitLeaderOf(g)
		c.requireStable(g, l, tm, x)
		if l == x {
			t.Fatalf("the removed %s leads group %d", x, g)
		}
	}
	c.write(cl, c.keyIn(0, 1), "z")
}

// TestRealOneGroupBrokenTheOtherContinues is the isolation test: group 0 is
// stopped on two of its three nodes — no quorum — while group 1, on the very
// same processes and connections, keeps committing by its own quorum; group 0
// commits nothing meanwhile; restarted from its files, group 0 serves again.
func TestRealOneGroupBrokenTheOtherContinues(t *testing.T) {
	c := newMCluster(t, 3, 0, 2)
	cl := c.sharded()
	c.write(cl, c.keyIn(0, 0), "a")
	c.write(cl, c.keyIn(1, 0), "b")
	for _, id := range []string{"n2", "n3"} {
		if resp, err := c.adminCall(id, multiraft.AdminRequest{Op: "stop-group", Group: 0}); err != nil || !resp.OK {
			t.Fatalf("stop group 0 on %s: %v %+v", id, err, resp)
		}
	}
	commit1 := func() uint64 {
		l, _ := c.waitLeaderOf(1)
		return c.status(l)[1].Commit
	}
	before1 := commit1()
	before0 := c.status("n1")[0].Commit
	for i := 0; i < 10; i++ {
		c.write(cl, c.keyIn(1, 1+i), "more")
	}
	if after := commit1(); after < before1+10 {
		t.Fatalf("group 1 committed %d -> %d while group 0 was broken", before1, after)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	out := c.sharded().Put(ctx, []byte(c.keyIn(0, 1)), []byte("lost"), nil)
	cancel()
	if out.Err == nil {
		t.Fatal("group 0 accepted a write with one of its three replicas")
	}
	if after := c.status("n1")[0].Commit; after != before0 {
		t.Fatalf("group 0 committed without its quorum: %d -> %d", before0, after)
	}
	for _, id := range []string{"n2", "n3"} {
		resp, err := c.adminCall(id, multiraft.AdminRequest{Op: "start-group", Group: 0})
		if err != nil || !resp.OK {
			t.Fatalf("restart group 0 on %s: %v %+v", id, err, resp)
		}
	}
	c.write(cl, c.keyIn(0, 2), "back")
	if v, ok := c.read(cl, c.keyIn(0, 0)); !ok || v != "a" {
		t.Fatalf("group 0 after its recovery: %q %v", v, ok)
	}
	if !strings.Contains(c.procs["n2"].out.String(), "event=group_stopped node=n2 group=0") {
		t.Fatal("no group_stopped event")
	}
}

// TestRealMembershipChangesUnderASessionWorkload (11, 12): session clients
// write and read keys of both groups throughout a full membership cycle of
// group 0 — a learner added and promoted, a voter removed, the leader
// SIGKILLed and restarted mid-way — retrying unknown outcomes under their
// request identities. The client-visible history is linearizable.
func TestRealMembershipChangesUnderASessionWorkload(t *testing.T) {
	c := newMCluster(t, 3, 1, 2)
	w := c.startWorkload(4, 2, 11)
	w.waitProgress(40)
	c.startReady("n4", "-join", "0")
	c.change(0, "add-learner", "n4", c.addrs["n4"])
	w.waitProgress(20)
	c.change(0, "promote", "n4", "")
	w.waitProgress(20)
	l, _ := c.waitLeaderOf(0)
	c.kill(l)
	w.waitProgress(20)
	c.startReady(l)
	c.change(0, "remove-voter", "n1", "")
	w.waitProgress(40)
	var rest []string
	for _, id := range c.ids {
		if id != "n1" {
			rest = append(rest, id)
		}
	}
	c.waitConf(0, rest, notMember("n1"))
	w.finish()
}
