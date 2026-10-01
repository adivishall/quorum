package raftnode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/raft"
)

// Phase 16 (docs/OBSERVABILITY.md): the driver's metrics against ground truth —
// what the test itself did, what each node's Status and files say. No
// assertion depends on which node leads when: where it matters, the premise is
// read from the nodes.

func scrape(t *testing.T, r *metrics.Registry) metrics.Samples {
	t.Helper()
	var b bytes.Buffer
	if err := r.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	ss, err := metrics.Parse(&b)
	if err != nil {
		t.Fatal(err)
	}
	return ss
}

func metricOf(t *testing.T, r *metrics.Registry, name string, match ...string) float64 {
	t.Helper()
	return scrape(t, r).Sum(name, match...)
}

// waitMetric waits until the metric satisfies ok: counts published by one
// goroutine are read by another, and a write's reply can precede its
// apply-latency observation by one actor cycle.
func waitMetric(t *testing.T, r *metrics.Registry, what, name string, ok func(float64) bool, match ...string) float64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var v float64
	for time.Now().Before(deadline) {
		if v = metricOf(t, r, name, match...); ok(v) {
			return v
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s: %s = %v", what, name, v)
	return v
}

// startMetered is startSnapCluster whose n nodes each run with a registry of
// their own, as each would in its own process.
func startMetered(t *testing.T, ctx context.Context, n int, every, retain uint64) *snapCluster {
	t.Helper()
	c := startSnapCluster(t, ctx, 0, every, retain)
	c.withMetrics()
	for i := 0; i < n; i++ {
		id := NodeID(fmt.Sprintf("n%d", i))
		c.ids = append(c.ids, id)
		c.addrs[id] = freeAddr(t)
		c.logs[id] = &strings.Builder{}
	}
	c.genesis = append([]NodeID(nil), c.ids...)
	for _, id := range c.ids {
		c.start(id, nil)
	}
	return c
}

// TestRoleGaugesShowOneLeader: every node reports exactly one role, and — in a
// check during which no node changed term — exactly one node reports leader,
// the one whose Status says so, which counts an election won.
func TestRoleGaugesShowOneLeader(t *testing.T) {
	c := startMetered(t, context.Background(), 3, 0, 0)
	c.converged(c.ids)
	for attempt := 0; ; attempt++ {
		terms := map[NodeID]uint64{}
		for _, id := range c.ids {
			terms[id] = c.nodes[id].Status().Term
		}
		var leaders []NodeID
		for _, id := range c.ids {
			r := c.regs[id]
			if f := metricOf(t, r, "dkv_raft_role", "group", "0"); f != 1 {
				t.Fatalf("%s: the role gauges sum to %v, want exactly one role", id, f)
			}
			if metricOf(t, r, "dkv_raft_role", "group", "0", "role", "leader") == 1 {
				leaders = append(leaders, id)
			}
		}
		stable := true
		for _, id := range c.ids {
			stable = stable && c.nodes[id].Status().Term == terms[id]
		}
		if !stable {
			if attempt == 2 {
				t.Fatal("premise: an election ran during every check")
			}
			continue // premise voided: elections ran during the check
		}
		if len(leaders) != 1 || c.nodes[leaders[0]].Role() != raft.Leader {
			t.Fatalf("role gauges name leaders %v", leaders)
		}
		if metricOf(t, c.regs[leaders[0]], "dkv_raft_elections_won_total", "group", "0") < 1 {
			t.Fatal("the leader counts no election won")
		}
		for _, id := range c.ids {
			if metricOf(t, c.regs[id], "dkv_raft_has_leader", "group", "0") != 1 {
				t.Fatalf("%s knows no leader", id)
			}
		}
		return
	}
}

// TestMetricsMatchWhatTheClusterDid drives a three-node group — writes, a
// stopped follower, snapshots and compaction, the leader stopped and
// replaced, the follower back by snapshot — and checks each metric against
// what happened, read independently.
func TestMetricsMatchWhatTheClusterDid(t *testing.T) {
	ctx := context.Background()
	c := startMetered(t, ctx, 3, 20, 2)
	reg := func(id NodeID) *metrics.Registry { return c.regs[id] }
	sumOver := func(name string, match ...string) float64 {
		var total float64
		for _, id := range c.ids {
			total += metricOf(t, reg(id), name, append([]string{"group", "0"}, match...)...)
		}
		return total
	}

	// Writes: every one accepted or refused is counted so; each write that
	// completed has its commit and its apply latency observed exactly once.
	accepted, refused, succeeded := 0, 0, 0
	for i := 0; i < 30; i++ {
		for {
			_, _, _, err := c.nodes[c.leader(c.ids)].Write(ctx, []byte(fmt.Sprintf("w%d", i)))
			if errors.Is(err, raft.ErrNotLeader) {
				refused++
				continue
			}
			accepted++
			if err == nil {
				succeeded++
				break
			}
		}
	}
	if a, r := sumOver("dkv_raft_proposals_total", "result", "accepted"), sumOver("dkv_raft_proposals_total", "result", "refused"); int(a) != accepted || int(r) != refused {
		t.Fatalf("proposals counted accepted %v refused %v; the test saw %d and %d", a, r, accepted, refused)
	}
	if n := sumOver("dkv_raft_commit_seconds_count"); int(n) != succeeded {
		t.Fatalf("commit latency observed %v times for %d writes that committed", n, succeeded)
	}
	deadline := time.Now().Add(10 * time.Second)
	for int(sumOver("dkv_raft_apply_seconds_count")) != succeeded && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := sumOver("dkv_raft_apply_seconds_count"); int(n) != succeeded {
		t.Fatalf("apply latency observed %v times for %d applied writes", n, succeeded)
	}

	c.converged(c.ids)
	for _, id := range c.ids {
		st := c.nodes[id].Status()
		if n := metricOf(t, reg(id), "dkv_raft_persisted_entries_total", "group", "0"); n < float64(succeeded) {
			t.Fatalf("%s persisted %v entries, fewer than the %d writes it holds", id, n, succeeded)
		}
		if metricOf(t, reg(id), "dkv_raft_persist_seconds_count", "group", "0") < 1 {
			t.Fatalf("%s timed no Save", id)
		}
		if v := metricOf(t, reg(id), "dkv_raft_applied_index", "group", "0"); v != float64(st.Applied) {
			t.Fatalf("%s: applied gauge %v, Status %d", id, v, st.Applied)
		}
		// Snapshots every 20 entries: each node published at least one, and
		// counted and timed each one it published.
		if st.Snapshot == 0 {
			t.Fatalf("%s published no snapshot", id)
		}
		if v := metricOf(t, reg(id), "dkv_raft_snapshot_index", "group", "0"); v != float64(st.Snapshot) {
			t.Fatalf("%s: snapshot gauge %v, Status %d", id, v, st.Snapshot)
		}
		created := metricOf(t, reg(id), "dkv_raft_snapshots_created_total", "group", "0", "trigger", "periodic")
		if timed := metricOf(t, reg(id), "dkv_raft_snapshot_create_seconds_count", "group", "0"); created < 1 || created != timed {
			t.Fatalf("%s: %v snapshots counted, %v timed", id, created, timed)
		}
		// The log's size is its file's length (quiescent: nothing is saved
		// between two heartbeats of a converged group).
		info, err := os.Stat(filepath.Join(c.dir, string(id)+".log"))
		if err != nil {
			t.Fatal(err)
		}
		if v := metricOf(t, reg(id), "dkv_raft_log_bytes", "group", "0"); v != float64(info.Size()) {
			t.Fatalf("%s: log bytes %v, the file holds %d", id, v, info.Size())
		}
	}

	// Lag: a stopped follower falls behind whatever leads by at least what was
	// appended since it stopped (a new leader starts it at match 0).
	ld := c.leader(c.ids)
	down := without(c.ids, ld)[0]
	waitMetric(t, reg(ld), "the caught-up follower", "dkv_raft_follower_lag_entries", func(v float64) bool { return v == 0 }, "group", "0", "peer", string(down))
	c.down(down)
	up := without(c.ids, down)
	before := c.nodes[ld].Status().LastIndex
	c.propose(up, cmds("b", 45)) // past two snapshots: down can catch up only by one
	cur := c.leader(up)
	after := c.nodes[cur].Status().LastIndex
	if v := metricOf(t, reg(cur), "dkv_raft_follower_lag_entries", "group", "0", "peer", string(down)); v < float64(after-before) {
		t.Fatalf("the stopped %s lags %v entries on %s; %d were appended since it stopped", down, v, cur, after-before)
	}
	c.compactedPast(up, before+10)

	// A new leader: the leader stops; the other survivor sees a new leader.
	ld = c.leader(up)
	survivor := without(up, ld)[0]
	changesBefore := metricOf(t, reg(survivor), "dkv_raft_leader_changes_total", "group", "0")
	wonBefore := metricOf(t, reg(survivor), "dkv_raft_elections_won_total", "group", "0")
	c.down(ld)
	c.start(down, nil) // with the survivor, a quorum
	nl := c.leader([]NodeID{survivor, down})
	c.start(ld, nil)
	c.propose(c.ids, cmds("c", 3))
	c.converged(c.ids)
	if v := metricOf(t, reg(survivor), "dkv_raft_leader_changes_total", "group", "0"); v <= changesBefore {
		t.Fatalf("the survivor saw %s lead a new term, but its count of leaders stayed %v", nl, v)
	}
	if nl == survivor {
		if v := metricOf(t, reg(survivor), "dkv_raft_elections_won_total", "group", "0"); v < wonBefore+1 {
			t.Fatalf("the survivor won: elections won %v -> %v", wonBefore, v)
		}
	}
	// The survivor never restarted: its counts are exactly its core's, which
	// counted each transition as it happened — published many times, counted
	// once.
	cc := c.nodes[survivor].Status().Counters
	for name, want := range map[string]uint64{
		"dkv_raft_campaigns_total": cc.Campaigns, "dkv_raft_elections_won_total": cc.ElectionsWon,
		"dkv_raft_leader_stepdowns_total": cc.StepDowns,
	} {
		if v := metricOf(t, reg(survivor), name, "group", "0"); v != float64(want) {
			t.Fatalf("%s: %s %v, its core counted %d", survivor, name, v, want)
		}
	}
	// The returning follower was behind the compaction: it installed a
	// snapshot, counted and timed once per install, and some node counted
	// the transfer sent.
	installed := metricOf(t, reg(down), "dkv_raft_snapshots_installed_total", "group", "0")
	if timed := metricOf(t, reg(down), "dkv_raft_snapshot_install_seconds_count", "group", "0"); installed < 1 || installed != timed {
		t.Fatalf("%s: %v installs counted, %v timed", down, installed, timed)
	}
	if sumOver("dkv_raft_snapshot_transfers_total", "result", "sent") < 1 {
		t.Fatal("a snapshot was installed but no transfer was counted sent")
	}
}

// TestMembershipMetrics: a learner added and promoted — each change counted
// completed once, by type; the joint configuration is counted adopted by the
// quorum that committed it, and every member ends in the final one with four
// voters.
func TestMembershipMetrics(t *testing.T) {
	c := startMetered(t, context.Background(), 3, 0, 0)
	genesis := append([]NodeID(nil), c.ids...)
	c.join("n3")
	c.change(genesis, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "n3"}})
	c.change(genesis, raft.ConfChange{Type: raft.Promote, Member: raft.Member{ID: "n3"}})
	count := func(typ string) float64 {
		var n float64
		for _, id := range genesis {
			n += metricOf(t, c.regs[id], "dkv_raft_membership_changes_total", "group", "0", "type", typ, "result", "completed")
		}
		return n
	}
	if count("add_learner") != 1 || count("promote") != 1 {
		t.Fatalf("completed changes counted: add_learner %v, promote %v; want 1 each", count("add_learner"), count("promote"))
	}
	for _, id := range c.ids {
		waitMetric(t, c.regs[id], string(id)+" has four voters", "dkv_raft_voters", func(v float64) bool { return v == 4 }, "group", "0")
		waitMetric(t, c.regs[id], string(id)+" is out of the joint configuration", "dkv_raft_configuration_joint", func(v float64) bool { return v == 0 }, "group", "0")
	}
	// Three configuration entries were appended: the learner's addition
	// (stable), the joint configuration and the final one (stable). A
	// configuration takes effect when appended, and each entry is proposed
	// only once the previous one is committed — the joint entry by a majority
	// of the new configuration, three of its four voters, each counting it in
	// the cycle that persisted it. A member outside a commit's majority can
	// receive that entry and the next in one batch, and counts only the later
	// one. So: at least three joint adoptions in all, at most one per member,
	// and every member counted the final configuration and at most the two
	// stable ones.
	var joint float64
	for _, id := range c.ids {
		j := metricOf(t, c.regs[id], "dkv_raft_configurations_total", "group", "0", "kind", "joint")
		s := metricOf(t, c.regs[id], "dkv_raft_configurations_total", "group", "0", "kind", "stable")
		if j > 1 || s < 1 || s > 2 {
			t.Fatalf("%s counted %v joint and %v stable configurations adopted; want at most 1 and 1 or 2", id, j, s)
		}
		joint += j
	}
	if joint < 3 {
		t.Fatalf("%v joint adoptions counted across the four members; the quorum that committed the joint entry is three", joint)
	}
}

// TestNodesWithoutMetricsAreUnchanged: a node started with no Metrics runs as
// before — it registers nothing and follows no write.
func TestNodesWithoutMetricsAreUnchanged(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 1, 0, 0)
	n := c.nodes[c.leader(c.ids)]
	if n.m.enabled() || n.store != Storage(n.dur) {
		t.Fatal("a node without Metrics wraps its storage")
	}
	if _, _, _, err := n.Write(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if len(n.inflight) != 0 {
		t.Fatal("a node without Metrics followed a write")
	}
}
