package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/ctl"
	"github.com/adivishall/quorum/internal/health"
	"github.com/adivishall/quorum/internal/lab"
)

// dkvctl runs the operator tool in-process against the cluster's admin ports.
func dkvctl(c *lab.Cluster, args ...string) (int, string) {
	var nodes []string
	for _, n := range c.Nodes() {
		if !n.Spare {
			nodes = append(nodes, n.ID+"="+n.AdminAddr)
		}
	}
	var out, errs strings.Builder
	code := ctl.Run(context.Background(), append([]string{"-nodes", strings.Join(nodes, ",")}, args...), &out, &errs)
	return code, out.String() + errs.String()
}

// waitDkvctl runs dkvctl until it exits with want, and returns its last output.
func waitDkvctl(t *testing.T, c *lab.Cluster, want int, args ...string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		code, out := dkvctl(c, args...)
		if code == want {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("dkvctl %v: exit %d, want %d\n%s", args, code, want, out)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func clusterHealth(t *testing.T, c *lab.Cluster) health.ClusterHealth {
	t.Helper()
	_, out := dkvctl(c, "-json", "health")
	var h health.ClusterHealth
	if err := json.Unmarshal([]byte(out), &h); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return h
}

// TestDkvctlOnARealCluster: dkvctl reads real dkvd processes and its verdicts
// follow what is done to them. Healthy: every node ready, the group healthy,
// the leader the lab sees. An isolated leader: the majority elects another,
// and the group is degraded with the old leader stale — while the old leader,
// by its own state, still looks ready (what a node-local check cannot see).
// Healed: healthy again. A restarting node: unreachable, then ready again.
// Two of three nodes down: the group unavailable, the survivor not ready.
func TestDkvctlOnARealCluster(t *testing.T) {
	bin := buildDkvd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := lab.Start(ctx, lab.ClusterConfig{Bin: bin, Mode: "raft", Nodes: 3, Tick: 25 * time.Millisecond, DataRoot: t.TempDir(),
		PortBase: 25000, Start: labStart(t), Links: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	// Healthy.
	out := waitDkvctl(t, c, ctl.ExitOK, "health")
	if !strings.Contains(out, "cluster: healthy") {
		t.Fatalf("%s", out)
	}
	leader, term, err := c.Leader(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if code, out := dkvctl(c, "leader"); code != ctl.ExitOK || !strings.Contains(out, leader) {
		t.Fatalf("leader: exit %d\n%s", code, out)
	}
	for _, n := range []string{"n1", "n2", "n3"} {
		if code, out := dkvctl(c, "ready", n); code != ctl.ExitOK {
			t.Fatalf("ready %s: exit %d\n%s", n, code, out)
		}
	}
	if code, out := dkvctl(c, "status"); code != ctl.ExitOK || strings.Count(out, "Follower") != 2 || !strings.Contains(out, "Leader") {
		t.Fatalf("status: exit %d\n%s", code, out)
	}

	// An isolated leader: degraded, with the old leader stale in the group's
	// view and ready by its own.
	if err := c.Isolate(leader); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.WaitLeader(ctx, 0, leader, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	waitDkvctl(t, c, ctl.ExitDegraded, "health")
	h := clusterHealth(t, c)
	g := h.Groups[0]
	if g.Verdict != health.Degraded || g.Leader == leader || g.Term <= term {
		t.Fatalf("after isolating %s (term %d): %+v", leader, term, g)
	}
	for _, v := range g.Voters {
		if v.Node == leader && v.State != "stale" {
			t.Fatalf("the isolated old leader is %+v, want stale", v)
		}
	}
	if code, out := dkvctl(c, "ready", leader); code != ctl.ExitOK {
		t.Logf("the isolated leader already stepped down (exit %d): %s", code, out)
	}

	// Healed.
	c.HealAll()
	waitDkvctl(t, c, ctl.ExitOK, "health")

	// A restarting node.
	follower := ""
	for _, n := range []string{"n1", "n2", "n3"} {
		if gs, err := labGroupStatus(t, c, n); err == nil && gs.Role == "Follower" {
			follower = n
			break
		}
	}
	if follower == "" {
		t.Fatal("no follower")
	}
	if err := c.Kill(follower); err != nil {
		t.Fatal(err)
	}
	if code, out := dkvctl(c, "ready", follower); code != ctl.ExitUnreachable {
		t.Fatalf("ready on a killed node: exit %d\n%s", code, out)
	}
	if code, out := dkvctl(c, "health"); code != ctl.ExitDegraded {
		t.Fatalf("health with one node down: exit %d\n%s", code, out)
	}
	if err := c.Restart(follower); err != nil {
		t.Fatal(err)
	}
	waitDkvctl(t, c, ctl.ExitOK, "ready", follower)
	waitDkvctl(t, c, ctl.ExitOK, "health")

	// Two of three down: unavailable; the survivor learns it has no leader.
	cur, _, err := c.Leader(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	survivor := ""
	for _, n := range []string{"n1", "n2", "n3"} {
		if n == cur {
			continue
		}
		if survivor == "" {
			survivor = n
			continue
		}
		if err := c.Kill(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Kill(cur); err != nil {
		t.Fatal(err)
	}
	waitDkvctl(t, c, ctl.ExitUnavailable, "health")
	out = waitDkvctl(t, c, ctl.ExitUnavailable, "ready", survivor)
	if !strings.Contains(out, "no leader known") {
		t.Fatalf("the survivor is not ready, but not because it lost its leader:\n%s", out)
	}
}
