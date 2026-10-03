package integration

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/lab"
	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/multiraft"
)

// labStart is the integration launcher for a lab cluster's processes: they
// are race-scanned, killed when t ends and never outlive the test binary.
func labStart(t *testing.T) func(*exec.Cmd) error {
	return func(cmd *exec.Cmd) error { return startProc(t, cmd) }
}

// TestLabLeaderKillOnRealProcesses (#3): the lab runs a short leader-kill
// experiment on three real dkvd processes under a closed-loop load. The
// clients kept completing operations, none took effect twice (the only
// outcomes are known ones or unknown ones), the leader's death shows as a
// measured election, the killed node came back and caught up, and every
// node's usage was read from its own metrics.
func TestLabLeaderKillOnRealProcesses(t *testing.T) {
	bin := buildDkvd(t)
	e, err := lab.Scenario("leader-kill", lab.Experiment{
		// Ports from this binary's own range (testport.Integration, below every
		// ephemeral range): 29000 and up belong to internal/raftnode's tests,
		// which `go test ./...` runs concurrently with this package.
		Cluster: lab.ClusterConfig{Bin: bin, Mode: "raft", Nodes: 3, Tick: 25 * time.Millisecond, DataRoot: t.TempDir(), PortBase: 25000,
			Start: labStart(t)},
		Load: load.Config{Clients: 4, Duration: 6 * time.Second, Warmup: time.Second, ReadPct: 50, Keys: 200, Seed: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := lab.Run(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	if res.Load.Classes[load.ClassOK] == 0 {
		t.Fatalf("no operation succeeded: %v", res.Load.Classes)
	}
	if len(res.Actions) != 2 || res.Actions[0].Action.Kind != "kill-leader" || res.Actions[1].Action.Kind != "restart" {
		t.Fatalf("actions: %+v", res.Actions)
	}
	kill, restart := res.Actions[0], res.Actions[1]
	if kill.Err != "" || kill.Settle <= 0 || kill.Settle > 10*time.Second {
		t.Fatalf("the election after the kill: %+v", kill)
	}
	if restart.Err != "" || restart.Node != kill.Node {
		t.Fatalf("the killed leader's restart: %+v", restart)
	}
	if len(res.Nodes) != 3 {
		t.Fatalf("usage for %d nodes", len(res.Nodes))
	}
	for id, u := range res.Nodes {
		if u.CPUPerSec <= 0 || u.MaxRSSBytes <= 0 {
			t.Fatalf("%s: %+v", id, u)
		}
	}
	if !res.Nodes[kill.Node].Restarted {
		t.Fatalf("%s was restarted but its usage is not marked so", kill.Node)
	}
}

// TestALabProcessDiesWithItsTest: the lab starts every process — a restart's
// included — through the launcher it is given, so a test that ends without
// closing its cluster (a t.Fatal before Close) leaves nothing running. Before,
// the lab started dkvd itself, outside the integration launcher: such a test,
// or a timed-out test binary, left its cluster running, and a race report in
// its output failed nothing.
func TestALabProcessDiesWithItsTest(t *testing.T) {
	bin := buildDkvd(t)
	root := t.TempDir() // the parent's: removed after the processes are killed
	var c *lab.Cluster
	starts := 0
	t.Run("cluster", func(t *testing.T) {
		start := labStart(t)
		cfg := lab.ClusterConfig{Bin: bin, Mode: "raft", Nodes: 1, Tick: 25 * time.Millisecond, DataRoot: root, PortBase: 25000,
			Start: func(cmd *exec.Cmd) error { starts++; return start(cmd) }}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var err error
		if c, err = lab.Start(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		if err := c.Kill("n1"); err != nil {
			t.Fatal(err)
		}
		if err := c.Restart("n1"); err != nil {
			t.Fatal(err)
		}
		// Ends without c.Close(): the test's own cleanup must stop n1.
	})
	if c == nil {
		t.Fatal("the cluster never started")
	}
	if starts != 2 {
		c.Close()
		t.Fatalf("the lab started %d processes through its launcher, want 2 (the start and the restart)", starts)
	}
	deadline := time.Now().Add(10 * time.Second)
	for c.Node("n1").Alive() {
		if time.Now().After(deadline) {
			c.Close()
			t.Fatal("n1 outlived the test that started it")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// labGroupStatus reads node id's status of group 0.
func labGroupStatus(t *testing.T, c *lab.Cluster, id string) (multiraft.GroupStatus, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.Admin(ctx, id, multiraft.AdminRequest{Op: "status"})
	if err != nil {
		return multiraft.GroupStatus{}, err
	}
	for _, gs := range resp.Groups {
		if gs.Group == 0 {
			return gs, nil
		}
	}
	return multiraft.GroupStatus{}, fmt.Errorf("%s hosts no group 0", id)
}

// waitFollowing waits until node id reports following leader in a term of at
// least term.
func waitFollowing(t *testing.T, c *lab.Cluster, id, leader string, term uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		gs, err := labGroupStatus(t, c, id)
		if err == nil && gs.Role == "Follower" && gs.Leader == leader && gs.Term >= term {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never followed %s at term >= %d (last: %+v, %v)\n%s", id, leader, term, gs, err, c.Node(id).Output())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLabPartitionsAndPausesARealCluster: a lab cluster started with Links can
// be partitioned and frozen. Isolating the leader makes the other two elect a
// new one in a higher term; healing makes the old leader follow it. Freezing
// the new leader (SIGSTOP) does the same, and resuming it makes it follow.
// The cluster's Close — registered first, so it runs even when the test fails
// — kills the processes and closes the proxies.
func TestLabPartitionsAndPausesARealCluster(t *testing.T) {
	bin := buildDkvd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := lab.Start(ctx, lab.ClusterConfig{Bin: bin, Mode: "raft", Nodes: 3, Tick: 25 * time.Millisecond, DataRoot: t.TempDir(),
		PortBase: 25000, Start: labStart(t), Links: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	old, term, err := c.Leader(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Isolate(old); err != nil {
		t.Fatal(err)
	}
	if cuts := c.Cuts(); len(cuts) != 2 {
		t.Fatalf("isolating %s cut %v, want its two links", old, cuts)
	}
	next, _, err := c.WaitLeader(ctx, 0, old, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nt, err := labGroupStatus(t, c, next)
	if err != nil {
		t.Fatal(err)
	}
	if nt.Term <= term {
		t.Fatalf("%s leads in term %d, not above the isolated leader's %d", next, nt.Term, term)
	}
	c.HealAll()
	if cuts := c.Cuts(); len(cuts) != 0 {
		t.Fatalf("links still cut after HealAll: %v", cuts)
	}
	waitFollowing(t, c, old, next, nt.Term)

	if err := c.Pause(next); err != nil {
		t.Fatal(err)
	}
	third, _, err := c.WaitLeader(ctx, 0, next, 20*time.Second)
	if err != nil {
		_ = c.Resume(next)
		t.Fatal(err)
	}
	if err := c.Resume(next); err != nil {
		t.Fatal(err)
	}
	tt, err := labGroupStatus(t, c, third)
	if err != nil {
		t.Fatal(err)
	}
	waitFollowing(t, c, next, third, tt.Term)
}
