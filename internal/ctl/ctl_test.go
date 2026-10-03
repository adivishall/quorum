package ctl

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/health"
	"github.com/adivishall/quorum/internal/multiraft"
)

// fakeAdmin serves the admin protocol's JSON lines with a canned status (and
// answers snapshot with a fixed index), until the test ends.
func fakeAdmin(t *testing.T, status multiraft.AdminResponse) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					var req multiraft.AdminRequest
					_ = json.Unmarshal(sc.Bytes(), &req)
					resp := status
					if req.Op == "snapshot" {
						resp = multiraft.AdminResponse{OK: true, Index: 42}
					}
					b, _ := json.Marshal(resp)
					_, _ = c.Write(append(b, '\n'))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// deadAddr is an address nothing listens on.
func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

var conf3 = multiraft.ConfStatus{Voters: []multiraft.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}

func st(node, role, leader string, term uint64) multiraft.AdminResponse {
	g := multiraft.GroupStatus{Group: 0, Role: role, Term: term, Leader: leader, Commit: 50, Applied: 50, LastIndex: 50, Snapshot: 40, Conf: conf3, Voter: true}
	if role == "Leader" {
		g.FollowerMatch = map[string]uint64{}
		for _, id := range []string{"n1", "n2", "n3"} {
			if id != node {
				g.FollowerMatch[id] = 50
			}
		}
	}
	return multiraft.AdminResponse{OK: true, Node: node, Groups: []multiraft.GroupStatus{g}}
}

func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb strings.Builder
	code := Run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func healthyNodes(t *testing.T) string {
	return "n1=" + fakeAdmin(t, st("n1", "Leader", "n1", 4)) + ",n2=" + fakeAdmin(t, st("n2", "Follower", "n1", 4)) +
		",n3=" + fakeAdmin(t, st("n3", "Follower", "n1", 4))
}

func TestHealthyClusterCommands(t *testing.T) {
	nodes := healthyNodes(t)
	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{"status"}, []string{"NODE", "n1", "Leader", "n2", "Follower"}},
		{[]string{"leader"}, []string{"0", "n1", "3 of quorum 2", "healthy"}},
		{[]string{"groups"}, []string{"n1,n2,n3"}},
		{[]string{"members", "-group", "0"}, []string{"n1,n2,n3"}},
		{[]string{"lag"}, []string{"following", "APPLY LAG"}},
		{[]string{"health"}, []string{"cluster: healthy", "ready"}},
		{[]string{"ready", "n2"}, []string{"n2: ready"}},
		{[]string{"snapshot", "-group", "0"}, []string{"n1: group 0 snapshot at index 42"}},
	} {
		code, out, errs := run(t, append([]string{"-nodes", nodes}, c.args...)...)
		if code != ExitOK {
			t.Fatalf("%v: exit %d\n%s%s", c.args, code, out, errs)
		}
		for _, w := range c.want {
			if !strings.Contains(out, w) {
				t.Fatalf("%v: output lacks %q:\n%s", c.args, w, out)
			}
		}
	}
}

// TestHealthJSONIsTheVerdict: -json writes the structure the verdict was
// made from, and the exit code agrees with it.
func TestHealthJSONIsTheVerdict(t *testing.T) {
	code, out, _ := run(t, "-json", "-nodes", healthyNodes(t), "health")
	var h health.ClusterHealth
	if err := json.Unmarshal([]byte(out), &h); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if code != ExitOK || h.Verdict != health.Healthy || len(h.Groups) != 1 || h.Groups[0].Leader != "n1" || len(h.Nodes) != 3 {
		t.Fatalf("exit %d: %+v", code, h)
	}
	code, out, _ = run(t, "-json", "-nodes", healthyNodes(t), "status")
	var ss []struct {
		Node   string                   `json:"node"`
		Status *multiraft.AdminResponse `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &ss); err != nil || code != ExitOK || len(ss) != 3 || ss[0].Status.Groups[0].Commit != 50 {
		t.Fatalf("status json (exit %d, %v): %s", code, err, out)
	}
}

// TestUnhealthyStatesExitNonZero: a node down leaves the group degraded (1);
// two down, unavailable (3) — and `leader` says no leader is confirmed; a
// node that knows no leader is not ready (3); nothing answering is
// unreachable (4).
func TestUnhealthyStatesExitNonZero(t *testing.T) {
	oneDown := "n1=" + fakeAdmin(t, st("n1", "Leader", "n1", 4)) + ",n2=" + fakeAdmin(t, st("n2", "Follower", "n1", 4)) + ",n3=" + deadAddr(t)
	if code, out, _ := run(t, "-nodes", oneDown, "health"); code != ExitDegraded || !strings.Contains(out, "degraded") || !strings.Contains(out, "n3 is unreachable") {
		t.Fatalf("one node down: exit %d\n%s", code, out)
	}
	twoDown := "n1=" + deadAddr(t) + ",n2=" + deadAddr(t) + ",n3=" + fakeAdmin(t, st("n3", "Candidate", "", 9))
	if code, out, _ := run(t, "-nodes", twoDown, "health"); code != ExitUnavailable || !strings.Contains(out, "unavailable") {
		t.Fatalf("two nodes down: exit %d\n%s", code, out)
	}
	if code, out, _ := run(t, "-nodes", twoDown, "leader"); code != ExitUnavailable || !strings.Contains(out, "none confirmed") {
		t.Fatalf("leader with no quorum: exit %d\n%s", code, out)
	}
	if code, out, _ := run(t, "-admin", fakeAdmin(t, st("n3", "Candidate", "", 9)), "ready"); code != ExitUnavailable || !strings.Contains(out, "no leader known") {
		t.Fatalf("a node with no leader: exit %d\n%s", code, out)
	}
	dead := "n1=" + deadAddr(t) + ",n2=" + deadAddr(t)
	for _, cmd := range []string{"health", "status", "leader", "lag", "groups"} {
		if code, _, _ := run(t, "-nodes", dead, cmd); code != ExitUnreachable {
			t.Fatalf("%s with nothing answering: exit %d", cmd, code)
		}
	}
	if code, out, _ := run(t, "-admin", deadAddr(t), "ready"); code != ExitUnreachable || !strings.Contains(out, "unreachable") {
		t.Fatalf("ready on a dead node: exit %d\n%s", code, out)
	}
}

func TestUsageErrors(t *testing.T) {
	nodes := healthyNodes(t)
	for _, args := range [][]string{
		{},
		{"status"},
		{"-nodes", nodes},
		{"-nodes", nodes, "frobnicate"},
		{"-nodes", "n1", "status"},
		{"-nodes", nodes, "-admin", "x", "status"},
		{"-nodes", nodes, "members"},
		{"-nodes", nodes, "ready"},
		{"-nodes", nodes, "ready", "n9"},
		{"-nodes", nodes, "snapshot"},
		{"-nodes", nodes, "status", "extra"},
		{"-timeout", "0s", "-nodes", nodes, "status"},
	} {
		if code, out, errs := run(t, args...); code != ExitUsage {
			t.Fatalf("%v: exit %d, want %d\n%s%s", args, code, ExitUsage, out, errs)
		}
	}
}
