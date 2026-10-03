package health

import (
	"testing"

	"github.com/adivishall/quorum/internal/multiraft"
)

var voters3 = multiraft.ConfStatus{Voters: []multiraft.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}

// gs builds one node's status of group 0.
func gs(role string, term uint64, leader string, commit, applied, last uint64) multiraft.GroupStatus {
	return multiraft.GroupStatus{Group: 0, Role: role, Term: term, Leader: leader, Commit: commit, Applied: applied, LastIndex: last, Conf: voters3, Voter: true}
}

func probe(node string, groups ...multiraft.GroupStatus) Probe {
	return Probe{Node: node, Status: &multiraft.AdminResponse{OK: true, Node: node, Groups: groups}}
}

func down(node string) Probe { return Probe{Node: node, Err: "dial tcp: connection refused"} }

// leaderOf is n1 leading term 5 with both followers matched at 100.
func leaderOf(last uint64, match map[string]uint64) multiraft.GroupStatus {
	g := gs("Leader", 5, "n1", last, last, last)
	g.FollowerMatch = match
	return g
}

func TestHealthyCluster(t *testing.T) {
	h := Cluster([]Probe{
		probe("n1", leaderOf(100, map[string]uint64{"n2": 100, "n3": 99})),
		probe("n2", gs("Follower", 5, "n1", 100, 100, 100)),
		probe("n3", gs("Follower", 5, "n1", 99, 99, 99)),
	}, Options{})
	if h.Verdict != Healthy || len(h.Groups) != 1 || h.Groups[0].Leader != "n1" || h.Groups[0].Agreeing != 3 || h.Groups[0].Quorum != 2 {
		t.Fatalf("%+v", h)
	}
	for _, n := range h.Nodes {
		if n.Verdict != Ready || !n.Live {
			t.Fatalf("node %+v", n)
		}
	}
}

// TestLeaderLost: the leader is gone and no new one is elected yet. The
// survivors know no leader: neither is ready, and the group is unavailable —
// the process being alive says nothing about the cluster.
func TestLeaderLost(t *testing.T) {
	h := Cluster([]Probe{
		down("n1"),
		probe("n2", gs("Candidate", 6, "", 100, 100, 100)),
		probe("n3", gs("Follower", 6, "", 100, 100, 100)),
	}, Options{})
	if h.Verdict != Unavailable || h.Groups[0].Verdict != Unavailable {
		t.Fatalf("%+v", h)
	}
	for _, n := range h.Nodes {
		if n.Node != "n1" && (n.Verdict != NotReady || !n.Live) {
			t.Fatalf("a survivor with no leader is %+v", n)
		}
		if n.Node == "n1" && (n.Verdict != Unreachable || n.Live) {
			t.Fatalf("the dead node is %+v", n)
		}
	}
}

// TestStaleIsolatedLeader: n1 was cut off and still believes it leads term 5;
// n2 and n3 elected n2 in term 6. n1 is ready by its own view — the node view
// cannot see it — but the group view follows the majority: n2 leads, n1 is a
// stale voter, and the group is degraded, not healthy.
func TestStaleIsolatedLeader(t *testing.T) {
	newLeader := gs("Leader", 6, "n2", 120, 120, 120)
	newLeader.FollowerMatch = map[string]uint64{"n1": 100, "n3": 120}
	h := Cluster([]Probe{
		probe("n1", leaderOf(104, nil)),
		probe("n2", newLeader),
		probe("n3", gs("Follower", 6, "n2", 120, 120, 120)),
	}, Options{})
	g := h.Groups[0]
	if g.Verdict != Degraded || g.Leader != "n2" || g.Term != 6 || g.Agreeing != 2 {
		t.Fatalf("%+v", g)
	}
	if g.Voters[0].Node != "n1" || g.Voters[0].State != "stale" {
		t.Fatalf("the isolated old leader is %+v", g.Voters[0])
	}
	if n := Node(probe("n1", leaderOf(104, nil)), Options{}); n.Verdict != Ready {
		t.Fatalf("by its own state the isolated leader should look ready (what the node view cannot see): %+v", n)
	}
}

// TestRestartingNode: a node restarting is unreachable; the others still form
// a quorum: degraded, and the restarting node is not counted ready.
func TestRestartingNode(t *testing.T) {
	h := Cluster([]Probe{
		probe("n1", leaderOf(100, map[string]uint64{"n2": 100})),
		probe("n2", gs("Follower", 5, "n1", 100, 100, 100)),
		down("n3"),
	}, Options{})
	if h.Verdict != Degraded || h.Groups[0].Verdict != Degraded || h.Groups[0].Agreeing != 2 {
		t.Fatalf("%+v", h)
	}
	if h.Groups[0].Voters[2].State != Unreachable {
		t.Fatalf("%+v", h.Groups[0].Voters)
	}
}

// TestMajorityDown: two of three voters unreachable; the survivor still
// believes in its leader of term 5 — but no quorum confirms it.
func TestMajorityDown(t *testing.T) {
	h := Cluster([]Probe{down("n1"), down("n2"), probe("n3", gs("Follower", 5, "n1", 100, 100, 100))}, Options{})
	if h.Verdict != Unavailable || h.Groups[0].Verdict != Unavailable {
		t.Fatalf("%+v", h)
	}
	if h := Cluster([]Probe{down("n1"), down("n2")}, Options{}); h.Verdict != Unreachable {
		t.Fatalf("nothing answered: %+v", h)
	}
	// A leader no other voter confirms — cut off, its peers unreachable — is
	// not a quorum.
	lone := Cluster([]Probe{probe("n1", leaderOf(100, nil)), down("n2"), down("n3")}, Options{})
	if lone.Verdict != Unavailable || lone.Groups[0].Agreeing != 1 || lone.Groups[0].Leader != "n1" {
		t.Fatalf("a lone leader: %+v", lone.Groups[0])
	}
}

// TestLaggingFollower: a follower the leader holds 5,000 entries behind its
// last index is lagging: the group is degraded; with a looser bound, healthy.
func TestLaggingFollower(t *testing.T) {
	probes := []Probe{
		probe("n1", leaderOf(6000, map[string]uint64{"n2": 6000, "n3": 1000})),
		probe("n2", gs("Follower", 5, "n1", 6000, 6000, 6000)),
		probe("n3", gs("Follower", 5, "n1", 1000, 1000, 1000)),
	}
	h := Cluster(probes, Options{})
	if h.Verdict != Degraded || h.Groups[0].Voters[2].State != "lagging" || h.Groups[0].Voters[2].Lag != 5000 {
		t.Fatalf("%+v", h.Groups[0])
	}
	if h := Cluster(probes, Options{MaxFollowerLag: 10000}); h.Verdict != Healthy {
		t.Fatalf("within the bound: %+v", h)
	}
}

// TestNodeReadinessReasons: the cases a node is not ready for — each named.
func TestNodeReadinessReasons(t *testing.T) {
	behind := gs("Follower", 5, "n1", 1000, 100, 1000)
	if n := Node(probe("n2", behind), Options{}); n.Verdict != NotReady || len(n.Reasons) != 1 {
		t.Fatalf("applied far behind commit: %+v", n)
	}
	if n := Node(probe("n2", behind), Options{MaxApplyLag: 1000}); n.Verdict != Ready {
		t.Fatalf("within the apply bound: %+v", n)
	}
	joiner := gs("Follower", 0, "", 0, 0, 0)
	joiner.Conf = multiraft.ConfStatus{}
	if n := Node(probe("n4", joiner), Options{}); n.Verdict != NotReady {
		t.Fatalf("a joiner not yet added: %+v", n)
	}
	removed := gs("Follower", 5, "n1", 100, 100, 100)
	removed.Removed = true
	if n := Node(probe("n4", removed), Options{}); n.Verdict != NotReady || n.Reasons[0] != "hosts no group" {
		t.Fatalf("a node whose only group removed it: %+v", n)
	}
	failed := probe("n2", gs("Follower", 5, "n1", 100, 100, 100))
	failed.Status.Failed = map[string]string{"3": "corrupt log"}
	if n := Node(failed, Options{}); n.Verdict != NotReady {
		t.Fatalf("a group that failed to recover: %+v", n)
	}
	if n := Node(probe("n2"), Options{}); n.Verdict != NotReady {
		t.Fatalf("no group at all: %+v", n)
	}
	refused := Probe{Node: "n2", Status: &multiraft.AdminResponse{OK: false, Error: "busy"}}
	if n := Node(refused, Options{}); n.Verdict != NotReady || !n.Live {
		t.Fatalf("a refused status: %+v", n)
	}
}

// TestUnnamedVoterIsNotAssumedWell: a voter the caller did not probe is not
// counted healthy — health is never claimed for a node nobody asked.
func TestUnnamedVoterIsNotAssumedWell(t *testing.T) {
	h := Cluster([]Probe{
		probe("n1", leaderOf(100, map[string]uint64{"n2": 100, "n3": 100})),
		probe("n2", gs("Follower", 5, "n1", 100, 100, 100)),
	}, Options{})
	if h.Verdict != Degraded || h.Groups[0].Voters[2].State != Unreachable {
		t.Fatalf("%+v", h.Groups[0])
	}
}
