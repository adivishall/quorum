package integration

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRealImpostorsNeverJoinTheGroup (audit H2, H5): a running three-node
// group loses n3, and other processes come up at n3's address under n3's id —
// a node of another cluster (a recycled address, a copied unit file), then a
// new node of this cluster whose replica settings differ. Before the
// handshake carried the cluster id and the settings digest, each was heard as
// n3: it held none of n3's state, yet its votes and acknowledgements counted
// as n3's, and its entries were decided under other settings. Now n1 and n2
// refuse every connection to it, both sides name why, and it never takes part
// in an election. The real n3, restarted, rejoins as before.
func TestRealImpostorsNeverJoinTheGroup(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitSettled(30 * time.Second)
	c.kill("n3")
	// The survivors' leadership once n3 is gone (it may have led): no impostor
	// may change it.
	leader, term := c.waitLeader([]string{"n1", "n2"}, 0, 20*time.Second)
	c.waitFollows(other(leader), leader, term, 20*time.Second)
	realDir := c.dirs["n3"]

	for i, tc := range []struct {
		name   string
		flags  []string
		reason string
	}{
		{"another cluster", []string{"-init", "-cluster-id", newClusterID()}, "another cluster"},
		{"other replica settings", []string{"-init", "-cluster-id", c.cluster, "-session-max", "7"}, "replica settings differ"},
	} {
		connects := map[string]int{}
		for _, id := range []string{"n1", "n2"} {
			connects[id] = strings.Count(c.procs[id].out.String(), "event=peer_connected peer=n3")
		}
		c.dirs["n3"] = filepath.Join(t.TempDir(), "impostor")
		c.launch("n3", tc.flags)
		imp := c.procs["n3"]
		// n1 and n2 dial n3 every 500 ms; give them several attempts.
		waitForLine(t, imp, "event=raft_started", 20*time.Second)
		time.Sleep(3 * time.Second)
		for _, id := range []string{"n1", "n2"} {
			out := c.procs[id].out.String()
			if n := strings.Count(out, "event=peer_connected peer=n3"); n != connects[id] {
				t.Fatalf("%s: %s connected to the impostor\n%s", tc.name, id, c.outputs())
			}
			if !strings.Contains(out, tc.reason) {
				t.Fatalf("%s: %s's log does not name %q\n%s", tc.name, id, tc.reason, out)
			}
		}
		out := imp.out.String()
		if strings.Contains(out, "event=peer_connected") || strings.Contains(out, "event=raft_leader") {
			t.Fatalf("%s: the impostor connected or led\n%s", tc.name, out)
		}
		if strings.Count(out, tc.reason) < 2 {
			t.Fatalf("%s: the impostor's log does not name %q for each refused dial\n%s", tc.name, tc.reason, out)
		}
		c.kill("n3")
		// The survivors still lead and follow in the term they settled in:
		// no vote request of the impostor's reached them.
		if l, tm, ok := c.latestLeader([]string{"n1", "n2"}); !ok || l != leader || tm != term {
			t.Fatalf("%s (round %d): the survivors' leadership changed from %s@%d to %s@%d\n%s", tc.name, i, leader, term, l, tm, c.outputs())
		}
	}

	// The real n3 rejoins: the three settle on one leader — whichever wins;
	// a restarted node may campaign before it hears the leader — with every
	// link up.
	c.dirs["n3"] = realDir
	c.startPlain("n3")
	c.waitSettled(30 * time.Second)
}

// other is the survivor that is not id.
func other(id string) string {
	if id == "n1" {
		return "n2"
	}
	return "n1"
}
