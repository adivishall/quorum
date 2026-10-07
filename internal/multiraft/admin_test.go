package multiraft

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

// serveAdmin starts id's admin listener and returns its address.
func (c *hostCluster) serveAdmin(id NodeID) string {
	c.t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.t.Cleanup(cancel)
	go ServeAdmin(ctx, ln, c.hosts[id], nil)
	return ln.Addr().String()
}

func call(t *testing.T, addr string, req AdminRequest) AdminResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := AdminCall(ctx, addr, req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestAdminDrivesMembershipThroughTheLog: status reports every hosted group;
// a membership operation on a follower is refused with the leader's name; on
// the leader it completes and reports the configuration the log reached; a
// joiner is created through its own admin listener; stop-group stops hosting;
// errors are reported, never guessed around.
func TestAdminDrivesMembershipThroughTheLog(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c", "d")
	for _, id := range []NodeID{"a", "b", "c"} {
		c.startHost(id, false)
	}
	c.startHost("d", true)
	c.create(1, "a", "b", "c")
	c.create(2, "a", "b", "c")
	admin := map[NodeID]string{}
	for _, id := range c.ids {
		admin[id] = c.serveAdmin(id)
	}
	c.write(1, "one-0", "a", "b", "c")
	st := call(t, admin["a"], AdminRequest{Op: "status"})
	if !st.OK || len(st.Groups) != 2 || st.Groups[0].Group != 1 || len(st.Groups[0].Conf.Voters) != 3 {
		t.Fatalf("status: %+v", st)
	}
	ld := c.leader(1, "a", "b", "c")
	var follower NodeID
	for _, id := range []NodeID{"a", "b", "c"} {
		if id != ld {
			follower = id
			break
		}
	}
	if r := call(t, admin["d"], AdminRequest{Op: "create-group", Group: 1, Join: true}); !r.OK {
		t.Fatalf("create-group join on d: %+v", r)
	}
	r := call(t, admin[follower], AdminRequest{Op: "add-learner", Group: 1, ID: "d", Addr: c.addrs["d"]})
	if r.OK || r.Leader != string(ld) || !strings.Contains(r.Error, "not leader") {
		t.Fatalf("add-learner on a follower: %+v", r)
	}
	r = call(t, admin[ld], AdminRequest{Op: "add-learner", Group: 1, ID: "d", Addr: c.addrs["d"]})
	if !r.OK || r.Conf == nil || len(r.Conf.Learners) != 1 || r.Conf.Learners[0].ID != "d" {
		t.Fatalf("add-learner on the leader: %+v", r)
	}
	r = call(t, admin[ld], AdminRequest{Op: "promote", Group: 1, ID: "d"})
	if !r.OK || len(r.Conf.Voters) != 4 || len(r.Conf.Outgoing) != 0 {
		t.Fatalf("promote: %+v", r)
	}
	c.write(1, "one-1", c.ids...)
	c.applied(1, []string{"one-0", "one-1"}, c.ids...)
	for name, req := range map[string]AdminRequest{
		"no such group":     {Op: "promote", Group: 9, ID: "d"},
		"promote a voter":   {Op: "promote", Group: 1, ID: "d"},
		"learner, no addr":  {Op: "add-learner", Group: 1, ID: "e"},
		"no id":             {Op: "remove-voter", Group: 1},
		"unknown op":        {Op: "rebalance", Group: 1},
		"create both":       {Op: "create-group", Group: 3, Join: true, Voters: []Member{{ID: "a", Addr: "h:1"}}},
		"create a running":  {Op: "create-group", Group: 1, Join: true},
		"snapshot, no snap": {Op: "snapshot", Group: 1},
	} {
		addr := admin[c.leader(1, c.ids...)]
		if name == "create a running" {
			addr = admin["d"]
		}
		if r := call(t, addr, req); r.OK || r.Error == "" {
			t.Errorf("%s: %+v", name, r)
		}
	}
	if r := call(t, admin["d"], AdminRequest{Op: "stop-group", Group: 1}); !r.OK || c.hosts["d"].Group(1) != nil {
		t.Fatalf("stop-group: %+v", r)
	}
	// start-group recovers it from its own files, as the member it now is.
	if r := call(t, admin["d"], AdminRequest{Op: "start-group", Group: 1}); !r.OK || c.hosts["d"].Group(1) == nil {
		t.Fatalf("start-group: %+v", r)
	}
	if st := c.hosts["d"].Group(1).Node.Status(); !st.Conf.IsVoter("d") {
		t.Fatalf("d restarted with %s", st.Conf)
	}
	if r := call(t, admin["d"], AdminRequest{Op: "start-group", Group: 1}); r.OK {
		t.Fatal("start-group of a running group succeeded")
	}
	if r := call(t, admin["d"], AdminRequest{Op: "start-group", Group: 7}); r.OK {
		t.Fatal("start-group of a group with no files succeeded")
	}
	// A malformed line is answered, and the connection stays usable.
	conn, err := net.Dial("tcp", admin["a"])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for _, line := range []string{"{not json\n", `{"op":"status"}` + "\n"} {
		if _, err := conn.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
		out, err := rd.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		var resp AdminResponse
		if err := json.Unmarshal([]byte(out), &resp); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "{not") != !resp.OK {
			t.Fatalf("%q answered %+v", line, resp)
		}
	}
}

// TestAdminStatusNamesItsNodeAndTheLeadersFollowers: status says which node
// answered; on a group's leader it reports each other member's match index —
// once the group is quiet, every follower's reaches the leader's last index
// (the lag an operator reads is their difference) — and on a follower it
// reports none.
func TestAdminStatusNamesItsNodeAndTheLeadersFollowers(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c")
	for _, id := range c.ids {
		c.startHost(id, false)
	}
	c.create(1, "a", "b", "c")
	admin := map[NodeID]string{}
	for _, id := range c.ids {
		admin[id] = c.serveAdmin(id)
	}
	c.write(1, "k", "a", "b", "c")
	ld := c.leader(1, "a", "b", "c")
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := call(t, admin[ld], AdminRequest{Op: "status"})
		if st.Node != string(ld) || len(st.Groups) != 1 {
			t.Fatalf("the leader's status: %+v", st)
		}
		gs := st.Groups[0]
		caughtUp := len(gs.FollowerMatch) == 2
		for _, id := range c.ids {
			if id == ld {
				if _, ok := gs.FollowerMatch[string(id)]; ok {
					t.Fatalf("the leader lists itself among its followers: %v", gs.FollowerMatch)
				}
				continue
			}
			caughtUp = caughtUp && gs.FollowerMatch[string(id)] == gs.LastIndex
		}
		if gs.Role == "Leader" && caughtUp {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the leader's followers never matched its last index: %+v", gs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range c.ids {
		if id == ld {
			continue
		}
		st := call(t, admin[id], AdminRequest{Op: "status"})
		if st.Node != string(id) || len(st.Groups) != 1 || st.Groups[0].FollowerMatch != nil {
			t.Fatalf("follower %s's status: %+v", id, st)
		}
	}
}
