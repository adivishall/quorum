package multiraft

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raftnode"
)

// TestGroupsSnapshotAndCompactIndependently: with kv stores as state machines,
// group 1 snapshots and compacts on every node while group 2 does neither;
// each group's files are its own (group 1 has a published snapshot and a
// compacted log, group 2 neither); after every host restarts, group 1
// recovers from its snapshot and group 2 from its full log, and both commit
// again.
func TestGroupsSnapshotAndCompactIndependently(t *testing.T) {
	c := newHostCluster(t, "a", "b", "c")
	start := func(id NodeID) {
		h, err := Start(c.ctx, Config{
			ID: id, DataDir: c.dirs[id], Transport: c.trs[id], StaticPeers: c.static(id),
			NewStateMachine: func(GroupID) raftnode.StateMachine { return kv.NewStore() },
			TickInterval:    10 * time.Millisecond, DisableSync: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		c.hosts[id] = h
	}
	for _, id := range c.ids {
		start(id)
	}
	c.create(1, "a", "b", "c")
	c.create(2, "a", "b", "c")
	put := func(g GroupID, i int) {
		t.Helper()
		cmd := kv.Command{Op: kv.OpPut, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}
		c.write(g, string(cmd.Encode()), c.ids...)
	}
	for i := 0; i < 10; i++ {
		put(1, i)
		put(2, i)
	}
	for _, id := range c.ids {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := c.hosts[id].Group(1).Node.Snapshot(ctx)
		cancel()
		if err != nil {
			t.Fatalf("%s group 1: %v", id, err)
		}
		s1, s2 := c.hosts[id].Group(1).Node.Status(), c.hosts[id].Group(2).Node.Status()
		if s1.Snapshot == 0 || s1.Boundary == 0 || s2.Snapshot != 0 || s2.Boundary != 0 {
			t.Fatalf("%s: group 1 %+v, group 2 %+v", id, s1, s2)
		}
		if _, err := os.Stat(LogPath(c.dirs[id], 1) + ".snap"); err != nil {
			t.Fatalf("%s: group 1's snapshot file: %v", id, err)
		}
		if _, err := os.Stat(LogPath(c.dirs[id], 2) + ".snap"); !os.IsNotExist(err) {
			t.Fatalf("%s: group 2 has a snapshot file: %v", id, err)
		}
	}
	for _, id := range c.ids {
		if err := c.hosts[id].Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.ids {
		start(id)
	}
	for _, id := range c.ids {
		if s1 := c.hosts[id].Group(1).Node.Status(); s1.Snapshot == 0 {
			t.Fatalf("%s: group 1 did not recover from its snapshot: %+v", id, s1)
		}
		if s2 := c.hosts[id].Group(2).Node.Status(); s2.Snapshot != 0 || s2.LastIndex < 10 {
			t.Fatalf("%s: group 2 did not recover from its full log: %+v", id, s2)
		}
	}
	put(1, 100)
	put(2, 100)
}
