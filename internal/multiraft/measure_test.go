package multiraft

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/routing"
)

// Phase 15 measurements (docs/MULTI_RAFT.md §9). Reported, never asserted as
// timings: they describe this machine. What IS asserted is structural — the
// goroutines a group costs.
//
//	go test ./internal/multiraft -run TestMeasure -multiraft.measure -v
//	go test ./internal/multiraft -run '^$' -bench 'Route' -benchtime 2s

var flagMeasure = flag.Bool("multiraft.measure", false, "run the Phase 15 measurements (TestMeasure*)")

func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func heapInUse() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}

// TestMeasureGroupsPerNode: n groups on one node (single-voter groups) and on
// three nodes in one process (every group on all three, heartbeating), at the
// default 50 ms tick: goroutines, heap and idle CPU per group.
func TestMeasureGroupsPerNode(t *testing.T) {
	if !*flagMeasure {
		t.Skip("-multiraft.measure")
	}
	for _, nodes := range []int{1, 3} {
		total := map[int]int{} // goroutines, by group count
		for _, n := range []int{1, 8, 32, 128} {
			g0, h0 := runtime.NumGoroutine(), heapInUse()
			ids := []NodeID{"a", "b", "c"}[:nodes]
			c := newHostCluster(t, ids...)
			for _, id := range ids {
				h, err := Start(c.ctx, Config{ID: id, DataDir: c.dirs[id], Transport: c.trs[id], StaticPeers: c.static(id),
					NewStateMachine: func(GroupID) raftnode.StateMachine { return kv.NewStore() }, DisableSync: true})
				if err != nil {
					t.Fatal(err)
				}
				c.hosts[id] = h
			}
			for g := 0; g < n; g++ {
				c.create(GroupID(g), ids...)
			}
			for g := 0; g < n; g++ {
				c.leader(GroupID(g), ids...)
			}
			time.Sleep(time.Second) // let elections and their writes settle
			total[n] = runtime.NumGoroutine() - g0
			heap := int64(heapInUse()) - int64(h0)
			cpu0 := cpuTime()
			const window = 3 * time.Second
			time.Sleep(window) // an idle window: only ticks and heartbeats
			cpu := cpuTime() - cpu0
			replicas := n * nodes
			t.Logf("nodes=%d groups=%3d  goroutines=%5d  heap/replica=%6.1f KiB  idle CPU/replica=%.3f ms/s",
				nodes, n, total[n], float64(heap)/float64(replicas)/1024, float64(cpu.Microseconds())/1000/window.Seconds()/float64(replicas))
			c.close()
		}
		// Structure: each replica adds its actor, its receive loop and one
		// sender per peer, and nothing else grows with the number of groups
		// (the transport's goroutines are per connection, the host's per node).
		slope := float64(total[128]-total[8]) / float64((128-8)*nodes)
		t.Logf("nodes=%d: %.2f goroutines per added replica", nodes, slope)
		if want := float64(2 + (nodes - 1)); slope > want+0.1 {
			t.Errorf("%.2f goroutines per added replica, want %.0f", slope, want)
		}
	}
}

// TestMeasureMembershipChanges times membership changes on a three-node group
// with fsync on (the durable path): AddLearner and RemoveLearner (one entry
// each), and Promote and RemoveVoter (a joint entry, then the final one).
func TestMeasureMembershipChanges(t *testing.T) {
	if !*flagMeasure {
		t.Skip("-multiraft.measure")
	}
	c := newHostCluster(t, "a", "b", "c", "d")
	for _, id := range c.ids {
		h, err := Start(c.ctx, Config{ID: id, DataDir: c.dirs[id], Transport: c.trs[id], StaticPeers: c.static(id),
			NewStateMachine: func(GroupID) raftnode.StateMachine { return kv.NewStore() }, TickInterval: 20 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		c.hosts[id] = h
	}
	c.create(1, "a", "b", "c")
	if _, err := c.hosts["d"].Create(1, nil); err != nil {
		t.Fatal(err)
	}
	timeIt := func(cc raft.ConfChange) time.Duration {
		t.Helper()
		start := time.Now()
		deadline := start.Add(20 * time.Second)
		for {
			l := c.leader(1, "a", "b", "c")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, _, err := c.hosts[l].Group(1).Node.ChangeMembership(ctx, cc)
			cancel()
			if err == nil {
				return time.Since(start)
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %v", cc, err)
			}
		}
	}
	const rounds = 20
	var learner []time.Duration
	for i := 0; i < rounds; i++ {
		learner = append(learner, timeIt(raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "d", Addr: c.addrs["d"]}}))
		learner = append(learner, timeIt(raft.ConfChange{Type: raft.RemoveLearner, Member: raft.Member{ID: "d"}}))
	}
	learnerAdd := timeIt(raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "d", Addr: c.addrs["d"]}})
	promote := timeIt(raft.ConfChange{Type: raft.Promote, Member: raft.Member{ID: "d"}})
	remove := timeIt(raft.ConfChange{Type: raft.RemoveVoter, Member: raft.Member{ID: "d"}})
	sort.Slice(learner, func(i, j int) bool { return learner[i] < learner[j] })
	t.Logf("learner change (1 entry, fsync): p50 %v  p90 %v  max %v over %d", learner[len(learner)/2], learner[len(learner)*9/10], learner[len(learner)-1], len(learner))
	t.Logf("AddLearner %v; Promote (joint + final) %v; RemoveVoter (joint + final) %v", learnerAdd, promote, remove)
}

// TestMeasureCatchUp: a new member catching up a group of 20,000 writes of
// 100-byte values over 2,000 keys — by entries (no snapshots), and by a
// snapshot (every 1,000 entries, retain 0) plus the suffix.
func TestMeasureCatchUp(t *testing.T) {
	if !*flagMeasure {
		t.Skip("-multiraft.measure")
	}
	for _, every := range []uint64{0, 1000} {
		c := newHostCluster(t, "a", "b", "c", "d")
		for _, id := range c.ids {
			h, err := Start(c.ctx, Config{ID: id, DataDir: c.dirs[id], Transport: c.trs[id], StaticPeers: c.static(id),
				NewStateMachine: func(GroupID) raftnode.StateMachine { return kv.NewStore() }, DisableSync: true,
				TickInterval: 20 * time.Millisecond, SnapshotEvery: every})
			if err != nil {
				t.Fatal(err)
			}
			c.hosts[id] = h
		}
		c.create(1, "a", "b", "c")
		l := c.leader(1, "a", "b", "c")
		const writes = 20000
		value := make([]byte, 100)
		ctx := context.Background()
		for i := 0; i < writes; i++ {
			cmd := kv.Command{Op: kv.OpPut, Key: []byte(fmt.Sprintf("k%05d", i%2000)), Value: value}
			if err := c.hosts[l].Group(1).Node.Propose(ctx, cmd.Encode()); err != nil {
				l = c.leader(1, "a", "b", "c")
				i--
			}
		}
		c.write(1, string(kv.Command{Op: kv.OpPut, Key: []byte("last"), Value: value}.Encode()), "a", "b", "c")
		target := c.hosts[l].Group(1).Node.Status()
		if _, err := c.hosts["d"].Create(1, nil); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if _, _, err := c.hosts[l].Group(1).Node.ChangeMembership(ctx, raft.ConfChange{Type: raft.AddLearner, Member: raft.Member{ID: "d", Addr: c.addrs["d"]}}); err != nil {
			t.Fatal(err)
		}
		for c.hosts["d"].Group(1).Node.Status().Applied < target.Commit {
			time.Sleep(5 * time.Millisecond)
		}
		st := c.hosts["d"].Group(1).Node.Status()
		how := "entries"
		if st.Snapshot > 0 {
			how = fmt.Sprintf("snapshot at %d + %d entries", st.Snapshot, target.Commit-st.Snapshot)
		}
		t.Logf("snapshot-every=%d: a new member caught up %d entries in %v (by %s)", every, target.Commit, time.Since(start), how)
		c.close()
	}
}

// BenchmarkRouteKey: a key's group — the routing's shard ring — for 4, 16 and
// 64 shards.
func BenchmarkRouteKey(b *testing.B) {
	for _, shards := range []int{4, 16, 64} {
		a, err := NewAssignment(routing.Config{ShardCount: shards, ReplicationFactor: 3, Nodes: []routing.NodeID{"n1", "n2", "n3", "n4", "n5"}})
		if err != nil {
			b.Fatal(err)
		}
		keys := make([][]byte, 1024)
		for i := range keys {
			keys[i] = []byte(fmt.Sprintf("user:%08d", i))
		}
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			var g GroupID
			for i := 0; i < b.N; i++ {
				g += a.GroupOf(keys[i&1023])
			}
			_ = g
		})
	}
}

// BenchmarkRouteRequest: the front's whole routing decision for a request —
// validation, the key's group, the group's server lookup — up to handing it
// over (here to a group this node does not host, so nothing else runs).
func BenchmarkRouteRequest(b *testing.B) {
	a, err := NewAssignment(routing.Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []routing.NodeID{"n1", "n2", "n3"}})
	if err != nil {
		b.Fatal(err)
	}
	f := kv.NewFront("n1", a.GroupOf)
	key := []byte("user:00000042")
	req := kv.Request{Op: kv.ReqGet, Group: a.GroupOf(key), Key: key}
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if resp, _ := f.Do(ctx, req); resp.Status != kv.StatusNotLeader {
			b.Fatal(resp)
		}
	}
}
