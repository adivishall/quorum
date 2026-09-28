package multiraft

import (
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/routing"
)

// TestAssignmentIsTheRouting: a key's group is its routing shard, for every
// key tried; each group's genesis is its shard's replica group (rf voters,
// with addresses); every node's genesis groups are exactly those naming it;
// and 2, 4 and 8 groups over five nodes each give every node a share.
func TestAssignmentIsTheRouting(t *testing.T) {
	nodes := []routing.NodeID{"n1", "n2", "n3", "n4", "n5"}
	addrs := map[NodeID]string{}
	for _, n := range nodes {
		addrs[NodeID(n)] = "h:" + string(n)
	}
	for _, shards := range []int{2, 4, 8} {
		a, err := NewAssignment(routing.Config{ShardCount: shards, ReplicationFactor: 3, Nodes: nodes})
		if err != nil {
			t.Fatal(err)
		}
		if len(a.Groups()) != shards {
			t.Fatalf("%d shards: %d groups", shards, len(a.Groups()))
		}
		for i := 0; i < 2000; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			if got, want := a.GroupOf(key), GroupID(a.Router().Route(key)); got != want {
				t.Fatalf("key %s: group %d, routing shard %d", key, got, want)
			}
		}
		hosted := map[NodeID]int{}
		for _, g := range a.Groups() {
			conf, err := a.Genesis(g, addrs)
			if err != nil {
				t.Fatal(err)
			}
			if len(conf.Voters) != 3 || conf.Joint() || len(conf.Learners) != 0 {
				t.Fatalf("group %d genesis %s", g, conf)
			}
			rg := a.Router().ReplicaGroup(routing.ShardID(g))
			for _, id := range rg {
				if addr, ok := conf.Addr(NodeID(id)); !ok || addr != "h:"+string(id) {
					t.Fatalf("group %d: %s missing from %s", g, id, conf)
				}
				hosted[NodeID(id)]++
			}
		}
		for _, n := range nodes {
			gs := a.GenesisGroups(NodeID(n))
			if len(gs) != hosted[NodeID(n)] {
				t.Fatalf("%d shards: %s genesis groups %v, hosted %d", shards, n, gs, hosted[NodeID(n)])
			}
		}
		if _, err := a.Genesis(GroupID(shards), addrs); err == nil {
			t.Fatal("a group past the shards has a genesis")
		}
		if _, err := a.Genesis(0, map[NodeID]string{}); err == nil {
			t.Fatal("a genesis without addresses")
		}
	}
}
