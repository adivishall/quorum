package replication

import (
	"errors"
	"testing"

	"github.com/adivishall/quorum/internal/routing"
)

// TestReplicaGroupValidConstruction covers a well-formed group and its accessors:
// shard, ordered replicas, primary = head, RF, Contains (INV-P1).
func TestReplicaGroupValidConstruction(t *testing.T) {
	g, err := NewReplicaGroup(5, []NodeID{"n2", "n0", "n1"}, 3)
	if err != nil {
		t.Fatalf("NewReplicaGroup: %v", err)
	}
	if g.Shard() != 5 {
		t.Errorf("Shard() = %d, want 5", g.Shard())
	}
	if g.ReplicationFactor() != 3 {
		t.Errorf("RF = %d, want 3", g.ReplicationFactor())
	}
	if g.Primary() != "n2" {
		t.Errorf("Primary() = %q, want n2 (the head, not the sorted-least)", g.Primary())
	}
	want := []NodeID{"n2", "n0", "n1"}
	got := g.Replicas()
	if len(got) != len(want) {
		t.Fatalf("Replicas() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Replicas() = %v, want %v (order must be preserved, not sorted)", got, want)
		}
	}
	for _, id := range []NodeID{"n0", "n1", "n2"} {
		if !g.Contains(id) {
			t.Errorf("Contains(%q) = false, want true", id)
		}
	}
	if g.Contains("n9") {
		t.Errorf("Contains(n9) = true, want false")
	}
}

// TestReplicaGroupRejectsInvalid proves construction refuses — never repairs —
// every invalid group (INV-P1).
func TestReplicaGroupRejectsInvalid(t *testing.T) {
	cases := []struct {
		name     string
		replicas []NodeID
		rf       int
		want     error
	}{
		{"empty group", nil, 0, ErrEmptyGroup},
		{"empty slice explicit", []NodeID{}, 3, ErrEmptyGroup},
		{"rf zero", []NodeID{"n0"}, 0, ErrInvalidReplicationFactor},
		{"rf negative", []NodeID{"n0"}, -1, ErrInvalidReplicationFactor},
		{"rf below count", []NodeID{"n0", "n1", "n2"}, 2, ErrInvalidReplicationFactor},
		{"rf above count", []NodeID{"n0", "n1"}, 3, ErrInvalidReplicationFactor},
		{"empty node id", []NodeID{"n0", "", "n2"}, 3, ErrEmptyReplicaID},
		{"duplicate replica", []NodeID{"n0", "n1", "n0"}, 3, ErrDuplicateReplica},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReplicaGroup(0, tc.replicas, tc.rf)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestReplicaGroupIsImmutable proves a caller cannot mutate a group's state
// through the slice it passed in or the slice Replicas returns.
func TestReplicaGroupIsImmutable(t *testing.T) {
	in := []NodeID{"n0", "n1", "n2"}
	g, err := NewReplicaGroup(0, in, 3)
	if err != nil {
		t.Fatal(err)
	}
	in[0] = "MUTATED" // mutate the caller's slice after construction
	if g.Primary() != "n0" {
		t.Fatalf("Primary() = %q, group aliased the caller's slice", g.Primary())
	}
	out := g.Replicas()
	out[1] = "MUTATED" // mutate the returned slice
	if g.Replicas()[1] != "n1" {
		t.Fatalf("Replicas() exposed internal storage; got %q", g.Replicas()[1])
	}
}

// TestReplicaGroupEqualityIsDeterministic covers Equal: same shard + same ordered
// replicas is equal; a different shard, a reordered replica set (different
// primary), or a different membership is not (INV-P1).
func TestReplicaGroupEqualityIsDeterministic(t *testing.T) {
	base, _ := NewReplicaGroup(3, []NodeID{"n0", "n1", "n2"}, 3)
	same, _ := NewReplicaGroup(3, []NodeID{"n0", "n1", "n2"}, 3)
	diffShard, _ := NewReplicaGroup(4, []NodeID{"n0", "n1", "n2"}, 3)
	reordered, _ := NewReplicaGroup(3, []NodeID{"n1", "n0", "n2"}, 3)
	diffMembers, _ := NewReplicaGroup(3, []NodeID{"n0", "n1", "n3"}, 3)

	if !base.Equal(same) {
		t.Error("identical groups are not Equal")
	}
	if !same.Equal(base) {
		t.Error("Equal is not symmetric")
	}
	if base.Equal(diffShard) {
		t.Error("groups with different shards are Equal")
	}
	if base.Equal(reordered) {
		t.Error("reordered replicas (different primary) are Equal — order must matter")
	}
	if base.Equal(diffMembers) {
		t.Error("different membership is Equal")
	}
}

// TestReplicaGroupsFromRouter proves the replication layer consumes the routing
// metadata: one validated group per shard, in shard-id order, head = the router's
// primary, and the group's replicas match the router's ReplicaGroup exactly.
func TestReplicaGroupsFromRouter(t *testing.T) {
	r, err := routing.NewRouter(routing.Config{
		ShardCount: 8, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2", "n3"},
	})
	if err != nil {
		t.Fatal(err)
	}
	groups, err := ReplicaGroupsFromRouter(r)
	if err != nil {
		t.Fatalf("ReplicaGroupsFromRouter: %v", err)
	}
	if len(groups) != r.ShardCount() {
		t.Fatalf("got %d groups, want %d", len(groups), r.ShardCount())
	}
	for s, g := range groups {
		if g.Shard() != ShardID(s) {
			t.Fatalf("group %d has shard %d, want shard-id order", s, g.Shard())
		}
		info := r.Shards()[s]
		if g.Primary() != info.Primary {
			t.Errorf("shard %d primary = %q, router says %q", s, g.Primary(), info.Primary)
		}
		if g.ReplicationFactor() != r.ReplicationFactor() {
			t.Errorf("shard %d rf = %d, router says %d", s, g.ReplicationFactor(), r.ReplicationFactor())
		}
		want := info.ReplicaGroup
		got := g.Replicas()
		if len(got) != len(want) {
			t.Fatalf("shard %d replicas = %v, router says %v", s, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("shard %d replicas = %v, router says %v", s, got, want)
			}
		}
	}
}
