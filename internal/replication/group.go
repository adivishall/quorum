package replication

import (
	"fmt"
	"strings"

	"github.com/adivishall/quorum/internal/routing"
)

// NodeID and ShardID are re-exported from internal/routing so the replication
// layer speaks the same identity types the routing layer produces, rather than a
// parallel set that would have to be converted at every boundary (ADR-015). The
// dependency direction is routing -> replica-group metadata -> replication.
type (
	NodeID  = routing.NodeID
	ShardID = routing.ShardID
)

// ReplicaGroup is an immutable, validated handle on one shard's ordered replica
// set. It is the replication layer's representation of the declarative metadata
// Phase 6 routing produces (docs/REPLICATION.md §2).
//
// It is immutable: the fields are unexported and Replicas returns a copy, so a
// caller can neither reorder nor mutate a group in place. Membership is static
// (ADR-005) — a ReplicaGroup has no join, leave, promote, demote, or rebalance.
// A "membership change" remains a new routing Config compared against the old.
type ReplicaGroup struct {
	shard    ShardID
	replicas []NodeID // ordered, head = primary; never empty, no empty/dup id
	rf       int      // == len(replicas)
}

// NewReplicaGroup builds a validated ReplicaGroup. It copies replicas defensively
// and refuses — never repairs — an invalid group: an empty replica list, an empty
// or duplicate replica id, an RF < 1, or an RF that does not equal the replica
// count (INV-P1). It does not deduplicate or reorder: a duplicate is a caller bug
// surfaced, and the order is the routing contract's order (head = primary).
func NewReplicaGroup(shard ShardID, replicas []NodeID, rf int) (ReplicaGroup, error) {
	if len(replicas) == 0 {
		return ReplicaGroup{}, ErrEmptyGroup
	}
	if rf < 1 {
		return ReplicaGroup{}, fmt.Errorf("replication: rf=%d: %w", rf, ErrInvalidReplicationFactor)
	}
	if rf != len(replicas) {
		return ReplicaGroup{}, fmt.Errorf("replication: rf=%d but %d replicas: %w",
			rf, len(replicas), ErrInvalidReplicationFactor)
	}
	cp := make([]NodeID, len(replicas))
	copy(cp, replicas)
	seen := make(map[NodeID]struct{}, len(cp))
	for _, id := range cp {
		if id == "" {
			return ReplicaGroup{}, ErrEmptyReplicaID
		}
		if _, dup := seen[id]; dup {
			return ReplicaGroup{}, fmt.Errorf("replication: replica %q: %w", id, ErrDuplicateReplica)
		}
		seen[id] = struct{}{}
	}
	return ReplicaGroup{shard: shard, replicas: cp, rf: rf}, nil
}

// Shard returns the shard this group is for.
func (g ReplicaGroup) Shard() ShardID { return g.shard }

// ReplicationFactor returns the group's replication factor, which equals the
// replica count.
func (g ReplicaGroup) ReplicationFactor() int { return g.rf }

// Primary returns the head replica — the shard's primary, matching the routing
// layer's ordering. It panics on a zero-value group, which is a programming error
// (a ReplicaGroup is only ever obtained from a constructor that rejects empties).
func (g ReplicaGroup) Primary() NodeID { return g.replicas[0] }

// Replicas returns a copy of the ordered replica set (head = primary), so a
// caller cannot mutate the group's state.
func (g ReplicaGroup) Replicas() []NodeID {
	out := make([]NodeID, len(g.replicas))
	copy(out, g.replicas)
	return out
}

// Contains reports whether id is one of the group's replicas.
func (g ReplicaGroup) Contains(id NodeID) bool {
	for _, r := range g.replicas {
		if r == id {
			return true
		}
	}
	return false
}

// Equal reports whether two groups have the same shard and the same replicas in
// the same order. Identity is deterministic and order-sensitive: two groups that
// differ only in replica order are not equal, because order encodes the primary.
func (g ReplicaGroup) Equal(other ReplicaGroup) bool {
	if g.shard != other.shard || g.rf != other.rf || len(g.replicas) != len(other.replicas) {
		return false
	}
	for i := range g.replicas {
		if g.replicas[i] != other.replicas[i] {
			return false
		}
	}
	return true
}

// String is a deterministic, human-readable rendering: "shard N [r0 r1 r2]".
func (g ReplicaGroup) String() string {
	ss := make([]string, len(g.replicas))
	for i, r := range g.replicas {
		ss[i] = string(r)
	}
	return fmt.Sprintf("shard %d [%s]", g.shard, strings.Join(ss, " "))
}

// ReplicaGroupsFromRouter builds one validated ReplicaGroup per shard directly
// from a routing.Router, so the declarative routing metadata flows into the
// replication model without either layer re-deriving the other (ADR-015). Groups
// are returned in shard-id order. It returns an error only if the router's
// metadata is itself inconsistent, which a valid Router never produces.
func ReplicaGroupsFromRouter(r *routing.Router) ([]ReplicaGroup, error) {
	shards := r.Shards()
	rf := r.ReplicationFactor()
	out := make([]ReplicaGroup, 0, len(shards))
	for _, info := range shards {
		g, err := NewReplicaGroup(info.ID, info.ReplicaGroup, rf)
		if err != nil {
			return nil, fmt.Errorf("replication: shard %d: %w", info.ID, err)
		}
		if g.Primary() != info.Primary {
			// The routing contract says ReplicaGroup[0] is the primary; if a
			// future routing change broke that, refuse rather than paper over it.
			return nil, fmt.Errorf("replication: shard %d: primary %q != head %q: %w",
				info.ID, info.Primary, g.Primary(), ErrInvalidReplicationFactor)
		}
		out = append(out, g)
	}
	return out, nil
}
