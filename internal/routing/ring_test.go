package routing

import "testing"

// TestRingIsSorted: both rings a Router builds are in non-decreasing token order.
// The sort invariant is what makes binary-search lookup and the coverage argument
// (INV-C2) valid.
func TestRingIsSorted(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 8, ReplicationFactor: 2, Nodes: []NodeID{"n0", "n1", "n2"}})
	if !r.shardRing.sorted() {
		t.Error("shard ring is not sorted")
	}
	if !r.nodeRing.sorted() {
		t.Error("node ring is not sorted")
	}
}

// TestSuccessorBoundaryAndWrap pins the ownership rule on a hand-built ring: a
// position P owns (predecessor, P], the boundary token equal to P belongs to P,
// and a token past the largest position wraps to index 0.
func TestSuccessorBoundaryAndWrap(t *testing.T) {
	// Positions at 10, 20, 30 owned by 1, 2, 3.
	r := newRing([]ringPoint{
		{token: 20, owner: 2},
		{token: 10, owner: 1},
		{token: 30, owner: 3},
	})
	cases := []struct {
		t     Token
		owner uint32
	}{
		{0, 1},         // below all -> first
		{10, 1},        // exact boundary -> that position (inclusive)
		{11, 2},        // just past 10 -> next arc
		{20, 2},        // exact boundary
		{21, 3},        // just past 20
		{30, 3},        // exact boundary
		{31, 1},        // past the largest -> wrap to index 0
		{^Token(0), 1}, // max uint64 -> wrap
	}
	for _, c := range cases {
		if got := r.owner(c.t); got != c.owner {
			t.Errorf("owner(%d) = %d, want %d", c.t, got, c.owner)
		}
	}
}

// TestTokenCollisionIsDeterministic: two virtual nodes on the same token are a
// legal case resolved by the (token, owner, vnode) tie-break — the smaller owner
// wins — not an error. The ring stays sorted and ownership is stable.
func TestTokenCollisionIsDeterministic(t *testing.T) {
	// owners 5 and 2 both at token 100; 2 must win the boundary.
	r := newRing([]ringPoint{
		{token: 100, owner: 5, vnode: 0},
		{token: 100, owner: 2, vnode: 0},
		{token: 200, owner: 9, vnode: 0},
	})
	if !r.sorted() {
		t.Fatal("ring with a token collision is not sorted")
	}
	if got := r.owner(100); got != 2 {
		t.Errorf("owner(100) with collision = %d, want the smaller owner 2", got)
	}
	if got := r.owner(50); got != 2 {
		t.Errorf("owner(50) = %d, want 2 (first point at token 100)", got)
	}
	// Stable across repeated construction in the other order.
	r2 := newRing([]ringPoint{
		{token: 200, owner: 9, vnode: 0},
		{token: 100, owner: 2, vnode: 0},
		{token: 100, owner: 5, vnode: 0},
	})
	if r2.owner(100) != r.owner(100) {
		t.Error("collision resolution depends on construction order")
	}
}

// TestEveryTokenIntervalHasExactlyOneOwner is INV-C2's coverage property on a
// real shard ring. Two independent checks:
//
//  1. The arcs partition the whole 64-bit space: each position owns
//     (predecessor, position], and the arc lengths sum to 2^64 (== 0 mod 2^64).
//     A gap or an overlap would break this sum.
//  2. Every arc resolves to its own position's owner at both the boundary token
//     and a token strictly inside the arc — so no interval is unowned and none
//     has two owners.
func TestEveryTokenIntervalHasExactlyOneOwner(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 4, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1"}})
	pts := r.shardRing.points
	n := len(pts)

	var sum Token // uint64 wrap; a perfect partition sums to 0 (== 2^64)
	for i := 0; i < n; i++ {
		prev := pts[(i-1+n)%n].token
		cur := pts[i].token
		length := cur - prev // wraps for i == 0
		sum += length

		// Boundary token belongs to this point (or, under a collision, to the
		// first point sharing the token — its owner is who successorIndex finds).
		if idx := r.shardRing.successorIndex(cur); pts[idx].token != cur {
			t.Fatalf("successor(%d) landed on token %d, not the boundary", cur, pts[idx].token)
		}
		// A token strictly inside the arc must resolve to this point exactly.
		if length > 1 {
			inside := prev + 1
			if idx := r.shardRing.successorIndex(inside); idx != i {
				t.Errorf("token %d inside arc (%d,%d] resolved to index %d, want %d", inside, prev, cur, idx, i)
			}
		}
	}
	if sum != 0 {
		t.Fatalf("arc lengths sum to %d (mod 2^64), want 0 — the ring has a gap or overlap", uint64(sum))
	}
}
