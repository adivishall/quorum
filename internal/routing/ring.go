package routing

import "sort"

// ringPoint is one virtual node on a ring: a token, the owner it belongs to
// (a ShardID on the shard ring, or an index into the sorted node slice on the
// node ring), and the vnode index that produced it. owner and vnode are carried
// only so the sort order is a total, deterministic tie-break at equal tokens.
type ringPoint struct {
	token Token
	owner uint32
	vnode uint32
}

// ring is a set of ringPoints sorted once by (token, owner, vnode) ascending.
// Lookup is the clockwise successor: the first point whose token is >= the query
// token, wrapping to index 0 (docs/ROUTING.md §3).
//
// The sort makes construction independent of the order points were generated in,
// and the (owner, vnode) tie-break makes ownership deterministic when two virtual
// nodes collide on the same token — a legal case, not an error.
type ring struct {
	points []ringPoint
}

// newRing sorts the given points into a ring. It takes ownership of the slice.
func newRing(points []ringPoint) ring {
	sort.Slice(points, func(i, j int) bool {
		a, b := points[i], points[j]
		if a.token != b.token {
			return a.token < b.token
		}
		if a.owner != b.owner {
			return a.owner < b.owner
		}
		return a.vnode < b.vnode
	})
	return ring{points: points}
}

// successorIndex returns the index of the first point whose token is >= t,
// wrapping to 0 if t is greater than every token. The ring is never empty by the
// time it is queried (NewRouter refuses an empty membership), so this always
// returns a valid index.
func (r ring) successorIndex(t Token) int {
	// sort.Search returns the smallest i in [0,n] for which points[i].token >= t.
	i := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].token >= t
	})
	if i == len(r.points) {
		return 0 // wrap-around
	}
	return i
}

// owner returns the owner of the point that owns token t.
func (r ring) owner(t Token) uint32 {
	return r.points[r.successorIndex(t)].owner
}

// sorted reports whether the ring's points are in non-decreasing token order.
// It exists for tests (the sort invariant is load-bearing for INV-C2).
func (r ring) sorted() bool {
	for i := 1; i < len(r.points); i++ {
		if r.points[i].token < r.points[i-1].token {
			return false
		}
	}
	return true
}
