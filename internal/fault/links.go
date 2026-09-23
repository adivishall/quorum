package fault

import "sort"

// Link is a directed network link: messages From -> To.
type Link struct{ From, To string }

// Links is a set of blocked directed links — the partition model shared by the
// deterministic simulator (internal/raftsim) and the transport decorator
// (Network). A message on a blocked link is lost. Every partition shape is a set
// of blocked links: a one-way (asymmetric) partition blocks a single direction, a
// symmetric one blocks both, isolating a node blocks every link touching it, and
// a split blocks every link crossing between two groups.
//
// Links is not safe for concurrent use; Network guards its copy with a mutex. It
// never iterates a map to produce an observable order (BlockedLinks sorts), so it
// preserves the determinism of whatever drives it.
type Links struct {
	blocked map[Link]bool
}

// Blocked reports whether messages from -> to are currently lost.
func (l *Links) Blocked(from, to string) bool { return l.blocked[Link{from, to}] }

// Block cuts the one direction from -> to (an asymmetric partition).
func (l *Links) Block(from, to string) {
	if l.blocked == nil {
		l.blocked = map[Link]bool{}
	}
	l.blocked[Link{from, to}] = true
}

// Unblock restores the one direction from -> to.
func (l *Links) Unblock(from, to string) { delete(l.blocked, Link{from, to}) }

// Cut blocks both directions between a and b (a symmetric partition of the pair).
func (l *Links) Cut(a, b string) {
	l.Block(a, b)
	l.Block(b, a)
}

// Isolate blocks every link into and out of node, among the given members.
func (l *Links) Isolate(node string, members []string) {
	for _, m := range members {
		if m != node {
			l.Cut(node, m)
		}
	}
}

// Split blocks every link crossing between group a and group b in both
// directions; links within a group are untouched.
func (l *Links) Split(a, b []string) {
	for _, x := range a {
		for _, y := range b {
			if x != y {
				l.Cut(x, y)
			}
		}
	}
}

// HealAll removes every partition.
func (l *Links) HealAll() { l.blocked = nil }

// BlockedLinks returns the blocked links in a deterministic (sorted) order.
func (l *Links) BlockedLinks() []Link {
	out := make([]Link, 0, len(l.blocked))
	for k := range l.blocked {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}
