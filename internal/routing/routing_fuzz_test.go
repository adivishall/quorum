package routing

import "testing"

// FuzzRouteIsDeterministicAndValid defends the core routing invariants against
// arbitrary byte keys (INV-C1, INV-C2): Route never panics, always returns a
// shard in range, is deterministic on repeated and independently-built routers,
// and Owner always returns a node that is actually in the membership.
func FuzzRouteIsDeterministicAndValid(f *testing.F) {
	for _, seed := range [][]byte{
		{}, {0x00}, []byte("user:123"), {0xff, 0xfe}, []byte("\xc3\x28"),
		[]byte("a longer key with spaces and \n newlines"),
	} {
		f.Add(seed)
	}
	cfg := Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}}
	r1 := mustRouterF(f, cfg)
	r2 := mustRouterF(f, cfg)
	members := map[NodeID]bool{"n0": true, "n1": true, "n2": true}

	f.Fuzz(func(t *testing.T, key []byte) {
		s := r1.Route(key)
		if int(s) < 0 || int(s) >= cfg.ShardCount {
			t.Fatalf("Route(%q) = %d out of [0,%d)", key, s, cfg.ShardCount)
		}
		if r1.Route(key) != s {
			t.Fatalf("Route(%q) not deterministic within a router", key)
		}
		if r2.Route(key) != s {
			t.Fatalf("Route(%q) differs across independently built routers", key)
		}
		owner := r1.Owner(key)
		if !members[owner] {
			t.Fatalf("Owner(%q) = %q, not in membership", key, owner)
		}
	})
}

func mustRouterF(f *testing.F, cfg Config) *Router {
	f.Helper()
	r, err := NewRouter(cfg)
	if err != nil {
		f.Fatalf("NewRouter: %v", err)
	}
	return r
}
