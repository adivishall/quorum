package routing

import "testing"

// The golden token values were produced by an independent Python reference
// (hashlib.sha256, first 8 bytes, big-endian), not by this package, so this test
// pins the documented algorithm rather than checking the implementation against
// itself (docs/ROUTING.md §2). token("") is the standard SHA-256 empty vector.
func TestGoldenTokenVectors(t *testing.T) {
	cases := []struct {
		key  []byte
		want Token
	}{
		{[]byte(""), 0xe3b0c44298fc1c14},
		{[]byte("user:123"), 0x61b7de306ccf11d6},
		{[]byte{0x00}, 0x6e340b9cffb37a98},
		{[]byte{0xff, 0xfe, 0xfd, 0x00, 0x01}, 0x491865266935dcc5},
		{[]byte("The quick brown fox"), 0x5cac4f980fedc3d3},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, 0xaf5570f5a1810b7a},
		{[]byte("shard-key-\xf0\x9f\x98\x80"), 0x5e6b1cd028288374},
	}
	for _, c := range cases {
		if got := TokenOf(c.key); got != c.want {
			t.Errorf("TokenOf(%q) = 0x%016x, want 0x%016x", c.key, uint64(got), uint64(c.want))
		}
	}
}

// TestGoldenKeyToShard pins key -> shard for the v1 default (16 shards). These
// come from the same independent reference; if the shard-vnode label or the
// successor rule ever changes, this fails rather than drifting silently.
func TestGoldenKeyToShard(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}})
	cases := []struct {
		key  []byte
		want ShardID
	}{
		{[]byte(""), 8},
		{[]byte("user:123"), 6},
		{[]byte{0x00}, 6},
		{[]byte{0xff, 0xfe, 0xfd, 0x00, 0x01}, 3},
		{[]byte("The quick brown fox"), 11},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, 1},
	}
	for _, c := range cases {
		if got := r.Route(c.key); got != c.want {
			t.Errorf("Route(%q) = %d, want %d", c.key, got, c.want)
		}
	}
}

// TestGoldenReplicaGroups pins shard -> replica group for nodes {n0,n1,n2}, RF 3.
func TestGoldenReplicaGroups(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}})
	want := map[ShardID][]NodeID{
		0:  {"n1", "n0", "n2"},
		1:  {"n0", "n1", "n2"},
		2:  {"n1", "n2", "n0"},
		3:  {"n1", "n2", "n0"},
		4:  {"n2", "n1", "n0"},
		5:  {"n2", "n0", "n1"},
		6:  {"n0", "n2", "n1"},
		7:  {"n1", "n2", "n0"},
		8:  {"n0", "n2", "n1"},
		9:  {"n0", "n1", "n2"},
		10: {"n0", "n2", "n1"},
		11: {"n0", "n2", "n1"},
		12: {"n0", "n2", "n1"},
		13: {"n1", "n2", "n0"},
		14: {"n1", "n2", "n0"},
		15: {"n0", "n2", "n1"},
	}
	for s := ShardID(0); s < 16; s++ {
		got := r.ReplicaGroup(s)
		if !equalNodes(got, want[s]) {
			t.Errorf("ReplicaGroup(%d) = %v, want %v", s, got, want[s])
		}
	}
}

func equalNodes(a, b []NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustRouter(t *testing.T, cfg Config) *Router {
	t.Helper()
	r, err := NewRouter(cfg)
	if err != nil {
		t.Fatalf("NewRouter(%+v): %v", cfg, err)
	}
	return r
}
