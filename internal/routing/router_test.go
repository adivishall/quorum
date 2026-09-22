package routing

import (
	"bytes"
	"fmt"
	"testing"
)

// deterministicKeys returns a fixed set of keys, so redistribution and
// determinism tests use a reproducible dataset, not randomness.
func deterministicKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
	}
	return keys
}

// ---- INV-C1: routing is a pure function of (key, configuration) ----

func TestRouteIsDeterministicAcrossManyCalls(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}})
	for _, k := range deterministicKeys(2000) {
		first := r.Route(k)
		for i := 0; i < 5; i++ {
			if got := r.Route(k); got != first {
				t.Fatalf("Route(%q) not stable: %d then %d", k, first, got)
			}
		}
	}
}

func TestEquivalentConfigsRouteIdentically(t *testing.T) {
	cfg := Config{ShardCount: 32, ReplicationFactor: 3, Nodes: []NodeID{"a", "b", "c", "d"}}
	a := mustRouter(t, cfg)
	b := mustRouter(t, cfg) // independently constructed
	for _, k := range deterministicKeys(3000) {
		if a.Route(k) != b.Route(k) {
			t.Fatalf("independently built routers disagree on %q: %d vs %d", k, a.Route(k), b.Route(k))
		}
		if !equalNodes(a.ReplicaGroup(a.Route(k)), b.ReplicaGroup(b.Route(k))) {
			t.Fatalf("replica groups disagree for %q", k)
		}
	}
}

func TestNodeOrderDoesNotAffectRouting(t *testing.T) {
	a := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2", "n3"}})
	b := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n3", "n1", "n0", "n2"}})
	for _, k := range deterministicKeys(2000) {
		if a.Route(k) != b.Route(k) {
			t.Fatalf("routing depends on node input order for %q", k)
		}
	}
	for s := ShardID(0); s < 16; s++ {
		if !equalNodes(a.ReplicaGroup(s), b.ReplicaGroup(s)) {
			t.Fatalf("replica group for shard %d depends on node input order: %v vs %v", s, a.ReplicaGroup(s), b.ReplicaGroup(s))
		}
	}
}

func TestConfigRoundTripThroughSerializationRoutesIdentically(t *testing.T) {
	orig := mustRouter(t, Config{ShardCount: 24, ReplicationFactor: 2, Nodes: []NodeID{"x", "y", "z"}})
	data, err := orig.Config().Canonical()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt := mustRouter(t, parsed)
	for _, k := range deterministicKeys(3000) {
		if orig.Route(k) != rebuilt.Route(k) {
			t.Fatalf("routing changed across serialization for %q", k)
		}
	}
	for s := ShardID(0); s < 24; s++ {
		if !equalNodes(orig.ReplicaGroup(s), rebuilt.ReplicaGroup(s)) {
			t.Fatalf("replica group changed across serialization for shard %d", s)
		}
	}
}

// ---- INV-C2: every key -> one shard; every shard -> one replica group ----

func TestRouteAlwaysReturnsAValidShard(t *testing.T) {
	for _, sc := range []int{1, 2, 3, 16, 64, 257} {
		r := mustRouter(t, Config{ShardCount: sc, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1"}})
		for _, k := range deterministicKeys(5000) {
			s := r.Route(k)
			if int(s) < 0 || int(s) >= sc {
				t.Fatalf("Route returned shard %d out of [0,%d)", s, sc)
			}
		}
	}
}

func TestEveryShardIsRepresentedExactlyOnce(t *testing.T) {
	const sc = 64
	r := mustRouter(t, Config{ShardCount: sc, ReplicationFactor: 1, Nodes: []NodeID{"n0"}})
	// Every shard owns at least one ring position.
	seen := make([]bool, sc)
	for _, p := range r.shardRing.points {
		seen[p.owner] = true
	}
	for s := 0; s < sc; s++ {
		if !seen[s] {
			t.Errorf("shard %d has no ring position", s)
		}
	}
	// Shards() returns exactly sc entries, ids 0..sc-1 each once.
	infos := r.Shards()
	if len(infos) != sc {
		t.Fatalf("Shards() returned %d entries, want %d", len(infos), sc)
	}
	for i, info := range infos {
		if info.ID != ShardID(i) {
			t.Fatalf("Shards()[%d].ID = %d, want %d", i, info.ID, i)
		}
	}
}

func TestEveryShardHasOneReplicaGroup(t *testing.T) {
	nodes := []NodeID{"n0", "n1", "n2", "n3", "n4"}
	rf := 3
	r := mustRouter(t, Config{ShardCount: 40, ReplicationFactor: rf, Nodes: nodes})
	valid := map[NodeID]bool{}
	for _, n := range nodes {
		valid[n] = true
	}
	for _, info := range r.Shards() {
		g := info.ReplicaGroup
		if len(g) != rf {
			t.Fatalf("shard %d group size %d, want %d", info.ID, len(g), rf)
		}
		if info.Primary != g[0] {
			t.Fatalf("shard %d primary %q != group head %q", info.ID, info.Primary, g[0])
		}
		distinct := map[NodeID]bool{}
		for _, n := range g {
			if !valid[n] {
				t.Fatalf("shard %d group has unknown node %q", info.ID, n)
			}
			if distinct[n] {
				t.Fatalf("shard %d group has duplicate node %q: %v", info.ID, n, g)
			}
			distinct[n] = true
		}
	}
}

// ---- INV-C3: a one-node change moves only ~1/N, not a reshuffle ----

func primaries(t *testing.T, cfg Config, keys [][]byte) []NodeID {
	t.Helper()
	r := mustRouter(t, cfg)
	out := make([]NodeID, len(keys))
	for i, k := range keys {
		out[i] = r.Owner(k)
	}
	return out
}

func TestKeyToShardIsStableAcrossNodeMembershipChange(t *testing.T) {
	keys := deterministicKeys(10000)
	a := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2"}})
	b := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2", "n3"}})
	changed := 0
	for _, k := range keys {
		if a.Route(k) != b.Route(k) {
			changed++
		}
	}
	if changed != 0 {
		t.Fatalf("key->shard changed for %d/%d keys on a node change; it must be stable (fixed shard count)", changed, len(keys))
	}
}

// TestOwnerMovesOnlyWhereItsShardPrimaryMoved cross-checks the attribution two
// independent ways: count keys whose owner changed, and count keys belonging to
// shards whose primary changed. They must be equal — key movement is fully
// explained by shard-primary reassignment, not by any key-level reshuffle.
func TestOwnerMovesOnlyWhereItsShardPrimaryMoved(t *testing.T) {
	keys := deterministicKeys(20000)
	cfgA := Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2"}}
	cfgB := Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2", "n3"}}
	ra, rb := mustRouter(t, cfgA), mustRouter(t, cfgB)

	shardPrimaryChanged := make(map[ShardID]bool)
	for s := ShardID(0); s < 16; s++ {
		if ra.Primary(s) != rb.Primary(s) {
			shardPrimaryChanged[s] = true
		}
	}

	ownerMoved, inChangedShard := 0, 0
	for _, k := range keys {
		if ra.Owner(k) != rb.Owner(k) {
			ownerMoved++
		}
		if shardPrimaryChanged[ra.Route(k)] {
			inChangedShard++
		}
	}
	if ownerMoved != inChangedShard {
		t.Fatalf("owner moved for %d keys but %d keys are in reassigned shards; movement is not explained by shard reassignment", ownerMoved, inChangedShard)
	}
}

func TestConsistentHashingBeatsModuloOnRedistribution(t *testing.T) {
	keys := deterministicKeys(20000)
	a := primaries(t, Config{ShardCount: 512, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2"}}, keys)
	b := primaries(t, Config{ShardCount: 512, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2", "n3"}}, keys)
	ringMoved := 0
	for i := range keys {
		if a[i] != b[i] {
			ringMoved++
		}
	}
	// Modulo baseline over the same shards: primary = sortedNodes[shard % n].
	ra := mustRouter(t, Config{ShardCount: 512, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2"}})
	rb := mustRouter(t, Config{ShardCount: 512, ReplicationFactor: 1, Nodes: []NodeID{"n0", "n1", "n2", "n3"}})
	na, nb := ra.Nodes(), rb.Nodes()
	modMoved := 0
	for _, k := range keys {
		if na[int(ra.Route(k))%len(na)] != nb[int(rb.Route(k))%len(nb)] {
			modMoved++
		}
	}
	if ringMoved*2 >= modMoved {
		t.Fatalf("ring moved %d keys, modulo moved %d — ring is not dramatically better (expected ring*2 < modulo)", ringMoved, modMoved)
	}
	// Principled band: expected ~ len(keys)/N_new. Assert within [0.5, 2]x.
	expect := len(keys) / 4
	if ringMoved < expect/2 || ringMoved > expect*2 {
		t.Fatalf("ring moved %d keys, outside the principled band [%d,%d] around ~len/N=%d", ringMoved, expect/2, expect*2, expect)
	}
	t.Logf("adding a 4th node: ring moved %d keys, modulo moved %d (of %d)", ringMoved, modMoved, len(keys))
}

// TestRedistributionGoldenCounts locks the exact deterministic shard-movement
// counts from the independent reference (docs/ROUTING.md §7). Because the ring is
// deterministic these are fixed numbers, not samples.
func TestRedistributionGoldenCounts(t *testing.T) {
	const S = 512
	shardPrimaryMoves := func(nodesA, nodesB []NodeID) (ring, modulo int) {
		ra := mustRouter(t, Config{ShardCount: S, ReplicationFactor: 1, Nodes: nodesA})
		rb := mustRouter(t, Config{ShardCount: S, ReplicationFactor: 1, Nodes: nodesB})
		na, nb := ra.Nodes(), rb.Nodes()
		for s := ShardID(0); s < S; s++ {
			if ra.Primary(s) != rb.Primary(s) {
				ring++
			}
			if na[int(s)%len(na)] != nb[int(s)%len(nb)] {
				modulo++
			}
		}
		return
	}
	cases := []struct {
		name              string
		a, b              []NodeID
		wantRing, wantMod int
	}{
		{"add 4th to 3", []NodeID{"n0", "n1", "n2"}, []NodeID{"n0", "n1", "n2", "n3"}, 141, 383},
		{"remove 4th from 4", []NodeID{"n0", "n1", "n2", "n3"}, []NodeID{"n0", "n1", "n2"}, 141, 383},
		{"add f to 5", []NodeID{"a", "b", "c", "d", "e"}, []NodeID{"a", "b", "c", "d", "e", "f"}, 68, 425},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ring, modulo := shardPrimaryMoves(c.a, c.b)
			if ring != c.wantRing || modulo != c.wantMod {
				t.Fatalf("ring=%d modulo=%d, want ring=%d modulo=%d", ring, modulo, c.wantRing, c.wantMod)
			}
		})
	}
}

// ---- Arbitrary keys (matrix F) ----

func TestArbitraryKeysRouteWithoutPanic(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}})
	maxKey := bytes.Repeat([]byte{0xAB}, 1<<20) // 1 MiB, larger than any storage key limit
	keys := [][]byte{
		{},                              // empty
		nil,                             // nil
		{0x00},                          // NUL
		{0x00, 0x00, 0x00},              // NULs
		{0xff, 0x00, 0xfe, 0x80, 0x01},  // binary
		[]byte("\xc3\x28"),              // invalid UTF-8
		[]byte("plain-key"),             // ascii
		bytes.Repeat([]byte("x"), 4096), // storage max key size
		maxKey,                          // very large
	}
	for _, k := range keys {
		s := r.Route(k)
		if int(s) < 0 || int(s) >= 16 {
			t.Fatalf("Route(%d-byte key) = %d out of range", len(k), s)
		}
		if r.Route(k) != s {
			t.Fatalf("Route not deterministic for %d-byte key", len(k))
		}
	}
}

// ---- Small rings (matrix B) ----

func TestOneNodeRing(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 1, Nodes: []NodeID{"solo"}})
	for _, info := range r.Shards() {
		if info.Primary != "solo" || len(info.ReplicaGroup) != 1 || info.ReplicaGroup[0] != "solo" {
			t.Fatalf("shard %d = %+v, want sole owner \"solo\"", info.ID, info)
		}
	}
	for _, k := range deterministicKeys(1000) {
		if r.Owner(k) != "solo" {
			t.Fatalf("Owner(%q) = %q, want solo", k, r.Owner(k))
		}
	}
}

func TestTwoNodeRing(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 2, Nodes: []NodeID{"n0", "n1"}})
	for _, info := range r.Shards() {
		g := info.ReplicaGroup
		if len(g) != 2 || g[0] == g[1] {
			t.Fatalf("shard %d group %v, want two distinct nodes", info.ID, g)
		}
	}
}
