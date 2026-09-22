package routing

import (
	"fmt"
	"testing"
)

// BenchmarkRoute exists to show the lookup is O(log P), not accidentally O(P):
// its cost must be flat as the shard count (and thus the ring) grows by orders
// of magnitude. It is not a performance target — Phase 6 is not a benchmark phase
// (docs/ROUTING.md, ADR-011 is Phase 5's benchmark ADR).
func BenchmarkRoute(b *testing.B) {
	for _, sc := range []int{16, 256, 4096} {
		r, err := NewRouter(Config{ShardCount: sc, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}})
		if err != nil {
			b.Fatal(err)
		}
		key := []byte("some-representative-key")
		b.Run(fmt.Sprintf("shards=%d", sc), func(b *testing.B) {
			b.ReportAllocs()
			var sink ShardID
			for i := 0; i < b.N; i++ {
				sink = r.Route(key)
			}
			_ = sink
		})
	}
}
