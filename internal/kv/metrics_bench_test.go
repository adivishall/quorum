package kv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/raft"
)

// BenchmarkInstrumentationOverhead (docs/OBSERVABILITY.md §5): an identified PUT
// through a node's front — at the leader, and through a follower that forwards
// it — on a three-node, one-group cluster over TCP loopback (15 ms ticks, no
// fsync, so the disk does not hide the difference), with no registry anywhere
// ("bare") and with every layer instrumented as dkvd -metrics-listen does
// ("metered").
func BenchmarkInstrumentationOverhead(b *testing.B) {
	for _, metered := range []bool{false, true} {
		name := "bare"
		if metered {
			name = "metered"
		}
		c := startMulti(b, 1, metered)
		ctx := context.Background()
		var leader, follower multiraft.NodeID
		deadline := time.Now().Add(10 * time.Second)
		for leader == "" && time.Now().Before(deadline) {
			for _, id := range c.ids {
				if s := c.fronts[id].Server(0); s != nil && s.Node().Role() == raft.Leader {
					leader = id
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		if leader == "" {
			b.Fatal("no leader")
		}
		for _, id := range c.ids {
			if id != leader {
				follower = id
				break
			}
		}
		reg, _ := c.fronts[leader].Do(ctx, kv.Request{Op: kv.ReqRegister, Group: 0})
		if reg.Status != kv.StatusOK {
			b.Fatalf("register: %+v", reg)
		}
		value := make([]byte, 100)
		rid := uint64(0)
		for _, at := range []multiraft.NodeID{leader, follower} {
			where := "at-leader"
			if at == follower {
				where = "via-follower"
			}
			b.Run(fmt.Sprintf("%s/%s", name, where), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rid++
					resp, _ := c.fronts[at].Do(ctx, kv.Request{Op: kv.ReqPut, Group: 0, ClientID: reg.ClientID, RequestID: rid, AckedBelow: rid,
						Key: []byte(fmt.Sprintf("k%d", i%64)), Value: value})
					if resp.Status != kv.StatusOK {
						b.Fatalf("%+v", resp)
					}
				}
			})
		}
		for _, h := range c.hosts {
			h.Close()
		}
	}
}
