package kv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raftnode"
)

// BenchmarkForwarding is the end-to-end cost of one identified PUT on a
// three-node group over TCP loopback (the test cluster: 15ms ticks, no fsync —
// so replication, not the disk, dominates), served by the leader directly and
// through a follower that forwards it one hop (two more internal messages).
// The anonymous case shows what request identity adds end to end.
func BenchmarkForwarding(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterWith(b, ctx, 3, false, kv.Limits{})
	l := c.waitLeader(0, 10*time.Second)
	var f raftnode.NodeID
	for _, id := range c.ids {
		if id != l {
			f = id
			break
		}
	}
	s := register(b, c, kv.SessionOptions{}, l)
	c.quiesce(l)
	value := []byte("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
	rid := uint64(0)
	run := func(name string, at raftnode.NodeID, identified bool) {
		b.Run(name, func(b *testing.B) {
			srv := c.server(at)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				req := kv.Request{Op: kv.ReqPut, Key: []byte(fmt.Sprintf("k%d", i%64)), Value: value}
				if identified {
					rid++
					req.ClientID, req.RequestID, req.AckedBelow = s.ID(), rid, rid
				}
				resp, _ := srv.Do(ctx, req)
				if resp.Status != kv.StatusOK || (at != l) != (resp.Via != "") {
					b.Fatalf("%+v", resp)
				}
			}
		})
	}
	run("anonymous-at-leader", l, false)
	run("identified-at-leader", l, true)
	run("identified-via-follower", f, true)
}
