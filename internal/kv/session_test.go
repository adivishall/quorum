package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv/workload"
	"github.com/adivishall/quorum/internal/lincheck"
)

// TestSessionWorkloadIsLinearizable: concurrent session clients on the real
// driver — every request identified, deliberate concurrent duplicates on a
// second connection — record one logical operation per request, and the
// logical history is linearizable.
func TestSessionWorkloadIsLinearizable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startCluster(t, ctx, 3, false)
	c.waitLeader(0, 10*time.Second)
	rec := lincheck.NewRecorder()
	st := workload.Run(ctx, c.endpoints(), workload.Options{Clients: 4, OpsPerClient: 40, Keys: 2, Timeout: 5 * time.Second, Seed: 3,
		GetPct: 30, DeletePct: 15, Sessions: true, DupPct: 30}, rec)
	check(t, rec, st)
	if st.Sessions != 4 || st.DupSends == 0 || st.Duplicates == 0 {
		t.Fatalf("the workload did not exercise sessions and duplicates: %s", st)
	}
}
