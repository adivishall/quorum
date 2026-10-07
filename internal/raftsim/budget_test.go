package raftsim

import (
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/lincheck"
)

// Audit H4: replication in budgeted batches under every fault the simulator
// injects. Every chaos and client profile runs with budgets of 2 entries and
// 32 bytes, so every backlog travels in batches — each acknowledged batch
// sending the next, a lost one resent by the next heartbeat — while the
// continuous invariants, the budgets themselves (check.go) and, for the client
// profiles, linearizability are checked. The existing seeds keep the default
// budgets; these runs add the batched paths to what the simulator explores.

func budgeted(p Profile) Profile {
	p.Name += "/budgeted"
	p.MaxEntriesPerMsg, p.MaxSizePerMsg = 2, 32
	return p
}

func TestBudgetedReplicationUnderFaults(t *testing.T) {
	seeds := []int64{1, 2, 3}
	if testing.Short() {
		seeds = seeds[:1]
	}
	full := 0
	for _, base := range Profiles {
		p := budgeted(base)
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/seed=%d", p.Name, seed), func(t *testing.T) {
				r := Run(p, seed)
				if r.Violation != nil {
					t.Fatalf("%s", r.Report(fmt.Sprintf("Run(budgeted(%s), %d)", base.Name, seed)))
				}
				requireProgressAndFaults(t, p, r.Stats)
				full += r.Stats.FullBatches
			})
		}
	}
	for _, base := range KVProfiles {
		p := budgeted(base)
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/seed=%d", p.Name, seed), func(t *testing.T) {
				r := RunKV(p, seed)
				if r.Violation != nil {
					t.Fatalf("%s", r.Report(fmt.Sprintf("RunKV(budgeted(%s), %d)", base.Name, seed)))
				}
				if ok, lr := linearizable(r.History); !ok {
					t.Fatalf("NOT LINEARIZABLE: %s\n%s", lr.Reason, lincheck.Format(lr.Counterexample))
				}
				if r.KV.WritesAcked == 0 || r.KV.ReadsServed == 0 {
					t.Fatalf("vacuous run: %+v", r.KV)
				}
				full += r.Stats.FullBatches
			})
		}
	}
	if full == 0 {
		t.Fatal("no AppendEntries was ever cut by the budget: the batched paths were not exercised")
	}
	t.Logf("%d full batches", full)
}
