package lincheck

import (
	"sync"
	"testing"
)

// TestHistoryIsASnapshotWhileClientsRecord: History may be read while clients
// are still recording attempts on ops in flight, and what it returns is a
// copy — the recorder never writes into it. (Under the race detector, the
// shallow copy it once returned shared the attempts of in-flight ops.)
func TestHistoryIsASnapshotWhileClientsRecord(t *testing.T) {
	r := NewRecorder()
	id := r.BeginRequest("c", Put, "k", []byte("v"), 1, 1)
	a := r.Attempt(id, "n1")
	h := r.History()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			r.AttemptDone(id, a, false, "unknown: timeout", 0)
		}
	}()
	for i := 0; i < 200; i++ {
		for _, op := range r.History().Ops {
			for _, at := range op.Attempts {
				_ = at.Result
			}
		}
	}
	wg.Wait()
	if got := h.Ops[0].Attempts[0].Result; got != "" {
		t.Fatalf("a history taken before the attempt ended changed afterwards: %q", got)
	}
	if got := r.History().Ops[0].Attempts[0].Result; got != "unknown: timeout" {
		t.Fatalf("a fresh history misses the attempt's end: %q", got)
	}
}
