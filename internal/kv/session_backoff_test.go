package kv

import (
	"context"
	"sync"
	"testing"
	"time"
)

// leaderlessFor is an endpoint with no leader until a moment after its first
// request — an election in progress: it answers NOT_LEADER naming no leader,
// then executes.
type leaderlessFor struct {
	d     time.Duration
	mu    sync.Mutex
	start time.Time
	calls int
}

func (e *leaderlessFor) Name() string { return "n1" }
func (e *leaderlessFor) Do(context.Context, Request) (Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.start.IsZero() {
		e.start = time.Now()
	}
	if time.Since(e.start) < e.d {
		return Response{Status: StatusNotLeader, Node: "n1"}, nil
	}
	return Response{Status: StatusOK, Node: "n1", Index: 9, Term: 2}, nil
}

// TestSessionRidesOutAnElectionLongerThanItsAttemptsTimesBackoff pins the fix
// for what the Phase 13 gate found (TestRetryAtEveryCrashPointOfAWrite under
// full-suite load): a session that backs off a fixed Backoff after every
// refusal naming no leader spends MaxAttempts × Backoff — 20 × 20ms — and gives
// up during an ordinary election, reporting unknown for a request that would
// have completed moments later. Here the cluster has no leader for 400ms and
// the session has 12 attempts of 20ms: a fixed back-off gives up after ~220ms;
// the doubling one is still asking at ~620ms.
func TestSessionRidesOutAnElectionLongerThanItsAttemptsTimesBackoff(t *testing.T) {
	ep := &leaderlessFor{d: 400 * time.Millisecond}
	s := ResumeSession([]Doer{ep}, SessionOptions{AttemptTimeout: time.Second, MaxAttempts: 12, Backoff: 20 * time.Millisecond}, 1, 1)
	out := s.Put(context.Background(), []byte("k"), []byte("v"), nil)
	if out.Err != nil || out.Response.Status != StatusOK {
		t.Fatalf("the session gave up during a 400ms election after %d attempts: %+v", out.Attempts, out)
	}
	if out.Attempts > 8 {
		t.Fatalf("%d attempts for a 400ms election: the back-off is not doubling", out.Attempts)
	}
}

func TestBackoffDoublesUpToItsCap(t *testing.T) {
	ms := time.Millisecond
	for n, want := range []time.Duration{20 * ms, 40 * ms, 80 * ms, 160 * ms, 320 * ms, 640 * ms, maxBackoff, maxBackoff, maxBackoff} {
		if got := backoffFor(20*ms, n); got != want {
			t.Errorf("backoffFor(20ms, %d) = %v, want %v", n, got, want)
		}
	}
	if got := backoffFor(3*time.Second, 5); got != 3*time.Second {
		t.Errorf("a base above the cap is used as is: %v", got)
	}
	if got := backoffFor(20*ms, 1000); got != maxBackoff {
		t.Errorf("no overflow on a long outage: %v", got)
	}
}
