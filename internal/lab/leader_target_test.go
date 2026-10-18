package lab

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/multiraft"
)

// A fault aimed at "the leader" meets a group between leaders whenever its
// instant falls inside an election — half a second or so after a leader
// dies, or any election a loaded cluster holds on its own. CI on the S4
// branch (run 38866705079) saw exactly that: the second fault of
// TestChaosLeaderLossUnderLoadIsLinearizable, 250 ms into an election, was
// skipped after one look, and the run passed with one fault of two. These pin
// the scheduler's contract: wait for a leader up to the fault's hold, record
// the wait, and fail the run — not skip — when none comes.

// leaderless answers "no leader" the first n times, then names id.
func leaderless(n int, id string) (func(context.Context, multiraft.GroupID) (string, uint64, error), *int) {
	calls := 0
	return func(ctx context.Context, g multiraft.GroupID) (string, uint64, error) {
		calls++
		if calls <= n {
			return "", 0, errors.New("lab: no leader of group 0")
		}
		return id, 7, nil
	}, &calls
}

func newTestRun(hold time.Duration, leader func(context.Context, multiraft.GroupID) (string, uint64, error)) *chaosRun {
	return &chaosRun{cc: ChaosConfig{Hold: hold}, rec: lincheck.NewRecorder(), start: time.Now(), leader: leader}
}

// TestALeaderFaultWaitsOutAnElection: a group leaderless at the fault's
// instant is looked at again until it has a leader, which the fault then
// targets; a leader at once costs no wait; a named node is never looked up.
func TestALeaderFaultWaitsOutAnElection(t *testing.T) {
	f := Fault{At: 7 * time.Second, Kind: FaultIsolate, Group: 0, Node: TargetLeader, Hold: 1500 * time.Millisecond}

	find, calls := leaderless(5, "n2")
	node, waited, err := newTestRun(time.Second, find).resolve(context.Background(), f)
	if err != nil || node != "n2" || *calls != 6 || waited <= 0 {
		t.Fatalf("a leader after 5 leaderless looks: node %q, waited %s, err %v, %d looks", node, waited, err, *calls)
	}

	find, calls = leaderless(0, "n3")
	node, waited, err = newTestRun(time.Second, find).resolve(context.Background(), f)
	if err != nil || node != "n3" || *calls != 1 || waited != 0 {
		t.Fatalf("a leader at once: node %q, waited %s, err %v, %d looks", node, waited, err, *calls)
	}

	find, calls = leaderless(100, "never")
	named := Fault{Kind: FaultKill, Node: "n1", Hold: time.Second}
	node, waited, err = newTestRun(time.Second, find).resolve(context.Background(), named)
	if err != nil || node != "n1" || *calls != 0 || waited != 0 {
		t.Fatalf("a named node: node %q, waited %s, err %v, %d looks", node, waited, err, *calls)
	}
}

// TestALeaderFaultWithoutALeaderFailsTheRun: a group still leaderless at the
// bound — the fault's hold, or the schedule's default hold for a fault
// without one — is an error naming the bound, recorded as an error event (so
// the run does not pass), never as a skip; and a cancelled run stops waiting.
func TestALeaderFaultWithoutALeaderFailsTheRun(t *testing.T) {
	find, calls := leaderless(1<<30, "")
	f := Fault{At: 7 * time.Second, Kind: FaultIsolate, Node: TargetLeader, Hold: 60 * time.Millisecond}
	r := newTestRun(time.Second, find)
	start := time.Now()
	if undo := r.inject(context.Background(), 1, f); undo != nil {
		t.Fatal("a fault without a target returned something to undo")
	}
	if since := time.Since(start); since < 60*time.Millisecond || *calls < 2 {
		t.Fatalf("gave up after %s and %d looks: the fault's hold was not waited out", since, *calls)
	}
	res := &ChaosResult{Events: r.events, Converged: true, Check: CheckResult{Linearizable: true},
		Load: &load.Result{Classes: map[string]int64{load.ClassOK: 10}}}
	if len(res.Events) != 1 || res.Events[0].Kind != "error" || !strings.Contains(res.Events[0].Detail, "no leader of group 0 within 60ms") {
		t.Fatalf("events: %+v, want one error naming the bound", res.Events)
	}
	if res.OK() {
		t.Fatal("a run whose planned fault was not carried out passed")
	}

	// No hold of its own: the schedule's default hold is the bound.
	r = newTestRun(40*time.Millisecond, find)
	if _, _, err := r.resolve(context.Background(), Fault{Kind: FaultSnapshot, Node: TargetLeader}); err == nil || !strings.Contains(err.Error(), "within 40ms") {
		t.Fatalf("a fault without a hold: %v, want the schedule's default hold as the bound", err)
	}

	// Cancelled while waiting: the wait ends with the context, well inside the bound.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	start = time.Now()
	_, _, err := newTestRun(time.Second, find).resolve(ctx, Fault{Kind: FaultIsolate, Node: TargetLeader, Hold: 10 * time.Second})
	if !errors.Is(err, context.Canceled) || time.Since(start) > 2*time.Second {
		t.Fatalf("cancelled while waiting: %v after %s", err, time.Since(start))
	}
}
