package raftnode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
)

// TestAnAnswerGivenBeforeTheStopIsTheOutcome: the actor answers a request it
// took, then the node stops (fail: every waiter completed, then cancel) before
// the caller has read the answer — the exact state TestApplyFailureStopsTheNode
// reached on the CI runner, where a plain select then chose ErrStopped half the
// time and the write never learnt ErrApply. The answer must win every time:
// a thousand trials, where a random choice fails one in two.
func TestAnAnswerGivenBeforeTheStopIsTheOutcome(t *testing.T) {
	stopped := make(chan struct{})
	close(stopped)
	for i := 0; i < 1000; i++ {
		ch := make(chan writeAccepted, 1)
		ch <- writeAccepted{index: 7, term: 3}
		acc, err := answer(context.Background(), ch, stopped)
		if err != nil || acc.index != 7 || acc.term != 3 {
			t.Fatalf("trial %d: the answer was given before the stop, got %+v, %v", i, acc, err)
		}
	}
	// With the caller's deadline also passed, the stop still does not hide an
	// answer: the caller's ctx may win (a timeout is a timeout), the stop may not.
	done, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 1000; i++ {
		ch := make(chan error, 1)
		ch <- ErrApply
		got, err := answer(done, ch, stopped)
		if errors.Is(err, raft.ErrStopped) || (err == nil && !errors.Is(got, ErrApply)) {
			t.Fatalf("trial %d: got %v, %v; the stop hid the answer", i, got, err)
		}
	}
}

// TestNoAnswerBeforeTheStopIsStopped: a node that stops without having
// answered — the actor never took the request, or took it and the node was
// closed before it answered — is ErrStopped (the outcome unknown); a caller
// whose ctx ends first learns ctx.Err(); and an answer that arrives while
// nothing has ended is the outcome.
func TestNoAnswerBeforeTheStopIsStopped(t *testing.T) {
	stopped := make(chan struct{})
	close(stopped)
	if _, err := answer(context.Background(), make(chan error, 1), stopped); !errors.Is(err, raft.ErrStopped) {
		t.Fatalf("no answer, stopped: %v, want ErrStopped", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := answer(ctx, make(chan error, 1), make(chan struct{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("no answer, the caller gave up: %v, want context.Canceled", err)
	}
	ch := make(chan int, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		ch <- 42
	}()
	if got, err := answer(context.Background(), ch, make(chan struct{})); err != nil || got != 42 {
		t.Fatalf("an answer with nothing ended: %d, %v", got, err)
	}
}
