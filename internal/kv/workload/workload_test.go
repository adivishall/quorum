package workload

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
)

// fakeEndpoint answers from a script of results, one per request (the last one
// repeating once the script runs out), and counts requests. It exercises the
// client policy without a cluster.
type fakeEndpoint struct {
	name    string
	results []error
	calls   int
	value   []byte
}

func (f *fakeEndpoint) Name() string { return f.name }
func (f *fakeEndpoint) next() error {
	f.calls++
	if len(f.results) == 0 {
		return nil
	}
	err := f.results[0]
	if len(f.results) > 1 {
		f.results = f.results[1:]
	}
	return err
}
func (f *fakeEndpoint) Put(ctx context.Context, key, value []byte) (kv.Meta, error) {
	return kv.Meta{Node: f.name, Term: 1, Index: 1}, f.next()
}
func (f *fakeEndpoint) Get(ctx context.Context, key []byte) ([]byte, kv.Meta, error) {
	return f.value, kv.Meta{Node: f.name, Term: 1, Index: 1}, f.next()
}
func (f *fakeEndpoint) Delete(ctx context.Context, key []byte) (kv.Meta, error) {
	return kv.Meta{Node: f.name, Term: 1, Index: 1}, f.next()
}

var errUnknown = fmt.Errorf("%w: connection reset", kv.ErrUnknown)

// TestClientPolicy pins the documented client policy, because it is part of
// what a recorded history means: an unknown write is NEVER retried (without
// deduplication a retry could apply it twice under one operation), an unknown
// read may be, a refusal is followed to the hinted leader, and every attempt is
// recorded.
func TestClientPolicy(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		kind     lincheck.Kind
		n1, n2   []error
		outcome  lincheck.Outcome
		attempts int
	}{
		{"unknown write is incomplete and not retried", lincheck.Put, []error{errUnknown}, nil, lincheck.Incomplete, 1},
		{"unknown delete is incomplete and not retried", lincheck.Delete, []error{errUnknown}, nil, lincheck.Incomplete, 1},
		{"unknown read is retried", lincheck.Get, []error{errUnknown, nil}, nil, lincheck.OK, 2},
		{"not-leader follows the hint", lincheck.Put, []error{&kv.NotLeaderError{Node: "n1", Leader: "n2"}}, []error{nil}, lincheck.OK, 2},
		{"unavailable is retried", lincheck.Put, []error{fmt.Errorf("%w: refused", kv.ErrUnavailable), nil}, nil, lincheck.OK, 2},
		{"lost is a definite rejection", lincheck.Put, []error{kv.ErrLost}, nil, lincheck.Rejected, 1},
		{"invalid is a definite rejection", lincheck.Put, []error{fmt.Errorf("%w: empty key", kv.ErrInvalid)}, nil, lincheck.Rejected, 1},
		{"refused everywhere is rejected", lincheck.Put,
			[]error{&kv.NotLeaderError{Node: "n1"}}, []error{&kv.NotLeaderError{Node: "n2"}}, lincheck.Rejected, 4},
		{"a read that never got an answer is incomplete", lincheck.Get,
			[]error{errUnknown}, []error{errUnknown}, lincheck.Incomplete, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n1 := &fakeEndpoint{name: "n1", results: tc.n1, value: []byte("v")}
			n2 := &fakeEndpoint{name: "n2", results: tc.n2, value: []byte("v")}
			rec := lincheck.NewRecorder()
			c := NewClient("c1", []Endpoint{n1, n2}, Options{MaxAttempts: 4}, rec)
			c.Prefer("n1")
			var op lincheck.Op
			switch tc.kind {
			case lincheck.Put:
				op = c.Put(ctx, "k", []byte("x"))
			case lincheck.Delete:
				op = c.Delete(ctx, "k")
			default:
				op = c.Get(ctx, "k")
			}
			if op.Outcome != tc.outcome || len(op.Attempts) != tc.attempts || n1.calls+n2.calls != tc.attempts {
				t.Fatalf("outcome %s with %d recorded attempts (%d requests made), want %s with %d: %+v",
					op.Outcome, len(op.Attempts), n1.calls+n2.calls, tc.outcome, tc.attempts, op.Attempts)
			}
			if tc.kind != lincheck.Get && op.Outcome == lincheck.Incomplete && n1.calls+n2.calls != 1 {
				t.Fatal("an unknown write was retried")
			}
			if err := rec.History().Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if !errors.Is(errUnknown, kv.ErrUnknown) {
		t.Fatal("test setup")
	}
}
