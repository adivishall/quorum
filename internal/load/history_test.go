package load

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
)

// mapServer is a linearizable key-value server with sessions and
// deduplication, in one process: every request is applied atomically under one
// lock. It answers UNKNOWN_OUTCOME — after applying — to the first attempt of
// every flakyEvery-th identified write, so a retry under the same identity
// settles it as a duplicate; and to every attempt on key lostKey, so those
// writes stay unknown however often they are retried.
type mapServer struct {
	flakyEvery int
	lostKey    string

	mu       sync.Mutex
	data     map[string][]byte
	nextID   uint64
	results  map[[2]uint64]kv.Response // (client, request) → the first answer
	writes   int
	attempts map[[2]uint64]int
}

func (s *mapServer) Name() string { return "map" }

func (s *mapServer) Do(_ context.Context, req kv.Request) (kv.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch req.Op {
	case kv.ReqRegister:
		s.nextID++
		return kv.Response{Status: kv.StatusOK, ClientID: s.nextID, Index: s.nextID}, nil
	case kv.ReqGet:
		v, ok := s.data[string(req.Key)]
		if !ok {
			return kv.Response{Status: kv.StatusNotFound, Node: "map"}, nil
		}
		return kv.Response{Status: kv.StatusOK, Value: append([]byte(nil), v...), Node: "map"}, nil
	}
	id := [2]uint64{req.ClientID, req.RequestID}
	s.attempts[id]++
	resp, seen := s.results[id]
	if !seen {
		if req.Op == kv.ReqPut {
			s.data[string(req.Key)] = append([]byte(nil), req.Value...)
		} else {
			delete(s.data, string(req.Key))
		}
		s.writes++
		resp = kv.Response{Status: kv.StatusOK, Node: "map", Index: uint64(s.writes)}
		s.results[id] = resp
	}
	if string(req.Key) == s.lostKey || (s.flakyEvery > 0 && s.attempts[id] == 1 && s.writes%s.flakyEvery == 0 && !seen) {
		return kv.Response{Status: kv.StatusUnknown, Message: "dropped by the test"}, nil
	}
	return resp, nil
}

func startMap(t *testing.T, s *mapServer) Endpoint {
	t.Helper()
	s.data, s.results, s.attempts = map[string][]byte{}, map[[2]uint64]kv.Response{}, map[[2]uint64]int{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go kv.Serve(ctx, ln, s, nil)
	return Endpoint{"map", ln.Addr().String()}
}

// TestHistoryRecordsEveryOperation: with a History, every operation the run
// issues — warmup included — is in it, with its session identity and its
// attempts; every write carries a distinct value; a write whose first answer
// was lost is retried under its identity and recorded once, with both
// attempts; and the history of a linearizable server checks out.
func TestHistoryRecordsEveryOperation(t *testing.T) {
	ep := startMap(t, &mapServer{flakyEvery: 5})
	rec := lincheck.NewRecorder()
	res, err := Run(context.Background(), Config{Endpoints: []Endpoint{ep}, Clients: 4, Duration: 600 * time.Millisecond,
		Warmup: 200 * time.Millisecond, ReadPct: 40, DeletePct: 10, Keys: 8, ValueSize: 32, Seed: 9, History: rec})
	if err != nil {
		t.Fatal(err)
	}
	h := rec.History()
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	if int64(len(h.Ops)) != res.Issued {
		t.Fatalf("%d operations in the history, %d issued", len(h.Ops), res.Issued)
	}
	values := map[string]bool{}
	retried := 0
	for _, op := range h.Ops {
		if op.Kind == lincheck.Get {
			continue
		}
		if op.ClientID == 0 || op.RequestID == 0 {
			t.Fatalf("an identified write recorded without its identity: %s", op)
		}
		if op.Kind == lincheck.Put {
			if values[string(op.Value)] || len(op.Value) < 32 {
				t.Fatalf("value %q is repeated or shorter than ValueSize", op.Value)
			}
			values[string(op.Value)] = true
		}
		if len(op.Attempts) > 1 {
			retried++
			if op.Outcome != lincheck.OK {
				t.Fatalf("a write whose first answer was lost was not settled by its retry: %s", op)
			}
		}
	}
	if retried == 0 {
		t.Fatal("no write was retried: the premise (lost first answers) never held")
	}
	if r := lincheck.Check(h, lincheck.Options{}); !r.OK || r.Unchecked {
		t.Fatalf("the history of a linearizable server does not check: %s", r.Reason)
	}
}

// TestTracesKeepEveryUnknownOperation: operations whose every attempt went
// unanswered end unknown — counted, recorded as Incomplete (never dropped),
// and traced attempt by attempt, each attempt with its node and its answer.
// Operations that ended with a definite answer are not traced.
func TestTracesKeepEveryUnknownOperation(t *testing.T) {
	ep := startMap(t, &mapServer{lostKey: string(KeyName(0))})
	rec := lincheck.NewRecorder()
	res, err := Run(context.Background(), Config{Endpoints: []Endpoint{ep}, Clients: 2, Duration: 500 * time.Millisecond,
		ReadPct: 0, Keys: 3, Seed: 4, MaxAttempts: 3, History: rec, Trace: true})
	if err != nil {
		t.Fatal(err)
	}
	unknown := res.Classes[ClassUnknown]
	if unknown == 0 {
		t.Fatalf("no unknown outcome: the premise (a key whose writes are never answered) never held: %v", res.Classes)
	}
	if int64(len(res.Traces)) != unknown || res.TracesDropped != 0 {
		t.Fatalf("%d traces, %d dropped, %d unknown outcomes", len(res.Traces), res.TracesDropped, unknown)
	}
	for _, tr := range res.Traces {
		if tr.Class != ClassUnknown || tr.Key != string(KeyName(0)) || len(tr.Attempts) != 3 || tr.Err == "" {
			t.Fatalf("trace: %+v", tr)
		}
		for _, a := range tr.Attempts {
			if a.Node != "map" || a.Status != kv.StatusUnknown.String() || a.Duration <= 0 {
				t.Fatalf("attempt: %+v", a)
			}
		}
	}
	incomplete := 0
	for _, op := range rec.History().Ops {
		if op.Outcome == lincheck.Incomplete {
			incomplete++
			if len(op.Attempts) != 3 {
				t.Fatalf("an unknown write recorded with %d attempts, want 3: %s", len(op.Attempts), op)
			}
		}
	}
	if int64(incomplete) != unknown {
		t.Fatalf("%d incomplete operations in the history, %d unknown outcomes", incomplete, unknown)
	}
	if r := lincheck.Check(rec.History(), lincheck.Options{}); !r.OK {
		t.Fatalf("history: %s", r.Reason)
	}
}
