package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Session is a client session (docs/CLIENT_SEMANTICS.md): the cluster-assigned
// ClientID, the next RequestID, and the requests in flight, from which every
// request's AckedBelow is computed. It retries a request whose outcome is
// unknown — a timeout, a dead connection, a forward that went unanswered, a
// lost entry — under the SAME identity, which the server's session table makes
// safe: however many of the attempts reach the state machine, the write takes
// effect once. It is safe for concurrent use: requests from several goroutines
// share one session and one watermark.
type Session struct {
	eps  []Doer
	opts SessionOptions

	mu       sync.Mutex
	id       uint64
	next     uint64
	inflight map[uint64]int // request id → sends outstanding (duplicates may share an id)
	hint     string
	rr       int
}

// SessionOptions shape a session's retry policy.
type SessionOptions struct {
	AttemptTimeout time.Duration // per attempt (default 2s)
	MaxAttempts    int           // per request, counting redirects and retries (default 8)
	Backoff        time.Duration // after a refusal that names no usable leader (default 20ms), doubling per consecutive one up to maxBackoff
}

// maxBackoff caps the back-off (unless Backoff itself is larger): an election
// under load can take far longer than MaxAttempts × Backoff, and a client that
// burns its attempts at a fixed short interval gives up on a request that
// would complete moments later.
const maxBackoff = time.Second

func (o *SessionOptions) defaults() {
	if o.AttemptTimeout <= 0 {
		o.AttemptTimeout = 2 * time.Second
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 8
	}
	if o.Backoff <= 0 {
		o.Backoff = 20 * time.Millisecond
	}
}

// AttemptHook is told about every attempt of a request: before it is sent
// (node), and — through the function it returns — how it ended. It is how a
// history recorder sees transport attempts. nil is allowed.
type AttemptHook func(node string) func(resp Response, err error)

// Outcome is how one logical request ended.
type Outcome struct {
	RequestID uint64
	// Response is the last response received (the final one when Err is nil).
	Response Response
	// Known is false when the request's effect is unknown: some attempt went
	// unanswered and no later answer settled it. A request with Known false
	// may have taken effect.
	Known bool
	// Err is nil for OK and NOT_FOUND; otherwise why the request stopped.
	Err      error
	Attempts int
}

// Register creates a session on the cluster reachable through eps. A REGISTER
// whose answer is lost may have created a session nobody will use; that is
// harmless (it is evicted eventually), so Register simply tries again.
func Register(ctx context.Context, eps []Doer, opts SessionOptions) (*Session, error) {
	opts.defaults()
	s := &Session{eps: eps, opts: opts, next: 1, inflight: map[uint64]int{}}
	var last error
	backoffs := 0
	for attempt := 0; attempt < opts.MaxAttempts && ctx.Err() == nil; attempt++ {
		ep := s.target()
		actx, cancel := context.WithTimeout(ctx, opts.AttemptTimeout)
		resp, err := ep.Do(actx, Request{Op: ReqRegister, Timeout: opts.AttemptTimeout})
		cancel()
		switch {
		case err != nil:
			last = err
			s.miss()
			s.sleep(ctx, &backoffs)
		case resp.Status == StatusOK && resp.ClientID != 0:
			s.id = resp.ClientID
			return s, nil
		case resp.Status == StatusNotLeader:
			last = errorOf(resp)
			if s.redirect(resp.Leader, ep.Name()) {
				backoffs = 0
			} else {
				s.sleep(ctx, &backoffs)
			}
		default:
			last = errorOf(resp)
			s.miss()
			s.sleep(ctx, &backoffs)
		}
	}
	if last == nil {
		last = ctx.Err()
	}
	return nil, fmt.Errorf("kv: register: %w", last)
}

// ResumeSession continues a session a client persisted: its ClientID and the
// next RequestID it had not used (a client restart that kept both may retry
// its unfinished requests safely).
func ResumeSession(eps []Doer, opts SessionOptions, clientID, next uint64) *Session {
	opts.defaults()
	return &Session{eps: eps, opts: opts, id: clientID, next: next, inflight: map[uint64]int{}}
}

// ID is the session's ClientID.
func (s *Session) ID() uint64 { return s.id }

// Next is the next RequestID the session will use (what a client persists to
// resume).
func (s *Session) Next() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

// Reserve takes the next RequestID and marks it in flight. Every reserved id
// must be released (Release), after which it is never sent again.
func (s *Session) Reserve() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	rid := s.next
	s.next++
	s.inflight[rid]++
	return rid
}

// Hold marks an already-reserved id as in flight once more, for a second
// concurrent send of the same request (a deliberate duplicate); each Hold needs
// its own Release.
func (s *Session) Hold(rid uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight[rid]++
}

// Release ends one send of rid; when none is left the id is acknowledged — the
// session promises never to send it again, so the server may forget its result.
func (s *Session) Release(rid uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[rid] <= 1 {
		delete(s.inflight, rid)
	} else {
		s.inflight[rid]--
	}
}

// ackedBelow is the lowest request id the session still has in flight (or the
// next one, if none): every response below it has been received or given up on.
func (s *Session) ackedBelow() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.next
	for rid := range s.inflight {
		if rid < w {
			w = rid
		}
	}
	return w
}

// Put, Get and Delete run one logical request each, with a fresh RequestID.
func (s *Session) Put(ctx context.Context, key, value []byte, hook AttemptHook) Outcome {
	return s.run(ctx, ReqPut, key, value, hook)
}
func (s *Session) Get(ctx context.Context, key []byte, hook AttemptHook) Outcome {
	return s.run(ctx, ReqGet, key, nil, hook)
}
func (s *Session) Delete(ctx context.Context, key []byte, hook AttemptHook) Outcome {
	return s.run(ctx, ReqDelete, key, nil, hook)
}

func (s *Session) run(ctx context.Context, op ReqOp, key, value []byte, hook AttemptHook) Outcome {
	rid := s.Reserve()
	defer s.Release(rid)
	return s.Send(ctx, rid, op, key, value, hook)
}

// Send runs the logical request (s.ID(), rid) to a definite answer or until the
// attempts run out, retrying unknown outcomes under the same identity. rid must
// be reserved (or held) by the caller. A caller that sends an id it already
// used — to test the contract, or after a restart — gets the contract's answer:
// the original result for the same command, REQUEST_CONFLICT for another.
func (s *Session) Send(ctx context.Context, rid uint64, op ReqOp, key, value []byte, hook AttemptHook) Outcome {
	out := Outcome{RequestID: rid, Known: true}
	unknown := false
	backoffs := 0 // consecutive back-offs: reset whenever a usable leader is named
	for out.Attempts < s.opts.MaxAttempts && ctx.Err() == nil {
		out.Attempts++
		ep := s.target()
		req := Request{Op: op, ClientID: s.id, RequestID: rid, AckedBelow: min(s.ackedBelow(), rid), Key: key, Value: value, Timeout: s.opts.AttemptTimeout}
		var done func(Response, error)
		if hook != nil {
			done = hook(ep.Name())
		}
		actx, cancel := context.WithTimeout(ctx, s.opts.AttemptTimeout)
		resp, err := ep.Do(actx, req)
		cancel()
		if done != nil {
			done(resp, err)
		}
		out.Response = resp
		switch {
		case err != nil && errors.Is(err, ErrUnavailable):
			s.miss() // nothing was sent: try elsewhere
			s.sleep(ctx, &backoffs)
		case err != nil:
			unknown = true // sent, no answer: retry the same request
			s.miss()
		case resp.Status == StatusOK || resp.Status == StatusNotFound:
			out.Known, out.Err = true, nil
			s.redirect(resp.Node, "")
			return out
		case resp.Status == StatusNotLeader:
			if s.redirect(resp.Leader, ep.Name()) {
				backoffs = 0
			} else {
				s.sleep(ctx, &backoffs)
			}
		case resp.Status == StatusUnavailable:
			s.miss()
			s.sleep(ctx, &backoffs)
		case resp.Status == StatusUnknown:
			unknown = true
			s.miss()
		case resp.Status == StatusLost:
			// This attempt's entry was overwritten: definitely no effect from
			// it. Retrying the same identity is safe.
		case resp.Status == StatusSessionLimit:
			s.sleep(ctx, &backoffs)
		default:
			// SESSION_EXPIRED, REQUEST_CONFLICT, REQUEST_STALE, INVALID: stop.
			// A conflict or stale answer means THIS command did not execute
			// under rid now, but an earlier unanswered attempt's fate is still
			// unknown — the session may have expired after it executed.
			out.Known, out.Err = !unknown, errorOf(resp)
			return out
		}
	}
	out.Known = !unknown
	if unknown {
		out.Err = fmt.Errorf("%w: request %d of client %d after %d attempts", ErrUnknown, rid, s.id, out.Attempts)
	} else {
		out.Err = fmt.Errorf("kv: request %d of client %d refused on every attempt: %w", rid, s.id, errorOf(out.Response))
	}
	return out
}

// target picks the endpoint for the next attempt: the leader hint if it names
// an endpoint, else round-robin.
func (s *Session) target() Doer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hint != "" {
		for _, ep := range s.eps {
			if ep.Name() == s.hint {
				return ep
			}
		}
		s.hint = ""
	}
	ep := s.eps[s.rr%len(s.eps)]
	s.rr++
	return ep
}

// redirect follows a leader hint; it reports whether the hint was usable.
func (s *Session) redirect(leader, from string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if leader == "" || leader == from {
		s.hint = ""
		return false
	}
	s.hint = leader
	return true
}

func (s *Session) miss() {
	s.mu.Lock()
	s.hint = ""
	s.mu.Unlock()
}

// sleep backs off before the next attempt: Backoff the first time, doubling
// for each consecutive back-off (*n counts them) up to maxBackoff.
func (s *Session) sleep(ctx context.Context, n *int) {
	d := backoffFor(s.opts.Backoff, *n)
	*n++
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

// backoffFor is the n-th consecutive back-off (from 0): base·2ⁿ, capped at
// maxBackoff — or at base, if base is larger.
func backoffFor(base time.Duration, n int) time.Duration {
	limit := max(base, maxBackoff)
	d := base
	for i := 0; i < n && d < limit; i++ {
		d *= 2
	}
	return min(d, limit)
}
