// Package workload drives concurrent test clients against a set of key-value
// endpoints — in-process Servers or wire Clients to real dkvd processes — and
// records every operation, every attempt and every outcome into a
// lincheck.History (Phase 12, docs/LINEARIZABILITY.md §6).
//
// The client policy is fixed and documented, because it is part of what the
// history means:
//
//   - An operation starts at the client's preferred node (the last leader hint
//     it saw, else round-robin). A definite rejection that executed nothing —
//     not leader (with or without a hint), node unavailable — is followed by a
//     retry at the hinted or next node, up to MaxAttempts. Every attempt is
//     recorded; nothing is hidden.
//   - A write whose attempt ended without a definite answer (a deadline, a dead
//     connection, a node that stopped) is Incomplete: the client does NOT retry
//     it, because without deduplication (Phase 13) a retry could apply it twice
//     and the checker would rightly reject the history. A read in the same
//     situation may be retried at another node — a read has no effect.
//   - A write the server reports lost (its entry was overwritten) or invalid is
//     Rejected: a definite no-effect the checker excludes; if it did have an
//     effect a later read exposes it.
//   - Values are unique per operation, so a read pins the write it observed.
package workload

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raft"
)

// Endpoint is one node a client can talk to.
type Endpoint interface {
	Name() string
	Put(ctx context.Context, key, value []byte) (kv.Meta, error)
	Get(ctx context.Context, key []byte) ([]byte, kv.Meta, error)
	Delete(ctx context.Context, key []byte) (kv.Meta, error)
}

// Options shape a run.
type Options struct {
	Clients      int
	OpsPerClient int
	Keys         int           // keys are k0..k{Keys-1}
	Timeout      time.Duration // per attempt
	Seed         int64         // each client's operation sequence is a function of Seed and its index
	GetPct       int           // percentage of gets; DeletePct of deletes; the rest are puts
	DeletePct    int
	MaxAttempts  int // per operation, counting redirects and read retries (0 = 4)
	// Pace, if > 0, is a sleep between one client's consecutive operations, so a
	// fault scenario has time to happen mid-workload.
	Pace time.Duration
	// Stop, if non-nil, ends every client's loop early once it returns true
	// (checked before each operation).
	Stop func() bool
}

// Stats counts what the clients saw.
type Stats struct {
	Ops, OK, NotFound, Rejected, Incomplete int
	Attempts, Redirects, Unavailable        int
	ReadRetries, Unknown                    int
	Lost, Invalid                           int
}

func (s Stats) String() string {
	return fmt.Sprintf("ops=%d ok=%d notfound=%d rejected=%d incomplete=%d attempts=%d redirects=%d unavailable=%d readretries=%d unknown=%d lost=%d invalid=%d",
		s.Ops, s.OK, s.NotFound, s.Rejected, s.Incomplete, s.Attempts, s.Redirects, s.Unavailable, s.ReadRetries, s.Unknown, s.Lost, s.Invalid)
}

// Run executes the workload and returns its statistics; the history is in rec.
// It returns when every client has finished (or ctx ended).
func Run(ctx context.Context, eps []Endpoint, opts Options, rec *lincheck.Recorder) Stats {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 4
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Second
	}
	var (
		mu    sync.Mutex
		stats Stats
		wg    sync.WaitGroup
	)
	for c := 0; c < opts.Clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cl := &client{
				name: fmt.Sprintf("c%d", c+1), eps: eps, opts: opts, rec: rec,
				rng: rand.New(rand.NewSource(opts.Seed*7919 + int64(c))), next: c % len(eps),
			}
			st := cl.run(ctx)
			mu.Lock()
			stats.add(st)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	return stats
}

func (s *Stats) add(o Stats) {
	s.Ops += o.Ops
	s.OK += o.OK
	s.NotFound += o.NotFound
	s.Rejected += o.Rejected
	s.Incomplete += o.Incomplete
	s.Attempts += o.Attempts
	s.Redirects += o.Redirects
	s.Unavailable += o.Unavailable
	s.ReadRetries += o.ReadRetries
	s.Unknown += o.Unknown
	s.Lost += o.Lost
	s.Invalid += o.Invalid
}

type client struct {
	name string
	eps  []Endpoint
	opts Options
	rec  *lincheck.Recorder
	rng  *rand.Rand
	next int    // round-robin cursor
	hint string // last leader hint
	st   Stats
}

func (c *client) run(ctx context.Context) Stats {
	for i := 0; i < c.opts.OpsPerClient; i++ {
		if ctx.Err() != nil || (c.opts.Stop != nil && c.opts.Stop()) {
			break
		}
		key := fmt.Sprintf("k%d", c.rng.Intn(max(c.opts.Keys, 1)))
		var kind lincheck.Kind
		switch p := c.rng.Intn(100); {
		case p < c.opts.GetPct:
			kind = lincheck.Get
		case p < c.opts.GetPct+c.opts.DeletePct:
			kind = lincheck.Delete
		default:
			kind = lincheck.Put
		}
		var value []byte
		if kind == lincheck.Put {
			value = []byte(fmt.Sprintf("%s-%d", c.name, i))
		}
		c.op(ctx, kind, key, value)
		if c.opts.Pace > 0 {
			select {
			case <-time.After(c.opts.Pace):
			case <-ctx.Done():
			}
		}
	}
	return c.st
}

// target picks the endpoint for the next attempt: the leader hint if it names a
// known endpoint, else round-robin.
func (c *client) target() Endpoint {
	if c.hint != "" {
		for _, ep := range c.eps {
			if ep.Name() == c.hint {
				return ep
			}
		}
		c.hint = ""
	}
	ep := c.eps[c.next%len(c.eps)]
	c.next++
	return ep
}

// op runs one operation to completion under the documented policy.
func (c *client) op(ctx context.Context, kind lincheck.Kind, key string, value []byte) {
	c.st.Ops++
	id := c.rec.Begin(c.name, kind, key, value)
	unknown := false
	for attempt := 0; attempt < c.opts.MaxAttempts; attempt++ {
		ep := c.target()
		a := c.rec.Attempt(id, ep.Name())
		c.st.Attempts++
		actx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
		var (
			m   kv.Meta
			out []byte
			err error
		)
		switch kind {
		case lincheck.Put:
			m, err = ep.Put(actx, []byte(key), value)
		case lincheck.Delete:
			m, err = ep.Delete(actx, []byte(key))
		case lincheck.Get:
			out, m, err = ep.Get(actx, []byte(key))
		}
		cancel()
		var nl *kv.NotLeaderError
		switch {
		case err == nil:
			c.rec.AttemptDone(id, a, true, "ok", m.Term)
			c.rec.End(id, lincheck.OK, out, ep.Name(), m.Term, m.Index)
			c.hint = ep.Name()
			c.st.OK++
			return
		case errors.Is(err, kv.ErrNotFound):
			c.rec.AttemptDone(id, a, true, "notfound", m.Term)
			c.rec.End(id, lincheck.NotFound, nil, ep.Name(), m.Term, m.Index)
			c.hint = ep.Name()
			c.st.NotFound++
			return
		case errors.As(err, &nl):
			c.rec.AttemptDone(id, a, true, "not-leader("+nl.Leader+")", m.Term)
			c.st.Redirects++
			c.hint = nl.Leader
			if c.hint == ep.Name() {
				c.hint = "" // a node naming itself while refusing: do not spin on it
			}
		case errors.Is(err, raft.ErrNotLeader):
			c.rec.AttemptDone(id, a, true, "not-leader", m.Term)
			c.st.Redirects++
			c.hint = ""
		case errors.Is(err, kv.ErrUnavailable):
			c.rec.AttemptDone(id, a, true, "unavailable", 0)
			c.st.Unavailable++
			c.hint = ""
		case errors.Is(err, kv.ErrLost):
			c.rec.AttemptDone(id, a, true, "lost", m.Term)
			c.rec.End(id, lincheck.Rejected, nil, ep.Name(), m.Term, m.Index)
			c.st.Rejected++
			c.st.Lost++
			return
		case errors.Is(err, kv.ErrInvalid):
			c.rec.AttemptDone(id, a, true, "invalid", 0)
			c.rec.End(id, lincheck.Rejected, nil, ep.Name(), 0, 0)
			c.st.Rejected++
			c.st.Invalid++
			return
		default:
			// A deadline, a dead connection, a stopped node: no answer.
			c.rec.AttemptDone(id, a, false, "unknown: "+err.Error(), m.Term)
			c.st.Unknown++
			c.hint = ""
			unknown = true
			if kind != lincheck.Get {
				c.rec.End(id, lincheck.Incomplete, nil, "", 0, 0)
				c.st.Incomplete++
				return
			}
			c.st.ReadRetries++
		}
	}
	// Out of attempts. If some attempt got no answer the effect is unknown;
	// otherwise every attempt was a definite refusal and nothing executed.
	if unknown {
		c.rec.End(id, lincheck.Incomplete, nil, "", 0, 0)
		c.st.Incomplete++
		return
	}
	c.rec.End(id, lincheck.Rejected, nil, "", 0, 0)
	c.st.Rejected++
}
