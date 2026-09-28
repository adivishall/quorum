package kv

import (
	"context"
	"sync"

	"github.com/adivishall/quorum/internal/replication"
)

// Sharded is a client of a multi-group cluster (Phase 15, docs/MULTI_RAFT.md
// §6): it routes each key to its group with the cluster's routing — the same
// function the servers check requests against — and keeps one session per
// group, registered on the group's first use. A ClientID is therefore scoped
// to one group (docs/CLIENT_SEMANTICS.md): the same number in two groups names
// two unrelated sessions, and a request's identity (ClientID, RequestID) is
// deduplicated by its group alone. Every request is for one key, hence one
// group; nothing spans groups. It is safe for concurrent use.
type Sharded struct {
	eps   []Doer
	opts  SessionOptions
	route func(key []byte) replication.GroupID

	mu       sync.Mutex
	sessions map[replication.GroupID]*Session
}

// NewSharded returns a client over eps (every node's endpoint: each serves the
// groups it hosts and redirects the others).
func NewSharded(eps []Doer, opts SessionOptions, route func(key []byte) replication.GroupID) *Sharded {
	return &Sharded{eps: eps, opts: opts, route: route, sessions: map[replication.GroupID]*Session{}}
}

// Session returns the session of group g, registering it on first use.
func (c *Sharded) Session(ctx context.Context, g replication.GroupID) (*Session, error) {
	c.mu.Lock()
	s := c.sessions[g]
	c.mu.Unlock()
	if s != nil {
		return s, nil
	}
	opts := c.opts
	opts.Group = g
	s, err := Register(ctx, c.eps, opts)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev := c.sessions[g]; prev != nil {
		return prev, nil // a concurrent first use won; this session is simply unused
	}
	c.sessions[g] = s
	return s, nil
}

// Group returns the group key belongs to.
func (c *Sharded) Group(key []byte) replication.GroupID { return c.route(key) }

// Put, Get and Delete run one identified request in the key's group.
func (c *Sharded) Put(ctx context.Context, key, value []byte, hook AttemptHook) Outcome {
	return c.run(ctx, ReqPut, key, value, hook)
}

func (c *Sharded) Get(ctx context.Context, key []byte, hook AttemptHook) Outcome {
	return c.run(ctx, ReqGet, key, nil, hook)
}

func (c *Sharded) Delete(ctx context.Context, key []byte, hook AttemptHook) Outcome {
	return c.run(ctx, ReqDelete, key, nil, hook)
}

func (c *Sharded) run(ctx context.Context, op ReqOp, key, value []byte, hook AttemptHook) Outcome {
	s, err := c.Session(ctx, c.route(key))
	if err != nil {
		return Outcome{Known: true, Err: err}
	}
	return s.run(ctx, op, key, value, hook)
}
