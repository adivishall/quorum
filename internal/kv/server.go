package kv

import (
	"context"
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
)

// Errors a client-visible operation can end with. Each is one of the three
// outcome classes docs/LINEARIZABILITY.md §4 distinguishes:
//
//	definite, no effect   NotLeaderError, ErrLost (raftnode.ErrLost), ErrInvalid
//	definite, effect      nil (and ErrNotFound for a Get: the read happened)
//	UNKNOWN               ErrUnknown (wrapping the cause): a deadline, a dead
//	                      connection, a node that stopped — the write may have
//	                      taken effect, the read has no effect
var (
	ErrNotFound = errors.New("kv: key not found")
	ErrInvalid  = errors.New("kv: invalid request")
	ErrUnknown  = errors.New("kv: outcome unknown")
	ErrLost     = raftnode.ErrLost
)

// NotLeaderError is the definite rejection of a node that is not the leader,
// with the leader it believes in (empty if it knows none). errors.Is(err,
// raft.ErrNotLeader) holds for it.
type NotLeaderError struct {
	Node   string
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return fmt.Sprintf("kv: %s is not the leader (leader unknown)", e.Node)
	}
	return fmt.Sprintf("kv: %s is not the leader (leader %s)", e.Node, e.Leader)
}

// Is makes errors.Is(err, raft.ErrNotLeader) true.
func (e *NotLeaderError) Is(target error) bool { return target == raft.ErrNotLeader }

// Meta is where and when a completed operation was served: the node, the term
// it was served in, and the log index it completed at (a write's entry, a
// read's read index).
type Meta struct {
	Node  string
	Term  uint64
	Index uint64
}

// Server serves PUT/GET/DELETE on one node of a Raft group: writes through
// Node.Write (completed only when committed and applied here in the proposal's
// term), reads through Node.ReadIndex then the local Store (docs/DESIGN.md §8.5).
// The Store must be the node's state machine.
type Server struct {
	id    string
	node  *raftnode.Node
	store *Store
}

// NewServer wires a node and its state machine.
func NewServer(id string, node *raftnode.Node, store *Store) *Server {
	return &Server{id: id, node: node, store: store}
}

// Name is the node id.
func (s *Server) Name() string { return s.id }

// Store returns the node's state machine.
func (s *Server) Store() *Store { return s.store }

// Node returns the node.
func (s *Server) Node() *raftnode.Node { return s.node }

func validate(key, value []byte, hasValue bool) error {
	if len(key) == 0 {
		return fmt.Errorf("%w: empty key", ErrInvalid)
	}
	if len(key) > MaxKeyLen {
		return fmt.Errorf("%w: key of %d bytes exceeds %d", ErrInvalid, len(key), MaxKeyLen)
	}
	if hasValue && len(value) > MaxValueLen {
		return fmt.Errorf("%w: value of %d bytes exceeds %d", ErrInvalid, len(value), MaxValueLen)
	}
	return nil
}

// Put stores value under key. It returns nil only once the write is committed
// by a quorum and applied on this node.
func (s *Server) Put(ctx context.Context, key, value []byte) (Meta, error) {
	if err := validate(key, value, true); err != nil {
		return Meta{Node: s.id}, err
	}
	return s.write(ctx, Command{Op: OpPut, Key: key, Value: value})
}

// Delete removes key (idempotent). Same completion rule as Put.
func (s *Server) Delete(ctx context.Context, key []byte) (Meta, error) {
	if err := validate(key, nil, false); err != nil {
		return Meta{Node: s.id}, err
	}
	return s.write(ctx, Command{Op: OpDelete, Key: key})
}

func (s *Server) write(ctx context.Context, c Command) (Meta, error) {
	idx, term, err := s.node.Write(ctx, c.Encode())
	m := Meta{Node: s.id, Term: term, Index: idx}
	return m, s.mapErr(err)
}

// Get returns the value under key after a confirmed ReadIndex has been applied
// locally, so the read is linearizable with respect to every write that
// completed before it was invoked. ErrNotFound means the key is absent.
func (s *Server) Get(ctx context.Context, key []byte) ([]byte, Meta, error) {
	if err := validate(key, nil, false); err != nil {
		return nil, Meta{Node: s.id}, err
	}
	idx, err := s.node.ReadIndex(ctx)
	m := Meta{Node: s.id, Term: s.node.Term(), Index: idx}
	if err != nil {
		return nil, m, s.mapErr(err)
	}
	v, ok := s.store.Get(key)
	if !ok {
		return nil, m, ErrNotFound
	}
	return v, m, nil
}

// mapErr classifies a driver error into the three outcome classes.
func (s *Server) mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrNotLeader):
		return &NotLeaderError{Node: s.id, Leader: string(s.node.LeaderID())}
	case errors.Is(err, ErrLost):
		return ErrLost
	default:
		return fmt.Errorf("%w: %v", ErrUnknown, err)
	}
}
