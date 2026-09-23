package raft

import (
	"math/rand"
	"sort"

	"github.com/adivishall/quorum/internal/replication"
)

// Default tick counts (docs/DESIGN.md §8.3). One tick is 50 ms in the driver, but
// the core only counts ticks and never knows the wall-clock duration.
const (
	DefaultElectionTicks  = 10
	DefaultHeartbeatTicks = 2
)

// Config constructs a Raft core. Membership is fixed (ADR-005): Peers is the whole
// group including this node.
type Config struct {
	// ID is this node's id; it must appear in Peers.
	ID NodeID
	// Peers is the fixed group membership, including ID. Order does not matter —
	// New sorts it — but it must be non-empty with no empty or duplicate id.
	Peers []NodeID

	// ElectionTicks and HeartbeatTicks default to 10 and 2. ElectionTicks must be
	// strictly greater than HeartbeatTicks.
	ElectionTicks  int
	HeartbeatTicks int

	// Rand supplies election-timeout jitter; it is injected so elections are
	// deterministic under a seed (ADR-002). Required.
	Rand *rand.Rand

	// Log is the replicated log the core drives (Phase 8). Usually a fresh
	// replication.NewMemoryLog(); on restart, one preloaded with recovered
	// entries and its commit index set. Required.
	Log replication.Log

	// Term and Vote are the recovered durable HardState (zero for a fresh node).
	Term uint64
	Vote NodeID
}

func (c *Config) withDefaults() {
	if c.ElectionTicks == 0 {
		c.ElectionTicks = DefaultElectionTicks
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = DefaultHeartbeatTicks
	}
}

// validate checks the config and returns the peers sorted (deterministic
// iteration) and dedup-verified. It never repairs: a duplicate or empty id is an
// error.
func (c *Config) validate() ([]NodeID, error) {
	if c.ID == "" {
		return nil, ErrNoID
	}
	if c.Rand == nil {
		return nil, ErrNoRand
	}
	if c.Log == nil {
		return nil, ErrNoLog
	}
	if c.ElectionTicks <= 0 || c.HeartbeatTicks <= 0 || c.ElectionTicks <= c.HeartbeatTicks {
		return nil, ErrInvalidTicks
	}
	if len(c.Peers) == 0 {
		return nil, ErrIDNotInPeers
	}
	peers := make([]NodeID, len(c.Peers))
	copy(peers, c.Peers)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	seenSelf := false
	for i, p := range peers {
		if p == "" {
			return nil, ErrEmptyPeer
		}
		if i > 0 && peers[i] == peers[i-1] {
			return nil, ErrDuplicatePeer
		}
		if p == c.ID {
			seenSelf = true
		}
	}
	if !seenSelf {
		return nil, ErrIDNotInPeers
	}
	// currentTerm can never be below a term already in the log.
	if last := c.Log.LastIndex(); last > 0 {
		lt, err := c.Log.Term(last)
		if err != nil {
			return nil, err
		}
		if c.Term < lt {
			return nil, ErrTermRegression
		}
	}
	return peers, nil
}
