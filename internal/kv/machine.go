package kv

import "github.com/adivishall/quorum/internal/raftnode"

// Machine is a replicated state machine the key-value front can serve: what
// the server, the metrics and the tests need of one, whichever holds its state
// (S2, docs/STORAGE_INTEGRATION.md §8.5). *Store holds it in memory and is the
// reference; *LSMMachine holds it in the storage engine. Nothing above a
// Machine — the front, the server, raftnode, multiraft — knows which it has.
type Machine interface {
	raftnode.StateMachine
	// Get returns a copy of the value under key and whether the key is present.
	Get(key []byte) ([]byte, bool)
	// Applied returns the highest log index applied.
	Applied() uint64
	// Sessions returns a copy of the session table.
	Sessions() map[uint64]SessionState
	// Snapshot returns a copy of every present key (tests compare replicas with it).
	Snapshot() map[string][]byte
	// Stats returns the decision counters.
	Stats() ApplyStats
	// setObserve installs the decision counters (Metrics.Observe).
	setObserve(observe func(Decision), evicted func())
}

var _ Machine = (*Store)(nil)
