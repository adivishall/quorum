package routing

import (
	"errors"
	"fmt"
)

// Sentinel errors returned when a Config is invalid.
//
// Like the storage layer (internal/storage/errors.go), these are the complete,
// closed set of *classes* of configuration failure. Callers branch on them with
// errors.Is, never on error strings. Every one of them is reported through a
// *ConfigError so the offending field is named without the caller having to
// parse the message.
//
// Routing never repairs an invalid configuration: it does not de-duplicate node
// identities, drop a node, invent a shard, or reorder in an undocumented way. An
// invalid Config is refused at construction, and a Router that was built is valid
// forever (docs/ROUTING.md §6).
var (
	// ErrInvalidShardCount means ShardCount was < 1 or > MaxShardCount.
	ErrInvalidShardCount = errors.New("invalid shard count")

	// ErrInvalidReplicationFactor means ReplicationFactor was < 1, or greater
	// than the number of nodes — you cannot place more distinct replicas than
	// there are nodes.
	ErrInvalidReplicationFactor = errors.New("invalid replication factor")

	// ErrNoNodes means the membership was empty. An empty ring is never valid
	// routing; a shard with no node to own it is not a state this package
	// represents.
	ErrNoNodes = errors.New("no nodes in membership")

	// ErrEmptyNodeID means a node identifier was the empty string. Node IDs are
	// opaque but must be non-empty, so that the ring label for a node
	// (docs/ROUTING.md §2) is never derived from nothing.
	ErrEmptyNodeID = errors.New("empty node id")

	// ErrDuplicateNode means the same node identity appeared twice. It is never
	// silently collapsed: a caller that meant two nodes and typed one twice has
	// a bug we surface rather than hide.
	ErrDuplicateNode = errors.New("duplicate node id")

	// ErrConfigVersion means a serialized Config carried a version this build
	// does not understand.
	ErrConfigVersion = errors.New("unsupported config version")

	// ErrMalformedConfig means serialized Config bytes could not be decoded.
	ErrMalformedConfig = errors.New("malformed config")
)

// ConfigError wraps a sentinel with the configuration field that produced it.
//
// It mirrors storage.OpError: errors.Is(err, ErrDuplicateNode) keeps working,
// while the message carries enough context to debug from a log line. The Value
// is an optional human-readable rendering of the offending input (a node id, a
// number) and may be empty.
type ConfigError struct {
	Field string // "shard_count", "replication_factor", "nodes", "version"
	Value string // the offending value, already safe to print; may be empty
	Err   error  // one of the sentinels above
}

func (e *ConfigError) Error() string {
	if e.Value == "" {
		return fmt.Sprintf("routing config: %s: %v", e.Field, e.Err)
	}
	return fmt.Sprintf("routing config: %s %s: %v", e.Field, e.Value, e.Err)
}

// Unwrap lets errors.Is and errors.As see the underlying sentinel.
func (e *ConfigError) Unwrap() error { return e.Err }

func cfgErr(field, value string, err error) error {
	return &ConfigError{Field: field, Value: value, Err: err}
}
