package routing

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
)

// NodeID is an opaque node identifier. It is compared bytewise and must be
// non-empty; it is never normalised. In v1 these are cluster-bootstrap names
// like "n0"; nothing in routing interprets their structure.
type NodeID string

// ShardID identifies one shard, in the range [0, ShardCount). It is a strong
// type so a shard number cannot be silently swapped with a node index or a raw
// int elsewhere in the system.
type ShardID uint32

// Defaults and bounds (docs/ROUTING.md §4, §6).
const (
	// DefaultShardCount is the v1 fixed shard count (docs/ARCHITECTURE.md §4).
	DefaultShardCount = 16

	// DefaultReplicationFactor is the declarative replica-group size Phase 6
	// records per shard. It does not replicate anything (docs/ROUTING.md §9).
	DefaultReplicationFactor = 3

	// MaxShardCount bounds the shard ring so a typo (ShardCount = 1e9) is a
	// clear error rather than an out-of-memory. Far beyond any v1 need.
	MaxShardCount = 4096

	// VNodesPerShard and VNodesPerNode are the fixed virtual-node multipliers.
	// Higher values reduce load variance (~1/sqrt(V)) and smooth redistribution;
	// 128 is a documented default, not a measured optimum (ADR-012).
	VNodesPerShard = 128
	VNodesPerNode  = 128
)

// Config is the immutable routing configuration — the "membership configuration"
// INV-C1 names. A Router is a pure function of a key and a Config.
//
// It is a plain struct so callers can build it literally; all validation happens
// in NewRouter, which copies the fields defensively. The zero Config is invalid
// (no nodes); use DefaultConfig and set Nodes.
type Config struct {
	// ShardCount is the fixed number of shards, in [1, MaxShardCount].
	ShardCount int

	// ReplicationFactor is the declarative replica-group size per shard, in
	// [1, len(Nodes)].
	ReplicationFactor int

	// Nodes is the cluster membership. Order does not matter — NewRouter sorts
	// and dedup-checks — but it must be non-empty with no empty or duplicate id.
	Nodes []NodeID
}

// DefaultConfig returns a Config with the v1 defaults and no nodes. The caller
// sets Nodes before passing it to NewRouter.
func DefaultConfig() Config {
	return Config{
		ShardCount:        DefaultShardCount,
		ReplicationFactor: DefaultReplicationFactor,
	}
}

// validate checks the Config and returns the nodes sorted and dedup-verified, so
// that routing is independent of the order nodes were supplied in (INV-C1). It
// never repairs: a duplicate or empty id is an error, not a silent drop.
func (c Config) validate() ([]NodeID, error) {
	if c.ShardCount < 1 || c.ShardCount > MaxShardCount {
		return nil, cfgErr("shard_count", strconv.Itoa(c.ShardCount), ErrInvalidShardCount)
	}
	if len(c.Nodes) == 0 {
		return nil, cfgErr("nodes", "", ErrNoNodes)
	}
	if c.ReplicationFactor < 1 {
		return nil, cfgErr("replication_factor", strconv.Itoa(c.ReplicationFactor), ErrInvalidReplicationFactor)
	}
	if c.ReplicationFactor > len(c.Nodes) {
		return nil, cfgErr("replication_factor",
			strconv.Itoa(c.ReplicationFactor)+" > "+strconv.Itoa(len(c.Nodes))+" nodes",
			ErrInvalidReplicationFactor)
	}

	nodes := make([]NodeID, len(c.Nodes))
	copy(nodes, c.Nodes)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	for i, n := range nodes {
		if n == "" {
			return nil, cfgErr("nodes", "", ErrEmptyNodeID)
		}
		if i > 0 && nodes[i] == nodes[i-1] {
			return nil, cfgErr("nodes", strconv.Quote(string(n)), ErrDuplicateNode)
		}
	}
	return nodes, nil
}

// configVersion is the on-disk/on-wire version of the canonical serialization.
// It is bumped if the serialized shape changes (ADR-009's versioning discipline).
const configVersion = 1

// canonicalConfig is the deterministic JSON shape. Nodes are always sorted, so
// the same membership serializes to the same bytes regardless of input order.
type canonicalConfig struct {
	Version           int      `json:"version"`
	ShardCount        int      `json:"shard_count"`
	ReplicationFactor int      `json:"replication_factor"`
	Nodes             []string `json:"nodes"`
}

// Canonical returns the deterministic, versioned JSON encoding of the Config
// after validation: nodes sorted, a version field, stable key order. Two Configs
// with the same membership in any order produce byte-identical output. This is
// what makes INV-C1's "same configuration reconstructed from serialized data"
// testable.
func (c Config) Canonical() ([]byte, error) {
	nodes, err := c.validate()
	if err != nil {
		return nil, err
	}
	ss := make([]string, len(nodes))
	for i, n := range nodes {
		ss[i] = string(n)
	}
	// json.Marshal of a struct emits fields in declaration order, so this is
	// deterministic without any post-processing.
	return json.Marshal(canonicalConfig{
		Version:           configVersion,
		ShardCount:        c.ShardCount,
		ReplicationFactor: c.ReplicationFactor,
		Nodes:             ss,
	})
}

// ParseConfig reconstructs a Config from Canonical bytes. It rejects an unknown
// version and malformed input; it does not validate the fields further (NewRouter
// does that), so a round trip is ParseConfig then NewRouter.
func ParseConfig(data []byte) (Config, error) {
	var cc canonicalConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cc); err != nil {
		return Config{}, cfgErr("version", "", ErrMalformedConfig)
	}
	if cc.Version != configVersion {
		return Config{}, cfgErr("version", strconv.Itoa(cc.Version), ErrConfigVersion)
	}
	nodes := make([]NodeID, len(cc.Nodes))
	for i, s := range cc.Nodes {
		nodes[i] = NodeID(s)
	}
	return Config{
		ShardCount:        cc.ShardCount,
		ReplicationFactor: cc.ReplicationFactor,
		Nodes:             nodes,
	}, nil
}
