// Package routing implements Quorum's consistent-hash sharding (Phase 6,
// docs/ROUTING.md, ADR-012).
//
// It answers one question deterministically: which shard owns a key, and which
// nodes make up that shard's replica group. It is a pure library — it imports
// nothing from internal/storage, internal/raft, internal/transport or
// internal/cluster, touches no clock, opens no socket, and uses no global
// randomness. A route is a function of (key, Config) and is bit-identical on
// every machine, which is INV-C1.
//
// # The pipeline
//
//	key --sha256, first 8 bytes, big-endian--> Token
//	    --successor on the SHARD ring--------> ShardID          (Route)
//	    --anchor, successor walk on NODE ring-> []NodeID         (replica group, metadata)
//
// Two rings are built from one immutable Config:
//
//   - The shard ring maps key -> ShardID. It depends only on ShardCount, so it
//     never changes when nodes are added or removed — that is what a fixed shard
//     count means. It is a real consistent-hash ring with virtual nodes, not
//     token % ShardCount.
//
//   - The node ring maps each shard's anchor to an ordered replica group of node
//     IDs. A one-node membership change reinserts one node's virtual positions
//     and reassigns only the ~1/N of shards adjacent to them (INV-C3), where
//     modulo would reshuffle almost everything.
//
// The replica group is declarative metadata: Phase 6 computes who would own a
// shard; it does not replicate, elect, forward, or move data. See
// docs/ROUTING.md §9 for the explicit list of what this phase does not build.
//
// # Ownership rule
//
// A key's token is owned by the ring position with the smallest token >= it,
// wrapping to the smallest position (clockwise successor); a position P owns the
// half-open arc (predecessor, P]. Positions are sorted by (token, owner, vnode),
// so a token collision between two virtual nodes is resolved deterministically
// rather than treated as an error. A duplicate node identity, an empty node id,
// an out-of-range shard count, and an empty membership are all refused at
// construction with a *ConfigError; a Router that was built is valid forever and
// Route never errors or panics for any []byte.
package routing
