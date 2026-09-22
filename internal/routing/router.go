package routing

// Router is an immutable, deterministic routing table built from a Config. It is
// safe for concurrent use: nothing about it mutates after NewRouter returns.
//
// A Router owns two rings (docs/ROUTING.md §1):
//   - the shard ring maps key -> ShardID (Route);
//   - the node ring maps each shard's anchor -> an ordered replica group of node
//     IDs (declarative metadata; Phase 6 does not replicate — §9).
type Router struct {
	shardCount int
	rf         int
	nodes      []NodeID // sorted, unique, non-empty

	shardRing ring
	nodeRing  ring

	// Precomputed per-shard replica groups (index = shard id). The head of each
	// is the shard's primary. Precomputed so lookups allocate nothing on the hot
	// path and every query returns the same answer.
	replicas [][]NodeID
}

// NewRouter validates cfg and builds the rings. It returns a *ConfigError (see
// errors.go) for any invalid configuration and never repairs one. On success the
// Router is complete and immutable.
func NewRouter(cfg Config) (*Router, error) {
	nodes, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	r := &Router{
		shardCount: cfg.ShardCount,
		rf:         cfg.ReplicationFactor,
		nodes:      nodes,
	}

	// Shard ring: every shard contributes VNodesPerShard positions; owner = shard id.
	shardPoints := make([]ringPoint, 0, cfg.ShardCount*VNodesPerShard)
	for s := 0; s < cfg.ShardCount; s++ {
		for i := 0; i < VNodesPerShard; i++ {
			shardPoints = append(shardPoints, ringPoint{
				token: shardVNodeToken(ShardID(s), uint32(i)),
				owner: uint32(s),
				vnode: uint32(i),
			})
		}
	}
	r.shardRing = newRing(shardPoints)

	// Node ring: every node contributes VNodesPerNode positions; owner = index
	// into the sorted node slice.
	nodePoints := make([]ringPoint, 0, len(nodes)*VNodesPerNode)
	for ni, id := range nodes {
		for i := 0; i < VNodesPerNode; i++ {
			nodePoints = append(nodePoints, ringPoint{
				token: nodeVNodeToken(id, uint32(i)),
				owner: uint32(ni),
				vnode: uint32(i),
			})
		}
	}
	r.nodeRing = newRing(nodePoints)

	// Precompute each shard's ordered replica group.
	r.replicas = make([][]NodeID, cfg.ShardCount)
	seen := make([]bool, len(nodes))
	for s := 0; s < cfg.ShardCount; s++ {
		r.replicas[s] = r.computeReplicaGroup(anchorToken(ShardID(s)), seen)
	}
	return r, nil
}

// computeReplicaGroup walks the node ring clockwise from t and collects the first
// rf distinct node IDs. seen is a scratch buffer sized len(nodes), cleared here
// before use so the caller can reuse it across shards.
func (r *Router) computeReplicaGroup(t Token, seen []bool) []NodeID {
	for i := range seen {
		seen[i] = false
	}
	n := len(r.nodeRing.points)
	start := r.nodeRing.successorIndex(t)
	group := make([]NodeID, 0, r.rf)
	for k := 0; k < n && len(group) < r.rf; k++ {
		owner := r.nodeRing.points[(start+k)%n].owner
		if !seen[owner] {
			seen[owner] = true
			group = append(group, r.nodes[owner])
		}
	}
	return group
}

// Route maps a key to its shard. It is pure, total over []byte (empty, NUL and
// invalid-UTF-8 keys included), never errors, and never panics.
func (r *Router) Route(key []byte) ShardID {
	return ShardID(r.shardRing.owner(TokenOf(key)))
}

// Primary returns the node that heads shard s's replica group — the node that
// owns s. It panics only on an out-of-range shard, which is a programming error,
// not invalid input.
func (r *Router) Primary(s ShardID) NodeID {
	return r.replicas[s][0]
}

// Owner returns the node that owns key: the primary of key's shard. This is the
// mapping INV-C3 tracks across membership changes.
func (r *Router) Owner(key []byte) NodeID {
	return r.Primary(r.Route(key))
}

// ReplicaGroup returns a copy of shard s's ordered replica group (head =
// primary). It is a copy so a caller cannot mutate the Router's state.
func (r *Router) ReplicaGroup(s ShardID) []NodeID {
	src := r.replicas[s]
	out := make([]NodeID, len(src))
	copy(out, src)
	return out
}

// ShardInfo is the read-only metadata for one shard.
type ShardInfo struct {
	ID           ShardID
	Primary      NodeID
	ReplicaGroup []NodeID
}

// Shards returns metadata for every shard, in shard-id order.
func (r *Router) Shards() []ShardInfo {
	out := make([]ShardInfo, r.shardCount)
	for s := 0; s < r.shardCount; s++ {
		out[s] = ShardInfo{
			ID:           ShardID(s),
			Primary:      r.replicas[s][0],
			ReplicaGroup: r.ReplicaGroup(ShardID(s)),
		}
	}
	return out
}

// ShardCount returns the number of shards.
func (r *Router) ShardCount() int { return r.shardCount }

// ReplicationFactor returns the declarative replica-group size.
func (r *Router) ReplicationFactor() int { return r.rf }

// Nodes returns a copy of the sorted membership.
func (r *Router) Nodes() []NodeID {
	out := make([]NodeID, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// Config reconstructs the (validated, node-sorted) Config this Router was built
// from. Passing it back to NewRouter yields an identically-routing Router.
func (r *Router) Config() Config {
	return Config{
		ShardCount:        r.shardCount,
		ReplicationFactor: r.rf,
		Nodes:             r.Nodes(),
	}
}
