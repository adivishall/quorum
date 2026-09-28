package multiraft

import (
	"fmt"
	"sort"

	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/routing"
)

// Assignment is the shard-to-group assignment of Phase 15 (docs/MULTI_RAFT.md
// §2), derived entirely from the Phase 6 routing — it adds no routing of its
// own. Each shard is one Raft group, and the identities coincide: GroupID =
// ShardID, permanently (a shard never moves to another group; Phase 15 moves
// replicas, not shards). A key's group is the group of Route(key)'s shard,
// through the consistent-hash shard ring (never a modulo), and a group's
// genesis voters are its shard's replica group from the node ring. After
// genesis a group's membership is its own replicated state: the routing
// configuration describes where groups STARTED, and a membership change does
// not rewrite it (a node added to a group is added by the group's log, not by
// the routing).
type Assignment struct {
	r *routing.Router
}

// NewAssignment builds the assignment from a routing configuration.
func NewAssignment(cfg routing.Config) (*Assignment, error) {
	r, err := routing.NewRouter(cfg)
	if err != nil {
		return nil, err
	}
	return &Assignment{r: r}, nil
}

// Router returns the routing the assignment derives from.
func (a *Assignment) Router() *routing.Router { return a.r }

// Groups returns every group, ascending: one per shard.
func (a *Assignment) Groups() []GroupID {
	out := make([]GroupID, a.r.ShardCount())
	for i := range out {
		out[i] = GroupID(i)
	}
	return out
}

// GroupOf returns the group key belongs to.
func (a *Assignment) GroupOf(key []byte) GroupID { return GroupID(a.r.Route(key)) }

// GenesisVoters returns group g's genesis voters: its shard's replica group,
// sorted.
func (a *Assignment) GenesisVoters(g GroupID) []NodeID {
	rg := a.r.ReplicaGroup(routing.ShardID(g))
	out := make([]NodeID, len(rg))
	for i, id := range rg {
		out[i] = NodeID(id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Genesis returns group g's genesis configuration with each voter's address
// from addrs; every voter must have one.
func (a *Assignment) Genesis(g GroupID, addrs map[NodeID]string) (replication.Configuration, error) {
	if int(g) >= a.r.ShardCount() {
		return replication.Configuration{}, fmt.Errorf("multiraft: group %d beyond the %d shards", g, a.r.ShardCount())
	}
	var voters []replication.Member
	for _, id := range a.GenesisVoters(g) {
		addr, ok := addrs[id]
		if !ok || addr == "" {
			return replication.Configuration{}, fmt.Errorf("multiraft: no address for %s, a genesis voter of group %d", id, g)
		}
		voters = append(voters, replication.Member{ID: id, Addr: addr})
	}
	return replication.NewConfiguration(voters, nil)
}

// GenesisGroups returns the groups whose genesis includes node, ascending.
func (a *Assignment) GenesisGroups(node NodeID) []GroupID {
	var out []GroupID
	for _, g := range a.Groups() {
		for _, id := range a.GenesisVoters(g) {
			if id == node {
				out = append(out, g)
				break
			}
		}
	}
	return out
}
