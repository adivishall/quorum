package kv

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
)

// Front serves the client protocol for every group a node hosts (Phase 15,
// docs/MULTI_RAFT.md §6). Each group has its own Server over its own node and
// store — its own leader, log, session table — and the Front only picks which:
//
//	key → shard → group (Route, the cluster's routing) → that group's Server
//	      → its leader (the Server forwards one hop within the group)
//
// A REGISTER names its group; a keyed request names one too, and must name the
// key's (INVALID otherwise), so a request never executes in a group other than
// the one its client — and its session — meant. A node that hosts no replica of
// the group answers NOT_LEADER with no leader hint: nothing was proposed, and
// the client tries another node. Nothing crosses groups: every request is for
// exactly one key, hence exactly one group, and there is no multi-key
// operation (docs/MULTI_RAFT.md §6).
type Front struct {
	id    string
	route func(key []byte) replication.GroupID

	mu        sync.RWMutex
	servers   map[replication.GroupID]*Server
	noForward bool
}

// NewFront returns a Front for node id with the given routing (nil: every key
// is group 0's — the single-group deployment).
func NewFront(id string, route func(key []byte) replication.GroupID) *Front {
	if route == nil {
		route = func([]byte) replication.GroupID { return 0 }
	}
	return &Front{id: id, route: route, servers: map[replication.GroupID]*Server{}}
}

// Attach serves group g through node (a member of g) and its store; it
// replaces any earlier Server of g. It returns the group's Server.
func (f *Front) Attach(g replication.GroupID, node *raftnode.Node, store *Store) *Server {
	srv := NewServer(f.id, node, store)
	f.mu.Lock()
	srv.SetForwarding(!f.noForward)
	f.servers[g] = srv
	f.mu.Unlock()
	return srv
}

// Detach stops serving group g (its node stopped): its requests are then
// answered as for a group this node does not host.
func (f *Front) Detach(g replication.GroupID) {
	f.mu.Lock()
	delete(f.servers, g)
	f.mu.Unlock()
}

// Server returns group g's Server, or nil.
func (f *Front) Server(g replication.GroupID) *Server {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.servers[g]
}

// Groups returns the groups the Front serves, ascending.
func (f *Front) Groups() []replication.GroupID {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]replication.GroupID, 0, len(f.servers))
	for g := range f.servers {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SetForwarding turns within-group forwarding on (the default) or off
// (redirect-only) for every group, current and future.
func (f *Front) SetForwarding(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noForward = !on
	for _, s := range f.servers {
		s.SetForwarding(on)
	}
}

// Name is the node id.
func (f *Front) Name() string { return f.id }

// Route returns the group a key belongs to.
func (f *Front) Route(key []byte) replication.GroupID { return f.route(key) }

// Do serves one client request (Doer).
func (f *Front) Do(ctx context.Context, req Request) (Response, error) {
	if err := req.validate(); err != nil {
		return Response{Status: StatusInvalid, Node: f.id, Message: err.Error()}, nil
	}
	if req.Op != ReqRegister {
		if g := f.route(req.Key); g != req.Group {
			return Response{Status: StatusInvalid, Node: f.id,
				Message: fmt.Sprintf("the key belongs to group %d, the request names group %d (the client's routing differs from the cluster's)", g, req.Group)}, nil
		}
	}
	srv := f.Server(req.Group)
	if srv == nil {
		return Response{Status: StatusNotLeader, Node: f.id,
			Message: fmt.Sprintf("%s hosts no replica of group %d", f.id, req.Group)}, nil
	}
	return srv.Do(ctx, req)
}
