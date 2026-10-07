package kv_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/routing"
	"github.com/adivishall/quorum/internal/transport"
)

// multiCluster is three node processes (in-process hosts over real TCP), each
// hosting the groups of a four-shard routing, each with a kv.Front.
type multiCluster struct {
	assign *multiraft.Assignment
	ids    []multiraft.NodeID
	fronts map[multiraft.NodeID]*kv.Front
	hosts  map[multiraft.NodeID]*multiraft.Host
	stores map[multiraft.NodeID]map[replication.GroupID]*kv.Store
	regs   map[multiraft.NodeID]*metrics.Registry // metered clusters only
	mu     sync.Mutex
}

func startMultiCluster(t testing.TB, shards int) *multiCluster {
	t.Helper()
	return startMulti(t, shards, false)
}

// startMulti is startMultiCluster; metered, every node runs with a registry of
// its own wired through every layer, as dkvd -metrics-listen does.
func startMulti(t testing.TB, shards int, metered bool) *multiCluster {
	t.Helper()
	ids := []routing.NodeID{"n1", "n2", "n3"}
	a, err := multiraft.NewAssignment(routing.Config{ShardCount: shards, ReplicationFactor: 3, Nodes: ids})
	if err != nil {
		t.Fatal(err)
	}
	c := &multiCluster{assign: a, fronts: map[multiraft.NodeID]*kv.Front{}, hosts: map[multiraft.NodeID]*multiraft.Host{},
		stores: map[multiraft.NodeID]map[replication.GroupID]*kv.Store{}, regs: map[multiraft.NodeID]*metrics.Registry{}}
	trs := map[multiraft.NodeID]*transport.TCPTransport{}
	addrs := map[multiraft.NodeID]string{}
	for _, rid := range ids {
		id := multiraft.NodeID(rid)
		c.ids = append(c.ids, id)
		if metered {
			c.regs[id] = metrics.NewRegistry()
		}
		tr, err := transport.NewTCPTransport(transport.Config{NodeID: transport.NodeID(id), ListenAddr: "127.0.0.1:0", DialRetryInterval: 20 * time.Millisecond, Metrics: c.regs[id]})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { tr.Close() })
		trs[id], addrs[id] = tr, tr.LocalAddr().String()
	}
	for _, id := range c.ids {
		id := id
		front := kv.NewFront(string(id), a.GroupOf)
		km := kv.NewMetrics(c.regs[id])
		front.SetMetrics(km)
		c.fronts[id] = front
		c.stores[id] = map[replication.GroupID]*kv.Store{}
		static := map[multiraft.NodeID]string{}
		for o, addr := range addrs {
			if o != id {
				static[o] = addr
			}
		}
		h, err := multiraft.Start(context.Background(), multiraft.Config{
			ID: id, DataDir: t.TempDir(), Transport: trs[id], StaticPeers: static,
			NewStateMachine: func(g replication.GroupID) raftnode.StateMachine {
				s := kv.NewStore()
				km.Observe(s, g)
				c.mu.Lock()
				c.stores[id][g] = s
				c.mu.Unlock()
				return s
			},
			OnGroup: func(g replication.GroupID, node *raftnode.Node, sm raftnode.StateMachine) {
				if node == nil {
					front.Detach(g)
					return
				}
				front.Attach(g, node, sm.(kv.Machine))
			},
			TickInterval: 15 * time.Millisecond, DisableSync: true, Metrics: c.regs[id],
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { h.Close() })
		c.hosts[id] = h
		for _, g := range a.GenesisGroups(id) {
			conf, err := a.Genesis(g, addrs)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.Create(g, &conf); err != nil {
				t.Fatal(err)
			}
		}
	}
	return c
}

func (c *multiCluster) endpoints() []kv.Doer {
	var out []kv.Doer
	for _, id := range c.ids {
		out = append(out, c.fronts[id])
	}
	return out
}

func (c *multiCluster) store(id multiraft.NodeID, g replication.GroupID) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[id][g]
}

// TestShardedClientRoutesEachKeyToItsGroup: through the Front of any node, a
// sharded client's writes land in their key's group only — every other
// group's store never holds the key — each group registers its own session,
// and reads see the writes.
func TestShardedClientRoutesEachKeyToItsGroup(t *testing.T) {
	c := startMultiCluster(t, 4)
	ctx := context.Background()
	cl := kv.NewSharded(c.endpoints(), kv.SessionOptions{AttemptTimeout: 2 * time.Second, MaxAttempts: 20}, c.assign.GroupOf)
	keys := map[replication.GroupID][]string{}
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("key-%d", i)
		if out := cl.Put(ctx, []byte(k), []byte("v-"+k), nil); out.Err != nil {
			t.Fatalf("put %s: %v", k, out.Err)
		}
		keys[c.assign.GroupOf([]byte(k))] = append(keys[c.assign.GroupOf([]byte(k))], k)
	}
	if len(keys) < 2 {
		t.Fatalf("40 keys landed in %d group(s)", len(keys))
	}
	for g, ks := range keys {
		for _, k := range ks {
			out := cl.Get(ctx, []byte(k), nil)
			if out.Err != nil || !bytes.Equal(out.Response.Value, []byte("v-"+k)) {
				t.Fatalf("get %s: %+v", k, out)
			}
		}
		s, err := cl.Session(ctx, g)
		if err != nil || s.ID() == 0 {
			t.Fatalf("group %d session: %v", g, err)
		}
	}
	// Every replica of each group holds exactly its group's keys.
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range c.ids {
		for _, g := range c.assign.Groups() {
			st := c.store(id, g)
			for owner, ks := range keys {
				for _, k := range ks {
					for {
						_, ok := st.Get([]byte(k))
						if ok == (owner == g) {
							break
						}
						if time.Now().After(deadline) || owner != g {
							t.Fatalf("%s group %d: key %s of group %d present=%v", id, g, k, owner, ok)
						}
						time.Sleep(5 * time.Millisecond)
					}
				}
			}
		}
	}
}

// TestFrontRefusesAMisroutedRequest: a keyed request naming a group other than
// its key's is INVALID — never executed anywhere — and a request for a group
// the node does not host is NOT_LEADER with no hint.
func TestFrontRefusesAMisroutedRequest(t *testing.T) {
	c := startMultiCluster(t, 4)
	ctx := context.Background()
	f := c.fronts["n1"]
	key := []byte("some-key")
	g := c.assign.GroupOf(key)
	wrong := (g + 1) % 4
	resp, _ := f.Do(ctx, kv.Request{Op: kv.ReqPut, Group: wrong, Key: key, Value: []byte("x")})
	if resp.Status != kv.StatusInvalid || !strings.Contains(resp.Message, "belongs to group") {
		t.Fatalf("misrouted put: %+v", resp)
	}
	resp, _ = f.Do(ctx, kv.Request{Op: kv.ReqRegister, Group: 99})
	if resp.Status != kv.StatusNotLeader || resp.Leader != "" {
		t.Fatalf("register in an unhosted group: %+v", resp)
	}
	for _, id := range c.ids {
		for _, gg := range c.assign.Groups() {
			if _, ok := c.store(id, gg).Get(key); ok {
				t.Fatalf("the refused put executed in group %d on %s", gg, id)
			}
		}
	}
}
