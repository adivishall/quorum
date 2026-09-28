package raftsim

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"

	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/routing"
)

// The multi-group simulator (Phase 15, docs/MULTI_RAFT.md): G Raft groups over
// N physical nodes, each group an independent Cluster — its own cores, durable
// logs, snapshots, state machines, configuration and network queue — whose
// genesis voters are its shard's replica group from the Phase 6 routing (the
// same multiraft.Assignment the node host uses). Events are either a group's
// own (a message delivered or lost, a proposal, a membership change, a client
// request, a tick of that group's timer — each group's node runs its own
// actor and ticker in the real host) or a NODE's, fanned out to every group:
// a process crash or power loss, a restart, a pause, a partition. A crash
// point firing in one group kills the whole process, so it kills the node in
// every group; a persistence failure fail-stops only its group, as the
// cluster-mode host does. Everything is still a pure function of the
// configuration and the event stream: same seed, same trace.
//
// INV-M9 is checked by construction and by replay: a group's trace in a
// multi-group run is reproduced exactly by replaying, on a lone Cluster, the
// events that group received (TestMultiGroupIsolation) — its behaviour is a
// function of its own inputs alone, with nothing shared between groups.

// MultiConfig describes a multi-group simulation.
type MultiConfig struct {
	Nodes  int // physical nodes n1..nN
	Groups int // one per shard
	RF     int // the routing's replication factor: each group's genesis size
	Seed   int64
	// Per-group snapshot policy and session limits.
	SnapshotEvery, SnapshotRetain uint64
}

// MultiCluster is a deterministic simulation of Groups groups over Nodes nodes.
type MultiCluster struct {
	cfg    MultiConfig
	ids    []NodeID
	assign *multiraft.Assignment
	groups []*Cluster
	step   int
	trace  *Trace // node-level fan-out, interleaved with the groups' own traces by step
	viol   *Violation
	vGroup int
}

// GroupConfig is the configuration group g's Cluster runs with.
func (m *MultiCluster) GroupConfig(g int) Config { return m.groups[g].cfg }

// NewMulti boots every group on every node.
func NewMulti(cfg MultiConfig) (*MultiCluster, error) {
	if cfg.Nodes < 1 || cfg.Nodes > 9 || cfg.Groups < 1 || cfg.RF < 1 || cfg.RF > cfg.Nodes {
		return nil, fmt.Errorf("raftsim: multi config %+v", cfg)
	}
	m := &MultiCluster{cfg: cfg, trace: newTrace()}
	var rids []routing.NodeID
	for i := 1; i <= cfg.Nodes; i++ {
		id := NodeID(fmt.Sprintf("n%d", i))
		m.ids = append(m.ids, id)
		rids = append(rids, routing.NodeID(id))
	}
	a, err := multiraft.NewAssignment(routing.Config{ShardCount: cfg.Groups, ReplicationFactor: cfg.RF, Nodes: rids})
	if err != nil {
		return nil, err
	}
	m.assign = a
	m.trace.add(0, "multi nodes=%d groups=%d rf=%d seed=%d", cfg.Nodes, cfg.Groups, cfg.RF, cfg.Seed)
	for g := 0; g < cfg.Groups; g++ {
		gcfg := Config{Nodes: cfg.Nodes, Seed: cfg.Seed*1000 + int64(g), GenesisIDs: a.GenesisVoters(multiraft.GroupID(g)),
			SnapshotEvery: cfg.SnapshotEvery, SnapshotRetain: cfg.SnapshotRetain}
		c, err := New(gcfg)
		if err != nil {
			return nil, fmt.Errorf("raftsim: group %d: %w", g, err)
		}
		m.groups = append(m.groups, c)
		m.trace.add(0, "group %d genesis=%v", g, gcfg.GenesisIDs)
	}
	return m, nil
}

// Group returns group g's simulation.
func (m *MultiCluster) Group(g int) *Cluster { return m.groups[g] }

// Groups is the number of groups.
func (m *MultiCluster) Groups() int { return len(m.groups) }

// Violation returns the first violation in any group, with its group.
func (m *MultiCluster) Violation() (*Violation, int) { return m.viol, m.vGroup }

// MultiEvent is one event of a multi-group script: Group >= 0 is that group's
// own event; Group < 0 is a node-level event fanned out to every group.
type MultiEvent struct {
	Group int
	Event Event
}

func (e MultiEvent) String() string {
	if e.Group < 0 {
		return "* " + e.Event.String()
	}
	return fmt.Sprintf("g%d %s", e.Group, e.Event)
}

// nodeLevel are the kinds that are a node's, fanned out to every group.
var nodeLevel = map[Kind]bool{Crash: true, Restart: true, Pause: true, Resume: true,
	Isolate: true, Cut: true, Block: true, Unblock: true, HealAll: true, Split: true, Disarm: true, Release: true}

// Apply executes one event.
func (m *MultiCluster) Apply(e MultiEvent) {
	if m.viol != nil {
		return
	}
	m.step++
	if e.Group < 0 {
		m.trace.add(m.step, "node-level %s", e.Event)
		for _, c := range m.groups {
			if !m.appliesTo(c, e.Event) {
				continue
			}
			c.Apply(e.Event)
		}
	} else if e.Group < len(m.groups) {
		c := m.groups[e.Group]
		crashes := c.stats.PointCrashes
		wasUp := map[NodeID]bool{}
		for _, id := range c.ids {
			wasUp[id] = c.Up(id)
		}
		c.Apply(e.Event)
		if c.stats.PointCrashes > crashes {
			// A crash point fired: the process died, so the node dies in every
			// group.
			for _, id := range c.ids {
				if wasUp[id] && !c.Up(id) {
					m.fanOutCrash(e.Group, id)
				}
			}
		}
	}
	for g, c := range m.groups {
		if v := c.Violation(); v != nil && m.viol == nil {
			m.viol, m.vGroup = v, g
		}
	}
}

// appliesTo says whether a node-level event means anything to group c (a
// restart of a node that is up there is skipped, not an error).
func (m *MultiCluster) appliesTo(c *Cluster, e Event) bool {
	switch e.Kind {
	case Restart:
		return !c.Up(e.Node)
	case Crash:
		return c.Up(e.Node) || e.Power
	case Pause:
		return c.Up(e.Node) && !c.Paused(e.Node)
	case Resume:
		return c.Paused(e.Node)
	}
	return true
}

// fanOutCrash kills, in every other group, the node whose process just died
// at a crash point in group src.
func (m *MultiCluster) fanOutCrash(src int, victim NodeID) {
	m.trace.add(m.step, "process of %s died at a crash point in group %d: down in every group", victim, src)
	for g, o := range m.groups {
		if g != src && o.Up(victim) {
			o.Apply(Event{Kind: Crash, Node: victim})
		}
	}
}

// Stabilize heals every node-level fault and stabilizes every group.
func (m *MultiCluster) Stabilize(maxRounds int) {
	for _, c := range m.groups {
		if m.viol == nil {
			c.Stabilize(maxRounds)
		}
	}
	for g, c := range m.groups {
		if v := c.Violation(); v != nil && m.viol == nil {
			m.viol, m.vGroup = v, g
		}
	}
}

// TraceHash fingerprints the whole run: the fan-out trace and every group's.
func (m *MultiCluster) TraceHash() string {
	h := sha256.New()
	h.Write([]byte(m.trace.Hash()))
	for _, c := range m.groups {
		h.Write([]byte(c.trace.Hash()))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// MultiProfile is a multi-group chaos mix: node-level fault weights, and the
// Profile each group's own events are drawn from (its node-level weights are
// ignored: those are the MultiProfile's).
type MultiProfile struct {
	Name                          string
	Nodes, Groups, RF, Steps      int
	Crash, Restart, Partition     int
	Heal, Pause, Resume, CrashAt  int
	PowerLossPercent, MaxTorn     int
	Group                         Profile
	SnapshotEvery, SnapshotRetain uint64
}

// MultiProfiles are the built-in multi-group mixes.
var MultiProfiles = []MultiProfile{
	{Name: "multi-2x3", Nodes: 3, Groups: 2, RF: 3, Steps: 3000, Crash: 3, Restart: 20, Partition: 3, Heal: 10, Pause: 2, Resume: 15, CrashAt: 3,
		PowerLossPercent: 40, MaxTorn: 64,
		Group: Profile{Tick: 300, Deliver: 600, Propose: 60, Member: 6, Drop: 10, Duplicate: 10, Delay: 10, FIFOPercent: 70, MaxDelay: 30,
			KVPut: 30, KVGet: 30, KVDelete: 8, KVTimeout: 6, KVSessions: true, KVRegister: 20, KVRetry: 25, KVDup: 6, Clients: 3, Keys: 2}},
	{Name: "multi-4x5", Nodes: 5, Groups: 4, RF: 3, Steps: 4000, Crash: 3, Restart: 20, Partition: 3, Heal: 10, CrashAt: 3,
		PowerLossPercent: 40, MaxTorn: 64, SnapshotEvery: 10, SnapshotRetain: 2,
		Group: Profile{Tick: 300, Deliver: 600, Propose: 60, Member: 8, Drop: 10, Duplicate: 10, SnapshotNow: 2, FIFOPercent: 70,
			KVPut: 30, KVGet: 30, KVDelete: 8, KVTimeout: 6, KVSessions: true, KVRegister: 20, KVRetry: 25, KVDup: 6, Clients: 3, Keys: 2}},
	{Name: "multi-8x5", Nodes: 5, Groups: 8, RF: 3, Steps: 5000, Crash: 3, Restart: 20, Partition: 3, Heal: 10,
		PowerLossPercent: 40, MaxTorn: 64,
		Group: Profile{Tick: 300, Deliver: 600, Propose: 60, Member: 6, Drop: 10, Duplicate: 10, FIFOPercent: 70}},
}

// MultiProfileByName returns the named multi-group profile.
func MultiProfileByName(name string) (MultiProfile, bool) {
	for _, p := range MultiProfiles {
		if p.Name == name {
			return p, true
		}
	}
	return MultiProfile{}, false
}

// Config is the multi-group configuration a run of the profile uses.
func (p MultiProfile) Config(seed int64) MultiConfig {
	return MultiConfig{Nodes: p.Nodes, Groups: p.Groups, RF: p.RF, Seed: seed, SnapshotEvery: p.SnapshotEvery, SnapshotRetain: p.SnapshotRetain}
}

// MultiResult is the outcome of a multi-group run.
type MultiResult struct {
	Seed      int64
	Profile   string
	Script    []MultiEvent
	TraceHash string
	Violation *Violation
	Group     int // the group of the violation
	Multi     *MultiCluster
}

// RunMulti executes a seeded multi-group chaos run, then stabilization. It is a
// pure function of (profile, seed).
func RunMulti(p MultiProfile, seed int64) *MultiResult {
	m, err := NewMulti(p.Config(seed))
	if err != nil {
		return &MultiResult{Seed: seed, Profile: p.Name, Violation: &Violation{Invariant: "harness", Detail: err.Error()}}
	}
	rng := rand.New(rand.NewSource(seed))
	var script []MultiEvent
	gp := p.Group
	gp.Crash, gp.Restart, gp.Partition, gp.Heal, gp.Pause, gp.Resume, gp.Split, gp.CrashAt = 0, 0, 0, 0, 0, 0, 0, 0
	for i := 0; i < p.Steps && m.viol == nil; i++ {
		e := m.generate(rng, p, gp)
		script = append(script, e)
		m.Apply(e)
	}
	if m.viol == nil {
		m.Stabilize(400)
	}
	for _, c := range m.groups {
		if m.viol == nil && c.kv != nil && len(c.kv.clients) > 0 {
			c.SettleClients(gp.Keys, 200)
		}
	}
	return &MultiResult{Seed: seed, Profile: p.Name, Script: script, TraceHash: m.TraceHash(), Violation: m.viol, Group: m.vGroup, Multi: m}
}

// ReplayMulti executes an explicit multi-group script on fresh groups.
func ReplayMulti(cfg MultiConfig, script []MultiEvent) *MultiCluster {
	m, err := NewMulti(cfg)
	if err != nil {
		return nil
	}
	for _, e := range script {
		m.Apply(e)
	}
	return m
}

// generate draws the next event: a node-level fault, or one group's own event.
func (m *MultiCluster) generate(rng *rand.Rand, p MultiProfile, gp Profile) MultiEvent {
	pick := func() NodeID { return m.ids[rng.Intn(len(m.ids))] }
	var down, paused, up []NodeID
	for _, id := range m.ids {
		switch c := m.groups[0]; {
		case !c.Up(id):
			down = append(down, id)
		case c.Paused(id):
			paused = append(paused, id)
		default:
			up = append(up, id)
		}
	}
	type opt struct {
		w  int
		fn func() MultiEvent
	}
	var opts []opt
	add := func(w int, ok bool, fn func() MultiEvent) {
		if w > 0 && ok {
			opts = append(opts, opt{w, fn})
		}
	}
	node := func(e Event) MultiEvent { return MultiEvent{Group: -1, Event: e} }
	add(p.Crash, len(up) > 0, func() MultiEvent {
		e := Event{Kind: Crash, Node: up[rng.Intn(len(up))]}
		if rng.Intn(100) < p.PowerLossPercent {
			e.Power, e.N = true, rng.Intn(p.MaxTorn+1)
		}
		return node(e)
	})
	add(p.Restart, len(down) > 0, func() MultiEvent { return node(Event{Kind: Restart, Node: down[rng.Intn(len(down))]}) })
	add(p.Pause, len(up) > 0, func() MultiEvent { return node(Event{Kind: Pause, Node: up[rng.Intn(len(up))]}) })
	add(p.Resume, len(paused) > 0, func() MultiEvent { return node(Event{Kind: Resume, Node: paused[rng.Intn(len(paused))]}) })
	add(p.Partition, true, func() MultiEvent {
		if rng.Intn(2) == 0 {
			return node(Event{Kind: Isolate, Node: pick()})
		}
		a, b := pick(), pick()
		if a == b {
			return node(Event{Kind: Isolate, Node: a})
		}
		return node(Event{Kind: Cut, From: a, To: b})
	})
	add(p.Heal, true, func() MultiEvent { return node(Event{Kind: HealAll}) })
	points := crashPointNames(Profile{SnapshotEvery: p.SnapshotEvery})
	add(p.CrashAt, len(up) > 0, func() MultiEvent {
		e := Event{Kind: CrashAt, Node: up[rng.Intn(len(up))], Point: points[rng.Intn(len(points))], Nth: 1 + rng.Intn(3)}
		if rng.Intn(100) < p.PowerLossPercent {
			e.Power, e.N = true, rng.Intn(p.MaxTorn+1)
		}
		return MultiEvent{Group: rng.Intn(len(m.groups)), Event: e}
	})
	groupW := 1000 // a group's own event, most of the time
	add(groupW, true, func() MultiEvent {
		g := rng.Intn(len(m.groups))
		return MultiEvent{Group: g, Event: m.groups[g].generate(rng, gp)}
	})
	total := 0
	for _, o := range opts {
		total += o.w
	}
	x := rng.Intn(total)
	for _, o := range opts {
		if x < o.w {
			return o.fn()
		}
		x -= o.w
	}
	panic("unreachable")
}
