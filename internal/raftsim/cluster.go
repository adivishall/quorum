package raftsim

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
)

// NodeID is a Raft node id.
type NodeID = raft.NodeID

type (
	raftlogHardState = raftlog.HardState
	raftEntry        = raft.Entry
)

// logPath is where every simulated node keeps its durable log. Each node has its
// own disk, so one path suffices.
const logPath = "/raft/raft.log"

// Config describes a simulated cluster.
type Config struct {
	// Nodes is the group size, 1..9. Nodes are named n1..nN.
	Nodes int
	// Seed derives every node's election-timeout randomness (per incarnation). A
	// run is a pure function of Config and the event script.
	Seed int64
	// ElectionTicks and HeartbeatTicks default to the core's defaults.
	ElectionTicks  int
	HeartbeatTicks int
}

// Violation is a broken invariant, with the logical step at which it was seen.
type Violation struct {
	Invariant string
	Step      int
	Detail    string
}

func (v *Violation) Error() string {
	return fmt.Sprintf("%s violated at step %d: %s", v.Invariant, v.Step, v.Detail)
}

// Stats counts what happened in a run.
type Stats struct {
	Delivered, DroppedPartition, DroppedDown, DroppedInjected int
	Duplicated, Delayed                                       int
	ProcessCrashes, PowerLosses, Restarts, RestartFailures    int
	PersistFailures, Pauses                                   int
	ProposalsAccepted, ProposalsRejected, Skipped             int
	MaxTerm                                                   uint64
	LeaderElections                                           int
	MaxCommit                                                 uint64
	PointCrashes                                              int // crashes fired at an armed crash point (Phase 11)
}

// flight is a message in the network.
type flight struct {
	seq       uint64
	msg       raft.Message
	holdUntil int
}

// node is one simulated server. disk, inj and shadow outlive incarnations; core
// and mem exist only while it is up.
type node struct {
	id     NodeID
	idx    int
	disk   *fault.MemFS
	inj    *fault.InjectFS
	shadow *shadowStore

	up     bool
	paused bool
	inc    int // incarnation, 1 on first boot
	ticks  int // logical clock: ticks received, across incarnations
	core   *raft.Raft
	mem    *replication.MemoryLog

	entries     []raft.Entry // the in-memory log, refreshed after every event touching the node
	applied     uint64       // highest index applied in this incarnation
	prevApplied uint64       // applied index when the last incarnation died
	verified    uint64       // committed prefix already checked against the global record

	// last observed, for transition tracing and step-local checks
	role   raft.Role
	term   uint64
	commit uint64
	lv     *leaderView

	// Phase 11 crash points (docs/CRASH_RECOVERY.md).
	armed         *armedCrash        // a crash waiting to fire at a driver point
	fired         *firedCrash        // set by the hook when a crash fired, consumed by drain/apply
	hits          map[string]int     // occurrences of each driver point since boot (enumeration)
	hist          []applied          // every application by every incarnation, in order
	lastRecovered *raftlog.Recovered // what the latest boot recovered

	// Phase 12 client tier (kv.go): this incarnation's state machine and the
	// driver's request tables. Nil while the node is down.
	store   *kv.Store
	waiters *raftnode.Waiters
	reads   *raftnode.Reads
}

// armedCrash is a crash armed at a driver point: it fires the nth time the
// point is reached, counting from arming.
type armedCrash struct {
	point string
	nth   int
	seen  int
	power bool
	torn  int
}

// firedCrash records a crash that fired at a crash point.
type firedCrash struct {
	point string
	nth   int
	power bool
	torn  int
	step  int
}

// applied is one state-machine application by one incarnation of a node.
type applied struct {
	inc int
	e   raft.Entry
}

// PointHit is one occurrence of a crash point on a node: the nth time the node
// reached the point since it booted — the address a CrashAt event uses.
type PointHit struct {
	Node  NodeID
	Point string
	Nth   int
}

// errCrashPoint is the hook's abort: the node's process died at a crash point.
var errCrashPoint = errors.New("raftsim: crashed at a crash point")

// leaderView is what a node looked like as leader of one term in one incarnation:
// its log (for Leader Append-Only) and how much of the committed record it has
// been checked against (for Leader Completeness).
type leaderView struct {
	inc       int
	term      uint64
	entries   []raft.Entry
	r4Through int
}

// Cluster is a deterministic simulated Raft group. Every node is a real
// internal/raft core over a real internal/raftlog durable log on its own crash-
// modeling disk (fault.MemFS + fault.InjectFS), driven through the real driver
// ordering (raftnode.DrainReady) and restarted through the real startup path
// (raftnode.Recover). The network is an explicit in-memory queue. Nothing runs
// concurrently, nothing reads a clock, and every random choice comes from the
// config seed or the caller's schedule — so a run is exactly reproducible.
//
// Apply executes one Event; after every event the continuous invariant checks run
// (check.go). The first violation stops the cluster; later events are ignored.
type Cluster struct {
	cfg     Config
	ids     []NodeID
	nodes   map[NodeID]*node
	links   fault.Links
	flights []*flight
	seq     uint64
	step    int

	trace  *Trace
	script []Event
	viol   *Violation
	stats  Stats
	chk    checker

	// OnViolation, if set, is called once with the first violation (e.g. to fail
	// a test immediately with a full report).
	OnViolation func(*Violation)

	// Crash-point recording (StartRecordingPoints / Points).
	recordPoints bool
	points       []PointHit
	opBase       map[NodeID]int // op-log length per node when recording started

	kv *kvState // Phase 12 clients and their history (kv.go)
}

// New boots a cluster: every node starts from an empty disk through the real
// startup path.
func New(cfg Config) (*Cluster, error) {
	if cfg.Nodes < 1 || cfg.Nodes > 9 {
		return nil, fmt.Errorf("raftsim: Nodes = %d, want 1..9", cfg.Nodes)
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = raft.DefaultElectionTicks
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = raft.DefaultHeartbeatTicks
	}
	c := &Cluster{cfg: cfg, nodes: map[NodeID]*node{}, trace: newTrace()}
	c.chk.init()
	c.kvInit()
	for i := 1; i <= cfg.Nodes; i++ {
		id := NodeID(fmt.Sprintf("n%d", i))
		c.ids = append(c.ids, id)
		mem := fault.NewMemFS()
		c.nodes[id] = &node{id: id, idx: i, disk: mem, inj: fault.NewInjectFS(mem), shadow: &shadowStore{}, hits: map[string]int{}}
	}
	c.trace.add(0, "boot nodes=%d seed=%d election=%d heartbeat=%d", cfg.Nodes, cfg.Seed, cfg.ElectionTicks, cfg.HeartbeatTicks)
	for _, id := range c.ids {
		if err := c.boot(c.nodes[id]); err != nil {
			return nil, fmt.Errorf("raftsim: boot %s: %w", id, err)
		}
	}
	return c, nil
}

// IDs returns the node ids in order.
func (c *Cluster) IDs() []NodeID { return append([]NodeID(nil), c.ids...) }

// Step is the number of events applied so far — the cluster's logical time.
func (c *Cluster) Step() int { return c.step }

// Trace returns the run's trace.
func (c *Cluster) Trace() *Trace { return c.trace }

// Script returns every event applied so far — a complete replay script.
func (c *Cluster) Script() []Event { return append([]Event(nil), c.script...) }

// Violation returns the first invariant violation, or nil.
func (c *Cluster) Violation() *Violation { return c.viol }

// Stats returns the run's counters.
func (c *Cluster) Stats() Stats { return c.stats }

// Up reports whether a node is running.
func (c *Cluster) Up(id NodeID) bool { return c.nodes[id] != nil && c.nodes[id].up }

// Paused reports whether a node is paused.
func (c *Cluster) Paused(id NodeID) bool { return c.nodes[id] != nil && c.nodes[id].paused }

// NodeState is a read-only view of one node, for scenario assertions.
type NodeState struct {
	Up        bool
	Paused    bool
	Role      raft.Role
	Term      uint64
	Vote      NodeID
	Leader    NodeID
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	Log       []raft.Entry // copies
}

// State returns a node's current state (zero Role/Term etc. if it is down).
func (c *Cluster) State(id NodeID) NodeState {
	n := c.nodes[id]
	if n == nil {
		return NodeState{}
	}
	s := NodeState{Up: n.up, Paused: n.paused}
	if !n.up {
		return s
	}
	s.Role, s.Term, s.Vote, s.Leader = n.core.Role(), n.core.Term(), n.core.VotedFor(), n.core.LeaderID()
	s.Commit, s.Applied, s.LastIndex = n.core.CommitIndex(), n.core.AppliedIndex(), n.core.LastIndex()
	s.Log, _ = n.mem.Slice(1, n.mem.LastIndex()+1)
	return s
}

// InFlight returns copies of the messages currently in the network, oldest first.
func (c *Cluster) InFlight() []raft.Message {
	out := make([]raft.Message, 0, len(c.flights))
	for _, f := range c.flights {
		out = append(out, f.msg)
	}
	return out
}

// Leaders returns the up nodes that currently believe they lead, in id order.
func (c *Cluster) Leaders() []NodeID {
	var out []NodeID
	for _, id := range c.ids {
		if n := c.nodes[id]; n.up && n.core.Role() == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// nodeSeed derives a node incarnation's election-timeout seed from the config
// seed (splitmix64), so every incarnation's randomness is fixed by the seed.
func (c *Cluster) nodeSeed(n *node) int64 {
	x := uint64(c.cfg.Seed)*0x9E3779B97F4A7C15 + uint64(n.idx)*0xBF58476D1CE4E5B9 + uint64(n.inc)*0x94D049BB133111EB
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return int64(x >> 1)
}

// boot starts a node from its disk through raftnode.Recover — the real startup
// path — and checks what it recovered (INV-F2, INV-R8).
func (c *Cluster) boot(n *node) error {
	n.inc++
	rc, err := raftnode.Recover(raftnode.Config{
		ID: n.id, Peers: c.ids, LogPath: logPath, FS: n.inj,
		Rand:          rand.New(rand.NewSource(c.nodeSeed(n))),
		ElectionTicks: c.cfg.ElectionTicks, HeartbeatTicks: c.cfg.HeartbeatTicks,
	})
	if err != nil {
		n.inc--
		return err
	}
	c.checkRecovered(n, rc.State)
	n.lastRecovered = &raftlog.Recovered{Entries: append([]raft.Entry(nil), rc.State.Entries...), HardState: rc.State.HardState}
	n.shadow.rebase(rc.Log, rc.State)
	n.core, n.mem, n.up, n.paused = rc.Core, rc.Mem, true, false
	n.applied, n.verified, n.lv = 0, 0, nil
	n.kvNodeUp()
	n.role, n.term, n.commit = n.core.Role(), n.core.Term(), n.core.CommitIndex()
	n.refresh()
	c.trace.add(c.step, "up %s inc=%d term=%d vote=%q last=%d commit=%d", n.id, n.inc, n.term, n.core.VotedFor(), n.core.LastIndex(), n.commit)
	return nil
}

// kill stops a node's process: its handles die, its volatile state is gone, and
// its disk keeps whatever the kernel had (a process crash). A later power loss is
// a separate step on the disk.
func (c *Cluster) kill(n *node, why string) {
	n.disk.CrashProcess()
	n.prevApplied = n.applied
	n.core, n.mem, n.up, n.paused, n.lv = nil, nil, false, false, nil
	n.kvNodeDown()
	n.shadow.detach()
	c.trace.add(c.step, "down %s (%s)", n.id, why)
}

func (n *node) refresh() {
	if n.up {
		n.entries, _ = n.mem.Slice(1, n.mem.LastIndex()+1)
	} else {
		n.entries = nil
	}
}

// Apply executes one event. An event that does not apply to the current state
// (a message that is not in flight, a tick on a down node, ...) is skipped and
// traced, never an error — so a minimized script stays replayable.
func (c *Cluster) Apply(e Event) {
	if c.viol != nil {
		return
	}
	c.step++
	c.script = append(c.script, e)
	defer c.kvPoll() // complete every client whose request now has an outcome
	var touched *node
	switch e.Kind {
	case Tick:
		n := c.runnable(e.Node)
		if n == nil {
			c.skip(e)
			return
		}
		n.ticks++
		c.trace.add(c.step, "tick %s clock=%d", n.id, n.ticks)
		n.core.Tick()
		c.drain(n)
		touched = n
	case Deliver:
		f := c.find(e.From, e.To, e.Pos)
		if f == nil || f.holdUntil > c.step || c.Paused(e.To) {
			c.skip(e)
			return
		}
		c.remove(f)
		touched = c.deliver(f)
	case Drop:
		f := c.find(e.From, e.To, e.Pos)
		if f == nil {
			c.skip(e)
			return
		}
		c.remove(f)
		c.stats.DroppedInjected++
		c.trace.add(c.step, "drop #%d %s>%s %s (injected)", f.seq, f.msg.From, f.msg.To, describe(f.msg))
	case Duplicate:
		f := c.find(e.From, e.To, e.Pos)
		if f == nil {
			c.skip(e)
			return
		}
		c.seq++
		c.flights = append(c.flights, &flight{seq: c.seq, msg: f.msg})
		c.stats.Duplicated++
		c.trace.add(c.step, "dup #%d -> #%d %s>%s %s", f.seq, c.seq, f.msg.From, f.msg.To, describe(f.msg))
	case Delay:
		f := c.find(e.From, e.To, e.Pos)
		if f == nil {
			c.skip(e)
			return
		}
		f.holdUntil = c.step + e.N
		c.stats.Delayed++
		c.trace.add(c.step, "delay #%d %s>%s until s=%d", f.seq, f.msg.From, f.msg.To, f.holdUntil)
	case Block, Unblock, Cut:
		if c.nodes[e.From] == nil || c.nodes[e.To] == nil || e.From == e.To {
			c.skip(e)
			return
		}
		switch e.Kind {
		case Block:
			c.links.Block(string(e.From), string(e.To))
		case Unblock:
			c.links.Unblock(string(e.From), string(e.To))
		case Cut:
			c.links.Cut(string(e.From), string(e.To))
		}
		c.trace.add(c.step, "%s %s %s", e.Kind, e.From, e.To)
	case Isolate:
		if c.nodes[e.Node] == nil {
			c.skip(e)
			return
		}
		members := make([]string, len(c.ids))
		for i, id := range c.ids {
			members[i] = string(id)
		}
		c.links.Isolate(string(e.Node), members)
		c.trace.add(c.step, "isolate %s", e.Node)
	case HealAll:
		c.links.HealAll()
		c.trace.add(c.step, "healall")
	case Crash:
		n := c.nodes[e.Node]
		if n == nil || (!n.up && !e.Power) {
			c.skip(e)
			return
		}
		if n.up {
			c.stats.ProcessCrashes++
			c.kill(n, "crash")
		}
		if e.Power {
			c.stats.PowerLosses++
			n.disk.CrashPowerLoss(e.N)
			c.trace.add(c.step, "powerloss %s torn<=%d", n.id, e.N)
		}
	case Restart:
		n := c.nodes[e.Node]
		if n == nil || n.up {
			c.skip(e)
			return
		}
		if err := c.boot(n); err != nil {
			c.stats.RestartFailures++
			c.trace.add(c.step, "restart-failed %s err=%v", n.id, err)
			if n.fired != nil {
				// The crash point was an I/O boundary of recovery itself (the
				// torn-tail truncate, the fsync of the recovered state): the
				// process died during Open, before it was up.
				f := n.fired
				n.fired = nil
				c.stats.PointCrashes++
				c.trace.add(c.step, "crashpoint %s %s #%d (during recovery)", n.id, f.point, f.nth)
				if f.power {
					c.stats.PowerLosses++
					n.disk.CrashPowerLoss(f.torn)
					c.trace.add(c.step, "powerloss %s torn<=%d", n.id, f.torn)
				}
			}
			return
		}
		c.stats.Restarts++
		touched = n
	case Pause, Resume:
		n := c.nodes[e.Node]
		if n == nil || !n.up || n.paused == (e.Kind == Pause) {
			c.skip(e)
			return
		}
		n.paused = e.Kind == Pause
		if n.paused {
			c.stats.Pauses++
		}
		c.trace.add(c.step, "%s %s", e.Kind, n.id)
	case FailPersist:
		n := c.nodes[e.Node]
		if n == nil {
			c.skip(e)
			return
		}
		inj := fault.Injection{Path: logPath}
		switch e.Op {
		case FailWrite:
			inj.Op = fault.OpWrite
		case ShortWrite:
			inj.Op, inj.Short = fault.OpWrite, e.N
		case FailSync:
			inj.Op = fault.OpSync
		default:
			c.skip(e)
			return
		}
		n.inj.Arm(inj)
		c.trace.add(c.step, "arm %s %s", n.id, e)
	case Disarm:
		for _, id := range c.ids {
			c.nodes[id].inj.Disarm()
			c.nodes[id].armed = nil
		}
		c.trace.add(c.step, "disarm")
	case CrashAt:
		n := c.nodes[e.Node]
		if n == nil || e.Nth < 1 || (n.armed != nil && !IsIOPoint(e.Point)) {
			c.skip(e)
			return
		}
		if _, ok := raftnode.ParsePoint(e.Point); !ok && !IsIOPoint(e.Point) {
			c.skip(e)
			return
		}
		c.arm(n, e)
		c.trace.add(c.step, "arm %s %s", n.id, e)
	case Release:
		for _, f := range c.flights {
			f.holdUntil = 0
		}
		c.trace.add(c.step, "release")
	case Propose:
		n := c.runnable(e.Node)
		if n == nil {
			c.skip(e)
			return
		}
		if err := n.core.Propose([]byte(e.Data)); err != nil {
			c.stats.ProposalsRejected++
			c.trace.add(c.step, "propose %s %q rejected (%v)", n.id, e.Data, err)
			return
		}
		c.stats.ProposalsAccepted++
		c.chk.proposed(e.Data, c.step)
		c.trace.add(c.step, "propose %s %q accepted idx=%d", n.id, e.Data, n.core.LastIndex())
		c.drain(n)
		touched = n
	case CheckConverged:
		c.checkConverged()
		return
	case KVPut, KVGet, KVDelete:
		touched = c.kvStart(e)
	case KVTimeout:
		c.kvTimeout(e)
	default:
		c.skip(e)
		return
	}
	c.afterEvent(touched)
}

func (c *Cluster) skip(e Event) {
	c.stats.Skipped++
	c.trace.add(c.step, "skip %s", e)
}

// runnable returns the node if it is up and not paused.
func (c *Cluster) runnable(id NodeID) *node {
	n := c.nodes[id]
	if n == nil || !n.up || n.paused {
		return nil
	}
	return n
}

// find returns the pos-th in-flight message on the link from -> to (oldest first).
func (c *Cluster) find(from, to NodeID, pos int) *flight {
	k := 0
	for _, f := range c.flights {
		if f.msg.From == from && f.msg.To == to {
			if k == pos {
				return f
			}
			k++
		}
	}
	return nil
}

func (c *Cluster) remove(f *flight) {
	for i, g := range c.flights {
		if g == f {
			c.flights = append(c.flights[:i], c.flights[i+1:]...)
			return
		}
	}
}

// deliver hands a message to its recipient, unless its link is partitioned or the
// recipient is down (the message is then lost). It returns the node touched.
func (c *Cluster) deliver(f *flight) *node {
	m := f.msg
	if c.links.Blocked(string(m.From), string(m.To)) {
		c.stats.DroppedPartition++
		c.trace.add(c.step, "drop #%d %s>%s %s (partition)", f.seq, m.From, m.To, describe(m))
		return nil
	}
	n := c.nodes[m.To]
	if !n.up {
		c.stats.DroppedDown++
		c.trace.add(c.step, "drop #%d %s>%s %s (recipient down)", f.seq, m.From, m.To, describe(m))
		return nil
	}
	c.stats.Delivered++
	c.trace.add(c.step, "deliver #%d %s>%s %s", f.seq, m.From, m.To, describe(m))
	stale := m.Term < n.core.Term()
	var before r10Snap
	if stale {
		before = snapR10(n)
	}
	if err := n.core.Step(m); err != nil {
		c.violate("harness", "Step(%s <- %s %s): %v", n.id, m.From, m.Type, err)
		return n
	}
	c.drain(n)
	if stale && n.up {
		c.checkR10(n, before, m)
	}
	return n
}

// drain performs the node's pending Ready effects through the REAL driver ordering
// (raftnode.DrainReady): persist, then send, then advance. A persistence failure
// fail-stops the node exactly as the real driver does, then it applies committed
// entries.
func (c *Cluster) drain(n *node) {
	confirm := func(rs raft.ReadState) { n.reads.Confirmed(rs, n.waiters, n.core.AppliedIndex()) }
	err := raftnode.DrainReadyAt(n.core, n.shadow, func(m raft.Message) { c.send(n, m) }, confirm, c.hook(n))
	if err != nil {
		if n.fired != nil {
			c.crashFired(n)
			return
		}
		c.stats.PersistFailures++
		c.trace.add(c.step, "persist-failed %s err=%v", n.id, err)
		c.kill(n, "fail-stop after persistence failure")
		return
	}
	c.apply(n)
	if n.up {
		// As the real driver does after every cycle: a read registered in a
		// term this node no longer leads will never be confirmed.
		n.reads.DropStale(n.core.Term(), n.core.Role() == raft.Leader)
	}
}

// send is DrainReady's network hand-off: it runs the send-time checks and puts the
// message in flight.
func (c *Cluster) send(n *node, m raft.Message) {
	c.checkSend(n, m)
	c.seq++
	c.flights = append(c.flights, &flight{seq: c.seq, msg: m})
	c.trace.add(c.step, "send #%d %s>%s %s", c.seq, m.From, m.To, describe(m))
}

// apply feeds committed entries to the node's recording state machine through
// the driver's own apply path (raftnode.ApplyCommitted), with the node's crash
// points in effect.
func (c *Cluster) apply(n *node) {
	sm := &simSM{c: c, n: n}
	err := raftnode.ApplyCommitted(n.core, sm, c.hook(n), func(e raft.Entry) { n.waiters.Applied(e.Index, e.Term) })
	if sm.last > 0 {
		c.trace.add(c.step, "apply %s %d..%d", n.id, sm.first, sm.last)
	}
	if err == nil {
		return
	}
	if n.fired != nil {
		c.crashFired(n)
		return
	}
	c.violate("INV-R7", "%s apply: %v", n.id, err)
}

// simSM is a node's state machine in the simulator: it runs the apply-time checks
// and records every application, across incarnations, so re-application after a
// restart is visible (docs/CRASH_RECOVERY.md, INV-CR4).
type simSM struct {
	c           *Cluster
	n           *node
	first, last uint64
}

func (s *simSM) Apply(index uint64, _ []byte) error {
	e, err := s.n.mem.At(index)
	if err != nil {
		return err
	}
	s.c.checkApply(s.n, e)
	if err := s.n.applyToStore(index, e.Data); err != nil {
		return err
	}
	s.n.applied = index
	s.n.hist = append(s.n.hist, applied{inc: s.n.inc, e: e})
	if s.first == 0 {
		s.first = index
	}
	s.last = index
	return nil
}

// --- crash points (Phase 11) ---

// hook returns the node's crash-point hook: it counts every point reached (for
// enumeration) and fires an armed crash on its nth occurrence.
func (c *Cluster) hook(n *node) raftnode.Hook {
	return func(p raftnode.Point, arg uint64) error {
		name := p.String()
		n.hits[name]++
		if c.recordPoints {
			c.points = append(c.points, PointHit{Node: n.id, Point: name, Nth: n.hits[name]})
		}
		if a := n.armed; a != nil && a.point == name {
			a.seen++
			if a.seen >= a.nth {
				n.armed = nil
				return c.fire(n, a)
			}
		}
		return nil
	}
}

// arm arms a CrashAt event: a driver point on the node, or an I/O boundary as an
// observation point at the vfs seam (fault.Injection.At) that kills the process
// when reached.
func (c *Cluster) arm(n *node, e Event) {
	a := &armedCrash{point: e.Point, nth: e.Nth, power: e.Power, torn: e.N}
	if !IsIOPoint(e.Point) {
		n.armed = a
		return
	}
	var op fault.Op
	switch e.Point {
	case "write":
		op = fault.OpWrite
	case "fsync":
		op = fault.OpSync
	case "truncate":
		op = fault.OpTruncate
	}
	n.inj.Arm(fault.Injection{Op: op, Path: logPath, Nth: e.Nth, At: func() { _ = c.fire(n, a) }})
}

// fire is the moment of death at a crash point: the disk's process is gone (its
// handles die; the kernel keeps every accepted byte), and the abort propagates
// out of the driver step so nothing after the point happens. The bookkeeping —
// power loss, statistics, trace — is done by crashFired once the step unwinds.
func (c *Cluster) fire(n *node, a *armedCrash) error {
	n.fired = &firedCrash{point: a.point, nth: a.nth, power: a.power, torn: a.torn, step: c.step}
	n.disk.CrashProcess()
	return errCrashPoint
}

// crashFired completes a crash that fired inside a driver step.
func (c *Cluster) crashFired(n *node) {
	f := n.fired
	n.fired = nil
	c.stats.PointCrashes++
	c.stats.ProcessCrashes++
	c.trace.add(c.step, "crashpoint %s %s #%d", n.id, f.point, f.nth)
	c.kill(n, "crash at "+f.point)
	if f.power {
		c.stats.PowerLosses++
		n.disk.CrashPowerLoss(f.torn)
		c.trace.add(c.step, "powerloss %s torn<=%d", n.id, f.torn)
	}
}

// StartRecordingPoints makes the cluster record every crash point each node
// reaches from now on (Points), so a crash matrix can enumerate them. Call it
// right after New, before any event: a CrashAt armed as the first event of a
// fresh cluster then counts occurrences from exactly the same origin.
func (c *Cluster) StartRecordingPoints() {
	c.recordPoints = true
	c.opBase = map[NodeID]int{}
	for _, id := range c.ids {
		c.nodes[id].hits = map[string]int{}
		c.opBase[id] = len(c.nodes[id].inj.Ops())
	}
}

// Points returns every crash point the nodes reached since StartRecordingPoints:
// the driver points in the order they occurred, then, per node, the I/O
// boundaries counted from the durable log's op log. Each is the address a
// CrashAt event armed at recording time would use to crash there.
func (c *Cluster) Points() []PointHit {
	out := append([]PointHit(nil), c.points...)
	for _, id := range c.ids {
		counts := map[string]int{}
		ops := c.nodes[id].inj.Ops()
		if c.opBase != nil {
			ops = ops[c.opBase[id]:]
		}
		for _, op := range ops {
			var name string
			switch op.Op {
			case fault.OpWrite:
				name = "write"
			case fault.OpSync:
				name = "fsync"
			case fault.OpTruncate:
				name = "truncate"
			default:
				continue
			}
			counts[name]++
			out = append(out, PointHit{Node: id, Point: name, Nth: counts[name]})
		}
	}
	return out
}

// Recovered returns what a node's latest boot recovered (nil if never booted).
func (c *Cluster) Recovered(id NodeID) *raftlog.Recovered { return c.nodes[id].lastRecovered }

// Applications returns every application a node's state machine ever saw, across
// all its incarnations, in order: (incarnation, entry).
func (c *Cluster) Applications(id NodeID) []Application {
	var out []Application
	for _, a := range c.nodes[id].hist {
		out = append(out, Application{Incarnation: a.inc, Entry: a.e})
	}
	return out
}

// Application is one state-machine application by one incarnation of a node.
type Application struct {
	Incarnation int
	Entry       raft.Entry
}

// describe renders a message's type-specific fields for the trace.
func describe(m raft.Message) string {
	switch m.Type {
	case raft.MsgVoteRequest:
		return fmt.Sprintf("Vote t=%d last=%d/%d", m.Term, m.LastLogIndex, m.LastLogTerm)
	case raft.MsgVoteResponse:
		return fmt.Sprintf("VoteResp t=%d granted=%v", m.Term, m.VoteGranted)
	case raft.MsgAppendRequest:
		return fmt.Sprintf("App t=%d prev=%d/%d n=%d commit=%d", m.Term, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.LeaderCommit)
	case raft.MsgAppendResponse:
		return fmt.Sprintf("AppResp t=%d ok=%v match=%d conflict=%d/%d", m.Term, m.Success, m.MatchIndex, m.ConflictIndex, m.ConflictTerm)
	default:
		return m.Type.String()
	}
}

// --- the durable-state shadow (INV-R6, INV-F2) ---

// durableState is a logical image of a node's durable log: its HardState and
// entries, reconstructed from the Save calls the driver made — independent of the
// log's byte encoding, so comparing it with what raftlog recovers checks the
// encoding, the crash policy and the recovery path end to end.
type durableState struct {
	hs      raftlog.HardState
	entries []raft.Entry
}

func (d durableState) clone() durableState {
	return durableState{hs: d.hs, entries: append([]raft.Entry(nil), d.entries...)}
}

// put applies one Entry record: set its index and drop everything above (the
// append-only truncation rule, ADR-016). A durableState's slice is never shared
// (clone copies), so truncating in place is safe.
func (d *durableState) put(e raft.Entry) {
	if e.Index == 0 || e.Index > uint64(len(d.entries))+1 {
		d.entries = append(d.entries, e) // impossible for a correct driver; the mismatch surfaces in checks
		return
	}
	d.entries = append(d.entries[:e.Index-1], e)
}

// shadowStore sits between DrainReady and the node's raftlog.Log (it is the
// raftnode.Storage the driver saves through). It forwards every Save unchanged and
// records what the node was told is durable (persisted) and, when a Save fails,
// what it was attempting (pending).
type shadowStore struct {
	log       *raftlog.Log
	persisted durableState
	pending   *pendingSave
}

type pendingSave struct {
	hs      *raftlog.HardState
	entries []raft.Entry
}

var _ raftnode.Storage = (*shadowStore)(nil)

// Save forwards to the real log and, only if it succeeds, records the new durable
// state.
func (s *shadowStore) Save(hs *raftlog.HardState, entries []raftlog.Entry) error {
	var hsCopy *raftlog.HardState
	if hs != nil {
		v := *hs
		hsCopy = &v
	}
	s.pending = &pendingSave{hs: hsCopy, entries: entries}
	if s.log == nil {
		return errors.New("raftsim: save on a node with no open log")
	}
	if err := s.log.Save(hs, entries); err != nil {
		return err
	}
	for _, e := range entries {
		s.persisted.put(e)
	}
	if hsCopy != nil {
		s.persisted.hs = *hsCopy
	}
	s.pending = nil
	return nil
}

func (s *shadowStore) detach() { s.log = nil }

// rebase adopts the state a restart recovered as the new durable baseline.
func (s *shadowStore) rebase(l *raftlog.Log, rec *raftlog.Recovered) {
	s.log = l
	s.persisted = durableState{hs: rec.HardState, entries: append([]raft.Entry(nil), rec.Entries...)}
	s.pending = nil
}

// candidates are the states a recovery may legitimately produce: the persisted
// state, extended by any prefix of the records an interrupted Save was writing —
// in the order the log writes them, which raftlog.SavePlan defines (a changed
// term/vote first, then the entries, then the new commit) — INV-F2.
func (s *shadowStore) candidates() []durableState {
	out := []durableState{s.persisted.clone()}
	if s.pending == nil {
		return out
	}
	cur := s.persisted.clone()
	lead, trail := raftlog.SavePlan(s.persisted.hs, s.pending.hs, s.pending.entries)
	if lead != nil {
		cur.hs = *lead
		out = append(out, cur.clone())
	}
	for _, e := range s.pending.entries {
		cur.put(e)
		out = append(out, cur.clone())
	}
	if trail != nil {
		cur.hs = *trail
		out = append(out, cur.clone())
	}
	return out
}

// matches reports whether a recovered state is one of the candidates.
func (s *shadowStore) matches(rec *raftlog.Recovered) bool {
	for _, cand := range s.candidates() {
		if sameDurable(cand, rec) {
			return true
		}
	}
	return false
}

func sameDurable(d durableState, rec *raftlog.Recovered) bool {
	if len(d.entries) != len(rec.Entries) {
		return false
	}
	for i := range d.entries {
		if !sameEntry(d.entries[i], rec.Entries[i]) {
			return false
		}
	}
	commit := d.hs.Commit
	if commit > uint64(len(d.entries)) {
		commit = uint64(len(d.entries))
	}
	return d.hs.Term == rec.HardState.Term && d.hs.Vote == rec.HardState.Vote && commit == rec.HardState.Commit
}

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Data, b.Data)
}

func (s *shadowStore) describe() string {
	p := "none"
	if s.pending != nil {
		p = fmt.Sprintf("%d entries, hardstate=%v", len(s.pending.entries), s.pending.hs != nil)
	}
	return fmt.Sprintf("persisted term=%d vote=%q entries=%d; interrupted save: %s",
		s.persisted.hs.Term, s.persisted.hs.Vote, len(s.persisted.entries), p)
}
