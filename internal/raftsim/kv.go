package raftsim

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
)

// The deterministic client tier (Phase 12, docs/LINEARIZABILITY.md §8).
//
// Every node's state machine is a kv.Store — the same code dkvd runs — and
// client operations are events in the script: KVPut, KVGet and KVDelete
// invoke an operation on a node, KVTimeout makes a client give up on the one it
// is waiting for. A write is proposed through the core and completes through
// the driver's own raftnode.Waiters (success only when applied in its term,
// ErrLost when a different entry is applied at its index); a read registers a
// ReadIndex with the core, is confirmed through the driver's own
// raftnode.Reads as the Ready's ReadStates are drained by the real
// DrainReadyAt, and is served from the node's store once applied through it.
// Nothing about completion is re-implemented here, so what this tier checks is
// the code the real node runs, under a simulated clock and network.
//
// Every invocation, attempt and completion is recorded, in event order, into a
// lincheck.Recorder: the run's client-visible history, which is checked for
// linearizability after the run — and, independently of that checker, three
// implementation invariants are checked at the instant each operation
// completes (INV-X5..X7, docs/INVARIANTS.md). A run with clients is still a
// pure function of (Config, script), so a non-linearizable history replays
// exactly and its script can be minimized.
//
// A client has at most one operation outstanding (a real client that is still
// waiting cannot issue its next request). A request to a node that is down is
// a definite rejection (connection refused: nothing was sent). A request to a
// paused node stalls until the client times out (the request sits unread in a
// frozen process). A not-leader answer is followed by one redirect to the
// leader it names, as the Phase 12 workload does, and every attempt is
// recorded. A write whose node dies before answering is Incomplete: it may
// still commit. Nothing is ever retried after an unknown outcome.

// maxKVAttempts bounds one operation's redirects.
const maxKVAttempts = 3

// KVStats counts client-visible outcomes.
type KVStats struct {
	Ops, OK, NotFound, Rejected, Incomplete int
	Redirects, Lost, Unavailable, Stalled   int
	ReadsServed, WritesAcked                int
}

type simClient struct {
	name    string
	op      int // recorder op id; 0 = idle
	kind    lincheck.Kind
	key     string
	value   []byte
	node    *node
	inc     int // the incarnation of node that holds the request
	attempt int
	stalled bool // the request sits in a paused node
	write   <-chan raftnode.Outcome
	read    <-chan raftnode.Outcome
	index   uint64 // the write's entry index
	term    uint64 // the term the write was proposed in / the read registered in
	data    []byte // the write's encoded command
	// regCommitted is the highest index committed anywhere in the cluster when
	// the read was registered: INV-X7 requires the read to be served at or
	// above it.
	regCommitted uint64
}

// kvState is the cluster's client-side state.
type kvState struct {
	rec     *lincheck.Recorder
	clients map[string]*simClient
	stats   KVStats
	// LocalReads is a deliberately BROKEN read path for tests of this tier
	// itself: a Get is served from the contacted node's store at once, with no
	// ReadIndex and no leadership check. It must produce non-linearizable
	// histories under faults — proof that the tier can see a stale read.
	localReads bool
}

func (c *Cluster) kvInit() {
	c.kv = &kvState{rec: lincheck.NewRecorder(), clients: map[string]*simClient{}}
}

// UnsafeLocalReads switches every later Get to the broken local-read path (see
// kvState.localReads). Only tests of the tier itself use it.
func (c *Cluster) UnsafeLocalReads() { c.kv.localReads = true }

// History returns the client-visible history recorded so far (operations still
// waiting are Incomplete).
func (c *Cluster) History() lincheck.History { return c.kv.rec.History() }

// KVStats returns the client outcome counters.
func (c *Cluster) KVStats() KVStats { return c.kv.stats }

// Store returns a node's current state machine (nil while it is down).
func (c *Cluster) Store(id NodeID) *kv.Store {
	n := c.nodes[id]
	if n == nil || !n.up {
		return nil
	}
	return n.store
}

// Busy reports whether a client is waiting for an operation.
func (c *Cluster) Busy(client string) bool {
	cl := c.kv.clients[client]
	return cl != nil && cl.op != 0
}

func (c *Cluster) client(name string) *simClient {
	cl := c.kv.clients[name]
	if cl == nil {
		cl = &simClient{name: name}
		c.kv.clients[name] = cl
	}
	return cl
}

// kvStart invokes a client operation (KVPut, KVGet, KVDelete). It returns the
// node the event touched, if any.
func (c *Cluster) kvStart(e Event) *node {
	cl := c.client(e.Client)
	if cl.op != 0 || e.Key == "" || len(e.Key) > kv.MaxKeyLen {
		c.skip(e)
		return nil
	}
	var kind lincheck.Kind
	var value []byte
	switch e.Kind {
	case KVPut:
		kind, value = lincheck.Put, []byte(e.Data)
	case KVGet:
		kind = lincheck.Get
	default:
		kind = lincheck.Delete
	}
	c.kv.stats.Ops++
	*cl = simClient{name: cl.name, kind: kind, key: e.Key, value: value}
	cl.op = c.kv.rec.Begin(cl.name, kind, e.Key, value)
	c.trace.add(c.step, "kv-invoke %s op=%d %s %q %q at %s", cl.name, cl.op, kind, e.Key, value, e.Node)
	return c.kvAttempt(cl, e.Node, maxKVAttempts)
}

// kvAttempt sends the client's request to target.
func (c *Cluster) kvAttempt(cl *simClient, target NodeID, left int) *node {
	cl.attempt = c.kv.rec.Attempt(cl.op, string(target))
	n := c.nodes[target]
	switch {
	case n == nil || !n.up:
		c.kv.rec.AttemptDone(cl.op, cl.attempt, true, "unavailable", 0)
		c.kv.stats.Unavailable++
		c.kvEnd(cl, lincheck.Rejected, nil, "", 0, 0, "unavailable")
		return nil
	case n.paused:
		cl.node, cl.inc, cl.stalled = n, n.inc, true
		c.kv.stats.Stalled++
		c.trace.add(c.step, "kv-stalled %s op=%d at paused %s", cl.name, cl.op, n.id)
		return nil
	}
	cl.node, cl.inc = n, n.inc
	var err error
	switch {
	case cl.kind == lincheck.Get && c.kv.localReads:
		v, ok := n.store.Get([]byte(cl.key))
		c.kv.rec.AttemptDone(cl.op, cl.attempt, true, "local", n.core.Term())
		if ok {
			c.kvEnd(cl, lincheck.OK, v, n.id, n.core.Term(), n.core.AppliedIndex(), "local read")
		} else {
			c.kvEnd(cl, lincheck.NotFound, nil, n.id, n.core.Term(), n.core.AppliedIndex(), "local read")
		}
		return nil
	case cl.kind == lincheck.Get:
		var rs raft.ReadState
		if rs, err = n.core.ReadIndex(); err == nil {
			cl.term = n.core.Term()
			cl.regCommitted = uint64(len(c.chk.committed))
			cl.read = n.reads.Add(rs.ID, cl.term)
			c.trace.add(c.step, "kv-read %s op=%d at %s id=%d index=%d term=%d committed=%d", cl.name, cl.op, n.id, rs.ID, rs.Index, cl.term, cl.regCommitted)
		}
	default:
		cmd := kv.Command{Op: kv.OpPut, Key: []byte(cl.key), Value: cl.value}
		if cl.kind == lincheck.Delete {
			cmd = kv.Command{Op: kv.OpDelete, Key: []byte(cl.key)}
		}
		data := cmd.Encode()
		if err = n.core.Propose(data); err == nil {
			cl.index, cl.term, cl.data = n.core.LastIndex(), n.core.Term(), data
			c.chk.proposed(string(data), c.step)
			c.chk.kvCommands[string(data)] = true
			cl.write = n.waiters.Add(cl.index, cl.term, n.core.AppliedIndex())
			c.trace.add(c.step, "kv-write %s op=%d at %s idx=%d term=%d", cl.name, cl.op, n.id, cl.index, cl.term)
		}
	}
	if err != nil {
		hint := n.core.LeaderID()
		c.kv.rec.AttemptDone(cl.op, cl.attempt, true, "not-leader("+string(hint)+")", n.core.Term())
		c.kv.stats.Redirects++
		c.trace.add(c.step, "kv-redirect %s op=%d from %s to %q", cl.name, cl.op, n.id, hint)
		if left > 1 && hint != "" && hint != n.id {
			return c.kvAttempt(cl, hint, left-1)
		}
		c.kvEnd(cl, lincheck.Rejected, nil, "", 0, 0, "not leader")
		return nil
	}
	c.drain(n)
	return n
}

// kvTimeout makes a waiting client give up: its operation is Incomplete — for a
// write, it may still take effect.
func (c *Cluster) kvTimeout(e Event) {
	cl := c.kv.clients[e.Client]
	if cl == nil || cl.op == 0 {
		c.skip(e)
		return
	}
	c.kv.rec.AttemptDone(cl.op, cl.attempt, false, "timeout", 0)
	c.kvEnd(cl, lincheck.Incomplete, nil, "", 0, 0, "timeout")
}

// kvPoll completes every client whose request has an outcome, in client-name
// order (so the history is a pure function of the script). It runs after every
// event, once the event's invariant checks have run — so the global committed
// record is current when INV-X5..X7 are checked.
func (c *Cluster) kvPoll() {
	if len(c.kv.clients) == 0 || c.viol != nil {
		return
	}
	names := make([]string, 0, len(c.kv.clients))
	for name := range c.kv.clients {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		cl := c.kv.clients[name]
		if cl.op == 0 || cl.stalled {
			continue
		}
		var out raftnode.Outcome
		ch := cl.write
		if ch == nil {
			ch = cl.read
		}
		select {
		case out = <-ch:
		default:
			continue
		}
		n := cl.node
		alive := n.up && n.inc == cl.inc
		switch {
		case out.Err == nil && !alive:
			// The outcome was reached, then the process died before the
			// response left it: the client heard nothing.
			c.kv.rec.AttemptDone(cl.op, cl.attempt, false, "died before replying", 0)
			c.kvEnd(cl, lincheck.Incomplete, nil, "", 0, 0, "no reply")
		case out.Err == nil && cl.write != nil:
			if !c.committedAs(cl.index, cl.term, cl.data) {
				c.violate("INV-X5", "%s op %d: write acknowledged at index %d term %d, but that entry is not the committed one there", cl.name, cl.op, cl.index, cl.term)
				return
			}
			c.kv.rec.AttemptDone(cl.op, cl.attempt, true, "ok", cl.term)
			c.kv.stats.WritesAcked++
			c.kvEnd(cl, lincheck.OK, nil, n.id, cl.term, cl.index, "applied")
		case out.Err == nil:
			if out.Index < cl.regCommitted || n.core.AppliedIndex() < cl.regCommitted {
				c.violate("INV-X7", "%s op %d: read served at read index %d (applied %d) on %s, but index %d was committed in the cluster when it was registered",
					cl.name, cl.op, out.Index, n.core.AppliedIndex(), n.id, cl.regCommitted)
				return
			}
			v, ok := n.store.Get([]byte(cl.key))
			c.kv.rec.AttemptDone(cl.op, cl.attempt, true, "ok", cl.term)
			c.kv.stats.ReadsServed++
			if ok {
				c.kvEnd(cl, lincheck.OK, v, n.id, cl.term, out.Index, "read")
			} else {
				c.kvEnd(cl, lincheck.NotFound, nil, n.id, cl.term, out.Index, "read")
			}
		case errors.Is(out.Err, raftnode.ErrLost):
			if c.committedAs(cl.index, cl.term, cl.data) {
				c.violate("INV-X6", "%s op %d: write reported lost, but its entry (index %d, term %d) is committed", cl.name, cl.op, cl.index, cl.term)
				return
			}
			c.kv.rec.AttemptDone(cl.op, cl.attempt, true, "lost", cl.term)
			c.kv.stats.Lost++
			c.kvEnd(cl, lincheck.Rejected, nil, n.id, cl.term, cl.index, "lost")
		case errors.Is(out.Err, raft.ErrNotLeader):
			// A read the core dropped when it stopped leading: a definite
			// no-effect (reads have none anyway).
			c.kv.rec.AttemptDone(cl.op, cl.attempt, true, "not-leader", cl.term)
			c.kv.stats.Redirects++
			c.kvEnd(cl, lincheck.Rejected, nil, "", 0, 0, "read dropped: no longer leader")
		default:
			c.kv.rec.AttemptDone(cl.op, cl.attempt, false, "unknown: "+out.Err.Error(), 0)
			c.kvEnd(cl, lincheck.Incomplete, nil, "", 0, 0, "unknown: "+out.Err.Error())
		}
	}
}

// committedAs reports whether the global committed record holds exactly this
// entry at index.
func (c *Cluster) committedAs(index, term uint64, data []byte) bool {
	if index == 0 || index > uint64(len(c.chk.committed)) {
		return false
	}
	e := c.chk.committed[index-1].e
	return e.Term == term && string(e.Data) == string(data)
}

func (c *Cluster) kvEnd(cl *simClient, o lincheck.Outcome, output []byte, node NodeID, term, index uint64, why string) {
	c.kv.rec.End(cl.op, o, output, string(node), term, index)
	switch o {
	case lincheck.OK:
		c.kv.stats.OK++
	case lincheck.NotFound:
		c.kv.stats.NotFound++
	case lincheck.Rejected:
		c.kv.stats.Rejected++
	default:
		c.kv.stats.Incomplete++
	}
	if o == lincheck.OK && cl.kind == lincheck.Get {
		c.trace.add(c.step, "kv-complete %s op=%d %s %q (%s) at %s index=%d", cl.name, cl.op, o, output, why, node, index)
	} else {
		c.trace.add(c.step, "kv-complete %s op=%d %s (%s) at %s index=%d", cl.name, cl.op, o, why, node, index)
	}
	*cl = simClient{name: cl.name}
}

// kvNodeUp gives a booting node a fresh, empty state machine and empty request
// tables: a restarted node rebuilds its store by re-applying its recovered
// committed prefix (docs/CRASH_RECOVERY.md §6), and no request survives a
// process.
func (n *node) kvNodeUp() {
	n.store = kv.NewStore()
	n.waiters = raftnode.NewWaiters()
	n.reads = raftnode.NewReads()
}

// kvNodeDown fails every request the dying process held (their clients hear
// nothing: unknown).
func (n *node) kvNodeDown() {
	if n.waiters != nil {
		n.waiters.FailAll(raft.ErrStopped)
		n.reads.FailAll(raft.ErrStopped)
	}
	n.store, n.waiters, n.reads = nil, nil, nil
}

// applyToStore applies a committed entry to the node's store: the election no-op
// and the non-key-value commands of plain Propose events apply as no-ops.
func (n *node) applyToStore(index uint64, data []byte) error {
	if !kv.IsCommand(data) {
		data = nil
	}
	return n.store.Apply(index, data)
}

// --- seeded client workloads ---

// KVProfiles are the built-in client workload mixes: clients against a cluster
// under each family of faults. Their runs are checked for linearizability
// (TestKVSeededHistoriesAreLinearizable) on top of every Raft invariant.
var KVProfiles = []Profile{
	{Name: "kv-steady", Nodes: 3, Steps: 1500, Tick: 300, Deliver: 700,
		KVPut: 60, KVGet: 60, KVDelete: 15, Clients: 4, Keys: 3, FIFOPercent: 90},
	{Name: "kv-partitions", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Partition: 12, Heal: 12,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 6, Keys: 2, FIFOPercent: 80},
	// Two-sided splits of a 5-node group: the minority side can keep a leader
	// AND a follower acknowledging its heartbeats in the old term — the
	// sharpest stale-leader case for ReadIndex's quorum rule.
	{Name: "kv-splits", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Split: 3, Heal: 1,
		KVPut: 50, KVGet: 70, KVDelete: 10, KVTimeout: 8, Clients: 6, Keys: 2, FIFOPercent: 80},
	{Name: "kv-crashes", Nodes: 3, Steps: 2500, Tick: 300, Deliver: 600, Crash: 10, Restart: 40,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 5, Keys: 2, FIFOPercent: 80, PowerLossPercent: 40, MaxTorn: 128},
	{Name: "kv-messages", Nodes: 5, Steps: 2500, Tick: 300, Deliver: 600, Drop: 50, Duplicate: 50, Delay: 50, Pause: 4, Resume: 30,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 6, Keys: 2, FIFOPercent: 30, MaxDelay: 60},
	{Name: "kv-crashpoints", Nodes: 3, Steps: 2500, Tick: 300, Deliver: 600, CrashAt: 12, Restart: 40,
		KVPut: 60, KVGet: 60, KVDelete: 10, KVTimeout: 8, Clients: 5, Keys: 2, FIFOPercent: 80, PowerLossPercent: 40, MaxTorn: 128},
	{Name: "kv-mixed", Nodes: 5, Steps: 3000, Tick: 300, Deliver: 600, Drop: 20, Duplicate: 20, Delay: 20,
		Partition: 6, Heal: 15, Crash: 5, Restart: 30, Pause: 5, Resume: 30, FailPersist: 3, CrashAt: 4,
		KVPut: 50, KVGet: 60, KVDelete: 10, KVTimeout: 6, Clients: 8, Keys: 3, FIFOPercent: 60, PowerLossPercent: 40, MaxDelay: 40, MaxTorn: 64},
}

// KVProfileByName returns the named client profile.
func KVProfileByName(name string) (Profile, bool) {
	for _, p := range KVProfiles {
		if p.Name == name {
			return p, true
		}
	}
	return Profile{}, false
}

// kvEvents adds the client events a profile weights to the generator's options.
func (c *Cluster) kvEvents(rng *rand.Rand, p Profile, add func(int, bool, func() Event)) {
	if p.Clients == 0 {
		return
	}
	var idle, busy []string
	for i := 1; i <= p.Clients; i++ {
		name := fmt.Sprintf("c%d", i)
		if c.Busy(name) {
			busy = append(busy, name)
		} else {
			idle = append(idle, name)
		}
	}
	// Clients mostly go to a node that believes it leads — possibly a deposed
	// one, which is exactly the stale-leader case ReadIndex must survive — and
	// otherwise anywhere, including down and paused nodes.
	target := func() NodeID {
		if ls := c.Leaders(); len(ls) > 0 && rng.Intn(100) < 75 {
			return ls[rng.Intn(len(ls))]
		}
		return c.ids[rng.Intn(len(c.ids))]
	}
	key := func() string { return fmt.Sprintf("k%d", rng.Intn(max(p.Keys, 1))) }
	pick := func(s []string) string { return s[rng.Intn(len(s))] }
	add(p.KVPut, len(idle) > 0, func() Event {
		cl := pick(idle)
		return Event{Kind: KVPut, Node: target(), Client: cl, Key: key(), Data: fmt.Sprintf("%s.%d", cl, c.step+1)}
	})
	add(p.KVGet, len(idle) > 0, func() Event {
		return Event{Kind: KVGet, Node: target(), Client: pick(idle), Key: key()}
	})
	add(p.KVDelete, len(idle) > 0, func() Event {
		return Event{Kind: KVDelete, Node: target(), Client: pick(idle), Key: key()}
	})
	add(p.KVTimeout, len(busy) > 0, func() Event { return Event{Kind: KVTimeout, Client: pick(busy)} })
}

// RunKV executes a seeded client workload: Steps events drawn from the profile
// (faults, scheduling and client operations), then stabilization, then every
// waiting client is allowed to finish or times out, and finally a fresh client
// reads every key through the leader — so the history ends with the converged
// state observed through the linearizable read path. It is a pure function of
// (profile, seed); the Result carries the history.
func RunKV(p Profile, seed int64) *Result {
	c, err := New(Config{Nodes: p.Nodes, Seed: seed})
	if err != nil {
		return &Result{Seed: seed, Profile: p.Name, Violation: &Violation{Invariant: "harness", Detail: err.Error()}}
	}
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < p.Steps && c.viol == nil; i++ {
		c.Apply(c.generate(rng, p))
	}
	if c.viol == nil {
		c.Stabilize(400)
	}
	if c.viol == nil {
		c.SettleClients(p.Keys, 200)
	}
	if c.viol == nil {
		c.checkStores()
	}
	return c.result(seed, p.Name)
}

// SettleClients runs fair rounds until no client is waiting (a client stalled in
// a node that was paused times out), then has a fresh client read every key
// k0..k{keys-1} through the leader and waits for those reads. Every action is an
// event, so a replay reproduces it.
func (c *Cluster) SettleClients(keys, maxRounds int) {
	round := func() {
		for _, id := range c.ids {
			c.Apply(Event{Kind: Tick, Node: id})
		}
		c.deliverAllFIFO()
	}
	names := func() []string {
		var out []string
		for name := range c.kv.clients {
			out = append(out, name)
		}
		sort.Strings(out)
		return out
	}
	for r := 0; r < maxRounds && c.viol == nil && c.anyBusy(); r++ {
		for _, name := range names() {
			if cl := c.kv.clients[name]; cl.op != 0 && cl.stalled {
				c.Apply(Event{Kind: KVTimeout, Client: name})
			}
		}
		round()
	}
	for _, name := range names() {
		if c.Busy(name) {
			c.Apply(Event{Kind: KVTimeout, Client: name})
		}
	}
	for k := 0; k < keys && c.viol == nil; k++ {
		ls := c.Leaders()
		if len(ls) != 1 {
			round()
			ls = c.Leaders()
		}
		if len(ls) != 1 {
			continue
		}
		cl := fmt.Sprintf("final%d", k)
		c.Apply(Event{Kind: KVGet, Node: ls[0], Client: cl, Key: fmt.Sprintf("k%d", k)})
		for r := 0; r < maxRounds && c.viol == nil && c.Busy(cl); r++ {
			round()
		}
	}
}

func (c *Cluster) anyBusy() bool {
	for _, cl := range c.kv.clients {
		if cl.op != 0 {
			return true
		}
	}
	return false
}

// checkStores is the reference-model differential after convergence: every
// node's kv.Store equals the lincheck register model folded over the committed
// log (two independent implementations of the Phase 1 semantics), key by key.
func (c *Cluster) checkStores() {
	model := map[string]lincheck.State{}
	for _, ce := range c.chk.committed {
		cmd, err := kv.Decode(ce.e.Data)
		if err != nil {
			continue // the no-op and plain Propose commands
		}
		op := lincheck.Op{Key: string(cmd.Key), Outcome: lincheck.OK, Kind: lincheck.Delete}
		if cmd.Op == kv.OpPut {
			op.Kind, op.Value = lincheck.Put, cmd.Value
		}
		model[op.Key], _ = lincheck.Step(model[op.Key], op)
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.up {
			continue
		}
		snap := n.store.Snapshot()
		var diffs []string
		for k, st := range model {
			v, ok := snap[k]
			if ok != st.Present || (ok && string(v) != st.Value) {
				diffs = append(diffs, fmt.Sprintf("%q: store (%v,%q) model (%v,%q)", k, ok, v, st.Present, st.Value))
			}
		}
		for k := range snap {
			if _, ok := model[k]; !ok {
				diffs = append(diffs, fmt.Sprintf("%q present in the store, never written in the committed log", k))
			}
		}
		if len(diffs) > 0 {
			sort.Strings(diffs)
			c.violate("INV-X8", "%s's store differs from the reference model over the committed log: %s", id, strings.Join(diffs, "; "))
			return
		}
	}
}
