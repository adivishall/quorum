package raftsim

import (
	"fmt"
	"math/rand"

	"github.com/adivishall/quorum/internal/raftnode"
)

// Profile weights the kinds of events a seeded chaos run generates. The weights
// are relative; a zero weight disables that fault.
type Profile struct {
	Name  string
	Nodes int
	Steps int // chaos events before stabilization

	Tick, Deliver, Propose        int
	Drop, Duplicate, Delay        int
	Partition, Heal               int
	Crash, Restart, Pause, Resume int
	FailPersist                   int
	CrashAt                       int // arm a crash at a driver or I/O crash point (Phase 11)
	FIFOPercent                   int // chance a delivery takes the oldest message (else a random one: reordering)
	PowerLossPercent              int // chance a crash also models power loss on the node's disk
	MaxDelay, MaxTorn             int // bounds on Delay steps and torn-tail bytes
}

// Profiles are the built-in chaos mixes. Every one exercises the continuous
// invariants; each emphasises a different part of the fault model. Fault weights
// are kept low relative to ticks and deliveries so that faults interleave with
// real replication — a run that is mostly down tests recovery but not much else —
// and every run is required to have injected its faults AND committed commands
// (chaos_test.go).
var Profiles = []Profile{
	{Name: "mixed", Nodes: 5, Steps: 3000,
		Tick: 300, Deliver: 600, Propose: 80, Drop: 20, Duplicate: 20, Delay: 20,
		Partition: 5, Heal: 15, Crash: 5, Restart: 30, Pause: 5, Resume: 30, FailPersist: 4,
		FIFOPercent: 60, PowerLossPercent: 40, MaxDelay: 40, MaxTorn: 64},
	{Name: "partitions", Nodes: 5, Steps: 3000,
		Tick: 300, Deliver: 600, Propose: 80, Partition: 15, Heal: 15,
		FIFOPercent: 80},
	{Name: "crashes", Nodes: 3, Steps: 3000,
		Tick: 300, Deliver: 600, Propose: 80, Crash: 12, Restart: 40,
		FIFOPercent: 80, PowerLossPercent: 50, MaxTorn: 128},
	{Name: "disk", Nodes: 3, Steps: 3000,
		Tick: 300, Deliver: 600, Propose: 100, Crash: 4, Restart: 40, FailPersist: 10,
		FIFOPercent: 80, PowerLossPercent: 50, MaxTorn: 128},
	{Name: "messages", Nodes: 5, Steps: 3000,
		Tick: 300, Deliver: 600, Propose: 80, Drop: 60, Duplicate: 60, Delay: 60, Pause: 5, Resume: 30,
		FIFOPercent: 30, MaxDelay: 60},
	// crashpoints (Phase 11): every crash lands on an exact boundary of the
	// persist → send → advance → apply cycle or of the durable log's I/O — before
	// a Save, after it, between record writes, before the fsync, after one message
	// of a Ready, between Apply and AppliedTo — half of them with a power loss
	// keeping a torn tail. Restarts recover through the real startup path.
	{Name: "crashpoints", Nodes: 3, Steps: 3000,
		Tick: 300, Deliver: 600, Propose: 100, CrashAt: 12, Restart: 40,
		FIFOPercent: 80, PowerLossPercent: 50, MaxTorn: 128},
}

// crashPointNames are the crash points a seeded run may arm: the driver's, then
// the durable log's I/O boundaries.
var crashPointNames = func() []string {
	var out []string
	for _, p := range raftnode.Points {
		out = append(out, p.String())
	}
	return append(out, IOPoints...)
}()

// ProfileByName returns the named built-in profile.
func ProfileByName(name string) (Profile, bool) {
	for _, p := range Profiles {
		if p.Name == name {
			return p, true
		}
	}
	return Profile{}, false
}

// Result is the outcome of a run: its fingerprint, its full replay script, and
// the first violation (nil if every invariant held).
type Result struct {
	Seed      int64
	Profile   string
	Script    []Event
	TraceHash string
	Violation *Violation
	Tail      []string
	Stats     Stats
}

// Run executes a seeded chaos run: Steps events drawn from the profile by a
// generator seeded with seed, then stabilization (every fault healed, every node
// restarted) and the convergence check. It is a pure function of (profile, seed).
func Run(p Profile, seed int64) *Result {
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
	return c.result(seed, p.Name)
}

// Replay executes an explicit script on a fresh cluster. Replaying the script of
// a Run reproduces that run exactly — the same trace, hash and violation.
func Replay(cfg Config, script []Event) *Result {
	c, err := New(cfg)
	if err != nil {
		return &Result{Seed: cfg.Seed, Violation: &Violation{Invariant: "harness", Detail: err.Error()}}
	}
	for _, e := range script {
		if c.viol != nil {
			break
		}
		c.Apply(e)
	}
	return c.result(cfg.Seed, "replay")
}

func (c *Cluster) result(seed int64, profile string) *Result {
	return &Result{
		Seed: seed, Profile: profile, Script: c.Script(), TraceHash: c.trace.Hash(),
		Violation: c.viol, Tail: c.trace.Tail(60), Stats: c.stats,
	}
}

// Stabilize ends the fault period: it heals every partition, disarms every
// persistence fault, releases every delayed message, resumes and restarts every
// node, then runs fair rounds — every node ticks once, then every in-flight
// message is delivered in order — proposing a sentinel command on the leader,
// until the sentinel is committed and applied everywhere or maxRounds pass. It
// ends with CheckConverged (INV-F3). Every action is an Event in the script, so a
// replay reproduces the stabilization too.
func (c *Cluster) Stabilize(maxRounds int) {
	c.Apply(Event{Kind: HealAll})
	c.Apply(Event{Kind: Disarm})
	c.Apply(Event{Kind: Release})
	for _, id := range c.ids {
		if c.nodes[id].paused {
			c.Apply(Event{Kind: Resume, Node: id})
		}
		if !c.nodes[id].up {
			c.Apply(Event{Kind: Restart, Node: id})
		}
	}
	proposedIn := uint64(0)
	for r := 0; r < maxRounds && c.viol == nil && !c.settled(); r++ {
		for _, id := range c.ids {
			c.Apply(Event{Kind: Tick, Node: id})
		}
		c.deliverAllFIFO()
		if ls := c.Leaders(); len(ls) == 1 {
			L := c.nodes[ls[0]]
			if t := L.core.Term(); t > proposedIn && !c.settled() {
				proposedIn = t
				c.Apply(Event{Kind: Propose, Node: L.id, Data: fmt.Sprintf("final-%d-%d", c.step, t)})
				c.deliverAllFIFO()
			}
		}
	}
	c.Apply(Event{Kind: CheckConverged})
}

// settled reports whether the cluster already satisfies the convergence check.
func (c *Cluster) settled() bool {
	ls := c.Leaders()
	if len(ls) != 1 {
		return false
	}
	L := c.nodes[ls[0]]
	last := L.core.LastIndex()
	if t, _ := L.mem.Term(last); last == 0 || t != L.core.Term() || L.core.CommitIndex() != last {
		return false
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.up || n.core.Term() != L.core.Term() || n.core.LastIndex() != last ||
			n.core.CommitIndex() != last || n.core.AppliedIndex() != last {
			return false
		}
	}
	return true
}

// DeliverAll delivers in-flight messages oldest first until none is deliverable
// (delayed messages and messages to paused nodes stay in flight). It is bounded, so
// a message storm cannot hang a run.
func (c *Cluster) DeliverAll() { c.deliverAllFIFO() }

func (c *Cluster) deliverAllFIFO() {
	for i := 0; i < 10000 && c.viol == nil; i++ {
		var next *flight
		for _, f := range c.flights {
			if f.holdUntil <= c.step && !c.Paused(f.msg.To) {
				next = f
				break
			}
		}
		if next == nil {
			return
		}
		pos := 0
		for _, g := range c.flights {
			if g == next {
				break
			}
			if g.msg.From == next.msg.From && g.msg.To == next.msg.To {
				pos++
			}
		}
		c.Apply(Event{Kind: Deliver, From: next.msg.From, To: next.msg.To, Pos: pos})
	}
}

// generate draws one event that applies to the current state, weighted by the
// profile. The choice depends only on the state and rng, so a run is a pure
// function of (profile, seed).
func (c *Cluster) generate(rng *rand.Rand, p Profile) Event {
	type option struct {
		w  int
		fn func() Event
	}
	var opts []option
	add := func(w int, ok bool, fn func() Event) {
		if w > 0 && ok {
			opts = append(opts, option{w, fn})
		}
	}
	var running, down, paused, armable, crashable []NodeID
	for _, id := range c.ids {
		n := c.nodes[id]
		switch {
		case !n.up:
			down = append(down, id)
		case n.paused:
			paused = append(paused, id)
		default:
			running = append(running, id)
		}
		if n.up && n.inj.Armed() == 0 {
			armable = append(armable, id)
		}
		if n.up && n.inj.Armed() == 0 && n.armed == nil {
			crashable = append(crashable, id)
		}
	}
	var deliverable []*flight
	for _, f := range c.flights {
		if f.holdUntil <= c.step && !c.Paused(f.msg.To) {
			deliverable = append(deliverable, f)
		}
	}
	pick := func(ids []NodeID) NodeID { return ids[rng.Intn(len(ids))] }
	addr := func(f *flight) (NodeID, NodeID, int) {
		pos := 0
		for _, g := range c.flights {
			if g == f {
				break
			}
			if g.msg.From == f.msg.From && g.msg.To == f.msg.To {
				pos++
			}
		}
		return f.msg.From, f.msg.To, pos
	}

	add(p.Tick, len(running) > 0, func() Event { return Event{Kind: Tick, Node: pick(running)} })
	add(p.Deliver, len(deliverable) > 0, func() Event {
		f := deliverable[0]
		if rng.Intn(100) >= p.FIFOPercent {
			f = deliverable[rng.Intn(len(deliverable))]
		}
		from, to, pos := addr(f)
		return Event{Kind: Deliver, From: from, To: to, Pos: pos}
	})
	add(p.Propose, len(running) > 0, func() Event {
		target := pick(running)
		if ls := c.Leaders(); len(ls) > 0 && rng.Intn(100) < 80 {
			if n := c.nodes[ls[rng.Intn(len(ls))]]; !n.paused {
				target = n.id
			}
		}
		return Event{Kind: Propose, Node: target, Data: fmt.Sprintf("p%d", c.step+1)}
	})
	msgEvent := func(k Kind) func() Event {
		return func() Event {
			from, to, pos := addr(c.flights[rng.Intn(len(c.flights))])
			e := Event{Kind: k, From: from, To: to, Pos: pos}
			if k == Delay {
				e.N = 1 + rng.Intn(p.MaxDelay+1)
			}
			return e
		}
	}
	add(p.Drop, len(c.flights) > 0, msgEvent(Drop))
	add(p.Duplicate, len(c.flights) > 0, msgEvent(Duplicate))
	add(p.Delay, len(c.flights) > 0 && p.MaxDelay > 0, msgEvent(Delay))
	add(p.Partition, len(c.ids) > 1, func() Event {
		a := pick(c.ids)
		switch rng.Intn(3) {
		case 0:
			return Event{Kind: Isolate, Node: a}
		case 1:
			b := pick(c.ids)
			if b == a {
				return Event{Kind: Isolate, Node: a}
			}
			return Event{Kind: Cut, From: a, To: b}
		default:
			b := pick(c.ids)
			if b == a {
				return Event{Kind: Isolate, Node: a}
			}
			return Event{Kind: Block, From: a, To: b} // one-way
		}
	})
	blocked := c.links.BlockedLinks()
	add(p.Heal, len(blocked) > 0, func() Event {
		if rng.Intn(3) == 0 {
			return Event{Kind: HealAll}
		}
		l := blocked[rng.Intn(len(blocked))]
		return Event{Kind: Unblock, From: NodeID(l.From), To: NodeID(l.To)}
	})
	add(p.Crash, len(running)+len(paused) > 0, func() Event {
		victims := append(append([]NodeID(nil), running...), paused...)
		e := Event{Kind: Crash, Node: pick(victims)}
		if rng.Intn(100) < p.PowerLossPercent {
			e.Power = true
			e.N = rng.Intn(p.MaxTorn + 1)
		}
		return e
	})
	add(p.Restart, len(down) > 0, func() Event { return Event{Kind: Restart, Node: pick(down)} })
	add(p.Pause, len(running) > 0, func() Event { return Event{Kind: Pause, Node: pick(running)} })
	add(p.Resume, len(paused) > 0, func() Event { return Event{Kind: Resume, Node: pick(paused)} })
	add(p.CrashAt, len(crashable) > 0, func() Event {
		e := Event{Kind: CrashAt, Node: pick(crashable), Point: crashPointNames[rng.Intn(len(crashPointNames))], Nth: 1 + rng.Intn(3)}
		if rng.Intn(100) < p.PowerLossPercent {
			e.Power = true
			e.N = rng.Intn(p.MaxTorn + 1)
		}
		return e
	})
	add(p.FailPersist, len(armable) > 0, func() Event {
		e := Event{Kind: FailPersist, Node: pick(armable)}
		switch rng.Intn(3) {
		case 0:
			e.Op = FailWrite
		case 1:
			e.Op, e.N = ShortWrite, 1+rng.Intn(24)
		default:
			e.Op = FailSync
		}
		return e
	})

	total := 0
	for _, o := range opts {
		total += o.w
	}
	if total == 0 {
		return Event{Kind: Release} // nothing applies (e.g. every node down and no restarts allowed)
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

// Minimize shrinks a failing script: it keeps any deletion after which a replay
// still violates the same invariant (see MinimizeFunc). It replays at most
// maxRuns candidates.
func Minimize(cfg Config, script []Event, maxRuns int) []Event {
	orig := Replay(cfg, script)
	if orig.Violation == nil {
		return script
	}
	want := orig.Violation.Invariant
	// Everything after the violating step is irrelevant.
	return MinimizeFunc(cfg, script[:orig.Violation.Step], maxRuns, func(s []Event) bool {
		r := Replay(cfg, s)
		return r.Violation != nil && r.Violation.Invariant == want
	})
}

// MinimizeFunc shrinks a script by greedy chunk deletion (delta debugging),
// keeping any deletion after which keep still holds. Events that stop applying
// after a deletion are skipped on replay, so every candidate is a valid script.
// It evaluates keep at most maxRuns times.
func MinimizeFunc(cfg Config, script []Event, maxRuns int, keep func([]Event) bool) []Event {
	cur := append([]Event(nil), script...)
	runs := 0
	for chunk := len(cur) / 2; chunk >= 1 && runs < maxRuns; {
		removed := false
		for start := 0; start < len(cur) && runs < maxRuns; {
			end := start + chunk
			if end > len(cur) {
				end = len(cur)
			}
			cand := append(append([]Event(nil), cur[:start]...), cur[end:]...)
			runs++
			if keep(cand) {
				cur, removed = cand, true
				continue // retry the same start against the shorter script
			}
			start = end
		}
		if !removed {
			chunk /= 2
		}
	}
	return cur
}

// Report renders a failing result for a test log: what broke, the trace leading
// to it, and how to reproduce it.
func (r *Result) Report(reproduce string) string {
	s := fmt.Sprintf("seed=%d profile=%s steps=%d trace=%s\n%v\n--- last trace lines ---\n",
		r.Seed, r.Profile, len(r.Script), r.TraceHash, r.Violation)
	for _, l := range r.Tail {
		s += l + "\n"
	}
	return s + "--- reproduce ---\n" + reproduce + "\n"
}
