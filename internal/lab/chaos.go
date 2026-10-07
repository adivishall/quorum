package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/multiraft"
)

// Fault kinds a chaos schedule can hold (docs/CHAOS.md).
const (
	FaultKill      = "kill"       // SIGKILL; restarted on its data directory after Hold
	FaultStop      = "stop"       // SIGTERM (a clean shutdown); restarted after Hold
	FaultCrash     = "crash"      // die by SIGKILL at the next CrashPoint (a persistence boundary); restarted after Hold
	FaultPause     = "pause"      // SIGSTOP; SIGCONT after Hold
	FaultIsolate   = "isolate"    // cut every link of the node; healed after Hold
	FaultCut       = "cut"        // cut one link; healed after Hold
	FaultSnapshot  = "snapshot"   // the group's leader takes a snapshot
	FaultAddMember = "add-member" // a spare joins the group: learner, caught up, promoted
)

// FaultKinds are every kind, in a fixed order.
var FaultKinds = []string{FaultKill, FaultStop, FaultCrash, FaultPause, FaultIsolate, FaultCut, FaultSnapshot, FaultAddMember}

// TargetLeader as a fault's Node is resolved when the fault starts: the node
// leading the fault's group then (recorded in the fault's events).
const TargetLeader = "leader"

// Fault is one planned fault. A schedule is a list of them, ordered by At, no
// two overlapping: each one's Hold ends before the next one starts, so at most
// one impairment is in force at a time and it always touches a minority.
type Fault struct {
	At    time.Duration     `json:"at"` // after the load starts (warmup included)
	Kind  string            `json:"kind"`
	Group multiraft.GroupID `json:"group"`
	Node  string            `json:"node,omitempty"` // a node id, or TargetLeader
	Peer  string            `json:"peer,omitempty"` // cut: the link's other end
	Hold  time.Duration     `json:"hold,omitempty"` // how long the impairment lasts
}

func (f Fault) String() string {
	s := fmt.Sprintf("%s at %s group %d", f.Kind, f.At, f.Group)
	if f.Node != "" {
		s += " node " + f.Node
	}
	if f.Peer != "" {
		s += " peer " + f.Peer
	}
	if f.Hold > 0 {
		s += " hold " + f.Hold.String()
	}
	return s
}

// ChaosConfig is one chaos run: a cluster, a recorded workload, and a fault
// schedule — drawn from Seed, or given (Schedule) to replay one exactly.
type ChaosConfig struct {
	Name    string        `json:"name"`
	Cluster ClusterConfig `json:"cluster"`
	Load    load.Config   `json:"load"`
	// Seed draws the schedule. The workload has its own seed (Load.Seed); a
	// zero Load.Seed takes this one.
	Seed  int64    `json:"seed"`
	Kinds []string `json:"kinds"` // the kinds the schedule may draw; empty: every kind the cluster supports
	// Every is the mean gap between one fault's recovery and the next fault
	// (drawn uniformly in [Every/2, 3·Every/2)); Hold the mean impairment
	// (drawn in [Hold/2, 3·Hold/2)). Quiet is the end of the window in which
	// no fault starts, so the run shows the cluster recovering.
	Every time.Duration `json:"every"`
	Hold  time.Duration `json:"hold"`
	Quiet time.Duration `json:"quiet"`
	// Sample is how often every node's status is read during the run.
	Sample time.Duration `json:"sample"`
	// CrashPoint is the driver crash point (internal/raftnode) a "crash"
	// fault kills its node at — the first time the node reaches it after the
	// fault arms it. Every node runs with dkvd's -crash-at seam for it when
	// the schedule holds a crash fault.
	CrashPoint string `json:"crash_point,omitempty"`
	// Settle bounds the wait, after the load, for the cluster to converge.
	Settle time.Duration `json:"settle"`
	// Schedule, if set, is run as given instead of drawn: the replay of a
	// recorded run.
	Schedule []Fault `json:"schedule,omitempty"`
	// Dir, if set, receives the run's artifacts (WriteArtifacts).
	Dir string `json:"-"`
}

func (cc *ChaosConfig) defaults() {
	if cc.Every <= 0 {
		cc.Every = 2 * time.Second
	}
	if cc.Hold <= 0 {
		cc.Hold = 1500 * time.Millisecond
	}
	if cc.Quiet <= 0 {
		cc.Quiet = 2 * time.Second
	}
	if cc.Sample <= 0 {
		cc.Sample = 250 * time.Millisecond
	}
	if cc.Settle <= 0 {
		cc.Settle = 30 * time.Second
	}
	if cc.CrashPoint == "" {
		cc.CrashPoint = "after-save"
	}
	if cc.Load.Seed == 0 {
		cc.Load.Seed = cc.Seed
	}
	cc.Cluster.Links = true
}

// PlanChaos draws a fault schedule from seed: a pure function of its
// arguments — the same seed, configuration, nodes, spares and groups always
// give the same schedule. Faults start after the load's warmup and before the
// window's last Quiet; each is followed by a gap; none overlaps the next. A
// node fault targets a seeded genesis node or, half the time, the group's
// leader (resolved when it starts); add-member uses each spare once.
func PlanChaos(cc ChaosConfig, nodes []string, spares []string, groups []multiraft.GroupID) ([]Fault, error) {
	cc.defaults()
	if len(nodes) < 3 {
		return nil, errors.New("lab: chaos needs at least three nodes, so one impaired node leaves a quorum")
	}
	if len(groups) == 0 {
		return nil, errors.New("lab: chaos needs a group")
	}
	kinds := cc.Kinds
	if len(kinds) == 0 {
		kinds = FaultKinds
	}
	for _, k := range kinds {
		known := false
		for _, kk := range FaultKinds {
			known = known || k == kk
		}
		if !known {
			return nil, fmt.Errorf("lab: unknown fault kind %q (want one of %s)", k, strings.Join(FaultKinds, ", "))
		}
	}
	rng := rand.New(rand.NewSource(cc.Seed))
	between := func(mean time.Duration) time.Duration {
		return mean/2 + time.Duration(rng.Int63n(int64(mean)))
	}
	end := cc.Load.Warmup + cc.Load.Duration - cc.Quiet
	sparesLeft := append([]string(nil), spares...)
	var out []Fault
	for t := cc.Load.Warmup + between(cc.Every/2); ; {
		kind := kinds[rng.Intn(len(kinds))]
		if kind == FaultAddMember && len(sparesLeft) == 0 {
			// No spare left: draw from the other kinds (if there are any).
			var others []string
			for _, k := range kinds {
				if k != FaultAddMember {
					others = append(others, k)
				}
			}
			if len(others) == 0 {
				break
			}
			kind = others[rng.Intn(len(others))]
		}
		f := Fault{At: t, Kind: kind, Group: groups[rng.Intn(len(groups))]}
		switch kind {
		case FaultKill, FaultStop, FaultCrash, FaultPause, FaultIsolate:
			f.Node = nodes[rng.Intn(len(nodes))]
			if rng.Intn(2) == 0 {
				f.Node = TargetLeader
			}
			f.Hold = between(cc.Hold)
		case FaultCut:
			i := rng.Intn(len(nodes))
			j := (i + 1 + rng.Intn(len(nodes)-1)) % len(nodes)
			f.Node, f.Peer = nodes[i], nodes[j]
			f.Hold = between(cc.Hold)
		case FaultSnapshot:
			f.Node = TargetLeader
		case FaultAddMember:
			f.Node, sparesLeft = sparesLeft[0], sparesLeft[1:]
		}
		if t+f.Hold > end {
			break
		}
		out = append(out, f)
		t += f.Hold + between(cc.Every)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("lab: no fault fits a %s window with a %s warmup and a %s quiet end", cc.Load.Duration, cc.Load.Warmup, cc.Quiet)
	}
	return out, nil
}

// ChaosEvent is something that happened during a chaos run: a fault injected
// or recovered (or skipped, or failed), a leader change, a node exiting. At is
// from the load's start; Pos is the history's position when it happened, so
// it falls between the operations it fell between (lincheck.Recorder.Mark).
type ChaosEvent struct {
	At     time.Duration `json:"at"`
	Pos    int64         `json:"pos"`
	Fault  int           `json:"fault"` // the schedule index, or -1
	Kind   string        `json:"kind"`  // inject, recover, skip, error, leader (a change), first-leader, converged
	Node   string        `json:"node,omitempty"`
	Detail string        `json:"detail,omitempty"`
}

// StatusSample is one node's view of one group at one moment.
type StatusSample struct {
	At      time.Duration     `json:"at"`
	Node    string            `json:"node"`
	Group   multiraft.GroupID `json:"group"`
	Role    string            `json:"role"`
	Term    uint64            `json:"term"`
	Leader  string            `json:"leader"`
	Commit  uint64            `json:"commit"`
	Applied uint64            `json:"applied"`
	Last    uint64            `json:"last_index"`
}

// CheckResult is the linearizability verdict on a run's history.
type CheckResult struct {
	Linearizable   bool            `json:"linearizable"`
	Unchecked      bool            `json:"unchecked"` // the checker's budget ran out: no verdict
	Reason         string          `json:"reason,omitempty"`
	Counterexample string          `json:"counterexample,omitempty"` // minimized, in the history's format
	Counts         lincheck.Counts `json:"counts"`
}

// ChaosResult is everything a chaos run recorded.
type ChaosResult struct {
	Config      ChaosConfig                        `json:"config"`
	Schedule    []Fault                            `json:"schedule"`
	Events      []ChaosEvent                       `json:"events"`
	Samples     []StatusSample                     `json:"samples"`
	Load        *load.Result                       `json:"load"`
	Check       CheckResult                        `json:"check"`
	Converged   bool                               `json:"converged"`
	ConvergeErr string                             `json:"converge_error,omitempty"`
	Final       map[string]multiraft.AdminResponse `json:"final"`
	Usage       map[string]NodeUsage               `json:"usage"`
	Elapsed     time.Duration                      `json:"elapsed"`
	History     lincheck.History                   `json:"-"`
	Logs        map[string]string                  `json:"-"` // node id → its processes' output
	Links       map[string]string                  `json:"-"` // "a-b" → the link proxy's log
	Artifacts   string                             `json:"artifacts,omitempty"`
}

// OK reports whether the run passed: its schedule carried out without an
// error event (a fault the runner could not inject or undo — a restart that
// failed, say); a linearizable history, checked; a cluster that converged;
// and clients that completed operations.
func (r *ChaosResult) OK() bool {
	return r.Errors() == 0 && r.Check.Linearizable && !r.Check.Unchecked && r.Converged && r.Load != nil &&
		r.Load.Classes[load.ClassOK]+r.Load.Classes[load.ClassNotFound] > 0
}

// Errors counts the run's error events.
func (r *ChaosResult) Errors() int {
	n := 0
	for _, e := range r.Events {
		if e.Kind == "error" {
			n++
		}
	}
	return n
}

// LeaderChanges counts the events in which some group's leader changed (a
// group's first leader, seen when sampling starts, is not a change).
func (r *ChaosResult) LeaderChanges() int {
	n := 0
	for _, e := range r.Events {
		if e.Kind == "leader" {
			n++
		}
	}
	return n
}

// chaosRun is the state of one run while it executes.
type chaosRun struct {
	c     *Cluster
	cc    ChaosConfig
	rec   *lincheck.Recorder
	start time.Time

	mu      sync.Mutex
	events  []ChaosEvent
	samples []StatusSample
	leaders map[multiraft.GroupID][2]string // group → (leader, term) last seen
	paused  map[string]bool
}

func (r *chaosRun) event(fault int, kind, node, format string, args ...any) {
	e := ChaosEvent{At: time.Since(r.start), Pos: r.rec.Mark(), Fault: fault, Kind: kind, Node: node, Detail: fmt.Sprintf(format, args...)}
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

// RunChaos runs one chaos experiment: it starts the cluster with every link
// proxied, starts the recorded workload, injects the schedule's faults one at
// a time while sampling every node's status, recovers everything once the
// load ends, waits for the cluster to converge, checks the history for
// linearizability and — with cc.Dir — writes the artifacts. The returned
// error is for a run that could not be carried out; a run that found a
// violation returns its result with OK() false.
func RunChaos(ctx context.Context, cc ChaosConfig) (*ChaosResult, error) {
	cc.defaults()
	if cc.Cluster.Nodes < 3 {
		return nil, errors.New("lab: chaos needs at least three nodes")
	}
	crashes := false
	for _, k := range cc.Kinds {
		crashes = crashes || k == FaultCrash
	}
	for _, f := range cc.Schedule {
		crashes = crashes || f.Kind == FaultCrash
	}
	if len(cc.Kinds) == 0 && len(cc.Schedule) == 0 {
		crashes = true
	}
	if crashes {
		cc.Cluster.Extra = append(append([]string(nil), cc.Cluster.Extra...), "-crash-at", cc.CrashPoint+":1", "-crash-armed-by-signal")
	}
	c, err := Start(ctx, cc.Cluster)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var genesis, spares []string
	for _, n := range c.Nodes() {
		if n.Spare {
			spares = append(spares, n.ID)
		} else {
			genesis = append(genesis, n.ID)
		}
	}
	schedule := cc.Schedule
	if schedule == nil {
		if schedule, err = PlanChaos(cc, genesis, spares, c.Groups()); err != nil {
			return nil, err
		}
	}
	cc.Schedule = schedule

	r := &chaosRun{c: c, cc: cc, rec: lincheck.NewRecorder(), leaders: map[multiraft.GroupID][2]string{}, paused: map[string]bool{}}
	lc := cc.Load
	lc.Endpoints = c.Endpoints()
	if rt := c.Route(); rt != nil {
		lc.Route = rt.GroupOf
		lc.RouteSpec = fmt.Sprintf("shards=%d rf=%d", cc.Cluster.Shards, cc.Cluster.RF)
	}
	lc.Groups = c.Groups()
	lc.History, lc.Trace = r.rec, true
	before := c.ScrapeAll()

	type loadOut struct {
		res *load.Result
		err error
	}
	loadDone := make(chan loadOut, 1)
	r.start = time.Now()
	go func() {
		res, err := load.Run(ctx, lc)
		loadDone <- loadOut{res, err}
	}()
	stopSampling := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		t := time.NewTicker(cc.Sample)
		defer t.Stop()
		for {
			r.sample()
			select {
			case <-stopSampling:
				return
			case <-t.C:
			}
		}
	}()

	// The schedule, one fault at a time, on this goroutine.
	var out loadOut
	finished := false
	waitUntil := func(at time.Time) bool { // false once the load has finished
		select {
		case out = <-loadDone:
			finished = true
			return false
		case <-time.After(time.Until(at)):
			return true
		case <-ctx.Done():
			return false
		}
	}
	for i, f := range schedule {
		if finished || !waitUntil(r.start.Add(f.At)) {
			r.event(i, "skip", f.Node, "the load had ended: %s", f)
			continue
		}
		undo := r.inject(ctx, i, f)
		if undo != nil {
			if !finished {
				waitUntil(time.Now().Add(f.Hold))
			}
			undo()
		}
	}
	if !finished {
		out = <-loadDone
	}
	r.recoverAll(ctx)
	close(stopSampling)
	<-sampled
	if out.err != nil && out.res == nil {
		return nil, out.err
	}

	res := &ChaosResult{Config: cc, Schedule: schedule, Load: out.res, Final: map[string]multiraft.AdminResponse{}}
	if err := r.converge(ctx, cc.Settle); err != nil {
		res.ConvergeErr = err.Error()
	} else {
		res.Converged = true
	}
	for _, n := range c.Nodes() {
		if n.Alive() {
			actx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if st, err := c.Admin(actx, n.ID, multiraft.AdminRequest{Op: "status"}); err == nil {
				res.Final[n.ID] = st
			}
			cancel()
		}
	}
	after := c.ScrapeAll()
	res.Usage = map[string]NodeUsage{}
	for id, ss := range after {
		res.Usage[id] = usage(before[id], ss, out.res.Elapsed, restartedIn(r.events, id))
	}
	res.History = r.rec.History()
	res.Check = check(res.History)
	r.mu.Lock()
	res.Events, res.Samples = r.events, r.samples
	r.mu.Unlock()
	res.Elapsed = time.Since(r.start)
	c.Close()
	res.Logs, res.Links = map[string]string{}, map[string]string{}
	for _, n := range c.Nodes() {
		if o := n.Output(); o != "" {
			res.Logs[n.ID] = o
		}
	}
	for k, p := range c.links {
		res.Links[k[0]+"-"+k[1]] = p.Log()
	}
	if cc.Dir != "" {
		if err := WriteArtifacts(cc.Dir, res); err != nil {
			return res, err
		}
		res.Artifacts = cc.Dir
	}
	return res, out.err
}

// restartedIn reports whether a node was restarted during the run: its
// counters then start again from zero (usage).
func restartedIn(events []ChaosEvent, id string) bool {
	for _, e := range events {
		if e.Node == id && e.Kind == "recover" && strings.HasPrefix(e.Detail, "restarted") {
			return true
		}
	}
	return false
}

// check runs the linearizability checker on h.
func check(h lincheck.History) CheckResult {
	out := CheckResult{Counts: h.Summary()}
	if err := h.Validate(); err != nil {
		out.Reason = "malformed history: " + err.Error()
		return out
	}
	r := lincheck.Check(h, lincheck.Options{Minimize: true})
	out.Linearizable, out.Unchecked, out.Reason = r.OK, r.Unchecked, r.Reason
	if !r.OK && len(r.Counterexample) > 0 {
		out.Counterexample = lincheck.Format(r.Counterexample)
	}
	return out
}

// resolve turns a fault's target into a node id: TargetLeader is the node
// leading the fault's group now.
func (r *chaosRun) resolve(ctx context.Context, f Fault) (string, error) {
	if f.Node != TargetLeader {
		return f.Node, nil
	}
	actx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	id, _, err := r.c.Leader(actx, f.Group)
	return id, err
}

// inject starts fault i and returns how to recover from it (nil for a fault
// with nothing to undo, or one that could not be injected).
func (r *chaosRun) inject(ctx context.Context, i int, f Fault) func() {
	c := r.c
	node, err := r.resolve(ctx, f)
	if err != nil {
		r.event(i, "skip", f.Node, "%s: no target: %v", f, err)
		return nil
	}
	fail := func(err error) func() {
		r.event(i, "error", node, "%s: %v", f, err)
		return nil
	}
	switch f.Kind {
	case FaultKill, FaultStop:
		var err error
		if f.Kind == FaultKill {
			err = c.Kill(node)
		} else {
			err = c.Stop(node)
		}
		if err != nil {
			return fail(err)
		}
		r.event(i, "inject", node, "%s (%s)", f.Kind, f)
		return func() { r.restart(ctx, i, node) }
	case FaultCrash:
		if err := c.send(node, syscall.SIGUSR1); err != nil {
			return fail(err)
		}
		r.event(i, "inject", node, "crash armed at %s (%s)", r.cc.CrashPoint, f)
		return func() {
			if c.Node(node).Alive() {
				// The point was not reached during the hold (an idle node):
				// kill it here, so the schedule's effect is the same either way.
				r.event(i, "inject", node, "crash point %s not reached in %s: killed", r.cc.CrashPoint, f.Hold)
				_ = c.Kill(node)
			} else {
				r.event(i, "inject", node, "died at crash point %s", r.cc.CrashPoint)
			}
			r.restart(ctx, i, node)
		}
	case FaultPause:
		if err := c.Pause(node); err != nil {
			return fail(err)
		}
		r.mu.Lock()
		r.paused[node] = true
		r.mu.Unlock()
		r.event(i, "inject", node, "paused (%s)", f)
		return func() {
			if err := c.Resume(node); err != nil {
				r.event(i, "error", node, "resume: %v", err)
				return
			}
			r.mu.Lock()
			delete(r.paused, node)
			r.mu.Unlock()
			r.event(i, "recover", node, "resumed")
		}
	case FaultIsolate:
		if err := c.Isolate(node); err != nil {
			return fail(err)
		}
		r.event(i, "inject", node, "isolated (%s)", f)
		return func() {
			for _, o := range c.Nodes() {
				if !o.Spare && o.ID != node {
					_ = c.Heal(node, o.ID)
				}
			}
			r.event(i, "recover", node, "links healed")
		}
	case FaultCut:
		if err := c.Cut(node, f.Peer); err != nil {
			return fail(err)
		}
		r.event(i, "inject", node, "link to %s cut (%s)", f.Peer, f)
		return func() {
			_ = c.Heal(node, f.Peer)
			r.event(i, "recover", node, "link to %s healed", f.Peer)
		}
	case FaultSnapshot:
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		resp, err := c.Admin(actx, node, multiraft.AdminRequest{Op: "snapshot", Group: uint32(f.Group)})
		switch {
		case err != nil:
			return fail(err)
		case !resp.OK:
			return fail(errors.New(resp.Error))
		}
		r.event(i, "inject", node, "snapshot at index %d (%s)", resp.Index, f)
		return nil
	case FaultAddMember:
		ar := c.act(ctx, Action{Kind: "add-member", Group: f.Group}, time.Since(r.start), map[string]bool{})
		for _, a := range ar {
			if a.Err != "" {
				return fail(errors.New(a.Err))
			}
			r.event(i, "inject", a.Node, "added to group %d in %s: %s", f.Group, a.Settle.Round(time.Millisecond), a.Detail)
		}
		return nil
	}
	return fail(fmt.Errorf("unknown fault kind %q", f.Kind))
}

// restart starts a stopped node again and records it.
func (r *chaosRun) restart(ctx context.Context, i int, node string) {
	if r.c.Node(node).Alive() {
		return
	}
	if err := r.c.Restart(node); err != nil {
		r.event(i, "error", node, "restart: %v", err)
		return
	}
	r.event(i, "recover", node, "restarted")
}

// recoverAll undoes whatever is still in force once the load has ended: every
// link healed, every paused node resumed, every node that should run
// restarted (a spare only if it was started).
func (r *chaosRun) recoverAll(ctx context.Context) {
	r.c.HealAll()
	r.mu.Lock()
	paused := r.paused
	r.paused = map[string]bool{}
	r.mu.Unlock()
	for id := range paused {
		_ = r.c.Resume(id)
		r.event(-1, "recover", id, "resumed after the load")
	}
	for _, n := range r.c.Nodes() {
		if (!n.Spare || n.Output() != "") && !n.Alive() {
			r.restart(ctx, -1, n.ID)
		}
	}
}

// sample reads every running node's status, records it, and records each
// group's leader changes: the leader of the highest term any node reports.
func (r *chaosRun) sample() {
	at := time.Since(r.start)
	type lead struct {
		id   string
		term uint64
	}
	best := map[multiraft.GroupID]lead{}
	var got []StatusSample
	for _, n := range r.c.Nodes() {
		r.mu.Lock()
		frozen := r.paused[n.ID]
		r.mu.Unlock()
		if frozen || !n.Alive() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		resp, err := r.c.Admin(ctx, n.ID, multiraft.AdminRequest{Op: "status"})
		cancel()
		if err != nil {
			continue
		}
		for _, gs := range resp.Groups {
			g := multiraft.GroupID(gs.Group)
			got = append(got, StatusSample{At: at, Node: n.ID, Group: g, Role: gs.Role, Term: gs.Term, Leader: gs.Leader,
				Commit: gs.Commit, Applied: gs.Applied, Last: gs.LastIndex})
			if gs.Role == "Leader" && gs.Term >= best[g].term {
				best[g] = lead{n.ID, gs.Term}
			}
		}
	}
	r.mu.Lock()
	r.samples = append(r.samples, got...)
	var changes []string
	first := map[string]bool{}
	var groups []multiraft.GroupID
	for g := range best {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	for _, g := range groups {
		l := best[g]
		cur := [2]string{l.id, fmt.Sprint(l.term)}
		if prev, ok := r.leaders[g]; !ok || prev != cur {
			r.leaders[g] = cur
			ch := fmt.Sprintf("%d|%s|%d", g, l.id, l.term)
			changes = append(changes, ch)
			first[ch] = !ok
		}
	}
	r.mu.Unlock()
	for _, ch := range changes {
		parts := strings.Split(ch, "|")
		kind := "leader"
		if first[ch] {
			kind = "first-leader"
		}
		r.event(-1, kind, parts[1], "leads group %s in term %s", parts[0], parts[2])
	}
}

// converge waits until every group has one leader that every running member
// follows in the same term, with every member's applied index at the
// leader's commit index.
func (r *chaosRun) converge(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for {
		why := r.converged(ctx)
		if why == "" {
			r.event(-1, "converged", "", "every group has one leader, followed and applied by every running member")
			return nil
		}
		last = why
		if time.Now().After(deadline) {
			return fmt.Errorf("lab: not converged within %s: %s", timeout, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// converged returns "" when the cluster has converged, or why not.
func (r *chaosRun) converged(ctx context.Context) string {
	type view struct {
		node string
		gs   multiraft.GroupStatus
	}
	byGroup := map[multiraft.GroupID][]view{}
	for _, n := range r.c.Nodes() {
		if !n.Alive() {
			if !n.Spare || n.Output() != "" {
				return fmt.Sprintf("%s is not running", n.ID)
			}
			continue
		}
		actx, cancel := context.WithTimeout(ctx, time.Second)
		resp, err := r.c.Admin(actx, n.ID, multiraft.AdminRequest{Op: "status"})
		cancel()
		if err != nil {
			return fmt.Sprintf("%s: status: %v", n.ID, err)
		}
		for _, gs := range resp.Groups {
			byGroup[multiraft.GroupID(gs.Group)] = append(byGroup[multiraft.GroupID(gs.Group)], view{n.ID, gs})
		}
	}
	for _, g := range r.c.Groups() {
		vs := byGroup[g]
		var leader *view
		for i := range vs {
			if vs[i].gs.Role == "Leader" && (leader == nil || vs[i].gs.Term > leader.gs.Term) {
				leader = &vs[i]
			}
		}
		if leader == nil {
			return fmt.Sprintf("group %d has no leader", g)
		}
		for _, v := range vs {
			if v.gs.Removed {
				continue
			}
			if v.node != leader.node && (v.gs.Leader != leader.node || v.gs.Term != leader.gs.Term) {
				return fmt.Sprintf("group %d: %s follows %q in term %d, the leader is %s in term %d", g, v.node, v.gs.Leader, v.gs.Term, leader.node, leader.gs.Term)
			}
			if v.gs.Applied < leader.gs.Commit {
				return fmt.Sprintf("group %d: %s applied %d of the leader's commit %d", g, v.node, v.gs.Applied, leader.gs.Commit)
			}
		}
	}
	return ""
}

// WriteArtifacts writes a run's evidence into dir:
//
//	chaos.json      the configuration, the schedule, every event and status
//	                sample, the load's result (with every unknown operation's
//	                trace), the verdict, the final state and each node's usage
//	history.txt     the client history, re-checkable with `go run ./cmd/lincheck`;
//	                its header names the seed, the schedule and every event at its
//	                position in the history
//	schedule.json   the schedule alone, to replay it (dkvlab -schedule)
//	nodes/<id>.log  every node's output, all its processes in order
//	links/<a>-<b>.log  each link proxy's activity
func WriteArtifacts(dir string, res *ChaosResult) error {
	for _, d := range []string{dir, filepath.Join(dir, "nodes"), filepath.Join(dir, "links")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	js, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "chaos.json"), js, 0o644); err != nil {
		return err
	}
	sched, err := json.MarshalIndent(res.Schedule, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "schedule.json"), sched, 0o644); err != nil {
		return err
	}
	var h strings.Builder
	fmt.Fprintf(&h, "# chaos run: seed=%d schedule-seed faults=%d nodes=%d mode=%s\n", res.Config.Seed, len(res.Schedule), res.Config.Cluster.Nodes, res.Config.Cluster.Mode)
	fmt.Fprintf(&h, "# workload: clients=%d keys=%d read=%d%% delete=%d%% seed=%d duration=%s warmup=%s\n",
		res.Config.Load.Clients, res.Config.Load.Keys, res.Config.Load.ReadPct, res.Config.Load.DeletePct, res.Config.Load.Seed, res.Config.Load.Duration, res.Config.Load.Warmup)
	for i, f := range res.Schedule {
		fmt.Fprintf(&h, "# fault %d: %s\n", i, f)
	}
	for _, e := range res.Events {
		fmt.Fprintf(&h, "# event pos=%d at=%s %s %s %s\n", e.Pos, e.At.Round(time.Millisecond), e.Kind, e.Node, e.Detail)
	}
	fmt.Fprintf(&h, "# verdict: linearizable=%v unchecked=%v converged=%v %s\n", res.Check.Linearizable, res.Check.Unchecked, res.Converged, res.Check.Reason)
	h.WriteString(res.History.String())
	if err := os.WriteFile(filepath.Join(dir, "history.txt"), []byte(h.String()), 0o644); err != nil {
		return err
	}
	for id, out := range res.Logs {
		if err := os.WriteFile(filepath.Join(dir, "nodes", id+".log"), []byte(out), 0o644); err != nil {
			return err
		}
	}
	for k, out := range res.Links {
		if err := os.WriteFile(filepath.Join(dir, "links", k+".log"), []byte(out), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// ReadSchedule reads a schedule written by WriteArtifacts (schedule.json), or
// the schedule of a whole chaos.json.
func ReadSchedule(path string) ([]Fault, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s []Fault
	if err := json.Unmarshal(b, &s); err == nil {
		return s, nil
	}
	var res struct {
		Schedule []Fault `json:"schedule"`
	}
	if err := json.Unmarshal(b, &res); err != nil {
		return nil, fmt.Errorf("lab: %s holds neither a schedule nor a chaos result: %w", path, err)
	}
	return res.Schedule, nil
}
