package lab

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/adivishall/quorum/internal/load"
	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/multiraft"
)

// Action is something a scenario does to the cluster while the load runs.
type Action struct {
	At   time.Duration `json:"at"`   // after the load starts (warmup included)
	Kind string        `json:"kind"` // kill-leader, restart, rolling-restart, add-member, snapshot
	// Group the action concerns (kill-leader, add-member, snapshot).
	Group multiraft.GroupID `json:"group"`
	// After is how long a restart waits after the kill (kill-leader) — 0 leaves
	// the node down.
	RestartAfter time.Duration `json:"restart_after,omitempty"`
}

// Experiment is one scenario: a cluster, a workload, and actions.
type Experiment struct {
	Name    string        `json:"name"`
	Cluster ClusterConfig `json:"cluster"`
	Load    load.Config   `json:"load"`
	Actions []Action      `json:"actions,omitempty"`
}

// ActionResult is what one action did and how long it took to settle.
type ActionResult struct {
	Action Action        `json:"action"`
	Node   string        `json:"node,omitempty"` // the node acted on
	Start  time.Duration `json:"start"`          // after the load started
	// Settle is the action's own measurement: for kill-leader, the time from
	// the SIGKILL until a new leader of the group was observed (polled every
	// 10 ms through the admin protocol); for restart and add-member, until the
	// node had caught up to the leader's commit index at the time; for
	// snapshot, the admin call.
	Settle time.Duration `json:"settle"`
	Err    string        `json:"error,omitempty"`
	Detail string        `json:"detail,omitempty"`
}

// NodeUsage is one node's resource use over the measured run, from its own
// /metrics: CPU seconds per second, peak RSS, Raft log growth, elections.
type NodeUsage struct {
	CPUPerSec      float64 `json:"cpu_per_sec"`
	MaxRSSBytes    float64 `json:"max_rss_bytes"`
	HeapInuseBytes float64 `json:"heap_inuse_bytes"`
	Goroutines     float64 `json:"goroutines"`
	LogBytes       float64 `json:"log_bytes"`
	ElectionsWon   float64 `json:"elections_won"`
	// SnapshotsCreated and SnapshotsInstalled count what the node did over
	// the run: a snapshot scenario that took none measured nothing.
	SnapshotsCreated   float64 `json:"snapshots_created"`
	SnapshotsInstalled float64 `json:"snapshots_installed"`
	Persists           float64 `json:"persists"`
	PersistMeanUs      float64 `json:"persist_mean_us"`
	CommitMeanUs       float64 `json:"commit_mean_us"`
	// CommitAdvance is how far the node's commit index (summed over its
	// groups) moved over the run: the entries committed, no-ops and
	// configuration entries included. AppendFramesSent and AppendBytesSent
	// are the AppendEntries frames and bytes it wrote — a leader's
	// replication traffic, heartbeats included, to every follower.
	CommitAdvance    float64 `json:"commit_advance"`
	AppendFramesSent float64 `json:"append_frames_sent"`
	AppendBytesSent  float64 `json:"append_bytes_sent"`
	Restarted        bool    `json:"restarted"` // counters restarted with the process: deltas are from zero
}

// RunResult is one run of an experiment.
type RunResult struct {
	Experiment Experiment           `json:"experiment"`
	Load       *load.Result         `json:"load"`
	Actions    []ActionResult       `json:"actions,omitempty"`
	Nodes      map[string]NodeUsage `json:"nodes"`
	// MaxFollowerLag is the largest follower lag any leader reported in the
	// scrapes taken every 500 ms during the run.
	MaxFollowerLag float64 `json:"max_follower_lag"`
}

// Run launches the experiment's cluster, runs its load and actions, and tears
// the cluster down.
func Run(ctx context.Context, e Experiment) (*RunResult, error) {
	c, err := Start(ctx, e.Cluster)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	lc := e.Load
	lc.Endpoints = c.Endpoints()
	if r := c.Route(); r != nil {
		lc.Route = r.GroupOf
		lc.RouteSpec = fmt.Sprintf("shards=%d rf=%d", e.Cluster.Shards, e.Cluster.RF)
	}
	lc.Groups = c.Groups()
	before := c.ScrapeAll()
	restarted := map[string]bool{}

	type loadOut struct {
		res *load.Result
		err error
	}
	loadDone := make(chan loadOut, 1)
	loadStart := time.Now()
	go func() {
		r, err := load.Run(ctx, lc)
		loadDone <- loadOut{r, err}
	}()

	// Sample the leaders' follower lag while the load runs.
	var maxLag float64
	stopSampling := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-t.C:
				for _, ss := range c.ScrapeAll() {
					for _, s := range ss.All("dkv_raft_follower_lag_entries") {
						maxLag = math.Max(maxLag, s.Value)
					}
				}
			}
		}
	}()

	var results []ActionResult
	for _, a := range e.Actions {
		if d := time.Until(loadStart.Add(a.At)); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		}
		ar := c.act(ctx, a, time.Since(loadStart), restarted)
		results = append(results, ar...)
	}
	out := <-loadDone
	close(stopSampling)
	<-sampled
	if out.err != nil && out.res == nil {
		return nil, out.err
	}
	after := c.ScrapeAll()
	elapsed := out.res.Elapsed
	nodes := map[string]NodeUsage{}
	for id, ss := range after {
		nodes[id] = usage(before[id], ss, elapsed, restarted[id])
	}
	return &RunResult{Experiment: e, Load: out.res, Actions: results, Nodes: nodes, MaxFollowerLag: maxLag}, out.err
}

// act performs one action.
func (c *Cluster) act(ctx context.Context, a Action, at time.Duration, restarted map[string]bool) []ActionResult {
	res := ActionResult{Action: a, Start: at}
	switch a.Kind {
	case "kill-leader":
		leader, term, err := c.Leader(ctx, a.Group)
		if err != nil {
			res.Err = err.Error()
			return []ActionResult{res}
		}
		res.Node = leader
		start := time.Now()
		if err := c.Kill(leader); err != nil {
			res.Err = err.Error()
			return []ActionResult{res}
		}
		next, _, err := c.waitLeaderAfter(ctx, a.Group, leader, term, 30*time.Second)
		res.Settle = time.Since(start)
		if err != nil {
			res.Err = err.Error()
		} else {
			res.Detail = "new leader " + next
		}
		out := []ActionResult{res}
		if a.RestartAfter > 0 {
			time.Sleep(a.RestartAfter)
			out = append(out, c.restartAndCatchUp(ctx, leader, a.Group, restarted, at+time.Since(start)))
		}
		return out
	case "rolling-restart":
		var out []ActionResult
		for _, id := range c.running() {
			r := ActionResult{Action: a, Node: id, Start: at}
			if err := c.Stop(id); err != nil {
				r.Err = err.Error()
				out = append(out, r)
				continue
			}
			rr := c.restartAndCatchUp(ctx, id, a.Group, restarted, at)
			rr.Action = a
			out = append(out, rr)
		}
		return out
	case "add-member":
		spare := ""
		for _, n := range c.nodes {
			if n.Spare && !n.Alive() {
				spare = n.ID
				break
			}
		}
		if spare == "" {
			res.Err = "no spare node left"
			return []ActionResult{res}
		}
		res.Node = spare
		start := time.Now()
		if err := c.StartSpare(spare, []multiraft.GroupID{a.Group}); err != nil {
			res.Err = err.Error()
			return []ActionResult{res}
		}
		for _, op := range []string{"add-learner", "promote"} {
			if err := c.change(ctx, a.Group, op, spare); err != nil {
				res.Err = op + ": " + err.Error()
				return []ActionResult{res}
			}
			if op == "add-learner" {
				if err := c.caughtUp(ctx, spare, a.Group, 60*time.Second); err != nil {
					res.Err = err.Error()
					return []ActionResult{res}
				}
			}
		}
		res.Settle = time.Since(start)
		res.Detail = "added as learner, caught up, promoted"
		return []ActionResult{res}
	case "snapshot":
		leader, _, err := c.Leader(ctx, a.Group)
		if err != nil {
			res.Err = err.Error()
			return []ActionResult{res}
		}
		res.Node = leader
		start := time.Now()
		resp, err := c.Admin(ctx, leader, multiraft.AdminRequest{Op: "snapshot", Group: uint32(a.Group)})
		res.Settle = time.Since(start)
		if err != nil {
			res.Err = err.Error()
		} else if !resp.OK {
			res.Err = resp.Error
		}
		return []ActionResult{res}
	}
	res.Err = "unknown action " + a.Kind
	return []ActionResult{res}
}

// restartAndCatchUp restarts id and waits until it has applied the leader's
// commit index of group g as it was when the restart began.
func (c *Cluster) restartAndCatchUp(ctx context.Context, id string, g multiraft.GroupID, restarted map[string]bool, at time.Duration) ActionResult {
	r := ActionResult{Action: Action{Kind: "restart", Group: g}, Node: id, Start: at}
	start := time.Now()
	if err := c.Restart(id); err != nil {
		r.Err = err.Error()
		return r
	}
	restarted[id] = true
	if err := c.caughtUp(ctx, id, g, 60*time.Second); err != nil {
		r.Err = err.Error()
	}
	r.Settle = time.Since(start)
	return r
}

// caughtUp waits until node id has applied group g's leader's commit index as
// it stood when the wait began.
func (c *Cluster) caughtUp(ctx context.Context, id string, g multiraft.GroupID, timeout time.Duration) error {
	// Restarting a node can start an election (it may have led): wait for
	// the group to have a leader again rather than read a transient absence.
	leader, _, err := c.WaitLeader(ctx, g, "", timeout)
	if err != nil {
		return err
	}
	target := uint64(0)
	if resp, err := c.Admin(ctx, leader, multiraft.AdminRequest{Op: "status"}); err == nil {
		for _, gs := range resp.Groups {
			if multiraft.GroupID(gs.Group) == g {
				target = gs.Commit
			}
		}
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		actx, cancel := context.WithTimeout(ctx, time.Second)
		resp, err := c.Admin(actx, id, multiraft.AdminRequest{Op: "status"})
		cancel()
		if err == nil {
			for _, gs := range resp.Groups {
				if multiraft.GroupID(gs.Group) == g && gs.Applied >= target {
					return nil
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("lab: %s did not apply group %d's commit %d within %s", id, g, target, timeout)
}

// change runs one membership operation at whichever node leads group g.
func (c *Cluster) change(ctx context.Context, g multiraft.GroupID, op, id string) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		leader, _, err := c.Leader(ctx, g)
		if err == nil {
			req := multiraft.AdminRequest{Op: op, Group: uint32(g), ID: id}
			if op == "add-learner" {
				req.Addr = c.byID[id].Addr
			}
			actx, cancel := context.WithTimeout(ctx, 15*time.Second)
			resp, err := c.Admin(actx, leader, req)
			cancel()
			if err == nil && resp.OK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("lab: %s %s in group %d did not complete", op, id, g)
}

// usage computes a node's resource use between two scrapes.
func usage(before, after metrics.Samples, elapsed time.Duration, restarted bool) NodeUsage {
	delta := func(name string, match ...string) float64 {
		a := after.Sum(name, match...)
		if restarted || before == nil {
			return a
		}
		return a - before.Sum(name, match...)
	}
	u := NodeUsage{Restarted: restarted}
	u.CPUPerSec = delta("process_cpu_seconds_total") / elapsed.Seconds()
	u.MaxRSSBytes, _ = after.Get("process_max_rss_bytes")
	u.HeapInuseBytes, _ = after.Get("go_heap_inuse_bytes")
	u.Goroutines, _ = after.Get("go_goroutines")
	u.LogBytes = after.Sum("dkv_raft_log_bytes")
	u.ElectionsWon = delta("dkv_raft_elections_won_total")
	u.SnapshotsCreated = delta("dkv_raft_snapshots_created_total")
	u.SnapshotsInstalled = delta("dkv_raft_snapshots_installed_total")
	u.Persists = delta("dkv_raft_persist_seconds_count")
	if u.Persists > 0 {
		u.PersistMeanUs = delta("dkv_raft_persist_seconds_sum") / u.Persists * 1e6
	}
	if n := delta("dkv_raft_commit_seconds_count"); n > 0 {
		u.CommitMeanUs = delta("dkv_raft_commit_seconds_sum") / n * 1e6
	}
	// The commit index is durable, so it is an index, not a counter: a
	// restart does not reset it.
	u.CommitAdvance = after.Sum("dkv_raft_commit_index") - before.Sum("dkv_raft_commit_index")
	u.AppendFramesSent = delta("dkv_transport_frames_sent_total", "kind", "append_entries")
	u.AppendBytesSent = delta("dkv_transport_bytes_sent_total", "kind", "append_entries")
	return u
}

// Stat summarizes one number across runs.
type Stat struct {
	N      int     `json:"n"`
	Median float64 `json:"median"`
	P95    float64 `json:"p95,omitempty"` // only with at least 10 runs
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	// Spread is (max − min) / median.
	Spread float64 `json:"spread"`
}

// Summarize computes a Stat; the median is the middle value (the mean of the
// two middle ones for an even count).
func Summarize(values []float64) Stat {
	if len(values) == 0 {
		return Stat{}
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)
	n := len(v)
	med := v[n/2]
	if n%2 == 0 {
		med = (v[n/2-1] + v[n/2]) / 2
	}
	s := Stat{N: n, Median: med, Min: v[0], Max: v[n-1]}
	if med != 0 {
		s.Spread = (s.Max - s.Min) / med
	}
	if n >= 10 {
		s.P95 = v[int(math.Ceil(0.95*float64(n)))-1]
	}
	return s
}
