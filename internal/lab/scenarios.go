package lab

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/adivishall/quorum/internal/bench"
)

// Scenario returns the named experiment built on base (its cluster and load).
//
//	steady           the load alone
//	leader-kill      SIGKILL group 0's leader at 40% of the run; restart it 3 s later
//	rolling-restart  at 30% of the run, stop (SIGTERM) and restart every node in turn,
//	                 each caught up before the next
//	membership       one spare node; at 30% it joins group 0 as a learner, catches up
//	                 and is promoted by joint consensus
//	snapshots        the load with a snapshot every 100 entries (a run at 50 ops/s
//	                 with half writes appends about 600 — a coarser interval
//	                 would take none)
func Scenario(name string, base Experiment) (Experiment, error) {
	e := base
	e.Name = name
	at := func(frac float64) time.Duration {
		return e.Load.Warmup + time.Duration(frac*float64(e.Load.Duration))
	}
	switch name {
	case "steady":
	case "leader-kill":
		e.Actions = []Action{{At: at(0.4), Kind: "kill-leader", Group: 0, RestartAfter: 3 * time.Second}}
	case "rolling-restart":
		e.Actions = []Action{{At: at(0.3), Kind: "rolling-restart", Group: 0}}
	case "membership":
		e.Cluster.Spares = 1
		e.Actions = []Action{{At: at(0.3), Kind: "add-member", Group: 0}}
	case "snapshots":
		e.Cluster.SnapshotEvery = 100
	default:
		return e, fmt.Errorf("lab: unknown scenario %q", name)
	}
	e.Name = fmt.Sprintf("%s/%s-%dn", name, e.Cluster.Mode, e.Cluster.Nodes)
	if e.Cluster.Mode == "cluster" {
		e.Name += fmt.Sprintf("-%dg", e.Cluster.Shards)
	}
	e.Name += fmt.Sprintf("/r%d", e.Load.ReadPct)
	if e.Load.Rate > 0 {
		e.Name += fmt.Sprintf("-rate%.0f", e.Load.Rate)
	}
	return e, nil
}

// Suite returns a named matrix of experiments.
//
//	report  the matrix docs/CLUSTER_BENCHMARKS.md reports: 1, 3 and 5 nodes
//	        (one group) under read-heavy, mixed and write-heavy closed-loop
//	        load; 3 nodes with 1, 4 and 16 groups; and on 3 nodes under an
//	        open-loop faultRate (below the cluster's capacity, so latency is
//	        service time, not a backlog), a leader kill, a rolling restart, a
//	        membership change and frequent snapshots.
func Suite(name string, base Experiment, faultRate float64) ([]Experiment, error) {
	if name != "report" {
		return nil, fmt.Errorf("lab: unknown suite %q", name)
	}
	var out []Experiment
	add := func(scenario string, mutate func(*Experiment)) error {
		b := base
		mutate(&b)
		e, err := Scenario(scenario, b)
		if err != nil {
			return err
		}
		out = append(out, e)
		return nil
	}
	for _, nodes := range []int{1, 3, 5} {
		for _, read := range []int{95, 50, 5} {
			nodes, read := nodes, read
			if err := add("steady", func(e *Experiment) {
				e.Cluster.Mode, e.Cluster.Nodes, e.Load.ReadPct, e.Load.Rate = "raft", nodes, read, 0
			}); err != nil {
				return nil, err
			}
		}
	}
	for _, groups := range []int{1, 4, 16} {
		groups := groups
		if err := add("steady", func(e *Experiment) {
			e.Cluster.Mode, e.Cluster.Nodes, e.Cluster.Shards, e.Cluster.RF, e.Load.ReadPct, e.Load.Rate = "cluster", 3, groups, 3, 50, 0
		}); err != nil {
			return nil, err
		}
	}
	if faultRate <= 0 {
		return nil, fmt.Errorf("lab: the report suite needs a positive fault rate")
	}
	for _, scenario := range []string{"steady", "leader-kill", "rolling-restart", "membership", "snapshots"} {
		if err := add(scenario, func(e *Experiment) {
			e.Cluster.Mode, e.Cluster.Nodes, e.Load.ReadPct, e.Load.Rate = "raft", 3, 50, faultRate
		}); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Summary is one configuration's numbers across its runs.
type Summary struct {
	Name     string `json:"name"`
	Runs     int    `json:"runs"`
	Failed   int    `json:"failed_runs"` // runs that returned no result
	OKPerSec Stat   `json:"ok_per_sec"`
	// Ops are the operations due in the window, every outcome; SuccessRate
	// the fraction that ended ok or not_found.
	Ops         Stat `json:"ops"`
	SuccessRate Stat `json:"success_rate"`
	// The Get/Put percentiles are of the successes only; the All ones are of
	// every operation, each to its definite answer or to the client giving
	// up, and Excluded counts what the success-only ones leave out
	// (docs/LOAD_TESTING.md). A failure scenario's tail is in the All ones.
	GetP50Us      Stat `json:"get_p50_us"`
	GetP95Us      Stat `json:"get_p95_us"`
	GetP99Us      Stat `json:"get_p99_us"`
	PutP50Us      Stat `json:"put_p50_us"`
	PutP95Us      Stat `json:"put_p95_us"`
	PutP99Us      Stat `json:"put_p99_us"`
	AllP50Us      Stat `json:"all_outcomes_p50_us"`
	AllP95Us      Stat `json:"all_outcomes_p95_us"`
	AllP99Us      Stat `json:"all_outcomes_p99_us"`
	Excluded      Stat `json:"excluded_from_latency"`
	Unknown       Stat `json:"unknown_ops"`
	Refused       Stat `json:"refused_ops"`
	LongestOutage Stat `json:"longest_outage_ms"`
	// UnknownCauses classifies every traced unknown outcome of every run
	// (load.Config.Trace; ClassifyUnknown).
	UnknownCauses map[string]int `json:"unknown_causes,omitempty"`
	// Settle is each action's settle time in milliseconds (elections for
	// kill-leader, catch-up for restarts and additions), across runs.
	Settle        map[string]Stat `json:"settle_ms,omitempty"`
	NodeCPU       Stat            `json:"node_cpu_per_sec"` // mean over the nodes of each run
	NodeMaxRSSMiB Stat            `json:"node_max_rss_mib"`
	GeneratorCPU  Stat            `json:"generator_cpu_per_sec"`
	MaxLag        Stat            `json:"max_follower_lag"`
}

// Report is a dkvlab output: the environment, every run and the summaries.
type Report struct {
	Env       bench.Environment `json:"env"`
	Runs      int               `json:"runs_per_configuration"`
	Summaries []Summary         `json:"summaries"`
	Results   []*RunResult      `json:"results"`
}

// Add summarizes one configuration's runs into the report.
func (r *Report) Add(e Experiment, results []*RunResult) {
	s := Summary{Name: e.Name, Runs: len(results), Failed: r.Runs - len(results), Settle: map[string]Stat{}}
	var ok, g50, g95, g99, p50, p95, p99, a50, a95, a99, excl, nops, rate, unk, ref, outage, cpu, rss, gen, lag []float64
	settle := map[string][]float64{}
	for _, res := range results {
		l := res.Load
		ok = append(ok, l.OKPerSec)
		var total int64
		for _, n := range l.Classes {
			total += n
		}
		nops = append(nops, float64(total))
		if total > 0 {
			rate = append(rate, float64(l.Classes["ok"]+l.Classes["not_found"])/float64(total))
		}
		if o, found := l.Ops["get"]; found && o.Latency.Count > 0 {
			g50, g95, g99 = append(g50, o.Latency.P50), append(g95, o.Latency.P95), append(g99, o.Latency.P99)
		}
		if o, found := l.Ops["put"]; found && o.Latency.Count > 0 {
			p50, p95, p99 = append(p50, o.Latency.P50), append(p95, o.Latency.P95), append(p99, o.Latency.P99)
		}
		if l.AllOutcomes.Count > 0 {
			a50, a95, a99 = append(a50, l.AllOutcomes.P50), append(a95, l.AllOutcomes.P95), append(a99, l.AllOutcomes.P99)
		}
		excl = append(excl, float64(l.Excluded))
		for _, t := range l.Traces {
			if t.Class == "unknown" {
				if s.UnknownCauses == nil {
					s.UnknownCauses = map[string]int{}
				}
				s.UnknownCauses[ClassifyUnknown(t)]++
			}
		}
		unk = append(unk, float64(l.Classes["unknown"]))
		ref = append(ref, float64(l.Classes["refused"]))
		outage = append(outage, float64(l.LongestOutage.Milliseconds()))
		gen = append(gen, l.GeneratorCPU.Seconds()/l.Elapsed.Seconds())
		lag = append(lag, res.MaxFollowerLag)
		var c, m float64
		for _, u := range res.Nodes {
			c += u.CPUPerSec
			m = max(m, u.MaxRSSBytes)
		}
		if len(res.Nodes) > 0 {
			cpu = append(cpu, c/float64(len(res.Nodes)))
		}
		rss = append(rss, m/(1<<20))
		for _, a := range res.Actions {
			if a.Err == "" {
				k := a.Action.Kind
				if a.Action.Kind == "rolling-restart" || a.Action.Kind == "restart" {
					k = "restart"
				}
				settle[k] = append(settle[k], float64(a.Settle.Microseconds())/1e3)
			}
		}
	}
	s.OKPerSec, s.GetP50Us, s.GetP99Us, s.PutP50Us, s.PutP99Us = Summarize(ok), Summarize(g50), Summarize(g99), Summarize(p50), Summarize(p99)
	s.GetP95Us, s.PutP95Us, s.AllP50Us, s.AllP95Us, s.AllP99Us = Summarize(g95), Summarize(p95), Summarize(a50), Summarize(a95), Summarize(a99)
	s.Ops, s.SuccessRate, s.Excluded = Summarize(nops), Summarize(rate), Summarize(excl)
	s.Unknown, s.Refused, s.LongestOutage = Summarize(unk), Summarize(ref), Summarize(outage)
	s.NodeCPU, s.NodeMaxRSSMiB, s.GeneratorCPU, s.MaxLag = Summarize(cpu), Summarize(rss), Summarize(gen), Summarize(lag)
	for k, v := range settle {
		s.Settle[k] = Summarize(v)
	}
	r.Summaries = append(r.Summaries, s)
	r.Results = append(r.Results, results...)
}

// PrintReport writes the summaries as a table: medians over each
// configuration's runs. The latencies are of every outcome (µs), so a failure
// scenario's slowest operations are in them; the success-only percentiles
// and every other figure are in the JSON.
func PrintReport(w io.Writer, r *Report) {
	fmt.Fprintf(w, "\n%-44s %4s %9s %8s %5s %5s %9s %9s %9s %9s %s\n", "configuration", "runs", "ok/s", "success", "unk", "ref",
		"p50 all", "p95 all", "p99 all", "outage ms", "settle ms (median)")
	for _, s := range r.Summaries {
		var keys []string
		for k := range s.Settle {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		settle := ""
		for _, k := range keys {
			settle += fmt.Sprintf("%s %.0f ", k, s.Settle[k].Median)
		}
		fmt.Fprintf(w, "%-44s %4d %9.0f %7.2f%% %5.0f %5.0f %9.0f %9.0f %9.0f %9.0f %s\n", s.Name, s.Runs, s.OKPerSec.Median,
			100*s.SuccessRate.Median, s.Unknown.Median, s.Refused.Median, s.AllP50Us.Median, s.AllP95Us.Median, s.AllP99Us.Median,
			s.LongestOutage.Median, settle)
		if len(s.UnknownCauses) > 0 {
			var causes []string
			for k, n := range s.UnknownCauses {
				causes = append(causes, fmt.Sprintf("%s %d", k, n))
			}
			sort.Strings(causes)
			fmt.Fprintf(w, "%-44s unknown outcomes by cause, all runs: %s\n", "", strings.Join(causes, ", "))
		}
	}
}
