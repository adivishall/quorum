package raftsim

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// The bounded exhaustive crash matrix (Phase 11, docs/CRASH_RECOVERY.md §7).
//
// A scenario is an explicit event script. RunCrashMatrix first runs it once,
// recording every crash point every node reaches — each driver point of the
// persist → send → advance → apply cycle and each I/O boundary of the durable log
// — and then re-runs it once per (point, crash mode): the node dies exactly there,
// is restarted immediately through the real startup path, the rest of the
// scenario runs, and the cluster is stabilized and must converge (INV-F3). Every
// continuous invariant runs throughout, and every restart is checked against the
// independent record of what the node had persisted (INV-F2, INV-CR1..3). The
// result is one machine-readable row per crash: the durable state before the
// crash, the Save it interrupted (if any), what recovery produced, how much the
// new incarnation re-applied, and the outcome. A failing row names its exact
// crash point, mode and seed, and the CrashAt event that reproduces it.

// CrashMode is how a node dies at a crash point: its process (the disk keeps
// every accepted byte) or, with Power, the machine (only fsynced bytes survive,
// plus at most Torn bytes of the un-synced tail).
type CrashMode struct {
	Power bool
	Torn  int
}

func (m CrashMode) String() string {
	if m.Power {
		return fmt.Sprintf("power(torn<=%d)", m.Torn)
	}
	return "process"
}

// DefaultCrashModes are a process crash, a clean power loss, and a power loss
// keeping a small torn tail — enough to cut inside a record.
var DefaultCrashModes = []CrashMode{{}, {Power: true}, {Power: true, Torn: 13}}

// DurableState summarises a node's durable Raft state for a report.
type DurableState struct {
	Term     uint64 `json:"term"`
	Vote     string `json:"vote"`
	Commit   uint64 `json:"commit"`
	Entries  int    `json:"entries"`
	LastTerm uint64 `json:"last_term"`
}

func (d DurableState) String() string {
	return fmt.Sprintf("t%d/v%q/c%d/n%d@t%d", d.Term, d.Vote, d.Commit, d.Entries, d.LastTerm)
}

// MatrixRow is one crash of the matrix and its outcome.
type MatrixRow struct {
	Node  string `json:"node"`
	Point string `json:"point"`
	Nth   int    `json:"nth"`
	Mode  string `json:"mode"`
	Event string `json:"event"` // the CrashAt event that reproduces this crash, armed first

	Fired     bool `json:"fired"`
	CrashStep int  `json:"crash_step"`

	Persisted   DurableState `json:"persisted"`   // the last completed Save's state, before the crash
	Interrupted string       `json:"interrupted"` // the Save in progress at the crash, or "none"
	Recovered   DurableState `json:"recovered"`   // what the restart recovered
	Reapplied   uint64       `json:"reapplied"`   // entries the dead incarnation had applied, all re-applied by the new one
	Restarts    int          `json:"restarts"`    // boots needed (a crash during recovery costs one more)

	Violation string `json:"violation,omitempty"`
	Converged bool   `json:"converged"`
	TraceHash string `json:"trace_hash"`
}

// OK reports whether the crash was survived: it fired, no invariant broke, and
// the cluster converged afterwards.
func (r MatrixRow) OK() bool { return r.Fired && r.Violation == "" && r.Converged }

// MatrixReport is the whole matrix.
type MatrixReport struct {
	Nodes    int         `json:"nodes"`
	Seed     int64       `json:"seed"`
	Scenario int         `json:"scenario_events"`
	BaseHash string      `json:"base_trace_hash"`
	Rows     []MatrixRow `json:"rows"`
}

// RunCrashMatrix runs scenario once to enumerate its crash points, then once
// per point and mode with the crash armed. It is a pure function of its inputs.
func RunCrashMatrix(cfg Config, scenario []Event, modes []CrashMode) (*MatrixReport, error) {
	base, err := New(cfg)
	if err != nil {
		return nil, err
	}
	base.StartRecordingPoints()
	for _, e := range scenario {
		base.Apply(e)
	}
	if v := base.Violation(); v != nil {
		return nil, fmt.Errorf("raftsim: the scenario itself violates %v", v)
	}
	rep := &MatrixReport{Nodes: cfg.Nodes, Seed: cfg.Seed, Scenario: len(scenario), BaseHash: base.Trace().Hash()}
	for _, hit := range base.Points() {
		for _, mode := range modes {
			rep.Rows = append(rep.Rows, runCrash(cfg, scenario, hit, mode))
		}
	}
	return rep, nil
}

// runCrash is one cell of the matrix.
func runCrash(cfg Config, scenario []Event, hit PointHit, mode CrashMode) MatrixRow {
	arm := Event{Kind: CrashAt, Node: hit.Node, Point: hit.Point, Nth: hit.Nth, Power: mode.Power, N: mode.Torn}
	row := MatrixRow{Node: string(hit.Node), Point: hit.Point, Nth: hit.Nth, Mode: mode.String(), Event: arm.String()}
	c, err := New(cfg)
	if err != nil {
		row.Violation = err.Error()
		return row
	}
	c.Apply(arm)
	n := c.nodes[hit.Node]
	restarted := false
	for _, e := range scenario {
		if c.viol != nil {
			break
		}
		c.Apply(e)
		if restarted || c.stats.PointCrashes == 0 {
			continue
		}
		// The crash fired during that event. Record what the node had persisted
		// and what it was in the middle of, then bring it back at once.
		row.Fired, row.CrashStep = true, c.step
		row.Persisted = summarize(n.shadow.persisted.hs, n.shadow.persisted.entries)
		row.Interrupted = "none"
		if p := n.shadow.pending; p != nil {
			row.Interrupted = fmt.Sprintf("%d entries, hardstate=%v", len(p.entries), p.hs != nil)
		}
		row.Reapplied = n.prevApplied
		for row.Restarts < 3 && !n.up && c.viol == nil {
			c.Apply(Event{Kind: Restart, Node: hit.Node})
			row.Restarts++
		}
		if n.lastRecovered != nil && n.inc > 1 {
			rec := n.lastRecovered
			row.Recovered = summarize(rec.HardState, rec.Entries)
		}
		restarted = true
	}
	if row.Fired && c.viol == nil {
		c.Stabilize(400)
	}
	if v := c.viol; v != nil {
		row.Violation = v.Error()
	}
	row.Converged = c.viol == nil && c.settled()
	row.TraceHash = c.trace.Hash()
	return row
}

func summarize(hs raftlogHardState, entries []raftEntry) DurableState {
	d := DurableState{Term: hs.Term, Vote: string(hs.Vote), Commit: hs.Commit, Entries: len(entries)}
	if n := len(entries); n > 0 {
		d.LastTerm = entries[n-1].Term
	}
	return d
}

// Failures returns the rows that were not survived.
func (r *MatrixReport) Failures() []MatrixRow {
	var out []MatrixRow
	for _, row := range r.Rows {
		if !row.OK() {
			out = append(out, row)
		}
	}
	return out
}

// Coverage counts the rows per crash point kind and per mode.
func (r *MatrixReport) Coverage() (byPoint, byMode map[string]int) {
	byPoint, byMode = map[string]int{}, map[string]int{}
	for _, row := range r.Rows {
		byPoint[row.Point]++
		byMode[row.Mode]++
	}
	return byPoint, byMode
}

// Summary is a few lines for a test log.
func (r *MatrixReport) Summary() string {
	byPoint, byMode := r.Coverage()
	points := make([]string, 0, len(byPoint))
	for p := range byPoint {
		points = append(points, p)
	}
	sort.Strings(points)
	var b strings.Builder
	fmt.Fprintf(&b, "crash matrix: nodes=%d seed=%d scenario=%d events, %d crashes, %d failures\n",
		r.Nodes, r.Seed, r.Scenario, len(r.Rows), len(r.Failures()))
	for _, p := range points {
		fmt.Fprintf(&b, "  %-18s %4d crashes\n", p, byPoint[p])
	}
	modes := make([]string, 0, len(byMode))
	for m := range byMode {
		modes = append(modes, m)
	}
	sort.Strings(modes)
	for _, m := range modes {
		fmt.Fprintf(&b, "  mode %-18s %4d crashes\n", m, byMode[m])
	}
	return b.String()
}

// Text renders every row, one per line, for a log or a file.
func (r *MatrixReport) Text() string {
	var b strings.Builder
	for _, row := range r.Rows {
		result := "ok"
		switch {
		case !row.Fired:
			result = "DID-NOT-FIRE"
		case row.Violation != "":
			result = "VIOLATION " + row.Violation
		case !row.Converged:
			result = "NOT-CONVERGED"
		}
		fmt.Fprintf(&b, "%s %s#%d %s step=%d persisted=%s interrupted=[%s] recovered=%s reapplied=%d restarts=%d %s  [%s]\n",
			row.Node, row.Point, row.Nth, row.Mode, row.CrashStep, row.Persisted, row.Interrupted, row.Recovered, row.Reapplied, row.Restarts, result, row.Event)
	}
	return b.String()
}

// JSON renders the report.
func (r *MatrixReport) JSON() ([]byte, error) { return json.MarshalIndent(r, "", "  ") }
