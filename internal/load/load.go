// Package load drives a real Quorum cluster with a client workload and
// measures what the clients observe (#2, docs/LOAD_TESTING.md): throughput,
// exact latency percentiles per operation, outcomes by class, and a timeline
// from which client-visible outages are read.
//
// It is a client of the ordinary protocol (internal/kv) and nothing else: it
// measures the cluster as a user would, from outside. Two properties keep it
// from measuring itself:
//
//   - Open loop measures latency from each operation's intended start. The
//     schedule is fixed in advance (operation k is due at start + k/rate); if
//     every client is busy when k falls due, k waits, and its wait is part of
//     its latency. A stalled cluster shows up as latency, not as operations
//     that were silently never issued (coordinated omission).
//   - Every simulated client has its own connections and its own sessions, so
//     no client serializes behind another's connection.
//
// Everything random — operation type, key, value — is drawn from a generator
// seeded by Config.Seed, so a run's workload is reproducible from its recorded
// configuration. Timing is real and is not.
package load

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/replication"
)

// Endpoint is one node's client port.
type Endpoint struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

// Config is one run.
type Config struct {
	Endpoints []Endpoint `json:"endpoints"`
	// Route is the cluster's key → group routing; nil means every key is group
	// 0 (the -raft deployment). RouteSpec records how it was built, for the
	// result.
	Route     func(key []byte) replication.GroupID `json:"-"`
	RouteSpec string                               `json:"route"`
	// Groups, if set, are registered by every client before the clock starts,
	// so no session registration falls inside a measurement.
	Groups []replication.GroupID `json:"groups,omitempty"`

	Clients  int           `json:"clients"`  // concurrent simulated clients
	Duration time.Duration `json:"duration"` // measured window
	Warmup   time.Duration `json:"warmup"`   // issued first, not recorded
	// Rate is the total target in operations per second; 0 runs closed loop:
	// each client issues its next operation when the previous one completes.
	Rate float64 `json:"rate"`

	ReadPct   int     `json:"read_pct"`   // GETs
	DeletePct int     `json:"delete_pct"` // DELETEs; the rest are PUTs
	Keys      int     `json:"keys"`
	KeyDist   string  `json:"key_dist"` // "uniform" or "zipf"
	ZipfS     float64 `json:"zipf_s"`   // > 1; used by "zipf"
	ValueSize int     `json:"value_size"`
	Seed      int64   `json:"seed"`

	// Anonymous sends requests with no identity: no session, one attempt at
	// one endpoint (an unknown anonymous write must not be retried —
	// docs/CLIENT_SEMANTICS.md §2).
	Anonymous      bool          `json:"anonymous"`
	AttemptTimeout time.Duration `json:"attempt_timeout"`
	MaxAttempts    int           `json:"max_attempts"`

	// Bucket is the timeline's resolution.
	Bucket time.Duration `json:"bucket"`
}

func (c *Config) defaults() error {
	if len(c.Endpoints) == 0 {
		return errors.New("load: no endpoints")
	}
	if c.Clients <= 0 {
		c.Clients = 1
	}
	if c.Duration <= 0 {
		return errors.New("load: duration must be positive")
	}
	if c.ReadPct < 0 || c.DeletePct < 0 || c.ReadPct+c.DeletePct > 100 {
		return fmt.Errorf("load: read %d%% + delete %d%% must be within 0..100", c.ReadPct, c.DeletePct)
	}
	if c.Keys <= 0 {
		c.Keys = 1000
	}
	switch c.KeyDist {
	case "", "uniform":
		c.KeyDist = "uniform"
	case "zipf":
		if c.ZipfS <= 1 {
			c.ZipfS = 1.1
		}
	default:
		return fmt.Errorf("load: unknown key distribution %q", c.KeyDist)
	}
	if c.ValueSize <= 0 {
		c.ValueSize = 100
	}
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = 2 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.Bucket <= 0 {
		c.Bucket = 100 * time.Millisecond
	}
	if c.Rate < 0 {
		return errors.New("load: rate must not be negative")
	}
	if c.Route == nil {
		c.Route = func([]byte) replication.GroupID { return 0 }
		if c.RouteSpec == "" {
			c.RouteSpec = "group 0"
		}
	}
	return nil
}

// Op is an operation type.
type Op int

const (
	Get Op = iota
	Put
	Delete
)

func (o Op) String() string { return [...]string{"get", "put", "delete"}[o] }

// Outcome classes, as a client must treat them (docs/CLIENT_SEMANTICS.md §6).
const (
	ClassOK       = "ok"        // the operation took effect (a GET found its value)
	ClassNotFound = "not_found" // a GET found no value: a definite answer
	ClassRefused  = "refused"   // a definite answer that the operation did not take effect
	ClassUnknown  = "unknown"   // the client cannot know whether it took effect
)

// task is one scheduled operation.
type task struct {
	seq      int64
	op       Op
	key      []byte
	intended time.Time // open loop: when it was due; closed loop: when it was issued
}

// workload draws operations deterministically from the seed.
type workload struct {
	rng   *rand.Rand
	zipf  *rand.Zipf
	cfg   *Config
	value []byte
}

func newWorkload(cfg *Config, seed int64) *workload {
	w := &workload{rng: rand.New(rand.NewSource(seed)), cfg: cfg}
	if cfg.KeyDist == "zipf" {
		w.zipf = rand.NewZipf(w.rng, cfg.ZipfS, 1, uint64(cfg.Keys-1))
	}
	return w
}

// next returns the next operation and key.
func (w *workload) next() (Op, []byte) {
	r := w.rng.Intn(100)
	op := Put
	switch {
	case r < w.cfg.ReadPct:
		op = Get
	case r < w.cfg.ReadPct+w.cfg.DeletePct:
		op = Delete
	}
	var k int
	if w.zipf != nil {
		k = int(w.zipf.Uint64())
	} else {
		k = w.rng.Intn(w.cfg.Keys)
	}
	return op, KeyName(k)
}

// KeyName is the key for index i: fixed width, so every key has one length.
func KeyName(i int) []byte { return []byte(fmt.Sprintf("key%09d", i)) }

// Value is the value a PUT writes: ValueSize bytes, deterministic.
func Value(size int) []byte {
	v := make([]byte, size)
	for i := range v {
		v[i] = byte('a' + i%26)
	}
	return v
}

// client is one simulated client: its own connections, its own sessions.
type client struct {
	id      int
	cfg     *Config
	doers   []kv.Doer
	closers []func() error
	sharded *kv.Sharded
	next    int // anonymous: round-robin endpoint
	rec     *recorder
	value   []byte
}

func newClient(id int, cfg *Config, rec *recorder) *client {
	c := &client{id: id, cfg: cfg, rec: rec, value: Value(cfg.ValueSize), next: id % len(cfg.Endpoints)}
	for _, e := range cfg.Endpoints {
		cl := kv.NewClient(e.Name, e.Addr)
		c.doers = append(c.doers, cl)
		c.closers = append(c.closers, cl.Close)
	}
	if !cfg.Anonymous {
		c.sharded = kv.NewSharded(c.doers, kv.SessionOptions{AttemptTimeout: cfg.AttemptTimeout, MaxAttempts: cfg.MaxAttempts}, cfg.Route)
	}
	return c
}

func (c *client) close() {
	for _, f := range c.closers {
		_ = f()
	}
}

// do runs one operation and returns its class.
func (c *client) do(ctx context.Context, t task) string {
	if c.cfg.Anonymous {
		return c.doAnonymous(ctx, t)
	}
	var out kv.Outcome
	switch t.op {
	case Get:
		out = c.sharded.Get(ctx, t.key, nil)
	case Put:
		out = c.sharded.Put(ctx, t.key, c.value, nil)
	case Delete:
		out = c.sharded.Delete(ctx, t.key, nil)
	}
	switch {
	case !out.Known:
		return ClassUnknown
	case out.Err == nil && out.Response.Status == kv.StatusOK:
		return ClassOK
	case out.Response.Status == kv.StatusNotFound:
		return ClassNotFound
	default:
		return ClassRefused
	}
}

func (c *client) doAnonymous(ctx context.Context, t task) string {
	d := c.doers[c.next]
	c.next = (c.next + 1) % len(c.doers)
	req := kv.Request{Group: c.cfg.Route(t.key), Key: t.key, Timeout: c.cfg.AttemptTimeout}
	switch t.op {
	case Get:
		req.Op = kv.ReqGet
	case Put:
		req.Op, req.Value = kv.ReqPut, c.value
	case Delete:
		req.Op = kv.ReqDelete
	}
	actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
	resp, err := d.Do(actx, req)
	cancel()
	switch {
	case err != nil:
		if errors.Is(err, kv.ErrUnavailable) {
			return ClassRefused // nothing was sent
		}
		return ClassUnknown
	case resp.Status == kv.StatusOK:
		return ClassOK
	case resp.Status == kv.StatusNotFound:
		return ClassNotFound
	case resp.Status.Unknown():
		return ClassUnknown
	default:
		return ClassRefused
	}
}

// Run executes one run against the cluster and returns what the clients
// observed.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	total := cfg.Warmup + cfg.Duration
	rec := newRecorder(&cfg, total)
	clients := make([]*client, cfg.Clients)
	for i := range clients {
		clients[i] = newClient(i, &cfg, rec)
	}
	defer func() {
		for _, c := range clients {
			c.close()
		}
	}()
	if err := register(ctx, clients, cfg.Groups); err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cpu0 := processCPU()
	start := time.Now()
	rec.start = start
	measureFrom, end := start.Add(cfg.Warmup), start.Add(total)
	rec.end = end
	var wg sync.WaitGroup
	if cfg.Rate > 0 {
		tasks := make(chan task)
		for _, c := range clients {
			wg.Add(1)
			go func(c *client) {
				defer wg.Done()
				for t := range tasks {
					class := c.do(runCtx, t)
					rec.record(c.id, t, class, time.Now(), measureFrom)
				}
			}(c)
		}
		w := newWorkload(&cfg, cfg.Seed)
		interval := time.Duration(float64(time.Second) / cfg.Rate)
	dispatch:
		for k := int64(0); ; k++ {
			due := start.Add(time.Duration(k) * interval)
			if !due.Before(end) {
				break
			}
			if d := time.Until(due); d > 0 {
				select {
				case <-time.After(d):
				case <-ctx.Done():
					break dispatch
				}
			}
			op, key := w.next()
			select {
			case tasks <- task{seq: k, op: op, key: key, intended: due}:
			case <-ctx.Done():
				break dispatch
			}
			rec.issued.Add(1)
		}
		close(tasks)
	} else {
		for _, c := range clients {
			wg.Add(1)
			go func(c *client) {
				defer wg.Done()
				w := newWorkload(&cfg, cfg.Seed+int64(c.id)+1)
				for k := int64(0); ; k++ {
					now := time.Now()
					if !now.Before(end) || ctx.Err() != nil {
						return
					}
					op, key := w.next()
					t := task{seq: k, op: op, key: key, intended: now}
					rec.issued.Add(1)
					class := c.do(runCtx, t)
					rec.record(c.id, t, class, time.Now(), measureFrom)
				}
			}(c)
		}
	}
	wg.Wait()
	finished := time.Now()
	return rec.result(&cfg, start, finished, processCPU()-cpu0), ctx.Err()
}

// register opens every client's session in every group before the run, in
// parallel.
func register(ctx context.Context, clients []*client, groups []replication.GroupID) error {
	if len(groups) == 0 || clients[0].sharded == nil {
		return nil
	}
	errs := make(chan error, len(clients))
	for _, c := range clients {
		go func(c *client) {
			for _, g := range groups {
				rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := c.sharded.Session(rctx, g)
				cancel()
				if err != nil {
					errs <- fmt.Errorf("load: client %d: registering in group %d: %w", c.id, g, err)
					return
				}
			}
			errs <- nil
		}(c)
	}
	var first error
	for range clients {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// recorder collects every measured operation, per client (each client writes
// only its own record: no lock on the hot path), and the timeline, in atomic
// buckets.
type recorder struct {
	cfg    *Config
	start  time.Time
	end    time.Time // the measured window's end
	issued atomic.Int64
	// inWindow counts successes that completed inside the measured window —
	// the achieved throughput; late counts operations due in the window that
	// completed after it (a backlog the cluster did not keep up with).
	inWindow, late atomic.Int64
	perCli         []*clientRecord
	buckets        []bucket
}

type bucket struct{ ok, failed atomic.Int64 }

type clientRecord struct {
	lat     [3][]int64 // by op, nanoseconds, measured window only
	classes map[string]int64
	byOp    [3]map[string]int64
}

func newRecorder(cfg *Config, total time.Duration) *recorder {
	n := int(total/cfg.Bucket) + 1 + int(10*time.Second/cfg.Bucket) // slack: operations finishing late
	r := &recorder{cfg: cfg, perCli: make([]*clientRecord, cfg.Clients), buckets: make([]bucket, n)}
	for i := range r.perCli {
		c := &clientRecord{classes: map[string]int64{}}
		for j := range c.byOp {
			c.byOp[j] = map[string]int64{}
		}
		r.perCli[i] = c
	}
	return r
}

func (r *recorder) record(id int, t task, class string, done time.Time, measureFrom time.Time) {
	if i := int(done.Sub(r.start) / r.cfg.Bucket); i >= 0 && i < len(r.buckets) {
		if class == ClassOK || class == ClassNotFound {
			r.buckets[i].ok.Add(1)
		} else {
			r.buckets[i].failed.Add(1)
		}
	}
	success := class == ClassOK || class == ClassNotFound
	if success && !done.Before(measureFrom) && done.Before(r.end) {
		r.inWindow.Add(1)
	}
	if t.intended.Before(measureFrom) {
		return // warmup
	}
	if !done.Before(r.end) {
		r.late.Add(1)
	}
	c := r.perCli[id]
	c.classes[class]++
	c.byOp[t.op][class]++
	if class == ClassOK || class == ClassNotFound {
		c.lat[t.op] = append(c.lat[t.op], int64(done.Sub(t.intended)))
	}
}

// Latency summarizes one operation's latencies: exact nearest-rank
// percentiles over every sample, in microseconds.
type Latency struct {
	Count int64   `json:"count"`
	Min   float64 `json:"min_us"`
	Mean  float64 `json:"mean_us"`
	P50   float64 `json:"p50_us"`
	P90   float64 `json:"p90_us"`
	P95   float64 `json:"p95_us"`
	P99   float64 `json:"p99_us"`
	P999  float64 `json:"p999_us"`
	Max   float64 `json:"max_us"`
}

// Summarize computes exact nearest-rank statistics of samples (nanoseconds);
// it sorts samples in place.
func Summarize(samples []int64) Latency {
	if len(samples) == 0 {
		return Latency{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var sum float64
	for _, s := range samples {
		sum += float64(s)
	}
	us := func(ns int64) float64 { return float64(ns) / 1e3 }
	rank := func(p float64) int64 {
		i := int(math.Ceil(p/100*float64(len(samples)))) - 1
		if i < 0 {
			i = 0
		}
		return samples[i]
	}
	return Latency{Count: int64(len(samples)), Min: us(samples[0]), Mean: sum / float64(len(samples)) / 1e3,
		P50: us(rank(50)), P90: us(rank(90)), P95: us(rank(95)), P99: us(rank(99)), P999: us(rank(99.9)), Max: us(samples[len(samples)-1])}
}

// OpResult is one operation type's outcome counts and latency.
type OpResult struct {
	Classes map[string]int64 `json:"classes"`
	Latency Latency          `json:"latency"`
}

// Point is one timeline bucket: operations that completed in it.
type Point struct {
	OffsetMs int64 `json:"offset_ms"`
	OK       int64 `json:"ok"`
	Failed   int64 `json:"failed"`
}

// Result is what a run observed.
type Result struct {
	Config  Config              `json:"config"`
	Start   time.Time           `json:"start"`
	Elapsed time.Duration       `json:"elapsed"`
	Issued  int64               `json:"issued"`
	Classes map[string]int64    `json:"classes"`
	Ops     map[string]OpResult `json:"ops"`
	All     Latency             `json:"all"`
	// OKPerSec is the achieved throughput: successes that completed inside
	// the measured window, per second of it. Classes and the latencies count
	// the operations DUE in the window, whenever they completed.
	OKPerSec  float64 `json:"ok_per_sec"`
	OfferedPS float64 `json:"offered_per_sec"` // open loop: the target
	// Late counts operations due in the window that completed after it: in
	// open loop, a rate above what the cluster sustained; in closed loop, at
	// most one in flight per client when the window closed.
	Late int64 `json:"late"`
	// LongestOutage is the longest run of timeline buckets, inside the
	// measured window, in which no operation completed successfully.
	LongestOutage time.Duration `json:"longest_outage"`
	Timeline      []Point       `json:"timeline"`
	// GeneratorCPU is the CPU time this process (the generator) used during
	// the run: a generator near a full core per client is measuring itself.
	GeneratorCPU time.Duration `json:"generator_cpu"`
}

func (r *recorder) result(cfg *Config, start, finished time.Time, cpu time.Duration) *Result {
	res := &Result{Config: *cfg, Start: start, Elapsed: finished.Sub(start), Issued: r.issued.Load(),
		Classes: map[string]int64{}, Ops: map[string]OpResult{}, GeneratorCPU: cpu}
	var all []int64
	for op := Get; op <= Delete; op++ {
		var lat []int64
		classes := map[string]int64{}
		for _, c := range r.perCli {
			lat = append(lat, c.lat[op]...)
			for k, v := range c.byOp[op] {
				classes[k] += v
			}
		}
		all = append(all, lat...)
		if len(classes) > 0 {
			res.Ops[op.String()] = OpResult{Classes: classes, Latency: Summarize(lat)}
		}
	}
	for _, c := range r.perCli {
		for k, v := range c.classes {
			res.Classes[k] += v
		}
	}
	res.All = Summarize(all)
	res.OKPerSec = float64(r.inWindow.Load()) / cfg.Duration.Seconds()
	res.OfferedPS = cfg.Rate
	res.Late = r.late.Load()
	last := int(finished.Sub(start)/cfg.Bucket) + 1
	if last > len(r.buckets) {
		last = len(r.buckets)
	}
	for i := 0; i < last; i++ {
		res.Timeline = append(res.Timeline, Point{OffsetMs: (time.Duration(i) * cfg.Bucket).Milliseconds(),
			OK: r.buckets[i].ok.Load(), Failed: r.buckets[i].failed.Load()})
	}
	res.LongestOutage = LongestOutage(res.Timeline, cfg.Bucket, cfg.Warmup, cfg.Warmup+cfg.Duration)
	return res
}

// LongestOutage is the longest run of consecutive buckets with no successful
// completion among those that lie entirely within [from, to).
func LongestOutage(tl []Point, bucket, from, to time.Duration) time.Duration {
	var best, cur time.Duration
	for _, p := range tl {
		off := time.Duration(p.OffsetMs) * time.Millisecond
		if off < from || off+bucket > to {
			continue
		}
		if p.OK == 0 {
			cur += bucket
			if cur > best {
				best = cur
			}
		} else {
			cur = 0
		}
	}
	return best
}

// PrintSummary writes a human-readable summary of r.
func PrintSummary(w io.Writer, r *Result) {
	if r == nil {
		return
	}
	loop := "closed loop"
	if r.Config.Rate > 0 {
		loop = fmt.Sprintf("open loop at %.0f ops/s", r.Config.Rate)
	}
	fmt.Fprintf(w, "%d clients, %s, %s measured after %s warmup; %d keys (%s), %d%% get, %d%% delete, %d-byte values\n",
		r.Config.Clients, loop, r.Config.Duration, r.Config.Warmup, r.Config.Keys, r.Config.KeyDist, r.Config.ReadPct, r.Config.DeletePct, r.Config.ValueSize)
	fmt.Fprintf(w, "completed %.0f ops/s in the window; outcomes %v; late %d; longest outage %s; generator CPU %s\n",
		r.OKPerSec, r.Classes, r.Late, r.LongestOutage, r.GeneratorCPU.Round(time.Millisecond))
	if r.Late > 0 && r.Config.Rate > 0 {
		fmt.Fprintf(w, "WARNING: %d operations due in the window completed after it: the offered rate exceeded what the cluster sustained, and latencies include the backlog\n", r.Late)
	}
	fmt.Fprintf(w, "%-7s %9s %9s %9s %9s %9s %9s %9s\n", "op", "count", "p50 µs", "p90", "p95", "p99", "p99.9", "max")
	for _, op := range []string{"get", "put", "delete"} {
		o, ok := r.Ops[op]
		if !ok {
			continue
		}
		l := o.Latency
		fmt.Fprintf(w, "%-7s %9d %9.0f %9.0f %9.0f %9.0f %9.0f %9.0f\n", op, l.Count, l.P50, l.P90, l.P95, l.P99, l.P999, l.Max)
	}
}
