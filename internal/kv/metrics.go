package kv

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/replication"
)

// Metrics is the client API's instrumentation (Phase 16, docs/OBSERVABILITY.md):
// requests as the node's Front answered them, forwards, and the state
// machine's decisions. A nil *Metrics (the default) instruments nothing.
type Metrics struct {
	requests   *metrics.CounterVec
	duration   *metrics.HistogramVec
	duplicates *metrics.CounterVec
	forwards   *metrics.CounterVec
	forwarded  *metrics.CounterVec
	inflight   *metrics.Gauge
	decisions  *metrics.CounterVec

	// The request path's series, resolved once per label set so a request
	// only increments (no label strings built per request).
	mu     sync.RWMutex
	reqs   map[reqKey]reqSeries
	fwd    map[fwdKey]*metrics.Counter
	served map[reqKey]*metrics.Counter
}

type fwdKey struct {
	g      replication.GroupID
	result string
}

type reqKey struct {
	g  replication.GroupID
	op ReqOp
	st Status
}

type reqSeries struct {
	count, dup *metrics.Counter
	dur        *metrics.Histogram
}

func (m *Metrics) series(k reqKey) reqSeries {
	m.mu.RLock()
	s, ok := m.reqs[k]
	m.mu.RUnlock()
	if ok {
		return s
	}
	g := groupOf(k.g)
	s = reqSeries{count: m.requests.With(g, opLabel(k.op), statusLabel(k.st)), dup: m.duplicates.With(g), dur: m.duration.With(opLabel(k.op))}
	m.mu.Lock()
	m.reqs[k] = s
	m.mu.Unlock()
	return s
}

// NewMetrics registers the client API's families in r (nil: a nil Metrics).
func NewMetrics(r *metrics.Registry) *Metrics {
	if r == nil {
		return nil
	}
	return &Metrics{
		reqs: map[reqKey]reqSeries{}, fwd: map[fwdKey]*metrics.Counter{}, served: map[reqKey]*metrics.Counter{},
		requests: r.CounterVec("dkv_kv_requests_total",
			"Client requests this node's front answered, by the group the request named, the operation and the status returned. A request relayed from a forward is counted here with the leader's status.", "group", "op", "status"),
		duration: r.HistogramVec("dkv_kv_request_seconds",
			"Server-side duration of each client request: from the front receiving it decoded to its response being ready (network time excluded).", metrics.LatencyBuckets, "op"),
		duplicates: r.CounterVec("dkv_kv_duplicate_responses_total",
			"OK responses this node's front returned for a retry answered from the session table (the request had already executed).", "group"),
		forwards: r.CounterVec("dkv_kv_forwards_total",
			"Requests this node forwarded to its group's leader, by outcome: answered, not_sent (no connection: definite no effect), send_unknown (the send failed after it may have left) or timeout (sent, unanswered before the deadline).", "group", "result"),
		forwarded: r.CounterVec("dkv_kv_forwarded_requests_total",
			"Requests forwarded to this node by a peer and served here, by operation and status.", "group", "op", "status"),
		inflight: r.Gauge("dkv_kv_inflight_requests",
			"Client requests this node's front is working on."),
		decisions: r.CounterVec("dkv_kv_apply_decisions_total",
			"State-machine decisions this replica made while applying committed commands, including re-applications after a restart; counted in the process, so a snapshot restore does not reset it. evicted counts sessions evicted by a REGISTER.", "group", "decision"),
	}
}

func opLabel(o ReqOp) string               { return strings.ToLower(o.String()) }
func statusLabel(s Status) string          { return strings.ToLower(s.String()) }
func groupOf(g replication.GroupID) string { return strconv.FormatUint(uint64(g), 10) }

// request counts one answered client request.
func (m *Metrics) request(req Request, resp Response, start time.Time) {
	if m == nil {
		return
	}
	s := m.series(reqKey{req.Group, req.Op, resp.Status})
	s.count.Inc()
	s.dur.Since(start)
	if resp.Duplicate {
		s.dup.Inc()
	}
}

func (m *Metrics) forward(g replication.GroupID, result string) {
	if m == nil {
		return
	}
	k := fwdKey{g, result}
	m.mu.RLock()
	c, ok := m.fwd[k]
	m.mu.RUnlock()
	if !ok {
		c = m.forwards.With(groupOf(g), result)
		m.mu.Lock()
		m.fwd[k] = c
		m.mu.Unlock()
	}
	c.Inc()
}

func (m *Metrics) servedForward(req Request, resp Response) {
	if m == nil {
		return
	}
	k := reqKey{req.Group, req.Op, resp.Status}
	m.mu.RLock()
	c, ok := m.served[k]
	m.mu.RUnlock()
	if !ok {
		c = m.forwarded.With(groupOf(req.Group), opLabel(req.Op), statusLabel(resp.Status))
		m.mu.Lock()
		m.served[k] = c
		m.mu.Unlock()
	}
	c.Inc()
}

// Observe makes store count its decisions into m under group g.
func (m *Metrics) Observe(store *Store, g replication.GroupID) {
	if m == nil || store == nil {
		return
	}
	gl := groupOf(g)
	counters := map[Decision]*metrics.Counter{}
	for d, name := range decisionNames {
		counters[d] = m.decisions.With(gl, name)
	}
	evicted := m.decisions.With(gl, "evicted")
	store.mu.Lock()
	store.observe = func(d Decision) { counters[d].Inc() }
	store.observeEvicted = evicted.Inc
	store.mu.Unlock()
}
