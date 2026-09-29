// Package metrics is the observability layer of a running node (Phase 16,
// docs/OBSERVABILITY.md): counters, gauges and histograms with fixed label sets,
// collectors evaluated at scrape time for values read from live state, and the
// Prometheus text exposition format. It has no dependencies beyond the standard
// library.
//
// Every value is produced by the code path it describes — a counter is
// incremented where the event happens, a histogram observes a duration measured
// around the operation, a collector reads the state it reports at the moment of
// the scrape. Nothing is sampled, interpolated or reconstructed.
//
// Every method is safe on a nil receiver and does nothing there, and a nil
// *Registry hands out nil metrics: code instrumented for metrics runs unchanged,
// at no cost, where no registry was configured (tests, the deterministic
// simulator).
package metrics

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Type is a metric family's type.
type Type string

// The family types.
const (
	CounterType   Type = "counter"
	GaugeType     Type = "gauge"
	HistogramType Type = "histogram"
)

// Registry holds metric families by name.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

type family struct {
	name    string
	help    string
	typ     Type
	labels  []string
	buckets []float64 // histograms: upper bounds, ascending, without +Inf

	mu       sync.RWMutex
	children map[string]child // by seriesKey(label values)

	// A collected family's samples come from these functions at scrape time.
	cmu        sync.Mutex
	collectors []func(emit func(v float64, labelValues ...string))
}

// child is one series: its label values and its metric (*Counter, *Gauge or
// *Histogram).
type child struct {
	values []string
	m      any
}

// seriesKey encodes label values without ambiguity: each is length-prefixed,
// so no value — whatever bytes it holds — can make two label sets collide.
func seriesKey(values []string) string {
	var b strings.Builder
	for _, v := range values {
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
	}
	return b.String()
}

// register returns the family named name, creating it; registering a name
// again with another type, help, label set or buckets is a programming error.
func (r *Registry) register(name, help string, typ Type, labels []string, buckets []float64) *family {
	if r == nil {
		return nil
	}
	if err := checkName(name); err != nil {
		panic(err)
	}
	for _, l := range labels {
		if err := checkName(l); err != nil || l == "le" || strings.HasPrefix(l, "__") {
			panic(fmt.Sprintf("metrics: %s: invalid label name %q", name, l))
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.families[name]; ok {
		if f.typ != typ || f.help != help || !equalStrings(f.labels, labels) || !equalFloats(f.buckets, buckets) {
			panic(fmt.Sprintf("metrics: %s registered twice with different definitions", name))
		}
		return f
	}
	f := &family{name: name, help: help, typ: typ, labels: append([]string(nil), labels...),
		buckets: append([]float64(nil), buckets...), children: map[string]child{}}
	r.families[name] = f
	return f
}

func checkName(s string) error {
	if s == "" {
		return fmt.Errorf("metrics: empty name")
	}
	for i, c := range s {
		ok := c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9')
		if !ok {
			return fmt.Errorf("metrics: invalid name %q", s)
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// child returns the family's child for labelValues, creating it with mk.
func (f *family) child(labelValues []string, mk func() any) any {
	if len(labelValues) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s takes %d label values, got %d", f.name, len(f.labels), len(labelValues)))
	}
	key := seriesKey(labelValues)
	f.mu.RLock()
	c, ok := f.children[key]
	f.mu.RUnlock()
	if ok {
		return c.m
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.children[key]; ok {
		return c.m
	}
	c = child{values: append([]string(nil), labelValues...), m: mk()}
	f.children[key] = c
	return c.m
}

// --- counters ---

// Counter is a monotonically increasing count.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() {
	if c != nil {
		c.v.Add(1)
	}
}

// Add adds n.
func (c *Counter) Add(n uint64) {
	if c != nil {
		c.v.Add(n)
	}
}

// Value returns the count (0 for nil).
func (c *Counter) Value() uint64 {
	if c == nil {
		return 0
	}
	return c.v.Load()
}

// CounterVec is a counter family with labels.
type CounterVec struct{ f *family }

// Counter registers (or returns) an unlabelled counter.
func (r *Registry) Counter(name, help string) *Counter {
	return r.CounterVec(name, help).With()
}

// CounterVec registers (or returns) a counter family with the given labels.
func (r *Registry) CounterVec(name, help string, labels ...string) *CounterVec {
	f := r.register(name, help, CounterType, labels, nil)
	if f == nil {
		return nil
	}
	return &CounterVec{f}
}

// With returns the counter for the label values, in the family's label order.
func (v *CounterVec) With(labelValues ...string) *Counter {
	if v == nil {
		return nil
	}
	return v.f.child(labelValues, func() any { return &Counter{} }).(*Counter)
}

// --- gauges ---

// Gauge is a value that goes up and down.
type Gauge struct{ bits atomic.Uint64 }

// Set sets the gauge.
func (g *Gauge) Set(v float64) {
	if g != nil {
		g.bits.Store(math.Float64bits(v))
	}
}

// Add adds d (which may be negative).
func (g *Gauge) Add(d float64) {
	if g == nil {
		return
	}
	for {
		old := g.bits.Load()
		if g.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+d)) {
			return
		}
	}
}

// Inc adds one; Dec subtracts one.
func (g *Gauge) Inc() { g.Add(1) }

// Dec subtracts one.
func (g *Gauge) Dec() { g.Add(-1) }

// Value returns the gauge (0 for nil).
func (g *Gauge) Value() float64 {
	if g == nil {
		return 0
	}
	return math.Float64frombits(g.bits.Load())
}

// GaugeVec is a gauge family with labels.
type GaugeVec struct{ f *family }

// Gauge registers (or returns) an unlabelled gauge.
func (r *Registry) Gauge(name, help string) *Gauge { return r.GaugeVec(name, help).With() }

// GaugeVec registers (or returns) a gauge family with the given labels.
func (r *Registry) GaugeVec(name, help string, labels ...string) *GaugeVec {
	f := r.register(name, help, GaugeType, labels, nil)
	if f == nil {
		return nil
	}
	return &GaugeVec{f}
}

// With returns the gauge for the label values.
func (v *GaugeVec) With(labelValues ...string) *Gauge {
	if v == nil {
		return nil
	}
	return v.f.child(labelValues, func() any { return &Gauge{} }).(*Gauge)
}

// --- histograms ---

// Histogram counts observations into buckets and keeps their sum and count.
type Histogram struct {
	upper  []float64       // shared with the family; ascending
	counts []atomic.Uint64 // one per upper bound, then +Inf; non-cumulative
	count  atomic.Uint64
	sum    atomic.Uint64 // float64 bits
}

func newHistogram(upper []float64) *Histogram {
	return &Histogram{upper: upper, counts: make([]atomic.Uint64, len(upper)+1)}
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	if h == nil || math.IsNaN(v) {
		return
	}
	i := sort.SearchFloat64s(h.upper, v) // first upper bound >= v: le is inclusive
	h.counts[i].Add(1)
	for {
		old := h.sum.Load()
		if h.sum.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			break
		}
	}
	h.count.Add(1)
}

// ObserveDuration records d in seconds.
func (h *Histogram) ObserveDuration(d time.Duration) { h.Observe(d.Seconds()) }

// Since records the time elapsed since start, in seconds.
func (h *Histogram) Since(start time.Time) {
	if h != nil {
		h.ObserveDuration(time.Since(start))
	}
}

// Count returns the number of observations (0 for nil).
func (h *Histogram) Count() uint64 {
	if h == nil {
		return 0
	}
	return h.count.Load()
}

// Sum returns the sum of the observations (0 for nil).
func (h *Histogram) Sum() float64 {
	if h == nil {
		return 0
	}
	return math.Float64frombits(h.sum.Load())
}

// HistogramVec is a histogram family with labels.
type HistogramVec struct{ f *family }

// Histogram registers (or returns) an unlabelled histogram.
func (r *Registry) Histogram(name, help string, buckets []float64) *Histogram {
	return r.HistogramVec(name, help, buckets).With()
}

// HistogramVec registers (or returns) a histogram family. buckets are the upper
// bounds, strictly ascending and finite; +Inf is implied.
func (r *Registry) HistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	for i, b := range buckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || (i > 0 && b <= buckets[i-1]) {
			panic(fmt.Sprintf("metrics: %s: buckets must be finite and strictly ascending", name))
		}
	}
	f := r.register(name, help, HistogramType, labels, buckets)
	if f == nil {
		return nil
	}
	return &HistogramVec{f}
}

// With returns the histogram for the label values.
func (v *HistogramVec) With(labelValues ...string) *Histogram {
	if v == nil {
		return nil
	}
	return v.f.child(labelValues, func() any { return newHistogram(v.f.buckets) }).(*Histogram)
}

// ExponentialBuckets returns count upper bounds start, start*factor, ...
func ExponentialBuckets(start, factor float64, count int) []float64 {
	if start <= 0 || factor <= 1 || count < 1 {
		panic("metrics: ExponentialBuckets needs start > 0, factor > 1, count >= 1")
	}
	out := make([]float64, count)
	for i := range out {
		out[i] = start
		start *= factor
	}
	return out
}

// LatencyBuckets are the default bounds for request and Raft stage latencies:
// 50 µs to about 26 s, doubling.
var LatencyBuckets = ExponentialBuckets(50e-6, 2, 20)

// --- collectors ---

// CollectCounter registers a counter family whose samples fn emits at scrape
// time, for a count kept elsewhere; fn must report monotonically
// non-decreasing values per label set.
func (r *Registry) CollectCounter(name, help string, labels []string, fn func(emit func(v float64, labelValues ...string))) {
	r.collect(name, help, CounterType, labels, fn)
}

// CollectGauge registers a gauge family whose samples fn emits at scrape time,
// read from live state.
func (r *Registry) CollectGauge(name, help string, labels []string, fn func(emit func(v float64, labelValues ...string))) {
	r.collect(name, help, GaugeType, labels, fn)
}

func (r *Registry) collect(name, help string, typ Type, labels []string, fn func(emit func(v float64, labelValues ...string))) {
	f := r.register(name, help, typ, labels, nil)
	if f == nil {
		return
	}
	f.cmu.Lock()
	f.collectors = append(f.collectors, fn)
	f.cmu.Unlock()
}
