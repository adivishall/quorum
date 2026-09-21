package bench

import (
	"sort"
	"time"
)

// Latencies collects per-operation durations and computes percentiles exactly.
//
// It stores every sample rather than bucketing into a histogram. That is a
// deliberate, *bounded* trade, not a silent one: the sample payload is 8 bytes
// per recorded operation (one int64), so a run of N operations holds 8N bytes of
// samples (plus slice-growth and per-worker overhead), and ApproxBytes reports
// that payload. The benchmark suite's operation counts are bounded by
// design — the largest is one read per key over a 1,000,000-key dataset, i.e.
// 1,000,000 samples ≈ 8 MB, split across workers — so the whole set of samples
// plus one sort is affordable, and in return the percentiles are exact with no
// histogram bucket-boundary error to reason about.
//
// The cost scales linearly with the operation count, so a caller that wants to
// time billions of operations should not use this directly; at that scale a
// bounded reservoir or a log-linear histogram (approximate percentiles) is the
// right structure. Nothing in this phase runs at that scale, and the bound is
// documented and asserted (TestApproxBytesIsEightPerSample) rather than assumed.
// Each worker fills its own Latencies with no lock; Merge combines them at the
// end.
type Latencies struct {
	ns []int64
}

// NewLatencies returns a recorder pre-sized for n samples, so the hot loop does
// no reallocation. n is a hint; more samples are accepted.
func NewLatencies(n int) *Latencies {
	return &Latencies{ns: make([]int64, 0, n)}
}

// Record appends one sample. Not safe for concurrent use: give each worker its
// own recorder and Merge afterwards.
func (l *Latencies) Record(d time.Duration) {
	l.ns = append(l.ns, int64(d))
}

// Merge appends every sample from other. Used to combine per-worker recorders.
func (l *Latencies) Merge(other *Latencies) {
	l.ns = append(l.ns, other.ns...)
}

// Len is the number of samples.
func (l *Latencies) Len() int { return len(l.ns) }

// ApproxBytes is the sample payload the collector holds: 8 bytes per recorded
// operation (one int64), counted over the backing array's length. It is the
// dominant, measurable term — not a claim of total Go memory. The real footprint
// is somewhat larger: slice capacity is over-allocated as the array grows, each
// worker holds its own Latencies, and Stats sorts in place. Those are constant
// or small-multiple overheads on top of this payload; ApproxBytes exists to make
// the payload a reported number rather than a comment (requirement: no silent
// unbounded latency collector), not to claim the process uses exactly 8N bytes.
func (l *Latencies) ApproxBytes() int64 { return int64(len(l.ns)) * 8 }

// LatencyStats is the summary of a set of samples, in microseconds.
type LatencyStats struct {
	Count  int     `json:"count"`
	MinUS  float64 `json:"latency_us_min"`
	MeanUS float64 `json:"latency_us_mean"`
	P50US  float64 `json:"latency_us_p50"`
	P95US  float64 `json:"latency_us_p95"`
	P99US  float64 `json:"latency_us_p99"`
	MaxUS  float64 `json:"latency_us_max"`
}

// Stats sorts the samples and computes the summary. Percentiles use the
// nearest-rank method on the sorted samples: the p-th percentile is the sample
// at 1-based rank ceil(p/100 * n). This is the definition documented in
// docs/BENCHMARKS.md §D; it needs no interpolation and is stable for small n.
// Stats sorts in place, so it is called once at the end.
func (l *Latencies) Stats() LatencyStats {
	n := len(l.ns)
	if n == 0 {
		return LatencyStats{}
	}
	sort.Slice(l.ns, func(i, j int) bool { return l.ns[i] < l.ns[j] })

	var sum int64
	for _, v := range l.ns {
		sum += v
	}
	us := func(ns int64) float64 { return float64(ns) / 1000.0 }
	return LatencyStats{
		Count:  n,
		MinUS:  us(l.ns[0]),
		MeanUS: us(sum) / float64(n),
		P50US:  us(l.percentile(50)),
		P95US:  us(l.percentile(95)),
		P99US:  us(l.percentile(99)),
		MaxUS:  us(l.ns[n-1]),
	}
}

// percentile returns the sample at the nearest rank for p in (0,100]. The
// samples must already be sorted ascending.
func (l *Latencies) percentile(p float64) int64 {
	n := len(l.ns)
	rank := int(float64(p)/100.0*float64(n) + 0.999999999) // ceil
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return l.ns[rank-1]
}
