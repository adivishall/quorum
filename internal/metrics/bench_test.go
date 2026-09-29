package metrics

import (
	"io"
	"testing"
	"time"
)

// The instrumentation's own costs (docs/OBSERVABILITY.md §5): what one event
// adds to the path that records it.

func BenchmarkCounterInc(b *testing.B) {
	c := NewRegistry().Counter("dkv_bench_total", "b")
	for i := 0; i < b.N; i++ {
		c.Inc()
	}
}

func BenchmarkCounterIncParallel(b *testing.B) {
	c := NewRegistry().Counter("dkv_bench_total", "b")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkCounterVecWith(b *testing.B) {
	v := NewRegistry().CounterVec("dkv_bench_total", "b", "group", "op", "status")
	for i := 0; i < b.N; i++ {
		v.With("3", "put", "ok").Inc()
	}
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := NewRegistry().Histogram("dkv_bench_seconds", "b", LatencyBuckets)
	d := 3 * time.Millisecond
	for i := 0; i < b.N; i++ {
		h.ObserveDuration(d)
	}
}

func BenchmarkHistogramSince(b *testing.B) {
	h := NewRegistry().Histogram("dkv_bench_seconds", "b", LatencyBuckets)
	for i := 0; i < b.N; i++ {
		h.Since(time.Now())
	}
}

func BenchmarkNilCounter(b *testing.B) {
	var c *Counter
	for i := 0; i < b.N; i++ {
		c.Inc()
	}
}

// BenchmarkScrape is one full exposition of a registry the size of a node
// hosting 16 groups.
func BenchmarkScrape(b *testing.B) {
	r := NewRegistry()
	RegisterProcess(r)
	req := r.CounterVec("dkv_kv_requests_total", "b", "group", "op", "status")
	lat := r.HistogramVec("dkv_raft_commit_seconds", "b", LatencyBuckets, "group")
	for g := 0; g < 16; g++ {
		gl := string(rune('a' + g))
		for _, op := range []string{"put", "get", "delete", "register"} {
			req.With(gl, op, "ok").Inc()
		}
		lat.With(gl).Observe(0.001)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.WriteText(io.Discard)
	}
}
