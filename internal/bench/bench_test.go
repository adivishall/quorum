package bench

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
	"time"
)

// A benchmark that measures the wrong thing is worse than none, so the harness
// that produces the numbers is itself tested. These check the parts that are
// pure computation — percentiles, key/value generation, mix selection — where a
// bug would silently corrupt every reported result.

func TestLatencyPercentilesNearestRank(t *testing.T) {
	l := NewLatencies(100)
	// 1..100 microseconds.
	for i := 1; i <= 100; i++ {
		l.Record(time.Duration(i) * time.Microsecond)
	}
	s := l.Stats()
	if s.Count != 100 {
		t.Fatalf("Count = %d, want 100", s.Count)
	}
	// nearest rank: p50 -> ceil(0.50*100)=50 -> sorted[49]=50us; p95 -> 95; p99 -> 99.
	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"min", s.MinUS, 1}, {"p50", s.P50US, 50}, {"p95", s.P95US, 95},
		{"p99", s.P99US, 99}, {"max", s.MaxUS, 100}, {"mean", s.MeanUS, 50.5},
	}
	for _, c := range checks {
		if math.Abs(c.got-c.want) > 1e-9 {
			t.Errorf("%s = %v us, want %v us", c.name, c.got, c.want)
		}
	}
}

func TestLatencyMergeCombinesSamples(t *testing.T) {
	a := NewLatencies(2)
	a.Record(10 * time.Microsecond)
	b := NewLatencies(2)
	b.Record(20 * time.Microsecond)
	a.Merge(b)
	if a.Len() != 2 {
		t.Fatalf("merged Len = %d, want 2", a.Len())
	}
	if s := a.Stats(); s.MinUS != 10 || s.MaxUS != 20 {
		t.Fatalf("merged stats min/max = %v/%v, want 10/20", s.MinUS, s.MaxUS)
	}
}

func TestEmptyLatenciesAreZero(t *testing.T) {
	if s := NewLatencies(0).Stats(); s.Count != 0 || s.P99US != 0 {
		t.Fatalf("empty stats = %+v, want zero", s)
	}
}

func TestKeyspaceIsDeterministicAndFixedWidth(t *testing.T) {
	ks := NewKeyspace("key", 1_000_000)
	k1 := ks.Key(42)
	k2 := ks.Key(42)
	if !bytes.Equal(k1, k2) {
		t.Fatalf("Key(42) not deterministic: %q vs %q", k1, k2)
	}
	if len(k1) != ks.KeyBytes() {
		t.Fatalf("KeyBytes() = %d, actual key len = %d", ks.KeyBytes(), len(k1))
	}
	// Every key the same width, so they sort in index order.
	prev := ks.Key(0)
	for i := 1; i < 2000; i++ {
		cur := ks.Key(i)
		if len(cur) != len(prev) {
			t.Fatalf("width changed at %d: %q vs %q", i, prev, cur)
		}
		if bytes.Compare(prev, cur) >= 0 {
			t.Fatalf("keys not increasing at %d: %q >= %q", i, prev, cur)
		}
		prev = cur
	}
}

func TestAppendKeyMatchesKey(t *testing.T) {
	ks := NewKeyspace("k", 100000)
	buf := []byte("prefix-")
	got := ks.AppendKey(buf, 7)
	if !bytes.Equal(got, append([]byte("prefix-"), ks.Key(7)...)) {
		t.Fatalf("AppendKey mismatch: %q", got)
	}
}

func TestValueRoundTripsItsIndex(t *testing.T) {
	for _, size := range []int{8, 64, 100, 1024} {
		v := Value(size, 123456)
		if len(v) != size {
			t.Fatalf("Value size = %d, want %d", len(v), size)
		}
		idx, ok := ValueIndex(v)
		if !ok || idx != 123456 {
			t.Fatalf("ValueIndex = %d ok=%v, want 123456", idx, ok)
		}
		// Deterministic.
		if !bytes.Equal(v, Value(size, 123456)) {
			t.Fatalf("Value not deterministic at size %d", size)
		}
		// Not a single repeated byte (size>8).
		if size > 8 {
			all := true
			for j := 9; j < size; j++ {
				if v[j] != v[8] {
					all = false
					break
				}
			}
			if all {
				t.Fatalf("value at size %d is a single repeated byte", size)
			}
		}
	}
}

func TestMixPickApproximatesWeights(t *testing.T) {
	m := Mix{Name: "test", Reads: 70, Writes: 25, Deletes: 5}
	r := rand.New(rand.NewSource(1))
	const n = 200000
	var counts [3]int
	for i := 0; i < n; i++ {
		counts[m.Pick(r)]++
	}
	frac := func(c int) float64 { return float64(c) / n * 100 }
	if d := math.Abs(frac(counts[OpRead]) - 70); d > 1.0 {
		t.Errorf("read fraction %.2f%%, want ~70%% (off by %.2f)", frac(counts[OpRead]), d)
	}
	if d := math.Abs(frac(counts[OpWrite]) - 25); d > 1.0 {
		t.Errorf("write fraction %.2f%%, want ~25%%", frac(counts[OpWrite]))
	}
	if d := math.Abs(frac(counts[OpDelete]) - 5); d > 1.0 {
		t.Errorf("delete fraction %.2f%%, want ~5%%", frac(counts[OpDelete]))
	}
	if m.Ratio() != "70/25/5" {
		t.Errorf("Ratio() = %q, want 70/25/5", m.Ratio())
	}
}

func TestMixPickIsReproducibleFromSeed(t *testing.T) {
	m := MixBalanced
	seq := func() []OpKind {
		r := rand.New(rand.NewSource(99))
		out := make([]OpKind, 1000)
		for i := range out {
			out[i] = m.Pick(r)
		}
		return out
	}
	a, b := seq(), seq()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("mix selection not reproducible at %d", i)
		}
	}
}

func TestNewResultComputesThroughput(t *testing.T) {
	r := NewResult("x", 1000, 100*1024*1024, time.Second)
	if math.Abs(r.OpsPerSec-1000) > 1e-9 {
		t.Errorf("OpsPerSec = %v, want 1000", r.OpsPerSec)
	}
	if math.Abs(r.MBPerSec-100) > 1e-6 {
		t.Errorf("MBPerSec = %v, want 100", r.MBPerSec)
	}
}

func TestRunSetSummaryMedianAndSpread(t *testing.T) {
	var s RunSet
	s.Benchmark = "x"
	for _, ops := range []float64{100, 200, 300} {
		s.Add(Result{OpsPerSec: ops, Latency: LatencyStats{P99US: ops}})
	}
	a := s.Summary()
	if a.OpsPerSecMed != 200 || a.OpsPerSecMin != 100 || a.OpsPerSecMax != 300 {
		t.Fatalf("summary med/min/max = %v/%v/%v, want 200/100/300", a.OpsPerSecMed, a.OpsPerSecMin, a.OpsPerSecMax)
	}
	if math.Abs(a.SpreadPct-100) > 1e-9 {
		t.Fatalf("SpreadPct = %v, want 100", a.SpreadPct)
	}
}

func TestApproxBytesIsEightPerSample(t *testing.T) {
	l := NewLatencies(0)
	if l.ApproxBytes() != 0 {
		t.Fatalf("empty ApproxBytes = %d, want 0", l.ApproxBytes())
	}
	for i := 0; i < 1000; i++ {
		l.Record(time.Microsecond)
	}
	if got := l.ApproxBytes(); got != 8000 {
		t.Fatalf("ApproxBytes for 1000 samples = %d, want 8000", got)
	}
	// Merging adds the other's samples, and the accounting tracks it.
	other := NewLatencies(10)
	other.Record(time.Microsecond)
	l.Merge(other)
	if got := l.ApproxBytes(); got != 8008 {
		t.Fatalf("ApproxBytes after merge = %d, want 8008", got)
	}
}
