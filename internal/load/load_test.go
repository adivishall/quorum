package load

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
)

// TestWorkloadIsDeterministic: the same seed draws the same operations and
// keys; another seed draws others; the mix matches its percentages.
func TestWorkloadIsDeterministic(t *testing.T) {
	cfg := Config{Endpoints: []Endpoint{{"x", "x"}}, Duration: time.Second, ReadPct: 60, DeletePct: 10, Keys: 1000}
	if err := cfg.defaults(); err != nil {
		t.Fatal(err)
	}
	a, b, c := newWorkload(&cfg, 7), newWorkload(&cfg, 7), newWorkload(&cfg, 8)
	counts := map[Op]int{}
	differs := false
	const n = 100000
	for i := 0; i < n; i++ {
		opA, kA := a.next()
		opB, kB := b.next()
		opC, kC := c.next()
		if opA != opB || string(kA) != string(kB) {
			t.Fatalf("draw %d: seed 7 gave %s %s and %s %s", i, opA, kA, opB, kB)
		}
		if opA != opC || string(kA) != string(kC) {
			differs = true
		}
		counts[opA]++
	}
	if !differs {
		t.Fatal("seeds 7 and 8 drew the same workload")
	}
	for op, want := range map[Op]float64{Get: 0.6, Delete: 0.1, Put: 0.3} {
		if got := float64(counts[op]) / n; got < want-0.01 || got > want+0.01 {
			t.Fatalf("%s: %.3f of the operations, want %.2f", op, got, want)
		}
	}
}

// TestZipfConcentratesOnFewKeys: under zipf the hottest key takes far more
// than its uniform share; under uniform no key does.
func TestZipfConcentratesOnFewKeys(t *testing.T) {
	top := func(dist string) float64 {
		cfg := Config{Endpoints: []Endpoint{{"x", "x"}}, Duration: time.Second, Keys: 1000, KeyDist: dist, ZipfS: 1.2}
		if err := cfg.defaults(); err != nil {
			t.Fatal(err)
		}
		w := newWorkload(&cfg, 1)
		seen := map[string]int{}
		const n = 50000
		best := 0
		for i := 0; i < n; i++ {
			_, k := w.next()
			seen[string(k)]++
			if seen[string(k)] > best {
				best = seen[string(k)]
			}
		}
		return float64(best) / n
	}
	if u := top("uniform"); u > 0.005 {
		t.Fatalf("uniform: the hottest key took %.3f of 1000 keys' draws", u)
	}
	if z := top("zipf"); z < 0.1 {
		t.Fatalf("zipf s=1.2: the hottest key took only %.3f", z)
	}
}

// TestSummarizeIsExactNearestRank pins the percentile definition on a known
// sample.
func TestSummarizeIsExactNearestRank(t *testing.T) {
	var s []int64
	for i := int64(100); i >= 1; i-- { // 1..100 µs, shuffled order
		s = append(s, i*1000)
	}
	l := Summarize(s)
	if l.Count != 100 || l.Min != 1 || l.Max != 100 || l.P50 != 50 || l.P90 != 90 || l.P99 != 99 || l.P999 != 100 || l.Mean != 50.5 {
		t.Fatalf("%+v", l)
	}
	if z := Summarize(nil); z != (Latency{}) {
		t.Fatalf("empty: %+v", z)
	}
}

// TestLongestOutage counts only buckets wholly inside the window.
func TestLongestOutage(t *testing.T) {
	b := 100 * time.Millisecond
	tl := []Point{{0, 0, 0}, {100, 0, 1}, {200, 5, 0}, {300, 0, 3}, {400, 0, 0}, {500, 0, 0}, {600, 7, 0}, {700, 0, 0}}
	if got := LongestOutage(tl, b, 0, 800*time.Millisecond); got != 300*time.Millisecond {
		t.Fatalf("whole window: %s", got)
	}
	if got := LongestOutage(tl, b, 200*time.Millisecond, 700*time.Millisecond); got != 300*time.Millisecond {
		t.Fatalf("window 200..700: %s", got)
	}
	if got := LongestOutage(tl, b, 500*time.Millisecond, 700*time.Millisecond); got != 100*time.Millisecond {
		t.Fatalf("window 500..700: %s", got)
	}
}

// fakeServer answers every request OK after its current delay.
type fakeServer struct {
	delay  atomic.Int64 // nanoseconds
	served atomic.Int64
	mu     sync.Mutex
	stall  time.Time // requests arriving before it wait until it
}

func (f *fakeServer) Name() string { return "fake" }
func (f *fakeServer) Do(ctx context.Context, req kv.Request) (kv.Response, error) {
	f.mu.Lock()
	until := f.stall
	f.mu.Unlock()
	if d := time.Until(until); d > 0 {
		time.Sleep(d)
	}
	if d := time.Duration(f.delay.Load()); d > 0 {
		time.Sleep(d)
	}
	f.served.Add(1)
	if req.Op == kv.ReqRegister {
		return kv.Response{Status: kv.StatusOK, ClientID: 1, Index: 1}, nil
	}
	return kv.Response{Status: kv.StatusOK, Index: 1}, nil
}

func startFake(t *testing.T) (*fakeServer, Endpoint) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeServer{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go kv.Serve(ctx, ln, f, nil)
	return f, Endpoint{"fake", ln.Addr().String()}
}

// TestOpenLoopCountsAStallAsLatency: a server that stalls for 400 ms in the
// middle of an open-loop run. Every scheduled operation is still issued —
// none silently skipped — and the stall appears as latency: the operations
// due during it are measured from when they were due.
func TestOpenLoopCountsAStallAsLatency(t *testing.T) {
	f, ep := startFake(t)
	f.delay.Store(int64(time.Millisecond))
	go func() {
		time.Sleep(600 * time.Millisecond)
		f.mu.Lock()
		f.stall = time.Now().Add(400 * time.Millisecond)
		f.mu.Unlock()
	}()
	res, err := Run(context.Background(), Config{Endpoints: []Endpoint{ep}, Clients: 4, Duration: 1500 * time.Millisecond,
		Rate: 200, ReadPct: 50, Keys: 100, Anonymous: true, AttemptTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(200 * 1.5); res.Issued != want {
		t.Fatalf("issued %d operations, the schedule had %d", res.Issued, want)
	}
	if res.Classes[ClassOK] != res.Issued {
		t.Fatalf("outcomes %v for %d issued", res.Classes, res.Issued)
	}
	if res.All.Max < 300e3 {
		t.Fatalf("max latency %.0f µs: a 400 ms stall did not show as latency", res.All.Max)
	}
	// About 80 of the 300 operations fell due during the stall and queued
	// behind it: measured from when they were due, well over a tenth of all
	// operations waited 100 ms or more. Measured from when a client took them
	// (coordinated omission), only the few in flight would.
	if res.All.P90 < 100e3 {
		t.Fatalf("p90 %.0f µs: the operations queued behind the stall were measured from when a client took them", res.All.P90)
	}
	if res.All.P50 > 50e3 {
		t.Fatalf("p50 %.0f µs: most operations never met the stall", res.All.P50)
	}
}

// TestClosedLoopRunsAgainstARealProtocolServer: sessions registered before
// the clock starts, operations counted by class, the timeline covering the
// run and the generator's CPU reported.
func TestClosedLoopRunsAgainstARealProtocolServer(t *testing.T) {
	f, ep := startFake(t)
	res, err := Run(context.Background(), Config{Endpoints: []Endpoint{ep}, Clients: 3, Duration: 500 * time.Millisecond,
		Warmup: 200 * time.Millisecond, ReadPct: 50, Keys: 100, Groups: nil})
	if err != nil {
		t.Fatal(err)
	}
	if res.Classes[ClassOK] == 0 || res.Classes[ClassOK]+res.Classes[ClassNotFound] < res.All.Count {
		t.Fatalf("outcomes %v, %d latencies", res.Classes, res.All.Count)
	}
	if res.Issued <= res.All.Count {
		t.Fatalf("issued %d, measured %d: warmup operations were measured", res.Issued, res.All.Count)
	}
	if f.served.Load() < res.Issued {
		t.Fatalf("the server served %d requests for %d issued", f.served.Load(), res.Issued)
	}
	if len(res.Timeline) < 7 || res.LongestOutage != 0 {
		t.Fatalf("timeline %d buckets, longest outage %s", len(res.Timeline), res.LongestOutage)
	}
	if res.GeneratorCPU <= 0 {
		t.Fatal("no generator CPU reported")
	}
}

// TestOverloadReportsAchievedThroughput: an open-loop rate five times what the
// server sustains. Throughput is what completed inside the window — about the
// server's capacity, never the offered rate — and the operations that fell due
// in the window but completed after it are reported late.
func TestOverloadReportsAchievedThroughput(t *testing.T) {
	f, ep := startFake(t)
	f.delay.Store(int64(10 * time.Millisecond)) // one client: at most 100 ops/s
	res, err := Run(context.Background(), Config{Endpoints: []Endpoint{ep}, Clients: 1, Duration: time.Second,
		Rate: 500, Keys: 10, Anonymous: true, AttemptTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.OKPerSec > 110 || res.OKPerSec < 50 {
		t.Fatalf("achieved %.0f ops/s against a server that sustains 100", res.OKPerSec)
	}
	if res.Late < 300 {
		t.Fatalf("%d late operations; about 400 of the 500 due could not complete in the window", res.Late)
	}
}

// slowRefuser answers GETs at once and refuses every PUT after a delay — the
// shape of a failure scenario, where the failed operations are the slow ones.
type slowRefuser struct{ delay time.Duration }

func (slowRefuser) Name() string { return "refuser" }
func (s slowRefuser) Do(_ context.Context, req kv.Request) (kv.Response, error) {
	if req.Op == kv.ReqPut {
		time.Sleep(s.delay)
		return kv.Response{Status: kv.StatusInvalid, Message: "refused"}, nil
	}
	return kv.Response{Status: kv.StatusOK, Index: 1}, nil
}

// TestFailedOperationsAreNotHiddenFromLatency (audit): the success-only
// percentiles leave out the refused and unknown operations — usually the
// slowest — so the result also reports every outcome's latency and how many
// the success-only figures exclude. Before, the failures' durations were not
// recorded at all, and a failure scenario's published p99 understated its tail.
func TestFailedOperationsAreNotHiddenFromLatency(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go kv.Serve(ctx, ln, slowRefuser{delay: 150 * time.Millisecond}, nil)
	res, err := Run(context.Background(), Config{Endpoints: []Endpoint{{"refuser", ln.Addr().String()}}, Clients: 4,
		Duration: time.Second, ReadPct: 50, Keys: 100, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	put, get := res.Ops["put"], res.Ops["get"]
	refused := put.Classes[ClassRefused] + put.Classes[ClassUnknown]
	if refused == 0 || put.Excluded != refused || put.Latency.Count != 0 {
		t.Fatalf("puts: classes %v, %d excluded, %d in the success-only latency", put.Classes, put.Excluded, put.Latency.Count)
	}
	if put.AllOutcomes.Count != refused || put.AllOutcomes.P50 < 150e3 {
		t.Fatalf("puts' every-outcome latency: %d samples, p50 %.0f µs; want all %d refused, each at least 150 ms", put.AllOutcomes.Count, put.AllOutcomes.P50, refused)
	}
	if get.Excluded != 0 || get.AllOutcomes.Count != get.Latency.Count {
		t.Fatalf("gets all succeeded: %d excluded, %d vs %d samples", get.Excluded, get.AllOutcomes.Count, get.Latency.Count)
	}
	if res.Excluded != refused || res.AllOutcomes.Count != res.All.Count+refused || res.AllOutcomes.Max < 150e3 || res.All.Max >= 150e3 {
		t.Fatalf("overall: %d excluded of %d; success-only max %.0f µs, every-outcome max %.0f µs", res.Excluded, res.AllOutcomes.Count, res.All.Max, res.AllOutcomes.Max)
	}
	var out strings.Builder
	PrintSummary(&out, res)
	if !strings.Contains(out.String(), "excluded above") || !strings.Contains(out.String(), "put*") {
		t.Fatalf("the summary does not show the excluded operations:\n%s", out.String())
	}
}
