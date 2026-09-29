package metrics

import (
	"bytes"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func text(t *testing.T, r *Registry) string {
	t.Helper()
	var b bytes.Buffer
	if err := r.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// TestExpositionIsExact pins the text format: HELP and TYPE lines, families by
// name, series by label values (numeric values numerically), label and help
// escaping, cumulative histogram buckets with an inclusive upper bound, +Inf,
// _sum and _count.
func TestExpositionIsExact(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("dkv_b_total", "Things\\done\nhere.", "group", "op")
	c.With("10", "put").Add(3)
	c.With("2", "get").Inc()
	c.With("2", "put").Add(2)
	r.Gauge("dkv_a", "A gauge.").Set(-1.5)
	h := r.HistogramVec("dkv_c_seconds", "A histogram.", []float64{0.1, 1}, "kind")
	for _, v := range []float64{0.05, 0.1, 0.5, 1, 2} {
		h.With(`q"x\y`).Observe(v)
	}
	r.CollectGauge("dkv_d", "Collected.", []string{"peer"}, func(emit func(float64, ...string)) {
		emit(7, "n2")
		emit(5, "n1")
	})
	want := `# HELP dkv_a A gauge.
# TYPE dkv_a gauge
dkv_a -1.5
# HELP dkv_b_total Things\\done\nhere.
# TYPE dkv_b_total counter
dkv_b_total{group="2",op="get"} 1
dkv_b_total{group="2",op="put"} 2
dkv_b_total{group="10",op="put"} 3
# HELP dkv_c_seconds A histogram.
# TYPE dkv_c_seconds histogram
dkv_c_seconds_bucket{kind="q\"x\\y",le="0.1"} 2
dkv_c_seconds_bucket{kind="q\"x\\y",le="1"} 4
dkv_c_seconds_bucket{kind="q\"x\\y",le="+Inf"} 5
dkv_c_seconds_sum{kind="q\"x\\y"} 3.65
dkv_c_seconds_count{kind="q\"x\\y"} 5
# HELP dkv_d Collected.
# TYPE dkv_d gauge
dkv_d{peer="n1"} 5
dkv_d{peer="n2"} 7
`
	if got := text(t, r); got != want {
		t.Fatalf("exposition:\n%s\nwant:\n%s", got, want)
	}
	if got := text(t, r); got != want {
		t.Fatal("a second scrape of the same state differs")
	}
}

// TestParseRoundTrip: what WriteText writes, Parse reads back — names, labels
// with escapes, values, including +Inf and NaN.
func TestParseRoundTrip(t *testing.T) {
	r := NewRegistry()
	r.CounterVec("dkv_x_total", "x", "k").With("a\"b\\c\nd").Add(42)
	r.Gauge("dkv_inf", "inf").Set(math.Inf(1))
	r.Gauge("dkv_nan", "nan").Set(math.NaN())
	h := r.Histogram("dkv_h", "h", []float64{1})
	h.Observe(0.5)
	ss, err := Parse(strings.NewReader(text(t, r)))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := ss.Get("dkv_x_total", "k", "a\"b\\c\nd"); !ok || v != 42 {
		t.Fatalf("counter: %v %v", v, ok)
	}
	if v, ok := ss.Get("dkv_inf"); !ok || !math.IsInf(v, 1) {
		t.Fatalf("inf: %v", v)
	}
	if v, ok := ss.Get("dkv_nan"); !ok || !math.IsNaN(v) {
		t.Fatalf("nan: %v", v)
	}
	if v, ok := ss.Get("dkv_h_bucket", "le", "+Inf"); !ok || v != 1 {
		t.Fatalf("+Inf bucket: %v", v)
	}
	if v, ok := ss.Get("dkv_h_count"); !ok || v != 1 {
		t.Fatalf("count: %v", v)
	}
	if _, err := Parse(strings.NewReader("dkv_bad{a=\"x} 1\n")); err == nil {
		t.Fatal("an unterminated label value parsed")
	}
	if _, err := Parse(strings.NewReader("dkv_novalue\n")); err == nil {
		t.Fatal("a line without a value parsed")
	}
}

// TestNilIsInert: a nil registry hands out nil metrics, and every method on a
// nil metric does nothing — instrumented code runs unchanged without one.
func TestNilIsInert(t *testing.T) {
	var r *Registry
	c := r.Counter("a_total", "a")
	g := r.Gauge("b", "b")
	h := r.Histogram("c", "c", LatencyBuckets)
	cv := r.CounterVec("d_total", "d", "l")
	hv := r.HistogramVec("e", "e", LatencyBuckets, "l")
	gv := r.GaugeVec("f", "f", "l")
	r.CollectGauge("g", "g", nil, func(func(float64, ...string)) { t.Fatal("collector of a nil registry ran") })
	RegisterProcess(r)
	c.Inc()
	c.Add(2)
	g.Set(1)
	g.Inc()
	h.Observe(1)
	h.Since(time.Now())
	cv.With("x").Inc()
	hv.With("x").Observe(1)
	gv.With("x").Dec()
	if c.Value() != 0 || g.Value() != 0 || h.Count() != 0 || h.Sum() != 0 {
		t.Fatal("a nil metric reported a value")
	}
	if err := r.WriteText(io.Discard); err != nil {
		t.Fatal(err)
	}
}

// TestRegistrationIsIdempotentAndChecked: registering the same definition
// again returns the same metric; a conflicting definition or a bad name
// panics.
func TestRegistrationIsIdempotentAndChecked(t *testing.T) {
	r := NewRegistry()
	a := r.CounterVec("dkv_same_total", "h", "l").With("x")
	a.Inc()
	if b := r.CounterVec("dkv_same_total", "h", "l").With("x"); b != a || b.Value() != 1 {
		t.Fatal("re-registration returned another counter")
	}
	for name, fn := range map[string]func(){
		"another type":    func() { r.Gauge("dkv_same_total", "h") },
		"another label":   func() { r.CounterVec("dkv_same_total", "h", "m") },
		"another help":    func() { r.CounterVec("dkv_same_total", "other", "l") },
		"bad name":        func() { r.Counter("dkv-dash", "h") },
		"reserved label":  func() { r.CounterVec("dkv_le_total", "h", "le") },
		"label count":     func() { r.CounterVec("dkv_same_total", "h", "l").With("x", "y") },
		"unsorted bucket": func() { r.Histogram("dkv_hb", "h", []float64{2, 1}) },
		"infinite bucket": func() { r.Histogram("dkv_hi", "h", []float64{math.Inf(1)}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			fn()
		}()
	}
}

// TestConcurrentUpdatesAreExactUnderScrapes: many goroutines update counters,
// gauges and histograms while others scrape; the final values are exact.
// (Run with -race.)
func TestConcurrentUpdatesAreExactUnderScrapes(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("dkv_n_total", "n", "w")
	g := r.Gauge("dkv_g", "g")
	h := r.Histogram("dkv_h_seconds", "h", LatencyBuckets)
	const workers, each = 8, 5000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var scrapes sync.WaitGroup
	for i := 0; i < 2; i++ {
		scrapes.Add(1)
		go func() {
			defer scrapes.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = r.WriteText(io.Discard)
				}
			}
		}()
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctr := c.With(string(rune('a' + w%2)))
			for i := 0; i < each; i++ {
				ctr.Inc()
				g.Add(1)
				h.Observe(0.001)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	scrapes.Wait()
	ss, err := Parse(strings.NewReader(text(t, r)))
	if err != nil {
		t.Fatal(err)
	}
	if got := ss.Sum("dkv_n_total"); got != workers*each {
		t.Fatalf("counter total %v, want %d", got, workers*each)
	}
	if g.Value() != workers*each {
		t.Fatalf("gauge %v", g.Value())
	}
	if v, _ := ss.Get("dkv_h_seconds_count"); v != workers*each {
		t.Fatalf("histogram count %v", v)
	}
	if math.Abs(h.Sum()-workers*each*0.001) > 1e-6 {
		t.Fatalf("histogram sum %v", h.Sum())
	}
}

// TestHandlerServesTheText: GET returns the exposition with its content type;
// other methods are refused.
func TestHandlerServesTheText(t *testing.T) {
	r := NewRegistry()
	r.Counter("dkv_served_total", "s").Add(9)
	srv := httptest.NewServer(Handler(r))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != ContentType || !strings.Contains(string(body), "dkv_served_total 9\n") {
		t.Fatalf("%d %q\n%s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
	resp, err = http.Post(srv.URL+"/metrics", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST: %d", resp.StatusCode)
	}
}

// TestProcessCollectors: the runtime and process figures are present and
// plausible for this very process.
func TestProcessCollectors(t *testing.T) {
	r := NewRegistry()
	RegisterProcess(r)
	ss, err := Parse(strings.NewReader(text(t, r)))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go_goroutines", "go_heap_objects_bytes", "go_heap_inuse_bytes", "go_memory_total_bytes", "process_start_time_seconds"} {
		if v, ok := ss.Get(name); !ok || v <= 0 {
			t.Errorf("%s = %v (present %v)", name, v, ok)
		}
	}
	if _, ok := ss.Get("go_gc_cycles_total"); !ok {
		t.Error("go_gc_cycles_total missing")
	}
	if v, ok := ss.Get("process_cpu_seconds_total"); ok && v <= 0 {
		t.Errorf("process_cpu_seconds_total = %v", v)
	}
	if v, ok := ss.Get("process_max_rss_bytes"); ok && v < 1<<20 {
		t.Errorf("process_max_rss_bytes = %v: below a megabyte, a unit error", v)
	}
}
