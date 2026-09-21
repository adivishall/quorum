package bench

import (
	"encoding/json"
	"io"
	"math"
	"sort"
	"time"
)

// Config records the exact parameters a benchmark ran under. It is embedded in
// every Result so a number is never separated from the configuration that
// produced it (docs/BENCHMARKS.md §C). Zero-valued fields mean "not applicable
// to this benchmark", not "default": the orchestrator fills in what it used.
type Config struct {
	DatasetSize     int    `json:"dataset_size"`
	KeyBytes        int    `json:"key_bytes"`
	ValueBytes      int    `json:"value_bytes"`
	Concurrency     int    `json:"concurrency"`
	Workload        string `json:"workload"`
	WorkloadRatio   string `json:"workload_ratio,omitempty"`
	SyncMode        string `json:"sync_mode"`
	MemTableBytes   int64  `json:"memtable_bytes"`
	BlockBytes      int    `json:"block_bytes"`
	BloomBitsPerKey int    `json:"bloom_bits_per_key"`
	BloomEnabled    bool   `json:"bloom_enabled"`
	L0Trigger       int    `json:"l0_compaction_trigger"`
	L1MaxBytes      int64  `json:"l1_max_bytes"`
	AutoCompaction  bool   `json:"auto_compaction"`
	OnTmpfs         bool   `json:"on_tmpfs"`
}

// Result is one measured benchmark run. It is the unit of machine-readable
// output. Field names carry their units; there is no bare number whose meaning
// depends on context.
type Result struct {
	Benchmark   string         `json:"benchmark"`
	RunIndex    int            `json:"run_index"`
	Seed        int64          `json:"seed"`
	Timestamp   time.Time      `json:"timestamp"`
	Env         Environment    `json:"env"`
	Config      Config         `json:"config"`
	Operations  int64          `json:"operations"`
	DurationSec float64        `json:"duration_sec"`
	OpsPerSec   float64        `json:"ops_per_sec"`
	MBPerSec    float64        `json:"mb_per_sec"`
	Latency     LatencyStats   `json:"latency"`
	Storage     StorageMetrics `json:"storage"`
	Note        string         `json:"note,omitempty"`
}

// NewResult computes throughput from an operation count, the bytes those
// operations moved (key+value, for MB/s) and the measured wall-clock interval.
// Throughput is total operations divided by the measured interval — no warmup
// samples are inside the interval; the caller times only the measured phase.
func NewResult(name string, ops int64, bytesMoved int64, elapsed time.Duration) Result {
	sec := elapsed.Seconds()
	r := Result{Benchmark: name, Operations: ops, DurationSec: sec, Timestamp: time.Now().UTC()}
	if sec > 0 {
		r.OpsPerSec = float64(ops) / sec
		r.MBPerSec = float64(bytesMoved) / (1024 * 1024) / sec
	}
	return r
}

// WriteJSON writes the result as one indented JSON object.
func (r Result) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// RunSet is repeated runs of the same benchmark. Reporting a set rather than a
// single number is how variance stays visible (docs/BENCHMARKS.md §6): one
// lucky run is not a result.
type RunSet struct {
	Benchmark string   `json:"benchmark"`
	Runs      []Result `json:"runs"`
}

// Add appends a run.
func (s *RunSet) Add(r Result) { s.Runs = append(s.Runs, r) }

// Aggregate summarises the set's throughput and tail latency across runs.
type Aggregate struct {
	Benchmark    string  `json:"benchmark"`
	Runs         int     `json:"runs"`
	OpsPerSecMed float64 `json:"ops_per_sec_median"`
	OpsPerSecMin float64 `json:"ops_per_sec_min"`
	OpsPerSecMax float64 `json:"ops_per_sec_max"`
	P50USMed     float64 `json:"latency_us_p50_median"`
	P95USMed     float64 `json:"latency_us_p95_median"`
	P99USMed     float64 `json:"latency_us_p99_median"`
	SpreadPct    float64 `json:"ops_per_sec_spread_pct"`
}

// Summary computes the aggregate. The spread is (max-min)/median as a percent,
// a blunt but honest indicator of run-to-run stability; a large spread is a
// signal to investigate noise, not something to smooth away.
func (s RunSet) Summary() Aggregate {
	a := Aggregate{Benchmark: s.Benchmark, Runs: len(s.Runs)}
	if len(s.Runs) == 0 {
		return a
	}
	ops := make([]float64, len(s.Runs))
	p50 := make([]float64, len(s.Runs))
	p95 := make([]float64, len(s.Runs))
	p99 := make([]float64, len(s.Runs))
	for i, r := range s.Runs {
		ops[i] = r.OpsPerSec
		p50[i] = r.Latency.P50US
		p95[i] = r.Latency.P95US
		p99[i] = r.Latency.P99US
	}
	a.OpsPerSecMed = median(ops)
	a.OpsPerSecMin = min(ops)
	a.OpsPerSecMax = max(ops)
	a.P50USMed = median(p50)
	a.P95USMed = median(p95)
	a.P99USMed = median(p99)
	if a.OpsPerSecMed > 0 {
		a.SpreadPct = (a.OpsPerSecMax - a.OpsPerSecMin) / a.OpsPerSecMed * 100
	}
	return a
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}

func min(xs []float64) float64 {
	m := math.Inf(1)
	for _, x := range xs {
		if x < m {
			m = x
		}
	}
	return m
}

func max(xs []float64) float64 {
	m := math.Inf(-1)
	for _, x := range xs {
		if x > m {
			m = x
		}
	}
	return m
}
