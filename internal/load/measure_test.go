package load

import (
	"context"
	"flag"
	"testing"
	"time"
)

var flagMeasure = flag.Bool("load.measure", false, "run the generator's ceiling measurement (TestMeasureGeneratorCeiling)")

// TestMeasureGeneratorCeiling (docs/LOAD_TESTING.md §6): closed loop against a
// protocol server that answers at once, over loopback — the most the
// generator and the client protocol can drive on this machine, and the CPU it
// takes. A cluster result near this ceiling measures the generator.
//
//	go test ./internal/load -run TestMeasureGeneratorCeiling -load.measure -v
func TestMeasureGeneratorCeiling(t *testing.T) {
	if !*flagMeasure {
		t.Skip("-load.measure")
	}
	for _, clients := range []int{1, 4, 16, 64} {
		_, ep := startFake(t)
		res, err := Run(context.Background(), Config{Endpoints: []Endpoint{ep}, Clients: clients, Duration: 3 * time.Second,
			Warmup: 500 * time.Millisecond, ReadPct: 50, Keys: 10000})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("clients=%2d  %8.0f ops/s  p50 %5.0f µs  p99 %5.0f µs  generator+server CPU %.2f cores",
			clients, res.OKPerSec, res.All.P50, res.All.P99, res.GeneratorCPU.Seconds()/res.Elapsed.Seconds())
	}
}
