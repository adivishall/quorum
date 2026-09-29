package metrics

import (
	rtmetrics "runtime/metrics"
	"time"
)

// RegisterProcess adds the Go runtime's and the process's own figures, read at
// scrape time: goroutines, heap, garbage collection, CPU time, peak resident
// memory and the start time. The runtime's are read through runtime/metrics,
// which does not stop the world (runtime.ReadMemStats does).
func RegisterProcess(r *Registry) {
	if r == nil {
		return
	}
	start := float64(time.Now().UnixNano()) / 1e9
	runtimeValue := func(names ...string) func(emit func(float64, ...string)) {
		return func(emit func(float64, ...string)) {
			samples := make([]rtmetrics.Sample, len(names))
			for i, n := range names {
				samples[i].Name = n
			}
			rtmetrics.Read(samples)
			var total float64
			for _, s := range samples {
				switch s.Value.Kind() {
				case rtmetrics.KindUint64:
					total += float64(s.Value.Uint64())
				case rtmetrics.KindFloat64:
					total += s.Value.Float64()
				default:
					return // not supported by this runtime: emit nothing rather than a guess
				}
			}
			emit(total)
		}
	}
	r.CollectGauge("go_goroutines", "Goroutines that currently exist.", nil,
		runtimeValue("/sched/goroutines:goroutines"))
	r.CollectGauge("go_heap_objects_bytes", "Bytes of memory occupied by live heap objects and dead ones not yet swept (runtime/metrics /memory/classes/heap/objects:bytes).", nil,
		runtimeValue("/memory/classes/heap/objects:bytes"))
	r.CollectGauge("go_heap_inuse_bytes", "Bytes in in-use heap spans: objects plus the spans' unused space (runtime/metrics /memory/classes/heap/objects + unused).", nil,
		runtimeValue("/memory/classes/heap/objects:bytes", "/memory/classes/heap/unused:bytes"))
	r.CollectGauge("go_memory_total_bytes", "All memory mapped by the Go runtime (runtime/metrics /memory/classes/total:bytes).", nil,
		runtimeValue("/memory/classes/total:bytes"))
	r.CollectCounter("go_gc_cycles_total", "Completed garbage-collection cycles.", nil,
		runtimeValue("/gc/cycles/total:gc-cycles"))
	r.CollectGauge("process_start_time_seconds", "Start time of the process since the Unix epoch.", nil,
		func(emit func(float64, ...string)) { emit(start) })
	registerRusage(r)
}
