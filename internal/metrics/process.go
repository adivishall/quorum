package metrics

import (
	"runtime"
	"time"
)

// RegisterProcess adds the Go runtime's and the process's own figures, read at
// scrape time: goroutines, heap, garbage collection, CPU time, peak resident
// memory and the start time.
func RegisterProcess(r *Registry) {
	if r == nil {
		return
	}
	start := float64(time.Now().UnixNano()) / 1e9
	r.CollectGauge("go_goroutines", "Goroutines that currently exist.", nil, func(emit func(float64, ...string)) {
		emit(float64(runtime.NumGoroutine()))
	})
	mem := func(pick func(*runtime.MemStats) float64) func(func(float64, ...string)) {
		return func(emit func(float64, ...string)) {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			emit(pick(&ms))
		}
	}
	r.CollectGauge("go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.", nil,
		mem(func(m *runtime.MemStats) float64 { return float64(m.HeapAlloc) }))
	r.CollectGauge("go_memstats_heap_inuse_bytes", "Bytes in in-use heap spans.", nil,
		mem(func(m *runtime.MemStats) float64 { return float64(m.HeapInuse) }))
	r.CollectGauge("go_memstats_sys_bytes", "Bytes of memory obtained from the OS.", nil,
		mem(func(m *runtime.MemStats) float64 { return float64(m.Sys) }))
	r.CollectCounter("go_gc_cycles_total", "Completed garbage-collection cycles.", nil,
		mem(func(m *runtime.MemStats) float64 { return float64(m.NumGC) }))
	r.CollectCounter("go_gc_pause_seconds_total", "Total stop-the-world pause time of garbage collection.", nil,
		mem(func(m *runtime.MemStats) float64 { return float64(m.PauseTotalNs) / 1e9 }))
	r.CollectGauge("process_start_time_seconds", "Start time of the process since the Unix epoch.", nil,
		func(emit func(float64, ...string)) { emit(start) })
	registerRusage(r)
}
