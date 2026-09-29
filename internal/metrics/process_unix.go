//go:build unix

package metrics

import (
	"runtime"
	"syscall"
)

func registerRusage(r *Registry) {
	ru := func() (syscall.Rusage, bool) {
		var u syscall.Rusage
		return u, syscall.Getrusage(syscall.RUSAGE_SELF, &u) == nil
	}
	r.CollectCounter("process_cpu_seconds_total", "User and system CPU time spent by the process.", nil,
		func(emit func(float64, ...string)) {
			if u, ok := ru(); ok {
				emit(float64(u.Utime.Nano()+u.Stime.Nano()) / 1e9)
			}
		})
	r.CollectGauge("process_max_rss_bytes", "Peak resident set size of the process.", nil,
		func(emit func(float64, ...string)) {
			if u, ok := ru(); ok {
				emit(maxRSSBytes(u.Maxrss))
			}
		})
}

// maxRSSBytes converts getrusage's ru_maxrss: bytes on Darwin, kilobytes on
// Linux and the BSDs.
func maxRSSBytes(v int64) float64 {
	if runtime.GOOS == "darwin" || runtime.GOOS == "ios" {
		return float64(v)
	}
	return float64(v) * 1024
}
