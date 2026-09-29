//go:build unix

package load

import (
	"syscall"
	"time"
)

// processCPU is the user and system CPU time this process has used.
func processCPU() time.Duration {
	var u syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &u) != nil {
		return 0
	}
	return time.Duration(u.Utime.Nano() + u.Stime.Nano())
}
