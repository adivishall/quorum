//go:build !unix

package load

import "time"

// processCPU is not available here.
func processCPU() time.Duration { return 0 }
