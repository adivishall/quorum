//go:build !linux

package integration

import (
	"os"
	"syscall"
)

// procAttr: elsewhere a test's process keeps the test's group; its cleanup
// still kills it.
func procAttr() *syscall.SysProcAttr { return nil }

func killGroup(pid int) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}
