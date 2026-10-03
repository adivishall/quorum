//go:build linux

package integration

import "syscall"

// procAttr gives a test's process its own process group and kills it if the
// test binary dies (Pdeathsig): a timeout panic skips every cleanup.
func procAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

// killGroup kills the process group of pid (its leader is pid: Setpgid).
func killGroup(pid int) { _ = syscall.Kill(-pid, syscall.SIGKILL) }
