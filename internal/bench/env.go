// Package bench is Quorum's benchmark harness: reusable workload generation,
// latency measurement, environment capture and a machine-readable result
// format shared by the dkvbench command and by correctness tests.
//
// It deliberately does not import testing. A benchmark is orchestration, not a
// unit test: it opens a real LSMStore, drives a defined workload against it,
// and records what happened. Keeping it a plain library means the same code
// runs from the command line, is unit-tested for correctness, and can be reused
// by a future regression harness without dragging in the testing framework.
//
// Nothing here fabricates, estimates or smooths a number. Every field in a
// Result comes from a clock reading or a counter the storage engine exposes.
// The methodology behind each measurement is written down in docs/BENCHMARKS.md.
package bench

import (
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Environment records the machine and build a benchmark ran on, so a result is
// interpretable without knowing where it came from. Missing fields are left
// empty rather than guessed: an absent CPU model is honest, an invented one is
// not.
type Environment struct {
	Hostname     string    `json:"hostname,omitempty"`
	OS           string    `json:"os"`
	Arch         string    `json:"arch"`
	CPUModel     string    `json:"cpu_model,omitempty"`
	CPUsLogical  int       `json:"cpus_logical"`
	CPUsPhysical int       `json:"cpus_physical,omitempty"`
	RAMBytes     int64     `json:"ram_bytes,omitempty"`
	GoVersion    string    `json:"go_version"`
	GitCommit    string    `json:"git_commit,omitempty"`
	GitDirty     bool      `json:"git_dirty"`
	CapturedAt   time.Time `json:"captured_at"`
}

// CaptureEnvironment reads what it can about the current machine. On failure to
// read a field it leaves that field at its zero value; it never errors, because
// a benchmark should still run and report on a machine whose sysctl names
// differ. dir is the repository directory, used to read the git commit.
func CaptureEnvironment(dir string) Environment {
	e := Environment{
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CPUsLogical: runtime.NumCPU(),
		GoVersion:   runtime.Version(),
		CapturedAt:  time.Now().UTC(),
	}
	if h, err := exec.Command("hostname").Output(); err == nil {
		e.Hostname = strings.TrimSpace(string(h))
	}
	e.CPUModel = sysctlString("machdep.cpu.brand_string")
	if n := sysctlInt("hw.physicalcpu"); n > 0 {
		e.CPUsPhysical = n
	}
	if n := sysctlInt("hw.memsize"); n > 0 {
		e.RAMBytes = int64(n)
	}
	e.GitCommit, e.GitDirty = gitState(dir)
	return e
}

// sysctlString returns a sysctl string value, or "" if sysctl is unavailable
// (non-Darwin, or the name does not exist).
func sysctlString(name string) string {
	out, err := exec.Command("sysctl", "-n", name).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// sysctlInt returns a sysctl integer value, or 0 on any failure.
func sysctlInt(name string) int {
	s := sysctlString(name)
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// gitState returns the HEAD commit and whether the working tree has uncommitted
// changes. A dirty tree matters: a number measured against uncommitted code is
// not reproducible from the recorded commit, and the flag says so.
func gitState(dir string) (commit string, dirty bool) {
	rev := exec.Command("git", "rev-parse", "HEAD")
	rev.Dir = dir
	out, err := rev.Output()
	if err != nil {
		return "", false
	}
	commit = strings.TrimSpace(string(out))

	status := exec.Command("git", "status", "--porcelain")
	status.Dir = dir
	so, err := status.Output()
	if err != nil {
		return commit, false
	}
	return commit, strings.TrimSpace(string(so)) != ""
}
