package bench

import "testing"

// TestLinuxEnvironmentParsing: the /proc and /etc/os-release parsers the
// Linux capture uses (CI runs on Linux; the capture must not leave the fields
// empty there as it did before #2).
func TestLinuxEnvironmentParsing(t *testing.T) {
	cpuinfo := `processor	: 0
model name	: AMD EPYC 7763 64-Core Processor
physical id	: 0
core id		: 0

processor	: 1
model name	: AMD EPYC 7763 64-Core Processor
physical id	: 0
core id		: 0

processor	: 2
model name	: AMD EPYC 7763 64-Core Processor
physical id	: 0
core id		: 1

processor	: 3
model name	: AMD EPYC 7763 64-Core Processor
physical id	: 1
core id		: 0
`
	model, cores := parseCPUInfo(cpuinfo)
	if model != "AMD EPYC 7763 64-Core Processor" || cores != 3 {
		t.Fatalf("model %q, %d physical cores (want 3: two threads share core 0 of socket 0)", model, cores)
	}
	if n := parseMemTotal("MemTotal:       16373740 kB\nMemFree:  1 kB\n"); n != 16373740*1024 {
		t.Fatalf("MemTotal %d", n)
	}
	if v := parseOSRelease("NAME=\"Ubuntu\"\nPRETTY_NAME=\"Ubuntu 24.04.1 LTS\"\n"); v != "Ubuntu 24.04.1 LTS" {
		t.Fatalf("PRETTY_NAME %q", v)
	}
	if m, c := parseCPUInfo(""); m != "" || c != 0 {
		t.Fatal("empty cpuinfo")
	}
}
