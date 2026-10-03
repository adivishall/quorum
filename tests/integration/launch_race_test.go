package integration

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestARaceReportFailsItsTest: a process whose output carries a race
// detector report fails the test that started it. A race-built dkvd prints
// the report and goes on running, and one ended by SIGKILL never exits with
// the detector's status: before, a race in dkvd passed every integration test
// that did not stop it gracefully (an injected race passed three of them).
//
// The test runs itself as a subprocess: that inner run starts a child that
// prints a report, and it must fail.
func TestARaceReportFailsItsTest(t *testing.T) {
	if os.Getenv("QUORUM_RACE_REPORT_PROBE") == "1" {
		var buf bytes.Buffer
		cmd := exec.Command("sh", "-c", "echo event=ready >&2; echo 'WARNING: DATA RACE' >&2; echo 'Write at 0x00c000012345 by goroutine 7:' >&2")
		cmd.Stdout, cmd.Stderr = &buf, &buf
		if err := startProc(t, cmd); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestARaceReportFailsItsTest$", "-test.count=1")
	cmd.Env = append(os.Environ(), "QUORUM_RACE_REPORT_PROBE=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "reported a data race") || !strings.Contains(string(out), "Write at 0x00c000012345") {
		t.Fatalf("a child's race report did not fail its test (exit %v):\n%s", err, out)
	}
}
