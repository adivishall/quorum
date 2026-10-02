package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// One launcher for every dkvd process a test starts (audit: integration could
// orphan processes, and built the binary once per test, without -race):
//
//   - dkvd is built once per test binary, with -race when the test binary is
//     (raceEnabled), so the real processes are race-checked too;
//   - a process's cleanup is registered BEFORE it starts, and kills its whole
//     process group, so no failure path — a t.Fatal mid-launch, a test that
//     forgets its process — leaves it running;
//   - on Linux each process has its own group and dies with the test binary
//     (Pdeathsig), so a timeout panic, which skips every cleanup, cannot
//     orphan it either.

var dkvdBuild struct {
	once sync.Once
	dir  string
	bin  string
	err  error
	out  []byte
}

// buildDkvd returns the dkvd binary, building it on first use.
func buildDkvd(t *testing.T) string {
	t.Helper()
	dkvdBuild.once.Do(func() {
		dir, err := os.MkdirTemp("", "quorum-dkvd-")
		if err != nil {
			dkvdBuild.err = err
			return
		}
		dkvdBuild.dir = dir
		dkvdBuild.bin = filepath.Join(dir, "dkvd")
		args := []string{"build", "-o", dkvdBuild.bin}
		if raceEnabled {
			args = append(args, "-race")
		}
		dkvdBuild.out, dkvdBuild.err = exec.Command("go", append(args, "github.com/adivishall/quorum/cmd/dkvd")...).CombinedOutput()
	})
	if dkvdBuild.err != nil {
		t.Fatalf("go build dkvd: %v\n%s", dkvdBuild.err, dkvdBuild.out)
	}
	return dkvdBuild.bin
}

// removeBuild removes the shared binary (TestMain, after the tests).
func removeBuild() {
	if dkvdBuild.dir != "" {
		_ = os.RemoveAll(dkvdBuild.dir)
	}
}

// startProc starts cmd as a test's process: its cleanup — kill the process
// group — is registered first, and it cannot outlive the test binary.
// Killing does not reap; a test that waits for its process still does.
func startProc(t *testing.T, cmd *exec.Cmd) error {
	t.Helper()
	cmd.SysProcAttr = procAttr()
	t.Cleanup(func() {
		if cmd.Process != nil {
			killGroup(cmd.Process.Pid)
		}
	})
	return cmd.Start()
}

// artifactRoot is where a failing test writes its evidence (a history, node
// logs): $QUORUM_ARTIFACTS when set — CI uploads that directory when a job
// fails — else the system's temporary directory.
func artifactRoot() string {
	if dir := os.Getenv("QUORUM_ARTIFACTS"); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err == nil {
			return dir
		}
	}
	return ""
}
