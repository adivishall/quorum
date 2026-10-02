package integration

import (
	"bytes"
	"io"
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
//     (raceEnabled), so the real processes are race-checked too: a race-built
//     process that reports a data race fails its test. It prints the report
//     and goes on running, and a SIGKILLed process never exits with the race
//     detector's status, so its output is the evidence (raceWatch);
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
	// One watch for both streams when they are one writer, so its writes stay
	// serialized as exec.Cmd promises for that case. A file (a StdoutPipe, the
	// test's own stderr) is handed to the process as it is: wrapping it would
	// put a copy between them, and a pipe's write end is closed at start.
	var watches []*raceWatch
	watch := func(w io.Writer) io.Writer {
		if _, file := w.(*os.File); file {
			return w
		}
		rw := &raceWatch{w: w}
		watches = append(watches, rw)
		return rw
	}
	if cmd.Stderr == cmd.Stdout {
		cmd.Stdout = watch(cmd.Stdout)
		cmd.Stderr = cmd.Stdout
	} else {
		cmd.Stdout, cmd.Stderr = watch(cmd.Stdout), watch(cmd.Stderr)
	}
	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		killGroup(cmd.Process.Pid)
		// A report is written when the race happens, while the test runs.
		// (No Wait here: a test may still be waiting for the process.)
		for _, w := range watches {
			if report, ok := w.report(); ok {
				t.Errorf("%s reported a data race:\n%s", filepath.Base(cmd.Path), report)
				return
			}
		}
	})
	return cmd.Start()
}

// raceMarker opens every race detector report.
var raceMarker = []byte("WARNING: DATA RACE")

// raceWatch passes a process's output on to w (if any) and remembers the
// first race report in it — a marker split across writes included.
type raceWatch struct {
	mu    sync.Mutex
	w     io.Writer
	tail  []byte // the last bytes seen, for a marker split across writes
	rep   []byte
	found bool
}

func (r *raceWatch) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.found {
		buf := append(r.tail, p...)
		if i := bytes.Index(buf, raceMarker); i >= 0 {
			r.found = true
			r.rep = append([]byte(nil), buf[i:]...)
		} else if keep := len(raceMarker) - 1; len(buf) > keep {
			r.tail = append(r.tail[:0], buf[len(buf)-keep:]...)
		} else {
			r.tail = buf
		}
	} else if len(r.rep) < 8<<10 {
		r.rep = append(r.rep, p...)
	}
	if r.w == nil {
		return len(p), nil
	}
	return r.w.Write(p)
}

// report returns the start of the first race report, if there was one.
func (r *raceWatch) report() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.rep), r.found
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
