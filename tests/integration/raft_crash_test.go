package integration

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raftlog"
)

// Phase 11 real-process crash windows (docs/CRASH_RECOVERY.md §8). A real dkvd
// process is started with -crash-at and kills itself with SIGKILL at an exact
// crash point — a boundary of the driver's persist → send → advance → apply cycle
// or an I/O boundary of the durable log — no sleep, no timing guess: the process
// logs the point and dies there. The test then reads the durable log the kill
// left behind (read-only), restarts the node on it, and requires the group to
// converge with every committed prefix intact.
//
// What this tier proves is the wiring on a real process: the seam fires where it
// says, a real SIGKILL at that instant leaves a log the real recovery path
// accepts (never refuses), the recovered term is what the file holds, and a
// real restart rejoins over real TCP. The exact durable-state contract at every
// point (what survives, what an interrupted Save may leave) is proven
// exhaustively in the deterministic simulator; timing on a live cluster is real,
// so which Save is the Nth one here depends on the election, and the assertions
// are the ones that hold whichever it was.

var (
	reCrashPoint  = regexp.MustCompile(`event=crash_point node=(\S+) point=(\S+) n=(\d+)`)
	reRaftStarted = regexp.MustCompile(`event=raft_started node=(\S+) peers=\d+ term=(\d+) lastIndex=(\d+)`)
)

// startWith is rcluster.start with extra dkvd flags (e.g. -crash-at).
func (c *rcluster) startWith(id string, extra ...string) {
	c.t.Helper()
	var peers []string
	for _, other := range c.ids {
		switch {
		case other == id:
		case id < other:
			peers = append(peers, other+"="+c.proxies[[2]string{id, other}].Addr())
		default:
			peers = append(peers, other+"="+c.addrs[other])
		}
	}
	buf := &safeBuf{}
	args := append([]string{"-id", id, "-listen", c.addrs[id], "-peers", strings.Join(peers, ","),
		"-raft", "-data-dir", c.dirs[id], "-tick-interval", "25ms"}, extra...)
	cmd := exec.Command(c.bin, args...)
	cmd.Stdout, cmd.Stderr = buf, buf
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	p := &dkvNode{id: id, addr: c.addrs[id], cmd: cmd, out: buf}
	c.procs[id] = p
	c.history = append(c.history, p)
}

// waitKilledAtPoint waits for a node to die by its own SIGKILL at a crash point
// and returns the point it logged. A node that exits any other way is a failure.
func (c *rcluster) waitKilledAtPoint(id string, d time.Duration) (point string, nth int) {
	c.t.Helper()
	p := c.procs[id]
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			c.t.Fatalf("%s exited with %v, want death by its own SIGKILL at the crash point\n%s", id, err, p.out.String())
		}
	case <-time.After(d):
		// Reap it here, not in killAll: a second concurrent Wait on the same
		// Cmd is a data race.
		_ = p.cmd.Process.Kill()
		<-done
		c.procs[id] = nil
		c.t.Fatalf("%s did not reach its crash point within %s\n%s", id, d, p.out.String())
	}
	c.procs[id] = nil
	m := reCrashPoint.FindStringSubmatch(p.out.String())
	if m == nil {
		c.t.Fatalf("%s died but never logged its crash point\n%s", id, p.out.String())
	}
	nth, _ = strconv.Atoi(m[3])
	return m[2], nth
}

// recoveredTerm returns the term a node's current process reported at raft_started.
func (c *rcluster) recoveredTerm(id string) (term, lastIndex uint64) {
	c.t.Helper()
	m := reRaftStarted.FindStringSubmatch(c.procs[id].out.String())
	if m == nil {
		c.t.Fatalf("%s did not log raft_started\n%s", id, c.procs[id].out.String())
	}
	term, _ = strconv.ParseUint(m[2], 10, 64)
	lastIndex, _ = strconv.ParseUint(m[3], 10, 64)
	return term, lastIndex
}

// TestRealCrashAtPoints kills a real dkvd process at each crash point in turn —
// driver points and I/O boundaries — while it rejoins a live 3-process group as a
// deposed leader: the others elected a new leader while it was down, so its
// catch-up Saves the new term and the new leader's no-op, sends replies, and
// re-applies its recovered prefix at boot — every point has something to die
// inside. (A follower rejoining a quiet group would never Save again: dkvd has no
// client, so the only entries are election no-ops.) Only points that occur on
// EVERY timing path are targeted: the catch-up is one Save when the new leader's
// first AppendEntries matches the old leader's log directly, or two when its own
// election timer fires first — so "the 2nd Save" and "the 3rd fsync" exist on one
// path only, and are not used. It requires: the seam fired at the named
// point; the durable log the SIGKILL left reopens (read-only inspection, so the
// file is untouched) and is coherent — no entry term above the HardState term,
// the commit within the log, the term never below the one the node had durably
// established; the restarted process recovers exactly the inspected term and
// log; the group converges on a leader everyone follows; and every prefix
// committed before the crash is at the head of every log.
func TestRealCrashAtPoints(t *testing.T) {
	points := []string{
		"before-save:1", "after-save:1", "after-send:1", "before-advance:2", "after-advance:2",
		"before-apply:1", "after-apply:1", "after-applied-to:1",
		"write:2", "write:3", "fsync:1", "fsync:2", // fsync:1 is recovery's own fsync: a crash during Open
	}
	for _, spec := range points {
		t.Run(spec, func(t *testing.T) {
			c := newRCluster(t, 3)
			l1, t1 := c.waitLeader(c.ids, 0, 20*time.Second)
			c.waitCommit(c.ids, 1, 20*time.Second)
			committed := c.committedPrefix(l1)

			// Depose the leader: the others elect a new one and commit its no-op
			// (an entry the old leader lacks). Then restart the old leader armed to
			// die at the point while it catches up.
			victim := l1
			c.kill(victim)
			l2, _ := c.waitLeader(others(c.ids, victim), t1, 20*time.Second)
			c.waitCommit(others(c.ids, victim), uint64(len(committed))+1, 20*time.Second)
			committed2 := c.committedPrefix(l2)
			c.startWith(victim, "-crash-at", spec)
			wantPoint, wantNth := spec, 1
			if i := strings.LastIndexByte(spec, ':'); i >= 0 {
				wantPoint = spec[:i]
				wantNth, _ = strconv.Atoi(spec[i+1:])
			}
			point, nth := c.waitKilledAtPoint(victim, 30*time.Second)
			if point != wantPoint || nth != wantNth {
				t.Fatalf("victim died at %s#%d, want %s#%d", point, nth, wantPoint, wantNth)
			}

			// The log the SIGKILL left: it must reopen, and be coherent.
			logPath := filepath.Join(c.dirs[victim], "raft-"+victim+".log")
			rec, err := raftlog.Inspect(logPath)
			if err != nil {
				t.Fatalf("the durable log after a SIGKILL at %s does not reopen: %v", spec, err)
			}
			if n := len(rec.Entries); n > 0 && rec.Entries[n-1].Term > rec.HardState.Term {
				t.Fatalf("after %s: entry term %d above the recovered term %d — recovery would refuse this log", spec, rec.Entries[n-1].Term, rec.HardState.Term)
			}
			if rec.HardState.Commit > uint64(len(rec.Entries)) {
				t.Fatalf("after %s: recovered commit %d beyond the %d recovered entries", spec, rec.HardState.Commit, len(rec.Entries))
			}
			if t1 > 0 && rec.HardState.Term < t1 && len(rec.Entries) > 0 {
				// The victim had persisted term t1 (it followed l1 and holds the no-op)
				// before it was first killed; a crash can never regress that.
				t.Fatalf("after %s: recovered term %d below the term %d the node had durably established", spec, rec.HardState.Term, t1)
			}

			// Restart it for real and require it to recover exactly what the file
			// holds, then converge and keep everything committed.
			c.start(victim)
			waitForLine(t, c.procs[victim], "event=raft_started", 20*time.Second)
			term, last := c.recoveredTerm(victim)
			if term != rec.HardState.Term || last != uint64(len(rec.Entries)) {
				t.Fatalf("after %s: the restarted process recovered term %d lastIndex %d, but the file holds term %d and %d entries", spec, term, last, rec.HardState.Term, len(rec.Entries))
			}
			l, _ := c.waitStable(c.ids, 0, 30*time.Second)
			c.waitCommit(c.ids, c.commitOf(l), 20*time.Second)
			c.finish(committed, committed2)
		})
	}
}

// TestRealCrashAtEveryEarlyPointIsRecoverable sweeps the first few occurrences of
// every driver point and every I/O boundary on a single-node group — whose
// election Save carries the term, its self-vote and the no-op at once, the
// window the crash matrix found — and requires each SIGKILL to leave a log that
// reopens with a term no lower than any entry it holds, and each restart to
// climb to a higher term (reachable only from the recovered one).
func TestRealCrashAtEveryEarlyPointIsRecoverable(t *testing.T) {
	bin := buildDkvd(t)
	var specs []string
	for _, p := range []string{"before-save", "after-save", "before-advance", "after-advance", "before-apply", "after-apply", "after-applied-to"} {
		specs = append(specs, p+":1")
	}
	for _, p := range []string{"write:1", "write:2", "write:3", "fsync:1", "fsync:2"} {
		specs = append(specs, p)
	}
	for _, spec := range specs {
		t.Run(spec, func(t *testing.T) {
			addr := freeTCPAddr(t)
			dir := filepath.Join(t.TempDir(), "n0")
			logPath := filepath.Join(dir, "raft-n0.log")
			buf := &safeBuf{}
			cmd := exec.Command(bin, "-id", "n0", "-listen", addr, "-raft", "-data-dir", dir, "-tick-interval", "25ms", "-crash-at", spec)
			cmd.Stdout, cmd.Stderr = buf, buf
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err := <-done:
				var ee *exec.ExitError
				if !errors.As(err, &ee) || ee.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
					t.Fatalf("exited with %v, want SIGKILL at %s\n%s", err, spec, buf.String())
				}
			case <-time.After(20 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("%s never fired\n%s", spec, buf.String())
			}
			if !reCrashPoint.MatchString(buf.String()) {
				t.Fatalf("no crash_point event logged\n%s", buf.String())
			}
			rec, err := raftlog.Inspect(logPath)
			if err != nil {
				t.Fatalf("log after SIGKILL at %s does not reopen: %v", spec, err)
			}
			if n := len(rec.Entries); n > 0 && rec.Entries[n-1].Term > rec.HardState.Term {
				t.Fatalf("after %s: entry term %d above HardState term %d", spec, rec.Entries[n-1].Term, rec.HardState.Term)
			}
			// Restart: it must recover that state and elect itself in a HIGHER term.
			n2 := launchRaftNode(t, bin, addr, dir)
			defer func() { _ = n2.cmd.Process.Kill(); _ = n2.cmd.Wait() }()
			waitForLine(t, n2, fmt.Sprintf("event=raft_started node=n0 peers=1 term=%d lastIndex=%d", rec.HardState.Term, len(rec.Entries)), 15*time.Second)
			waitForLine(t, n2, fmt.Sprintf("event=raft_leader node=n0 term=%d", rec.HardState.Term+1), 15*time.Second)
		})
	}
}
