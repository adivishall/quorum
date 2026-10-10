package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
)

// dkvd -state-machine on a real process (S2, docs/STORAGE_INTEGRATION.md §8.5).

// TestRealStateMachineKindIsAnOperatorsChoice: a node that ran on the LSM
// machine refuses to restart as memory — its engine would be ignored and go
// stale behind the memory machine's rebuild — naming why; and a node that ran
// in memory may be restarted on the engine, which bootstraps from the
// published snapshot and the log and serves what the node held.
func TestRealStateMachineKindIsAnOperatorsChoice(t *testing.T) {
	// Every node in memory, whatever the suite runs on; only n1 switches.
	c := newRClusterEvery(t, 3, []string{"-state-machine", "memory"})
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	if resp := c.do(kv.Request{Op: kv.ReqPut, Key: []byte("k"), Value: []byte("v")}); resp.Status != kv.StatusOK {
		t.Fatalf("put: %+v", resp)
	}
	engine := filepath.Join(c.dirs["n1"], "lsm")
	if _, err := os.Stat(engine); err == nil {
		t.Fatal("a memory node made an engine directory")
	}

	// memory -> lsm: allowed. The engine is built from the published snapshot
	// and the log: n1 catches up to the group's commit, into its engine.
	c.kill("n1")
	c.launch("n1", []string{"-state-machine", "lsm"})
	waitForLine(t, c.procs["n1"], "event=client_ready", 20*time.Second)
	if !strings.Contains(strings.Join(c.procs["n1"].cmd.Args, " "), "-state-machine lsm") {
		t.Fatal("premise: the restart did not carry -state-machine lsm")
	}
	if resp := c.do(kv.Request{Op: kv.ReqPut, Key: []byte("k2"), Value: []byte("v2")}); resp.Status != kv.StatusOK {
		t.Fatalf("put with n1 on lsm: %+v", resp)
	}
	l, _ := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit([]string{"n1"}, c.commitOf(l), 20*time.Second)
	if _, err := os.Stat(filepath.Join(engine, "CURRENT")); err != nil {
		t.Fatalf("n1 on lsm has no engine: %v", err)
	}

	// lsm -> memory: refused before Raft starts, naming the engine.
	c.kill("n1")
	c.launch("n1", []string{"-state-machine", "memory"})
	p := c.procs["n1"]
	if err := waitExit(p, 10*time.Second); err == nil {
		t.Fatalf("an lsm node restarted as memory kept running:\n%s", p.out.String())
	}
	c.procs["n1"] = nil // reaped
	out := p.out.String()
	if !strings.Contains(out, "holds an LSM state machine") || strings.Contains(out, "event=raft_started") {
		t.Fatalf("the restart as memory was not refused before starting Raft:\n%s", out)
	}

	// Back on lsm, n1 recovers its engine and rejoins; the group holds all.
	c.launch("n1", []string{"-state-machine", "lsm"})
	waitForLine(t, c.procs["n1"], "event=client_ready", 20*time.Second)
	l, _ = c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitCommit([]string{"n1"}, c.commitOf(l), 20*time.Second)
	for k, v := range map[string]string{"k": "v", "k2": "v2"} {
		if resp := c.do(kv.Request{Op: kv.ReqGet, Key: []byte(k)}); resp.Status != kv.StatusOK || string(resp.Value) != v {
			t.Fatalf("get %s after the restart on lsm: %+v", k, resp)
		}
	}
}
