package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/nodedir"
)

// initFlags are the flags a node's start needs for its data directory (audit
// H1, internal/nodedir): -init and the cluster id on the directory's first use,
// nothing once the directory holds a node — what an operator passes to a new
// node, and never to one being restarted. A node whose first start died before
// its directory recorded a node therefore gets -init again, and one that
// recorded it resumes its initialization without it.
func initFlags(dir, cluster string) []string {
	if cluster == "" {
		panic("integration harness: a cluster with no cluster id (use newClusterID)")
	}
	if _, err := os.Stat(filepath.Join(dir, nodedir.IdentityFile)); err == nil {
		return nil
	}
	return []string{"-init", "-cluster-id", cluster}
}

var clusterSeq atomic.Uint64

// newClusterID names one test cluster uniquely, so a stray process of another
// cluster can never be mistaken for one of its nodes.
func newClusterID() string {
	return fmt.Sprintf("itest-%d-%d", time.Now().UnixNano(), clusterSeq.Add(1))
}

// TestRealWipedNodeIsRefused (audit H1): a member of a running three-node
// group loses its data directory and is restarted with its ordinary flags —
// the operator's unit, which never carries -init. Before the data directory
// rules it started empty under its old id: term 0, no vote, no log, a voter
// its peers counted on for state it no longer held. Now it is refused (exit
// 2, naming why) and nothing is written; the other two go on serving; and a
// second process on a running node's directory is refused too.
func TestRealWipedNodeIsRefused(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)

	// A second process on a running node's directory (its own ports, so only
	// the directory is shared): refused, the first unharmed.
	buf := &safeBuf{}
	cmd := exec.Command(c.bin, "-id", "n1", "-listen", freeTCPAddr(t), "-raft", "-data-dir", c.dirs["n1"], "-tick-interval", "25ms")
	cmd.Stdout, cmd.Stderr = buf, buf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitExit(&dkvNode{id: "n1-second", cmd: cmd, out: buf}, 10*time.Second); err == nil || !strings.Contains(buf.String(), "in use by another process") {
		t.Fatalf("a second process on n1's directory: %v\n%s", err, buf.String())
	}

	// n3 loses its directory and is restarted as it always is.
	c.kill("n3")
	if err := os.RemoveAll(c.dirs["n3"]); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(c.dirs["n3"], 0o700); err != nil {
		t.Fatal(err)
	}
	c.startPlain("n3")
	p := c.procs["n3"]
	if err := waitExit(p, 10*time.Second); err == nil {
		t.Fatalf("the wiped n3 kept running:\n%s", p.out.String())
	}
	out := p.out.String()
	if !strings.Contains(out, "holds no node") || strings.Contains(out, "event=raft_started") {
		t.Fatalf("the wiped n3 was not refused before starting Raft:\n%s", out)
	}
	if entries, err := os.ReadDir(c.dirs["n3"]); err != nil || len(entries) > 1 { // LOCK at most
		t.Fatalf("the refused start wrote into the wiped directory: %v %v", entries, err)
	}
	c.procs["n3"] = nil

	// The two survivors are a majority and keep serving.
	leader, _ := c.waitLeader([]string{"n1", "n2"}, 0, 20*time.Second)
	ep := &procEndpoint{node: leader, addr: c.kvAddrs[leader]}
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = ep.Put(ctx, []byte("k"), []byte("v"))
		cancel()
		if err == nil {
			break
		}
		leader, _ = c.waitLeader([]string{"n1", "n2"}, 0, 20*time.Second)
		ep = &procEndpoint{node: leader, addr: c.kvAddrs[leader]}
	}
	if err != nil {
		t.Fatalf("the two survivors do not serve a write: %v", err)
	}
}
