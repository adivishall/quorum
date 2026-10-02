package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/multiraft"
	"github.com/adivishall/quorum/internal/nodedir"
	"github.com/adivishall/quorum/internal/routing"
	"github.com/adivishall/quorum/internal/transport"
)

// The data directory rules (audit H1, internal/nodedir), through dkvd's own
// raft-mode code on a single-node group: what a start is refused for, and
// that a refusal is a configuration error (exit 2) that writes no Raft state.

// soloRun is a running single-node raft-mode dkvd.
type soloRun struct {
	out    *syncBuffer
	errb   *syncBuffer
	cancel context.CancelFunc
	done   chan int
}

func startSolo(t *testing.T, id, dir string, init bool, cluster string) *soloRun {
	t.Helper()
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: transport.NodeID(id), ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	s := &soloRun{out: &syncBuffer{}, errb: &syncBuffer{}, cancel: cancel, done: make(chan int, 1)}
	go func() {
		s.done <- runRaft(ctx, raftRun{id: id, tr: tr, dataDir: dir, init: init, clusterID: cluster,
			tick: 5 * time.Millisecond, lg: &logger{w: s.out}, stderr: s.errb})
	}()
	t.Cleanup(func() { cancel(); <-s.done })
	return s
}

// exitCode waits for the run to end by itself.
func (s *soloRun) exitCode(t *testing.T) int {
	t.Helper()
	select {
	case code := <-s.done:
		s.done <- code // for the cleanup
		return code
	case <-time.After(5 * time.Second):
		t.Fatalf("dkvd kept running; stderr %q", s.errb.String())
		return -1
	}
}

// stop cancels the run and requires a clean exit.
func (s *soloRun) stop(t *testing.T) {
	t.Helper()
	s.cancel()
	if code := s.exitCode(t); code != 0 {
		t.Fatalf("exit code %d after a clean shutdown; stderr %q", code, s.errb.String())
	}
}

// refused requires the start to exit 2 naming why, without writing any Raft
// state.
func refused(t *testing.T, s *soloRun, dir, why string) {
	t.Helper()
	if code := s.exitCode(t); code != 2 {
		t.Fatalf("exit code %d, want 2 (%s); stderr %q", code, why, s.errb.String())
	}
	if !strings.Contains(s.errb.String(), why) {
		t.Fatalf("stderr %q does not say %q", s.errb.String(), why)
	}
	if strings.Contains(s.out.String(), "event=raft_started") {
		t.Fatalf("a refused start started Raft:\n%s", s.out.String())
	}
}

func TestDataDirectoryRules(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "solo")

	t.Run("a fresh directory without -init is refused", func(t *testing.T) {
		refused(t, startSolo(t, "solo", dir, false, "c1"), dir, "does not exist")
	})
	t.Run("-init without a cluster id is refused", func(t *testing.T) {
		refused(t, startSolo(t, "solo", dir, true, ""), dir, "cluster id")
	})

	// The first start initializes the directory and commits.
	s := startSolo(t, "solo", dir, true, "c1")
	waitFor(t, s.out, "event=data_dir_initialized node=solo", 5*time.Second)
	waitFor(t, s.out, "event=raft_commit node=solo index=1", 5*time.Second)

	t.Run("a second process on the directory is refused while the first runs", func(t *testing.T) {
		refused(t, startSolo(t, "solo", dir, false, ""), dir, "in use by another process")
	})
	s.stop(t)

	t.Run("a restart recovers its own state, with or without the cluster id", func(t *testing.T) {
		for _, cluster := range []string{"", "c1"} {
			r := startSolo(t, "solo", dir, false, cluster)
			waitFor(t, r.out, "event=data_dir node=solo dir="+dir+" cluster=c1 state=running", 5*time.Second)
			waitFor(t, r.out, "event=raft_started node=solo peers=1 term=", 5*time.Second)
			if strings.Contains(r.out.String(), "term=0 lastIndex=0") {
				t.Fatalf("the restart did not recover the durable state:\n%s", r.out.String())
			}
			r.stop(t)
		}
	})
	t.Run("-init on an initialized directory is refused", func(t *testing.T) {
		refused(t, startSolo(t, "solo", dir, true, "c1"), dir, "already initialized")
	})
	t.Run("another node id is refused", func(t *testing.T) {
		refused(t, startSolo(t, "other", dir, false, "c1"), dir, `belongs to node "solo"`)
	})
	t.Run("another cluster id is refused", func(t *testing.T) {
		refused(t, startSolo(t, "solo", dir, false, "c2"), dir, `belongs to cluster "c1"`)
	})
	t.Run("an initialized directory whose group state was lost is refused", func(t *testing.T) {
		lost := filepath.Join(t.TempDir(), "lost")
		r := startSolo(t, "solo", lost, true, "c1")
		waitFor(t, r.out, "event=raft_commit node=solo index=1", 5*time.Second)
		r.stop(t)
		for _, f := range []string{"raft-solo.log", "raft-solo.log.group"} {
			if err := os.Remove(filepath.Join(lost, f)); err != nil {
				t.Fatal(err)
			}
		}
		refused(t, startSolo(t, "solo", lost, false, ""), lost, "has no state")
	})
	t.Run("a wiped directory is refused without -init", func(t *testing.T) {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		refused(t, startSolo(t, "solo", dir, false, "c1"), dir, "does not exist")
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("a refused start created the wiped directory: %v", err)
		}
		if err := os.Mkdir(dir, 0o700); err != nil { // wiped, the directory itself kept
			t.Fatal(err)
		}
		refused(t, startSolo(t, "solo", dir, false, "c1"), dir, "holds no node")
	})
}

// TestRunRequiresADataDirAndAPositiveTick (audit H1, M5): raft and cluster
// modes refuse to run without a data directory — a temporary one would be
// forgotten by the next start, votes and log with it — and a non-positive
// tick is configuration, not a panic in every group's actor.
func TestRunRequiresADataDirAndAPositiveTick(t *testing.T) {
	base := []string{"-id", "a", "-listen", "127.0.0.1:0"}
	for _, c := range []struct {
		args []string
		why  string
	}{
		{[]string{"-raft"}, "require -data-dir"},
		{[]string{"-cluster"}, "require -data-dir"},
		{[]string{"-raft", "-data-dir", t.TempDir(), "-tick-interval", "0"}, "-tick-interval must be positive"},
		{[]string{"-raft", "-data-dir", t.TempDir(), "-tick-interval", "-1ms"}, "-tick-interval must be positive"},
		{[]string{"-tick-interval", "-5s"}, "-tick-interval must be positive"},
		{[]string{"-init"}, "require -raft or -cluster"},
		{[]string{"-cluster-id", "c1"}, "require -raft or -cluster"},
		{[]string{"-raft", "-data-dir", t.TempDir(), "-init", "-cluster-id", "has space"}, "cluster id"},
	} {
		var out, errb bytes.Buffer
		if code := run(context.Background(), append(append([]string(nil), base...), c.args...), &out, &errb); code != 2 {
			t.Fatalf("%v: exit code %d, want 2 (stderr %q)", c.args, code, errb.String())
		}
		if !strings.Contains(errb.String(), c.why) {
			t.Fatalf("%v: stderr %q does not say %q", c.args, errb.String(), c.why)
		}
	}
}

// runNode runs dkvd with args until cancelled, returning its output and a wait
// for its exit code.
func runNode(t *testing.T, args ...string) (out *syncBuffer, errb *syncBuffer, cancel context.CancelFunc, wait func() int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out, errb = &syncBuffer{}, &syncBuffer{}
	done := make(chan int, 1)
	go func() { done <- run(ctx, args, out, errb) }()
	var code *int
	wait = func() int {
		if code == nil {
			select {
			case c := <-done:
				code = &c
			case <-time.After(10 * time.Second):
				t.Fatalf("dkvd %v did not exit; stderr %q", args, errb.String())
			}
		}
		return *code
	}
	t.Cleanup(func() { cancel(); wait() })
	return out, errb, cancel, wait
}

// TestALostJoinGroupIsReportedNotRecreated (audit H1): in -cluster mode, a
// group this node joined (-join) whose state was lost from an initialized
// directory is reported, as a lost genesis group is, not created again empty:
// the joiner may have been promoted, voted and acknowledged entries, and an
// empty replica under its id would do so again without them.
func TestALostJoinGroupIsReportedNotRecreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "z")
	base := []string{"-id", "z", "-listen", "127.0.0.1:0", "-cluster", "-nodes", "a", "-rf", "1", "-shards", "1",
		"-data-dir", dir, "-tick-interval", "5ms", "-join", "0"}
	out, _, cancel, wait := runNode(t, append(append([]string(nil), base...), "-init", "-cluster-id", "c1")...)
	waitFor(t, out, "event=data_dir_initialized node=z", 5*time.Second)
	if !strings.Contains(out.String(), "event=group_started node=z group=0") {
		t.Fatalf("premise: the joiner did not start group 0:\n%s", out.String())
	}
	cancel()
	if code := wait(); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if err := os.RemoveAll(multiraft.GroupDir(dir, 0)); err != nil {
		t.Fatal(err)
	}
	out, _, cancel, wait = runNode(t, base...)
	waitFor(t, out, "event=group_failed node=z group=0", 5*time.Second)
	if !strings.Contains(out.String(), "its state was lost") {
		t.Fatalf("the report does not say the state was lost:\n%s", out.String())
	}
	if strings.Contains(out.String(), "event=group_started node=z group=0") {
		t.Fatalf("the lost joiner group was created again empty:\n%s", out.String())
	}
	if _, err := os.Stat(multiraft.GroupDir(dir, 0)); !os.IsNotExist(err) {
		t.Fatalf("the refused group's directory was created again: %v", err)
	}
	cancel()
	_ = wait()
}

// TestReplicaSettingsArePinned (audit H5): the settings that are part of the
// replicated state machine's definition — the session limits; in -cluster
// mode the routing — are recorded when a node's directory is initialized, and
// a restart with others is refused: two replicas deciding the same entries
// under different limits diverge. A restart repeating them runs.
func TestReplicaSettingsArePinned(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "n")
	base := []string{"-id", "solo", "-listen", "127.0.0.1:0", "-raft", "-data-dir", dir, "-tick-interval", "5ms"}
	out, _, cancel, wait := runNode(t, append(append([]string(nil), base...), "-init", "-cluster-id", "c1", "-session-max", "5")...)
	waitFor(t, out, "event=raft_commit node=solo index=1", 5*time.Second)
	cancel()
	if code := wait(); code != 0 {
		t.Fatalf("exit %d", code)
	}
	for _, other := range [][]string{{"-session-max", "6"}, {}, {"-session-max", "5", "-session-max-unacked", "7"}} {
		_, errb, _, wait := runNode(t, append(append([]string(nil), base...), other...)...)
		if code := wait(); code != 2 || !strings.Contains(errb.String(), "replica settings") {
			t.Fatalf("a restart with %v: exit %d, stderr %q; want 2 naming the replica settings", other, code, errb.String())
		}
	}
	out, _, cancel, wait = runNode(t, append(append([]string(nil), base...), "-session-max", "5")...)
	waitFor(t, out, "event=raft_leader node=solo", 5*time.Second)
	cancel()
	if code := wait(); code != 0 {
		t.Fatalf("a restart repeating the settings: exit %d", code)
	}
}

// TestRoutingFlagsBelongToClusterMode (audit H5/D5): -shards, -rf and -nodes
// define -cluster mode's groups; given in another mode they were silently
// ignored, and are refused now. And in -cluster mode, a node initialized
// outside the routing that joins nothing would host nothing: refused.
func TestRoutingFlagsBelongToClusterMode(t *testing.T) {
	base := []string{"-id", "a", "-listen", "127.0.0.1:0"}
	for _, extra := range [][]string{
		{"-raft", "-data-dir", t.TempDir(), "-shards", "8"},
		{"-raft", "-data-dir", t.TempDir(), "-rf", "1"},
		{"-raft", "-data-dir", t.TempDir(), "-nodes", "a,b"},
		{"-nodes", "a"},
	} {
		var out, errb bytes.Buffer
		if code := run(context.Background(), append(append([]string(nil), base...), extra...), &out, &errb); code != 2 || !strings.Contains(errb.String(), "applies to -cluster mode only") {
			t.Fatalf("%v: exit %d, stderr %q", extra, code, errb.String())
		}
	}
	_, errb, _, wait := runNode(t, "-id", "z", "-listen", "127.0.0.1:0", "-cluster", "-nodes", "a,b,c", "-rf", "3",
		"-data-dir", t.TempDir(), "-init", "-cluster-id", "c1")
	if code := wait(); code != 2 || !strings.Contains(errb.String(), "would host no group") {
		t.Fatalf("a node outside the routing joining nothing: exit %d, stderr %q", code, errb.String())
	}
}

// TestReplicaSettingsRendering: the rendering is canonical — the node order
// of -nodes does not change it (the routing sorts its nodes too) — and every
// setting appears in it.
func TestReplicaSettingsRendering(t *testing.T) {
	limits := kv.Limits{MaxSessions: 3, MaxUnacked: 4}
	a := replicaSettings(true, limits, 4, 3, []routing.NodeID{"n2", "n1", "n3"})
	b := replicaSettings(true, limits, 4, 3, []routing.NodeID{"n3", "n2", "n1"})
	if a != b || a != "mode=cluster shards=4 rf=3 nodes=n1,n2,n3 session-max=3 session-max-unacked=4" {
		t.Fatalf("cluster settings: %q vs %q", a, b)
	}
	if got := replicaSettings(false, limits, 4, 3, nil); got != "mode=raft session-max=3 session-max-unacked=4" {
		t.Fatalf("raft settings: %q", got)
	}
}

// unfinishedInit records node z's identity in dir as an -init that stopped
// before any group (cluster c1, the default session limits, a one-node
// routing of shards groups), and puts a file where group blocked's directory
// goes, so that group cannot be recorded.
func unfinishedInit(t *testing.T, dir string, shards int, blocked multiraft.GroupID) (blocker string) {
	t.Helper()
	settings := replicaSettings(true, kv.Limits{MaxSessions: kv.DefaultLimits.MaxSessions, MaxUnacked: kv.DefaultLimits.MaxUnacked},
		shards, 1, []routing.NodeID{"z"})
	nd, err := nodedir.Open(dir, nodedir.Options{Node: "z", Cluster: "c1", Init: true, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	_ = nd.Close()
	if err := os.MkdirAll(filepath.Join(dir, "groups"), 0o755); err != nil {
		t.Fatal(err)
	}
	blocker = multiraft.GroupDir(dir, blocked)
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return blocker
}

// TestAnUnfinishedInitRunsNoGroup (audit H1): an initialization records every
// group's identity, then itself, before any group runs. When a group cannot be
// recorded, the start exits 2 with nothing started; a later start resumes and
// finishes it; and a group that ran and then lost its state is reported, not
// created again empty. Before, a -cluster start ran the groups it could create
// as voting members while the initialization stayed unfinished, and every
// later start — still initializing — created a lost one again empty.
func TestAnUnfinishedInitRunsNoGroup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "z")
	blocker := unfinishedInit(t, dir, 2, 1)
	base := []string{"-id", "z", "-listen", "127.0.0.1:0", "-cluster", "-nodes", "z", "-rf", "1", "-shards", "2",
		"-data-dir", dir, "-tick-interval", "5ms"}
	out, errb, _, wait := runNode(t, base...)
	if code := wait(); code != 2 || !strings.Contains(errb.String(), "initializing group 1") {
		t.Fatalf("an initialization that cannot record group 1: exit %d, stderr %q", code, errb.String())
	}
	for _, ev := range []string{"event=raft_started", "event=group_started", "event=data_dir_initialized"} {
		if strings.Contains(out.String(), ev) {
			t.Fatalf("the failed initialization ran something (%s):\n%s", ev, out.String())
		}
	}
	// Resumed once the obstacle is gone: it finishes, and the groups run.
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	out, _, cancel, wait := runNode(t, base...)
	waitFor(t, out, "event=data_dir_initialized node=z", 5*time.Second)
	waitFor(t, out, "event=raft_commit node=z index=1 group=0", 5*time.Second)
	cancel()
	if code := wait(); code != 0 {
		t.Fatalf("exit %d", code)
	}
	// Group 0 — elected, committed — loses its state: reported, not recreated.
	if err := os.RemoveAll(multiraft.GroupDir(dir, 0)); err != nil {
		t.Fatal(err)
	}
	out, _, cancel, wait = runNode(t, base...)
	waitFor(t, out, "event=group_failed node=z group=0", 5*time.Second)
	if !strings.Contains(out.String(), "its state was lost") || strings.Contains(out.String(), "event=group_started node=z group=0") {
		t.Fatalf("the lost group 0 was not reported as lost, or was created again:\n%s", out.String())
	}
	cancel()
	_ = wait()
}

// TestAnInitRefusedByItsFlagsRecordsNothing: a check decidable from the flags
// alone — this node is in no group of the routing and joins none — refuses
// before the data directory records anything, so the corrected retry is not
// refused for settings no group was ever created under.
func TestAnInitRefusedByItsFlagsRecordsNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "z")
	args := func(nodes string) []string {
		return []string{"-id", "z", "-listen", "127.0.0.1:0", "-cluster", "-nodes", nodes, "-rf", "1", "-shards", "1",
			"-data-dir", dir, "-tick-interval", "5ms", "-init", "-cluster-id", "c1"}
	}
	_, errb, _, wait := runNode(t, args("a")...)
	if code := wait(); code != 2 || !strings.Contains(errb.String(), "would host no group") {
		t.Fatalf("a node outside the routing: exit %d, stderr %q", code, errb.String())
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("the refused start created its data directory: %v", err)
	}
	out, _, cancel, wait := runNode(t, args("z")...)
	waitFor(t, out, "event=data_dir_initialized node=z", 5*time.Second)
	cancel()
	if code := wait(); code != 0 {
		t.Fatalf("the corrected retry: exit %d", code)
	}
}

// TestAStartupErrorClosesTheTransport: run returns 2 from a startup error
// after its transport listens, and the listen address is free again. The
// error paths returned without closing it: an in-process caller (these tests)
// kept its listener and goroutines.
func TestAStartupErrorClosesTheTransport(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	dir := filepath.Join(t.TempDir(), "z")
	unfinishedInit(t, dir, 1, 0) // group 0 cannot be recorded: runRaft fails after the transport listens
	var out, errb syncBuffer
	code := run(context.Background(), []string{"-id", "z", "-listen", addr, "-cluster", "-nodes", "z", "-rf", "1", "-shards", "1",
		"-data-dir", dir, "-tick-interval", "5ms"}, &out, &errb)
	if code != 2 || !strings.Contains(out.String(), "event=ready") {
		t.Fatalf("premise: exit %d after the transport listened (stdout %q, stderr %q)", code, out.String(), errb.String())
	}
	l2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("after run returned 2 its transport still holds %s: %v", addr, err)
	}
	_ = l2.Close()
}
