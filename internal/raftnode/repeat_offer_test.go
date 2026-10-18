package raftnode

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/snapshot"
)

// A leader offers a snapshot again when its follower has not answered within
// SnapshotRetryTicks, and a large snapshot's proof and install outlast that.
// Before S4's closeout the repeat was received whole into the follower's
// transport queue while its actor was busy with the first copy — a second copy
// of the state in memory — and then written, proven and ignored. These pin the
// fix: the node's frame pump drops a transfer of a snapshot no newer than the
// one being proven or installed (Snapshots.Installing), and only then.

// probeSM is snapSM recording, at each proof, which snapshot the node reports
// it is installing.
type probeSM struct {
	*snapSM
	snaps *Snapshots
	seen  []uint64
	from  []string
}

func (p *probeSM) ValidateSnapshotFrom(index uint64, r io.Reader) error {
	from, idx := p.snaps.Installing()
	p.seen = append(p.seen, idx)
	p.from = append(p.from, from)
	return p.snapSM.ValidateSnapshotFrom(index, r)
}

// TestInstallingCoversExactlyTheProofAndTheInstall: Installing names a
// snapshot from the chunk that completes its transfer — in many chunks or in
// one — until the core installs or unstages it: never while a transfer is
// still arriving, and never after a refused proof. A refused snapshot left
// "installing" would have every later offer of it dropped, and the follower
// could not catch up.
func TestInstallingCoversExactlyTheProofAndTheInstall(t *testing.T) {
	conf := replication.VotersOf([]NodeID{"n0", "n1", "n2"})
	sm := &probeSM{snapSM: &snapSM{}}
	snaps := &Snapshots{Files: snapshot.Files{FS: fault.NewMemFS(), Base: "/n/raft.log"}, SM: sm, Group: 3}
	sm.snaps = snaps
	src := &snapSM{}
	for i := 1; i <= 3; i++ {
		_ = src.Apply(uint64(i), []byte("a command of some length"))
	}
	_, state, _ := src.EncodeSnapshot()
	send := func(meta snapshot.Meta, size int) (*raft.Message, error) {
		t.Helper()
		file, err := snapshot.Encode(meta, state)
		if err != nil {
			t.Fatal(err)
		}
		chunks := snapshot.SplitSize(2, meta, file, size)
		for i, c := range chunks[:len(chunks)-1] {
			if m, err := snaps.Receive("n1", c.Marshal()); m != nil || err != nil {
				t.Fatalf("chunk %d of %d: %v %v", i+1, len(chunks), m, err)
			}
			if _, got := snaps.Installing(); got != 0 {
				t.Fatalf("chunk %d of %d: installing %d while the transfer is still arriving", i+1, len(chunks), got)
			}
		}
		return snaps.Receive("n1", chunks[len(chunks)-1].Marshal())
	}
	if _, got := snaps.Installing(); got != 0 {
		t.Fatalf("installing %d before any transfer", got)
	}

	// A refused proof: the state is at 3, the snapshot claims 4.
	if m, err := send(snapshot.Meta{Group: 3, Conf: conf, Index: 4, Term: 1}, 16); m != nil || err == nil {
		t.Fatalf("a state the machine refuses: %v %v", m, err)
	}
	if want := []uint64{4}; !slices.Equal(sm.seen, want) || !slices.Equal(sm.from, []string{"n1"}) {
		t.Fatalf("installing during the proof: %v from %v, want %v from [n1]", sm.seen, sm.from, want)
	}
	if from, got := snaps.Installing(); got != 0 || from != "" {
		t.Fatalf("installing %d from %q after its proof was refused: every later offer of it would be dropped", got, from)
	}

	// A valid one, in many chunks and then in one.
	for _, size := range []int{16, snapshot.MaxChunk} {
		sm.seen = nil
		m, err := send(snapshot.Meta{Group: 3, Conf: conf, Index: 3, Term: 1}, size)
		if m == nil || err != nil {
			t.Fatalf("chunks of %d bytes: the valid snapshot: %v %v", size, m, err)
		}
		if want := []uint64{3}; !slices.Equal(sm.seen, want) {
			t.Fatalf("chunks of %d bytes: installing during the proof: %v, want %v", size, sm.seen, want)
		}
		if from, got := snaps.Installing(); got != 3 || from != "n1" {
			t.Fatalf("chunks of %d bytes: installing %d from %q once staged, want 3 from n1", size, got, from)
		}
		snaps.Unstage()
		if _, got := snaps.Installing(); got != 0 {
			t.Fatalf("chunks of %d bytes: installing %d after the core unstaged it", size, got)
		}
	}
}

// TestOnlyTheSendersOwnRepeatIsDropped: the pump's rule. While n1's snapshot
// at 100 is being proven or installed, n1's chunks at or below 100 are its
// repeated offer and are dropped; n1's chunks above 100 are a newer snapshot
// and pass; another sender's chunks pass at any index — it may be a newer
// leader whose snapshot the node needs if the one from n1 is a stale leader's
// (independent review of S4); and with no install under way nothing is dropped.
func TestOnlyTheSendersOwnRepeatIsDropped(t *testing.T) {
	for _, tc := range []struct {
		name       string
		peer       string
		index      uint64
		from       string
		installing uint64
		want       bool
	}{
		{"the sender's repeat at the same index", "n1", 100, "n1", 100, true},
		{"the sender's repeat of an older snapshot", "n1", 90, "n1", 100, true},
		{"the sender's newer snapshot", "n1", 101, "n1", 100, false},
		{"another sender at the same index", "n2", 100, "n1", 100, false},
		{"another sender at a lower index", "n2", 90, "n1", 100, false},
		{"no install under way", "n1", 100, "", 0, false},
	} {
		if got := dropsRepeat(tc.peer, tc.index, tc.from, tc.installing); got != tc.want {
			t.Errorf("%s: dropsRepeat(%q, %d, %q, %d) = %v, want %v", tc.name, tc.peer, tc.index, tc.from, tc.installing, got, tc.want)
		}
	}
}

// TestARepeatedOfferIsDroppedWhileTheFirstIsProven: a lagging follower whose
// proof of a snapshot takes longer than the leader waits for an answer (here
// 2 s against 40 ticks of 15 ms) is offered the snapshot again meanwhile. The
// repeats are dropped as they arrive — the follower proves the snapshot once,
// not once per offer — and it still catches up, and afterwards nothing is
// marked as installing.
func TestARepeatedOfferIsDroppedWhileTheFirstIsProven(t *testing.T) {
	ctx := context.Background()
	c := startSnapCluster(t, ctx, 3, 16, 4)
	c.propose(c.ids, cmds("a", 5))
	c.converged(c.ids)
	ld := c.leader(c.ids)
	lag := without(c.ids, ld)[0]
	up := without(c.ids, lag)
	last := c.nodes[lag].Status().LastIndex
	c.down(lag)

	c.propose(up, cmds("b", 80))
	c.compactedPast(up, last)
	c.slow = map[NodeID]time.Duration{lag: 2 * time.Second}
	c.start(lag, nil)
	c.propose(c.ids, cmds("c", 3))
	c.converged(c.ids)

	log := c.log(lag)
	dropped := strings.Count(log, "event=raft_snapshot_chunk_dropped node="+string(lag)+" ")
	received := strings.Count(log, "event=raft_snapshot_received node="+string(lag)+" ")
	c.sms[lag].mu.Lock()
	proofs := c.sms[lag].proofs
	c.sms[lag].mu.Unlock()
	t.Logf("%s: %d proofs, %d received, %d repeated offers dropped", lag, proofs, received, dropped)
	if _, _, restores := c.sms[lag].state(); restores < 1 || received < 1 {
		t.Fatalf("premise: the follower did not catch up by a snapshot (restores %d, received %d):\n%s", restores, received, log)
	}
	if dropped == 0 {
		t.Fatalf("no repeated offer was dropped while the first copy was proven for 2 s:\n%s", log)
	}
	// One proof — two at most, if a repeat sent just before the install
	// completed arrives just after it — not one per offer.
	if proofs > 2 {
		t.Fatalf("the follower proved %d snapshots: the repeats were proven too:\n%s", proofs, log)
	}
	if _, got := c.nodes[lag].dur.Snap.Installing(); got != 0 {
		t.Fatalf("installing %d after the follower caught up", got)
	}
}
