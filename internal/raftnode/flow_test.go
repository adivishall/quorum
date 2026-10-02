package raftnode

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/transport"
)

// Replication flow control through real drivers over real TCP (audit H4).

// TestFollowerBehindByMoreThanAFrameCatchesUp: a follower cut off while the
// leader commits 18 MB — more than the transport's 16 MiB frame — catches up
// once it is reachable again, with no snapshot to help it (SnapshotEvery 0).
// Before, every AppendEntries to it carried the whole backlog: once the
// backlog passed the frame size the message could never be sent, and the
// follower stayed behind for good.
func TestFollowerBehindByMoreThanAFrameCatchesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	net := fault.NewNetwork()
	// A 100 ms tick: heartbeats resend the batch in flight to a lagging
	// follower until it is acknowledged, and under the race detector a 1 MiB
	// batch takes long enough to make that redundancy dominate the test's time.
	h := startClusterTick(t, ctx, 3, func(tr transport.Transport) transport.Transport { return net.Wrap(tr) }, 100*time.Millisecond)
	defer h.stop()
	l := h.waitLeader(10 * time.Second)
	var lag NodeID
	for _, id := range h.ids {
		if id != l {
			lag = id
			break
		}
	}
	var members []string
	for _, id := range h.ids {
		members = append(members, string(id))
	}
	net.Isolate(string(lag), members)
	value := bytes.Repeat([]byte{'v'}, 1000<<10)
	for i := 0; i < 18; i++ {
		value[0] = byte(i)
		if _, _, err := writeWithin(h.nodes[l], value, 10*time.Second); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	target := h.nodes[l].Status().Commit
	if h.nodes[lag].Status().LastIndex >= target {
		t.Fatal("premise: the isolated follower is not behind")
	}
	net.HealAll()
	start := time.Now()
	deadline := start.Add(60 * time.Second)
	for h.nodes[lag].Status().Applied < target {
		if time.Now().After(deadline) {
			st := h.nodes[lag].Status()
			t.Fatalf("the follower 18 MB behind reached %d (applied %d) of %d in 60s", st.LastIndex, st.Applied, target)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("caught up in %s", time.Since(start).Round(time.Millisecond))
	var writes []string
	for _, c := range h.sms[lag].snapshot() {
		if len(c) == len(value) { // not an election's no-op
			writes = append(writes, c)
		}
	}
	if len(writes) != 18 {
		t.Fatalf("the follower applied %d of the 18 writes", len(writes))
	}
	for i, c := range writes {
		if c[0] != byte(i) {
			t.Fatalf("the follower's write %d is the leader's write %d", i, c[0])
		}
	}
}

// TestConcurrentReadsShareRounds: hundreds of concurrent reads on a leader
// cost far fewer messages than reads: the actor takes the reads waiting with
// one, and the core confirms them with one round of entry-less heartbeats.
// Before, every read broadcast the unacknowledged tail to every peer.
func TestConcurrentReadsShareRounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	net := fault.NewNetwork()
	h := startClusterWith(t, ctx, 3, func(tr transport.Transport) transport.Transport { return net.Wrap(tr) })
	defer h.stop()
	l := h.waitLeader(5 * time.Second)
	if _, _, err := writeWithin(h.nodes[l], []byte("x"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	const reads = 512
	before := net.Stats().Passed
	var wg sync.WaitGroup
	errs := make(chan error, reads)
	start := make(chan struct{})
	for i := 0; i < reads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := readWithin(h.nodes[l], 10*time.Second); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("a read on a healthy leader (role now %s): %v", h.nodes[l].Status().Role, err)
	}
	sent := net.Stats().Passed - before
	if sent >= reads {
		t.Fatalf("%d reads cost %d messages; reads sharing rounds cost far fewer than one each", reads, sent)
	}
	t.Logf("%d reads cost %d messages", reads, sent)
}
