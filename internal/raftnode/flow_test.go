package raftnode

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
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

// TestConcurrentReadsShareRounds: reads that wait for the actor together are
// taken together — up to maxReadsPerCycle in one cycle — and the core confirms
// each cycle's reads with one round of entry-less heartbeats. The leader's
// actor is held (an apply hook) while 512 reads queue for it, then released:
// they cost a few rounds, not a round each. Before, every read broadcast the
// unacknowledged tail to every peer. (Reads that merely run concurrently are
// spread over cycles by the scheduler — 8 to 300 messages for the same 512 —
// so only reads that are queued together measure the batching.)
func TestConcurrentReadsShareRounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	net := fault.NewNetwork()
	var holdOn atomic.Value // NodeID whose actor the hook holds
	var holdAt atomic.Uint64
	held := make(chan struct{}, 1)
	release := make(chan struct{})
	h := startClusterHooked(t, ctx, 3, func(tr transport.Transport) transport.Transport { return net.Wrap(tr) }, 15*time.Millisecond,
		func(id NodeID) Hook {
			return func(p Point, idx uint64) error {
				if p == BeforeApply && idx == holdAt.Load() && holdOn.Load() == id {
					select {
					case held <- struct{}{}:
					default:
					}
					<-release
				}
				return nil
			}
		})
	defer h.stop()
	l := h.waitLeader(5 * time.Second)
	if _, _, err := writeWithin(h.nodes[l], []byte("x"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	// Hold the leader's actor in the apply of one more write.
	holdOn.Store(l)
	holdAt.Store(h.nodes[l].Status().LastIndex + 1)
	go func() { _, _, _ = writeWithin(h.nodes[l], []byte("hold"), 10*time.Second) }()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("premise: the leader's actor was not held")
	}
	const reads = 512
	var wg sync.WaitGroup
	errs := make(chan error, reads)
	for i := 0; i < reads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := readWithin(h.nodes[l], 10*time.Second); err != nil {
				errs <- err
			}
		}()
	}
	time.Sleep(40 * time.Millisecond) // every read is waiting for the actor (well inside an election timeout)
	before := net.Stats().Passed
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("a read on a healthy leader (role now %s): %v", h.nodes[l].Status().Role, err)
	}
	// Two cycles of 256 cost two rounds — 2 heartbeats and 2 acknowledgements
	// each — plus the held write's and the ticks' traffic meanwhile. A cycle
	// per few reads would cost hundreds.
	sent := net.Stats().Passed - before
	if sent >= 64 {
		t.Fatalf("%d reads queued together cost %d messages; taken %d per cycle they cost a few rounds", reads, sent, maxReadsPerCycle)
	}
	t.Logf("%d reads queued together cost %d messages", reads, sent)
}
