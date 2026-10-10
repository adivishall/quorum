package kv_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/raftnode"
)

// The entry-size limit end to end (C1, docs/RAFT.md §16), on a real three-node
// group: real drivers, durable logs, TCP. Before the limit was enforced at the
// front, a PUT with a 1 MiB value — within the documented value limit — was
// appended and persisted by the leader as a 1,048,582-byte entry that every
// follower's decoder refused: the write ended LOST, an election deposed the
// leader, and the old leader could never restart ("raftlog: corrupt log:
// length 1048582 out of range").
//
// Timing is real, so the test asserts only what holds under any timing: a
// write at the limit may meet an election (it is retried until it succeeds —
// a PUT of one value is idempotent in effect, and LOST is a definite no-op),
// and "nothing was proposed" is read from every node's durable log, not
// inferred from the absence of an election.

// atLimitValue is the longest value an anonymous PUT of key can carry: the
// command's encoding (op, key length, key, value length, value) is then exactly
// kv.MaxCommandLen bytes.
func atLimitValue(key []byte) []byte {
	return bytes.Repeat([]byte("v"), kv.MaxCommandLen-1-1-len(key)-3)
}

// putUntilOK retries an anonymous PUT through whichever node leads until one
// attempt succeeds, and returns that attempt's index.
func putUntilOK(t *testing.T, ctx context.Context, c *cluster, key, value []byte) uint64 {
	t.Helper()
	var last error
	for attempt := 0; attempt < 10; attempt++ {
		leader, _ := c.waitSettled(20 * time.Second)
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		meta, err := c.eps[leader].Put(wctx, key, value)
		cancel()
		if err == nil {
			return meta.Index
		}
		if errors.Is(err, kv.ErrInvalid) {
			t.Fatalf("PUT of a %d-byte value refused: %v", len(value), err)
		}
		last = err // LOST, UNKNOWN or NOT_LEADER: an election met the write
	}
	t.Fatalf("PUT did not succeed in 10 attempts; last: %v", last)
	return 0
}

// durableEntries reads a node's durable log without modifying it.
func durableEntries(t *testing.T, c *cluster, id raftnode.NodeID) []raftlog.Entry {
	t.Helper()
	rec, err := raftlog.Inspect(filepath.Join(c.dir, string(id)+".log"))
	if err != nil {
		t.Fatalf("%s's log does not read back: %v", id, err)
	}
	return rec.Entries
}

func TestEntryLimitEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startClusterTicking(t, ctx, 3, false, 50*time.Millisecond) // dkvd's default tick
	key := []byte("big")
	value := atLimitValue(key)
	if n := (kv.Command{Op: kv.OpPut, Key: key, Value: value}).EncodedLen(); n != kv.MaxCommandLen {
		t.Fatalf("the test's command encodes to %d bytes, want exactly %d", n, kv.MaxCommandLen)
	}

	// Exactly at the limit: committed and applied on every node, intact.
	idx := putUntilOK(t, ctx, c, key, value)
	for _, id := range c.ids {
		c.waitApplied(id, idx)
		got, ok, lerr := c.eps[id].srv.Store().Lookup(key)
		if lerr != nil {
			t.Fatal(lerr)
		}
		if !ok || !bytes.Equal(got, value) {
			t.Fatalf("%s holds %d bytes for the key, want the %d-byte value", id, len(got), len(value))
		}
	}

	// One byte over: a definite INVALID at the leader and at a follower (the
	// front refuses it before anything is proposed or forwarded); below the
	// front the driver refuses it too, definitely, and every node goes on.
	leader, _ := c.waitSettled(20 * time.Second)
	durable := map[raftnode.NodeID]int{}
	for _, id := range c.ids {
		durable[id] = len(durableEntries(t, c, id))
	}
	over := append(append([]byte(nil), value...), 'v')
	for _, at := range append([]raftnode.NodeID{leader}, othersOf(c, leader)[0]) {
		rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := c.eps[at].Put(rctx, key, over)
		rcancel()
		if !errors.Is(err, kv.ErrInvalid) {
			t.Fatalf("PUT one byte over the limit at %s = %v, want ErrInvalid", at, err)
		}
	}
	cmd := kv.Command{Op: kv.OpPut, Key: key, Value: over}.Encode()
	for _, id := range c.ids {
		if _, _, _, err := c.node(id).Write(ctx, cmd); !errors.Is(err, raft.ErrEntryTooLarge) {
			t.Fatalf("%s: Node.Write of a %d-byte entry = %v, want raft.ErrEntryTooLarge", id, len(cmd), err)
		}
	}
	for _, id := range c.ids {
		select {
		case <-c.node(id).Done():
			t.Fatalf("%s stopped after refusing an oversized write: %v", id, c.node(id).Err())
		default:
		}
		// Nothing was persisted for the refused writes: any entry since is an
		// election's no-op, and no entry anywhere exceeds the limit.
		entries := durableEntries(t, c, id)
		for _, e := range entries {
			if len(e.Data) > raft.MaxEntryDataLen {
				t.Fatalf("%s's log holds entry %d of %d bytes, over the limit", id, e.Index, len(e.Data))
			}
		}
		for _, e := range entries[min(durable[id], len(entries)):] {
			if len(e.Data) != 0 {
				t.Fatalf("%s persisted entry %d (%d bytes) after the refused writes", id, e.Index, len(e.Data))
			}
		}
	}

	// Every node restarts from its own log — the old leader first — and the
	// group still serves the value it committed at the limit, and new writes.
	for _, id := range append([]raftnode.NodeID{leader}, othersOf(c, leader)...) {
		c.crash(id)
		c.startNode(id)
		c.waitSettled(20 * time.Second)
	}
	for _, id := range c.ids {
		c.waitApplied(id, idx)
		if got, ok, err := c.eps[id].srv.Store().Lookup(key); err != nil || !ok || !bytes.Equal(got, value) {
			t.Fatalf("after the restarts %s holds %d bytes for the key, want the %d-byte value", id, len(got), len(value))
		}
	}
	putUntilOK(t, ctx, c, []byte("after"), []byte("restart"))
}

func othersOf(c *cluster, id raftnode.NodeID) []raftnode.NodeID {
	var out []raftnode.NodeID
	for _, other := range c.ids {
		if other != id {
			out = append(out, other)
		}
	}
	return out
}
