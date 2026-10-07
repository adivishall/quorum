package integration

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
)

// TestRealEntryLimit is the real-process regression for C1 (docs/RAFT.md §16):
// three dkvd processes over TCP, the client protocol, SIGKILL and restart.
//
// Before the entry-size limit was enforced at the front, a PUT with a 1 MiB
// value — within the documented value limit — became a 1,048,582-byte entry:
// the leader persisted it, every follower refused it, the write ended LOST, and
// the old leader could never restart ("raftlog: corrupt log: length 1048582
// out of range"). Now a write whose entry is exactly at the limit commits on
// every node and survives a full-cluster SIGKILL; one byte more is a definite
// INVALID at every node, anonymous or in a session, and nothing reaches any
// log. The assertions hold under any timing: the write at the limit is retried
// across elections, and "nothing reached any log" is read from the logs.
func TestRealEntryLimit(t *testing.T) {
	c := newRCluster(t, 3)
	c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	ctx := context.Background()

	key := []byte("big")
	value := bytes.Repeat([]byte("v"), kv.MaxCommandLen-1-1-len(key)-3) // entry exactly at the limit
	put := func(value []byte) error {
		var last error
		for attempt := 0; attempt < 10; attempt++ {
			leader, _ := c.waitLeader(c.running(), 0, 20*time.Second)
			ep := &procEndpoint{node: leader, addr: c.kvAddrs[leader]}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := ep.Put(wctx, key, value)
			cancel()
			if err == nil || errors.Is(err, kv.ErrInvalid) {
				return err
			}
			last = err // LOST, UNKNOWN, NOT_LEADER: an election met the write
		}
		return last
	}
	if err := put(value); err != nil {
		t.Fatalf("PUT whose entry is exactly %d bytes: %v\n%s", kv.MaxCommandLen, err, c.outputs())
	}

	// One byte over: INVALID at every node, anonymous or in a session —
	// definite (Known), before anything is proposed or forwarded.
	over := append(append([]byte(nil), value...), 'v')
	for _, id := range c.ids {
		ep := &procEndpoint{node: id, addr: c.kvAddrs[id]}
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := ep.Put(rctx, key, over)
		cancel()
		if !errors.Is(err, kv.ErrInvalid) {
			t.Fatalf("anonymous PUT one byte over the limit at %s = %v, want ErrInvalid", id, err)
		}
	}
	s, err := kv.Register(ctx, c.doers(), kv.SessionOptions{AttemptTimeout: 5 * time.Second, MaxAttempts: 20, Backoff: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if out := s.Put(ctx, key, value, nil); !errors.Is(out.Err, kv.ErrInvalid) || !out.Known {
		// The session's identity adds bytes to the entry: the same value that
		// fits anonymously is over the limit when identified.
		t.Fatalf("identified PUT over the limit: %+v, want a known ErrInvalid", out)
	}

	// Every process dies at once and restarts on its own log; none refuses
	// its log, and the value committed at the limit is served.
	for _, id := range c.ids {
		c.kill(id)
	}
	for _, id := range c.ids {
		rec, err := raftlog.Inspect(filepath.Join(c.dirs[id], "raft-"+id+".log"))
		if err != nil {
			t.Fatalf("%s's log does not read back: %v", id, err)
		}
		for _, e := range rec.Entries {
			if len(e.Data) > raft.MaxEntryDataLen {
				t.Fatalf("%s's log holds entry %d of %d bytes, over the limit", id, e.Index, len(e.Data))
			}
		}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	leader, _ := c.waitLeader(c.ids, 0, 20*time.Second)
	c.waitClientReady(20 * time.Second)
	if out := c.outputs(); strings.Contains(out, "corrupt log") || strings.Contains(out, "raft_fatal") {
		t.Fatalf("a node refused its log or failed after the restart:\n%s", out)
	}
	ep := &procEndpoint{node: leader, addr: c.kvAddrs[leader]}
	var got []byte
	for attempt := 0; attempt < 10; attempt++ {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		got, _, err = ep.Get(rctx, key)
		cancel()
		if err == nil {
			break
		}
		leader, _ = c.waitLeader(c.ids, 0, 20*time.Second)
		ep = &procEndpoint{node: leader, addr: c.kvAddrs[leader]}
	}
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("after a full-cluster restart GET = %d bytes, %v; want the %d-byte value", len(got), err, len(value))
	}
}
