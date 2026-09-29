package multiraft

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/transport"
)

func scrapeHost(t *testing.T, r *metrics.Registry) metrics.Samples {
	t.Helper()
	var b bytes.Buffer
	if err := r.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	ss, err := metrics.Parse(&b)
	if err != nil {
		t.Fatal(err)
	}
	return ss
}

// TestHostMetrics (Phase 16, docs/OBSERVABILITY.md): the host counts the groups
// it runs and every frame it drops, by reason, exactly as the frames the test
// sent; each group's Raft series carry its group label.
func TestHostMetrics(t *testing.T) {
	reg := metrics.NewRegistry()
	tr := &chanTransport{id: "a", ch: make(chan transport.Envelope, 16)}
	h, err := Start(context.Background(), Config{
		ID: "a", DataDir: t.TempDir(), Transport: tr, InboxSize: 4,
		NewStateMachine: func(GroupID) raftnode.StateMachine { return &recSM{} },
		TickInterval:    time.Hour, DisableSync: true, Metrics: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	gen := replication.VotersOf([]NodeID{"a", "b", "c"})
	for _, g := range []GroupID{1, 2} {
		if _, err := h.Create(g, &gen); err != nil {
			t.Fatal(err)
		}
	}
	send := func(payload []byte) {
		tr.ch <- transport.Envelope{Peer: "b", Kind: transport.MsgAppendEntries, Payload: payload}
	}
	appendAt := func(term uint64) []byte {
		return raft.Message{Type: raft.MsgAppendRequest, Term: term, From: "b", To: "a"}.Marshal()
	}
	send(raftnode.WrapGroup(9, appendAt(5))) // no such group
	send(raftnode.WrapGroup(9, appendAt(6)))
	send([]byte{0x80}) // a malformed envelope
	send(raftnode.WrapGroup(2, appendAt(4)))
	deadline := time.Now().Add(5 * time.Second)
	for h.Group(2).Node.Term() != 4 || h.Dropped()["unknown_group"] < 2 || h.Dropped()["malformed"] < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("frames not handled: %v, group 2 term %d", h.Dropped(), h.Group(2).Node.Term())
		}
		time.Sleep(time.Millisecond)
	}
	ss := scrapeHost(t, reg)
	for reason, want := range map[string]float64{"unknown_group": 2, "malformed": 1, "inbox_full": 0} {
		if v, ok := ss.Get("dkv_host_frames_dropped_total", "reason", reason); !ok || v != want {
			t.Fatalf("dropped %s = %v (present %v), the test sent %v", reason, v, ok, want)
		}
	}
	if v, _ := ss.Get("dkv_host_groups", "state", "running"); v != 2 {
		t.Fatalf("running groups %v, want 2", v)
	}
	// Each group is its own series of the driver's metrics.
	if v, _ := ss.Get("dkv_raft_term", "group", "2"); v != 4 {
		t.Fatalf("group 2's term gauge %v, want 4", v)
	}
	if v, _ := ss.Get("dkv_raft_term", "group", "1"); v != 0 {
		t.Fatalf("group 1's term gauge %v, want 0 (the frames were group 2's)", v)
	}
	if err := h.Stop(1); err != nil {
		t.Fatal(err)
	}
	ss = scrapeHost(t, reg)
	if v, _ := ss.Get("dkv_host_groups", "state", "running"); v != 1 {
		t.Fatalf("running groups %v after a stop, want 1", v)
	}
	if len(ss.All("dkv_raft_term", "group", "1")) != 0 {
		t.Fatal("a stopped group still reports gauges")
	}
}
