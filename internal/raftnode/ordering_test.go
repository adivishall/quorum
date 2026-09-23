package raftnode

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/raftlog"
	"github.com/adivishall/quorum/internal/transport"
)

// hookTransport is a fake transport for driver-path tests. Receive() delivers
// messages the test injects; Send() runs a hook the test supplies, which lets the
// test observe the durable log at the exact moment the driver sends a reply.
type hookTransport struct {
	id     transport.NodeID
	recv   chan transport.Envelope
	onSend func(kind transport.MsgKind, payload []byte)
}

func newHookTransport(id transport.NodeID) *hookTransport {
	return &hookTransport{id: id, recv: make(chan transport.Envelope, 16)}
}

func (h *hookTransport) Send(ctx context.Context, peer transport.NodeID, kind transport.MsgKind, payload []byte) error {
	if h.onSend != nil {
		h.onSend(kind, payload)
	}
	return nil
}
func (h *hookTransport) Receive() <-chan transport.Envelope { return h.recv }
func (h *hookTransport) LocalID() transport.NodeID          { return h.id }
func (h *hookTransport) Close() error                       { return nil }

// TestPersistBeforeReplyOnDriverPath proves, on the REAL driver path (not just the
// core's Ready object), that a node's HardState is durably written BEFORE the
// dependent reply is sent (INV-R6). The hook transport, when the reply goes out,
// reads the durable log from disk and requires the expected HardState to already
// be there. A tick is never allowed to fire (huge interval), so the only activity
// is the injected message.
func TestPersistBeforeReplyOnDriverPath(t *testing.T) {
	t.Run("vote grant is durable before the vote reply", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "a.log")
		ht := newHookTransport("a")
		checkCh := make(chan error, 1)

		ht.onSend = func(kind transport.MsgKind, payload []byte) {
			if kind != transport.MsgRequestVoteResponse {
				return
			}
			rec, err := raftlog.Inspect(logPath)
			select {
			case checkCh <- verifyHardState(err, rec, 5, "z"):
			default:
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		n, err := Start(ctx, Config{
			ID: "a", Peers: []NodeID{"a", "z", "c"}, Transport: ht,
			LogPath: logPath, TickInterval: time.Hour, // never tick during the test
		})
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()

		// Inject a RequestVote at term 5 from z with an up-to-date (empty) log.
		ht.recv <- transport.Envelope{
			Peer: "z", Kind: transport.MsgRequestVote,
			Payload: raft.Message{Type: raft.MsgVoteRequest, Term: 5, LastLogIndex: 0, LastLogTerm: 0}.Marshal(),
		}
		awaitCheck(t, checkCh)
	})

	t.Run("higher-term step-down is durable before the append reply", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "a.log")
		ht := newHookTransport("a")
		checkCh := make(chan error, 1)

		ht.onSend = func(kind transport.MsgKind, payload []byte) {
			if kind != transport.MsgAppendEntriesResponse {
				return
			}
			rec, err := raftlog.Inspect(logPath)
			// After adopting term 7 the vote must be cleared ("").
			select {
			case checkCh <- verifyHardState(err, rec, 7, ""):
			default:
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		n, err := Start(ctx, Config{
			ID: "a", Peers: []NodeID{"a", "b", "c"}, Transport: ht,
			LogPath: logPath, TickInterval: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()

		// Inject an AppendEntries at term 7 (a heartbeat). The node adopts term 7,
		// clears its vote, persists, then replies — the reply must not precede the
		// persisted term.
		ht.recv <- transport.Envelope{
			Peer: "b", Kind: transport.MsgAppendEntries,
			Payload: raft.Message{Type: raft.MsgAppendRequest, Term: 7, PrevLogIndex: 0, PrevLogTerm: 0}.Marshal(),
		}
		awaitCheck(t, checkCh)
	})
}

func verifyHardState(err error, rec *raftlog.Recovered, wantTerm uint64, wantVote NodeID) error {
	if err != nil {
		return fmt.Errorf("inspect durable log at reply time: %w", err)
	}
	if rec.HardState.Term != wantTerm {
		return fmt.Errorf("reply sent before term %d was durable (durable term = %d)", wantTerm, rec.HardState.Term)
	}
	if rec.HardState.Vote != wantVote {
		return fmt.Errorf("reply sent before vote %q was durable (durable vote = %q)", wantVote, rec.HardState.Vote)
	}
	return nil
}

func awaitCheck(t *testing.T, checkCh chan error) {
	t.Helper()
	select {
	case err := <-checkCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no dependent reply was sent within the timeout")
	}
}
