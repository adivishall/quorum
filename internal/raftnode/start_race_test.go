package raftnode

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
	"github.com/adivishall/quorum/internal/transport"
)

// preloadedTransport is a transport whose inbound queue is filled before the
// node starts; it sends nothing.
type preloadedTransport struct {
	id   transport.NodeID
	ch   chan transport.Envelope
	once sync.Once
}

func (p *preloadedTransport) Send(context.Context, transport.NodeID, transport.MsgKind, []byte) error {
	return nil
}
func (p *preloadedTransport) Receive() <-chan transport.Envelope { return p.ch }
func (p *preloadedTransport) LocalID() transport.NodeID          { return p.id }
func (p *preloadedTransport) Close() error {
	p.once.Do(func() { close(p.ch) })
	return nil
}

// TestStartDoesNotTouchTheCoreOnceTheActorOwnsIt is the regression for a data
// race the race suite found in the Phase 13 gate: Start read the core's term
// and last index for its raft_started line AFTER starting the actor goroutine,
// which owns the core and may already be stepping it — a restarted node that
// receives a message at once. Here a message from a higher term is queued
// before the node starts, so the actor changes the core's term immediately;
// the race detector reports any unordered access from Start, whatever the
// actual interleaving.
func TestStartDoesNotTouchTheCoreOnceTheActorOwnsIt(t *testing.T) {
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		tr := &preloadedTransport{id: "a", ch: make(chan transport.Envelope, 1)}
		tr.ch <- transport.Envelope{Peer: "b", Kind: transport.MsgAppendEntries,
			Payload: raft.Message{Type: raft.MsgAppendRequest, Term: 9, From: "b", To: "a"}.Marshal()}
		n, err := Start(ctx, Config{ID: "a", Peers: []NodeID{"a", "b", "c"}, Transport: tr,
			LogPath: filepath.Join(t.TempDir(), "a.log"), TickInterval: time.Hour, DisableSync: true})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for n.Status().Term != 9 {
			if time.Now().After(deadline) {
				t.Fatalf("the queued message was never stepped: %+v", n.Status())
			}
			time.Sleep(time.Millisecond)
		}
		cancel()
		_ = n.Close()
	}
}
