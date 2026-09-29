package transport

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/record"
)

func scrapeTransport(t *testing.T, r *metrics.Registry) metrics.Samples {
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

// TestTransportMetrics (Phase 16, docs/OBSERVABILITY.md): frames and bytes are
// counted by kind exactly as sent and received; one connection is counted in
// each direction; a send without a connection, and one after close, are
// counted by their reason.
func TestTransportMetrics(t *testing.T) {
	ra, rb := metrics.NewRegistry(), metrics.NewRegistry()
	a, err := NewTCPTransport(Config{NodeID: "a", ListenAddr: "127.0.0.1:0", DialRetryInterval: 20 * time.Millisecond, Metrics: ra})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTCPTransport(Config{NodeID: "b", ListenAddr: "127.0.0.1:0", DialRetryInterval: 20 * time.Millisecond, Metrics: rb})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.AddPeer("a", a.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	if err := a.AddPeer("b", b.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := a.Send(ctx, "nobody", MsgAppendEntries, []byte("x")); err != ErrPeerNotConnected {
		t.Fatalf("send to an unknown peer: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for a.Send(ctx, "b", MsgProbe, nil) != nil { // the first frame that goes through
		if time.Now().After(deadline) {
			t.Fatal("a never connected to b")
		}
		time.Sleep(5 * time.Millisecond)
	}
	failedSends := scrapeTransport(t, ra).Sum("dkv_transport_send_failures_total", "reason", "not_connected")
	payloads := [][]byte{[]byte("one"), make([]byte, 1000), nil}
	for _, p := range payloads {
		if err := a.Send(ctx, "b", MsgAppendEntries, p); err != nil {
			t.Fatal(err)
		}
	}
	got := 0
	for got < 1+len(payloads) {
		select {
		case <-b.Receive():
			got++
		case <-time.After(5 * time.Second):
			t.Fatalf("b received %d frames", got)
		}
	}
	wantBytes := 0
	for _, p := range payloads {
		wantBytes += record.HeaderSize + len(p)
	}
	sa, sb := scrapeTransport(t, ra), scrapeTransport(t, rb)
	if v, _ := sa.Get("dkv_transport_frames_sent_total", "kind", "append_entries"); v != 3 {
		t.Fatalf("append_entries frames sent %v, want 3", v)
	}
	if v, _ := sa.Get("dkv_transport_bytes_sent_total", "kind", "append_entries"); int(v) != wantBytes {
		t.Fatalf("append_entries bytes sent %v, want %d", v, wantBytes)
	}
	if v, _ := sb.Get("dkv_transport_frames_received_total", "kind", "append_entries"); v != 3 {
		t.Fatalf("append_entries frames received %v, want 3", v)
	}
	if v, _ := sb.Get("dkv_transport_bytes_received_total", "kind", "append_entries"); int(v) != wantBytes {
		t.Fatalf("append_entries bytes received %v, want %d", v, wantBytes)
	}
	if v, _ := sb.Get("dkv_transport_frames_received_total", "kind", "probe"); v != 1 {
		t.Fatalf("probes received %v, want the one that went through", v)
	}
	if failedSends < 1 {
		t.Fatalf("not_connected sends counted %v; the send to nobody failed so", failedSends)
	}
	if v := sa.Sum("dkv_transport_connections_total") + sb.Sum("dkv_transport_connections_total"); v != 2 {
		t.Fatalf("connections counted %v across both ends, want 2 (one each way)", v)
	}
	if v, _ := sa.Get("dkv_transport_peers", "state", "connected"); v != 1 {
		t.Fatalf("a's connected peers %v", v)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	_ = a.Send(ctx, "b", MsgProbe, nil)
	if v, _ := scrapeTransport(t, ra).Get("dkv_transport_send_failures_total", "reason", "closed"); v != 1 {
		t.Fatalf("sends after close counted %v, want 1", v)
	}
}
