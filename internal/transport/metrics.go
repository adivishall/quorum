package transport

import (
	"strings"
	"unicode"

	"github.com/adivishall/quorum/internal/metrics"
	"github.com/adivishall/quorum/internal/record"
)

// transportMetrics is the transport's instrumentation (Phase 16,
// docs/OBSERVABILITY.md). Frames are counted by kind, where they are written
// in full or read whole; the per-kind series are resolved once, so the send
// and receive paths only increment. Nil fields are inert.
type transportMetrics struct {
	sent, recv           [256]*metrics.Counter // frames, by kind
	sentBytes, recvBytes [256]*metrics.Counter
	notConnected, closed *metrics.Counter
	writeFailed          *metrics.Counter
	inbound, outbound    *metrics.Counter
	dialFailed           *metrics.Counter
}

// kindLabel is a kind's name in snake case (AppendEntries → append_entries).
func kindLabel(k MsgKind) string {
	var b strings.Builder
	for i, r := range k.String() {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

func newTransportMetrics(r *metrics.Registry, t *TCPTransport) *transportMetrics {
	m := &transportMetrics{}
	if r == nil {
		return m
	}
	frames := r.CounterVec("dkv_transport_frames_sent_total", "Frames this node wrote in full to a peer's connection, by kind.", "kind")
	framesIn := r.CounterVec("dkv_transport_frames_received_total", "Frames this node read whole from a peer's connection, by kind.", "kind")
	bytes := r.CounterVec("dkv_transport_bytes_sent_total", "Bytes of the frames counted in dkv_transport_frames_sent_total, record headers included.", "kind")
	bytesIn := r.CounterVec("dkv_transport_bytes_received_total", "Bytes of the frames counted in dkv_transport_frames_received_total, record headers included.", "kind")
	for k := 0; k < 256; k++ {
		if kind := MsgKind(k); kind.defined() {
			l := kindLabel(kind)
			m.sent[k], m.recv[k] = frames.With(l), framesIn.With(l)
			m.sentBytes[k], m.recvBytes[k] = bytes.With(l), bytesIn.With(l)
		}
	}
	fails := r.CounterVec("dkv_transport_send_failures_total", "Sends that did not write a frame: not_connected (no live connection to the peer), closed (the transport is shut down), write (the write failed or its deadline passed; the peer may have received part of it).", "reason")
	m.notConnected, m.closed, m.writeFailed = fails.With("not_connected"), fails.With("closed"), fails.With("write")
	conns := r.CounterVec("dkv_transport_connections_total", "Connections established with a peer (handshake done, registered), by direction.", "dir")
	m.inbound, m.outbound = conns.With("inbound"), conns.With("outbound")
	m.dialFailed = r.Counter("dkv_transport_dial_failures_total", "Outbound connection attempts that failed to connect or to send the handshake.")
	r.CollectGauge("dkv_transport_peers", "Peers this node knows (accepts and, with the smaller id, dials) and peers it has a live connection to.", []string{"state"}, func(emit func(float64, ...string)) {
		t.mu.Lock()
		known, connected := len(t.peers), len(t.conns)
		t.mu.Unlock()
		emit(float64(known), "known")
		emit(float64(connected), "connected")
	})
	return m
}

func (m *transportMetrics) frameSent(k MsgKind, payload int) {
	m.sent[k].Inc()
	m.sentBytes[k].Add(uint64(record.HeaderSize + payload))
}

func (m *transportMetrics) frameReceived(k MsgKind, payload int) {
	m.recv[k].Inc()
	m.recvBytes[k].Add(uint64(record.HeaderSize + payload))
}
