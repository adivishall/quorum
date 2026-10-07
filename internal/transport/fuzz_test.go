package transport

import (
	"bytes"
	"testing"

	"github.com/adivishall/quorum/internal/record"
)

// FuzzFrameDecode: readFrame must never panic on arbitrary bytes, and any frame
// it accepts must round-trip through the encoder to the same bytes.
func FuzzFrameDecode(f *testing.F) {
	f.Add([]byte{})
	f.Add(frameBytesRaw(MsgProbe, []byte("hi")))
	f.Add([]byte("DKV1 not a frame"))
	hdr := make([]byte, record.HeaderSize)
	f.Fuzz(func(t *testing.T, data []byte) {
		kind, payload, err := readFrame(bytes.NewReader(data), hdr)
		if err != nil {
			return // any explicit error is fine; the point is no panic
		}
		// Accepted: re-encoding the same kind+payload must reproduce a valid frame
		// that decodes back identically (the decoder accepted a well-formed frame).
		re, encErr := record.Encode(nil, record.Kind(kind), payload)
		if encErr != nil {
			t.Fatalf("accepted a frame the encoder rejects: %v", encErr)
		}
		k2, p2, err2 := readFrame(bytes.NewReader(re), hdr)
		if err2 != nil || k2 != kind || !bytes.Equal(p2, payload) {
			t.Fatalf("re-decode mismatch: kind %v->%v err=%v", kind, k2, err2)
		}
	})
}

func frameBytesRaw(kind MsgKind, payload []byte) []byte {
	b, _ := record.Encode(nil, record.Kind(kind), payload)
	return b
}

// FuzzHandshakeDecode: readHandshake must never panic on arbitrary bytes.
func FuzzHandshakeDecode(f *testing.F) {
	f.Add([]byte("DKV1"))
	var seed bytes.Buffer
	_ = writeHandshake(&seed, hello{id: "seed-node", cluster: "c", digest: []byte{1}})
	f.Add(seed.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		h, err := readHandshake(bytes.NewReader(data))
		if err == nil && (h.id == "" || len(h.id) > MaxNodeIDLen || len(h.cluster) > MaxClusterIDLen || len(h.digest) > MaxDigestLen) {
			t.Fatalf("accepted an invalid hello %+v", h)
		}
		if _, h, err := readReply(bytes.NewReader(data)); err == nil && h.id == "" {
			t.Fatalf("accepted a reply with no node id")
		}
	})
}

// FuzzProbeDecode: ParseProbe must never panic; a valid probe round-trips.
func FuzzProbeDecode(f *testing.F) {
	f.Add(uint64(0), []byte(nil))
	f.Add(uint64(123), []byte("token"))
	f.Fuzz(func(t *testing.T, id uint64, token []byte) {
		if len(token) > MaxProbeTokenLen {
			token = token[:MaxProbeTokenLen]
		}
		p := Probe{RequestID: id, Token: token}
		got, err := ParseProbe(p.Marshal())
		if err != nil {
			t.Fatalf("valid probe failed to parse: %v", err)
		}
		if got.RequestID != id {
			t.Fatalf("request id %d != %d", got.RequestID, id)
		}
		if !bytes.Equal(got.Token, token) && !(len(got.Token) == 0 && len(token) == 0) {
			t.Fatalf("token mismatch")
		}
		// Arbitrary bytes must not panic ParseProbe either.
		_, _ = ParseProbe(token)
	})
}
