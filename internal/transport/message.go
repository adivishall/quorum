package transport

import "encoding/binary"

// MaxProbeTokenLen bounds the echo token a Probe/ProbeResponse may carry, so a
// declared token length can never size an unbounded allocation.
const MaxProbeTokenLen = 4096

// The message codec is hand-written, deterministic, reflection-free and bounded
// (ADR-003, docs/TRANSPORT.md §6). Every decoder checks a declared length
// against the bytes remaining and against a maximum before allocating, consumes
// exactly its frame payload, and returns an error rather than panicking on
// hostile input.

// --- decode reader ---

type reader struct {
	b []byte
	i int
}

func (r *reader) readUint64() (uint64, error) {
	if r.i+8 > len(r.b) {
		return 0, ErrMalformedPayload
	}
	v := binary.LittleEndian.Uint64(r.b[r.i:])
	r.i += 8
	return v, nil
}

func (r *reader) readUvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.i:])
	if n <= 0 {
		return 0, ErrMalformedPayload
	}
	r.i += n
	return v, nil
}

// readBytes reads a uvarint length then that many bytes, rejecting a length
// larger than max or larger than the bytes that remain — never trusting the
// declared length before checking it. It returns a fresh copy.
func (r *reader) readBytes(max int) ([]byte, error) {
	n, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(max) || int(n) > len(r.b)-r.i {
		return nil, ErrMalformedPayload
	}
	out := make([]byte, n)
	copy(out, r.b[r.i:r.i+int(n)])
	r.i += int(n)
	return out, nil
}

// done reports ErrTrailingBytes unless every byte of the payload was consumed.
func (r *reader) done() error {
	if r.i != len(r.b) {
		return ErrTrailingBytes
	}
	return nil
}

// --- Probe / ProbeResponse ---

// Probe is the Phase 7 liveness message (docs/TRANSPORT.md §7). It is not a Raft
// heartbeat. RequestID correlates a response to its request; Token is arbitrary
// bounded bytes the responder echoes, so a round trip proves payload integrity
// beyond the frame checksum.
type Probe struct {
	RequestID uint64
	Token     []byte
}

// Kind returns MsgProbe.
func (p Probe) Kind() MsgKind { return MsgProbe }

// Marshal encodes the probe payload (deterministic).
func (p Probe) Marshal() []byte { return marshalProbe(p.RequestID, p.Token) }

// ParseProbe decodes a probe payload, consuming exactly the frame payload.
func ParseProbe(payload []byte) (Probe, error) {
	id, tok, err := parseProbe(payload)
	return Probe{RequestID: id, Token: tok}, err
}

// ProbeResponse answers a Probe with the same RequestID and echoed Token.
type ProbeResponse struct {
	RequestID uint64
	Token     []byte
}

// Kind returns MsgProbeResponse.
func (p ProbeResponse) Kind() MsgKind { return MsgProbeResponse }

// Marshal encodes the response payload (deterministic).
func (p ProbeResponse) Marshal() []byte { return marshalProbe(p.RequestID, p.Token) }

// ParseProbeResponse decodes a response payload.
func ParseProbeResponse(payload []byte) (ProbeResponse, error) {
	id, tok, err := parseProbe(payload)
	return ProbeResponse{RequestID: id, Token: tok}, err
}

// marshalProbe and parseProbe are shared because Probe and ProbeResponse have
// the same field layout; only their kind byte differs.
func marshalProbe(id uint64, token []byte) []byte {
	buf := make([]byte, 0, 8+binary.MaxVarintLen64+len(token))
	var u [8]byte
	binary.LittleEndian.PutUint64(u[:], id)
	buf = append(buf, u[:]...)
	var lv [binary.MaxVarintLen64]byte
	m := binary.PutUvarint(lv[:], uint64(len(token)))
	buf = append(buf, lv[:m]...)
	buf = append(buf, token...)
	return buf
}

func parseProbe(payload []byte) (uint64, []byte, error) {
	r := reader{b: payload}
	id, err := r.readUint64()
	if err != nil {
		return 0, nil, err
	}
	tok, err := r.readBytes(MaxProbeTokenLen)
	if err != nil {
		return 0, nil, err
	}
	if err := r.done(); err != nil {
		return 0, nil, err
	}
	return id, tok, nil
}
