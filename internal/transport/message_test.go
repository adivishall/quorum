package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestProbeRoundTrip(t *testing.T) {
	cases := []Probe{
		{RequestID: 0, Token: nil},
		{RequestID: 1, Token: []byte{}},
		{RequestID: 0xdeadbeefcafef00d, Token: []byte("echo me")},
		{RequestID: 42, Token: bytes.Repeat([]byte{0xAB}, MaxProbeTokenLen)},
	}
	for _, p := range cases {
		got, err := ParseProbe(p.Marshal())
		if err != nil {
			t.Fatalf("ParseProbe(%+v): %v", p, err)
		}
		if got.RequestID != p.RequestID || !bytes.Equal(got.Token, p.Token) && !(len(got.Token) == 0 && len(p.Token) == 0) {
			t.Errorf("round trip = %+v, want %+v", got, p)
		}
	}
}

func TestProbeResponseRoundTrip(t *testing.T) {
	p := ProbeResponse{RequestID: 7, Token: []byte("pong")}
	got, err := ParseProbeResponse(p.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestID != 7 || string(got.Token) != "pong" {
		t.Errorf("got %+v", got)
	}
	if MsgProbe == MsgProbeResponse || (Probe{}).Kind() != MsgProbe || (ProbeResponse{}).Kind() != MsgProbeResponse {
		t.Error("Probe/ProbeResponse kinds are wrong")
	}
}

func TestProbeMalformedRejected(t *testing.T) {
	// too short for the uint64 request id
	if _, err := ParseProbe([]byte{1, 2, 3}); !errors.Is(err, ErrMalformedPayload) {
		t.Errorf("short: err = %v, want ErrMalformedPayload", err)
	}
	// valid id, but the token length varint points past the end
	bad := make([]byte, 0, 16)
	var u [8]byte
	binary.LittleEndian.PutUint64(u[:], 1)
	bad = append(bad, u[:]...)
	var lv [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lv[:], 9999) // claims 9999 bytes, none follow
	bad = append(bad, lv[:n]...)
	if _, err := ParseProbe(bad); !errors.Is(err, ErrMalformedPayload) {
		t.Errorf("length past end: err = %v, want ErrMalformedPayload", err)
	}
	// token longer than MaxProbeTokenLen
	over := marshalProbe(1, bytes.Repeat([]byte{0}, MaxProbeTokenLen+1))
	if _, err := ParseProbe(over); !errors.Is(err, ErrMalformedPayload) {
		t.Errorf("oversized token: err = %v, want ErrMalformedPayload", err)
	}
	// trailing bytes after a complete probe
	good := marshalProbe(1, []byte("x"))
	good = append(good, 0xFF)
	if _, err := ParseProbe(good); !errors.Is(err, ErrTrailingBytes) {
		t.Errorf("trailing bytes: err = %v, want ErrTrailingBytes", err)
	}
}
