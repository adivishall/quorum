package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
)

// TestRequestGroupRoundTrips (wire v3, Phase 15): the request's group
// round-trips for every width; one beyond 32 bits is a protocol error.
func TestRequestGroupRoundTrips(t *testing.T) {
	for _, g := range []replication.GroupID{0, 1, 127, 128, 1<<32 - 1} {
		for _, r := range []Request{
			{Op: ReqPut, Group: g, ClientID: 3, RequestID: 9, AckedBelow: 4, Key: []byte("k"), Value: []byte("v"), Timeout: time.Second},
			{Op: ReqRegister, Group: g},
		} {
			b := encodeRequest(r)
			got, err := decodeRequest(b)
			if err != nil || got.Group != g || !bytes.Equal(encodeRequest(got), b) {
				t.Fatalf("group %d: %+v %v", g, got, err)
			}
		}
	}
	b := []byte{byte(ReqGet)}
	b = binary.AppendUvarint(b, 1<<32)
	b = append(b, 0, 0, 0, 0, 1, 'k')
	if _, err := decodeRequest(b); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a group over 32 bits decoded: %v", err)
	}
}

// fixedDoer answers every request with OK, recording it.
type fixedDoer struct{ got []Request }

func (d *fixedDoer) Name() string { return "fixed" }
func (d *fixedDoer) Do(_ context.Context, r Request) (Response, error) {
	d.got = append(d.got, r)
	return Response{Status: StatusOK, Node: "fixed"}, nil
}

// TestRetiredRequestKindsAreRefused: a Phase 13 (version 2, kind 3) or Phase
// 12 (version 1, kind 1) request frame is not this protocol — the connection is
// closed unanswered, and nothing reaches the server — while a version 3 frame
// is served.
func TestRetiredRequestKindsAreRefused(t *testing.T) {
	for _, kind := range []record.Kind{1, 3} {
		d := &fixedDoer{}
		a, b := net.Pipe()
		go serveConn(context.Background(), b, d, nil, ServeConfig{})
		frame, _ := record.Encode(nil, kind, []byte{byte(ReqGet), 0, 0, 0, 0, 1, 'k'})
		go a.Write(frame)
		_ = a.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := a.Read(make([]byte, 1)); err == nil {
			t.Fatalf("kind %d was answered", kind)
		}
		a.Close()
		if len(d.got) != 0 {
			t.Fatalf("kind %d reached the server: %+v", kind, d.got)
		}
	}
	d := &fixedDoer{}
	a, b := net.Pipe()
	defer a.Close()
	go serveConn(context.Background(), b, d, nil, ServeConfig{})
	if err := writeFrame(a, kindRequest, encodeRequest(Request{Op: ReqGet, Group: 7, Key: []byte("k")})); err != nil {
		t.Fatal(err)
	}
	payload, err := readFrame(a, kindResponse, maxResponseFrame)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := decodeResponse(payload); err != nil || resp.Status != StatusOK || len(d.got) != 1 || d.got[0].Group != 7 {
		t.Fatalf("v3: %+v %v %+v", resp, err, d.got)
	}
}
