package kv

import (
	"bytes"
	"testing"
	"time"
)

func FuzzDecodeRequestIsTotal(f *testing.F) {
	f.Add(encodeRequest(Request{Op: ReqPut, Key: []byte("k"), Value: []byte("v")}))
	f.Add(encodeRequest(Request{Op: ReqGet, Key: []byte("k")}))
	f.Add(encodeRequest(Request{Op: ReqDelete, Key: []byte("k")}))
	f.Add(encodeRequest(Request{Op: ReqRegister}))
	f.Add(encodeRequest(Request{Op: ReqPut, Key: []byte("k"), Value: []byte("v"), ClientID: 9, RequestID: 4, AckedBelow: 2, Timeout: 1500 * time.Millisecond}))
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeRequest(b) // must never panic
		if err != nil {
			return
		}
		if !bytes.Equal(encodeRequest(r), b) {
			t.Fatalf("not canonical: %x -> %x", b, encodeRequest(r))
		}
		_ = r.validate() // must never panic either
	})
}

func FuzzDecodeResponseIsTotal(f *testing.F) {
	f.Add(encodeResponse(Response{Status: StatusOK, Term: 3, Index: 9, Value: []byte("v"), Node: "n1"}))
	f.Add(encodeResponse(Response{Status: StatusNotLeader, Leader: "n2", Node: "n1", Via: "n3"}))
	f.Add(encodeResponse(Response{Status: StatusOK, Duplicate: true, Index: 7, ClientID: 5}))
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeResponse(b)
		if err != nil {
			return
		}
		if !bytes.Equal(encodeResponse(r), b) {
			t.Fatalf("not canonical: %x -> %x", b, encodeResponse(r))
		}
	})
}

func FuzzDecodeForwardIsTotal(f *testing.F) {
	f.Add(encodeForward(77, 2*time.Second, Request{Op: ReqPut, Key: []byte("k"), Value: []byte("v"), ClientID: 3, RequestID: 1, AckedBelow: 1}))
	f.Add(encodeForwardResponse(77, Response{Status: StatusOK, Node: "n1"}))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _, _ = decodeForward(b) // total: never panics
		_, _, _ = decodeForwardResponse(b)
	})
}

func TestWireCodecsRoundTrip(t *testing.T) {
	reqs := []Request{
		{Op: ReqPut, Key: []byte("k"), Value: []byte{}},
		{Op: ReqPut, Key: bytes.Repeat([]byte("k"), MaxKeyLen), Value: bytes.Repeat([]byte("v"), MaxValueLen)},
		{Op: ReqGet, Key: []byte("a b")},
		{Op: ReqDelete, Key: []byte("\x00")},
		{Op: ReqRegister},
		{Op: ReqDelete, Key: []byte("k"), ClientID: 1 << 40, RequestID: 99, AckedBelow: 98, Timeout: 250 * time.Millisecond},
	}
	for _, r := range reqs {
		got, err := decodeRequest(encodeRequest(r))
		if err != nil || got.Op != r.Op || !bytes.Equal(got.Key, r.Key) || (r.Op == ReqPut && !bytes.Equal(got.Value, r.Value)) ||
			got.ClientID != r.ClientID || got.RequestID != r.RequestID || got.AckedBelow != r.AckedBelow || got.Timeout != r.Timeout {
			t.Fatalf("request %+v -> %+v, %v", r, got, err)
		}
	}
	resps := []Response{
		{Status: StatusOK, Term: 1, Index: 2, Value: []byte("v"), Node: "n1"},
		{Status: StatusOK, Value: []byte{}},
		{Status: StatusNotFound, Term: 5, Index: 7},
		{Status: StatusUnknown, Message: "boom"},
		{Status: StatusOK, Duplicate: true, Index: 12, Node: "n2", Via: "n1"},
		{Status: StatusOK, ClientID: 33, Index: 33},
		{Status: StatusNotLeader, Leader: "n3", Node: "n1"},
	}
	for _, r := range resps {
		got, err := decodeResponse(encodeResponse(r))
		if err != nil || got.Status != r.Status || got.Term != r.Term || got.Index != r.Index || !bytes.Equal(got.Value, r.Value) ||
			got.Duplicate != r.Duplicate || got.ClientID != r.ClientID || got.Node != r.Node || got.Via != r.Via || got.Leader != r.Leader || got.Message != r.Message {
			t.Fatalf("response %+v -> %+v, %v", r, got, err)
		}
	}
	if v, _ := decodeResponse(encodeResponse(Response{Status: StatusOK, Value: []byte{}})); v.Value == nil {
		t.Fatal("an OK read of an empty value must decode as present-and-empty, not nil")
	}
	bad := [][]byte{{}, {9, 0, 0, 0, 0, 1, 'k'}, {byte(ReqPut), 0, 0, 0, 0},
		{byte(ReqGet), 0, 0, 0, 0, 1, 'k', 0}, // trailing byte
		{byte(ReqPut), 0, 0, 0, 0, 1, 'k', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		{byte(ReqPut), 0, 0, 0, 0, 0x00, 0x80, 0x00}, // non-minimal varints (found by FuzzDecodeRequestIsTotal)
		{byte(ReqGet), 0x80, 0x00, 0, 0, 0, 1, 'k'},
		{byte(ReqGet), 0, 0, 0, 0, 0x81, 0x00, 'k'}}
	for _, b := range bad {
		if _, err := decodeRequest(b); err == nil {
			t.Fatalf("accepted malformed request %x", b)
		}
	}
	// Responses: a non-minimal term, index or payload length is refused too
	// (the last found by FuzzDecodeResponseIsTotal), as are unknown statuses and flags.
	for _, b := range [][]byte{{byte(StatusOK), 0, 0, 0x80, 0x00, 0, 0, 0, 0, 0, 0}, {byte(StatusOK), 0, 0, 0, 0x81, 0x00, 0, 0, 0, 0, 0},
		{byte(StatusOK), 0, 0, 0, 0, 0, 0, 0, 0x80, 0x00, 0}, {11, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 2, 0, 0, 0, 0, 0, 0, 0, 0}} {
		if _, err := decodeResponse(b); err == nil {
			t.Fatalf("accepted a non-canonical or out-of-range response %x", b)
		}
	}
}
