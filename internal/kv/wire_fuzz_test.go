package kv

import (
	"bytes"
	"testing"
)

func FuzzDecodeRequestIsTotal(f *testing.F) {
	f.Add(request{op: wirePut, key: []byte("k"), value: []byte("v")}.encode())
	f.Add(request{op: wireGet, key: []byte("k")}.encode())
	f.Add(request{op: wireDel, key: []byte("k")}.encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeRequest(b) // must never panic
		if err != nil {
			return
		}
		if !bytes.Equal(r.encode(), b) {
			t.Fatalf("not canonical: %x -> %x", b, r.encode())
		}
	})
}

func FuzzDecodeResponseIsTotal(f *testing.F) {
	f.Add(response{status: statusOK, term: 3, index: 9, payload: []byte("v")}.encode())
	f.Add(response{status: statusNotLeader, payload: []byte("n2")}.encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeResponse(b)
		if err != nil {
			return
		}
		if !bytes.Equal(r.encode(), b) {
			t.Fatalf("not canonical: %x -> %x", b, r.encode())
		}
	})
}

func TestWireCodecsRoundTrip(t *testing.T) {
	reqs := []request{
		{op: wirePut, key: []byte("k"), value: []byte{}},
		{op: wirePut, key: bytes.Repeat([]byte("k"), MaxKeyLen), value: bytes.Repeat([]byte("v"), MaxValueLen)},
		{op: wireGet, key: []byte("a b")},
		{op: wireDel, key: []byte("\x00")},
	}
	for _, r := range reqs {
		got, err := decodeRequest(r.encode())
		if err != nil || got.op != r.op || !bytes.Equal(got.key, r.key) || (r.op == wirePut && !bytes.Equal(got.value, r.value)) {
			t.Fatalf("request %+v -> %+v, %v", r, got, err)
		}
	}
	resps := []response{
		{status: statusOK, term: 1, index: 2, payload: []byte("v")},
		{status: statusOK, payload: []byte{}},
		{status: statusNotFound, term: 5, index: 7},
		{status: statusError, payload: []byte("boom")},
	}
	for _, r := range resps {
		got, err := decodeResponse(r.encode())
		if err != nil || got.status != r.status || got.term != r.term || got.index != r.index || !bytes.Equal(got.payload, r.payload) {
			t.Fatalf("response %+v -> %+v, %v", r, got, err)
		}
	}
	bad := [][]byte{{}, {9, 1, 'k'}, {wirePut, 0}, {wireGet, 1, 'k', 0}, {wirePut, 1, 'k', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		{wirePut, 0x00, 0x80, 0x00}, // non-minimal varints (found by FuzzDecodeRequestIsTotal)
		{wireGet, 0x81, 0x00, 'k'}}
	for _, b := range bad {
		if _, err := decodeRequest(b); err == nil {
			t.Fatalf("accepted malformed request %x", b)
		}
	}
	// Responses: a non-minimal term, index or payload length is refused too
	// (the last found by FuzzDecodeResponseIsTotal).
	for _, b := range [][]byte{{statusOK, 0x80, 0x00, 0, 0}, {statusOK, 0, 0x81, 0x00, 0}, {'0', '0', '0', 0x80, 0x00}} {
		if _, err := decodeResponse(b); err == nil {
			t.Fatalf("accepted a non-canonical response %x", b)
		}
	}
}
