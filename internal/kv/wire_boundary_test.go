package kv

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// TestWireClientNeverMatchesALateResponseToANewRequest pins the response
// boundary: a request whose deadline passes is UNKNOWN, and the connection it
// used is abandoned — so its response, if it arrives late, can never be read
// as the answer to the client's NEXT request. A client that kept the
// connection would return the first request's (stale) answer for the second
// read: a history-corrupting bug no checker could attribute correctly. The
// server here is a scripted fake speaking the real framing: it answers the
// first request only after the client gave up on it.
func TestWireClientNeverMatchesALateResponseToANewRequest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	gaveUp := make(chan struct{})
	lateSent := make(chan struct{})
	go func() {
		first := true
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn, first bool) {
				defer conn.Close()
				for {
					if _, err := readFrame(conn, kindRequest, MaxRequestFrame); err != nil {
						return
					}
					if first {
						first = false
						<-gaveUp // answer only after the client abandoned the request
						_ = writeFrame(conn, kindResponse, encodeResponse(Response{Status: StatusOK, Value: []byte("stale")}))
						close(lateSent)
						continue
					}
					if err := writeFrame(conn, kindResponse, encodeResponse(Response{Status: StatusOK, Value: []byte("fresh")})); err != nil {
						return
					}
				}
			}(conn, first)
			first = false
		}
	}()
	cl, err := Dial("fake", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, _, err = cl.Get(ctx, []byte("k"))
	cancel()
	if !errors.Is(err, ErrUnknown) {
		t.Fatalf("a request whose deadline passed must be unknown, got %v", err)
	}
	close(gaveUp)
	<-lateSent
	v, _, err := cl.Get(context.Background(), []byte("k"))
	if err != nil || string(v) != "fresh" {
		t.Fatalf("the second request got %q (%v): a late response was matched to a new request", v, err)
	}
}
