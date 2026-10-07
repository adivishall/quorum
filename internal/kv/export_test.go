package kv

import "context"

// ForwardTo sends one forward directly (tests of the forwarding path: a
// forwarded request is never forwarded again).
func (s *Server) ForwardTo(ctx context.Context, peer string, req Request) Response {
	return s.forward(ctx, peer, req)
}

// HoldForwardSlots takes n of the server's forward slots, as n forwards in
// progress would, until release is called.
func (s *Server) HoldForwardSlots(n int) (release func()) {
	for i := 0; i < n; i++ {
		s.fwdSlots <- struct{}{}
	}
	return func() {
		for i := 0; i < n; i++ {
			<-s.fwdSlots
		}
	}
}
