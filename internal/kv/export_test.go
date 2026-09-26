package kv

import "context"

// ForwardTo sends one forward directly (tests of the forwarding path: a
// forwarded request is never forwarded again).
func (s *Server) ForwardTo(ctx context.Context, peer string, req Request) Response {
	return s.forward(ctx, peer, req)
}
