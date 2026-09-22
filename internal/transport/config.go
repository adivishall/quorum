package transport

import (
	"fmt"
	"net"
	"time"
)

// NodeID is a node's protocol-level identity. It is opaque, compared bytewise,
// and non-empty. It is a *protocol* label, not a cryptographic identity: the
// transport is unauthenticated (docs/TRANSPORT.md §10). It is a distinct type
// from routing.NodeID so the transport layer does not depend on routing.
type NodeID string

// Timeout defaults (docs/TRANSPORT.md §8). All are bounded; none is arbitrary.
const (
	DefaultDialTimeout       = 3 * time.Second
	DefaultHandshakeTimeout  = 5 * time.Second
	DefaultWriteTimeout      = 5 * time.Second
	DefaultDialRetryInterval = 500 * time.Millisecond
	// DefaultReadIdleTimeout of 0 disables the idle read deadline; a connection
	// is then closed only by an explicit shutdown or a read/write failure.
	DefaultReadIdleTimeout = 0
)

// Config is a node's transport configuration. Membership is static (ADR-005):
// Peers is fixed at construction and never mutated at runtime.
type Config struct {
	// NodeID is this node's identity, announced in the handshake.
	NodeID NodeID

	// ListenAddr is the "host:port" this node accepts connections on.
	ListenAddr string

	// Peers maps every other node's id to its "host:port". It must not contain
	// this node's own id (that would be a self-dial). An empty map is legal — a
	// lone node with no peers.
	Peers map[NodeID]string

	// Timeouts; zero values are replaced by the Default* constants, except
	// ReadIdleTimeout whose zero legitimately means "disabled".
	DialTimeout       time.Duration
	HandshakeTimeout  time.Duration
	WriteTimeout      time.Duration
	DialRetryInterval time.Duration
	ReadIdleTimeout   time.Duration

	// Logf, if non-nil, receives structured transport events (peer connected,
	// handshake failed, peer disconnected, shutdown). It is the minimal
	// observability hook (docs/TRANSPORT.md); nil disables logging. It must be
	// safe for concurrent use.
	Logf func(format string, args ...any)
}

// withDefaults returns a copy of c with zero timeout fields filled in.
func (c Config) withDefaults() Config {
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.DialRetryInterval == 0 {
		c.DialRetryInterval = DefaultDialRetryInterval
	}
	// ReadIdleTimeout: zero means disabled, so it is left as-is.
	return c
}

// validate checks the configuration. It never repairs: a self-dial, an empty or
// oversized id, or a malformed address is an error, not a silent fix.
func (c Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("%w: empty node id", ErrInvalidConfig)
	}
	if len(c.NodeID) > MaxNodeIDLen {
		return fmt.Errorf("%w: node id exceeds %d bytes", ErrInvalidConfig, MaxNodeIDLen)
	}
	if err := validateAddr(c.ListenAddr); err != nil {
		return fmt.Errorf("%w: listen addr %q: %v", ErrInvalidConfig, c.ListenAddr, err)
	}
	for id, addr := range c.Peers {
		if id == "" {
			return fmt.Errorf("%w: empty peer id", ErrInvalidConfig)
		}
		if len(id) > MaxNodeIDLen {
			return fmt.Errorf("%w: peer id %q exceeds %d bytes", ErrInvalidConfig, id, MaxNodeIDLen)
		}
		if id == c.NodeID {
			return fmt.Errorf("%w: peer id %q is this node's own id (self-dial)", ErrInvalidConfig, id)
		}
		if err := validateAddr(addr); err != nil {
			return fmt.Errorf("%w: peer %q addr %q: %v", ErrInvalidConfig, id, addr, err)
		}
	}
	return nil
}

// validateAddr rejects an empty or structurally malformed "host:port".
func validateAddr(addr string) error {
	if addr == "" {
		return fmt.Errorf("empty")
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return err
	}
	return nil
}
