package transport

import (
	"context"
	"net"
	"sync"
	"time"
)

// conn is one established, handshaked connection to a peer. A single conn is
// used bidirectionally (ADR-014): a shared reader loop (owned by the transport)
// and a mutex-guarded writer, so concurrent Sends never interleave their bytes
// (INV-T5).
type conn struct {
	peer NodeID
	nc   net.Conn

	writeMu      sync.Mutex
	writeBuf     []byte // reused frame buffer, guarded by writeMu
	writeTimeout time.Duration

	closeOnce sync.Once
}

func newConn(peer NodeID, nc net.Conn, writeTimeout time.Duration) *conn {
	return &conn{peer: peer, nc: nc, writeTimeout: writeTimeout}
}

// send writes one frame in full under the writer lock. ctx is honoured until
// the frame starts; a started frame is bounded by the write timeout alone —
// never by a caller's deadline, which could cut it short on a connection every
// group of the node shares. A write that fails or times out may have left part
// of the frame on the wire, after which every later frame would be read as
// garbage; so any failure closes the connection (audit M2): its reader loop
// deregisters it and the dialer reconnects, and nothing more is written into
// a stream whose state is unknown. Closing the conn (shutdown) also unblocks a
// blocked write.
func (c *conn) send(ctx context.Context, kind MsgKind, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err // ended while waiting for the writer: nothing was written
	}
	if c.writeTimeout > 0 {
		_ = c.nc.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	buf, err := writeFrame(c.nc, c.writeBuf, kind, payload)
	c.writeBuf = buf
	if err != nil {
		c.close()
	}
	return err
}

// close closes the underlying connection. It is idempotent, so both the reader
// loop's teardown and an explicit transport shutdown may call it.
func (c *conn) close() {
	c.closeOnce.Do(func() { _ = c.nc.Close() })
}
