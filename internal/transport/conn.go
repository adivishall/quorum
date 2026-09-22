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

// send writes one frame in full under the writer lock. The write deadline is the
// sooner of the configured write timeout and any deadline on ctx, so a blocked
// write cannot hang forever and honours caller cancellation; closing the conn
// (shutdown) also unblocks it.
func (c *conn) send(ctx context.Context, kind MsgKind, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.writeTimeout > 0 {
		dl := time.Now().Add(c.writeTimeout)
		if d, ok := ctx.Deadline(); ok && d.Before(dl) {
			dl = d
		}
		_ = c.nc.SetWriteDeadline(dl)
	}
	buf, err := writeFrame(c.nc, c.writeBuf, kind, payload)
	c.writeBuf = buf
	return err
}

// close closes the underlying connection. It is idempotent, so both the reader
// loop's teardown and an explicit transport shutdown may call it.
func (c *conn) close() {
	c.closeOnce.Do(func() { _ = c.nc.Close() })
}
