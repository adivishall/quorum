package snapshot

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/adivishall/quorum/internal/vfs"
)

// MaxChunk bounds one chunk's data: a snapshot travels as many frames far below
// the transport's 16 MiB frame limit (docs/TRANSPORT.md §2), so heartbeats on
// the same connection interleave between them.
const MaxChunk = 1 << 20

// Chunk is one piece of a snapshot file in transit from a leader (transport
// kind InstallSnapshot). The sender is the connection's handshake identity,
// never a field. Term is the leader's term when it offered the snapshot; Index
// and SnapTerm identify the snapshot; Total is the whole file's size, Offset
// where Data belongs. The transfer is complete when Offset+len(Data) == Total.
type Chunk struct {
	Term     uint64
	Index    uint64
	SnapTerm uint64
	Total    uint64
	Offset   uint64
	Data     []byte
}

// Marshal encodes a chunk: canonical uvarints and a length-prefixed data block.
func (c Chunk) Marshal() []byte {
	b := binary.AppendUvarint(nil, c.Term)
	b = binary.AppendUvarint(b, c.Index)
	b = binary.AppendUvarint(b, c.SnapTerm)
	b = binary.AppendUvarint(b, c.Total)
	b = binary.AppendUvarint(b, c.Offset)
	b = binary.AppendUvarint(b, uint64(len(c.Data)))
	return append(b, c.Data...)
}

// UnmarshalChunk decodes a chunk, strictly: canonical integers, a snapshot
// index and term of at least 1, a total within MaxFile, non-empty data of at
// most MaxChunk that lies inside the file, and nothing after it.
func UnmarshalChunk(b []byte) (Chunk, error) {
	d := decoder{b: b}
	c := Chunk{Term: d.uint(), Index: d.uint(), SnapTerm: d.uint(), Total: d.uint(), Offset: d.uint()}
	c.Data = d.bytes(MaxChunk)
	if err := d.done(); err != nil {
		return Chunk{}, err
	}
	if c.Index == 0 || c.SnapTerm == 0 || c.Total > MaxFile || c.Offset >= c.Total || c.Offset+uint64(len(c.Data)) > c.Total {
		return Chunk{}, fmt.Errorf("%w: chunk (index %d term %d) of %d bytes at %d of %d", ErrCorrupt, c.Index, c.SnapTerm, len(c.Data), c.Offset, c.Total)
	}
	return c, nil
}

// Split cuts a snapshot file into the chunks that carry it, in order, of
// MaxChunk bytes each (the last may be shorter).
func Split(term uint64, m Meta, file []byte) []Chunk { return SplitSize(term, m, file, MaxChunk) }

// SplitSize is Split with chunks of at most size bytes (clamped to [1,
// MaxChunk]). The deterministic simulator uses small chunks, so that a
// transfer is many messages its network can drop, duplicate, delay and
// reorder; the receiver accepts any chunk size up to MaxChunk.
func SplitSize(term uint64, m Meta, file []byte, size int) []Chunk {
	size = max(1, min(size, MaxChunk))
	var out []Chunk
	for off := 0; off < len(file); off += size {
		end := min(off+size, len(file))
		out = append(out, Chunk{Term: term, Index: m.Index, SnapTerm: m.Term, Total: uint64(len(file)), Offset: uint64(off), Data: file[off:end]})
	}
	return out
}

// Receiver reassembles a snapshot a leader is sending into Files.RecvPath. It
// accepts chunks strictly in order: a chunk at offset 0 starts a new transfer
// (abandoning any other); any other chunk must continue the current transfer
// exactly where it left off — same sender, term and snapshot — or it is ignored
// (a duplicate, a delayed chunk from an earlier attempt, a stale leader's). When
// the last chunk arrives, the file is fsynced and read back in full by
// streaming it (S4: one record at a time, never the whole file in memory);
// only a snapshot that validates, and whose metadata matches what the chunks
// announced, is reported complete. Until then — and if it does not validate —
// nothing of it is active state: it is a temporary file (docs/SNAPSHOTS.md §8).
// The state inside is the state machine's to validate (raftnode does, from
// OpenReceived) before anything installs it.
//
// A Receiver is used by one goroutine (the node's actor, or the simulator).
type Receiver struct {
	files Files
	cur   *transfer
}

type transfer struct {
	from                  string
	term, index, snapTerm uint64
	total, received       uint64
	f                     vfs.File
}

// NewReceiver returns a receiver that stages into files.RecvPath.
func NewReceiver(files Files) *Receiver { return &Receiver{files: files} }

// Received is a complete, validated snapshot a leader sent, staged at RecvPath
// (its state is read from there: Files.OpenReceived).
type Received struct {
	From  string // the sender (the leader)
	Term  uint64 // the leader's term when it sent it
	Meta  Meta
	Bytes int64 // the file's size
}

// Accept takes one chunk from a peer. It returns a Received when the chunk
// completed a snapshot that validates; nil otherwise. An error means the
// transfer failed and was abandoned (an I/O error, or a complete file that does
// not validate) — the leader will offer the snapshot again.
func (r *Receiver) Accept(from string, c Chunk) (*Received, error) {
	if c.Offset == 0 {
		r.abandon()
		f, err := r.files.fs().OpenFile(r.files.RecvPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, err
		}
		r.cur = &transfer{from: from, term: c.Term, index: c.Index, snapTerm: c.SnapTerm, total: c.Total, f: f}
	}
	t := r.cur
	if t == nil || t.from != from || t.term != c.Term || t.index != c.Index || t.snapTerm != c.SnapTerm ||
		t.total != c.Total || t.received != c.Offset {
		return nil, nil // not the continuation of the transfer in progress
	}
	if _, err := t.f.Write(c.Data); err != nil {
		r.abandon()
		return nil, err
	}
	t.received += uint64(len(c.Data))
	if t.received < t.total {
		return nil, nil
	}
	// Complete: make it durable, then validate what is actually on disk.
	if err := t.f.Sync(); err != nil {
		r.abandon()
		return nil, err
	}
	_ = t.f.Close()
	r.cur = nil
	rd, file, err := r.files.OpenReceived()
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := rd.Verify(); err != nil {
		return nil, err
	}
	m := rd.Meta()
	if m.Index != t.index || m.Term != t.snapTerm {
		return nil, fmt.Errorf("%w: received snapshot (%d,%d) announced as (%d,%d)", ErrCorrupt, m.Index, m.Term, t.index, t.snapTerm)
	}
	return &Received{From: t.from, Term: t.term, Meta: m, Bytes: int64(t.total)}, nil
}

// Active reports the transfer in progress, if any: its sender, snapshot index and
// bytes received so far.
func (r *Receiver) Active() (from string, index, received uint64, ok bool) {
	if r.cur == nil {
		return "", 0, 0, false
	}
	return r.cur.from, r.cur.index, r.cur.received, true
}

// Abandon drops a transfer in progress (its staging file is an orphan until the
// next transfer truncates it or startup removes it).
func (r *Receiver) Abandon() { r.abandon() }

func (r *Receiver) abandon() {
	if r.cur != nil {
		_ = r.cur.f.Close()
		r.cur = nil
	}
}

// Sending is a published snapshot opened to be sent to a follower (S4,
// docs/STORAGE_INTEGRATION.md §10.5). The transfer reads its chunks from this
// handle, never from the whole file in memory: a later publish renames a new
// file over the published name, and this handle still reads the snapshot the
// transfer started with.
type Sending struct {
	f    vfs.File
	meta Meta
	size int64
}

// OpenSend opens the published snapshot to send it, reading its header.
func (f Files) OpenSend() (*Sending, error) {
	rd, file, err := openStream(f.fs(), f.Path())
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Sending{f: file, meta: rd.Meta(), size: info.Size()}, nil
}

// Meta returns the snapshot's metadata, from its header.
func (s *Sending) Meta() Meta { return s.meta }

// Size returns the file's size.
func (s *Sending) Size() int64 { return s.size }

// Verify validates the whole file by streaming it, so a leader never streams a
// snapshot it has not proven whole. It stops early, with ctx's error, once
// ctx is done (a node shutting down does not wait for a large file's read).
func (s *Sending) Verify(ctx context.Context) error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	rd, err := NewReader(ctxReader{ctx: ctx, r: s.f}, s.size)
	if err != nil {
		return err
	}
	return rd.Verify()
}

// ctxReader fails its reads once ctx is done.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// Chunks reads the file from its start and hands send each chunk, in order, of
// at most size bytes (clamped to [1, MaxChunk]) — Split's chunks, read from
// disk one at a time — and returns how many it sent. A chunk's Data is reused
// for the next chunk: send must not keep it (Marshal copies it). It stops at
// the first error send returns.
func (s *Sending) Chunks(term uint64, size int, send func(Chunk) error) (int, error) {
	size = max(1, min(size, MaxChunk))
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	buf := make([]byte, size)
	n := 0
	for off := int64(0); off < s.size; {
		want := min(int64(size), s.size-off)
		if _, err := io.ReadFull(s.f, buf[:want]); err != nil {
			return n, fmt.Errorf("snapshot: reading the published file at %d: %w", off, err)
		}
		c := Chunk{Term: term, Index: s.meta.Index, SnapTerm: s.meta.Term, Total: uint64(s.size), Offset: uint64(off), Data: buf[:want]}
		if err := send(c); err != nil {
			return n, err
		}
		off += want
		n++
	}
	return n, nil
}

// Close releases the handle.
func (s *Sending) Close() error { return s.f.Close() }
