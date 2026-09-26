package snapshot

import (
	"encoding/binary"
	"fmt"
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

// Split cuts a snapshot file into the chunks that carry it, in order.
func Split(term uint64, m Meta, file []byte) []Chunk {
	var out []Chunk
	for off := 0; off < len(file); off += MaxChunk {
		end := min(off+MaxChunk, len(file))
		out = append(out, Chunk{Term: term, Index: m.Index, SnapTerm: m.Term, Total: uint64(len(file)), Offset: uint64(off), Data: file[off:end]})
	}
	return out
}

// Receiver reassembles a snapshot a leader is sending into Files.RecvPath. It
// accepts chunks strictly in order: a chunk at offset 0 starts a new transfer
// (abandoning any other); any other chunk must continue the current transfer
// exactly where it left off — same sender, term and snapshot — or it is ignored
// (a duplicate, a delayed chunk from an earlier attempt, a stale leader's). When
// the last chunk arrives, the file is fsynced, read back and decoded in full;
// only a snapshot that validates, and whose metadata matches what the chunks
// announced, is reported complete. Until then — and if it does not validate —
// nothing of it is active state: it is a temporary file (docs/SNAPSHOTS.md §8).
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

// Received is a complete, validated snapshot a leader sent, staged at RecvPath.
type Received struct {
	From  string // the sender (the leader)
	Term  uint64 // the leader's term when it sent it
	Meta  Meta
	Data  []byte // the state bytes
	Bytes int    // the file's size
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
	file, err := readFile(r.files.fs(), r.files.RecvPath())
	if err != nil {
		return nil, err
	}
	m, data, err := Decode(file)
	if err != nil {
		return nil, err
	}
	if m.Index != t.index || m.Term != t.snapTerm {
		return nil, fmt.Errorf("%w: received snapshot (%d,%d) announced as (%d,%d)", ErrCorrupt, m.Index, m.Term, t.index, t.snapTerm)
	}
	return &Received{From: t.from, Term: t.term, Meta: m, Data: data, Bytes: len(file)}, nil
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
