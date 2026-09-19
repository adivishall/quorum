// Package compaction implements the merge half of Quorum's size-tiered
// compaction (docs/DESIGN.md §7, ADR-007, docs/COMPACTION.md).
//
// This package is deliberately only the merge. Choosing which files to compact,
// writing the output, and publishing it through the MANIFEST all live in package
// storage, because they need the live version and the file-set lock. What is
// here is the part that can be reasoned about and tested with no store, no
// directory and no locks: given k sorted entry streams, produce one sorted
// stream with obsolete entries removed.
//
// # Streaming, not buffering
//
// The merge holds one cursor per input plus a fixed per-cursor key/value buffer.
// It never materialises the inputs, because the whole point of compacting an LSM
// tree is to reorganise more data than fits in memory. A compaction that built a
// map of the database would work fine in a test and fall over on the first file
// larger than RAM.
//
// # What gets dropped
//
// Entries arrive in internal-key order: user key ascending, then sequence
// descending (docs/DESIGN.md §1). So the first entry for a user key is its
// newest version, and every entry after it for the same user key is obsolete.
//
//	newest version of each user key   kept (but see tombstones)
//	every older version              dropped
//
// Dropping older versions is always safe. Nothing in this engine exposes a
// snapshot read — Get always asks for the newest version — so no reader can ever
// want a superseded value. Versions living in *older* files outside this
// compaction are shadowed by the retained newest version, because the output
// file's sequence range covers the inputs' and the read path consults files
// newest-first.
//
// # Tombstones: the rule that resurrects data if you get it wrong
//
// A tombstone is not garbage. It is the only thing hiding an older value that
// may still exist in a file this compaction is not reading. Dropping it early
// makes a deleted key come back (INV-S3).
//
// docs/DESIGN.md §7 states the rule as "a tombstone may only be discarded when
// compacting into the bottom-most level". This engine has no leveled hierarchy in
// which to look "below", so the rule is stated in terms of what actually
// determines age here — sequence numbers:
//
//	A tombstone may be dropped only when the compaction's input set contains
//	the oldest live data in the database: that is, when no live file outside
//	the input set holds a sequence number below the input set's minimum.
//
// When that holds, there is no file anywhere that could hold an older value for
// any key, so the tombstone hides nothing and can go. When it does not hold, the
// tombstone is retained even though it is the newest version of its key. The
// caller computes this and passes it as BottomMost; package storage derives it
// from the live version, and a test asserts that a non-bottom-most compaction
// keeps its tombstones.
package compaction

import (
	"container/heap"
	"fmt"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage/ikey"
)

// ErrCorrupt means the merge found something its inputs should have made
// impossible. It is the same sentinel the rest of the storage layer uses.
var ErrCorrupt = record.ErrCorrupt

// Stream is one input to the merge. It is exactly the shape of
// sstable.Iterator, and of the memtable iterator, so either can be an input.
//
// Key and Value must stay valid until the next call to Next on that same
// stream, which is the contract sstable.Iterator documents.
type Stream interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
}

// Stats records what one merge did. The counts are how docs/COMPACTION.md
// reports work done rather than asserting that compaction helps.
type Stats struct {
	InputEntries      int64
	OutputEntries     int64
	VersionsDropped   int64 // superseded versions of a key that survives
	TombstonesDropped int64 // tombstones discarded because the merge was bottom-most
	TombstonesKept    int64 // tombstones retained because it was not
	DistinctKeys      int64
}

// Options configures a merge.
type Options struct {
	// BottomMost permits dropping tombstones. It must be true only when the
	// input set contains the oldest live data in the database; see the package
	// comment. Defaulting to false is the safe direction: retaining a tombstone
	// wastes space, dropping one early loses a delete.
	BottomMost bool
}

// cursor is one input positioned at an entry.
//
// key and val are per-cursor buffers that are reused on every advance, so the
// merge does no allocation per entry after warm-up, and the entry a cursor holds
// stays valid while it waits in the heap even though the underlying stream's own
// buffers move on.
type cursor struct {
	s   Stream
	key []byte
	val []byte
}

// advance pulls the next entry into the cursor's own buffers.
func (c *cursor) advance() (bool, error) {
	if !c.s.Next() {
		return false, c.s.Err()
	}
	c.key = append(c.key[:0], c.s.Key()...)
	c.val = append(c.val[:0], c.s.Value()...)
	return true, nil
}

// cursorHeap orders cursors by their current internal key.
type cursorHeap []*cursor

func (h cursorHeap) Len() int           { return len(h) }
func (h cursorHeap) Less(i, j int) bool { return ikey.Compare(h[i].key, h[j].key) < 0 }
func (h cursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)        { *h = append(*h, x.(*cursor)) }
func (h *cursorHeap) Pop() any {
	old := *h
	n := len(old)
	c := old[n-1]
	*h = old[:n-1]
	return c
}

// Merger is a streaming k-way merge with obsolete entries removed.
//
// It implements sstable.FailableSource, so the output is written by handing the
// Merger straight to sstable.WriteFile with no intermediate buffer.
type Merger struct {
	h    cursorHeap
	opts Options

	key []byte // current output entry
	val []byte

	// lastUserKey is the user key most recently *seen* (not necessarily
	// emitted). Every entry sharing it after the first is an obsolete version.
	// It is tracked separately from what was emitted so that dropping a
	// tombstone still suppresses the older versions behind it.
	lastUserKey []byte
	haveLast    bool

	// lastSeenKey is the previous entry of the merged stream, emitted or not. It
	// guards ordering and, more importantly, catches two inputs holding the same
	// internal key — which version elimination would otherwise absorb silently.
	lastSeenKey []byte
	haveSeen    bool

	stats Stats
	err   error
	done  bool
}

// New returns a Merger over inputs.
//
// It positions every input, so an input that fails immediately fails here.
func New(inputs []Stream, opts Options) *Merger {
	m := &Merger{opts: opts}
	for _, s := range inputs {
		c := &cursor{s: s}
		ok, err := c.advance()
		if err != nil {
			m.err = err
			return m
		}
		if ok {
			m.h = append(m.h, c)
		}
	}
	heap.Init(&m.h)
	return m
}

// Next advances to the next output entry.
//
// A false return means either the merge finished or it failed; the caller must
// check Err to tell those apart. sstable.WriteFile does exactly that.
func (m *Merger) Next() bool {
	if m.err != nil || m.done {
		return false
	}
	for {
		if len(m.h) == 0 {
			m.done = true
			return false
		}
		c := m.h[0]

		// Copy the candidate out of the cursor BEFORE advancing it.
		//
		// A cursor reuses its key and value buffers on every advance, so holding
		// the cursor's slices across an advance would silently hand back the
		// *next* entry instead of this one — producing out-of-order output and
		// the wrong version of a key. The buffers here are the Merger's own, and
		// the contract that Key and Value are valid only until the next call to
		// Next is what makes reusing them safe.
		m.key = append(m.key[:0], c.key...)
		m.val = append(m.val[:0], c.val...)
		key := m.key

		// Re-position the cursor, so every path through the loop leaves the heap
		// valid.
		ok, err := c.advance()
		if err != nil {
			m.err = err
			return false
		}
		if ok {
			heap.Fix(&m.h, 0)
		} else {
			heap.Pop(&m.h)
		}

		if !ikey.Valid(key) {
			m.err = fmt.Errorf("compaction: input produced a malformed internal key %s: %w",
				ikey.String(key), ErrCorrupt)
			return false
		}
		m.stats.InputEntries++

		// Every entry of the merged stream must be strictly greater than the one
		// before it, and this is checked on every entry rather than only on the
		// ones that get emitted.
		//
		// Checking only the output would miss the case worth catching. Two inputs
		// holding the *same* internal key is impossible by construction — a
		// sequence number belongs to exactly one mutation and a mutation lives in
		// exactly one file — so it means the live files' sequence ranges overlap.
		// Version elimination would quietly absorb the duplicate as though it
		// were an older version of the key and the output would look perfect,
		// throwing away the only evidence that the file set is not one this
		// engine could have produced.
		if m.haveSeen && ikey.Compare(m.lastSeenKey, key) >= 0 {
			m.err = fmt.Errorf(
				"compaction: merged stream produced %s at or before %s; "+
					"inputs are not disjoint and ascending: %w",
				ikey.String(key), ikey.String(m.lastSeenKey), ErrCorrupt)
			return false
		}
		m.lastSeenKey = append(m.lastSeenKey[:0], key...)
		m.haveSeen = true

		uk := ikey.UserKey(key)
		if m.haveLast && bytesEqual(m.lastUserKey, uk) {
			// An older version of a key already decided. Dropping it is safe
			// whether the decision was "emit" or "drop the tombstone": either
			// way nothing can see past it.
			m.stats.VersionsDropped++
			continue
		}
		m.lastUserKey = append(m.lastUserKey[:0], uk...)
		m.haveLast = true
		m.stats.DistinctKeys++

		if ikey.KindOf(key) == ikey.KindTombstone {
			if m.opts.BottomMost {
				// Nothing older than this compaction exists, so the tombstone
				// hides nothing.
				m.stats.TombstonesDropped++
				continue
			}
			// An older file may still hold a value for this key. The tombstone
			// is the only thing hiding it.
			m.stats.TombstonesKept++
		}

		// The output is a subsequence of the merged stream, which the check above
		// has already established is strictly increasing, so the output is too.
		// m.key and m.val already hold this entry.
		m.stats.OutputEntries++
		return true
	}
}

// Key returns the current entry's internal key. It stays valid until the next
// call to Next.
func (m *Merger) Key() []byte { return m.key }

// Value returns the current entry's value, with the same lifetime as Key.
func (m *Merger) Value() []byte { return m.val }

// Err returns the failure that stopped the merge, if any.
func (m *Merger) Err() error { return m.err }

// Stats returns the counts accumulated so far.
func (m *Merger) Stats() Stats { return m.stats }

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
