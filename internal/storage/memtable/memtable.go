// Package memtable implements Quorum's in-memory write buffer.
//
// A MemTable holds the mutations that have been written to the WAL but not yet
// written to an SSTable. It is ordered by the internal-key comparison in
// package ikey, which is what lets a flush produce a sorted SSTable with a
// single forward pass and no sort step.
//
// # Why a skip list rather than a map
//
// A Go map would be faster to write and would serve point lookups perfectly
// well. It cannot serve the operation the engine actually needs: ordered
// iteration. Flushing a memtable means emitting every entry in internal-key
// order, and Phase 4's compaction means merging several such streams. A map
// would have to be sorted at flush time — O(n log n) with an allocation for
// every key, performed while writers are blocked — and would still give no way
// to iterate a live memtable.
//
// A skip list gives ordered iteration, O(log n) expected lookup and insert,
// and — the property that matters most here — it never moves or removes a
// node. That is what makes concurrent reads safe (see "Concurrency").
//
// # Structure
//
// Standard Pugh skip list. A node appears at level 0 always, and at each
// higher level with probability 1/branching, up to maxHeight levels. Search
// starts at the highest level in use and drops a level whenever the next node
// at that level would overshoot.
//
// Height generation uses a per-MemTable PRNG seeded from New's argument, not
// math/rand's global source. Two reasons: a global source is shared mutable
// state that the race detector would (correctly) complain about, and a fixed
// seed makes a failing test reproduce identically instead of "sometimes".
// Heights affect only performance — the iteration order of a skip list is
// fully determined by its keys — so seeding it does not make the engine's
// behaviour depend on the seed.
//
// # Concurrency
//
// A sync.RWMutex guards the list. Writers take the write lock; readers and
// iterators take the read lock. This is not lock-free and does not pretend to
// be; a lock-free skip list is a Phase 5 optimisation that has to be justified
// by a benchmark first.
//
// Nodes are never removed and a node's key is never modified after insertion,
// so an iterator may release and re-take the read lock between steps without
// ever observing a torn entry or an out-of-order key. An iterator over a
// frozen memtable is a stable snapshot; an iterator over a live one may or may
// not observe inserts that happen while it runs.
package memtable

import (
	"sync"

	"github.com/adivishall/quorum/internal/storage/ikey"
)

const (
	// maxHeight bounds the tower. 12 levels with a branching factor of 4
	// indexes roughly 4^12 = 16M entries before the top level stops helping,
	// which is far above the entry count a memtable reaches before it is
	// flushed. This is the LevelDB choice and it is not claimed to be tuned.
	maxHeight = 12
	// branching is the inverse probability of promoting a node one level.
	branching = 4

	// DefaultSeed is the height-PRNG seed used when none is supplied. A fixed
	// default is deliberate: the engine's behaviour must not vary run to run.
	DefaultSeed = 0x5DEECE66D

	// nodeOverhead approximates the bytes a node costs beyond its key and
	// value: the struct header plus the base of the next-pointer slice. It
	// exists so that ApproxSize tracks real memory rather than only payload.
	// It is an estimate and is documented as one; nothing correctness-related
	// depends on its accuracy.
	nodeOverhead = 48
	// pointerSize is added per tower level.
	pointerSize = 8
)

// node is one skip-list entry. next has exactly the node's height.
type node struct {
	key  []byte // internal key (ikey encoding); immutable after insertion
	val  []byte // value bytes; empty for a tombstone
	next []*node
}

// MemTable is an ordered, concurrency-safe in-memory table of internal keys.
//
// The zero value is not usable; call New.
type MemTable struct {
	mu     sync.RWMutex
	head   *node
	height int // number of levels currently in use, >= 1
	n      int
	size   int64
	frozen bool

	rng uint64 // xorshift64 state for height generation

	// prev is scratch reused by Add to avoid allocating a maxHeight-element
	// slice per insertion. It is only touched under the write lock.
	prev [maxHeight]*node
}

// New returns an empty MemTable whose height generator is seeded with seed.
// A zero seed selects DefaultSeed, because a zero xorshift state is a fixed
// point that would make every node height 1.
func New(seed uint64) *MemTable {
	if seed == 0 {
		seed = DefaultSeed
	}
	return &MemTable{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rng:    seed,
	}
}

// randomHeight returns a tower height in [1, maxHeight].
func (m *MemTable) randomHeight() int {
	h := 1
	for h < maxHeight && m.next()%branching == 0 {
		h++
	}
	return h
}

// next advances the xorshift64 generator. Called only under the write lock.
func (m *MemTable) next() uint64 {
	x := m.rng
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	m.rng = x
	return x
}

// Add inserts a mutation.
//
// The internal key is built here rather than by the caller so that no caller
// can construct one with a different encoding. Both userKey and value are
// copied: the memtable outlives the caller's buffers, and the Store contract
// promises a caller may reuse its slices the moment Put returns.
//
// Add panics on a frozen memtable. Freezing is the engine's own signal that
// the table is being flushed, so writing to one afterwards is a bug in the
// engine, not a condition a caller can provoke.
func (m *MemTable) Add(seq uint64, kind ikey.Kind, userKey, value []byte) {
	internal := ikey.Encode(make([]byte, 0, len(userKey)+ikey.TrailerSize), userKey, seq, kind)

	val := []byte{}
	if kind == ikey.KindValue {
		val = make([]byte, len(value))
		copy(val, value)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		panic("memtable: Add on a frozen memtable")
	}

	x := m.head
	for lvl := m.height - 1; lvl >= 0; lvl-- {
		for x.next[lvl] != nil && ikey.Compare(x.next[lvl].key, internal) < 0 {
			x = x.next[lvl]
		}
		m.prev[lvl] = x
	}

	// A duplicate internal key cannot occur: a sequence number is consumed by
	// exactly one mutation. Handled anyway, by replacing the value pointer
	// under the write lock, so that a bug upstream degrades to "last write
	// wins" rather than to a list with two entries for one key.
	if succ := x.next[0]; succ != nil && ikey.Compare(succ.key, internal) == 0 {
		m.size += int64(len(val)) - int64(len(succ.val))
		succ.val = val
		return
	}

	h := m.randomHeight()
	if h > m.height {
		for lvl := m.height; lvl < h; lvl++ {
			m.prev[lvl] = m.head
		}
		m.height = h
	}

	nd := &node{key: internal, val: val, next: make([]*node, h)}
	for lvl := 0; lvl < h; lvl++ {
		nd.next[lvl] = m.prev[lvl].next[lvl]
		m.prev[lvl].next[lvl] = nd
	}

	m.n++
	m.size += int64(len(internal)) + int64(len(val)) + nodeOverhead + int64(h)*pointerSize
}

// Get returns the newest version of userKey visible at seq.
//
// found reports whether this memtable holds any version of the key at all.
// When it does, kind says whether that version is a value or a tombstone — a
// tombstone is an answer, not an absence, and the caller must stop searching
// older tables when it sees one.
//
// The returned value aliases the memtable. Callers that hand it outside the
// engine must copy it; storage.LSMStore does.
func (m *MemTable) Get(userKey []byte, seq uint64) (value []byte, kind ikey.Kind, found bool) {
	target := ikey.Seek(userKey, seq)

	m.mu.RLock()
	defer m.mu.RUnlock()

	nd := m.seekLocked(target)
	if nd == nil {
		return nil, 0, false
	}
	// seekLocked lands on the first entry >= target. Because target sorts at
	// or before every version of userKey, that entry is userKey's newest
	// visible version — if it belongs to userKey at all.
	if !equalUserKey(nd.key, userKey) {
		return nil, 0, false
	}
	return nd.val, ikey.KindOf(nd.key), true
}

// seekLocked returns the first node whose key is >= target, or nil.
func (m *MemTable) seekLocked(target []byte) *node {
	x := m.head
	for lvl := m.height - 1; lvl >= 0; lvl-- {
		for x.next[lvl] != nil && ikey.Compare(x.next[lvl].key, target) < 0 {
			x = x.next[lvl]
		}
	}
	return x.next[0]
}

func equalUserKey(internal, userKey []byte) bool {
	uk := ikey.UserKey(internal)
	if len(uk) != len(userKey) {
		return false
	}
	for i := range uk {
		if uk[i] != userKey[i] {
			return false
		}
	}
	return true
}

// Freeze marks the memtable immutable. It is idempotent.
//
// After Freeze the table is a stable snapshot: it can be iterated and read
// concurrently for as long as the flush takes, with no further synchronisation
// beyond the read lock the iterator already takes.
func (m *MemTable) Freeze() {
	m.mu.Lock()
	m.frozen = true
	m.mu.Unlock()
}

// Frozen reports whether Freeze has been called.
func (m *MemTable) Frozen() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.frozen
}

// Len returns the number of entries, counting every version of every key
// separately. Three writes to one key are three entries.
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.n
}

// ApproxSize returns an estimate of the bytes this memtable occupies: keys,
// values, and a fixed per-node allowance.
//
// It is the flush trigger, so it deliberately over-counts rather than under:
// a memtable that believes it is smaller than it is would grow past its budget
// and the "memory is bounded" claim would be false.
func (m *MemTable) ApproxSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Empty reports whether the memtable holds no entries.
func (m *MemTable) Empty() bool { return m.Len() == 0 }

// Iterator walks entries in internal-key order.
//
// Use is: for it.Next() { it.Key(); it.Value() }. Key and Value alias the
// memtable and stay valid for as long as the memtable does; they must not be
// modified.
type Iterator struct {
	m  *MemTable
	nd *node
	// started distinguishes "before the first entry" from "positioned at the
	// first entry", so that Next() is correct on a fresh iterator.
	started bool
}

// NewIterator returns an iterator positioned before the first entry.
func (m *MemTable) NewIterator() *Iterator { return &Iterator{m: m} }

// Next advances to the next entry and reports whether one exists.
func (it *Iterator) Next() bool {
	it.m.mu.RLock()
	defer it.m.mu.RUnlock()

	if !it.started {
		it.started = true
		it.nd = it.m.head.next[0]
	} else if it.nd != nil {
		it.nd = it.nd.next[0]
	}
	return it.nd != nil
}

// Seek positions the iterator at the first entry >= target, so that the next
// call to Key returns it. It reports whether such an entry exists.
func (it *Iterator) Seek(target []byte) bool {
	it.m.mu.RLock()
	defer it.m.mu.RUnlock()

	it.started = true
	it.nd = it.m.seekLocked(target)
	return it.nd != nil
}

// Key returns the internal key at the current position, or nil if exhausted.
func (it *Iterator) Key() []byte {
	if it.nd == nil {
		return nil
	}
	it.m.mu.RLock()
	defer it.m.mu.RUnlock()
	return it.nd.key
}

// Value returns the value at the current position. A tombstone's value is a
// zero-length slice; use ikey.KindOf(it.Key()) to tell it from an empty value.
//
// The read lock is taken even though a node's fields are written only before
// it is linked into the list, with one exception: Add replaces the value of a
// duplicate internal key in place. That cannot happen with sequence numbers
// the engine generates, but "cannot happen" is not a synchronisation strategy,
// and an unsynchronised read here would be a data race the day it did.
func (it *Iterator) Value() []byte {
	if it.nd == nil {
		return nil
	}
	it.m.mu.RLock()
	defer it.m.mu.RUnlock()
	return it.nd.val
}

// Valid reports whether the iterator is positioned at an entry.
func (it *Iterator) Valid() bool { return it.nd != nil }
