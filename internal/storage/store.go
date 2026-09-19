// Package storage implements Quorum's local key-value storage.
//
// Phase 1 provides a single in-memory implementation, MemStore. Later phases
// replace it with the write-ahead log / memtable / SSTable engine described in
// docs/DESIGN.md. The Store interface below is the seam: everything above it
// (CLI, HTTP API, Raft state machine) is written against the interface, so the
// engine swap must not change any client-visible semantic. The semantics that
// the swap must preserve are documented on the interface and enforced by the
// shared conformance tests in store_conformance_test.go.
package storage

import (
	"context"

	"github.com/adivishall/quorum/internal/storage/bloom"
	"github.com/adivishall/quorum/internal/storage/sstable"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// Default limits. These match docs/DESIGN.md §1 so that the in-memory store and
// the eventual on-disk engine reject exactly the same inputs; a key accepted in
// Phase 1 must still be accepted in Phase 3, and vice versa.
const (
	DefaultMaxKeySize   = 4 << 10 // 4 KiB
	DefaultMaxValueSize = 1 << 20 // 1 MiB

	// DefaultMemTableSize is the memtable's flush threshold in bytes. It
	// bounds how much data a restart has to replay from the WAL and how much
	// memory the engine holds for unflushed writes. 4 MiB is the LevelDB
	// write-buffer default and is a deliberate middle choice, not a measured
	// one: small enough that a flush is quick and a restart is cheap, large
	// enough that a workload of small values does not produce a file per
	// handful of keys. Phase 5 is where a number earns the right to be called
	// tuned.
	DefaultMemTableSize = 4 << 20 // 4 MiB
)

// Options configures a Store.
type Options struct {
	// MaxKeySize is the largest permitted key, in bytes. Must be > 0.
	MaxKeySize int
	// MaxValueSize is the largest permitted value, in bytes. Must be >= 0;
	// zero would permit only empty values, which is legal but useless.
	MaxValueSize int

	// WAL configures durability. It is ignored by MemStore, which has none.
	// Its zero value is the documented default (batch sync, 16 MiB segments).
	WAL wal.Options

	// MemTableSize is the byte threshold at which LSMStore flushes its
	// memtable to an SSTable. Zero selects DefaultMemTableSize. Ignored by
	// MemStore and WALStore, neither of which has a memtable.
	MemTableSize int64

	// BlockSize is the SSTable data-block target in bytes. Zero selects
	// sstable.DefaultBlockSize (docs/DESIGN.md §4).
	BlockSize int

	// MemTableSeed seeds the skip list's height generator. Zero selects the
	// package default. It exists so that a test can pin the structure the
	// memtable builds; it cannot change what the memtable contains or the
	// order it iterates in, only the tower heights.
	MemTableSeed uint64

	// BitsPerKey sizes each SSTable's Bloom filter (docs/DESIGN.md §5). Zero
	// selects bloom.DefaultBitsPerKey, which gives ~1% false positives.
	BitsPerKey int

	// DisableBloomFilter writes every SSTable's filter block empty, which is
	// exactly what Phase 3 did.
	//
	// It exists so that docs/BLOOM.md can measure with and without the filter
	// over the same data, and so the filterless read path is exercised against
	// real files. It is not a tuning knob: a store opened with it does strictly
	// more work per lookup.
	DisableBloomFilter bool

	// L0CompactionTrigger is the level-0 file count that starts a compaction.
	// Zero selects DefaultL0CompactionTrigger (4, per docs/DESIGN.md §7).
	L0CompactionTrigger int

	// L1MaxBytes is level 1's size budget; each deeper level gets ten times the
	// one above (docs/DESIGN.md §7). Zero selects DefaultL1MaxBytes.
	L1MaxBytes int64

	// DisableAutoCompaction stops the background compactor from being signalled.
	//
	// Compaction still runs when Compact or CompactAll is called. It exists so a
	// test can control exactly when the file set changes; a store that compacted
	// underneath an assertion about file counts would be untestable.
	DisableAutoCompaction bool

	// VerifySSTablesOnOpen reads every block of every referenced SSTable at
	// startup and checks every checksum, as Phase 3 did unconditionally.
	//
	// Phase 3 had to: the sequence range a file covered had nowhere on disk to
	// live, so recovering it meant reading the file. The MANIFEST records it now,
	// and startup instead cross-checks each file's footer against the MANIFEST's
	// record of it — which catches disagreement between the two for the price of
	// a 48-byte read.
	//
	// The cost of the default is stated rather than hidden: damage inside a data
	// block is found at the read that needs it instead of at startup. It is still
	// found, and still reported as corruption rather than as a missing key
	// (INV-L7). Set this when startup cost matters less than finding damage
	// early. See docs/LIMITATIONS.md.
	VerifySSTablesOnOpen bool

	// AdoptLegacySSTables permits opening a data directory that holds SSTables
	// but no CURRENT, by scanning those files and writing a MANIFEST describing
	// them.
	//
	// This is the one-time Phase 3 to Phase 4 upgrade path, and it is opt-in
	// because nothing on the filesystem distinguishes a Phase 3 database from a
	// Phase 4 database whose CURRENT was lost. Adopting automatically would mean
	// that deleting CURRENT silently returns the engine to inferring the file set
	// from the directory, which is the behaviour the MANIFEST exists to replace
	// (INV-S6).
	AdoptLegacySSTables bool
}

// DefaultOptions returns the limits from docs/DESIGN.md §1.
func DefaultOptions() Options {
	return Options{
		MaxKeySize:   DefaultMaxKeySize,
		MaxValueSize: DefaultMaxValueSize,
		WAL:          wal.DefaultOptions(),
		MemTableSize: DefaultMemTableSize,
		BlockSize:    sstable.DefaultBlockSize,
	}
}

// applyLSMDefaults fills in the LSM-specific zero values.
//
// It is separate from validate so that a caller can keep building Options the
// way Phase 1 and Phase 2 code does — MaxKeySize and MaxValueSize only — and
// still get a working engine. The conformance suite constructs Options that
// way, and it must keep passing unchanged.
func (o *Options) applyLSMDefaults() {
	if o.MemTableSize <= 0 {
		o.MemTableSize = DefaultMemTableSize
	}
	if o.BlockSize <= 0 {
		o.BlockSize = sstable.DefaultBlockSize
	}
	if o.BitsPerKey <= 0 {
		o.BitsPerKey = bloom.DefaultBitsPerKey
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = DefaultL0CompactionTrigger
	}
	if o.L1MaxBytes <= 0 {
		o.L1MaxBytes = DefaultL1MaxBytes
	}
}

// validate reports whether the options are usable.
func (o Options) validate() error {
	if o.MaxKeySize <= 0 {
		return opErr("open", nil, ErrInvalidOptions)
	}
	if o.MaxValueSize < 0 {
		return opErr("open", nil, ErrInvalidOptions)
	}
	if o.MemTableSize < 0 || o.BlockSize < 0 {
		return opErr("open", nil, ErrInvalidOptions)
	}
	if o.BitsPerKey < 0 || o.L0CompactionTrigger < 0 || o.L1MaxBytes < 0 {
		return opErr("open", nil, ErrInvalidOptions)
	}
	return nil
}

// Store is Quorum's local key-value store.
//
// # Ownership of byte slices
//
// Store never retains or exposes a caller's slice:
//
//   - Put copies both key and value before returning. The caller may reuse or
//     mutate its slices immediately afterwards.
//   - Get returns a freshly allocated copy. Mutating it cannot affect stored
//     data, and a later Get returns the original bytes.
//
// This costs an allocation per operation. It is deliberate: the alternative is
// an aliasing bug that manifests as silent data corruption under concurrency,
// which is precisely the class of defect this project cannot afford. The
// eventual LSM engine decodes into fresh buffers anyway, so the copying
// contract is free there and this is not a semantic we will have to walk back.
//
// # Keys and values
//
// Keys and values are opaque byte strings. Any byte sequence is a valid key,
// including whitespace, newlines, NUL bytes, and invalid UTF-8 — the only
// constraints are non-empty and within MaxKeySize. Keys are compared bytewise,
// so they are case-sensitive: "Key" and "key" are different keys. Validating
// the *shape* of a key (rejecting control characters, enforcing a namespace
// convention) is an API-boundary concern, not a storage concern; a byte store
// that second-guesses its keys cannot faithfully store what it is given.
//
// A value may be empty. An empty value is not the same as an absent key: Get on
// the former returns a zero-length, non-nil slice and a nil error; Get on the
// latter returns ErrNotFound.
//
// # Concurrency
//
// All methods are safe for concurrent use by multiple goroutines. Each
// individual operation is atomic with respect to every other operation on the
// same Store: a Get observes either the whole of a Put or none of it, never a
// torn value. There is no atomicity *across* operations — no transactions, no
// compare-and-swap, no multi-key batch. That restriction is load-bearing for
// the consistency argument in docs/CONSISTENCY.md §C2 and is not an oversight.
//
// # Context
//
// Every method checks ctx before doing work and returns ctx.Err() if it is
// already cancelled or expired. Phase 1 operations are non-blocking, so ctx
// cannot be observed mid-operation; it is present now because Raft proposals in
// Phase 9 can block indefinitely, and adding the parameter later would be a
// breaking change to every caller.
type Store interface {
	// Put stores value under key, replacing any existing value.
	//
	// Errors: ErrKeyEmpty, ErrKeyTooLarge, ErrValueTooLarge, ErrClosed,
	// or ctx.Err(). All are wrapped in *OpError.
	Put(ctx context.Context, key, value []byte) error

	// Get returns a copy of the value stored under key.
	//
	// Errors: ErrNotFound, ErrKeyEmpty, ErrKeyTooLarge, ErrClosed,
	// or ctx.Err(). All are wrapped in *OpError.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Delete removes key.
	//
	// Delete is idempotent and does not report whether the key existed:
	// deleting an absent key succeeds. This is not a convenience — it is
	// forced by the engine that replaces this one. An LSM tree deletes by
	// writing a tombstone, a blind append that does not consult the current
	// state. Reporting existence would require a full read (memtable, then
	// every SSTable, consulting a Bloom filter per file) before every delete,
	// and under Raft it would additionally have to happen inside the state
	// machine to stay deterministic across replicas. Promising it here would
	// mean breaking the promise in Phase 3 or paying for it forever.
	//
	// Errors: ErrKeyEmpty, ErrKeyTooLarge, ErrClosed, or ctx.Err().
	// All are wrapped in *OpError.
	Delete(ctx context.Context, key []byte) error

	// Close releases resources. It is idempotent: calling it more than once
	// returns nil. After it returns, every other method returns ErrClosed.
	Close() error
}

// validateKey applies the key rules shared by every Store implementation.
func (o Options) validateKey(op string, key []byte) error {
	if len(key) == 0 {
		return opErr(op, key, ErrKeyEmpty)
	}
	if len(key) > o.MaxKeySize {
		return opErr(op, key, ErrKeyTooLarge)
	}
	return nil
}

// validateValue applies the value rules shared by every Store implementation.
func (o Options) validateValue(op string, key, value []byte) error {
	if len(value) > o.MaxValueSize {
		return opErr(op, key, ErrValueTooLarge)
	}
	return nil
}

// clone returns a copy of b. A nil or empty input yields a non-nil, zero-length
// slice, so callers can always distinguish "empty value" from "no value" by the
// error rather than by nil-ness of the slice.
func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
