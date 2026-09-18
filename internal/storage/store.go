// Package storage implements dkv's local key-value storage.
//
// Phase 1 provides a single in-memory implementation, MemStore. Later phases
// replace it with the write-ahead log / memtable / SSTable engine described in
// docs/DESIGN.md. The Store interface below is the seam: everything above it
// (CLI, HTTP API, Raft state machine) is written against the interface, so the
// engine swap must not change any client-visible semantic. The semantics that
// the swap must preserve are documented on the interface and enforced by the
// shared conformance tests in store_conformance_test.go.
package storage

import "context"

// Default limits. These match docs/DESIGN.md §1 so that the in-memory store and
// the eventual on-disk engine reject exactly the same inputs; a key accepted in
// Phase 1 must still be accepted in Phase 3, and vice versa.
const (
	DefaultMaxKeySize   = 4 << 10 // 4 KiB
	DefaultMaxValueSize = 1 << 20 // 1 MiB
)

// Options configures a Store.
type Options struct {
	// MaxKeySize is the largest permitted key, in bytes. Must be > 0.
	MaxKeySize int
	// MaxValueSize is the largest permitted value, in bytes. Must be >= 0;
	// zero would permit only empty values, which is legal but useless.
	MaxValueSize int
}

// DefaultOptions returns the limits from docs/DESIGN.md §1.
func DefaultOptions() Options {
	return Options{
		MaxKeySize:   DefaultMaxKeySize,
		MaxValueSize: DefaultMaxValueSize,
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
	return nil
}

// Store is dkv's local key-value store.
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
