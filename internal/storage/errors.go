package storage

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// Sentinel errors returned by every Store implementation.
//
// These are the complete, closed set of *classes* of failure the storage layer
// can report. Callers must branch on them with errors.Is, never on error
// strings. Later phases add durability and consensus errors; they will be added
// to this list rather than returned as bare fmt.Errorf values.
var (
	// ErrNotFound means the key has no value. It is distinct from a key whose
	// value is the empty byte slice: that is a present key, and Get returns a
	// zero-length (non-nil) slice with a nil error.
	ErrNotFound = errors.New("key not found")

	// ErrKeyEmpty means a zero-length key was supplied. Zero-length keys are
	// rejected because they cannot be distinguished from "no key" in the wire
	// protocol or in the on-disk internal key encoding (docs/DESIGN.md §1).
	ErrKeyEmpty = errors.New("key is empty")

	// ErrKeyTooLarge means the key exceeds Options.MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge means the value exceeds Options.MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")

	// ErrClosed means the Store has been closed. It is returned by every
	// operation after Close returns, and it is permanent: a closed Store is
	// never reopened.
	ErrClosed = errors.New("store is closed")

	// ErrInvalidOptions means Options failed validation at construction time.
	ErrInvalidOptions = errors.New("invalid options")

	// ErrCorrupt means on-disk data could not be trusted and the store refused
	// to open rather than serving a state that may be silently missing writes.
	//
	// This is deliberately the same error value as record.ErrCorrupt, so a
	// caller has one sentinel to test whether the damage was caught by a
	// framing checksum, by payload decoding, or by a structural check such as
	// a missing WAL segment.
	ErrCorrupt = record.ErrCorrupt

	// ErrIO means the filesystem failed. The underlying error is preserved in
	// the message; it is not part of the API, because the set of errors a
	// filesystem can produce is open-ended and platform-specific.
	ErrIO = errors.New("i/o error")
)

// OpError wraps a sentinel error with the operation and key that produced it.
//
// It exists so that error messages are actionable without callers having to
// parse them: errors.Is(err, ErrNotFound) keeps working, while the message
// carries enough context to debug from a log line.
//
// The message deliberately does NOT carry a "dkv: " program prefix. Naming the
// program is the job of whatever is printing — the CLI prefixes its stderr
// output, a server would emit a structured field instead. Baking it in here
// produced "dkv: dkv: get \"k\": key not found" in the CLI, which is how this
// was found. This mirrors the standard library: *fs.PathError renders as
// `open /foo: no such file or directory`, and the command adds its own name.
type OpError struct {
	Op  string // "put", "get", "delete"
	Key []byte // the key involved; may be nil for non-key-specific failures
	Err error  // one of the sentinels above
}

func (e *OpError) Error() string {
	if e.Key == nil {
		return fmt.Sprintf("%s: %v", e.Op, e.Err)
	}
	return fmt.Sprintf("%s %s: %v", e.Op, SafeKey(e.Key), e.Err)
}

// Unwrap lets errors.Is and errors.As see the underlying sentinel.
func (e *OpError) Unwrap() error { return e.Err }

func opErr(op string, key []byte, err error) error {
	return &OpError{Op: op, Key: key, Err: err}
}

// maxDisplayKey bounds how much of a key is ever rendered into an error string
// or a log line. Keys may be up to MaxKeySize (4 KiB by default) and may
// contain arbitrary bytes, including newlines and NULs; neither belongs
// unbounded and unescaped in a log.
const maxDisplayKey = 64

// SafeKey renders a key for human consumption: quoted, escaped, and truncated.
// It is the only sanctioned way to put a key into a message or a log.
func SafeKey(key []byte) string {
	if len(key) <= maxDisplayKey {
		return strconv.Quote(string(key))
	}
	return fmt.Sprintf("%s...(%d bytes)", strconv.Quote(string(key[:maxDisplayKey])), len(key))
}

// classify maps an error from a lower layer onto the storage taxonomy.
//
// The mapping is total: corruption and closure keep their identity, and
// anything else becomes ErrIO with the cause preserved in the message. Nothing
// falls through unclassified, because an unclassified error at this boundary
// would reach the CLI as an internal error with no useful category.
func classify(op string, key []byte, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrCorrupt):
		return opErr(op, key, err)
	case errors.Is(err, wal.ErrClosed), errors.Is(err, ErrClosed):
		return opErr(op, key, ErrClosed)
	default:
		return opErr(op, key, fmt.Errorf("%w: %v", ErrIO, err))
	}
}
