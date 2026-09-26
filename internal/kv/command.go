package kv

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/adivishall/quorum/internal/storage"
)

// Op is a write operation carried in a Raft log entry.
type Op uint8

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	}
	return fmt.Sprintf("op(%d)", o)
}

// Limits, the Phase 1 contract (docs/DESIGN.md §1).
const (
	MaxKeyLen   = storage.DefaultMaxKeySize
	MaxValueLen = storage.DefaultMaxValueSize
)

// ErrMalformedCommand means bytes in a log entry are not a Command. The Store
// refuses to apply them rather than guessing (a corrupt or foreign command is a
// bug, and applying nothing would silently diverge from the replicas that did).
var ErrMalformedCommand = errors.New("kv: malformed command")

// Command is one write: PUT key=value or DELETE key.
type Command struct {
	Op    Op
	Key   []byte
	Value []byte // PUT only; may be empty (an empty value is a present key)
}

// Encode renders the command deterministically:
//
//	op u8 | keyLen uvarint | key | valueLen uvarint | value   (value only for PUT)
func (c Command) Encode() []byte {
	b := make([]byte, 0, 1+2*binary.MaxVarintLen64+len(c.Key)+len(c.Value))
	b = append(b, byte(c.Op))
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	if c.Op == OpPut {
		b = binary.AppendUvarint(b, uint64(len(c.Value)))
		b = append(b, c.Value...)
	}
	return b
}

// Decode parses a command. It is total on hostile input: every length is checked
// against the bytes remaining and the limits before anything is allocated, and
// trailing bytes are an error. Key and Value are fresh copies.
func Decode(b []byte) (Command, error) {
	if len(b) < 1 {
		return Command{}, ErrMalformedCommand
	}
	c := Command{Op: Op(b[0])}
	if c.Op != OpPut && c.Op != OpDelete {
		return Command{}, fmt.Errorf("%w: unknown op %d", ErrMalformedCommand, b[0])
	}
	i := 1
	key, n, err := readBytes(b[i:], MaxKeyLen)
	if err != nil {
		return Command{}, err
	}
	if len(key) == 0 {
		return Command{}, fmt.Errorf("%w: empty key", ErrMalformedCommand)
	}
	c.Key = key
	i += n
	if c.Op == OpPut {
		val, n, err := readBytes(b[i:], MaxValueLen)
		if err != nil {
			return Command{}, err
		}
		c.Value = val
		i += n
	}
	if i != len(b) {
		return Command{}, fmt.Errorf("%w: %d trailing bytes", ErrMalformedCommand, len(b)-i)
	}
	return c, nil
}

// readBytes reads a uvarint length then that many bytes (bounded by max),
// returning a copy and the bytes consumed. A zero length yields a non-nil empty
// slice, so an empty value stays distinguishable from "no value".
func readBytes(b []byte, max int) ([]byte, int, error) {
	l, n := uvarint(b)
	if n <= 0 {
		return nil, 0, fmt.Errorf("%w: bad length", ErrMalformedCommand)
	}
	if l > uint64(max) || l > uint64(len(b)-n) {
		return nil, 0, fmt.Errorf("%w: length %d out of range", ErrMalformedCommand, l)
	}
	out := make([]byte, l)
	copy(out, b[n:n+int(l)])
	return out, n + int(l), nil
}

// uvarint is binary.Uvarint restricted to the minimal (canonical) encoding: a
// value written in more bytes than it needs (e.g. 0 as 0x80 0x00) is refused
// (n = 0), so every byte string these codecs accept has exactly one meaning and
// is exactly what Encode would produce for it — byte identity is command
// identity. Found by fuzzing (docs/LINEARIZABILITY.md §13).
func uvarint(b []byte) (uint64, int) {
	v, n := binary.Uvarint(b)
	if n > 0 && n != uvarintLen(v) {
		return 0, 0
	}
	return v, n
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// IsCommand reports whether b decodes as a Command (a cheap way for a harness to
// tell client writes from other log entries, e.g. the election no-op).
func IsCommand(b []byte) bool {
	_, err := Decode(b)
	return err == nil
}
