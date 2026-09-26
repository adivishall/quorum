package kv

import (
	"crypto/sha256"
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
	// OpRegister creates a client session (Phase 13, docs/CLIENT_SEMANTICS.md
	// §2); its id is the log index of the entry that carries it.
	OpRegister Op = 3
)

// Wire op codes of an identified write (Phase 13). An anonymous PUT/DELETE keeps
// its Phase 12 encoding (op 1/2) byte for byte; an identified one is op 4/5 with
// its identity in front of the key. Command.Op is always Put/Delete/Register —
// these codes are only the encoding.
const (
	opPutIdentified    = 4
	opDeleteIdentified = 5
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpRegister:
		return "register"
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

// Command is one write: PUT key=value, DELETE key, or REGISTER (a new client
// session). A PUT or DELETE with ClientID != 0 is identified (Phase 13): it
// belongs to session ClientID as its request RequestID, and AckedBelow is the
// client's acknowledgement watermark (docs/CLIENT_SEMANTICS.md §3). ClientID 0
// is anonymous (Phase 12 semantics: no deduplication).
type Command struct {
	Op    Op
	Key   []byte
	Value []byte // PUT only; may be empty (an empty value is a present key)

	ClientID, RequestID, AckedBelow uint64
}

// Encode renders the command deterministically:
//
//	anonymous PUT/DELETE  op(1|2) | keyLen | key | [valueLen | value]            (Phase 12)
//	identified            op(4|5) | clientID | requestID | ackedBelow | keyLen | key | [valueLen | value]
//	REGISTER              op(3)
//
// all integers uvarint (canonical). Encode assumes a valid command (Validate).
func (c Command) Encode() []byte {
	b := make([]byte, 0, 1+5*binary.MaxVarintLen64+len(c.Key)+len(c.Value))
	switch {
	case c.Op == OpRegister:
		return append(b, byte(OpRegister))
	case c.ClientID != 0 && c.Op == OpPut:
		b = append(b, opPutIdentified)
	case c.ClientID != 0:
		b = append(b, opDeleteIdentified)
	default:
		b = append(b, byte(c.Op))
	}
	if c.ClientID != 0 {
		b = binary.AppendUvarint(b, c.ClientID)
		b = binary.AppendUvarint(b, c.RequestID)
		b = binary.AppendUvarint(b, c.AckedBelow)
	}
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	if c.Op == OpPut {
		b = binary.AppendUvarint(b, uint64(len(c.Value)))
		b = append(b, c.Value...)
	}
	return b
}

// Fingerprint identifies the command's EFFECT — operation, key and value, never
// its identity or watermark — so the same request re-sent with a newer
// AckedBelow is still recognised as the same command. It is SHA-256 of the
// anonymous canonical encoding, computed by the state machine itself.
func (c Command) Fingerprint() [32]byte {
	return sha256.Sum256(Command{Op: c.Op, Key: c.Key, Value: c.Value}.Encode())
}

// Validate reports whether the command satisfies the protocol's rules: a
// non-empty key within MaxKeyLen, a value within MaxValueLen, and for an
// identified command RequestID ≥ 1 and 1 ≤ AckedBelow ≤ RequestID. Decode
// accepts exactly the commands Validate accepts, so a command a server has
// validated always applies.
func (c Command) Validate() error {
	switch c.Op {
	case OpRegister:
		if len(c.Key) != 0 || len(c.Value) != 0 || c.ClientID != 0 || c.RequestID != 0 || c.AckedBelow != 0 {
			return fmt.Errorf("%w: register carries no key, value or identity", ErrMalformedCommand)
		}
		return nil
	case OpPut, OpDelete:
	default:
		return fmt.Errorf("%w: unknown op %d", ErrMalformedCommand, c.Op)
	}
	if len(c.Key) == 0 || len(c.Key) > MaxKeyLen {
		return fmt.Errorf("%w: key of %d bytes", ErrMalformedCommand, len(c.Key))
	}
	if len(c.Value) > MaxValueLen || (c.Op == OpDelete && c.Value != nil) {
		return fmt.Errorf("%w: bad value", ErrMalformedCommand)
	}
	if c.ClientID == 0 {
		if c.RequestID != 0 || c.AckedBelow != 0 {
			return fmt.Errorf("%w: an anonymous command carries no request id", ErrMalformedCommand)
		}
		return nil
	}
	if c.RequestID == 0 || c.AckedBelow == 0 || c.AckedBelow > c.RequestID {
		return fmt.Errorf("%w: identified command needs requestID >= 1 and 1 <= ackedBelow <= requestID (got %d, %d)", ErrMalformedCommand, c.RequestID, c.AckedBelow)
	}
	return nil
}

// Decode parses a command. It is total on hostile input: every length is checked
// against the bytes remaining and the limits before anything is allocated,
// integers must be canonical, and trailing bytes are an error. Key and Value are
// fresh copies. It accepts exactly what Encode produces from a valid Command.
func Decode(b []byte) (Command, error) {
	if len(b) < 1 {
		return Command{}, ErrMalformedCommand
	}
	var c Command
	i := 1
	switch b[0] {
	case byte(OpRegister):
		c.Op = OpRegister
	case byte(OpPut), byte(OpDelete):
		c.Op = Op(b[0])
	case opPutIdentified, opDeleteIdentified:
		c.Op = OpPut
		if b[0] == opDeleteIdentified {
			c.Op = OpDelete
		}
		for _, f := range []*uint64{&c.ClientID, &c.RequestID, &c.AckedBelow} {
			v, n := uvarint(b[i:])
			if n <= 0 {
				return Command{}, fmt.Errorf("%w: bad identity field", ErrMalformedCommand)
			}
			*f = v
			i += n
		}
		if c.ClientID == 0 {
			return Command{}, fmt.Errorf("%w: identified command with client id 0", ErrMalformedCommand)
		}
	default:
		return Command{}, fmt.Errorf("%w: unknown op %d", ErrMalformedCommand, b[0])
	}
	if c.Op != OpRegister {
		key, n, err := readBytes(b[i:], MaxKeyLen)
		if err != nil {
			return Command{}, err
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
	}
	if i != len(b) {
		return Command{}, fmt.Errorf("%w: %d trailing bytes", ErrMalformedCommand, len(b)-i)
	}
	if err := c.Validate(); err != nil {
		return Command{}, err
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
