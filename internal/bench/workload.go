package bench

import (
	"encoding/binary"
	"fmt"
	"math/rand"
)

// Keyspace generates the deterministic keys a benchmark operates on. Keys are
// fixed width and zero-padded so they sort lexicographically in index order,
// which matches how an LSM stores them and keeps sequential-write benchmarks
// genuinely sequential. The same index always produces the same key, so a read
// benchmark can look up keys a write benchmark stored, in a separate process,
// without sharing state.
type Keyspace struct {
	prefix string
	width  int
}

// NewKeyspace returns a keyspace whose keys are "<prefix>" followed by a
// zero-padded decimal index wide enough for n distinct keys (minimum 9 digits,
// so a key is a stable width across the dataset sizes a scaling benchmark uses).
func NewKeyspace(prefix string, n int) Keyspace {
	width := 9
	for d, cap := 1, 10; d < 18; d, cap = d+1, cap*10 {
		if n <= cap {
			if d > width {
				width = d
			}
			break
		}
	}
	return Keyspace{prefix: prefix, width: width}
}

// Key returns the key at index i. It allocates; callers in a hot loop that can
// reuse a buffer should use AppendKey.
func (k Keyspace) Key(i int) []byte {
	return k.AppendKey(nil, i)
}

// AppendKey appends the key at index i to dst and returns the extended slice.
func (k Keyspace) AppendKey(dst []byte, i int) []byte {
	return fmt.Appendf(dst, "%s%0*d", k.prefix, k.width, i)
}

// KeyBytes is the exact byte length of every key in this keyspace.
func (k Keyspace) KeyBytes() int { return len(k.prefix) + k.width }

// Value returns a deterministic value of the given size for index i. The bytes
// are derived from i so that a read benchmark can verify it got the value the
// write benchmark stored, and so that two runs with the same parameters store
// identical bytes. The first eight bytes encode i (for verification); the rest
// is a cheap keyed pattern that is not a single repeated byte, so the value is
// not trivially compressible were compression ever added.
func Value(size, i int) []byte {
	v := make([]byte, size)
	FillValue(v, i)
	return v
}

// FillValue writes the deterministic value for index i into buf, filling its
// whole length. Reused buffers avoid an allocation per operation.
func FillValue(buf []byte, i int) {
	if len(buf) >= 8 {
		binary.BigEndian.PutUint64(buf, uint64(i))
		for j := 8; j < len(buf); j++ {
			buf[j] = byte(i*31 + j*7)
		}
		return
	}
	for j := range buf {
		buf[j] = byte(i*31 + j*7)
	}
}

// ValueIndex reads back the index FillValue encoded, for read verification. It
// is valid only for values of at least eight bytes.
func ValueIndex(buf []byte) (uint64, bool) {
	if len(buf) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(buf), true
}

// OpKind is the operation a mixed workload chose for one step.
type OpKind int

const (
	OpRead OpKind = iota
	OpWrite
	OpDelete
)

func (o OpKind) String() string {
	switch o {
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Mix is a workload's read/write/delete proportions. The weights are relative,
// not required to sum to 100, and Name labels the mix in results and docs so a
// table never shows an unexplained ratio.
type Mix struct {
	Name    string
	Reads   int
	Writes  int
	Deletes int
}

// Total is the sum of the weights.
func (m Mix) Total() int { return m.Reads + m.Writes + m.Deletes }

// Pick chooses an operation according to the weights, using r for randomness so
// the sequence is reproducible from the run's seed.
func (m Mix) Pick(r *rand.Rand) OpKind {
	total := m.Total()
	if total <= 0 {
		return OpRead
	}
	x := r.Intn(total)
	if x < m.Reads {
		return OpRead
	}
	if x < m.Reads+m.Writes {
		return OpWrite
	}
	return OpDelete
}

// Ratio renders the mix as a "R/W/D" percentage string for a results label.
func (m Mix) Ratio() string {
	t := m.Total()
	if t == 0 {
		return "0/0/0"
	}
	pct := func(w int) int { return int(float64(w)*100.0/float64(t) + 0.5) }
	return fmt.Sprintf("%d/%d/%d", pct(m.Reads), pct(m.Writes), pct(m.Deletes))
}

// Standard mixes used by the mixed-workload benchmark, documented in
// docs/BENCHMARKS.md §3.4. They are named for what they resemble, not called
// "realistic": the point of running several is that no single ratio is.
var (
	MixReadHeavy  = Mix{Name: "read-heavy", Reads: 90, Writes: 9, Deletes: 1}
	MixBalanced   = Mix{Name: "balanced", Reads: 50, Writes: 45, Deletes: 5}
	MixWriteHeavy = Mix{Name: "write-heavy", Reads: 20, Writes: 75, Deletes: 5}
)

// StandardMixes returns the documented mixes in a stable order.
func StandardMixes() []Mix { return []Mix{MixReadHeavy, MixBalanced, MixWriteHeavy} }
