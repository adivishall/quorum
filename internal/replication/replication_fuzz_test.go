package replication

import (
	"bytes"
	"testing"
)

// nextByte returns data[*i] and advances i, or 0 once the input is exhausted, so
// a program can be decoded from an arbitrary byte string without bounds panics.
func nextByte(data []byte, i *int) byte {
	if *i >= len(data) {
		return 0
	}
	b := data[*i]
	*i++
	return b
}

// execProgram interprets data as a bounded sequence of log operations and runs
// them against l, asserting the structural invariants after every step. Entry
// data is generated (not taken from the fuzz input) and the op count is capped,
// so allocation is bounded regardless of input length. It is a pure function of
// data, which is what makes the determinism check meaningful.
func execProgram(t *testing.T, l *MemoryLog, data []byte) {
	t.Helper()
	i := 0
	for ops := 0; i < len(data) && ops < 200; ops++ {
		op := nextByte(data, &i)
		switch op % 4 {
		case 0: // append at the end (structurally contiguous; term may regress)
			n := 1 + int(nextByte(data, &i))%3
			term := uint64(nextByte(data, &i)) % 6
			_ = l.Append(makeBatch(l.LastIndex()+1, term, n)...)
		case 1: // truncateAndAppend; f may hit a committed entry or open a gap
			last := l.LastIndex()
			f := 1 + uint64(nextByte(data, &i))%(last+3)
			term := uint64(nextByte(data, &i)) % 6
			n := 1 + int(nextByte(data, &i))%3
			_ = l.TruncateAndAppend(makeBatch(f, term, n)...)
		case 2: // commit, target possibly illegal
			target := uint64(nextByte(data, &i)) % (l.LastIndex() + 2)
			_ = l.Commit(target)
		case 3: // apply, target possibly illegal
			target := uint64(nextByte(data, &i)) % (l.LastIndex() + 2)
			_ = l.Apply(target)
		}
		assertInvariants(t, ops, l)
	}
}

// FuzzLogOperations feeds arbitrary programs of log operations and establishes:
// nothing panics on malformed indexes/ranges/entries, the invariants hold after
// every step, behaviour is deterministic (a replay produces identical state), and
// reads do not alias internal storage.
func FuzzLogOperations(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{0, 3, 2, 0, 0, 5, 0, 2, 8, 1, 4, 2})
	f.Add(bytes.Repeat([]byte{0x11, 0x02, 0x33, 0x01}, 30))

	f.Fuzz(func(t *testing.T, data []byte) {
		l1 := NewMemoryLog()
		execProgram(t, l1, data)

		// Determinism: the same program on a fresh log yields identical state.
		l2 := NewMemoryLog()
		execProgram(t, l2, data)
		if l1.LastIndex() != l2.LastIndex() || l1.CommitIndex() != l2.CommitIndex() || l1.AppliedIndex() != l2.AppliedIndex() {
			t.Fatalf("non-deterministic: l1=(%d,%d,%d) l2=(%d,%d,%d)",
				l1.LastIndex(), l1.CommitIndex(), l1.AppliedIndex(),
				l2.LastIndex(), l2.CommitIndex(), l2.AppliedIndex())
		}
		for i := uint64(1); i <= l1.LastIndex(); i++ {
			a, _ := l1.At(i)
			b, _ := l2.At(i)
			if a.Index != b.Index || a.Term != b.Term || !bytes.Equal(a.Data, b.Data) {
				t.Fatalf("non-deterministic entry %d: %+v vs %+v", i, a, b)
			}
		}

		// Aliasing: mutating a read cannot change stored bytes.
		if l1.LastIndex() >= 1 {
			e, _ := l1.At(1)
			orig := append([]byte(nil), e.Data...)
			for j := range e.Data {
				e.Data[j] ^= 0xff
			}
			again, _ := l1.At(1)
			if !bytes.Equal(again.Data, orig) {
				t.Fatalf("read exposed internal storage at index 1: %q became %q", orig, again.Data)
			}
		}
	})
}

// FuzzReplicaGroup feeds arbitrary replica lists and factors and establishes that
// construction never panics, and that any group it accepts is self-consistent:
// RF equals the replica count, no id is empty or duplicated, the head is the
// primary, and the group is immutable.
func FuzzReplicaGroup(f *testing.F) {
	f.Add(uint32(0), "n0,n1,n2", 3)
	f.Add(uint32(1), "", 0)
	f.Add(uint32(2), "a,a,b", 3)
	f.Add(uint32(3), "x,,z", 3)

	f.Fuzz(func(t *testing.T, shard uint32, csv string, rf int) {
		replicas := splitCSV(csv)
		g, err := NewReplicaGroup(ShardID(shard), replicas, rf)
		if err != nil {
			return // a rejected group is a valid outcome; it must not panic
		}
		// An accepted group is self-consistent.
		got := g.Replicas()
		if len(got) != g.ReplicationFactor() {
			t.Fatalf("accepted group with rf=%d but %d replicas", g.ReplicationFactor(), len(got))
		}
		seen := map[NodeID]bool{}
		for _, id := range got {
			if id == "" {
				t.Fatalf("accepted group contains an empty id: %v", got)
			}
			if seen[id] {
				t.Fatalf("accepted group contains a duplicate id: %v", got)
			}
			seen[id] = true
		}
		if g.Primary() != got[0] {
			t.Fatalf("Primary() %q != head %q", g.Primary(), got[0])
		}
		// Immutability: mutating the returned slice cannot change the group.
		got[0] = "MUTATED"
		if g.Primary() == "MUTATED" {
			t.Fatalf("Replicas() exposed internal storage")
		}
	})
}

// splitCSV splits a comma-separated list into node ids, preserving empty fields
// so the fuzzer can exercise the empty-id rejection. An empty string yields no
// replicas (an empty group).
func splitCSV(s string) []NodeID {
	if s == "" {
		return nil
	}
	var out []NodeID
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			out = append(out, NodeID(s[start:i]))
			start = i + 1
		}
	}
	return out
}
