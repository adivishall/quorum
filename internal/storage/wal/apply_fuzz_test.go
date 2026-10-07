package wal_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/adivishall/quorum/internal/storage/wal"
)

// FuzzApplyBatchRoundTrip: whatever this package encodes as an apply batch, it
// decodes back to the same batch — the index, the term and every operation.
func FuzzApplyBatchRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(1), []byte("k"), []byte("v"), true, uint8(2))
	f.Add(uint64(1<<63), uint64(7), []byte("key"), []byte(""), false, uint8(0))
	f.Fuzz(func(t *testing.T, index, term uint64, key, value []byte, isDelete bool, n uint8) {
		if index == 0 || term == 0 || len(key) == 0 || len(key) > 4096 || len(value) > 4096 {
			t.Skip()
		}
		ab := wal.ApplyBatch{Applied: wal.AppliedIndex{Index: index, Term: term}}
		for i := 0; i < int(n%8); i++ {
			op := wal.Op{Kind: wal.OpPut, Key: append(append([]byte(nil), key...), byte(i)), Value: value}
			if isDelete && i%2 == 1 {
				op = wal.Op{Kind: wal.OpDelete, Key: op.Key}
			}
			ab.Ops = append(ab.Ops, op)
		}
		got, err := wal.DecodeApplyBatch(ab.AppendTo(nil))
		if err != nil {
			t.Fatalf("an apply batch this package encoded failed to decode: %v", err)
		}
		if got.Applied != ab.Applied || len(got.Ops) != len(ab.Ops) {
			t.Fatalf("got %+v with %d ops, want %+v with %d", got.Applied, len(got.Ops), ab.Applied, len(ab.Ops))
		}
		for i := range ab.Ops {
			if got.Ops[i].Kind != ab.Ops[i].Kind || !bytes.Equal(got.Ops[i].Key, ab.Ops[i].Key) || !bytes.Equal(got.Ops[i].Value, ab.Ops[i].Value) {
				t.Fatalf("op %d changed: %+v, want %+v", i, got.Ops[i], ab.Ops[i])
			}
		}
	})
}

// FuzzDecodeApplyBatchIsTotal: its input comes off disk, so on any bytes the
// decoder fails cleanly with ErrCorrupt or returns a batch that names an entry
// and re-encodes to exactly the bytes it came from (the encoding is canonical:
// a payload that decodes has one meaning).
func FuzzDecodeApplyBatchIsTotal(f *testing.F) {
	f.Add(wal.ApplyBatch{Applied: wal.AppliedIndex{Index: 3, Term: 2}, Ops: wal.Batch{{Kind: wal.OpPut, Key: []byte("k"), Value: []byte("v")}}}.AppendTo(nil))
	f.Add(wal.ApplyBatch{Applied: wal.AppliedIndex{Index: 1, Term: 1}}.AppendTo(nil))
	f.Add([]byte{0x01})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, payload []byte) {
		ab, err := wal.DecodeApplyBatch(payload)
		if err != nil {
			if !errors.Is(err, wal.ErrCorrupt) {
				t.Fatalf("DecodeApplyBatch returned %v, which is not ErrCorrupt", err)
			}
			return
		}
		if ab.Applied.Index == 0 || ab.Applied.Term == 0 {
			t.Fatalf("decoded an apply batch at (%d, %d), which names no entry", ab.Applied.Index, ab.Applied.Term)
		}
		for _, op := range ab.Ops {
			if len(op.Key) == 0 || (op.Kind != wal.OpPut && op.Kind != wal.OpDelete) {
				t.Fatalf("decoded an invalid operation %+v", op)
			}
		}
		if again := ab.AppendTo(nil); !bytes.Equal(again, payload) {
			t.Fatalf("a decoded payload re-encodes differently:\n % x\n % x", payload, again)
		}
	})
}
