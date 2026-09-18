package wal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/adivishall/distributed-kv/internal/storage/wal"
)

func TestBatchRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		batch wal.Batch
	}{
		{"single put", wal.Batch{{Kind: wal.OpPut, Key: []byte("k"), Value: []byte("v")}}},
		{"single delete", wal.Batch{{Kind: wal.OpDelete, Key: []byte("k")}}},
		{"empty value", wal.Batch{{Kind: wal.OpPut, Key: []byte("k"), Value: []byte{}}}},
		{"nil value on put", wal.Batch{{Kind: wal.OpPut, Key: []byte("k"), Value: nil}}},
		{"binary key", wal.Batch{{Kind: wal.OpPut, Key: []byte{0x00, 0xff, 0x00}, Value: []byte{0xde, 0xad}}}},
		{"whitespace key", wal.Batch{{Kind: wal.OpPut, Key: []byte("  a b\n"), Value: []byte("v")}}},
		{"mixed", wal.Batch{
			{Kind: wal.OpPut, Key: []byte("a"), Value: []byte("1")},
			{Kind: wal.OpDelete, Key: []byte("b")},
			{Kind: wal.OpPut, Key: []byte("c"), Value: bytes.Repeat([]byte("x"), 4096)},
			{Kind: wal.OpDelete, Key: []byte("d")},
		}},
		{"large key and value", wal.Batch{
			{Kind: wal.OpPut, Key: bytes.Repeat([]byte("k"), 4<<10), Value: bytes.Repeat([]byte("v"), 1<<20)},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := tc.batch.AppendTo(nil)
			got, err := wal.DecodeBatch(encoded)
			if err != nil {
				t.Fatalf("DecodeBatch: %v", err)
			}
			if len(got) != len(tc.batch) {
				t.Fatalf("decoded %d operations, want %d", len(got), len(tc.batch))
			}
			for i := range got {
				if got[i].Kind != tc.batch[i].Kind {
					t.Errorf("op %d: kind = %v, want %v", i, got[i].Kind, tc.batch[i].Kind)
				}
				if !bytes.Equal(got[i].Key, tc.batch[i].Key) {
					t.Errorf("op %d: key = %q, want %q", i, got[i].Key, tc.batch[i].Key)
				}
				if tc.batch[i].Kind == wal.OpDelete {
					if got[i].Value != nil {
						t.Errorf("op %d: delete decoded with a value %q", i, got[i].Value)
					}
					continue
				}
				if !bytes.Equal(got[i].Value, tc.batch[i].Value) {
					t.Errorf("op %d: value = %q, want %q", i, got[i].Value, tc.batch[i].Value)
				}
			}
		})
	}
}

// TestDecodeBatchRejectsMalformedPayloads is the strictness contract. A
// write-ahead log that guesses at a damaged payload turns corruption into
// plausible-looking state, which is worse than refusing to start: the operator
// sees a database that came up fine and is quietly wrong.
func TestDecodeBatchRejectsMalformedPayloads(t *testing.T) {
	valid := wal.Batch{
		{Kind: wal.OpPut, Key: []byte("alpha"), Value: []byte("one")},
		{Kind: wal.OpDelete, Key: []byte("beta")},
	}.AppendTo(nil)

	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty payload", []byte{}},
		{"count only", []byte{0x02}},
		{"zero operation count", []byte{0x00}},
		{"count larger than the payload", append([]byte{0x7f}, 0x01, 0x01, 'k', 0x01, 'v')},
		{"truncated mid-key", valid[:len(valid)-12]},
		{"truncated mid-value", valid[:len(valid)-2]},
		{"one trailing byte", append(append([]byte(nil), valid...), 0x00)},
		{"trailing garbage", append(append([]byte(nil), valid...), 1, 2, 3, 4)},
		{"unknown operation kind", []byte{0x01, 0x7f, 0x01, 'k', 0x01, 'v'}},
		{"empty key", []byte{0x01, 0x01, 0x00, 0x01, 'v'}},
		{"key length beyond payload", []byte{0x01, 0x01, 0x40, 'k'}},
		{"value length beyond payload", []byte{0x01, 0x01, 0x01, 'k', 0x40, 'v'}},
		{"unterminated uvarint count", []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := wal.DecodeBatch(tc.payload)
			if err == nil {
				t.Fatalf("DecodeBatch accepted a malformed payload and returned %d operations", len(got))
			}
			if !errors.Is(err, wal.ErrCorrupt) {
				t.Errorf("error = %v, want errors.Is(..., ErrCorrupt)", err)
			}
		})
	}
}

func TestDecodeBatchDoesNotAllocateOnAbsurdCount(t *testing.T) {
	// A corrupt count must be rejected on the strength of the remaining byte
	// count, before it is used to size an allocation.
	payload := binary.AppendUvarint(nil, 1<<40)
	payload = append(payload, 0x01, 0x01, 'k', 0x01, 'v')

	if _, err := wal.DecodeBatch(payload); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("DecodeBatch = %v, want ErrCorrupt", err)
	}
}

func TestAppliedIndexRoundTrip(t *testing.T) {
	cases := []wal.AppliedIndex{
		{Index: 0, Term: 0},
		{Index: 1, Term: 1},
		{Index: 42, Term: 7},
		{Index: ^uint64(0), Term: ^uint64(0)},
	}
	for _, want := range cases {
		encoded := want.AppendTo(nil)
		if len(encoded) != 16 {
			t.Fatalf("encoded applied index is %d bytes, want 16", len(encoded))
		}
		got, err := wal.DecodeAppliedIndex(encoded)
		if err != nil {
			t.Fatalf("DecodeAppliedIndex: %v", err)
		}
		if got != want {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func TestDecodeAppliedIndexRejectsWrongLength(t *testing.T) {
	for _, n := range []int{0, 1, 8, 15, 17, 32} {
		if _, err := wal.DecodeAppliedIndex(make([]byte, n)); !errors.Is(err, wal.ErrCorrupt) {
			t.Errorf("DecodeAppliedIndex(%d bytes) = %v, want ErrCorrupt", n, err)
		}
	}
}
