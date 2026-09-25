package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/adivishall/quorum/internal/lincheck"
	"github.com/adivishall/quorum/internal/storage"
)

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Key: []byte("k"), Value: []byte("v")},
		{Op: OpPut, Key: []byte("k"), Value: []byte{}},
		{Op: OpPut, Key: []byte("a b\n\x00"), Value: bytes.Repeat([]byte{0xff}, 1000)},
		{Op: OpDelete, Key: []byte("k")},
		{Op: OpPut, Key: bytes.Repeat([]byte("k"), MaxKeyLen), Value: bytes.Repeat([]byte("v"), MaxValueLen)},
	}
	for _, c := range cases {
		got, err := Decode(c.Encode())
		if err != nil {
			t.Fatalf("%v: %v", c.Op, err)
		}
		if got.Op != c.Op || !bytes.Equal(got.Key, c.Key) {
			t.Fatalf("round trip changed op/key: %+v -> %+v", c, got)
		}
		if c.Op == OpPut && (got.Value == nil || !bytes.Equal(got.Value, c.Value)) {
			t.Fatalf("round trip changed value: %q -> %q (nil=%v)", c.Value, got.Value, got.Value == nil)
		}
		if c.Op == OpDelete && got.Value != nil {
			t.Fatalf("delete decoded with a value: %q", got.Value)
		}
		// Copies, never aliases.
		enc := c.Encode()
		got, _ = Decode(enc)
		for i := range enc {
			enc[i] = 0
		}
		if !bytes.Equal(got.Key, c.Key) {
			t.Fatal("decoded key aliases the input buffer")
		}
	}
}

func TestDecodeRejectsMalformedCommands(t *testing.T) {
	bad := map[string][]byte{
		"empty":            {},
		"unknown op":       {9, 1, 'k'},
		"empty key":        {byte(OpPut), 0, 0},
		"key too long":     append([]byte{byte(OpPut), 0x81, 0x80, 0x01}, make([]byte, 100)...), // declares 16385
		"truncated key":    {byte(OpPut), 5, 'k'},
		"truncated value":  {byte(OpPut), 1, 'k', 3, 'v'},
		"trailing bytes":   append(Command{Op: OpDelete, Key: []byte("k")}.Encode(), 0),
		"delete with data": append(Command{Op: OpDelete, Key: []byte("k")}.Encode(), 1, 'v'),
		"bad varint":       {byte(OpPut), 0x80},
	}
	for name, b := range bad {
		if _, err := Decode(b); !errors.Is(err, ErrMalformedCommand) {
			t.Fatalf("%s: err = %v, want ErrMalformedCommand", name, err)
		}
		if IsCommand(b) {
			t.Fatalf("%s: IsCommand true", name)
		}
	}
	if !IsCommand(Command{Op: OpPut, Key: []byte("k")}.Encode()) {
		t.Fatal("a valid command is not recognised")
	}
}

func FuzzDecodeIsTotal(f *testing.F) {
	f.Add(Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")}.Encode())
	f.Add(Command{Op: OpDelete, Key: []byte("k")}.Encode())
	f.Add([]byte{1, 200})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := Decode(b) // must never panic
		if err != nil {
			return
		}
		if !bytes.Equal(c.Encode(), b) {
			t.Fatalf("decode/encode not canonical: %x -> %x", b, c.Encode())
		}
	})
}

// TestStoreMatchesTheStorageContract runs the same random operation sequence
// through the Store (via encoded commands) and through the Phase 1 MemStore, and
// requires every Get to agree — the Store IS the Phase 1 semantics, not a new
// interpretation of them.
func TestStoreMatchesTheStorageContract(t *testing.T) {
	ref, err := storage.NewMemStore(storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	s := NewStore()
	rng := rand.New(rand.NewSource(5))
	ctx := context.Background()
	var index uint64
	for i := 0; i < 5000; i++ {
		key := []byte(fmt.Sprintf("k%d", rng.Intn(16)))
		switch rng.Intn(4) {
		case 0, 1:
			val := make([]byte, rng.Intn(6))
			rng.Read(val)
			if err := ref.Put(ctx, key, val); err != nil {
				t.Fatal(err)
			}
			index++
			if err := s.Apply(index, Command{Op: OpPut, Key: key, Value: val}.Encode()); err != nil {
				t.Fatal(err)
			}
		case 2:
			if err := ref.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
			index++
			if err := s.Apply(index, Command{Op: OpDelete, Key: key}.Encode()); err != nil {
				t.Fatal(err)
			}
		default:
			want, err := ref.Get(ctx, key)
			got, ok := s.Get(key)
			switch {
			case errors.Is(err, storage.ErrNotFound):
				if ok {
					t.Fatalf("step %d: store has %q=%q but the reference says not found", i, key, got)
				}
			case err != nil:
				t.Fatal(err)
			default:
				if !ok || !bytes.Equal(got, want) {
					t.Fatalf("step %d: store has %q=%q (present=%v), reference %q", i, key, got, ok, want)
				}
				if got == nil {
					t.Fatalf("step %d: a present empty value must be a non-nil slice", i)
				}
			}
		}
	}
}

// TestStoreMatchesTheReferenceModel: the Store agrees with lincheck's sequential
// register model op for op, including the no-op and the copy semantics.
func TestStoreMatchesTheReferenceModel(t *testing.T) {
	s := NewStore()
	if err := s.Apply(1, nil); err != nil || s.Applied() != 1 {
		t.Fatalf("no-op apply: err=%v applied=%d", err, s.Applied())
	}
	model := lincheck.Initial
	ops := []lincheck.Op{
		{Kind: lincheck.Put, Key: "k", Value: []byte("a"), Outcome: lincheck.OK},
		{Kind: lincheck.Get, Key: "k"},
		{Kind: lincheck.Put, Key: "k", Value: []byte{}, Outcome: lincheck.OK},
		{Kind: lincheck.Get, Key: "k"},
		{Kind: lincheck.Delete, Key: "k", Outcome: lincheck.OK},
		{Kind: lincheck.Delete, Key: "k", Outcome: lincheck.OK},
		{Kind: lincheck.Get, Key: "k"},
	}
	index := uint64(1)
	for _, op := range ops {
		switch op.Kind {
		case lincheck.Put:
			index++
			if err := s.Apply(index, Command{Op: OpPut, Key: []byte(op.Key), Value: op.Value}.Encode()); err != nil {
				t.Fatal(err)
			}
		case lincheck.Delete:
			index++
			if err := s.Apply(index, Command{Op: OpDelete, Key: []byte(op.Key)}.Encode()); err != nil {
				t.Fatal(err)
			}
		case lincheck.Get:
			v, ok := s.Get([]byte(op.Key))
			if ok {
				op.Outcome, op.Output = lincheck.OK, v
			} else {
				op.Outcome = lincheck.NotFound
			}
		}
		var ok bool
		if model, ok = lincheck.Step(model, op); !ok {
			t.Fatalf("store and model disagree at %s: model state %+v", op.Kind, model)
		}
	}
	// A malformed command is refused and changes nothing.
	before := s.Snapshot()
	if err := s.Apply(index+1, []byte{7}); !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("malformed apply: %v", err)
	}
	if after := s.Snapshot(); len(after) != len(before) || s.Applied() != index {
		t.Fatal("a refused command changed the store")
	}
	// Get returns copies.
	_ = s.Apply(index+1, Command{Op: OpPut, Key: []byte("c"), Value: []byte("orig")}.Encode())
	v, _ := s.Get([]byte("c"))
	v[0] = 'X'
	if w, _ := s.Get([]byte("c")); string(w) != "orig" {
		t.Fatal("Get aliases stored memory")
	}
}
