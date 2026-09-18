package storage_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/adivishall/distributed-kv/internal/storage"
)

// FuzzPutGetRoundTrip asserts the central storage contract over arbitrary
// inputs: either the input is rejected by a documented rule, or it round-trips
// byte-for-byte. There is no third outcome.
//
// The seed corpus runs on every `go test`; extended exploration is
// `go test ./internal/storage -run=Fuzz -fuzz=FuzzPutGetRoundTrip`.
func FuzzPutGetRoundTrip(f *testing.F) {
	f.Add([]byte("user:123"), []byte("Adi"))
	f.Add([]byte(" "), []byte(""))
	f.Add([]byte("\x00"), []byte("\x00\xff"))
	f.Add([]byte("a\nb"), []byte("multi\nline"))
	f.Add([]byte("日本語"), []byte("🔑"))
	f.Add([]byte{}, []byte("rejected: empty key"))
	f.Add([]byte{0xc3, 0x28}, []byte{0xff, 0xfe})

	opts := storage.DefaultOptions()

	f.Fuzz(func(t *testing.T, key, value []byte) {
		s, err := storage.NewMemStore(opts)
		if err != nil {
			t.Fatalf("NewMemStore: %v", err)
		}
		defer func() { _ = s.Close() }()
		ctx := context.Background()

		putErr := s.Put(ctx, key, value)

		switch {
		case len(key) == 0:
			if !errors.Is(putErr, storage.ErrKeyEmpty) {
				t.Fatalf("Put(empty key) = %v, want ErrKeyEmpty", putErr)
			}
			return
		case len(key) > opts.MaxKeySize:
			if !errors.Is(putErr, storage.ErrKeyTooLarge) {
				t.Fatalf("Put(key of %d bytes) = %v, want ErrKeyTooLarge", len(key), putErr)
			}
			return
		case len(value) > opts.MaxValueSize:
			if !errors.Is(putErr, storage.ErrValueTooLarge) {
				t.Fatalf("Put(value of %d bytes) = %v, want ErrValueTooLarge", len(value), putErr)
			}
			return
		}

		if putErr != nil {
			t.Fatalf("Put(%s) = %v, want nil: the input violates no documented rule", storage.SafeKey(key), putErr)
		}

		got, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s) after a successful Put: %v", storage.SafeKey(key), err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("round trip of %s: got %q, want %q", storage.SafeKey(key), got, value)
		}

		// And the mirror: delete makes it absent, idempotently.
		if err := s.Delete(ctx, key); err != nil {
			t.Fatalf("Delete(%s): %v", storage.SafeKey(key), err)
		}
		if err := s.Delete(ctx, key); err != nil {
			t.Fatalf("second Delete(%s) = %v, want nil", storage.SafeKey(key), err)
		}
		if _, err := s.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("Get(%s) after Delete = %v, want ErrNotFound", storage.SafeKey(key), err)
		}
	})
}
