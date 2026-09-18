package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adivishall/quorum/internal/storage"
)

// newMemStore adapts MemStore to the conformance suite. When the Phase 3 LSM
// engine lands it gets a sibling of this function and inherits every test.
func newMemStore(tb testing.TB, opts storage.Options) storage.Store {
	tb.Helper()
	s, err := storage.NewMemStore(opts)
	if err != nil {
		tb.Fatalf("NewMemStore: %v", err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMemStoreConformance(t *testing.T) {
	runConformance(t, newMemStore)
}

func TestMemStoreConcurrency(t *testing.T) {
	runConcurrency(t, newMemStore)
}

func TestNewMemStoreRejectsInvalidOptions(t *testing.T) {
	cases := []struct {
		name string
		opts storage.Options
	}{
		{"zero max key size", storage.Options{MaxKeySize: 0, MaxValueSize: 1024}},
		{"negative max key size", storage.Options{MaxKeySize: -1, MaxValueSize: 1024}},
		{"negative max value size", storage.Options{MaxKeySize: 64, MaxValueSize: -1}},
		{"zero value options", storage.Options{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := storage.NewMemStore(tc.opts)
			if !errors.Is(err, storage.ErrInvalidOptions) {
				t.Fatalf("NewMemStore(%+v) error = %v, want ErrInvalidOptions", tc.opts, err)
			}
			if s != nil {
				t.Fatal("NewMemStore returned a non-nil store alongside an error")
			}
		})
	}
}

func TestNewMemStoreAcceptsZeroMaxValueSize(t *testing.T) {
	// MaxValueSize == 0 is legal (only empty values fit) and must not be
	// confused with "unset". This pins that the validation rule is
	// MaxValueSize >= 0, not > 0.
	s, err := storage.NewMemStore(storage.Options{MaxKeySize: 8, MaxValueSize: 0})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	if err := s.Put(ctx, []byte("k"), nil); err != nil {
		t.Errorf("Put(empty value) = %v, want nil", err)
	}
	if err := s.Put(ctx, []byte("k"), []byte("x")); !errors.Is(err, storage.ErrValueTooLarge) {
		t.Errorf("Put(1-byte value) = %v, want ErrValueTooLarge", err)
	}
}

func TestMemStoreLen(t *testing.T) {
	s, err := storage.NewMemStore(storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	if got := s.Len(); got != 0 {
		t.Fatalf("Len on empty store = %d, want 0", got)
	}
	for i := 0; i < 10; i++ {
		if err := s.Put(ctx, keyN(i), valN(i)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if got := s.Len(); got != 10 {
		t.Fatalf("Len = %d, want 10", got)
	}
	// Overwriting must not change the count.
	if err := s.Put(ctx, keyN(0), []byte("replacement")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := s.Len(); got != 10 {
		t.Fatalf("Len after overwrite = %d, want 10", got)
	}
	if err := s.Delete(ctx, keyN(0)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := s.Len(); got != 9 {
		t.Fatalf("Len after delete = %d, want 9", got)
	}
	// Deleting an absent key must not change the count either.
	if err := s.Delete(ctx, []byte("absent")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := s.Len(); got != 9 {
		t.Fatalf("Len after deleting absent key = %d, want 9", got)
	}
}

func TestSafeKey(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
		want string
	}{
		{"simple", []byte("user:1"), `"user:1"`},
		{"empty", []byte{}, `""`},
		{"newline escaped", []byte("a\nb"), `"a\nb"`},
		{"NUL escaped", []byte("a\x00b"), `"a\x00b"`},
		{"invalid utf8 escaped", []byte{0xc3, 0x28}, `"\xc3("`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := storage.SafeKey(tc.key); got != tc.want {
				t.Errorf("SafeKey(%v) = %s, want %s", tc.key, got, tc.want)
			}
		})
	}

	// Long keys must be truncated: a 4 KiB key must never be pasted whole into
	// a log line or an error message.
	long := make([]byte, storage.DefaultMaxKeySize)
	for i := range long {
		long[i] = 'x'
	}
	got := storage.SafeKey(long)
	if len(got) > 128 {
		t.Errorf("SafeKey of a %d-byte key produced %d characters; want it truncated", len(long), len(got))
	}
	if !contains(got, "4096 bytes") {
		t.Errorf("SafeKey(long) = %s, want it to report the true length", got)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
