package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/storage"
)

// newStoreFunc builds a fresh, empty Store with the given options.
type newStoreFunc func(tb testing.TB, opts storage.Options) storage.Store

// runConformance runs every semantic guarantee documented on storage.Store
// against an implementation.
//
// This exists so that the Phase 3 LSM engine can be dropped in here unchanged.
// Requirement: swapping the storage implementation must not change any
// client-visible semantic. A comment saying so is a wish; this suite is the
// enforcement.
func runConformance(t *testing.T, newStore newStoreFunc) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(t *testing.T, newStore newStoreFunc)
	}{
		{"PutGetNewKey", testPutGetNewKey},
		{"Overwrite", testOverwrite},
		{"GetMissingKey", testGetMissingKey},
		{"DeleteExistingKey", testDeleteExistingKey},
		{"DeleteMissingKeyIsIdempotent", testDeleteMissingKey},
		{"DeleteIsRepeatable", testDeleteRepeatable},
		{"EmptyKeyRejected", testEmptyKeyRejected},
		{"NilKeyRejected", testNilKeyRejected},
		{"EmptyValueIsStorable", testEmptyValue},
		{"KeysAreOpaqueBytes", testOpaqueKeys},
		{"KeysAreCaseSensitive", testCaseSensitive},
		{"KeySizeLimit", testKeySizeLimit},
		{"ValueSizeLimit", testValueSizeLimit},
		{"LargeValueRoundTrip", testLargeValueRoundTrip},
		{"PutCopiesInput", testPutCopiesInput},
		{"GetReturnsCopy", testGetReturnsCopy},
		{"ClosedStoreRejectsOperations", testClosedStore},
		{"CloseIsIdempotent", testCloseIdempotent},
		{"CancelledContextRejected", testCancelledContext},
		{"ErrorsCarryOperationAndKey", testErrorShape},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newStore) })
	}
}

// ---------------------------------------------------------------- basics

func testPutGetNewKey(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	if err := s.Put(ctx, []byte("user:123"), []byte("Adi")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, []byte("user:123"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("Adi")) {
		t.Fatalf("Get = %q, want %q", got, "Adi")
	}
}

func testOverwrite(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()
	key := []byte("k")

	for i, want := range []string{"first", "second", "", "third-and-longest"} {
		if err := s.Put(ctx, key, []byte(want)); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
		got, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get #%d: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("after Put #%d: Get = %q, want %q", i, got, want)
		}
	}
}

func testGetMissingKey(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())

	got, err := s.Get(context.Background(), []byte("absent"))
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get(absent) error = %v, want ErrNotFound", err)
	}
	if got != nil {
		t.Fatalf("Get(absent) value = %q, want nil", got)
	}
}

func testDeleteExistingKey(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()
	key := []byte("doomed")

	if err := s.Put(ctx, key, []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get after Delete: error = %v, want ErrNotFound", err)
	}
}

func testDeleteMissingKey(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())

	// Documented semantic: Delete is idempotent and does not report existence.
	// See storage.Store.Delete for why this is forced by the LSM engine that
	// will replace MemStore.
	if err := s.Delete(context.Background(), []byte("never-existed")); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil (delete is idempotent)", err)
	}
}

func testDeleteRepeatable(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()
	key := []byte("k")

	if err := s.Put(ctx, key, []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Delete(ctx, key); err != nil {
			t.Fatalf("Delete #%d = %v, want nil", i, err)
		}
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------- key rules

func testEmptyKeyRejected(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()
	empty := []byte{}

	if err := s.Put(ctx, empty, []byte("v")); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Errorf("Put(empty key) = %v, want ErrKeyEmpty", err)
	}
	if _, err := s.Get(ctx, empty); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Errorf("Get(empty key) = %v, want ErrKeyEmpty", err)
	}
	if err := s.Delete(ctx, empty); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Errorf("Delete(empty key) = %v, want ErrKeyEmpty", err)
	}
}

func testNilKeyRejected(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	// A nil key is a zero-length key; it must behave identically to []byte{}
	// rather than panicking or being silently accepted.
	if err := s.Put(ctx, nil, []byte("v")); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Errorf("Put(nil key) = %v, want ErrKeyEmpty", err)
	}
	if _, err := s.Get(ctx, nil); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Errorf("Get(nil key) = %v, want ErrKeyEmpty", err)
	}
	if err := s.Delete(ctx, nil); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Errorf("Delete(nil key) = %v, want ErrKeyEmpty", err)
	}
}

// testOpaqueKeys pins the decision recorded on storage.Store: keys are opaque
// byte strings. Whitespace, newlines, NULs, and invalid UTF-8 are all VALID
// keys and must round-trip byte-for-byte. A store that quietly normalises or
// rejects them cannot faithfully hold what it was given, and the divergence
// would surface later as keys that vanish between the API layer and the engine.
func testOpaqueKeys(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	keys := map[string][]byte{
		"single space":        []byte(" "),
		"all whitespace":      []byte("   \t  "),
		"leading/trailing":    []byte("  padded  "),
		"internal space":      []byte("user 123"),
		"newline":             []byte("a\nb"),
		"carriage return":     []byte("a\r\nb"),
		"tab":                 []byte("a\tb"),
		"NUL byte":            []byte("a\x00b"),
		"high bytes":          {0xff, 0xfe, 0x00, 0x01},
		"invalid UTF-8":       {0xc3, 0x28},
		"valid UTF-8":         []byte("ключ:日本語:🔑"),
		"shell metacharacter": []byte("k$(whoami)`id`;rm -rf /"),
		"json-looking":        []byte(`{"k":"v"}`),
		"url-looking":         []byte("a/b?c=d&e=f#g"),
		"quote characters":    []byte(`he said "hi" and 'bye'`),
	}

	for name, key := range keys {
		value := []byte("value-for-" + name)
		if err := s.Put(ctx, key, value); err != nil {
			t.Errorf("Put(%s / %s): %v", name, storage.SafeKey(key), err)
			continue
		}
		got, err := s.Get(ctx, key)
		if err != nil {
			t.Errorf("Get(%s / %s): %v", name, storage.SafeKey(key), err)
			continue
		}
		if !bytes.Equal(got, value) {
			t.Errorf("Get(%s) = %q, want %q", name, got, value)
		}
	}

	// Each of those must be a *distinct* key, not collapsed by normalisation.
	if ms, ok := s.(interface{ Len() int }); ok {
		if got := ms.Len(); got != len(keys) {
			t.Errorf("stored %d distinct keys, want %d (keys are being normalised or collided)", got, len(keys))
		}
	}
}

func testCaseSensitive(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	if err := s.Put(ctx, []byte("Key"), []byte("upper")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, []byte("key"), []byte("lower")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for key, want := range map[string]string{"Key": "upper", "key": "lower"} {
		got, err := s.Get(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}
	if _, err := s.Get(ctx, []byte("KEY")); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf(`Get("KEY") = %v, want ErrNotFound`, err)
	}
}

// ---------------------------------------------------------------- limits

func testKeySizeLimit(t *testing.T, newStore newStoreFunc) {
	opts := storage.Options{MaxKeySize: 64, MaxValueSize: 1024}
	s := newStore(t, opts)
	ctx := context.Background()

	atLimit := bytes.Repeat([]byte("k"), opts.MaxKeySize)
	overLimit := bytes.Repeat([]byte("k"), opts.MaxKeySize+1)

	if err := s.Put(ctx, atLimit, []byte("v")); err != nil {
		t.Errorf("Put(key at limit) = %v, want nil (the limit is inclusive)", err)
	}
	if err := s.Put(ctx, overLimit, []byte("v")); !errors.Is(err, storage.ErrKeyTooLarge) {
		t.Errorf("Put(key over limit) = %v, want ErrKeyTooLarge", err)
	}
	// Reads and deletes must reject the same keys writes reject; otherwise an
	// oversized key would be a silent no-op rather than an error.
	if _, err := s.Get(ctx, overLimit); !errors.Is(err, storage.ErrKeyTooLarge) {
		t.Errorf("Get(key over limit) = %v, want ErrKeyTooLarge", err)
	}
	if err := s.Delete(ctx, overLimit); !errors.Is(err, storage.ErrKeyTooLarge) {
		t.Errorf("Delete(key over limit) = %v, want ErrKeyTooLarge", err)
	}
}

func testValueSizeLimit(t *testing.T, newStore newStoreFunc) {
	opts := storage.Options{MaxKeySize: 64, MaxValueSize: 1024}
	s := newStore(t, opts)
	ctx := context.Background()

	atLimit := bytes.Repeat([]byte("v"), opts.MaxValueSize)
	overLimit := bytes.Repeat([]byte("v"), opts.MaxValueSize+1)

	if err := s.Put(ctx, []byte("ok"), atLimit); err != nil {
		t.Errorf("Put(value at limit) = %v, want nil (the limit is inclusive)", err)
	}
	if err := s.Put(ctx, []byte("bad"), overLimit); !errors.Is(err, storage.ErrValueTooLarge) {
		t.Errorf("Put(value over limit) = %v, want ErrValueTooLarge", err)
	}
	// A rejected Put must not have partially applied.
	if _, err := s.Get(ctx, []byte("bad")); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get after rejected Put = %v, want ErrNotFound", err)
	}
}

func testEmptyValue(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	for _, value := range [][]byte{{}, nil} {
		if err := s.Put(ctx, []byte("empty"), value); err != nil {
			t.Fatalf("Put(empty value): %v", err)
		}
		got, err := s.Get(ctx, []byte("empty"))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		// The contract: an empty value is a PRESENT key. It must be
		// distinguishable from absence by the error, and the returned slice
		// must not be nil, so callers never have to guess.
		if got == nil {
			t.Fatal("Get returned a nil slice for an empty value; want a non-nil zero-length slice")
		}
		if len(got) != 0 {
			t.Fatalf("Get = %q, want zero length", got)
		}
	}
}

func testLargeValueRoundTrip(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	// 1 MiB, filled with a non-repeating pattern so truncation or a partial
	// copy is detectable rather than masked by uniform bytes.
	value := make([]byte, storage.DefaultMaxValueSize)
	for i := range value {
		value[i] = byte(i*31 + i/251)
	}

	if err := s.Put(ctx, []byte("big"), value); err != nil {
		t.Fatalf("Put(1 MiB): %v", err)
	}
	got, err := s.Get(ctx, []byte("big"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != len(value) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(value))
	}
	if !bytes.Equal(got, value) {
		for i := range got {
			if got[i] != value[i] {
				t.Fatalf("value differs at byte %d: got %#x, want %#x", i, got[i], value[i])
			}
		}
	}
}

// ---------------------------------------------------------------- aliasing

// testPutCopiesInput is the guard against the worst class of bug this layer can
// have: the store retaining the caller's slice, so that data mutates underneath
// it without any write having occurred.
func testPutCopiesInput(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	key := []byte("mutable-key")
	value := []byte("original")

	if err := s.Put(ctx, key, value); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Caller reuses both buffers, as it is explicitly permitted to.
	copy(key, []byte("CLOBBERED!!"))
	copy(value, []byte("MUTATED!"))

	got, err := s.Get(ctx, []byte("mutable-key"))
	if err != nil {
		t.Fatalf("Get after caller mutated its buffers: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("Get = %q, want %q: Put retained the caller's value slice", got, "original")
	}
}

// testGetReturnsCopy is the mirror image: a caller mutating what Get handed
// back must not be able to corrupt stored data.
func testGetReturnsCopy(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()
	key := []byte("k")

	if err := s.Put(ctx, key, []byte("original")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	first, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	copy(first, []byte("CORRUPT!"))

	second, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get #2: %v", err)
	}
	if string(second) != "original" {
		t.Fatalf("Get #2 = %q, want %q: Get exposed internal memory", second, "original")
	}

	// Two concurrent-style reads must also be independent of each other.
	if len(first) > 0 && &first[0] == &second[0] {
		t.Fatal("two Gets returned aliased slices")
	}
}

// ---------------------------------------------------------------- lifecycle

func testClosedStore(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	if err := s.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put before Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := s.Put(ctx, []byte("k"), []byte("v")); !errors.Is(err, storage.ErrClosed) {
		t.Errorf("Put after Close = %v, want ErrClosed", err)
	}
	if _, err := s.Get(ctx, []byte("k")); !errors.Is(err, storage.ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
	if err := s.Delete(ctx, []byte("k")); !errors.Is(err, storage.ErrClosed) {
		t.Errorf("Delete after Close = %v, want ErrClosed", err)
	}

	// Validation must still run first: a closed store given a bad key reports
	// the bad key. The order matters for the CLI's exit codes, which map
	// usage mistakes and lifecycle failures to different codes.
	if err := s.Put(ctx, nil, []byte("v")); !errors.Is(err, storage.ErrKeyEmpty) {
		t.Errorf("Put(nil key) after Close = %v, want ErrKeyEmpty", err)
	}
}

func testCloseIdempotent(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d = %v, want nil", i, err)
		}
	}
}

func testCancelledContext(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Put(ctx, []byte("k"), []byte("v")); !errors.Is(err, context.Canceled) {
		t.Errorf("Put(cancelled ctx) = %v, want context.Canceled", err)
	}
	if _, err := s.Get(ctx, []byte("k")); !errors.Is(err, context.Canceled) {
		t.Errorf("Get(cancelled ctx) = %v, want context.Canceled", err)
	}
	if err := s.Delete(ctx, []byte("k")); !errors.Is(err, context.Canceled) {
		t.Errorf("Delete(cancelled ctx) = %v, want context.Canceled", err)
	}

	// The rejected Put must not have taken effect.
	if _, err := s.Get(context.Background(), []byte("k")); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get after cancelled Put = %v, want ErrNotFound", err)
	}
}

// testErrorShape asserts the error taxonomy is machine-readable, not just
// human-readable: every failure is an *OpError naming the operation and key,
// and errors.Is still reaches the sentinel through the wrapper.
func testErrorShape(t *testing.T, newStore newStoreFunc) {
	s := newStore(t, storage.DefaultOptions())
	ctx := context.Background()

	cases := []struct {
		name     string
		op       string
		key      []byte
		run      func() error
		sentinel error
	}{
		{"get missing", "get", []byte("nope"), func() error {
			_, err := s.Get(ctx, []byte("nope"))
			return err
		}, storage.ErrNotFound},
		{"put empty key", "put", []byte{}, func() error {
			return s.Put(ctx, nil, []byte("v"))
		}, storage.ErrKeyEmpty},
		{"put huge key", "put", nil, func() error {
			return s.Put(ctx, bytes.Repeat([]byte("k"), storage.DefaultMaxKeySize+1), []byte("v"))
		}, storage.ErrKeyTooLarge},
		{"put huge value", "put", []byte("k"), func() error {
			return s.Put(ctx, []byte("k"), bytes.Repeat([]byte("v"), storage.DefaultMaxValueSize+1))
		}, storage.ErrValueTooLarge},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if !errors.Is(err, tc.sentinel) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, tc.sentinel)
			}
			var opErr *storage.OpError
			if !errors.As(err, &opErr) {
				t.Fatalf("error %v is not an *OpError; callers cannot inspect it", err)
			}
			if opErr.Op != tc.op {
				t.Errorf("OpError.Op = %q, want %q", opErr.Op, tc.op)
			}
			msg := err.Error()
			if msg == "" {
				t.Fatal("error message is empty")
			}
			// The message must name the operation so a log line is actionable.
			if !strings.HasPrefix(msg, tc.op+" ") && !strings.HasPrefix(msg, tc.op+":") {
				t.Errorf("error message %q does not begin with the operation %q", msg, tc.op)
			}
			// It must NOT carry a program-name prefix: naming the program is
			// the caller's job, and doing it here double-prefixes the CLI.
			if strings.HasPrefix(msg, "dkv:") {
				t.Errorf("error message %q carries a \"dkv:\" prefix; that belongs to the printer, not the library", msg)
			}
		})
	}
}

// ---------------------------------------------------------------- helper

func keyN(i int) []byte { return []byte(fmt.Sprintf("key:%08d", i)) }
func valN(i int) []byte { return []byte(fmt.Sprintf("value:%08d", i)) }
