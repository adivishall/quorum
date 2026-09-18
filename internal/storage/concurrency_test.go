package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/adivishal/dkv/internal/storage"
)

// These tests are only fully meaningful under `go test -race`. Without the race
// detector they verify the observable contract; with it, they also verify the
// absence of unsynchronised access. `make race` is the gate.

// TestConcurrentWritersDistinctKeys checks that parallel writes to disjoint
// keys all land, none are lost, and none corrupt each other.
func TestConcurrentWritersDistinctKeys(t *testing.T) {
	const (
		writers      = 8
		opsPerWriter = 500
	)
	s := newMemStore(t, storage.DefaultOptions())
	ctx := context.Background()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < opsPerWriter; i++ {
				key := []byte(fmt.Sprintf("w%02d:k%05d", w, i))
				val := []byte(fmt.Sprintf("w%02d:v%05d", w, i))
				if err := s.Put(ctx, key, val); err != nil {
					t.Errorf("Put(%s): %v", key, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	for w := 0; w < writers; w++ {
		for i := 0; i < opsPerWriter; i++ {
			key := []byte(fmt.Sprintf("w%02d:k%05d", w, i))
			want := fmt.Sprintf("w%02d:v%05d", w, i)
			got, err := s.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get(%s): %v", key, err)
			}
			if string(got) != want {
				t.Fatalf("Get(%s) = %q, want %q", key, got, want)
			}
		}
	}
}

// TestConcurrentWritersSameKey is the torn-value test. Every writer writes a
// large value made entirely of its own ID byte. A reader that ever observes a
// value containing two different bytes has seen a partially-applied write,
// which would violate the per-operation atomicity documented on storage.Store.
//
// Under the current RWMutex this cannot happen. The test exists so that a
// future sharded or lock-free rewrite cannot silently break the guarantee.
func TestConcurrentWritersSameKey(t *testing.T) {
	const (
		writers   = 8
		readers   = 4
		rounds    = 300
		valueSize = 8 << 10 // large enough that a memcpy could realistically tear
	)
	s := newMemStore(t, storage.DefaultOptions())
	ctx := context.Background()
	key := []byte("contended")

	values := make([][]byte, writers)
	for w := range values {
		values[w] = bytes.Repeat([]byte{byte('A' + w)}, valueSize)
	}
	if err := s.Put(ctx, key, values[0]); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	var (
		writerWG sync.WaitGroup
		readerWG sync.WaitGroup
		stop     atomic.Bool
		reads    atomic.Int64
	)

	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
			for i := 0; i < rounds; i++ {
				if err := s.Put(ctx, key, values[w]); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for !stop.Load() {
				got, err := s.Get(ctx, key)
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				reads.Add(1)
				if len(got) != valueSize {
					t.Errorf("Get returned %d bytes, want %d: torn value", len(got), valueSize)
					return
				}
				first := got[0]
				if first < 'A' || first >= 'A'+writers {
					t.Errorf("Get returned a value of %#x bytes, which no writer wrote", first)
					return
				}
				for i, b := range got {
					if b != first {
						t.Errorf("torn value: byte 0 = %#x but byte %d = %#x", first, i, b)
						return
					}
				}
			}
		}()
	}

	writerWG.Wait()
	stop.Store(true)
	readerWG.Wait()

	if reads.Load() == 0 {
		t.Error("readers never observed a value; the test proved nothing")
	}
}

// TestConcurrentReadersDuringWrites runs many readers against a key whose value
// is being continuously rewritten. Every read must return one of the values
// that was actually written — never a mixture, never a partial value.
func TestConcurrentReadersDuringWrites(t *testing.T) {
	const (
		readers = 16
		rounds  = 1000
	)
	s := newMemStore(t, storage.DefaultOptions())
	ctx := context.Background()
	key := []byte("hot")

	valid := make(map[string]bool, rounds)
	for i := 0; i < rounds; i++ {
		valid[string(valN(i))] = true
	}
	if err := s.Put(ctx, key, valN(0)); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	var (
		writerDone = make(chan struct{})
		wg         sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writerDone)
		for i := 0; i < rounds; i++ {
			if err := s.Put(ctx, key, valN(i)); err != nil {
				t.Errorf("Put: %v", err)
				return
			}
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-writerDone:
					return
				default:
				}
				got, err := s.Get(ctx, key)
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if !valid[string(got)] {
					t.Errorf("Get returned %q, which was never written", got)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentMixedOperations runs puts, gets, and deletes simultaneously
// over a shared key space. The checkable invariant under full concurrency is
// not "which value is present" — that is genuinely nondeterministic and
// asserting it would be asserting a race. It is: every Get either succeeds with
// a value that some Put actually wrote, or fails with exactly ErrNotFound.
// Anything else (a torn value, a foreign value, a surprising error class)
// is a real defect.
func TestConcurrentMixedOperations(t *testing.T) {
	const (
		workers  = 12
		opsEach  = 800
		keySpace = 64
	)
	s := newMemStore(t, storage.DefaultOptions())
	ctx := context.Background()

	valid := make(map[string]bool, keySpace)
	for i := 0; i < keySpace; i++ {
		valid[string(valN(i))] = true
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < opsEach; i++ {
				n := rng.Intn(keySpace)
				switch rng.Intn(3) {
				case 0:
					if err := s.Put(ctx, keyN(n), valN(n)); err != nil {
						t.Errorf("Put: %v", err)
						return
					}
				case 1:
					got, err := s.Get(ctx, keyN(n))
					switch {
					case err == nil:
						if !valid[string(got)] {
							t.Errorf("Get(%s) = %q, which no Put ever wrote", keyN(n), got)
							return
						}
					case errors.Is(err, storage.ErrNotFound):
						// Legitimate: a concurrent Delete won.
					default:
						t.Errorf("Get(%s): unexpected error class: %v", keyN(n), err)
						return
					}
				case 2:
					if err := s.Delete(ctx, keyN(n)); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
		}(int64(w) + 1)
	}
	wg.Wait()
}

// TestConcurrentGetsReturnIndependentCopies checks that the copy-on-read
// contract holds under concurrency: two goroutines reading the same key must
// not receive slices backed by the same array, or one mutating its result
// would corrupt the other's.
func TestConcurrentGetsReturnIndependentCopies(t *testing.T) {
	const goroutines = 16
	s := newMemStore(t, storage.DefaultOptions())
	ctx := context.Background()
	key := []byte("shared")
	original := bytes.Repeat([]byte("o"), 1024)

	if err := s.Put(ctx, key, original); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				got, err := s.Get(ctx, key)
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if !bytes.Equal(got, original) {
					t.Errorf("Get returned %q-prefixed value; another goroutine's mutation leaked through", got[:1])
					return
				}
				// Scribble on our copy. If Get handed out shared memory, some
				// other goroutine's comparison above will fail.
				for j := range got {
					got[j] = byte('a' + g)
				}
			}
		}(g)
	}
	wg.Wait()

	final, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if !bytes.Equal(final, original) {
		t.Fatalf("stored value was corrupted by readers: got %q..., want %q...", final[:8], original[:8])
	}
}

// TestConcurrentCloseDuringOperations closes the store while operations are in
// flight. Every in-flight operation must either succeed or fail cleanly with
// ErrClosed. It must never panic (MemStore drops its map on Close) and must
// never trip the race detector.
func TestConcurrentCloseDuringOperations(t *testing.T) {
	const workers = 8

	s, err := storage.NewMemStore(storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		if err := s.Put(ctx, keyN(i), valN(i)); err != nil {
			t.Fatalf("seed Put: %v", err)
		}
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < 500; i++ {
				var err error
				switch w % 3 {
				case 0:
					err = s.Put(ctx, keyN(i%64), valN(i%64))
				case 1:
					_, err = s.Get(ctx, keyN(i%64))
				case 2:
					err = s.Delete(ctx, keyN(i%64))
				}
				if err == nil || errors.Is(err, storage.ErrClosed) || errors.Is(err, storage.ErrNotFound) {
					continue
				}
				t.Errorf("worker %d: unexpected error class: %v", w, err)
				return
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	close(start)
	wg.Wait()

	// After the dust settles the store is definitively closed.
	if _, err := s.Get(ctx, keyN(0)); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
}

// TestConcurrentCloseIsIdempotent races several Close calls against each other.
func TestConcurrentCloseIsIdempotent(t *testing.T) {
	s, err := storage.NewMemStore(storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := s.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
}
