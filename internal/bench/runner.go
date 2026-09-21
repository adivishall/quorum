package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/storage"
)

// PhaseResult is the measured outcome of one workload phase: how many
// operations ran, how many key+value bytes they moved (for MB/s), the wall-clock
// time of the measured interval, and the per-operation latencies. Verify is a
// count of operations whose result was checked against what it should have been,
// so a benchmark can prove it exercised the real store rather than a shortcut.
type PhaseResult struct {
	Ops     int64
	Bytes   int64
	Elapsed time.Duration
	Lat     *Latencies
	Verify  int64
}

// Result turns the phase into a reportable Result with throughput computed from
// the measured interval only.
func (p PhaseResult) Result(name string) Result {
	r := NewResult(name, p.Ops, p.Bytes, p.Elapsed)
	if p.Lat != nil {
		r.Latency = p.Lat.Stats()
	}
	return r
}

// PutSequential writes indices [from,to) in order, one at a time, timing each.
// It is the sequential-write path: a single writer, keys ascending, which is the
// cheapest ordering for an LSM and the honest baseline for "how fast can one
// goroutine store keys". Values encode their index.
func PutSequential(ctx context.Context, s storage.Store, ks Keyspace, valueSize, from, to int) (PhaseResult, error) {
	lat := NewLatencies(to - from)
	val := make([]byte, valueSize)
	var key []byte
	var bytesMoved int64
	start := time.Now()
	for i := from; i < to; i++ {
		key = ks.AppendKey(key[:0], i)
		FillValue(val, i)
		t0 := time.Now()
		if err := s.Put(ctx, key, val); err != nil {
			return PhaseResult{}, fmt.Errorf("put %d: %w", i, err)
		}
		lat.Record(time.Since(t0))
		bytesMoved += int64(len(key) + valueSize)
	}
	return PhaseResult{Ops: int64(to - from), Bytes: bytesMoved, Elapsed: time.Since(start), Lat: lat}, nil
}

// PutConcurrent writes n distinct keys across the given number of workers. Keys
// are partitioned so no two workers write the same key, which keeps the write a
// pure insertion workload (no overwrite contention) and makes the post-condition
// checkable: exactly n keys must exist afterwards. Each worker times its own
// operations into its own recorder; the recorders merge at the end.
func PutConcurrent(ctx context.Context, s storage.Store, ks Keyspace, valueSize, n, workers int) (PhaseResult, error) {
	if workers < 1 {
		workers = 1
	}
	perWorker := make([]*Latencies, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			lat := NewLatencies(n/workers + 1)
			val := make([]byte, valueSize)
			var key []byte
			for i := w; i < n; i += workers {
				key = ks.AppendKey(key[:0], i)
				FillValue(val, i)
				t0 := time.Now()
				if err := s.Put(ctx, key, val); err != nil {
					errs[w] = fmt.Errorf("put %d: %w", i, err)
					break
				}
				lat.Record(time.Since(t0))
			}
			perWorker[w] = lat
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	for _, e := range errs {
		if e != nil {
			return PhaseResult{}, e
		}
	}
	merged := NewLatencies(n)
	for _, l := range perWorker {
		merged.Merge(l)
	}
	return PhaseResult{Ops: int64(n), Bytes: int64(n) * int64(ks.KeyBytes()+valueSize), Elapsed: elapsed, Lat: merged}, nil
}

// GetRandom performs n random point reads over a populated keyspace of live
// size. When missing is false it reads indices in [0,live) — every read must
// hit — and verifies a sampled fraction against the value the index encodes.
// When missing is true it reads indices in [live,2*live) — keys never written —
// and every read must miss with ErrNotFound. Either way the benchmark asserts
// the store did what the workload claims, so a silently empty database cannot
// masquerade as a fast one.
func GetRandom(ctx context.Context, s storage.Store, ks Keyspace, live, n, workers int, seed int64, missing bool) (PhaseResult, error) {
	if workers < 1 {
		workers = 1
	}
	valueBytesSeen := make([]int64, workers)
	perWorker := make([]*Latencies, workers)
	verified := make([]int64, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(w)))
			lat := NewLatencies(n/workers + 1)
			count := n / workers
			if w < n%workers {
				count++
			}
			var key []byte
			for j := 0; j < count; j++ {
				idx := r.Intn(live)
				if missing {
					idx += live
				}
				key = ks.AppendKey(key[:0], idx)
				t0 := time.Now()
				v, err := s.Get(ctx, key)
				lat.Record(time.Since(t0))
				if missing {
					if !errors.Is(err, storage.ErrNotFound) {
						errs[w] = fmt.Errorf("get %d: expected ErrNotFound, got value=%d err=%v", idx, len(v), err)
						break
					}
					verified[w]++
					continue
				}
				if err != nil {
					errs[w] = fmt.Errorf("get %d: %w", idx, err)
					break
				}
				valueBytesSeen[w] += int64(len(v))
				// Verify roughly one read in 16 to keep the check off the hot
				// path while still proving the values are the right ones.
				if j%16 == 0 {
					if got, ok := ValueIndex(v); !ok || got != uint64(idx) {
						errs[w] = fmt.Errorf("get %d: value encodes index %d", idx, got)
						break
					}
					verified[w]++
				}
			}
			perWorker[w] = lat
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	for _, e := range errs {
		if e != nil {
			return PhaseResult{}, e
		}
	}
	merged := NewLatencies(n)
	var vbytes, verifyTotal int64
	for w := 0; w < workers; w++ {
		merged.Merge(perWorker[w])
		vbytes += valueBytesSeen[w]
		verifyTotal += verified[w]
	}
	bytes := vbytes + int64(n)*int64(ks.KeyBytes())
	return PhaseResult{Ops: int64(n), Bytes: bytes, Elapsed: elapsed, Lat: merged, Verify: verifyTotal}, nil
}

// DeleteRandom deletes n distinct keys chosen from [0,live) without replacement
// (a shuffled prefix), timing each. Deleting distinct keys means n real
// tombstones are created; the caller can then verify the keys are absent.
func DeleteRandom(ctx context.Context, s storage.Store, ks Keyspace, live, n int, seed int64) (PhaseResult, error) {
	if n > live {
		n = live
	}
	perm := rand.New(rand.NewSource(seed)).Perm(live)[:n]
	lat := NewLatencies(n)
	var key []byte
	start := time.Now()
	for _, idx := range perm {
		key = ks.AppendKey(key[:0], idx)
		t0 := time.Now()
		if err := s.Delete(ctx, key); err != nil {
			return PhaseResult{}, fmt.Errorf("delete %d: %w", idx, err)
		}
		lat.Record(time.Since(t0))
	}
	elapsed := time.Since(start)
	// Verify the deleted keys are now absent.
	var verified int64
	for _, idx := range perm {
		key = ks.AppendKey(key[:0], idx)
		if _, err := s.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			return PhaseResult{}, fmt.Errorf("delete %d: key still present after delete (err=%v)", idx, err)
		}
		verified++
	}
	return PhaseResult{Ops: int64(n), Bytes: int64(n) * int64(ks.KeyBytes()), Elapsed: elapsed, Lat: lat, Verify: verified}, nil
}

// Mixed runs n operations chosen by the mix, over a populated keyspace of live
// size, across workers. Reads target live keys and must hit; writes overwrite
// existing keys with a fresh value; deletes target live keys. It models steady
// state on a warm dataset, not a cold build. Latencies cover every operation
// regardless of kind, which is the tail a client actually sees.
func Mixed(ctx context.Context, s storage.Store, ks Keyspace, mix Mix, valueSize, live, n, workers int, seed int64) (PhaseResult, error) {
	if workers < 1 {
		workers = 1
	}
	perWorker := make([]*Latencies, workers)
	bytesMoved := make([]int64, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(w)))
			count := n / workers
			if w < n%workers {
				count++
			}
			lat := NewLatencies(count)
			val := make([]byte, valueSize)
			var key []byte
			var moved int64
			for j := 0; j < count; j++ {
				idx := r.Intn(live)
				key = ks.AppendKey(key[:0], idx)
				kb := int64(len(key))
				switch mix.Pick(r) {
				case OpWrite:
					FillValue(val, idx)
					t0 := time.Now()
					if err := s.Put(ctx, key, val); err != nil {
						errs[w] = fmt.Errorf("mixed put %d: %w", idx, err)
					}
					lat.Record(time.Since(t0))
					// A write moves the key and the value.
					moved += kb + int64(valueSize)
				case OpDelete:
					t0 := time.Now()
					err := s.Delete(ctx, key)
					lat.Record(time.Since(t0))
					if err != nil {
						errs[w] = fmt.Errorf("mixed delete %d: %w", idx, err)
					}
					// A delete moves only the key (a tombstone has no value).
					moved += kb
				default: // OpRead
					t0 := time.Now()
					v, err := s.Get(ctx, key)
					lat.Record(time.Since(t0))
					if err != nil && !errors.Is(err, storage.ErrNotFound) {
						errs[w] = fmt.Errorf("mixed get %d: %w", idx, err)
					}
					// A read moves the key in and whatever value came back
					// (zero on a miss). Per-operation accounting, not a flat
					// key+value for every op — GET, PUT and DELETE differ.
					moved += kb + int64(len(v))
				}
				if errs[w] != nil {
					break
				}
			}
			perWorker[w] = lat
			bytesMoved[w] = moved
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	for _, e := range errs {
		if e != nil {
			return PhaseResult{}, e
		}
	}
	merged := NewLatencies(n)
	var totalBytes int64
	for w := 0; w < workers; w++ {
		merged.Merge(perWorker[w])
		totalBytes += bytesMoved[w]
	}
	return PhaseResult{Ops: int64(n), Bytes: totalBytes, Elapsed: elapsed, Lat: merged}, nil
}
