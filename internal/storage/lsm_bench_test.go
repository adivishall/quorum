package storage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/ikey"
	"github.com/adivishall/quorum/internal/storage/memtable"
	"github.com/adivishall/quorum/internal/storage/sstable"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// Development measurements, not benchmarks in the sense the word will mean in
// Phase 5. They exist to notice an order-of-magnitude regression while the
// engine is being built, and to have a number to point at when deciding
// whether an optimisation is worth writing.
//
// Nothing here is a claim. The figures depend on the machine, the filesystem,
// the page cache and what else is running, none of which is controlled or
// recorded. docs/BENCHMARKS.md, and the right to quote a number anywhere, are
// Phase 5's.

func benchValue(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte('a' + i%26)
	}
	return v
}

func BenchmarkMemTableAdd(b *testing.B) {
	m := memtable.New(1)
	value := benchValue(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Add(uint64(i+1), ikey.KindValue, []byte(fmt.Sprintf("key%09d", i)), value)
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	const n = 100000
	m := memtable.New(1)
	value := benchValue(100)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key%09d", i))
		m.Add(uint64(i+1), ikey.KindValue, keys[i], value)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, found := m.Get(keys[i%n], ikey.MaxSeq); !found {
			b.Fatal("missing key")
		}
	}
}

// memSource feeds a fixed set of entries to the SSTable writer.
type memSource struct {
	m  *memtable.MemTable
	it *memtable.Iterator
}

func newMemSource(n, valueSize int) *memSource {
	m := memtable.New(1)
	value := benchValue(valueSize)
	for i := 0; i < n; i++ {
		m.Add(uint64(i+1), ikey.KindValue, []byte(fmt.Sprintf("key%09d", i)), value)
	}
	return &memSource{m: m}
}

func (s *memSource) reset()        { s.it = s.m.NewIterator() }
func (s *memSource) Next() bool    { return s.it.Next() }
func (s *memSource) Key() []byte   { return s.it.Key() }
func (s *memSource) Value() []byte { return s.it.Value() }

func BenchmarkSSTableWrite(b *testing.B) {
	const n = 10000
	src := newMemSource(n, 100)
	dir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src.reset()
		path := fmt.Sprintf("%s/%06d.sst", dir, i)
		meta, err := sstable.WriteFile(path, src, sstable.WriterOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(meta.FileSize)
	}
	// Custom metrics are reported after the loop: ResetTimer clears them.
	b.ReportMetric(float64(n), "entries/file")
}

func BenchmarkSSTableGet(b *testing.B) {
	const n = 100000
	src := newMemSource(n, 100)
	src.reset()
	path := fmt.Sprintf("%s/%06d.sst", b.TempDir(), 1)
	if _, err := sstable.WriteFile(path, src, sstable.WriterOptions{}); err != nil {
		b.Fatal(err)
	}
	r, err := sstable.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%09d", i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, found, err := r.Get(keys[i%n], ikey.MaxSeq)
		if err != nil || !found {
			b.Fatalf("Get: found=%v err=%v", found, err)
		}
	}
}

func BenchmarkLSMPut(b *testing.B) {
	for _, mode := range []wal.SyncMode{wal.SyncOff, wal.SyncBatch} {
		b.Run(mode.String(), func(b *testing.B) {
			opts := storage.DefaultOptions()
			opts.WAL.SyncMode = mode
			s, err := storage.OpenLSMStore(b.TempDir(), opts)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = s.Close() }()

			ctx := context.Background()
			value := benchValue(100)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Put(ctx, []byte(fmt.Sprintf("key%09d", i)), value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLSMGet(b *testing.B) {
	const n = 50000
	opts := storage.DefaultOptions()
	opts.WAL.SyncMode = wal.SyncOff
	opts.MemTableSize = 512 << 10 // several SSTables, so the read path is real
	s, err := storage.OpenLSMStore(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	value := benchValue(100)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key%09d", i))
		if err := s.Put(ctx, keys[i], value); err != nil {
			b.Fatal(err)
		}
	}
	tables := len(s.SSTables())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get(ctx, keys[i%n]); err != nil {
			b.Fatal(err)
		}
	}
	// Every SSTable is consulted on every lookup, because Phase 3 has no
	// Bloom filter and no compaction. This metric is here so that the cost is
	// visible next to the timing rather than inferred from it.
	b.ReportMetric(float64(tables), "sstables")
}

// BenchmarkLSMRecovery measures reopening a directory: verifying every SSTable
// and replaying the WAL tail. It is the number that matters for restart time.
func BenchmarkLSMRecovery(b *testing.B) {
	const n = 50000
	opts := storage.DefaultOptions()
	opts.WAL.SyncMode = wal.SyncOff
	opts.MemTableSize = 512 << 10

	dir := b.TempDir()
	s, err := storage.OpenLSMStore(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	value := benchValue(100)
	for i := 0; i < n; i++ {
		if err := s.Put(ctx, []byte(fmt.Sprintf("key%09d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
	tables := len(s.SSTables())
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := storage.OpenLSMStore(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		if err := r.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(tables), "sstables")
	b.ReportMetric(float64(n), "mutations")
}
