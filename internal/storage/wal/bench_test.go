package wal_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/adivishall/quorum/internal/storage/wal"
)

// These benchmarks exist to make the cost of each sync mode visible and
// reproducible, not to produce a number worth quoting. Phase 5 is where
// benchmarking is done properly: controlled dataset sizes, percentiles,
// recorded hardware, and a methodology written down. Nothing here should be
// cited as a performance result.

func benchAppend(b *testing.B, mode wal.SyncMode, valueSize int) {
	b.Helper()
	dir := b.TempDir()
	opts := wal.DefaultOptions()
	opts.SyncMode = mode

	w, err := wal.Create(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	value := bytes.Repeat([]byte("v"), valueSize)
	b.SetBytes(int64(valueSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		op := wal.Op{Kind: wal.OpPut, Key: []byte(fmt.Sprintf("key%08d", i)), Value: value}
		if err := w.AppendBatch(wal.Batch{op}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendSyncOff100B(b *testing.B)   { benchAppend(b, wal.SyncOff, 100) }
func BenchmarkAppendSyncBatch100B(b *testing.B) { benchAppend(b, wal.SyncBatch, 100) }
func BenchmarkAppendSyncAlways100B(b *testing.B) {
	benchAppend(b, wal.SyncAlways, 100)
}
func BenchmarkAppendSyncBatch4KiB(b *testing.B) { benchAppend(b, wal.SyncBatch, 4096) }

// BenchmarkReplay measures recovery throughput, which is what restart time is
// proportional to. In Phase 2 the whole log is replayed on every open, with no
// snapshot or truncation to bound it — that is the limitation Phases 3 and 4
// exist to remove, and this benchmark is how its cost stays visible.
func BenchmarkReplay(b *testing.B) {
	const records = 20000
	dir := b.TempDir()
	opts := wal.DefaultOptions()
	opts.SyncMode = wal.SyncOff

	w, err := wal.Create(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	value := bytes.Repeat([]byte("v"), 100)
	for i := 0; i < records; i++ {
		op := wal.Op{Kind: wal.OpPut, Key: []byte(fmt.Sprintf("key%08d", i)), Value: value}
		if err := w.AppendBatch(wal.Batch{op}); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	var bytesScanned int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		rec, err := wal.Recover(dir, wal.Handler{Batch: func(batch wal.Batch) error { n += len(batch); return nil }})
		if err != nil {
			b.Fatal(err)
		}
		if n != records {
			b.Fatalf("replayed %d records, want %d", n, records)
		}
		bytesScanned = rec.BytesScanned
	}
	b.SetBytes(bytesScanned)
	b.ReportMetric(float64(records)/1e3, "krecords/op")
}
