package bench

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/adivishall/quorum/internal/storage"
)

// StorageMetrics is a snapshot of the engine-internal counters a benchmark
// reports alongside timing. Every field is read from the store or the
// filesystem, never estimated. Units are in the field names.
type StorageMetrics struct {
	SSTables          int    `json:"sstables"`
	LiveBytes         int64  `json:"live_sstable_bytes"`
	WALBytes          int64  `json:"wal_bytes_on_disk"`
	FlushCount        int64  `json:"flush_count"`
	FlushBytes        int64  `json:"flush_bytes"`
	FlushEntries      int64  `json:"flush_entries"`
	CompactionRuns    int64  `json:"compaction_runs"`
	CompactInBytes    int64  `json:"compaction_input_bytes"`
	CompactOutBytes   int64  `json:"compaction_output_bytes"`
	VersionsDropped   int64  `json:"compaction_versions_dropped"`
	TombstonesDropped int64  `json:"compaction_tombstones_dropped"`
	TombstonesKept    int64  `json:"compaction_tombstones_kept"`
	BlockReads        uint64 `json:"block_reads"`
	FilterSkips       uint64 `json:"filter_skips"`
}

// SnapshotStorage reads the store's counters and sums the on-disk WAL. WAL bytes
// are taken from the segment files rather than a counter because the WAL is
// never truncated in this phase, so the files on disk are exactly the cumulative
// bytes written — a physical measurement, not an engine estimate. dir is the
// store's data directory.
func SnapshotStorage(s *storage.LSMStore, dir string) StorageMetrics {
	fs := s.FlushStats()
	cs := s.CompactionStats()
	rc := s.ReadCounters()

	var live int64
	for _, t := range s.SSTables() {
		live += t.Bytes
	}

	return StorageMetrics{
		SSTables:          len(s.SSTables()),
		LiveBytes:         live,
		WALBytes:          walBytesOnDisk(dir),
		FlushCount:        fs.Flushes,
		FlushBytes:        fs.Bytes,
		FlushEntries:      fs.Entries,
		CompactionRuns:    cs.Runs,
		CompactInBytes:    cs.InputBytes,
		CompactOutBytes:   cs.OutputBytes,
		VersionsDropped:   cs.VersionsDropped,
		TombstonesDropped: cs.TombstonesDropped,
		TombstonesKept:    cs.TombstonesKept,
		BlockReads:        rc.BlockReads,
		FilterSkips:       rc.FilterSkips,
	}
}

// walBytesOnDisk sums the sizes of the WAL segment files (%06d.log) in dir. It
// returns 0 if the directory cannot be read; a benchmark reports the number it
// measured and does not fail on an unreadable directory.
func walBytesOnDisk(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total
}

// DirBytes sums the sizes of every regular file under dir, recursively: the
// engine's total on-disk footprint (WAL + SSTables + MANIFEST + CURRENT). Used
// for the storage-overhead and dataset-size measurements.
func DirBytes(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
