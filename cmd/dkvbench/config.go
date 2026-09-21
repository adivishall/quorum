package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/adivishall/quorum/internal/bench"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// storeSpec is the configuration one benchmarked store opens with. Every field a
// suite varies is here, so the Config recorded in a Result is derived from the
// same values the store actually used — a result can never disagree with the
// configuration printed beside it.
type storeSpec struct {
	sync        wal.SyncMode
	memtable    int64
	blockSize   int
	bloom       bool
	bitsPerKey  int
	l0Trigger   int
	l1MaxBytes  int64
	autoCompact bool
}

// defaultSpec is the engine's real default configuration (docs/DESIGN.md), with
// batch sync. Suites that need a specific behaviour — several SSTables, forced
// compaction — override individual fields and the override is recorded, not
// hidden.
func defaultSpec() storeSpec {
	d := storage.DefaultOptions()
	return storeSpec{
		sync:        wal.SyncBatch,
		memtable:    d.MemTableSize,
		blockSize:   d.BlockSize,
		bloom:       true,
		bitsPerKey:  d.BitsPerKey,
		l0Trigger:   d.L0CompactionTrigger,
		l1MaxBytes:  d.L1MaxBytes,
		autoCompact: true,
	}
}

// options turns a spec into storage.Options.
func (sp storeSpec) options() storage.Options {
	o := storage.DefaultOptions()
	o.WAL.SyncMode = sp.sync
	o.MemTableSize = sp.memtable
	if sp.blockSize > 0 {
		o.BlockSize = sp.blockSize
	}
	o.DisableBloomFilter = !sp.bloom
	if sp.bitsPerKey > 0 {
		o.BitsPerKey = sp.bitsPerKey
	}
	if sp.l0Trigger > 0 {
		o.L0CompactionTrigger = sp.l0Trigger
	}
	if sp.l1MaxBytes > 0 {
		o.L1MaxBytes = sp.l1MaxBytes
	}
	o.DisableAutoCompaction = !sp.autoCompact
	return o
}

// config records the spec (plus the workload parameters the suite chose) into a
// bench.Config for the result.
func (sp storeSpec) config(dataset, keyBytes, valueBytes, concurrency int, workload, ratio string, onTmpfs bool) bench.Config {
	o := sp.options()
	return bench.Config{
		DatasetSize:     dataset,
		KeyBytes:        keyBytes,
		ValueBytes:      valueBytes,
		Concurrency:     concurrency,
		Workload:        workload,
		WorkloadRatio:   ratio,
		SyncMode:        sp.sync.String(),
		MemTableBytes:   o.MemTableSize,
		BlockBytes:      o.BlockSize,
		BloomBitsPerKey: o.BitsPerKey,
		BloomEnabled:    sp.bloom,
		L0Trigger:       o.L0CompactionTrigger,
		L1MaxBytes:      o.L1MaxBytes,
		AutoCompaction:  sp.autoCompact,
		OnTmpfs:         onTmpfs,
	}
}

// dataDir returns a fresh, empty directory for one store, unique per name so a
// suite's runs never share state. It lives under the harness's base directory.
func (h *harness) dataDir(name string) (string, error) {
	dir := filepath.Join(h.baseDir, name)
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

// openStore opens a fresh store for a spec in a uniquely named directory.
func (h *harness) openStore(name string, sp storeSpec) (*storage.LSMStore, string, error) {
	dir, err := h.dataDir(name)
	if err != nil {
		return nil, "", err
	}
	s, err := storage.OpenLSMStore(dir, sp.options())
	if err != nil {
		return nil, "", fmt.Errorf("open store %s: %w", name, err)
	}
	return s, dir, nil
}
