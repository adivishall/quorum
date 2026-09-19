package storage

import (
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/adivishall/quorum/internal/storage/manifest"
	"github.com/adivishall/quorum/internal/storage/memtable"
	"github.com/adivishall/quorum/internal/storage/sstable"
)

// sstFile is one live SSTable: its MANIFEST metadata and an open reader.
//
// refs counts how many live versions contain this file, not how many readers are
// using it. It changes only when a version is created or destroyed — once per
// flush or compaction — so it is guarded by LSMStore.mu rather than being atomic,
// and the read path never touches it.
type sstFile struct {
	meta manifest.FileMeta
	r    *sstable.Reader
	path string

	refs int // guarded by LSMStore.mu
}

// version is an immutable snapshot of every place data can live.
//
// This is the abstraction that makes a compaction's file-set change atomic from a
// reader's point of view (INV-S5). A reader acquires the current version once and
// holds it for the whole operation, so it sees either the file set before the
// compaction or the one after it, and never a mixture — not "the output plus one
// of the inputs", and not "two inputs with the third already gone".
//
// refs is the number of holders: one for being the current version, plus one per
// in-flight operation. It is atomic because the read path increments it, and it
// is the only per-read synchronisation the version machinery costs — O(1),
// independent of how many files the version holds.
type version struct {
	mem   *memtable.MemTable
	imm   []*memtable.MemTable
	files []*sstFile // newest first: descending LargestSeq

	refs atomic.Int32
}

// newVersion returns a version holding these sources, with one reference for the
// caller that is about to install it.
//
// files is sorted newest-first by sequence range. That ordering is the read
// path's correctness argument: live files have pairwise-disjoint sequence ranges,
// so sorting by LargestSeq descending is a total order from newest data to
// oldest, and "first source holding any version of the key wins" follows.
// Sorting here rather than trusting the caller means the ordering cannot be got
// wrong by whichever code path built the list.
func newVersion(mem *memtable.MemTable, imm []*memtable.MemTable, files []*sstFile) *version {
	sorted := append([]*sstFile(nil), files...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].meta.LargestSeq != sorted[j].meta.LargestSeq {
			return sorted[i].meta.LargestSeq > sorted[j].meta.LargestSeq
		}
		// Unreachable for a coherent file set, since disjoint ranges cannot share
		// a largest sequence. Ordering by file number keeps the sort total, and
		// therefore deterministic, even for a set that is about to be rejected.
		return sorted[i].meta.Num > sorted[j].meta.Num
	})
	v := &version{mem: mem, imm: imm, files: sorted}
	v.refs.Store(1)
	return v
}

// fileByNum returns the live file with this number, or nil.
func (v *version) fileByNum(num uint64) *sstFile {
	for _, f := range v.files {
		if f.meta.Num == num {
			return f
		}
	}
	return nil
}

// metas returns the version's file metadata, for building a MANIFEST snapshot.
func (v *version) metas() []manifest.FileMeta {
	out := make([]manifest.FileMeta, 0, len(v.files))
	for _, f := range v.files {
		out = append(out, f.meta)
	}
	return out
}

// maxFlushedSeq returns the highest sequence number any live file covers, which
// is the boundary WAL replay skips up to.
func (v *version) maxFlushedSeq() uint64 {
	var max uint64
	for _, f := range v.files {
		if f.meta.LargestSeq > max {
			max = f.meta.LargestSeq
		}
	}
	return max
}

// byLevel groups the files by level, each group newest-first.
func (v *version) byLevel() map[int][]*sstFile {
	out := make(map[int][]*sstFile)
	for _, f := range v.files {
		out[f.meta.Level] = append(out[f.meta.Level], f)
	}
	return out
}

// ---------------------------------------------------------------- refcounting

// acquire returns the current version with a reference held.
//
// The critical section is a pointer read and one atomic increment: a reader never
// waits on disk I/O, and a compaction publishing a new version never waits on a
// reader's block read.
func (s *LSMStore) acquire() *version {
	s.mu.RLock()
	v := s.cur
	if v != nil {
		v.refs.Add(1)
	}
	s.mu.RUnlock()
	return v
}

// release drops a reference and reaps the version if it was the last one.
//
// A version whose count reaches zero cannot be the current version, because
// being current is itself a reference. So reaching zero means no reader can ever
// reach this version again, and any file that no other live version holds is
// now unreachable and can be closed and deleted.
func (s *LSMStore) release(v *version) {
	if v == nil {
		return
	}
	if v.refs.Add(-1) != 0 {
		return
	}
	s.reap(v)
}

// reap closes and deletes the files that became unreachable with v.
//
// Deleting the file is the last step of the compaction protocol (docs/DESIGN.md
// §6 step 5) and it is safe here for two reasons held together: the MANIFEST no
// longer names the file, so no future startup will look for it; and no live
// version holds it, so no present or future reader can ask for it.
//
// A crash before this point leaves the file on disk and unreferenced, which is an
// orphan and is swept at the next startup. That is why the deletion can be this
// casual about failing: an undeleted obsolete file costs space, never
// correctness.
func (s *LSMStore) reap(v *version) {
	s.mu.Lock()
	var dead []*sstFile
	for _, f := range v.files {
		f.refs--
		if f.refs == 0 {
			dead = append(dead, f)
		}
	}
	s.mu.Unlock()

	for _, f := range dead {
		_ = f.r.Close()
		if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
			// Nothing to do but record it. The file is already invisible to the
			// database; the next startup's orphan sweep will try again.
			s.noteObsoleteRemovalFailure(f.meta.Num, err)
			continue
		}
		s.countObsoleteRemoved()
	}
}

// install publishes a new version and releases the previous one.
//
// The swap is a pointer assignment under mu, so from a reader's point of view the
// entire file-set change — output added, inputs removed — happens at one instant.
// Every file in the new version gains a reference before the old version's
// reference is dropped, so a file present in both is never momentarily at zero.
//
// mu must not be held by the caller.
func (s *LSMStore) install(v *version) {
	s.mu.Lock()
	for _, f := range v.files {
		f.refs++
	}
	old := s.cur
	s.cur = v
	s.mu.Unlock()

	s.release(old)
}

// closeAllReaders closes every reader the current version holds, without
// deleting anything. Used by Close, where files are being released rather than
// retired.
func (s *LSMStore) closeAllReaders() {
	s.mu.Lock()
	v := s.cur
	s.cur = nil
	s.mu.Unlock()
	if v == nil {
		return
	}
	for _, f := range v.files {
		_ = f.r.Close()
	}
}

// sstPath returns the on-disk path of an SSTable number.
func (s *LSMStore) sstPath(num uint64) string {
	return filepath.Join(s.dir, sstName(num))
}
