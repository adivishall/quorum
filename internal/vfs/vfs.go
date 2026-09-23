// Package vfs is the filesystem seam of Quorum's durable Raft log (Phase 10,
// docs/FAULTS.md, ADR-017).
//
// internal/raftlog reads and writes its log through the two small interfaces
// here instead of calling package os directly. Production code uses OS, which is
// a zero-cost pass-through to the real filesystem and is what a nil FS means
// everywhere. Tests substitute internal/fault's implementations at exactly this
// boundary: a crash-consistent in-memory filesystem (to model what a process
// crash and a power loss each leave on disk) and a decorator that makes a chosen
// write or fsync fail. The seam is the real persistence boundary — the durable
// log's own code runs unchanged on top of it — so an injected I/O error exercises
// the same Save/recovery paths a real one would.
//
// The interfaces are deliberately the subset of *os.File and package os that the
// durable log uses, so *os.File satisfies File as-is and nothing is re-invented.
package vfs

import (
	"io"
	"io/fs"
	"os"
)

// File is the subset of *os.File a durable log uses. *os.File implements it.
type File interface {
	io.Reader
	io.Writer
	io.Seeker
	// Stat reports the file's current size (other fields are not relied on).
	Stat() (fs.FileInfo, error)
	// Truncate changes the file's size.
	Truncate(size int64) error
	// Sync commits the file's current contents to stable storage (fsync).
	Sync() error
	// Close releases the file. It does not imply Sync.
	Close() error
}

// FS opens files and makes directory entries durable.
type FS interface {
	// OpenFile opens name with os.OpenFile's flag and permission semantics.
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	// Stat reports on name; a missing file yields an error satisfying
	// errors.Is(err, fs.ErrNotExist).
	Stat(name string) (fs.FileInfo, error)
	// SyncDir fsyncs the directory dir, making the creation of the files in it
	// durable.
	SyncDir(dir string) error
}

// OS is the real filesystem, and the production default: every method is a
// direct call into package os.
type OS struct{}

var _ FS = OS{}

// OpenFile calls os.OpenFile.
func (OS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err // never a typed-nil *os.File inside a non-nil File
	}
	return f, nil
}

// Stat calls os.Stat.
func (OS) Stat(name string) (fs.FileInfo, error) { return os.Stat(name) }

// SyncDir opens dir and fsyncs it.
func (OS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// Or returns fsys, or OS when fsys is nil — so a zero-valued option means the
// real filesystem.
func Or(fsys FS) FS {
	if fsys == nil {
		return OS{}
	}
	return fsys
}
