package snapshot

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/adivishall/quorum/internal/vfs"
)

// Files are one node's snapshot files, beside its Raft log (Base is the log's
// path): the published snapshot and two temporaries.
//
//	Base+".snap"       the published snapshot — at most one; replaced atomically
//	Base+".snap.tmp"   a snapshot being written by this node (creation)
//	Base+".snap.recv"  a snapshot being received from a leader (installation)
//
// A temporary is never loaded: RemoveOrphans deletes both at startup, so a crash
// in the middle of writing or receiving one leaves nothing behind that matters.
type Files struct {
	FS   vfs.FS // nil: the real OS filesystem
	Base string
}

// Path, TmpPath and RecvPath name the three files.
func (f Files) Path() string     { return f.Base + ".snap" }
func (f Files) TmpPath() string  { return f.Base + ".snap.tmp" }
func (f Files) RecvPath() string { return f.Base + ".snap.recv" }

func (f Files) fs() vfs.FS { return vfs.Or(f.FS) }

// Publish makes file (a complete, encoded snapshot) the published snapshot,
// crash-safely: write it to the temporary, fsync it, rename it over the
// published name, fsync the directory. Before the rename the published snapshot
// is the previous one (the temporary is an orphan); after the directory fsync it
// is this one; between the two, a power loss may undo the rename — leaving the
// previous one, still whole. Callers never compact the log before Publish
// returns (docs/SNAPSHOTS.md §5).
func (f Files) Publish(file []byte) error {
	if err := writeSynced(f.fs(), f.TmpPath(), file); err != nil {
		return err
	}
	return f.promote(f.TmpPath())
}

// PublishReceived makes a received snapshot — already written to RecvPath and
// fsynced by a Receiver — the published one: rename, then directory fsync.
func (f Files) PublishReceived() error { return f.promote(f.RecvPath()) }

func (f Files) promote(from string) error {
	if err := f.fs().Rename(from, f.Path()); err != nil {
		return err
	}
	return f.fs().SyncDir(filepath.Dir(f.Path()))
}

// Load reads and fully validates the published snapshot. found is false when
// there is none. A published snapshot that does not decode is an error — the
// node must refuse to start rather than run without the only record of its
// compacted prefix (docs/SNAPSHOTS.md §7).
func (f Files) Load() (m Meta, data, file []byte, found bool, err error) {
	file, err = readFile(f.fs(), f.Path())
	if errors.Is(err, fs.ErrNotExist) {
		return Meta{}, nil, nil, false, nil
	}
	if err != nil {
		return Meta{}, nil, nil, false, err
	}
	m, data, err = Decode(file)
	if err != nil {
		return Meta{}, nil, nil, true, fmt.Errorf("%s: %w", f.Path(), err)
	}
	return m, data, file, true, nil
}

// RemoveOrphans deletes both temporaries if present, and makes the deletion
// durable.
func (f Files) RemoveOrphans() error {
	removed := false
	for _, p := range []string{f.TmpPath(), f.RecvPath()} {
		err := f.fs().Remove(p)
		switch {
		case err == nil:
			removed = true
		case !errors.Is(err, fs.ErrNotExist):
			return err
		}
	}
	if removed {
		return f.fs().SyncDir(filepath.Dir(f.Path()))
	}
	return nil
}

// writeSynced creates (truncating) name, writes data and fsyncs it. A fresh
// file's directory entry is made durable by the rename's directory fsync that
// follows, never needed before: a temporary lost to a power loss is simply gone.
func writeSynced(fsys vfs.FS, name string, data []byte) error {
	file, err := fsys.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// readFile reads a whole file through fsys, refusing one larger than a
// snapshot can be.
func readFile(fsys vfs.FS, name string) ([]byte, error) {
	file, err := fsys.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > MaxFile {
		return nil, fmt.Errorf("%w: %s is %d bytes", ErrTooLarge, name, info.Size())
	}
	buf := make([]byte, info.Size())
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// MaxFile bounds a snapshot file: the state bound plus framing and metadata.
const MaxFile = MaxData + MaxData/MaxDataRecord*16 + 1<<20
