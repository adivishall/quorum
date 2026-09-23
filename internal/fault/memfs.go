package fault

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/vfs"
)

// ErrCrashed is returned by a file handle opened before a simulated crash: the
// process that owned it is gone, so every later use of it fails.
var ErrCrashed = errors.New("fault: file handle belongs to a crashed process")

// MemFS is a deterministic, in-memory filesystem (vfs.FS) that models what a
// crash leaves on disk.
//
// Every file has two views:
//
//   - the cached view: every byte a Write accepted. Reads return it, and it is
//     what survives a PROCESS crash — the kernel outlives the process, so bytes
//     that reached it are still there after SIGKILL (CrashProcess).
//   - the durable view: the file's content as of its last successful Sync. This,
//     and only this, survives a modeled POWER LOSS (CrashPowerLoss), optionally
//     with a torn prefix of the bytes written after that Sync.
//
// A newly created file's existence is itself durable only once SyncDir has been
// called on its directory; a power loss before that removes it.
//
// This is a software model used to check that code issues its writes and fsyncs
// in the right order. It is NOT evidence about hardware: it assumes an fsync that
// returned success is honest, and it models un-synced data as either lost or kept
// as a torn prefix — never as holes, reordered sectors, or bit rot. Real
// power-loss durability remains untested (docs/FAULTS.md, docs/FAILURE_MODEL.md).
//
// MemFS is safe for concurrent use and uses no clock or randomness, so a run
// against it is exactly reproducible.
type MemFS struct {
	mu    sync.Mutex
	files map[string]*memNode
	epoch uint64 // bumped by every crash; handles from an older epoch are dead
}

// memNode is one file. The durable view is data[:syncedLen] while the file has
// only been appended to since its last Sync ("prefix mode"); an overwrite or
// truncation below syncedLen first detaches a private copy of the durable bytes,
// so appends — the common case for a log — never copy.
type memNode struct {
	data        []byte
	syncedLen   int
	detached    []byte
	isDetached  bool
	nameDurable bool
}

func (n *memNode) durable() []byte {
	if n.isDetached {
		return n.detached
	}
	return n.data[:n.syncedLen]
}

// detach preserves the durable bytes before a mutation inside them.
func (n *memNode) detach() {
	if !n.isDetached {
		n.detached = append([]byte(nil), n.data[:n.syncedLen]...)
		n.isDetached = true
	}
}

// NewMemFS returns an empty filesystem.
func NewMemFS() *MemFS { return &MemFS{files: map[string]*memNode{}} }

var _ vfs.FS = (*MemFS)(nil)

// OpenFile opens or creates name. It supports the os flags a durable log uses:
// O_RDONLY/O_WRONLY/O_RDWR, O_CREATE, O_EXCL, O_TRUNC, and O_APPEND.
func (m *MemFS) OpenFile(name string, flag int, perm fs.FileMode) (vfs.File, error) {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	acc := flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	writable := acc == os.O_WRONLY || acc == os.O_RDWR
	n, ok := m.files[name]
	switch {
	case ok && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrExist}
	case !ok && flag&os.O_CREATE == 0:
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	case !ok:
		n = &memNode{}
		m.files[name] = n
	}
	if flag&os.O_TRUNC != 0 && writable {
		if n.syncedLen > 0 {
			n.detach()
		}
		n.data = n.data[:0]
	}
	return &memFile{
		fs: m, name: name, node: n, epoch: m.epoch,
		readable: acc == os.O_RDONLY || acc == os.O_RDWR,
		writable: writable, appendMode: flag&os.O_APPEND != 0,
	}, nil
}

// Stat reports name's cached size.
func (m *MemFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.files[name]
	if !ok {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return memInfo{name: filepath.Base(name), size: int64(len(n.data))}, nil
}

// SyncDir makes the creation of every file directly inside dir durable.
func (m *MemFS) SyncDir(dir string) error {
	dir = filepath.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, n := range m.files {
		if filepath.Dir(name) == dir {
			n.nameDurable = true
		}
	}
	return nil
}

// CrashProcess models the owning process being killed (SIGKILL): every open
// handle dies, and every byte any Write accepted survives, synced or not.
func (m *MemFS) CrashProcess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
}

// CrashPowerLoss models losing power: every open handle dies, a file whose
// creation was never made durable disappears, and every other file reverts to its
// durable view plus the first tornTail bytes (at most) of what was written after
// its last Sync — a torn, partially flushed append. Bytes that were overwritten or
// truncated inside the durable region since the last Sync are simply lost (the
// durable view wins). Files are processed in name order, so the outcome is
// deterministic.
func (m *MemFS) CrashPowerLoss(tornTail int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	names := make([]string, 0, len(m.files))
	for name := range m.files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		n := m.files[name]
		if !n.nameDurable {
			delete(m.files, name)
			continue
		}
		kept := append([]byte(nil), n.durable()...)
		if !n.isDetached && tornTail > 0 {
			tail := n.data[n.syncedLen:]
			if tornTail < len(tail) {
				tail = tail[:tornTail]
			}
			kept = append(kept, tail...)
		}
		// After the power cycle, what is on the device is simply the file.
		m.files[name] = &memNode{data: kept, syncedLen: len(kept), nameDurable: true}
	}
}

// Cached returns a copy of name's cached view (what a reader sees now, and what a
// process crash preserves).
func (m *MemFS) Cached(name string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.files[filepath.Clean(name)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), n.data...), true
}

// Durable returns a copy of name's durable view (what a power loss preserves,
// ignoring a torn tail). ok is false if the file does not exist or its creation
// is not yet durable.
func (m *MemFS) Durable(name string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.files[filepath.Clean(name)]
	if !ok || !n.nameDurable {
		return nil, false
	}
	return append([]byte(nil), n.durable()...), true
}

// DurableCopy returns a new MemFS holding exactly what a power loss (with no torn
// tail) would leave right now — every durably created file at its durable view —
// without disturbing m. Reading a log through it shows the state a node could
// recover to if the machine lost power at this instant.
func (m *MemFS) DurableCopy() *MemFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := NewMemFS()
	for name, n := range m.files {
		if !n.nameDurable {
			continue
		}
		d := append([]byte(nil), n.durable()...)
		out.files[name] = &memNode{data: d, syncedLen: len(d), nameDurable: true}
	}
	return out
}

// FullySynced reports whether name exists, its creation is durable, and its
// cached view equals its durable view — i.e. a power loss right now would lose
// nothing. It is how a test asserts "everything written has been fsynced".
func (m *MemFS) FullySynced(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.files[filepath.Clean(name)]
	return ok && n.nameDurable && !n.isDetached && n.syncedLen == len(n.data)
}

// memFile is an open handle.
type memFile struct {
	fs         *MemFS
	name       string
	node       *memNode
	epoch      uint64
	off        int64
	readable   bool
	writable   bool
	appendMode bool
	closed     bool
}

var _ vfs.File = (*memFile)(nil)

// check validates the handle under fs.mu.
func (f *memFile) check(op string) error {
	if f.closed {
		return &fs.PathError{Op: op, Path: f.name, Err: fs.ErrClosed}
	}
	if f.epoch != f.fs.epoch {
		return &fs.PathError{Op: op, Path: f.name, Err: ErrCrashed}
	}
	return nil
}

func (f *memFile) Read(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check("read"); err != nil {
		return 0, err
	}
	if !f.readable {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrPermission}
	}
	if f.off >= int64(len(f.node.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.node.data[f.off:])
	f.off += int64(n)
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check("write"); err != nil {
		return 0, err
	}
	if !f.writable {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
	}
	n := f.node
	if f.appendMode {
		f.off = int64(len(n.data))
	}
	if f.off < int64(n.syncedLen) {
		n.detach() // overwriting durable bytes: keep the durable view intact
	}
	end := f.off + int64(len(p))
	if end > int64(len(n.data)) {
		if f.off > int64(len(n.data)) {
			n.data = append(n.data, make([]byte, f.off-int64(len(n.data)))...) // sparse gap reads as zeros
		}
		n.data = append(n.data[:f.off], p...)
	} else {
		copy(n.data[f.off:end], p)
	}
	f.off = end
	return len(p), nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check("seek"); err != nil {
		return 0, err
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = f.off
	case io.SeekEnd:
		base = int64(len(f.node.data))
	default:
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	if base+offset < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	f.off = base + offset
	return f.off, nil
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check("stat"); err != nil {
		return nil, err
	}
	return memInfo{name: filepath.Base(f.name), size: int64(len(f.node.data))}, nil
}

func (f *memFile) Truncate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check("truncate"); err != nil {
		return err
	}
	if !f.writable {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrPermission}
	}
	if size < 0 {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrInvalid}
	}
	n := f.node
	if size < int64(n.syncedLen) {
		n.detach()
	}
	if size <= int64(len(n.data)) {
		n.data = n.data[:size]
	} else {
		n.data = append(n.data, make([]byte, size-int64(len(n.data)))...)
	}
	return nil
}

// Sync makes the file's cached view its durable view. It does not make the
// file's creation durable — that is SyncDir's job, as with a real directory.
func (f *memFile) Sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check("sync"); err != nil {
		return err
	}
	n := f.node
	n.isDetached = false
	n.detached = nil
	n.syncedLen = len(n.data)
	return nil
}

func (f *memFile) Close() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	return nil
}

// memInfo is the fs.FileInfo MemFS reports. ModTime is the zero time, so no
// clock is read and results stay reproducible.
type memInfo struct {
	name string
	size int64
}

func (i memInfo) Name() string       { return i.name }
func (i memInfo) Size() int64        { return i.size }
func (i memInfo) Mode() fs.FileMode  { return 0o644 }
func (i memInfo) ModTime() time.Time { return time.Time{} }
func (i memInfo) IsDir() bool        { return false }
func (i memInfo) Sys() any           { return nil }
