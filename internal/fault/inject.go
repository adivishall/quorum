package fault

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/adivishall/quorum/internal/vfs"
)

// ErrInjected marks every error an InjectFS fabricates, so a test can tell an
// injected failure from a real one with errors.Is.
var ErrInjected = errors.New("fault: injected I/O error")

// Op names a filesystem operation for fault injection and for the op log.
type Op uint8

const (
	OpOpen Op = iota + 1
	OpWrite
	OpSync
	OpTruncate
	OpSyncDir
)

func (o Op) String() string {
	switch o {
	case OpOpen:
		return "open"
	case OpWrite:
		return "write"
	case OpSync:
		return "fsync"
	case OpTruncate:
		return "truncate"
	case OpSyncDir:
		return "syncdir"
	default:
		return "op(?)"
	}
}

// Injection is one armed, one-shot I/O fault: the Nth matching operation after
// it is armed fails. It fires exactly once and is then spent.
type Injection struct {
	// Op is the operation to fail.
	Op Op
	// Path restricts the fault to one file (or, for OpSyncDir, one directory).
	// Empty matches any path.
	Path string
	// Nth selects which matching operation fails: 1 (or 0) is the next one.
	Nth int
	// Short, for OpWrite only, is how many bytes of the failing write still reach
	// the file before the error — a torn, partial record. 0 writes nothing. It is
	// clamped below the write's length so the write is genuinely short.
	Short int
	// Err is the underlying error, e.g. syscall.ENOSPC for a full disk. Nil means
	// syscall.EIO. The returned error wraps both ErrInjected and Err.
	Err error
	// Gate, if non-nil, turns the fault into a STALL instead of a failure: the
	// matching operation blocks until Gate is closed and then completes normally —
	// a disk that is slow, not broken (e.g. an fsync that takes seconds). Err and
	// Short are ignored for a stall. Stalls apply to OpWrite, OpSync and OpTruncate.
	Gate <-chan struct{}
	// At, if non-nil, turns the injection into an OBSERVATION POINT instead of a
	// failure: it is called when the matching operation is reached, before the
	// operation is performed, and the operation then proceeds normally. It is how
	// a crash is placed at an exact I/O boundary (Phase 11, docs/CRASH_RECOVERY.md):
	// a real process kills itself in At, so nothing after that point happens; the
	// simulator marks the disk's process as crashed in At, so the operation — and
	// every later one on the same handles — fails with ErrCrashed. (An operation
	// that has no handle — OpenFile, SyncDir — still completes after At; a
	// simulated crash before those is modelled with a failing injection instead.)
	// Err, Short and Gate are ignored when At is set.
	At func()
}

// OpRecord is one entry in an InjectFS op log.
type OpRecord struct {
	Op       Op
	Path     string
	Bytes    int   // OpWrite: bytes that reached the file
	Err      error // nil on success
	Injected bool  // the failure was fabricated by an Injection
}

// InjectFS decorates any vfs.FS (the real OS or a MemFS) with armed, one-shot I/O
// faults, and records every mutating operation it forwards. It is how a test
// makes a real durable-log write or fsync fail at the exact point it chooses,
// underneath the unchanged production code, and then proves what happened after:
// the op log shows whether anything was written or synced after the failure.
//
// It adds no randomness: which operation fails is fixed by the Injection, so a
// scenario that arms the same faults and performs the same operations fails
// identically every time.
type InjectFS struct {
	base vfs.FS

	mu    sync.Mutex
	armed []*armed
	ops   []OpRecord
}

type armed struct {
	inj  Injection
	seen int
}

// NewInjectFS wraps base (nil means the real OS filesystem).
func NewInjectFS(base vfs.FS) *InjectFS { return &InjectFS{base: vfs.Or(base)} }

var _ vfs.FS = (*InjectFS)(nil)

// Arm queues a one-shot fault.
func (f *InjectFS) Arm(inj Injection) {
	if inj.Nth < 1 {
		inj.Nth = 1
	}
	if inj.Err == nil {
		inj.Err = syscall.EIO
	}
	if inj.Path != "" {
		inj.Path = filepath.Clean(inj.Path)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = append(f.armed, &armed{inj: inj})
}

// Disarm removes every fault that has not fired yet.
func (f *InjectFS) Disarm() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = nil
}

// Armed reports how many faults are still waiting to fire.
func (f *InjectFS) Armed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.armed)
}

// Ops returns a copy of the op log: every open, write, fsync, truncate and
// directory sync forwarded or failed, in order.
func (f *InjectFS) Ops() []OpRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]OpRecord(nil), f.ops...)
}

// take reports whether op on path must fail now, consuming the injection if so.
// Called with f.mu held.
func (f *InjectFS) take(op Op, path string) (Injection, bool) {
	for i, a := range f.armed {
		if a.inj.Op != op || (a.inj.Path != "" && a.inj.Path != path) {
			continue
		}
		a.seen++
		if a.seen < a.inj.Nth {
			continue
		}
		f.armed = append(f.armed[:i], f.armed[i+1:]...)
		return a.inj, true
	}
	return Injection{}, false
}

func injectedErr(op Op, path string, inj Injection) error {
	return &fs.PathError{Op: op.String(), Path: path, Err: fmt.Errorf("%w: %w", ErrInjected, inj.Err)}
}

func (f *InjectFS) record(r OpRecord) {
	f.ops = append(f.ops, r)
}

// OpenFile forwards to the base filesystem unless an OpOpen fault fires.
func (f *InjectFS) OpenFile(name string, flag int, perm fs.FileMode) (vfs.File, error) {
	name = filepath.Clean(name)
	f.mu.Lock()
	inj, fire := f.take(OpOpen, name)
	f.mu.Unlock()
	if fire && inj.At != nil {
		inj.At()
		fire = false
	}
	if fire {
		err := injectedErr(OpOpen, name, inj)
		f.mu.Lock()
		f.record(OpRecord{Op: OpOpen, Path: name, Err: err, Injected: true})
		f.mu.Unlock()
		return nil, err
	}
	file, err := f.base.OpenFile(name, flag, perm)
	f.mu.Lock()
	f.record(OpRecord{Op: OpOpen, Path: name, Err: err})
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &injectFile{File: file, fs: f, path: name}, nil
}

// Stat forwards to the base filesystem.
func (f *InjectFS) Stat(name string) (fs.FileInfo, error) { return f.base.Stat(name) }

// SyncDir forwards to the base filesystem unless an OpSyncDir fault fires.
func (f *InjectFS) SyncDir(dir string) error {
	dir = filepath.Clean(dir)
	f.mu.Lock()
	inj, fire := f.take(OpSyncDir, dir)
	f.mu.Unlock()
	if fire && inj.At != nil {
		inj.At()
		fire = false
	}
	if fire {
		err := injectedErr(OpSyncDir, dir, inj)
		f.mu.Lock()
		f.record(OpRecord{Op: OpSyncDir, Path: dir, Err: err, Injected: true})
		f.mu.Unlock()
		return err
	}
	err := f.base.SyncDir(dir)
	f.mu.Lock()
	f.record(OpRecord{Op: OpSyncDir, Path: dir, Err: err})
	f.mu.Unlock()
	return err
}

// injectFile intercepts the mutating operations of one open file.
type injectFile struct {
	vfs.File
	fs   *InjectFS
	path string
}

// proceed handles a firing injection that is not a failure — an observation
// point (At) or a stall (Gate) — and reports whether the operation should then
// proceed normally (true) or fail as injected (false).
func proceed(inj Injection) bool {
	if inj.At != nil {
		inj.At()
		return true
	}
	if inj.Gate == nil {
		return false
	}
	<-inj.Gate
	return true
}

func (h *injectFile) Write(p []byte) (int, error) {
	h.fs.mu.Lock()
	inj, fire := h.fs.take(OpWrite, h.path)
	h.fs.mu.Unlock()
	if fire && proceed(inj) {
		fire = false
	}
	if fire {
		n := 0
		if inj.Short > 0 && len(p) > 0 {
			short := inj.Short
			if short >= len(p) {
				short = len(p) - 1
			}
			n, _ = h.File.Write(p[:short])
		}
		err := injectedErr(OpWrite, h.path, inj)
		h.fs.mu.Lock()
		h.fs.record(OpRecord{Op: OpWrite, Path: h.path, Bytes: n, Err: err, Injected: true})
		h.fs.mu.Unlock()
		return n, err
	}
	n, err := h.File.Write(p)
	h.fs.mu.Lock()
	h.fs.record(OpRecord{Op: OpWrite, Path: h.path, Bytes: n, Err: err})
	h.fs.mu.Unlock()
	return n, err
}

// Sync fails WITHOUT forwarding when a fault fires: the data stays wherever the
// earlier writes left it (cached, not durable), which is what a failed fsync
// guarantees and all it guarantees.
func (h *injectFile) Sync() error {
	h.fs.mu.Lock()
	inj, fire := h.fs.take(OpSync, h.path)
	h.fs.mu.Unlock()
	if fire && proceed(inj) {
		fire = false
	}
	if fire {
		err := injectedErr(OpSync, h.path, inj)
		h.fs.mu.Lock()
		h.fs.record(OpRecord{Op: OpSync, Path: h.path, Err: err, Injected: true})
		h.fs.mu.Unlock()
		return err
	}
	err := h.File.Sync()
	h.fs.mu.Lock()
	h.fs.record(OpRecord{Op: OpSync, Path: h.path, Err: err})
	h.fs.mu.Unlock()
	return err
}

func (h *injectFile) Truncate(size int64) error {
	h.fs.mu.Lock()
	inj, fire := h.fs.take(OpTruncate, h.path)
	h.fs.mu.Unlock()
	if fire && proceed(inj) {
		fire = false
	}
	if fire {
		err := injectedErr(OpTruncate, h.path, inj)
		h.fs.mu.Lock()
		h.fs.record(OpRecord{Op: OpTruncate, Path: h.path, Err: err, Injected: true})
		h.fs.mu.Unlock()
		return err
	}
	err := h.File.Truncate(size)
	h.fs.mu.Lock()
	h.fs.record(OpRecord{Op: OpTruncate, Path: h.path, Err: err})
	h.fs.mu.Unlock()
	return err
}
