package storage_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/storage"
	"github.com/adivishall/quorum/internal/storage/wal"
	"github.com/adivishall/quorum/internal/vfs"
)

// The S1 crash matrix (docs/STORAGE_INTEGRATION.md §7.7): R1's storage half,
// proven against the process-crash AND power-loss model, at every WAL I/O
// operation a script of apply batches, syncs and a flush performs.

// errDead is what every operation of a crashed process gets.
var errDead = errors.New("matrix: the process crashed")

// crashFS is the WAL's filesystem in the matrix: a MemFS that counts every
// operation that changes it and, at the chosen one, kills the process there —
// the operation does not happen (or, for a torn write, half of it does) and
// nothing the dead process does afterwards reaches the disk.
type crashFS struct {
	mem *fault.MemFS

	mu   sync.Mutex
	n    int  // mutating operations seen
	at   int  // crash at the at'th (0: never)
	torn bool // a crashing write lets half its bytes through first
	dead bool
	ops  []string // what each operation was, for a failing cell's report
}

var _ vfs.FS = (*crashFS)(nil)

// step accounts for one mutating operation and reports whether the process
// dies at it.
func (c *crashFS) step(desc string) (die bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead {
		return false, errDead
	}
	c.n++
	c.ops = append(c.ops, desc)
	if c.n == c.at {
		c.dead = true
		return true, nil
	}
	return false, nil
}

// kill crashes the process between operations.
func (c *crashFS) kill() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.dead {
		c.dead = true
		c.mem.CrashProcess()
	}
}

func (c *crashFS) isDead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead
}

// revive starts a new process on the same disk, counting from zero.
func (c *crashFS) revive(at int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n, c.at, c.dead, c.ops = 0, at, false, nil
}

func (c *crashFS) handleless(desc string, do func() error) error {
	die, err := c.step(desc)
	if err != nil {
		return err
	}
	if die {
		c.mem.CrashProcess()
		return errDead
	}
	return do()
}

func (c *crashFS) OpenFile(name string, flag int, perm fs.FileMode) (vfs.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) == 0 {
		if c.isDead() {
			return nil, errDead
		}
		return c.mem.OpenFile(name, flag, perm)
	}
	var f vfs.File
	err := c.handleless("open "+filepath.Base(name), func() error {
		var err error
		f, err = c.mem.OpenFile(name, flag, perm)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &crashFile{File: f, c: c, name: filepath.Base(name)}, nil
}

func (c *crashFS) Stat(name string) (fs.FileInfo, error) {
	if c.isDead() {
		return nil, errDead
	}
	return c.mem.Stat(name)
}

func (c *crashFS) ReadDir(dir string) ([]fs.DirEntry, error) {
	if c.isDead() {
		return nil, errDead
	}
	return c.mem.ReadDir(dir)
}

func (c *crashFS) SyncDir(dir string) error {
	return c.handleless("syncdir "+filepath.Base(dir), func() error { return c.mem.SyncDir(dir) })
}

func (c *crashFS) MkdirAll(dir string, perm fs.FileMode) error {
	return c.handleless("mkdir "+filepath.Base(dir), func() error { return c.mem.MkdirAll(dir, perm) })
}

func (c *crashFS) Rename(oldname, newname string) error {
	return c.handleless("rename "+filepath.Base(newname), func() error { return c.mem.Rename(oldname, newname) })
}

func (c *crashFS) Remove(name string) error {
	return c.handleless("remove "+filepath.Base(name), func() error { return c.mem.Remove(name) })
}

type crashFile struct {
	vfs.File
	c    *crashFS
	name string
}

func (f *crashFile) Write(p []byte) (int, error) {
	die, err := f.c.step(fmt.Sprintf("write %s %dB", f.name, len(p)))
	if err != nil {
		return 0, err
	}
	if die {
		n := 0
		if f.c.torn && len(p) > 1 {
			n, _ = f.File.Write(p[:len(p)/2])
		}
		f.c.mem.CrashProcess()
		return n, errDead
	}
	return f.File.Write(p)
}

func (f *crashFile) Sync() error {
	die, err := f.c.step("fsync " + f.name)
	if err != nil {
		return err
	}
	if die {
		f.c.mem.CrashProcess()
		return errDead
	}
	return f.File.Sync()
}

func (f *crashFile) Truncate(size int64) error {
	die, err := f.c.step("truncate " + f.name)
	if err != nil {
		return err
	}
	if die {
		f.c.mem.CrashProcess()
		return errDead
	}
	return f.File.Truncate(size)
}

// matrixStep is one step of the matrix's script: an apply batch, an explicit
// Sync, or a Flush (whose MANIFEST edit is the metadata it publishes).
type matrixStep struct {
	batch *applyStep
	sync  bool
	flush bool
}

func (st matrixStep) String() string {
	switch {
	case st.sync:
		return "sync"
	case st.flush:
		return "flush"
	default:
		return fmt.Sprintf("apply %+v", st.batch.applied)
	}
}

func matrixScript() ([]matrixStep, []applyStep) {
	batches := append(applyScript(), applyStep{[]storage.Mutation{mput("e", "6"), mdel("c")}, ai(10, 3)})
	b := func(i int) matrixStep { return matrixStep{batch: &batches[i]} }
	return []matrixStep{b(0), b(1), {sync: true}, b(2), b(3), {flush: true}, b(4), b(5)}, batches
}

// crashKind is what the crash leaves on the disk.
type crashKind struct {
	name  string
	power bool
	torn  int // power loss: the most bytes past each file's last fsync that survive
}

var crashKinds = []crashKind{
	{name: "process"},
	{name: "power", power: true},
	{name: "power+torn7", power: true, torn: 7},
	{name: "power+tornall", power: true, torn: 1 << 30},
}

// matrixCell is one run of the script under a crash.
type matrixCell struct {
	mode     wal.SyncMode
	at       int  // crash at this WAL operation (0: none)
	after    int  // or crash after this many steps (when at is 0)
	torn     bool // a crashing write is torn halfway
	kind     crashKind
	manifest *fault.Injection // a failure of the flush's MANIFEST append, then a crash
}

func (c matrixCell) String() string {
	where := fmt.Sprintf("op %d", c.at)
	if c.at == 0 {
		where = fmt.Sprintf("after step %d", c.after)
	}
	if c.torn {
		where += " (torn)"
	}
	if c.manifest != nil {
		where = fmt.Sprintf("manifest %s short %d", c.manifest.Op, c.manifest.Short)
	}
	return fmt.Sprintf("%s/%s/%s", c.mode, where, c.kind.name)
}

func matrixOpts(mode wal.SyncMode, cfs *crashFS) storage.Options {
	o := lsmOpts(64 << 20) // no flush but the script's
	o.WAL.SyncMode = mode
	o.WAL.SyncInterval = time.Duration(1 << 62) // batch mode: no timer fsync — every one is the script's
	o.WAL.SyncBytes = 1 << 40
	o.WAL.SegmentSize = 96 // rotate: crashes across segment boundaries too
	o.WAL.FS = cfs
	return o
}

// runScript runs the script until the process dies and reports how many batches
// were acknowledged written (Apply returned nil), acknowledged durable (by the
// sync mode, or a Sync or Flush that returned nil since), and attempted (Apply
// was called, whether or not it succeeded). A batch whose Apply failed at its
// fsync can still be recovered after a PROCESS crash — its record reached the
// kernel — so attempted, not written, bounds what recovery may observe (§7.2).
func runScript(t *testing.T, s *storage.LSMStore, cfs *crashFS, cell matrixCell, inj *fault.InjectFS) (written, durable, attempted int) {
	steps, _ := matrixScript()
	ctx := context.Background()
	for i, st := range steps {
		if cfs.isDead() {
			return
		}
		switch {
		case st.batch != nil:
			attempted++
			if err := s.Apply(ctx, st.batch.muts, st.batch.applied); err == nil {
				written++
				if cell.mode == wal.SyncAlways {
					durable = written
				}
			}
		case st.sync:
			if err := s.Sync(); err == nil {
				durable = written
			}
		case st.flush:
			if cell.manifest != nil {
				inj.Arm(*cell.manifest)
			}
			err := s.Flush()
			if cell.manifest != nil {
				if err == nil {
					t.Fatalf("%s: premise: the flush succeeded although its MANIFEST append failed", cell)
				}
				cfs.kill()
				return
			}
			if err == nil {
				durable = written // the WAL is synced before the edit (D10)
			}
		}
		if cell.at == 0 && cell.after == i+1 {
			cfs.kill()
			return
		}
	}
	return
}

// dryRun runs the script with no crash and returns how many filesystem
// operations it performs and what each one was. Since S3 a flush rotates the WAL
// and truncates the segments below the new one, so the script spans several
// segments by construction; the operation list drives the crash iteration.
func dryRun(t *testing.T, mode wal.SyncMode) (int, []string) {
	t.Helper()
	dir := t.TempDir()
	mem := fault.NewMemFS()
	durableStoreRoot(t, mem, dir)
	cfs := &crashFS{mem: mem}
	s := openLSM(t, dir, matrixOpts(mode, cfs))
	defer s.Close()
	steps, _ := matrixScript()
	segs := map[uint64]bool{}
	for _, st := range steps {
		switch {
		case st.batch != nil:
			mustApply(t, s, *st.batch)
			segs[s.WALStats().ActiveSegment] = true
		case st.sync:
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
		case st.flush:
			mustFlush(t, s)
			segs[s.WALStats().ActiveSegment] = true
		}
	}
	if len(segs) < 2 {
		t.Fatalf("premise: the script should span WAL segments, it used %d", len(segs))
	}
	return cfs.n, append([]string(nil), cfs.ops...)
}

// durableStoreRoot makes dir a durable directory of mem, as the store's data
// directory is on the real disk: the WAL directory inside it is what S1's
// directory model is about.
func durableStoreRoot(t *testing.T, mem *fault.MemFS, dir string) {
	t.Helper()
	if err := mem.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for d := dir; filepath.Dir(d) != d; d = filepath.Dir(d) {
		if err := mem.SyncDir(filepath.Dir(d)); err != nil {
			t.Fatal(err)
		}
	}
}

// runCell runs one cell and checks it. It reports the batches recovered.
func runCell(t *testing.T, cell matrixCell, opNames []string) int {
	t.Helper()
	_, batches := matrixScript()
	want := models(batches)
	keys := keysOf(batches)
	dir := t.TempDir()
	mem := fault.NewMemFS()
	durableStoreRoot(t, mem, dir)
	cfs := &crashFS{mem: mem, at: cell.at, torn: cell.torn}
	var inj *fault.InjectFS
	if cell.manifest != nil {
		inj = injectManifest(t)
	}
	describe := func() string {
		if cell.at > 0 && cell.at <= len(opNames) {
			return fmt.Sprintf("%s [%s]", cell, opNames[cell.at-1])
		}
		return cell.String()
	}

	s, err := storage.OpenLSMStore(dir, matrixOpts(cell.mode, cfs))
	written, durable, attempted := 0, 0, 0
	if err == nil {
		written, durable, attempted = runScript(t, s, cfs, cell, inj)
		cfs.kill() // whatever is left of the script, the process dies here
		if cell.kind.power {
			mem.CrashPowerLoss(cell.kind.torn)
		}
		_ = s.Close() // the dead process's handles; nothing reaches the disk
	} else {
		// The crash came during the first open, before any batch.
		if !cfs.isDead() {
			t.Fatalf("%s: open: %v", describe(), err)
		}
		if cell.kind.power {
			mem.CrashPowerLoss(cell.kind.torn)
		}
	}
	if inj != nil {
		inj.Disarm()
	}

	// 1. The store opens, on what the crash left.
	cfs.revive(0)
	s, err = storage.OpenLSMStore(dir, matrixOpts(cell.mode, cfs))
	if err != nil {
		t.Fatalf("%s: the store does not open after the crash: %v\nops: %v", describe(), err, opNames)
	}
	// 2. R1: the recovered state is a WHOLE prefix of the script. Each batch has
	//    a distinct applied index, so the recovered applied index names the
	//    prefix; the state, both sequences and every key must then match that
	//    prefix entirely — never a batch in part. Since S3 the flushed prefix is
	//    recovered from the table and the MANIFEST watermark (its WAL segments
	//    are truncated), the rest from the surviving WAL suffix; this check does
	//    not care which medium holds which, only that the whole is a clean prefix.
	got := s.AppliedIndex()
	m := -1
	for i := range want {
		if want[i].applied == got {
			m = i
			break
		}
	}
	if m < 0 {
		t.Fatalf("%s: recovered applied index %+v is no prefix of the script\nops: %v", describe(), got, opNames)
	}
	if d := diff(s, want[m], keys); d != "" {
		t.Fatalf("%s: recovered state is not prefix %d whole (R1): %s\nops: %v", describe(), m, d, opNames)
	}
	// 3. The contract's bounds: recovery keeps at least everything acknowledged
	//    durable — a Sync, or the flush, advances `durable`; a process crash also
	//    keeps everything written, since its records reached the kernel — and
	//    never observes a batch that was never attempted (§7.2).
	floor := written
	if cell.kind.power {
		floor = durable
	}
	if m < floor || m > attempted {
		t.Fatalf("%s: recovered %d batches, outside the contract's [%d,%d] (floor..attempted)\nops: %v",
			describe(), m, floor, attempted, opNames)
	}
	// 4. Recovered implies durable: a power loss right after the open changes nothing.
	cfs.kill()
	mem.CrashPowerLoss(0)
	_ = s.Close()
	cfs.revive(0)
	s, err = storage.OpenLSMStore(dir, matrixOpts(cell.mode, cfs))
	if err != nil {
		t.Fatalf("%s: after a second, immediate power loss the store does not open: %v", describe(), err)
	}
	if d := diff(s, want[m], keys); d != "" {
		t.Fatalf("%s: what the first recovery returned was not durable — after a power loss right after it: %s", describe(), d)
	}
	// 5. The store goes on: the next batch is applied and recovered.
	next := applyStep{[]storage.Mutation{mput("after", "crash")}, ai(want[m].applied.Index+1, max(want[m].applied.Term, 1))}
	if err := s.Apply(context.Background(), next.muts, next.applied); err != nil {
		t.Fatalf("%s: the next batch after recovery: %v", describe(), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = storage.OpenLSMStore(dir, matrixOpts(cell.mode, cfs))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if d := diff(s, want[m].after(next), append(keys, "after")); d != "" {
		t.Fatalf("%s: the batch after recovery: %s", describe(), d)
	}
	return m
}

// TestApplyCrashMatrix: every WAL operation of the script — directory and
// segment creation, every append (whole or torn halfway), every fsync, every
// rotation — and every point between steps, in sync and batch mode, under a
// process crash, a power loss, and a power loss keeping a torn prefix of what
// was not fsynced; and the flush's MANIFEST append failed before, during and
// after its write. Each cell must recover exactly the batches whose records
// survived — state, applied index and sequences together (R1) — at least those
// the contract made durable, durably, and go on.
func TestApplyCrashMatrix(t *testing.T) {
	cells := 0
	byM := map[string]map[int]int{}
	for _, mode := range []wal.SyncMode{wal.SyncAlways, wal.SyncBatch} {
		nOps, opNames := dryRun(t, mode)
		steps, _ := matrixScript()
		var all []matrixCell
		for _, kind := range crashKinds {
			for at := 1; at <= nOps; at++ {
				all = append(all, matrixCell{mode: mode, at: at, kind: kind})
				if len(opNames[at-1]) > 5 && opNames[at-1][:5] == "write" {
					all = append(all, matrixCell{mode: mode, at: at, torn: true, kind: kind})
				}
			}
			for after := 1; after <= len(steps); after++ {
				all = append(all, matrixCell{mode: mode, after: after, kind: kind})
			}
			for _, inj := range []fault.Injection{
				{Op: fault.OpWrite},           // nothing of the edit is written
				{Op: fault.OpWrite, Short: 9}, // a torn edit
				{Op: fault.OpSync},            // the edit written, its fsync failed
			} {
				inj := inj
				all = append(all, matrixCell{mode: mode, kind: kind, manifest: &inj})
			}
		}
		for _, cell := range all {
			m := runCell(t, cell, opNames)
			cells++
			key := mode.String() + "/" + cell.kind.name
			if byM[key] == nil {
				byM[key] = map[int]int{}
			}
			byM[key][m]++
		}
	}
	for key, dist := range byM {
		t.Logf("%-20s batches recovered -> cells: %v", key, dist)
	}
	t.Logf("%d cells, every one R1-consistent, durable as acknowledged, and recoverable again", cells)
}
