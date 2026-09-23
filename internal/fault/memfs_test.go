package fault

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"

	"github.com/adivishall/quorum/internal/vfs"
)

func mustOpen(t *testing.T, m *MemFS, name string, flag int) vfs.File {
	t.Helper()
	f, err := m.OpenFile(name, flag, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	return f
}

func mustWrite(t *testing.T, f vfs.File, s string) {
	t.Helper()
	if n, err := f.Write([]byte(s)); err != nil || n != len(s) {
		t.Fatalf("write %q: n=%d err=%v", s, n, err)
	}
}

func cached(t *testing.T, m *MemFS, name string) string {
	t.Helper()
	b, ok := m.Cached(name)
	if !ok {
		t.Fatalf("%s does not exist", name)
	}
	return string(b)
}

// TestProcessCrashKeepsEveryWrittenByte proves the process-crash model: bytes a
// Write accepted survive whether or not they were fsynced (the kernel outlives
// the process), and every handle of the crashed process is dead afterwards.
func TestProcessCrashKeepsEveryWrittenByte(t *testing.T) {
	m := NewMemFS()
	f := mustOpen(t, m, "/d/log", os.O_RDWR|os.O_CREATE)
	mustWrite(t, f, "synced")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, "+unsynced")

	m.CrashProcess()

	if got := cached(t, m, "/d/log"); got != "synced+unsynced" {
		t.Fatalf("after process crash cached = %q, want every written byte", got)
	}
	if _, err := f.Write([]byte("x")); !errors.Is(err, ErrCrashed) {
		t.Fatalf("write on a crashed handle: err = %v, want ErrCrashed", err)
	}
	if err := f.Sync(); !errors.Is(err, ErrCrashed) {
		t.Fatalf("sync on a crashed handle: err = %v, want ErrCrashed", err)
	}
}

// TestPowerLossKeepsOnlySyncedBytes proves the power-loss model: only the durable
// view survives, and a torn tail keeps at most a prefix of the un-synced bytes.
func TestPowerLossKeepsOnlySyncedBytes(t *testing.T) {
	for _, tc := range []struct {
		torn int
		want string
	}{
		{0, "synced"},
		{3, "synced+un"},
		{100, "synced+unsynced"}, // clamped to what was written
	} {
		m := NewMemFS()
		f := mustOpen(t, m, "/d/log", os.O_RDWR|os.O_CREATE)
		if err := m.SyncDir("/d"); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, f, "synced")
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, f, "+unsynced")
		if m.FullySynced("/d/log") {
			t.Fatal("FullySynced reported true with un-synced bytes pending")
		}

		m.CrashPowerLoss(tc.torn)

		if got := cached(t, m, "/d/log"); got != tc.want {
			t.Fatalf("torn=%d: after power loss = %q, want %q", tc.torn, got, tc.want)
		}
		if !m.FullySynced("/d/log") {
			t.Fatalf("torn=%d: what survived a power loss must itself be durable", tc.torn)
		}
		if _, err := f.Read(make([]byte, 1)); !errors.Is(err, ErrCrashed) {
			t.Fatalf("read on a pre-crash handle: err = %v, want ErrCrashed", err)
		}
	}
}

// TestFileCreationNeedsSyncDir proves a new file's existence is durable only once
// its directory was synced: fsyncing the file alone does not save it from a power
// loss, exactly as on a real filesystem.
func TestFileCreationNeedsSyncDir(t *testing.T) {
	m := NewMemFS()
	f := mustOpen(t, m, "/d/new", os.O_RDWR|os.O_CREATE)
	mustWrite(t, f, "data")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if m.FullySynced("/d/new") {
		t.Fatal("FullySynced true before the directory entry was synced")
	}
	m.CrashPowerLoss(0)
	if _, err := m.Stat("/d/new"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("un-dir-synced file survived a power loss: err = %v", err)
	}

	// With SyncDir, the same sequence survives.
	f = mustOpen(t, m, "/d/new", os.O_RDWR|os.O_CREATE)
	if err := m.SyncDir("/d"); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, "data")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	m.CrashPowerLoss(0)
	if got := cached(t, m, "/d/new"); got != "data" {
		t.Fatalf("after SyncDir+Sync, power loss left %q, want data", got)
	}
}

// TestOverwriteAndTruncateInsideSyncedRegion proves the durable view is protected
// from mutations below the synced length until the next Sync: a power loss
// restores exactly the last synced bytes, not the partially mutated cache.
func TestOverwriteAndTruncateInsideSyncedRegion(t *testing.T) {
	m := NewMemFS()
	f := mustOpen(t, m, "/d/f", os.O_RDWR|os.O_CREATE)
	_ = m.SyncDir("/d")
	mustWrite(t, f, "abcdef")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(2); err != nil { // below the synced length
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, "XY")
	if got := cached(t, m, "/d/f"); got != "abXY" {
		t.Fatalf("cached = %q, want abXY", got)
	}
	if d, _ := m.Durable("/d/f"); string(d) != "abcdef" {
		t.Fatalf("durable view = %q, want the last synced abcdef", d)
	}
	m.CrashPowerLoss(100) // a torn tail does not apply to a detached (non-append) file
	if got := cached(t, m, "/d/f"); got != "abcdef" {
		t.Fatalf("after power loss = %q, want abcdef", got)
	}
}

// TestMemFSReadSeekStatSemantics proves the file behaves like an os.File for the
// operations a durable log uses: sequential reads to EOF, seeking, size.
func TestMemFSReadSeekStatSemantics(t *testing.T) {
	m := NewMemFS()
	f := mustOpen(t, m, "/d/f", os.O_RDWR|os.O_CREATE)
	mustWrite(t, f, "hello world")
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(f)
	if err != nil || string(all) != "hello world" {
		t.Fatalf("ReadAll = %q, %v", all, err)
	}
	if off, err := f.Seek(-5, io.SeekEnd); err != nil || off != 6 {
		t.Fatalf("Seek(-5,End) = %d, %v", off, err)
	}
	info, err := f.Stat()
	if err != nil || info.Size() != 11 {
		t.Fatalf("Stat size = %v, %v", info, err)
	}
	if _, err := m.OpenFile("/d/missing", os.O_RDONLY, 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open missing: %v, want ErrNotExist", err)
	}
	if _, err := m.OpenFile("/d/f", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("O_EXCL on existing: %v, want ErrExist", err)
	}
	ro := mustOpen(t, m, "/d/f", os.O_RDONLY)
	if _, err := ro.Write([]byte("x")); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("write on read-only handle: %v, want ErrPermission", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("double close: %v, want ErrClosed", err)
	}
}

// TestMemFSIsDeterministic proves two identical operation sequences, including a
// power loss with a torn tail, produce byte-identical filesystems.
func TestMemFSIsDeterministic(t *testing.T) {
	run := func() []byte {
		m := NewMemFS()
		f := mustOpen(t, m, "/d/a", os.O_RDWR|os.O_CREATE)
		_ = m.SyncDir("/d")
		for i := 0; i < 50; i++ {
			mustWrite(t, f, string(rune('a'+i%26)))
			if i%7 == 0 {
				_ = f.Sync()
			}
		}
		m.CrashPowerLoss(3)
		b, _ := m.Cached("/d/a")
		return b
	}
	if a, b := run(), run(); !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic: %q vs %q", a, b)
	}
}
