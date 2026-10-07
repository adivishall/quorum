package fault

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/vfs"
)

// writeSynced creates name with data, fsyncs it, and makes its creation durable.
func writeSynced(t *testing.T, m *MemFS, name string, data []byte) {
	t.Helper()
	f := mustOpen(t, m, name, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := m.SyncDir(filepath.Dir(name)); err != nil {
		t.Fatal(err)
	}
}

// bound returns name's cached content and whether the name is bound at all.
func bound(t *testing.T, m *MemFS, name string) ([]byte, bool) {
	t.Helper()
	return m.Cached(name)
}

// TestRenameIsDurableOnlyAfterSyncDir pins the Phase 14 directory model the
// snapshot publication protocol relies on: rename(tmp, final) atomically
// replaces final in the cached directory — which a process crash keeps — but a
// power loss before the directory is fsynced undoes it, restoring final's
// previous file and tmp; after SyncDir the new binding survives.
func TestRenameIsDurableOnlyAfterSyncDir(t *testing.T) {
	for _, synced := range []bool{false, true} {
		m := NewMemFS()
		writeSynced(t, m, "/d/final", []byte("old"))
		writeSynced(t, m, "/d/tmp", []byte("new"))
		if err := m.Rename("/d/tmp", "/d/final"); err != nil {
			t.Fatal(err)
		}
		if b, ok := bound(t, m, "/d/final"); !ok || string(b) != "new" {
			t.Fatalf("after rename, final = %q %v", b, ok)
		}
		if _, ok := bound(t, m, "/d/tmp"); ok {
			t.Fatal("after rename, tmp still bound")
		}
		if synced {
			if err := m.SyncDir("/d"); err != nil {
				t.Fatal(err)
			}
			if !m.FullySynced("/d/final") {
				t.Fatal("a synced rename's destination must be fully synced")
			}
		} else if m.FullySynced("/d/final") {
			t.Fatal("an unsynced rename's destination must not count as fully synced")
		}
		// A process crash keeps the cached directory.
		m.CrashProcess()
		if b, _ := bound(t, m, "/d/final"); string(b) != "new" {
			t.Fatalf("a process crash undid the rename: %q", b)
		}
		m.CrashPowerLoss(0)
		b, _ := bound(t, m, "/d/final")
		tmp, tmpBound := bound(t, m, "/d/tmp")
		switch {
		case synced && (string(b) != "new" || tmpBound):
			t.Fatalf("synced rename lost by power loss: final=%q tmp bound=%v", b, tmpBound)
		case !synced && (string(b) != "old" || !tmpBound || string(tmp) != "new"):
			t.Fatalf("unsynced rename survived a power loss: final=%q tmp=%q (bound %v)", b, tmp, tmpBound)
		}
	}
}

// TestRemoveIsDurableOnlyAfterSyncDir: an unsynced removal comes back after a
// power loss; a synced one does not.
func TestRemoveIsDurableOnlyAfterSyncDir(t *testing.T) {
	for _, synced := range []bool{false, true} {
		m := NewMemFS()
		writeSynced(t, m, "/d/f", []byte("x"))
		if err := m.Remove("/d/f"); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Stat("/d/f"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("removed file still visible: %v", err)
		}
		if synced {
			_ = m.SyncDir("/d")
		}
		m.CrashPowerLoss(0)
		_, ok := bound(t, m, "/d/f")
		if ok == synced {
			t.Fatalf("synced=%v: after power loss the file is bound=%v", synced, ok)
		}
	}
	if err := NewMemFS().Remove("/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removing a missing file: %v", err)
	}
	if err := NewMemFS().Rename("/nope", "/x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("renaming a missing file: %v", err)
	}
}

// TestRenameKeepsOpenHandlesOnTheFile: a handle opened before a rename still
// refers to the renamed file (POSIX), and a rename's durability is independent
// of the file content's: an unsynced write to a renamed-and-dir-synced file is
// still lost to a power loss.
func TestRenameKeepsOpenHandlesOnTheFile(t *testing.T) {
	m := NewMemFS()
	writeSynced(t, m, "/d/a", []byte("abc"))
	f := mustOpen(t, m, "/d/a", os.O_RDWR)
	if err := m.Rename("/d/a", "/d/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("def")); err != nil {
		t.Fatal(err)
	}
	if b, _ := bound(t, m, "/d/b"); string(b) != "abcdef" {
		t.Fatalf("write through a handle opened before the rename: %q", b)
	}
	_ = m.SyncDir("/d")
	m.CrashPowerLoss(0)
	if b, _ := bound(t, m, "/d/b"); !bytes.Equal(b, []byte("abc")) {
		t.Fatalf("unsynced bytes survived a power loss: %q", b)
	}
}

// TestDurableCopyFollowsTheDurableDirectory: DurableCopy and Durable see the
// directory as a power loss would, renames included.
func TestDurableCopyFollowsTheDurableDirectory(t *testing.T) {
	m := NewMemFS()
	writeSynced(t, m, "/d/final", []byte("old"))
	writeSynced(t, m, "/d/tmp", []byte("new"))
	_ = m.Rename("/d/tmp", "/d/final")
	if b, ok := m.Durable("/d/final"); !ok || string(b) != "old" {
		t.Fatalf("durable view before SyncDir: %q %v", b, ok)
	}
	c := m.DurableCopy()
	if b, _ := c.Cached("/d/tmp"); string(b) != "new" {
		t.Fatalf("durable copy lost the unsynced rename's source: %q", b)
	}
	_ = m.SyncDir("/d")
	if b, ok := m.Durable("/d/final"); !ok || string(b) != "new" {
		t.Fatalf("durable view after SyncDir: %q %v", b, ok)
	}
	if _, ok := m.Durable("/d/tmp"); ok {
		t.Fatal("the rename's source is still durably bound after SyncDir")
	}
}

// TestInjectFSRenameAndRemove: a rename or removal can fail as injected
// (changing nothing), be observed at its exact boundary, and is recorded.
func TestInjectFSRenameAndRemove(t *testing.T) {
	m := NewMemFS()
	writeSynced(t, m, "/d/tmp", []byte("x"))
	f := NewInjectFS(m)
	f.Arm(Injection{Op: OpRename, Path: "/d/final"})
	if err := f.Rename("/d/tmp", "/d/final"); !errors.Is(err, ErrInjected) {
		t.Fatalf("armed rename: %v", err)
	}
	if _, ok := m.Cached("/d/final"); ok {
		t.Fatal("a failed rename changed the directory")
	}
	seen := false
	f.Arm(Injection{Op: OpRename, Path: "/d/final", At: func() {
		seen = true
		if _, ok := m.Cached("/d/final"); ok {
			t.Error("the observation point ran after the rename")
		}
	}})
	if err := f.Rename("/d/tmp", "/d/final"); err != nil || !seen {
		t.Fatalf("observed rename: %v (seen %v)", err, seen)
	}
	f.Arm(Injection{Op: OpRemove})
	if err := f.Remove("/d/final"); !errors.Is(err, ErrInjected) {
		t.Fatalf("armed remove: %v", err)
	}
	if err := f.Remove("/d/final"); err != nil {
		t.Fatal(err)
	}
	var ops []string
	for _, r := range f.Ops() {
		ops = append(ops, r.Op.String())
	}
	if want := "rename rename remove remove"; joinOps(ops) != want {
		t.Fatalf("op log %v, want %s", ops, want)
	}
}

func joinOps(ops []string) string {
	out := ""
	for i, o := range ops {
		if i > 0 {
			out += " "
		}
		out += o
	}
	return out
}

// TestOSRenameAndRemove: the production seam is rename(2)/unlink(2).
func TestOSRenameAndRemove(t *testing.T) {
	dir := t.TempDir()
	var fsys vfs.FS = vfs.OS{}
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	if err := os.WriteFile(a, []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Rename(a, b); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(b); string(got) != "1" {
		t.Fatalf("rename did not replace: %q", got)
	}
	if err := fsys.Remove(b); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Stat(b); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed file still there: %v", err)
	}
}

// TestCorruptFlipsBothViews: Corrupt damages a file as a crash cannot — the
// byte is flipped in what a reader sees now and in what a power loss keeps.
func TestCorruptFlipsBothViews(t *testing.T) {
	m := NewMemFS()
	f, err := m.OpenFile("/d/f", os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("/d"); err != nil {
		t.Fatal(err)
	}
	if !m.Corrupt("/d/f", 8) { // 8 mod 6 = byte 2
		t.Fatal("Corrupt reported no file")
	}
	c, _ := m.Cached("/d/f")
	d, _ := m.Durable("/d/f")
	if string(c) == "abcdef" || string(c) != string(d) || c[2] == 'c' {
		t.Fatalf("cached %q durable %q", c, d)
	}
	m.CrashPowerLoss(0)
	if after, _ := m.Cached("/d/f"); string(after) != string(c) {
		t.Fatalf("the corruption did not survive a power loss: %q", after)
	}
	if m.Corrupt("/d/missing", 0) {
		t.Fatal("Corrupt of a missing file reported success")
	}
}

// TestAMadeDirectoryIsDurableOnlyOnceItsParentIsSynced pins S1's directory
// model (docs/STORAGE_INTEGRATION.md §7.6): a directory made with MkdirAll is
// there at once and survives a process crash, but a power loss removes it —
// and every file in it, however well synced — until its parent is fsynced.
func TestAMadeDirectoryIsDurableOnlyOnceItsParentIsSynced(t *testing.T) {
	for _, parentSynced := range []bool{false, true} {
		m := NewMemFS()
		writeSynced(t, m, "/db/MANIFEST", []byte("m")) // /db exists implicitly
		if err := m.MkdirAll("/db/wal", 0o755); err != nil {
			t.Fatal(err)
		}
		writeSynced(t, m, "/db/wal/000001.log", []byte("record")) // synced, and bound in /db/wal
		if info, err := m.Stat("/db/wal"); err != nil || !info.IsDir() {
			t.Fatalf("Stat of a made directory: %v, %v", info, err)
		}
		if parentSynced {
			if err := m.SyncDir("/db"); err != nil {
				t.Fatal(err)
			}
		}
		if got := m.FullySynced("/db/wal/000001.log"); got != parentSynced {
			t.Fatalf("parent synced %v: FullySynced = %v", parentSynced, got)
		}
		m.CrashProcess()
		if _, ok := m.Cached("/db/wal/000001.log"); !ok {
			t.Fatal("a process crash removed a file in a made directory")
		}
		m.CrashPowerLoss(0)
		_, ok := m.Cached("/db/wal/000001.log")
		_, statErr := m.Stat("/db/wal")
		if ok != parentSynced || (statErr == nil) != parentSynced {
			t.Fatalf("parent synced %v: after a power loss the file survives %v, the directory stat %v",
				parentSynced, ok, statErr)
		}
		if _, ok := m.Cached("/db/MANIFEST"); !ok {
			t.Fatal("a file in an implicit directory did not survive")
		}
	}
}

// TestReadDirListsFilesAndDirectories: ReadDir lists a directory's files and
// subdirectories, made or implicit, sorted; a missing one is fs.ErrNotExist.
func TestReadDirListsFilesAndDirectories(t *testing.T) {
	m := NewMemFS()
	writeSynced(t, m, "/db/b.sst", []byte("bb"))
	writeSynced(t, m, "/db/x/deep/f", []byte("f"))
	if err := m.MkdirAll("/db/wal", 0o755); err != nil {
		t.Fatal(err)
	}
	ents, err := m.ReadDir("/db")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		got = append(got, name)
	}
	if want := []string{"b.sst", "wal/", "x/"}; !equalStrings(got, want) {
		t.Fatalf("ReadDir(/db) = %v, want %v", got, want)
	}
	if ents, err := m.ReadDir("/db/wal"); err != nil || len(ents) != 0 {
		t.Fatalf("an empty made directory: %v, %v", ents, err)
	}
	if _, err := m.ReadDir("/nowhere"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadDir of a missing directory: %v", err)
	}
	if err := m.MkdirAll("/db/b.sst/sub", 0o755); err == nil {
		t.Fatal("MkdirAll through a file succeeded")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
