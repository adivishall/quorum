package manifest_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage/manifest"
)

// ---------------------------------------------------------------- helpers

func meta(num uint64, level int, smallSeq, largeSeq uint64) manifest.FileMeta {
	return manifest.FileMeta{
		Level:       level,
		Num:         num,
		Size:        int64(num) * 1000,
		NumEntries:  num * 10,
		SmallestKey: []byte(fmt.Sprintf("small%03d", num)),
		LargestKey:  []byte(fmt.Sprintf("large%03d", num)),
		SmallestSeq: smallSeq,
		LargestSeq:  largeSeq,
	}
}

func sortedFiles(s manifest.State) []manifest.FileMeta {
	out := append([]manifest.FileMeta(nil), s.Files...)
	sort.Slice(out, func(i, j int) bool { return out[i].Num < out[j].Num })
	return out
}

// installWith writes a fresh manifest holding state, then appends extra edits.
func installWith(t *testing.T, dir string, state manifest.State, extra ...*manifest.Edit) {
	t.Helper()
	w, err := manifest.Install(dir, 1, state)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	for i, e := range extra {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func manifestPath(dir string, num uint64) string {
	return filepath.Join(dir, manifest.Name(num))
}

// ---------------------------------------------------------------- encoding

func TestEditRoundTripCarriesEveryField(t *testing.T) {
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 100))
	e.AddFile(meta(2, 1, 101, 200))
	e.DeleteFile(0, 7)
	e.DeleteFile(2, 8)
	e.SetNextFileNum(42)
	e.SetLastSequence(999)
	e.SetLogNumber(3)
	e.SetApplied(manifest.Applied{Index: 17, Term: 5})

	got, err := manifest.DecodeEdit(e.AppendTo(nil))
	if err != nil {
		t.Fatalf("DecodeEdit: %v", err)
	}
	if !reflect.DeepEqual(e, got) {
		t.Fatalf("round trip changed the edit:\n have %+v\n want %+v", got, e)
	}
}

func TestEditRoundTripWithEmptyKeys(t *testing.T) {
	// A zero-length key cannot come from the store, but the encoding must still
	// round-trip it rather than silently produce something else.
	var e manifest.Edit
	f := meta(1, 0, 1, 1)
	f.SmallestKey = nil
	f.LargestKey = []byte{}
	e.AddFile(f)

	got, err := manifest.DecodeEdit(e.AppendTo(nil))
	if err != nil {
		t.Fatalf("DecodeEdit: %v", err)
	}
	if len(got.Added) != 1 || len(got.Added[0].SmallestKey) != 0 || len(got.Added[0].LargestKey) != 0 {
		t.Fatalf("empty keys did not round-trip: %+v", got.Added)
	}
}

func TestDecodeEditRejectsMalformedPayloads(t *testing.T) {
	var good manifest.Edit
	good.AddFile(meta(1, 0, 1, 100))
	good.SetLastSequence(5)
	goodBytes := good.AppendTo(nil)

	cases := []struct {
		name    string
		payload []byte
	}{
		{"unknown tag", []byte{0x7f}},
		// 0x06 (SetApplied) is the highest tag this version knows, so 0x07 is the
		// first one a future format could add. It must be refused, not skipped.
		{"tag just past the known range", []byte{0x07}},
		{"truncated after a tag", []byte{0x01}},
		{"truncated mid-record", goodBytes[:len(goodBytes)-3]},
		{"truncated at one byte", goodBytes[:1]},
		{"key length overruns the payload", func() []byte {
			// AddFile, level 0, num 1, size 1, entries 1, then a key claiming
			// 200 bytes with nothing following.
			p := []byte{0x01, 0x00, 0x01, 0x01, 0x01}
			return append(p, 200)
		}()},
		{"inverted sequence range", func() []byte {
			var e manifest.Edit
			f := meta(1, 0, 100, 1) // smallest > largest
			e.AddFile(f)
			return e.AppendTo(nil)
		}()},
		{"level absurd", func() []byte {
			p := []byte{0x01}
			return append(p, binary.AppendUvarint(nil, 5000)...)
		}()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := manifest.DecodeEdit(tc.payload)
			if err == nil {
				t.Fatal("DecodeEdit accepted a malformed payload; a lenient decoder here " +
					"turns damage into a plausible-looking file set")
			}
			if !errors.Is(err, manifest.ErrCorrupt) {
				t.Fatalf("DecodeEdit = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestDecodeEditOfAnEmptyPayloadIsAnEmptyEdit(t *testing.T) {
	// An empty payload decodes to an edit that changes nothing. Recover never
	// sees one, because Append refuses to write it, but the decoder must be total.
	e, err := manifest.DecodeEdit(nil)
	if err != nil {
		t.Fatalf("DecodeEdit(nil) = %v", err)
	}
	if !e.Empty() {
		t.Fatalf("DecodeEdit(nil) produced %+v, want an empty edit", e)
	}
}

func TestEditEncodingIsDeterministic(t *testing.T) {
	build := func() []byte {
		var e manifest.Edit
		e.AddFile(meta(3, 1, 1, 50))
		e.AddFile(meta(1, 0, 51, 60))
		e.DeleteFile(0, 9)
		e.SetLastSequence(60)
		return e.AppendTo(nil)
	}
	first := build()
	for i := 0; i < 3; i++ {
		if !bytes.Equal(first, build()) {
			t.Fatal("encoding the same edit twice produced different bytes")
		}
	}
}

// ---------------------------------------------------------------- state

func TestApplyAddsAndDeletes(t *testing.T) {
	var s manifest.State

	var e1 manifest.Edit
	e1.AddFile(meta(1, 0, 1, 10))
	e1.AddFile(meta(2, 0, 11, 20))
	e1.SetNextFileNum(3)
	e1.SetLastSequence(20)
	if err := s.Apply(e1); err != nil {
		t.Fatal(err)
	}
	if len(s.Files) != 2 || s.NextFileNum != 3 || s.LastSequence != 20 {
		t.Fatalf("state after the first edit = %+v", s)
	}

	// A compaction: one output replaces both inputs.
	var e2 manifest.Edit
	e2.AddFile(meta(3, 1, 1, 20))
	e2.DeleteFile(0, 1)
	e2.DeleteFile(0, 2)
	e2.SetNextFileNum(4)
	if err := s.Apply(e2); err != nil {
		t.Fatal(err)
	}
	if len(s.Files) != 1 || s.Files[0].Num != 3 {
		t.Fatalf("state after the compaction edit = %+v", sortedFiles(s))
	}
}

func TestApplyRejectsIncoherentEdits(t *testing.T) {
	t.Run("deleting a file that is not live", func(t *testing.T) {
		var s manifest.State
		var e manifest.Edit
		e.DeleteFile(0, 99)
		if err := s.Apply(e); !errors.Is(err, manifest.ErrCorrupt) {
			t.Fatalf("Apply = %v, want ErrCorrupt", err)
		}
	})
	t.Run("adding a file that is already live", func(t *testing.T) {
		var s manifest.State
		var e1 manifest.Edit
		e1.AddFile(meta(1, 0, 1, 10))
		if err := s.Apply(e1); err != nil {
			t.Fatal(err)
		}
		var e2 manifest.Edit
		e2.AddFile(meta(1, 0, 11, 20))
		if err := s.Apply(e2); !errors.Is(err, manifest.ErrCorrupt) {
			t.Fatalf("Apply = %v, want ErrCorrupt", err)
		}
	})
}

func TestApplyDeletesBeforeAddsSoANumberCanBeReused(t *testing.T) {
	var s manifest.State
	var e1 manifest.Edit
	e1.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e1); err != nil {
		t.Fatal(err)
	}
	var e2 manifest.Edit
	e2.AddFile(meta(1, 1, 1, 10))
	e2.DeleteFile(0, 1)
	if err := s.Apply(e2); err != nil {
		t.Fatalf("Apply: %v (deletions must be applied before additions)", err)
	}
	if len(s.Files) != 1 || s.Files[0].Level != 1 {
		t.Fatalf("state = %+v", s.Files)
	}
}

func TestSnapshotReproducesState(t *testing.T) {
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	e.AddFile(meta(2, 1, 11, 20))
	e.AddFile(meta(5, 2, 21, 30))
	e.SetNextFileNum(6)
	e.SetLastSequence(30)
	e.SetLogNumber(2)
	e.SetApplied(manifest.Applied{Index: 3, Term: 1})
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}

	var rebuilt manifest.State
	if err := rebuilt.Apply(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedFiles(s), sortedFiles(rebuilt)) {
		t.Fatalf("snapshot did not reproduce the file set:\n have %+v\n want %+v",
			sortedFiles(rebuilt), sortedFiles(s))
	}
	if rebuilt.NextFileNum != s.NextFileNum || rebuilt.LastSequence != s.LastSequence ||
		rebuilt.LogNumber != s.LogNumber || rebuilt.Applied != s.Applied {
		t.Fatalf("snapshot lost a scalar: %+v vs %+v", rebuilt, s)
	}
}

func TestCloneIsDeep(t *testing.T) {
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	c := s.Clone()
	c.Files[0].SmallestKey[0] = 'X'
	if s.Files[0].SmallestKey[0] == 'X' {
		t.Fatal("Clone shares key memory with the original")
	}
}

// ---------------------------------------------------------------- install / recover

func TestInstallAndRecoverRoundTrip(t *testing.T) {
	dir := t.TempDir()

	var want manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	e.AddFile(meta(2, 1, 11, 20))
	e.SetNextFileNum(3)
	e.SetLastSequence(20)
	e.SetApplied(manifest.Applied{Index: 4, Term: 2})
	if err := want.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, want)

	got, rec, err := manifest.Recover(dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Num != 1 || rec.EditsApplied != 1 || rec.Truncated {
		t.Fatalf("recovery = %+v, want one edit from manifest 1 with no truncation", rec)
	}
	if !reflect.DeepEqual(sortedFiles(want), sortedFiles(got)) {
		t.Fatalf("recovered file set:\n have %+v\n want %+v", sortedFiles(got), sortedFiles(want))
	}
	if got.NextFileNum != 3 || got.LastSequence != 20 || got.Applied != (manifest.Applied{Index: 4, Term: 2}) {
		t.Fatalf("recovered scalars = %+v", got)
	}
}

func TestRecoverReplaysEveryEdit(t *testing.T) {
	dir := t.TempDir()

	var initial manifest.State
	var e0 manifest.Edit
	e0.AddFile(meta(1, 0, 1, 10))
	e0.SetNextFileNum(2)
	if err := initial.Apply(e0); err != nil {
		t.Fatal(err)
	}

	// Two flushes then a compaction that retires all three L0 files.
	var e1, e2, e3 manifest.Edit
	e1.AddFile(meta(2, 0, 11, 20))
	e1.SetNextFileNum(3)
	e2.AddFile(meta(3, 0, 21, 30))
	e2.SetNextFileNum(4)
	e3.AddFile(meta(4, 1, 1, 30))
	e3.DeleteFile(0, 1)
	e3.DeleteFile(0, 2)
	e3.DeleteFile(0, 3)
	e3.SetNextFileNum(5)
	e3.SetLastSequence(30)

	installWith(t, dir, initial, &e1, &e2, &e3)

	got, rec, err := manifest.Recover(dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.EditsApplied != 4 {
		t.Fatalf("EditsApplied = %d, want 4 (snapshot + 3)", rec.EditsApplied)
	}
	if len(got.Files) != 1 || got.Files[0].Num != 4 || got.Files[0].Level != 1 {
		t.Fatalf("recovered file set = %+v, want only file 4 at level 1", sortedFiles(got))
	}
	if got.NextFileNum != 5 || got.LastSequence != 30 {
		t.Fatalf("recovered scalars = %+v", got)
	}
}

func TestRecoverWithNoCurrentIsErrNoManifest(t *testing.T) {
	_, _, err := manifest.Recover(t.TempDir())
	if !errors.Is(err, manifest.ErrNoManifest) {
		t.Fatalf("Recover on an empty directory = %v, want ErrNoManifest", err)
	}
}

func TestInstallIsAtomicallyVisibleViaCurrent(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	raw, err := os.ReadFile(filepath.Join(dir, manifest.CurrentName))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); got != manifest.Name(1)+"\n" {
		t.Fatalf("CURRENT holds %q, want %q", got, manifest.Name(1)+"\n")
	}
	// No staging file may survive a completed install.
	if _, err := os.Stat(filepath.Join(dir, "CURRENT.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("CURRENT.tmp survived a completed install")
	}
}

// ---------------------------------------------------------------- corruption

func TestCurrentNamingAMissingManifestIsRefused(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	if err := os.Remove(manifestPath(dir, 1)); err != nil {
		t.Fatal(err)
	}
	_, _, err := manifest.Recover(dir)
	if !errors.Is(err, manifest.ErrCorrupt) {
		t.Fatalf("Recover = %v, want ErrCorrupt: CURRENT is only ever pointed at a "+
			"manifest that was already fsynced, so its absence is not a state the "+
			"protocol can produce", err)
	}
}

func TestGarbageInCurrentIsRefused(t *testing.T) {
	for _, content := range []string{"", "\n", "hello", "MANIFEST-", "MANIFEST-abc123",
		"MANIFEST-1", "MANIFEST-0000001", "../../etc/passwd", "000001.sst"} {
		t.Run(fmt.Sprintf("%q", content), func(t *testing.T) {
			dir := t.TempDir()
			var s manifest.State
			var e manifest.Edit
			e.AddFile(meta(1, 0, 1, 10))
			if err := s.Apply(e); err != nil {
				t.Fatal(err)
			}
			installWith(t, dir, s)

			if err := os.WriteFile(filepath.Join(dir, manifest.CurrentName),
				[]byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, _, err := manifest.Recover(dir); !errors.Is(err, manifest.ErrCorrupt) {
				t.Fatalf("Recover with CURRENT = %q gave %v, want ErrCorrupt", content, err)
			}
		})
	}
}

func TestEmptyManifestIsRefused(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	if err := os.Truncate(manifestPath(dir, 1), 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manifest.Recover(dir); !errors.Is(err, manifest.ErrCorrupt) {
		t.Fatalf("Recover on an empty manifest = %v, want ErrCorrupt: a manifest always "+
			"begins with a snapshot, so an empty one was replaced or emptied", err)
	}
}

// TestTornFinalRecordIsRepaired is the one damage a crash genuinely explains: a
// partial append of the last edit. That edit was never fsynced, so it never took
// effect, and the state from the preceding edits is correct.
func TestTornFinalRecordIsRepaired(t *testing.T) {
	dir := t.TempDir()

	var s manifest.State
	var e0 manifest.Edit
	e0.AddFile(meta(1, 0, 1, 10))
	e0.SetNextFileNum(2)
	if err := s.Apply(e0); err != nil {
		t.Fatal(err)
	}
	var e1 manifest.Edit
	e1.AddFile(meta(2, 0, 11, 20))
	e1.SetNextFileNum(3)
	installWith(t, dir, s, &e1)

	path := manifestPath(dir, 1)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	full := info.Size()

	// Cut the file a few bytes into the final record.
	if err := os.Truncate(path, full-4); err != nil {
		t.Fatal(err)
	}

	got, rec, err := manifest.Recover(dir)
	if err != nil {
		t.Fatalf("Recover on a torn tail = %v, want a repair", err)
	}
	if !rec.Truncated {
		t.Fatal("recovery did not report the torn tail")
	}
	// The truncated edit did not take effect: file 2 must not be live.
	if len(got.Files) != 1 || got.Files[0].Num != 1 {
		t.Fatalf("recovered file set = %+v, want only file 1: the torn edit never happened",
			sortedFiles(got))
	}
	if got.NextFileNum != 2 {
		t.Fatalf("NextFileNum = %d, want 2", got.NextFileNum)
	}

	// The file was repaired on disk, so the next append cannot follow garbage.
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != rec.TruncatedAt {
		t.Fatalf("manifest is %d bytes after repair, want %d", after.Size(), rec.TruncatedAt)
	}

	// And recovery is now stable and repeatable.
	again, rec2, err := manifest.Recover(dir)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	if rec2.Truncated {
		t.Fatal("the second recovery still reports a torn tail; the repair did not stick")
	}
	if !reflect.DeepEqual(sortedFiles(got), sortedFiles(again)) {
		t.Fatal("two recoveries of the same directory disagree")
	}
}

// TestDamageInTheMiddleIsRefused is the other half of the policy. A bad record
// with valid records after it cannot be a crash, and skipping it would apply a
// version history that never happened.
func TestDamageInTheMiddleIsRefused(t *testing.T) {
	dir := t.TempDir()

	var s manifest.State
	var e0 manifest.Edit
	e0.AddFile(meta(1, 0, 1, 10))
	e0.SetNextFileNum(2)
	if err := s.Apply(e0); err != nil {
		t.Fatal(err)
	}
	var e1, e2 manifest.Edit
	e1.AddFile(meta(2, 0, 11, 20))
	e1.SetNextFileNum(3)
	e2.AddFile(meta(3, 0, 21, 30))
	e2.SetNextFileNum(4)
	installWith(t, dir, s, &e1, &e2)

	path := manifestPath(dir, 1)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the first record's payload. Records follow it, so this
	// is not a torn tail.
	damaged := append([]byte(nil), raw...)
	damaged[record.HeaderSize+2] ^= 0xff
	if err := os.WriteFile(path, damaged, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := manifest.Recover(dir); !errors.Is(err, manifest.ErrCorrupt) {
		t.Fatalf("Recover with mid-file damage = %v, want ErrCorrupt", err)
	}
	// And it did not rewrite the file.
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(damaged, after) {
		t.Fatal("a refused recovery modified the manifest; the bytes must stay available to investigate")
	}
}

func TestUnknownRecordKindIsRefused(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	// Append a well-framed record of an unknown kind.
	buf, err := record.Encode(nil, record.Kind(0x7e), []byte("whatever"))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(manifestPath(dir, 1), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := manifest.Recover(dir); !errors.Is(err, manifest.ErrCorrupt) {
		t.Fatalf("Recover with an unknown record kind = %v, want ErrCorrupt", err)
	}
}

func TestMalformedEditPayloadIsRefused(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	// A correctly framed record whose payload is not a decodable edit.
	buf, err := record.Encode(nil, manifest.KindVersionEdit, []byte{0x7f, 0x7f, 0x7f})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(manifestPath(dir, 1), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := manifest.Recover(dir); !errors.Is(err, manifest.ErrCorrupt) {
		t.Fatalf("Recover with a malformed edit = %v, want ErrCorrupt", err)
	}
}

// TestIncoherentHistoryIsRefused: a manifest whose edits do not compose — here,
// deleting a file that was never added — is damage, not something to tolerate.
func TestIncoherentHistoryIsRefused(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	var bad manifest.Edit
	bad.DeleteFile(0, 999)
	buf, err := record.Encode(nil, manifest.KindVersionEdit, bad.AppendTo(nil))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(manifestPath(dir, 1), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := manifest.Recover(dir); !errors.Is(err, manifest.ErrCorrupt) {
		t.Fatalf("Recover = %v, want ErrCorrupt", err)
	}
}

// ---------------------------------------------------------------- lifecycle

func TestAppendRefusesAnEmptyEdit(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	w, err := manifest.Install(dir, 1, s)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	var empty manifest.Edit
	if err := w.Append(&empty); err == nil {
		t.Fatal("Append accepted an edit that changes nothing")
	}
}

func TestReopenAppendsAfterTheLastRecord(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e0 manifest.Edit
	e0.AddFile(meta(1, 0, 1, 10))
	e0.SetNextFileNum(2)
	if err := s.Apply(e0); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	_, rec, err := manifest.Recover(dir)
	if err != nil {
		t.Fatal(err)
	}

	w, err := manifest.Reopen(dir, rec.Num, rec.EndOffset)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	var e1 manifest.Edit
	e1.AddFile(meta(2, 0, 11, 20))
	e1.SetNextFileNum(3)
	if err := w.Append(&e1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, rec2, err := manifest.Recover(dir)
	if err != nil {
		t.Fatalf("Recover after Reopen+Append: %v", err)
	}
	if rec2.EditsApplied != 2 {
		t.Fatalf("EditsApplied = %d, want 2", rec2.EditsApplied)
	}
	if len(got.Files) != 2 {
		t.Fatalf("recovered %d files, want 2", len(got.Files))
	}
}

func TestInstallRefusesToOverwriteAnExistingManifest(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	if _, err := manifest.Install(dir, 1, s); err == nil {
		t.Fatal("Install overwrote an existing manifest")
	}
}

func TestRemoveObsoleteKeepsOnlyTheLiveManifest(t *testing.T) {
	dir := t.TempDir()
	var s manifest.State
	var e manifest.Edit
	e.AddFile(meta(1, 0, 1, 10))
	if err := s.Apply(e); err != nil {
		t.Fatal(err)
	}
	installWith(t, dir, s)

	// Two leftovers: a superseded manifest and one an interrupted Install left.
	for _, n := range []uint64{2, 7} {
		if err := os.WriteFile(manifestPath(dir, n), []byte("stale"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "CURRENT.tmp"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	removed, err := manifest.RemoveObsolete(dir, 1)
	if err != nil {
		t.Fatalf("RemoveObsolete: %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed %d manifests, want 2", removed)
	}
	nums, err := manifest.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(nums) != 1 || nums[0] != 1 {
		t.Fatalf("manifests remaining = %v, want [1]", nums)
	}
	if _, err := os.Stat(filepath.Join(dir, "CURRENT.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("CURRENT.tmp survived RemoveObsolete")
	}
	// And the live manifest still recovers.
	if _, _, err := manifest.Recover(dir); err != nil {
		t.Fatalf("Recover after RemoveObsolete: %v", err)
	}
}

func TestNameAndParseName(t *testing.T) {
	if got := manifest.Name(1); got != "MANIFEST-000001" {
		t.Fatalf("Name(1) = %q", got)
	}
	if got := manifest.Name(999999); got != "MANIFEST-999999" {
		t.Fatalf("Name(999999) = %q", got)
	}
	for _, ok := range []string{"MANIFEST-000001", "MANIFEST-123456"} {
		if _, valid := manifest.ParseName(ok); !valid {
			t.Errorf("ParseName(%q) rejected a valid name", ok)
		}
	}
	for _, bad := range []string{
		"MANIFEST", "MANIFEST-", "MANIFEST-1", "MANIFEST-0000001", "MANIFEST-abcdef",
		"manifest-000001", "CURRENT", "000001.sst", "MANIFEST-000001.tmp", "",
	} {
		if _, valid := manifest.ParseName(bad); valid {
			t.Errorf("ParseName(%q) accepted an invalid name", bad)
		}
	}
}

func TestStrayFilesAreNotMistakenForManifests(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		".DS_Store", "notes.txt", "MANIFEST", "MANIFEST-", "MANIFEST-1",
		"MANIFEST-0000001", "manifest-000001", "000001.sst", "CURRENT",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("junk"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	nums, err := manifest.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(nums) != 0 {
		t.Fatalf("List found %v; none of these are manifest names", nums)
	}
}

// ---------------------------------------------------------------- fuzz

// FuzzEditRoundTrip is the property the format has to hold: whatever this package
// encodes, it decodes back to the same edit.
func FuzzEditRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(2), uint64(3), []byte("small"), []byte("large"))
	f.Add(uint64(0), uint64(0), uint64(0), []byte(""), []byte(""))

	f.Fuzz(func(t *testing.T, num, smallSeq, largeSeq uint64, small, large []byte) {
		if smallSeq > largeSeq {
			smallSeq, largeSeq = largeSeq, smallSeq
		}
		if len(small) > 4096 || len(large) > 4096 {
			t.Skip()
		}
		var e manifest.Edit
		e.AddFile(manifest.FileMeta{
			Level: 1, Num: num, Size: 10, NumEntries: 2,
			SmallestKey: small, LargestKey: large,
			SmallestSeq: smallSeq, LargestSeq: largeSeq,
		})
		e.SetLastSequence(largeSeq)

		got, err := manifest.DecodeEdit(e.AppendTo(nil))
		if err != nil {
			t.Fatalf("an edit this package encoded failed to decode: %v", err)
		}
		if len(got.Added) != 1 {
			t.Fatalf("got %d added files, want 1", len(got.Added))
		}
		a := got.Added[0]
		if a.Num != num || a.SmallestSeq != smallSeq || a.LargestSeq != largeSeq {
			t.Fatalf("scalars changed: %+v", a)
		}
		if !bytes.Equal(a.SmallestKey, small) || !bytes.Equal(a.LargestKey, large) {
			t.Fatalf("keys changed: %q/%q want %q/%q", a.SmallestKey, a.LargestKey, small, large)
		}
	})
}

// FuzzDecodeEditIsTotal requires DecodeEdit to fail cleanly rather than panic on
// arbitrary bytes, because its input comes off disk.
func FuzzDecodeEditIsTotal(f *testing.F) {
	f.Add([]byte{0x01, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x01, 0x01})
	f.Add([]byte{0x7f})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, payload []byte) {
		e, err := manifest.DecodeEdit(payload)
		if err != nil {
			if !errors.Is(err, manifest.ErrCorrupt) {
				t.Fatalf("DecodeEdit returned %v, which is not ErrCorrupt", err)
			}
			return
		}
		// Anything that decodes must re-encode and decode again to the same thing.
		again, err := manifest.DecodeEdit(e.AppendTo(nil))
		if err != nil {
			t.Fatalf("re-encoding a decoded edit produced something undecodable: %v", err)
		}
		if !reflect.DeepEqual(e, again) {
			t.Fatalf("decode/encode/decode is not stable:\n %+v\n %+v", e, again)
		}
		// And applying it to an empty state must not panic.
		var s manifest.State
		_ = s.Apply(e)
	})
}
