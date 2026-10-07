package wal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/quorum/internal/fault"
	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/storage/wal"
)

// Apply batches in the WAL (S1, docs/STORAGE_INTEGRATION.md §7.3): one record
// holding a state-machine application's mutations and its applied index.

func applyBatch(index, term uint64, ops ...wal.Op) wal.ApplyBatch {
	return wal.ApplyBatch{Applied: wal.AppliedIndex{Index: index, Term: term}, Ops: ops}
}

func sameApply(a, b wal.ApplyBatch) bool {
	if a.Applied != b.Applied || len(a.Ops) != len(b.Ops) {
		return false
	}
	for i := range a.Ops {
		if a.Ops[i].Kind != b.Ops[i].Kind || !bytes.Equal(a.Ops[i].Key, b.Ops[i].Key) || !bytes.Equal(a.Ops[i].Value, b.Ops[i].Value) {
			return false
		}
	}
	return true
}

func TestApplyBatchRoundTrip(t *testing.T) {
	for _, ab := range []wal.ApplyBatch{
		applyBatch(1, 1),
		applyBatch(7, 2, put("k", "v")),
		applyBatch(1<<40, 1<<33, put("a", ""), del("b"), put("a", "second"), del("a")),
		applyBatch(3, 3, put("big", string(bytes.Repeat([]byte("x"), 70000)))),
	} {
		got, err := wal.DecodeApplyBatch(ab.AppendTo(nil))
		if err != nil {
			t.Fatalf("%+v: %v", ab.Applied, err)
		}
		if !sameApply(got, ab) {
			t.Fatalf("round trip: got %+v, want %+v", got, ab)
		}
	}
	// The layout is the documented one: version, index, term, then exactly a
	// WriteBatch's encoding of the operations.
	ab := applyBatch(0x0102, 0x03, put("k", "v"))
	enc := ab.AppendTo(nil)
	if enc[0] != 1 || binary.LittleEndian.Uint64(enc[1:9]) != 0x0102 || binary.LittleEndian.Uint64(enc[9:17]) != 0x03 ||
		!bytes.Equal(enc[17:], wal.Batch(ab.Ops).AppendTo(nil)) {
		t.Fatalf("layout: % x", enc)
	}
}

// TestDecodeApplyBatchRefusesMalformedPayloads: every way a payload can fail
// to be one this package wrote is corruption, never a guess.
func TestDecodeApplyBatchRefusesMalformedPayloads(t *testing.T) {
	good := applyBatch(5, 2, put("k", "v"), del("d")).AppendTo(nil)
	header := func(version byte, index, term uint64) []byte {
		b := []byte{version}
		b = binary.LittleEndian.AppendUint64(b, index)
		return binary.LittleEndian.AppendUint64(b, term)
	}
	cases := map[string][]byte{
		"empty":                 {},
		"short header":          good[:10],
		"unknown version":       append(header(2, 5, 2), good[17:]...),
		"zero index":            append(header(1, 0, 2), good[17:]...),
		"zero term":             append(header(1, 5, 0), good[17:]...),
		"no count":              header(1, 5, 2),
		"count past the bytes":  append(header(1, 5, 2), 0x7f, 0x01),
		"unknown operation":     append(header(1, 5, 2), 0x01, 0x09, 0x01, 'k'),
		"empty key":             append(header(1, 5, 2), 0x01, 0x00, 0x00),
		"value past the bytes":  append(header(1, 5, 2), 0x01, 0x01, 0x01, 'k', 0x05, 'v'),
		"trailing byte":         append(append([]byte(nil), good...), 0x00),
		"truncated last op":     good[:len(good)-1],
		"count 0 with an op":    append(header(1, 5, 2), 0x00, 0x00, 0x01, 'k'),
		"count 2 with only one": append(header(1, 5, 2), 0x02, 0x00, 0x01, 'k'),
	}
	for name, p := range cases {
		if _, err := wal.DecodeApplyBatch(p); !errors.Is(err, wal.ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

func TestAppendApplyRefusesAnIndexOrTermOfZero(t *testing.T) {
	w, err := wal.Create(t.TempDir(), wal.Options{SyncMode: wal.SyncOff})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, ab := range []wal.ApplyBatch{applyBatch(0, 1, put("k", "v")), applyBatch(1, 0)} {
		if err := w.AppendApply(ab); err == nil {
			t.Fatalf("AppendApply(%+v) succeeded", ab.Applied)
		}
	}
	if st := w.Stats(); st.ActiveBytes != 0 {
		t.Fatalf("a refused apply batch wrote %d bytes", st.ActiveBytes)
	}
}

// TestRecoverReplaysApplyBatchesAsUnits: replay hands each apply batch to the
// handler whole, in order, and reports the last applied index; a batch whose
// index does not advance past the one in effect before it — from either record
// kind — is refused, since the writer never produces one.
func TestRecoverReplaysApplyBatchesAsUnits(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Create(dir, wal.Options{SyncMode: wal.SyncOff})
	if err != nil {
		t.Fatal(err)
	}
	written := []wal.ApplyBatch{applyBatch(1, 1, put("a", "1")), applyBatch(2, 1), applyBatch(5, 3, del("a"), put("b", "2"))}
	for _, ab := range written {
		if err := w.AppendApply(ab); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var got []wal.ApplyBatch
	rec, err := wal.Recover(dir, wal.Handler{Apply: func(ab wal.ApplyBatch) error {
		got = append(got, wal.ApplyBatch{Applied: ab.Applied, Ops: append(wal.Batch(nil), ab.Ops...)})
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(written) || rec.ApplyBatches != 3 || rec.OpsApplied != 3 || rec.AppliedIndex != (wal.AppliedIndex{Index: 5, Term: 3}) {
		t.Fatalf("recovered %d batches, %+v", len(got), rec)
	}
	for i := range written {
		if !sameApply(got[i], written[i]) {
			t.Fatalf("batch %d: got %+v, want %+v", i, got[i], written[i])
		}
	}

	type rec_ struct {
		kind    record.Kind
		payload []byte
	}
	apply := func(i, term uint64) rec_ {
		return rec_{wal.KindApplyBatch, applyBatch(i, term, put("k", "v")).AppendTo(nil)}
	}
	legacy := func(i, term uint64) rec_ {
		return rec_{wal.KindAppliedIndex, wal.AppliedIndex{Index: i, Term: term}.AppendTo(nil)}
	}
	for name, recs := range map[string][]rec_{
		"the same index twice":            {apply(4, 1), apply(4, 1)},
		"a lower index":                   {apply(4, 1), apply(3, 1)},
		"a lower term":                    {apply(4, 2), apply(5, 1)},
		"not past a legacy applied index": {legacy(9, 1), apply(9, 1)},
	} {
		d := t.TempDir()
		var raw []struct {
			kind    record.Kind
			payload []byte
		}
		for _, r := range recs {
			raw = append(raw, struct {
				kind    record.Kind
				payload []byte
			}{r.kind, r.payload})
		}
		writeRawSegment(t, d, 1, raw...)
		before, _ := os.ReadFile(filepath.Join(d, "000001.log"))
		if _, err := wal.Recover(d, wal.Handler{}); !errors.Is(err, wal.ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
		if after, _ := os.ReadFile(filepath.Join(d, "000001.log")); !bytes.Equal(before, after) {
			t.Errorf("%s: a refused recovery changed the log", name)
		}
	}
}

// durableRoot makes dir a directory of m that a power loss keeps, as a store's
// data directory would be.
func durableRoot(t *testing.T, m *fault.MemFS, dir string) {
	t.Helper()
	if err := m.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for d := dir; filepath.Dir(d) != d; d = filepath.Dir(d) {
		if err := m.SyncDir(filepath.Dir(d)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestANewWALDirectoryIsDurable (S1, gap (a)): creating the WAL directory
// fsyncs its parent, so a record synced into the first segment survives a
// power loss — not only the segment's own entry in the WAL directory, but the
// WAL directory's entry in its parent.
func TestANewWALDirectoryIsDurable(t *testing.T) {
	m := fault.NewMemFS()
	durableRoot(t, m, "/db")
	w, err := wal.Create("/db/wal", wal.Options{SyncMode: wal.SyncAlways, FS: m})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendApply(applyBatch(1, 1, put("k", "v"))); err != nil {
		t.Fatal(err)
	}
	m.CrashPowerLoss(0)
	var got []wal.ApplyBatch
	if _, err := wal.RecoverWith("/db/wal", wal.Options{SyncMode: wal.SyncAlways, FS: m}, wal.Handler{
		Apply: func(ab wal.ApplyBatch) error { got = append(got, ab); return nil },
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("an apply batch acknowledged in sync mode did not survive a power loss: %d recovered", len(got))
	}
}

// TestRecoverSyncsTheNewestSegment (S1, gap (b)): records a killed process
// left in the page cache are fsynced before they are replayed, so what
// recovery hands the engine is durable — unless the sync mode is off, which
// never fsyncs.
func TestRecoverSyncsTheNewestSegment(t *testing.T) {
	for _, mode := range []wal.SyncMode{wal.SyncBatch, wal.SyncAlways, wal.SyncOff} {
		m := fault.NewMemFS()
		durableRoot(t, m, "/db")
		opts := wal.Options{SyncMode: mode, FS: m, SyncInterval: 1 << 40, SyncBytes: 1 << 40}
		w, err := wal.Create("/db/wal", wal.Options{SyncMode: wal.SyncOff, FS: m})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.AppendApply(applyBatch(1, 1, put("k", "v"))); err != nil {
			t.Fatal(err)
		}
		// The directory entries are durable; the record is only cached.
		if err := m.SyncDir("/db/wal"); err != nil {
			t.Fatal(err)
		}
		if err := m.SyncDir("/db"); err != nil {
			t.Fatal(err)
		}
		m.CrashProcess()
		seg := "/db/wal/000001.log"
		if m.FullySynced(seg) {
			t.Fatal("premise: the record is already durable")
		}
		rec, err := wal.RecoverWith("/db/wal", opts, wal.Handler{})
		if err != nil {
			t.Fatal(err)
		}
		if want := mode != wal.SyncOff; m.FullySynced(seg) != want || rec.SyncedNewest != want || rec.ApplyBatches != 1 {
			t.Fatalf("mode %s: after recovery the segment is synced %v (%+v), want %v", mode, m.FullySynced(seg), rec, want)
		}
	}
}
