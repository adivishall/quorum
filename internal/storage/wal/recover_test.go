package wal_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/adivishall/distributed-kv/internal/record"
	"github.com/adivishall/distributed-kv/internal/storage/wal"
)

// writeRawSegment writes a segment file from hand-framed records, so a test can
// produce byte patterns the WAL itself would never emit.
func writeRawSegment(t *testing.T, dir string, seg uint64, recs ...struct {
	kind    record.Kind
	payload []byte
}) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var buf []byte
	for _, r := range recs {
		var err error
		buf, err = record.Encode(buf, r.kind, r.payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, fmt.Sprintf("%06d.log", seg))
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// buildLog writes n single-put batches and returns the directory plus the byte
// offset at which each record starts, so tests can corrupt a chosen record.
func buildLog(t *testing.T, opts wal.Options, n int, valueSize int) (dir string, offsets []int64) {
	t.Helper()
	dir = testDir(t)

	w, err := wal.Create(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("v"), valueSize)
	for i := 0; i < n; i++ {
		op := wal.Op{Kind: wal.OpPut, Key: []byte(fmt.Sprintf("key%04d", i)), Value: value}
		if err := w.AppendBatch(one(op)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Recompute record offsets by scanning, so the test does not duplicate the
	// encoder's arithmetic.
	names := segments(t, dir)
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		r := record.NewReader(bytes.NewReader(data), path, int64(len(data)))
		for {
			_, _, err := r.Next()
			if err != nil {
				break
			}
			offsets = append(offsets, r.Offset())
		}
	}
	return dir, offsets
}

// assertRefuses checks that recovery refused to open and explained itself.
func assertRefuses(t *testing.T, dir string, wantSubstrings ...string) error {
	t.Helper()
	c := &collector{}
	_, err := wal.Recover(dir, c.handler())
	if err == nil {
		t.Fatalf("Recover succeeded on a corrupt log, replaying %d batches", len(c.batches))
	}
	if !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("error = %v, want errors.Is(..., ErrCorrupt)", err)
	}
	msg := err.Error()
	for _, want := range wantSubstrings {
		if !bytes.Contains([]byte(msg), []byte(want)) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
	return err
}

// ============================================================ TORN TAIL: repaired

// TestIncompleteFinalHeaderIsTruncated covers the case where a crash left fewer
// than a header's worth of bytes. The framing calls that a clean end of file,
// but the stray bytes must still be removed or the next append would begin
// after garbage.
func TestIncompleteFinalHeaderIsTruncated(t *testing.T) {
	for _, stray := range []int64{1, 4, 8} {
		t.Run(fmt.Sprintf("%d stray bytes", stray), func(t *testing.T) {
			dir, _ := buildLog(t, wal.DefaultOptions(), 5, 16)
			path := filepath.Join(dir, "000001.log")
			good := fileSize(t, path)

			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(bytes.Repeat([]byte{0xab}, int(stray))); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			c, rec := mustRecover(t, dir)
			if len(c.batches) != 5 {
				t.Fatalf("replayed %d batches, want all 5 to survive", len(c.batches))
			}
			if !rec.Truncated {
				t.Fatal("the stray bytes were left in place; the next append would start after garbage")
			}
			if rec.TruncatedAt != good {
				t.Errorf("TruncatedAt = %d, want %d", rec.TruncatedAt, good)
			}
			if got := fileSize(t, path); got != good {
				t.Errorf("file is %d bytes after recovery, want %d", got, good)
			}
		})
	}
}

// TestTruncatedFinalPayloadIsRepaired is the ordinary crash case: the process
// died partway through writing a record.
func TestTruncatedFinalPayloadIsRepaired(t *testing.T) {
	dir, offsets := buildLog(t, wal.DefaultOptions(), 6, 64)
	path := filepath.Join(dir, "000001.log")
	lastAt := offsets[len(offsets)-1]

	// Cut the final record in half.
	truncateFile(t, path, lastAt+record.HeaderSize+10)

	c, rec := mustRecover(t, dir)
	if len(c.batches) != 5 {
		t.Fatalf("replayed %d batches, want 5 (the six was torn)", len(c.batches))
	}
	if !rec.Truncated {
		t.Fatal("a torn final record was not repaired")
	}
	if rec.TruncatedAt != lastAt {
		t.Errorf("TruncatedAt = %d, want %d (the last good boundary)", rec.TruncatedAt, lastAt)
	}
	if got := fileSize(t, path); got != lastAt {
		t.Errorf("file is %d bytes after recovery, want %d", got, lastAt)
	}
	for i := 0; i < 5; i++ {
		if want := fmt.Sprintf("key%04d", i); string(c.batches[i][0].Key) != want {
			t.Errorf("batch %d key = %q, want %q", i, c.batches[i][0].Key, want)
		}
	}
}

// TestBadChecksumInFinalRecordIsRepaired: the record is complete but its
// checksum fails and nothing follows it, which is indistinguishable from an
// interrupted append.
func TestBadChecksumInFinalRecordIsRepaired(t *testing.T) {
	dir, offsets := buildLog(t, wal.DefaultOptions(), 4, 32)
	path := filepath.Join(dir, "000001.log")
	lastAt := offsets[len(offsets)-1]

	flipByte(t, path, lastAt+record.HeaderSize+3)

	c, rec := mustRecover(t, dir)
	if len(c.batches) != 3 {
		t.Fatalf("replayed %d batches, want 3", len(c.batches))
	}
	if !rec.Truncated || rec.TruncatedAt != lastAt {
		t.Fatalf("Recovery = %+v, want a truncation at %d", rec, lastAt)
	}
}

// TestRecoveryIsIdempotent: replaying a repaired log again must change nothing.
func TestRecoveryIsIdempotent(t *testing.T) {
	dir, offsets := buildLog(t, wal.DefaultOptions(), 5, 32)
	path := filepath.Join(dir, "000001.log")
	truncateFile(t, path, offsets[len(offsets)-1]+record.HeaderSize+2)

	first, firstRec := mustRecover(t, dir)
	if !firstRec.Truncated {
		t.Fatal("expected the first recovery to repair the tail")
	}

	for i := 0; i < 3; i++ {
		again, rec := mustRecover(t, dir)
		if again.flat() != first.flat() {
			t.Fatalf("recovery %d produced different state than the first", i)
		}
		if rec.Truncated {
			t.Fatalf("recovery %d truncated an already-repaired log: %+v", i, rec)
		}
	}
}

// TestAppendAfterRepairedTail: the repaired log must be appendable, and the
// result must replay as one clean sequence.
func TestAppendAfterRepairedTail(t *testing.T) {
	dir, offsets := buildLog(t, wal.DefaultOptions(), 4, 32)
	path := filepath.Join(dir, "000001.log")
	truncateFile(t, path, offsets[len(offsets)-1]+record.HeaderSize+1)

	if _, err := wal.Recover(dir, wal.Handler{}); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	w, err := wal.Create(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Create after repair: %v", err)
	}
	if err := w.AppendBatch(one(put("after-repair", "v"))); err != nil {
		t.Fatalf("AppendBatch after repair: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	c, rec := mustRecover(t, dir)
	if rec.Truncated {
		t.Fatalf("the reopened log needed repair again: %+v", rec)
	}
	if len(c.batches) != 4 {
		t.Fatalf("replayed %d batches, want 3 survivors plus 1 new", len(c.batches))
	}
	if got := string(c.batches[3][0].Key); got != "after-repair" {
		t.Fatalf("last key = %q, want %q", got, "after-repair")
	}
}

// TestTornTailInFinalSegmentOfMany: with several segments, only the newest may
// be repaired, and the older ones must replay untouched.
func TestTornTailInFinalSegmentOfMany(t *testing.T) {
	opts := wal.DefaultOptions()
	opts.SegmentSize = 512
	dir, _ := buildLog(t, opts, 60, 32)

	names := segments(t, dir)
	if len(names) < 3 {
		t.Fatalf("need several segments, got %v", names)
	}
	last := filepath.Join(dir, names[len(names)-1])

	before, _ := mustRecover(t, dir)
	total := len(before.batches)

	// Cut the newest segment mid-record.
	truncateFile(t, last, fileSize(t, last)-5)

	c, rec := mustRecover(t, dir)
	if !rec.Truncated {
		t.Fatal("the newest segment's torn tail was not repaired")
	}
	if rec.TruncatedFile != last {
		t.Errorf("TruncatedFile = %q, want %q", rec.TruncatedFile, last)
	}
	if len(c.batches) != total-1 {
		t.Fatalf("replayed %d batches, want %d (exactly one lost to the tear)", len(c.batches), total-1)
	}
	for i := range c.batches {
		if want := fmt.Sprintf("key%04d", i); string(c.batches[i][0].Key) != want {
			t.Fatalf("batch %d key = %q, want %q", i, c.batches[i][0].Key, want)
		}
	}
}

// ============================================================ CORRUPTION: refused

// TestBadChecksumInMiddleRecordIsRefused is the single most important test in
// this phase. Truncating here would discard every valid record that follows and
// the database would open looking healthy.
func TestBadChecksumInMiddleRecordIsRefused(t *testing.T) {
	dir, offsets := buildLog(t, wal.DefaultOptions(), 10, 32)
	path := filepath.Join(dir, "000001.log")
	victim := offsets[4]

	flipByte(t, path, victim+record.HeaderSize+5)
	sizeBefore := fileSize(t, path)

	err := assertRefuses(t, dir, "000001.log", fmt.Sprintf("%d", victim))
	if errors.Is(err, record.ErrTornTail) {
		t.Fatal("mid-log corruption was classified as a torn tail")
	}
	// Refusing must not have modified anything: an operator needs the bytes
	// intact in order to investigate or recover them by hand.
	if got := fileSize(t, path); got != sizeBefore {
		t.Errorf("the log was modified during a refused recovery: %d bytes, was %d", got, sizeBefore)
	}
}

// TestCorruptionInAnOlderSegmentIsRefused: even damage at the very end of an
// older segment is not a crash artifact, because that segment was completed
// before its successor was created.
func TestCorruptionInAnOlderSegmentIsRefused(t *testing.T) {
	opts := wal.DefaultOptions()
	opts.SegmentSize = 512
	dir, _ := buildLog(t, opts, 60, 32)

	names := segments(t, dir)
	if len(names) < 3 {
		t.Fatalf("need several segments, got %v", names)
	}
	older := filepath.Join(dir, names[0])

	t.Run("checksum failure mid-segment", func(t *testing.T) {
		flipByte(t, older, record.HeaderSize+2)
		assertRefuses(t, dir, names[0])
		flipByte(t, older, record.HeaderSize+2) // restore
	})

	t.Run("truncated older segment", func(t *testing.T) {
		size := fileSize(t, older)
		truncateFile(t, older, size-3)
		assertRefuses(t, dir, names[0], "not the newest segment")
	})
}

// TestTrailingBytesInOlderSegmentAreRefused: stray bytes are a repairable tail
// only in the newest segment.
func TestTrailingBytesInOlderSegmentAreRefused(t *testing.T) {
	opts := wal.DefaultOptions()
	opts.SegmentSize = 512
	dir, _ := buildLog(t, opts, 40, 32)

	names := segments(t, dir)
	if len(names) < 2 {
		t.Fatalf("need several segments, got %v", names)
	}
	older := filepath.Join(dir, names[0])

	f, err := os.OpenFile(older, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xab, 0xcd}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	assertRefuses(t, dir, names[0], "trailing bytes", "not the newest segment")
}

func TestImpossibleRecordLengthIsRefused(t *testing.T) {
	dir, offsets := buildLog(t, wal.DefaultOptions(), 4, 32)
	path := filepath.Join(dir, "000001.log")

	// Set the second record's declared length beyond MaxRecordSize.
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, offsets[1]+4); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	assertRefuses(t, dir, "exceeds maximum")
}

func TestUnknownRecordKindIsRefused(t *testing.T) {
	dir := testDir(t)
	writeRawSegment(t, dir, 1,
		struct {
			kind    record.Kind
			payload []byte
		}{wal.KindWriteBatch, one(put("k", "v")).AppendTo(nil)},
		struct {
			kind    record.Kind
			payload []byte
		}{record.Kind(0x7f), []byte("a record kind that does not exist")},
	)

	// An unknown kind is not forward compatibility; it is a record whose
	// meaning is unknown, and replaying around it produces a state missing a
	// mutation with no sign that anything is wrong.
	assertRefuses(t, dir, "unknown record kind", "0x7f")
}

func TestMalformedBatchPayloadIsRefused(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		want    string
	}{
		{"zero operation count", []byte{0x00}, "operation count is zero"},
		{"count beyond payload", []byte{0x7f, 0x01, 0x01, 'k', 0x01, 'v'}, "exceeds"},
		{"unknown operation kind", []byte{0x01, 0x7f, 0x01, 'k', 0x01, 'v'}, "unknown kind"},
		{"empty key", []byte{0x01, 0x01, 0x00, 0x01, 'v'}, "empty key"},
		{"trailing bytes", append(one(put("k", "v")).AppendTo(nil), 0x00), "unconsumed"},
		{"truncated operation", []byte{0x02, 0x01, 0x01, 'k', 0x01, 'v'}, "truncated"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := testDir(t)
			// The framing checksum is valid; only the payload is wrong. This
			// is what silent bit rot inside a record looks like after the
			// checksum has been recomputed, and what a format bug looks like.
			writeRawSegment(t, dir, 1, struct {
				kind    record.Kind
				payload []byte
			}{wal.KindWriteBatch, tc.payload})

			assertRefuses(t, dir, tc.want)
		})
	}
}

func TestMalformedAppliedIndexPayloadIsRefused(t *testing.T) {
	dir := testDir(t)
	writeRawSegment(t, dir, 1, struct {
		kind    record.Kind
		payload []byte
	}{wal.KindAppliedIndex, []byte{1, 2, 3}})

	assertRefuses(t, dir, "want exactly 16")
}

// TestZeroFilledHoleIsRefused: a hole with valid records after it is damage,
// not a tail.
func TestZeroFilledHoleIsRefused(t *testing.T) {
	dir, offsets := buildLog(t, wal.DefaultOptions(), 6, 32)
	path := filepath.Join(dir, "000001.log")

	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Zero out the third record's header and part of its payload.
	if _, err := f.WriteAt(make([]byte, 20), offsets[2]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	assertRefuses(t, dir, "zero-filled")
}

// TestHandlerErrorAborts: if the state machine refuses a record, recovery stops
// rather than continuing with a partially applied log.
func TestHandlerErrorAborts(t *testing.T) {
	dir, _ := buildLog(t, wal.DefaultOptions(), 5, 16)

	sentinel := errors.New("handler said no")
	seen := 0
	_, err := wal.Recover(dir, wal.Handler{
		Batch: func(wal.Batch) error {
			seen++
			if seen == 3 {
				return sentinel
			}
			return nil
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Recover = %v, want the handler's error", err)
	}
	if seen != 3 {
		t.Errorf("handler saw %d batches, want it to stop at 3", seen)
	}
}

// TestNilHandlerStillValidates: passing no callbacks must still decode every
// record, so that a caller using Recover purely to check a log's integrity gets
// a real answer.
func TestNilHandlerStillValidates(t *testing.T) {
	dir := testDir(t)
	writeRawSegment(t, dir, 1, struct {
		kind    record.Kind
		payload []byte
	}{wal.KindWriteBatch, []byte{0x01, 0x7f, 0x01, 'k', 0x01, 'v'}})

	if _, err := wal.Recover(dir, wal.Handler{}); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("Recover with a nil handler = %v, want ErrCorrupt", err)
	}
}
