package record_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/adivishall/quorum/internal/record"
)

// encodeAll frames a sequence of records into one buffer.
func encodeAll(t *testing.T, recs ...struct {
	kind    record.Kind
	payload []byte
}) []byte {
	t.Helper()
	var buf []byte
	for _, r := range recs {
		var err error
		buf, err = record.Encode(buf, r.kind, r.payload)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	return buf
}

func readAll(t *testing.T, data []byte) ([]record.Kind, [][]byte, error) {
	t.Helper()
	r := record.NewReader(bytes.NewReader(data), "test.log", int64(len(data)))
	var kinds []record.Kind
	var payloads [][]byte
	for {
		k, p, err := r.Next()
		if errors.Is(err, io.EOF) {
			return kinds, payloads, nil
		}
		if err != nil {
			return kinds, payloads, err
		}
		kinds = append(kinds, k)
		payloads = append(payloads, append([]byte(nil), p...))
	}
}

// ------------------------------------------------------------ round trip

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		kind    record.Kind
		payload []byte
	}{
		{"empty payload", 0x01, []byte{}},
		{"nil payload", 0x01, nil},
		{"one byte", 0x02, []byte{0xff}},
		{"text", 0x01, []byte("hello, world")},
		{"binary with NULs", 0x03, []byte{0x00, 0x01, 0x00, 0xff, 0x00}},
		{"kind zero", 0x00, []byte("kind 0 is a legal kind")},
		{"kind max", 0xff, []byte("kind 255 is a legal kind")},
		{"4 KiB", 0x01, bytes.Repeat([]byte("k"), 4<<10)},
		{"1 MiB", 0x01, bytes.Repeat([]byte("v"), 1<<20)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := encodeAll(t, struct {
				kind    record.Kind
				payload []byte
			}{tc.kind, tc.payload})

			if got, want := len(data), record.EncodedLen(len(tc.payload)); got != want {
				t.Errorf("encoded length = %d, want %d", got, want)
			}

			kinds, payloads, err := readAll(t, data)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(kinds) != 1 {
				t.Fatalf("read %d records, want 1", len(kinds))
			}
			if kinds[0] != tc.kind {
				t.Errorf("kind = %#x, want %#x", kinds[0], tc.kind)
			}
			if !bytes.Equal(payloads[0], tc.payload) && !(len(payloads[0]) == 0 && len(tc.payload) == 0) {
				t.Errorf("payload = %q, want %q", payloads[0], tc.payload)
			}
		})
	}
}

func TestRoundTripManyRecords(t *testing.T) {
	const n = 1000
	var buf []byte
	want := make([][]byte, n)
	offsets := make([]int64, n)

	for i := 0; i < n; i++ {
		offsets[i] = int64(len(buf))
		want[i] = []byte("payload-" + string(rune('a'+i%26)) + "-" + itoa(i))
		var err error
		buf, err = record.Encode(buf, record.Kind(i%256), want[i])
		if err != nil {
			t.Fatalf("Encode #%d: %v", i, err)
		}
	}

	r := record.NewReader(bytes.NewReader(buf), "many.log", int64(len(buf)))
	for i := 0; i < n; i++ {
		k, p, err := r.Next()
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		if k != record.Kind(i%256) {
			t.Fatalf("record %d: kind = %#x, want %#x", i, k, record.Kind(i%256))
		}
		if !bytes.Equal(p, want[i]) {
			t.Fatalf("record %d: payload = %q, want %q", i, p, want[i])
		}
		// Offsets must be exact: recovery truncates to them.
		if got := r.Offset(); got != offsets[i] {
			t.Fatalf("record %d: Offset() = %d, want %d", i, got, offsets[i])
		}
		wantNext := offsets[i] + int64(record.EncodedLen(len(want[i])))
		if got := r.NextOffset(); got != wantNext {
			t.Fatalf("record %d: NextOffset() = %d, want %d", i, got, wantNext)
		}
	}
	if _, _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after %d records: Next() = %v, want io.EOF", n, err)
	}
	if got := r.NextOffset(); got != int64(len(buf)) {
		t.Fatalf("final NextOffset() = %d, want %d (the whole file was consumed)", got, len(buf))
	}
}

func TestEmptyStream(t *testing.T) {
	r := record.NewReader(bytes.NewReader(nil), "empty.log", 0)
	if _, _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() on an empty stream = %v, want io.EOF", err)
	}
	if got := r.NextOffset(); got != 0 {
		t.Fatalf("NextOffset() = %d, want 0", got)
	}
}

func TestMaximumSizedRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~192 MiB")
	}
	payload := make([]byte, record.MaxRecordSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	data, err := record.Encode(nil, 0x01, payload)
	if err != nil {
		t.Fatalf("Encode(MaxRecordSize): %v", err)
	}

	r := record.NewReader(bytes.NewReader(data), "max.log", int64(len(data)))
	_, got, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("maximum-sized record did not round-trip")
	}
}

func TestEncodeRejectsOversizedPayload(t *testing.T) {
	payload := make([]byte, record.MaxRecordSize+1)
	if _, err := record.Encode(nil, 0x01, payload); !errors.Is(err, record.ErrCorrupt) {
		t.Fatalf("Encode(MaxRecordSize+1) = %v, want an error wrapping ErrCorrupt", err)
	}
}

// ------------------------------------------------------------ corruption

// assertErr checks the sentinel, the offset, and that the message names the
// file — recovery is driven by exactly these three things.
func assertErr(t *testing.T, err error, sentinel error, wantOff int64) *record.Error {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want errors.Is(..., %v)", err, sentinel)
	}
	var re *record.Error
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *record.Error; recovery cannot locate it", err)
	}
	if re.Offset != wantOff {
		t.Errorf("Offset = %d, want %d", re.Offset, wantOff)
	}
	if re.File == "" {
		t.Error("File is empty; the error does not say which file is bad")
	}
	if re.Reason == "" {
		t.Error("Reason is empty")
	}
	return re
}

func TestShortHeaderIsCleanEOF(t *testing.T) {
	// Fewer than HeaderSize trailing bytes is defined as a clean end of file
	// (docs/DESIGN.md §2). The bytes are still reported through NextOffset so
	// the caller can truncate them away before appending.
	full := encodeAll(t, struct {
		kind    record.Kind
		payload []byte
	}{0x01, []byte("complete")})

	for trailing := 1; trailing < record.HeaderSize; trailing++ {
		data := append(append([]byte(nil), full...), bytes.Repeat([]byte{0xab}, trailing)...)
		r := record.NewReader(bytes.NewReader(data), "short.log", int64(len(data)))

		if _, _, err := r.Next(); err != nil {
			t.Fatalf("%d trailing bytes: first record failed: %v", trailing, err)
		}
		if _, _, err := r.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("%d trailing bytes: Next() = %v, want io.EOF", trailing, err)
		}
		if got, want := r.NextOffset(), int64(len(full)); got != want {
			t.Fatalf("%d trailing bytes: NextOffset() = %d, want %d", trailing, got, want)
		}
		if r.NextOffset() >= int64(len(data)) {
			t.Fatalf("%d trailing bytes: the stray bytes were not reported as needing truncation", trailing)
		}
	}
}

func TestTruncatedPayloadIsTornTail(t *testing.T) {
	data := encodeAll(t,
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("first record")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("second record, cut short")},
	)
	firstLen := record.EncodedLen(len("first record"))

	// Cut the second record's payload in half.
	cut := firstLen + record.HeaderSize + 5
	data = data[:cut]

	r := record.NewReader(bytes.NewReader(data), "torn.log", int64(len(data)))
	if _, _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	_, _, err := r.Next()
	assertErr(t, err, record.ErrTornTail, int64(firstLen))
	if got, want := r.NextOffset(), int64(firstLen); got != want {
		t.Errorf("NextOffset() = %d, want %d (the last good boundary)", got, want)
	}
}

func TestBadCRCInFinalRecordIsTornTail(t *testing.T) {
	// A checksum failure on a record whose extent reaches EOF is
	// indistinguishable from an interrupted append, and is classified as a
	// tail so the caller's recovery policy can truncate it.
	data := encodeAll(t,
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("good")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("bad checksum")},
	)
	secondAt := record.EncodedLen(len("good"))

	data[len(data)-1] ^= 0xff // flip a payload bit in the final record

	r := record.NewReader(bytes.NewReader(data), "tail.log", int64(len(data)))
	if _, _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	_, _, err := r.Next()
	assertErr(t, err, record.ErrTornTail, int64(secondAt))
}

// TestBadCRCInMiddleRecordIsCorrupt is the property that matters most: damage
// to an established record must not be mistaken for a crash-torn tail, because
// truncating there would silently discard every valid record that follows.
func TestBadCRCInMiddleRecordIsCorrupt(t *testing.T) {
	data := encodeAll(t,
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("first")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("second, will be corrupted")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("third, still valid and must not be discarded")},
	)
	secondAt := record.EncodedLen(len("first"))

	// Flip a bit inside the second record's payload.
	data[secondAt+record.HeaderSize+2] ^= 0xff

	r := record.NewReader(bytes.NewReader(data), "mid.log", int64(len(data)))
	if _, _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	_, _, err := r.Next()
	re := assertErr(t, err, record.ErrCorrupt, int64(secondAt))
	if errors.Is(err, record.ErrTornTail) {
		t.Fatal("mid-log corruption was classified as a torn tail; recovery would discard valid records")
	}
	if !contains(re.Reason, "follow this record") {
		t.Errorf("Reason = %q, want it to explain that data follows the bad record", re.Reason)
	}
}

func TestCorruptedCRCFieldItselfIsDetected(t *testing.T) {
	data := encodeAll(t,
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("payload is fine, checksum field is not")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("follower")},
	)
	data[0] ^= 0xff // corrupt the stored CRC of record 0

	r := record.NewReader(bytes.NewReader(data), "crcfield.log", int64(len(data)))
	_, _, err := r.Next()
	assertErr(t, err, record.ErrCorrupt, 0)
}

func TestCorruptedKindByteIsDetected(t *testing.T) {
	// The checksum covers the kind byte, so flipping it is caught rather than
	// producing a record of a plausible-looking wrong type.
	data := encodeAll(t, struct {
		kind    record.Kind
		payload []byte
	}{0x01, []byte("body")})
	follow, err := record.Encode(data, 0x01, []byte("after"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	data = follow
	data[8] ^= 0xff // the kind byte of record 0

	r := record.NewReader(bytes.NewReader(data), "kind.log", int64(len(data)))
	_, _, err = r.Next()
	assertErr(t, err, record.ErrCorrupt, 0)
}

func TestImpossibleLengthIsCorrupt(t *testing.T) {
	// A length beyond MaxRecordSize must be rejected on the strength of the
	// length alone, before it is used to size an allocation — the checksum
	// cannot be checked until the payload has been read.
	data := encodeAll(t, struct {
		kind    record.Kind
		payload []byte
	}{0x01, []byte("small")})
	binary.LittleEndian.PutUint32(data[4:], uint32(record.MaxRecordSize+1))

	r := record.NewReader(bytes.NewReader(data), "huge.log", int64(len(data)))
	_, _, err := r.Next()
	re := assertErr(t, err, record.ErrCorrupt, 0)
	if !contains(re.Reason, "exceeds maximum") {
		t.Errorf("Reason = %q, want it to mention the maximum", re.Reason)
	}
}

func TestLengthWithinLimitButBeyondEOFIsTornTail(t *testing.T) {
	data := encodeAll(t, struct {
		kind    record.Kind
		payload []byte
	}{0x01, []byte("small")})
	binary.LittleEndian.PutUint32(data[4:], 1<<20) // legal size, but the file is tiny

	r := record.NewReader(bytes.NewReader(data), "beyond.log", int64(len(data)))
	_, _, err := r.Next()
	assertErr(t, err, record.ErrTornTail, 0)
}

func TestZeroFilledTailIsNotAcceptedAsARecord(t *testing.T) {
	// A filesystem that extends a file with zeros must not produce a
	// zero-length, zero-kind record that reads as valid.
	data := encodeAll(t, struct {
		kind    record.Kind
		payload []byte
	}{0x01, []byte("real record")})
	good := len(data)
	data = append(data, make([]byte, 64)...)

	r := record.NewReader(bytes.NewReader(data), "zeros.log", int64(len(data)))
	if _, _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	_, _, err := r.Next()
	if err == nil {
		t.Fatal("a run of zero bytes was accepted as a valid record")
	}
	assertErr(t, err, record.ErrTornTail, int64(good))
}

func TestReaderStopsAtFirstProblem(t *testing.T) {
	// There is no resynchronisation: after a corrupt record the reader must
	// not skip ahead and start returning later records as if nothing happened.
	data := encodeAll(t,
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("one")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("two")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("three")},
	)
	secondAt := record.EncodedLen(3)
	data[secondAt+record.HeaderSize] ^= 0xff

	r := record.NewReader(bytes.NewReader(data), "stop.log", int64(len(data)))
	if _, _, err := r.Next(); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, _, err := r.Next(); !errors.Is(err, record.ErrCorrupt) {
		t.Fatalf("second: %v, want ErrCorrupt", err)
	}
	// The reader must stay stuck rather than resynchronising onto record three.
	for i := 0; i < 3; i++ {
		if _, _, err := r.Next(); errors.Is(err, io.EOF) || err == nil {
			t.Fatalf("call %d after corruption returned %v; the reader resynchronised", i, err)
		}
	}
}

// ------------------------------------------------------------ helpers

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestZeroFilledRegionInMiddleIsCorrupt is the counterpart to
// TestZeroFilledTailIsNotAcceptedAsARecord. A zero-filled header is only a
// benign end-of-log when nothing follows it. With real data after it, the zeros
// are damage in the middle of the log, and truncating there would discard the
// valid records that follow.
//
// Regression test: the first implementation classified this purely on the
// record's declared extent, which reads as zero-length for an all-zero header.
// That made the answer depend on how many zero bytes happened to be present —
// exactly nine trailing zeros were recoverable, sixty-four were fatal.
func TestZeroFilledRegionInMiddleIsCorrupt(t *testing.T) {
	head := encodeAll(t, struct {
		kind    record.Kind
		payload []byte
	}{0x01, []byte("before the hole")})
	holeAt := len(head)

	data := append(append([]byte(nil), head...), make([]byte, 40)...)
	tail, err := record.Encode(data, 0x01, []byte("after the hole, still valid"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	data = tail

	r := record.NewReader(bytes.NewReader(data), "hole.log", int64(len(data)))
	if _, _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	_, _, err = r.Next()
	re := assertErr(t, err, record.ErrCorrupt, int64(holeAt))
	if errors.Is(err, record.ErrTornTail) {
		t.Fatal("a zero-filled hole with valid data after it was classified as a torn tail")
	}
	if !contains(re.Reason, "non-zero data following") {
		t.Errorf("Reason = %q, want it to explain that data follows the zeros", re.Reason)
	}
}

// TestReaderIsStickyAfterTornTail pins that the latch applies to torn tails as
// well as corruption. Recovery truncates at the reported offset; if a caller
// looped instead, it must not be handed further records.
func TestReaderIsStickyAfterTornTail(t *testing.T) {
	data := encodeAll(t,
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("one")},
		struct {
			kind    record.Kind
			payload []byte
		}{0x01, []byte("two, truncated")},
	)
	data = data[:len(data)-4]

	r := record.NewReader(bytes.NewReader(data), "sticky.log", int64(len(data)))
	if _, _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}

	_, _, want := r.Next()
	if !errors.Is(want, record.ErrTornTail) {
		t.Fatalf("second Next = %v, want ErrTornTail", want)
	}

	// Every subsequent call must return the identical latched error, not a
	// record read from beyond the failure point.
	for i := 0; i < 3; i++ {
		_, _, got := r.Next()
		if got != want {
			t.Fatalf("call %d after the tail returned %v, want the latched %v", i, got, want)
		}
	}

	if got, wantOff := r.NextOffset(), int64(record.EncodedLen(3)); got != wantOff {
		t.Errorf("NextOffset() = %d, want %d (the last good boundary)", got, wantOff)
	}
}
