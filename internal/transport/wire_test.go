package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/record"
)

// chunkWriter is a deliberately hostile io.Writer for the partial-write tests. It
// accepts at most perWrite bytes per call (forcing a single frame to take several
// Write calls), can fail after a threshold, and can make zero progress with a nil
// error to prove writeFull does not spin forever.
type chunkWriter struct {
	buf      bytes.Buffer
	perWrite int   // max bytes accepted per Write (0 = unlimited)
	failAt   int   // if failErr != nil, return it once written >= failAt
	failErr  error // error to return after failAt bytes
	zero     bool  // if true, every Write returns (0, nil)
	written  int
	calls    int
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.zero {
		return 0, nil
	}
	n := len(p)
	if w.perWrite > 0 && n > w.perWrite {
		n = w.perWrite
	}
	w.buf.Write(p[:n])
	w.written += n
	if w.failErr != nil && w.written >= w.failAt {
		return n, w.failErr
	}
	return n, nil
}

// TestFrameSurvivesPartialWrites proves writeFrame writes a whole frame even when
// the writer accepts only a few bytes per call, and that the reassembled bytes
// decode back to the exact message (strengthens INV-T5 / stream correctness).
func TestFrameSurvivesPartialWrites(t *testing.T) {
	payload := []byte("a frame long enough to require many Write calls to complete")
	for _, per := range []int{1, 3, 7} {
		cw := &chunkWriter{perWrite: per}
		if _, err := writeFrame(cw, nil, MsgProbe, payload); err != nil {
			t.Fatalf("perWrite=%d: writeFrame: %v", per, err)
		}
		// The whole frame must have been written across multiple calls.
		full := frameBytes(t, MsgProbe, payload)
		if cw.written != len(full) {
			t.Fatalf("perWrite=%d: wrote %d bytes, want the full %d", per, cw.written, len(full))
		}
		if cw.calls < 2 {
			t.Fatalf("perWrite=%d: only %d Write call(s) — the writer did not force a short write", per, cw.calls)
		}
		kind, got, err := readOneFrame(t, &cw.buf)
		if err != nil || kind != MsgProbe || !bytes.Equal(got, payload) {
			t.Fatalf("perWrite=%d: decode kind=%v payload=%q err=%v", per, kind, got, err)
		}
	}
}

// TestWriteErrorAfterPartialWriteIsReturned proves a write error that occurs
// after some bytes have already been written is propagated, not swallowed.
func TestWriteErrorAfterPartialWriteIsReturned(t *testing.T) {
	boom := errors.New("write failed mid-frame")
	cw := &chunkWriter{perWrite: 4, failAt: 4, failErr: boom}
	if _, err := writeFrame(cw, nil, MsgProbe, []byte("this payload needs several writes")); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the mid-frame write error", err)
	}
	if cw.written < 4 {
		t.Fatalf("expected a partial write before the error, wrote %d", cw.written)
	}
}

// TestZeroProgressWriterDoesNotLoopForever proves a writer that reports (0, nil)
// forever cannot hang writeFrame: it returns io.ErrShortWrite. The goroutine +
// deadline is what catches a regression that reintroduces an infinite loop.
func TestZeroProgressWriterDoesNotLoopForever(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		_, err := writeFrame(&chunkWriter{zero: true}, nil, MsgProbe, []byte("x"))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("err = %v, want io.ErrShortWrite", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writeFrame looped forever on a zero-progress writer")
	}
}

// frameBytes builds a valid on-the-wire frame for a payload.
func frameBytes(t *testing.T, kind MsgKind, payload []byte) []byte {
	t.Helper()
	b, err := record.Encode(nil, record.Kind(kind), payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// oneByteReader returns exactly one byte per Read, to prove the frame decoder
// reassembles a frame regardless of TCP fragmentation.
type oneByteReader struct {
	b []byte
	i int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.b[r.i]
	r.i++
	return 1, nil
}

func readOneFrame(t *testing.T, r io.Reader) (MsgKind, []byte, error) {
	t.Helper()
	hdr := make([]byte, record.HeaderSize)
	return readFrame(r, hdr)
}

func TestFrameRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, []byte("hello"), bytes.Repeat([]byte("x"), 4096)} {
		var buf bytes.Buffer
		if _, err := writeFrame(&buf, nil, MsgProbe, payload); err != nil {
			t.Fatalf("writeFrame: %v", err)
		}
		kind, got, err := readOneFrame(t, &buf)
		if err != nil {
			t.Fatalf("readFrame: %v", err)
		}
		if kind != MsgProbe {
			t.Errorf("kind = %v, want Probe", kind)
		}
		if !bytes.Equal(got, payload) && !(len(got) == 0 && len(payload) == 0) {
			t.Errorf("payload = %q, want %q", got, payload)
		}
	}
}

// TestFrameReassembledFromFragments feeds a valid frame one byte at a time.
func TestFrameReassembledFromFragments(t *testing.T) {
	payload := []byte("a message that spans several reads")
	frame := frameBytes(t, MsgProbeResponse, payload)
	kind, got, err := readOneFrame(t, &oneByteReader{b: frame})
	if err != nil {
		t.Fatalf("readFrame from 1-byte reader: %v", err)
	}
	if kind != MsgProbeResponse || !bytes.Equal(got, payload) {
		t.Fatalf("reassembled kind=%v payload=%q, want ProbeResponse %q", kind, got, payload)
	}
}

// TestConcatenatedFramesDecodeIndividually reads two frames from one stream.
func TestConcatenatedFramesDecodeIndividually(t *testing.T) {
	var buf bytes.Buffer
	if _, err := writeFrame(&buf, nil, MsgProbe, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := writeFrame(&buf, nil, MsgProbeResponse, []byte("second")); err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, record.HeaderSize)
	k1, p1, err := readFrame(&buf, hdr)
	if err != nil || k1 != MsgProbe || string(p1) != "first" {
		t.Fatalf("frame 1: kind=%v payload=%q err=%v", k1, p1, err)
	}
	k2, p2, err := readFrame(&buf, hdr)
	if err != nil || k2 != MsgProbeResponse || string(p2) != "second" {
		t.Fatalf("frame 2: kind=%v payload=%q err=%v", k2, p2, err)
	}
	if _, _, err := readFrame(&buf, hdr); err != io.EOF {
		t.Fatalf("after two frames, err = %v, want io.EOF", err)
	}
}

func TestCleanEOFAtFrameBoundary(t *testing.T) {
	if _, _, err := readOneFrame(t, bytes.NewReader(nil)); err != io.EOF {
		t.Fatalf("empty stream: err = %v, want io.EOF", err)
	}
}

func TestTruncatedFrameIsError(t *testing.T) {
	frame := frameBytes(t, MsgProbe, []byte("payload"))
	// Header present but payload cut short.
	truncated := frame[:len(frame)-3]
	if _, _, err := readOneFrame(t, bytes.NewReader(truncated)); !errors.Is(err, ErrTruncatedFrame) {
		t.Fatalf("truncated payload: err = %v, want ErrTruncatedFrame", err)
	}
	// Header itself cut short.
	if _, _, err := readOneFrame(t, bytes.NewReader(frame[:5])); !errors.Is(err, ErrTruncatedFrame) {
		t.Fatalf("truncated header: err = %v, want ErrTruncatedFrame", err)
	}
}

// TestFrameTooLargeIsRejectedBeforeAlloc supplies only a header declaring an
// enormous length and NO payload; getting ErrFrameTooLarge (not a truncation or
// a huge allocation) proves the length is checked before the payload is read.
func TestFrameTooLargeIsRejectedBeforeAlloc(t *testing.T) {
	hdr := make([]byte, record.HeaderSize)
	binary.LittleEndian.PutUint32(hdr[4:8], MaxFrameSize+1)
	hdr[8] = byte(MsgProbe)
	// no crc set, no payload: the size check must fire first.
	if _, _, err := readFrame(bytes.NewReader(hdr), make([]byte, record.HeaderSize)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized frame: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestBadChecksumIsError(t *testing.T) {
	frame := frameBytes(t, MsgProbe, []byte("payload"))
	frame[len(frame)-1] ^= 0xff // corrupt a payload byte
	if _, _, err := readOneFrame(t, bytes.NewReader(frame)); !errors.Is(err, ErrBadChecksum) {
		t.Fatalf("corrupt payload: err = %v, want ErrBadChecksum", err)
	}
}

func TestUnknownKindIsError(t *testing.T) {
	// Build a frame with an undefined kind byte (99) and a correct checksum, so
	// the failure is specifically the unknown kind, not the checksum.
	payload := []byte("x")
	body := append([]byte{0, 0, 0, 0, 0}, payload...) // length(4)+kind(1)+payload placeholder
	binary.LittleEndian.PutUint32(body[0:4], uint32(len(payload)))
	body[4] = 99
	sum := crc32.Checksum(body, castagnoli)
	frame := make([]byte, 4+len(body))
	binary.LittleEndian.PutUint32(frame[0:4], sum)
	copy(frame[4:], body)
	if _, _, err := readOneFrame(t, bytes.NewReader(frame)); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("unknown kind: err = %v, want ErrUnknownKind", err)
	}
}
