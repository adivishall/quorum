package record

import (
	"io"
)

// Writer appends framed records to an underlying stream.
//
// It performs no buffering of its own. Each Append issues exactly one Write to
// the underlying writer, which for a *os.File means exactly one write(2)
// syscall. That is a deliberate durability property, not an oversight: if the
// framing layer buffered records in user space, a record could be "written"
// from the caller's point of view while still living only inside this process,
// and would then vanish on SIGKILL. Deciding when bytes must reach the kernel
// and when they must reach the platter is the caller's job (see the WAL's sync
// modes); the framing layer's job is not to quietly undermine that decision.
type Writer struct {
	w   io.Writer
	buf []byte // reused encode scratch, grown as needed
}

// NewWriter returns a Writer appending to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Append encodes and writes one record, returning the number of bytes written.
//
// On a short or failed write the caller must assume the stream now holds a
// partial record. That is recoverable — it is exactly the torn tail the reader
// is built to detect — but the caller must not treat the record as durable.
func (w *Writer) Append(kind Kind, payload []byte) (int, error) {
	var err error
	w.buf, err = Encode(w.buf[:0], kind, payload)
	if err != nil {
		return 0, err
	}
	n, err := w.w.Write(w.buf)
	if err != nil {
		return n, err
	}
	if n != len(w.buf) {
		return n, io.ErrShortWrite
	}
	return n, nil
}
