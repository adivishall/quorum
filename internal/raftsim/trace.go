package raftsim

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
)

// traceKeep is how many recent trace lines are kept for failure reports. Every
// line, not just these, feeds the hash.
const traceKeep = 400

// Trace is the machine-readable record of a run: one key=value line per event and
// per observable consequence (sends, drops, role changes, commits, applies,
// crashes, recoveries, persistence failures), each prefixed with the logical step.
// The SHA-256 of every line is the run's fingerprint — two runs with the same seed
// and script must produce the same hash, which is how determinism is tested — and
// the last lines are kept for failure reports.
type Trace struct {
	h     hash.Hash
	ring  []string
	next  int
	full  bool
	lines int
	sink  io.Writer
}

func newTrace() *Trace { return &Trace{h: sha256.New(), ring: make([]string, traceKeep)} }

// Mirror sends every subsequent trace line to w as well (for verbose runs). It does
// not affect the hash.
func (t *Trace) Mirror(w io.Writer) { t.sink = w }

func (t *Trace) add(step int, format string, args ...any) {
	line := fmt.Sprintf("s=%d ", step) + fmt.Sprintf(format, args...)
	if t.sink != nil {
		fmt.Fprintln(t.sink, line)
	}
	t.h.Write([]byte(line))
	t.h.Write([]byte{'\n'})
	t.ring[t.next] = line
	t.next = (t.next + 1) % len(t.ring)
	if t.next == 0 {
		t.full = true
	}
	t.lines++
}

// Hash is the hex SHA-256 of every line so far.
func (t *Trace) Hash() string { return hex.EncodeToString(t.h.Sum(nil)) }

// Lines is the total number of lines recorded.
func (t *Trace) Lines() int { return t.lines }

// Tail returns up to the last n lines, oldest first.
func (t *Trace) Tail(n int) []string {
	var all []string
	if t.full {
		all = append(all, t.ring[t.next:]...)
	}
	all = append(all, t.ring[:t.next]...)
	if n < len(all) {
		all = all[len(all)-n:]
	}
	return all
}
