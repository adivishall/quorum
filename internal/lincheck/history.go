package lincheck

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Kind is a client operation type.
type Kind uint8

const (
	Put Kind = iota + 1
	Get
	Delete
)

func (k Kind) String() string {
	switch k {
	case Put:
		return "put"
	case Get:
		return "get"
	case Delete:
		return "delete"
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Outcome is how an operation ended from the client's point of view.
type Outcome uint8

const (
	// Incomplete: the client never received a response — it timed out, the node
	// it was talking to died, or the error it got leaves the effect ambiguous.
	// The operation MAY have taken effect, at any time after its invocation.
	Incomplete Outcome = iota
	// OK: the operation completed successfully (for Get, with Output).
	OK
	// NotFound: a Get completed and the key was absent.
	NotFound
	// Rejected: the operation completed with a definite statement that it had NO
	// effect — not leader, the proposal was lost to a different committed entry,
	// invalid input, or the node could not be reached before it accepted the
	// request. A Rejected operation is excluded from linearization; if it did
	// have an effect after all, a later read exposes it and the check fails.
	Rejected
)

func (o Outcome) String() string {
	switch o {
	case Incomplete:
		return "incomplete"
	case OK:
		return "ok"
	case NotFound:
		return "notfound"
	case Rejected:
		return "rejected"
	}
	return "outcome(" + strconv.Itoa(int(o)) + ")"
}

// Attempt is one request a client made for an operation: the node it contacted
// and what came back. An operation may have several (a redirect to the leader,
// a retry of a read), each recorded, never hidden.
type Attempt struct {
	Node     string
	Invoke   int64
	Complete int64 // 0 if this attempt got no response
	Result   string
	Term     uint64
}

// Op is one client operation in a history.
type Op struct {
	ID     int
	Client string
	Kind   Kind
	Key    string
	Value  []byte // Put: the value written

	// Invoke and Complete are positions in the history's event order; Complete is
	// 0 for an Incomplete operation. Positions are strictly increasing in real
	// time within one history (a sequence counter), so "A completed before B was
	// invoked" is exactly A.Complete < B.Invoke.
	Invoke   int64
	Complete int64

	Outcome Outcome
	Output  []byte // Get with Outcome OK: the value read

	// Where and when the completing response came from, when known.
	Node  string
	Term  uint64
	Index uint64

	Attempts []Attempt
}

// Effective reports whether the operation must or may be part of a
// linearization: OK and NotFound operations must; Incomplete ones may; Rejected
// ones may not.
func (o Op) Effective() bool { return o.Outcome != Rejected }

// Optional reports whether the operation is Incomplete (may be linearized or
// omitted).
func (o Op) Optional() bool { return o.Outcome == Incomplete }

// String renders an op in the one-line form Parse reads back:
//
//	id client kind key inv complete outcome [value=<v>] [output=<v>] [node=n term=t index=i]
func (o Op) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %s %s %s %d %d %s", o.ID, o.Client, o.Kind, quote(o.Key), o.Invoke, o.Complete, o.Outcome)
	if o.Kind == Put {
		fmt.Fprintf(&b, " value=%s", quote(string(o.Value)))
	}
	if o.Kind == Get && o.Outcome == OK {
		fmt.Fprintf(&b, " output=%s", quote(string(o.Output)))
	}
	if o.Node != "" {
		fmt.Fprintf(&b, " node=%s term=%d index=%d", o.Node, o.Term, o.Index)
	}
	return b.String()
}

func quote(s string) string { return strconv.Quote(s) }

// tokens splits a line on whitespace, keeping Go-quoted strings whole.
func tokens(line string) ([]string, error) {
	var out []string
	i := 0
	for i < len(line) {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			break
		}
		start := i
		if j := strings.IndexByte(line[i:], '"'); j >= 0 && (j == 0 || strings.IndexByte(line[i:i+j], ' ') < 0) {
			// a token that contains a quoted string: scan to the closing quote
			i += j + 1
			for i < len(line) && line[i] != '"' {
				if line[i] == '\\' {
					i++
				}
				i++
			}
			if i >= len(line) {
				return nil, fmt.Errorf("lincheck: unterminated quote in %q", line)
			}
			i++ // the closing quote
		}
		for i < len(line) && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		out = append(out, line[start:i])
	}
	return out, nil
}

// Parse reads one line of Op.String's form. Attempts are not serialized.
func Parse(line string) (Op, error) {
	f, err := tokens(line)
	if err != nil {
		return Op{}, err
	}
	if len(f) < 7 {
		return Op{}, fmt.Errorf("lincheck: op line wants at least 7 fields: %q", line)
	}
	var op Op
	if op.ID, err = strconv.Atoi(f[0]); err != nil {
		return Op{}, fmt.Errorf("lincheck: bad id in %q", line)
	}
	op.Client = f[1]
	switch f[2] {
	case "put":
		op.Kind = Put
	case "get":
		op.Kind = Get
	case "delete":
		op.Kind = Delete
	default:
		return Op{}, fmt.Errorf("lincheck: bad kind %q in %q", f[2], line)
	}
	if op.Key, err = strconv.Unquote(f[3]); err != nil {
		return Op{}, fmt.Errorf("lincheck: bad key in %q", line)
	}
	if op.Invoke, err = strconv.ParseInt(f[4], 10, 64); err != nil {
		return Op{}, fmt.Errorf("lincheck: bad invoke in %q", line)
	}
	if op.Complete, err = strconv.ParseInt(f[5], 10, 64); err != nil {
		return Op{}, fmt.Errorf("lincheck: bad complete in %q", line)
	}
	switch f[6] {
	case "incomplete":
		op.Outcome = Incomplete
	case "ok":
		op.Outcome = OK
	case "notfound":
		op.Outcome = NotFound
	case "rejected":
		op.Outcome = Rejected
	default:
		return Op{}, fmt.Errorf("lincheck: bad outcome %q in %q", f[6], line)
	}
	for _, kv := range f[7:] {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			return Op{}, fmt.Errorf("lincheck: bad field %q in %q", kv, line)
		}
		k, v := kv[:eq], kv[eq+1:]
		switch k {
		case "value":
			s, err := strconv.Unquote(v)
			if err != nil {
				return Op{}, fmt.Errorf("lincheck: bad value in %q", line)
			}
			op.Value = []byte(s)
		case "output":
			s, err := strconv.Unquote(v)
			if err != nil {
				return Op{}, fmt.Errorf("lincheck: bad output in %q", line)
			}
			op.Output = []byte(s)
		case "node":
			op.Node = v
		case "term":
			op.Term, _ = strconv.ParseUint(v, 10, 64)
		case "index":
			op.Index, _ = strconv.ParseUint(v, 10, 64)
		default:
			return Op{}, fmt.Errorf("lincheck: unknown field %q in %q", k, line)
		}
	}
	if (op.Outcome == Incomplete) != (op.Complete == 0) {
		return Op{}, fmt.Errorf("lincheck: outcome %s with completion %d in %q", op.Outcome, op.Complete, line)
	}
	return op, nil
}

// History is a recorded set of operations.
type History struct {
	Ops []Op
}

// ParseHistory reads one op per line (blank lines and '#' comments ignored).
func ParseHistory(text string) (History, error) {
	var h History
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		op, err := Parse(line)
		if err != nil {
			return History{}, fmt.Errorf("line %d: %w", n+1, err)
		}
		h.Ops = append(h.Ops, op)
	}
	return h, nil
}

// Format renders ops one per line, in ID order.
func Format(ops []Op) string {
	sorted := append([]Op(nil), ops...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	var b strings.Builder
	for _, op := range sorted {
		b.WriteString(op.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// String renders the whole history.
func (h History) String() string { return Format(h.Ops) }

// Keys returns the distinct keys, sorted.
func (h History) Keys() []string {
	seen := map[string]bool{}
	var keys []string
	for _, op := range h.Ops {
		if !seen[op.Key] {
			seen[op.Key] = true
			keys = append(keys, op.Key)
		}
	}
	sort.Strings(keys)
	return keys
}

// ForKey returns the operations on one key, in invocation order.
func (h History) ForKey(key string) []Op {
	var ops []Op
	for _, op := range h.Ops {
		if op.Key == key {
			ops = append(ops, op)
		}
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].Invoke < ops[j].Invoke })
	return ops
}

// Counts summarizes a history.
type Counts struct {
	Total, OK, NotFound, Rejected, Incomplete int
	Puts, Gets, Deletes                       int
	Attempts                                  int // every request made, redirects and retries included
	Keys, Clients                             int
}

// Summary counts the history's operations by outcome and kind.
func (h History) Summary() Counts {
	var c Counts
	keys, clients := map[string]bool{}, map[string]bool{}
	for _, op := range h.Ops {
		c.Total++
		switch op.Outcome {
		case OK:
			c.OK++
		case NotFound:
			c.NotFound++
		case Rejected:
			c.Rejected++
		case Incomplete:
			c.Incomplete++
		}
		switch op.Kind {
		case Put:
			c.Puts++
		case Get:
			c.Gets++
		case Delete:
			c.Deletes++
		}
		c.Attempts += len(op.Attempts)
		keys[op.Key] = true
		clients[op.Client] = true
	}
	c.Keys, c.Clients = len(keys), len(clients)
	return c
}

// Validate checks the structural rules a recorded history must satisfy: every
// op has a client, kind and key; Invoke > 0; a completed op has Complete >
// Invoke; an Incomplete op has Complete == 0; positions are distinct across all
// invocations and completions; NotFound only on a Get; Output only on an OK Get.
func (h History) Validate() error {
	seen := map[int64]int{}
	for _, op := range h.Ops {
		if op.Client == "" || op.Key == "" || op.Kind == 0 {
			return fmt.Errorf("lincheck: op %d is missing client, key or kind", op.ID)
		}
		if op.Invoke <= 0 {
			return fmt.Errorf("lincheck: op %d has no invocation position", op.ID)
		}
		if prev, dup := seen[op.Invoke]; dup {
			return fmt.Errorf("lincheck: ops %d and %d share position %d", prev, op.ID, op.Invoke)
		}
		seen[op.Invoke] = op.ID
		if op.Outcome == Incomplete {
			if op.Complete != 0 {
				return fmt.Errorf("lincheck: op %d is incomplete but has completion %d", op.ID, op.Complete)
			}
			continue
		}
		if op.Complete <= op.Invoke {
			return fmt.Errorf("lincheck: op %d completed at %d, not after its invocation %d", op.ID, op.Complete, op.Invoke)
		}
		if prev, dup := seen[op.Complete]; dup {
			return fmt.Errorf("lincheck: ops %d and %d share position %d", prev, op.ID, op.Complete)
		}
		seen[op.Complete] = op.ID
		if op.Outcome == NotFound && op.Kind != Get {
			return fmt.Errorf("lincheck: op %d is a %s with outcome notfound", op.ID, op.Kind)
		}
		if op.Output != nil && (op.Kind != Get || op.Outcome != OK) {
			return fmt.Errorf("lincheck: op %d has an output but is not a successful get", op.ID)
		}
	}
	return nil
}

// Recorder builds a History from concurrent clients. Every Begin and End takes
// the next position from one counter under one mutex, so the recorded order of
// any two events is the order in which they happened — the real-time precedence
// the checker relies on — without a wall clock.
type Recorder struct {
	mu   sync.Mutex
	seq  int64
	next int
	ops  map[int]*Op
	done []Op
}

// NewRecorder returns an empty recorder.
func NewRecorder() *Recorder { return &Recorder{ops: map[int]*Op{}} }

// Begin records an invocation and returns the op's id. Call it BEFORE sending
// the first request.
func (r *Recorder) Begin(client string, kind Kind, key string, value []byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	r.next++
	op := &Op{ID: r.next, Client: client, Kind: kind, Key: key, Value: append([]byte(nil), value...), Invoke: r.seq}
	r.ops[op.ID] = op
	return op.ID
}

// Attempt records that the op is now trying node. It returns the attempt's index.
func (r *Recorder) Attempt(id int, node string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	op := r.ops[id]
	op.Attempts = append(op.Attempts, Attempt{Node: node, Invoke: r.seq})
	return len(op.Attempts) - 1
}

// AttemptDone records how an attempt ended (result text; term if known). An
// attempt that got no response passes complete=false.
func (r *Recorder) AttemptDone(id, attempt int, complete bool, result string, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	a := &r.ops[id].Attempts[attempt]
	a.Result, a.Term = result, term
	if complete {
		a.Complete = r.seq
	}
}

// End records the op's completion. Call it AFTER the response was received; for
// an op that never got one, call it with Incomplete (it then has no completion
// position). Node/term/index are optional metadata of the completing response.
func (r *Recorder) End(id int, outcome Outcome, output []byte, node string, term, index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.ops[id]
	op.Outcome = outcome
	op.Node, op.Term, op.Index = node, term, index
	if outcome == OK && op.Kind == Get {
		op.Output = append([]byte{}, output...)
	}
	if outcome != Incomplete {
		r.seq++
		op.Complete = r.seq
	}
	delete(r.ops, id)
	r.done = append(r.done, *op)
}

// History returns everything recorded so far. Ops that have Begun but not Ended
// are included as Incomplete: a client still waiting is, for the checker, a
// client that never heard back.
func (r *Recorder) History() History {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := History{Ops: append([]Op(nil), r.done...)}
	for _, op := range r.ops {
		cp := *op
		cp.Outcome, cp.Complete = Incomplete, 0
		h.Ops = append(h.Ops, cp)
	}
	sort.Slice(h.Ops, func(i, j int) bool { return h.Ops[i].ID < h.Ops[j].ID })
	return h
}

// Position returns the current event position (for callers that record other
// events against the same clock, e.g. "the leader was killed here").
func (r *Recorder) Position() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// Mark advances the clock by one and returns the new position, so an external
// event (a fault) can be placed in the history's real-time order.
func (r *Recorder) Mark() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	return r.seq
}
