package lincheck

// SessionModel is the sequential specification of request identity (Phase 13,
// docs/CLIENT_SEMANTICS.md §4 and §8): what applying each committed command
// does to the sessions and to the key-value state, in log order. It is the
// reference the replicated state machine (internal/kv) is diffed against, so it
// is written independently of it: it remembers each executed request's whole
// command rather than a fingerprint, keeps sessions and records in plain slices
// scanned linearly, and shares no code with the implementation.
//
// It is also the answer, per command, to "new logical request, duplicate, or
// conflicting reuse?" — the question the contract makes precise.
type SessionModel struct {
	limits   SessionLimits
	sessions []*modelSession
	state    map[string]State
}

// SessionLimits bound the session table. They are part of the state machine's
// definition: every replica must use the same ones.
type SessionLimits struct {
	MaxSessions int // sessions kept; registering one more evicts the least recently used
	MaxUnacked  int // remembered results per session; a new request beyond it is refused
}

// Decision is what applying one command did.
type Decision uint8

const (
	// Executed: a new logical request (or an anonymous write) changed the state.
	Executed Decision = iota + 1
	// Duplicate: the request had already executed with the same command; no change.
	Duplicate
	// Conflict: the request id had already executed with a DIFFERENT command; no change.
	Conflict
	// Stale: the request id is below the session's AckedBelow; no change.
	Stale
	// Expired: the session does not exist (never registered, or evicted); no change.
	Expired
	// Limit: the session holds its maximum of unacknowledged results; no change.
	Limit
	// Registered: a new session, whose id is the command's log index.
	Registered
)

var decisionNames = map[Decision]string{Executed: "executed", Duplicate: "duplicate", Conflict: "conflict",
	Stale: "stale", Expired: "expired", Limit: "limit", Registered: "registered"}

func (d Decision) String() string {
	if s, ok := decisionNames[d]; ok {
		return s
	}
	return "decision(?)"
}

// SessionCommand is one committed command, as the model sees it.
type SessionCommand struct {
	Index                           uint64 // its log index
	Register                        bool
	ClientID, RequestID, AckedBelow uint64 // ClientID 0: anonymous
	Kind                            Kind   // Put or Delete (not for Register)
	Key, Value                      string
}

type modelSession struct {
	id, last, ackedBelow uint64
	done                 []modelRecord
}

type modelRecord struct {
	rid   uint64
	cmd   string // the whole command, not a hash
	index uint64
}

// NewSessionModel returns an empty model.
func NewSessionModel(l SessionLimits) *SessionModel {
	return &SessionModel{limits: l, state: map[string]State{}}
}

// Apply applies one command and returns its decision and, for Executed,
// Duplicate and Registered, the log index of the execution it reports (for a
// duplicate, the ORIGINAL execution's).
func (m *SessionModel) Apply(c SessionCommand) (Decision, uint64) {
	if c.Register {
		m.sessions = append(m.sessions, &modelSession{id: c.Index, last: c.Index, ackedBelow: 1})
		for len(m.sessions) > m.limits.MaxSessions {
			lru := 0
			for i, s := range m.sessions {
				if s.last < m.sessions[lru].last {
					lru = i
				}
			}
			m.sessions = append(m.sessions[:lru], m.sessions[lru+1:]...)
		}
		return Registered, c.Index
	}
	if c.ClientID == 0 {
		m.write(c)
		return Executed, c.Index
	}
	var s *modelSession
	for _, x := range m.sessions {
		if x.id == c.ClientID {
			s = x
		}
	}
	if s == nil {
		return Expired, 0
	}
	s.last = c.Index
	w := c.AckedBelow
	if w > c.RequestID {
		w = c.RequestID
	}
	if w > s.ackedBelow {
		s.ackedBelow = w
		kept := s.done[:0]
		for _, r := range s.done {
			if r.rid >= w {
				kept = append(kept, r)
			}
		}
		s.done = kept
	}
	if c.RequestID < s.ackedBelow {
		return Stale, 0
	}
	text := c.Kind.String() + "\x00" + c.Key + "\x00" + c.Value
	for _, r := range s.done {
		if r.rid == c.RequestID {
			if r.cmd == text {
				return Duplicate, r.index
			}
			return Conflict, 0
		}
	}
	if len(s.done) >= m.limits.MaxUnacked {
		return Limit, 0
	}
	s.done = append(s.done, modelRecord{rid: c.RequestID, cmd: text, index: c.Index})
	m.write(c)
	return Executed, c.Index
}

func (m *SessionModel) write(c SessionCommand) {
	if c.Kind == Put {
		m.state[c.Key] = State{Present: true, Value: c.Value}
	} else {
		delete(m.state, c.Key)
	}
}

// State returns a key's register state.
func (m *SessionModel) State(key string) State { return m.state[key] }

// Keys returns every present key.
func (m *SessionModel) Keys() []string {
	var out []string
	for k := range m.state {
		out = append(out, k)
	}
	return out
}

// Sessions returns the live session ids, in registration order.
func (m *SessionModel) Sessions() []uint64 {
	var out []uint64
	for _, s := range m.sessions {
		out = append(out, s.id)
	}
	return out
}

// SessionInfo reports a live session's AckedBelow and remembered request ids
// (in execution order), for diffing against an implementation's table.
func (m *SessionModel) SessionInfo(id uint64) (ackedBelow uint64, rids []uint64, ok bool) {
	for _, s := range m.sessions {
		if s.id == id {
			for _, r := range s.done {
				rids = append(rids, r.rid)
			}
			return s.ackedBelow, rids, true
		}
	}
	return 0, nil, false
}
