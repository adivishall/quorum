package raft

import (
	"bytes"
	"errors"
	"testing"
)

// TestMessageRoundTrip proves every message type marshals and unmarshals back to
// an equal value (From/To are transport-level and deliberately not encoded).
func TestMessageRoundTrip(t *testing.T) {
	msgs := []Message{
		{Type: MsgVoteRequest, Term: 7, LastLogIndex: 12, LastLogTerm: 5},
		{Type: MsgVoteResponse, Term: 7, VoteGranted: true},
		{Type: MsgVoteResponse, Term: 8, VoteGranted: false},
		{Type: MsgAppendRequest, Term: 9, PrevLogIndex: 3, PrevLogTerm: 2, LeaderCommit: 3},
		{Type: MsgAppendRequest, Term: 9, PrevLogIndex: 3, PrevLogTerm: 2, LeaderCommit: 4,
			Entries: []Entry{{Index: 4, Term: 9, Data: []byte("hello")}, {Index: 5, Term: 9, Data: nil}, {Index: 6, Term: 9, Data: []byte{}}}},
		{Type: MsgAppendResponse, Term: 9, Success: true, MatchIndex: 6},
		{Type: MsgAppendResponse, Term: 9, Success: false, ConflictTerm: 4, ConflictIndex: 2},
		{Type: MsgAppendRequest, Term: 9, PrevLogIndex: 3, PrevLogTerm: 2, LeaderCommit: 3, Seq: 1 << 40},
		{Type: MsgAppendResponse, Term: 9, Success: true, MatchIndex: 6, Seq: 77},
	}
	for _, m := range msgs {
		got, err := Unmarshal(m.Marshal())
		if err != nil {
			t.Fatalf("Unmarshal(%s): %v", m.Type, err)
		}
		if !messagesEqual(m, got) {
			t.Fatalf("round trip mismatch:\n in: %+v\nout: %+v", m, got)
		}
	}
}

func messagesEqual(a, b Message) bool {
	if a.Type != b.Type || a.Term != b.Term || a.LastLogIndex != b.LastLogIndex ||
		a.LastLogTerm != b.LastLogTerm || a.VoteGranted != b.VoteGranted ||
		a.PrevLogIndex != b.PrevLogIndex || a.PrevLogTerm != b.PrevLogTerm ||
		a.LeaderCommit != b.LeaderCommit || a.Success != b.Success ||
		a.ConflictTerm != b.ConflictTerm || a.ConflictIndex != b.ConflictIndex ||
		a.MatchIndex != b.MatchIndex || a.Seq != b.Seq || len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		if a.Entries[i].Index != b.Entries[i].Index || a.Entries[i].Term != b.Entries[i].Term ||
			!bytes.Equal(a.Entries[i].Data, b.Entries[i].Data) {
			return false
		}
	}
	return true
}

// TestMarshalIsDeterministic proves the same message marshals to identical bytes.
func TestMarshalIsDeterministic(t *testing.T) {
	m := Message{Type: MsgAppendRequest, Term: 5, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 2,
		Entries: []Entry{{Index: 2, Term: 5, Data: []byte("x")}}}
	if !bytes.Equal(m.Marshal(), m.Marshal()) {
		t.Fatal("marshal is not deterministic")
	}
}

// TestUnmarshalRejectsMalformed proves the decoder rejects, never panics on,
// hostile input: truncation, an unknown type, trailing bytes, and an oversized
// entry count.
func TestUnmarshalRejectsMalformed(t *testing.T) {
	// Empty.
	if _, err := Unmarshal(nil); !errors.Is(err, ErrMalformedMessage) {
		t.Errorf("empty: err = %v, want ErrMalformedMessage", err)
	}
	// Unknown type byte.
	if _, err := Unmarshal([]byte{99, 1}); !errors.Is(err, ErrUnknownMessageType) {
		t.Errorf("unknown type: err = %v, want ErrUnknownMessageType", err)
	}
	// Truncated: a vote request needs more fields.
	if _, err := Unmarshal([]byte{byte(MsgVoteRequest)}); !errors.Is(err, ErrMalformedMessage) {
		t.Errorf("truncated: err = %v, want ErrMalformedMessage", err)
	}
	// Trailing bytes after a complete VoteResponse.
	full := Message{Type: MsgVoteResponse, Term: 1, VoteGranted: true}.Marshal()
	if _, err := Unmarshal(append(full, 0xff)); !errors.Is(err, ErrMalformedMessage) {
		t.Errorf("trailing bytes: err = %v, want ErrMalformedMessage", err)
	}
	// A bool byte other than 0/1.
	if _, err := Unmarshal([]byte{byte(MsgVoteResponse), 1, 2}); !errors.Is(err, ErrMalformedMessage) {
		t.Errorf("bad bool: err = %v, want ErrMalformedMessage", err)
	}
	// An oversized entry count (claims many entries, no data follows).
	w := msgWriter{}
	w.byte(byte(MsgAppendRequest))
	w.uvarint(1)                        // term
	w.uvarint(0)                        // prevLogIndex
	w.uvarint(0)                        // prevLogTerm
	w.uvarint(0)                        // leaderCommit
	w.uvarint(MaxEntriesPerMessage + 1) // too many
	if _, err := Unmarshal(w.b); !errors.Is(err, ErrMalformedMessage) {
		t.Errorf("oversized entry count: err = %v, want ErrMalformedMessage", err)
	}
}

// FuzzMessageDecode proves Unmarshal never panics on arbitrary input, and that a
// value it accepts re-marshals to bytes that decode identically (canonical).
func FuzzMessageDecode(f *testing.F) {
	for _, m := range []Message{
		{Type: MsgVoteRequest, Term: 1, LastLogIndex: 2, LastLogTerm: 1},
		{Type: MsgVoteResponse, Term: 1, VoteGranted: true},
		{Type: MsgAppendRequest, Term: 2, Entries: []Entry{{Index: 1, Term: 2, Data: []byte("x")}}},
		{Type: MsgAppendResponse, Term: 2, Success: false, ConflictTerm: 1, ConflictIndex: 1},
	} {
		f.Add(m.Marshal())
	}
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := Unmarshal(data)
		if err != nil {
			return // rejecting malformed input is fine; it must not panic
		}
		// A decoded message re-encodes and re-decodes to an equal value.
		m2, err := Unmarshal(m.Marshal())
		if err != nil {
			t.Fatalf("re-decode of accepted message failed: %v", err)
		}
		if !messagesEqual(m, m2) {
			t.Fatalf("not canonical:\n%+v\n%+v", m, m2)
		}
	})
}
