package raft

import (
	"bytes"
	"errors"
	"testing"
)

// The entry-size limit (C1, docs/RAFT.md §16): MaxEntryDataLen is the system's
// one bound on an entry's bytes, and the core never creates, holds, sends or
// accepts a larger entry.

// TestProposeAtTheEntryLimitReplicates: a proposal of exactly MaxEntryDataLen
// bytes is accepted by the leader, replicated, committed and applied with its
// bytes intact on every node.
func TestProposeAtTheEntryLimitReplicates(t *testing.T) {
	nw := newNetwork(t, ids(3), 700)
	nw.electLeader("a")
	data := bytes.Repeat([]byte{'x'}, MaxEntryDataLen)
	if err := nw.nodes["a"].Propose(data); err != nil {
		t.Fatalf("Propose(%d bytes) = %v, want nil", len(data), err)
	}
	nw.drain("a")
	nw.deliverAll()
	idx := nw.nodes["a"].LastIndex()
	// Followers learn the commit index from the leader's next AppendEntries.
	for i := 0; i < nw.nodes["a"].heartbeatTicks; i++ {
		nw.tick("a")
	}
	nw.deliverAll()
	for _, id := range ids(3) {
		if c := nw.nodes[id].CommitIndex(); c < idx {
			t.Fatalf("%s commit = %d, want >= %d", id, c, idx)
		}
		e, err := nw.logs[id].At(idx)
		if err != nil || !bytes.Equal(e.Data, data) {
			t.Fatalf("%s entry %d: %d bytes, %v; want the %d-byte proposal", id, idx, len(e.Data), err, len(data))
		}
	}
	nw.assertLogMatching()
}

// TestProposeOverTheEntryLimitIsRefused: one byte more is refused with
// ErrEntryTooLarge before anything happens — nothing appended, nothing to send
// or persist — on the leader and, definitely, on a follower too.
func TestProposeOverTheEntryLimitIsRefused(t *testing.T) {
	nw := newNetwork(t, ids(3), 701)
	nw.electLeader("a")
	over := bytes.Repeat([]byte{'x'}, MaxEntryDataLen+1)
	for _, id := range ids(3) {
		r := nw.nodes[id]
		last, term := r.LastIndex(), r.Term()
		if err := r.Propose(over); !errors.Is(err, ErrEntryTooLarge) {
			t.Fatalf("%s (%s): Propose(%d bytes) = %v, want ErrEntryTooLarge", id, r.Role(), len(over), err)
		}
		if r.LastIndex() != last || r.Term() != term {
			t.Fatalf("%s: a refused proposal changed the log or term: last %d->%d, term %d->%d", id, last, r.LastIndex(), term, r.Term())
		}
		if r.HasReady() {
			t.Fatalf("%s: a refused proposal produced a Ready: %+v", id, r.Ready())
		}
	}
	// The group goes on serving proposals within the limit.
	nw.propose("a", "after")
	nw.assertLogMatching()
}

// TestStepRefusesAnOversizedAppendEntries: an AppendEntries carrying an entry
// larger than the limit — which no correct leader sends and the codec cannot
// decode — is refused with no effect at all, not even the adoption of its
// higher term.
func TestStepRefusesAnOversizedAppendEntries(t *testing.T) {
	b, lg := newCore(t, "b", ids(3), 702)
	term, last := b.Term(), lg.LastIndex()
	m := Message{
		Type: MsgAppendRequest, From: "a", To: "b", Term: term + 5,
		Entries: []Entry{{Index: 1, Term: term + 5, Data: bytes.Repeat([]byte{'x'}, MaxEntryDataLen+1)}},
	}
	err := b.Step(m)
	if !errors.Is(err, ErrMalformedMessage) || !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("Step(oversized AppendEntries) = %v, want ErrMalformedMessage and ErrEntryTooLarge", err)
	}
	if b.Term() != term || lg.LastIndex() != last || b.Role() != Follower {
		t.Fatalf("the refused message had an effect: term %d->%d, last %d->%d, role %s", term, b.Term(), last, lg.LastIndex(), b.Role())
	}
	if b.HasReady() {
		t.Fatalf("the refused message produced a Ready: %+v", b.Ready())
	}
	// The same message with an entry exactly at the limit is accepted.
	m.Entries[0].Data = m.Entries[0].Data[:MaxEntryDataLen]
	if err := b.Step(m); err != nil {
		t.Fatalf("Step(AppendEntries at the limit) = %v", err)
	}
	if lg.LastIndex() != 1 {
		t.Fatalf("LastIndex = %d after an AppendEntries at the limit, want 1", lg.LastIndex())
	}
}

// TestCodecEntryLimit: the AppendEntries codec carries an entry of exactly
// MaxEntryDataLen bytes and refuses to decode one byte more.
func TestCodecEntryLimit(t *testing.T) {
	at := Message{Type: MsgAppendRequest, Term: 1, Entries: []Entry{{Index: 1, Term: 1, Data: bytes.Repeat([]byte{'x'}, MaxEntryDataLen)}}}
	got, err := Unmarshal(at.Marshal())
	if err != nil || len(got.Entries) != 1 || !bytes.Equal(got.Entries[0].Data, at.Entries[0].Data) {
		t.Fatalf("round trip at the limit: %v", err)
	}
	over := at
	over.Entries = []Entry{{Index: 1, Term: 1, Data: bytes.Repeat([]byte{'x'}, MaxEntryDataLen+1)}}
	if _, err := Unmarshal(over.Marshal()); !errors.Is(err, ErrMalformedMessage) {
		t.Fatalf("decoding an entry one byte over the limit: %v, want ErrMalformedMessage", err)
	}
}
