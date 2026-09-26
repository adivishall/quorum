package lincheck

import (
	"strings"
	"testing"
)

// TestLogicalMergesTheSendsOfOneRequest pins History.Logical: sends of one
// identified write become one operation invoked at the first send of its
// command and completed at the first acknowledgement; reads and anonymous ops
// are left alone; identities are per client.
func TestLogicalMergesTheSendsOfOneRequest(t *testing.T) {
	h := mustParse(t, `
1 c1 put "k" 1 0 incomplete value="A" cid=1 rid=1
2 c1 put "k" 2 9 ok value="A" cid=1 rid=1
3 c1 put "k" 3 4 ok value="A" cid=1 rid=1
4 c1 put "k" 5 6 rejected value="Z" cid=1 rid=1
5 c2 put "k" 7 8 ok value="B" cid=2 rid=1
6 c3 get "k" 10 11 ok output="A" cid=1 rid=1
7 c4 put "k" 12 13 ok value="C"
`)
	lh, err := h.Logical()
	if err != nil {
		t.Fatal(err)
	}
	if len(lh.Ops) != 4 {
		t.Fatalf("want 4 logical ops (the merged request, c2's, the read, the anonymous put), got:\n%s", lh)
	}
	m := lh.Ops[0]
	if m.ID != 1 || m.Invoke != 1 || m.Complete != 4 || m.Outcome != OK || string(m.Value) != "A" || m.ClientID != 1 || m.RequestID != 1 {
		t.Fatalf("merged request: %s", m)
	}
	for _, op := range lh.Ops[1:] {
		if op.ID == 2 || op.ID == 3 || op.ID == 4 {
			t.Fatalf("send %d survived the merge", op.ID)
		}
	}
	// A request never acknowledged but sent unanswered is optional; one whose
	// every send was refused is excluded.
	h = mustParse(t, `
1 c1 put "k" 1 0 incomplete value="A" cid=1 rid=1
2 c1 put "k" 2 3 rejected value="A" cid=1 rid=1
3 c1 delete "k" 4 5 rejected cid=1 rid=2
4 c1 delete "k" 6 7 rejected cid=1 rid=2
`)
	if lh, err = h.Logical(); err != nil {
		t.Fatal(err)
	}
	if lh.Ops[0].Outcome != Incomplete || lh.Ops[0].Complete != 0 || lh.Ops[1].Outcome != Rejected {
		t.Fatalf("outcomes: %s", lh)
	}
}

// TestLogicalRefusesWhatTheContractForbidsOrLeavesOpen: two acknowledged
// commands under one identity is a violation; two unanswered different commands
// with none acknowledged is refused as undecidable, never guessed at.
func TestLogicalRefusesWhatTheContractForbidsOrLeavesOpen(t *testing.T) {
	for text, want := range map[string]string{
		"1 c1 put \"k\" 1 2 ok value=\"A\" cid=1 rid=1\n2 c1 put \"k\" 3 4 ok value=\"B\" cid=1 rid=1":                 "conflicting reuse was accepted",
		"1 c1 put \"k\" 1 0 incomplete value=\"A\" cid=1 rid=1\n2 c1 put \"k\" 3 0 incomplete value=\"B\" cid=1 rid=1": "ambiguous",
	} {
		h := mustParse(t, text)
		_, err := h.Logical()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("want an error mentioning %q, got %v", want, err)
		}
		if r := Check(h, Options{}); r.OK || len(r.Counterexample) != 2 {
			t.Fatalf("Check must reject it with the request's sends as counterexample: %+v", r)
		}
	}
}

// TestSessionModelFollowsTheContract walks every row of the decision table of
// docs/CLIENT_SEMANTICS.md §4, and the bounds of §8, on the reference model.
func TestSessionModelFollowsTheContract(t *testing.T) {
	m := NewSessionModel(SessionLimits{MaxSessions: 2, MaxUnacked: 2})
	put := func(idx, c, r, w uint64, k, v string) SessionCommand {
		return SessionCommand{Index: idx, ClientID: c, RequestID: r, AckedBelow: w, Kind: Put, Key: k, Value: v}
	}
	expect := func(what string, c SessionCommand, want Decision, wantIdx uint64) {
		t.Helper()
		got, idx := m.Apply(c)
		if got != want || idx != wantIdx {
			t.Fatalf("%s: got %s@%d, want %s@%d", what, got, idx, want, wantIdx)
		}
	}
	expect("register", SessionCommand{Index: 10, Register: true}, Registered, 10)
	expect("unknown session", put(11, 99, 1, 1, "k", "x"), Expired, 0)
	expect("new request", put(12, 10, 1, 1, "k", "A"), Executed, 12)
	expect("retry, same command", put(13, 10, 1, 1, "k", "A"), Duplicate, 12)
	expect("same id, different command", put(14, 10, 1, 1, "k", "B"), Conflict, 0)
	if s := m.State("k"); !s.Present || s.Value != "A" {
		t.Fatalf("only the first command took effect: %+v", s)
	}
	expect("second request", put(15, 10, 2, 1, "k", "B"), Executed, 15)
	expect("third request beyond MaxUnacked", put(16, 10, 3, 1, "k", "C"), Limit, 0)
	expect("acknowledge 1 and 2; now 3 fits", put(17, 10, 3, 3, "k", "C"), Executed, 17)
	expect("below AckedBelow", put(18, 10, 1, 3, "k", "A"), Stale, 0)
	expect("AckedBelow never moves back", put(19, 10, 2, 1, "k", "B"), Stale, 0)
	expect("anonymous", SessionCommand{Index: 20, Kind: Delete, Key: "k"}, Executed, 20)
	expect("register a second", SessionCommand{Index: 21, Register: true}, Registered, 21)
	// Session 10's last command was index 19, session 21's is 21: registering
	// a third evicts 10, the least recently used.
	expect("register a third", SessionCommand{Index: 22, Register: true}, Registered, 22)
	if got := m.Sessions(); len(got) != 2 || got[0] != 21 || got[1] != 22 {
		t.Fatalf("sessions after eviction: %v", got)
	}
	expect("an evicted session's retry is refused, not re-executed", put(23, 10, 3, 3, "k", "C"), Expired, 0)
	expect("a delete request", SessionCommand{Index: 24, ClientID: 21, RequestID: 1, AckedBelow: 1, Kind: Delete, Key: "j"}, Executed, 24)
	expect("its retry", SessionCommand{Index: 25, ClientID: 21, RequestID: 1, AckedBelow: 1, Kind: Delete, Key: "j"}, Duplicate, 24)
	expect("same id as a put: a different command", put(26, 21, 1, 1, "j", ""), Conflict, 0)
	expect("AckedBelow above the request id is clamped", put(27, 21, 2, 9, "j", "v"), Executed, 27)
	if w, rids, _ := m.SessionInfo(21); w != 2 || len(rids) != 1 || rids[0] != 2 {
		t.Fatalf("session 21: AckedBelow %d, remembered %v", w, rids)
	}
}
