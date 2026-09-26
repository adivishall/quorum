package kv

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/adivishall/quorum/internal/lincheck"
)

// genSessionCommands draws a random sequence of committed commands built to
// reach every decision of the contract: registrations (with eviction pressure),
// new requests, retries of executed ones (duplicates), the same id with another
// command (conflicts), ids below the watermark (stale), unknown and evicted
// sessions (expired), too many unacknowledged results (limit), watermark jumps,
// anonymous writes and deletes. Keys and values come from tiny domains so
// states collide.
func genSessionCommands(rng *rand.Rand, n int) []Command {
	type client struct {
		id, next, acked uint64
		sent            map[uint64]Command
	}
	var clients []*client
	var out []Command
	index := uint64(0)
	for len(out) < n {
		index++
		switch r := rng.Intn(100); {
		case r < 8 || len(clients) == 0:
			out = append(out, Command{Op: OpRegister})
			clients = append(clients, &client{id: index, next: 1, acked: 1, sent: map[uint64]Command{}})
		case r < 14:
			out = append(out, randomWrite(rng, 0, 0, 0))
		default:
			c := clients[rng.Intn(len(clients))]
			if rng.Intn(20) == 0 {
				c.acked = c.next // acknowledge everything so far
			}
			var cmd Command
			switch k := rng.Intn(10); {
			case k < 5 || len(c.sent) == 0: // a new request
				cmd = randomWrite(rng, c.id, c.next, c.acked)
				c.sent[c.next] = cmd
				c.next++
			case k < 8: // a retry, possibly with a newer watermark
				rid := anyKey(rng, c.sent)
				cmd = c.sent[rid]
				cmd.AckedBelow = min(c.acked, rid)
			case k < 9: // the same id, another command
				rid := anyKey(rng, c.sent)
				cmd = randomWrite(rng, c.id, rid, min(c.acked, rid))
			default: // an unknown session
				cmd = randomWrite(rng, c.id+1000, 1, 1)
			}
			out = append(out, cmd)
		}
	}
	return out
}

func randomWrite(rng *rand.Rand, cid, rid, acked uint64) Command {
	c := Command{Key: []byte(fmt.Sprintf("k%d", rng.Intn(3))), ClientID: cid, RequestID: rid, AckedBelow: acked}
	if rng.Intn(3) == 0 {
		c.Op = OpDelete
	} else {
		c.Op, c.Value = OpPut, []byte([]string{"", "a", "b"}[rng.Intn(3)])
	}
	return c
}

func anyKey(rng *rand.Rand, m map[uint64]Command) uint64 {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys[rng.Intn(len(keys))]
}

func modelCommand(index uint64, c Command) lincheck.SessionCommand {
	mc := lincheck.SessionCommand{Index: index, Register: c.Op == OpRegister, ClientID: c.ClientID, RequestID: c.RequestID,
		AckedBelow: c.AckedBelow, Key: string(c.Key), Value: string(c.Value), Kind: lincheck.Put}
	if c.Op == OpDelete {
		mc.Kind = lincheck.Delete
	}
	return mc
}

var decisionOf = map[lincheck.Decision]Decision{
	lincheck.Executed: Executed, lincheck.Duplicate: Duplicate, lincheck.Conflict: Conflict, lincheck.Stale: Stale,
	lincheck.Expired: Expired, lincheck.Limit: Limit, lincheck.Registered: Registered,
}

// TestStoreAgreesWithTheSessionModel is the reference-model differential for
// request identity: thousands of random committed-command sequences, applied to
// the replicated state machine and to the independent lincheck.SessionModel;
// every command's decision and reported index, every key's state and the whole
// session table must agree after every step. Small limits make eviction and the
// unacknowledged-results limit routine. Every decision must occur often.
func TestStoreAgreesWithTheSessionModel(t *testing.T) {
	limits := Limits{MaxSessions: 3, MaxUnacked: 4}
	rng := rand.New(rand.NewSource(1313))
	seen := map[Decision]int{}
	for run := 0; run < 400; run++ {
		store := NewStoreWithLimits(limits)
		model := lincheck.NewSessionModel(lincheck.SessionLimits{MaxSessions: limits.MaxSessions, MaxUnacked: limits.MaxUnacked})
		cmds := genSessionCommands(rng, 60)
		for i, c := range cmds {
			index := uint64(i + 1)
			res, err := store.ApplyResult(index, c.Encode())
			if err != nil {
				t.Fatalf("run %d index %d: %v", run, index, err)
			}
			got := res.(Result)
			wantD, wantIdx := model.Apply(modelCommand(index, c))
			if got.Decision != decisionOf[wantD] || got.Index != wantIdx {
				t.Fatalf("run %d index %d %+v: store %s@%d, model %s@%d", run, index, c, got.Decision, got.Index, wantD, wantIdx)
			}
			seen[got.Decision]++
			requireSameSessions(t, store, model)
		}
		for _, k := range []string{"k0", "k1", "k2"} {
			v, ok := store.Get([]byte(k))
			if st := model.State(k); ok != st.Present || (ok && string(v) != st.Value) {
				t.Fatalf("run %d key %s: store (%v,%q) model %+v", run, k, ok, v, st)
			}
		}
	}
	for d := Executed; d <= Registered; d++ {
		if seen[d] < 200 {
			t.Fatalf("decision %s occurred only %d times: %v", d, seen[d], seen)
		}
	}
	t.Logf("decisions: %v", seen)
}

func requireSameSessions(t *testing.T, store *Store, model *lincheck.SessionModel) {
	t.Helper()
	table := store.Sessions()
	ids := model.Sessions()
	if len(ids) != len(table) {
		t.Fatalf("store has %d sessions, model %v", len(table), ids)
	}
	for _, id := range ids {
		st, ok := table[id]
		acked, rids, _ := model.SessionInfo(id)
		sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
		if !ok || st.AckedBelow != acked || fmt.Sprint(st.Requests) != fmt.Sprint(rids) {
			t.Fatalf("session %d: store %+v (present %v), model acked=%d rids=%v", id, st, ok, acked, rids)
		}
	}
}

// TestReplayRebuildsTheSessionTable is the recovery half of the argument
// (docs/DEDUP.md §4): the session table is a function of the committed log, so a
// restarted node — a fresh Store re-applying its committed prefix from index 1,
// as raftnode does — rebuilds it exactly, and every command after the restart is
// decided exactly as it would have been without one. Physical replay is not a
// duplicate of anything.
func TestReplayRebuildsTheSessionTable(t *testing.T) {
	rng := rand.New(rand.NewSource(77))
	limits := Limits{MaxSessions: 3, MaxUnacked: 4}
	for run := 0; run < 200; run++ {
		cmds := genSessionCommands(rng, 50)
		cut := 1 + rng.Intn(len(cmds)-1)
		steady := NewStoreWithLimits(limits)
		var steadyResults []any
		for i, c := range cmds {
			r, _ := steady.ApplyResult(uint64(i+1), c.Encode())
			steadyResults = append(steadyResults, r)
		}
		// The dead incarnation applied `cut` entries; the restarted one starts
		// empty and replays the committed prefix from index 1 — at the cut its
		// table must equal the dead one's, and from there on every decision
		// must be the one an uninterrupted node made.
		dead := NewStoreWithLimits(limits)
		for i, c := range cmds[:cut] {
			_, _ = dead.ApplyResult(uint64(i+1), c.Encode())
		}
		restarted := NewStoreWithLimits(limits)
		for i, c := range cmds {
			r, _ := restarted.ApplyResult(uint64(i+1), c.Encode())
			if r != steadyResults[i] {
				t.Fatalf("run %d: entry %d decided %v after the restart at %d, %v without it", run, i+1, r, cut, steadyResults[i])
			}
			if i+1 == cut && (fmt.Sprint(restarted.Sessions()) != fmt.Sprint(dead.Sessions()) || fmt.Sprint(restarted.Snapshot()) != fmt.Sprint(dead.Snapshot())) {
				t.Fatalf("run %d: replaying %d entries rebuilt a different table than the incarnation that applied them", run, cut)
			}
		}
		if fmt.Sprint(restarted.Sessions()) != fmt.Sprint(steady.Sessions()) || fmt.Sprint(restarted.Snapshot()) != fmt.Sprint(steady.Snapshot()) {
			t.Fatalf("run %d: replay rebuilt a different state", run)
		}
	}
}

// TestIdentifiedCommandCodec: identified commands and REGISTER round-trip; the
// fingerprint ignores identity and watermark; out-of-contract identities are
// refused by Validate and by Decode alike.
func TestIdentifiedCommandCodec(t *testing.T) {
	good := []Command{
		{Op: OpRegister},
		{Op: OpPut, Key: []byte("k"), Value: []byte{}, ClientID: 7, RequestID: 3, AckedBelow: 2},
		{Op: OpDelete, Key: []byte("k"), ClientID: 1 << 40, RequestID: 1 << 50, AckedBelow: 1 << 50},
		{Op: OpPut, Key: []byte("k"), Value: []byte("v")}, // anonymous: the Phase 12 bytes
	}
	for _, c := range good {
		if err := c.Validate(); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		got, err := Decode(c.Encode())
		if err != nil || got.Op != c.Op || got.ClientID != c.ClientID || got.RequestID != c.RequestID || got.AckedBelow != c.AckedBelow || !bytes.Equal(got.Key, c.Key) {
			t.Fatalf("%+v -> %+v, %v", c, got, err)
		}
	}
	if !bytes.Equal((Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")}).Encode(), []byte{1, 1, 'k', 1, 'v'}) {
		t.Fatal("an anonymous command's encoding changed from Phase 12")
	}
	a := Command{Op: OpPut, Key: []byte("k"), Value: []byte("v"), ClientID: 7, RequestID: 3, AckedBelow: 1}
	b := a
	b.AckedBelow, b.ClientID = 3, 9
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("the fingerprint must identify the effect, not the identity or watermark")
	}
	b.Value = []byte("w")
	if a.Fingerprint() == b.Fingerprint() {
		t.Fatal("different values, same fingerprint")
	}
	bad := map[string]Command{
		"identified, request 0":         {Op: OpPut, Key: []byte("k"), ClientID: 7, RequestID: 0, AckedBelow: 1},
		"identified, acked 0":           {Op: OpPut, Key: []byte("k"), ClientID: 7, RequestID: 3, AckedBelow: 0},
		"acked above request":           {Op: OpPut, Key: []byte("k"), ClientID: 7, RequestID: 3, AckedBelow: 4},
		"anonymous with a request id":   {Op: OpPut, Key: []byte("k"), RequestID: 3},
		"register with a key":           {Op: OpRegister, Key: []byte("k")},
		"register with an identity":     {Op: OpRegister, ClientID: 7},
		"identified delete with value":  {Op: OpDelete, Key: []byte("k"), Value: []byte("v"), ClientID: 7, RequestID: 1, AckedBelow: 1},
		"identified with an empty key":  {Op: OpPut, ClientID: 7, RequestID: 1, AckedBelow: 1},
		"identified with a huge key":    {Op: OpPut, Key: make([]byte, MaxKeyLen+1), ClientID: 7, RequestID: 1, AckedBelow: 1},
		"identified with a huge value":  {Op: OpPut, Key: []byte("k"), Value: make([]byte, MaxValueLen+1), ClientID: 7, RequestID: 1, AckedBelow: 1},
		"an op that is not a write":     {Op: 9, Key: []byte("k")},
		"identified register-like zero": {Op: OpPut, Key: []byte("k"), ClientID: 0, RequestID: 1, AckedBelow: 1},
	}
	for name, c := range bad {
		if c.Validate() == nil {
			t.Fatalf("%s: Validate accepted %+v", name, c)
		}
	}
	for name, b := range map[string][]byte{
		"client 0 in an identified encoding": {opPutIdentified, 0, 1, 1, 1, 'k', 0},
		"acked above request":                {opPutIdentified, 7, 1, 2, 1, 'k', 0},
		"non-canonical client id":            {opPutIdentified, 0x87, 0x00, 1, 1, 1, 'k', 0},
		"truncated identity":                 {opDeleteIdentified, 7, 1},
		"register with trailing bytes":       {byte(OpRegister), 0},
	} {
		if _, err := Decode(b); !errors.Is(err, ErrMalformedCommand) {
			t.Fatalf("%s: Decode = %v, want ErrMalformedCommand", name, err)
		}
	}
}
