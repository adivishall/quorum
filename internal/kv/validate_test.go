package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raft"
)

// TestRequestValidationRejectsEveryOutOfContractField is the input-validation
// audit of a request (docs/API.md §4): every rule has a case that breaks only
// it, each is refused as INVALID_REQUEST (ErrInvalid) before anything is
// proposed, and the boundary values just inside each rule are accepted.
func TestRequestValidationRejectsEveryOutOfContractField(t *testing.T) {
	maxKey := bytes.Repeat([]byte("k"), MaxKeyLen)
	maxValue := bytes.Repeat([]byte("v"), MaxValueLen)
	k := []byte("k")
	invalid := map[string]Request{
		"unknown op 0":                     {Op: 0, Key: k},
		"unknown op 5":                     {Op: 5, Key: k},
		"empty key":                        {Op: ReqPut, Value: []byte("v")},
		"key over the limit":               {Op: ReqGet, Key: append(maxKey, 'x')},
		"value over the limit":             {Op: ReqPut, Key: k, Value: append(maxValue, 'x')},
		"a GET with a value":               {Op: ReqGet, Key: k, Value: []byte("v")},
		"a DELETE with a value":            {Op: ReqDelete, Key: k, Value: []byte("v")},
		"anonymous with a request id":      {Op: ReqPut, Key: k, RequestID: 1},
		"anonymous with a watermark":       {Op: ReqPut, Key: k, AckedBelow: 1},
		"identified, request id 0":         {Op: ReqPut, Key: k, ClientID: 7, AckedBelow: 1},
		"identified, watermark 0":          {Op: ReqPut, Key: k, ClientID: 7, RequestID: 1},
		"identified, watermark above id":   {Op: ReqPut, Key: k, ClientID: 7, RequestID: 3, AckedBelow: 4},
		"REGISTER with a key":              {Op: ReqRegister, Key: k},
		"REGISTER with a value":            {Op: ReqRegister, Value: []byte("v")},
		"REGISTER with a client id":        {Op: ReqRegister, ClientID: 7},
		"REGISTER with a request id":       {Op: ReqRegister, RequestID: 1},
		"REGISTER with a watermark":        {Op: ReqRegister, AckedBelow: 1},
		"identified GET, watermark above":  {Op: ReqGet, Key: k, ClientID: 7, RequestID: 1, AckedBelow: 2},
		"identified DELETE, request id 0":  {Op: ReqDelete, Key: k, ClientID: 7, AckedBelow: 1},
		"identified PUT at the id ceiling": {Op: ReqPut, Key: k, ClientID: 7, RequestID: math.MaxUint64, AckedBelow: 0},
		// Within the raw value limit, but its entry is 1,048,582 bytes: over the
		// entry limit, so refused (C1; it used to be accepted and then broke the
		// leader that persisted it).
		"anonymous PUT, max value": {Op: ReqPut, Key: k, Value: maxValue},
	}
	for name, r := range invalid {
		if err := r.validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %+v validated as %v, want ErrInvalid", name, r, err)
		}
	}
	valid := map[string]Request{
		"REGISTER":                         {Op: ReqRegister},
		"anonymous PUT, empty value":       {Op: ReqPut, Key: k},
		"anonymous GET, max key":           {Op: ReqGet, Key: maxKey},
		"anonymous PUT at the entry limit": {Op: ReqPut, Key: k, Value: maxValue[:MaxCommandLen-6]},
		"identified PUT, watermark = id":   {Op: ReqPut, Key: k, ClientID: 7, RequestID: 3, AckedBelow: 3},
		"identified DELETE, watermark 1":   {Op: ReqDelete, Key: k, ClientID: 7, RequestID: 3, AckedBelow: 1},
		"identified GET":                   {Op: ReqGet, Key: k, ClientID: 7, RequestID: 1, AckedBelow: 1},
		"ids at the ceiling":               {Op: ReqPut, Key: k, ClientID: math.MaxUint64, RequestID: math.MaxUint64, AckedBelow: math.MaxUint64},
	}
	for name, r := range valid {
		if err := r.validate(); err != nil {
			t.Errorf("%s: %+v refused: %v", name, r, err)
		}
	}
}

// TestValidatedRequestsAlwaysApply: a write request the server has validated
// becomes a log command that decodes back to itself — so no entry a server
// proposed can be refused at apply as malformed on any replica — and a request
// that fails validation is never turned into one.
func TestValidatedRequestsAlwaysApply(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	pick := func(vs ...uint64) uint64 { return vs[rng.Intn(len(vs))] }
	accepted := 0
	for i := 0; i < 50000; i++ {
		r := Request{
			Op:         ReqOp(rng.Intn(6)),
			ClientID:   pick(0, 0, 1, 7, math.MaxUint64),
			RequestID:  pick(0, 1, 2, 3, math.MaxUint64),
			AckedBelow: pick(0, 1, 2, 3, math.MaxUint64),
		}
		if rng.Intn(4) > 0 {
			r.Key = make([]byte, rng.Intn(4))
		}
		if rng.Intn(2) == 0 {
			r.Value = make([]byte, rng.Intn(3))
		}
		if r.validate() != nil || r.Op == ReqGet {
			continue
		}
		accepted++
		cmd := r.command()
		got, err := Decode(cmd.Encode())
		if err != nil {
			t.Fatalf("validated %+v became a command that does not decode: %v", r, err)
		}
		if got.Op != cmd.Op || got.ClientID != cmd.ClientID || got.RequestID != cmd.RequestID || got.AckedBelow != cmd.AckedBelow ||
			!bytes.Equal(got.Key, cmd.Key) || !bytes.Equal(got.Value, cmd.Value) {
			t.Fatalf("round trip changed the command: %+v → %+v", cmd, got)
		}
		s := NewStore()
		if _, err := s.ApplyResult(1, cmd.Encode()); err != nil {
			t.Fatalf("validated %+v was refused at apply: %v", r, err)
		}
	}
	if accepted < 1000 {
		t.Fatalf("only %d requests validated: the generator does not reach the accepted space", accepted)
	}
}

// TestMalformedIdentifiersAreProtocolErrors: identifiers that are not
// canonical 64-bit varints never reach validation — the frame is refused
// (and the connection closed): an overlong encoding of a small id, an id
// longer than 64 bits, a truncated id, trailing bytes after the request.
func TestMalformedIdentifiersAreProtocolErrors(t *testing.T) {
	good := encodeRequest(Request{Op: ReqPut, ClientID: 7, RequestID: 3, AckedBelow: 3, Key: []byte("k"), Value: []byte("v")})
	if _, err := decodeRequest(good); err != nil {
		t.Fatalf("control: %v", err)
	}
	withClientID := func(id []byte) []byte {
		b := []byte{byte(ReqPut)}
		b = append(b, id...)
		return append(b, good[2:]...) // good[1] is ClientID 7, one byte
	}
	cases := map[string][]byte{
		"overlong ClientID (7 in two bytes)": withClientID([]byte{0x87, 0x00}),
		"ClientID over 64 bits":              withClientID([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}),
		"ClientID never terminated":          withClientID([]byte{0xff, 0xff, 0xff}),
		"truncated after the op":             {byte(ReqPut)},
		"trailing bytes":                     append(append([]byte(nil), good...), 0),
	}
	for name, b := range cases {
		if _, err := decodeRequest(b); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: decoded with %v, want ErrProtocol", name, err)
		}
	}
	// The largest identifiers are ordinary values, not errors.
	max := Request{Op: ReqPut, ClientID: math.MaxUint64, RequestID: math.MaxUint64, AckedBelow: math.MaxUint64, Key: []byte("k")}
	if got, err := decodeRequest(encodeRequest(max)); err != nil || got.ClientID != math.MaxUint64 || got.RequestID != math.MaxUint64 {
		t.Fatalf("max identifiers: %+v %v", got, err)
	}
}

// TestDurationsThatOverflowAreProtocolErrors pins the fix for a canonicality
// bug `make fuzz` found (FuzzDecodeRequestIsTotal, seed 4acea4bd2e6f9ba9): a
// timeoutMillis too large for a time.Duration overflowed on conversion, so the
// frame decoded to a request that re-encoded to different bytes. A request
// timeout or a forward budget beyond maxMillis is now refused; maxMillis itself
// round-trips; and the encoder never writes a negative duration, nor turns a
// positive one below a millisecond into 0 (which would mean "the default").
func TestDurationsThatOverflowAreProtocolErrors(t *testing.T) {
	req := func(ms uint64) []byte {
		b := []byte{byte(ReqGet), 0, 0, 0, 0} // op, group, client, request, acked-below
		b = binary.AppendUvarint(b, ms)
		return append(b, 1, 'k')
	}
	if _, err := decodeRequest(req(maxMillis + 1)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a timeout past time.Duration decoded: %v", err)
	}
	if _, err := decodeRequest(req(math.MaxUint64)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("the largest varint timeout decoded: %v", err)
	}
	r, err := decodeRequest(req(maxMillis))
	if err != nil || !bytes.Equal(encodeRequest(r), req(maxMillis)) {
		t.Fatalf("the largest valid timeout must round-trip: %+v %v", r, err)
	}
	fwd := func(ms uint64) []byte {
		b := binary.AppendUvarint(nil, 5)
		b = binary.AppendUvarint(b, ms)
		return append(b, req(1)...)
	}
	if _, _, _, err := decodeForward(fwd(maxMillis + 1)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a forward budget past time.Duration decoded: %v", err)
	}
	if _, budget, _, err := decodeForward(fwd(maxMillis)); err != nil || budget != time.Duration(maxMillis)*time.Millisecond {
		t.Fatalf("the largest valid budget: %v %v", budget, err)
	}
	for in, want := range map[time.Duration]uint64{-time.Second: 0, 0: 0, time.Microsecond: 1, time.Millisecond: 1, 1500 * time.Microsecond: 1, 2 * time.Second: 2000} {
		if got := millisOf(in); got != want {
			t.Errorf("millisOf(%v) = %d, want %d", in, got, want)
		}
	}
}

// largestValue is the longest value a PUT with this key and identity can carry:
// its command's encoding is then exactly MaxCommandLen bytes.
func largestValue(t *testing.T, r Request) int {
	t.Helper()
	r.Value = []byte{}
	fixed := r.command().EncodedLen() - 1 // all but the value and its length
	n := MaxCommandLen - fixed - 3        // a value near 1 MiB has a 3-byte length
	if uvarintLen(uint64(n)) != 3 {
		t.Fatalf("value length %d does not have a 3-byte uvarint", n)
	}
	return n
}

// TestEncodedEntryLimitDecidesWriteAdmission (C1): the limit that decides
// whether a write is admitted is the size of the Raft entry it becomes, not
// its raw key and value. For keys and identities from the smallest to the
// largest, the largest admissible value makes an entry of exactly MaxCommandLen
// bytes — admitted, and it decodes and applies — and one more byte is refused
// at the front, by Command.Validate and by Decode alike.
func TestEncodedEntryLimitDecidesWriteAdmission(t *testing.T) {
	maxKey := bytes.Repeat([]byte("k"), MaxKeyLen)
	cases := map[string]Request{
		"anonymous, 1-byte key":           {Op: ReqPut, Key: []byte("k")},
		"anonymous, max key":              {Op: ReqPut, Key: maxKey},
		"identified, small ids":           {Op: ReqPut, Key: []byte("k"), ClientID: 1, RequestID: 1, AckedBelow: 1},
		"identified, max ids and max key": {Op: ReqPut, Key: maxKey, ClientID: math.MaxUint64, RequestID: math.MaxUint64, AckedBelow: math.MaxUint64},
	}
	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			n := largestValue(t, r)
			if n >= MaxValueLen {
				t.Fatalf("the largest value %d is not below the raw limit %d: the encoded limit would not bind", n, MaxValueLen)
			}
			r.Value = bytes.Repeat([]byte("v"), n)
			if err := r.validate(); err != nil {
				t.Fatalf("a request whose entry is exactly %d bytes was refused: %v", MaxCommandLen, err)
			}
			enc := r.command().Encode()
			if len(enc) != MaxCommandLen || r.command().EncodedLen() != MaxCommandLen {
				t.Fatalf("encoded %d bytes (EncodedLen %d), want exactly %d", len(enc), r.command().EncodedLen(), MaxCommandLen)
			}
			if _, err := Decode(enc); err != nil {
				t.Fatalf("an entry at the limit does not decode: %v", err)
			}
			if _, err := NewStore().ApplyResult(1, enc); err != nil {
				t.Fatalf("an entry at the limit does not apply: %v", err)
			}

			r.Value = append(r.Value, 'v') // one byte over
			if err := r.validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("a request whose entry is %d bytes validated as %v, want ErrInvalid", MaxCommandLen+1, err)
			}
			if err := r.command().Validate(); !errors.Is(err, ErrMalformedCommand) {
				t.Fatalf("Command.Validate of a %d-byte command = %v, want ErrMalformedCommand", MaxCommandLen+1, err)
			}
			if _, err := Decode(r.command().Encode()); !errors.Is(err, ErrMalformedCommand) {
				t.Fatalf("Decode of a %d-byte command = %v, want ErrMalformedCommand", MaxCommandLen+1, err)
			}
		})
	}
	// A DELETE carries no value: with the longest key and identity it still fits.
	del := Request{Op: ReqDelete, Key: maxKey, ClientID: math.MaxUint64, RequestID: math.MaxUint64, AckedBelow: math.MaxUint64}
	if err := del.validate(); err != nil {
		t.Fatalf("the largest DELETE was refused: %v", err)
	}
}

// TestEncodedLenIsTheEncodingLength: EncodedLen, which admission control
// uses, is exactly len(Encode()) — including at every uvarint width boundary
// of the lengths and identity fields.
func TestEncodedLenIsTheEncodingLength(t *testing.T) {
	sizes := []int{0, 1, 127, 128, 16383, 16384, MaxKeyLen}
	ids := []uint64{1, 127, 128, 1<<14 - 1, 1 << 14, 1<<63 - 1, 1 << 63, math.MaxUint64}
	check := func(c Command) {
		t.Helper()
		if got, want := c.EncodedLen(), len(c.Encode()); got != want {
			t.Fatalf("EncodedLen(%v key=%d value=%d ids=%d/%d/%d) = %d, len(Encode()) = %d",
				c.Op, len(c.Key), len(c.Value), c.ClientID, c.RequestID, c.AckedBelow, got, want)
		}
	}
	check(Command{Op: OpRegister})
	for _, ks := range sizes {
		if ks == 0 {
			continue
		}
		key := bytes.Repeat([]byte("k"), ks)
		for _, vs := range append(sizes, 1<<20-1, 1<<20) {
			value := make([]byte, vs)
			check(Command{Op: OpPut, Key: key, Value: value})
			check(Command{Op: OpDelete, Key: key})
			for _, id := range ids {
				check(Command{Op: OpPut, Key: key, Value: value, ClientID: id, RequestID: id, AckedBelow: id})
				check(Command{Op: OpDelete, Key: key, ClientID: id, RequestID: id, AckedBelow: 1})
			}
		}
	}
}

// TestEntryTooLargeFromBelowIsInvalid: if the layer below the front refuses a
// write as too large (raft.ErrEntryTooLarge — nothing was appended), the
// client is told INVALID_REQUEST, a definite refusal, never UNKNOWN. The front
// refuses such a request first; this is the mapping for the layer below.
func TestEntryTooLargeFromBelowIsInvalid(t *testing.T) {
	s := &Server{id: "n1"}
	resp := s.failed(Response{Node: "n1"}, fmt.Errorf("propose: %w", raft.ErrEntryTooLarge))
	if resp.Status != StatusInvalid {
		t.Fatalf("a proposal refused as too large is answered %v, want %v", resp.Status, StatusInvalid)
	}
}

// TestBusyFromBelowIsUnavailable (audit M3): a leader at its bound of
// uncommitted entries or pending reads refuses with raft.ErrBusy before
// appending or registering anything — a definite no-effect, answered
// UNAVAILABLE, never the UNKNOWN that would make a client treat it as
// possibly executed.
func TestBusyFromBelowIsUnavailable(t *testing.T) {
	s := &Server{id: "n1"}
	resp := s.failed(Response{Node: "n1"}, fmt.Errorf("propose: %w", raft.ErrBusy))
	if resp.Status != StatusUnavailable {
		t.Fatalf("a request refused as busy is answered %v, want %v", resp.Status, StatusUnavailable)
	}
}
