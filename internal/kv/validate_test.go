package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"
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
	}
	for name, r := range invalid {
		if err := r.validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %+v validated as %v, want ErrInvalid", name, r, err)
		}
	}
	valid := map[string]Request{
		"REGISTER":                       {Op: ReqRegister},
		"anonymous PUT, empty value":     {Op: ReqPut, Key: k},
		"anonymous GET, max key":         {Op: ReqGet, Key: maxKey},
		"anonymous PUT, max value":       {Op: ReqPut, Key: k, Value: maxValue},
		"identified PUT, watermark = id": {Op: ReqPut, Key: k, ClientID: 7, RequestID: 3, AckedBelow: 3},
		"identified DELETE, watermark 1": {Op: ReqDelete, Key: k, ClientID: 7, RequestID: 3, AckedBelow: 1},
		"identified GET":                 {Op: ReqGet, Key: k, ClientID: 7, RequestID: 1, AckedBelow: 1},
		"ids at the ceiling":             {Op: ReqPut, Key: k, ClientID: math.MaxUint64, RequestID: math.MaxUint64, AckedBelow: math.MaxUint64},
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
		b := []byte{byte(ReqGet), 0, 0, 0}
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
