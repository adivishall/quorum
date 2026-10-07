package snapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
)

func mem(ids ...string) []replication.Member {
	out := make([]replication.Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, replication.Member{ID: replication.NodeID(id), Addr: "127.0.0.1:" + id})
	}
	return out
}

var conf3 = replication.Configuration{Voters: mem("n1", "n2", "n3")}

func meta(index, term uint64) Meta { return Meta{Group: 0, Conf: conf3, Index: index, Term: term} }

func mustEncode(t *testing.T, m Meta, data []byte) []byte {
	t.Helper()
	b, err := Encode(m, data)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestRoundTripAndDeterminism: every state size — empty, one byte, exactly one
// data record, several — round-trips with its group and configuration, and the
// same input always encodes to the same bytes.
func TestRoundTripAndDeterminism(t *testing.T) {
	joint := replication.Configuration{Voters: mem("n1", "n2", "n4"), Outgoing: mem("n1", "n2", "n3"), Learners: mem("n5")}
	for _, n := range []int{0, 1, MaxDataRecord - 1, MaxDataRecord, MaxDataRecord + 1, 3*MaxDataRecord + 17} {
		data := bytes.Repeat([]byte{byte(n)}, n)
		for i := range data {
			data[i] ^= byte(i)
		}
		want := Meta{Group: 7, Conf: joint, Index: 42, Term: 7}
		a := mustEncode(t, want, data)
		if b := mustEncode(t, want, data); !bytes.Equal(a, b) {
			t.Fatalf("%d bytes: two encodings differ", n)
		}
		m, got, err := Decode(a)
		if err != nil {
			t.Fatalf("%d bytes: %v", n, err)
		}
		if m.Index != 42 || m.Term != 7 || m.Group != 7 || !m.Conf.Equal(joint) || !bytes.Equal(got, data) {
			t.Fatalf("%d bytes: decoded %+v, %d bytes", n, m, len(got))
		}
	}
}

// TestEncodeRefusesInvalidMetadata: index and term of at least 1, a valid
// configuration with at least one voter, a bounded state.
func TestEncodeRefusesInvalidMetadata(t *testing.T) {
	for name, m := range map[string]Meta{
		"index 0":            {Conf: conf3, Index: 0, Term: 1},
		"term 0":             {Conf: conf3, Index: 1, Term: 0},
		"empty conf":         {Index: 1, Term: 1},
		"learners only":      {Conf: replication.Configuration{Learners: mem("n4")}, Index: 1, Term: 1},
		"unsorted voters":    {Conf: replication.Configuration{Voters: mem("n2", "n1")}, Index: 1, Term: 1},
		"duplicate voter":    {Conf: replication.Configuration{Voters: mem("n1", "n1")}, Index: 1, Term: 1},
		"empty member id":    {Conf: replication.Configuration{Voters: []replication.Member{{ID: ""}}}, Index: 1, Term: 1},
		"long member id":     {Conf: replication.Configuration{Voters: []replication.Member{{ID: replication.NodeID(strings.Repeat("x", replication.MaxMemberLen+1))}}}, Index: 1, Term: 1},
		"learner is a voter": {Conf: replication.Configuration{Voters: mem("n1", "n2"), Learners: mem("n2")}, Index: 1, Term: 1},
	} {
		if _, err := Encode(m, nil); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestLargestConfigurationFits: the largest valid configuration — every member
// at the id and address bounds, a joint one listing all of them as voters and
// all but one of them again as outgoing voters (a removal in progress; an
// outgoing set equal to the voters is not a valid joint configuration) — fits
// the header's bound and round-trips.
func TestLargestConfigurationFits(t *testing.T) {
	var all []replication.Member
	for i := 0; i < replication.MaxMembers; i++ {
		id := fmt.Sprintf("%03d", i) + strings.Repeat("i", replication.MaxMemberLen-3)
		all = append(all, replication.Member{ID: replication.NodeID(id), Addr: strings.Repeat("a", replication.MaxMemberLen)})
	}
	c := replication.Configuration{Voters: all, Outgoing: all[:len(all)-1]}
	if n := len(replication.EncodeConfiguration(c)); n > replication.MaxEncodedConfiguration {
		t.Fatalf("the largest configuration encodes to %d bytes, over the %d bound", n, replication.MaxEncodedConfiguration)
	}
	m, _, err := Decode(mustEncode(t, Meta{Group: 1, Conf: c, Index: 1, Term: 1}, []byte("s")))
	if err != nil || !m.Conf.Equal(c) {
		t.Fatalf("round trip: %v", err)
	}
}

// TestEveryTruncationAndEveryBitFlipIsCorruption: a snapshot cut at any byte,
// or with any single bit flipped, never decodes — it is refused, never repaired
// or half-read.
func TestEveryTruncationAndEveryBitFlipIsCorruption(t *testing.T) {
	data := bytes.Repeat([]byte("state"), 700) // several KiB: header, 1 data record, footer
	file := mustEncode(t, meta(9, 3), data)
	for n := 0; n < len(file); n++ {
		if _, _, err := Decode(file[:n]); err == nil {
			t.Fatalf("a snapshot truncated to %d of %d bytes decoded", n, len(file))
		}
	}
	for i := 0; i < len(file); i++ {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), file...)
			bad[i] ^= 1 << bit
			if _, _, err := Decode(bad); err == nil {
				t.Fatalf("a flip of bit %d of byte %d decoded", bit, i)
			}
		}
	}
}

// rec builds one framed record.
func rec(t *testing.T, kind record.Kind, payload []byte) []byte {
	t.Helper()
	b, err := record.Encode(nil, kind, payload)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// header builds a header payload with full control over every field; conf is
// the configuration's bytes as they appear on disk.
func header(magicStr string, version, group uint64, conf []byte, index, term, dataLen uint64, sum [32]byte) []byte {
	h := headerPrefix(magicStr, version, group, conf)
	h = binary.AppendUvarint(h, index)
	h = binary.AppendUvarint(h, term)
	h = binary.AppendUvarint(h, dataLen)
	return append(h, sum[:]...)
}

// headerPrefix is a header up to (not including) its index.
func headerPrefix(magicStr string, version, group uint64, conf []byte) []byte {
	h := []byte(magicStr)
	h = binary.AppendUvarint(h, version)
	h = binary.AppendUvarint(h, group)
	h = binary.AppendUvarint(h, uint64(len(conf)))
	return append(h, conf...)
}

// headerV1 is a Phase 14 (format version 1) header: a member list as the
// identity, no group id, no configuration.
func headerV1(members []string, index, term, dataLen uint64, sum [32]byte) []byte {
	h := []byte(magic)
	h = binary.AppendUvarint(h, 1)
	h = binary.AppendUvarint(h, uint64(len(members)))
	for _, m := range members {
		h = binary.AppendUvarint(h, uint64(len(m)))
		h = append(h, m...)
	}
	h = binary.AppendUvarint(h, index)
	h = binary.AppendUvarint(h, term)
	h = binary.AppendUvarint(h, dataLen)
	return append(h, sum[:]...)
}

func footer(index, term uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(nil, index), term)
}

// corpus returns the known-good and known-bad snapshot files: name → (file,
// the error class a bad one must be refused with, or nil for a good one).
func corpus(t *testing.T) map[string]struct {
	file []byte
	want error
} {
	t.Helper()
	data := []byte("the state machine's bytes")
	sum := sha256.Sum256(data)
	empty := sha256.Sum256(nil)
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	good := mustEncode(t, meta(10, 2), data)
	c3 := replication.EncodeConfiguration(conf3)
	// withConf is a whole file around a header carrying conf's bytes verbatim.
	withConf := func(conf []byte) []byte {
		return cat(rec(t, kindHeader, header(magic, Version, 0, conf, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2)))
	}
	type entry = struct {
		file []byte
		want error
	}
	c := map[string]entry{
		// Known good.
		"good/state":             {good, nil},
		"good/empty-state":       {mustEncode(t, meta(1, 1), nil), nil},
		"good/single-voter":      {mustEncode(t, Meta{Conf: replication.Configuration{Voters: mem("solo")}, Index: 5, Term: 5}, data), nil},
		"good/no-addresses":      {mustEncode(t, Meta{Conf: replication.VotersOf([]replication.NodeID{"n1", "n2", "n3"}), Index: 5, Term: 5}, data), nil},
		"good/with-learners":     {mustEncode(t, Meta{Conf: replication.Configuration{Voters: mem("n1", "n2", "n3"), Learners: mem("n4")}, Index: 6, Term: 2}, data), nil},
		"good/joint":             {mustEncode(t, Meta{Conf: replication.Configuration{Voters: mem("n1", "n2", "n4"), Outgoing: mem("n1", "n2", "n3")}, Index: 7, Term: 2}, data), nil},
		"good/group-7":           {mustEncode(t, Meta{Group: 7, Conf: conf3, Index: 10, Term: 2}, data), nil},
		"good/largest-group-id":  {mustEncode(t, Meta{Group: 1<<32 - 1, Conf: conf3, Index: 10, Term: 2}, data), nil},
		"good/multi-record":      {mustEncode(t, meta(99, 4), bytes.Repeat([]byte{7}, 2*MaxDataRecord+5)), nil},
		"good/large-index-terms": {mustEncode(t, meta(1<<62, 1<<40), data), nil},

		// Known bad: structure.
		"bad/empty-file":          {nil, ErrCorrupt},
		"bad/truncated-in-header": {good[:20], ErrCorrupt},
		"bad/truncated-in-data":   {good[:len(good)-15], ErrCorrupt},
		"bad/missing-footer":      {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data)), ErrCorrupt},
		"bad/bytes-after-footer":  {cat(good, rec(t, kindData, []byte("x"))), ErrCorrupt},
		// Found by FuzzDecode: a remainder shorter than a record header looked
		// like a clean end of file to the framing's reader.
		"bad/one-stray-byte-after-footer":    {cat(good, []byte("0")), ErrCorrupt},
		"bad/eight-stray-bytes-after-footer": {cat(good, []byte("01234567")), ErrCorrupt},
		"bad/two-headers":                    {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, uint64(len(data)), sum)), good), ErrCorrupt},
		"bad/data-first":                     {cat(rec(t, kindData, data), good), ErrCorrupt},
		"bad/unknown-kind":                   {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, 0, empty)), rec(t, 9, []byte("?")), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/empty-data-record":              {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, 0, empty)), rec(t, kindData, nil), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},

		// Known bad: checksums and lengths.
		"bad/corrupted-crc":         {func() []byte { b := append([]byte(nil), good...); b[len(b)-1] ^= 0x40; return b }(), ErrCorrupt},
		"bad/state-hash-mismatch":   {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, uint64(len(data)), sha256.Sum256([]byte("other")))), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/data-shorter":          {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, uint64(len(data)+1), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/data-longer":           {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, 3, sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/oversized-state":       {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, MaxData+1, sum)), rec(t, kindFooter, footer(10, 2))), ErrTooLarge},
		"bad/oversized-data-record": {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, MaxDataRecord+1, empty)), rec(t, kindData, make([]byte, MaxDataRecord+1)), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},

		// Known bad: metadata.
		"bad/wrong-magic":          {cat(rec(t, kindHeader, header("QSNX", Version, 0, c3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/future-version":       {cat(rec(t, kindHeader, header(magic, Version+1, 0, c3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrVersion},
		"bad/phase14-version-1":    {cat(rec(t, kindHeader, headerV1([]string{"n1", "n2", "n3"}, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrVersion},
		"bad/zero-index":           {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 0, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(0, 2))), ErrCorrupt},
		"bad/zero-term":            {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 0, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 0))), ErrCorrupt},
		"bad/footer-wrong-term":    {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 3))), ErrCorrupt},
		"bad/footer-wrong-index":   {cat(rec(t, kindHeader, header(magic, Version, 0, c3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(11, 2))), ErrCorrupt},
		"bad/group-over-32-bits":   {cat(rec(t, kindHeader, header(magic, Version, 1<<32, c3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/header-trailing-byte": {cat(rec(t, kindHeader, append(header(magic, Version, 0, c3, 10, 2, uint64(len(data)), sum), 0)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/non-canonical-index": {cat(rec(t, kindHeader, func() []byte {
			// The index 10 as an overlong uvarint (0x8a 0x00).
			h := append(headerPrefix(magic, Version, 0, c3), 0x8a, 0x00)
			h = binary.AppendUvarint(binary.AppendUvarint(h, 2), uint64(len(data)))
			return append(h, sum[:]...)
		}()), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},

		// Known bad: the configuration (Phase 15).
		"bad/conf-absent":             {withConf(nil), ErrCorrupt},
		"bad/conf-undecodable":        {withConf([]byte{0xff}), ErrCorrupt},
		"bad/conf-future-version":     {withConf(append([]byte{2}, c3[1:]...)), ErrCorrupt},
		"bad/conf-trailing-byte":      {withConf(append(append([]byte(nil), c3...), 0)), ErrCorrupt},
		"bad/conf-no-voters":          {withConf(replication.EncodeConfiguration(replication.Configuration{Learners: mem("n4")})), ErrCorrupt},
		"bad/conf-empty":              {withConf(replication.EncodeConfiguration(replication.Configuration{})), ErrCorrupt},
		"bad/conf-voters-unsorted":    {withConf(replication.EncodeConfiguration(replication.Configuration{Voters: mem("n2", "n1")})), ErrCorrupt},
		"bad/conf-duplicate-voter":    {withConf(replication.EncodeConfiguration(replication.Configuration{Voters: mem("n1", "n1")})), ErrCorrupt},
		"bad/conf-learner-is-a-voter": {withConf(replication.EncodeConfiguration(replication.Configuration{Voters: mem("n1", "n2"), Learners: mem("n2")})), ErrCorrupt},
		// A joint configuration whose outgoing voters are its voters is the
		// stable one represented twice; one member may not carry two addresses.
		"bad/conf-joint-outgoing-equals-voters": {withConf(replication.EncodeConfiguration(replication.Configuration{Voters: mem("n1", "n2"), Outgoing: mem("n1", "n2")})), ErrCorrupt},
		"bad/conf-joint-member-two-addresses": {withConf(replication.EncodeConfiguration(replication.Configuration{Voters: mem("n1", "n2"),
			Outgoing: []replication.Member{{ID: "n1", Addr: "elsewhere"}, {ID: "n3", Addr: "127.0.0.1:n3"}}})), ErrCorrupt},
		"bad/conf-length-over-bound": {cat(rec(t, kindHeader, header(magic, Version, 0, make([]byte, replication.MaxEncodedConfiguration+1), 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
	}
	return c
}

var update = flag.Bool("update", false, "rewrite the snapshot corpus in testdata/corpus")

// TestCorpus: every known-good snapshot decodes, every known-bad one is refused
// with its error class. The corpus is committed under testdata/corpus (so a
// format change shows up as a diff); -update rewrites it.
func TestCorpus(t *testing.T) {
	c := corpus(t)
	names := make([]string, 0, len(c))
	for n := range c {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		e := c[name]
		path := filepath.Join("testdata", "corpus", name+".snap")
		if *update {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, e.file, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		onDisk, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v (run with -update to write the corpus)", name, err)
		}
		if !bytes.Equal(onDisk, e.file) {
			t.Fatalf("%s: the committed corpus file differs from what the encoder produces now", name)
		}
		_, _, err = Decode(onDisk)
		switch {
		case e.want == nil && err != nil:
			t.Errorf("%s: known-good refused: %v", name, err)
		case e.want != nil && !errors.Is(err, e.want):
			t.Errorf("%s: got %v, want %v", name, err, e.want)
		}
	}
	good, bad := 0, 0
	for _, n := range names {
		if strings.HasPrefix(n, "good/") {
			good++
		} else {
			bad++
		}
	}
	t.Logf("corpus: %d known-good, %d known-bad", good, bad)
}

// FuzzDecode: Decode never panics, and whatever it accepts re-encodes to the
// same bytes (the format is canonical).
func FuzzDecode(f *testing.F) {
	for _, n := range []int{0, 5, 300} {
		b, _ := Encode(meta(3, 2), bytes.Repeat([]byte("z"), n))
		f.Add(b)
	}
	f.Add([]byte("QSNP"))
	f.Fuzz(func(t *testing.T, b []byte) {
		m, data, err := Decode(b)
		if err != nil {
			return
		}
		again, err := Encode(m, data)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("accepted a non-canonical snapshot: %v", err)
		}
	})
}

// FuzzUnmarshalChunk: the chunk decoder is total and canonical.
func FuzzUnmarshalChunk(f *testing.F) {
	f.Add(Chunk{Term: 2, Index: 5, SnapTerm: 1, Total: 10, Offset: 3, Data: []byte("abc")}.Marshal())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := UnmarshalChunk(b)
		if err != nil {
			return
		}
		if !bytes.Equal(c.Marshal(), b) {
			t.Fatal("accepted a non-canonical chunk")
		}
	})
}

func Example() {
	conf := replication.VotersOf([]replication.NodeID{"n1", "n2", "n3"})
	file, _ := Encode(Meta{Group: 1, Conf: conf, Index: 100, Term: 3}, []byte("state"))
	m, data, err := Decode(file)
	fmt.Println(m.Index, m.Term, string(data), err)
	// Output: 100 3 state <nil>
}

// TestDecodeAllocatesNoMoreThanItsInput (audit M1): a header declaring the
// largest state, MaxData, in a file of a few hundred bytes — what a peer's
// chunks can deliver — is refused without allocating what it declares; the
// state's buffer is sized by the bytes that exist.
func TestDecodeAllocatesNoMoreThanItsInput(t *testing.T) {
	m := meta(9, 2)
	h := []byte(magic)
	h = binary.AppendUvarint(h, Version)
	h = binary.AppendUvarint(h, uint64(m.Group))
	conf := replication.EncodeConfiguration(m.Conf)
	h = binary.AppendUvarint(h, uint64(len(conf)))
	h = append(h, conf...)
	h = binary.AppendUvarint(h, m.Index)
	h = binary.AppendUvarint(h, m.Term)
	h = binary.AppendUvarint(h, MaxData)
	h = append(h, make([]byte, 32)...)
	file, err := record.Encode(nil, kindHeader, h)
	if err != nil {
		t.Fatal(err)
	}
	f := binary.AppendUvarint(nil, m.Index)
	f = binary.AppendUvarint(f, m.Term)
	if file, err = record.Encode(file, kindFooter, f); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if _, _, err := Decode(file); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a %d-byte file declaring %d state bytes: %v, want ErrCorrupt", len(file), uint64(MaxData), err)
	}
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Fatalf("decoding a %d-byte file allocated %d bytes: the header's declared length sized the buffer", len(file), got)
	}
}
