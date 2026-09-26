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
	"sort"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/record"
)

var members3 = []string{"n1", "n2", "n3"}

func meta(index, term uint64) Meta { return Meta{Members: members3, Index: index, Term: term} }

func mustEncode(t *testing.T, m Meta, data []byte) []byte {
	t.Helper()
	b, err := Encode(m, data)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestRoundTripAndDeterminism: every state size — empty, one byte, exactly one
// data record, several — round-trips, and the same input always encodes to the
// same bytes.
func TestRoundTripAndDeterminism(t *testing.T) {
	for _, n := range []int{0, 1, MaxDataRecord - 1, MaxDataRecord, MaxDataRecord + 1, 3*MaxDataRecord + 17} {
		data := bytes.Repeat([]byte{byte(n)}, n)
		for i := range data {
			data[i] ^= byte(i)
		}
		a := mustEncode(t, meta(42, 7), data)
		if b := mustEncode(t, meta(42, 7), data); !bytes.Equal(a, b) {
			t.Fatalf("%d bytes: two encodings differ", n)
		}
		m, got, err := Decode(a)
		if err != nil {
			t.Fatalf("%d bytes: %v", n, err)
		}
		if m.Index != 42 || m.Term != 7 || !SameGroup(m.Members, members3) || !bytes.Equal(got, data) {
			t.Fatalf("%d bytes: decoded %+v, %d bytes", n, m, len(got))
		}
	}
}

// TestEncodeRefusesInvalidMetadata: index and term of at least 1, a non-empty
// strictly ascending member list of bounded ids, a bounded state.
func TestEncodeRefusesInvalidMetadata(t *testing.T) {
	for name, m := range map[string]Meta{
		"index 0":         {Members: members3, Index: 0, Term: 1},
		"term 0":          {Members: members3, Index: 1, Term: 0},
		"no members":      {Index: 1, Term: 1},
		"unsorted":        {Members: []string{"n2", "n1"}, Index: 1, Term: 1},
		"duplicate":       {Members: []string{"n1", "n1"}, Index: 1, Term: 1},
		"empty member id": {Members: []string{""}, Index: 1, Term: 1},
		"long member id":  {Members: []string{strings.Repeat("x", MaxMemberLen+1)}, Index: 1, Term: 1},
	} {
		if _, err := Encode(m, nil); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if !SameGroup(Group([]string{"n3", "n1", "n2"}), members3) {
		t.Fatal("Group must sort")
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

// header builds a header payload with full control over every field.
func header(magicStr string, version uint64, members []string, index, term, dataLen uint64, sum [32]byte) []byte {
	h := []byte(magicStr)
	h = binary.AppendUvarint(h, version)
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
	type entry = struct {
		file []byte
		want error
	}
	c := map[string]entry{
		// Known good.
		"good/state":             {good, nil},
		"good/empty-state":       {mustEncode(t, meta(1, 1), nil), nil},
		"good/single-member":     {mustEncode(t, Meta{Members: []string{"solo"}, Index: 5, Term: 5}, data), nil},
		"good/multi-record":      {mustEncode(t, meta(99, 4), bytes.Repeat([]byte{7}, 2*MaxDataRecord+5)), nil},
		"good/large-index-terms": {mustEncode(t, meta(1<<62, 1<<40), data), nil},

		// Known bad: structure.
		"bad/empty-file":          {nil, ErrCorrupt},
		"bad/truncated-in-header": {good[:20], ErrCorrupt},
		"bad/truncated-in-data":   {good[:len(good)-15], ErrCorrupt},
		"bad/missing-footer":      {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data)), ErrCorrupt},
		"bad/bytes-after-footer":  {cat(good, rec(t, kindData, []byte("x"))), ErrCorrupt},
		"bad/two-headers":         {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, uint64(len(data)), sum)), good), ErrCorrupt},
		"bad/data-first":          {cat(rec(t, kindData, data), good), ErrCorrupt},
		"bad/unknown-kind":        {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, 0, empty)), rec(t, 9, []byte("?")), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/empty-data-record":   {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, 0, empty)), rec(t, kindData, nil), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},

		// Known bad: checksums and lengths.
		"bad/corrupted-crc":         {func() []byte { b := append([]byte(nil), good...); b[len(b)-1] ^= 0x40; return b }(), ErrCorrupt},
		"bad/state-hash-mismatch":   {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, uint64(len(data)), sha256.Sum256([]byte("other")))), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/data-shorter":          {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, uint64(len(data)+1), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/data-longer":           {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, 3, sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/oversized-state":       {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, MaxData+1, sum)), rec(t, kindFooter, footer(10, 2))), ErrTooLarge},
		"bad/oversized-data-record": {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, MaxDataRecord+1, empty)), rec(t, kindData, make([]byte, MaxDataRecord+1)), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},

		// Known bad: metadata.
		"bad/wrong-magic":          {cat(rec(t, kindHeader, header("QSNX", 1, members3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/future-version":       {cat(rec(t, kindHeader, header(magic, 2, members3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrVersion},
		"bad/zero-index":           {cat(rec(t, kindHeader, header(magic, 1, members3, 0, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(0, 2))), ErrCorrupt},
		"bad/zero-term":            {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 0, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 0))), ErrCorrupt},
		"bad/footer-wrong-term":    {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 3))), ErrCorrupt},
		"bad/footer-wrong-index":   {cat(rec(t, kindHeader, header(magic, 1, members3, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(11, 2))), ErrCorrupt},
		"bad/members-unsorted":     {cat(rec(t, kindHeader, header(magic, 1, []string{"n2", "n1"}, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/members-duplicate":    {cat(rec(t, kindHeader, header(magic, 1, []string{"n1", "n1"}, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/no-members":           {cat(rec(t, kindHeader, header(magic, 1, nil, 10, 2, uint64(len(data)), sum)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/header-trailing-byte": {cat(rec(t, kindHeader, append(header(magic, 1, members3, 10, 2, uint64(len(data)), sum), 0)), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
		"bad/non-canonical-index": {cat(rec(t, kindHeader, func() []byte {
			h := header(magic, 1, members3, 0, 2, uint64(len(data)), sum)
			// Replace the one-byte index 0 by an overlong 10 (0x8a 0x00).
			i := len(magic) + 1 + 1 + 3*3
			return append(append(append([]byte(nil), h[:i]...), 0x8a, 0x00), h[i+1:]...)
		}()), rec(t, kindData, data), rec(t, kindFooter, footer(10, 2))), ErrCorrupt},
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
		b, _ := Encode(Meta{Members: members3, Index: 3, Term: 2}, bytes.Repeat([]byte("z"), n))
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
	file, _ := Encode(Meta{Members: []string{"n1", "n2", "n3"}, Index: 100, Term: 3}, []byte("state"))
	m, data, err := Decode(file)
	fmt.Println(m.Index, m.Term, string(data), err)
	// Output: 100 3 state <nil>
}
