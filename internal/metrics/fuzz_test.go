package metrics

import (
	"bytes"
	"strings"
	"testing"
)

// FuzzParse: Parse never panics on any input, and whatever it accepts, it
// accepts again after the samples are written back in this package's format.
func FuzzParse(f *testing.F) {
	r := NewRegistry()
	r.CounterVec("dkv_x_total", "x", "k").With(`a"b\c`).Add(3)
	r.Histogram("dkv_h", "h", []float64{0.5, 1}).Observe(0.7)
	var b bytes.Buffer
	_ = r.WriteText(&b)
	f.Add(b.String())
	f.Add("dkv_y{a=\"1\",b=\"2\"} 3 1700000000\n# comment\n")
	f.Add("dkv_bad{a=\"x} 1\n")
	f.Fuzz(func(t *testing.T, in string) {
		ss, err := Parse(strings.NewReader(in))
		if err != nil {
			return
		}
		re := NewRegistry()
		for i, s := range ss {
			if checkName(s.Name) != nil {
				return // a name this package would never write
			}
			var labels, values []string
			for k, v := range s.Labels {
				if checkName(k) != nil || k == "le" {
					return
				}
				labels, values = append(labels, k), append(values, v)
			}
			// Re-emit each sample as its own gauge family.
			name := s.Name + "_" + string(rune('a'+i%26))
			func() {
				defer func() { _ = recover() }() // duplicate names with other label sets
				re.GaugeVec(name, "fuzz", labels...).With(values...).Set(s.Value)
			}()
		}
		var out bytes.Buffer
		if err := re.WriteText(&out); err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(&out); err != nil {
			t.Fatalf("this package's own output does not parse: %v\n%s", err, out.String())
		}
	})
}
