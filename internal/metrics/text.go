package metrics

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// ContentType is the Prometheus text exposition format this package writes.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// sample is one line of exposition: a series name (the family name, or it with
// _bucket, _sum or _count), its label pairs in order, and its value.
type sample struct {
	name   string
	labels []string // name, value, name, value, ...
	value  float64
}

// WriteText writes every family in the Prometheus text format, families sorted
// by name and series by their label values, so two scrapes of the same state
// are byte-identical.
func (r *Registry) WriteText(w io.Writer) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	fams := make([]*family, 0, len(r.families))
	for _, f := range r.families {
		fams = append(fams, f)
	}
	r.mu.Unlock()
	sort.Slice(fams, func(i, j int) bool { return fams[i].name < fams[j].name })
	bw := bufio.NewWriter(w)
	for _, f := range fams {
		fmt.Fprintf(bw, "# HELP %s %s\n", f.name, escapeHelp(f.help))
		fmt.Fprintf(bw, "# TYPE %s %s\n", f.name, f.typ)
		for _, s := range f.samples() {
			bw.WriteString(s.name)
			if len(s.labels) > 0 {
				bw.WriteByte('{')
				for i := 0; i < len(s.labels); i += 2 {
					if i > 0 {
						bw.WriteByte(',')
					}
					fmt.Fprintf(bw, "%s=\"%s\"", s.labels[i], escapeLabel(s.labels[i+1]))
				}
				bw.WriteByte('}')
			}
			bw.WriteByte(' ')
			bw.WriteString(formatValue(s.value))
			bw.WriteByte('\n')
		}
	}
	return bw.Flush()
}

// samples returns the family's series: its children's, then its collectors',
// each group sorted by label values.
func (f *family) samples() []sample {
	type keyed struct {
		values []string
		m      any
	}
	f.mu.RLock()
	kids := make([]keyed, 0, len(f.children))
	for k, m := range f.children {
		var values []string
		if len(f.labels) > 0 {
			values = strings.Split(k, sep)
		}
		kids = append(kids, keyed{values, m})
	}
	f.mu.RUnlock()
	sort.Slice(kids, func(i, j int) bool { return lessStrings(kids[i].values, kids[j].values) })

	pairs := func(values []string, extra ...string) []string {
		out := make([]string, 0, 2*len(values)+len(extra))
		for i, v := range values {
			out = append(out, f.labels[i], v)
		}
		return append(out, extra...)
	}
	var out []sample
	for _, k := range kids {
		switch m := k.m.(type) {
		case *Counter:
			out = append(out, sample{f.name, pairs(k.values), float64(m.Value())})
		case *Gauge:
			out = append(out, sample{f.name, pairs(k.values), m.Value()})
		case *Histogram:
			var cum uint64
			for i, ub := range m.upper {
				cum += m.counts[i].Load()
				out = append(out, sample{f.name + "_bucket", pairs(k.values, "le", formatValue(ub)), float64(cum)})
			}
			cum += m.counts[len(m.upper)].Load()
			out = append(out,
				sample{f.name + "_bucket", pairs(k.values, "le", "+Inf"), float64(cum)},
				sample{f.name + "_sum", pairs(k.values), m.Sum()},
				sample{f.name + "_count", pairs(k.values), float64(cum)})
		}
	}

	f.cmu.Lock()
	fns := make([]func(emit func(v float64, labelValues ...string)), len(f.collectors))
	copy(fns, f.collectors)
	f.cmu.Unlock()
	var collected []keyed
	for _, fn := range fns {
		fn(func(v float64, labelValues ...string) {
			if len(labelValues) != len(f.labels) {
				panic(fmt.Sprintf("metrics: collector of %s emitted %d label values, want %d", f.name, len(labelValues), len(f.labels)))
			}
			collected = append(collected, keyed{append([]string(nil), labelValues...), v})
		})
	}
	sort.SliceStable(collected, func(i, j int) bool { return lessStrings(collected[i].values, collected[j].values) })
	for _, c := range collected {
		out = append(out, sample{f.name, pairs(c.values), c.m.(float64)})
	}
	return out
}

func lessStrings(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			// Numeric label values (group ids, peers) sort numerically.
			ai, aerr := strconv.ParseFloat(a[i], 64)
			bi, berr := strconv.ParseFloat(b[i], 64)
			if aerr == nil && berr == nil && ai != bi {
				return ai < bi
			}
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func formatValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

func escapeLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

// Handler serves the registry at any path it is mounted on (GET or HEAD).
func Handler(r *Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", ContentType)
		if req.Method == http.MethodHead {
			return
		}
		_ = r.WriteText(w)
	})
}

// --- parsing (tests and tools that read a node's /metrics) ---

// Sample is one parsed series value.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// Samples is a parsed scrape.
type Samples []Sample

// Parse reads the text format this package writes (and the common subset of
// the Prometheus text format: comments, optional labels, a value, an optional
// timestamp, which is ignored).
func Parse(r io.Reader) (Samples, error) {
	var out Samples
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	line := 0
	for sc.Scan() {
		line++
		t := strings.TrimSpace(sc.Text())
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		s, err := parseLine(t)
		if err != nil {
			return nil, fmt.Errorf("metrics: line %d: %w", line, err)
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

func parseLine(t string) (Sample, error) {
	s := Sample{Labels: map[string]string{}}
	i := strings.IndexAny(t, "{ ")
	if i <= 0 {
		return s, fmt.Errorf("no value in %q", t)
	}
	s.Name = t[:i]
	rest := t[i:]
	if rest[0] == '{' {
		rest = rest[1:]
		for {
			rest = strings.TrimLeft(rest, " ,")
			if strings.HasPrefix(rest, "}") {
				rest = rest[1:]
				break
			}
			eq := strings.IndexByte(rest, '=')
			if eq <= 0 || len(rest) < eq+2 || rest[eq+1] != '"' {
				return s, fmt.Errorf("malformed labels in %q", t)
			}
			name := strings.TrimSpace(rest[:eq])
			rest = rest[eq+2:]
			var b strings.Builder
			j := 0
			for ; j < len(rest) && rest[j] != '"'; j++ {
				if rest[j] == '\\' && j+1 < len(rest) {
					j++
					switch rest[j] {
					case 'n':
						b.WriteByte('\n')
					default:
						b.WriteByte(rest[j])
					}
					continue
				}
				b.WriteByte(rest[j])
			}
			if j == len(rest) {
				return s, fmt.Errorf("unterminated label value in %q", t)
			}
			s.Labels[name] = b.String()
			rest = rest[j+1:]
		}
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 || len(fields) > 2 {
		return s, fmt.Errorf("malformed value in %q", t)
	}
	v, err := parseValue(fields[0])
	if err != nil {
		return s, err
	}
	s.Value = v
	return s, nil
}

func parseValue(s string) (float64, error) {
	switch s {
	case "+Inf", "Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, 64)
}

// Get returns the value of the one series named name whose labels include
// every pair in match (name, value, name, value, ...), and whether exactly one
// matched.
func (ss Samples) Get(name string, match ...string) (float64, bool) {
	var v float64
	n := 0
	for _, s := range ss.All(name, match...) {
		v = s.Value
		n++
	}
	return v, n == 1
}

// Sum returns the sum over every series named name whose labels include every
// pair in match.
func (ss Samples) Sum(name string, match ...string) float64 {
	var t float64
	for _, s := range ss.All(name, match...) {
		t += s.Value
	}
	return t
}

// All returns the series named name whose labels include every pair in match.
func (ss Samples) All(name string, match ...string) []Sample {
	if len(match)%2 != 0 {
		panic("metrics: match takes name, value pairs")
	}
	var out []Sample
next:
	for _, s := range ss {
		if s.Name != name {
			continue
		}
		for i := 0; i < len(match); i += 2 {
			if s.Labels[match[i]] != match[i+1] {
				continue next
			}
		}
		out = append(out, s)
	}
	return out
}
