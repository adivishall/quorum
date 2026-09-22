package routing

import (
	"fmt"
	"strings"
	"testing"
)

func renderToString(t *testing.T, r *Router, svg bool) string {
	t.Helper()
	var b strings.Builder
	var err error
	if svg {
		err = RenderSVG(&b, r)
	} else {
		err = RenderText(&b, r)
	}
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return b.String()
}

// TestVisualizationsAreDeterministic: rendering the same Router twice — and two
// independently built routers for the same Config — is byte-for-byte identical
// (matrix G, docs/ROUTING.md §8).
func TestVisualizationsAreDeterministic(t *testing.T) {
	cfg := Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n2", "n0", "n1"}}
	r1 := mustRouter(t, cfg)
	r2 := mustRouter(t, cfg)
	for _, svg := range []bool{false, true} {
		a := renderToString(t, r1, svg)
		b := renderToString(t, r1, svg)
		if a != b {
			t.Fatalf("re-render differs (svg=%v)", svg)
		}
		c := renderToString(t, r2, svg)
		if a != c {
			t.Fatalf("independently built router renders differently (svg=%v)", svg)
		}
	}
}

// TestVisualizationContainsEveryNodeAndShard: the output is generated from the
// real ring and mentions every configured node and every shard's group.
func TestVisualizationContainsEveryNodeAndShard(t *testing.T) {
	cfg := Config{ShardCount: 8, ReplicationFactor: 2, Nodes: []NodeID{"alpha", "bravo", "charlie"}}
	r := mustRouter(t, cfg)
	for _, svg := range []bool{false, true} {
		out := renderToString(t, r, svg)
		for _, id := range []string{"alpha", "bravo", "charlie"} {
			if !strings.Contains(out, id) {
				t.Errorf("output (svg=%v) is missing node %q", svg, id)
			}
		}
		for s := 0; s < cfg.ShardCount; s++ {
			// The two renderers format the shard table differently (text pads to
			// a fixed width for alignment; SVG uses "shard N:"). Match each.
			marker := fmt.Sprintf("shard %3d  primary", s)
			if svg {
				marker = fmt.Sprintf("shard %d:", s)
			}
			if !strings.Contains(out, marker) {
				t.Errorf("output (svg=%v) is missing %q", svg, marker)
			}
		}
	}
}

// TestSVGIsWellFormedEnough: a light structural check that the SVG opens/closes
// and carries drawn content. Not a full XML validator — just enough to catch a
// broken template.
func TestSVGIsWellFormedEnough(t *testing.T) {
	r := mustRouter(t, Config{ShardCount: 16, ReplicationFactor: 3, Nodes: []NodeID{"n0", "n1", "n2"}})
	out := renderToString(t, r, true)
	if !strings.HasPrefix(out, "<svg") || !strings.HasSuffix(strings.TrimSpace(out), "</svg>") {
		t.Fatal("SVG is not wrapped in <svg>...</svg>")
	}
	if !strings.Contains(out, "<path ") {
		t.Error("SVG has no shard wedges")
	}
	if !strings.Contains(out, "wrap") {
		t.Error("SVG does not mark the wrap-around point")
	}
}
