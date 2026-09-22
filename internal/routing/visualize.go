package routing

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// This file renders a Router as a human-readable ring. Both renderers read the
// Router's actual ring structures — nothing is hard-coded or illustrative — and
// both are deterministic: the same Config produces byte-identical output every
// time (no timestamps, no map iteration, fixed number formatting). docs/ROUTING.md
// §8.

// shardAt looks up the shard owning a raw ring token. It is the token-space view
// the visualization needs (Route takes a key; this takes a position).
func (r *Router) shardAt(t Token) ShardID {
	return ShardID(r.shardRing.owner(t))
}

// RenderText writes an ASCII view: the config, a coarse strip of shard ownership
// sampled evenly around the token ring (showing wrap-around), and the full
// shard → replica-group table. It is meant for terminals and CI diffs.
func RenderText(w io.Writer, r *Router) error {
	b := &strings.Builder{}
	nodes := r.Nodes()
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = string(n)
	}
	fmt.Fprintf(b, "Quorum routing ring\n")
	fmt.Fprintf(b, "  shards=%d  replication_factor=%d  nodes=[%s]\n\n", r.shardCount, r.rf, strings.Join(names, " "))

	const cells = 64
	step := (^uint64(0)) / cells
	fmt.Fprintf(b, "Shard owning the token ring, sampled at %d points from 0x0 clockwise (wraps at the end):\n", cells)
	for c := 0; c < cells; c++ {
		if c%16 == 0 {
			if c != 0 {
				fmt.Fprint(b, "\n")
			}
			fmt.Fprintf(b, "  ")
		}
		fmt.Fprintf(b, "%3d ", r.shardAt(Token(uint64(c)*step)))
	}
	fmt.Fprintf(b, "\n\n")

	fmt.Fprintf(b, "Shard -> replica group (primary first):\n")
	for _, info := range r.Shards() {
		gs := make([]string, len(info.ReplicaGroup))
		for i, n := range info.ReplicaGroup {
			gs[i] = string(n)
		}
		fmt.Fprintf(b, "  shard %3d  primary %-8s  group [%s]\n", info.ID, info.Primary, strings.Join(gs, " "))
	}
	fmt.Fprintf(b, "\nRing sizes: shard ring %d positions (%d shards x %d vnodes), node ring %d positions (%d nodes x %d vnodes)\n",
		len(r.shardRing.points), r.shardCount, VNodesPerShard, len(r.nodeRing.points), len(nodes), VNodesPerNode)
	_, err := io.WriteString(w, b.String())
	return err
}

// SVG geometry constants. Fixed so output is deterministic and stable.
const (
	svgSize   = 720.0
	svgCX     = 360.0
	svgCY     = 300.0
	svgRadius = 240.0
	svgTick   = 14.0 // node vnode tick length outside the disc
)

// ftoa formats a float with fixed precision, so SVG coordinates are byte-stable.
func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', 2, 64) }

// angleXY maps a token to a point on the circle of the given radius, with token 0
// at the top (12 o'clock) and increasing tokens going clockwise.
func angleXY(t Token, radius float64) (x, y float64) {
	frac := float64(t) / float64(^uint64(0))
	theta := frac * 2 * math.Pi
	return svgCX + radius*math.Sin(theta), svgCY - radius*math.Cos(theta)
}

// shardColor and nodeColor derive stable HSL colors from an index, so the same
// shard/node is always the same color and no randomness is involved.
func shardColor(s ShardID, count int) string {
	h := float64(int(s)*360) / float64(count)
	return fmt.Sprintf("hsl(%s 65%% 62%%)", ftoa(h))
}
func nodeColor(idx, count int) string {
	h := float64(idx*360) / float64(count)
	return fmt.Sprintf("hsl(%s 70%% 38%%)", ftoa(h))
}

// RenderSVG draws the token ring as a disc partitioned into coloured wedges by
// shard ownership, node virtual-node positions as ticks around the rim, a legend
// of the membership, and the shard → replica-group table. Deterministic.
func RenderSVG(w io.Writer, r *Router) error {
	b := &strings.Builder{}
	nodes := r.Nodes()

	fmt.Fprintf(b, `<svg xmlns="http://www.w3.org/2000/svg" width="%s" height="%s" viewBox="0 0 %s %s" font-family="monospace" font-size="12">`,
		ftoa(svgSize), ftoa(svgSize), ftoa(svgSize), ftoa(svgSize))
	fmt.Fprint(b, "\n")
	fmt.Fprintf(b, `<rect width="%s" height="%s" fill="white"/>`+"\n", ftoa(svgSize), ftoa(svgSize))

	// One filled wedge per shard-ring arc (predecessor, current], coloured by the
	// shard that owns it. This is the shard ownership of the token space, drawn
	// directly from the ring.
	pts := r.shardRing.points
	n := len(pts)
	for i := 0; i < n; i++ {
		prev := pts[(i-1+n)%n].token
		cur := pts[i].token
		if cur == prev {
			continue // zero-width arc (token collision)
		}
		x0, y0 := angleXY(prev, svgRadius)
		x1, y1 := angleXY(cur, svgRadius)
		large := 0
		frac := float64(cur-prev) / float64(^uint64(0))
		if frac > 0.5 {
			large = 1
		}
		fmt.Fprintf(b, `<path d="M %s %s L %s %s A %s %s 0 %d 1 %s %s Z" fill="%s" stroke="none"/>`+"\n",
			ftoa(svgCX), ftoa(svgCY), ftoa(x0), ftoa(y0), ftoa(svgRadius), ftoa(svgRadius), large, ftoa(x1), ftoa(y1),
			shardColor(ShardID(pts[i].owner), r.shardCount))
	}

	// Node virtual-node ticks around the rim, coloured per node.
	for _, p := range r.nodeRing.points {
		x0, y0 := angleXY(p.token, svgRadius)
		x1, y1 := angleXY(p.token, svgRadius+svgTick)
		fmt.Fprintf(b, `<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="%s" stroke-width="1.5"/>`+"\n",
			ftoa(x0), ftoa(y0), ftoa(x1), ftoa(y1), nodeColor(int(p.owner), len(nodes)))
	}

	// Token 0 marker at the top, making wrap-around legible.
	fmt.Fprintf(b, `<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="black" stroke-width="1.5"/>`+"\n",
		ftoa(svgCX), ftoa(svgCY-svgRadius-svgTick-6), ftoa(svgCX), ftoa(svgCY-svgRadius+18))
	fmt.Fprintf(b, `<text x="%s" y="%s" text-anchor="middle">token 0 / 2^64 (wrap)</text>`+"\n",
		ftoa(svgCX), ftoa(svgCY-svgRadius-svgTick-12))

	// Title and legend.
	fmt.Fprintf(b, `<text x="12" y="20" font-size="14">Quorum routing ring — %d shards, RF %d, %d nodes</text>`+"\n",
		r.shardCount, r.rf, len(nodes))
	for i, id := range nodes {
		y := 40 + i*18
		fmt.Fprintf(b, `<rect x="12" y="%d" width="12" height="12" fill="%s"/>`+"\n", y, nodeColor(i, len(nodes)))
		fmt.Fprintf(b, `<text x="30" y="%d">node %s</text>`+"\n", y+11, escapeXML(string(id)))
	}

	// Shard -> replica group table, below the disc.
	y := int(svgCY+svgRadius) + 40
	fmt.Fprintf(b, `<text x="12" y="%d" font-size="13">shard -&gt; replica group:</text>`+"\n", y)
	for _, info := range r.Shards() {
		y += 16
		gs := make([]string, len(info.ReplicaGroup))
		for i, nn := range info.ReplicaGroup {
			gs[i] = string(nn)
		}
		fmt.Fprintf(b, `<text x="20" y="%d" fill="%s">shard %d: %s</text>`+"\n",
			y, shardColor(info.ID, r.shardCount), info.ID, escapeXML(strings.Join(gs, " ")))
	}

	fmt.Fprint(b, "</svg>\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// escapeXML escapes the five XML special characters so an arbitrary node id is
// safe in SVG text. Node ids are opaque bytes.
func escapeXML(s string) string {
	rep := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;")
	return rep.Replace(s)
}
