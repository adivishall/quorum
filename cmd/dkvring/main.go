// Command dkvring renders Quorum's routing ring for a given configuration.
//
// It builds a real internal/routing.Router from the flags and writes a
// deterministic visualization — SVG or text — to stdout or a file. The output is
// generated from the router's actual ring structures, so it always reflects what
// Route would do; running it twice on the same configuration produces
// byte-identical output (docs/ROUTING.md §8).
//
//	dkvring -shards 16 -rf 3 -nodes n0,n1,n2 -format svg  > ring.svg
//	dkvring -shards 16 -rf 3 -nodes n0,n1,n2 -format text
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/adivishall/quorum/internal/routing"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "dkvring: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("dkvring", flag.ContinueOnError)
	var (
		shards = fs.Int("shards", routing.DefaultShardCount, "number of shards")
		rf     = fs.Int("rf", routing.DefaultReplicationFactor, "replication factor (declarative replica-group size)")
		nodes  = fs.String("nodes", "", "comma-separated node ids (required), e.g. n0,n1,n2")
		format = fs.String("format", "text", "output format: text | svg")
		out    = fs.String("out", "", "output file (default: stdout)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*nodes) == "" {
		return fmt.Errorf("-nodes is required (comma-separated, e.g. n0,n1,n2)")
	}

	var ids []routing.NodeID
	for _, s := range strings.Split(*nodes, ",") {
		ids = append(ids, routing.NodeID(s))
	}

	r, err := routing.NewRouter(routing.Config{
		ShardCount:        *shards,
		ReplicationFactor: *rf,
		Nodes:             ids,
	})
	if err != nil {
		return err
	}

	w := bufio.NewWriter(stdout)
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		w = bufio.NewWriter(f)
	}
	defer func() { _ = w.Flush() }()

	switch *format {
	case "text":
		return routing.RenderText(w, r)
	case "svg":
		return routing.RenderSVG(w, r)
	default:
		return fmt.Errorf("unknown -format %q (want text or svg)", *format)
	}
}
