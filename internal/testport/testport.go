// Package testport hands tests TCP addresses to give to a listener that binds
// them later — a child process, or a transport started after the address is
// chosen — without the race of reserving a port by binding ":0" and closing it.
// That old way left a window in which the kernel could give the same port to
// any outgoing connection as its source port (every ":0" port is an ephemeral
// port), and the listener then failed with "address already in use" (CI run
// 36244993180). The ports here come from ranges below every common ephemeral
// range — Linux 32768–60999, macOS/BSD 49152–65535, Windows 49152–65535 — so
// the kernel never hands them out; a port is only ever taken by a listener.
// The ranges are disjoint per test binary, because `go test ./...` runs
// packages concurrently. It is imported only by tests.
package testport

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
)

// Range is a half-open port range [Lo, Hi).
type Range struct{ Lo, Hi int }

// One range per test binary that reserves ports.
var (
	Integration = Range{20000, 26000} // tests/integration: dkvd processes
	KV          = Range{26000, 29000} // internal/kv: in-process nodes
	RaftNode    = Range{29000, 32000} // internal/raftnode: in-process nodes
)

var (
	mu   sync.Mutex
	next = map[Range]int{}
)

// Addr returns a 127.0.0.1 address in r that nothing listens on now. Ports are
// handed out in turn from a random starting point (so two runs of the same
// binary at once rarely meet), each once until the range wraps.
func Addr(t testing.TB, r Range) string {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if _, ok := next[r]; !ok {
		if lo, hi, ok := ephemeralRange(); ok && lo < r.Hi && hi >= r.Lo {
			t.Fatalf("testport: this host's ephemeral port range %d–%d overlaps the test range %d–%d; the kernel could hand a test's port to another connection", lo, hi, r.Lo, r.Hi-1)
		}
		next[r] = r.Lo + rand.Intn(r.Hi-r.Lo)
	}
	for tries := 0; tries < r.Hi-r.Lo; tries++ {
		p := next[r]
		next[r]++
		if next[r] >= r.Hi {
			next[r] = r.Lo
		}
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			continue // something listens there: skip it
		}
		_ = l.Close()
		return addr
	}
	t.Fatalf("testport: no free port in %d–%d", r.Lo, r.Hi-1)
	return ""
}

// ephemeralRange is Linux's configured range, when it can be read.
func ephemeralRange() (lo, hi int, ok bool) {
	b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(strings.TrimSpace(string(b)), "%d %d", &lo, &hi); err != nil {
		return 0, 0, false
	}
	return lo, hi, true
}
