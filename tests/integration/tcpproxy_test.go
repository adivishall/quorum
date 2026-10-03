package integration

import (
	"testing"

	"github.com/adivishall/quorum/internal/netproxy"
)

// tcpProxy is the link proxy of internal/netproxy, placed on one node-to-node
// link so a test can partition real dkvd processes at the network level. Cut
// resets every connection through it and refuses new ones until Heal; its own
// behaviour is pinned by internal/netproxy's tests. The lab (internal/lab)
// partitions its clusters with the same proxy.
type tcpProxy = netproxy.Proxy

// startProxy starts a proxy to target that the test closes when it ends.
func startProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	p, err := netproxy.Start(target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
