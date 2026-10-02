package multiraft

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/adivishall/quorum/internal/raftnode"
	"github.com/adivishall/quorum/internal/transport"
	"github.com/adivishall/quorum/internal/vfs"
)

// TestHostForwardsEveryNodeSetting: a group's driver configuration is copied
// field by field from the host's. Every raftnode.Config field must come out
// set, or be named in hostLeavesUnset with its reason — a setting added to the
// driver and forgotten here would otherwise be silently dropped for every
// hosted group (audit: manually copied struct fields without validation).
func TestHostForwardsEveryNodeSetting(t *testing.T) {
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "n1", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	h := &Host{cfg: Config{
		ID: "n1", Transport: tr, TickInterval: time.Millisecond, DisableSync: true, FS: vfs.OS{},
		Hook: func(raftnode.Point, uint64) error { return nil }, SnapshotEvery: 1, SnapshotRetain: 1,
		ElectionTicks: 10, HeartbeatTicks: 1, Logf: func(string, ...any) {},
	}, nodeMetrics: &raftnode.Metrics{}}
	nc := h.nodeConfig(5, raftnode.Config{}, "/x/raft.log", &recSM{}, make(chan transport.Envelope))
	v := reflect.ValueOf(nc)
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		_, unset := hostLeavesUnset[name]
		switch zero := v.Field(i).IsZero(); {
		case zero && !unset:
			t.Errorf("raftnode.Config.%s is not forwarded by the host: set it in nodeConfig, or name it in hostLeavesUnset with the reason", name)
		case !zero && unset:
			t.Errorf("raftnode.Config.%s is listed as left unset, but nodeConfig sets it", name)
		}
	}
	for name := range hostLeavesUnset {
		if _, ok := v.Type().FieldByName(name); !ok {
			t.Errorf("hostLeavesUnset names %s, which raftnode.Config no longer has", name)
		}
	}
}

// TestHostRefusesANegativeTick (audit M5): a negative tick is configuration
// the host refuses at Start, before any group: every group's driver would
// refuse it (a ticker panics on it), one by one, as a failed group.
func TestHostRefusesANegativeTick(t *testing.T) {
	tr, err := transport.NewTCPTransport(transport.Config{NodeID: "a", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	h, err := Start(context.Background(), Config{ID: "a", DataDir: t.TempDir(), Transport: tr, TickInterval: -time.Millisecond,
		NewStateMachine: func(GroupID) raftnode.StateMachine { return &recSM{} }})
	if err == nil {
		_ = h.Close()
		t.Fatal("the host accepted a negative tick interval")
	}
}
