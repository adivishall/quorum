// Command dkvctl is the operator's command line for a Quorum cluster: status,
// leaders, configurations, lag, health and readiness, and snapshots, over
// dkvd's admin protocol (docs/OPERATIONS.md).
//
//	dkvctl -nodes n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 health
//	dkvctl -admin 127.0.0.1:9001 ready
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/adivishall/quorum/internal/ctl"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(ctl.Run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
