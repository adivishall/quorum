// Command dkv is the dkv command-line client.
//
// Phase 1: the store is in-process and in-memory, so state does not survive
// this process. Phase 2 adds a write-ahead log and a data directory; Phase 7
// makes this a network client. The command surface is intended to stay the
// same across those changes.
package main

import (
	"fmt"
	"os"

	"github.com/adivishall/distributed-kv/internal/cli"
	"github.com/adivishall/distributed-kv/internal/storage"
)

func main() {
	// os.Exit does not run deferred functions, so the real work happens in
	// run() and main does nothing but translate its result into an exit status.
	os.Exit(run())
}

func run() int {
	store, err := storage.NewMemStore(storage.DefaultOptions())
	if err != nil {
		fmt.Fprintf(os.Stderr, "dkv: %v\n", err)
		return cli.ExitInternal
	}
	defer func() { _ = store.Close() }()

	app := &cli.App{
		Store:     store,
		Stdin:     os.Stdin,
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		Ephemeral: true,
	}
	return app.Run(os.Args[1:])
}
