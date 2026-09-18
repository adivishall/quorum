//go:build !darwin

package wal

import "os"

// fullSync flushes the file to stable storage.
//
// On Linux and the BSDs, fsync(2) is specified to flush the device's write
// cache as well, so there is no separate call to make. Whether the hardware
// honours the barrier is a different question, and one no system call can
// answer — see docs/FAILURE_MODEL.md §4.
func fullSync(f *os.File) error {
	return f.Sync()
}

// supportsFullSync reports whether a full flush is available. On these
// platforms fsync is the full flush, so it always is.
func supportsFullSync(f *os.File) bool {
	return f.Sync() == nil
}

// platformSyncNote describes what a successful `sync`-mode flush means here.
const platformSyncNote = "fsync(2)"
