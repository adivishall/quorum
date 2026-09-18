//go:build darwin

package wal

import (
	"os"
	"syscall"
)

// fFullFsync is F_FULLFSYNC from <sys/fcntl.h>.
//
// On Darwin, fsync(2) only guarantees that data has been handed to the drive —
// it does not ask the drive to flush its own volatile write cache. A power cut
// after a successful fsync can therefore still lose the write. F_FULLFSYNC asks
// for the cache flush as well, and is what the `sync` mode's durability claim
// rests on (docs/DESIGN.md §3).
const fFullFsync = 51

// fullSync flushes the file all the way to stable storage, as far as the
// platform allows.
func fullSync(f *os.File) error {
	for {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(fFullFsync), 0)
		switch errno {
		case 0:
			return nil
		case syscall.EINTR:
			continue
		default:
			return &os.PathError{Op: "fcntl(F_FULLFSYNC)", Path: f.Name(), Err: errno}
		}
	}
}

// supportsFullSync reports whether F_FULLFSYNC works on this file.
//
// Not every filesystem implements it — network and virtual filesystems commonly
// do not. Rather than either failing to open or silently degrading, the WAL
// probes once at open time and reports the answer, so that a weaker guarantee
// is a fact the caller can see rather than an assumption it cannot check.
func supportsFullSync(f *os.File) bool {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(fFullFsync), 0)
	return errno == 0
}

// platformSyncNote describes what a successful `sync`-mode flush means here.
const platformSyncNote = "fcntl(F_FULLFSYNC), which asks the drive to flush its write cache"
