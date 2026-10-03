//go:build unix

package nodedir

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lock is an exclusive flock(2) on a file in the data directory. It belongs to
// the open file description, so it excludes a second Open in this process as
// well as in another, and the kernel releases it when the process dies —
// SIGKILL included — so a crashed node never leaves its directory locked.
type lock struct{ f *os.File }

func acquire(path string) (*lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w (%s)", ErrLocked, path)
		}
		return nil, fmt.Errorf("nodedir: locking %s: %w", path, err)
	}
	return &lock{f: f}, nil
}

func (l *lock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}
