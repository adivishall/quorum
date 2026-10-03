//go:build !unix

package nodedir

// lock is a no-op where flock(2) does not exist: the data directory is NOT
// protected against a second process (docs/LIMITATIONS.md). Quorum is built
// and tested on Linux and macOS.
type lock struct{}

func acquire(string) (*lock, error) { return &lock{}, nil }

func (l *lock) release() error { return nil }
