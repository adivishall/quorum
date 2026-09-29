//go:build !unix

package metrics

// registerRusage has nothing to report where getrusage does not exist.
func registerRusage(*Registry) {}
