// Package integration holds tests that need more than one process.
//
// The crash tests here launch a real child process, let it perform writes that
// are acknowledged, then kill it with SIGKILL and reopen its data directory.
// Nothing is simulated: the child is a separate OS process and it is destroyed
// without any opportunity to flush, close files, or run deferred functions.
//
// What these tests establish and what they do not:
//
//	established: an acknowledged write survives the writing process being
//	             destroyed, because the bytes had already reached the kernel.
//	NOT established: survival of an operating-system crash or a power cut.
//	             That needs the data to have reached the physical device, which
//	             a userspace test on a laptop cannot verify. See
//	             docs/FAILURE_MODEL.md §4.
package integration
