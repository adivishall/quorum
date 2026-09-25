// Package integration holds tests that need more than one process.
//
// Since Phase 10 it also holds the real-process fault tests (raft_fault_test.go):
// dkvd processes are SIGKILLed, frozen with SIGSTOP, restarted on their data
// directories, and partitioned by test-owned TCP proxies (tcpproxy_test.go), and
// the durable logs are checked from outside afterwards (docs/FAULTS.md §9).
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
//
// raft_crash_test.go (Phase 11, docs/CRASH_RECOVERY.md §8) kills real dkvd
// processes at exact crash points with `dkvd -crash-at` — the process logs the
// point and SIGKILLs itself there — and requires the log it leaves to reopen, the
// restart to recover exactly what the file holds, and the group to re-converge.
package integration
