//go:build race

package integration

// raceEnabled: the test binary is race-built, and so is the dkvd it launches.
const raceEnabled = true
