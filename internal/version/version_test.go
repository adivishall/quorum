package version

import "testing"

func TestVersionIsSet(t *testing.T) {
	if Version == "" {
		t.Fatal("version must not be empty")
	}
}
