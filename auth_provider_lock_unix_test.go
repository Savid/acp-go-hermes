//go:build unix

package hermesacp

import (
	"os"
	"testing"
)

// A closed descriptor carries no flock, and the unix lock must report that as a
// failure rather than as ordinary contention: contention is retried until the
// acquisition deadline, so misreporting it would spin instead of failing.
func TestTryAuthProviderFileLockRejectsClosedDescriptor(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tryAuthProviderFileLock(file); err == nil {
		t.Fatal("closed file lock succeeded")
	}
}
