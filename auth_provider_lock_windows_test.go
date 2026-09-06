//go:build windows

package hermesacp

import (
	"os"
	"testing"
)

// A closed handle carries no LockFileEx range, and the windows lock must report
// that as a failure rather than as ordinary contention: contention is retried
// until the acquisition deadline, so misreporting it would spin instead of
// failing.
func TestTryAuthProviderFileLockRejectsClosedHandle(t *testing.T) {
	file, err := os.CreateTemp(durableTempDir(t), "closed")
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
