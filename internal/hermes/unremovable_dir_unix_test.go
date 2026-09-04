//go:build !windows

package hermes

import (
	"os"
	"path/filepath"
	"testing"
)

// unremovableDirPath names a directory that os.RemoveAll cannot remove, so a
// rollback's retain accounting is driven by the filesystem rather than a seam.
// A name beneath a regular file is ENOTDIR to every operation that touches it.
func unremovableDirPath(t *testing.T) string {
	t.Helper()

	parent := filepath.Join(t.TempDir(), "parent-file")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	return filepath.Join(parent, "shim")
}
