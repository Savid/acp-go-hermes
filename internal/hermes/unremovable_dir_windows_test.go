//go:build windows

package hermes

import (
	"os"
	"path/filepath"
	"testing"
)

// unremovableDirPath names a directory that os.RemoveAll cannot remove, so a
// rollback's retain accounting is driven by the filesystem rather than a seam.
// Windows has no ENOTDIR to lean on — a name beneath a regular file simply does
// not exist, and removing what does not exist succeeds — so the directory is
// held instead: a file inside it stays open, and Windows refuses to delete a
// file another handle still has.
func unremovableDirPath(t *testing.T) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "held-dir")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	file, err := os.Create(filepath.Join(dir, "held"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = file.Close() })

	return dir
}
