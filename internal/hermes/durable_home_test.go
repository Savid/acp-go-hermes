package hermes

import (
	"os"
	"path/filepath"
	"testing"
)

// durableTempDir returns a per-test directory on a filesystem the shared
// Hermes home accepts. t.TempDir lives on a volatile filesystem on some
// hosts, and the shared home refuses those by design, so the fallback is a
// directory under the user cache.
func durableTempDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	if EnsureLocalSharedHermesHome(dir) == nil {
		return dir
	}

	base, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("user cache dir: %v", err)
	}

	root := filepath.Join(base, "acp-go-hermes-test")
	if err = os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create %s: %v", root, err)
	}

	dir, err = os.MkdirTemp(root, "durable-*")
	if err != nil {
		t.Fatalf("create durable temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	if err = EnsureLocalSharedHermesHome(dir); err != nil {
		t.Skipf("no durable local filesystem for a shared Hermes home: %v", err)
	}

	return dir
}
