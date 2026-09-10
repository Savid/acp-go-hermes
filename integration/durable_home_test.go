//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/savid/acp-go-hermes/internal/hermes"
)

// durableTempDir returns a temp directory a shared Hermes home accepts.
// t.TempDir lives on a volatile filesystem on some hosts, which the shared
// home refuses, so the fallback lives under the user cache directory.
func durableTempDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	if hermes.EnsureLocalSharedHermesHome(dir) == nil {
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

	if err = hermes.EnsureLocalSharedHermesHome(dir); err != nil {
		t.Skipf("no durable local filesystem for a shared Hermes home: %v", err)
	}

	return dir
}
