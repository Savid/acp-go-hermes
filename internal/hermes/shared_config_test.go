package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAtomicSharedHermesWriteKeepsCompleteOldBytesOnRenameFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("old-complete\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	previous := sharedAtomicRename
	sharedAtomicRename = func(string, string) error { return errors.New("rename failed") }
	t.Cleanup(func() { sharedAtomicRename = previous })

	if err := atomicSharedHermesWriteFile(path, []byte("new-complete\n"), 0o600); err == nil {
		t.Fatal("atomic write accepted rename failure")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old-complete\n" {
		t.Fatalf("old bytes changed after failed atomic commit: %q", data)
	}
}

// sharedTestControlDir resolves the adapter control root paired with home, the
// residence every adapter-owned coordination artifact is written to.
func sharedTestControlDir(t *testing.T, home string) string {
	t.Helper()

	control, err := EnsureSharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}

	return control
}
