//go:build !windows

package hermesacp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSessionOperationAtomicWriteSyncFailure(t *testing.T) {
	info, err := os.Stat("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile("/dev/null", os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	previous, previousRemove := sessionOperationCreateTemp, sessionOperationRemove
	sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return file, nil }
	sessionOperationRemove = func(string) error { return os.ErrNotExist }
	t.Cleanup(func() {
		sessionOperationCreateTemp = previous
		sessionOperationRemove = previousRemove
	})

	if err := atomicWriteSessionOperationFile(filepath.Join(t.TempDir(), "value"), []byte("x"), info.Mode().Perm()); err == nil {
		t.Fatal("FIFO sync failure was ignored")
	}
	current, statErr := os.Stat("/dev/null")
	if statErr != nil {
		t.Fatal(statErr)
	}
	if current.Mode().Perm() != info.Mode().Perm() {
		t.Fatalf("/dev/null mode changed: mode=%v", current.Mode().Perm())
	}
}

// TestSessionOperationEnsureRequiresTheParentFlush pins that ensuring the
// journal root includes flushing its parent: a parent that vanished between the
// chmod and the flush fails the ensure rather than passing silently.
func TestSessionOperationEnsureRequiresTheParentFlush(t *testing.T) {
	previousChmod := sessionOperationChmod
	t.Cleanup(func() { sessionOperationChmod = previousChmod })

	path := filepath.Join(t.TempDir(), "operations")
	sessionOperationChmod = func(string, os.FileMode) error { return os.RemoveAll(filepath.Dir(path)) }

	if err := ensureSessionOperationDirectory(path); err == nil {
		t.Fatal("sync parent failure ignored")
	}
}
