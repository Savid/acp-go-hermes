//go:build windows

package hermesacp

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSessionOperationEnsureDoesNotFlushTheParent pins the documented Windows
// behaviour: FlushFileBuffers refuses a directory handle opened through
// os.Open, so the parent flush is the one step of the ensure that does nothing
// here. A parent that vanished after the chmod therefore cannot be what fails
// the ensure, and every earlier step still decides the outcome.
func TestSessionOperationEnsureDoesNotFlushTheParent(t *testing.T) {
	previousChmod := sessionOperationChmod
	t.Cleanup(func() { sessionOperationChmod = previousChmod })

	path := filepath.Join(t.TempDir(), "operations")
	sessionOperationChmod = func(string, os.FileMode) error { return os.RemoveAll(filepath.Dir(path)) }

	if err := ensureSessionOperationDirectory(path); err != nil {
		t.Fatalf("ensure session-operation directory: %v", err)
	}
}
