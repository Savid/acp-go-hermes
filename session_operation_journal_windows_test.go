//go:build windows

package hermesacp

import (
	"os"
	"path/filepath"
	"strings"
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

	path := filepath.Join(durableTempDir(t), "operations")
	sessionOperationChmod = func(string, os.FileMode) error { return os.RemoveAll(filepath.Dir(path)) }

	if err := ensureSessionOperationDirectory(path); err != nil {
		t.Fatalf("ensure session-operation directory: %v", err)
	}
}

// TestSessionOperationJournalRefusesTheSharedHomeOnWindows pins that the
// shared session-operation journal is refused for the same reason, so a
// shared-home session mutation cannot begin here rather than half-beginning.
func TestSessionOperationJournalRefusesTheSharedHomeOnWindows(t *testing.T) {
	journal, err := beginSessionOperationJournal(durableTempDir(t), sessionOperationJournalFields{
		OperationID: "operation", Kind: sessionOperationKindNew, Mode: sessionOperationModeShared,
		LogicalSessionID: "session-1",
	})
	if err == nil {
		t.Fatal("windows began a shared session-operation journal")
	}

	if journal != nil {
		t.Fatal("refused journal was returned anyway")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("journal refusal = %v", err)
	}
}
