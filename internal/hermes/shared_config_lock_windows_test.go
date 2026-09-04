//go:build windows

package hermes

import (
	"context"
	"strings"
	"testing"
)

// TestSharedHermesConfigLockRefusesOnWindows pins the documented deviation:
// Windows has no shared Hermes home, so the adapter control root the config
// lock is taken in is refused before any callback runs, and the refusal names
// the shared home rather than surfacing as a lock failure.
func TestSharedHermesConfigLockRefusesOnWindows(t *testing.T) {
	ran := false

	err := withHermesConfigLock(context.Background(), t.TempDir(), func(string) error {
		ran = true

		return nil
	})
	if err == nil {
		t.Fatal("shared config lock succeeded on windows")
	}

	if ran {
		t.Fatal("shared config lock ran its callback on windows")
	}

	if !strings.Contains(err.Error(), "shared Hermes home is unsupported on windows") {
		t.Fatalf("shared config lock refusal = %v", err)
	}
}
