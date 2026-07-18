//go:build !darwin

package hermes

import (
	"errors"
	"testing"
)

func TestContainmentUnavailableOffDarwin(t *testing.T) {
	if err := validateProcessContainment(true); err == nil {
		t.Fatal("Darwin best-effort containment accepted")
	}

	originalGOOS := processRuntimeGOOS
	t.Cleanup(func() { processRuntimeGOOS = originalGOOS })
	processRuntimeGOOS = "plan9"
	if err := validateProcessContainment(false); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("unsupported containment = %v", err)
	}
	processRuntimeGOOS = "linux"
	if err := validateProcessContainment(false); err != nil {
		t.Fatalf("Linux containment = %v", err)
	}

	if _, err := DiagnoseContainment("scratch"); err == nil {
		t.Fatal("diagnose unexpectedly available")
	}
	if _, err := CleanupContainment("scratch", "runtime", true); err == nil {
		t.Fatal("cleanup unexpectedly available")
	}
}
