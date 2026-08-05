//go:build !linux

package hermesacp

import (
	"os"
	"testing"
)

func TestValidateNativeOwnedDirectoryUnsupportedPlatform(t *testing.T) {
	if err := validateNativeOwnedDirectory("", nil); err != nil {
		t.Fatalf("nil process isolation: %v", err)
	}

	current := &ProcessIsolation{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}
	if err := validateNativeOwnedDirectory(t.TempDir(), current); err != nil {
		t.Fatalf("current process identity: %v", err)
	}

	other := &ProcessIsolation{UID: current.UID + 1, GID: current.GID + 1}
	if err := validateNativeOwnedDirectory(t.TempDir(), other); err == nil {
		t.Fatal("different process identity was accepted")
	}
}
