//go:build !linux

package hermes

import (
	"os"
	"testing"
)

func TestHandoffGeneratedNativeTreeUnsupportedPlatform(t *testing.T) {
	if err := handoffGeneratedNativeTree("", nil); err != nil {
		t.Fatalf("nil process isolation: %v", err)
	}
	if err := handoffGeneratedNativeTree("", &ProcessIsolation{TestOnlyNoCredential: true}); err != nil {
		t.Fatalf("test-only process isolation: %v", err)
	}

	current := &ProcessIsolation{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}
	if err := handoffGeneratedNativeTree(t.TempDir(), current); err != nil {
		t.Fatalf("current process identity: %v", err)
	}

	other := &ProcessIsolation{UID: current.UID + 1, GID: current.GID + 1}
	if err := handoffGeneratedNativeTree(t.TempDir(), other); err == nil {
		t.Fatal("different process identity was accepted")
	}
}
