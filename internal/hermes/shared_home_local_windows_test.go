//go:build windows

package hermes

import (
	"strings"
	"testing"
)

func TestWindowsSharedHomeFailsClosedWithoutInheritedLockProof(t *testing.T) {
	err := EnsureLocalSharedHermesHome(`C:\durable-hermes`)
	if err == nil || !strings.Contains(err.Error(), "inherited session-owner lock handles") {
		t.Fatalf("shared home error = %v", err)
	}
}
