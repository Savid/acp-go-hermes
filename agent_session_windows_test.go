//go:build windows

package hermesacp

import (
	"strings"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// TestSharedSessionSetLockRefusesOnWindows pins the fence a shared-home turn
// takes before touching the native session set: it is unavailable here, so no
// shared-home turn can claim one and proceed unfenced.
func TestSharedSessionSetLockRefusesOnWindows(t *testing.T) {
	lock, err := nativehermes.AcquireSharedSessionSetLock(
		t.Context(), t.TempDir(), nativehermes.SharedSessionSetLockExclusive)
	if err == nil {
		t.Fatal("windows acquired a shared session-set lock")
	}

	if lock != nil {
		t.Fatal("refused session-set lock was returned anyway")
	}

	if !strings.Contains(err.Error(), windowsSharedHomeRefusal) {
		t.Fatalf("session-set lock refusal = %v", err)
	}
}
