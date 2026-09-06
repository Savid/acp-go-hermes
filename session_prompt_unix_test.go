//go:build !windows

package hermesacp

import (
	"context"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestSharedHomePromptSessionSetLock(t *testing.T) {
	home := durableTempDir(t)
	agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
	client := newFakeHermesClient()
	session := testSession(agent, client)
	response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "shared-lock", "reply"))
	if err != nil || response.StopReason == "" {
		t.Fatalf("shared-home prompt=%+v err=%v", response, err)
	}

	lock, err := nativehermes.AcquireSharedSessionSetLock(t.Context(), home, nativehermes.SharedSessionSetLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Release() }()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := session.Prompt(ctx, TextPromptRequest(session.id, "blocked-lock", "blocked")); err == nil {
		t.Fatal("contended shared-home turn lock succeeded")
	}
}
