//go:build integration

package hermes

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLiveServeRoundTrip(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_INTEGRATION") == "" {
		t.Skip("set ACP_GO_HERMES_RUN_INTEGRATION=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	home := t.TempDir()
	proc, err := Start(ctx, ProcessOptions{
		Home:    home,
		Cwd:     t.TempDir(),
		Timeout: 120 * time.Second,
		Env: map[string]string{
			"NO_COLOR": "1",
		},
		AcquireDiscoveryResources: testDiscoveryResourceAdmission,
		RetainDiscoveryRoot:       func(string, error) {},
	})
	if err != nil {
		t.Fatalf("start hermes serve: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = proc.Close(closeCtx)
	}()

	created, err := proc.Client.CreateSession(ctx, map[string]any{"cwd": t.TempDir(), "title": "acp-go-hermes live probe"})
	if err != nil {
		t.Fatalf("session.create: %v", err)
	}
	if created.SessionID == "" || created.StoredSessionID == "" {
		t.Fatalf("create result missing ids: %#v", created)
	}
	if _, err := proc.Client.ModelOptions(ctx, created.SessionID); err != nil {
		t.Fatalf("model.options: %v", err)
	}
	// No filename hint, exactly as the prompt path uploads: the live gateway has
	// to accept the attachment on the PNG signature alone.
	if err := proc.Client.AttachImageBytes(ctx, created.SessionID, []byte(
		"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01",
	)); err != nil {
		t.Fatalf("image.attach_bytes: %v", err)
	}
	if os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") != "" {
		if err := proc.Client.SubmitPrompt(ctx, created.SessionID, "Reply with exactly HERMES_LIVE_OK."); err != nil {
			t.Fatalf("prompt.submit: %v", err)
		}
		waitForEvent(t, ctx, proc.Client, "message.complete")
	}
	if _, err := os.Stat(filepath.Join(home, "state.db")); err != nil {
		t.Fatalf("state.db not created: %v", err)
	}
	if os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") != "" {
		if _, err := proc.Client.Branch(ctx, created.SessionID, "acp-go-hermes branch probe"); err != nil {
			t.Fatalf("session.branch: %v", err)
		}
	}
	if err := proc.Client.CloseSession(ctx, created.SessionID); err != nil {
		t.Fatalf("session.close: %v", err)
	}
	if err := proc.Client.DeleteSession(ctx, created.StoredSessionID); err != nil && !IsNotFound(err) {
		t.Fatalf("session.delete: %v", err)
	}
}

func waitForEvent(t *testing.T, ctx context.Context, client *Client, eventType string) {
	t.Helper()
	for {
		select {
		case event, ok := <-client.Events():
			if !ok {
				t.Fatalf("event channel closed waiting for %s", eventType)
			}
			if event.Type == eventType {
				return
			}
		case err := <-client.Errors():
			t.Fatalf("event error waiting for %s: %v", eventType, err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s: %v", eventType, ctx.Err())
		}
	}
}
