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

	scratch := t.TempDir()
	home, err := os.MkdirTemp(scratch, "acp-go-hermes-runtime-")
	if err != nil {
		t.Fatalf("create generation root: %v", err)
	}
	opts := ProcessOptions{
		Home:          home,
		Cwd:           t.TempDir(),
		ScratchParent: scratch,
		Timeout:       120 * time.Second,
		Env: map[string]string{
			"NO_COLOR": "1",
			"PATH":     os.Getenv("PATH"),
		},
	}
	// image.attach_bytes is a local upload rather than a model call, but the
	// gateway refuses it until some inference provider is configured. A
	// placeholder key satisfies that precondition; no prompt is submitted and
	// no credential or real Hermes home is involved.
	opts.Env["OPENAI_API_KEY"] = "acp-go-hermes-smoke-placeholder-not-a-credential"
	proc, err := Start(ctx, opts)
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
	// to accept the attachment on the PNG signature alone. The upload is a
	// gateway call rather than a model turn, so it stays outside the token gate
	// and every smoke run exercises it.
	if err := proc.Client.AttachImageBytes(ctx, created.SessionID, []byte(
		"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01",
	)); err != nil {
		t.Fatalf("image.attach_bytes: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, "state.db")); err != nil {
		t.Fatalf("state.db not created: %v", err)
	}
	if err := proc.Client.CloseSession(ctx, created.SessionID); err != nil {
		t.Fatalf("session.close: %v", err)
	}
	if err := proc.Client.DeleteSession(ctx, created.StoredSessionID); err != nil && !IsNotFound(err) {
		t.Fatalf("session.delete: %v", err)
	}
}
