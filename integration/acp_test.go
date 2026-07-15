//go:build integration

package integration

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

func TestHermesACPAgentLiveCompletionText(t *testing.T) {
	requireRunLiveTokens(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	args := []string{}
	if model := os.Getenv("ACP_GO_HERMES_MODEL"); model != "" {
		args = append(args, "-model", model)
	}
	agent := startLiveAgent(t, ctx, t.TempDir(), args...)
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	const sentinel = "ACP_HERMES_COMPLETION_TEXT_OK"
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-completion-text", "Reply with exactly "+sentinel+".")); err != nil {
		t.Fatalf("prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if got := client.agentText(); !strings.Contains(got, sentinel) {
		t.Fatalf("agent text = %q, want sentinel %q\nstderr:\n%s", got, sentinel, agent.stderrString())
	}
}

func TestHermesACPAgentBinarySessionLifecycle(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()

	home := t.TempDir()
	agent := startLiveAgent(t, ctx, home)
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	initResp, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if initResp.AgentCapabilities.SessionCapabilities.Fork != nil {
		t.Fatalf("stable fork advertised: %#v", initResp.AgentCapabilities.SessionCapabilities.Fork)
	}
	// Fork is exposed only through the namespaced extension method; the stable
	// ACP session/fork route must be method-not-found (-32601) on the wire.
	_, forkErr := conn.UnstableForkSession(ctx, acp.UnstableForkSessionRequest{})
	if forkErr == nil {
		t.Fatal("stable session/fork unexpectedly succeeded")
	}
	var forkReqErr *acp.RequestError
	if !errors.As(forkErr, &forkReqErr) || forkReqErr.Code != -32601 {
		t.Fatalf("stable session/fork error = %#v, want method-not-found", forkErr)
	}

	cwd := t.TempDir()
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}
	listResp, err := conn.ListSessions(ctx, hermesacp.ListSessionsRequest(hermesacp.WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("list sessions: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if len(listResp.Sessions) == 0 {
		t.Fatal("session/list returned no sessions")
	}
	if _, err := conn.UnstableDeleteSession(ctx, hermesacp.DeleteSessionRequest(session.SessionId)); err != nil {
		t.Fatalf("delete session: %v\nstderr:\n%s", err, agent.stderrString())
	}
}

func TestHermesACPAgentLivePromptPermissionElicitation(t *testing.T) {
	requireRunLiveTokens(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	home := t.TempDir()
	args := []string{}
	if model := os.Getenv("ACP_GO_HERMES_MODEL"); model != "" {
		args = append(args, "-model", model)
	}
	agent := startLiveAgent(t, ctx, home, args...)
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		},
	}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	cwd := t.TempDir()
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	permissionPrompt := envOrDefault("ACP_GO_HERMES_PERMISSION_PROMPT", "Create a file named acp-permission-probe.txt in the working directory, then stop.")
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-permission", permissionPrompt)); err != nil {
		t.Fatalf("permission prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if client.permissionCount() == 0 {
		t.Skipf("native Hermes prompt did not emit approval.request in this environment; stderr:\n%s", agent.stderrString())
	}

	questionPrompt := envOrDefault("ACP_GO_HERMES_QUESTION_PROMPT", `Use the question tool to ask the user "Continue?" with options "Yes" and "No", then stop after receiving the answer.`)
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-question", questionPrompt)); err != nil {
		t.Fatalf("question prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if client.elicitationCount() == 0 {
		t.Skipf("native Hermes prompt did not emit clarify.request in this environment; stderr:\n%s", agent.stderrString())
	}
}
