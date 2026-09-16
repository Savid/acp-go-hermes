package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

type fakeAgent struct {
	prompts []string
	closed  bool
}

func (*fakeAgent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{}, nil
}

func (*fakeAgent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionId: "s"}, nil
}

func (f *fakeAgent) Prompt(_ context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	f.prompts = append(f.prompts, params.Prompt[0].Text.Text)

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (f *fakeAgent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	f.closed = true

	return acp.CloseSessionResponse{}, nil
}

func TestConverse(t *testing.T) {
	t.Parallel()

	agent := &fakeAgent{}

	var out bytes.Buffer

	require.NoError(t, converse(context.Background(), agent, "/w", "hello", &out))
	require.Equal(t, []string{"hello"}, agent.prompts)
	require.True(t, agent.closed)
	require.Contains(t, out.String(), "stop reason: end_turn")
}

func TestClientRendersUpdates(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	c := &client{output: &out}
	status := acp.ToolCallStatusCompleted

	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{Update: acp.UpdateAgentMessageText("hi")}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{Update: acp.UpdateAgentThoughtText("t")}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{Update: acp.StartToolCall("c", "bash")}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{Update: acp.UpdateToolCall("c", acp.WithUpdateStatus(status))}))
	require.Contains(t, out.String(), "hi")
	require.Contains(t, out.String(), "[thought] t")
	require.Contains(t, out.String(), "[tool] c bash")
	require.Contains(t, out.String(), "[tool] c completed")

	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{Options: []acp.PermissionOption{{OptionId: "a", Kind: acp.PermissionOptionKindAllowOnce}}})
	require.NoError(t, err)
	require.NotNil(t, resp.Outcome.Selected)

	resp, err = c.RequestPermission(context.Background(), acp.RequestPermissionRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Outcome.Cancelled)

	_, err = c.CreateTerminal(context.Background(), acp.CreateTerminalRequest{})
	require.Error(t, err)
}

func TestRunRejectsBadFlags(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	require.Equal(t, 2, run(context.Background(), []string{"-bogus"}, &out, &errOut))
}
