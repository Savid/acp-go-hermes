package main

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

type fakeAgent struct {
	prompts []string
	model   string
}

func (*fakeAgent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{}, nil
}

func (f *fakeAgent) NewSession(_ context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	if hermes, ok := params.Meta["hermes"].(map[string]any); ok {
		options, _ := hermes["options"].(map[string]any)
		f.model, _ = options["model"].(string)
	}

	return acp.NewSessionResponse{SessionId: "s"}, nil
}

func (f *fakeAgent) Prompt(_ context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	f.prompts = append(f.prompts, params.Prompt[0].Text.Text)

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (*fakeAgent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}

func TestChatReadsLinesUntilBlank(t *testing.T) {
	t.Parallel()

	agent := &fakeAgent{}

	var out bytes.Buffer

	require.NoError(t, chat(context.Background(), agent, bufio.NewReader(strings.NewReader("one\ntwo\n\nignored\n")), "/w", "p/m", &out))
	require.Equal(t, []string{"one", "two"}, agent.prompts)
	require.Equal(t, "p/m", agent.model)
	require.Contains(t, out.String(), "> ")
}

func TestTerminalPermission(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	title := "bash"
	request := acp.RequestPermissionRequest{
		ToolCall: acp.ToolCallUpdate{Title: &title},
		Options:  []acp.PermissionOption{{OptionId: "allow", Kind: acp.PermissionOptionKindAllowOnce}},
	}

	term := &terminal{output: &out, input: bufio.NewReader(strings.NewReader("y\nn\n"))}

	resp, err := term.RequestPermission(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp.Outcome.Selected)

	resp, err = term.RequestPermission(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp.Outcome.Cancelled)

	require.NoError(t, term.SessionUpdate(context.Background(), acp.SessionNotification{Update: acp.UpdateAgentMessageText("hi")}))
	require.NoError(t, term.SessionUpdate(context.Background(), acp.SessionNotification{Update: acp.StartToolCall("c", "bash")}))
	require.Contains(t, out.String(), "hi")
	require.Contains(t, out.String(), "[tool] bash")
}

func TestRunRejectsBadFlags(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	require.Equal(t, 2, run(context.Background(), []string{"-bogus"}, strings.NewReader(""), &out, &errOut))
}
