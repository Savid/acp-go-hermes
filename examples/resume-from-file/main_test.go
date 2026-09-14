package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	acpcore "github.com/savid/acp-go-core"
)

type fakeAgent struct {
	resumed acp.SessionId
	prompts int
}

func (*fakeAgent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{}, nil
}

func (*fakeAgent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionId: "new"}, nil
}

func (f *fakeAgent) ResumeSession(_ context.Context, params acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	f.resumed = params.SessionId

	return acp.ResumeSessionResponse{}, nil
}

func (f *fakeAgent) Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, error) {
	f.prompts++

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (*fakeAgent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}

func TestConverseNewAndResume(t *testing.T) {
	t.Parallel()

	agent := &fakeAgent{}

	id, err := converse(context.Background(), agent, "/w", "", "hi")
	require.NoError(t, err)
	require.Equal(t, acp.SessionId("new"), id)

	id, err = converse(context.Background(), agent, "/w", "old", "hi")
	require.NoError(t, err)
	require.Equal(t, acp.SessionId("old"), id)
	require.Equal(t, acp.SessionId("old"), agent.resumed)
	require.Equal(t, 2, agent.prompts)
}

func TestFileStoreRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.json")
	store, err := loadFileStore(path)
	require.NoError(t, err)

	ctx := context.Background()
	key := acpcore.SessionKey{SessionID: "s"}
	require.NoError(t, store.Append(ctx, key, []acpcore.SessionStoreEntry{[]byte(`{"a":1}`)}))
	require.NoError(t, store.Append(ctx, acpcore.SessionKey{SessionID: "s", Subpath: "config"}, []acpcore.SessionStoreEntry{[]byte(`{}`)}))

	reloaded, err := loadFileStore(path)
	require.NoError(t, err)

	rows, err := reloaded.Load(ctx, key)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	sessions, err := reloaded.ListSessions(ctx)
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	subkeys, err := reloaded.ListSubkeys(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []string{"config"}, subkeys)

	require.NoError(t, reloaded.Replace(ctx, key, []acpcore.SessionStoreReplacement{{Key: key, Entries: nil}}))
	require.NoError(t, reloaded.Delete(ctx, key))

	sessions, err = reloaded.ListSessions(ctx)
	require.NoError(t, err)
	require.Empty(t, sessions)

	var out bytes.Buffer

	p := &printer{output: &out}
	require.NoError(t, p.SessionUpdate(ctx, acp.SessionNotification{Update: acp.UpdateAgentMessageText("x")}))
	require.Equal(t, "x", out.String())
}

func TestRunRejectsBadFlags(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	require.Equal(t, 2, run(context.Background(), []string{"-bogus"}, &out, &errOut))
}
