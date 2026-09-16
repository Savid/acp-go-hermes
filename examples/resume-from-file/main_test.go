package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/storetest"
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

func TestFileStoreMeetsTheStoreContract(t *testing.T) {
	t.Parallel()

	storetest.Run(t, func(t *testing.T) acpcore.SessionStore {
		t.Helper()

		store, err := loadFileStore(filepath.Join(t.TempDir(), "store.json"))
		require.NoError(t, err)

		return store
	})
}

func TestFileStoreSurvivesReload(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.json")
	store, err := loadFileStore(path)
	require.NoError(t, err)

	ctx := context.Background()
	key := acpcore.SessionKey{SessionID: "s"}
	config := acpcore.SessionKey{SessionID: "s", Subpath: "config"}

	require.NoError(t, store.Replace(ctx, key, []acpcore.SessionStoreReplacement{
		{Key: key, Entries: []acpcore.SessionStoreEntry{[]byte(`{"a":1}`)}},
		{Key: config, Entries: []acpcore.SessionStoreEntry{[]byte(`{}`)}},
	}))

	reloaded, err := loadFileStore(path)
	require.NoError(t, err)

	generation, err := reloaded.Load(ctx, "s")
	require.NoError(t, err)
	require.Len(t, generation[""], 1)
	require.Len(t, generation["config"], 1)

	sessions, err := reloaded.ListSessions(ctx)
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	require.NoError(t, reloaded.Delete(ctx, key))

	final, err := loadFileStore(path)
	require.NoError(t, err)

	sessions, err = final.ListSessions(ctx)
	require.NoError(t, err)
	require.Empty(t, sessions, "a tombstone survives the reload that a host restarts through")

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
