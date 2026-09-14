package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/storetest"
	"github.com/stretchr/testify/require"
)

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(_ *testing.T) acpcore.SessionStore { return acpcore.NewInMemorySessionStore() })
}

func TestConversationRestoreAndNativeContinuation(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	home, cwd := t.TempDir(), t.TempDir()
	h := newHarness(t, WithHome(home), WithSessionStore(store))
	h.initialize(withLifecycle())
	created, err := h.conn.NewSession(h.ctx(), NewSessionRequest(cwd, WithSessionAdditionalDirectories(t.TempDir())))
	require.NoError(t, err)
	response, err := h.prompt(created.SessionId, "HELLO", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Equal(t, "Hello world", agentText(h.rec.snapshot()))
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	path := filepath.Join(home, string(created.SessionId)+".json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var native map[string]any
	require.NoError(t, json.Unmarshal(data, &native))
	messages, ok := native["messages"].([]any)
	require.True(t, ok)
	native["messages"] = append(messages, map[string]any{"role": roleUser, "content": "native continuation"}, map[string]any{"role": roleAssistant, "content": "native answer"})
	data, err = json.Marshal(native)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	restored := newHarness(t, WithHome(home), WithSessionStore(store))
	restored.initialize()
	_, err = restored.conn.LoadSession(restored.ctx(), LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "native answer")
	_, err = restored.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = restored.conn.CloseSession(restored.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	newHome := t.TempDir()
	hydrated := newHarness(t, WithHome(newHome), WithSessionStore(store))
	hydrated.initialize()
	_, err = hydrated.conn.ResumeSession(hydrated.ctx(), ResumeSessionRequest(created.SessionId, cwd, WithSessionHermesOptions(NewHermesOptions(WithHermesEffort("high")))))
	require.NoError(t, err)
	require.Empty(t, agentText(hydrated.rec.snapshot()))
	require.FileExists(t, filepath.Join(newHome, string(created.SessionId)+".json"))
	_, err = hydrated.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = hydrated.conn.UnstableDeleteSession(hydrated.ctx(), DeleteSessionRequest(created.SessionId))
	require.NoError(t, err)
	_, err = hydrated.conn.LoadSession(hydrated.ctx(), LoadSessionRequest(created.SessionId, cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)[stopReasonError])
	require.FileExists(t, filepath.Join(newHome, string(created.SessionId)+".json"))
}

func TestNativePermissionsAndElicitation(t *testing.T) {
	t.Parallel()
	for _, choice := range []string{approvalOnce, "deny"} {
		t.Run(choice, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.rec.answer = func(acp.RequestPermissionRequest) acp.RequestPermissionResponse {
				return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(acp.PermissionOptionId(choice))}
			}
			h.initialize(withLifecycle())
			session := h.newSession()
			_, err := h.prompt(session.SessionId, "PERMISSION", promptMeta(1))
			require.NoError(t, err)
			require.Contains(t, agentText(h.rec.snapshot()), "permission="+choice)
		})
	}
	h := newHarness(t)
	h.rec.elicit = func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
		return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{"answer": "blue"}}}, nil
	}
	h.initialize(withLifecycle(), withFormElicitation())
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "QUESTION", promptMeta(1))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "blue")
}

type faultStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *faultStore) Replace(ctx context.Context, main acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("store unavailable")
	}

	return s.SessionStore.Replace(ctx, main, rows)
}

func TestFailedSnapshotKeepsCommittedConversation(t *testing.T) {
	t.Parallel()
	store := &faultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	key := acpcore.SessionKey{SessionID: string(session.SessionId)}
	before, err := store.Load(t.Context(), key)
	require.NoError(t, err)
	store.fail.Store(true)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.Equal(t, "hermes_turn_failed", requestErrorData(t, err)[stopReasonError])
	after, err := store.Load(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, before, after)
	store.fail.Store(false)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
}

func TestFailureAndTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		prompt, cause string
		timeout       time.Duration
	}{{"ERROR", "provider", 0}, {"CRASH", "process_exit", 0}, {"SLOW", "timeout", 100 * time.Millisecond}} {
		t.Run(tc.cause, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, WithTurnTimeout(tc.timeout))
			h.initialize()
			session := h.newSession()
			_, err := h.prompt(session.SessionId, tc.prompt, nil)
			require.Equal(t, tc.cause, requestErrorData(t, err)["cause"])
		})
	}
}

func TestQueuedPromptKeepsNativeOwnership(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "QUEUED", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, "earlierHello world", agentText(h.rec.snapshot()))
	_, err = h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.NoError(t, err)
}

func TestIdentityDriftPoisonsSession(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "ROTATE", nil)
	require.Error(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.Equal(t, "hermes_session_poisoned", requestErrorData(t, err)[stopReasonError])
}

func TestReplayPreservesToolsAndTextParts(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	agent := NewAgent(testOptions(t)...)
	agent.attach(h.rec, nil)
	s := &session{agent: agent, id: "conversation"}
	snapshot := []byte(`{"id":"conversation","messages":[{"role":"user","content":[{"type":"text","text":"read it"}]},{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","function":{"name":"read_file","arguments":"{\"path\":\"hello.txt\"}"}}]},{"role":"tool","tool_call_id":"call-1","tool_name":"read_file","content":"file contents"},{"role":"assistant","content":"done"}]}`)
	require.NoError(t, s.replay(t.Context(), [][]byte{snapshot}))
	require.Eventually(t, func() bool { return agentText(h.rec.snapshot()) == "done" }, time.Second, time.Millisecond)
	var started, completed bool
	for _, n := range h.rec.snapshot() {
		if call := n.Update.ToolCall; call != nil {
			started = call.ToolCallId == "call-1" && call.RawInput != nil
		}
		if call := n.Update.ToolCallUpdate; call != nil {
			completed = call.ToolCallId == "call-1" && call.Status != nil && *call.Status == acp.ToolCallStatusCompleted
		}
	}
	require.True(t, started)
	require.True(t, completed)
}

func TestClarifyChoiceValidation(t *testing.T) {
	t.Parallel()
	q := clarifyQuestion{Question: "Colors", Choices: []string{"red", "blue"}, Multi: true}
	require.Equal(t, `["red","blue"]`, q.answer([]any{"red", "blue"}))
	require.Nil(t, q.answer([]any{"red", "green"}))
	require.Nil(t, q.answer([]any{"red", "red"}))
	require.Equal(t, "array", q.schema()[fieldType])
	q.Multi = false
	require.Equal(t, "red", q.answer("red"))
	require.Nil(t, q.answer("green"))
}
