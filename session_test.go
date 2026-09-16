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

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

func TestConversationRestoreAndNativeContinuation(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	home, cwd := t.TempDir(), t.TempDir()
	h := newHarness(t, WithHome(home), WithSessionStore(store))
	h.initialize(withLifecycle())
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, wire.WithSessionAdditionalDirectories(t.TempDir())))
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
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "native answer")
	_, err = restored.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = restored.conn.CloseSession(restored.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	newHome := t.TempDir()
	hydrated := newHarness(t, WithHome(newHome), WithSessionStore(store))
	hydrated.initialize()
	_, err = hydrated.conn.ResumeSession(hydrated.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd, WithSessionHermesOptions(NewHermesOptions(WithHermesEffort("high")))))
	require.NoError(t, err)
	require.Empty(t, agentText(hydrated.rec.snapshot()))
	require.FileExists(t, filepath.Join(newHome, string(created.SessionId)+".json"))
	_, err = hydrated.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = hydrated.conn.UnstableDeleteSession(hydrated.ctx(), wire.DeleteSessionRequest(created.SessionId))
	require.NoError(t, err)
	_, err = hydrated.conn.LoadSession(hydrated.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
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
		return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{clarifyAnswerKey: "blue"}}}, nil
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
	before, err := loadEntries(t.Context(), store, key)
	require.NoError(t, err)
	store.fail.Store(true)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.Equal(t, "hermes_turn_failed", requestErrorData(t, err)[stopReasonError])
	after, err := loadEntries(t.Context(), store, key)
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
	s := &session{agent: agent, id: "acp-conversation", nativeID: "conversation"}
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

type backgroundFaultStore struct {
	acpcore.SessionStore
	fail bool
}

func (s *backgroundFaultStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail {
		return errors.New("mirror store unavailable")
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}
func TestBackgroundCommitFailureFencesStream(t *testing.T) {
	store := &backgroundFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(newRecorder(), nil)
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	store.fail = true
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	s.handleEvent(t.Context(), rt, hermes.Event{Type: eventMessageComplete, Payload: []byte(`{"text":"background","status":"complete"}`)})
	require.False(t, s.lc.Active(), "a failed background commit fences the lifecycle stream")
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.runtime == nil
	}, testTimeout, time.Millisecond, "a failed background commit drops the gateway binding")
	store.fail = false
	request := wire.TextPromptRequest(created.SessionId, "HELLO")
	request.Meta = promptMeta(1)
	_, err = a.Prompt(t.Context(), request)
	require.NoError(t, err)
	require.True(t, s.lc.Active(), "the next prompt relaunches and publishes a new incarnation")
}

// gatewayLossStore ends the gateway generation from inside a durable commit,
// after the snapshot has been read, and holds the commit until the loss has
// been reaped, so the turn is committed and still settling when the generation
// ends.
type gatewayLossStore struct {
	acpcore.SessionStore
	lose  func()
	armed atomic.Bool
}

func (s *gatewayLossStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.armed.CompareAndSwap(true, false) {
		s.lose()
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

// The generation, not the way the turn ended, decides the fence: a gateway
// lost while a turn that already reached its terminal result is still
// settling ends the incarnation, so the next generation publishes its own.
func TestGatewayLossAfterASettledTurnFencesTheIncarnation(t *testing.T) {
	t.Parallel()

	store := &gatewayLossStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })

	rec := newRecorder()
	a.attach(rec, nil)

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	store.lose = func() {
		rt.cancel()

		for {
			s.mu.Lock()
			reaped := s.runtime == nil
			s.mu.Unlock()

			if reaped {
				return
			}

			time.Sleep(time.Millisecond)
		}
	}
	store.armed.Store(true)

	first := wire.TextPromptRequest(created.SessionId, "HELLO")
	first.Meta = promptMeta(1)
	_, err = a.Prompt(t.Context(), first)
	require.NoError(t, err)

	second := wire.TextPromptRequest(created.SessionId, "HELLO")
	second.Meta = promptMeta(2)
	_, err = a.Prompt(t.Context(), second)
	require.NoError(t, err)

	require.Len(t, lifecycleStreams(rec.snapshot()), 2, "the relaunched gateway publishes a new incarnation")
}

// The readiness window ends with the child instead of running to the settle
// timeout.
func TestStartupFailsFastWhenTheChildDies(t *testing.T) {
	t.Parallel()

	// hermes cannot create its home under a regular file, so the child exits
	// before it can serve the gateway.
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))

	h := newHarness(t, WithHome(filepath.Join(blocker, "home")))
	h.initialize()

	start := time.Now()
	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Equal(t, "hermes_internal_failure", requestErrorData(t, err)[stopReasonError])
	require.Less(t, time.Since(start), 5*time.Second, "a child that already exited ends the readiness window")
}

func TestConfiguredModelPrecedesNativeBuild(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithDefaultModel("fake/text-only"), WithEnv(map[string]string{fakeHermesEnv: "1", "ACP_GO_HERMES_TEST_BUILD_MODEL": "fake/text-only"}))
	h.initialize()
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	_, err = h.conn.Prompt(h.ctx(), wire.TextPromptRequest(session.SessionId, "HELLO"))
	require.NoError(t, err)
}
