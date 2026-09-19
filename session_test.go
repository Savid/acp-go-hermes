package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
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

func TestFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		prompt, cause string
	}{{"ERROR", "provider"}, {"CRASH", "process_exit"}} {
		t.Run(tc.cause, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
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
	rec := newRecorder()
	a.attach(rec, nil)
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
	result, _, _, err := rt.client.SubmitPromptWatermark(t.Context(), rt.liveID, "HELLO")
	require.NoError(t, err)
	require.Equal(t, promptStreaming, result.Status)
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.runtime == nil
	}, testTimeout, time.Millisecond, "a failed background commit drops the gateway binding")
	require.False(t, s.lc.Active(), "a failed background commit fences the lifecycle stream")
	for _, event := range lifecycleEvents(rec.snapshot()) {
		if event["type"] == "state_update" {
			require.NotEqual(t, "idle", event["state"], "failed mirror publication cannot assert a durable terminal state")
		}
	}

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

// A close that lands while a relaunch is still waiting for the gateway stops
// the process the relaunch started instead of binding it to a closed session.
func TestCloseDuringRelaunchStopsTheUnboundProcess(t *testing.T) {
	t.Parallel()

	held := filepath.Join(t.TempDir(), "ready-held")
	h := newHarness(t, WithEnv(map[string]string{fakeHermesEnv: "1", fakeHermesEnvReadyHold: held}))
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "CRASH", promptMeta(1))
	require.Equal(t, "process_exit", requestErrorData(t, err)["cause"])
	require.NoError(t, os.WriteFile(held+".armed", nil, 0o600))

	relaunched := make(chan error, 1)

	go func() {
		_, configErr := h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "fake/text-only"))
		relaunched <- configErr
	}()

	var pid int

	require.Eventually(t, func() bool {
		data, readErr := os.ReadFile(held)
		if readErr != nil {
			return false
		}

		pid, _ = strconv.Atoi(string(data))

		return pid > 0
	}, testTimeout, time.Millisecond)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	require.NoError(t, os.Remove(held))

	err = <-relaunched
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"], "the relaunch answers for the session that closed under it")
	require.Eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) }, testTimeout, time.Millisecond, "the gateway the relaunch started is gone")

	_, err = h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])
}

// An empty executable path resolves the hermes binary from the base PATH.
func TestEmptyExecutablePathResolvesHermesFromPath(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	require.NoError(t, os.Symlink(os.Args[0], filepath.Join(base, "hermes")))

	h := newHarness(t, WithExecutablePath(""), WithEnv(map[string]string{fakeHermesEnv: "1", "PATH": base}))
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
	require.NoError(t, err)
}

func TestCloseJoinsFirstMirrorAndFencesOpening(t *testing.T) {
	t.Parallel()
	for _, agentClose := range []bool{false, true} {
		name := "close_session"
		if agentClose {
			name = "close_agent"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := &commitBarrier{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan acpcore.SessionKey, 1), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(store.release) })
			t.Cleanup(release)
			store.block.Store(true)
			a := NewAgent(testOptions(t, WithSessionStore(store))...)
			t.Cleanup(func() { _ = a.Close() })
			rec := newRecorder()
			a.attach(rec, nil)
			request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&request)
			_, err := a.Initialize(t.Context(), request)
			require.NoError(t, err)
			cwd := t.TempDir()
			created := make(chan error, 1)
			go func() {
				_, createErr := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
				created <- createErr
			}()
			var key acpcore.SessionKey
			select {
			case key = <-store.entered:
			case <-time.After(testTimeout):
				t.Fatal("creation did not reach its first mirror")
			}
			s, err := a.session(t.Context(), acp.SessionId(key.SessionID))
			require.NoError(t, err)
			s.mu.Lock()
			rt := s.runtime
			s.mu.Unlock()
			closed := make(chan error, 1)
			go func() {
				if agentClose {
					closed <- a.Close()

					return
				}
				_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: acp.SessionId(key.SessionID)})
				closed <- err
			}()
			require.Eventually(t, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()

				return s.closing
			}, testTimeout, time.Millisecond)
			select {
			case err := <-closed:
				t.Fatalf("close returned while the first mirror was blocked: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			release()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(testTimeout):
				t.Fatal("close did not join creation")
			}
			select {
			case err := <-created:
				require.Error(t, err, "a closing session must refuse its opening publication")
			case <-time.After(testTimeout):
				t.Fatal("creation did not release its gate before cleanup")
			}
			require.False(t, s.lc.Active())
			before := len(rec.snapshot())
			require.Error(t, s.openStream(t.Context(), rt))
			require.Len(t, rec.snapshot(), before, "closed session published commands or a lifecycle snapshot")
			require.False(t, s.lc.Active())
		})
	}
}

func TestOpeningRejectsReplacedNativeGeneration(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	stale := s.runtime
	s.mu.Unlock()
	transport, meta := prepareOpeningResponse(t)
	a.attach(rec, transport)
	require.NoError(t, a.scheduleOpen(transport.RequestContext(t.Context(), meta), s))
	s.stopRuntime(t.Context(), stale)
	require.False(t, s.lc.Active())
	fresh, err := s.ensureRuntime(t.Context())
	require.NoError(t, err)
	require.NotSame(t, stale, fresh)
	require.True(t, s.lc.Active())
	before := len(rec.snapshot())
	require.Error(t, s.openStream(t.Context(), stale))
	require.Len(t, rec.snapshot(), before, "stale deferred opening published on the replacement generation")
	require.True(t, s.lc.Active(), "stale opening fenced the replacement stream")
	finishOpeningResponse(t, transport, s.id)
	require.Len(t, rec.snapshot(), before, "stale hook published on the replacement generation")
	current, err := a.session(t.Context(), s.id)
	require.NoError(t, err)
	require.Same(t, s, current)
	require.True(t, s.lc.Active(), "stale hook closed the replacement stream")
}

// openingCallbackClient exercises a synchronous embedded callback into admission.
type openingCallbackClient struct {
	*recorder
	agent *Agent
}

func (c *openingCallbackClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if err := c.agent.Cancel(ctx, acp.CancelNotification{SessionId: notification.SessionId}); err != nil {
		return err
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestOpeningAllowsSynchronousSessionCallback(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(&openingCallbackClient{recorder: rec, agent: a}, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.NotEmpty(t, rec.snapshot())
}

func prepareOpeningResponse(t *testing.T) (*wire.Transport, map[string]any) {
	t.Helper()
	transport := wire.NewTransport(strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session/new\",\"params\":{}}\n"), io.Discard)
	t.Cleanup(transport.Close)
	transport.Start()
	inbound, err := io.ReadAll(transport.Reader())
	require.NoError(t, err)

	var frame struct {
		Params acp.NewSessionRequest `json:"params"`
	}

	require.NoError(t, json.Unmarshal(inbound, &frame))

	return transport, frame.Params.Meta
}

func finishOpeningResponse(t *testing.T, transport *wire.Transport, id acp.SessionId) {
	t.Helper()
	_, err := transport.Writer().Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	require.NoError(t, transport.AwaitSession(ctx, id))
}

func TestDeferredOpeningFailureDetachesSession(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))...)
	t.Cleanup(func() { _ = a.Close() })
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	transport, meta := prepareOpeningResponse(t)
	a.attach(&firstOpenFailureClient{recorder: newRecorder()}, transport)
	newRequest := wire.NewSessionRequest(t.TempDir())
	newRequest.Meta = meta
	created, err := a.NewSession(t.Context(), newRequest)
	require.NoError(t, err)
	a.mu.Lock()
	s := a.sessions[created.SessionId]
	a.mu.Unlock()
	require.NotNil(t, s)
	finishOpeningResponse(t, transport, created.SessionId)
	require.False(t, s.lc.Active())
	a.mu.Lock()
	_, installed := a.sessions[created.SessionId]
	a.mu.Unlock()
	require.False(t, installed, "failed deferred publication retained the active slot")
	s.mu.Lock()
	closed := s.closing
	s.mu.Unlock()
	require.True(t, closed)
}

func TestRuntimeDrainCompletesBeforeReplacement(t *testing.T) {
	t.Parallel()
	proc, err := process.Start(t.Context(), process.Request{Executable: "/usr/bin/true"})
	require.NoError(t, err)
	defer proc.Close()
	old := &runtime{proc: proc, done: make(chan struct{})}
	old.controls = make(chan func())
	old.controlsDone = make(chan struct{})
	close(old.controlsDone)
	s := &session{runtime: old}
	var streams []string
	deliver := func(_ context.Context, envelope map[string]any) error {
		streamID, ok := envelope["streamId"].(string)
		require.True(t, ok)
		streams = append(streams, streamID)

		return nil
	}
	negotiated := lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true}
	require.NoError(t, s.lc.Open(t.Context(), "old", negotiated, deliver))
	callbackCtx, cancelCallback := context.WithCancelCause(t.Context())
	defer cancelCallback(nil)
	release := s.registerDialog("pending", cancelCallback)
	defer release()
	go func() { s.runtimeEnded(t.Context(), old); close(old.done) }()
	select {
	case <-callbackCtx.Done():
	case <-time.After(testTimeout):
		t.Fatal("runtime did not cancel its pending callback")
	}
	s.mu.Lock()
	bound := s.runtime
	s.mu.Unlock()
	if bound != old {
		release()
		<-old.done
		t.Fatal("runtime released its binding before its callback drained")
	}
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	cancelRequest()
	returned, requestErr := s.ensureRuntime(requestCtx)
	release()
	select {
	case <-old.done:
	case <-time.After(testTimeout):
		t.Fatal("runtime did not finish teardown")
	}
	require.Nil(t, returned, "an operation cannot acquire the runtime being drained")
	require.ErrorIs(t, requestErr, context.Canceled)
	require.NoError(t, s.lc.Open(t.Context(), "replacement", negotiated, deliver))
	require.True(t, s.lc.Active())
	require.Equal(t, []string{"old", "replacement"}, streams)
}

type cancellingBackgroundClient struct {
	*recorder
	agent          *Agent
	terminalOnly   bool
	terminalCalled bool
}

func (c *cancellingBackgroundClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if c.terminalOnly {
		envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["state"] != "idle" {
			return c.recorder.SessionUpdate(ctx, notification)
		}
		c.terminalCalled = true
	}
	done := make(chan error, 1)
	go func() { done <- c.agent.Cancel(ctx, wire.CancelRequest(notification.SessionId)) }()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		return context.DeadlineExceeded
	}
}

func TestBackgroundPublicationAllowsCancelCallback(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	a.attach(&cancellingBackgroundClient{recorder: rec, agent: a}, nil)
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: "message.delta", Payload: json.RawMessage(`{"text":"background"}`)})
	a.attach(rec, nil)
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.NoError(t, s.cycleFailure(c))
	require.True(t, s.cycleCancelled(c))
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: eventMessageComplete, Payload: json.RawMessage(`{"status":"complete"}`)})
}

func TestCancelAgentOriginResolvesDialogsAndSettlesCancelled(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: "message.delta", Payload: json.RawMessage(`{"text":"background"}`)})
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	dialogCtx, cancelDialog := context.WithCancelCause(t.Context())
	defer cancelDialog(nil)
	unregister := s.registerDialog("permission", cancelDialog)
	require.NoError(t, s.lc.ActionPending(t.Context(), c.Cycle, "permission", lifecycle.ActionPermission))
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	require.ErrorIs(t, context.Cause(dialogCtx), errDialogCancelled)
	unregister()
	lateCtx, cancelLate := context.WithCancelCause(t.Context())
	defer cancelLate(nil)
	release := s.registerDialog("late", cancelLate)
	release()
	require.ErrorIs(t, context.Cause(lateCtx), errDialogCancelled)
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	prompt := wire.TextPromptRequest(created.SessionId, "HELLO")
	prompt.Meta = promptMeta(1)
	_, err = a.Prompt(t.Context(), prompt)
	require.Equal(t, "backpressure", requestErrorData(t, err)["error"])
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: eventMessageComplete, Payload: json.RawMessage(`{"status":"complete"}`)})
	s.mu.Lock()
	active := s.cycle
	s.mu.Unlock()
	require.Nil(t, active)
	cancelled := false
	for _, notification := range rec.snapshot() {
		envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "cancelled" {
			cancelled = true
		}
	}
	require.True(t, cancelled)
}

type backgroundPublicationBarrier struct {
	*recorder
	entered chan struct{}
	release chan struct{}
}

func (c *backgroundPublicationBarrier) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	close(c.entered)
	<-c.release

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestBackgroundReservationRefusesConcurrentPrompt(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	barrier := &backgroundPublicationBarrier{recorder: rec, entered: make(chan struct{}), release: make(chan struct{})}
	a.attach(barrier, nil)
	release := sync.OnceFunc(func() { close(barrier.release) })
	defer release()
	done := make(chan struct{})
	go func() {
		s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: eventMessageStart})
		close(done)
	}()
	select {
	case <-barrier.entered:
	case <-time.After(testTimeout):
		t.Fatal("background publication did not start")
	}
	prompt := wire.TextPromptRequest(created.SessionId, "HELLO")
	prompt.Meta = promptMeta(1)
	promptDone := make(chan error, 1)
	go func() { _, promptErr := a.Prompt(t.Context(), prompt); promptDone <- promptErr }()
	select {
	case err := <-promptDone:
		require.Equal(t, "backpressure", requestErrorData(t, err)["error"])
	case <-time.After(testTimeout):
		t.Fatal("prompt waited on background publication")
	}
	release()
	<-done
	a.attach(rec, nil)
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: eventMessageComplete, Payload: json.RawMessage(`{"status":"complete"}`)})
}

func TestQueuedRequestWaitsWhilePriorNativeWorkDrains(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	queuedCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pending := &turn{cancel: cancel, disposition: promptQueued, watermark: 40, ready: make(chan struct{}), finished: make(chan struct{}), settled: make(chan struct{})}
	close(pending.ready)
	close(pending.finished)
	s.mu.Lock()
	s.turn = pending
	rt := s.runtime
	s.mu.Unlock()
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, InboundSequence: 41, Type: "message.delta", Payload: json.RawMessage(`{"text":"prior work"}`)})
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.False(t, pending.accepted)
	require.Equal(t, "prior work", agentText(rec.snapshot()))
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	require.ErrorIs(t, queuedCtx.Err(), context.Canceled)
	require.True(t, s.cycleCancelled(&pending.cycle))
	require.True(t, s.cycleCancelled(c))
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, InboundSequence: 42, Type: eventMessageComplete, Payload: json.RawMessage(`{"status":"complete"}`)})
	require.False(t, pending.accepted)
	s.mu.Lock()
	s.turn = nil
	s.mu.Unlock()
}

func TestRuntimeLossSettlesBackgroundBeforeQueuedRequest(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	_, cancel := context.WithCancel(t.Context())
	defer cancel()
	pending := &turn{cancel: cancel, disposition: promptQueued, watermark: 40, ready: make(chan struct{}), finished: make(chan struct{}), settled: make(chan struct{})}
	close(pending.ready)
	close(pending.finished)
	s.mu.Lock()
	s.turn = pending
	rt := s.runtime
	s.mu.Unlock()
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, InboundSequence: 41, Type: "message.delta", Payload: json.RawMessage(`{"text":"prior work"}`)})
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	s.dropRuntime(rt)
	select {
	case <-rt.done:
	case <-time.After(testTimeout):
		t.Fatal("runtime loss did not settle")
	}
	select {
	case <-c.done:
	default:
		t.Fatal("background cycle remained open after runtime loss")
	}
	select {
	case <-pending.settled:
	default:
		t.Fatal("queued request remained open after runtime loss")
	}
	require.Equal(t, turnTransportEnded, pending.ended)
	require.False(t, pending.accepted)
	failed := false
	for _, notification := range rec.snapshot() {
		envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "failed" {
			failed = true
		}
	}
	require.True(t, failed)
	s.mu.Lock()
	s.turn = nil
	s.mu.Unlock()
}

func TestTerminalPublicationCannotInterruptNextCycle(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	terminalClient := &cancellingBackgroundClient{recorder: rec, agent: a, terminalOnly: true}
	a.attach(terminalClient, nil)
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: "message.delta", Payload: json.RawMessage(`{"text":"background"}`)})
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.NoError(t, s.cycleFailure(c))
	require.False(t, s.cycleCancelled(c))
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, Type: eventMessageComplete, Payload: json.RawMessage(`{"status":"complete"}`)})
	require.False(t, s.cycleCancelled(c))
	require.True(t, terminalClient.terminalCalled)
	s.callbacks.Wait()
	a.attach(rec, nil)
}

func TestTerminalCancelStillCancelsQueuedRequest(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	queuedCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pending := &turn{cancel: cancel, disposition: promptQueued, watermark: 40, ready: make(chan struct{}), finished: make(chan struct{}), settled: make(chan struct{})}
	close(pending.ready)
	close(pending.finished)
	s.mu.Lock()
	s.turn = pending
	rt := s.runtime
	s.mu.Unlock()
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, InboundSequence: 41, Type: "message.delta", Payload: json.RawMessage(`{"text":"prior work"}`)})
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.False(t, pending.accepted)
	require.Equal(t, "prior work", agentText(rec.snapshot()))
	a.attach(&cancellingBackgroundClient{recorder: rec, agent: a, terminalOnly: true}, nil)
	s.handleEvent(t.Context(), rt, hermes.Event{SessionID: rt.liveID, InboundSequence: 42, Type: eventMessageComplete, Payload: json.RawMessage(`{"status":"complete"}`)})
	require.ErrorIs(t, queuedCtx.Err(), context.Canceled)
	require.True(t, s.cycleCancelled(&pending.cycle))
	require.False(t, s.cycleCancelled(c))
	a.attach(rec, nil)
	require.False(t, pending.accepted)
	s.mu.Lock()
	s.turn = nil
	s.mu.Unlock()
}
