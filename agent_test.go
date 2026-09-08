package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

type blockingSessionIDReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func TestInvalidConcurrencyOptionsFailWithoutPanickingOrLaunching(t *testing.T) {
	for _, limits := range []ConcurrencyLimits{
		{MaxActiveSessions: -1},
		{MaxConcurrentClientCalls: -1},
		{MaxActiveSessions: -1, MaxConcurrentClientCalls: -1},
	} {
		t.Run(fmt.Sprint(limits), func(t *testing.T) {
			agent := NewAgent(WithConcurrencyLimits(limits), func(options *Options) {
				options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
					t.Fatal("invalid options reached native startup")

					return nil, errors.New("unexpected native startup")
				}
			})
			t.Cleanup(func() { require.NoError(t, agent.Close()) })

			cwd := absTestPath("workspace")
			_, initializeErr := agent.Initialize(t.Context(), acp.InitializeRequest{})
			_, newErr := agent.NewSession(t.Context(), NewSessionRequest(cwd))
			_, loadErr := agent.LoadSession(t.Context(), LoadSessionRequest("session", cwd))
			_, resumeErr := agent.ResumeSession(t.Context(), ResumeSessionRequest("session", cwd))
			_, forkErr := agent.HandleExtensionMethod(t.Context(), ForkSessionMethod, mustJSON(t, ForkSessionRequest("session", cwd)))
			for _, err := range []error{initializeErr, newErr, loadErr, resumeErr, forkErr} {
				var requestErr *acp.RequestError
				require.ErrorAs(t, err, &requestErr)
				require.Equal(t, -32603, requestErr.Code)
				require.Equal(t, map[string]any{jsonFieldError: valHermesInvalidOptions}, requestErr.Data)
			}
		})
	}
}

func (r *blockingSessionIDReader) Read(buffer []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	clear(buffer)

	return len(buffer), nil
}

func TestServeCloseErrorAndAgentCloneFallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := newFakeHermesClient()
	client.closeErr = errors.Join(errors.New("close failed"), ErrContainmentIncomplete)
	agent := newTestAgent()
	session := testSession(t, agent, client)
	agent.sessions[session.id] = session

	started := make(chan struct{})
	oldNewAgent := newAgentForServe
	newAgentForServe = func(...Option) *Agent {
		close(started)

		return agent
	}
	t.Cleanup(func() { newAgentForServe = oldNewAgent })
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		_ = input.Close()
		_ = inputWriter.Close()
	})
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, input, io.Discard) }()
	<-started
	cancel()
	if err := <-errCh; !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("Serve close proof error = %v", err)
	}

	oldMarshal := agentJSONMarshal
	oldUnmarshal := agentJSONUnmarshal
	t.Cleanup(func() {
		agentJSONMarshal = oldMarshal
		agentJSONUnmarshal = oldUnmarshal
	})
	agentJSONMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal failed") }
	if cloneClientCapabilities(acp.ClientCapabilities{Meta: map[string]any{"a": "b"}}).Meta["a"] != "b" {
		t.Fatal("cloneClientCapabilities marshal fallback changed caps")
	}
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	if agent.clientElicitationCapabilities() == nil {
		t.Fatal("clientElicitationCapabilities marshal fallback returned nil")
	}
	agentJSONMarshal = oldMarshal
	agentJSONUnmarshal = func([]byte, any) error { return errors.New("unmarshal failed") }
	if cloneClientCapabilities(acp.ClientCapabilities{Meta: map[string]any{"a": "b"}}).Meta["a"] != "b" {
		t.Fatal("cloneClientCapabilities unmarshal fallback changed caps")
	}
	if agent.clientElicitationCapabilities() == nil {
		t.Fatal("clientElicitationCapabilities unmarshal fallback returned nil")
	}
}

func TestAgentCloseSingleflightPreservesContainmentEvidence(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	client.closeFunc = func(context.Context) error {
		startedOnce.Do(func() { close(started) })
		<-release

		return ErrContainmentIncomplete
	}
	agent := newTestAgent()
	session := testSession(t, agent, client)
	agent.sessions[session.id] = session

	results := make(chan error, 2)
	go func() { results <- agent.Close() }()
	<-started
	go func() { results <- agent.Close() }()

	select {
	case err := <-results:
		t.Fatalf("concurrent Close returned before shared cleanup completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	for range 2 {
		require.ErrorIs(t, <-results, ErrContainmentIncomplete)
	}
	require.Equal(t, 1, client.closeCalls)
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

func TestCloseAndServeJoinAdmittedIncompleteSessionConstruction(t *testing.T) {
	previous := newAgentForServe
	t.Cleanup(func() { newAgentForServe = previous })

	spawnStarted := make(chan struct{})
	releaseSpawn := make(chan struct{})
	agent := newTestAgent(
		WithScratchDir(durableTempDir(t)),
		WithLogger(slog.New(slog.DiscardHandler)),
		func(options *Options) {
			options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
				close(spawnStarted)
				<-releaseSpawn

				return nil, ErrContainmentIncomplete
			}
		},
	)

	newSessionErr := make(chan error, 1)
	go func() {
		_, err := agent.NewSession(context.Background(), NewSessionRequest(durableTempDir(t)))
		newSessionErr <- err
	}()
	<-spawnStarted

	serveCreated := make(chan struct{})
	newAgentForServe = func(...Option) *Agent {
		close(serveCreated)

		return agent
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		_ = input.Close()
		_ = inputWriter.Close()
	})
	serveErr := make(chan error, 1)
	go func() { serveErr <- Serve(serveCtx, input, io.Discard) }()
	<-serveCreated
	cancelServe()

	closeErr := make(chan error, 1)
	go func() { closeErr <- agent.Close() }()
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()

		return agent.closed
	}, time.Second, time.Millisecond)

	select {
	case err := <-closeErr:
		t.Fatalf("Close returned before admitted construction published containment: %v", err)
	default:
	}
	select {
	case err := <-serveErr:
		t.Fatalf("Serve returned before admitted construction published containment: %v", err)
	default:
	}

	close(releaseSpawn)
	require.ErrorIs(t, <-newSessionErr, ErrContainmentIncomplete)
	require.ErrorIs(t, <-closeErr, ErrContainmentIncomplete)
	require.ErrorIs(t, <-serveErr, ErrContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

func TestClosedAgentRejectsConstructionsAtLateAdmissionPoints(t *testing.T) {
	t.Run("new session after id allocation", func(t *testing.T) {
		oldReader := sessionIDRandReader
		reader := &blockingSessionIDReader{entered: make(chan struct{}), release: make(chan struct{})}
		sessionIDRandReader = reader
		t.Cleanup(func() { sessionIDRandReader = oldReader })

		agent := newTestAgent()
		done := make(chan error, 1)
		go func() {
			_, err := agent.NewSession(context.Background(), NewSessionRequest(durableTempDir(t)))
			done <- err
		}()
		<-reader.entered
		require.NoError(t, agent.Close())
		close(reader.release)
		require.ErrorContains(t, <-done, "agent closed")
	})

	t.Run("fork", func(t *testing.T) {
		agent := newTestAgent()
		require.NoError(t, agent.Close())
		_, err := agent.forkSession(t.Context(), acp.UnstableForkSessionRequest{SessionId: "parent", Cwd: durableTempDir(t)})
		require.ErrorContains(t, err, "agent closed")
	})
}

// A containment verdict is terminal for the agent's life: the wrapper cannot
// forget a tree it failed to prove empty, whatever happens to the session
// afterwards. The id that failed the boundary stays addressable, because the
// boundary is still owed, and the embedded shutdown still reports the verdict
// its sweep re-reaches.
func TestFailedContainmentEvidenceRemainsTerminal(t *testing.T) {
	client := newFakeHermesClient()
	client.closeErr = ErrContainmentIncomplete
	agent := newTestAgent()
	session := testSession(t, agent, client)
	agent.sessions[session.id] = session

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.NotNil(t, agent.activeSession(session.id), "the failed boundary detached the id its retry needs")
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

func TestServePreservesContainmentEvidenceAfterFailedClose(t *testing.T) {
	client := newFakeHermesClient()
	client.closeErr = ErrContainmentIncomplete
	agent := newTestAgent()
	session := testSession(t, agent, client)
	agent.sessions[session.id] = session
	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.ErrorIs(t, err, ErrContainmentIncomplete)

	started := make(chan struct{})
	previousNewAgent := newAgentForServe
	newAgentForServe = func(...Option) *Agent {
		close(started)

		return agent
	}
	t.Cleanup(func() { newAgentForServe = previousNewAgent })
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		_ = input.Close()
		_ = inputWriter.Close()
	})
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- Serve(ctx, input, io.Discard) }()
	<-started
	cancel()
	require.ErrorIs(t, <-result, ErrContainmentIncomplete)
}

func TestLifecycleAdmissionRemainingBranches(t *testing.T) {
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{
		MaxActiveSessions: 2, MaxConcurrentClientCalls: 1,
	}))
	session := testSession(t, agent, newFakeHermesClient())
	agent.sessions[session.id] = session
	_, _, firstRelease, err := agent.beginActiveReuse(t.Context(), session.id)
	require.NoError(t, err)
	if _, _, _, err := agent.beginActiveReuse(t.Context(), session.id); err == nil {
		t.Fatal("reuse admission ignored its bound")
	}
	firstRelease()

	existing, reuseCtx, release, reuseErr := agent.beginActiveReuse(t.Context(), session.id)
	require.NoError(t, reuseErr)
	require.Same(t, session, existing)
	agent.mu.Lock()
	delete(agent.sessions, session.id)
	agent.mu.Unlock()
	if _, err := agent.completeActiveReuse(reuseCtx, session.id, session, false, release); err == nil {
		t.Fatal("replaced active session completed reuse")
	}
	release()

	leaseCtx := context.WithValue(t.Context(), clientCallLeaseKey{}, &clientCallLease{agent: agent})
	operationCtx, releaseOperation, operationErr := agent.beginClientOperation(leaseCtx)
	require.NoError(t, operationErr)
	require.Equal(t, leaseCtx, operationCtx)
	releaseOperation()
	agent.clientCalls <- struct{}{}
	if _, _, err := agent.beginClientOperation(t.Context()); err == nil {
		t.Fatal("client operation ignored backpressure")
	}
	<-agent.clientCalls

	closed := newTestAgent()
	require.NoError(t, closed.Close())
	if _, err := closed.storeStartedSessionWithOpening(t.Context(), session); err == nil {
		t.Fatal("closed agent stored a started session")
	}

	bounded := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{
		MaxActiveSessions: 1, MaxConcurrentClientCalls: 1,
	}))
	require.NoError(t, bounded.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	}))
	first := testSession(t, bounded, newFakeHermesClient())
	require.NoError(t, first.openLifecycleStream())
	require.NoError(t, bounded.storeStartedSession(first))
	second := testSession(t, bounded, newFakeHermesClient())
	require.NoError(t, second.openLifecycleStream())
	owed, storeErr := bounded.storeStartedSessionWithOpening(lifecycleRequestContext(t.Context(), 91), second)
	if storeErr == nil || owed != nil {
		t.Fatalf("bounded session store = owed %#v, err %v", owed, storeErr)
	}
	require.Empty(t, bounded.streamOpens, "failed started-session install retained its opening")

	deferredBounded := newTestAgent()
	require.NoError(t, deferredBounded.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	}))
	deferred := testSession(t, deferredBounded, newFakeHermesClient())
	require.NoError(t, deferred.openLifecycleStream())
	for range maxDeferredStreamOpens {
		deferredBounded.streamOpens = append(deferredBounded.streamOpens, &deferredStreamOpen{
			state: deferredStreamOpenPending,
		})
	}
	if got, err := deferredBounded.storeStartedSessionWithOpening(lifecycleRequestContext(t.Context(), 92), deferred); err == nil || got != nil {
		t.Fatalf("deferred opening backpressure = owed %#v, err %v", got, err)
	}
	deferredBounded.streamOpens = nil
}

func TestFailedSessionStartContainmentEvidenceRemainsTerminal(t *testing.T) {
	client := newFakeHermesClient()
	client.createErr = errors.New("create failed")
	client.closeErr = ErrContainmentIncomplete
	agent := newTestAgent(func(options *Options) {
		options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return client, nil
		}
	})

	_, err := agent.NewSession(t.Context(), NewSessionRequest(durableTempDir(t)))
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

// TestExtensionRouteRefusalsCarryClosedTokens pins the extension routes'
// refusal vocabulary. The fork route reports the same two tokens the stable
// routes' decoder reports, and neither carries decoder prose: a JSON syntax
// error quotes the offending byte of the request, which is host input this
// surface never echoes back.
func TestExtensionRouteRefusalsCarryClosedTokens(t *testing.T) {
	t.Parallel()

	agent := newTestAgent()
	ctx := context.Background()

	for _, test := range []struct {
		name   string
		params string
		want   string
	}{
		{"undecodable", `{"cwd":`, valUnsupported},
		{"decoded but invalid", `{}`, valUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := agent.HandleExtensionMethod(ctx, ForkSessionMethod, json.RawMessage(test.params))

			var reqErr *acp.RequestError
			require.ErrorAs(t, err, &reqErr)
			require.Equal(t, -32602, reqErr.Code)
			require.Equal(t, map[string]any{jsonFieldError: test.want, jsonFieldField: keyParams}, reqErr.Data)
		})
	}
}

// TestUnknownExtensionMethodNamesTheMethod pins the -32601 data an unrouted
// extension carries. It names the method the peer asked for, matching what the
// SDK dispatcher emits for a core method it cannot route, so one connection
// answers the code one way.
func TestUnknownExtensionMethodNamesTheMethod(t *testing.T) {
	t.Parallel()

	_, err := newTestAgent().HandleExtensionMethod(context.Background(),
		"_hermes/does/not/exist", json.RawMessage(`{}`))

	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32601, reqErr.Code)
	require.Equal(t, acp.NewMethodNotFound("_hermes/does/not/exist").Data, reqErr.Data)
}
