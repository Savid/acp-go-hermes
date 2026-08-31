package hermesacp

import (
	"context"
	"errors"
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

func (r *blockingSessionIDReader) Read(buffer []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	clear(buffer)

	return len(buffer), nil
}

func TestServeCloseErrorAndAgentCloneFallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := newFakeHermesClient()
	client.closeErr = errors.Join(errors.New("close failed"), ErrProcessContainmentIncomplete)
	agent := newTestAgent()
	session := testSession(agent, client)
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
	if err := <-errCh; !errors.Is(err, ErrProcessContainmentIncomplete) {
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

		return ErrProcessContainmentIncomplete
	}
	agent := newTestAgent()
	session := testSession(agent, client)
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
		require.ErrorIs(t, <-results, ErrProcessContainmentIncomplete)
	}
	require.Equal(t, 1, client.closeCalls)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)
}

func TestCloseAndServeJoinAdmittedIncompleteSessionConstruction(t *testing.T) {
	previous := newAgentForServe
	t.Cleanup(func() { newAgentForServe = previous })

	spawnStarted := make(chan struct{})
	releaseSpawn := make(chan struct{})
	agent := newTestAgent(
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
		func(options *Options) {
			options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
				close(spawnStarted)
				<-releaseSpawn

				return nil, nativehermes.ErrProcessContainmentIncomplete
			}
		},
	)

	newSessionErr := make(chan error, 1)
	go func() {
		_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
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
	require.ErrorIs(t, <-newSessionErr, nativehermes.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, <-closeErr, nativehermes.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, <-serveErr, nativehermes.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), nativehermes.ErrProcessContainmentIncomplete)
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
			_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
			done <- err
		}()
		<-reader.entered
		require.NoError(t, agent.Close())
		close(reader.release)
		require.ErrorContains(t, <-done, "agent closed")
	})

	t.Run("load after deleted cleanup", func(t *testing.T) {
		oldReap := reapHermesLeaseFile
		entered := make(chan struct{})
		release := make(chan struct{})
		reapHermesLeaseFile = func(string, *slog.Logger) bool {
			close(entered)
			<-release

			return false
		}
		t.Cleanup(func() { reapHermesLeaseFile = oldReap })

		agent := newTestAgent()
		cleanupRoot := t.TempDir()
		agent.deleteCleanup["deleted"] = deleteCleanupRecord{SessionID: "deleted", XDGRoot: cleanupRoot}
		done := make(chan error, 1)
		go func() {
			_, err := agent.loadOrResumeSession(context.Background(), "load", t.TempDir(), nil, nil, nil, false)
			done <- err
		}()
		<-entered
		require.NoError(t, agent.Close())
		close(release)
		require.ErrorContains(t, <-done, "agent closed")
	})

	t.Run("fork", func(t *testing.T) {
		agent := newTestAgent()
		require.NoError(t, agent.Close())
		_, err := agent.forkSession(t.Context(), acp.UnstableForkSessionRequest{SessionId: "parent", Cwd: t.TempDir()})
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
	client.closeErr = ErrProcessContainmentIncomplete
	agent := newTestAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	require.NotNil(t, agent.activeSession(session.id), "the failed boundary detached the id its retry needs")
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)
}

func TestServePreservesContainmentEvidenceAfterFailedClose(t *testing.T) {
	client := newFakeHermesClient()
	client.closeErr = ErrProcessContainmentIncomplete
	agent := newTestAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session
	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

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
	require.ErrorIs(t, <-result, ErrProcessContainmentIncomplete)
}

func TestLifecycleAdmissionRemainingBranches(t *testing.T) {
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{
		MaxActiveSessions: 2, MaxConcurrentClientCalls: 1,
	}))
	session := testSession(agent, newFakeHermesClient())
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
	bounded.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	first := testSession(bounded, newFakeHermesClient())
	require.NoError(t, first.openLifecycleStream())
	require.NoError(t, bounded.storeStartedSession(first))
	second := testSession(bounded, newFakeHermesClient())
	require.NoError(t, second.openLifecycleStream())
	owed, storeErr := bounded.storeStartedSessionWithOpening(lifecycleRequestContext(t.Context(), 91), second)
	if storeErr == nil || owed != nil {
		t.Fatalf("bounded session store = owed %#v, err %v", owed, storeErr)
	}
	require.Empty(t, bounded.streamOpens, "failed started-session install retained its opening")

	deferredBounded := newTestAgent()
	deferredBounded.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	deferred := testSession(deferredBounded, newFakeHermesClient())
	require.NoError(t, deferred.openLifecycleStream())
	for index := 0; index < maxDeferredStreamOpens; index++ {
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
	client.closeErr = ErrProcessContainmentIncomplete
	agent := newTestAgent(func(options *Options) {
		options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return client, nil
		}
	})

	_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)
}
