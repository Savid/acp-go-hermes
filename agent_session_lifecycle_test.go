package hermesacp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestConcurrentColdLoadPublishesOneNativeIncarnation(t *testing.T) {
	cwd := t.TempDir()
	store := validHydrateStore(t, t.Context())
	first := newFakeHermesClient()
	first.getSession = testNativeSession("n")
	second := newFakeHermesClient()
	second.getSession = testNativeSession("n")
	entered := make(chan struct{})
	release := make(chan struct{})
	secondStart := make(chan struct{}, 1)
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	var (
		factoryMu    sync.Mutex
		factoryCalls int
	)
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()), WithMeterProvider(meterProvider), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			factoryMu.Lock()
			factoryCalls++
			call := factoryCalls
			factoryMu.Unlock()
			client := first
			if call == 1 {
				first.xdg = opts.ExistingXDG
				close(entered)
			} else {
				client = second
				second.xdg = opts.ExistingXDG
				secondStart <- struct{}{}
			}
			<-release

			return client, nil
		}
	})

	results := make(chan error, 2)
	go func() {
		_, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", cwd))
		results <- err
	}()
	<-entered
	go func() {
		_, err := agent.ResumeSession(t.Context(), ResumeSessionRequest("s", cwd))
		results <- err
	}()
	waitForLifecycleLeaseRefs(t, agent, "s", 2)
	select {
	case <-secondStart:
		t.Fatal("same-id waiter launched a second native incarnation")
	default:
	}
	close(release)
	require.NoError(t, <-results)
	require.NoError(t, <-results)

	factoryMu.Lock()
	require.Equal(t, 1, factoryCalls)
	factoryMu.Unlock()
	require.NotNil(t, agent.activeSession("s"))
	require.Empty(t, agent.lifecycleLeases)
	require.NoError(t, agent.Close())
	require.Equal(t, 1, first.closeCount())
	require.Zero(t, second.closeCount())
	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Zero(t, lifecycleMetricSum(metrics, "acp_go_hermes.session.active"))
}

func TestCloseSessionWaitsForColdLoadPublication(t *testing.T) {
	cwd := t.TempDir()
	store := validHydrateStore(t, t.Context())
	client := newFakeHermesClient()
	client.getSession = testNativeSession("n")
	closeResult := make(chan error, 1)

	var agent *Agent
	agent = newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = opts.ExistingXDG
			go func() {
				_, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "s"})
				closeResult <- closeErr
			}()
			waitForLifecycleLeaseRefs(t, agent, "s", 2)

			return client, nil
		}
	})

	_, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", cwd))
	require.NoError(t, err)
	require.NoError(t, <-closeResult)
	require.Nil(t, agent.activeSession("s"))
	require.Equal(t, 1, client.closeCount())
	require.Empty(t, agent.lifecycleLeases)
}

type gatedLifecycleDeleteStore struct {
	*InMemorySessionStore
	entered chan struct{}
	release chan struct{}
}

func (s *gatedLifecycleDeleteStore) Delete(ctx context.Context, key SessionKey) error {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	return s.InMemorySessionStore.Delete(ctx, key)
}

func TestDeleteTombstonesBeforeWaitingLoadCanOpen(t *testing.T) {
	cwd := t.TempDir()
	store := &gatedLifecycleDeleteStore{
		InMemorySessionStore: validHydrateStore(t, t.Context()),
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}
	factoryCalls := 0
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()), func(options *Options) {
		options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			factoryCalls++

			return newFakeHermesClient(), nil
		}
	})
	deleteResult := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest("s"))
		deleteResult <- err
	}()
	<-store.entered
	loadResult := make(chan error, 1)
	go func() {
		_, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", cwd))
		loadResult <- err
	}()
	waitForLifecycleLeaseRefs(t, agent, "s", 2)
	close(store.release)
	require.NoError(t, <-deleteResult)
	requireUnknownSession(t, <-loadResult)
	require.Zero(t, factoryCalls)
	require.True(t, agent.isDeleted("s"))
	require.Nil(t, agent.activeSession("s"))
	require.Empty(t, agent.lifecycleLeases)
}

func TestActiveCarrierRebindSerializesASecondOpen(t *testing.T) {
	cwd := t.TempDir()
	oldDir := t.TempDir()
	first := newFakeHermesClient()
	first.createSession = testNativeSession("n")
	first.getSession = first.createSession
	second := newFakeHermesClient()
	second.getSession = first.createSession
	third := newFakeHermesClient()
	third.getSession = first.createSession
	secondEntered := make(chan struct{})
	releaseSecond := make(chan struct{})
	thirdStart := make(chan struct{}, 1)

	var (
		factoryMu    sync.Mutex
		factoryCalls int
	)
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()), WithScratchDir(t.TempDir()), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			factoryMu.Lock()
			factoryCalls++
			call := factoryCalls
			factoryMu.Unlock()
			switch call {
			case 1:
				first.xdg = opts.ExistingXDG

				return first, nil
			case 2:
				second.xdg = opts.ExistingXDG
				close(secondEntered)
				<-releaseSecond

				return second, nil
			default:
				third.xdg = opts.ExistingXDG
				thirdStart <- struct{}{}

				return third, nil
			}
		}
	})
	created, err := agent.NewSession(t.Context(), NewSessionRequest(cwd, WithSessionHermesOptions(HermesOptions{
		Env: map[string]string{"TOKEN": "old"}, ExtraPathDirs: []string{oldDir},
	})))
	require.NoError(t, err)
	oldSession := agent.activeSession(created.SessionId)

	request := ResumeSessionRequest(created.SessionId, cwd, WithSessionHermesOptions(HermesOptions{
		Env: map[string]string{"TOKEN": "new"}, ExtraPathDirs: []string{oldDir},
	}))
	results := make(chan error, 2)
	go func() {
		_, resumeErr := agent.ResumeSession(t.Context(), request)
		results <- resumeErr
	}()
	<-secondEntered
	go func() {
		_, resumeErr := agent.ResumeSession(t.Context(), request)
		results <- resumeErr
	}()
	waitForLifecycleLeaseRefs(t, agent, created.SessionId, 2)
	select {
	case <-thirdStart:
		t.Fatal("second open crossed active carrier replacement")
	default:
	}
	close(releaseSecond)
	require.NoError(t, <-results)
	require.NoError(t, <-results)

	factoryMu.Lock()
	require.Equal(t, 2, factoryCalls)
	factoryMu.Unlock()
	require.Equal(t, 1, first.closeCount())
	active := agent.activeSession(created.SessionId)
	require.NotSame(t, oldSession, active)
	managed, ok := active.client.(*managedHermesServer)
	require.True(t, ok)
	require.Same(t, second, managed.Server)
	require.Empty(t, agent.lifecycleLeases)
	require.NoError(t, agent.Close())
	require.Equal(t, 1, second.closeCount())
	require.Zero(t, third.closeCount())
}

func TestSessionLifecycleLeaseCancellationAndIndependentIDs(t *testing.T) {
	agent := newTestAgent()
	_, releaseFirst, err := agent.acquireSessionLifecycle(t.Context(), "first")
	require.NoError(t, err)

	_, releaseOther, err := agent.acquireSessionLifecycle(t.Context(), "other")
	require.NoError(t, err)
	releaseOther()

	waitCtx, cancel := context.WithCancel(t.Context())
	waitResult := make(chan error, 1)
	go func() {
		_, release, acquireErr := agent.acquireSessionLifecycle(waitCtx, "first")
		if release != nil {
			release()
		}
		waitResult <- acquireErr
	}()
	waitForLifecycleLeaseRefs(t, agent, "first", 2)
	cancel()
	select {
	case acquireErr := <-waitResult:
		require.ErrorIs(t, acquireErr, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled lifecycle waiter did not return")
	}
	waitForLifecycleLeaseRefs(t, agent, "first", 1)
	releaseFirst()
	require.Empty(t, agent.lifecycleLeases)
}

func TestAgentCloseCancelsAndAwaitsSessionLifecycleLease(t *testing.T) {
	agent := newTestAgent()
	operationCtx, release, err := agent.acquireSessionLifecycle(t.Context(), "s")
	require.NoError(t, err)
	closed := make(chan error, 1)
	go func() { closed <- agent.Close() }()

	select {
	case <-operationCtx.Done():
		require.Contains(t, context.Cause(operationCtx).Error(), agentClosedMessage)
	case <-time.After(time.Second):
		t.Fatal("Agent.Close did not cancel the lifecycle holder")
	}
	select {
	case <-closed:
		t.Fatal("Agent.Close returned before the lifecycle holder released")
	default:
	}
	release()
	require.NoError(t, <-closed)
	require.Empty(t, agent.lifecycleLeases)
}

func TestDuplicateSessionPublicationFailsClosed(t *testing.T) {
	agent := newTestAgent()
	installedClient := newFakeHermesClient()
	installed := testSession(agent, installedClient)
	require.NoError(t, agent.storeStartedSession(installed))

	refusedClient := newFakeHermesClient()
	refused := testSession(agent, refusedClient)
	refused.id = installed.id
	_, err := agent.storeStartedSessionWithOpening(t.Context(), refused)
	require.Error(t, err)
	var requestErr *acp.RequestError
	require.True(t, errors.As(err, &requestErr))
	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, valSessionActive, data[jsonFieldError])
	require.Error(t, agent.refuseStartedSession(t.Context(), refused, err))
	require.Same(t, installed, agent.activeSession(installed.id))
	require.Zero(t, installedClient.closeCount())
	require.Equal(t, 1, refusedClient.closeCount())
	require.NoError(t, agent.Close())
	require.Equal(t, 1, installedClient.closeCount())
}

func waitForLifecycleLeaseRefs(t *testing.T, agent *Agent, id acp.SessionId, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()

		lease := agent.lifecycleLeases[id]

		return lease != nil && lease.refs == want
	}, time.Second, time.Millisecond)
}

func lifecycleMetricSum(metrics metricdata.ResourceMetrics, name string) int64 {
	var sum int64
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			data, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range data.DataPoints {
				sum += point.Value
			}
		}
	}

	return sum
}

func TestAgentAndSessionLifecycleAdmissionResidualBranches(t *testing.T) {
	closed := newTestAgent()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := closed.beginActiveReuse(t.Context(), "session"); err == nil {
		t.Fatal("closed agent admitted active reuse")
	}
	if _, _, err := closed.acquireSessionLifecycle(t.Context(), "session"); err == nil {
		t.Fatal("closed agent admitted session lifecycle")
	}
	if _, err := closed.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "session"}); err == nil {
		t.Fatal("closed agent admitted session close")
	}

	agent := newTestAgent()
	session := testSession(agent, newFakeHermesClient())
	agent.sessions[session.id] = session
	if err := agent.cleanupFailedStartedSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if agent.activeSession(session.id) != nil {
		t.Fatal("failed started session remained active")
	}
}
