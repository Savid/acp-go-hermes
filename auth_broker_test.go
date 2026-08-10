package hermesacp

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// brokerLaunches counts the native launches an agent performs and answers each
// with the supplied gateway, which is how a test tells "the broker started a
// process" apart from "a leg reused the one already running".
type brokerLaunches struct {
	mu       sync.Mutex
	launches int
}

func (l *brokerLaunches) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.launches
}

func (l *brokerLaunches) record() {
	l.mu.Lock()
	l.launches++
	l.mu.Unlock()
}

// withCountingClientFactory answers every native launch with client and counts
// them.
func withCountingClientFactory(launches *brokerLaunches, client nativehermes.Server) Option {
	return func(options *Options) {
		options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			launches.record()

			return client, nil
		}
	}
}

// newBrokerAgent builds an agent with a usable provider-auth root whose native
// launches resolve through factory, and one registered session backed by a
// gateway of its own. The two gateways are deliberately different objects: a
// leg that answers from the session's process rather than the broker's is then
// a visible failure rather than a coincidence.
func newBrokerAgent(t *testing.T, factory Option) (*Agent, *fakeHermesClient) {
	t.Helper()

	agent := newTestAgent(
		WithProviderAuthRoot(t.TempDir()),
		WithProviderAuthHome(t.TempDir()),
		WithScratchDir(t.TempDir()),
		factory,
	)
	if agent.providerAuth == nil {
		t.Fatal("provider auth surface is unavailable with a usable root")
	}

	t.Cleanup(func() { _ = agent.Close() })

	sessionClient := newFakeHermesClient()
	sessionClient.xdg = nativehermes.XDGDirs{Root: t.TempDir()}

	session := newSession(agent, testSessionID, "/cwd", nil, nil, nativehermes.Session{ID: "native"}, sessionClient, sessionMeta{}, idmapRecord{})
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	return agent, sessionClient
}

// requireBrokerCatalog drives the catalog leg and reports the provider ids it
// answered with.
func requireBrokerCatalog(t *testing.T, agent *Agent) []string {
	t.Helper()

	result, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("methods: %v", err)
	}

	providers := mustType[authMethodsResult](t, result).Providers

	ids := make([]string, 0, len(providers))
	for id := range providers {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

// TestAuthLegsAreAnsweredByTheBrokerAndCostOneNativeStart is the whole point of
// the broker. The catalog leg is a single credential-residence read, and it used
// to be answered by whatever process the naming ACP session happened to own —
// so an owner's first click on an idle agent paid a full cold native start
// before the read could run, and a second click paid another. Here the session
// has a gateway of its own and never answers: one broker process does, and it
// stays up across every later leg.
func TestAuthLegsAreAnsweredByTheBrokerAndCostOneNativeStart(t *testing.T) {
	t.Parallel()

	broker := newFakeHermesClient()
	broker.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode}}

	var launches brokerLaunches

	agent, sessionClient := newBrokerAgent(t, withCountingClientFactory(&launches, broker))
	sessionClient.authProviders = []nativehermes.AuthProvider{{ID: "session-process", Name: "Session Process", Flow: nativehermes.AuthFlowDeviceCode}}

	for attempt := range 3 {
		ids := requireBrokerCatalog(t, agent)
		if len(ids) != 1 || ids[0] != testProviderID {
			t.Fatalf("catalog attempt %d answered %v, want the broker's providers", attempt, ids)
		}

		if got := launches.count(); got != 1 {
			t.Fatalf("catalog attempt %d left %d native launches, want 1", attempt, got)
		}
	}

	if _, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)}); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if got := launches.count(); got != 1 {
		t.Fatalf("a second leg left %d native launches, want 1", got)
	}
}

// TestAuthBrokerStartFailureRefusesTheLegAndIsRetriedByTheNextOne keeps a failed
// launch from poisoning the surface. The refusal is the same closed transport
// cause an absent gateway has always produced — the native reason never crosses
// the wire — and the next leg tries again, because a harness that could not
// start once is routinely startable a moment later.
func TestAuthBrokerStartFailureRefusesTheLegAndIsRetriedByTheNextOne(t *testing.T) {
	t.Parallel()

	broker := newFakeHermesClient()
	broker.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode}}

	refused := errors.New("harness unavailable")

	var (
		mu       sync.Mutex
		attempts int
	)

	agent, _ := newBrokerAgent(t, func(options *Options) {
		options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			mu.Lock()
			attempts++
			first := attempts == 1
			mu.Unlock()

			if first {
				return nil, refused
			}

			return broker, nil
		}
	})

	_, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseTransport)

	if ids := requireBrokerCatalog(t, agent); len(ids) != 1 || ids[0] != testProviderID {
		t.Fatalf("catalog after a failed start answered %v", ids)
	}
}

// TestAuthBrokerRefusesEveryLegUnderConfigurationNoSessionMayRunUnder holds the
// broker to the same admission a session launch answers to. The broker is an
// extra native process this adapter starts on its own initiative, so an agent
// configured in a way no session may run under must not acquire one through the
// auth surface.
func TestAuthBrokerRefusesEveryLegUnderConfigurationNoSessionMayRunUnder(t *testing.T) {
	t.Parallel()

	launched := false

	agent, _ := newBrokerAgent(t, func(options *Options) {
		options.Home = "/configured-home"
		options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			launched = true

			return newFakeHermesClient(), nil
		}
	})

	_, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseTransport)

	if launched {
		t.Fatal("a rejected configuration still launched a native process")
	}
}

// TestAuthBrokerPanicInTheLaunchPathSettlesEveryWaitingLeg pins the failure mode
// that would be worst here: the start runs on a goroutine of its own, so a panic
// inside it must publish an outcome rather than leave every leg blocked on a
// start nobody will finish.
func TestAuthBrokerPanicInTheLaunchPathSettlesEveryWaitingLeg(t *testing.T) {
	t.Parallel()

	agent, _ := newBrokerAgent(t, func(options *Options) {
		options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			panic("launch path panicked")
		}
	})

	_, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseTransport)
}

// blockingBrokerFactory answers a launch only once release is closed, and
// reports the launch context so a test can observe whether an abandoned leg
// took the start down with it.
type blockingBrokerFactory struct {
	entered chan context.Context
	release chan struct{}
	client  nativehermes.Server
}

func newBlockingBrokerFactory(client nativehermes.Server) *blockingBrokerFactory {
	return &blockingBrokerFactory{
		entered: make(chan context.Context, 1),
		release: make(chan struct{}),
		client:  client,
	}
}

func (f *blockingBrokerFactory) option() Option {
	return func(options *Options) {
		options.clientFactory = func(ctx context.Context, _ nativehermes.StartOptions) (nativehermes.Server, error) {
			f.entered <- ctx

			select {
			case <-f.release:
				return f.client, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}

// TestAbandonedLegLeavesWithoutEndingTheStartTheNextLegNeeds is the cancellation
// contract. A cold native start costs seconds and the host that triggered it may
// give up first; the leg must return the moment its own context ends, and the
// start must survive it, or every abandoned request would destroy the process
// the next request is already waiting for.
func TestAbandonedLegLeavesWithoutEndingTheStartTheNextLegNeeds(t *testing.T) {
	t.Parallel()

	client := newFakeHermesClient()
	client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode}}

	factory := newBlockingBrokerFactory(client)
	agent, _ := newBrokerAgent(t, factory.option())

	abandoning, abandon := context.WithCancel(context.Background())
	left := make(chan error, 1)

	go func() {
		_, _, err := agent.providerAuth.broker.borrow(abandoning)
		left <- err
	}()

	startCtx := <-factory.entered

	abandon()

	if err := <-left; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned leg error = %v, want context canceled", err)
	}

	select {
	case <-startCtx.Done():
		t.Fatal("an abandoned leg ended the native start")
	default:
	}

	close(factory.release)

	if ids := requireBrokerCatalog(t, agent); len(ids) != 1 || ids[0] != testProviderID {
		t.Fatalf("catalog after an abandoned leg answered %v", ids)
	}
}

// TestAgentCloseEndsAStartInFlightRatherThanWaitingOutTheColdPath keeps teardown
// bounded. Close is the only handle left on a start no leg owns any more, so it
// must end that start instead of joining a readiness poll and a method sweep
// that answer to no host's close budget.
func TestAgentCloseEndsAStartInFlightRatherThanWaitingOutTheColdPath(t *testing.T) {
	t.Parallel()

	factory := newBlockingBrokerFactory(newFakeHermesClient())
	agent, _ := newBrokerAgent(t, factory.option())

	go func() {
		client, release, ok := agent.providerAuth.nativeClient(context.Background())
		if ok {
			_ = client
			release()
		}
	}()

	startCtx := <-factory.entered

	closed := make(chan error, 1)
	go func() { closed <- agent.Close() }()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(closeTimeout):
		t.Fatal("close waited out the native start instead of ending it")
	}

	select {
	case <-startCtx.Done():
	default:
		t.Fatal("close left the native start running")
	}
}

// TestAuthBrokerCompletesItsProcessOnAgentCloseAndRefusesEveryLaterLeg is the
// lifetime the broker trades for: it outlives every ACP session, and the agent
// is what ends it. Nothing this adapter started may be left running afterwards.
func TestAuthBrokerCompletesItsProcessOnAgentCloseAndRefusesEveryLaterLeg(t *testing.T) {
	t.Parallel()

	broker := newFakeHermesClient()
	broker.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode}}

	var launches brokerLaunches

	agent, _ := newBrokerAgent(t, withCountingClientFactory(&launches, broker))
	requireBrokerCatalog(t, agent)

	if err := agent.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if broker.closeCount() != 1 {
		t.Fatal("agent close left the broker's native process running")
	}

	// Close is idempotent, and a leg arriving afterwards is refused rather than
	// starting a replacement process for an agent that is gone.
	if err := agent.providerAuth.closeBroker(context.Background()); err != nil {
		t.Fatalf("second close: %v", err)
	}

	if _, _, ok := agent.providerAuth.nativeClient(context.Background()); ok {
		t.Fatal("a closed broker still lent a native client")
	}

	if got := launches.count(); got != 1 {
		t.Fatalf("%d native launches survived close, want 1", got)
	}
}
