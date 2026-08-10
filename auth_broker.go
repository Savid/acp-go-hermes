package hermesacp

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// authBrokerSessionID is the identity the broker's native generation is
// accounted under. The broker is not an ACP session — no host can address,
// list, prompt, load, or fork it — but every native launch this adapter makes
// is tracked against an id, so the broker carries a reserved one that names
// what it is.
const authBrokerSessionID = acp.SessionId("acp-go-hermes/provider-auth-broker")

const authBrokerGoroutine = "provider auth broker start"

// errAuthBrokerUnavailable is the internal reason a leg could not reach a
// native gateway. It never crosses the wire: every leg maps it to the same
// closed transport cause an absent client has always produced.
var errAuthBrokerUnavailable = errors.New("provider auth broker is unavailable")

// authBroker owns the native Hermes process the provider-auth legs speak to.
//
// The legs address a credential residence, not a conversation: every one of
// them is a values-free call against the durable ProviderAuthHome, and none of
// them needs a native session, a model, or a prompt turn. Binding them to an
// ACP session's process made the first login of an idle agent pay a whole cold
// native start — a second process, a port bind, a readiness poll, a websocket
// dial and a method sweep — before the owner's actual request, a single
// loopback GET, could run. It also gave a running login the lifetime of a
// session the host is free to tear down underneath it.
//
// So the process here is started lazily on the first leg that needs it and
// lives as long as the agent does. The session id every leg carries keeps its
// whole meaning: it is still the fence that admits the leg, still what
// publishes and addresses a flow, and still what close sweeps. It simply no
// longer decides which process answers.
//
// Lifecycle is reference counted. Each leg borrows the client for the length of
// its native call, close refuses every later borrow, and close joins both the
// start that may still be in flight and every borrow still outstanding before
// it completes the process. That join is what makes agent close contain the
// broker: a native process this adapter started must never outlive it.
type authBroker struct {
	agent *Agent

	mu sync.Mutex
	// server is the live native gateway, nil until a start settles one. A start
	// that failed leaves it nil and logs why: the native reason is deliberately
	// not carried back to the leg, because every leg answers a failure to reach
	// a gateway with the one closed transport cause regardless of its detail.
	server nativehermes.Server
	// starting is non-nil while a start is in flight and is closed when that
	// start settles, so concurrent legs share one native start instead of each
	// launching a process.
	starting chan struct{}
	// cancelStart ends the start in flight. It is the only handle left on that
	// start, because the start deliberately outlives the leg that triggered it,
	// so close is what must carry it.
	cancelStart context.CancelFunc
	closed      bool

	// work counts the start in flight plus every outstanding borrow. Close waits
	// on it, and every increment happens under mu while closed is false, so no
	// increment can race the wait.
	work sync.WaitGroup
}

func newAuthBroker(agent *Agent) *authBroker {
	return &authBroker{agent: agent}
}

// borrow resolves the native client for one leg and holds a reference until the
// returned release runs. The first borrow starts the process; a borrow that
// arrives while a start is in flight waits for that start rather than beginning
// another, and leaves on its own context without disturbing it.
func (b *authBroker) borrow(ctx context.Context) (nativehermes.Server, func(), error) {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()

		return nil, nil, errAuthBrokerUnavailable
	}

	if b.server != nil {
		server := b.server
		b.work.Add(1)
		b.mu.Unlock()

		return server, sync.OnceFunc(b.work.Done), nil
	}

	settled := b.starting
	if settled == nil {
		settled = make(chan struct{})
		b.starting = settled

		b.work.Add(1)

		// The start is deliberately detached from the borrowing leg's context. A
		// cold native start costs seconds, and an owner who abandons the request
		// that triggered it must not destroy the process the next request is
		// already waiting for. The leg below still leaves the moment its own
		// context ends, and close keeps the cancel so the start is never
		// unstoppable.
		startCtx, cancelStart := context.WithCancel(context.WithoutCancel(ctx))
		b.cancelStart = cancelStart

		go b.start(startCtx, cancelStart, settled)
	}

	b.mu.Unlock()

	select {
	case <-settled:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// A start that failed leaves the broker startable again rather than poisoned:
	// a harness that could not launch once is routinely launchable a moment
	// later, and the next leg through takes that turn.
	if b.closed || b.server == nil {
		return nil, nil, errAuthBrokerUnavailable
	}

	b.work.Add(1)

	return b.server, sync.OnceFunc(b.work.Done), nil
}

// start launches the broker's native process and publishes the outcome to every
// waiting leg. It publishes from a defer so a panic in the launch path settles
// the waiters too, rather than leaving them blocked on a start nobody will
// finish.
func (b *authBroker) start(ctx context.Context, cancel context.CancelFunc, settled chan struct{}) {
	var (
		server nativehermes.Server
		err    error
	)

	defer cancel()
	defer recoverAgentGoroutine(ctx, b.agent.log, authBrokerGoroutine)
	defer b.work.Done()
	defer func() { b.settle(server, err, settled) }()

	// The broker launches under exactly the configuration a session launches
	// under, so an agent no session may run under starts no broker either.
	if err = b.agent.rejectInvalidConfiguration(); err != nil {
		return
	}

	// The broker owns no working directory: it answers credential-residence
	// calls, never a conversation about a project, so it is given the empty cwd
	// rather than a host's.
	server, err = b.agent.newHermesClient(ctx, authBrokerSessionID, "", sessionMeta{}, nativehermes.XDGDirs{})
}

// settle publishes the start's outcome and releases every waiting leg. A
// failure is logged here rather than returned, because this is the only place
// that still knows why the launch did not happen.
func (b *authBroker) settle(server nativehermes.Server, err error, settled chan struct{}) {
	b.mu.Lock()
	b.server = server
	b.starting = nil
	b.cancelStart = nil
	b.mu.Unlock()

	if err != nil {
		b.agent.log.DebugContext(context.Background(), "start provider auth broker failed", slog.String(jsonFieldError, err.Error()))
	}

	close(settled)
}

// close ends the broker. It refuses every later borrow, ends the start that may
// still be in flight, joins that start and every leg still holding a reference,
// and only then completes the native process, so nothing is torn down
// underneath a call this adapter is still making.
//
// Cancelling the start first is what keeps the join bounded by the caller's
// close budget instead of by a cold native start, which costs a readiness poll
// and a method sweep and answers to no host's deadline. A start that had
// already produced a process when the cancel landed still publishes it, so the
// process is completed here rather than outliving the agent.
func (b *authBroker) close(ctx context.Context) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()

		return nil
	}

	b.closed = true
	cancelStart := b.cancelStart
	b.cancelStart = nil
	b.mu.Unlock()

	if cancelStart != nil {
		cancelStart()
	}

	b.work.Wait()

	b.mu.Lock()
	server := b.server
	b.server = nil
	b.mu.Unlock()

	if server == nil {
		return nil
	}

	err := server.Close(ctx)

	// The broker's generation is accounted exactly like a session's: an
	// incomplete containment retains its root under the broker's reserved id
	// rather than being dropped because no ACP session owns it.
	b.agent.recordIncompleteContainment(err, authBrokerSessionID, hermesServerRoot(server))

	return err
}

// nativeClient resolves the broker's client for one leg in the shape every leg
// consumes: a client and its release, or a false that carries no native detail.
func (p *providerAuth) nativeClient(ctx context.Context) (nativehermes.Server, func(), bool) {
	client, release, err := p.broker.borrow(ctx)
	if err != nil {
		return nil, nil, false
	}

	return client, release, true
}

// closeBroker completes the broker's native process as part of agent close.
func (p *providerAuth) closeBroker(ctx context.Context) error {
	return p.broker.close(ctx)
}
