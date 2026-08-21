package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/coder/websocket"
)

const (
	gatewayActorMailboxCapacity = 256
	gatewayPendingFrameCapacity = 256

	// gatewayPendingFrameLiveCapacity bounds one native session's share of the
	// retention budget. Hermes mirrors watched child runs onto live ids this
	// adapter never bound, and they arrive on the same transport as the session
	// being bound: no single live id may spend more than a quarter of what the
	// bind is waiting to claim.
	gatewayPendingFrameLiveCapacity = 64

	gatewayMappedDeliveryCapacity = 256

	// Cycle budgets bind state retained outside the actor mailbox. The mailbox
	// limits queued work; these limits independently bind one cycle that is
	// processed as fast as it arrives.
	gatewayCycleTextByteLimit        = 10 * 1024 * 1024
	gatewayCycleToolCountLimit       = 256
	gatewayCycleToolDataByteLimit    = 10 * 1024 * 1024
	gatewayCycleControlCountLimit    = 256
	gatewayCycleControlMapLimit      = 256
	gatewayCycleControlDataByteLimit = 10 * 1024 * 1024
	gatewayProjectionLimit           = 256
)

var (
	ErrGatewayActorOverflow  = errors.New("hermes gateway session actor overflow")
	ErrGatewayMappedOverflow = errors.New("hermes mapped event overflow")
	ErrGatewayAmbiguousTurn  = errors.New("hermes gateway turn correlation is ambiguous")
	ErrGatewayCycleOverflow  = errors.New("hermes gateway cycle state overflow")

	// ErrGatewayAgentBusy refuses a prompt because agent-origin work owns the
	// native stream. Hermes runs whole autonomous turns between prompts, and one
	// of them holding the session is backpressure the caller can retry, never a
	// reason to end the incarnation running it. The refusal happens before
	// submission, so retrying it cannot run the same text twice.
	ErrGatewayAgentBusy = errors.New("hermes gateway session is running agent-origin work")

	// ErrGatewayTurnAbsorbedPrompt reports a prompt hermes folded into the turn
	// it was already running. The text is not lost and not pending: it is inside
	// a turn whose output this adapter projects as agent-origin work. Resending
	// it would fold the same text in a second time, so this is an outcome and
	// never backpressure.
	ErrGatewayTurnAbsorbedPrompt = errors.New("hermes folded the prompt into the turn already running")

	// errGatewayCycleAbandoned states an agent-origin cycle that outlived the
	// native turn which opened it. Hermes admits a prompt synchronously only for
	// an idle session, so such a cycle has every frame it will ever get.
	errGatewayCycleAbandoned = errors.New("hermes agent-origin cycle ended without a native terminal frame")
)

type gatewayTransportDispatcher struct {
	generation   uint64
	client       *Client
	transport    *gatewayTransport
	done         chan struct{}
	barriers     chan gatewayDispatchBarrier
	handshakes   chan gatewayHandshakeCommand
	causeMu      sync.Mutex
	cause        error
	terminalOnce sync.Once

	pendingFrames         map[string][]Event
	pendingFrameCount     int
	pendingHandshakes     map[uint64]gatewayHandshakeKind
	beforeBarrierDrain    func()
	beforeHandshakeSelect func()
	beforeHandshakeCancel func()
}

type gatewayTransport struct {
	client     *Client
	generation uint64
	dispatcher *gatewayTransportDispatcher
	mappings   *gatewaySessionMappings
}

func (d *gatewayTransportDispatcher) setCause(cause error) error {
	cause = gatewayTransportCause(cause)

	d.causeMu.Lock()
	if d.cause == nil {
		d.cause = cause
	}

	installed := d.cause
	d.causeMu.Unlock()

	return installed
}

func (d *gatewayTransportDispatcher) terminalCause() error {
	if d == nil {
		return nil
	}

	d.causeMu.Lock()
	defer d.causeMu.Unlock()

	return d.cause
}

type gatewaySessionMappings struct {
	mu       sync.Mutex
	bindings map[string]gatewaySessionBinding
}

type gatewaySessionBinding struct {
	live  string
	actor *gatewaySessionActor
}

type gatewayHandshakeKind uint8

const (
	gatewayHandshakeCreate gatewayHandshakeKind = iota + 1
	gatewayHandshakeResume
)

type gatewayHandshakeCommand struct {
	action    gatewayHandshakeAction
	id        uint64
	kind      gatewayHandshakeKind
	stored    string
	live      string
	watermark GatewayWatermark
	reply     chan gatewayHandshakeResult
}

type gatewayHandshakeAction uint8

const (
	gatewayHandshakeRecord gatewayHandshakeAction = iota + 1
	gatewayHandshakeBind
	gatewayHandshakeTombstone
)

type gatewayHandshakeState uint8

const (
	gatewayHandshakeRecorded gatewayHandshakeState = iota + 1
	gatewayHandshakeCommitted
	gatewayHandshakeTombstoned
)

type gatewayHandshakeResult struct {
	state gatewayHandshakeState
	err   error
}

type gatewayDispatchBarrier struct {
	watermark uint64
	reply     chan error
}

type gatewayActorMessage struct {
	event      *Event
	generation uint64
	processed  chan struct{}
	bind       *gatewayActorBinding
	register   *gatewayPromptRegistration
	watermark  *gatewayPromptWatermark
	cancel     *gatewayPromptCancel
	release    *gatewayPromptRelease
	projected  *gatewayProjection
	control    *gatewayControlCommand
	end        chan bool
	failure    error
	terminal   bool
	stop       bool
}

type gatewayActorBinding struct {
	live       string
	generation uint64
	transport  *gatewayTransport
	done       chan struct{}
}

type gatewayControlKind uint8

const (
	gatewayControlPermission gatewayControlKind = iota + 1
	gatewayControlQuestion
)

type gatewayControlIdentity struct {
	kind      gatewayControlKind
	requestID string
}

type gatewayControlRoute struct {
	transport  *gatewayTransport
	mappings   *gatewaySessionMappings
	actor      *gatewaySessionActor
	generation uint64
	stored     string
	live       string
	cycleID    string
	requestID  string
	kind       gatewayControlKind
}

type gatewayControlCommand struct {
	ctx       context.Context //nolint:containedctx // Actor-owned RPC retains caller cancellation through its immediate RPC.
	route     *gatewayControlRoute
	kind      gatewayControlKind
	choice    string
	answers   any
	completed chan error
}

type gatewayPromptRegistration struct {
	id     string
	result chan gatewayCycleResult
	reply  chan gatewayPromptHandle

	mu        sync.Mutex
	cancelled bool
	cancelErr error
}

// gatewayPromptDisposition is hermes's own answer to one prompt.submit, read
// back to the actor that has to attribute the frames which follow it.
type gatewayPromptDisposition uint8

const (
	// gatewayPromptOwnsTurn is the "streaming" claim: hermes took an idle
	// session for this exact prompt and the turn that follows is this prompt's.
	gatewayPromptOwnsTurn gatewayPromptDisposition = iota

	// gatewayPromptQueuedTurn is the "queued" claim: a turn is already running,
	// and hermes will run this prompt as the next one.
	gatewayPromptQueuedTurn

	// gatewayPromptJoinedTurn is the "redirected"/"steered" claim: hermes folded
	// this prompt's text into the turn it is already running.
	gatewayPromptJoinedTurn
)

type gatewayPromptWatermark struct {
	cycleID     string
	watermark   uint64
	disposition gatewayPromptDisposition
	reply       chan gatewayPromptWatermarkResult
}

type gatewayPromptWatermarkResult struct {
	projections []*gatewayProjection

	// deferral carries the second half of a queued prompt's admission: the
	// registered cycle claims nothing until the running turn ends, and this
	// reports the projections that turn left behind when it does.
	deferral <-chan gatewayPromptWatermarkResult
	err      error
}

type gatewayPromptRelease struct {
	cycleID string
	reply   chan error
}

type gatewayPromptCancel struct {
	registrationID string
	err            error
	accepted       bool
	generation     uint64
	watermark      uint64
	done           chan struct{}
}

type gatewayPromptTombstone struct {
	generation uint64
	watermark  uint64
}

type gatewayPromptHandle struct {
	cycleID string
	result  <-chan gatewayCycleResult
	err     error
}

type gatewayCycleResult struct {
	message NativeMessage
	err     error
}

type gatewayProjection struct {
	done chan error
	once sync.Once
}

func (r *gatewayPromptRegistration) cancel(err error) {
	r.mu.Lock()
	if !r.cancelled {
		r.cancelled = true
		r.cancelErr = err
	}
	r.mu.Unlock()
}

func (r *gatewayPromptRegistration) cancellation() (error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.cancelErr, r.cancelled
}

func (p *gatewayProjection) resolve(actor *gatewaySessionActor, err error) {
	if p == nil {
		return
	}

	resolved := false

	p.once.Do(func() {
		resolved = true

		p.done <- err

		close(p.done)
	})

	if resolved && !actor.enqueue(gatewayActorMessage{projected: p}) {
		actor.server.failGatewayGeneration(actor.generation, ErrGatewayActorOverflow)
	}
}

func (p *gatewayProjection) resolveOnActorExit(err error) {
	p.once.Do(func() {
		p.done <- err

		close(p.done)
	})
}

type gatewaySessionActor struct {
	server *hermesServer
	stored string

	mailbox        chan gatewayActorMessage
	terminal       chan error
	done           chan struct{}
	mailboxMu      sync.Mutex
	terminalQueued bool

	generation      uint64
	live            string
	transport       *gatewayTransport
	cycles          uint64
	active          *gatewayCycle
	prompt          *gatewayCycle
	fencedPrompt    *gatewayPromptTombstone
	buffered        []Event
	projections     map[*gatewayProjection]struct{}
	beforeRegister  func()
	beforeWatermark func()
	beforeRelease   func()
	beforeCancel    func()
}

type gatewayCycle struct {
	id             string
	messageID      string
	origin         CycleOrigin
	registrationID string
	result         chan gatewayCycleResult

	watermark        uint64
	watermarkSet     bool
	held             bool
	deferred         bool
	deferral         chan gatewayPromptWatermarkResult
	heldEvents       []Event
	started          bool
	text             strings.Builder
	textBytes        int
	activeTools      map[string]struct{}
	toolStates       map[string]gatewayActiveTool
	toolParts        []Part
	toolCount        int
	toolDataBytes    int
	controls         map[gatewayControlIdentity]struct{}
	controlCount     int
	controlDataBytes int
	permissions      uint64
}

func (s *hermesServer) installGatewayDispatcher(client *Client) uint64 {
	if client == nil {
		return 0
	}

	s.connMu.Lock()
	if s.dispatchers == nil {
		s.dispatchers = make(map[uint64]*gatewayTransportDispatcher)
	}

	s.transportGeneration++
	generation := s.transportGeneration

	mappings := &gatewaySessionMappings{
		bindings: make(map[string]gatewaySessionBinding),
	}

	dispatcher := &gatewayTransportDispatcher{
		generation:        generation,
		client:            client,
		done:              make(chan struct{}),
		barriers:          make(chan gatewayDispatchBarrier),
		handshakes:        make(chan gatewayHandshakeCommand),
		pendingFrames:     make(map[string][]Event),
		pendingHandshakes: make(map[uint64]gatewayHandshakeKind),
	}
	transport := &gatewayTransport{
		client: client, generation: generation, dispatcher: dispatcher, mappings: mappings,
	}
	dispatcher.transport = transport

	s.dispatchers[generation] = dispatcher
	if s.beforeTransportPublish != nil {
		s.beforeTransportPublish()
	}

	s.transport = transport
	s.dispatcherWG.Add(1)
	s.connMu.Unlock()

	go s.dispatchGateway(dispatcher)

	return generation
}

func (s *hermesServer) dispatchGateway(dispatcher *gatewayTransportDispatcher) {
	defer s.dispatcherWG.Done()
	defer func() {
		close(dispatcher.done)

		s.connMu.Lock()
		if s.dispatchers[dispatcher.generation] == dispatcher {
			delete(s.dispatchers, dispatcher.generation)
		}
		s.connMu.Unlock()
	}()

	deliveries := dispatcher.client.Deliveries()

	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				s.finishGatewayGeneration(dispatcher, gatewayDisconnectCause(dispatcher.client))

				return
			}

			if delivery.Err != nil {
				s.finishGatewayGeneration(dispatcher, delivery.Err)

				return
			}

			if delivery.Event == nil {
				continue
			}

			if err := s.dispatchGatewayOrderedEvent(dispatcher, *delivery.Event); err != nil {
				s.finishGatewayGeneration(dispatcher, err)

				return
			}
		case barrier := <-dispatcher.barriers:
			if dispatcher.beforeBarrierDrain != nil {
				dispatcher.beforeBarrierDrain()
			}

			err := s.drainGatewayThroughWatermark(dispatcher, deliveries, barrier.watermark)
			barrier.reply <- err

			if err != nil {
				s.finishGatewayGeneration(dispatcher, err)

				return
			}
		case command := <-dispatcher.handshakes:
			s.handleGatewayHandshake(dispatcher, deliveries, command)
		case <-s.closed:
			return
		}
	}
}

func (s *hermesServer) drainGatewayThroughWatermark(
	dispatcher *gatewayTransportDispatcher,
	deliveries <-chan GatewayDelivery,
	watermark uint64,
) error {
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return gatewayDisconnectCause(dispatcher.client)
			}

			if delivery.Err != nil {
				return delivery.Err
			}

			if delivery.Event == nil {
				continue
			}

			event := *delivery.Event
			if err := s.dispatchGatewayOrderedEvent(dispatcher, event); err != nil {
				return err
			}

			if event.InboundSequence > watermark {
				return nil
			}
		default:
			return nil
		}
	}
}

func (s *hermesServer) beginGatewayHandshake(
	ctx context.Context,
	transport *gatewayTransport,
	kind gatewayHandshakeKind,
) (uint64, error) {
	if transport == nil || transport.dispatcher == nil {
		return 0, ErrGatewayDisconnected
	}

	s.connMu.Lock()
	s.registrations++
	id := s.registrations
	s.connMu.Unlock()

	reply := make(chan gatewayHandshakeResult, 1)
	command := gatewayHandshakeCommand{action: gatewayHandshakeRecord, id: id, kind: kind, reply: reply}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if transport.dispatcher.beforeHandshakeSelect != nil {
		transport.dispatcher.beforeHandshakeSelect()
	}

	select {
	case transport.dispatcher.handshakes <- command:
	case <-transport.dispatcher.done:
		return 0, gatewayDispatcherCause(transport.dispatcher)
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	result := <-reply
	if result.state != gatewayHandshakeRecorded && result.err == nil {
		result.err = ErrGatewayAmbiguousTurn
	}

	return id, result.err
}

func (s *hermesServer) cancelGatewayHandshake(transport *gatewayTransport, id uint64) gatewayHandshakeResult {
	if transport == nil || transport.dispatcher == nil || id == 0 {
		return gatewayHandshakeResult{state: gatewayHandshakeTombstoned, err: ErrGatewayDisconnected}
	}

	reply := make(chan gatewayHandshakeResult, 1)
	command := gatewayHandshakeCommand{action: gatewayHandshakeTombstone, id: id, reply: reply}

	select {
	case transport.dispatcher.handshakes <- command:
	case <-transport.dispatcher.done:
		return gatewayHandshakeResult{state: gatewayHandshakeTombstoned, err: gatewayDispatcherCause(transport.dispatcher)}
	}

	return <-reply
}

func (s *hermesServer) bindGatewayHandshake(
	ctx context.Context,
	transport *gatewayTransport,
	id uint64,
	kind gatewayHandshakeKind,
	stored string,
	live string,
	watermark GatewayWatermark,
) gatewayHandshakeResult {
	if transport == nil || transport.dispatcher == nil {
		return gatewayHandshakeResult{err: ErrGatewayDisconnected}
	}

	reply := make(chan gatewayHandshakeResult, 1)

	command := gatewayHandshakeCommand{
		action: gatewayHandshakeBind,
		id:     id, kind: kind, stored: stored, live: live, watermark: watermark, reply: reply,
	}
	if err := ctx.Err(); err != nil {
		result := s.cancelGatewayHandshake(transport, id)
		result.err = errors.Join(err, result.err)

		return result
	}

	if transport.dispatcher.beforeHandshakeSelect != nil {
		transport.dispatcher.beforeHandshakeSelect()
	}

	select {
	case transport.dispatcher.handshakes <- command:
	case <-transport.dispatcher.done:
		return gatewayHandshakeResult{err: gatewayDispatcherCause(transport.dispatcher)}
	case <-ctx.Done():
		if transport.dispatcher.beforeHandshakeCancel != nil {
			transport.dispatcher.beforeHandshakeCancel()
		}

		result := s.cancelGatewayHandshake(transport, id)
		result.err = errors.Join(ctx.Err(), result.err)

		return result
	}

	// Dispatch admission is the ownership boundary. Once accepted, wait for the
	// state machine's acknowledgement so caller cancellation cannot turn a
	// committed binding into a reported failure.
	return <-reply
}

func (s *hermesServer) handleGatewayHandshake(
	dispatcher *gatewayTransportDispatcher,
	deliveries <-chan GatewayDelivery,
	command gatewayHandshakeCommand,
) {
	if command.action == gatewayHandshakeRecord {
		if command.id == 0 || command.kind == 0 {
			command.reply <- gatewayHandshakeResult{err: ErrGatewayAmbiguousTurn}

			return
		}

		dispatcher.pendingHandshakes[command.id] = command.kind
		command.reply <- gatewayHandshakeResult{state: gatewayHandshakeRecorded}

		return
	}

	if command.action == gatewayHandshakeTombstone {
		delete(dispatcher.pendingHandshakes, command.id)

		command.reply <- gatewayHandshakeResult{state: gatewayHandshakeTombstoned}

		dropUnmatchedGatewayFrames(dispatcher)

		return
	}

	kind, ok := dispatcher.pendingHandshakes[command.id]
	if !ok || kind != command.kind || command.stored == "" || command.live == "" ||
		command.watermark.TransportGeneration != dispatcher.generation {
		command.reply <- gatewayHandshakeResult{err: fmt.Errorf("%w: invalid gateway create/resume binding", ErrGatewayAmbiguousTurn)}

		return
	}

	if err := s.drainGatewayThroughWatermark(dispatcher, deliveries, command.watermark.Sequence); err != nil {
		delete(dispatcher.pendingHandshakes, command.id)

		command.reply <- gatewayHandshakeResult{state: gatewayHandshakeTombstoned, err: err}

		return
	}

	actor := s.actorForTransportSession(dispatcher.transport, command.stored, command.live)
	if actor == nil {
		delete(dispatcher.pendingHandshakes, command.id)

		command.reply <- gatewayHandshakeResult{state: gatewayHandshakeTombstoned, err: ErrGatewayDisconnected}

		return
	}

	buffered := dispatcher.pendingFrames[command.live]
	delete(dispatcher.pendingFrames, command.live)
	dispatcher.pendingFrameCount -= len(buffered)
	delete(dispatcher.pendingHandshakes, command.id)

	for index := range buffered {
		if command.kind == gatewayHandshakeResume && buffered[index].InboundSequence <= command.watermark.Sequence {
			continue
		}

		copyEvent := buffered[index]
		if err := s.enqueueGatewayEvent(actor, copyEvent, dispatcher.generation, true); err != nil {
			cause := actor.enqueueCause()
			s.failGatewayGeneration(dispatcher.generation, cause)

			command.reply <- gatewayHandshakeResult{state: gatewayHandshakeTombstoned, err: cause}

			return
		}
	}

	command.reply <- gatewayHandshakeResult{state: gatewayHandshakeCommitted}

	dropUnmatchedGatewayFrames(dispatcher)
}

// dropUnmatchedGatewayFrames releases the frames retained for a handshake that
// can no longer claim them. With no handshake left in flight, a frame naming a
// native session this adapter never bound names another session's work — a
// mirrored child run, a scheduled turn — and this adapter has nothing to say
// about it.
func dropUnmatchedGatewayFrames(dispatcher *gatewayTransportDispatcher) {
	if len(dispatcher.pendingHandshakes) != 0 || dispatcher.pendingFrameCount == 0 {
		return
	}

	clear(dispatcher.pendingFrames)

	dispatcher.pendingFrameCount = 0
}

func (s *hermesServer) synchronizeGatewayWatermark(
	ctx context.Context,
	transport *gatewayTransport,
	watermark uint64,
) error {
	if transport == nil || transport.dispatcher == nil {
		return ErrGatewayDisconnected
	}

	dispatcher := transport.dispatcher

	reply := make(chan error, 1)

	barrier := gatewayDispatchBarrier{watermark: watermark, reply: reply}
	select {
	case dispatcher.barriers <- barrier:
	case <-dispatcher.done:
		return gatewayDispatcherCause(dispatcher)
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-reply:
		return err
	case <-dispatcher.done:
		return gatewayDispatcherCause(dispatcher)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func gatewayDispatcherCause(dispatcher *gatewayTransportDispatcher) error {
	if cause := dispatcher.terminalCause(); cause != nil {
		return cause
	}

	return gatewayTransportCause(errGatewayStreamClosed)
}

func gatewayTransportCause(cause error) error {
	if cause == nil {
		cause = errGatewayStreamClosed
	}

	if errors.Is(cause, ErrGatewayDisconnected) {
		return cause
	}

	return fmt.Errorf("%w: %w", ErrGatewayDisconnected, cause)
}

func gatewayTransportFailure(cause error) *TurnFailureError {
	if cause == nil {
		cause = errGatewayStreamClosed
	}

	return &TurnFailureError{
		cause: CauseTransport, message: cause.Error(), wrapped: gatewayTransportCause(cause),
	}
}

func gatewaySessionEvent(eventType string) bool {
	switch eventType {
	case evtApprovalRequest, evtClarifyRequest, evtSecretRequest, evtMessageDelta,
		evtMessageComplete, evtError, evtSudoRequest, evtThinkingDelta,
		evtTerminalReadReq, evtToolComplete, evtToolStart:
		return true
	default:
		return false
	}
}

func (s *hermesServer) dispatchGatewayEvent(dispatcher *gatewayTransportDispatcher, event Event) error {
	return s.dispatchGatewayEventWithOrder(dispatcher, event, false)
}

func (s *hermesServer) dispatchGatewayOrderedEvent(dispatcher *gatewayTransportDispatcher, event Event) error {
	return s.dispatchGatewayEventWithOrder(dispatcher, event, true)
}

func (s *hermesServer) dispatchGatewayEventWithOrder(
	dispatcher *gatewayTransportDispatcher,
	event Event,
	ordered bool,
) error {
	// Hermes broadcasts session-less change events to every connected peer and
	// mirrors a watched child run onto a live id this adapter never bound.
	// Neither names a turn this adapter owns, so neither is this adapter's to
	// act on.
	if event.SessionID == "" {
		return nil
	}

	mappings := dispatcher.transport.mappings
	mappings.mu.Lock()

	var actor *gatewaySessionActor

	for _, binding := range mappings.bindings {
		if binding.live == event.SessionID {
			if actor != nil && actor != binding.actor {
				mappings.mu.Unlock()

				return ErrGatewayAmbiguousTurn
			}

			actor = binding.actor
		}
	}
	mappings.mu.Unlock()

	if actor == nil {
		// A handshake still in flight has not published its live id, so a frame
		// naming no bound session may yet belong to the session being bound. It
		// is retained only while a handshake can still claim it, and only while
		// that retention stays inside its bounds.
		if len(dispatcher.pendingHandshakes) == 0 {
			return nil
		}

		retained := dispatcher.pendingFrames[event.SessionID]

		if len(retained) >= gatewayPendingFrameLiveCapacity {
			s.observeRetentionOverflow(event, len(retained), "live_session")

			return nil
		}

		if dispatcher.pendingFrameCount >= gatewayPendingFrameCapacity {
			s.observeRetentionOverflow(event, dispatcher.pendingFrameCount, "transport")

			return nil
		}

		dispatcher.pendingFrames[event.SessionID] = append(retained, event)
		dispatcher.pendingFrameCount++

		return nil
	}

	if err := s.enqueueGatewayEvent(actor, event, dispatcher.generation, ordered); err != nil {
		cause := actor.enqueueCause()
		s.failGatewayGeneration(dispatcher.generation, cause)

		return cause
	}

	return nil
}

// observeRetentionOverflow states a frame the handshake retention could not
// hold. The drop is deliberate — a bind waiting for its own frames must not be
// closed by another native session's traffic — but a silent drop leaves the
// session being bound short of frames with nothing to read it from. The frame
// body is never logged; the bound that refused it and the depth are.
func (s *hermesServer) observeRetentionOverflow(event Event, depth int, bound string) {
	if s.log == nil {
		return
	}

	s.log.Debug("hermes gateway handshake retention overflowed",
		slog.String("classification", "handshake_retention_overflow"),
		slog.String("bound", bound),
		slog.String("event_type", event.Type),
		slog.Int("retained", depth),
	)
}

func (s *hermesServer) enqueueGatewayEvent(
	actor *gatewaySessionActor,
	event Event,
	generation uint64,
	ordered bool,
) error {
	copyEvent := event

	message := gatewayActorMessage{event: &copyEvent, generation: generation}
	if ordered {
		message.processed = make(chan struct{})
	}

	if !actor.enqueue(message) {
		return actor.enqueueCause()
	}

	if message.processed == nil {
		return nil
	}

	select {
	case <-message.processed:
		return nil
	case <-actor.done:
		return actor.enqueueCause()
	case <-s.closed:
		return gatewayTransportCause(errGatewayStreamClosed)
	}
}

func (s *hermesServer) finishGatewayGeneration(dispatcher *gatewayTransportDispatcher, cause error) {
	s.failGatewayTransport(dispatcher.transport, cause)
}

func (s *hermesServer) failGatewayGeneration(generation uint64, cause error) {
	s.connMu.Lock()
	dispatcher := s.dispatchers[generation]

	var transport *gatewayTransport
	if dispatcher != nil {
		transport = dispatcher.transport
	}
	s.connMu.Unlock()

	s.failGatewayTransport(transport, cause)
}

func (s *hermesServer) failGatewayTransport(transport *gatewayTransport, cause error) {
	if transport == nil || transport.dispatcher == nil {
		return
	}

	dispatcher := transport.dispatcher
	cause = dispatcher.setCause(cause)

	s.connMu.Lock()
	current := s.transport == transport && s.transport.generation == transport.generation &&
		s.dispatchers[transport.generation] == dispatcher
	s.connMu.Unlock()

	if !current {
		if transport.client != nil {
			s.closeGatewayClient(transport.client, websocket.StatusInternalError, "stale gateway fenced")
		}

		return
	}

	dispatcher.terminalOnce.Do(func() {
		failure := gatewayTransportFailure(cause)

		s.gatewayMu.Lock()

		actors := make([]*gatewaySessionActor, 0, len(s.actorsByStored))
		for _, actor := range s.actorsByStored {
			actors = append(actors, actor)
		}
		s.gatewayMu.Unlock()

		for _, actor := range actors {
			actor.failPending(dispatcher.generation, failure)
		}

		s.publishTurnError(cause)

		if transport.client != nil {
			s.closeGatewayClient(transport.client, websocket.StatusInternalError, "gateway fenced")
		}
	})
}

func (s *hermesServer) closeGatewayClient(client *Client, status websocket.StatusCode, reason string) {
	s.transportCloseWG.Add(1)
	go func() {
		defer s.transportCloseWG.Done()

		_ = client.Close(status, reason)
	}()
}

func (s *hermesServer) actorForTransportSession(
	transport *gatewayTransport,
	stored string,
	live string,
) *gatewaySessionActor {
	if transport == nil || transport.mappings == nil {
		return nil
	}

	s.gatewayMu.Lock()

	if s.actorsByStored == nil {
		s.actorsByStored = make(map[string]*gatewaySessionActor)
	}

	actor := s.actorsByStored[stored]
	if actor != nil && actor.terminalized() {
		actor = nil
	}

	if actor == nil {
		actor = &gatewaySessionActor{
			server:      s,
			stored:      stored,
			mailbox:     make(chan gatewayActorMessage, gatewayActorMailboxCapacity+1),
			terminal:    make(chan error, 1),
			done:        make(chan struct{}),
			projections: make(map[*gatewayProjection]struct{}),
		}
		s.actorsByStored[stored] = actor
		s.actorWG.Add(1)

		go actor.run()
	}

	s.gatewayMu.Unlock()

	bound := make(chan struct{})
	if !actor.enqueue(gatewayActorMessage{bind: &gatewayActorBinding{
		live: live, generation: transport.generation, transport: transport, done: bound,
	}}) {
		s.failGatewayTransport(transport, actor.enqueueCause())

		return nil
	}

	select {
	case <-bound:
	case <-actor.done:
		return nil
	case <-s.closed:
		return nil
	}

	transport.mappings.mu.Lock()

	transport.mappings.bindings[stored] = gatewaySessionBinding{live: live, actor: actor}
	transport.mappings.mu.Unlock()

	return actor
}

// terminalized reports whether this actor can no longer accept ownership. A
// reconnect may retain the logical stored-session key, but it must never enqueue
// a generation-2 bind onto the terminal mailbox generation 1 left behind.
func (a *gatewaySessionActor) terminalized() bool {
	if a == nil {
		return true
	}

	a.mailboxMu.Lock()
	terminal := len(a.terminal) != 0
	a.mailboxMu.Unlock()

	if terminal {
		return true
	}

	select {
	case <-a.done:
		return true
	default:
		return false
	}
}

func (a *gatewaySessionActor) run() {
	defer a.server.actorWG.Done()
	defer close(a.done)

	for {
		message := <-a.mailbox

		switch {
		case message.terminal:
			a.failCycles(message.failure)

			return
		case message.stop:
			a.failCycles(gatewayTransportFailure(errGatewayStreamClosed))

			return
		case message.bind != nil:
			if message.bind.generation != a.generation {
				a.fencedPrompt = nil
			}

			a.live = message.bind.live
			a.generation = message.bind.generation
			a.transport = message.bind.transport
			close(message.bind.done)
		case message.end != nil:
			active := message.generation == a.generation && (a.active != nil || a.prompt != nil)
			if active {
				failure := message.failure
				if failure == nil {
					failure = &TurnFailureError{cause: CauseTransport, message: errGatewayStreamClosed.Error(), wrapped: ErrGatewayDisconnected}
				}

				a.failCycles(failure)
			}

			message.end <- active
		case message.failure != nil:
			a.failCycles(message.failure)
		case message.register != nil:
			if a.beforeRegister != nil {
				a.beforeRegister()
			}

			a.registerPrompt(message.register)
		case message.watermark != nil:
			if a.beforeWatermark != nil {
				a.beforeWatermark()
			}

			message.watermark.reply <- a.applyPromptWatermark(message.watermark)
		case message.release != nil:
			if a.beforeRelease != nil {
				a.beforeRelease()
			}

			message.release.reply <- a.releasePrompt(message.release)
		case message.projected != nil:
			delete(a.projections, message.projected)
		case message.control != nil:
			a.completeControl(message.control)
		case message.cancel != nil:
			a.cancelPrompt(message.cancel)
		case message.event != nil:
			a.handleRaw(message.generation, *message.event)

			if message.processed != nil {
				close(message.processed)
			}
		}

		a.queueLatchedTerminal()
	}
}

func (a *gatewaySessionActor) cancelPrompt(command *gatewayPromptCancel) {
	if a.beforeCancel != nil {
		a.beforeCancel()
	}

	if a.prompt != nil && a.prompt.registrationID == command.registrationID {
		cycle := a.prompt
		if command.accepted && command.generation == a.generation {
			a.fencedPrompt = &gatewayPromptTombstone{
				generation: command.generation,
				watermark:  command.watermark,
			}
		}

		a.prompt = nil
		a.buffered = nil

		if cycle.result != nil {
			cycle.result <- gatewayCycleResult{err: command.err}

			close(cycle.result)
		}
	}

	close(command.done)
}

func (a *gatewaySessionActor) registerPrompt(registration *gatewayPromptRegistration) {
	if cancelled, ok := registration.cancellation(); ok {
		registration.reply <- gatewayPromptHandle{err: cancelled}

		return
	}

	if a.active != nil {
		registration.reply <- gatewayPromptHandle{err: ErrGatewayAgentBusy}

		return
	}

	if a.prompt != nil || a.fencedPrompt != nil {
		registration.reply <- gatewayPromptHandle{err: fmt.Errorf("%w: more than one prompt is registered", ErrGatewayAmbiguousTurn)}

		return
	}

	cycle := a.newCycle(CycleOriginPrompt)
	cycle.registrationID = registration.id
	cycle.result = registration.result

	a.prompt = cycle
	registration.reply <- gatewayPromptHandle{cycleID: cycle.id, result: cycle.result}
}

func (a *gatewaySessionActor) newCycle(origin CycleOrigin) *gatewayCycle {
	a.cycles++
	id := fmt.Sprintf("hermes/%s/cycle-%d", a.stored, a.cycles)

	return &gatewayCycle{
		id:          id,
		messageID:   id + "/message",
		origin:      origin,
		activeTools: make(map[string]struct{}),
		toolStates:  make(map[string]gatewayActiveTool),
		controls:    make(map[gatewayControlIdentity]struct{}),
	}
}

func (a *gatewaySessionActor) applyPromptWatermark(command *gatewayPromptWatermark) gatewayPromptWatermarkResult {
	if a.prompt == nil || a.prompt.id != command.cycleID {
		return gatewayPromptWatermarkResult{err: fmt.Errorf(
			"%w: prompt cycle changed before submit acknowledgement",
			ErrGatewayAmbiguousTurn,
		)}
	}

	a.prompt.watermark = command.watermark
	a.prompt.watermarkSet = true
	a.prompt.held = true

	buffered := a.buffered

	a.buffered = nil
	for index := range buffered {
		if buffered[index].InboundSequence > a.prompt.watermark {
			a.prompt.heldEvents = append(a.prompt.heldEvents, buffered[index])
			if len(a.prompt.heldEvents) > gatewayActorMailboxCapacity {
				a.failClosed(ErrGatewayActorOverflow)

				return gatewayPromptWatermarkResult{err: ErrGatewayActorOverflow}
			}

			continue
		}

		if err := a.routeRaw(buffered[index]); err != nil {
			a.failClosed(err)

			return gatewayPromptWatermarkResult{err: err}
		}
	}

	switch command.disposition {
	case gatewayPromptQueuedTurn:
		return a.deferPromptToQueuedTurn()
	case gatewayPromptJoinedTurn:
		if a.active != nil {
			return gatewayPromptWatermarkResult{err: a.releasePromptToLiveCycle()}
		}
	case gatewayPromptOwnsTurn:
		if a.active != nil {
			if err := a.retireAbandonedCycle(); err != nil {
				a.failClosed(err)

				return gatewayPromptWatermarkResult{err: err}
			}
		}
	}

	return gatewayPromptWatermarkResult{projections: a.pendingProjections()}
}

func (a *gatewaySessionActor) pendingProjections() []*gatewayProjection {
	projections := make([]*gatewayProjection, 0, len(a.projections))
	for projection := range a.projections {
		projections = append(projections, projection)
	}

	return projections
}

// deferPromptToQueuedTurn adopts hermes's decision to run this prompt as the
// next turn. Hermes queued the text because a turn is already running, so the
// registered cycle claims nothing while that turn streams: every frame it was
// holding, and every frame until that turn's native terminal, is agent-origin
// work. The prompt is neither lost nor resubmitted — it is the turn hermes runs
// when it drains the queue.
func (a *gatewaySessionActor) deferPromptToQueuedTurn() gatewayPromptWatermarkResult {
	cycle := a.prompt
	cycle.deferred = true
	cycle.deferral = make(chan gatewayPromptWatermarkResult, 1)
	deferral := cycle.deferral

	held := cycle.heldEvents
	cycle.heldEvents = nil

	for index := range held {
		// The running turn's own terminal can be in this batch. Everything after
		// it belongs to the turn hermes drains the queue to run, so it goes back
		// to being held until the prompt is released.
		if !cycle.deferred {
			cycle.heldEvents = append(cycle.heldEvents, held[index:]...)

			break
		}

		if err := a.routeRaw(held[index]); err != nil {
			a.failClosed(err)

			return gatewayPromptWatermarkResult{err: err}
		}
	}

	return gatewayPromptWatermarkResult{deferral: deferral}
}

// resumeDeferredPrompt hands a queued prompt the turn boundary it was waiting
// for. The agent-origin cycle that owned the stream has stated its native
// terminal, so the frames after it are the turn hermes drained the queue to run.
func (a *gatewaySessionActor) resumeDeferredPrompt() {
	if a.prompt == nil || a.prompt.deferral == nil {
		return
	}

	deferral := a.prompt.deferral
	a.prompt.deferral = nil
	a.prompt.deferred = false

	deferral <- gatewayPromptWatermarkResult{projections: a.pendingProjections()}
}

// releasePromptToLiveCycle reports a prompt hermes folded into the turn already
// running. That turn owns every frame the prompt was holding and every frame
// that follows, including the answer to the text just folded in: the prompt has
// no turn of its own to register, and resubmitting it would fold the same text
// in twice.
func (a *gatewaySessionActor) releasePromptToLiveCycle() error {
	held := a.prompt.heldEvents

	a.prompt = nil
	a.buffered = nil

	for index := range held {
		if err := a.routeRaw(held[index]); err != nil {
			a.failClosed(err)

			return err
		}
	}

	return ErrGatewayTurnAbsorbedPrompt
}

// retireAbandonedCycle closes out an agent-origin cycle that outlived the turn
// which opened it. Hermes claims an idle session synchronously and a busy one
// never answers "streaming", so this cycle already holds every frame it will
// ever get: it is stated as a cycle with no native terminal instead of being
// left open to swallow the turn this prompt just started.
func (a *gatewaySessionActor) retireAbandonedCycle() error {
	return a.completeCycle(a.active, NativeMessage{}, errGatewayCycleAbandoned, Event{})
}

func (a *gatewaySessionActor) releasePrompt(command *gatewayPromptRelease) error {
	if a.prompt == nil || a.prompt.id != command.cycleID || !a.prompt.watermarkSet {
		return fmt.Errorf("%w: prompt cycle changed before projection release", ErrGatewayAmbiguousTurn)
	}

	cycle := a.prompt
	cycle.held = false
	held := cycle.heldEvents
	cycle.heldEvents = nil

	for index := range held {
		if err := a.routeRaw(held[index]); err != nil {
			a.failClosed(err)

			return err
		}
	}

	return nil
}

func (a *gatewaySessionActor) handleRaw(generation uint64, event Event) {
	if generation != a.generation {
		return
	}

	if a.fencedPrompt != nil && a.fencedPrompt.generation == generation && gatewaySessionEvent(event.Type) {
		if event.InboundSequence > a.fencedPrompt.watermark &&
			(event.Type == evtMessageComplete || event.Type == evtError) {
			a.fencedPrompt = nil
		}

		return
	}

	if !gatewaySessionEvent(event.Type) {
		if err := a.emitMapped(TurnEvent{
			Type:                EventGatewayRaw,
			Raw:                 event.Raw,
			TransportGeneration: a.generation,
		}); err != nil {
			a.failClosed(err)
		}

		return
	}

	if a.prompt != nil && !a.prompt.watermarkSet {
		a.buffered = append(a.buffered, event)
		if len(a.buffered) > gatewayActorMailboxCapacity {
			a.failClosed(ErrGatewayActorOverflow)
		}

		return
	}

	if a.prompt != nil && a.prompt.held && !a.prompt.deferred && event.InboundSequence > a.prompt.watermark {
		a.prompt.heldEvents = append(a.prompt.heldEvents, event)
		if len(a.prompt.heldEvents) > gatewayActorMailboxCapacity {
			a.failClosed(ErrGatewayActorOverflow)
		}

		return
	}

	if err := a.routeRaw(event); err != nil {
		a.failClosed(err)
	}
}

func (a *gatewaySessionActor) routeRaw(event Event) error {
	if a.prompt != nil && !a.prompt.deferred && event.InboundSequence > a.prompt.watermark {
		return a.applyEvent(a.prompt, event)
	}

	if a.active == nil {
		a.active = a.newCycle(CycleOriginActivity)
		if err := a.emitMapped(TurnEvent{
			Type:                EventCycleStarted,
			TransportGeneration: a.generation,
			CycleID:             a.active.id,
			Origin:              CycleOriginActivity,
		}); err != nil {
			return err
		}
	}

	return a.applyEvent(a.active, event)
}

func (a *gatewaySessionActor) applyEvent(cycle *gatewayCycle, event Event) error {
	cycle.started = true

	switch event.Type {
	case evtApprovalRequest:
		return a.mapPermission(cycle, event)
	case evtClarifyRequest:
		return a.mapQuestion(cycle, event)
	case evtTerminalReadReq, evtSudoRequest, evtSecretRequest:
		a.server.declineGatewayQuestion(context.Background(), a.generation, a.live, event.Type)

		return nil
	case evtError:
		return a.completeCycle(cycle, NativeMessage{}, gatewayEventFailure(event.Payload), event)
	case evtToolStart:
		if err := cycle.chargeTool(event); err != nil {
			return err
		}

		part, ok := gatewayToolPart(a.stored, cycle.messageID, event, gatewayActiveTool{})
		if !ok {
			return fmt.Errorf("%w: tool start has no native tool identity", ErrGatewayAmbiguousTurn)
		}

		cycle.activeTools[part.CallID] = struct{}{}
		cycle.toolStates[part.CallID] = gatewayActiveTool{
			rawInput: append(json.RawMessage(nil), event.Payload...),
			name:     part.Tool,
		}
		cycle.toolParts = append(cycle.toolParts, part)

		return a.emitPart(cycle, part, event)
	case evtToolComplete:
		if err := cycle.chargeTool(event); err != nil {
			return err
		}

		toolCallID := gatewayToolCallID(event.Payload)

		part, ok := gatewayToolPart(a.stored, cycle.messageID, event, cycle.toolStates[toolCallID])
		if !ok {
			return fmt.Errorf("%w: tool completion has no native tool identity", ErrGatewayAmbiguousTurn)
		}

		delete(cycle.activeTools, part.CallID)
		delete(cycle.toolStates, part.CallID)
		cycle.toolParts = append(cycle.toolParts, part)

		return a.emitPart(cycle, part, event)
	case evtMessageDelta, evtThinkingDelta:
		chunk := gatewayPayloadString(event.Payload, valText)
		if chunk == "" {
			return nil
		}

		if err := cycle.chargeText(chunk); err != nil {
			return err
		}

		if event.Type == evtMessageDelta {
			cycle.text.WriteString(chunk)
		}

		partType := valText
		if event.Type == evtThinkingDelta {
			partType = valReasoning
		}

		part := Part{
			ID:        cycle.messageID + "-" + partType,
			SessionID: a.stored,
			MessageID: cycle.messageID,
			Type:      partType,
			Text:      chunk,
			Raw:       event.Raw,
		}

		return a.emitPart(cycle, part, event)
	case evtMessageComplete:
		if failure := gatewayCompleteFailure(event.Payload); failure != nil {
			return a.completeCycle(cycle, NativeMessage{}, failure, event)
		}

		message, err := a.messageFor(cycle, event)
		if err != nil {
			return err
		}

		return a.completeCycle(cycle, message, nil, event)
	default:
		return nil
	}
}

func (c *gatewayCycle) chargeText(text string) error {
	if len(text) > gatewayCycleTextByteLimit-c.textBytes {
		return ErrGatewayCycleOverflow
	}

	c.textBytes += len(text)

	return nil
}

func (c *gatewayCycle) chargeTool(event Event) error {
	if c.toolCount >= gatewayCycleToolCountLimit ||
		len(event.Payload) > gatewayCycleToolDataByteLimit-c.toolDataBytes {
		return ErrGatewayCycleOverflow
	}

	c.toolCount++
	c.toolDataBytes += len(event.Payload)

	return nil
}

func (c *gatewayCycle) registerControl(event Event, requestID string, kind gatewayControlKind) error {
	identity := gatewayControlIdentity{kind: kind, requestID: requestID}
	if _, exists := c.controls[identity]; exists {
		return fmt.Errorf("%w: duplicate native control identity", ErrGatewayAmbiguousTurn)
	}

	if c.controlCount >= gatewayCycleControlCountLimit || len(c.controls) >= gatewayCycleControlMapLimit ||
		len(event.Payload) > gatewayCycleControlDataByteLimit-c.controlDataBytes {
		return ErrGatewayCycleOverflow
	}

	c.controlCount++
	c.controlDataBytes += len(event.Payload)
	c.controls[identity] = struct{}{}

	return nil
}

func (a *gatewaySessionActor) controlRoute(
	cycle *gatewayCycle,
	requestID string,
	kind gatewayControlKind,
) *gatewayControlRoute {
	transport := a.transport

	var mappings *gatewaySessionMappings
	if transport != nil {
		mappings = transport.mappings
	}

	return &gatewayControlRoute{
		transport: transport, mappings: mappings, actor: a, generation: a.generation,
		stored: a.stored, live: a.live, cycleID: cycle.id, requestID: requestID, kind: kind,
	}
}

func (s *hermesServer) completeGatewayControl(
	ctx context.Context,
	route *gatewayControlRoute,
	kind gatewayControlKind,
	choice string,
	answers any,
) error {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if route == nil || transport == nil || route.transport != transport || route.actor == nil ||
		route.kind != kind || route.generation == 0 || route.requestID == "" {
		return fmt.Errorf("%w: native control route is stale or incomplete", ErrGatewayAmbiguousTurn)
	}

	completed := make(chan error, 1)

	command := &gatewayControlCommand{
		ctx: ctx, route: route, kind: kind, choice: choice, answers: answers, completed: completed,
	}
	if !route.actor.enqueue(gatewayActorMessage{control: command}) {
		return route.actor.enqueueCause()
	}

	select {
	case err := <-completed:
		return err
	case <-route.actor.done:
		return gatewayDispatcherCause(transport.dispatcher)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *gatewaySessionActor) completeControl(command *gatewayControlCommand) {
	route := command.route
	if route == nil || command.kind != route.kind || route.actor != a || route.transport == nil ||
		route.transport != a.transport || route.mappings == nil || route.mappings != route.transport.mappings ||
		route.generation != a.generation || route.stored != a.stored || route.live != a.live {
		command.completed <- fmt.Errorf("%w: native control route changed before completion", ErrGatewayAmbiguousTurn)

		return
	}

	cycle := a.prompt
	if cycle == nil || cycle.id != route.cycleID {
		cycle = a.active
	}

	identity := gatewayControlIdentity{kind: route.kind, requestID: route.requestID}
	if cycle == nil || cycle.id != route.cycleID {
		command.completed <- fmt.Errorf("%w: native control owner changed before completion", ErrGatewayAmbiguousTurn)

		return
	}

	if _, exists := cycle.controls[identity]; !exists {
		command.completed <- fmt.Errorf("%w: native control owner changed before completion", ErrGatewayAmbiguousTurn)

		return
	}

	a.server.connMu.Lock()
	currentTransport := a.server.transport == route.transport &&
		a.server.dispatchers[route.generation] == route.transport.dispatcher
	a.server.connMu.Unlock()

	if !currentTransport {
		command.completed <- fmt.Errorf("%w: native control transport changed before completion", ErrGatewayAmbiguousTurn)

		return
	}

	route.mappings.mu.Lock()
	binding := route.mappings.bindings[route.stored]
	routeCurrent := binding.actor == a && binding.live == route.live
	route.mappings.mu.Unlock()

	if !routeCurrent {
		command.completed <- fmt.Errorf("%w: native control mapping changed before completion", ErrGatewayAmbiguousTurn)

		return
	}

	var err error

	switch command.kind {
	case gatewayControlPermission:
		// approval.respond carries a session and a choice and nothing else, so
		// the cycle's own control registry above is the whole of what ownership
		// can mean here. Recorded deviation: hermes answers the approval that
		// was appended to the session queue first, which guarantees that exactly
		// one queued approval is answered and does not guarantee it is the one
		// the host was shown — the queue entry is appended under hermes's lock
		// but the notify callback fires outside it, so two approvals raised
		// concurrently can reach this adapter in the opposite order to the
		// queue. This adapter presents one approval at a time and blocks its
		// event pump on the host's answer, which bounds the window to approvals
		// a single native turn raises concurrently but does not close it. An
		// approval the adapter could not attribute to a native tool still
		// reaches the user, and the user's answer still reaches hermes.
		err = route.transport.client.ApprovalRespond(command.ctx, route.live, command.choice)
	case gatewayControlQuestion:
		err = route.transport.client.ClarifyRespond(command.ctx, route.live, route.requestID, command.answers)
	default:
		err = ErrGatewayAmbiguousTurn
	}

	if err == nil {
		delete(cycle.controls, identity)
	}

	command.completed <- err
}

func (a *gatewaySessionActor) emitPart(cycle *gatewayCycle, part Part, event Event) error {
	data, err := json.Marshal(part)
	if err != nil {
		return err
	}

	return a.emitMapped(TurnEvent{
		Type:                evtMessagePartUpdated,
		Properties:          data,
		Raw:                 event.Raw,
		TransportGeneration: a.generation,
		CycleID:             cycle.id,
		Origin:              cycle.origin,
	})
}

func (a *gatewaySessionActor) mapPermission(cycle *gatewayCycle, event Event) error {
	// Hermes 0.20 approval.request has no request identity, so the actor mints
	// one inside the exact cycle. It has no tool identity either, and hermes
	// emits tool.start only for a progress-enabled surface and runs tool calls
	// concurrently, so a callback can reach an adapter that sees no active
	// native tool or several. The approval binds to the sole active native tool
	// where exactly one owns the cycle, and is otherwise its own addressable
	// call: an approval the adapter cannot attribute is still an approval the
	// user must answer, and hermes settles it from the session and the choice
	// alone, so a minted identity answers exactly as a tool-bound one does.
	nextPermission := cycle.permissions + 1

	requestID := fmt.Sprintf("%s/permission-%d", cycle.id, nextPermission)
	if err := cycle.registerControl(event, requestID, gatewayControlPermission); err != nil {
		return err
	}

	cycle.permissions = nextPermission

	toolCallID := firstNonEmpty(uniqueGatewayToolCallID(cycle.activeTools), requestID)

	req := PermissionRequest{
		ID:                  requestID,
		SessionID:           a.stored,
		Action:              firstNonEmpty(gatewayPayloadString(event.Payload, "command"), "approval"),
		Metadata:            map[string]any{"liveSessionId": a.live},
		Tool:                permissionTool{MessageID: cycle.messageID, CallID: toolCallID},
		CycleID:             cycle.id,
		TransportGeneration: a.generation,
	}
	req.route = a.controlRoute(cycle, requestID, gatewayControlPermission)

	data, _ := json.Marshal(req)

	copyRequest := req

	return a.emitMapped(TurnEvent{
		Type:                evtApprovalRequest,
		Properties:          data,
		Raw:                 event.Raw,
		TransportGeneration: a.generation,
		CycleID:             cycle.id,
		Origin:              cycle.origin,
		Permission:          &copyRequest,
	})
}

func (a *gatewaySessionActor) mapQuestion(cycle *gatewayCycle, event Event) error {
	nativeID := gatewayPayloadString(event.Payload, "request_id")
	if nativeID == "" {
		return fmt.Errorf("%w: clarify request has no native request id", ErrGatewayAmbiguousTurn)
	}

	if err := cycle.registerControl(event, nativeID, gatewayControlQuestion); err != nil {
		return err
	}

	req := QuestionRequest{
		ID:        nativeID,
		SessionID: a.stored,
		Questions: []QuestionInfo{{
			Question: firstNonEmpty(gatewayPayloadString(event.Payload, keyQuestion), gatewayPayloadString(event.Payload, "prompt"), msgHermesNeedsInput),
			Header:   "Hermes question",
			Custom:   true,
		}},
		Tool:                QuestionTool{MessageID: cycle.messageID, CallID: uniqueGatewayToolCallID(cycle.activeTools)},
		CycleID:             cycle.id,
		TransportGeneration: a.generation,
	}
	req.route = a.controlRoute(cycle, nativeID, gatewayControlQuestion)

	data, _ := json.Marshal(req)

	copyRequest := req

	return a.emitMapped(TurnEvent{
		Type:                evtClarifyRequest,
		Properties:          data,
		Raw:                 event.Raw,
		TransportGeneration: a.generation,
		CycleID:             cycle.id,
		Origin:              cycle.origin,
		Question:            &copyRequest,
	})
}

func (a *gatewaySessionActor) messageFor(cycle *gatewayCycle, event Event) (NativeMessage, error) {
	streamedText := cycle.text.String()

	completeText := gatewayCompleteText(event.Payload)
	if completeText == "" {
		completeText = streamedText
	}

	if completeText != "" && strings.TrimSpace(completeText) == completeText && strings.TrimSpace(streamedText) == completeText {
		completeText = streamedText
	}

	if len(completeText) > gatewayCycleTextByteLimit {
		return NativeMessage{}, ErrGatewayCycleOverflow
	}

	if err := cycle.chargeText(gatewayCompletionSuffix(completeText, streamedText)); err != nil {
		return NativeMessage{}, err
	}

	parts := append([]Part(nil), cycle.toolParts...)
	parts = append(parts, Part{
		ID:           cycle.messageID + "-text",
		SessionID:    a.stored,
		MessageID:    cycle.messageID,
		Type:         valText,
		Text:         completeText,
		StreamedText: streamedText,
	})

	return NativeMessage{
		Info: NativeMessageInfo{
			ID:            cycle.messageID,
			SessionID:     a.stored,
			Role:          valAssistant,
			Finish:        gatewayCompleteFinish(event.Payload),
			Tokens:        gatewayUsageTokens(event.Payload),
			ContextWindow: gatewayContextWindow(event.Payload),
		},
		Parts: parts,
	}, nil
}

// gatewayCompletionSuffix reports the text a completion adds beyond the deltas
// the cycle already carried. Hermes streams interim commentary through the same
// delta stream the final answer arrives on while message.complete carries the
// final answer alone, so a completion is under no obligation to extend the
// concatenated deltas: text the stream already ended with adds nothing, and any
// other completion is charged whole rather than judged a conflict.
func gatewayCompletionSuffix(complete string, streamed string) string {
	if streamed == "" || complete == "" {
		return complete
	}

	if suffix, ok := strings.CutPrefix(complete, streamed); ok {
		return suffix
	}

	if strings.HasSuffix(strings.TrimSpace(streamed), strings.TrimSpace(complete)) {
		return ""
	}

	return complete
}

func (a *gatewaySessionActor) completeCycle(cycle *gatewayCycle, message NativeMessage, cycleErr error, event Event) error {
	outcomeErr := cycleErr

	eventType := EventCycleComplete
	if cycleErr != nil {
		eventType = EventCycleFailed
	}

	terminal := TurnEvent{
		Type:                eventType,
		Raw:                 event.Raw,
		TransportGeneration: a.generation,
		CycleID:             cycle.id,
		Origin:              cycle.origin,
		Err:                 cycleErr,
	}
	if cycle.origin == CycleOriginActivity {
		if len(a.projections) >= gatewayProjectionLimit {
			return ErrGatewayCycleOverflow
		}

		projection := &gatewayProjection{done: make(chan error, 1)}
		a.projections[projection] = struct{}{}
		terminal.ProjectionDone = func(err error) { projection.resolve(a, err) }
	}

	if cycleErr == nil {
		copyMessage := message
		terminal.Message = &copyMessage
	}

	publicationErr := a.emitMapped(terminal)
	if publicationErr != nil {
		outcomeErr = errors.Join(outcomeErr, publicationErr)
	}

	if cycle.result != nil {
		cycle.result <- gatewayCycleResult{message: message, err: outcomeErr}

		close(cycle.result)
	}

	if a.prompt == cycle {
		a.prompt = nil
	}

	if a.active == cycle {
		a.active = nil

		a.resumeDeferredPrompt()
	}

	// A provider-declared failure is a normal cycle outcome. Only failure to
	// publish that outcome corrupts the permanent dispatcher.
	return publicationErr
}

func (a *gatewaySessionActor) emitMapped(event TurnEvent) error {
	return a.server.publishTurnEvent(event)
}

func (a *gatewaySessionActor) failClosed(err error) {
	a.failCycles(err)
	a.server.failGatewayGeneration(a.generation, err)
}

func (a *gatewaySessionActor) enqueue(message gatewayActorMessage) bool {
	a.mailboxMu.Lock()
	defer a.mailboxMu.Unlock()

	if len(a.terminal) != 0 || len(a.mailbox) >= gatewayActorMailboxCapacity {
		return false
	}

	select {
	case a.mailbox <- message:
		return true
	default:
		return false
	}
}

func (a *gatewaySessionActor) enqueueCause() error {
	a.mailboxMu.Lock()
	defer a.mailboxMu.Unlock()

	if len(a.terminal) != 0 {
		err := <-a.terminal
		a.terminal <- err

		return err
	}

	if len(a.mailbox) >= gatewayActorMailboxCapacity {
		return ErrGatewayActorOverflow
	}

	select {
	case <-a.done:
		return gatewayTransportCause(errGatewayStreamClosed)
	default:
		return ErrGatewayActorOverflow
	}
}

func (a *gatewaySessionActor) failPending(generation uint64, err error) {
	a.mailboxMu.Lock()
	defer a.mailboxMu.Unlock()

	if len(a.terminal) != 0 {
		return
	}

	a.terminal <- err

	select {
	case a.mailbox <- gatewayActorMessage{generation: generation, failure: err, terminal: true}:
		a.terminalQueued = true
	default:
	}
}

func (a *gatewaySessionActor) queueLatchedTerminal() {
	a.mailboxMu.Lock()
	defer a.mailboxMu.Unlock()

	if a.terminalQueued || len(a.terminal) == 0 {
		return
	}

	err := <-a.terminal
	a.terminal <- err

	select {
	case a.mailbox <- gatewayActorMessage{generation: a.generation, failure: err, terminal: true}:
		a.terminalQueued = true
	default:
	}
}

func (a *gatewaySessionActor) failCycles(err error) {
	if a.prompt != nil && a.prompt.deferral != nil {
		a.prompt.deferral <- gatewayPromptWatermarkResult{err: err}

		a.prompt.deferral = nil
	}

	seen := make(map[*gatewayCycle]struct{}, 2)

	for _, cycle := range []*gatewayCycle{a.active, a.prompt} {
		if cycle == nil {
			continue
		}

		if _, ok := seen[cycle]; ok {
			continue
		}

		seen[cycle] = struct{}{}

		_ = a.emitMapped(TurnEvent{
			Type:                EventCycleFailed,
			TransportGeneration: a.generation,
			CycleID:             cycle.id,
			Origin:              cycle.origin,
			Err:                 err,
		})
		if cycle.result != nil {
			cycle.result <- gatewayCycleResult{err: err}

			close(cycle.result)
		}
	}

	a.active = nil
	a.prompt = nil
	a.fencedPrompt = nil

	a.buffered = nil

	for projection := range a.projections {
		projection.resolveOnActorExit(err)
		delete(a.projections, projection)
	}
}

func (s *hermesServer) stopGatewayActors() {
	s.gatewayMu.Lock()

	actors := make([]*gatewaySessionActor, 0, len(s.actorsByStored))
	for _, actor := range s.actorsByStored {
		actors = append(actors, actor)
	}
	s.gatewayMu.Unlock()

	for _, actor := range actors {
		actor.failPending(actor.generation, gatewayTransportFailure(errGatewayStreamClosed))
	}
}
