package hermesacp

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
)

// One incarnation names its cycles, turns, and actions from its own identity, so
// a reader can see at a glance which stream an entity belongs to.
const (
	lifecycleOpenCycleSuffix = "/open"
	lifecycleCyclePrefix     = "/cycle-"
	lifecycleTurnPrefix      = "/turn-"
	lifecycleActionPrefix    = "/action-"
	// lifecycleBarrierPrefix labels the process-containment proof identifier with
	// the contained native root it covers.
	lifecycleBarrierPrefix = "hermes-process-tree/"
)

// sessionStream is one `hermes serve` generation's lifecycle incarnation. The
// native process owns the session for its whole lifetime and survives ordinary
// prompts, so one stream spans every prompt that generation serves and ends only
// where the generation does: a cancel or failure fence, incarnation loss, or
// session close.
//
// A nil sessionStream is the connection where the host offered nothing. Every
// method is a no-op on it, so no caller carries a conditional of its own.
type sessionStream struct {
	mu         sync.Mutex
	stream     *lifecycle.Stream
	negotiated lifecycle.Negotiated
	session    *session
	// opened records that the whole-state assertion was reduced and delivered.
	// A missing connection leaves it unopened and consumes no sequence, so every
	// later projection remains gated behind the snapshot the host must see first.
	opened bool
	// openCycleID is the idle cycle the snapshot reports; cycleID is the one an
	// accepted submission runs in. They are distinct because a snapshot's
	// foreground state predates every turn.
	openCycleID string
	// releaseProjection is false while a replacement incarnation is staged.
	// Its opening snapshot may be prepared, but native event projection remains
	// gated until the complete replacement tuple is published.
	releaseProjection bool
	cycleID           string
	turnID            string
	turns             uint64
	actions           uint64
	turnCause         lifecycle.Cause
	// blockers counts the announced actions still blocking the current cycle. The
	// cycle returns to running when the last one resolves, never the first.
	blockers int
}

// lifecycleStreamID mints one incarnation identity. It names the native
// generation, so it is minted exactly where a generation is.
func lifecycleStreamID() (string, error) {
	return newSessionID()
}

// openLifecycleStream installs the incarnation for the session's current native
// generation. It emits nothing: a stream's first event is ordered after the
// establishing response, so the snapshot is delivered by ensureLifecycleOpened.
func (s *session) openLifecycleStream() error {
	stream, err := s.prepareLifecycleStream(true)
	if err != nil {
		return err
	}

	s.streamMu.Lock()
	s.stream = stream
	s.streamMu.Unlock()

	return nil
}

func (s *session) prepareLifecycleStream(releaseProjection bool) (*sessionStream, error) {
	negotiated := s.agent.negotiatedLifecycle()
	if !negotiated.Present() {
		return nil, nil //nolint:nilnil // Nil is the intentional unnegotiated stream state.
	}

	id, err := lifecycleStreamID()
	if err != nil {
		return nil, err
	}

	return &sessionStream{
		stream:            lifecycle.NewStream(id, negotiated),
		negotiated:        negotiated,
		session:           s,
		openCycleID:       id + lifecycleOpenCycleSuffix,
		releaseProjection: releaseProjection,
	}, nil
}

// streamID reports the incarnation identity, or the empty string on a connection
// that negotiated nothing.
func (p *sessionStream) streamID() string {
	if p == nil {
		return ""
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.stream.ID()
}

// turnIdentity reports the turn this incarnation currently holds open.
func (p *sessionStream) turnIdentity() string {
	if p == nil {
		return ""
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.turnID
}

func (p *sessionStream) hasOpenTurn() bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.turnID != ""
}

func (s *session) lifecycleStream() *sessionStream {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()

	return s.stream
}

// ensureLifecycleOpened states the incarnation's opening whole-state assertion
// exactly once. Both the ordered post-response release and the next prompt call
// it, and the first one to arrive opens the stream; the other observes an
// already-open stream and emits nothing.
//
// The fact it carries is always negative. This configuration's only proof class
// is whole-tree vacancy, and the generation this stream speaks for is running,
// so no boundary has proved its tree empty and there is nothing to certify.
func (p *sessionStream) ensureLifecycleOpened(ctx context.Context) error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.opened {
		return nil
	}

	if err := p.emitLocked(ctx, lifecycle.SnapshotEvent(p.openCycleID, lifecycle.QuiescenceFact{})); err != nil {
		return err
	}

	p.opened = true
	if p.releaseProjection {
		p.session.releaseProjectionGate()
	}

	return nil
}

func (p *sessionStream) publishProjection() {
	if p == nil {
		return
	}

	p.mu.Lock()
	p.releaseProjection = true
	opened := p.opened
	p.mu.Unlock()

	if opened {
		p.session.releaseProjectionGate()
	}
}

// accept records the dispatch linearization point: the gateway acknowledged the
// submitted frame and owns it, so the turn exists and the foreground cycle the
// submission opened is running. A failure before this point emits no acceptance
// and creates neither submission nor turn.
func (p *sessionStream) accept(ctx context.Context, submission lifecycle.Submission) error {
	if p == nil {
		return nil
	}

	if err := p.ensureLifecycleOpened(ctx); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.turns++
	p.turnID = p.stream.ID() + lifecycleTurnPrefix + strconv.FormatUint(p.turns, 10)
	p.cycleID = p.stream.ID() + lifecyclePrefixForCycle(p.turns)
	p.blockers = 0
	p.turnCause = lifecycle.CauseSubmission

	if err := p.emitLocked(ctx, lifecycle.AcceptedEvent(submission, p.turnID)); err != nil {
		return err
	}

	return p.emitLocked(ctx, lifecycle.RunningEvent(p.cycleID, p.turnID))
}

func (p *sessionStream) startActivity(ctx context.Context) error {
	if p == nil {
		return nil
	}

	if err := p.ensureLifecycleOpened(ctx); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.turnID != "" {
		return acp.NewInternalError(map[string]any{jsonFieldError: "hermes_lifecycle_overlap"})
	}

	p.turns++
	p.turnID = p.stream.ID() + lifecycleTurnPrefix + strconv.FormatUint(p.turns, 10)
	p.cycleID = p.stream.ID() + lifecyclePrefixForCycle(p.turns)
	p.blockers = 0
	p.turnCause = lifecycle.CauseActivity

	return p.emitLocked(ctx, lifecycle.RunningEventWithCause(p.cycleID, p.turnID, p.turnCause))
}

func lifecyclePrefixForCycle(turn uint64) string {
	return lifecycleCyclePrefix + strconv.FormatUint(turn, 10)
}

// reserveAction mints one blocking action's identity and the correlation value
// its request will carry, emitting nothing. Reservation is separate from
// announcement so the held request can be registered against the action id first:
// a host must never see an action id it cannot yet answer.
//
// The second result is false where no accepted turn owns the request, which is
// the only owner this adapter's gateway vocabulary can name.
func (p *sessionStream) reserveAction(kind lifecycle.ActionKind) (lifecycle.ActionUpdate, map[string]any, bool) {
	if p == nil {
		return lifecycle.ActionUpdate{}, nil, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.turnID == "" {
		return lifecycle.ActionUpdate{}, nil, false
	}

	p.actions++
	action := lifecycle.BlockingAction(
		p.stream.ID()+lifecycleActionPrefix+strconv.FormatUint(p.actions, 10), kind, p.turnID,
	)

	return action, lifecycle.ActionCorrelation(p.stream.ID(), action), true
}

// announceAction records the reserved action and the foreground state it blocked.
// The action's own event always comes first: the blocker is the reason the
// foreground may not proceed, so the transition is never derived from it.
func (p *sessionStream) announceAction(ctx context.Context, action lifecycle.ActionUpdate) error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.emitLocked(ctx, lifecycle.ActionEvent(action)); err != nil {
		return err
	}

	p.blockers++

	return p.emitLocked(ctx, lifecycle.RequiresActionEventWithCause(p.cycleID, p.turnID, p.turnCause))
}

// resolveAction terminalizes one announced action and releases the cycle it
// blocked. Every blocker terminalizes before the transition that unblocks its
// cycle, and the cycle returns to running only when the last of them resolves.
func (p *sessionStream) resolveAction(ctx context.Context, actionID string, state lifecycle.ActionState) error {
	if p == nil || actionID == "" {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.resolveActionLocked(ctx, actionID, state)
}

func (p *sessionStream) resolveActionLocked(ctx context.Context, actionID string, state lifecycle.ActionState) error {
	if err := p.emitLocked(ctx, lifecycle.ActionEvent(lifecycle.ResolvedAction(actionID, state))); err != nil {
		return err
	}

	p.blockers--
	if p.blockers > 0 {
		return nil
	}

	return p.emitLocked(ctx, lifecycle.RunningEventWithCause(p.cycleID, p.turnID, p.turnCause))
}

// terminalizeBlockers resolves every action still blocking the cycle as
// cancelled, without returning the foreground to running: the caller is about to
// end the cycle, and a blocker that outlived its turn would leave the terminal
// transition inconsistent with the actions the stream still holds.
func (p *sessionStream) terminalizeBlockers(ctx context.Context) error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, record := range p.stream.State().Actions {
		if record.State.Terminal() {
			continue
		}

		if err := p.emitLocked(ctx, lifecycle.ActionEvent(
			lifecycle.ResolvedAction(record.ActionID, lifecycle.ActionCancelled),
		)); err != nil {
			return err
		}

		p.blockers--
	}

	return nil
}

// settle ends the foreground cycle with the outcome the turn actually reached.
// It runs after the durable foreground-prefix commit, so a terminal boundary
// never claims a prefix the store does not hold.
func (p *sessionStream) settle(ctx context.Context, outcome lifecycleTurnOutcome) error {
	if p == nil || p.turnID == "" {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.emitLocked(ctx, lifecycle.IdleEventWithCause(
		p.cycleID, p.turnID, outcome.stopReason, outcome.outcome, p.turnCause,
	)); err != nil {
		return err
	}

	p.turnID = ""
	p.turnCause = ""

	return nil
}

// certify states the quiescence fact a completed containment boundary produced.
// It runs after the durable resumable-snapshot commit, and only where the
// configuration's proof class actually completed: cancel is not evidence about
// background work, and a boundary that could not enumerate its tree proves
// nothing.
func (p *sessionStream) certify(ctx context.Context, barrier string) error {
	if p == nil || !p.negotiated.AuthoritativeQuiescence {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.emitLocked(ctx, lifecycle.QuiescenceEvent(lifecycle.QuiescenceFact{
		Quiescent: true,
		Source:    p.negotiated.QuiescenceSource,
		Watermark: p.stream.State().ReducedThrough,
		Barrier:   lifecycleBarrierPrefix + barrier,
	}))
}

// fence ends the incarnation. A fenced stream is terminal: the next generation
// opens its own stream with its own sequence space, its own entities, and its
// own opening snapshot.
func (p *sessionStream) fence() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.stream.Fence()
}

// live reports whether there is a stream for a boundary to speak on: one whose
// opening whole-state assertion has been stated and which nothing has fenced
// since. A never-opened incarnation and a fenced one are the same fact to an
// emitter — an event on either is exactly what a conforming reducer refuses —
// and a connection that negotiated nothing has no stream at all.
func (p *sessionStream) live() bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.opened && !p.stream.Fenced()
}

func (p *sessionStream) fenced() bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.stream.Fenced()
}

// emitLocked claims the next sequence, validates the event through the same
// reducer the canonical vectors drive, and delivers it on its own identity-only
// carrier. An event this adapter cannot state truthfully fails here rather than
// reaching a consumer, and a delivery this adapter cannot complete ends the
// incarnation rather than leaving an undetectable gap behind it.
func (p *sessionStream) emitLocked(ctx context.Context, event lifecycle.Event) error {
	conn := p.session.agent.connection()
	if conn == nil {
		return errors.New("ACP client connection is unavailable")
	}

	envelope, err := p.stream.Emit(event)
	if err != nil {
		p.stream.Fence()

		return acp.NewInternalError(map[string]any{
			jsonFieldError: "hermes_lifecycle_violation",
			jsonFieldCause: err.Error(),
		})
	}

	// The envelope rides the notification's own `_meta`, beside sessionId and
	// update, and the carrier sets neither title nor updatedAt: a carrier mutates
	// no state, so it can never be coalesced away with the envelope on it.
	if err := conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: p.session.id,
		Meta:      map[string]any{lifecycle.MetaKey: envelope},
		Update:    acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}},
	}); err != nil {
		p.stream.Fence()

		return err
	}

	return nil
}

// lifecycleTurnOutcome is one settled cycle's truthful boundary: the recorded
// outcome and, where an ACP v1 stop reason names it, that stop reason. A failed
// outcome carries none, because no v1 stop reason names a failure and the v1
// error carries it instead.
type lifecycleTurnOutcome struct {
	stopReason string
	outcome    lifecycle.Outcome
}
