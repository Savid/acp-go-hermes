package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
)

// sessionSettlementTimeout bounds one turn's whole settlement: the containment
// boundary, the durable commit, and the terminal lifecycle emissions. It is the
// store write budget plus the bounded native teardown steps that can precede it,
// so settlement never fails for lack of the time the commit alone was given.
const sessionSettlementTimeout = sessionStoreWriteTimeout + 3*closeTimeout

// promptRun is one turn's native outcome with no settlement applied. Every exit
// of the native loop reports one, so the turn has exactly one settlement point.
type promptRun struct {
	// settle reports that the gateway took durable ownership of the frame, so
	// this exit owes the settlement boundary. A failure before that point creates
	// neither submission nor turn, and its response and error are already final.
	settle   bool
	response acp.PromptResponse
	err      error
	usage    *acp.Usage
	// finish is the structured native finish the gateway reported, or the empty
	// string where the turn reached no native terminal.
	finish string
	// nativeMessageID is the native assistant message the turn was streaming
	// into, which is what the recorded foreground prefix belongs to.
	nativeMessageID string
	// cancelled reports a user cancel, which settles as a cancelled success
	// rather than as a failure.
	cancelled bool
	// endsIncarnation reports that this exit must destroy the native generation,
	// so settlement runs the whole close-fenced order rather than the ordinary
	// between-prompts commit.
	endsIncarnation bool
	markCancelled   bool
}

// turnSettlement is the completion latch close and delete wait on. It is
// released only once the prompt is wholly settled — the containment boundary,
// the durable commit, the terminal idle, and the quiescence fact — so a close
// response can never fence a stream this prompt is still writing to, and can
// never return before the frames its host was shown are durable. The
// settlement's own verdict belongs to the prompt that produced it: close and
// delete wait for the boundary, they do not inherit its error.
type turnSettlement struct {
	done chan struct{}
	once sync.Once
}

func (t *turnSettlement) complete() {
	if t == nil {
		return
	}

	t.once.Do(func() {
		close(t.done)
	})
}

// await blocks until the settlement finishes. A caller whose own context ends
// first reports that instead: waiting is not a licence to hang a request
// forever.
func (t *turnSettlement) await(ctx context.Context) error {
	if t == nil {
		return nil
	}

	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// awaitSettlement waits for any in-flight turn to settle wholly. Close and
// delete call it before touching durable state, so neither races a commit or a
// terminal emission the prompt still owes.
func (s *session) awaitSettlement(ctx context.Context) error {
	s.mu.Lock()
	settlement := s.settlement
	s.mu.Unlock()

	return settlement.await(ctx)
}

// closeLifecycleAdmission stops admitting prompts before close or delete waits
// for the turn in flight. Without it a second prompt could be admitted in the
// window between the wait and the teardown, and the teardown would then race a
// turn it never waited for. A turn still running natively is marked cancelled so
// its settlement records the close's verdict; a turn already capturing its
// terminal commit is left alone — the commit in progress wins and close simply
// waits for it.
func (s *session) closeLifecycleAdmission() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lifecycleClosing = true

	if !s.turnInFlight || (s.turnSettlement != turnSettlementIdle && s.turnSettlement != turnSettlementOpen) {
		return false
	}

	s.turnSettlement = turnSettlementCancelled

	return true
}

// containmentProof is what one completed containment boundary proved about the
// native tree it owned.
type containmentProof struct {
	vacantProven bool
	empty        bool
	barrier      string
}

// vacant reports whether the boundary proved its whole tree empty. An
// enumeration that was unavailable or failed proves nothing, so it never yields a
// positive claim.
func (p containmentProof) vacant() bool { return p.vacantProven && p.empty }

// fenceIncarnation ends the native generation and reports what its containment
// boundary proved. The enumeration is read from the boundary that just
// completed: a proof taken across an incomplete teardown describes a tree the
// supervisor lost, and is no proof at all.
func (s *session) fenceIncarnation(ctx context.Context, turnEpoch uint64, markCancelled bool) (containmentProof, error) {
	s.mu.Lock()
	client := s.client
	root := s.idmap.NativeSessionID
	s.mu.Unlock()

	if err := s.fenceTurn(ctx, turnEpoch, markCancelled); err != nil {
		return containmentProof{}, err
	}

	inventory, ok := client.(providerTreeInventory)
	if !ok {
		return containmentProof{}, nil
	}

	empty, proven := inventory.ProviderTreeVacant()

	return containmentProof{vacantProven: proven, empty: empty, barrier: root}, nil
}

// settlePrompt is the one durability boundary every accepted turn passes
// through. An ordinary prompt of a surviving generation is ordered
//
//	native terminal -> durable foreground-prefix commit -> terminal idle
//	                -> prompt response
//
// and emits no quiescence fact merely because a prompt ended: foreground
// completion proves nothing about background work. An exit that actually ends the
// native generation runs the whole close-fenced order instead, with the
// containment and vacancy proof ahead of the commit and the quiescence fact the
// completed proof produced after it.
//
// Settlement runs on a context detached from the request's and bounded on its
// own. A cancelled request still gets its durable commit and its terminal
// boundary: the cancellation ends the native turn, never the settlement of what
// that turn already streamed.
func (s *session) settlePrompt(
	ctx context.Context,
	turnCtx context.Context,
	turnEpoch uint64,
	baseline SessionStoreTerminalState,
	run promptRun,
	messageID *string,
) (acp.PromptResponse, bool, error) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettlementTimeout)
	defer cancel()

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	defer s.finishTurn()

	s.mu.Lock()

	cancelledBeforeSettlement := s.turnSettlement == turnSettlementCancelled
	if s.turnSettlement == turnSettlementOpen {
		s.turnSettlement = turnSettlementCapturing
	}
	s.mu.Unlock()

	if cancelledBeforeSettlement {
		run.err = nil
		run.cancelled = true
		run.endsIncarnation = true
		run.markCancelled = true
	}

	// The commit claim is the settlement linearization point and the single race
	// authority: after it, a cancel is a post-settlement no-op, and a cancel that
	// won before it is recorded as this turn's outcome without abandoning the
	// commit the accepted turn owes.
	stream := s.lifecycleStream()
	proof := containmentProof{}

	if run.endsIncarnation {
		var fenceErr error

		proof, fenceErr = s.fenceIncarnation(settleCtx, turnEpoch, run.markCancelled)
		if fenceErr != nil {
			// Durability outranks the terminal event: a containment boundary that
			// did not complete commits nothing, emits no terminal idle, and leaves
			// the incarnation unsettled for the next snapshot to state truthfully.
			stream.fence()

			return acp.PromptResponse{}, false, errors.Join(run.err, fenceErr)
		}
	}

	outcome, response := s.terminalMapping(run, messageID)

	// Every blocking action terminalizes before the transition that ends its
	// cycle, and before the commit: a pending foreground blocker is exactly the
	// state no snapshot may be taken under.
	if err := stream.terminalizeBlockers(settleCtx); err != nil {
		return acp.PromptResponse{}, false, errors.Join(run.err, err)
	}

	raced, published, err := s.commitForegroundPrefix(settleCtx, baseline, turnEpoch, run, outcome)
	if err != nil {
		return acp.PromptResponse{}, false, errors.Join(run.err, err)
	}

	if raced {
		run.cancelled = true
		run.endsIncarnation = true
		outcome, response = s.terminalMapping(run, messageID)
	}

	if err := stream.settle(settleCtx, outcome); err != nil {
		return acp.PromptResponse{}, published, errors.Join(run.err, err)
	}

	if run.endsIncarnation {
		// One durable write discharges both commits where the boundaries coincide:
		// this exit ends the incarnation, so the generation just committed is also
		// the resumable snapshot the quiescence fact stands on.
		if proof.vacant() {
			if err := stream.certify(settleCtx, proof.barrier); err != nil {
				return acp.PromptResponse{}, published, errors.Join(run.err, err)
			}
		}

		stream.fence()
	}

	if run.err != nil {
		return acp.PromptResponse{}, published, run.err
	}

	if !run.cancelled {
		terminal := s.committedTerminalState()
		terminal.Outcome = ""
		terminal.StopReason = ""
		response.Meta = terminalResponseMeta(terminal)
	}

	return response, true, nil
}

// terminalMapping derives the turn's recorded boundary and its ACP v1 response.
// The cancel guard runs here, immediately before every terminal mapping and the
// success one included: a native terminal observed while the turn is cancelled
// settles as cancelled and never as a completed turn.
func (s *session) terminalMapping(run promptRun, messageID *string) (lifecycleTurnOutcome, acp.PromptResponse) {
	if run.cancelled {
		return lifecycleTurnOutcome{
			stopReason: lifecycle.StopReasonCancelled,
			outcome:    lifecycle.OutcomeCancelled,
		}, acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: messageID}
	}

	if run.err != nil {
		// No ACP v1 stop reason names a failure; the v1 error carries it.
		return lifecycleTurnOutcome{outcome: lifecycle.OutcomeFailed}, acp.PromptResponse{}
	}

	stopReason, outcome := terminalOutcomeFromHermes(run.finish)

	return lifecycleTurnOutcome{stopReason: string(stopReason), outcome: outcome}, acp.PromptResponse{
		StopReason:    stopReason,
		Usage:         run.usage,
		UserMessageId: messageID,
	}
}

// commitForegroundPrefix is the durable foreground-prefix commit every accepted
// exit owes. It records how the turn actually ended and the largest prefix of it
// this wrapper can state truthfully, and it never advances the native archive's
// completed assistant identity for a turn that completed none.
// The results report whether the claim observed a pre-claim cancel and whether
// this exit actually published a generation.
func (s *session) commitForegroundPrefix(
	ctx context.Context,
	baseline SessionStoreTerminalState,
	turnEpoch uint64,
	run promptRun,
	outcome lifecycleTurnOutcome,
) (bool, bool, error) {
	stream := s.lifecycleStream()

	requirement := &terminalSnapshotRequirement{
		baseline:  baseline,
		turnEpoch: turnEpoch,
		foreground: stateSnapshotForeground{
			StreamID:            stream.streamID(),
			TurnID:              stream.turnIdentity(),
			Outcome:             string(outcome.outcome),
			StopReason:          outcome.stopReason,
			MessageID:           run.nativeMessageID,
			Text:                s.foregroundPrefix(),
			CapturedAtUnixMilli: time.Now().UnixMilli(),
		},
		completed:         run.err == nil && !run.cancelled,
		nativeUnavailable: run.endsIncarnation,
		settlementCapture: true,
	}

	if s.wasCancelled() {
		requirement.completed = false
		requirement.nativeUnavailable = true
		requirement.foreground.Outcome = string(lifecycle.OutcomeCancelled)
		requirement.foreground.StopReason = lifecycle.StopReasonCancelled
	}

	commit, commitErr := s.captureSnapshotLocked(ctx, requirement)
	if commitErr == nil {
		var raced bool

		raced, commitErr = s.claimTerminalCommit(turnEpoch)
		if commitErr == nil && raced {
			requirement.completed = false
			requirement.nativeUnavailable = true
			requirement.foreground.Outcome = string(lifecycle.OutcomeCancelled)
			requirement.foreground.StopReason = lifecycle.StopReasonCancelled
			commit, commitErr = s.captureSnapshotLocked(ctx, requirement)
		}

		if commitErr == nil {
			commitErr = s.publishSnapshotLocked(ctx, commit)
		}

		if commitErr == nil {
			return raced, true, nil
		}
	}

	// A commit this session cannot complete is a wrapper-invariant break: the
	// store no longer holds what the host was shown, so the session is poisoned
	// and its runtime fenced rather than left addressable over durable state
	// nobody can trust.
	poisonErr := s.poisonWithError(ctx, "hermes_terminal_snapshot_failed", commitErr.Error())
	fenceErr := s.fenceTurn(ctx, turnEpoch, false)

	stream.fence()

	return false, false, errors.Join(commitErr, poisonErr, fenceErr)
}

// unacceptedCancel settles a cancel that won before the dispatch point. No
// submission and no turn exist, so there is no boundary to report: the
// incarnation is fenced and the request answers with the cancelled stop reason
// alone.
func (s *session) unacceptedCancel(turnEpoch uint64, messageID *string) promptRun {
	if fenceErr := s.fenceTurn(context.Background(), turnEpoch, true); fenceErr != nil {
		return promptRun{err: fenceErr}
	}

	s.lifecycleStream().fence()

	return promptRun{response: acp.PromptResponse{
		StopReason:    acp.StopReasonCancelled,
		UserMessageId: messageID,
	}}
}

// observedCancel reports whether this turn has been cancelled, by the routed
// cancel or by the request context that admitted it.
func (s *session) observedCancel(turnCtx context.Context) bool {
	return s.wasCancelled() || turnCtx.Err() != nil
}

// runPromptTurn drives the native turn to its end and reports what it reached.
// It fences nothing after the dispatch point and commits nothing at all: every
// post-acceptance exit reports one promptRun, so settlement stays in one place.
func (s *session) runPromptTurn(
	ctx context.Context,
	turnCtx context.Context,
	turnEpoch uint64,
	submission lifecycle.Submission,
	req nativehermes.MessageRequest,
	messageID *string,
) promptRun {
	var abortOnce sync.Once

	abortTurn := func() {
		abortOnce.Do(func() {
			abortCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			_ = s.client.Abort(abortCtx, s.idmap.NativeSessionID)

			cancel()
		})
	}

	if err := s.reconcileConnected(turnCtx); err != nil {
		if errors.Is(err, errPromptCancelled) || s.observedCancel(turnCtx) {
			return s.unacceptedCancel(turnEpoch, messageID)
		}

		abortTurn()

		return promptRun{err: err}
	}

	type nativeResult struct {
		message nativehermes.NativeMessage
		err     error
	}

	var acceptErr error

	done := make(chan nativeResult, 1)
	// Acceptance is emitted at the dispatch linearization point the gateway
	// reports, so it precedes every event the submitted frame causes and follows
	// nothing the frame did not cause.
	dispatchCtx := nativehermes.WithPromptDispatch(turnCtx, func(hookCtx context.Context) error {
		acceptErr = s.lifecycleStream().accept(hookCtx, submission)

		return acceptErr
	})

	go func() {
		defer recoverAgentGoroutine(turnCtx, agentLogger(s.agent), "Hermes turn send")

		message, err := s.client.SendMessage(dispatchCtx, s.idmap.NativeSessionID, req)
		done <- nativeResult{message: message, err: err}
	}()

	timeout, stopTimer := s.promptDeadline()
	defer stopTimer()

	for {
		select {
		case event := <-s.client.Events():
			if event.Type == evtServerConnected {
				if err := s.reconcileConnected(turnCtx); err != nil {
					return s.failedRun(turnCtx, err)
				}

				continue
			}

			if err := s.handleEvent(turnCtx, event); err != nil {
				return s.failedRun(turnCtx, err)
			}
		case err := <-s.client.EventErrors():
			// Cancel guard runs before all failure mapping: a stream error
			// observed while the turn is cancelled stays cancelled.
			cancelled := s.observedCancel(turnCtx)
			s.markStreamFailed(nativehermes.StreamErrorEpoch(err))

			if cancelled {
				return s.cancelledRun()
			}

			return s.failedRun(turnCtx,
				mapTurnFailure(nativehermes.NewTurnFailure(nativehermes.CauseTransport, err.Error())))
		case result := <-done:
			return s.nativeRun(turnCtx, result.message, result.err, acceptErr)
		case <-timeout:
			// The cancel guard runs before all failure mapping, the turn deadline
			// included: when a user cancel and the timeout fire together the result
			// is deterministically cancelled, never cause "timeout".
			if s.observedCancel(turnCtx) {
				return s.cancelledRun()
			}

			// A turn deadline is a failure, not a user cancel.
			return promptRun{
				settle:          true,
				endsIncarnation: true,
				err: mapTurnFailure(nativehermes.NewTurnFailure(
					nativehermes.CauseTimeout,
					fmt.Sprintf("hermes turn exceeded %s deadline", s.agent.turnTimeout()),
				)),
			}
		case <-turnCtx.Done():
			return s.cancelledRun()
		}
	}
}

// promptDeadline builds the optional per-turn deadline channel.
func (s *session) promptDeadline() (<-chan time.Time, func() bool) {
	turnTimeout := s.agent.turnTimeout()
	if turnTimeout <= 0 {
		return nil, func() bool { return false }
	}

	newTimer := s.agent.options.newPromptTimer
	if newTimer == nil {
		newTimer = func(timeout time.Duration) promptTimer {
			timer := time.NewTimer(timeout)

			return promptTimer{C: timer.C, Stop: timer.Stop}
		}
	}

	timer := newTimer(turnTimeout)

	return timer.C, timer.Stop
}

// cancelledRun reports a post-acceptance user cancel. The incarnation ends: the
// cancel path contains the native generation, and settlement states the cancelled
// terminal boundary once that containment and the durable commit are done.
func (s *session) cancelledRun() promptRun {
	return promptRun{settle: true, cancelled: true, endsIncarnation: true, markCancelled: true}
}

// failedRun reports a post-acceptance failure, keeping the cancel guard ahead of
// every failure mapping.
func (s *session) failedRun(turnCtx context.Context, err error) promptRun {
	if errors.Is(err, errPromptCancelled) || s.observedCancel(turnCtx) {
		return s.cancelledRun()
	}

	return promptRun{settle: true, endsIncarnation: true, err: err}
}

// nativeRun reports the native terminal the gateway returned. The acceptance
// emission is inspected first: the gateway owned the frame by then, so a stream
// this adapter could not state truthfully fails the accepted turn rather than
// letting the turn report a boundary the stream never carried.
func (s *session) nativeRun(
	turnCtx context.Context,
	message nativehermes.NativeMessage,
	sendErr error,
	acceptErr error,
) promptRun {
	if acceptErr != nil {
		return promptRun{settle: true, endsIncarnation: true, err: acceptErr}
	}

	if sendErr != nil {
		if s.observedCancel(turnCtx) {
			return s.cancelledRun()
		}

		if nativehermes.IsGatewayDisconnect(sendErr) {
			s.markStreamFailed(0)
		}

		return promptRun{settle: true, endsIncarnation: true, err: mapTurnFailure(sendErr)}
	}

	if err := s.emitMessage(turnCtx, message, false); err != nil {
		return s.failedRun(turnCtx, err)
	}

	s.markMessageCompleted(message.Info.ID)

	if s.observedCancel(turnCtx) {
		return s.cancelledRun()
	}

	if beforeCommit := s.agent.options.beforeTerminalCommit; beforeCommit != nil {
		beforeCommit()
	}

	return promptRun{
		settle:          true,
		usage:           usageFromTokens(message.Info.Tokens),
		finish:          message.Info.Finish,
		nativeMessageID: message.Info.ID,
	}
}

// settleClosedSession runs the close-fenced settlement boundary for one session.
// The order is the contract's: the whole-tree containment and vacancy proof
// completes first, then every residual owned entity is terminalized on the
// stream, then the resumable snapshot is made durable, then the quiescence fact
// the completed proof produced is stated, and only then is the stream fenced.
//
// A close reached from an already-committed between-turn idle invents no second
// cycle and duplicates no terminal idle: the turn that settled already emitted
// its own, and this boundary settles the incarnation rather than a foreground
// cycle.
//
// The resumable generation is read before the containment boundary and made
// durable after it. Containment removes the generation's own files, so the read
// cannot follow it; the commit is the durability boundary the ordering rule is
// stated against, and it is what happens afterwards.
func (s *session) settleClosedSession(ctx context.Context) error {
	stream := s.lifecycleStream()

	var (
		commit     *sessionStoreCommit
		captureErr error
	)

	committed := s.committedState()
	if s.snapshotBlockedReason() == "" && s.ensureNotPoisoned() == nil && !stream.fenced() && committed.foreground == nil {
		commit, captureErr = s.captureSnapshotLocked(context.WithoutCancel(ctx), nil)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
	closeErr := s.closeLocked(closeCtx, false)

	closeCancel()

	if closeErr != nil {
		// The boundary did not complete. Terminalizing here would declare terminal
		// a set of entities this session has just proved it cannot contain, and
		// terminal is immutable, so nothing is terminalized, nothing new is
		// committed, no quiescence fact is stated, and the stream is still fenced.
		stream.fence()

		return errors.Join(captureErr, closeErr)
	}

	proof := s.closedContainmentProof()
	settleErr := s.publishClosedBoundary(ctx, stream, commit, proof)
	stream.fence()

	return errors.Join(captureErr, settleErr)
}

// publishClosedBoundary runs the three ordered rungs a completed close boundary
// owes: terminalize what the session still holds, make the resumable snapshot
// durable, and state the quiescence fact the proof produced. A failed rung stops
// the ones after it, so no boundary claims a fact the store does not back.
func (s *session) publishClosedBoundary(
	ctx context.Context,
	stream *sessionStream,
	commit *sessionStoreCommit,
	proof containmentProof,
) error {
	if err := stream.terminalizeBlockers(ctx); err != nil {
		return err
	}

	if commit != nil {
		if err := s.publishSnapshotLocked(context.WithoutCancel(ctx), commit); err != nil {
			return err
		}
	}

	if !proof.vacant() {
		return nil
	}

	return stream.certify(ctx, proof.barrier)
}

// closedContainmentProof reads what the close boundary that just completed proved
// about the native tree it owned.
func (s *session) closedContainmentProof() containmentProof {
	s.mu.Lock()
	client := s.client
	barrier := s.idmap.NativeSessionID
	s.mu.Unlock()

	inventory, ok := client.(providerTreeInventory)
	if !ok {
		return containmentProof{}
	}

	empty, proven := inventory.ProviderTreeVacant()

	return containmentProof{vacantProven: proven, empty: empty, barrier: barrier}
}

// announcedAction is one held request's lifecycle identity and the correlation
// value its ACP request carries.
type announcedAction struct {
	id          string
	correlation map[string]any
}

// announceBlockingAction registers one held request against its lifecycle action
// id and then announces it on the ordered stream. The second result is false when
// the connection negotiated the extension but no accepted turn owns the request:
// such a request is residue of a fenced incarnation, and it is refused natively
// rather than shown to a host that could not answer it.
func (s *session) announceBlockingAction(
	ctx context.Context,
	kind lifecycle.ActionKind,
	requestID string,
) (announcedAction, bool, error) {
	stream := s.lifecycleStream()
	if stream == nil {
		return announcedAction{}, true, nil
	}

	action, correlation, owned := stream.reserveAction(kind)
	if !owned {
		return announcedAction{}, false, nil
	}

	s.registerActionRequest(action.ActionID, requestID)

	if err := stream.announceAction(ctx, action); err != nil {
		return announcedAction{}, false, err
	}

	return announcedAction{id: action.ActionID, correlation: correlation}, true, nil
}

// resolveBlockingAction terminalizes one announced action exactly once. A second
// resolution finds no registration and changes nothing, so a callback that
// arrives twice never mutates a terminal action.
func (s *session) resolveBlockingAction(
	ctx context.Context,
	action announcedAction,
	state lifecycle.ActionState,
) error {
	if action.id == "" {
		return nil
	}

	if !s.takeActionRequest(action.id) {
		return nil
	}

	return s.lifecycleStream().resolveAction(ctx, action.id, state)
}

func (s *session) registerActionRequest(actionID string, requestID string) {
	s.mu.Lock()
	if s.actionRequests == nil {
		s.actionRequests = map[string]string{}
	}

	s.actionRequests[actionID] = requestID
	s.mu.Unlock()
}

func (s *session) takeActionRequest(actionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.actionRequests[actionID]; !ok {
		return false
	}

	delete(s.actionRequests, actionID)

	return true
}

// actionMeta stamps the lifecycle correlation beside whatever vendor or route
// metadata a held request already carries. Response `_meta` is read by nobody, so
// this value exists only on the way out.
func actionMeta(meta map[string]any, action announcedAction) map[string]any {
	if action.correlation == nil {
		return meta
	}

	stamped := cloneAnyMap(meta)
	if stamped == nil {
		stamped = map[string]any{}
	}

	stamped[lifecycle.MetaKey] = action.correlation

	return stamped
}

// permissionActionState maps one answered permission to the action state it
// resolved to. The outcome is derived only from the structural outcome union;
// `_meta` on the response is read by nobody.
func permissionActionState(resp acp.RequestPermissionResponse, reply string) lifecycle.ActionState {
	switch {
	case resp.Outcome.Cancelled != nil:
		return lifecycle.ActionCancelled
	case reply == valReject:
		return lifecycle.ActionDeclined
	default:
		return lifecycle.ActionAccepted
	}
}
