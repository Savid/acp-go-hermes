package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

type promptDispatchResult struct {
	projection <-chan error
	registered bool
	err        error
}

func consumePromptDispatchBeforeResult(
	dispatched bool,
	observed bool,
	results <-chan promptDispatchResult,
	apply func(promptDispatchResult),
) {
	if dispatched && !observed {
		apply(<-results)
	}
}

func (s *session) awaitPromptProjection(turnCtx context.Context, projection <-chan error) *promptRun {
	select {
	case projectionErr := <-projection:
		if projectionErr != nil {
			run := s.failedRun(turnCtx, projectionErr)

			return &run
		}

		return nil
	case <-turnCtx.Done():
		run := s.cancelledRun()

		return &run
	}
}

// turnSettlement is the completion latch close and delete wait on. It is
// released only once the prompt is wholly settled — the containment boundary,
// the durable commit, the terminal idle, and the quiescence fact — so a close
// response can never fence a stream this prompt is still writing to, and can
// never return before the frames its host was shown are durable. The
// settlement's own verdict belongs to the prompt that produced it: close and
// delete wait for the boundary, they do not inherit its error.
type turnSettlement struct {
	done    chan struct{}
	once    sync.Once
	release func()
	notify  func()
}

func (t *turnSettlement) complete() {
	if t == nil {
		return
	}

	t.once.Do(func() {
		if t.release != nil {
			t.release()
		}

		close(t.done)

		if t.notify != nil {
			t.notify()
		}
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
	foreground := s.foreground
	reservation := s.promptReservation
	s.mu.Unlock()

	foregroundErr := foreground.await(ctx)
	if reservation == foreground {
		return foregroundErr
	}

	return errors.Join(foregroundErr, reservation.await(ctx))
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
	s.lifecycleClosing = true
	reuseCancel := s.reuseCancel

	cancelTurn := s.promptReservation != nil && s.turnInFlight &&
		(s.turnSettlement == turnSettlementIdle || s.turnSettlement == turnSettlementOpen)
	if cancelTurn {
		s.turnSettlement = turnSettlementCancelled
	}
	s.mu.Unlock()

	if reuseCancel != nil {
		reuseCancel(acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed}))
	}

	if !cancelTurn {
		return false
	}

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
// authority no longer owns, and is no proof at all.
func (s *session) fenceIncarnation(ctx context.Context, turnEpoch uint64, markCancelled bool) (containmentProof, error) {
	s.mu.Lock()
	client := s.client
	root := s.idmap.NativeSessionID
	s.mu.Unlock()

	if err := s.fenceTurn(ctx, turnEpoch, markCancelled); err != nil {
		return containmentProof{}, err
	}

	if client == nil || s.agent.options.HostAuthority == nil {
		return containmentProof{}, nil
	}

	return containmentProof{vacantProven: true, empty: true, barrier: root}, nil
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
		// The committed boundary is reported as it was committed. The commit
		// above recorded the outcome this turn actually reached — derived from
		// the native finish reason, not from anything this wrapper decided — so
		// blanking it here would publish a field that could only ever be empty.
		response.Meta = terminalResponseMeta(s.committedTerminalState())
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

	// The finish was proved mappable before this run was reported, so the
	// mapping verdict has nothing left to decide here.
	stopReason, outcome, _ := terminalOutcomeFromHermes(run.finish)

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
	if commitErr != nil && s.wasCancelled() {
		// A cancel that won during the pre-claim capture fenced the native
		// runtime the capture was reading, so the capture failed underneath a
		// turn whose truthful outcome is cancelled. Rebuild the requirement in
		// the cancelled shape — restating the last durable native identity
		// rather than reading the fenced generation — then claim, re-capture,
		// and publish; the session is poisoned only when that retry also fails.
		requirement.completed = false
		requirement.nativeUnavailable = true
		requirement.foreground.Outcome = string(lifecycle.OutcomeCancelled)
		requirement.foreground.StopReason = lifecycle.StopReasonCancelled

		var raced bool

		raced, commitErr = s.claimTerminalCommit(turnEpoch)
		if commitErr == nil {
			commit, commitErr = s.captureSnapshotLocked(ctx, requirement)
		}

		if commitErr == nil {
			commitErr = s.publishSnapshotLocked(ctx, commit)
		}

		if commitErr == nil {
			return raced, true, nil
		}
	} else if commitErr == nil {
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
	poisonErr := s.poisonWithCause(ctx, poisonTerminalSnapshotFailed, commitErr)
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

	if err := s.synchronizePump(turnCtx); err != nil {
		if errors.Is(err, errPromptCancelled) || s.observedCancel(turnCtx) {
			return s.unacceptedCancel(turnEpoch, messageID)
		}

		abortTurn()

		return promptRun{err: err}
	}

	type nativeResult struct {
		message    nativehermes.NativeMessage
		err        error
		dispatched bool
	}

	var (
		projectionResult     <-chan error
		projected            bool
		projectionRegistered bool
		acceptErr            error
		dispatchObserved     bool
	)

	dispatchResults := make(chan promptDispatchResult, 1)
	applyDispatch := func(result promptDispatchResult) {
		dispatchObserved = true
		projectionResult = result.projection
		projectionRegistered = result.registered
		acceptErr = result.err
	}

	done := make(chan nativeResult, 1)

	var dispatchSent atomic.Bool
	// Acceptance is emitted at the dispatch linearization point the gateway
	// reports, so it precedes every event the submitted frame causes and follows
	// nothing the frame did not cause.
	dispatchCtx := nativehermes.WithPromptDispatch(turnCtx, func(hookCtx context.Context, info nativehermes.PromptDispatchInfo) error {
		dispatchSent.Store(true)

		if promotionErr := s.promotePromptForeground(); promotionErr != nil {
			dispatchResults <- promptDispatchResult{err: promotionErr}

			return promotionErr
		}

		projectionRoute, projection, registrationErr := s.registerPromptProjection(info, turnNonceFromContext(turnCtx), turnEpoch)
		if registrationErr != nil {
			dispatchResults <- promptDispatchResult{err: registrationErr}

			return registrationErr
		}

		dispatchErr := s.lifecycleStream().accept(hookCtx, submission)
		if dispatchErr != nil {
			s.resolvePromptProjection(projectionRoute, dispatchErr)
		}

		dispatchResults <- promptDispatchResult{
			projection: projection,
			registered: true,
			err:        dispatchErr,
		}

		return dispatchErr
	})

	go func() {
		defer func() {
			if recover() != nil {
				done <- nativeResult{err: nativehermes.NewTurnFailure(
					nativehermes.CauseTransport,
					"hermes prompt source corrupted",
				), dispatched: dispatchSent.Load()}
			}
		}()

		message, err := s.client.SendMessage(dispatchCtx, s.idmap.NativeSessionID, req)
		done <- nativeResult{message: message, err: err, dispatched: dispatchSent.Load()}
	}()

	timeout, stopTimer := s.promptDeadline()
	defer stopTimer()

	for {
		select {
		case dispatch := <-dispatchResults:
			applyDispatch(dispatch)
		case projectionErr := <-projectionResult:
			projectionResult = nil

			if projectionErr != nil {
				return s.failedRun(turnCtx, projectionErr)
			}

			projected = true

			if s.afterProjectionAck != nil {
				s.afterProjectionAck()
			}
		case result := <-done:
			consumePromptDispatchBeforeResult(result.dispatched, dispatchObserved, dispatchResults, applyDispatch)

			waitForProjection := result.err == nil
			if result.err != nil {
				var failure *nativehermes.TurnFailureError

				waitForProjection = errors.As(result.err, &failure) && failure.Cause() == nativehermes.CauseProvider &&
					!s.observedCancel(turnCtx)
			}

			if projectionRegistered && waitForProjection && !projected {
				if projectionRun := s.awaitPromptProjection(turnCtx, projectionResult); projectionRun != nil {
					return *projectionRun
				}
			}

			return s.nativeRun(turnCtx, result.message, result.err, acceptErr, result.dispatched)
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
	dispatched bool,
) promptRun {
	if !dispatched {
		if acceptErr != nil {
			return promptRun{err: acceptErr}
		}

		if errors.Is(sendErr, nativehermes.ErrGatewayTurnAbsorbedPrompt) {
			// Hermes folded this prompt into the turn already running. The text
			// ran exactly once, inside that turn, and the turn projects its own
			// output: a retry would fold the same text in a second time.
			return promptRun{err: sessionPromptAbsorbed()}
		}

		if errors.Is(sendErr, nativehermes.ErrGatewayPromptQueuedAsNextTurn) {
			// Hermes queued this prompt as the session's next native turn and ran
			// it in a turn that announced no start of its own. The text runs
			// exactly once, and the turn running it projects as agent-origin work:
			// a retry would run the same text a second time.
			return promptRun{err: sessionPromptQueuedTurn()}
		}

		if errors.Is(sendErr, nativehermes.ErrGatewayAgentBusy) {
			// Hermes runs whole autonomous turns between prompts. One of them
			// holding the native session is the same contention the ACP foreground
			// already states, and the caller can retry it once the turn settles.
			return promptRun{err: sessionForegroundBackpressure()}
		}

		if sendErr != nil {
			return promptRun{err: mapTurnFailure(sendErr)}
		}

		return promptRun{err: mapTurnFailure(nativehermes.ErrPromptDispatchIdentity)}
	}

	if acceptErr != nil {
		return promptRun{settle: true, endsIncarnation: true, err: acceptErr}
	}

	if sendErr != nil {
		if s.observedCancel(turnCtx) {
			return s.cancelledRun()
		}

		endsIncarnation := true

		var failure *nativehermes.TurnFailureError
		if errors.As(sendErr, &failure) && failure.Cause() == nativehermes.CauseProvider {
			endsIncarnation = false
		}

		return promptRun{settle: true, endsIncarnation: endsIncarnation, err: mapTurnFailure(sendErr)}
	}

	if err := s.emitMessage(turnCtx, message, false); err != nil {
		return s.failedRun(turnCtx, err)
	}

	s.markMessageCompleted(message.Info.ID)

	if s.observedCancel(turnCtx) {
		return s.cancelledRun()
	}

	// The native terminal is read before the turn is called complete. A finish
	// outside the closed vocabulary is a terminal this adapter cannot state, so
	// it settles as a failure carried by the v1 turn error rather than as the
	// clean end of turn a default arm would have invented.
	if _, _, mapped := terminalOutcomeFromHermes(message.Info.Finish); !mapped {
		return s.failedRun(turnCtx, mapTurnFailure(nativehermes.NewTurnFailure(
			nativehermes.CauseProvider,
			fmt.Sprintf("hermes reported unmapped turn finish %q", message.Info.Finish),
		)))
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
// The emission rungs of that order apply only to a live incarnation. A stream a
// cancel or an incarnation loss already fenced, and one whose opening snapshot
// was never delivered, are skipped entirely; the containment proof and the
// durable commit run either way, and a capture failure still fails the close.
// Every exit fences, so no path leaves a close behind with an incarnation still
// able to speak.
//
// A close reached from an already-committed between-turn idle invents no second
// cycle and duplicates no terminal idle: the turn that settled already emitted
// its own, and this boundary settles the incarnation rather than a foreground
// cycle.
//
// Managed capture reads protocol state first, then settles and reclaims the
// native residence before reading its state database. Ordinary capture reads
// its same-identity residence before shutdown. In both modes publication is the
// durability boundary and follows the completed capture.
//
// A boundary that fails is not spent. The generation it captured but could not
// publish is retained on the session, and the next close of the same id publishes
// exactly those bytes: the runtime they were read from is gone by then, so a
// retry that re-read would have nothing to read and the owed commit would be lost
// for good.
func (s *session) settleClosedSession(ctx context.Context) error {
	stream := s.lifecycleStream()

	pumpSyncCtx, pumpSyncCancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	pumpSyncErr := s.synchronizePump(pumpSyncCtx)

	pumpSyncCancel()

	if pumpSyncErr != nil {
		s.mu.Lock()
		alreadyContained := s.runtimeNeedsResume || s.containmentSettled
		s.mu.Unlock()
		s.pumpMu.Lock()
		stopping := s.pumpStopping && s.pumpErr == nil
		s.pumpMu.Unlock()

		if alreadyContained || stopping {
			pumpSyncErr = nil
		}
	}

	var captureErr error

	// A retry owes the commit the failed boundary already captured, so it never
	// captures a second one.
	commit := s.takeOwedCloseCommit()

	committed := s.committedState()
	if commit == nil && s.snapshotBlockedReason() == "" && s.ensureNotPoisoned() == nil && !stream.fenced() {
		var (
			requirement *terminalSnapshotRequirement
			capture     = true
		)

		switch {
		case stream.hasOpenTurn():
			requirement = &terminalSnapshotRequirement{
				baseline: s.committedTerminalState(),
				foreground: stateSnapshotForeground{
					StreamID:            stream.streamID(),
					TurnID:              stream.turnIdentity(),
					Outcome:             string(lifecycle.OutcomeCancelled),
					StopReason:          lifecycle.StopReasonCancelled,
					Text:                s.foregroundPrefix(),
					CapturedAtUnixMilli: time.Now().UnixMilli(),
				},
				nativeUnavailable: true,
				settlementCapture: true,
			}
		case committed.foreground == nil:
			requirement = nil
		default:
			capture = false
		}

		if capture {
			commit, captureErr = s.captureSnapshotLocked(context.WithoutCancel(ctx), requirement)
		}
	}

	if captureErr != nil {
		// A failed ordinary capture leaves the live generation intact. A failed
		// managed filesystem capture retains its already-reclaimed residence and
		// partial commit. Both remain addressable for the exact retry.
		s.retainOwedCloseCommit(commit)

		return errors.Join(pumpSyncErr, captureErr)
	}

	if commitErr := s.completeManagedSnapshotCommit(commit); commitErr != nil {
		s.retainOwedCloseCommit(commit)

		return errors.Join(pumpSyncErr, commitErr)
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
		s.retainOwedCloseCommit(commit)

		return errors.Join(pumpSyncErr, captureErr, closeErr)
	}

	if !stream.live() {
		// There is no stream for the boundary to speak on. It is terminal, or its
		// opening whole-state assertion was never delivered, and either way the
		// terminalize and certify rungs have nothing truthful to add: an event on
		// a fenced stream is a stale_stream refusal, and one on an unopened stream
		// is a delta before the snapshot, so emitting would only join a violation
		// into a close that succeeded. The durable rung is not a stream rung, and
		// the fence may have landed while the containment boundary was running, so
		// the generation this boundary already captured is still published and a
		// capture failure it already observed is still reported.
		//
		// The incarnation still ends here, exactly as it does on the other two
		// exits. A never-opened stream is not yet terminal, and the owed opening
		// snapshot is delivered from a detached goroutine this close never joins,
		// so leaving it unfenced would let that snapshot reach the host after the
		// close answered success. Fencing is idempotent, so the already-fenced half
		// of this branch is unchanged.
		stream.fence()

		var commitErr error
		if commit != nil {
			commitErr = s.publishSnapshotLocked(context.WithoutCancel(ctx), commit)
			if commitErr != nil {
				s.retainOwedCloseCommit(commit)
			}
		}

		return errors.Join(pumpSyncErr, captureErr, commitErr)
	}

	proof := s.closedContainmentProof()
	published, settleErr := s.publishClosedBoundary(ctx, stream, commit, proof)

	if !published {
		s.retainOwedCloseCommit(commit)
	}

	stream.fence()

	return errors.Join(pumpSyncErr, captureErr, settleErr)
}

// takeOwedCloseCommit hands back the generation a failed close boundary captured
// and could not publish, clearing it so exactly one retry owns it.
func (s *session) takeOwedCloseCommit() *sessionStoreCommit {
	s.mu.Lock()
	defer s.mu.Unlock()

	commit := s.owedCloseCommit
	s.owedCloseCommit = nil

	return commit
}

// retainOwedCloseCommit keeps an unpublished generation for the next close of
// this id. It is only ever reached with the commit this boundary took, so a
// boundary that carried none records none and owes none.
func (s *session) retainOwedCloseCommit(commit *sessionStoreCommit) {
	s.mu.Lock()
	s.owedCloseCommit = commit
	s.mu.Unlock()
}

// publishClosedBoundary runs the three ordered rungs a completed close boundary
// owes: terminalize what the session still holds, make the resumable snapshot
// durable, and state the quiescence fact the proof produced. A failed rung stops
// the ones after it, so no boundary claims a fact the store does not back.
//
// Every rung runs on the same detached context the durable commit does. The
// containment proof has already completed and the stream is fenced the moment
// this returns, so a caller that cancelled mid-close cannot be answered with a
// boundary that skipped its terminal transitions and its quiescence fact: those
// emissions would have nowhere to be made afterwards, and the settlement
// response is not allowed to precede them.
//
// The first result reports whether the durable rung is discharged — the commit
// reached the store, or there was none to make — so a failed boundary knows
// whether the retry still owes it.
func (s *session) publishClosedBoundary(
	ctx context.Context,
	stream *sessionStream,
	commit *sessionStoreCommit,
	proof containmentProof,
) (bool, error) {
	ctx = context.WithoutCancel(ctx)

	if err := stream.terminalizeBlockers(ctx); err != nil {
		return false, err
	}

	if commit != nil {
		if err := s.publishSnapshotLocked(ctx, commit); err != nil {
			return false, err
		}
	}

	if stream.hasOpenTurn() {
		if err := stream.settle(ctx, lifecycleTurnOutcome{
			stopReason: lifecycle.StopReasonCancelled,
			outcome:    lifecycle.OutcomeCancelled,
		}); err != nil {
			return true, err
		}
	}

	if !proof.vacant() {
		return true, nil
	}

	return true, stream.certify(ctx, proof.barrier)
}

// closedContainmentProof reads what the close boundary that just completed proved
// about the native tree it owned.
func (s *session) closedContainmentProof() containmentProof {
	s.mu.Lock()
	client := s.client
	barrier := s.idmap.NativeSessionID
	s.mu.Unlock()

	if client == nil || s.agent.options.HostAuthority == nil {
		return containmentProof{}
	}

	return containmentProof{vacantProven: true, empty: true, barrier: barrier}
}

// announcedAction is one held request's lifecycle identity and the correlation
// value its ACP request carries.
type announcedAction struct {
	id          string
	correlation map[string]any
}

type actionRequestOwnership struct {
	incarnation uint64
	generation  uint64
	epoch       uint64
	cycleID     string
	turnID      string
	actionID    string
	requestID   string
	nonce       string
}

// reserveBlockingAction mints and registers one held request without emitting
// lifecycle state. The host JSON-RPC request must be registered and written
// successfully before publishBlockingAction exposes its pending action.
func (s *session) reserveBlockingAction(
	kind lifecycle.ActionKind,
	requestID string,
	route permissionTurnRoute,
) (announcedAction, lifecycle.ActionUpdate, bool) {
	stream := s.lifecycleStream()
	if stream == nil {
		return announcedAction{}, lifecycle.ActionUpdate{}, true
	}

	action, correlation, owned := stream.reserveAction(kind)
	if !owned {
		return announcedAction{}, lifecycle.ActionUpdate{}, false
	}

	s.registerActionRequest(action, requestID, route)

	return announcedAction{id: action.ActionID, correlation: correlation}, action, true
}

func (s *session) publishBlockingAction(
	ctx context.Context,
	reserved announcedAction,
	action lifecycle.ActionUpdate,
) error {
	if reserved.id == "" {
		return nil
	}

	stream := s.lifecycleStream()
	if err := stream.announceAction(ctx, action); err != nil {
		s.mu.Lock()
		delete(s.actionRequests, action.ActionID)
		s.mu.Unlock()

		return err
	}

	return nil
}

func (s *session) discardBlockingAction(action announcedAction) {
	if action.id == "" {
		return
	}

	s.mu.Lock()
	delete(s.actionRequests, action.id)
	s.mu.Unlock()
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

	taken, current := s.takeActionRequest(ctx, action.id)
	if !current {
		return routeInvalid("lifecycle action callback crossed its owning cycle")
	}

	if !taken {
		return nil
	}

	return s.lifecycleStream().resolveAction(ctx, action.id, state)
}

func (s *session) registerActionRequest(
	action lifecycle.ActionUpdate,
	requestID string,
	route permissionTurnRoute,
) {
	s.mu.Lock()
	if s.actionRequests == nil {
		s.actionRequests = map[string]actionRequestOwnership{}
	}

	s.actionRequests[action.ActionID] = actionRequestOwnership{
		incarnation: route.incarnation,
		generation:  route.generation,
		epoch:       route.epoch,
		cycleID:     route.cycleID,
		turnID:      action.Owner.ID,
		actionID:    action.ActionID,
		requestID:   requestID,
		nonce:       route.nonce,
	}
	s.mu.Unlock()
}

func (s *session) takeActionRequest(ctx context.Context, actionID string) (bool, bool) {
	route, active := s.permissionTurnRoute(ctx)
	if !active {
		return false, false
	}

	turnID := s.lifecycleStream().turnIdentity()

	s.mu.Lock()
	defer s.mu.Unlock()

	owned, ok := s.actionRequests[actionID]
	if !ok {
		return false, true
	}

	if owned.incarnation != route.incarnation || owned.generation != route.generation ||
		owned.epoch != route.epoch || owned.cycleID != route.cycleID || owned.turnID != turnID ||
		owned.actionID != actionID || owned.nonce != route.nonce {
		return false, false
	}

	delete(s.actionRequests, actionID)

	return true, true
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
