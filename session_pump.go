package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
)

const sessionPumpBacklogCapacity = 256

var ErrSessionPumpOverflow = errors.New("hermes session actor mailbox overflow")

type pumpRouteContextKey struct{}

type pumpCycleRoute struct {
	incarnation uint64
	generation  uint64
	cycleID     string
	nonce       string
	epoch       uint64
	prompt      bool
	settlement  *turnSettlement
	projected   chan error
	once        sync.Once
}

type pumpItem struct {
	event *nativehermes.TurnEvent
	err   error
	eof   bool
}

type pumpBarrierRequest struct {
	reply chan error
}

type sessionForegroundContentionError struct {
	cause error
}

func (e *sessionForegroundContentionError) Error() string { return e.cause.Error() }
func (e *sessionForegroundContentionError) Unwrap() error { return e.cause }

func withPumpRoute(ctx context.Context, route *pumpCycleRoute) context.Context {
	if route == nil {
		return ctx
	}

	ctx = context.WithValue(ctx, pumpRouteContextKey{}, route)

	return withTurnRoute(ctx, route.nonce)
}

func pumpRouteFromContext(ctx context.Context) (*pumpCycleRoute, bool) {
	if ctx == nil {
		return nil, false
	}

	route, ok := ctx.Value(pumpRouteContextKey{}).(*pumpCycleRoute)

	return route, ok && route != nil
}

func (s *session) startPump(client nativehermes.Server) {
	if client == nil {
		return
	}

	s.pumpMu.Lock()
	if s.pumpDone != nil {
		s.pumpMu.Unlock()

		return
	}

	s.pumpIncarnation++
	incarnation := s.pumpIncarnation
	ctx, cancel := context.WithCancel(context.Background())
	controlCtx, controlCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.pumpCancel = cancel
	s.pumpControlCancel = controlCancel
	s.pumpDone = done
	s.pumpClient = client
	s.pumpErr = nil
	s.pumpStopping = false
	s.pumpRoutes = map[string]*pumpCycleRoute{}
	s.pumpForegroundSettle = nil
	s.pumpDeferred = nil
	s.pumpRetry = make(chan struct{}, 1)
	s.pumpBarriers = make(chan pumpBarrierRequest)
	s.pumpMu.Unlock()

	go s.runPump(ctx, controlCtx, done, client, incarnation)
}

func (s *session) beginPumpShutdown() chan struct{} {
	s.pumpMu.Lock()
	s.pumpStopping = true
	controlCancel := s.pumpControlCancel
	done := s.pumpDone
	s.pumpMu.Unlock()

	if controlCancel != nil {
		controlCancel()
	}

	return done
}

func (s *session) stopPump() {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	_ = s.stopPumpContext(ctx)

	cancel()
}

func (s *session) stopPumpContext(ctx context.Context) error {
	done := s.beginPumpShutdown()
	s.cancelPump()

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *session) cancelPump() {
	s.pumpMu.Lock()
	cancel := s.pumpCancel
	s.pumpMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (s *session) notifyPumpRetry() {
	s.pumpMu.Lock()
	retry := s.pumpRetry
	s.pumpMu.Unlock()

	if retry == nil {
		return
	}

	select {
	case retry <- struct{}{}:
	default:
	}
}

func (s *session) detachPump() {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	_ = s.detachPumpContext(ctx)

	cancel()
}

func (s *session) detachPumpContext(ctx context.Context) error {
	if err := s.stopPumpContext(ctx); err != nil {
		return err
	}

	s.pumpMu.Lock()
	s.pumpCancel = nil
	s.pumpControlCancel = nil
	s.pumpDone = nil
	s.pumpClient = nil
	s.pumpRoutes = map[string]*pumpCycleRoute{}
	s.pumpForegroundSettle = nil
	s.pumpDeferred = nil
	s.pumpRetry = nil
	s.pumpBarriers = nil
	s.pumpMu.Unlock()

	return nil
}

func (s *session) synchronizePump(ctx context.Context) error {
	s.pumpMu.Lock()
	barriers := s.pumpBarriers
	done := s.pumpDone
	pumpErr := s.pumpErr
	s.pumpMu.Unlock()

	if pumpErr != nil {
		return pumpErr
	}

	if barriers == nil || done == nil {
		return errors.New("hermes session pump is unavailable")
	}

	reply := make(chan error, 1)
	select {
	case barriers <- pumpBarrierRequest{reply: reply}:
	case <-done:
		return s.stoppedPumpError()
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-reply:
		return err
	case <-done:
		return s.stoppedPumpError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *session) stoppedPumpError() error {
	s.pumpMu.Lock()
	defer s.pumpMu.Unlock()

	if s.pumpErr != nil {
		return s.pumpErr
	}

	return errors.New("hermes session pump stopped")
}

func (s *session) registerPromptProjection(
	info nativehermes.PromptDispatchInfo,
	nonce string,
	epoch uint64,
) (*pumpCycleRoute, <-chan error, error) {
	s.mu.Lock()
	settlement := s.settlement
	s.mu.Unlock()

	s.pumpMu.Lock()
	defer s.pumpMu.Unlock()

	if s.pumpDone == nil {
		return nil, nil, errors.New("hermes session pump is unavailable")
	}

	if s.pumpErr != nil {
		return nil, nil, s.pumpErr
	}

	if info.CycleID == "" || info.TransportGeneration == 0 {
		return nil, nil, routeInvalid("prompt dispatch is missing exact native identity")
	}

	if _, exists := s.pumpRoutes[info.CycleID]; exists {
		return nil, nil, errors.New("hermes prompt cycle is already registered")
	}

	route := &pumpCycleRoute{
		incarnation: s.pumpIncarnation,
		generation:  info.TransportGeneration,
		cycleID:     info.CycleID,
		nonce:       nonce,
		epoch:       epoch,
		prompt:      true,
		settlement:  settlement,
		projected:   make(chan error, 1),
	}
	s.pumpRoutes[info.CycleID] = route

	return route, route.projected, nil
}

func (s *session) resolvePromptProjection(route *pumpCycleRoute, err error) {
	if route == nil {
		return
	}

	route.once.Do(func() {
		route.projected <- err

		close(route.projected)
	})

	s.pumpMu.Lock()
	s.pumpForegroundSettle = route.settlement
	delete(s.pumpRoutes, route.cycleID)
	s.pumpMu.Unlock()
	s.compactCycleControls()
}

func (s *session) awaitPromptForegroundSettlement(ctx context.Context) error {
	s.pumpMu.Lock()
	settlement := s.pumpForegroundSettle
	s.pumpMu.Unlock()

	if err := settlement.await(ctx); err != nil {
		return err
	}

	s.pumpMu.Lock()
	if s.pumpForegroundSettle == settlement {
		s.pumpForegroundSettle = nil
	}
	s.pumpMu.Unlock()

	return nil
}

func (s *session) routeForEvent(event nativehermes.TurnEvent) *pumpCycleRoute {
	s.pumpMu.Lock()
	defer s.pumpMu.Unlock()

	route := s.pumpRoutes[event.CycleID]
	if event.CycleID == "" || event.TransportGeneration == 0 || route == nil || route.incarnation != s.pumpIncarnation {
		return nil
	}

	if route.generation == 0 || route.generation != event.TransportGeneration {
		return nil
	}

	return route
}

func (s *session) pumpRouteCurrent(route *pumpCycleRoute) bool {
	if route == nil {
		return false
	}

	s.pumpMu.Lock()
	defer s.pumpMu.Unlock()

	current := s.pumpRoutes[route.cycleID]

	return current == route && route.cycleID != "" && route.generation != 0 &&
		route.incarnation == s.pumpIncarnation && s.pumpErr == nil && !s.pumpStopping
}

func clonePumpItem(item pumpItem) pumpItem {
	if item.event != nil {
		event := *item.event
		item.event = &event
	}

	return item
}

func (s *session) deferBehindPumpFIFO(item pumpItem) (bool, error) {
	s.pumpMu.Lock()

	if len(s.pumpDeferred) == 0 {
		s.pumpMu.Unlock()

		return false, nil
	}

	if len(s.pumpDeferred) >= sessionPumpBacklogCapacity {
		s.pumpMu.Unlock()

		return true, ErrSessionPumpOverflow
	}

	s.pumpDeferred = append(s.pumpDeferred, clonePumpItem(item))
	s.pumpMu.Unlock()

	return true, nil
}

func (s *session) deferPumpItem(item pumpItem) {
	s.pumpMu.Lock()
	defer s.pumpMu.Unlock()

	s.pumpDeferred = append(s.pumpDeferred, clonePumpItem(item))
}

func (s *session) takeDeferredPumpItems() []pumpItem {
	s.pumpMu.Lock()
	items := s.pumpDeferred
	s.pumpDeferred = nil
	s.pumpMu.Unlock()

	return items
}

func resolvePumpItems(items []pumpItem, cause error) {
	for _, item := range items {
		if item.event != nil && item.event.ProjectionDone != nil {
			item.event.ProjectionDone(cause)
		}
	}
}

func (s *session) retryDeferredPumpItems(ctx context.Context) error {
	items := s.takeDeferredPumpItems()
	for index, item := range items {
		if err := s.handlePumpItem(ctx, item); err != nil {
			resolvePumpItems(items[index+1:], err)

			return err
		}
	}

	return nil
}

func (s *session) resolveDeferredPumpItems(cause error) {
	resolvePumpItems(s.takeDeferredPumpItems(), cause)
}

func (s *session) awaitPumpProjectionGate(
	ctx context.Context,
	deliveries <-chan nativehermes.TurnDelivery,
) ([]pumpItem, error) {
	backlog := make([]pumpItem, 0, sessionPumpBacklogCapacity)
	source := deliveries

	for {
		select {
		case <-s.projectionGate:
			return backlog, nil
		case delivery, ok := <-source:
			switch {
			case !ok:
				backlog = append(backlog, pumpItem{eof: true})
				source = nil

				if len(backlog) == 1 {
					return backlog, nil
				}
			case delivery.Event != nil:
				copyEvent := *delivery.Event
				backlog = append(backlog, pumpItem{event: &copyEvent})
			default:
				backlog = append(backlog, pumpItem{err: delivery.Err})
			}
		case <-ctx.Done():
			return backlog, errPromptCancelled
		}

		if len(backlog) > sessionPumpBacklogCapacity {
			return backlog, ErrSessionPumpOverflow
		}
	}
}

func (s *session) runPump(
	ctx context.Context,
	controlCtx context.Context,
	done chan struct{},
	client nativehermes.Server,
	incarnation uint64,
) {
	var terminal error

	var acceptedBarrier *pumpBarrierRequest

	var backlog []pumpItem

	s.pumpMu.Lock()
	retry := s.pumpRetry
	s.pumpMu.Unlock()

	defer func() {
		if recover() != nil {
			terminal = mapTurnFailure(nativehermes.NewTurnFailure(
				nativehermes.CauseTransport,
				"hermes session source corrupted",
			))
		}

		if acceptedBarrier != nil {
			acceptedBarrier.reply <- terminal

			acceptedBarrier = nil
		}

		for _, item := range backlog {
			if item.event != nil && item.event.ProjectionDone != nil {
				item.event.ProjectionDone(terminal)
			}
		}

		s.resolveDeferredPumpItems(terminal)

		s.pumpMu.Lock()
		stopping := s.pumpStopping && s.pumpIncarnation == incarnation
		s.pumpMu.Unlock()

		var resumeWait chan struct{}
		if terminal != nil && !stopping {
			resumeWait = s.publishPumpResumeNeeded(client, incarnation)
		}

		s.resolvePumpRoutes(terminal, !stopping)

		if resumeWait != nil {
			s.fencePumpClient(client, incarnation, terminal, resumeWait)
		}

		close(done)
	}()

	deliveries := client.Deliveries()

	backlog, terminal = s.awaitPumpProjectionGate(ctx, deliveries)
	if terminal != nil {
		return
	}

	for _, item := range backlog {
		if err := s.handlePumpItem(controlCtx, item); err != nil {
			terminal = err

			return
		}
	}
	// Every accepted projection in the pre-open backlog has now been resolved by
	// handlePumpItem. Keep only genuinely unprocessed entries for the deferred
	// failure path so ProjectionDone is invoked exactly once.
	backlog = nil

	for {
		retryReady := false

		select {
		case <-retry:
			retryReady = true
		default:
		}

		if !retryReady {
			select {
			case delivery, ok := <-deliveries:
				item := pumpItem{eof: !ok}
				switch {
				case !ok:
					deliveries = nil
				case delivery.Event != nil:
					copyEvent := *delivery.Event
					item = pumpItem{event: &copyEvent}
				default:
					item.err = delivery.Err
				}

				if err := s.handlePumpItem(controlCtx, item); err != nil {
					terminal = err

					return
				}

				continue
			case barrier := <-s.pumpBarriers:
				acceptedBarrier = &barrier

				s.pumpBarrierAccepted()
			case <-retry:
				retryReady = true
			case <-ctx.Done():
				terminal = errPromptCancelled

				return
			}
		}

		if retryReady {
			if err := s.retryDeferredPumpItems(controlCtx); err != nil {
				terminal = err

				return
			}

			continue
		}

		sourceClosed, err := s.drainPumpBarrier(controlCtx, deliveries)
		if sourceClosed {
			deliveries = nil
		}

		if err != nil {
			terminal = err
			acceptedBarrier.reply <- err

			acceptedBarrier = nil

			return
		}

		acceptedBarrier.reply <- nil

		acceptedBarrier = nil
	}
}

func (s *session) pumpBarrierAccepted() {
	if s.afterPumpBarrierAccept != nil {
		s.afterPumpBarrierAccept()
	}
}

func (s *session) drainPumpBarrier(
	ctx context.Context,
	deliveries <-chan nativehermes.TurnDelivery,
) (bool, error) {
	for {
		select {
		case delivery, ok := <-deliveries:
			item := pumpItem{eof: !ok}
			if ok && delivery.Event != nil {
				copyEvent := *delivery.Event
				item = pumpItem{event: &copyEvent}
			} else if ok {
				item.err = delivery.Err
			}

			if handleErr := s.handlePumpItem(ctx, item); handleErr != nil {
				return !ok, handleErr
			}

			if !ok {
				return true, nil
			}

			continue
		default:
		}

		return false, nil
	}
}

func (s *session) handlePumpItem(ctx context.Context, item pumpItem) (result error) {
	s.pumpMu.Lock()
	stopping := s.pumpStopping
	s.pumpMu.Unlock()

	if stopping {
		if item.event != nil && item.event.ProjectionDone != nil {
			item.event.ProjectionDone(nil)
		}

		// Close has stopped admission and cancelled every control callback. The
		// permanent reader still drains the source to containment, but the close
		// boundary owns the one durable cancelled terminal for any open cycle.
		return nil
	}

	if deferred, err := s.deferBehindPumpFIFO(item); deferred {
		if err != nil && item.event != nil && item.event.ProjectionDone != nil {
			item.event.ProjectionDone(err)
		}

		return err
	}

	if item.eof {
		return mapTurnFailure(nativehermes.NewTurnFailure(
			nativehermes.CauseTransport,
			"hermes session source ended",
		))
	}

	if item.err != nil {
		return mapTurnFailure(nativehermes.NewTurnFailure(nativehermes.CauseTransport, item.err.Error()))
	}

	if item.event == nil {
		return nil
	}

	event := *item.event
	if event.Type == nativehermes.EventCycleStarted && event.Origin == nativehermes.CycleOriginActivity {
		if err := s.awaitPromptForegroundSettlement(ctx); err != nil {
			if event.ProjectionDone != nil {
				event.ProjectionDone(err)
			}

			return err
		}

		err := s.startAutonomousCycle(ctx, event)

		var contention *sessionForegroundContentionError
		if errors.As(err, &contention) {
			s.deferPumpItem(item)

			return nil
		}

		if event.ProjectionDone != nil {
			event.ProjectionDone(err)
		}

		return err
	}

	if event.ProjectionDone != nil {
		defer func() { event.ProjectionDone(result) }()
	}

	if event.Type == nativehermes.EventGatewayRaw {
		if err := s.emitRawHermesEvent(ctx, event); err != nil {
			s.agent.observe.RecordRawEventEmitFailure(ctx)
		}

		return nil
	}

	route := s.routeForEvent(event)
	if route == nil {
		return routeInvalid("mapped Hermes event has no current cycle route")
	}

	routedCtx := withPumpRoute(ctx, route)
	if err := s.emitRawHermesEvent(routedCtx, event); err != nil {
		s.agent.observe.RecordRawEventEmitFailure(routedCtx)
	}

	switch event.Type {
	case evtApprovalRequest, evtClarifyRequest, evtMessagePartUpdated:
		return s.handleEvent(routedCtx, event)
	case nativehermes.EventCycleComplete:
		if route.prompt {
			s.resolvePromptProjection(route, nil)

			return nil
		}

		return s.settleAutonomousCycle(routedCtx, route, event)
	case nativehermes.EventCycleFailed:
		if route.prompt {
			err := event.Err

			var failure *nativehermes.TurnFailureError
			if errors.As(err, &failure) && failure.Cause() == nativehermes.CauseProvider {
				// The provider outcome travels on SendMessage's result. This channel
				// acknowledges only that the permanent pump projected its terminal;
				// reporting the provider cause here would race the two channels and
				// misclassify a normal failed cycle as dispatcher corruption.
				err = nil
			} else if err == nil {
				err = errors.New("hermes prompt cycle failed")
			}

			s.resolvePromptProjection(route, err)

			return nil
		}

		return s.failAutonomousCycle(routedCtx, route, event.Err)
	default:
		return nil
	}
}

func (s *session) startAutonomousCycle(ctx context.Context, event nativehermes.TurnEvent) error {
	if event.CycleID == "" || event.TransportGeneration == 0 {
		return routeInvalid("autonomous Hermes cycle is missing exact native identity")
	}

	s.mu.Lock()
	if s.closed || s.lifecycleClosing {
		s.mu.Unlock()

		return acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
	}

	if s.foreground != nil {
		s.mu.Unlock()

		return &sessionForegroundContentionError{cause: sessionForegroundBackpressure()}
	}

	settlement := s.claimForegroundLocked(foregroundAutonomous)
	s.mu.Unlock()

	nonce, err := s.newPumpNonce()
	if err != nil {
		settlement.complete()

		return err
	}

	s.pumpMu.Lock()
	if _, exists := s.pumpRoutes[event.CycleID]; exists {
		s.pumpMu.Unlock()
		settlement.complete()

		return routeInvalid("autonomous Hermes cycle is already active")
	}

	route := &pumpCycleRoute{
		incarnation: s.pumpIncarnation,
		generation:  event.TransportGeneration,
		cycleID:     event.CycleID,
		nonce:       nonce,
		settlement:  settlement,
	}
	s.pumpRoutes[event.CycleID] = route
	s.pumpMu.Unlock()

	s.mu.Lock()
	s.seenParts = map[string]string{}
	s.activeMessageIDs = map[string]struct{}{}
	s.toolStates = map[string]hermesToolState{}
	s.actionRequests = map[string]actionRequestOwnership{}
	s.foregroundText = nil
	s.foregroundTruncated = false
	s.mu.Unlock()

	if err := s.lifecycleStream().startActivity(withPumpRoute(ctx, route)); err != nil {
		s.removePumpRoute(route)

		return err
	}

	return nil
}

func (s *session) settleAutonomousCycle(
	ctx context.Context,
	route *pumpCycleRoute,
	event nativehermes.TurnEvent,
) error {
	defer s.finishAutonomousRoute(route)

	if event.Message == nil {
		return errors.New("hermes autonomous cycle completed without a message")
	}

	message := *event.Message
	if err := s.emitMessage(ctx, message, false); err != nil {
		return err
	}

	s.markMessageCompleted(message.Info.ID)

	stopReason, outcome, mapped := terminalOutcomeFromHermes(message.Info.Finish)
	if !mapped {
		return mapTurnFailure(nativehermes.NewTurnFailure(
			nativehermes.CauseProvider,
			fmt.Sprintf("hermes reported unmapped autonomous finish %q", message.Info.Finish),
		))
	}

	stream := s.lifecycleStream()
	if err := stream.terminalizeBlockers(ctx); err != nil {
		return err
	}

	requirement := &terminalSnapshotRequirement{
		baseline: s.committedTerminalState(),
		foreground: stateSnapshotForeground{
			StreamID:            stream.streamID(),
			TurnID:              stream.turnIdentity(),
			Outcome:             string(outcome),
			StopReason:          string(stopReason),
			MessageID:           message.Info.ID,
			Text:                s.foregroundPrefix(),
			CapturedAtUnixMilli: time.Now().UnixMilli(),
		},
		completed:         true,
		settlementCapture: true,
	}

	commit, err := s.captureSnapshotLocked(context.WithoutCancel(ctx), requirement)
	if err == nil {
		err = s.publishSnapshotLocked(context.WithoutCancel(ctx), commit)
	}

	if err != nil {
		poisonErr := s.poisonWithError(ctx, "hermes_terminal_snapshot_failed", err.Error())

		stream.fence()

		return errors.Join(err, poisonErr)
	}

	return stream.settle(ctx, lifecycleTurnOutcome{stopReason: string(stopReason), outcome: outcome})
}

func (s *session) failAutonomousCycle(ctx context.Context, route *pumpCycleRoute, _ error) error {
	defer s.finishAutonomousRoute(route)

	stream := s.lifecycleStream()
	if err := stream.terminalizeBlockers(ctx); err != nil {
		return err
	}

	requirement := &terminalSnapshotRequirement{
		baseline: s.committedTerminalState(),
		foreground: stateSnapshotForeground{
			StreamID:            stream.streamID(),
			TurnID:              stream.turnIdentity(),
			Outcome:             string(lifecycle.OutcomeFailed),
			Text:                s.foregroundPrefix(),
			CapturedAtUnixMilli: time.Now().UnixMilli(),
		},
		settlementCapture: true,
	}

	commit, err := s.captureSnapshotLocked(context.WithoutCancel(ctx), requirement)
	if err == nil {
		err = s.publishSnapshotLocked(context.WithoutCancel(ctx), commit)
	}

	if err != nil {
		stream.fence()

		return err
	}

	if err := stream.settle(ctx, lifecycleTurnOutcome{outcome: lifecycle.OutcomeFailed}); err != nil {
		return err
	}

	// A provider failure is a committed foreground outcome, not corruption of
	// the permanent event pump.
	return nil
}

func (s *session) removePumpRoute(route *pumpCycleRoute) {
	s.pumpMu.Lock()
	delete(s.pumpRoutes, route.cycleID)
	s.pumpMu.Unlock()

	if !route.prompt {
		route.settlement.complete()
	}
}

func (s *session) finishAutonomousRoute(route *pumpCycleRoute) {
	s.pumpMu.Lock()
	delete(s.pumpRoutes, route.cycleID)
	s.pumpMu.Unlock()
	s.compactCycleControls()
	route.settlement.complete()
}

func (s *session) compactCycleControls() {
	s.mu.Lock()
	s.processedPermission = map[string]struct{}{}

	s.processedQuestion = map[string]struct{}{}
	if len(s.pending) == 0 {
		s.pending = map[string]nativehermes.PermissionRequest{}
	}

	if len(s.questions) == 0 {
		s.questions = map[string]nativehermes.QuestionRequest{}
	}

	if len(s.actionRequests) == 0 {
		s.actionRequests = map[string]actionRequestOwnership{}
	}
	s.mu.Unlock()
}

func (s *session) resolvePumpRoutes(cause error, fence bool) {
	if cause == nil {
		cause = errPromptCancelled
	}

	s.pumpMu.Lock()
	if fence && s.pumpErr == nil {
		s.pumpErr = cause
	}

	routes := make([]*pumpCycleRoute, 0, len(s.pumpRoutes))
	for _, route := range s.pumpRoutes {
		routes = append(routes, route)
	}
	s.pumpMu.Unlock()

	var settlementErr error

	for _, route := range routes {
		switch {
		case route.prompt:
			s.resolvePromptProjection(route, cause)
		case fence:
			settleCtx, cancel := context.WithTimeout(context.Background(), sessionSettlementTimeout)
			settlementErr = errors.Join(settlementErr,
				s.failAutonomousCycle(withPumpRoute(settleCtx, route), route, cause))

			cancel()
		default:
			// Close owns the final capture and terminal boundary for autonomous
			// work. Remove its pump route now so shutdown joins the projection,
			// but do not publish a competing failure/fence from the pump.
			s.finishAutonomousRoute(route)
		}
	}

	if settlementErr != nil {
		s.pumpMu.Lock()
		s.pumpErr = errors.Join(s.pumpErr, settlementErr)
		s.pumpMu.Unlock()
	}
}

func (s *session) publishPumpResumeNeeded(
	client nativehermes.Server,
	incarnation uint64,
) chan struct{} {
	s.pumpMu.Lock()
	current := s.pumpIncarnation == incarnation && s.pumpClient == client
	s.pumpMu.Unlock()

	if !current {
		return nil
	}

	wait := make(chan struct{})

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		close(wait)

		return nil
	}

	s.runtimeNeedsResume = true
	s.runtimeResumeWait = wait
	s.runtimeResumeErr = nil
	s.mu.Unlock()

	return wait
}

func (s *session) fencePumpClient(
	client nativehermes.Server,
	incarnation uint64,
	cause error,
	resumeWait chan struct{},
) {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	closeErr := client.Close(ctx)

	cancel()

	s.pumpMu.Lock()
	current := s.pumpIncarnation == incarnation && s.pumpClient == client
	s.pumpMu.Unlock()

	if !current {
		close(resumeWait)

		return
	}

	stream := s.lifecycleStream()
	if closeErr == nil {
		proof := s.closedContainmentProof()
		if proof.vacant() {
			settleCtx, settleCancel := context.WithTimeout(context.Background(), closeTimeout)
			closeErr = stream.certify(settleCtx, proof.barrier)

			settleCancel()
		}
	}

	stream.fence()

	s.mu.Lock()
	s.runtimeResumeErr = closeErr

	if s.poisonCause == "" && closeErr != nil {
		s.poisonCause = errors.Join(cause, closeErr).Error()
	}

	if s.runtimeResumeWait == resumeWait {
		close(resumeWait)
	}
	s.mu.Unlock()
}
