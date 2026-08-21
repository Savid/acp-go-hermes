//nolint:goconst // User-facing native fallback titles remain explicit at their lifecycle boundaries.
package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

// Turn-lifecycle reply vocabulary shared with the prompt mapping.
const (
	valCancelled     = "cancelled"
	valReject        = "reject"
	valSessionClosed = "session closed"
	valAbsorbedTurn  = "absorbed by running turn"
)

type turnSettlementState uint8

const (
	turnSettlementIdle turnSettlementState = iota
	turnSettlementOpen
	// turnSettlementCapturing marks a turn whose settlement has begun:
	// a routed cancel still wins until the commit claim, but a close only waits
	// for the boundary instead of cancelling the turn.
	turnSettlementCapturing
	turnSettlementCommitting
	turnSettlementCancelled
)

type sessionForegroundKind uint8

const (
	foregroundPrompt sessionForegroundKind = iota + 1
	foregroundAutonomous
	foregroundReuse
)

type session struct {
	agent                 *Agent
	id                    acp.SessionId
	cwd                   string
	additionalDirectories []string
	mcpServers            []acp.McpServer
	idmap                 idmapRecord
	title                 string
	updatedAt             string
	providerID            string
	modelID               string
	env                   map[string]string
	extraPathDirs         []string
	rawMessages           rawMessageConfig

	client nativehermes.Server
	// operationJournal is non-nil only until the initial New/Fork store bundle
	// has been durably published and the session registered in this Agent.
	operationJournal *sessionOperationJournal

	turn                   chan struct{}
	lifecycleMu            sync.Mutex
	streamMu               sync.Mutex
	stream                 *sessionStream
	pumpMu                 sync.Mutex
	pumpCancel             context.CancelFunc
	pumpControlCancel      context.CancelFunc
	pumpDone               chan struct{}
	pumpClient             nativehermes.Server
	pumpIncarnation        uint64
	pumpRoutes             map[string]*pumpCycleRoute
	pumpForegroundSettle   *turnSettlement
	pumpDeferred           []pumpItem
	pumpRetry              chan struct{}
	pumpBarriers           chan pumpBarrierRequest
	pumpErr                error
	pumpStopping           bool
	newPumpNonce           func() (string, error)
	projectionGate         chan struct{}
	projectionOnce         sync.Once
	afterProjectionAck     func()
	afterHostControlWrite  func()
	afterPumpBarrierAccept func()
	cancelMu               sync.Mutex
	toolMu                 sync.Mutex
	rawEventMu             sync.Mutex
	mu                     sync.Mutex
	foreground             *turnSettlement
	foregroundKind         sessionForegroundKind
	foregroundToken        uint64
	promptReservation      *turnSettlement
	turnInFlight           bool
	cancel                 context.CancelFunc
	turnDone               <-chan struct{}
	cancelled              bool
	rawSeq                 int64
	seenParts              map[string]string
	pending                map[string]nativehermes.PermissionRequest
	questions              map[string]nativehermes.QuestionRequest
	processedPermission    map[string]struct{}
	processedQuestion      map[string]struct{}
	turnEpoch              uint64
	turnNonce              string
	turnSettlement         turnSettlementState
	reuseCancel            context.CancelCauseFunc
	reuseDone              chan struct{}
	activeMessageIDs       map[string]struct{}
	toolStates             map[string]hermesToolState
	mcpReloadComplete      bool
	runtimeNeedsResume     bool
	runtimeResumeWait      chan struct{}
	runtimeResumeErr       error
	fencedTurnEpoch        uint64
	turnFenceErr           error
	poisonCause            string
	committed              committedState
	settlement             *turnSettlement
	actionRequests         map[string]actionRequestOwnership
	lifecycleClosing       bool
	deleteNeedsAbort       bool
	foregroundText         []byte
	foregroundTruncated    bool
	closed                 bool
	containmentSettled     bool
	owedCloseCommit        *sessionStoreCommit
}

// reloadMCPForAuthorizedTurn closes the gap between native process startup and
// turn-scoped MCP authorization. The descriptor is stable across both phases,
// but an HTTP MCP server can intentionally expose only runtime_ready until the
// host arms the first turn. Hermes caches its startup discovery, so force one
// bounded native reload after the authorized Prompt has begun and before the
// model sees its tool surface.
func (s *session) reloadMCPForAuthorizedTurn(ctx context.Context) error {
	s.mu.Lock()
	if s.mcpReloadComplete || len(s.mcpServers) == 0 {
		s.mcpReloadComplete = true
		s.mu.Unlock()

		return nil
	}

	client := s.client
	nativeID := s.idmap.NativeSessionID
	s.mu.Unlock()

	reloadCtx, cancel := context.WithTimeout(ctx, mcpReloadTimeout)
	err := client.ReloadMCP(reloadCtx, nativeID)

	cancel()

	if err == nil {
		s.mu.Lock()
		s.mcpReloadComplete = true
		s.mu.Unlock()

		return nil
	}

	if errors.Is(err, context.Canceled) {
		// No native reload remains in flight after Call observes the turn
		// cancellation. Let a later authorized turn make the one real attempt.
		return err
	}

	return s.poisonWithError(ctx, "hermes_mcp_reload_failed", err.Error())
}

// committedState is the last durable generation this session published. A
// boundary that has already contained the native generation restates it rather
// than reading a process that is gone.
type committedState struct {
	terminal   SessionStoreTerminalState
	native     *stateSnapshotTerminal
	foreground *stateSnapshotForeground
	archives   map[string]archiveInfo
}

// nativeTerminal returns the committed native assistant identity, or the empty
// summary where no turn has completed one. The store shape requires the section,
// so it is never nil.
func (c committedState) nativeTerminal() *stateSnapshotTerminal {
	if c.native == nil {
		return &stateSnapshotTerminal{}
	}

	record := *c.native

	return &record
}

func (s *session) committedState() committedState {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.committed
}

type sessionSnapshot struct {
	id                    acp.SessionId
	cwd                   string
	additionalDirectories []string
	mcpServers            []acp.McpServer
	idmap                 idmapRecord
	title                 string
	updatedAt             string
	providerID            string
	modelID               string
	env                   map[string]string
	extraPathDirs         []string
	rawMessages           rawMessageConfig
	client                nativehermes.Server
}

func newSession(agent *Agent, id acp.SessionId, cwd string, additionalDirectories []string, mcpServers []acp.McpServer, native nativehermes.Session, client nativehermes.Server, meta sessionMeta, idmap idmapRecord) *session {
	title := native.Title
	if title == "" {
		title = "Hermes session"
	}

	updatedAt := time.Now().UTC().Format(time.RFC3339)
	if native.Time.Updated > 0 {
		updatedAt = time.UnixMilli(native.Time.Updated).UTC().Format(time.RFC3339)
	}

	providerID := native.Model.ProviderID

	modelID := firstNonEmpty(native.Model.ModelID, native.Model.ID)
	if meta.Model != "" {
		providerID, modelID = splitModelValue(meta.Model, providerID, modelID)
	}

	if idmap.SessionID == "" {
		idmap.SessionID = string(id)
	}

	if native.ID != "" {
		idmap.NativeSessionID = native.ID
	}

	if idmap.Format == "" {
		idmap.Format = SessionStoreFormat
	}

	now := time.Now().UnixMilli()
	if idmap.CreatedAtUnixMilli == 0 {
		idmap.CreatedAtUnixMilli = now
	}

	idmap.UpdatedAtUnixMilli = now

	session := &session{
		agent:                 agent,
		id:                    id,
		cwd:                   cwd,
		additionalDirectories: append([]string(nil), additionalDirectories...),
		mcpServers:            cloneMCPServers(mcpServers),
		idmap:                 idmap,
		title:                 title,
		updatedAt:             updatedAt,
		providerID:            providerID,
		modelID:               modelID,
		env:                   cloneStringMap(meta.Env),
		extraPathDirs:         append([]string(nil), meta.ExtraPathDirs...),
		rawMessages:           meta.RawMessages,
		client:                client,
		seenParts:             map[string]string{},
		pending:               map[string]nativehermes.PermissionRequest{},
		questions:             map[string]nativehermes.QuestionRequest{},
		processedPermission:   map[string]struct{}{},
		processedQuestion:     map[string]struct{}{},
		activeMessageIDs:      map[string]struct{}{},
		toolStates:            map[string]hermesToolState{},
		pumpRoutes:            map[string]*pumpCycleRoute{},
		newPumpNonce:          newSessionID,
		projectionGate:        make(chan struct{}),
	}
	if !agent.negotiatedLifecycle().Present() {
		session.releaseProjectionGate()
	}

	session.startPump(client)

	return session
}

func (s *session) releaseProjectionGate() {
	s.projectionOnce.Do(func() { close(s.projectionGate) })
}

func (s *session) resetProjectionGate() {
	s.pumpMu.Lock()
	s.projectionGate = make(chan struct{})
	s.projectionOnce = sync.Once{}
	s.pumpMu.Unlock()

	if !s.agent.negotiatedLifecycle().Present() {
		s.releaseProjectionGate()
	}
}

func (s *session) acquireTurn(ctx context.Context) (func(), *turnSettlement, error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	turn := s.turnQueue()
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	if err := s.poisonedErrorLocked(); err != nil {
		return nil, nil, err
	}

	if s.closed || s.lifecycleClosing {
		return nil, nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
	}

	if s.foreground != nil || s.promptReservation != nil || len(turn) >= cap(turn) {
		return nil, nil, sessionForegroundBackpressure()
	}

	turn <- struct{}{}

	settlement := s.reservePromptLocked()
	s.turnInFlight = true
	s.settlement = settlement

	return func() {
		s.mu.Lock()
		s.turnInFlight = false

		<-turn
		s.mu.Unlock()
	}, settlement, nil
}

// beginReuse admits one load/resume operation only while the session is idle.
// Its latch is session-owned so close can cancel and join replay without
// holding the Agent registry lock, and prompt admission cannot cross it.
func (s *session) beginReuse(ctx context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	if s.closed || s.lifecycleClosing {
		s.mu.Unlock()

		return nil, nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
	}

	if s.foreground != nil || s.promptReservation != nil {
		s.mu.Unlock()

		return nil, nil, sessionForegroundBackpressure()
	}

	reuseCtx, release := s.startReuseLocked(ctx)
	s.mu.Unlock()

	return reuseCtx, release, nil
}

func (s *session) beginInitialReuse(ctx context.Context) (context.Context, func()) {
	s.mu.Lock()
	reuseCtx, release := s.startReuseLocked(ctx)
	s.mu.Unlock()

	return reuseCtx, release
}

func (s *session) startReuseLocked(ctx context.Context) (context.Context, func()) {
	reuseCtx, cancel := context.WithCancelCause(ctx)
	settlement := s.claimForegroundLocked(foregroundReuse)
	done := settlement.done
	s.reuseCancel = cancel
	s.reuseDone = done

	var once sync.Once

	release := func() {
		once.Do(func() {
			s.mu.Lock()
			if s.reuseDone == done {
				s.reuseCancel = nil
				s.reuseDone = nil
			}
			s.mu.Unlock()
			cancel(nil)
			settlement.complete()
		})
	}

	return reuseCtx, release
}

// sessionPromptAbsorbed states a prompt Hermes folded into the turn it was
// already running. The text was accepted, so this is not backpressure and must
// not be retried: the running turn carries it, and that turn's output is
// projected as agent-origin work on this same session.
func sessionPromptAbsorbed() error {
	return acp.NewInvalidRequest(map[string]any{jsonFieldError: valAbsorbedTurn})
}

func sessionForegroundBackpressure() error {
	return acp.NewInvalidRequest(map[string]any{
		jsonFieldError: valBackpressure,
		keyLimit:       "session_foreground",
	})
}

func (s *session) reservePromptLocked() *turnSettlement {
	settlement := &turnSettlement{done: make(chan struct{})}
	settlement.release = func() {
		s.mu.Lock()
		if s.promptReservation == settlement {
			s.promptReservation = nil
		}

		if s.foreground == settlement {
			s.foreground = nil
			s.foregroundKind = 0
		}
		s.mu.Unlock()
	}
	settlement.notify = s.notifyPumpRetry
	s.promptReservation = settlement

	return settlement
}

func (s *session) promotePromptForeground() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	settlement := s.settlement
	if settlement == nil || s.promptReservation != settlement {
		return routeInvalid("prompt reservation is no longer current")
	}

	if s.closed || s.lifecycleClosing {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
	}

	if s.foreground != nil {
		return sessionForegroundBackpressure()
	}

	s.foregroundToken++
	s.foreground = settlement
	s.foregroundKind = foregroundPrompt
	s.activeMessageIDs = map[string]struct{}{}
	s.toolStates = map[string]hermesToolState{}
	s.actionRequests = map[string]actionRequestOwnership{}
	s.foregroundText = nil
	s.foregroundTruncated = false

	return nil
}

func (s *session) claimForegroundLocked(kind sessionForegroundKind) *turnSettlement {
	s.foregroundToken++
	token := s.foregroundToken

	settlement := &turnSettlement{done: make(chan struct{})}
	settlement.release = func() {
		s.mu.Lock()
		if s.foreground == settlement && s.foregroundToken == token {
			s.foreground = nil
			s.foregroundKind = 0
		}
		s.mu.Unlock()
	}
	settlement.notify = s.notifyPumpRetry

	s.foreground = settlement
	s.foregroundKind = kind

	return settlement
}

func (s *session) turnQueue() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.turn == nil {
		// Hermes serializes prompts per session; admission capacity is fixed at 1.
		s.turn = make(chan struct{}, sessionTurnCapacity)
	}

	return s.turn
}

func (s *session) beginTurn(ctx context.Context, turnNonce string) context.Context {
	s.toolMu.Lock()
	defer s.toolMu.Unlock()

	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.beginTurnLocked(ctx, turnNonce)
}

// beginTurnLocked starts one turn while toolMu and cancelMu hold the runtime
// generation stable. Keeping runtime installation and turn admission under the
// same lock prevents Close from landing between a lazy resume and its first
// routed operation.
func (s *session) beginTurnLocked(ctx context.Context, turnNonce string) context.Context {
	s.mu.Lock()

	turnCtx, cancel := context.WithCancel(ctx)
	turnCtx = withTurnRoute(turnCtx, turnNonce)
	cancelled := s.turnSettlement == turnSettlementCancelled
	s.cancel = cancel
	s.turnDone = turnCtx.Done()
	s.cancelled = cancelled
	s.turnEpoch++

	s.turnNonce = turnNonce
	if !cancelled {
		s.turnSettlement = turnSettlementOpen
	}

	s.activeMessageIDs = map[string]struct{}{}
	s.toolStates = map[string]hermesToolState{}
	s.actionRequests = map[string]actionRequestOwnership{}
	s.foregroundText = nil
	s.foregroundTruncated = false
	s.mu.Unlock()

	if cancelled {
		cancel()
	}

	return turnCtx
}

func (s *session) preparePromptTurn(ctx context.Context, turnNonce string) (context.Context, uint64, error) {
	s.toolMu.Lock()
	defer s.toolMu.Unlock()

	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	if err := s.resumeRuntimeForTurnLocked(ctx); err != nil {
		return nil, 0, err
	}

	turnCtx := s.beginTurnLocked(ctx, turnNonce)

	return turnCtx, s.currentTurnEpoch(), nil
}

func (s *session) currentTurnEpoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnEpoch
}

func (s *session) needsRuntimeResume() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.runtimeNeedsResume
}

func (s *session) finishTurn() {
	s.toolMu.Lock()
	defer s.toolMu.Unlock()

	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.turnDone = nil
	s.turnInFlight = false
	s.cancelled = false
	s.turnNonce = ""
	s.turnSettlement = turnSettlementIdle
	s.updatedAt = time.Now().UTC().Format(time.RFC3339)
	s.pending = map[string]nativehermes.PermissionRequest{}
	s.questions = map[string]nativehermes.QuestionRequest{}
	s.activeMessageIDs = map[string]struct{}{}
	s.toolStates = map[string]hermesToolState{}
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (s *session) cancelTurn() {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.cancelTurnLocked(s.client, true)
}

func (s *session) cancelTurnLocked(client nativehermes.Server, markCancelled bool) {
	s.mu.Lock()

	cancel := s.cancel
	if cancel != nil && markCancelled {
		s.cancelled = true
	}

	pending := make([]nativehermes.PermissionRequest, 0, len(s.pending))
	for id := range s.pending {
		pending = append(pending, s.pending[id])
	}

	s.pending = map[string]nativehermes.PermissionRequest{}

	questions := make([]nativehermes.QuestionRequest, 0, len(s.questions))
	for id := range s.questions {
		questions = append(questions, s.questions[id])
	}

	s.questions = map[string]nativehermes.QuestionRequest{}
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	ctx, done := context.WithTimeout(context.Background(), closeTimeout)
	defer done()

	if client == nil {
		return
	}

	for i := range pending {
		_ = client.ReplyPermission(ctx, pending[i], valReject, valCancelled)
	}

	for index := range questions {
		_ = client.RejectQuestion(ctx, questions[index])
	}
}

// fenceTurnLocked is the single destructive turn fence. cancelMu must be held.
// It memoizes by epoch so Cancel, the prompt context, and the deadline can all
// race without issuing duplicate shutdowns. A normal cancellation/timeout is
// returned only after Close completes the selected native containment boundary
// and releases its isolated root.
func (s *session) fenceTurnLocked(ctx context.Context, epoch uint64, markCancelled bool) error {
	if epoch == 0 {
		return nil
	}

	s.mu.Lock()
	if s.fencedTurnEpoch == epoch {
		if markCancelled {
			s.cancelled = true
		}

		err := s.turnFenceErr
		s.mu.Unlock()

		return err
	}

	currentEpoch := s.turnEpoch
	client := s.client
	nativeID := s.idmap.NativeSessionID
	s.mu.Unlock()

	if currentEpoch != epoch {
		return routeInvalid("stale turn epoch")
	}

	s.mu.Lock()
	s.fencedTurnEpoch = epoch
	s.mu.Unlock()

	s.beginPumpShutdown()
	s.cancelTurnLocked(client, markCancelled)

	if client == nil {
		err := s.poisonWithError(ctx, "hermes_runtime_fence_failed", "Hermes runtime is unavailable")

		s.mu.Lock()
		s.turnFenceErr = err
		s.mu.Unlock()

		return err
	}

	abortCtx, abortCancel := context.WithTimeout(context.Background(), closeTimeout)
	_ = client.Abort(abortCtx, nativeID)

	abortCancel()

	closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
	closeErr := client.Close(closeCtx)

	closeCancel()

	pumpCtx, pumpCancel := context.WithTimeout(context.Background(), closeTimeout)
	pumpErr := s.stopPumpContext(pumpCtx)

	pumpCancel()

	closeErr = errors.Join(closeErr, pumpErr)
	if closeErr != nil {
		name := "hermes_runtime_fence_failed"
		if errors.Is(closeErr, nativehermes.ErrProcessContainmentIncomplete) {
			name = "hermes_process_containment_incomplete"
		}

		err := errors.Join(s.poisonWithError(ctx, name, closeErr.Error()), closeErr)

		s.mu.Lock()
		s.turnFenceErr = err
		s.mu.Unlock()

		return err
	}

	s.mu.Lock()
	s.runtimeNeedsResume = !s.closed
	s.pending = map[string]nativehermes.PermissionRequest{}
	s.questions = map[string]nativehermes.QuestionRequest{}
	s.processedPermission = map[string]struct{}{}
	s.processedQuestion = map[string]struct{}{}
	s.activeMessageIDs = map[string]struct{}{}
	s.toolStates = map[string]hermesToolState{}
	s.mcpReloadComplete = false
	s.turnFenceErr = nil
	s.mu.Unlock()

	return nil
}

func (s *session) fenceTurn(ctx context.Context, epoch uint64, markCancelled bool) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.fenceTurnLocked(ctx, epoch, markCancelled)
}

func (s *session) wasCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.cancelled
}

func (s *session) ensureNotPoisoned() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.poisonedErrorLocked()
}

func (s *session) poisonedErrorLocked() error {
	if s.poisonCause == "" {
		return nil
	}

	return acp.NewInvalidRequest(map[string]any{
		jsonFieldError: "session_poisoned",
		"cause":        s.poisonCause,
	})
}

func (s *session) poisonNativeSessionDrift(ctx context.Context, field string, actual string) error {
	expected := s.idmap.NativeSessionID
	cause := fmt.Sprintf("%s native session id drift: expected %q, got %q", field, expected, actual)

	return s.poison(ctx, cause)
}

func (s *session) poison(ctx context.Context, cause string) error {
	return s.poisonWithError(ctx, "hermes_native_session_id_drift", cause)
}

func (s *session) poisonWithError(ctx context.Context, errorName string, cause string) error {
	err := acp.NewInternalError(map[string]any{
		jsonFieldError: errorName,
		"cause":        cause,
	})

	s.mu.Lock()
	if s.poisonCause != "" {
		existing := s.poisonedErrorLocked()
		s.mu.Unlock()

		return existing
	}

	s.poisonCause = cause
	s.mu.Unlock()

	return err
}

func (s *session) poisonMissingLiveSessionMapping(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	var missing nativehermes.MissingLiveSessionMappingError
	if errors.As(err, &missing) {
		return s.poisonWithError(ctx, "hermes_missing_live_session_mapping", missing.Error())
	}

	return err
}

func (s *session) markActiveMessageID(messageID string) {
	if messageID == "" {
		return
	}

	s.mu.Lock()
	if s.activeMessageIDs == nil {
		s.activeMessageIDs = map[string]struct{}{}
	}

	s.activeMessageIDs[messageID] = struct{}{}
	s.mu.Unlock()
}

func (s *session) markMessageCompleted(messageID string) {
	if messageID == "" {
		return
	}

	s.mu.Lock()
	delete(s.activeMessageIDs, messageID)
	s.mu.Unlock()
}

func (s *session) addPendingPermission(req nativehermes.PermissionRequest) {
	s.mu.Lock()
	if s.pending == nil {
		s.pending = map[string]nativehermes.PermissionRequest{}
	}

	s.pending[req.ID] = req
	s.mu.Unlock()
}

func (s *session) claimPermissionRequest(id string) bool {
	if id == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.processedPermission == nil {
		s.processedPermission = map[string]struct{}{}
	}

	if _, ok := s.processedPermission[id]; ok {
		return false
	}

	s.processedPermission[id] = struct{}{}

	return true
}

func (s *session) takePendingPermission(id string) (nativehermes.PermissionRequest, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	req, ok := s.pending[id]
	if ok {
		delete(s.pending, id)
	}

	return req, ok, s.cancelled
}

func (s *session) pendingPermission(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.pending[id]

	return ok
}

func (s *session) addPendingQuestion(req nativehermes.QuestionRequest) {
	s.mu.Lock()
	if s.questions == nil {
		s.questions = map[string]nativehermes.QuestionRequest{}
	}

	s.questions[req.ID] = req
	s.mu.Unlock()
}

func (s *session) claimQuestionRequest(id string) bool {
	if id == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.processedQuestion == nil {
		s.processedQuestion = map[string]struct{}{}
	}

	if _, ok := s.processedQuestion[id]; ok {
		return false
	}

	s.processedQuestion[id] = struct{}{}

	return true
}

func (s *session) takePendingQuestion(id string) (nativehermes.QuestionRequest, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	req, ok := s.questions[id]
	if ok {
		delete(s.questions, id)
	}

	return req, ok, s.cancelled
}

func (s *session) pendingQuestion(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.questions[id]

	return ok
}

func (s *session) snapshot() sessionSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionSnapshot{
		id:                    s.id,
		cwd:                   s.cwd,
		additionalDirectories: append([]string(nil), s.additionalDirectories...),
		mcpServers:            cloneMCPServers(s.mcpServers),
		idmap:                 s.idmap,
		title:                 s.title,
		updatedAt:             s.updatedAt,
		providerID:            s.providerID,
		modelID:               s.modelID,
		env:                   cloneStringMap(s.env),
		extraPathDirs:         append([]string(nil), s.extraPathDirs...),
		rawMessages:           s.rawMessages,
		client:                s.client,
	}
}

func (s *session) setModel(value string) {
	provider, model := splitModelValue(value, "", "")

	s.mu.Lock()
	s.providerID = provider
	s.modelID = model
	s.mu.Unlock()
}

func (s *session) currentModel() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return modelSelectionValue(s.providerID, s.modelID)
}

func (s *session) modelSelector() *nativehermes.ModelSelector {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.providerID == "" || s.modelID == "" {
		return nil
	}

	return &nativehermes.ModelSelector{ProviderID: s.providerID, ModelID: s.modelID}
}

func (s *session) markPart(part nativehermes.Part) bool {
	if part.ID == "" {
		return true
	}

	encoded := string(part.Raw)

	s.mu.Lock()
	defer s.mu.Unlock()

	if seen, ok := s.seenParts[part.ID]; ok && seen == encoded {
		return false
	}

	s.seenParts[part.ID] = encoded

	return true
}

func (s *session) Close(ctx context.Context) error {
	return s.closeAfterTurns(ctx, false)
}

func (s *session) DeleteNativeAndClose(ctx context.Context) error {
	return s.closeAfterTurns(ctx, true)
}

func (s *session) closeAfterTurns(ctx context.Context, deleteNative bool) error {
	if deleteNative {
		s.prepareDelete()
	} else {
		s.prepareClose()
	}

	waitErr := s.awaitSettlement(ctx)

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	if deleteNative {
		return errors.Join(waitErr, s.closeLocked(ctx, true))
	}

	return errors.Join(waitErr, s.settleClosedSession(ctx))
}

func (s *session) prepareClose() {
	s.prepareCloseAdmission(true)
}

func (s *session) prepareDelete() {
	s.closeLifecycleAdmission()
	s.beginPumpShutdown()

	s.mu.Lock()
	s.deleteNeedsAbort = s.foreground != nil || s.promptReservation != nil || s.cancel != nil
	s.mu.Unlock()
	s.pumpMu.Lock()
	pumpActive := len(s.pumpRoutes) != 0 || len(s.pumpDeferred) != 0
	s.pumpMu.Unlock()

	if pumpActive {
		s.mu.Lock()
		s.deleteNeedsAbort = true
		s.mu.Unlock()
	}

	s.cancelTurn()
	s.resolvePumpRoutes(errPromptCancelled, false)
	s.resolveDeferredPumpItems(nil)
	// Keep the source reader acknowledgement-draining so gateway events cannot
	// block the native delete response. closeLocked joins it after transport
	// close.
	s.releaseProjectionGate()
}

func (s *session) prepareCloseAdmission(cancelSource bool) {
	s.closeLifecycleAdmission()
	// Establish the projection stop before cancelling callbacks or draining
	// native controls. A permission/elicitation woken by close must observe a
	// stale pump route and cannot publish a resume or lifecycle fence of its own.
	s.beginPumpShutdown()
	// This cancels an admitted prompt when present and rejects every pending
	// permission/elicitation control for either prompt or autonomous work.
	s.cancelTurn()

	if cancelSource {
		s.cancelPump()
	}

	s.releaseProjectionGate()
}

// closeLocked runs the native containment boundary once it completes. A
// boundary that did not complete is not spent: the tree it owned may still be
// running, so the latch that short-circuits a repeat close is the completion,
// never the attempt, and the next close re-runs every step. The lifetime latch
// is separate and set on the first attempt, because the session object's
// teardown has begun either way and no prompt or resume may cross it.
func (s *session) closeLocked(ctx context.Context, deleteNative bool) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	if s.containmentSettled {
		s.mu.Unlock()

		return nil
	}

	s.closed = true
	client := s.client
	nativeID := s.idmap.NativeSessionID
	runtimeUnavailable := s.runtimeNeedsResume
	deleteNeedsAbort := s.deleteNeedsAbort
	s.mu.Unlock()

	// Pending provider-auth flows are cancelled after pending elicitation is
	// resolved and before the native interrupt, so a flow is never abandoned to
	// a process already being torn down.
	if s.agent != nil && s.agent.providerAuth != nil {
		s.agent.providerAuth.closeSession(ctx, s.id)
	}

	s.beginPumpShutdown()
	s.cancelTurnLocked(client, true)

	if client == nil || runtimeUnavailable {
		var resumeErr error

		if runtimeUnavailable {
			s.mu.Lock()
			resumeWait := s.runtimeResumeWait
			s.mu.Unlock()

			if resumeWait != nil {
				select {
				case <-resumeWait:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			s.mu.Lock()
			resumeErr = s.runtimeResumeErr
			s.mu.Unlock()
		}

		pumpErr := s.stopPumpContext(ctx)
		if err := errors.Join(resumeErr, pumpErr); err != nil {
			return err
		}

		s.markContainmentSettled()

		return nil
	}

	if nativeID != "" && (!deleteNative || deleteNeedsAbort) {
		abortCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		_ = client.Abort(abortCtx, nativeID)

		cancel()
	}

	var deleteErr error

	if deleteNative && nativeID != "" {
		deleteCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)

		deleteErr = client.DeleteSession(deleteCtx, nativeID)
		if deleteErr != nil && s.agent != nil && s.agent.log != nil {
			s.agent.log.DebugContext(deleteCtx, "delete native Hermes session failed",
				slog.String("classification", "native_delete_failed"))
		}

		cancel()
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
	err := client.Close(closeCtx)

	closeCancel()

	pumpCtx, pumpCancel := context.WithTimeout(context.Background(), closeTimeout)
	pumpErr := s.stopPumpContext(pumpCtx)

	pumpCancel()

	joined := errors.Join(deleteErr, err, pumpErr)
	if joined == nil {
		s.markContainmentSettled()
	}

	return joined
}

// markContainmentSettled records that the native containment boundary completed,
// which is what makes a later close a no-op rather than a retry.
func (s *session) markContainmentSettled() {
	s.mu.Lock()
	s.containmentSettled = true
	s.mu.Unlock()
}

func (s *session) info() acp.SessionInfo {
	snapshot := s.snapshot()
	title := snapshot.title
	updatedAt := snapshot.updatedAt

	return acp.SessionInfo{
		SessionId:             snapshot.id,
		Cwd:                   snapshot.cwd,
		AdditionalDirectories: snapshot.additionalDirectories,
		Title:                 &title,
		UpdatedAt:             &updatedAt,
		Meta:                  sessionInfoMeta(snapshot),
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func splitModelValue(value string, fallbackProvider string, fallbackModel string) (string, string) {
	if value == "" {
		return fallbackProvider, fallbackModel
	}

	provider, model, ok := strings.Cut(value, "/")
	if !ok || provider == "" || model == "" {
		return fallbackProvider, value
	}

	return provider, model
}

func joinModelValue(provider string, model string) string {
	if provider == "" {
		return model
	}

	if model == "" {
		return provider
	}

	return provider + "/" + model
}

// committedStateFromSnapshot reads the durable generation a hydrated snapshot
// holds. It is what a later boundary restates when the native generation it
// settles has already been contained.
func committedStateFromSnapshot(snapshot stateSnapshot) committedState {
	state := committedState{
		terminal: publicTerminalState(snapshot.Terminal, foregroundOf(snapshot.Wrapper)),
		native:   snapshot.Terminal,
	}
	if snapshot.Wrapper != nil {
		state.foreground = snapshot.Wrapper.Foreground
	}

	state.archives = cloneArchiveInfo(snapshot.Archives)

	return state
}

func foregroundOf(wrapper *stateSnapshotWrapper) *stateSnapshotForeground {
	if wrapper == nil {
		return nil
	}

	return wrapper.Foreground
}

// lifecycleForegroundPrefixBytes bounds the streamed foreground prefix one
// commit records. The journal entry is a durability record rather than a
// transcript, so the prefix is retained up to this bound and truncated beyond it
// and a long turn cannot grow the committed generation without limit.
const lifecycleForegroundPrefixBytes = 64 * 1024

// recordForegroundPrefix accumulates the assistant text this turn actually
// streamed to the host. An incarnation-ending boundary destroys the native
// generation before its own commit, so this is the only prefix of a failed or
// cancelled turn the store can hold truthfully.
func (s *session) recordForegroundPrefix(text string) {
	if text == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.foregroundTruncated {
		return
	}

	room := lifecycleForegroundPrefixBytes - len(s.foregroundText)
	if room <= 0 {
		s.foregroundTruncated = true

		return
	}

	if len(text) > room {
		s.foregroundTruncated = true

		end := room
		for end > 0 && !utf8.ValidString(text[:end]) {
			end--
		}

		text = text[:end]
	}

	s.foregroundText = append(s.foregroundText, text...)
}

func (s *session) foregroundPrefix() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return string(s.foregroundText)
}
