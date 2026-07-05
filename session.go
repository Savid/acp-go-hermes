package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
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
	mode                  string
	env                   map[string]string
	rawMessages           rawMessageConfig

	client hermesClient

	turn                chan struct{}
	mu                  sync.Mutex
	turnInFlight        bool
	cancel              context.CancelFunc
	turnDone            <-chan struct{}
	cancelled           bool
	rawSeq              int64
	seenParts           map[string]string
	pending             map[string]permissionRequest
	questions           map[string]questionRequest
	processedPermission map[string]struct{}
	processedQuestion   map[string]struct{}
	turnEpoch           uint64
	activeMessageIDs    map[string]struct{}
	failedStreamEpochs  map[uint64]struct{}
	failedMessageIDs    map[string]struct{}
	suppressNextBacklog bool
	poisonCause         string
	closed              bool
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
	mode                  string
	env                   map[string]string
	rawMessages           rawMessageConfig
	client                hermesClient
}

func newSession(agent *Agent, id acp.SessionId, cwd string, additionalDirectories []string, mcpServers []acp.McpServer, native nativeSession, client hermesClient, meta sessionMeta, idmap idmapRecord) *session {
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

	return &session{
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
		mode:                  firstNonEmpty(native.Agent, "default"),
		env:                   cloneStringMap(meta.Env),
		rawMessages:           meta.RawMessages,
		client:                client,
		seenParts:             map[string]string{},
		pending:               map[string]permissionRequest{},
		questions:             map[string]questionRequest{},
		processedPermission:   map[string]struct{}{},
		processedQuestion:     map[string]struct{}{},
		activeMessageIDs:      map[string]struct{}{},
		failedStreamEpochs:    map[uint64]struct{}{},
		failedMessageIDs:      map[string]struct{}{},
	}
}

func (s *session) acquireTurn(ctx context.Context) (func(), error) {
	turn := s.turnQueue()
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if err := s.poisonedErrorLocked(); err != nil {
		return nil, err
	}
	if len(turn) >= cap(turn) {
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: "backpressure", "limit": "session_prompt"})
	}
	turn <- struct{}{}
	s.turnInFlight = true
	return func() {
		s.mu.Lock()
		s.turnInFlight = false
		<-turn
		s.mu.Unlock()
	}, nil
}

func (s *session) turnQueue() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turn == nil {
		limit := defaultMaxConcurrentPrompts
		if s.agent != nil && s.agent.options.ConcurrencyLimits.MaxConcurrentPrompts > 0 {
			limit = s.agent.options.ConcurrencyLimits.MaxConcurrentPrompts
		}
		s.turn = make(chan struct{}, limit)
	}
	return s.turn
}

func (s *session) beginTurn(ctx context.Context) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	turnCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.turnDone = turnCtx.Done()
	s.cancelled = false
	s.turnEpoch++
	s.activeMessageIDs = map[string]struct{}{}
	return turnCtx
}

func (s *session) finishTurn() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.turnDone = nil
	s.turnInFlight = false
	s.cancelled = false
	s.updatedAt = time.Now().UTC().Format(time.RFC3339)
	s.pending = map[string]permissionRequest{}
	s.questions = map[string]questionRequest{}
	s.activeMessageIDs = map[string]struct{}{}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *session) cancelTurn() {
	s.mu.Lock()
	cancel := s.cancel
	if cancel != nil {
		s.cancelled = true
	}
	pending := make([]permissionRequest, 0, len(s.pending))
	for _, req := range s.pending {
		pending = append(pending, req)
	}
	s.pending = map[string]permissionRequest{}
	questions := make([]questionRequest, 0, len(s.questions))
	for _, req := range s.questions {
		questions = append(questions, req)
	}
	s.questions = map[string]questionRequest{}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	ctx, done := context.WithTimeout(context.Background(), closeTimeout)
	defer done()
	for _, req := range pending {
		_ = s.poisonMissingLiveSessionMapping(ctx, s.client.ReplyPermission(ctx, req, "reject", "cancelled"))
	}
	for _, req := range questions {
		_ = s.poisonMissingLiveSessionMapping(ctx, s.client.RejectQuestion(ctx, req))
	}
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
	var missing missingLiveSessionMappingError
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

func (s *session) markStreamFailed(epoch uint64) {
	s.mu.Lock()
	if s.failedMessageIDs == nil {
		s.failedMessageIDs = map[string]struct{}{}
	}
	for messageID := range s.activeMessageIDs {
		s.failedMessageIDs[messageID] = struct{}{}
	}
	if epoch > 0 {
		if s.failedStreamEpochs == nil {
			s.failedStreamEpochs = map[uint64]struct{}{}
		}
		s.failedStreamEpochs[epoch] = struct{}{}
	}
	s.suppressNextBacklog = true
	s.mu.Unlock()
}

func (s *session) shouldSuppressEvent(event hermesEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.StreamEpoch > 0 {
		if _, ok := s.failedStreamEpochs[event.StreamEpoch]; ok {
			return true
		}
	}
	if part, ok := eventPart(event.Properties); ok && part.MessageID != "" {
		_, ok := s.failedMessageIDs[part.MessageID]
		return ok
	}
	return false
}

func (s *session) suppressBacklog() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.suppressNextBacklog
}

func (s *session) clearSuppressBacklog() {
	s.mu.Lock()
	s.suppressNextBacklog = false
	s.mu.Unlock()
}

func (s *session) addPendingPermission(req permissionRequest) {
	s.mu.Lock()
	if s.pending == nil {
		s.pending = map[string]permissionRequest{}
	}
	s.pending[req.ID] = req
	s.mu.Unlock()
}

func (s *session) claimPermissionRequest(id string) bool {
	if id == "" {
		return true
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

func (s *session) takePendingPermission(id string) (permissionRequest, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.pending[id]
	if ok {
		delete(s.pending, id)
	}
	return req, ok, s.cancelled
}

func (s *session) addPendingQuestion(req questionRequest) {
	s.mu.Lock()
	if s.questions == nil {
		s.questions = map[string]questionRequest{}
	}
	s.questions[req.ID] = req
	s.mu.Unlock()
}

func (s *session) claimQuestionRequest(id string) bool {
	if id == "" {
		return true
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

func (s *session) takePendingQuestion(id string) (questionRequest, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.questions[id]
	if ok {
		delete(s.questions, id)
	}
	return req, ok, s.cancelled
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
		mode:                  s.mode,
		env:                   cloneStringMap(s.env),
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

func (s *session) modelSelector() *hermesModelSelector {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.providerID == "" || s.modelID == "" {
		return nil
	}
	return &hermesModelSelector{ProviderID: s.providerID, ModelID: s.modelID}
}

func (s *session) currentMode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

func (s *session) nextRawEventSequence() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rawSeq++
	return s.rawSeq
}

func (s *session) markPart(part nativePart) bool {
	if part.ID == "" {
		return true
	}
	encoded := string(part.Raw)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seenParts[part.ID] == encoded {
		return false
	}
	s.seenParts[part.ID] = encoded
	return true
}

func (s *session) Close(_ context.Context) error {
	s.cancelTurn()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	client := s.client
	nativeID := s.idmap.NativeSessionID
	s.mu.Unlock()
	var err error
	if client != nil && nativeID != "" {
		abortCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		_ = client.Abort(abortCtx, nativeID)
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
		err = errors.Join(err, client.Close(closeCtx))
		closeCancel()
	}
	return err
}

func (s *session) DeleteNativeAndClose(ctx context.Context) error {
	s.cancelTurn()
	s.mu.Lock()
	client := s.client
	nativeID := s.idmap.NativeSessionID
	s.mu.Unlock()

	var err error
	if client != nil && nativeID != "" {
		deleteCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		if deleteErr := client.DeleteSession(deleteCtx, nativeID); deleteErr != nil && s.agent != nil && s.agent.log != nil {
			s.agent.log.DebugContext(deleteCtx, "delete native Hermes session failed", slog.String("error", deleteErr.Error()))
		}
		cancel()
	}

	return errors.Join(err, s.Close(ctx))
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
