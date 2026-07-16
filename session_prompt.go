//nolint:tagliatelle // Hermes native event payloads use sessionID wire names.
package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/observer"
)

// Native Hermes wire vocabulary used by the prompt, event, permission, and
// elicitation mapping.
const (
	valAssistant  = "assistant"
	valText       = "text"
	valReasoning  = "reasoning"
	valAlways     = "always"
	valUser       = "user"
	valStepFinish = "step-finish"
	valTool       = "tool"
	valOnce       = "once"
	valQuestion1  = "question_1"
	valString     = "string"
	valLength     = "length"
	valPending    = "pending"
	valCompleted  = "completed"
	valRead       = "read"
	valEdit       = "edit"
	valDelete     = "delete"
	valHigh       = "high"
	valLow        = "low"
	valSuccess    = "success"
	valFile       = "file"

	defaultMimeType = "application/octet-stream"

	keyType      = "type"
	keyTitle     = "title"
	keyMime      = "mime"
	keyFilename  = "filename"
	keyQuestion  = "question"
	keyRequest   = "request"
	keyMessageID = "messageId"

	jsonFieldCause        = "cause"
	jsonFieldStatusCode   = "statusCode"
	jsonFieldProviderCode = "providerCode"
	valHermesTurnFailed   = "hermes_turn_failed"
	valHermesServeSource  = "hermes-serve"

	msgHermesNeedsInput = "Hermes needs input"

	acpFieldPrompt = "prompt"

	evtApprovalRequest    = "approval.request"
	evtClarifyRequest     = "clarify.request"
	evtMessagePartUpdated = "message.part.updated"
	evtMessagePartCreated = "message.part.created"
	evtServerConnected    = "server.connected"
)

var errPromptCancelled = errors.New("prompt cancelled")

// mapTurnFailure maps a classified native turn failure to the uniform
// hermes_turn_failed JSON-RPC error. hermes advertises no auth methods, so every
// turn failure is an Internal error (-32603). An unclassified error is surfaced
// as a transport failure carrying its real cause, never a fixed placeholder.
func mapTurnFailure(err error) error {
	data := map[string]any{jsonFieldError: valHermesTurnFailed}

	var failure *nativehermes.TurnFailureError
	if errors.As(err, &failure) {
		data[jsonFieldCause] = string(failure.Cause())
		data[jsonFieldMessage] = firstNonEmpty(failure.Message(), err.Error())

		if failure.StatusCode() != 0 {
			data[jsonFieldStatusCode] = failure.StatusCode()
		}

		if failure.ProviderCode() != "" {
			data[jsonFieldProviderCode] = failure.ProviderCode()
		}
	} else {
		data[jsonFieldCause] = string(nativehermes.CauseTransport)
		data[jsonFieldMessage] = err.Error()
	}

	return acp.NewInternalError(data)
}

// reconcileConnected drives the reconnect reconciliation for a turn: pending
// permissions first, then pending questions.
func (s *session) reconcileConnected(ctx context.Context) error {
	if err := s.reloadMCPForAuthorizedTurn(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return errPromptCancelled
		}

		return err
	}

	if err := s.reconcilePermissions(ctx); err != nil {
		return err
	}

	return s.reconcileQuestions(ctx)
}

// failedTurnResult maps a native SendMessage error to the turn outcome. The
// cancel guard runs before all failure mapping: an error observed while the turn
// is cancelled stays cancelled; otherwise the native turn is aborted and the
// error becomes the uniform hermes_turn_failed error.
func (s *session) failedTurnResult(turnCtx context.Context, sendErr error, messageID *string, abortTurn func()) (acp.PromptResponse, error) {
	cancelled := s.wasCancelled() || turnCtx.Err() != nil
	if cancelled {
		return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: messageID}, nil
	}

	abortTurn()

	if nativehermes.IsGatewayDisconnect(sendErr) {
		s.markStreamFailed(0)
	}

	return acp.PromptResponse{}, mapTurnFailure(sendErr)
}

func (a *Agent) Prompt(ctx context.Context, params acp.PromptRequest) (resp acp.PromptResponse, err error) {
	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	_, err = parseInboundTurnRoute(params.Meta)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	ctx, finish := a.observe.StartPrompt(ctx, params.Meta, session.currentModel())
	defer func() { finish(promptResultForObserver(resp, err, session.currentModel())) }()

	resp, err = session.Prompt(ctx, params)

	return resp, err
}

func promptResultForObserver(resp acp.PromptResponse, err error, model string) observer.PromptResult {
	result := observer.PromptResult{
		Err:        err,
		Model:      model,
		StopReason: string(resp.StopReason),
	}
	if resp.Usage == nil {
		return result
	}

	result.InputTokens = resp.Usage.InputTokens
	result.OutputTokens = resp.Usage.OutputTokens
	result.TotalTokens = resp.Usage.TotalTokens

	if resp.Usage.CachedReadTokens != nil {
		result.CachedReadTokens = *resp.Usage.CachedReadTokens
	}

	if resp.Usage.CachedWriteTokens != nil {
		result.CachedWriteTokens = *resp.Usage.CachedWriteTokens
	}

	if resp.Usage.ThoughtTokens != nil {
		result.ThoughtTokens = *resp.Usage.ThoughtTokens
	}

	return result
}

func (a *Agent) Cancel(ctx context.Context, params acp.CancelNotification) error {
	session, err := a.session(params.SessionId)
	if err != nil {
		return err
	}

	if err := session.ensureNotPoisoned(); err != nil {
		return err
	}

	return session.cancelRouted(params.Meta)
}

// cancelRouted validates the active turn and keeps its native abort fenced
// from turn completion and admission of the next turn.
func (s *session) cancelRouted(meta map[string]any) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	activeNonce := s.turnNonce
	active := s.turnInFlight && activeNonce != ""
	s.mu.Unlock()

	if active {
		route, err := parseInboundTurnRoute(meta)
		if err != nil {
			return err
		}

		if route.turnNonce != activeNonce {
			return routeInvalid("stale route turnNonce")
		}
	}

	s.cancelTurn()

	cancelCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	return s.client.Abort(cancelCtx, s.idmap.NativeSessionID)
}

func (s *session) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	route, err := parseInboundTurnRoute(params.Meta)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	if poisonErr := s.ensureNotPoisoned(); poisonErr != nil {
		return acp.PromptResponse{}, poisonErr
	}

	release, err := s.acquireTurn(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()

	parts, err := promptToHermesParts(params.Prompt)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	req := nativehermes.MessageRequest{
		Parts: parts,
		Model: s.modelSelector(),
		Agent: s.currentMode(),
	}
	if params.MessageId != nil {
		req.MessageID = *params.MessageId
	}

	if err := s.drainClientBacklog(ctx); err != nil {
		if errors.Is(err, errPromptCancelled) {
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}, nil
		}

		return acp.PromptResponse{}, err
	}

	turnCtx := s.beginTurn(ctx, route.turnNonce)

	turnActive := true
	defer func() {
		if turnActive {
			s.finishTurn()
		}
	}()

	var abortOnce sync.Once

	abortTurn := func() {
		abortOnce.Do(func() {
			abortCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			_ = s.client.Abort(abortCtx, s.idmap.NativeSessionID)

			cancel()
		})
	}

	failTurn := func(err error) (acp.PromptResponse, error) {
		if errors.Is(err, errPromptCancelled) {
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}, nil
		}

		abortTurn()

		return acp.PromptResponse{}, err
	}
	if err := s.reconcileConnected(turnCtx); err != nil {
		return failTurn(err)
	}

	type result struct {
		message nativehermes.NativeMessage
		err     error
	}

	done := make(chan result, 1)

	go func() {
		defer recoverAgentGoroutine(turnCtx, agentLogger(s.agent), "Hermes turn send")

		message, err := s.client.SendMessage(turnCtx, s.idmap.NativeSessionID, req)
		done <- result{message: message, err: err}
	}()

	turnTimeout := s.agent.turnTimeout()

	var timeout <-chan time.Time

	if turnTimeout > 0 {
		timer := time.NewTimer(turnTimeout)
		defer timer.Stop()

		timeout = timer.C
	}

	var (
		final nativehermes.NativeMessage
		usage *acp.Usage
	)

	for {
		select {
		case event := <-s.client.Events():
			if event.Type == evtServerConnected {
				if err := s.reconcileConnected(turnCtx); err != nil {
					return failTurn(err)
				}

				continue
			}

			if err := s.handleEvent(turnCtx, event); err != nil {
				return failTurn(err)
			}
		case err := <-s.client.EventErrors():
			// Cancel guard runs before all failure mapping: a stream error
			// observed while the turn is cancelled stays cancelled.
			cancelled := s.wasCancelled() || turnCtx.Err() != nil
			s.markStreamFailed(nativehermes.StreamErrorEpoch(err))
			s.cancelTurn()
			abortTurn()

			if cancelled {
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}, nil
			}

			return acp.PromptResponse{}, mapTurnFailure(nativehermes.NewTurnFailure(nativehermes.CauseTransport, err.Error()))
		case result := <-done:
			if result.err != nil {
				return s.failedTurnResult(turnCtx, result.err, params.MessageId, abortTurn)
			}

			final = result.message
			if err := s.emitMessage(turnCtx, final, false); err != nil {
				return acp.PromptResponse{}, err
			}

			usage = usageFromTokens(final.Info.Tokens)

			stopReason := stopReasonFromHermes(final.Info.Finish)
			if s.wasCancelled() || turnCtx.Err() != nil {
				stopReason = acp.StopReasonCancelled
			}

			s.finishTurn()

			turnActive = false

			if err := s.snapshotToStore(context.WithoutCancel(ctx)); err != nil {
				return acp.PromptResponse{}, err
			}

			return acp.PromptResponse{StopReason: stopReason, Usage: usage, UserMessageId: params.MessageId}, nil
		case <-timeout:
			// The cancel guard runs before all failure mapping, including the
			// turn deadline: when a user cancel and the timeout fire together the
			// result is deterministically cancelled, never cause "timeout".
			if s.wasCancelled() || turnCtx.Err() != nil {
				s.cancelTurn()
				abortTurn()

				return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}, nil
			}

			// A turn deadline is a failure, not a user cancel: abort the native
			// turn and surface cause "timeout", never StopReason cancelled.
			abortTurn()

			return acp.PromptResponse{}, mapTurnFailure(nativehermes.NewTurnFailure(nativehermes.CauseTimeout, fmt.Sprintf("hermes turn exceeded %s deadline", turnTimeout)))
		case <-turnCtx.Done():
			abortTurn()

			return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}, nil
		}
	}
}

func promptToHermesParts(blocks []acp.ContentBlock) ([]map[string]any, error) {
	parts := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		switch {
		case block.Text != nil:
			parts = append(parts, map[string]any{keyType: valText, valText: block.Text.Text})
		case block.ResourceLink != nil:
			parts = append(parts, map[string]any{keyType: valText, valText: block.ResourceLink.Uri})
		case block.Resource != nil:
			part, err := embeddedResourceHermesPart(block.Resource.Resource)
			if err != nil {
				return nil, err
			}

			parts = append(parts, part)
		case block.Image != nil:
			part, err := imageHermesPart(block.Image)
			if err != nil {
				return nil, err
			}

			parts = append(parts, part)
		default:
			return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, keyField: acpFieldPrompt})
		}
	}

	if len(parts) == 0 {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, keyField: acpFieldPrompt})
	}

	return parts, nil
}

func imageHermesPart(image *acp.ContentBlockImage) (map[string]any, error) {
	mimeType := image.MimeType
	if mimeType == "" {
		mimeType = defaultMimeType
	}

	part := map[string]any{
		keyType: valFile,
		keyMime: mimeType,
	}

	switch {
	case image.Data != "":
		part[valURL] = "data:" + mimeType + ";base64," + image.Data
	case image.Uri != nil && strings.HasPrefix(*image.Uri, "data:image/"):
		part[valURL] = *image.Uri
	default:
		return nil, acp.NewInvalidParams(map[string]any{keyField: "prompt.image", jsonFieldError: "embedded image data is required"})
	}

	if image.Uri != nil && *image.Uri != "" {
		if filename := filenameFromURI(*image.Uri); filename != "" {
			part[keyFilename] = filename
		}
	}

	return part, nil
}

func filenameFromURI(uri string) string {
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}

	name := filepath.Base(parsed.Path)
	if name == "." || name == "/" {
		return ""
	}

	return name
}

func embeddedResourceHermesPart(resource acp.EmbeddedResourceResource) (map[string]any, error) {
	if resource.TextResourceContents != nil {
		text := resource.TextResourceContents.Text
		if text == "" {
			text = resource.TextResourceContents.Uri
		}

		if text == "" {
			return nil, acp.NewInvalidParams(map[string]any{keyField: "prompt.resource", jsonFieldError: "embedded resource is empty"})
		}

		return map[string]any{keyType: valText, valText: text}, nil
	}

	if resource.BlobResourceContents != nil {
		mimeType := ""
		if resource.BlobResourceContents.MimeType != nil {
			mimeType = *resource.BlobResourceContents.MimeType
		}

		if strings.HasPrefix(strings.ToLower(mimeType), "image/") {
			return imageHermesPart(&acp.ContentBlockImage{
				Data:     resource.BlobResourceContents.Blob,
				MimeType: mimeType,
			})
		}

		if resource.BlobResourceContents.Uri != "" {
			return map[string]any{keyType: valText, valText: resource.BlobResourceContents.Uri}, nil
		}
	}

	return nil, acp.NewInvalidParams(map[string]any{keyField: "prompt.resource", jsonFieldError: valUnsupported})
}

func (s *session) replayMessages(ctx context.Context) error {
	if err := s.ensureNotPoisoned(); err != nil {
		return err
	}

	messages, err := s.client.Messages(ctx, s.idmap.NativeSessionID)
	if err != nil {
		return err
	}

	for i := range messages {
		if err := s.emitMessage(ctx, messages[i], true); err != nil {
			return err
		}
	}

	return nil
}

func (s *session) emitMessage(ctx context.Context, message nativehermes.NativeMessage, includeUser bool) error {
	if err := s.validateNativeMessageSession(ctx, message); err != nil {
		return err
	}

	isUser := message.Info.Role == valUser
	if isUser && !includeUser {
		return nil
	}

	// Resolve the context window at most once per message, and only when a
	// usage update is actually emitted.
	window := -1
	resolveWindow := func() int {
		if window < 0 {
			window = message.Info.ContextWindow
			if window <= 0 {
				window = s.contextWindow(ctx)
			}
		}

		return window
	}

	for i := range message.Parts {
		part := &message.Parts[i]
		if !s.markPart(*part) {
			continue
		}

		if err := s.emitPartUpdates(ctx, message.Info.Role, *part); err != nil {
			return err
		}

		if part.Type == valStepFinish {
			if update := usageUpdateFromTokens(part.MessageID, part.Tokens, resolveWindow()); update != nil {
				if err := s.emitUpdate(ctx, *update); err != nil {
					return err
				}
			}
		}
	}

	if message.Info.Tokens.Total > 0 {
		if update := usageUpdateFromTokens(message.Info.ID, message.Info.Tokens, resolveWindow()); update != nil {
			return s.emitUpdate(ctx, *update)
		}
	}

	return nil
}

func (s *session) validateNativeMessageSession(ctx context.Context, message nativehermes.NativeMessage) error {
	expected := s.idmap.NativeSessionID
	if expected == "" {
		return nil
	}

	if message.Info.SessionID != "" && message.Info.SessionID != expected {
		return s.poisonNativeSessionDrift(ctx, "message info.sessionID", message.Info.SessionID)
	}

	for i := range message.Parts {
		part := &message.Parts[i]
		if part.SessionID == "" || part.SessionID == expected {
			continue
		}

		return s.poisonNativeSessionDrift(ctx, "message part sessionID", part.SessionID)
	}

	return nil
}

func partUpdates(role string, part nativehermes.Part) []acp.SessionUpdate {
	messageID := part.MessageID
	switch part.Type {
	case valText:
		text := unstreamedText(part.Text, part.StreamedText)
		if text == "" {
			return nil
		}

		if role == valUser {
			return []acp.SessionUpdate{{UserMessageChunk: &acp.SessionUpdateUserMessageChunk{
				SessionUpdate: "user_message_chunk",
				MessageId:     &messageID,
				Content:       acp.TextBlock(text),
			}}}
		}

		return []acp.SessionUpdate{{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			SessionUpdate: "agent_message_chunk",
			MessageId:     &messageID,
			Content:       acp.TextBlock(text),
		}}}
	case valReasoning:
		if part.Text == "" {
			return nil
		}

		return []acp.SessionUpdate{{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			SessionUpdate: "agent_thought_chunk",
			MessageId:     &messageID,
			Content:       acp.TextBlock(part.Text),
		}}}
	default:
		return nil
	}
}

func unstreamedText(complete string, streamed string) string {
	if streamed == "" {
		return complete
	}

	if complete == streamed {
		return ""
	}

	if strings.HasPrefix(complete, streamed) {
		return strings.TrimPrefix(complete, streamed)
	}

	// ACP text chunks append and cannot replace a previously streamed prefix.
	// On an inconsistent native completion, preserve the already-delivered
	// stream instead of appending a second, conflicting full response.
	return ""
}

type hermesToolState struct {
	title    string
	kind     acp.ToolKind
	status   acp.ToolCallStatus
	rawInput any
}

func (s *session) emitPartUpdates(ctx context.Context, role string, part nativehermes.Part) error {
	if part.Type == valTool {
		return s.emitToolPartUpdate(ctx, part)
	}

	for _, update := range partUpdates(role, part) {
		if err := s.emitUpdate(ctx, update); err != nil {
			return err
		}
	}

	return nil
}

func (s *session) emitToolPartUpdate(ctx context.Context, part nativehermes.Part) error {
	id, next := hermesToolPartState(part)

	s.toolMu.Lock()
	defer s.toolMu.Unlock()

	previous, seen := s.toolStates[string(id)]
	if !seen {
		if err := s.emitUpdate(ctx, startHermesToolCall(id, next)); err != nil {
			return err
		}

		s.toolStates[string(id)] = next

		return nil
	}

	update, merged, changed := updateHermesToolCall(id, previous, next)
	if !changed {
		return nil
	}

	if err := s.emitUpdate(ctx, update); err != nil {
		return err
	}

	s.toolStates[string(id)] = merged

	return nil
}

func hermesToolPartState(part nativehermes.Part) (acp.ToolCallId, hermesToolState) {
	id := acp.ToolCallId(firstNonEmpty(part.CallID, part.ID, "hermes-tool"))
	state := hermesToolState{
		title:    firstNonEmpty(part.Tool, string(id)),
		kind:     toolKind(part.Tool),
		status:   acp.ToolCallStatusInProgress,
		rawInput: append(json.RawMessage(nil), part.Raw...),
	}

	if len(part.State) > 0 {
		var nativeState map[string]any

		_ = json.Unmarshal(part.State, &nativeState)
		if stateStatus, _ := nativeState["status"].(string); stateStatus != "" {
			state.status = toolStatus(stateStatus)
		}

		if titleValue, _ := nativeState[keyTitle].(string); titleValue != "" {
			state.title = titleValue
		}
	}

	return id, state
}

func startHermesToolCall(id acp.ToolCallId, state hermesToolState) acp.SessionUpdate {
	return acp.StartToolCall(
		id,
		state.title,
		acp.WithStartKind(state.kind),
		acp.WithStartStatus(state.status),
		acp.WithStartRawInput(state.rawInput),
	)
}

func updateHermesToolCall(
	id acp.ToolCallId,
	previous hermesToolState,
	next hermesToolState,
) (acp.SessionUpdate, hermesToolState, bool) {
	if hermesToolStatusTerminal(previous.status) {
		return acp.SessionUpdate{}, previous, false
	}

	merged := next
	if hermesToolStatusRank(next.status) < hermesToolStatusRank(previous.status) {
		merged.status = previous.status
	}

	opts := make([]acp.ToolCallUpdateOpt, 0, 4)
	if merged.status != previous.status {
		opts = append(opts, acp.WithUpdateStatus(merged.status))
	}

	if merged.title != previous.title {
		opts = append(opts, acp.WithUpdateTitle(merged.title))
	}

	if merged.kind != previous.kind {
		opts = append(opts, acp.WithUpdateKind(merged.kind))
	}

	if !reflect.DeepEqual(merged.rawInput, previous.rawInput) {
		opts = append(opts, acp.WithUpdateRawInput(merged.rawInput))
	}

	if len(opts) == 0 {
		return acp.SessionUpdate{}, previous, false
	}

	return acp.UpdateToolCall(id, opts...), merged, true
}

func hermesToolStatusTerminal(status acp.ToolCallStatus) bool {
	return status == acp.ToolCallStatusCompleted || status == acp.ToolCallStatusFailed
}

func hermesToolStatusRank(status acp.ToolCallStatus) int {
	switch status {
	case acp.ToolCallStatusPending:
		return 1
	case acp.ToolCallStatusInProgress:
		return 2
	case acp.ToolCallStatusCompleted, acp.ToolCallStatusFailed:
		return 3
	default:
		return 0
	}
}

func (s *session) handleEvent(ctx context.Context, event nativehermes.TurnEvent) error {
	if s.shouldSuppressEvent(event) {
		return nil
	}

	// Raw events are non-authoritative debug output: a failed emit is recorded
	// on the internal observer hook and never aborts the authoritative turn.
	if err := s.emitRawHermesEvent(ctx, event); err != nil {
		s.agent.observe.RecordRawEventEmitFailure(ctx)
	}

	switch event.Type {
	case evtApprovalRequest:
		var req nativehermes.PermissionRequest
		if err := json.Unmarshal(event.Properties, &req); err != nil {
			return err
		}

		req.ReplyRoute = nativehermes.PermissionRouteAPI
		if req.SessionID == s.idmap.NativeSessionID {
			return s.handlePermission(ctx, req)
		}
	case "todo.updated":
		var payload struct {
			SessionID string              `json:"sessionID"`
			Todos     []nativehermes.Todo `json:"todos"`
		}
		if err := json.Unmarshal(event.Properties, &payload); err == nil && payload.SessionID == s.idmap.NativeSessionID {
			return s.emitPlan(ctx, payload.Todos)
		}
	case evtMessagePartUpdated, evtMessagePartCreated:
		part, ok := eventPart(event.Properties)
		if ok && part.SessionID == s.idmap.NativeSessionID && s.markPart(part) {
			s.markActiveMessageID(part.MessageID)

			if err := s.emitPartUpdates(ctx, valAssistant, part); err != nil {
				return err
			}
		}
	case evtClarifyRequest:
		req, ok := eventQuestion(event.Properties)
		if ok && req.SessionID == s.idmap.NativeSessionID {
			req.ReplyRoute = nativehermes.QuestionRouteAPI

			return s.handleQuestion(ctx, req)
		}
	}

	return nil
}

func eventPart(data json.RawMessage) (nativehermes.Part, bool) {
	var part nativehermes.Part
	if err := json.Unmarshal(data, &part); err == nil && part.Type != "" {
		return part, true
	}

	var wrapper struct {
		Part nativehermes.Part `json:"part"`
	}
	if err := json.Unmarshal(data, &wrapper); err == nil && wrapper.Part.Type != "" {
		return wrapper.Part, true
	}

	return nativehermes.Part{}, false
}

func eventQuestion(data json.RawMessage) (nativehermes.QuestionRequest, bool) {
	var req nativehermes.QuestionRequest
	if err := json.Unmarshal(data, &req); err == nil && req.ID != "" {
		return req, true
	}

	for _, key := range []string{keyQuestion, keyRequest, keyData} {
		var wrapper map[string]json.RawMessage
		if err := json.Unmarshal(data, &wrapper); err != nil {
			continue
		}

		raw := wrapper[key]
		if len(raw) == 0 {
			continue
		}

		if err := json.Unmarshal(raw, &req); err == nil && req.ID != "" {
			return req, true
		}
	}

	return nativehermes.QuestionRequest{}, false
}

func (s *session) reconcilePermissions(ctx context.Context) error {
	requests, err := s.client.PendingPermissions(ctx)
	if err != nil {
		return err
	}

	for i := range requests {
		req := &requests[i]
		if req.SessionID == s.idmap.NativeSessionID {
			if err := s.handlePermission(ctx, *req); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *session) reconcileQuestions(ctx context.Context) error {
	requests, err := s.client.PendingQuestions(ctx)
	if err != nil {
		return err
	}

	for _, req := range requests {
		if req.SessionID == s.idmap.NativeSessionID {
			if err := s.handleQuestion(ctx, req); err != nil {
				return err
			}
		}
	}

	return nil
}

type permissionTurnRoute struct {
	nonce string
	epoch uint64
}

func (s *session) permissionTurnRoute(ctx context.Context) (permissionTurnRoute, bool) {
	if ctx == nil || ctx.Err() != nil {
		return permissionTurnRoute{}, false
	}

	contextNonce := turnNonceFromContext(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	active := s.cancel != nil && s.turnDone != nil && !s.cancelled && !s.closed &&
		contextNonce != "" && contextNonce == s.turnNonce

	return permissionTurnRoute{nonce: s.turnNonce, epoch: s.turnEpoch}, active
}

func (s *session) permissionTurnRouteCurrent(ctx context.Context, route permissionTurnRoute) bool {
	current, active := s.permissionTurnRoute(ctx)

	return active && current == route
}

func (s *session) rejectInvalidPermission(
	req nativehermes.PermissionRequest,
	message string,
	cause error,
) error {
	replyCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	replyErr := s.poisonMissingLiveSessionMapping(
		replyCtx,
		s.client.ReplyPermission(replyCtx, req, valReject, "stale or unknown tool call"),
	)

	return errors.Join(routeInvalid(message), cause, replyErr)
}

func permissionToolRawInput(req nativehermes.PermissionRequest) map[string]any {
	return map[string]any{
		"action":       req.ActionName(),
		"resources":    req.ResourceList(),
		"metadata":     req.Metadata,
		keySource:      req.Source,
		"save":         req.Save,
		valAlways:      req.Always,
		routeFieldTool: req.Tool.CallID,
		keyMessageID:   req.Tool.MessageID,
	}
}

func permissionHermesToolState(req nativehermes.PermissionRequest) hermesToolState {
	title := req.ActionName()
	if title == "" {
		title = "Hermes permission"
	}

	return hermesToolState{
		title:    title,
		kind:     acp.ToolKindOther,
		status:   acp.ToolCallStatusPending,
		rawInput: permissionToolRawInput(req),
	}
}

func (s *session) ensurePermissionToolPending(
	ctx context.Context,
	req nativehermes.PermissionRequest,
	route permissionTurnRoute,
) error {
	s.toolMu.Lock()
	defer s.toolMu.Unlock()

	if !s.permissionTurnRouteCurrent(ctx, route) {
		return errors.New("permission callback crossed its active turn")
	}

	toolCallID := acp.ToolCallId(req.Tool.CallID)
	if current, exists := s.toolStates[req.Tool.CallID]; exists {
		if hermesToolStatusTerminal(current.status) {
			return errors.New("permission callback targets a terminal tool call")
		}

		return nil
	}

	state := permissionHermesToolState(req)

	routedCtx := withTurnRoute(ctx, route.nonce)
	if err := s.emitUpdate(routedCtx, startHermesToolCall(toolCallID, state)); err != nil {
		return err
	}

	s.toolStates[req.Tool.CallID] = state

	return nil
}

func (s *session) handlePermission(ctx context.Context, req nativehermes.PermissionRequest) error {
	if req.ID == "" || req.SessionID == "" {
		return nil
	}

	if req.Tool.CallID == "" {
		return s.rejectInvalidPermission(req, "permission request is missing its native tool call id", nil)
	}

	route, active := s.permissionTurnRoute(ctx)
	if !active {
		return s.rejectInvalidPermission(req, "permission callback arrived outside its active turn", nil)
	}

	if !s.claimPermissionRequest(req.ID) {
		return nil
	}

	if err := s.ensurePermissionToolPending(ctx, req, route); err != nil {
		return s.rejectInvalidPermission(req, "permission tool call is not pending in its active turn", err)
	}

	if !s.permissionTurnRouteCurrent(ctx, route) {
		return s.rejectInvalidPermission(req, "permission callback crossed its active turn", nil)
	}

	s.addPendingPermission(req)

	conn := s.agent.connection()
	if conn == nil {
		_, _, _ = s.takePendingPermission(req.ID)

		replyCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		return s.poisonMissingLiveSessionMapping(replyCtx, s.client.ReplyPermission(replyCtx, req, valReject, "client unavailable"))
	}

	toolState := permissionHermesToolState(req)

	status := acp.ToolCallStatusPending
	kind := acp.ToolKindOther

	resp, err := conn.RequestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: s.id,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: acp.ToolCallId(req.Tool.CallID),
			Title:      &toolState.title,
			Kind:       &kind,
			Status:     &status,
			RawInput:   toolState.rawInput,
		},
		Options: []acp.PermissionOption{
			{OptionId: valOnce, Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: valAlways, Name: "Always allow", Kind: acp.PermissionOptionKindAllowAlways},
			{OptionId: valReject, Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
		},
		Meta: map[string]any{hermesMetaKey: map[string]any{routeFieldReq: req.ID, "nativeSessionId": req.SessionID}},
	})
	if err != nil {
		_, ok, cancelled := s.takePendingPermission(req.ID)
		if !ok {
			return errPromptCancelled
		}

		if cancelled || s.wasCancelled() || ctx.Err() != nil || !s.permissionTurnRouteCurrent(ctx, route) {
			replyCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			_ = s.poisonMissingLiveSessionMapping(replyCtx, s.client.ReplyPermission(replyCtx, req, valReject, valCancelled))

			cancel()

			return errPromptCancelled
		}

		replyCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		replyErr := s.poisonMissingLiveSessionMapping(replyCtx, s.client.ReplyPermission(replyCtx, req, valReject, "client permission request failed"))

		cancel()

		if replyErr != nil {
			return errors.Join(err, replyErr)
		}

		return err
	}

	reply := valReject

	if resp.Outcome.Selected != nil {
		switch resp.Outcome.Selected.OptionId {
		case valOnce, valAlways, valReject:
			reply = string(resp.Outcome.Selected.OptionId)
		}
	}

	if resp.Outcome.Cancelled != nil {
		reply = valReject
	}

	_, ok, cancelled := s.takePendingPermission(req.ID)
	if !ok {
		return errPromptCancelled
	}

	if cancelled || ctx.Err() != nil || !s.permissionTurnRouteCurrent(ctx, route) {
		replyCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		if err := s.poisonMissingLiveSessionMapping(replyCtx, s.client.ReplyPermission(replyCtx, req, valReject, valCancelled)); err != nil {
			return err
		}

		return errPromptCancelled
	}

	return s.poisonMissingLiveSessionMapping(ctx, s.client.ReplyPermission(ctx, req, reply, ""))
}

func (s *session) handleQuestion(ctx context.Context, req nativehermes.QuestionRequest) error {
	if req.ID == "" || req.SessionID == "" {
		return nil
	}

	if !s.claimQuestionRequest(req.ID) {
		return nil
	}

	s.addPendingQuestion(req)

	conn := s.agent.connection()
	if conn == nil || !s.agent.clientSupportsFormElicitation() {
		_, _, cancelled := s.takePendingQuestion(req.ID)

		rejectCtx := ctx
		if cancelled || ctx.Err() != nil {
			backgroundCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			defer cancel()

			rejectCtx = backgroundCtx
		}

		return s.poisonMissingLiveSessionMapping(rejectCtx, s.client.RejectQuestion(rejectCtx, req))
	}

	request, propertyIDs := questionElicitationRequest(req)

	requestID := req.ID

	resp, err := conn.CreateElicitation(ctx, request, elicitationScope{
		SessionID: s.id,
		TurnNonce: s.currentTurnNonce(),
		RequestID: &requestID,
	})
	if err != nil {
		_, ok, cancelled := s.takePendingQuestion(req.ID)
		if !ok {
			return errPromptCancelled
		}

		if cancelled || s.wasCancelled() || ctx.Err() != nil {
			rejectCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			_ = s.poisonMissingLiveSessionMapping(rejectCtx, s.client.RejectQuestion(rejectCtx, req))

			cancel()

			return errPromptCancelled
		}

		rejectCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		rejectErr := s.poisonMissingLiveSessionMapping(rejectCtx, s.client.RejectQuestion(rejectCtx, req))

		cancel()

		if rejectErr != nil {
			return errors.Join(err, rejectErr)
		}

		return err
	}

	if resp.Accept == nil {
		_, ok, cancelled := s.takePendingQuestion(req.ID)
		if !ok {
			return errPromptCancelled
		}

		rejectCtx := ctx
		if cancelled || ctx.Err() != nil {
			backgroundCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			defer cancel()

			rejectCtx = backgroundCtx
		}

		if err := s.poisonMissingLiveSessionMapping(rejectCtx, s.client.RejectQuestion(rejectCtx, req)); err != nil {
			return err
		}

		if cancelled || ctx.Err() != nil {
			return errPromptCancelled
		}

		return nil
	}

	_, ok, cancelled := s.takePendingQuestion(req.ID)
	if !ok {
		return errPromptCancelled
	}

	if cancelled || ctx.Err() != nil {
		rejectCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		if err := s.poisonMissingLiveSessionMapping(rejectCtx, s.client.RejectQuestion(rejectCtx, req)); err != nil {
			return err
		}

		return errPromptCancelled
	}

	return s.poisonMissingLiveSessionMapping(ctx, s.client.ReplyQuestion(ctx, req, questionAnswersFromContent(resp.Accept.Content, propertyIDs)))
}

func (s *session) drainClientBacklog(ctx context.Context) error {
	suppress := s.suppressBacklog()
	defer s.clearSuppressBacklog()

	for {
		select {
		case event := <-s.client.Events():
			if suppress || event.Type == evtServerConnected || s.shouldSuppressEvent(event) {
				continue
			}

			if err := s.handleEvent(ctx, event); err != nil {
				return err
			}
		case <-s.client.EventErrors():
			continue
		default:
			return nil
		}
	}
}

func questionElicitationRequest(req nativehermes.QuestionRequest) (acp.UnstableCreateElicitationRequest, []string) {
	properties := make(map[string]any, len(req.Questions))
	required := make([]string, 0, len(req.Questions))

	propertyIDs := make([]string, 0, len(req.Questions))
	for index, question := range req.Questions {
		id := fmt.Sprintf("question_%d", index+1)
		propertyIDs = append(propertyIDs, id)
		required = append(required, id)
		properties[id] = questionPropertySchema(index, question)
	}

	if len(properties) == 0 {
		propertyIDs = []string{valQuestion1}
		required = []string{valQuestion1}
		properties[valQuestion1] = map[string]any{keyType: valString, keyTitle: "Question 1"}
	}

	title := "Hermes question"

	return acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{
			Message: questionElicitationMessage(req.Questions),
			Mode:    valForm,
			RequestedSchema: acp.UnstableElicitationSchema{
				Title:      &title,
				Type:       acp.UnstableElicitationSchemaTypeObject,
				Properties: properties,
				Required:   required,
			},
			Meta: map[string]any{hermesMetaKey: map[string]any{
				routeFieldReq:     req.ID,
				"nativeSessionId": req.SessionID,
				valTool: map[string]any{
					keyMessageID: req.Tool.MessageID,
					"callId":     req.Tool.CallID,
				},
			}},
		},
	}, propertyIDs
}

func questionPropertySchema(index int, question nativehermes.QuestionInfo) map[string]any {
	title := firstNonEmpty(question.Header, fmt.Sprintf("Question %d", index+1))

	description := question.Question
	if question.Multiple {
		items := map[string]any{keyType: valString}

		if !question.Custom {
			if options := questionOptionSchemas(question.Options); len(options) > 0 {
				items["anyOf"] = options
			}
		}

		return map[string]any{
			keyType:       "array",
			keyTitle:      title,
			"description": description,
			"items":       items,
		}
	}

	property := map[string]any{
		keyType:       valString,
		keyTitle:      title,
		"description": description,
	}

	if !question.Custom {
		if options := questionOptionSchemas(question.Options); len(options) > 0 {
			property["oneOf"] = options
		}
	}

	return property
}

func questionOptionSchemas(options []nativehermes.QuestionOption) []map[string]any {
	out := make([]map[string]any, 0, len(options))
	for _, option := range options {
		label := strings.TrimSpace(option.Label)
		if label == "" {
			continue
		}

		item := map[string]any{
			"const":  label,
			keyTitle: label,
		}
		if option.Description != "" {
			item["description"] = option.Description
		}

		out = append(out, item)
	}

	return out
}

func questionElicitationMessage(questions []nativehermes.QuestionInfo) string {
	if len(questions) == 1 && questions[0].Question != "" {
		return questions[0].Question
	}

	return msgHermesNeedsInput
}

func questionAnswersFromContent(content map[string]any, propertyIDs []string) [][]string {
	answers := make([][]string, len(propertyIDs))
	for index, id := range propertyIDs {
		answers[index] = stringAnswersFromAny(content[id])
	}

	return answers
}

func stringAnswersFromAny(value any) []string {
	switch typed := value.(type) {
	case nil:
		return []string{}
	case string:
		return []string{typed}
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if item == nil {
				continue
			}

			if str, ok := item.(string); ok {
				out = append(out, str)

				continue
			}

			out = append(out, fmt.Sprint(item))
		}

		return out
	default:
		return []string{fmt.Sprint(value)}
	}
}

func (s *session) emitPlan(ctx context.Context, todos []nativehermes.Todo) error {
	entries := make([]acp.PlanEntry, 0, len(todos))
	for _, todo := range todos {
		if todo.Content == "" {
			continue
		}

		entries = append(entries, acp.PlanEntry{
			Content:  todo.Content,
			Priority: planPriority(todo.Priority),
			Status:   planStatus(todo.Status),
		})
	}

	if len(entries) == 0 {
		return nil
	}

	return s.emitUpdate(ctx, acp.UpdatePlan(entries...))
}

func (s *session) emitUpdate(ctx context.Context, update acp.SessionUpdate) error {
	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	s.agent.observe.ObserveFirstPromptUpdate(ctx)

	return conn.SessionUpdate(ctx, acp.SessionNotification{
		Meta:      turnRouteMetaFromContext(ctx),
		SessionId: s.id,
		Update:    update,
	})
}

func (s *session) emitRawHermesEvent(ctx context.Context, event nativehermes.TurnEvent) error {
	if !s.rawMessages.Enabled() {
		return nil
	}

	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	var raw map[string]any
	if len(event.Raw) > 0 {
		_ = json.Unmarshal(event.Raw, &raw)
	}

	// A native event without a payload is skipped without consuming a
	// sequence: consumers never receive "event": null and never see a gap.
	if raw == nil {
		return nil
	}

	// Serialize reservation through delivery so successful notifications cannot
	// reorder and a failed attempt leaves the next contiguous sequence reusable.
	s.rawEventMu.Lock()
	defer s.rawEventMu.Unlock()

	sequence := s.rawSeq + 1

	payload := map[string]any{
		jsonFieldSessionID: s.id,
		keySequence:        sequence,
		keySource:          valHermesServeSource,
		keyEvent:           raw,
	}
	if meta := turnRouteMetaFromContext(ctx); meta != nil {
		payload["_meta"] = meta
	}

	capped, err := capRawEventPayload(payload)
	if err != nil {
		return err
	}

	if err := conn.NotifyExtension(ctx, RawEventMethod, capped); err != nil {
		return err
	}

	s.rawSeq = sequence

	return nil
}

// usageUpdateFromTokens builds a usage_update. size is the model's true context
// window in tokens, or 0 when unknown; it is never fabricated from used.
func usageUpdateFromTokens(messageID string, tokens nativehermes.Tokens, size int) *acp.SessionUpdate {
	used := int(tokens.Total)
	if used <= 0 {
		used = int(tokens.Input + tokens.Output + tokens.Reasoning)
	}

	if used <= 0 {
		return nil
	}

	meta := map[string]any{hermesMetaKey: map[string]any{keyMessageID: messageID}}

	return &acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
		SessionUpdate: "usage_update",
		Used:          used,
		Size:          size,
		Meta:          meta,
	}}
}

func usageFromTokens(tokens nativehermes.Tokens) *acp.Usage {
	used := int(tokens.Total)
	if used <= 0 {
		used = int(tokens.Input + tokens.Output + tokens.Reasoning)
	}

	if used <= 0 {
		return nil
	}

	thought := int(tokens.Reasoning)
	cacheRead := int(tokens.Cache.Read)
	cacheWrite := int(tokens.Cache.Write)

	return &acp.Usage{
		InputTokens:       int(tokens.Input),
		OutputTokens:      int(tokens.Output),
		ThoughtTokens:     &thought,
		CachedReadTokens:  &cacheRead,
		CachedWriteTokens: &cacheWrite,
		TotalTokens:       used,
	}
}

func stopReasonFromHermes(reason string) acp.StopReason {
	switch strings.ToLower(reason) {
	case valLength, "max_tokens":
		return acp.StopReasonMaxTokens
	case valCancelled, "canceled":
		return acp.StopReasonCancelled
	case "refusal":
		return acp.StopReasonRefusal
	default:
		return acp.StopReasonEndTurn
	}
}

func toolStatus(value string) acp.ToolCallStatus {
	switch strings.ToLower(value) {
	case valPending:
		return acp.ToolCallStatusPending
	case valCompleted, valSuccess:
		return acp.ToolCallStatusCompleted
	case "failed", jsonFieldError:
		return acp.ToolCallStatusFailed
	default:
		return acp.ToolCallStatusInProgress
	}
}

func toolKind(tool string) acp.ToolKind {
	switch strings.ToLower(tool) {
	case valRead, "view":
		return acp.ToolKindRead
	case valEdit, "write":
		return acp.ToolKindEdit
	case valDelete, "remove":
		return acp.ToolKindDelete
	case "move", "rename":
		return acp.ToolKindMove
	case "grep", "search", "find":
		return acp.ToolKindSearch
	case "bash", "shell", "run":
		return acp.ToolKindExecute
	case "fetch", "webfetch":
		return acp.ToolKindFetch
	case "think":
		return acp.ToolKindThink
	default:
		return acp.ToolKindOther
	}
}

func planPriority(value string) acp.PlanEntryPriority {
	switch strings.ToLower(value) {
	case valHigh:
		return acp.PlanEntryPriorityHigh
	case valLow:
		return acp.PlanEntryPriorityLow
	default:
		return acp.PlanEntryPriorityMedium
	}
}

func planStatus(value string) acp.PlanEntryStatus {
	switch strings.ToLower(value) {
	case valCompleted, "done":
		return acp.PlanEntryStatusCompleted
	case "in_progress", "running":
		return acp.PlanEntryStatusInProgress
	default:
		return acp.PlanEntryStatusPending
	}
}
