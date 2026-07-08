//nolint:tagliatelle // Hermes native event payloads use sessionID wire names.
package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/observer"
)

var errPromptCancelled = errors.New("prompt cancelled")

// mapTurnFailure maps a classified native turn failure to the uniform
// hermes_turn_failed JSON-RPC error. hermes advertises no auth methods, so every
// turn failure is an Internal error (-32603). An unclassified error is surfaced
// as a transport failure carrying its real cause, never a fixed placeholder.
func mapTurnFailure(err error) error {
	data := map[string]any{jsonFieldError: valHermesTurnFailed}

	var failure *turnFailureError
	if errors.As(err, &failure) {
		data[jsonFieldCause] = string(failure.cause)
		data[jsonFieldMessage] = firstNonEmpty(failure.message, err.Error())

		if failure.statusCode != 0 {
			data[jsonFieldStatusCode] = failure.statusCode
		}

		if failure.providerCode != "" {
			data[jsonFieldProviderCode] = failure.providerCode
		}
	} else {
		data[jsonFieldCause] = string(causeTransport)
		data[jsonFieldMessage] = err.Error()
	}

	return acp.NewInternalError(data)
}

// reconcileConnected drives the reconnect reconciliation for a turn: pending
// permissions first, then pending questions.
func (s *session) reconcileConnected(ctx context.Context) error {
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

	if isGatewayDisconnect(sendErr) {
		s.markStreamFailed(0)
	}

	return acp.PromptResponse{}, mapTurnFailure(sendErr)
}

func (a *Agent) Prompt(ctx context.Context, params acp.PromptRequest) (resp acp.PromptResponse, err error) {
	session, err := a.session(params.SessionId)
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

	session.cancelTurn()

	cancelCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	return session.client.Abort(cancelCtx, session.idmap.NativeSessionID)
}

func (s *session) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if err := s.ensureNotPoisoned(); err != nil {
		return acp.PromptResponse{}, err
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

	req := hermesMessageRequest{
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

	turnCtx := s.beginTurn(ctx)

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
		message nativeMessage
		err     error
	}

	done := make(chan result, 1)

	go func() {
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
		final nativeMessage
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
			s.markStreamFailed(streamErrorEpoch(err))
			s.cancelTurn()
			abortTurn()

			if cancelled {
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}, nil
			}

			return acp.PromptResponse{}, mapTurnFailure(&turnFailureError{cause: causeTransport, message: err.Error()})
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

			return acp.PromptResponse{}, mapTurnFailure(&turnFailureError{
				cause:   causeTimeout,
				message: fmt.Sprintf("hermes turn exceeded %s deadline", turnTimeout),
			})
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
			text := embeddedResourceText(block.Resource.Resource)
			if text != "" {
				parts = append(parts, map[string]any{keyType: valText, valText: text})
			}
		case block.Image != nil:
			part, err := imageHermesPart(block.Image)
			if err != nil {
				return nil, err
			}

			parts = append(parts, part)
		default:
			return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, keyField: "prompt"})
		}
	}

	if len(parts) == 0 {
		return nil, acp.NewInvalidParams(map[string]any{keyField: "prompt"})
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
	case image.Uri != nil && *image.Uri != "":
		part[valURL] = *image.Uri
	default:
		return nil, acp.NewInvalidParams(map[string]any{keyField: "prompt.image", jsonFieldError: "missing image data or uri"})
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

func embeddedResourceText(resource acp.EmbeddedResourceResource) string {
	data, _ := json.Marshal(resource)

	var raw map[string]any

	_ = json.Unmarshal(data, &raw)
	if text, _ := raw[valText].(string); text != "" {
		return text
	}

	if uri, _ := raw["uri"].(string); uri != "" {
		return uri
	}

	return ""
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

func (s *session) emitMessage(ctx context.Context, message nativeMessage, includeUser bool) error {
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
			window = s.contextWindow(ctx)
		}

		return window
	}

	for i := range message.Parts {
		part := &message.Parts[i]
		if !s.markPart(*part) {
			continue
		}

		for _, update := range partUpdates(message.Info.Role, *part) {
			if err := s.emitUpdate(ctx, update); err != nil {
				return err
			}
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

func (s *session) validateNativeMessageSession(ctx context.Context, message nativeMessage) error {
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

func partUpdates(role string, part nativePart) []acp.SessionUpdate {
	messageID := part.MessageID
	switch part.Type {
	case valText:
		if part.Text == "" {
			return nil
		}

		if role == valUser {
			return []acp.SessionUpdate{{UserMessageChunk: &acp.SessionUpdateUserMessageChunk{
				SessionUpdate: "user_message_chunk",
				MessageId:     &messageID,
				Content:       acp.TextBlock(part.Text),
			}}}
		}

		return []acp.SessionUpdate{{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			SessionUpdate: "agent_message_chunk",
			MessageId:     &messageID,
			Content:       acp.TextBlock(part.Text),
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
	case valTool:
		return toolPartUpdates(part)
	default:
		return nil
	}
}

func toolPartUpdates(part nativePart) []acp.SessionUpdate {
	id := acp.ToolCallId(firstNonEmpty(part.CallID, part.ID, "hermes-tool"))
	title := firstNonEmpty(part.Tool, string(id))
	status := acp.ToolCallStatusInProgress

	if len(part.State) > 0 {
		var state map[string]any

		_ = json.Unmarshal(part.State, &state)
		if stateStatus, _ := state["status"].(string); stateStatus != "" {
			status = toolStatus(stateStatus)
		}

		if titleValue, _ := state[keyTitle].(string); titleValue != "" {
			title = titleValue
		}
	}

	return []acp.SessionUpdate{acp.StartToolCall(
		id,
		title,
		acp.WithStartKind(toolKind(part.Tool)),
		acp.WithStartStatus(status),
		acp.WithStartRawInput(part.Raw),
	)}
}

func (s *session) handleEvent(ctx context.Context, event hermesEvent) error {
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
		var req permissionRequest
		if err := json.Unmarshal(event.Properties, &req); err != nil {
			return err
		}

		req.ReplyRoute = permissionRouteAPI
		if req.SessionID == s.idmap.NativeSessionID {
			return s.handlePermission(ctx, req)
		}
	case "todo.updated":
		var payload struct {
			SessionID string       `json:"sessionID"`
			Todos     []nativeTodo `json:"todos"`
		}
		if err := json.Unmarshal(event.Properties, &payload); err == nil && payload.SessionID == s.idmap.NativeSessionID {
			return s.emitPlan(ctx, payload.Todos)
		}
	case evtMessagePartUpdated, evtMessagePartCreated:
		part, ok := eventPart(event.Properties)
		if ok && part.SessionID == s.idmap.NativeSessionID && s.markPart(part) {
			s.markActiveMessageID(part.MessageID)

			for _, update := range partUpdates(valAssistant, part) {
				if err := s.emitUpdate(ctx, update); err != nil {
					return err
				}
			}
		}
	case evtClarifyRequest:
		req, ok := eventQuestion(event.Properties)
		if ok && req.SessionID == s.idmap.NativeSessionID {
			req.ReplyRoute = questionRouteAPI

			return s.handleQuestion(ctx, req)
		}
	}

	return nil
}

func eventPart(data json.RawMessage) (nativePart, bool) {
	var part nativePart
	if err := json.Unmarshal(data, &part); err == nil && part.Type != "" {
		return part, true
	}

	var wrapper struct {
		Part nativePart `json:"part"`
	}
	if err := json.Unmarshal(data, &wrapper); err == nil && wrapper.Part.Type != "" {
		return wrapper.Part, true
	}

	return nativePart{}, false
}

func eventQuestion(data json.RawMessage) (questionRequest, bool) {
	var req questionRequest
	if err := json.Unmarshal(data, &req); err == nil && req.ID != "" {
		return req, true
	}

	for _, key := range []string{keyQuestion, "request", "data"} {
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

	return questionRequest{}, false
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

func (s *session) handlePermission(ctx context.Context, req permissionRequest) error {
	if req.ID == "" || req.SessionID == "" {
		return nil
	}

	if !s.claimPermissionRequest(req.ID) {
		return nil
	}

	s.addPendingPermission(req)

	conn := s.agent.connection()
	if conn == nil {
		_, _, cancelled := s.takePendingPermission(req.ID)

		replyCtx := ctx
		if cancelled || ctx.Err() != nil {
			backgroundCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			defer cancel()

			replyCtx = backgroundCtx
		}

		return s.poisonMissingLiveSessionMapping(replyCtx, s.client.ReplyPermission(replyCtx, req, valReject, "client unavailable"))
	}

	title := req.actionName()
	if title == "" {
		title = "Hermes permission"
	}

	status := acp.ToolCallStatusPending
	kind := acp.ToolKindOther

	resp, err := conn.RequestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: s.id,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: acp.ToolCallId(firstNonEmpty(req.ID, "hermes-permission")),
			Title:      &title,
			Kind:       &kind,
			Status:     &status,
			RawInput: map[string]any{
				"action":     req.actionName(),
				"resources":  req.resourceList(),
				"metadata":   req.Metadata,
				keySource:    req.Source,
				"save":       req.Save,
				valAlways:    req.Always,
				"toolCallId": req.Tool.CallID,
				keyMessageID: req.Tool.MessageID,
			},
		},
		Options: []acp.PermissionOption{
			{OptionId: valOnce, Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: valAlways, Name: "Always allow", Kind: acp.PermissionOptionKindAllowAlways},
			{OptionId: valReject, Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
		},
		Meta: map[string]any{hermesMetaKey: map[string]any{"requestId": req.ID, "nativeSessionId": req.SessionID}},
	})
	if err != nil {
		_, ok, cancelled := s.takePendingPermission(req.ID)
		if !ok {
			return errPromptCancelled
		}

		if cancelled || s.wasCancelled() || ctx.Err() != nil {
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

	if cancelled || ctx.Err() != nil {
		replyCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		if err := s.poisonMissingLiveSessionMapping(replyCtx, s.client.ReplyPermission(replyCtx, req, valReject, valCancelled)); err != nil {
			return err
		}

		return errPromptCancelled
	}

	return s.poisonMissingLiveSessionMapping(ctx, s.client.ReplyPermission(ctx, req, reply, ""))
}

func (s *session) handleQuestion(ctx context.Context, req questionRequest) error {
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

	resp, err := conn.CreateElicitation(ctx, request, elicitationScope{
		SessionID:  s.id,
		ToolCallID: acp.ToolCallId(firstNonEmpty(req.Tool.CallID, req.ID)),
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

func questionElicitationRequest(req questionRequest) (acp.UnstableCreateElicitationRequest, []string) {
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
				"requestId":       req.ID,
				"nativeSessionId": req.SessionID,
				valTool: map[string]any{
					keyMessageID: req.Tool.MessageID,
					"callId":     req.Tool.CallID,
				},
			}},
		},
	}, propertyIDs
}

func questionPropertySchema(index int, question questionInfo) map[string]any {
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

func questionOptionSchemas(options []questionOption) []map[string]any {
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

func questionElicitationMessage(questions []questionInfo) string {
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

func (s *session) emitPlan(ctx context.Context, todos []nativeTodo) error {
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

	return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update})
}

func (s *session) emitRawHermesEvent(ctx context.Context, event hermesEvent) error {
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

	payload := map[string]any{
		jsonFieldSessionID: s.id,
		keySequence:        s.nextRawEventSequence(),
		keySource:          valHermesServeSource,
		keyEvent:           raw,
	}

	return conn.NotifyExtension(ctx, RawEventMethod, capRawEventPayload(payload))
}

// usageUpdateFromTokens builds a usage_update. size is the model's true context
// window in tokens, or 0 when unknown; it is never fabricated from used.
func usageUpdateFromTokens(messageID string, tokens nativeTokens, size int) *acp.SessionUpdate {
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

func usageFromTokens(tokens nativeTokens) *acp.Usage {
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
