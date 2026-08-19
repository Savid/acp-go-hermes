//nolint:tagliatelle // Hermes native event payloads use sessionID wire names.
package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
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
	valStop       = "stop"
	valPending    = "pending"
	valCompleted  = "completed"
	valRead       = "read"
	valEdit       = "edit"
	valDelete     = "delete"
	valHigh       = "high"
	valLow        = "low"
	valSuccess    = "success"
	valDone       = "done"
	valFile       = "file"
	valTerminal   = "terminal"

	keyType       = "type"
	keyTitle      = "title"
	keyMime       = "mime"
	keyQuestion   = "question"
	keyRequest    = "request"
	keyMessageID  = "messageId"
	keyOutcome    = "outcome"
	keyStopReason = "stopReason"

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

// failedTurnResult maps a native SendMessage error to the turn outcome only
// after fencing the admitted native runtime back to its last committed state.
func (s *session) failedTurnResult(ctx context.Context, turnEpoch uint64, sendErr error) (acp.PromptResponse, error) {
	if nativehermes.IsGatewayDisconnect(sendErr) {
		s.markStreamFailed(0)
	}

	if fenceErr := s.fenceTurn(context.WithoutCancel(ctx), turnEpoch, false); fenceErr != nil {
		return acp.PromptResponse{}, fenceErr
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
	epoch := s.turnEpoch
	s.mu.Unlock()

	// Route validation runs first, so a cancel that is both stale and malformed
	// reports one verdict and never an implementation-defined choice of two.
	route, err := parseInboundTurnRoute(meta)
	if err != nil {
		return err
	}

	// No native side effect happens without a nonce that authenticates the
	// addressed session's current turn. A missing or stale nonce fails closed
	// before the native interrupt, and with no turn in flight there is nothing
	// for a cancel to authorize at all, so nothing native runs: an abort issued
	// on an unvalidated envelope would tear into whatever the session is doing
	// between turns on the word of a caller that proved nothing.
	if !active || route.turnNonce != activeNonce {
		return routeInvalid("stale route turnNonce")
	}

	// The reserved lifecycle literal fails the cancel closed too, and likewise
	// before the native interrupt: a cancel naming a key this surface never
	// carries never reaches the gateway.
	if err := rejectLifecycleMeta(meta); err != nil {
		return err
	}

	// The terminal store replacement is the turn's settlement linearization
	// point. Cancellation that wins before that claim fences the native runtime;
	// cancellation after the claim is a post-settlement no-op and must not close
	// the runtime underneath an atomic commit already in progress.
	s.mu.Lock()
	if s.turnSettlement == turnSettlementCommitting {
		s.mu.Unlock()

		return nil
	}

	s.turnSettlement = turnSettlementCancelled
	s.mu.Unlock()

	return s.fenceTurnLocked(context.Background(), epoch, true)
}

func (s *session) Prompt(ctx context.Context, params acp.PromptRequest) (_ acp.PromptResponse, returnErr error) {
	route, err := parseInboundTurnRoute(params.Meta)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	// Route validation runs first: the route nonce is the anti-stale turn
	// authenticator. The lifecycle correlation is read next and carries submission
	// identity for the same admitted turn. Neither value is derived from the
	// other, and both bind.
	submission, correlationErr := lifecycle.DecodePromptCorrelation(params.Meta, s.agent.negotiatedLifecycle())
	if correlationErr != nil {
		return acp.PromptResponse{}, lifecycleParamError(correlationErr)
	}

	if poisonErr := s.ensureNotPoisoned(); poisonErr != nil {
		return acp.PromptResponse{}, poisonErr
	}

	release, settlement, err := s.acquireTurn(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer settlement.complete()
	defer release()

	turnPublished := false

	if s.agent.options.SharedHermesHome != "" {
		sessionSetLock, lockErr := acquireSharedSessionSetLock(
			ctx, s.agent.options.SharedHermesHome, nativehermes.SharedSessionSetLockShared,
		)
		if lockErr != nil {
			return acp.PromptResponse{}, lockErr
		}
		defer func() {
			releaseErr := sessionSetLock.Release()
			if turnPublished && releaseErr != nil {
				s.agent.log.DebugContext(ctx, "release committed Hermes turn session-set lock", slog.String(jsonFieldError, releaseErr.Error()))

				return
			}

			returnErr = errors.Join(returnErr, releaseErr)
		}()
	}

	parts, err := promptToHermesParts(ctx, params.Prompt, s.agent.options.ImageLimits, s.agent.options.InputHandoffRoot)
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

	if !s.needsRuntimeResume() {
		if backlogErr := s.drainClientBacklog(ctx); backlogErr != nil {
			if errors.Is(backlogErr, errPromptCancelled) {
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled, UserMessageId: params.MessageId}, nil
			}

			return acp.PromptResponse{}, backlogErr
		}
	}

	turnCtx, turnEpoch, err := s.preparePromptTurn(ctx, route.turnNonce)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	baseline := s.committedTerminalState()

	run := s.runPromptTurn(ctx, turnCtx, turnEpoch, submission, req, params.MessageId)
	if !run.settle {
		s.finishTurn()

		return run.response, run.err
	}

	response, committed, settleErr := s.settlePrompt(ctx, turnCtx, turnEpoch, baseline, run, params.MessageId)
	turnPublished = committed

	return response, settleErr
}

func promptToHermesParts(ctx context.Context, blocks []acp.ContentBlock, limits ImageLimits, handoffRoot string) ([]map[string]any, error) {
	budget := newImagePromptBudget(limits, handoffRoot)

	defer budget.closeHandoffRoot()

	parts := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		switch {
		case block.Text != nil:
			parts = append(parts, map[string]any{keyType: valText, valText: block.Text.Text})
		case block.ResourceLink != nil:
			parts = append(parts, map[string]any{keyType: valText, valText: block.ResourceLink.Uri})
		case block.Resource != nil:
			part, err := embeddedResourceHermesPart(block.Resource.Resource, budget)
			if err != nil {
				return nil, err
			}

			parts = append(parts, part)
		case block.Image != nil:
			part, err := imageHermesPart(ctx, block.Image, budget)
			if err != nil {
				return nil, err
			}

			parts = append(parts, part)
		default:
			return nil, unsupportedField(acpFieldPrompt)
		}
	}

	if len(parts) == 0 {
		return nil, unsupportedField(acpFieldPrompt)
	}

	return parts, nil
}

// imageHermesPart validates one image block and shapes its bytes as a native
// attachment part. Embedded data wins whenever it is present; only a block with
// empty data and handoff intent takes the local-handoff form.
func imageHermesPart(ctx context.Context, image *acp.ContentBlockImage, budget *imagePromptBudget) (map[string]any, error) {
	if imageBlockIsHandoff(image) {
		return handoffImageHermesPart(ctx, image, budget)
	}

	return embeddedImageHermesPart(acpFieldPromptImage, image.Data, image.MimeType, budget)
}

// embeddedImageHermesPart validates base64 bytes carried in the block and shapes
// them as a native attachment part. A block URI is provenance only: it is never
// fetched and nothing derived from it enters the native request, because the
// handoff form cannot derive anything from its own URI and the two forms build
// the same request over the same bytes.
func embeddedImageHermesPart(field, data, mimeType string, budget *imagePromptBudget) (map[string]any, error) {
	decoded, err := budget.validateEmbedded(field, data, mimeType)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyType: valFile,
		keyMime: mimeType,
		keyData: decoded,
	}, nil
}

// handoffImageHermesPart validates a handoff block and shapes the bytes read
// from its file as a native attachment part. The handoff path is a host-owned
// read location rather than provenance, so nothing derived from it enters the
// native request: the part a handoff block builds is byte-identical to the part
// the same bytes build embedded.
func handoffImageHermesPart(ctx context.Context, image *acp.ContentBlockImage, budget *imagePromptBudget) (map[string]any, error) {
	decoded, err := budget.validateHandoff(ctx, image)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyType: valFile,
		keyMime: image.MimeType,
		keyData: decoded,
	}, nil
}

// embeddedResourceHermesPart maps an embedded resource to a native part. An
// image-MIME blob resource carries pixels, so it runs the identical image
// validation pipeline and occupies the next image index instead of degrading
// to a URI reference. Every other blob resource still carries bytes, so base64
// validity and the decoded-byte budget bind before it degrades to its URI.
func embeddedResourceHermesPart(resource acp.EmbeddedResourceResource, budget *imagePromptBudget) (map[string]any, error) {
	if resource.TextResourceContents != nil {
		if err := budget.chargeText(int64(len(resource.TextResourceContents.Text))); err != nil {
			return nil, err
		}

		text := resource.TextResourceContents.Text
		if text == "" {
			text = resource.TextResourceContents.Uri
		}

		if text == "" {
			return nil, unsupportedField(acpFieldPrompt)
		}

		return map[string]any{keyType: valText, valText: text}, nil
	}

	if resource.BlobResourceContents != nil {
		mimeType := ""
		if resource.BlobResourceContents.MimeType != nil {
			mimeType = *resource.BlobResourceContents.MimeType
		}

		if strings.HasPrefix(normalizeMediaType(mimeType), "image/") {
			return embeddedImageHermesPart(acpFieldPromptResource, resource.BlobResourceContents.Blob, mimeType, budget)
		}

		if err := budget.accountBlobResource(resource.BlobResourceContents.Blob); err != nil {
			return nil, err
		}

		if resource.BlobResourceContents.Uri != "" {
			return map[string]any{keyType: valText, valText: resource.BlobResourceContents.Uri}, nil
		}
	}

	return nil, unsupportedField(acpFieldPrompt)
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
			if update := usageUpdateFromTokens(part.Tokens, resolveWindow()); update != nil {
				if err := s.emitUpdate(ctx, *update); err != nil {
					return err
				}
			}
		}
	}

	if message.Info.Tokens.Total > 0 {
		if update := usageUpdateFromTokens(message.Info.Tokens, resolveWindow()); update != nil {
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
	title     string
	kind      acp.ToolKind
	status    acp.ToolCallStatus
	rawInput  any
	rawOutput any
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

	if role == valAssistant && part.Type == valText {
		// The prefix is recorded from what was actually delivered, so a settled
		// boundary states the visible work its host was shown rather than work the
		// native side had merely begun.
		s.recordForegroundPrefix(part.Text)
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

		if rawInput, ok := nativeState["rawInput"]; ok {
			state.rawInput = rawInput
		}

		if rawOutput, ok := nativeState["rawOutput"]; ok {
			state.rawOutput = rawOutput
		}
	}

	return id, state
}

func startHermesToolCall(id acp.ToolCallId, state hermesToolState) acp.SessionUpdate {
	opts := []acp.ToolCallStartOpt{
		acp.WithStartKind(state.kind),
		acp.WithStartStatus(state.status),
		acp.WithStartRawInput(state.rawInput),
	}
	if state.rawOutput != nil {
		opts = append(opts, acp.WithStartRawOutput(state.rawOutput))
	}

	return acp.StartToolCall(id, state.title, opts...)
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

	if !reflect.DeepEqual(merged.rawOutput, previous.rawOutput) {
		opts = append(opts, acp.WithUpdateRawOutput(merged.rawOutput))
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
		"action":       req.Action,
		"metadata":     req.Metadata,
		routeFieldTool: req.Tool.CallID,
		keyMessageID:   req.Tool.MessageID,
	}
}

func permissionHermesToolState(req nativehermes.PermissionRequest) hermesToolState {
	title := req.Action
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

	action, owned, actionErr := s.announceBlockingAction(ctx, lifecycle.ActionPermission, req.ID)

	switch {
	case actionErr != nil:
		_, _, _ = s.takePendingPermission(req.ID)

		replyCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		return errors.Join(actionErr, s.client.ReplyPermission(replyCtx, req, valReject, valCancelled))
	case !owned:
		_, _, _ = s.takePendingPermission(req.ID)

		return s.rejectInvalidPermission(req, "permission callback arrived outside an accepted lifecycle turn", nil)
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
		// Route metadata stays off a permission: it is correlated by its structural
		// sessionId and toolCallId. The lifecycle correlation adds a stable action
		// name for the same held request without replacing either.
		Meta: actionMeta(map[string]any{
			hermesMetaKey: map[string]any{routeFieldReq: req.ID, "nativeSessionId": req.SessionID},
		}, action),
	})
	if err != nil {
		resolveErr := s.resolveBlockingAction(ctx, action, lifecycle.ActionFailed)

		_, ok, cancelled := s.takePendingPermission(req.ID)
		if !ok {
			return errors.Join(resolveErr, errPromptCancelled)
		}

		if resolveErr != nil {
			return resolveErr
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

	// The blocker terminalizes before the transition that unblocks its cycle, and
	// the outcome is read only from the structural union the client answered with.
	if resolveErr := s.resolveBlockingAction(ctx, action, permissionActionState(resp, reply)); resolveErr != nil {
		return resolveErr
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

	action, owned, actionErr := s.announceBlockingAction(ctx, lifecycle.ActionElicitation, req.ID)

	switch {
	case actionErr != nil:
		_, _, _ = s.takePendingQuestion(req.ID)

		rejectCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		return errors.Join(actionErr, s.client.RejectQuestion(rejectCtx, req))
	case !owned:
		_, _, _ = s.takePendingQuestion(req.ID)

		return s.poisonMissingLiveSessionMapping(ctx, s.client.RejectQuestion(ctx, req))
	}

	request, propertyIDs := questionElicitationRequest(req)
	// A turn-scoped elicitation carries the reserved route object and the
	// lifecycle correlation side by side: route routes and authenticates the
	// callback, and the correlation names the same held request as an action.
	request.Form.Meta = actionMeta(request.Form.Meta, action)

	requestID := req.ID

	resp, err := conn.CreateElicitation(ctx, request, elicitationScope{
		SessionID: s.id,
		TurnNonce: s.currentTurnNonce(),
		RequestID: &requestID,
	})
	if err != nil {
		if resolveErr := s.resolveBlockingAction(ctx, action, lifecycle.ActionFailed); resolveErr != nil {
			return resolveErr
		}

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
		if resolveErr := s.resolveBlockingAction(ctx, action, lifecycle.ActionDeclined); resolveErr != nil {
			return resolveErr
		}

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

	if resolveErr := s.resolveBlockingAction(ctx, action, lifecycle.ActionAccepted); resolveErr != nil {
		return resolveErr
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

	// Serialize the candidate through delivery so successful notifications
	// cannot reorder. This stream is non-authoritative: the counter advances
	// only once a notification has actually been delivered, so an unsent number
	// is reused by the next event and what the consumer holds is contiguous
	// across the deliveries it received.
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
func usageUpdateFromTokens(tokens nativehermes.Tokens, size int) *acp.SessionUpdate {
	used := int(tokens.Total)
	if used <= 0 {
		used = int(tokens.Input + tokens.Output + tokens.Reasoning)
	}

	if used <= 0 {
		return nil
	}

	return &acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
		SessionUpdate: "usage_update",
		Used:          used,
		Size:          size,
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

// terminalOutcomeFromHermes maps one structured native finish to the ACP v1 stop
// reason it names and the outcome the settled cycle recorded. A clean native
// completion always has both. A failure never reaches here: no ACP v1 stop reason
// names a failure, so inventing one would report a turn that ended badly as a
// turn that ended.
//
// The finish vocabulary is closed, so an unrecognized or empty value is
// reported as unmapped rather than defaulted to a clean end of turn. Defaulting
// would state a completed cycle over a terminal this adapter cannot read, which
// is the one thing a settled boundary must never do; the caller fails the turn
// and lets the v1 error carry the cause instead.
func terminalOutcomeFromHermes(finish string) (acp.StopReason, lifecycle.Outcome, bool) {
	switch strings.ToLower(strings.TrimSpace(finish)) {
	case valStop, "end_turn", valCompleted, valDone:
		return acp.StopReasonEndTurn, lifecycle.OutcomeSuccess, true
	case valLength, "max_tokens":
		return acp.StopReasonMaxTokens, lifecycle.OutcomeLimit, true
	case "max_turn_requests", "max_turns":
		return acp.StopReasonMaxTurnRequests, lifecycle.OutcomeLimit, true
	case valCancelled, "canceled":
		return acp.StopReasonCancelled, lifecycle.OutcomeCancelled, true
	case "refusal", "content_filter":
		return acp.StopReasonRefusal, lifecycle.OutcomeRefused, true
	default:
		return "", lifecycle.OutcomeFailed, false
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
	switch tool {
	case "read_file", "skill_view", "skills_list", "browser_snapshot", "browser_vision", "browser_get_images", "vision_analyze":
		return acp.ToolKindRead
	case "write_file", "patch", "skill_manage":
		return acp.ToolKindEdit
	case "search_files":
		return acp.ToolKindSearch
	case valTerminal, "process", "execute_code", "browser_click", "browser_type", "browser_scroll", "browser_press", "browser_back",
		"delegate_task", "image_generate", "text_to_speech":
		return acp.ToolKindExecute
	case "web_search", "web_extract", "browser_navigate":
		return acp.ToolKindFetch
	case "_thinking":
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
	case valCompleted, valDone:
		return acp.PlanEntryStatusCompleted
	case "in_progress", "running":
		return acp.PlanEntryStatusInProgress
	default:
		return acp.PlanEntryStatusPending
	}
}
