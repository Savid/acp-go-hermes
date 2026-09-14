package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

// handleControl reserves callbacks in wire order and preserves native request
// identity. Requests without an id are answered in FIFO order.
func (s *session) handleControl(ctx context.Context, rt *runtime, c *cycle, event hermes.Event) {
	id := fmt.Sprintf("native-%d", event.InboundSequence)
	callbackCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	release := s.registerDialog(id, cancel)

	job := func() {
		defer release()
		defer cancel(nil)

		method := ""
		params := map[string]any{nativeSessionIDKey: rt.liveID}

		switch event.Type {
		case eventApprovalRequest:
			method = "approval.respond"
			params["choice"] = s.requestPermission(callbackCtx, c, id, event.Payload)

			params["all"] = false
			if requestID := hermes.String(event.Payload, "request_id"); requestID != "" {
				params["request_id"] = requestID
			}
		case eventClarifyRequest:
			method = "clarify.respond"
			params["request_id"] = hermes.String(event.Payload, "request_id")
			params["answer"] = s.elicit(callbackCtx, c, event.Payload)
		case "sudo.request":
			method = "sudo.respond"
			params["password"] = ""
		case "secret.request":
			method = "secret.respond"
			params[fieldValue] = ""
		case "terminal.read_request":
			method = "terminal.read.respond"
			params[fieldText] = ""
		}

		replyCtx, replyCancel := context.WithTimeout(context.WithoutCancel(ctx), sessionAbortTimeout)
		defer replyCancel()

		if err := rt.client.Call(replyCtx, method, params, nil); err != nil {
			rt.cancel()
			_ = rt.proc.Kill()
		}
	}
	select {
	case rt.controls <- job:
	default:
		cancel(errDialogCancelled)
		release()
		rt.cancel()
		_ = rt.proc.Kill()
	}
}

func (s *session) requestPermission(ctx context.Context, c *cycle, id string, payload json.RawMessage) string {
	const deny = "deny"

	conn := s.agent.connection()
	if conn == nil || ctx.Err() != nil {
		return deny
	}

	var request struct {
		Command string   `json:"command"`
		Choices []string `json:"choices"`
	}
	if json.Unmarshal(payload, &request) != nil {
		return deny
	}

	options := []acp.PermissionOption{}

	if len(request.Choices) == 0 {
		request.Choices = []string{deny}
	}

	for _, choice := range request.Choices {
		kind := acp.PermissionOptionKindRejectOnce

		switch choice {
		case approvalOnce:
			kind = acp.PermissionOptionKindAllowOnce
		case nativeScopeSession, approvalAlways:
			kind = acp.PermissionOptionKindAllowAlways
		case deny:
		default:
			continue
		}

		options = append(options, acp.PermissionOption{OptionId: acp.PermissionOptionId(choice), Name: choice, Kind: kind})
	}

	title := request.Command
	if title == "" {
		title = "Hermes approval"
	}

	if err := s.emit(ctx, acp.StartToolCall(acp.ToolCallId(id), title, acp.WithStartStatus(acp.ToolCallStatusPending))); err != nil {
		return deny
	}

	ctx, finish := s.agent.observe.StartPermission(ctx, title, "native")
	response, err := announcedRequest(ctx, s, c, lifecycle.ActionPermission,
		func(ctx context.Context, meta map[string]any) (acp.RequestPermissionResponse, error) {
			status := acp.ToolCallStatusPending

			return conn.RequestPermission(ctx, acp.RequestPermissionRequest{Meta: meta, SessionId: s.id, ToolCall: acp.ToolCallUpdate{ToolCallId: acp.ToolCallId(id), Title: &title, Status: &status}, Options: options})
		}, func(response acp.RequestPermissionResponse, err error) lifecycle.ActionState {
			if err != nil {
				return lifecycle.ActionFailed
			}

			if response.Outcome.Selected != nil {
				if choice := string(response.Outcome.Selected.OptionId); choice == approvalOnce || choice == nativeScopeSession || choice == approvalAlways {
					return lifecycle.ActionAccepted
				}

				return lifecycle.ActionDeclined
			}

			return lifecycle.ActionCancelled
		})

	choice := deny
	if err == nil && ctx.Err() == nil && response.Outcome.Selected != nil && slices.Contains(request.Choices, string(response.Outcome.Selected.OptionId)) {
		choice = string(response.Outcome.Selected.OptionId)
	}

	status := acp.ToolCallStatusFailed
	if choice == approvalOnce || choice == nativeScopeSession || choice == approvalAlways {
		status = acp.ToolCallStatusCompleted
	}

	_ = s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(id), acp.WithUpdateStatus(status)))

	finish(observer.PermissionResult{Behavior: choice, Mode: "native", ToolName: title})

	return choice
}

func (s *session) elicit(ctx context.Context, c *cycle, payload json.RawMessage) any {
	conn := s.agent.connection()
	if conn == nil || !s.agent.clientSupportsFormElicitation() || ctx.Err() != nil {
		return nil
	}

	var request struct {
		clarifyQuestion
		Questions []clarifyQuestion `json:"questions"`
	}

	if json.Unmarshal(payload, &request) != nil {
		return nil
	}

	schema := acp.UnstableElicitationSchema{Type: acp.UnstableElicitationSchemaTypeObject, Properties: map[string]any{}}
	if len(request.Questions) == 0 {
		schema.Properties["answer"] = request.schema()
		schema.Required = []string{"answer"}
	} else {
		for _, question := range request.Questions {
			schema.Properties[question.ID] = question.schema()
			schema.Required = append(schema.Required, question.ID)
		}
	}

	message := strings.TrimSpace(request.Question)
	if message == "" {
		message = "Hermes needs input"
	}

	ctx, finish := s.agent.observe.StartElicitation(ctx)
	response, err := announcedRequest(ctx, s, c, lifecycle.ActionElicitation,
		func(ctx context.Context, meta map[string]any) (acp.UnstableCreateElicitationResponse, error) {
			return conn.UnstableCreateElicitation(ctx, acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{Meta: meta, Mode: "form", Message: message, RequestedSchema: schema}})
		}, func(response acp.UnstableCreateElicitationResponse, err error) lifecycle.ActionState {
			if err != nil {
				return lifecycle.ActionFailed
			}

			if response.Accept != nil {
				return lifecycle.ActionAccepted
			}

			if response.Decline != nil {
				return lifecycle.ActionDeclined
			}

			return lifecycle.ActionCancelled
		})
	finish(observer.ElicitationResult{Accepted: err == nil && response.Accept != nil, Err: err})

	if err != nil || ctx.Err() != nil || response.Accept == nil {
		return nil
	}

	if len(request.Questions) == 0 {
		return request.answer(response.Accept.Content["answer"])
	}

	answers := make(map[string]any, len(request.Questions))
	for _, question := range request.Questions {
		value := question.answer(response.Accept.Content[question.ID])
		if value == nil {
			return nil
		}

		answers[question.ID] = value
	}

	return answers
}

// announcedRequest sends one client request that holds native work, announces
// the action it answers once the request is on the wire, and resolves that
// action exactly once.
func announcedRequest[T any](
	ctx context.Context,
	s *session,
	c *cycle,
	kind lifecycle.ActionKind,
	send func(context.Context, map[string]any) (T, error),
	resolved func(T, error) lifecycle.ActionState,
) (T, error) {
	var zero T

	releaseCall, err := s.agent.acquireClientCall()
	if err != nil {
		return zero, err
	}
	defer releaseCall()

	actionID, err := s.reserveAction(c)
	if err != nil {
		return zero, err
	}

	if actionID == "" {
		return send(ctx, nil)
	}

	type answer struct {
		value T
		err   error
	}

	answers := make(chan answer, 1)

	var written <-chan struct{}
	if t := s.agent.transportRef(); t != nil {
		written = t.AwaitRequestWrite(actionID)
	}

	go func() {
		value, err := send(ctx, s.actionCorrelation(c, actionID))
		answers <- answer{value: value, err: err}
	}()

	if written != nil {
		select {
		case <-written:
		case result := <-answers:
			answers <- result
		}
	}

	if err := s.lcActionPendingWithID(ctx, c, actionID, kind); err != nil {
		s.agent.log.ErrorContext(ctx, "announce lifecycle action failed",
			slog.String(nativeSessionIDKey, string(s.id)), slog.String("reason", err.Error()))
	}

	result := <-answers
	state := resolved(result.value, result.err)

	if result.err != nil && errors.Is(context.Cause(ctx), errDialogCancelled) {
		state = lifecycle.ActionCancelled
	}

	if err := s.lcActionResolved(context.WithoutCancel(ctx), c, actionID, state); err != nil {
		s.agent.log.ErrorContext(ctx, "resolve lifecycle action failed",
			slog.String(nativeSessionIDKey, string(s.id)), slog.String("reason", err.Error()))
	}

	return result.value, result.err
}

func (s *session) reserveAction(c *cycle) (string, error) {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if s.lc.stream == nil || s.lc.stream.Fenced() || c.turnID == "" {
		return "", nil
	}

	return s.nextLifecycleID("action"), nil
}

//nolint:tagliatelle // Native clarify payloads use multi_select.
type clarifyQuestion struct {
	ID       string   `json:"qid"`
	Question string   `json:"question"`
	Choices  []string `json:"choices"`
	Multi    bool     `json:"multi_select"`
}

func (q clarifyQuestion) schema() map[string]any {
	value := map[string]any{fieldType: "string", "title": q.Question}
	if len(q.Choices) == 0 {
		return value
	}

	if q.Multi {
		return map[string]any{fieldType: "array", "title": q.Question, "minItems": 1, "uniqueItems": true, "items": map[string]any{fieldType: "string", "enum": q.Choices}}
	}

	value["enum"] = q.Choices

	return value
}

func (q clarifyQuestion) answer(value any) any {
	if !q.Multi || len(q.Choices) == 0 {
		text, ok := value.(string)
		if !ok || (len(q.Choices) != 0 && !slices.Contains(q.Choices, text)) {
			return nil
		}

		return text
	}

	values, ok := value.([]any)
	if !ok || len(values) == 0 {
		return nil
	}

	selected := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok || !slices.Contains(q.Choices, text) || slices.Contains(selected, text) {
			return nil
		}

		selected = append(selected, text)
	}

	data, _ := json.Marshal(selected)

	return string(data)
}
