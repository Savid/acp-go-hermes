package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	eventMessageComplete = "message.complete"
	eventMessageStart    = "message.start"
	eventMessageDelta    = "message.delta"
	eventMessageInterim  = "message.interim"
	eventSessionUsage    = "session.usage"
	statusComplete       = "complete"
	fieldSource          = "source"
	promptStreaming      = "streaming"
	promptQueued         = "queued"
	fieldName            = "name"
	fieldResult          = "result"
	eventToolComplete    = "tool.complete"
	fieldToolID          = "tool_id"
	fieldID              = "id"
	nativeSource         = "desktop"
	approvalOnce         = "once"
	eventToolStart       = "tool.start"
	nativeScopeSession   = "session"
	fieldCwd             = "cwd"
	nativeSessionIDKey   = "session_id"
	effortMedium         = "medium"
	eventApprovalRequest = "approval"
	eventClarifyRequest  = "clarify"
	fieldValue           = "value"
	fieldText            = "text"
	approvalAlways       = "always"
	fieldType            = "type"
	statusInterrupted    = "interrupted"
	roleUser             = "user"
	roleAssistant        = "assistant"

	eventSudoRequest         = "sudo"
	eventSecretRequest       = "secret"
	eventTerminalReadRequest = "terminal.read"
)

type cycleState struct {
	text    strings.Builder
	thought strings.Builder
	// openTools holds the tool calls started and not yet complete; a
	// completed call leaves it, since Hermes completes each call once.
	openTools    map[string]struct{}
	stopReason   string
	errorMessage string
	// usage sums the tokens of every provider response the cycle's usage
	// readings recorded.
	usage hermes.Usage
}

// usageReading is what one session.usage or message.complete reports: the
// gateway's current usage and the consumption since its previous reading.
type usageReading struct {
	current  hermes.Usage
	consumed hermes.Usage
}

// readUsage records the usage a gateway event carries against the runtime's
// previous reading. Every reading on the runtime is recorded, owned by a cycle
// or not, so a cycle sums only the consumption that happened while it ran.
func (rt *runtime) readUsage(event hermes.Event) usageReading {
	if event.Type != eventSessionUsage && event.Type != eventMessageComplete {
		return usageReading{}
	}

	current, ok := hermes.DecodeUsage(event.Payload)
	if !ok {
		return usageReading{}
	}

	reading := usageReading{current: current, consumed: current.Since(rt.usage)}
	rt.usage = current

	return reading
}

// bearsWork reports whether an event is native work a lifecycle cycle must
// own. Every kind projectEvent acts on is listed: a native dialog that arrives
// outside a prompt is answered only if it opens an agent-origin cycle first.
func bearsWork(event hermes.Event) bool {
	switch event.Type {
	case eventMessageStart, eventMessageDelta, eventMessageComplete, "thinking.delta",
		eventMessageInterim,
		eventToolStart, eventToolComplete, eventApprovalRequest, eventClarifyRequest,
		eventSudoRequest, eventSecretRequest, eventTerminalReadRequest, stopReasonError:
		return true
	default:
		return false
	}
}

// projectEvent translates one ordered gateway event, with the usage reading
// it carries, into ACP updates.
func (s *session) projectEvent(ctx context.Context, rt *runtime, c *cycle, event hermes.Event, reading usageReading) (bool, error) {
	// Responses that finish after a cancel still count toward the cycle's
	// consumption, though the cycle reports nothing more.
	c.state.usage = c.state.usage.Add(reading.consumed)

	if s.cycleCancelled(c) {
		switch event.Type {
		case eventApprovalRequest, eventClarifyRequest, eventSudoRequest, eventSecretRequest, eventTerminalReadRequest:
			s.handleControl(ctx, rt, c, event)
		}

		return event.Type == eventMessageComplete || event.Type == stopReasonError, nil
	}

	state := &c.state

	switch event.Type {
	case eventMessageDelta:
		text := hermes.String(event.Payload, fieldText)
		state.text.WriteString(text)

		return false, s.emit(ctx, acp.UpdateAgentMessageText(text))
	case "thinking.delta":
		text := hermes.String(event.Payload, fieldText)
		state.thought.WriteString(text)

		return false, s.emit(ctx, acp.UpdateAgentThoughtText(text))
	case eventMessageInterim:
		//nolint:tagliatelle // Hermes uses already_streamed on the wire.
		var interim struct {
			Text            string `json:"text"`
			AlreadyStreamed bool   `json:"already_streamed"`
		}
		if err := json.Unmarshal(event.Payload, &interim); err != nil {
			return false, err
		}

		text := interim.Text
		if interim.AlreadyStreamed {
			text = wire.UnstreamedSuffix(state.text.String(), text)
		}

		state.text.Reset()

		if text != "" {
			return false, s.emit(ctx, acp.UpdateAgentMessageText(text))
		}

		return false, nil
	case eventMessageComplete:
		var result struct {
			Text      string `json:"text"`
			Status    string `json:"status"`
			Error     string `json:"error"`
			Reasoning string `json:"reasoning"`
		}
		if err := json.Unmarshal(event.Payload, &result); err != nil {
			return true, err
		}

		state.stopReason = result.Status

		state.errorMessage = result.Error
		if state.stopReason == stopReasonError && state.errorMessage == "" {
			state.errorMessage = result.Text
		}

		var errs []error
		if text := wire.UnstreamedSuffix(state.text.String(), result.Text); text != "" {
			errs = append(errs, s.emit(ctx, acp.UpdateAgentMessageText(text)))
		}

		if thought := completionSuffix(result.Reasoning, state.thought.String()); thought != "" {
			errs = append(errs, s.emit(ctx, acp.UpdateAgentThoughtText(thought)))
		}

		errs = append(errs, s.emitResponseUsage(ctx, reading))

		return true, errors.Join(errs...)
	case eventSessionUsage:
		return false, s.emitResponseUsage(ctx, reading)
	case stopReasonError:
		state.stopReason = stopReasonError
		state.errorMessage = hermes.String(event.Payload, "message")

		return true, nil
	case eventToolStart, eventToolComplete:
		if event.Type == eventToolStart {
			state.text.Reset()
		}

		return false, s.emitTool(ctx, state, event)
	case eventApprovalRequest, eventClarifyRequest, eventSudoRequest, eventSecretRequest, eventTerminalReadRequest:
		s.handleControl(ctx, rt, c, event)
	}

	return false, nil
}

func completionSuffix(complete, streamed string) string {
	if suffix, ok := strings.CutPrefix(complete, streamed); ok {
		return suffix
	}

	if strings.HasSuffix(strings.TrimSpace(streamed), strings.TrimSpace(complete)) {
		return ""
	}

	return complete
}

//nolint:tagliatelle // Hermes uses tool_id on the wire.
func (s *session) emitTool(ctx context.Context, state *cycleState, event hermes.Event) error {
	var tool struct {
		ID      string `json:"tool_id"`
		Name    string `json:"name"`
		Args    any    `json:"args"`
		Result  any    `json:"result"`
		Summary string `json:"summary"`
	}
	if err := json.Unmarshal(event.Payload, &tool); err != nil {
		return err
	}

	if tool.ID == "" {
		return errors.New("native tool identity missing")
	}

	if state.openTools == nil {
		state.openTools = make(map[string]struct{})
	}

	if _, open := state.openTools[tool.ID]; !open {
		if err := s.emit(ctx, acp.StartToolCall(acp.ToolCallId(tool.ID), tool.Name, acp.WithStartStatus(acp.ToolCallStatusInProgress), acp.WithStartRawInput(tool.Args))); err != nil {
			return err
		}

		state.openTools[tool.ID] = struct{}{}
	}

	if event.Type == eventToolStart {
		return nil
	}

	delete(state.openTools, tool.ID)

	status := acp.ToolCallStatusCompleted

	if result, ok := tool.Result.(map[string]any); ok {
		if result[stopReasonError] != nil || result["success"] == false {
			status = acp.ToolCallStatusFailed
		}
	}

	content := []acp.ToolCallContent{}

	if tool.Result != nil {
		if text, ok := tool.Result.(string); ok {
			content = append(content, acp.ToolContent(acp.TextBlock(text)))
		} else if data, err := json.Marshal(tool.Result); err == nil {
			content = append(content, acp.ToolContent(acp.TextBlock(string(data))))
		}
	} else if tool.Summary != "" {
		content = append(content, acp.ToolContent(acp.TextBlock(tool.Summary)))
	}

	opts := []acp.ToolCallUpdateOpt{acp.WithUpdateStatus(status), acp.WithUpdateRawOutput(tool.Result)}
	if len(content) > 0 {
		opts = append(opts, acp.WithUpdateContent(content))
	}

	return s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(tool.ID), opts...))
}

func (s *session) emit(ctx context.Context, updates ...acp.SessionUpdate) error {
	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	for _, update := range updates {
		if err := conn.SessionUpdate(context.WithoutCancel(ctx), acp.SessionNotification{SessionId: s.id, Update: update}); err != nil {
			return err
		}
	}

	return nil
}

// contextTokens is the context Hermes counts after the provider responses a
// usage reading recorded: the prompt tokens of the latest, which Hermes
// compacts against. A reading that recorded no response with usage, or that
// carries no context because Hermes compacted since or does not know the
// model's window, has no usable figure. A response whose usage is all zero,
// as a gateway's response-cache replay reports, moves no counter and makes
// Hermes drop the context, so it never reports 0.
func contextTokens(reading usageReading) (int, bool) {
	if reading.consumed.Prompt == 0 || reading.current.ContextUsed <= 0 {
		return 0, false
	}

	return int(reading.current.ContextUsed), true
}

// emitResponseUsage reports the context the responses a usage reading
// recorded leave occupied. size is the context window the same reading
// states, which Hermes sends whenever it sends the context.
func (s *session) emitResponseUsage(ctx context.Context, reading usageReading) error {
	used, ok := contextTokens(reading)
	if !ok {
		return nil
	}

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: int(reading.current.ContextMax), Used: used}})
}

// promptUsage is a turn's summed consumption: the prompt tokens of every
// provider response, cached ones included, and their completion tokens.
func promptUsage(consumed hermes.Usage) *acp.Usage {
	if consumed == (hermes.Usage{}) {
		return nil
	}

	usage := &acp.Usage{InputTokens: int(consumed.Prompt), OutputTokens: int(consumed.Completion), TotalTokens: int(consumed.Total)}
	if consumed.Reasoning > 0 {
		usage.ThoughtTokens = new(int(consumed.Reasoning))
	}

	return usage
}

func (s *session) emitRawEvent(ctx context.Context, event hermes.Event) {
	if !s.rawEvents.Enabled() {
		return
	}

	conn := s.agent.connection()
	if conn == nil {
		return
	}

	var payload map[string]any
	if json.Unmarshal(event.Raw, &payload) != nil {
		return
	}

	if err := s.rawEvents.Emit(ctx, func(ctx context.Context, method string, params map[string]any) error {
		return conn.NotifyExtension(ctx, method, params)
	}, payload); err != nil {
		s.agent.observe.RecordRawMessageEmitFailure(ctx, err)
	}
}

func (s *session) emitSessionInfo(ctx context.Context, prompt []acp.ContentBlock) {
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	update := acp.SessionSessionInfoUpdate{UpdatedAt: &updatedAt}

	s.mu.Lock()
	s.updatedAt = updatedAt

	if s.title == "" {
		if title := wire.PromptTitle(prompt); title != "" {
			s.title = title
			update.Title = &title
		}
	}
	s.mu.Unlock()

	_ = s.emit(ctx, acp.SessionUpdate{SessionInfoUpdate: &update})
}

func (s *session) sessionInfo() acp.SessionInfo {
	s.mu.Lock()
	title := s.title
	updatedAt := s.updatedAt
	s.mu.Unlock()

	if title == "" {
		title = string(s.id)
	}

	info := acp.SessionInfo{
		Meta:                  wire.NativeSessionMeta(vendor, s.nativeID),
		SessionId:             s.id,
		Title:                 &title,
		Cwd:                   s.cwd,
		AdditionalDirectories: append([]string(nil), s.additionalDirectories...),
	}
	if updatedAt != "" {
		info.UpdatedAt = &updatedAt
	}

	return info
}
