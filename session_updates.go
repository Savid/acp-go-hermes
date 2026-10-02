package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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
	// heldCalls are call reports that arrived before the runtime learned the
	// context window of the model that served them; the next usage reading
	// states it.
	heldCalls []callReport
}

// callReport is one model response the plugin reported: its breakdown, the
// context it was sent with, which is the context Hermes counts after it, and
// the model that served it.
type callReport struct {
	usage wire.CallUsage
	used  int
	model string
}

// size is the context window of the model that served the call, as window,
// the latest reading that stated one, gives it; 0 while no reading has stated
// that model's window.
func (call callReport) size(window hermes.Usage) int {
	if call.model != window.Model {
		return 0
	}

	return int(window.ContextMax)
}

// usageReading is what one event reports about usage. A session.usage or
// message.complete carries the gateway's current usage and the consumption
// since its previous reading, and covered marks a reading whose responses
// call reports already reported. A call report carries call. Both carry
// window, the latest reading that stated a context window.
type usageReading struct {
	current  hermes.Usage
	consumed hermes.Usage
	covered  bool
	call     *callReport
	window   hermes.Usage
}

// readUsage records the usage a gateway event carries against the runtime's
// previous reading. Every reading on the runtime is recorded, owned by a cycle
// or not, so a cycle sums only the consumption that happened while it ran.
func (rt *runtime) readUsage(event hermes.Event) usageReading {
	switch event.Type {
	case hermes.CallEvent:
		call, ok := hermes.DecodeCall(event.Payload)
		if !ok {
			return usageReading{}
		}

		report, ok := reportCall(call.Usage, call.ResponseID)
		if !ok {
			return usageReading{}
		}

		report.model = call.Model
		rt.reported = append(rt.reported, int64(report.used))

		return usageReading{call: &report, window: rt.window}
	case eventSessionUsage, eventMessageComplete:
	default:
		return usageReading{}
	}

	current, ok := hermes.DecodeUsage(event.Payload)
	if !ok {
		return usageReading{}
	}

	if current.ContextMax > 0 {
		rt.window = current
	}

	reading := usageReading{current: current, consumed: current.Since(rt.usage), window: rt.window}
	rt.usage = current

	if reading.consumed.Prompt > 0 {
		reading.covered = rt.coverReading(reading.consumed.Prompt)
	}

	return reading
}

// coverReading reports whether the responses a reading recorded, whose prompt
// tokens sum to consumed, are a run of the reported calls, and forgets the
// reported calls up to the end of that run. Hermes records each response it
// accepts right after the plugin reports it, so a reported call before the
// run is one Hermes never recorded, such as a rejected attempt, and one after
// it is not recorded yet. A reading no run covers recorded a response no call
// report covers, and every reported call is forgotten.
func (rt *runtime) coverReading(consumed int64) bool {
	reported := rt.reported
	rt.reported = nil

	for start := range slices.Backward(reported) {
		var sum int64

		for end := start; end < len(reported) && sum < consumed; end++ {
			if sum += reported[end]; sum == consumed {
				rt.reported = reported[end+1:]

				return true
			}
		}
	}

	return false
}

// reportCall is a response's call report from the Chat Completions usage its
// gateway sent. The prompt tokens include those read from and written to a
// prompt cache, so the uncached input is what remains of them once the cache
// reads are known, less the cache writes where the gateway states them; the
// completion tokens include reasoning. Hermes counts the prompt tokens as the
// context the response leaves. A report stating no token, or no prompt, is no
// usable report.
func reportCall(usage hermes.ChatUsage, responseID string) (callReport, bool) {
	figure := func(value *int64) *int {
		if value == nil {
			return nil
		}

		return new(int(*value))
	}

	report := callReport{usage: wire.CallUsage{ResponseID: responseID, OutputTokens: figure(usage.CompletionTokens)}}

	if details := usage.PromptTokensDetails; details != nil {
		report.usage.CachedReadTokens = figure(details.CachedTokens)
		report.usage.CachedWriteTokens = figure(details.CacheWriteTokens)

		if usage.PromptTokens != nil && details.CachedTokens != nil {
			uncached := *usage.PromptTokens - *details.CachedTokens
			if details.CacheWriteTokens != nil {
				uncached -= *details.CacheWriteTokens
			}

			if uncached >= 0 {
				report.usage.InputTokens = new(int(uncached))
			}
		}
	}

	if usage.PromptTokens != nil {
		report.used = int(*usage.PromptTokens)
	}

	return report, report.used > 0 && report.usage.Known()
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

		errs = append(errs, s.emitResponseUsage(ctx, state, reading))

		return true, errors.Join(errs...)
	case eventSessionUsage:
		return false, s.emitResponseUsage(ctx, state, reading)
	case hermes.CallEvent:
		if reading.call == nil {
			return false, nil
		}

		size := reading.call.size(reading.window)
		if size == 0 {
			state.heldCalls = append(state.heldCalls, *reading.call)

			return false, nil
		}

		return false, s.emitCall(ctx, *reading.call, size)
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

// emitResponseUsage reports the calls held for a context window, then the
// context the responses a usage reading recorded leave occupied, unless call
// reports already reported those responses. size is the context window the
// same reading states, which Hermes sends whenever it sends the context.
func (s *session) emitResponseUsage(ctx context.Context, state *cycleState, reading usageReading) error {
	held := state.heldCalls
	state.heldCalls = nil

	for _, call := range held {
		if err := s.emitCall(ctx, call, call.size(reading.window)); err != nil {
			return err
		}
	}

	used, ok := contextTokens(reading)
	if !ok || reading.covered {
		return nil
	}

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: int(reading.current.ContextMax), Used: used}})
}

// emitCall reports one model response with its breakdown.
func (s *session) emitCall(ctx context.Context, call callReport, size int) error {
	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: size, Used: call.used, Meta: call.usage.Apply(nil)}})
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
