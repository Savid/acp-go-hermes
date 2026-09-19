package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	limitSessionPrompt = "session_prompt"

	stopReasonLength    = "length"
	stopReasonMaxTokens = "max_tokens"
	stopReasonError     = "error"
)

// nativePrompt is one mapped prompt: the message text plus attached images.
type nativePrompt struct {
	message string
	images  [][]byte
}

// mapPrompt converts ACP prompt content to hermes's prompt shape. Embedded
// context is appended to the message text; images run the core input gates
// and travel as inline base64.
func (s *session) mapPrompt(ctx context.Context, blocks []acp.ContentBlock) (nativePrompt, error) {
	if len(blocks) == 0 {
		return nativePrompt{}, wire.Unsupported("prompt")
	}

	decoded, refusal, err := image.ValidatePrompt(ctx, blocks, image.Options{
		Limits:           s.agent.options.ImageLimits.core(),
		HandoffRoot:      s.agent.options.InputHandoffRoot,
		TextBeforeImages: true,
	})
	if err != nil {
		return nativePrompt{}, err
	}

	if refusal != nil {
		return nativePrompt{}, refusal.InvalidParams()
	}

	textParts := make([]string, 0, len(blocks))
	contextParts := make([]string, 0)

	for _, block := range blocks {
		switch {
		case block.Text != nil:
			if wire.AudienceIsUserOnly(block.Text.Annotations) {
				continue
			}

			textParts = append(textParts, block.Text.Text)
		case block.Image != nil:
		case block.ResourceLink != nil:
			textParts = append(textParts, strings.TrimSpace(block.ResourceLink.Uri))
		case block.Resource != nil:
			if blob := block.Resource.Resource.BlobResourceContents; blob != nil {
				if blob.MimeType == nil || !image.IsImageMIME(*blob.MimeType) {
					textParts = append(textParts, strings.TrimSpace(blob.Uri))
				}
			}

			if text := block.Resource.Resource.TextResourceContents; text != nil {
				textParts = append(textParts, strings.TrimSpace(text.Uri))
				contextParts = append(contextParts, wire.ContextResourceText(text.Uri, text.Text))
			}
		default:
			return nativePrompt{}, wire.Unsupported("prompt")
		}
	}

	prompt := nativePrompt{
		message: strings.Join(append(textParts, contextParts...), "\n"),
		images:  make([][]byte, 0, len(decoded)),
	}

	for index := range decoded {
		if decoded[index].Field == image.FieldPromptResource && !image.IsImageMIME(decoded[index].MIME) {
			continue
		}

		prompt.images = append(prompt.images, decoded[index].Data)
	}

	if strings.TrimSpace(prompt.message) == "" && len(prompt.images) == 0 {
		return nativePrompt{}, wire.Unsupported("prompt")
	}

	return prompt, nil
}

// prompt sends one turn to hermes and streams updates until the run settles.
func (s *session) prompt(ctx context.Context, params acp.PromptRequest, raw json.RawMessage) (acp.PromptResponse, error) {
	meta := lifecycle.RetainRequestMetadata(params.Meta, raw)

	submission, paramErr := lifecycle.DecodePromptCorrelation(meta, s.lifecycleNegotiated())
	if paramErr != nil {
		return acp.PromptResponse{}, wire.ParamRefusal(paramErr)
	}

	if err := s.admissionError(); err != nil {
		return acp.PromptResponse{}, err
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()

	// The turn owns its own cancellation. The SDK cancels this request's
	// context whenever another prompt arrives for the same session, so the
	// request context cannot decide whether this turn was cancelled; only
	// cancel, timeout, and close do.
	turnCtx, cancelTurn := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelTurn()

	t := &turn{
		cycle:      cycle{Cycle: lifecycle.Cycle{Origin: lifecycle.CauseSubmission}, state: cycleState{}},
		submission: submission,
		cancel:     cancelTurn,
		settled:    make(chan struct{}),
		finished:   make(chan struct{}),
		ready:      make(chan struct{}),
	}
	defer close(t.finished)

	// The turn is installed before every piece of request-scoped work a
	// session/cancel must be able to interrupt: mapping, the lazy relaunch and
	// the image upload all run under turnCtx.
	s.mu.Lock()
	if s.cycle != nil || s.closing {
		s.mu.Unlock()

		return acp.PromptResponse{}, wire.Backpressure(limitSessionPrompt)
	}

	s.turn = t
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.turn == t {
			s.turn = nil
		}
		s.mu.Unlock()
	}()

	ready := sync.OnceFunc(func() { close(t.ready) })
	defer ready()

	mapped, err := s.mapPrompt(turnCtx, params.Prompt)
	if err != nil {
		if turnCtx.Err() != nil {
			return wire.CancelledResponse(params), nil
		}

		return acp.PromptResponse{}, err
	}

	// A cancel that lands before native dispatch creates no native turn and
	// publishes no acceptance.
	if turnCtx.Err() != nil {
		return wire.CancelledResponse(params), nil
	}

	rt, err := s.ensureRuntime(turnCtx)
	if err != nil {
		if turnCtx.Err() != nil {
			return wire.CancelledResponse(params), nil
		}

		return acp.PromptResponse{}, err
	}

	s.mu.Lock()
	t.floor = rt.client.Sequence()
	s.mu.Unlock()

	for _, data := range mapped.images {
		if err := rt.client.AttachImageBytes(turnCtx, rt.liveID, data); err != nil {
			ready()
			// Hermes queues attachments for the next prompt.submit, so a
			// half-uploaded prompt must not outlive this turn.
			s.stopRuntime(context.WithoutCancel(ctx), rt)

			if turnCtx.Err() != nil {
				return wire.CancelledResponse(params), nil
			}

			return acp.PromptResponse{}, s.dispatchFailure(context.WithoutCancel(ctx), rt, err)
		}
	}

	result, watermark, uncertain, submitErr := rt.client.SubmitPromptWatermark(turnCtx, rt.liveID, mapped.message)

	t.disposition, t.watermark = result.Status, watermark
	if submitErr == nil && result.Status == promptStreaming {
		s.acceptTurn(turnCtx, t)
	}

	ready()

	if submitErr != nil {
		if uncertain {
			s.stopRuntime(context.WithoutCancel(ctx), rt)
		}

		if turnCtx.Err() != nil {
			return wire.CancelledResponse(params), nil
		}

		return acp.PromptResponse{}, s.dispatchFailure(context.WithoutCancel(ctx), rt, submitErr)
	}

	switch result.Status {
	case promptStreaming, promptQueued:
	case "redirected", "steered":
		return acp.PromptResponse{}, wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: "prompt absorbed by the running native turn; do not resubmit"})
	default:
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return acp.PromptResponse{}, wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: "unknown native prompt disposition: " + result.Status})
	}

	select {
	case <-t.settled:
	case <-turnCtx.Done():
		s.cancel(context.WithoutCancel(ctx))

		select {
		case <-t.settled:
		case <-time.After(sessionAbortTimeout):
			s.stopRuntime(context.WithoutCancel(ctx), rt)
			t.settle(turnTransportEnded)
		}
	}

	return s.settleTurn(ctx, rt, t, params)
}

// dispatchFailure classifies a prompt command hermes never accepted: a native
// rejection carries its text as a provider failure, a dead child is a
// process exit, and everything else is transport.
func (s *session) dispatchFailure(ctx context.Context, rt *runtime, err error) error {
	var commandErr *hermes.RPCError
	if errors.As(err, &commandErr) {
		return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: commandErr.Message})
	}

	return s.transportFailure(ctx, rt, err)
}

// transportFailure recovers the real cause behind a lost native stream: the
// child's exit status and last stderr line where it died, otherwise the
// transport error.
func (s *session) transportFailure(ctx context.Context, rt *runtime, err error) error {
	return wire.TurnFailed(vendor, wire.TransportFailure(ctx, rt.proc, "hermes process", err, rt.client.Err))
}

// cycleVerdict is how one cycle ended, in the terms the lifecycle stream and
// the prompt response need.
type cycleVerdict struct {
	outcome    lifecycle.Outcome
	stopReason string
	failure    error
}

// judgeCycle records how a natively settled cycle finished. The cancel guard
// runs before every failure mapping.
func (s *session) judgeCycle(c *cycle, cancelled bool) cycleVerdict {
	failure := s.cycleFailure(c)

	switch {
	case cancelled:
		return cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case failure != nil:
		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: failure}
	case c.state.stopReason == stopReasonError:
		message := strings.TrimSpace(c.state.errorMessage)
		if message == "" {
			message = "hermes reported a turn error"
		}

		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: message})}
	}

	stop := acp.StopReasonEndTurn
	outcome := lifecycle.OutcomeSuccess

	switch c.state.stopReason {
	case stopReasonLength, stopReasonMaxTokens:
		stop = acp.StopReasonMaxTokens
		outcome = lifecycle.OutcomeLimit
	case statusInterrupted:
		stop = acp.StopReasonCancelled
		outcome = lifecycle.OutcomeCancelled
	case statusComplete:
	default:
		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: "unknown native finish status: " + c.state.stopReason})}
	}

	return cycleVerdict{outcome: outcome, stopReason: string(stop)}
}

// settleTurn is the one settlement point every accepted prompt reaches:
// usage and session info, the durable mirror commit, the terminal idle, and
// only then the response or error.
func (s *session) settleTurn(ctx context.Context, rt *runtime, t *turn, params acp.PromptRequest) (acp.PromptResponse, error) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	s.mu.Lock()
	cancelled := t.cancelled
	// The generation that ran this turn decides the fence: a turn that reached
	// its own terminal result never fences, so a gateway lost afterwards would
	// otherwise leave the incarnation open for the next process.
	generationLost := s.runtime != rt
	s.mu.Unlock()

	var verdict cycleVerdict

	switch {
	case cancelled:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case t.ended == turnTransportEnded:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: s.transportFailure(settleCtx, rt, nil)}
	default:
		verdict = s.judgeCycle(&t.cycle, false)
	}

	if t.ended == turnSettled {
		if !cancelled {
			s.emitUsage(settleCtx, &t.state)
			s.emitSessionInfo(settleCtx, params.Prompt)
		}

		if err := s.commitMirror(settleCtx, rt); err != nil {
			s.stopRuntime(settleCtx, rt)
			s.lc.Fence()
			verdict.failure = s.mirrorFailure(err)
			verdict.outcome = lifecycle.OutcomeFailed
		}
	}

	if err := s.lc.Idle(settleCtx, t.Cycle, verdict.stopReason, verdict.outcome); err != nil && verdict.failure == nil {
		verdict.failure = err
	}

	if t.ended == turnTransportEnded || generationLost {
		s.lc.Fence()
	}

	if verdict.failure != nil {
		return acp.PromptResponse{}, verdict.failure
	}

	return acp.PromptResponse{
		StopReason:    acp.StopReason(verdict.stopReason),
		UserMessageId: params.MessageId,
	}, nil
}

// mirrorFailure reports a failed durability boundary without exposing native content.
func (s *session) mirrorFailure(err error) error {
	s.agent.log.Error("session mirror commit failed", slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))

	return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "session mirror commit failed"})
}
