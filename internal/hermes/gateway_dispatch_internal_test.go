package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestGatewayCycleStateBudgetsAreIndependentOfMailbox(t *testing.T) {
	t.Run("text bytes", func(t *testing.T) {
		cycle := &gatewayCycle{}
		if err := cycle.chargeText(strings.Repeat("x", gatewayCycleTextByteLimit)); err != nil {
			t.Fatal(err)
		}
		if err := cycle.chargeText("x"); !errors.Is(err, ErrGatewayCycleOverflow) {
			t.Fatalf("limit+1 text = %v", err)
		}
		if cycle.textBytes != gatewayCycleTextByteLimit {
			t.Fatalf("retained text charge = %d", cycle.textBytes)
		}
	})

	t.Run("tool count and data", func(t *testing.T) {
		cycle := &gatewayCycle{}
		for range gatewayCycleToolCountLimit {
			if err := cycle.chargeTool(Event{}); err != nil {
				t.Fatal(err)
			}
		}
		if err := cycle.chargeTool(Event{}); !errors.Is(err, ErrGatewayCycleOverflow) {
			t.Fatalf("limit+1 tool = %v", err)
		}
		if cycle.toolCount != gatewayCycleToolCountLimit {
			t.Fatalf("retained tool count = %d", cycle.toolCount)
		}

		dataCycle := &gatewayCycle{}
		if err := dataCycle.chargeTool(Event{Payload: json.RawMessage(strings.Repeat("x", gatewayCycleToolDataByteLimit))}); err != nil {
			t.Fatal(err)
		}
		if err := dataCycle.chargeTool(Event{Payload: json.RawMessage("x")}); !errors.Is(err, ErrGatewayCycleOverflow) {
			t.Fatalf("limit+1 tool data = %v", err)
		}
		if dataCycle.toolDataBytes != gatewayCycleToolDataByteLimit {
			t.Fatalf("retained tool bytes = %d", dataCycle.toolDataBytes)
		}
	})

	t.Run("control count map and collision", func(t *testing.T) {
		cycle := &gatewayCycle{controls: make(map[gatewayControlIdentity]struct{})}
		for index := range gatewayCycleControlCountLimit {
			id := fmt.Sprintf("request-%d", index)
			if err := cycle.registerControl(Event{}, id, gatewayControlQuestion); err != nil {
				t.Fatal(err)
			}
		}
		delete(cycle.controls, gatewayControlIdentity{kind: gatewayControlQuestion, requestID: "request-0"})
		if err := cycle.registerControl(Event{}, "limit+1", gatewayControlQuestion); !errors.Is(err, ErrGatewayCycleOverflow) {
			t.Fatalf("cumulative control limit+1 = %v", err)
		}
		if cycle.controlCount != gatewayCycleControlCountLimit || len(cycle.controls) != gatewayCycleControlMapLimit-1 {
			t.Fatalf("bounded controls count=%d map=%d", cycle.controlCount, len(cycle.controls))
		}

		collision := &gatewayCycle{controls: make(map[gatewayControlIdentity]struct{})}
		if err := collision.registerControl(Event{}, "same", gatewayControlQuestion); err != nil {
			t.Fatal(err)
		}
		if err := collision.registerControl(Event{}, "same", gatewayControlQuestion); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("duplicate control = %v", err)
		}
		if err := collision.registerControl(Event{}, "same", gatewayControlPermission); err != nil {
			t.Fatalf("kind-qualified control collided: %v", err)
		}
		if len(collision.controls) != 2 || collision.controlCount != 2 {
			t.Fatalf("kind-qualified controls count=%d map=%d", collision.controlCount, len(collision.controls))
		}

		dataCycle := &gatewayCycle{controls: make(map[gatewayControlIdentity]struct{})}
		if err := dataCycle.registerControl(Event{Payload: json.RawMessage(strings.Repeat("x", gatewayCycleControlDataByteLimit))}, "data", gatewayControlQuestion); err != nil {
			t.Fatal(err)
		}
		if err := dataCycle.registerControl(Event{Payload: json.RawMessage("x")}, "data+1", gatewayControlQuestion); !errors.Is(err, ErrGatewayCycleOverflow) {
			t.Fatalf("limit+1 control data = %v", err)
		}
		if dataCycle.controlDataBytes != gatewayCycleControlDataByteLimit || len(dataCycle.controls) != 1 {
			t.Fatalf("bounded control data bytes=%d map=%d", dataCycle.controlDataBytes, len(dataCycle.controls))
		}
	})
}

func TestGatewayMappedControlAndCompletionLimitsFailClosed(t *testing.T) {
	_, actor := newDirectGatewayActor()
	toolStart := actor.newCycle(CycleOriginActivity)
	toolStart.toolCount = gatewayCycleToolCountLimit
	if err := actor.applyEvent(toolStart, Event{Type: evtToolStart}); !errors.Is(err, ErrGatewayCycleOverflow) {
		t.Fatalf("tool-start limit = %v", err)
	}
	toolComplete := actor.newCycle(CycleOriginActivity)
	toolComplete.toolCount = gatewayCycleToolCountLimit
	if err := actor.applyEvent(toolComplete, Event{Type: evtToolComplete}); !errors.Is(err, ErrGatewayCycleOverflow) {
		t.Fatalf("tool-complete limit = %v", err)
	}

	permission := actor.newCycle(CycleOriginActivity)
	permission.activeTools["tool"] = struct{}{}
	permission.controlCount = gatewayCycleControlCountLimit
	if err := actor.mapPermission(permission, Event{Payload: json.RawMessage(`{"command":"run"}`)}); !errors.Is(err, ErrGatewayCycleOverflow) {
		t.Fatalf("permission control limit = %v", err)
	}

	question := actor.newCycle(CycleOriginActivity)
	question.controlCount = gatewayCycleControlCountLimit
	if err := actor.mapQuestion(question, Event{Payload: json.RawMessage(`{"request_id":"question"}`)}); !errors.Is(err, ErrGatewayCycleOverflow) {
		t.Fatalf("question control limit = %v", err)
	}

	oversize := actor.newCycle(CycleOriginActivity)
	if _, err := actor.messageFor(oversize, Event{Payload: json.RawMessage(`{"text":"` + strings.Repeat("x", gatewayCycleTextByteLimit+1) + `"}`)}); !errors.Is(err, ErrGatewayCycleOverflow) {
		t.Fatalf("oversize completion = %v", err)
	}

	projected := actor.newCycle(CycleOriginActivity)
	for range gatewayProjectionLimit {
		actor.projections[&gatewayProjection{}] = struct{}{}
	}
	if err := actor.completeCycle(projected, NativeMessage{}, nil, Event{}); !errors.Is(err, ErrGatewayCycleOverflow) {
		t.Fatalf("projection limit = %v", err)
	}
}

func TestOrderedGatewayEventWaitReportsActorAndServerTermination(t *testing.T) {
	t.Run("actor stopped", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		close(actor.done)
		err := server.enqueueGatewayEvent(actor, Event{Type: "raw"}, actor.generation, true)
		if !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("stopped actor enqueue = %v", err)
		}
	})

	t.Run("server stopped", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		close(server.closed)
		err := server.enqueueGatewayEvent(actor, Event{Type: "raw"}, actor.generation, true)
		if !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("stopped server enqueue = %v", err)
		}
	})
}

func TestClarificationCannotOverwriteGeneratedPermissionOwner(t *testing.T) {
	_, actor := newDirectGatewayActor()
	cycle := actor.newCycle(CycleOriginActivity)
	cycle.activeTools["tool-1"] = struct{}{}

	if err := actor.mapPermission(cycle, Event{Payload: json.RawMessage(`{"command":"run"}`)}); err != nil {
		t.Fatal(err)
	}
	requestID := cycle.id + "/permission-1"
	if err := actor.mapQuestion(cycle, Event{Payload: json.RawMessage(`{"request_id":"` + requestID + `","prompt":"continue?"}`)}); err != nil {
		t.Fatal(err)
	}

	if _, ok := cycle.controls[gatewayControlIdentity{kind: gatewayControlPermission, requestID: requestID}]; !ok {
		t.Fatal("generated permission owner was overwritten")
	}
	if _, ok := cycle.controls[gatewayControlIdentity{kind: gatewayControlQuestion, requestID: requestID}]; !ok {
		t.Fatal("clarification owner was not registered independently")
	}
}

func TestGatewayActorExitResolvesAndRemovesEveryProjection(t *testing.T) {
	_, actor := newDirectGatewayActor()
	want := errors.New("actor stopped")
	projections := make([]*gatewayProjection, 0, gatewayProjectionLimit)
	for range gatewayProjectionLimit {
		projection := &gatewayProjection{done: make(chan error, 1)}
		actor.projections[projection] = struct{}{}
		projections = append(projections, projection)
	}

	actor.failCycles(want)
	if len(actor.projections) != 0 {
		t.Fatalf("projection map retained %d routes", len(actor.projections))
	}
	for _, projection := range projections {
		if err := <-projection.done; !errors.Is(err, want) {
			t.Fatalf("projection result = %v", err)
		}
	}
}

func TestGatewayCycleLimitPlusOneFailsAndContainsGeneration(t *testing.T) {
	server, actor := newDirectGatewayActor()
	cycle := actor.newCycle(CycleOriginActivity)
	cycle.textBytes = gatewayCycleTextByteLimit
	actor.active = cycle

	actor.handleRaw(actor.generation, Event{
		Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"x"}`), InboundSequence: 1,
	})
	if cause := server.transport.dispatcher.terminalCause(); !errors.Is(cause, ErrGatewayCycleOverflow) {
		t.Fatalf("generation cause = %v", cause)
	}
	if actor.active != nil || len(actor.terminal) != 1 {
		t.Fatalf("overflow retained active=%#v terminal=%d", actor.active, len(actor.terminal))
	}
	terminal := mustTurnEvent(t, server.deliveries)
	if terminal.Type != EventCycleFailed || !errors.Is(terminal.Err, ErrGatewayCycleOverflow) {
		t.Fatalf("cycle terminal = %#v", terminal)
	}
}

// TestGatewayNarratedTurnCompletesOnItsFinalResponse pins the emission shape a
// narrated tool turn produces: the delta stream carries interim commentary and
// then the answer, while message.complete carries the final response alone. The
// completion extends nothing, and the cycle still completes cleanly.
func TestGatewayNarratedTurnCompletesOnItsFinalResponse(t *testing.T) {
	server, actor := newDirectGatewayActor()
	cycle := actor.newCycle(CycleOriginActivity)
	cycle.text.WriteString("Let me read that file.\n\nThe file says PATH_OK.\n")
	cycle.textBytes = cycle.text.Len()
	actor.active = cycle

	actor.handleRaw(actor.generation, Event{
		Type:            evtMessageComplete,
		Payload:         json.RawMessage(`{"text":"The file says PATH_OK.","status":"complete"}`),
		InboundSequence: 2,
	})
	if cause := server.transport.dispatcher.terminalCause(); cause != nil {
		t.Fatalf("generation cause = %v", cause)
	}
	if actor.active != nil || len(actor.terminal) != 0 {
		t.Fatalf("completion retained active=%#v terminal=%d", actor.active, len(actor.terminal))
	}
	terminal := mustTurnEvent(t, server.deliveries)
	if terminal.Type != EventCycleComplete || terminal.Err != nil || terminal.Message == nil {
		t.Fatalf("narrated completion terminal = %#v", terminal)
	}
	if terminal.Message.Info.Finish != valStop {
		t.Fatalf("narrated completion finish = %q", terminal.Message.Info.Finish)
	}
	part := terminal.Message.Parts[len(terminal.Message.Parts)-1]
	if part.Text != "The file says PATH_OK." || part.StreamedText != cycle.text.String() {
		t.Fatalf("narrated completion part = %#v", part)
	}
}

// TestGatewayCompletionSuffixReadsTheDeltaStreamLeniently pins the fragment a
// completion adds for every shape hermes can produce, none of which is a
// conflict.
func TestGatewayCompletionSuffixReadsTheDeltaStreamLeniently(t *testing.T) {
	for _, test := range []struct {
		name     string
		complete string
		streamed string
		want     string
	}{
		{name: "no stream", complete: "final answer", want: "final answer"},
		{name: "empty completion", streamed: "commentary"},
		{name: "fully streamed", complete: "final answer", streamed: "final answer"},
		{name: "completion extends the stream", complete: "final answer", streamed: "final ", want: "answer"},
		{name: "narrated turn", complete: "the answer", streamed: "narration. the answer\n"},
		{name: "answer the stream never carried", complete: "the answer", streamed: "an abandoned attempt", want: "the answer"},
	} {
		if got := gatewayCompletionSuffix(test.complete, test.streamed); got != test.want {
			t.Fatalf("%s: suffix = %q, want %q", test.name, got, test.want)
		}
	}
}

func TestGatewayCompletionPreservesStreamedPresentationWhitespace(t *testing.T) {
	_, actor := newDirectGatewayActor()
	cycle := actor.newCycle(CycleOriginActivity)
	cycle.text.WriteString("\n\nPATH_OK")
	cycle.textBytes = len("\n\nPATH_OK")

	message, err := actor.messageFor(cycle, Event{
		Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"PATH_OK"}`), InboundSequence: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(message.Parts) != 1 || message.Parts[0].Text != "\n\nPATH_OK" || message.Parts[0].StreamedText != "\n\nPATH_OK" {
		t.Fatalf("completion parts = %#v", message.Parts)
	}
}

// TestGatewayInterruptedCompletionFinishesCancelled pins that the status hermes
// reports for a stopped, steered, or barged-in turn settles as a cancel rather
// than as the clean stop a successful turn reports.
func TestGatewayInterruptedCompletionFinishesCancelled(t *testing.T) {
	_, actor := newDirectGatewayActor()

	interrupted, err := actor.messageFor(actor.newCycle(CycleOriginPrompt), Event{
		Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"partial answer","status":"interrupted"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if interrupted.Info.Finish != valCancelled {
		t.Fatalf("interrupted finish = %q", interrupted.Info.Finish)
	}

	complete, err := actor.messageFor(actor.newCycle(CycleOriginPrompt), Event{
		Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"answer","status":"complete"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if complete.Info.Finish != valStop {
		t.Fatalf("complete finish = %q", complete.Info.Finish)
	}
}

// TestGatewayBareErrorEventFailsTheCycle pins the frame hermes ends a turn with
// when the turn dies before or outside its own terminal path: the cycle fails on
// the error event instead of waiting for a message.complete that never comes.
func TestGatewayBareErrorEventFailsTheCycle(t *testing.T) {
	server, actor := newDirectGatewayActor()
	actor.active = actor.newCycle(CycleOriginActivity)

	actor.handleRaw(actor.generation, Event{
		Type:            evtError,
		Payload:         json.RawMessage(`{"message":"Turn cancelled before the agent was ready"}`),
		InboundSequence: 1,
	})
	if cause := server.transport.dispatcher.terminalCause(); cause != nil {
		t.Fatalf("generation cause = %v", cause)
	}
	if actor.active != nil {
		t.Fatalf("error event retained active=%#v", actor.active)
	}
	terminal := mustTurnEvent(t, server.deliveries)
	if terminal.Type != EventCycleFailed || terminal.Message != nil {
		t.Fatalf("error event terminal = %#v", terminal)
	}

	var failure *TurnFailureError
	if !errors.As(terminal.Err, &failure) || failure.Cause() != CauseProvider ||
		failure.Message() != "Turn cancelled before the agent was ready" {
		t.Fatalf("error event failure = %#v", terminal.Err)
	}
}

func TestGatewayCompletionCumulativeTextLimitPlusOneFailsClosed(t *testing.T) {
	server, actor := newDirectGatewayActor()
	cycle := actor.newCycle(CycleOriginActivity)
	cycle.text.WriteString("x")
	cycle.textBytes = gatewayCycleTextByteLimit
	actor.active = cycle

	actor.handleRaw(actor.generation, Event{
		Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"xy"}`), InboundSequence: 2,
	})
	if cause := server.transport.dispatcher.terminalCause(); !errors.Is(cause, ErrGatewayCycleOverflow) {
		t.Fatalf("generation cause = %v", cause)
	}
	terminal := mustTurnEvent(t, server.deliveries)
	if terminal.Type != EventCycleFailed || !errors.Is(terminal.Err, ErrGatewayCycleOverflow) {
		t.Fatalf("completion overflow terminal = %#v", terminal)
	}
}

func TestGatewayDispatchHelpers(t *testing.T) {
	if generation := (&hermesServer{}).installGatewayDispatcher(nil); generation != 0 {
		t.Fatalf("nil dispatcher generation = %d", generation)
	}

	t.Run("projection", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		projection := &gatewayProjection{done: make(chan error, 1)}
		projection.resolve(actor, nil)
		projection.resolve(actor, errors.New("ignored"))
		if err := <-projection.done; err != nil {
			t.Fatalf("projection = %v", err)
		}
		(*gatewayProjection)(nil).resolve(actor, nil)

		for len(actor.mailbox) < gatewayActorMailboxCapacity {
			actor.mailbox <- gatewayActorMessage{}
		}
		(&gatewayProjection{done: make(chan error, 1)}).resolve(actor, nil)
		if err := mustTurnError(t, server.deliveries); !errors.Is(err, ErrGatewayActorOverflow) {
			t.Fatalf("overflow = %v", err)
		}
	})

	t.Run("dispatcher cause", func(t *testing.T) {
		if got := gatewayDispatcherCause(nil); !errors.Is(got, errGatewayStreamClosed) ||
			!errors.Is(got, ErrGatewayDisconnected) {
			t.Fatalf("nil cause = %v", got)
		}
		want := errors.New("stopped")
		dispatcher := &gatewayTransportDispatcher{}
		_ = dispatcher.setCause(want)
		_ = dispatcher.setCause(errors.New("later"))
		if got := gatewayDispatcherCause(dispatcher); !errors.Is(got, want) ||
			!errors.Is(got, ErrGatewayDisconnected) {
			t.Fatalf("cause = %v", got)
		}
	})

	t.Run("classification", func(t *testing.T) {
		if gatewaySessionEvent("unknown") {
			t.Fatal("unknown event classified as session work")
		}
		for _, eventType := range []string{
			evtApprovalRequest, evtClarifyRequest, evtSecretRequest, evtMessageDelta,
			evtMessageComplete, evtError, evtSudoRequest, evtThinkingDelta,
			evtTerminalReadReq, evtToolComplete, evtToolStart,
		} {
			if !gatewaySessionEvent(eventType) {
				t.Fatalf("%q was not classified", eventType)
			}
		}
	})
}

func TestGatewayDispatchRoutingEdges(t *testing.T) {
	t.Run("global unknown is ignored but unmatched session fails closed", func(t *testing.T) {
		server, _ := newDirectGatewayActor()
		_ = server.dispatchGatewayEvent(server.transport.dispatcher, Event{Type: "unknown"})
		_ = server.dispatchGatewayEvent(server.transport.dispatcher, Event{Type: evtMessageDelta, SessionID: "missing"})
		if len(server.deliveries) != 1 {
			t.Fatal("unmatched session did not fail closed")
		}
	})

	t.Run("empty session refuses even with one actor", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		if err := server.dispatchGatewayEvent(server.transport.dispatcher, Event{Type: evtMessageDelta}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("missing session identity = %v", err)
		}
		if err := mustTurnError(t, server.deliveries); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("ambiguity = %v", err)
		}
		if len(actor.terminal) != 1 {
			t.Fatal("existing actor was not fenced")
		}
	})
}

func TestGatewayActorPromptEdges(t *testing.T) {
	t.Run("duplicate registration", func(t *testing.T) {
		_, actor := newDirectGatewayActor()
		registerDirectPrompt(actor)
		second := registerDirectPrompt(actor)
		if !errors.Is(second.err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("duplicate = %v", second.err)
		}
	})

	t.Run("watermark changed refuses without failed-cycle tombstone", func(t *testing.T) {
		_, actor := newDirectGatewayActor()
		result := actor.applyPromptWatermark(&gatewayPromptWatermark{cycleID: "missing"})
		if !errors.Is(result.err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("missing = %v", result.err)
		}
	})

	t.Run("watermark held overflow and route failure", func(t *testing.T) {
		for _, routeFailure := range []bool{false, true} {
			server, actor := newDirectGatewayActor()
			handle := registerDirectPrompt(actor)
			if routeFailure {
				actor.prompt.started = true
				actor.buffered = []Event{{Type: evtMessageDelta, InboundSequence: 1}}
			} else {
				actor.buffered = make([]Event, gatewayActorMailboxCapacity+1)
				for index := range actor.buffered {
					actor.buffered[index] = Event{Type: evtMessageDelta, InboundSequence: 2}
				}
			}
			result := actor.applyPromptWatermark(&gatewayPromptWatermark{cycleID: handle.cycleID, watermark: 1})
			if routeFailure && !errors.Is(result.err, ErrGatewayAmbiguousTurn) {
				t.Fatalf("route failure = %v", result.err)
			}
			if !routeFailure && !errors.Is(result.err, ErrGatewayActorOverflow) {
				t.Fatalf("held overflow = %v", result.err)
			}
			if err := mustTurnError(t, server.deliveries); err == nil {
				t.Fatal("watermark failure did not fence")
			}
		}
	})

	t.Run("release validation", func(t *testing.T) {
		_, actor := newDirectGatewayActor()
		if err := actor.releasePrompt(&gatewayPromptRelease{cycleID: "missing"}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("invalid release = %v", err)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		_, actor := newDirectGatewayActor()
		handle := registerDirectPrompt(actor)
		done := make(chan struct{})
		want := errors.New("cancel")
		actor.cancelPrompt(&gatewayPromptCancel{registrationID: "other", done: make(chan struct{})})
		actor.cancelPrompt(&gatewayPromptCancel{registrationID: actor.prompt.registrationID, err: want, done: done})
		<-done
		if result := <-handle.result; !errors.Is(result.err, want) {
			t.Fatalf("cancel result = %v", result.err)
		}
	})
}

func TestGatewayAcceptedPromptCancellationFencesQueuedNativeOutput(t *testing.T) {
	server, actor := newDirectGatewayActor()
	server.actorWG.Add(1)
	go actor.run()
	t.Cleanup(func() {
		actor.mailbox <- gatewayActorMessage{stop: true}
		<-actor.done
	})

	reply := make(chan gatewayPromptHandle, 1)
	registration := &gatewayPromptRegistration{
		id: "accepted", result: make(chan gatewayCycleResult, 1), reply: reply,
	}
	actor.mailbox <- gatewayActorMessage{register: registration}
	handle := <-reply
	watermarkReply := make(chan gatewayPromptWatermarkResult, 1)
	actor.mailbox <- gatewayActorMessage{watermark: &gatewayPromptWatermark{
		cycleID: handle.cycleID, watermark: 10, reply: watermarkReply,
	}}
	if result := <-watermarkReply; result.err != nil {
		t.Fatalf("watermark: %v", result.err)
	}

	cancelled := make(chan struct{})
	actor.mailbox <- gatewayActorMessage{cancel: &gatewayPromptCancel{
		registrationID: registration.id,
		err:            context.Canceled,
		accepted:       true,
		generation:     1,
		watermark:      10,
		done:           cancelled,
	}}
	actor.mailbox <- gatewayActorMessage{event: &Event{
		Type: evtMessageDelta, InboundSequence: 11, Payload: json.RawMessage(`{"text":"late"}`),
	}, generation: 1}
	actor.mailbox <- gatewayActorMessage{event: &Event{
		Type: evtMessageComplete, InboundSequence: 12, Payload: json.RawMessage(`{"text":"late"}`),
	}, generation: 1}
	<-cancelled
	if result := <-handle.result; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("cancel result = %v", result.err)
	}

	actor.mailbox <- gatewayActorMessage{event: &Event{
		Type: evtMessageDelta, InboundSequence: 13, Payload: json.RawMessage(`{"text":"background"}`),
	}, generation: 1}
	actor.mailbox <- gatewayActorMessage{event: &Event{
		Type: evtMessageComplete, InboundSequence: 14, Payload: json.RawMessage(`{"text":"background"}`),
	}, generation: 1}

	started := mustTurnEvent(t, server.deliveries)
	part := mustTurnEvent(t, server.deliveries)
	completed := mustTurnEvent(t, server.deliveries)
	if started.Type != EventCycleStarted || started.Origin != CycleOriginActivity ||
		part.Type != evtMessagePartUpdated || completed.Type != EventCycleComplete {
		t.Fatalf("post-cancel deliveries = %#v / %#v / %#v", started, part, completed)
	}
	select {
	case extra := <-server.deliveries:
		t.Fatalf("cancelled prompt produced an agent-origin delivery: %#v", extra)
	default:
	}
}

func TestGatewayPromptTombstoneSurvivesSameGenerationBinding(t *testing.T) {
	server, actor := newDirectGatewayActor()
	actor.fencedPrompt = &gatewayPromptTombstone{generation: 1, watermark: 7}
	server.actorWG.Add(1)
	go actor.run()
	t.Cleanup(func() {
		actor.mailbox <- gatewayActorMessage{stop: true}
		<-actor.done
	})

	bound := make(chan struct{})
	actor.mailbox <- gatewayActorMessage{bind: &gatewayActorBinding{
		live: "live-again", generation: 1, done: bound,
	}}
	<-bound
	reply := make(chan gatewayPromptHandle, 1)
	actor.mailbox <- gatewayActorMessage{register: &gatewayPromptRegistration{
		id: "same-generation", result: make(chan gatewayCycleResult, 1), reply: reply,
	}}
	if handle := <-reply; !errors.Is(handle.err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("same-generation bind cleared prompt tombstone: %#v", handle)
	}

	bound = make(chan struct{})
	actor.mailbox <- gatewayActorMessage{bind: &gatewayActorBinding{
		live: "live-next", generation: 2, done: bound,
	}}
	<-bound
	reply = make(chan gatewayPromptHandle, 1)
	actor.mailbox <- gatewayActorMessage{register: &gatewayPromptRegistration{
		id: "next-generation", result: make(chan gatewayCycleResult, 1), reply: reply,
	}}
	if handle := <-reply; handle.err != nil || handle.cycleID == "" {
		t.Fatalf("next-generation bind retained stale tombstone: %#v", handle)
	}
}

func TestGatewayActorRawEdges(t *testing.T) {
	t.Run("raw opt in", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		actor.handleRaw(1, Event{Type: "unknown", Raw: json.RawMessage(`{"x":1}`)})
		if event := mustTurnEvent(t, server.deliveries); event.Type != EventGatewayRaw {
			t.Fatalf("raw = %#v", event)
		}
	})

	t.Run("buffer and held overflow", func(t *testing.T) {
		for _, held := range []bool{false, true} {
			server, actor := newDirectGatewayActor()
			registerDirectPrompt(actor)
			actor.prompt.held = held
			actor.prompt.watermarkSet = held
			actor.prompt.watermark = 1
			for index := 0; index <= gatewayActorMailboxCapacity; index++ {
				actor.handleRaw(1, Event{Type: evtMessageDelta, InboundSequence: uint64(index + 2)})
			}
			if err := mustTurnError(t, server.deliveries); !errors.Is(err, ErrGatewayActorOverflow) {
				t.Fatalf("held=%v overflow = %v", held, err)
			}
		}
	})

	t.Run("prompt and active ambiguity", func(t *testing.T) {
		_, actor := newDirectGatewayActor()
		handle := registerDirectPrompt(actor)
		actor.prompt.watermarkSet = true
		actor.prompt.watermark = 5
		actor.active = actor.newCycle(CycleOriginActivity)
		if err := actor.routeRaw(Event{Type: evtMessageDelta, InboundSequence: 6}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("post-submit overlap = %v", err)
		}
		actor.active = nil
		actor.prompt.started = true
		if err := actor.routeRaw(Event{Type: evtMessageDelta, InboundSequence: 4}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("pre-submit overlap = %v", err)
		}
		if actor.prompt.id != handle.cycleID {
			t.Fatal("prompt changed")
		}
	})

	t.Run("handle route failure", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		registerDirectPrompt(actor)
		actor.prompt.watermarkSet = true
		actor.prompt.started = true
		actor.handleRaw(1, Event{Type: evtMessageDelta})
		if err := mustTurnError(t, server.deliveries); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("route failure = %v", err)
		}
	})
}

func TestGatewayActorMappingEdges(t *testing.T) {
	_, actor := newDirectGatewayActor()
	cycle := actor.newCycle(CycleOriginActivity)

	if err := actor.applyEvent(cycle, Event{Type: evtToolStart, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("missing tool-start identity = %v", err)
	}
	if err := actor.applyEvent(cycle, Event{Type: evtToolComplete, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("missing tool-complete identity = %v", err)
	}
	if err := actor.applyEvent(cycle, Event{Type: evtMessageDelta, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("empty delta: %v", err)
	}
	if err := actor.applyEvent(cycle, Event{Type: "ignored"}); err != nil {
		t.Fatalf("ignored: %v", err)
	}

	if err := actor.mapPermission(cycle, Event{}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("permission ambiguity = %v", err)
	}
	if err := actor.mapQuestion(cycle, Event{}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("question ambiguity = %v", err)
	}

	cycle.activeTools["tool-1"] = struct{}{}
	cycle.activeTools["tool-2"] = struct{}{}
	if err := actor.mapPermission(cycle, Event{}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("permission multi-tool ambiguity = %v", err)
	}

	part := Part{State: json.RawMessage(`{`)}
	if err := actor.emitPart(cycle, part, Event{}); err == nil {
		t.Fatal("invalid part JSON succeeded")
	}
}

func TestGatewayActorDeclinesUnsupportedControls(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	actor := &gatewaySessionActor{server: server, live: "live", stored: "stored"}
	cycle := &gatewayCycle{}
	for _, eventType := range []string{evtTerminalReadReq, evtSudoRequest, evtSecretRequest} {
		if err := actor.applyEvent(cycle, Event{Type: eventType}); err != nil {
			t.Fatalf("decline %q: %v", eventType, err)
		}
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestGatewayActorCompletionAndFailureEdges(t *testing.T) {
	t.Run("streamed text fallback and failed completion", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		cycle := actor.newCycle(CycleOriginActivity)
		cycle.text.WriteString("streamed")
		message, err := actor.messageFor(cycle, Event{Payload: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if message.Parts[0].Text != "streamed" {
			t.Fatalf("fallback = %#v", message)
		}
		actor.active = cycle
		want := errors.New("failed")
		if err := actor.completeCycle(cycle, NativeMessage{}, want, Event{}); err != nil {
			t.Fatalf("provider outcome corrupted dispatcher = %v", err)
		}
		terminal := mustTurnEvent(t, server.deliveries)
		if terminal.Type != EventCycleFailed || terminal.Message != nil {
			t.Fatalf("terminal = %#v", terminal)
		}
		terminal.ProjectionDone(nil)
	})

	t.Run("failed cycles deduplicate and latch", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		cycle := actor.newCycle(CycleOriginPrompt)
		cycle.result = make(chan gatewayCycleResult, 1)
		actor.active, actor.prompt = cycle, cycle
		want := errors.New("fenced")
		actor.failPending(actor.generation, want)
		actor.failPending(actor.generation, errors.New("ignored"))
		if len(actor.terminal) != 1 {
			t.Fatalf("terminal latch = %d", len(actor.terminal))
		}
		actor.failCycles(want)
		if len(server.deliveries) != 1 {
			t.Fatalf("deduplicated terminals = %d", len(server.deliveries))
		}
		if result := <-cycle.result; !errors.Is(result.err, want) {
			t.Fatalf("cycle result = %v", result.err)
		}
	})
}

func TestGatewayActorRunCommands(t *testing.T) {
	server, actor := newDirectGatewayActor()
	server.actorWG.Add(1)
	go actor.run()

	bound := make(chan struct{})
	actor.mailbox <- gatewayActorMessage{bind: &gatewayActorBinding{live: "replacement", generation: 2, done: bound}}
	<-bound
	if actor.live != "replacement" || actor.generation != 2 {
		t.Fatalf("binding = %q/%d", actor.live, actor.generation)
	}

	registration := &gatewayPromptRegistration{result: make(chan gatewayCycleResult, 1), reply: make(chan gatewayPromptHandle, 1)}
	actor.mailbox <- gatewayActorMessage{register: registration}
	handle := <-registration.reply
	watermarkReply := make(chan gatewayPromptWatermarkResult, 1)
	actor.mailbox <- gatewayActorMessage{watermark: &gatewayPromptWatermark{cycleID: handle.cycleID, watermark: 1, reply: watermarkReply}}
	if result := <-watermarkReply; result.err != nil {
		t.Fatalf("watermark = %v", result.err)
	}
	releaseReply := make(chan error, 1)
	actor.mailbox <- gatewayActorMessage{release: &gatewayPromptRelease{cycleID: handle.cycleID, reply: releaseReply}}
	if err := <-releaseReply; err != nil {
		t.Fatalf("release = %v", err)
	}

	projection := &gatewayProjection{done: make(chan error, 1)}
	actor.projections[projection] = struct{}{}
	actor.mailbox <- gatewayActorMessage{projected: projection}
	end := make(chan bool, 1)
	actor.mailbox <- gatewayActorMessage{generation: 1, end: end}
	if <-end {
		t.Fatal("stale generation reported active")
	}
	if len(actor.projections) != 0 {
		t.Fatal("projection command not handled")
	}
	end = make(chan bool, 1)
	actor.mailbox <- gatewayActorMessage{generation: 2, end: end}
	if !<-end {
		t.Fatal("current generation did not report active")
	}
	if result := <-handle.result; result.err == nil {
		t.Fatal("implicit transport failure did not resolve prompt")
	}
	actor.mailbox <- gatewayActorMessage{failure: errors.New("no pending cycle")}
	actor.mailbox <- gatewayActorMessage{stop: true}
	select {
	case <-actor.done:
	case <-t.Context().Done():
		t.Fatal("actor did not stop")
	}
}

func TestGatewaySynchronizeAbsentAndContext(t *testing.T) {
	server := &hermesServer{closed: make(chan struct{}), dispatchers: map[uint64]*gatewayTransportDispatcher{}}
	if err := server.synchronizeGatewayWatermark(t.Context(), nil, 1); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("missing dispatcher = %v", err)
	}

	dispatcher := &gatewayTransportDispatcher{done: make(chan struct{}), barriers: make(chan gatewayDispatchBarrier)}
	transport := &gatewayTransport{generation: 1, dispatcher: dispatcher}
	dispatcher.transport = transport
	server.dispatchers[1] = dispatcher
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := server.synchronizeGatewayWatermark(ctx, transport, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("send cancellation = %v", err)
	}

	dispatcher.barriers = make(chan gatewayDispatchBarrier)
	ctx, cancel = context.WithCancel(t.Context())
	received := make(chan struct{})
	go func() {
		<-dispatcher.barriers
		close(received)
	}()
	go func() {
		<-received
		cancel()
	}()
	if err := server.synchronizeGatewayWatermark(ctx, transport, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("reply cancellation = %v", err)
	}

	dispatcher.done = make(chan struct{})
	dispatcher.barriers = make(chan gatewayDispatchBarrier)
	dispatcherEnded := errors.New("dispatcher ended")
	_ = dispatcher.setCause(dispatcherEnded)
	go func() {
		<-dispatcher.barriers
		close(dispatcher.done)
	}()
	if err := server.synchronizeGatewayWatermark(t.Context(), transport, 1); !errors.Is(err, dispatcherEnded) {
		t.Fatalf("reply dispatcher end = %v", err)
	}

	terminal := errors.New("malformed gateway frame")
	retired := &gatewayTransportDispatcher{done: make(chan struct{})}
	_ = retired.setCause(terminal)
	close(retired.done)
	retiredTransport := &gatewayTransport{generation: 2, dispatcher: retired}
	if err := server.synchronizeGatewayWatermark(t.Context(), retiredTransport, 4); !errors.Is(err, terminal) {
		t.Fatalf("retained terminal cause = %v", err)
	}
}

func TestGatewayDrainAndGenerationEdges(t *testing.T) {
	t.Run("closed event stream", func(t *testing.T) {
		server, _ := newDirectGatewayActor()
		deliveries := make(chan GatewayDelivery)
		close(deliveries)
		dispatcher := &gatewayTransportDispatcher{client: &Client{deliveries: deliveries}}
		if err := server.drainGatewayThroughWatermark(dispatcher, deliveries, 1); !errors.Is(err, errGatewayStreamClosed) {
			t.Fatalf("drain = %v", err)
		}
	})

	t.Run("closed server generation", func(t *testing.T) {
		server, _ := newDirectGatewayActor()
		close(server.closed)
		server.finishGatewayGeneration(&gatewayTransportDispatcher{}, errors.New("ended"))
	})

	t.Run("actor end wins generation", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		actor.mailbox = nil
		close(actor.done)
		server.finishGatewayGeneration(&gatewayTransportDispatcher{}, errors.New("ended"))
	})

	t.Run("actor ends after generation message", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		actor.mailbox = make(chan gatewayActorMessage)
		go func() {
			<-actor.mailbox
			close(actor.done)
		}()
		server.finishGatewayGeneration(&gatewayTransportDispatcher{}, errors.New("ended"))
	})

	t.Run("nil failure cause", func(t *testing.T) {
		server, _ := newDirectGatewayActor()
		server.failGatewayGeneration(1, nil)
		if err := mustTurnError(t, server.deliveries); !errors.Is(err, errGatewayStreamClosed) {
			t.Fatalf("nil failure = %v", err)
		}
	})
}

func TestMappedOverflowTerminalDoesNotBlockClose(t *testing.T) {
	server := &hermesServer{
		deliveries:     make(chan TurnDelivery, 2),
		closed:         make(chan struct{}),
		actorsByStored: make(map[string]*gatewaySessionActor),
		dispatchers:    make(map[uint64]*gatewayTransportDispatcher),
	}
	if err := server.publishTurnEvent(TurnEvent{Type: EventGatewayRaw}); err != nil {
		t.Fatalf("fill mapped delivery lane: %v", err)
	}
	if err := server.publishTurnEvent(TurnEvent{Type: EventGatewayRaw}); !errors.Is(err, ErrGatewayMappedOverflow) {
		t.Fatalf("mapped overflow = %v", err)
	}

	published := make(chan struct{})
	go func() {
		server.publishTurnError(ErrGatewayMappedOverflow)
		close(published)
	}()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("mapped overflow terminal blocked behind ordinary deliveries")
	}

	closed := make(chan error, 1)
	go func() { closed <- server.Close(t.Context()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join after mapped delivery overflow")
	}
}

func TestStopGatewayActorsSkipsStoppedActor(t *testing.T) {
	server, actor := newDirectGatewayActor()
	close(actor.done)
	server.stopGatewayActors()
}

func TestGatewayDisconnectAndSessionMappingEdges(t *testing.T) {
	want := errors.New("socket failed")
	if got := gatewayDisconnectCause(&Client{terminal: want}); !errors.Is(got, want) {
		t.Fatalf("disconnect cause = %v", got)
	}

	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored", "old-live")
	bindTestGatewaySession(t, server, "stored", "new-live")
	mappings := server.gatewayTransport().mappings
	mappings.mu.Lock()
	binding := mappings.bindings["stored"]
	mappings.mu.Unlock()
	if binding.live != "new-live" || binding.actor == nil {
		t.Fatalf("rotated mapping = %#v", binding)
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestGatewayControlTransportGeneration(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored", "live")
	permission, question := testGatewayControlRequests(t, server, "stored", "live")

	if err := server.ReplyPermission(t.Context(), permission, valOnce, ""); err != nil {
		t.Fatalf("current permission: %v", err)
	}
	if err := server.ReplyQuestion(t.Context(), question, [][]string{{"yes"}}); err != nil {
		t.Fatalf("current question: %v", err)
	}
	if err := server.RejectQuestion(t.Context(), question); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("duplicate rejection: %v", err)
	}
	generation := permission.TransportGeneration
	for name, err := range map[string]error{
		"permission": server.ReplyPermission(t.Context(), PermissionRequest{SessionID: "stored", TransportGeneration: generation + 1}, valOnce, ""),
		"question":   server.ReplyQuestion(t.Context(), QuestionRequest{SessionID: "stored", TransportGeneration: generation + 1}, nil),
		"rejection":  server.RejectQuestion(t.Context(), QuestionRequest{SessionID: "stored", TransportGeneration: generation + 1}),
	} {
		if !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("stale %s = %v", name, err)
		}
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestGatewayControlFromCycleOneCannotApplyToCycleTwo(t *testing.T) {
	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	bindTestGatewaySession(t, server, "stored", "live")

	cycleOne, _ := testGatewayControlRequests(t, server, "stored", "live")
	transport := server.gatewayTransport()
	if err := server.dispatchGatewayEvent(transport.dispatcher, Event{
		Type: evtMessageComplete, SessionID: "live", InboundSequence: 4,
		Payload: json.RawMessage(`{"text":"cycle one"}`),
	}); err != nil {
		t.Fatalf("complete cycle one: %v", err)
	}
	for {
		event := mustTurnEvent(t, server.deliveries)
		if event.Type == EventCycleComplete {
			break
		}
	}

	cycleTwo, _ := testGatewayControlRequests(t, server, "stored", "live")
	if cycleOne.route.cycleID == cycleTwo.route.cycleID {
		t.Fatal("test did not create distinct native cycles")
	}
	if err := server.ReplyPermission(t.Context(), cycleOne, valOnce, ""); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("delayed cycle-one permission = %v", err)
	}
	if calls := fake.callsFor("approval.respond"); len(calls) != 0 {
		t.Fatalf("stale cycle-one control reached RPC: %#v", calls)
	}
	if err := server.ReplyPermission(t.Context(), cycleTwo, valOnce, ""); err != nil {
		t.Fatalf("cycle-two permission: %v", err)
	}
	if calls := fake.callsFor("approval.respond"); len(calls) != 1 {
		t.Fatalf("cycle-two control RPCs = %#v", calls)
	}
}

func TestGatewayHandshakeControlStopsAtEveryLostGenerationBoundary(t *testing.T) {
	server := &hermesServer{closed: make(chan struct{})}
	if _, err := server.beginGatewayHandshake(t.Context(), nil, gatewayHandshakeCreate); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("begin without transport = %v", err)
	}
	if result := server.bindGatewayHandshake(t.Context(), nil, 1, gatewayHandshakeCreate, "stored", "live", GatewayWatermark{}); !errors.Is(result.err, ErrGatewayDisconnected) {
		t.Fatalf("bind without transport = %v", result.err)
	}
	server.cancelGatewayHandshake(nil, 1)
	server.cancelGatewayHandshake(&gatewayTransport{}, 0)

	newTransport := func() *gatewayTransport {
		dispatcher := &gatewayTransportDispatcher{
			done:       make(chan struct{}),
			handshakes: make(chan gatewayHandshakeCommand),
		}

		return &gatewayTransport{generation: 1, dispatcher: dispatcher}
	}

	t.Run("begin observes caller cancellation before admission", func(t *testing.T) {
		transport := newTransport()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := server.beginGatewayHandshake(ctx, transport, gatewayHandshakeCreate); !errors.Is(err, context.Canceled) {
			t.Fatalf("begin cancellation = %v", err)
		}
	})

	t.Run("begin observes dispatcher loss before admission", func(t *testing.T) {
		transport := newTransport()
		generationEnded := errors.New("generation ended")
		_ = transport.dispatcher.setCause(generationEnded)
		close(transport.dispatcher.done)
		if _, err := server.beginGatewayHandshake(t.Context(), transport, gatewayHandshakeCreate); !errors.Is(err, generationEnded) {
			t.Fatalf("begin generation loss = %v", err)
		}
	})

	t.Run("begin returns recorded identity despite caller cancellation after admission", func(t *testing.T) {
		transport := newTransport()
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			command := <-transport.dispatcher.handshakes
			cancel()
			command.reply <- gatewayHandshakeResult{state: gatewayHandshakeRecorded}
		}()
		if id, err := server.beginGatewayHandshake(ctx, transport, gatewayHandshakeCreate); err != nil || id == 0 {
			t.Fatalf("recorded begin = %d, %v", id, err)
		}
	})

	t.Run("bind observes caller cancellation before admission", func(t *testing.T) {
		transport := newTransport()
		go func() {
			command := <-transport.dispatcher.handshakes
			command.reply <- gatewayHandshakeResult{state: gatewayHandshakeTombstoned}
		}()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result := server.bindGatewayHandshake(ctx, transport, 1, gatewayHandshakeCreate, "stored", "live", GatewayWatermark{})
		if !errors.Is(result.err, context.Canceled) || result.state != gatewayHandshakeTombstoned {
			t.Fatalf("bind cancellation = %#v", result)
		}
	})

	t.Run("bind observes dispatcher loss before admission", func(t *testing.T) {
		transport := newTransport()
		generationEnded := errors.New("generation ended")
		_ = transport.dispatcher.setCause(generationEnded)
		close(transport.dispatcher.done)
		result := server.bindGatewayHandshake(t.Context(), transport, 1, gatewayHandshakeCreate, "stored", "live", GatewayWatermark{})
		if !errors.Is(result.err, generationEnded) {
			t.Fatalf("bind generation loss = %#v", result)
		}
	})

	t.Run("bind reports committed despite caller cancellation after admission", func(t *testing.T) {
		transport := newTransport()
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			command := <-transport.dispatcher.handshakes
			cancel()
			command.reply <- gatewayHandshakeResult{state: gatewayHandshakeCommitted}
		}()
		result := server.bindGatewayHandshake(ctx, transport, 1, gatewayHandshakeCreate, "stored", "live", GatewayWatermark{})
		if result.err != nil || result.state != gatewayHandshakeCommitted {
			t.Fatalf("bind commit = %#v", result)
		}
	})
}

func TestGatewayHandshakeBindingRejectsAmbiguityWithoutPublishingAnActor(t *testing.T) {
	server, actor := newDirectGatewayActor()
	dispatcher := server.transport.dispatcher
	events := make(chan GatewayDelivery)

	invalidStart := gatewayHandshakeCommand{action: gatewayHandshakeRecord, reply: make(chan gatewayHandshakeResult, 1)}
	server.handleGatewayHandshake(dispatcher, events, invalidStart)
	if result := <-invalidStart.reply; !errors.Is(result.err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("invalid start = %#v", result)
	}

	invalidBind := gatewayHandshakeCommand{
		action: gatewayHandshakeBind, id: 99, kind: gatewayHandshakeCreate, stored: "stored", live: "live",
		watermark: GatewayWatermark{TransportGeneration: dispatcher.generation}, reply: make(chan gatewayHandshakeResult, 1),
	}
	server.handleGatewayHandshake(dispatcher, events, invalidBind)
	if result := <-invalidBind.reply; !errors.Is(result.err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("invalid bind = %#v", result)
	}

	disconnectedEvents := make(chan GatewayDelivery)
	close(disconnectedEvents)
	dispatcher.client = &Client{deliveries: disconnectedEvents}
	dispatcher.pendingHandshakes[1] = gatewayHandshakeCreate
	drainFailure := gatewayHandshakeCommand{
		action: gatewayHandshakeBind, id: 1, kind: gatewayHandshakeCreate, stored: "stored", live: "live",
		watermark: GatewayWatermark{TransportGeneration: dispatcher.generation, Sequence: 1}, reply: make(chan gatewayHandshakeResult, 1),
	}
	server.handleGatewayHandshake(dispatcher, disconnectedEvents, drainFailure)
	if result := <-drainFailure.reply; !errors.Is(result.err, errGatewayStreamClosed) {
		t.Fatalf("bind drain failure = %#v", result)
	}

	readyEvents := make(chan GatewayDelivery, 1)
	ready := Event{Type: "global", InboundSequence: 2}
	readyEvents <- GatewayDelivery{Event: &ready}
	if err := server.drainGatewayThroughWatermark(dispatcher, readyEvents, 1); err != nil {
		t.Fatalf("drain through post-response frame = %v", err)
	}

	actorless := &gatewayTransportDispatcher{
		generation: 2, transport: &gatewayTransport{generation: 2},
		pendingFrames: make(map[string][]Event), pendingHandshakes: map[uint64]gatewayHandshakeKind{2: gatewayHandshakeCreate},
	}
	actorlessBind := gatewayHandshakeCommand{
		action: gatewayHandshakeBind, id: 2, kind: gatewayHandshakeCreate, stored: "stored", live: "live",
		watermark: GatewayWatermark{TransportGeneration: 2}, reply: make(chan gatewayHandshakeResult, 1),
	}
	server.handleGatewayHandshake(actorless, events, actorlessBind)
	if result := <-actorlessBind.reply; !errors.Is(result.err, ErrGatewayDisconnected) {
		t.Fatalf("actorless bind = %#v", result)
	}

	actor.mailbox = make(chan gatewayActorMessage, gatewayActorMailboxCapacity+1)
	go func() {
		message := <-actor.mailbox
		actor.live = message.bind.live
		actor.generation = message.bind.generation
		for len(actor.mailbox) < gatewayActorMailboxCapacity {
			actor.mailbox <- gatewayActorMessage{}
		}
		close(message.bind.done)
	}()
	dispatcher.client = nil
	dispatcher.pendingHandshakes[3] = gatewayHandshakeCreate
	dispatcher.pendingFrames[actor.live] = []Event{{Type: evtMessageDelta, InboundSequence: 2}}
	dispatcher.pendingFrameCount = 1
	overflowBind := gatewayHandshakeCommand{
		action: gatewayHandshakeBind, id: 3, kind: gatewayHandshakeCreate, stored: actor.stored, live: actor.live,
		watermark: GatewayWatermark{TransportGeneration: dispatcher.generation, Sequence: 1}, reply: make(chan gatewayHandshakeResult, 1),
	}
	server.handleGatewayHandshake(dispatcher, events, overflowBind)
	if result := <-overflowBind.reply; !errors.Is(result.err, ErrGatewayActorOverflow) {
		t.Fatalf("binding delivery overflow = %#v", result)
	}

	unmatched, _ := newDirectGatewayActor()
	unmatched.transport.dispatcher.pendingFrames["foreign-live"] = []Event{{Type: evtMessageDelta}}
	unmatched.transport.dispatcher.pendingFrameCount = 1
	unmatched.failUnmatchedGatewayFrames(unmatched.transport.dispatcher)
	if err := mustTurnError(t, unmatched.deliveries); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("unmatched binding = %v", err)
	}

	if got := server.actorForTransportSession(nil, "stored", "live"); got != nil {
		t.Fatalf("actor from nil transport = %#v", got)
	}
}

func TestGatewayGenerationShutdownHonorsServerCloseAtEitherActorExchange(t *testing.T) {
	t.Run("before actor notification", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		actor.mailbox = make(chan gatewayActorMessage)
		close(server.closed)
		server.finishGatewayGeneration(&gatewayTransportDispatcher{generation: 1}, errors.New("ended"))
	})

	t.Run("while waiting for actor acknowledgement", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		actor.mailbox = make(chan gatewayActorMessage)
		received := make(chan struct{})
		go func() {
			<-actor.mailbox
			close(received)
		}()
		go func() {
			<-received
			close(server.closed)
		}()
		server.finishGatewayGeneration(&gatewayTransportDispatcher{generation: 1}, errors.New("ended"))
	})
}

func TestGatewayOperationsRequireOnePublishedTransportTuple(t *testing.T) {
	server := &hermesServer{
		closed: make(chan struct{}), deliveries: make(chan TurnDelivery, 1),
	}
	if client := server.gatewayClient(); client != nil {
		t.Fatalf("gateway without transport = %#v", client)
	}
	if _, err := server.CreateSession(t.Context(), "missing"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("create without transport = %v", err)
	}
	if _, err := server.GetSession(t.Context(), "missing"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("get without transport = %v", err)
	}
	server.dropGatewayBindingOn(server.gatewayTransport(), "missing")
	if live := server.liveSessionIDOn(nil, "missing"); live != "" {
		t.Fatalf("live id without transport = %q", live)
	}
	if _, err := server.resumeGatewaySessionOn(t.Context(), nil, "missing"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("resume without transport = %v", err)
	}
	if _, err := server.submitGatewayParts(t.Context(), "missing", nil); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("submit without transport = %v", err)
	}
	if _, err := server.submitGatewayTextForLive(t.Context(), nil, "stored", "live", "prompt", nil); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("live submit without transport = %v", err)
	}

	broken := &gatewayTransport{client: &Client{}, generation: 1}
	server.transport = broken
	if _, err := server.CreateSession(t.Context(), "broken"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("create without dispatcher = %v", err)
	}
	if _, err := server.resumeGatewaySessionOn(t.Context(), broken, "stored"); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("resume without dispatcher = %v", err)
	}
	if _, err := server.submitGatewayTextForLive(t.Context(), broken, "stored", "live", "prompt", nil); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("submit without mappings = %v", err)
	}
}

func TestGatewayServerCloseJoinsFailedTransportClose(t *testing.T) {
	closeEntered := make(chan struct{})
	closeRelease := make(chan struct{})
	client := &Client{closeTransport: func(websocket.StatusCode, string) error {
		close(closeEntered)
		<-closeRelease

		return nil
	}}
	dispatcher := &gatewayTransportDispatcher{generation: 1}
	transport := &gatewayTransport{client: client, generation: 1, dispatcher: dispatcher}
	dispatcher.transport = transport
	server := &hermesServer{
		closed:         make(chan struct{}),
		deliveries:     make(chan TurnDelivery, 2),
		actorsByStored: make(map[string]*gatewaySessionActor),
		dispatchers:    map[uint64]*gatewayTransportDispatcher{1: dispatcher},
		transport:      transport,
	}
	server.failGatewayTransport(transport, errors.New("failed"))
	<-closeEntered
	done := make(chan error, 1)
	go func() { done <- server.Close(t.Context()) }()
	select {
	case err := <-done:
		t.Fatalf("server close returned while its transport close was live: %v", err)
	default:
	}
	close(closeRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestGatewayLifecycleCorrectionRemainingBranches(t *testing.T) {
	server, actor := newDirectGatewayActor()
	dispatcher := server.transport.dispatcher

	if got := gatewayTransportFailure(nil); !errors.Is(got, ErrGatewayDisconnected) {
		t.Fatalf("nil transport failure = %v", got)
	}
	if !((*gatewaySessionActor)(nil)).terminalized() {
		t.Fatal("nil actor was live")
	}
	doneActor := &gatewaySessionActor{done: make(chan struct{}), terminal: make(chan error, 1)}
	close(doneActor.done)
	if !doneActor.terminalized() {
		t.Fatal("completed actor was live")
	}

	deliveries := make(chan GatewayDelivery, 3)
	deliveries <- GatewayDelivery{}
	deliveries <- GatewayDelivery{Err: errors.New("delivery failed")}
	if err := server.drainGatewayThroughWatermark(dispatcher, deliveries, 1); err == nil {
		t.Fatal("nil delivery did not continue to exact failure")
	}

	ambiguousMappings := &gatewaySessionMappings{bindings: map[string]gatewaySessionBinding{
		"one": {live: "shared", actor: actor},
		"two": {live: "shared", actor: &gatewaySessionActor{}},
	}}
	ambiguousDispatcher := &gatewayTransportDispatcher{
		generation: 1, transport: &gatewayTransport{generation: 1, mappings: ambiguousMappings},
	}
	if err := server.dispatchGatewayEvent(ambiguousDispatcher, Event{Type: evtMessageDelta, SessionID: "shared"}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("ambiguous live binding = %v", err)
	}

	server.actorsByStored = nil
	created := server.actorForTransportSession(server.transport, "new-stored", "new-live")
	if created == nil {
		t.Fatal("nil actor map was not initialized")
	}
	created.failPending(server.transport.generation, errors.New("stop created actor"))
	<-created.done

	closedServer, closedActor := newDirectGatewayActor()
	close(closedServer.closed)
	closedActor.mailbox <- gatewayActorMessage{bind: &gatewayActorBinding{done: make(chan struct{})}}
	if got := closedServer.actorForTransportSession(closedServer.transport, "stored", "live"); got != nil {
		t.Fatalf("server-close bind returned actor %#v", got)
	}

	cycle := &gatewayCycle{
		id: "cycle", controls: map[gatewayControlIdentity]struct{}{{kind: gatewayControlPermission, requestID: "request"}: {}},
		activeTools: map[string]struct{}{"tool": {}},
	}
	actor.transport = server.transport
	actor.active = cycle
	baseRoute := gatewayControlRoute{
		actor: actor, transport: server.transport, mappings: server.transport.mappings,
		generation: 1, stored: actor.stored, live: actor.live, cycleID: cycle.id,
		requestID: "request", toolCallID: "tool", kind: gatewayControlPermission,
	}
	runControl := func(route gatewayControlRoute, kind gatewayControlKind) error {
		completed := make(chan error, 1)
		actor.completeControl(&gatewayControlCommand{route: &route, kind: kind, completed: completed})

		return <-completed
	}
	missingTool := baseRoute
	missingTool.toolCallID = ""
	if err := runControl(missingTool, gatewayControlPermission); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("missing tool = %v", err)
	}
	inactiveTool := baseRoute
	inactiveTool.toolCallID = "inactive"
	if err := runControl(inactiveTool, gatewayControlPermission); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("inactive tool = %v", err)
	}
	staleTransport := baseRoute
	server.dispatchers = map[uint64]*gatewayTransportDispatcher{}
	if err := runControl(staleTransport, gatewayControlPermission); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("stale transport = %v", err)
	}
	server.dispatchers[1] = dispatcher
	staleMapping := baseRoute
	server.transport.mappings.bindings[actor.stored] = gatewaySessionBinding{live: "other", actor: actor}
	if err := runControl(staleMapping, gatewayControlPermission); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("stale mapping = %v", err)
	}
	server.transport.mappings.bindings[actor.stored] = gatewaySessionBinding{live: actor.live, actor: actor}
	unknown := baseRoute
	unknown.kind = gatewayControlKind(99)
	cycle.controls[gatewayControlIdentity{kind: unknown.kind, requestID: unknown.requestID}] = struct{}{}
	if err := runControl(unknown, unknown.kind); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("unknown control kind = %v", err)
	}

	deadRoute := baseRoute
	deadRoute.actor = &gatewaySessionActor{
		mailbox: make(chan gatewayActorMessage, 1), terminal: make(chan error, 1), done: make(chan struct{}),
	}
	close(deadRoute.actor.done)
	if err := server.completeGatewayControl(t.Context(), &deadRoute, gatewayControlPermission, "", false, nil); err == nil {
		t.Fatal("dead control actor completed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	waitingActor := &gatewaySessionActor{
		mailbox: make(chan gatewayActorMessage, 1), terminal: make(chan error, 1), done: make(chan struct{}),
	}
	cancelledRoute := baseRoute
	cancelledRoute.actor = waitingActor
	if err := server.completeGatewayControl(ctx, &cancelledRoute, gatewayControlPermission, "", false, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled control = %v", err)
	}

	latched := &gatewaySessionActor{
		generation: 1, mailbox: make(chan gatewayActorMessage, 1), terminal: make(chan error, 1), done: make(chan struct{}),
	}
	latched.terminal <- errors.New("latched")
	latched.queueLatchedTerminal()
	if !latched.terminalQueued {
		t.Fatal("latched terminal was not queued")
	}
	overflow := &gatewaySessionActor{
		generation: 1, mailbox: make(chan gatewayActorMessage, 1), terminal: make(chan error, 1), done: make(chan struct{}),
	}
	overflow.mailbox <- gatewayActorMessage{}
	overflow.terminal <- errors.New("latched")
	overflow.queueLatchedTerminal()
	if overflow.terminalQueued {
		t.Fatal("full mailbox reported terminal queued")
	}

	if err := server.completeGatewayControl(t.Context(), nil, gatewayControlPermission, "", false, nil); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("nil control route = %v", err)
	}

	fullServer, fullActor := newDirectGatewayActor()
	for len(fullActor.mailbox) < gatewayActorMailboxCapacity {
		fullActor.mailbox <- gatewayActorMessage{}
	}
	if got := fullServer.actorForTransportSession(fullServer.transport, fullActor.stored, fullActor.live); got != nil {
		t.Fatalf("full actor bind returned %#v", got)
	}

	doneServer, doneBindingActor := newDirectGatewayActor()
	bindReceived := make(chan struct{})
	go func() {
		<-doneBindingActor.mailbox
		close(bindReceived)
		close(doneBindingActor.done)
	}()
	gotDone := make(chan *gatewaySessionActor, 1)
	go func() {
		gotDone <- doneServer.actorForTransportSession(doneServer.transport, doneBindingActor.stored, doneBindingActor.live)
	}()
	<-bindReceived
	if got := <-gotDone; got != nil {
		t.Fatalf("actor completion bind returned %#v", got)
	}

	emitServer, emitActor := newDirectGatewayActor()
	emitServer.deliveries = make(chan TurnDelivery, 1)
	emitActor.handleRaw(emitActor.generation, Event{Type: "global", Raw: json.RawMessage(`{}`)})
	if emitServer.transport.dispatcher.terminalCause() == nil {
		t.Fatal("raw publication failure did not fence generation")
	}

	routeServer, routeActor := newDirectGatewayActor()
	routeServer.deliveries = make(chan TurnDelivery, 1)
	if err := routeActor.routeRaw(Event{Type: evtMessageDelta}); err == nil {
		t.Fatal("autonomous start publication failure was ignored")
	}

	completeServer, completeActor := newDirectGatewayActor()
	completeServer.deliveries = make(chan TurnDelivery, 1)
	completeCycle := &gatewayCycle{id: "complete", origin: CycleOriginActivity}
	completeActor.active = completeCycle
	if err := completeActor.completeCycle(completeCycle, NativeMessage{}, nil, Event{}); err == nil {
		t.Fatal("terminal publication failure was ignored")
	}

	ended := &gatewaySessionActor{
		mailbox: make(chan gatewayActorMessage, 1), terminal: make(chan error, 1), done: make(chan struct{}),
	}
	close(ended.done)
	if err := ended.enqueueCause(); !errors.Is(err, ErrGatewayDisconnected) {
		t.Fatalf("ended actor enqueue cause = %v", err)
	}
	openActor := &gatewaySessionActor{
		mailbox: make(chan gatewayActorMessage, 1), terminal: make(chan error, 1), done: make(chan struct{}),
	}
	if err := openActor.enqueueCause(); !errors.Is(err, ErrGatewayActorOverflow) {
		t.Fatalf("open actor fallback cause = %v", err)
	}

	heldActor := &gatewaySessionActor{
		server: server, stored: "stored", live: "live", generation: 1,
		mailbox: make(chan gatewayActorMessage, 2), terminal: make(chan error, 1), done: make(chan struct{}),
		projections: make(map[*gatewayProjection]struct{}),
	}
	heldActor.prompt = &gatewayCycle{
		id: "prompt", watermarkSet: true, watermark: 1,
		heldEvents: []Event{{Type: evtMessageDelta, InboundSequence: 2}},
	}
	heldActor.active = &gatewayCycle{id: "activity"}
	if err := heldActor.releasePrompt(&gatewayPromptRelease{cycleID: "prompt"}); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("held event route failure = %v", err)
	}
	invalidRoute := baseRoute
	invalidRoute.transport = nil
	if err := runControl(invalidRoute, gatewayControlPermission); !errors.Is(err, ErrGatewayAmbiguousTurn) {
		t.Fatalf("structurally invalid control = %v", err)
	}
}

func TestGatewayDispatcherAndHandshakeDeterministicFailureBarriers(t *testing.T) {
	t.Run("dispatcher initializes map and skips nil delivery", func(t *testing.T) {
		deliveries := make(chan GatewayDelivery, 1)
		client := &Client{
			deliveries: deliveries, done: make(chan struct{}), pending: make(map[int64]chan rpcResponse),
			closeTransport: func(websocket.StatusCode, string) error { return nil },
		}
		server := &hermesServer{closed: make(chan struct{}), deliveries: make(chan TurnDelivery, 2)}
		generation := server.installGatewayDispatcher(client)
		if generation == 0 || server.dispatchers == nil {
			t.Fatal("dispatcher map was not initialized")
		}
		server.transport.client = nil
		deliveries <- GatewayDelivery{}
		close(deliveries)
		<-server.transport.dispatcher.done
		if err := server.Close(t.Context()); err != nil {
			t.Fatalf("close initialized dispatcher fixture: %v", err)
		}
	})

	t.Run("accepted barrier drains exact error", func(t *testing.T) {
		deliveries := make(chan GatewayDelivery, 1)
		client := &Client{
			deliveries: deliveries, done: make(chan struct{}),
			closeTransport: func(websocket.StatusCode, string) error { return nil },
		}
		server := &hermesServer{
			closed: make(chan struct{}), deliveries: make(chan TurnDelivery, 2),
			dispatchers: make(map[uint64]*gatewayTransportDispatcher),
		}
		dispatcher := &gatewayTransportDispatcher{
			generation: 1, client: client, done: make(chan struct{}), barriers: make(chan gatewayDispatchBarrier),
			pendingFrames: make(map[string][]Event), pendingHandshakes: make(map[uint64]gatewayHandshakeKind),
		}
		transport := &gatewayTransport{
			client: client, generation: 1, dispatcher: dispatcher,
			mappings: &gatewaySessionMappings{bindings: make(map[string]gatewaySessionBinding)},
		}
		dispatcher.transport = transport
		server.transport = transport
		server.dispatchers[1] = dispatcher
		want := errors.New("barrier drain failed")
		dispatcher.beforeBarrierDrain = func() { deliveries <- GatewayDelivery{Err: want} }
		server.dispatcherWG.Add(1)
		barrier := gatewayDispatchBarrier{reply: make(chan error, 1)}
		barrierSent := make(chan struct{})
		go func() {
			dispatcher.barriers <- barrier
			close(barrierSent)
		}()
		go server.dispatchGateway(dispatcher)
		<-barrierSent
		if err := <-barrier.reply; !errors.Is(err, want) {
			t.Fatalf("barrier result = %v", err)
		}
		<-dispatcher.done
		if err := server.Close(t.Context()); err != nil {
			t.Fatalf("close failed-barrier dispatcher fixture: %v", err)
		}
	})

	t.Run("drain routes exact dispatch error", func(t *testing.T) {
		server, actor := newDirectGatewayActor()
		mappings := &gatewaySessionMappings{bindings: map[string]gatewaySessionBinding{
			"one": {live: "shared", actor: actor}, "two": {live: "shared", actor: &gatewaySessionActor{}},
		}}
		dispatcher := &gatewayTransportDispatcher{generation: 1}
		dispatcher.transport = &gatewayTransport{generation: 1, dispatcher: dispatcher, mappings: mappings}
		deliveries := make(chan GatewayDelivery, 1)
		event := Event{Type: evtMessageDelta, SessionID: "shared", InboundSequence: 1}
		deliveries <- GatewayDelivery{Event: &event}
		if err := server.drainGatewayThroughWatermark(dispatcher, deliveries, 1); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("dispatch drain = %v", err)
		}
	})

	t.Run("begin cancellation and malformed acknowledgement", func(t *testing.T) {
		server := &hermesServer{closed: make(chan struct{})}
		transport := &gatewayTransport{dispatcher: &gatewayTransportDispatcher{
			done: make(chan struct{}), handshakes: make(chan gatewayHandshakeCommand),
		}}
		ctx, cancel := context.WithCancel(t.Context())
		transport.dispatcher.beforeHandshakeSelect = cancel
		if _, err := server.beginGatewayHandshake(ctx, transport, gatewayHandshakeCreate); !errors.Is(err, context.Canceled) {
			t.Fatalf("begin select cancellation = %v", err)
		}
		transport.dispatcher.beforeHandshakeSelect = nil
		go func() {
			command := <-transport.dispatcher.handshakes
			command.reply <- gatewayHandshakeResult{}
		}()
		if _, err := server.beginGatewayHandshake(t.Context(), transport, gatewayHandshakeCreate); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("malformed begin acknowledgement = %v", err)
		}
		_ = transport.dispatcher.setCause(errors.New("dispatcher done"))
		close(transport.dispatcher.done)
		if result := server.cancelGatewayHandshake(transport, 9); result.err == nil {
			t.Fatal("cancel ignored dispatcher loss")
		}
	})

	t.Run("bind cancellation after precheck", func(t *testing.T) {
		server := &hermesServer{closed: make(chan struct{})}
		transport := &gatewayTransport{dispatcher: &gatewayTransportDispatcher{
			done: make(chan struct{}), handshakes: make(chan gatewayHandshakeCommand),
		}}
		ctx, cancel := context.WithCancel(t.Context())
		transport.dispatcher.beforeHandshakeSelect = cancel
		transport.dispatcher.beforeHandshakeCancel = func() {
			go func() {
				command := <-transport.dispatcher.handshakes
				command.reply <- gatewayHandshakeResult{state: gatewayHandshakeTombstoned}
			}()
		}
		result := server.bindGatewayHandshake(ctx, transport, 1, gatewayHandshakeCreate, "stored", "live", GatewayWatermark{})
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("bind select cancellation = %#v", result)
		}
	})
}

func TestCancelledPromptRegistrationRefusesWithoutRetainedTombstone(t *testing.T) {
	_, actor := newDirectGatewayActor()
	want := errors.New("caller cancelled")
	registration := &gatewayPromptRegistration{
		id: "registration", result: make(chan gatewayCycleResult, 1), reply: make(chan gatewayPromptHandle, 1),
	}
	registration.cancel(want)
	actor.registerPrompt(registration)
	if handle := <-registration.reply; !errors.Is(handle.err, want) {
		t.Fatalf("registration cancellation = %v", handle.err)
	}
}
