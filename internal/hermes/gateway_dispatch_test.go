package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func newDirectGatewayActor() (*hermesServer, *gatewaySessionActor) {
	server := &hermesServer{
		deliveries:     make(chan TurnDelivery, 32),
		closed:         make(chan struct{}),
		actorsByStored: make(map[string]*gatewaySessionActor),
		dispatchers:    make(map[uint64]*gatewayTransportDispatcher),
	}
	actor := &gatewaySessionActor{
		server:      server,
		stored:      "stored",
		live:        "live",
		generation:  1,
		mailbox:     make(chan gatewayActorMessage, gatewayActorMailboxCapacity+1),
		terminal:    make(chan error, 1),
		done:        make(chan struct{}),
		projections: make(map[*gatewayProjection]struct{}),
	}
	server.actorsByStored[actor.stored] = actor
	mappings := &gatewaySessionMappings{
		bindings: map[string]gatewaySessionBinding{
			actor.stored: {live: actor.live, actor: actor},
		},
	}
	dispatcher := &gatewayTransportDispatcher{
		generation: 1, pendingFrames: make(map[string][]Event), pendingHandshakes: make(map[uint64]gatewayHandshakeKind),
	}
	server.transport = &gatewayTransport{generation: 1, dispatcher: dispatcher, mappings: mappings}
	dispatcher.transport = server.transport
	server.dispatchers[1] = dispatcher

	return server, actor
}

func mustTurnEvent(tb testing.TB, deliveries <-chan TurnDelivery) TurnEvent {
	tb.Helper()

	return turnEventFromDelivery(tb, <-deliveries)
}

func turnEventFromDelivery(tb testing.TB, delivery TurnDelivery) TurnEvent {
	tb.Helper()
	if delivery.Err != nil || delivery.Event == nil {
		tb.Fatalf("ordered delivery is not an event: %#v", delivery)
	}

	return *delivery.Event
}

func mustTurnError(tb testing.TB, deliveries <-chan TurnDelivery) error {
	tb.Helper()
	for delivery := range deliveries {
		if delivery.Err != nil && delivery.Event == nil {
			return delivery.Err
		}
		if delivery.Event == nil {
			tb.Fatalf("ordered delivery has neither event nor error: %#v", delivery)
		}
	}

	tb.Fatal("ordered delivery source closed without an error")

	return nil
}

func bindTestGatewaySession(tb testing.TB, server *hermesServer, stored string, live string) {
	tb.Helper()
	transport := server.beginGatewayTurn()
	if transport == nil {
		tb.Fatal("gateway transport unavailable")

		return
	}
	defer server.endGatewayTurn()
	handshake, err := server.beginGatewayHandshake(context.Background(), transport, gatewayHandshakeResume)
	if err != nil {
		tb.Fatalf("record test gateway handshake: %v", err)
	}
	result := server.bindGatewayHandshake(context.Background(), transport, handshake, gatewayHandshakeResume,
		stored, live, GatewayWatermark{TransportGeneration: transport.generation})
	if result.err != nil || result.state != gatewayHandshakeCommitted {
		tb.Fatalf("commit test gateway handshake: state=%v err=%v", result.state, result.err)
	}
}

func testGatewayControlRequests(
	tb testing.TB,
	server *hermesServer,
	stored string,
	live string,
) (PermissionRequest, QuestionRequest) {
	tb.Helper()

	transport := server.gatewayTransport()
	if transport == nil {
		tb.Fatal("gateway transport unavailable")
	}

	events := []Event{
		{Type: evtToolStart, SessionID: live, Payload: json.RawMessage(`{"tool_id":"tool","name":"edit","args":{}}`)},
		{Type: evtApprovalRequest, SessionID: live, Payload: json.RawMessage(`{"command":"edit"}`)},
		{Type: evtClarifyRequest, SessionID: live, Payload: json.RawMessage(`{"request_id":"test-active-question","question":"continue?"}`)},
	}
	for index := range events {
		events[index].InboundSequence = uint64(index + 1)
		if err := server.dispatchGatewayEvent(transport.dispatcher, events[index]); err != nil {
			tb.Fatalf("dispatch control event: %v", err)
		}
	}

	var permission *PermissionRequest
	var question *QuestionRequest
	for permission == nil || question == nil {
		event := mustTurnEvent(tb, server.deliveries)
		if event.Permission != nil {
			permission = event.Permission
		}
		if event.Question != nil && event.Question.ID == "test-active-question" {
			question = event.Question
		}
	}
	if !strings.HasPrefix(permission.ID, "hermes/"+stored+"/cycle-") ||
		!strings.HasSuffix(permission.ID, "/permission-1") || permission.Tool.CallID != "tool" {
		tb.Fatalf("adapter permission identity = %#v", permission)
	}

	return *permission, *question
}

// TestGatewayMintedApprovalIdentityIsAnswerable pins the settlement half of the
// minted permission identity. Hermes 0.20 hands an approval no request identity
// and no tool identity, and approval.respond takes a session and a choice: an
// approval the adapter had to mint an identity for is answered exactly as a
// tool-bound one is, never refused for owning no native tool.
func TestGatewayMintedApprovalIdentityIsAnswerable(t *testing.T) {
	for _, test := range []struct {
		name  string
		tools []Event
	}{
		{name: "no active native tool"},
		{name: "several active native tools", tools: []Event{
			{Type: evtToolStart, Payload: json.RawMessage(`{"tool_id":"tool-a","name":"edit","args":{}}`)},
			{Type: evtToolStart, Payload: json.RawMessage(`{"tool_id":"tool-b","name":"read","args":{}}`)},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			server := newGatewayBackedHermesServer(t, fake, "")
			t.Cleanup(func() { _ = server.Close(context.Background()) })
			bindTestGatewaySession(t, server, "stored", "live-stored")

			transport := server.gatewayTransport()
			events := append(append([]Event(nil), test.tools...),
				Event{Type: evtApprovalRequest, Payload: json.RawMessage(`{"command":"rm -rf"}`)})

			var permission *PermissionRequest
			for index := range events {
				events[index].SessionID = "live-stored"
				events[index].InboundSequence = uint64(index + 1)
				if err := server.dispatchGatewayEvent(transport.dispatcher, events[index]); err != nil {
					t.Fatalf("dispatch %s: %v", events[index].Type, err)
				}
			}
			for permission == nil {
				if event := mustTurnEvent(t, server.deliveries); event.Permission != nil {
					permission = event.Permission
				}
			}

			if permission.Tool.CallID != permission.ID {
				t.Fatalf("unattributable approval did not mint its own call identity: %#v", permission)
			}

			if err := server.ReplyPermission(t.Context(), *permission, valAlways, ""); err != nil {
				t.Fatalf("ReplyPermission on a minted identity: %v", err)
			}

			calls := fake.callsFor("approval.respond")
			if len(calls) != 1 || calls[0].Params["choice"] != valAlways ||
				calls[0].Params["all"] != true || calls[0].Params[fieldSessionID] != "live-stored" {
				t.Fatalf("native approval.respond calls = %#v", calls)
			}

			// The control is settled, so the same minted identity cannot be
			// answered twice into one native FIFO queue.
			if err := server.ReplyPermission(t.Context(), *permission, valOnce, ""); !errors.Is(err, ErrGatewayAmbiguousTurn) {
				t.Fatalf("repeat answer = %v", err)
			}
			if calls := fake.callsFor("approval.respond"); len(calls) != 1 {
				t.Fatalf("approval.respond calls after repeat = %d, want 1", len(calls))
			}
		})
	}
}

func TestGatewayDispatcherPreservesGlobalInboundPublicationOrder(t *testing.T) {
	server := &hermesServer{
		deliveries:     make(chan TurnDelivery, 8),
		closed:         make(chan struct{}),
		actorsByStored: make(map[string]*gatewaySessionActor),
		dispatchers:    make(map[uint64]*gatewayTransportDispatcher),
	}
	client := &Client{
		deliveries:     make(chan GatewayDelivery, 4),
		done:           make(chan struct{}),
		pending:        make(map[int64]chan rpcResponse),
		closeTransport: func(websocket.StatusCode, string) error { return nil },
	}
	server.installGatewayDispatcher(client)
	transport := server.gatewayTransport()
	if transport == nil {
		t.Fatal("gateway transport was not installed")
	}
	actorA := server.actorForTransportSession(transport, "stored-a", "live-a")
	actorB := server.actorForTransportSession(transport, "stored-b", "live-b")
	if actorA == nil || actorB == nil {
		t.Fatal("gateway actors were not installed")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	actorA.beforeRegister = func() {
		close(entered)
		<-release
	}
	registration := &gatewayPromptRegistration{
		id: "publication-order-blocker", result: make(chan gatewayCycleResult, 1), reply: make(chan gatewayPromptHandle, 1),
	}
	if !actorA.enqueue(gatewayActorMessage{register: registration}) {
		t.Fatal("could not install publication-order blocker")
	}
	<-entered

	client.deliveries <- GatewayDelivery{Event: &Event{
		Type: "test.a", SessionID: "live-a", Raw: json.RawMessage(`{"source":"A"}`), InboundSequence: 41,
	}}
	client.deliveries <- GatewayDelivery{Event: &Event{
		Type: "test.b", SessionID: "live-b", Raw: json.RawMessage(`{"source":"B"}`), InboundSequence: 42,
	}}

	select {
	case early := <-server.deliveries:
		t.Fatalf("later actor published before earlier inbound work completed: %#v", early)
	default:
	}

	close(release)
	first := mustTurnEvent(t, server.deliveries)
	second := mustTurnEvent(t, server.deliveries)
	if string(first.Raw) != `{"source":"A"}` || string(second.Raw) != `{"source":"B"}` {
		t.Fatalf("global publication order = %s then %s", first.Raw, second.Raw)
	}

	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("close gateway dispatcher fixture: %v", err)
	}
}

func registerDirectPrompt(actor *gatewaySessionActor) gatewayPromptHandle {
	reply := make(chan gatewayPromptHandle, 1)
	actor.registerPrompt(&gatewayPromptRegistration{
		id:     "direct-registration",
		result: make(chan gatewayCycleResult, 1),
		reply:  reply,
	})

	return <-reply
}

func TestGatewayPromptWatermarkProjectsOlderCycleBeforePrompt(t *testing.T) {
	server, actor := newDirectGatewayActor()
	handle := registerDirectPrompt(actor)

	for _, event := range []Event{
		{Type: evtMessageDelta, InboundSequence: 4, Payload: json.RawMessage(`{"text":"late"}`)},
		{Type: evtMessageComplete, InboundSequence: 5, Payload: json.RawMessage(`{"text":"late"}`)},
		{Type: evtMessageDelta, InboundSequence: 7, Payload: json.RawMessage(`{"text":"new"}`)},
		{Type: evtMessageComplete, InboundSequence: 8, Payload: json.RawMessage(`{"text":"new"}`)},
	} {
		actor.handleRaw(1, event)
	}

	watermark := actor.applyPromptWatermark(&gatewayPromptWatermark{cycleID: handle.cycleID, watermark: 6})
	if watermark.err != nil || len(watermark.projections) != 1 {
		t.Fatalf("watermark result = %#v", watermark)
	}

	started := mustTurnEvent(t, server.deliveries)
	part := mustTurnEvent(t, server.deliveries)
	completed := mustTurnEvent(t, server.deliveries)
	if started.Type != EventCycleStarted || started.Origin != CycleOriginActivity ||
		part.Type != evtMessagePartUpdated || completed.Type != EventCycleComplete ||
		completed.Origin != CycleOriginActivity || completed.Message == nil || completed.Message.Parts[0].Text != "late" {
		t.Fatalf("autonomous projection = %#v / %#v / %#v", started, part, completed)
	}
	select {
	case result := <-handle.result:
		t.Fatalf("old completion settled the prompt: %#v", result)
	default:
	}

	completed.ProjectionDone(nil)
	if err := <-watermark.projections[0].done; err != nil {
		t.Fatalf("autonomous projection result = %v", err)
	}
	if err := actor.releasePrompt(&gatewayPromptRelease{cycleID: handle.cycleID}); err != nil {
		t.Fatalf("release prompt: %v", err)
	}

	promptPart := mustTurnEvent(t, server.deliveries)
	promptTerminal := mustTurnEvent(t, server.deliveries)
	result := <-handle.result
	if promptPart.CycleID != handle.cycleID || promptTerminal.CycleID != handle.cycleID ||
		result.err != nil || result.message.Parts[0].Text != "new" {
		t.Fatalf("prompt projection = %#v / %#v / %#v", promptPart, promptTerminal, result)
	}
	if started.CycleID == handle.cycleID {
		t.Fatal("autonomous and prompt work reused one cycle id")
	}
}

func TestGatewayDispatcherWatermarkWaitsForOlderProjection(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setPromptBeforeResult(
		Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"late"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"late"}`)},
	)
	fake.setPromptEvents(
		Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"new"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"new"}`)},
	)
	server := newGatewayBackedHermesServer(t, fake, "")
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	bindTestGatewaySession(t, server, "stored", "live-stored")

	activityProjected := make(chan struct{})
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for delivery := range server.deliveries {
			if delivery.Event == nil {
				continue
			}
			event := *delivery.Event
			if event.Origin == CycleOriginActivity && event.Type == EventCycleComplete {
				close(activityProjected)
				event.ProjectionDone(nil)
			}
		}
	}()

	dispatchCtx := WithPromptDispatch(t.Context(), func(context.Context, PromptDispatchInfo) error {
		select {
		case <-activityProjected:
			return nil
		default:
			return errors.New("prompt dispatch preceded older autonomous projection")
		}
	})
	message, err := server.SendMessage(dispatchCtx, "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}})
	if err != nil || len(message.Parts) != 1 || message.Parts[0].Text != "new" {
		t.Fatalf("SendMessage = %#v, %v", message, err)
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-consumerDone
}

func TestGatewayResumeHandshakeSuppressesReplayAndDeliversPostResponseWorkOnce(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setResumeEvents(
		[]Event{
			{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"replay"}`)},
			{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"replay"}`)},
		},
		[]Event{
			{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"new"}`)},
			{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"new"}`)},
		},
	)
	server := newGatewayBackedHermesServer(t, fake, "")

	live, err := server.ensureLiveGatewaySession(t.Context(), "stored")
	if err != nil || live != "live-stored" {
		t.Fatalf("resume = %q, %v", live, err)
	}

	started := mustTurnEvent(t, server.deliveries)
	part := mustTurnEvent(t, server.deliveries)
	completed := mustTurnEvent(t, server.deliveries)
	if started.Type != EventCycleStarted || part.Type != evtMessagePartUpdated ||
		completed.Type != EventCycleComplete || completed.Message == nil ||
		completed.Message.Parts[0].Text != "new" {
		t.Fatalf("post-response projection = %#v / %#v / %#v", started, part, completed)
	}
	completed.ProjectionDone(nil)
	select {
	case delivery := <-server.deliveries:
		extra := turnEventFromDelivery(t, delivery)
		t.Fatalf("resume replay or duplicate escaped: %#v", extra)
	default:
	}

	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestGatewayCreateHandshakeDeliversFramesAcrossResponseWatermark(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setCreateEvents(
		[]Event{{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"created "}`)}},
		[]Event{{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"created work"}`)}},
	)
	server := newGatewayBackedHermesServer(t, fake, "")

	created, err := server.CreateSession(t.Context(), "Created")
	if err != nil || created.ID != "stored-1" {
		t.Fatalf("CreateSession = %#v, %v", created, err)
	}
	started := mustTurnEvent(t, server.deliveries)
	part := mustTurnEvent(t, server.deliveries)
	completed := mustTurnEvent(t, server.deliveries)
	if started.Type != EventCycleStarted || part.Type != evtMessagePartUpdated ||
		completed.Type != EventCycleComplete || completed.Message == nil ||
		completed.Message.Parts[0].Text != "created work" {
		t.Fatalf("create projection = %#v / %#v / %#v", started, part, completed)
	}
	completed.ProjectionDone(nil)
	select {
	case delivery := <-server.deliveries:
		extra := turnEventFromDelivery(t, delivery)
		t.Fatalf("create frame duplicated: %#v", extra)
	default:
	}

	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestGatewayCreateAndResumeRefuseCancelledHandshakeBindings(t *testing.T) {
	for _, operation := range []string{"create", "resume"} {
		t.Run(operation, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			server := newGatewayBackedHermesServer(t, fake, "")
			t.Cleanup(func() { _ = server.Close(context.Background()) })
			server.beforeHandshakeBind = func(_ gatewayHandshakeKind, id uint64) {
				server.cancelGatewayHandshake(server.gatewayTransport(), id)
			}

			var err error
			if operation == "create" {
				_, err = server.CreateSession(t.Context(), "cancelled binding")
			} else {
				_, err = server.ensureLiveGatewaySession(t.Context(), "stored")
			}
			if !errors.Is(err, ErrGatewayAmbiguousTurn) {
				t.Fatalf("%s cancelled binding = %v", operation, err)
			}
		})
	}
}

// TestGatewayAbandonedCycleIsRetiredForTheTurnHermesJustStarted pins the
// "streaming" claim against a cycle that outlived its turn. Hermes claims an
// idle session synchronously, so the open cycle has every frame it will ever
// get: it is stated as a cycle with no native terminal and the prompt keeps the
// turn hermes accepted for it, rather than being disowned into that cycle.
func TestGatewayAbandonedCycleIsRetiredForTheTurnHermesJustStarted(t *testing.T) {
	server, actor := newDirectGatewayActor()
	handle := registerDirectPrompt(actor)
	actor.handleRaw(1, Event{
		Type: evtMessageDelta, InboundSequence: 4, Payload: json.RawMessage(`{"text":"unfinished"}`),
	})
	actor.handleRaw(1, Event{
		Type: evtMessageDelta, InboundSequence: 7, Payload: json.RawMessage(`{"text":"prompt answer"}`),
	})

	result := actor.applyPromptWatermark(&gatewayPromptWatermark{cycleID: handle.cycleID, watermark: 6})
	if result.err != nil || len(result.projections) != 1 {
		t.Fatalf("streaming claim over an abandoned cycle = %#v", result)
	}
	if actor.prompt == nil || actor.prompt.id != handle.cycleID {
		t.Fatalf("prompt lost the turn hermes accepted for it: %#v", actor.prompt)
	}
	if actor.active != nil {
		t.Fatalf("abandoned cycle stayed open: %#v", actor.active)
	}

	started := mustTurnEvent(t, server.deliveries)
	part := mustTurnEvent(t, server.deliveries)
	retired := mustTurnEvent(t, server.deliveries)
	if started.Type != EventCycleStarted || part.Type != evtMessagePartUpdated ||
		retired.Type != EventCycleFailed || retired.Origin != CycleOriginActivity ||
		!errors.Is(retired.Err, errGatewayCycleAbandoned) {
		t.Fatalf("abandoned projection = %#v / %#v / %#v", started, part, retired)
	}

	if err := actor.releasePrompt(&gatewayPromptRelease{cycleID: handle.cycleID}); err != nil {
		t.Fatalf("release prompt: %v", err)
	}
	promptPart := mustTurnEvent(t, server.deliveries)
	if promptPart.CycleID != handle.cycleID || promptPart.Origin != CycleOriginPrompt {
		t.Fatalf("prompt frame = %#v", promptPart)
	}
	if cause := server.transport.dispatcher.terminalCause(); cause != nil {
		t.Fatalf("retirement failed the generation: %v", cause)
	}
}

// TestGatewayAbandonedCycleRetirementFailsClosedOnAnUnpublishableTerminal pins
// that retirement is a stated terminal, not a bookkeeping erasure: a cycle this
// adapter cannot publish the end of is a stream it cannot speak for.
func TestGatewayAbandonedCycleRetirementFailsClosedOnAnUnpublishableTerminal(t *testing.T) {
	server, actor := newDirectGatewayActor()
	handle := registerDirectPrompt(actor)
	actor.active = actor.newCycle(CycleOriginActivity)
	for len(server.deliveries) < cap(server.deliveries)-1 {
		server.deliveries <- TurnDelivery{Event: &TurnEvent{Type: EventGatewayRaw}}
	}

	result := actor.applyPromptWatermark(&gatewayPromptWatermark{cycleID: handle.cycleID, watermark: 6})
	if !errors.Is(result.err, ErrGatewayMappedOverflow) {
		t.Fatalf("unpublishable retirement = %v", result.err)
	}
	if cause := server.transport.dispatcher.terminalCause(); !errors.Is(cause, ErrGatewayMappedOverflow) {
		t.Fatalf("generation cause = %v", cause)
	}
}

// TestGatewayReleasedPromptStillFailsClosedOnAnUnroutableHeldFrame pins that the
// leniency stops at the frames themselves: a held frame the cycle taking them
// cannot state is still a stream this adapter cannot speak for.
func TestGatewayReleasedPromptStillFailsClosedOnAnUnroutableHeldFrame(t *testing.T) {
	for _, test := range []struct {
		name        string
		disposition gatewayPromptDisposition
	}{
		{name: "folded into the running turn", disposition: gatewayPromptJoinedTurn},
		{name: "queued as the next turn", disposition: gatewayPromptQueuedTurn},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, actor := newDirectGatewayActor()
			handle := registerDirectPrompt(actor)
			actor.handleRaw(1, Event{
				Type: evtMessageDelta, InboundSequence: 4, Payload: json.RawMessage(`{"text":"unfinished"}`),
			})
			actor.handleRaw(1, Event{Type: evtToolStart, InboundSequence: 7, Payload: json.RawMessage(`{}`)})

			result := actor.applyPromptWatermark(&gatewayPromptWatermark{
				cycleID: handle.cycleID, watermark: 6, disposition: test.disposition,
			})
			if !errors.Is(result.err, ErrGatewayAmbiguousTurn) {
				t.Fatalf("unroutable held frame = %v", result.err)
			}
			if cause := server.transport.dispatcher.terminalCause(); !errors.Is(cause, ErrGatewayAmbiguousTurn) {
				t.Fatalf("generation cause = %v", cause)
			}
		})
	}
}

// TestGatewayFoldedPromptIsReleasedToTheRunningCycle pins the redirect/steer
// disposition against a turn this adapter is already projecting: hermes folded
// the text into that turn, so the turn keeps every frame it was holding and the
// prompt states an outcome no host may retry.
func TestGatewayFoldedPromptIsReleasedToTheRunningCycle(t *testing.T) {
	server, actor := newDirectGatewayActor()
	handle := registerDirectPrompt(actor)
	actor.handleRaw(1, Event{
		Type: evtMessageDelta, InboundSequence: 4, Payload: json.RawMessage(`{"text":"unfinished"}`),
	})
	actor.handleRaw(1, Event{
		Type: evtMessageDelta, InboundSequence: 7, Payload: json.RawMessage(`{"text":" and more"}`),
	})

	result := actor.applyPromptWatermark(&gatewayPromptWatermark{
		cycleID: handle.cycleID, watermark: 6, disposition: gatewayPromptJoinedTurn,
	})
	if !errors.Is(result.err, ErrGatewayTurnAbsorbedPrompt) {
		t.Fatalf("folded prompt = %v, want %v", result.err, ErrGatewayTurnAbsorbedPrompt)
	}
	if errors.Is(result.err, ErrGatewayAgentBusy) {
		t.Fatal("an accepted prompt was reported as retryable contention")
	}
	if actor.prompt != nil {
		t.Fatalf("folded prompt was retained: %#v", actor.prompt)
	}
	if actor.active == nil || actor.active.text.String() != "unfinished and more" {
		t.Fatalf("running cycle lost the held frames: %#v", actor.active)
	}
	if cause := server.transport.dispatcher.terminalCause(); cause != nil {
		t.Fatalf("folded prompt failed the generation: %v", cause)
	}
}

// TestGatewayQueuedPromptIsServedByTheTurnHermesRunsForIt pins the queued
// disposition end to end. Hermes queues a prompt that lands on a busy session
// and runs it as the next turn: the running turn's frames stay agent-origin
// work, the queued turn fills the registered prompt cycle, and the user's text
// runs exactly once.
func TestGatewayQueuedPromptIsServedByTheTurnHermesRunsForIt(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setPromptStatus("queued")
	fake.setPromptEvents(
		Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"running-turn"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"running-turn"}`)},
		Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"queued-turn"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"queued-turn"}`)},
	)
	server := newGatewayBackedHermesServer(t, fake, "")
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	bindTestGatewaySession(t, server, "stored", "live-stored")

	activity := make(chan TurnEvent, 4)
	prompted := make(chan TurnEvent, 8)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for delivery := range server.deliveries {
			if delivery.Event == nil {
				continue
			}
			event := *delivery.Event
			if event.Origin == CycleOriginActivity {
				if event.ProjectionDone != nil {
					activity <- event
					event.ProjectionDone(nil)
				}

				continue
			}
			prompted <- event
		}
	}()

	message, err := server.SendMessage(
		withTestPromptDispatch(t.Context()), "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}},
	)
	if err != nil || len(message.Parts) != 1 || message.Parts[0].Text != "queued-turn" {
		t.Fatalf("queued SendMessage = %#v, %v", message, err)
	}

	select {
	case running := <-activity:
		if running.Type != EventCycleComplete || running.Message == nil ||
			running.Message.Parts[0].Text != "running-turn" {
			t.Fatalf("running turn projection = %#v", running)
		}
		if running.CycleID == message.Info.ID {
			t.Fatal("running turn and queued prompt shared one cycle")
		}
	default:
		t.Fatal("the turn hermes was already running was never projected")
	}

	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-consumerDone
	for len(prompted) > 0 {
		event := <-prompted
		if event.Origin != CycleOriginPrompt {
			continue
		}
		if strings.Contains(string(event.Raw), "running-turn") {
			t.Fatalf("the running turn's work was projected under the prompt: %#v", event)
		}
	}
}

// TestGatewayQueuedPromptHoldsEveryFramePastTheRunningTurnsTerminal pins the
// boundary inside one batch of held frames: the running turn's terminal can
// arrive with the queued turn's first frames behind it, and those frames are
// the prompt's — held until it is released, never spent on the cycle that just
// ended.
func TestGatewayQueuedPromptHoldsEveryFramePastTheRunningTurnsTerminal(t *testing.T) {
	server, actor := newDirectGatewayActor()
	handle := registerDirectPrompt(actor)
	for _, event := range []Event{
		{Type: evtMessageDelta, InboundSequence: 7, Payload: json.RawMessage(`{"text":"running-turn"}`)},
		{Type: evtMessageComplete, InboundSequence: 8, Payload: json.RawMessage(`{"text":"running-turn"}`)},
		{Type: evtMessageDelta, InboundSequence: 9, Payload: json.RawMessage(`{"text":"queued-turn"}`)},
	} {
		actor.handleRaw(1, event)
	}

	result := actor.applyPromptWatermark(&gatewayPromptWatermark{
		cycleID: handle.cycleID, watermark: 6, disposition: gatewayPromptQueuedTurn,
	})
	if result.err != nil || result.deferral == nil {
		t.Fatalf("queued watermark = %#v", result)
	}

	var resumed gatewayPromptWatermarkResult
	select {
	case resumed = <-result.deferral:
	default:
		t.Fatal("the running turn's terminal did not resume the queued prompt")
	}
	if resumed.err != nil || len(resumed.projections) != 1 {
		t.Fatalf("resumed queued prompt = %#v", resumed)
	}
	if actor.prompt == nil || actor.prompt.deferred || len(actor.prompt.heldEvents) != 1 {
		t.Fatalf("queued prompt state = %#v", actor.prompt)
	}
	if actor.active != nil {
		t.Fatalf("running cycle outlived its terminal: %#v", actor.active)
	}

	started := mustTurnEvent(t, server.deliveries)
	part := mustTurnEvent(t, server.deliveries)
	completed := mustTurnEvent(t, server.deliveries)
	if started.Origin != CycleOriginActivity || part.Origin != CycleOriginActivity ||
		completed.Type != EventCycleComplete || completed.Message == nil ||
		completed.Message.Parts[0].Text != "running-turn" || completed.CycleID == handle.cycleID {
		t.Fatalf("running turn projection = %#v / %#v / %#v", started, part, completed)
	}

	if err := actor.releasePrompt(&gatewayPromptRelease{cycleID: handle.cycleID}); err != nil {
		t.Fatalf("release queued prompt: %v", err)
	}
	promptPart := mustTurnEvent(t, server.deliveries)
	if promptPart.Origin != CycleOriginPrompt || promptPart.CycleID != handle.cycleID ||
		!strings.Contains(actor.prompt.text.String(), "queued-turn") {
		t.Fatalf("queued turn projection = %#v", promptPart)
	}
}

// TestGatewayFoldedPromptStreamsTheTurnItJoined pins the redirect/steer
// disposition in its ordinary shape: every autonomous driver marks the session
// running before it emits a frame, so a folded prompt commonly meets a turn
// this adapter has not seen a frame of, and that turn's output is the answer to
// the text hermes just folded into it.
func TestGatewayFoldedPromptStreamsTheTurnItJoined(t *testing.T) {
	for _, status := range []string{"redirected", "steered"} {
		t.Run(status, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			fake.setPromptStatus(status)
			fake.setPromptEvents(
				Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"folded answer"}`)},
				Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"folded answer"}`)},
			)
			server := newGatewayBackedHermesServer(t, fake, "")
			t.Cleanup(func() { _ = server.Close(context.Background()) })
			bindTestGatewaySession(t, server, "stored", "live-stored")

			transport := server.gatewayTransport()
			message, err := server.SendMessage(
				withTestPromptDispatch(t.Context()), "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}},
			)
			if err != nil || len(message.Parts) != 1 || message.Parts[0].Text != "folded answer" {
				t.Fatalf("%s SendMessage = %#v, %v", status, message, err)
			}
			if cause := transport.dispatcher.terminalCause(); cause != nil {
				t.Fatalf("%s status failed the generation: %v", status, cause)
			}
		})
	}
}

func TestGatewayUnprovenPromptResponseJoinsActorCancellationBeforeFencingTransport(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*fakeGatewayServer)
		wantType  bool
	}{
		{
			// Hermes answers a typed voice stop phrase by ending the voice chat
			// and stating no turn disposition at all. An answer this adapter
			// cannot attribute frames by is the one thing that still fails closed.
			name: "no stated disposition",
			configure: func(fake *fakeGatewayServer) {
				fake.setPromptResult(map[string]any{"voice_stopped": true})
			},
		},
		{
			name: "malformed successful result",
			configure: func(fake *fakeGatewayServer) {
				fake.setPromptResult(map[string]any{"status": map[string]any{"SECRET_SENTINEL": true}})
			},
			wantType: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeGatewayServer(t)
			test.configure(fake)
			server := newGatewayBackedHermesServer(t, fake, "")
			t.Cleanup(func() { _ = server.Close(context.Background()) })
			bindTestGatewaySession(t, server, "stored", "live-stored")

			transport := server.gatewayTransport()
			transport.mappings.mu.Lock()
			actor := transport.mappings.bindings["stored"].actor
			transport.mappings.mu.Unlock()
			cancelEntered := make(chan struct{})
			cancelRelease := make(chan struct{})
			actor.beforeCancel = func() {
				close(cancelEntered)
				<-cancelRelease
			}

			result := make(chan error, 1)
			go func() {
				_, err := server.SendMessage(withTestPromptDispatch(t.Context()), "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}})
				result <- err
			}()
			<-cancelEntered
			select {
			case err := <-result:
				t.Fatalf("SendMessage returned before actor cancellation acknowledgement: %v", err)
			default:
			}
			if server.gatewayTransport() != transport {
				t.Fatal("transport fenced before actor cancellation acknowledgement")
			}
			close(cancelRelease)

			err := <-result
			if !errors.Is(err, ErrGatewayDisconnected) {
				t.Fatalf("SendMessage error = %v, want disconnected", err)
			}
			if !errors.Is(err, ErrGatewayAmbiguousTurn) && !test.wantType {
				t.Fatalf("unknown status error = %v, want ambiguity", err)
			}
			if test.wantType {
				var typeError *json.UnmarshalTypeError
				if !errors.As(err, &typeError) {
					t.Fatalf("malformed result lost decode cause: %v", err)
				}
			}
			if cause := transport.dispatcher.terminalCause(); !errors.Is(cause, ErrGatewayDisconnected) {
				t.Fatalf("unproven response did not fence transport generation: %v", cause)
			}
		})
	}
}

func TestGatewayActorMailboxOverflowFencesTransport(t *testing.T) {
	server, actor := newDirectGatewayActor()
	for range gatewayActorMailboxCapacity {
		if !actor.enqueue(gatewayActorMessage{}) {
			t.Fatal("ordinary actor mailbox filled before its declared capacity")
		}
	}

	_ = server.dispatchGatewayEvent(server.transport.dispatcher, Event{Type: evtMessageDelta, SessionID: "live"})
	if err := mustTurnError(t, server.deliveries); !errors.Is(err, ErrGatewayActorOverflow) {
		t.Fatalf("actor overflow error = %v, want %v", err, ErrGatewayActorOverflow)
	}
	if len(actor.terminal) != 1 {
		t.Fatalf("actor terminal latch depth = %d, want 1", len(actor.terminal))
	}

	_ = server.dispatchGatewayEvent(server.transport.dispatcher, Event{Type: evtMessageDelta, SessionID: "live"})
	if len(server.deliveries) != 0 || len(actor.terminal) != 1 {
		t.Fatal("actor overflow was not latched exactly once")
	}
}

func TestGatewayTerminalBypassesSaturatedOrdinaryActorMailbox(t *testing.T) {
	server, actor := newDirectGatewayActor()
	server.actorWG.Add(1)
	go actor.run()

	entered := make(chan struct{})
	release := make(chan struct{})
	actor.beforeRegister = func() {
		close(entered)
		<-release
	}
	registration := &gatewayPromptRegistration{
		id: "blocked", result: make(chan gatewayCycleResult, 1), reply: make(chan gatewayPromptHandle, 1),
	}
	if !actor.enqueue(gatewayActorMessage{register: registration}) {
		t.Fatal("initial registration overflowed")
	}
	<-entered
	for range gatewayActorMailboxCapacity {
		if !actor.enqueue(gatewayActorMessage{}) {
			t.Fatal("ordinary mailbox saturated before its declared capacity")
		}
	}

	dispatched := make(chan struct{})
	wantCause := errors.New("native terminal")
	go func() {
		server.finishGatewayGeneration(server.transport.dispatcher, wantCause)
		close(dispatched)
	}()
	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("terminal generation blocked behind the ordinary actor mailbox")
	}
	if err := mustTurnError(t, server.deliveries); !errors.Is(err, ErrGatewayDisconnected) ||
		!strings.Contains(err.Error(), "native terminal") {
		t.Fatalf("terminal publication = %v", err)
	}

	close(release)
	handle := <-registration.reply
	if handle.err != nil || handle.cycleID == "" {
		t.Fatalf("registration = %#v", handle)
	}
	select {
	case <-actor.done:
	case <-time.After(time.Second):
		t.Fatal("actor did not consume its ordered terminal")
	}
	failed := mustTurnEvent(t, server.deliveries)
	if failed.Type != EventCycleFailed || failed.CycleID != handle.cycleID {
		t.Fatalf("ordered cycle failure = %#v", failed)
	}
	if !errors.Is(failed.Err, ErrGatewayDisconnected) || !errors.Is(failed.Err, wantCause) {
		t.Fatalf("ordered cycle cause = %v, want public transport marker and exact native cause", failed.Err)
	}
	select {
	case extra := <-server.deliveries:
		t.Fatalf("duplicate terminal delivery = %#v", extra)
	default:
	}
}

// TestGatewayPendingHandshakeRetentionStaysBounded pins that retention for a
// handshake in flight is bounded per native session as well as per transport,
// that reaching either bound costs the generation nothing — another native
// session's traffic must not be able to close the transport that owns the
// process — and that a frame the retention drops is stated rather than lost in
// silence.
func TestGatewayPendingHandshakeRetentionStaysBounded(t *testing.T) {
	server, _ := newDirectGatewayActor()
	overflows := &recordingLogHandler{}
	server.log = slog.New(overflows)
	dispatcher := server.transport.dispatcher
	dispatcher.pendingHandshakes[1] = gatewayHandshakeCreate

	retain := func(live string, count int) {
		t.Helper()
		for index := range count {
			if err := server.dispatchGatewayEvent(dispatcher, Event{
				Type: evtMessageDelta, SessionID: live, InboundSequence: uint64(index + 1),
			}); err != nil {
				t.Fatalf("%s retained frame %d = %v", live, index, err)
			}
		}
	}

	// One mirrored child run cannot spend more than its own share, so the
	// budget the session being bound is waiting to claim survives it.
	retain("mirrored-child", gatewayPendingFrameLiveCapacity+1)
	if got := len(dispatcher.pendingFrames["mirrored-child"]); got != gatewayPendingFrameLiveCapacity {
		t.Fatalf("one live id retained %d frames", got)
	}
	if dispatcher.pendingFrameCount != gatewayPendingFrameLiveCapacity {
		t.Fatalf("live-id overflow grew the transport buffer to %d", dispatcher.pendingFrameCount)
	}

	for index := range gatewayPendingFrameCapacity/gatewayPendingFrameLiveCapacity - 1 {
		retain(fmt.Sprintf("mirrored-sibling-%d", index), gatewayPendingFrameLiveCapacity)
	}
	retain("late-live", 1)
	if dispatcher.pendingFrameCount != gatewayPendingFrameCapacity {
		t.Fatalf("transport overflow grew pending buffer to %d", dispatcher.pendingFrameCount)
	}
	if _, retained := dispatcher.pendingFrames["late-live"]; retained {
		t.Fatal("a frame past the transport bound was retained")
	}
	if cause := dispatcher.terminalCause(); cause != nil {
		t.Fatalf("retention bound failed the generation: %v", cause)
	}

	if bounds := overflows.attributes("bound"); len(bounds) != 2 ||
		bounds[0] != "live_session" || bounds[1] != "transport" {
		t.Fatalf("stated retention overflows = %#v", bounds)
	}

	// The observability seam is optional: an embedder without a logger drops
	// the same frame and says nothing.
	server.log = nil
	retain("late-live", 1)
	if len(overflows.attributes("bound")) != 2 {
		t.Fatal("a dropped frame was stated without a logger")
	}
}

type recordingLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingLogHandler) WithGroup(string) slog.Handler { return h }

func (h *recordingLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record)

	return nil
}

func (h *recordingLogHandler) attributes(key string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	values := []string{}
	for _, record := range h.records {
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == key {
				values = append(values, attr.Value.String())
			}

			return true
		})
	}

	return values
}

func TestGatewayStaleTransportGenerationDoesNotProjectAfterResume(t *testing.T) {
	server, actor := newDirectGatewayActor()
	actor.generation = 2
	actor.handleRaw(1, Event{
		Type: evtMessageComplete, InboundSequence: 10, Payload: json.RawMessage(`{"text":"stale"}`),
	})
	if len(server.deliveries) != 0 {
		t.Fatalf("stale generation projected %d events", len(server.deliveries))
	}

	actor.handleRaw(2, Event{
		Type: evtMessageComplete, InboundSequence: 1, Payload: json.RawMessage(`{"text":"current"}`),
	})
	if started := mustTurnEvent(t, server.deliveries); started.TransportGeneration != 2 || started.Type != EventCycleStarted {
		t.Fatalf("current generation start = %#v", started)
	}
	if completed := mustTurnEvent(t, server.deliveries); completed.TransportGeneration != 2 || completed.Type != EventCycleComplete {
		t.Fatalf("current generation terminal = %#v", completed)
	}
}

func TestGatewayProviderFailureKeepsActorAvailableForLaterCycles(t *testing.T) {
	server, actor := newDirectGatewayActor()
	actor.handleRaw(1, Event{
		Type: evtMessageComplete, InboundSequence: 1,
		Payload: json.RawMessage(`{"status":"error","error":"provider failed"}`),
	})
	failedStart := mustTurnEvent(t, server.deliveries)
	failed := mustTurnEvent(t, server.deliveries)
	if failedStart.Type != EventCycleStarted || failed.Type != EventCycleFailed || failed.Err == nil {
		t.Fatalf("failed cycle = %#v / %#v", failedStart, failed)
	}
	failed.ProjectionDone(nil)
	if len(server.deliveries) != 0 {
		t.Fatalf("provider failure terminated gateway: %v", mustTurnError(t, server.deliveries))
	}

	actor.handleRaw(1, Event{
		Type: evtMessageComplete, InboundSequence: 2, Payload: json.RawMessage(`{"text":"autonomous"}`),
	})
	if started, completed := mustTurnEvent(t, server.deliveries), mustTurnEvent(t, server.deliveries); started.Type != EventCycleStarted ||
		completed.Type != EventCycleComplete || completed.Message == nil || completed.Message.Parts[0].Text != "autonomous" {
		t.Fatalf("later autonomous cycle = %#v / %#v", started, completed)
	} else {
		completed.ProjectionDone(nil)
	}

	handle := registerDirectPrompt(actor)
	watermark := actor.applyPromptWatermark(&gatewayPromptWatermark{cycleID: handle.cycleID, watermark: 3})
	if watermark.err != nil {
		t.Fatalf("later prompt watermark = %v", watermark.err)
	}
	if err := actor.releasePrompt(&gatewayPromptRelease{cycleID: handle.cycleID}); err != nil {
		t.Fatalf("later prompt release = %v", err)
	}
	actor.handleRaw(1, Event{
		Type: evtMessageComplete, InboundSequence: 4, Payload: json.RawMessage(`{"text":"prompt"}`),
	})
	terminal := mustTurnEvent(t, server.deliveries)
	result := <-handle.result
	if terminal.CycleID != handle.cycleID || result.err != nil || result.message.Parts[0].Text != "prompt" {
		t.Fatalf("later prompt = %#v / %#v", terminal, result)
	}
}

func TestGatewayCancelledRegistrationCannotLeakAnInstalledPrompt(t *testing.T) {
	server, actor := newDirectGatewayActor()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	actor.beforeRegister = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	server.actorWG.Add(1)
	go actor.run()
	t.Cleanup(func() {
		actor.mailbox <- gatewayActorMessage{stop: true}
		<-actor.done
	})

	first := &gatewayPromptRegistration{
		id: "cancelled-registration", result: make(chan gatewayCycleResult, 1), reply: make(chan gatewayPromptHandle, 1),
	}
	actor.mailbox <- gatewayActorMessage{register: first}
	<-entered
	want := context.Canceled
	first.cancel(want)
	cancelled := make(chan struct{})
	actor.mailbox <- gatewayActorMessage{cancel: &gatewayPromptCancel{
		registrationID: first.id, err: want, done: cancelled,
	}}
	close(release)

	if handle := <-first.reply; !errors.Is(handle.err, want) {
		t.Fatalf("cancelled registration reply = %#v", handle)
	}
	<-cancelled

	second := &gatewayPromptRegistration{
		id: "later-registration", result: make(chan gatewayCycleResult, 1), reply: make(chan gatewayPromptHandle, 1),
	}
	actor.mailbox <- gatewayActorMessage{register: second}
	if handle := <-second.reply; handle.err != nil || handle.cycleID == "" {
		t.Fatalf("later registration = %#v", handle)
	}
}

func TestGatewaySubmitCancellationBeforeActorReplyDoesNotBlockLaterPrompt(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"later"}`)})
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored", "live-stored")

	server.gatewayMu.Lock()
	actor := server.actorsByStored["stored"]
	server.gatewayMu.Unlock()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	actor.beforeRegister = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() {
		_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "cancel"}}})
		first <- err
	}()
	<-entered
	cancel()
	close(release)
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled registration = %v", err)
	}

	promptCtx := WithPromptDispatch(t.Context(), func(context.Context, PromptDispatchInfo) error { return nil })
	message, err := server.SendMessage(promptCtx, "stored", MessageRequest{Parts: []map[string]any{{"text": "later"}}})
	if err != nil || len(message.Parts) != 1 || message.Parts[0].Text != "later" {
		t.Fatalf("later prompt = %#v, %v", message, err)
	}

	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestGatewayPromptCancellationAfterWireWriteFencesDelayedResponse(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.setPromptEvents(
		Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"late"}`)},
		Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"late"}`)},
	)
	resultEntered, releaseResult := fake.gatePromptResult()
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored", "live-stored")

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "cancel"}}})
		result <- err
	}()
	<-resultEntered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("post-write cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-write cancellation did not join actor acknowledgement")
	}
	releaseResult()

	deadline := time.After(time.Second)
	for {
		select {
		case delivery := <-server.deliveries:
			if delivery.Event != nil && delivery.Event.Origin == CycleOriginActivity {
				t.Fatalf("delayed prompt output became autonomous work: %#v", delivery.Event)
			}
		case <-deadline:
			return
		case <-server.transport.dispatcher.done:
			select {
			case delivery := <-server.deliveries:
				if delivery.Event != nil && delivery.Event.Origin == CycleOriginActivity {
					t.Fatalf("delayed prompt output became autonomous work: %#v", delivery.Event)
				}
			default:
			}

			return
		}
	}
}

func newPromptOwnershipBoundary(t *testing.T) (*fakeGatewayServer, *hermesServer, *gatewaySessionActor) {
	t.Helper()
	fake := newFakeGatewayServer(t)
	fake.setPromptEvents()
	server := newGatewayBackedHermesServer(t, fake, "")
	bindTestGatewaySession(t, server, "stored", "live-stored")
	server.gatewayMu.Lock()
	actor := server.actorsByStored["stored"]
	server.gatewayMu.Unlock()

	return fake, server, actor
}

func replacePromptOwnershipActor(t *testing.T, server *hermesServer) *gatewaySessionActor {
	t.Helper()
	server.gatewayMu.Lock()
	old := server.actorsByStored["stored"]
	server.gatewayMu.Unlock()
	old.mailbox <- gatewayActorMessage{stop: true}
	<-old.done

	actor := &gatewaySessionActor{
		server: server, stored: "stored", live: "live-stored", generation: server.transport.generation,
		mailbox: make(chan gatewayActorMessage, gatewayActorMailboxCapacity+1), terminal: make(chan error, 1),
		done: make(chan struct{}), projections: make(map[*gatewayProjection]struct{}),
	}
	server.gatewayMu.Lock()
	server.actorsByStored[actor.stored] = actor
	server.gatewayMu.Unlock()
	server.transport.mappings.mu.Lock()
	binding := server.transport.mappings.bindings[actor.stored]
	binding.actor = actor
	server.transport.mappings.bindings[actor.stored] = binding
	server.transport.mappings.mu.Unlock()

	return actor
}

func sendOwnershipPrompt(ctx context.Context, server *hermesServer) error {
	ctx = WithPromptDispatch(ctx, func(context.Context, PromptDispatchInfo) error { return nil })
	_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}})

	return err
}

func TestGatewayPromptSubmissionFailsAtEveryLostOwnershipBoundary(t *testing.T) { //nolint:gocyclo,maintidx // One state-machine test names each phase where transport ownership can be lost.
	t.Run("active autonomous cycle rejects before submit", func(t *testing.T) {
		fake, server, actor := newPromptOwnershipBoundary(t)
		actor.active = actor.newCycle(CycleOriginActivity)
		dispatches := 0
		ctx := WithPromptDispatch(t.Context(), func(context.Context, PromptDispatchInfo) error {
			dispatches++

			return nil
		})
		_, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "prompt"}}})
		if !errors.Is(err, ErrGatewayAgentBusy) {
			t.Fatalf("active autonomous registration = %v", err)
		}
		if calls := fake.callsFor("prompt.submit"); len(calls) != 0 || dispatches != 0 {
			t.Fatalf("refused prompt reached submit/hook: calls=%d dispatches=%d", len(calls), dispatches)
		}
		if cause := server.transport.dispatcher.terminalCause(); cause != nil {
			t.Fatalf("busy refusal failed the generation: %v", cause)
		}

		actor.active = nil
		fake.setPromptEvents(Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"later"}`)})
		if _, err := server.SendMessage(ctx, "stored", MessageRequest{Parts: []map[string]any{{"text": "later"}}}); err != nil {
			t.Fatalf("prompt after autonomous settlement: %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("actor rejects registration", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase == promptPhaseRegister {
				actor.prompt = actor.newCycle(CycleOriginActivity)
			}
		}
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("registration ownership rejection = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("actor ends before registration admission", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase != promptPhaseRegister {
				return
			}
			actor.mailbox <- gatewayActorMessage{stop: true}
			<-actor.done
			actor.mailbox = make(chan gatewayActorMessage)
		}
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("registration actor loss = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("registration mailbox is saturated", func(t *testing.T) {
		_, server, _ := newPromptOwnershipBoundary(t)
		actor := replacePromptOwnershipActor(t, server)
		bindingHandled := make(chan struct{})
		go func() {
			message := <-actor.mailbox
			actor.live = message.bind.live
			actor.generation = message.bind.generation
			close(message.bind.done)
			close(bindingHandled)
		}()
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase != promptPhaseRegister {
				return
			}
			<-bindingHandled
			for len(actor.mailbox) < gatewayActorMailboxCapacity {
				actor.mailbox <- gatewayActorMessage{}
			}
		}
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayActorOverflow) {
			t.Fatalf("registration overflow = %v", err)
		}
		close(actor.done)
		requireGatewayClose(t, server)
	})

	t.Run("actor ends after accepting registration", func(t *testing.T) {
		_, server, _ := newPromptOwnershipBoundary(t)
		actor := replacePromptOwnershipActor(t, server)
		go func() {
			binding := <-actor.mailbox
			actor.live = binding.bind.live
			actor.generation = binding.bind.generation
			close(binding.bind.done)
			<-actor.mailbox
			close(actor.done)
		}()
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("registration reply loss = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("cancellation command cannot be queued", func(t *testing.T) {
		fake, server, _ := newPromptOwnershipBoundary(t)
		fake.setPromptResult(map[string]any{"voice_stopped": true})
		actor := replacePromptOwnershipActor(t, server)
		go func() {
			binding := <-actor.mailbox
			actor.live = binding.bind.live
			actor.generation = binding.bind.generation
			close(binding.bind.done)
			message := <-actor.mailbox
			actor.registerPrompt(message.register)
			for len(actor.mailbox) < gatewayActorMailboxCapacity {
				actor.mailbox <- gatewayActorMessage{}
			}
		}()
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("unqueueable cancellation = %v", err)
		}
		close(actor.done)
		requireGatewayClose(t, server)
	})

	t.Run("caller cancellation during dispatcher synchronization", func(t *testing.T) {
		_, server, _ := newPromptOwnershipBoundary(t)
		ctx, cancel := context.WithCancel(t.Context())
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase != promptPhaseSynchronize {
				return
			}
			cancel()
			server.connMu.Lock()
			delete(server.dispatchers, server.transport.generation)
			server.connMu.Unlock()
		}
		if err := sendOwnershipPrompt(ctx, server); !errors.Is(err, context.Canceled) {
			t.Fatalf("synchronization cancellation = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("watermark mailbox is saturated", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase != promptPhaseWatermark {
				return
			}
			actor.mailbox <- gatewayActorMessage{stop: true}
			<-actor.done
			for len(actor.mailbox) < gatewayActorMailboxCapacity {
				actor.mailbox <- gatewayActorMessage{}
			}
		}
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayActorOverflow) {
			t.Fatalf("watermark overflow = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("the running turn folds the prompt into itself", func(t *testing.T) {
		fake, server, actor := newPromptOwnershipBoundary(t)
		fake.setPromptStatus("redirected")
		actor.beforeWatermark = func() { actor.active = actor.newCycle(CycleOriginActivity) }
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayTurnAbsorbedPrompt) {
			t.Fatalf("folded prompt = %v", err)
		}
		if cause := server.transport.dispatcher.terminalCause(); cause != nil {
			t.Fatalf("folded prompt failed the generation: %v", cause)
		}
		requireGatewayClose(t, server)
	})

	t.Run("caller cancels while the queued prompt waits for its turn", func(t *testing.T) {
		fake, server, _ := newPromptOwnershipBoundary(t)
		fake.setPromptStatus("queued")
		ctx, cancel := context.WithCancel(t.Context())
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase == promptPhaseDefer {
				cancel()
			}
		}
		if err := sendOwnershipPrompt(ctx, server); !errors.Is(err, context.Canceled) {
			t.Fatalf("queued wait cancellation = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("actor ends before watermark acknowledgement", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase != promptPhaseWatermark {
				return
			}
			actor.mailbox <- gatewayActorMessage{stop: true}
			<-actor.done
		}
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("watermark actor loss = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("caller cancels while actor computes watermark", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		actor.beforeWatermark = func() { close(entered); <-release }
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(ctx, server) }()
		<-entered
		cancel()
		close(release)
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("watermark cancellation = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("server closes while actor computes watermark", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		actor.beforeWatermark = func() { close(entered); <-release }
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(t.Context(), server) }()
		<-entered
		closeResult := make(chan error, 1)
		go func() { closeResult <- server.Close(t.Context()) }()
		close(release)
		if err := <-result; !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("watermark server close = %v", err)
		}
		if err := <-closeResult; err != nil {
			t.Fatalf("close after watermark wait: %v", err)
		}
	})

	t.Run("closed signal wins while actor computes watermark", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		actor.beforeWatermark = func() { close(entered); <-release }
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(t.Context(), server) }()
		<-entered
		server.admissionOnce.Do(func() { close(server.closed) })
		if err := <-result; !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("watermark closed signal = %v", err)
		}
		close(release)
		requireGatewayClose(t, server)
	})

	t.Run("actor rejects watermark ownership", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		actor.beforeWatermark = func() { actor.prompt = nil }
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("watermark ownership rejection = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("older projection failure blocks prompt release", func(t *testing.T) {
		fake, server, _ := newPromptOwnershipBoundary(t)
		fake.setPromptBeforeResult(
			Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"old"}`)},
			Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"old"}`)},
		)
		want := errors.New("projection failed")
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(t.Context(), server) }()
		mustTurnEvent(t, server.deliveries)
		mustTurnEvent(t, server.deliveries)
		completed := mustTurnEvent(t, server.deliveries)
		completed.ProjectionDone(want)
		if err := <-result; !errors.Is(err, want) {
			t.Fatalf("projection failure = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("caller cancellation while older projection is pending", func(t *testing.T) {
		fake, server, _ := newPromptOwnershipBoundary(t)
		fake.setPromptBeforeResult(
			Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"old"}`)},
			Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"old"}`)},
		)
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(ctx, server) }()
		mustTurnEvent(t, server.deliveries)
		mustTurnEvent(t, server.deliveries)
		completed := mustTurnEvent(t, server.deliveries)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("projection cancellation = %v", err)
		}
		completed.ProjectionDone(nil)
		requireGatewayClose(t, server)
	})

	t.Run("release mailbox is saturated", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase != promptPhaseRelease {
				return
			}
			actor.mailbox <- gatewayActorMessage{stop: true}
			<-actor.done
			for len(actor.mailbox) < gatewayActorMailboxCapacity {
				actor.mailbox <- gatewayActorMessage{}
			}
		}
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayActorOverflow) {
			t.Fatalf("release overflow = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("actor ends before release acknowledgement", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase != promptPhaseRelease {
				return
			}
			actor.mailbox <- gatewayActorMessage{stop: true}
			<-actor.done
		}
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("release actor loss = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("caller cancels while actor releases prompt", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		actor.beforeRelease = func() { close(entered); <-release }
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(ctx, server) }()
		<-entered
		cancel()
		close(release)
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("release cancellation = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("actor rejects release ownership", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		actor.beforeRelease = func() { actor.prompt = nil }
		if err := sendOwnershipPrompt(t.Context(), server); !errors.Is(err, ErrGatewayAmbiguousTurn) {
			t.Fatalf("release ownership rejection = %v", err)
		}
		requireGatewayClose(t, server)
	})

	t.Run("server closes while actor releases prompt", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		actor.beforeRelease = func() { close(entered); <-release }
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(t.Context(), server) }()
		<-entered
		closeResult := make(chan error, 1)
		go func() { closeResult <- server.Close(t.Context()) }()
		close(release)
		if err := <-result; !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("release server close = %v", err)
		}
		if err := <-closeResult; err != nil {
			t.Fatalf("close after release wait: %v", err)
		}
	})

	t.Run("closed signal wins while actor releases prompt", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		actor.beforeRelease = func() { close(entered); <-release }
		result := make(chan error, 1)
		go func() { result <- sendOwnershipPrompt(t.Context(), server) }()
		<-entered
		server.admissionOnce.Do(func() { close(server.closed) })
		if err := <-result; !errors.Is(err, ErrGatewayDisconnected) {
			t.Fatalf("release closed signal = %v", err)
		}
		close(release)
		requireGatewayClose(t, server)
	})

	t.Run("result phase hook", func(t *testing.T) {
		fake, server, _ := newPromptOwnershipBoundary(t)
		fake.setPromptEvents(
			Event{Type: evtMessageDelta, Payload: json.RawMessage(`{"text":"done"}`)},
			Event{Type: evtMessageComplete, Payload: json.RawMessage(`{"text":"done"}`)},
		)
		reached := false
		server.beforePromptPhase = func(phase string, _ *gatewaySessionActor) {
			if phase == promptPhaseResult {
				reached = true
			}
		}
		if err := sendOwnershipPrompt(t.Context(), server); err != nil {
			t.Fatalf("result-hook prompt = %v", err)
		}
		if !reached {
			t.Fatal("result phase hook was not reached")
		}
		requireGatewayClose(t, server)
	})

	t.Run("released cycle closes without a result", func(t *testing.T) {
		_, server, actor := newPromptOwnershipBoundary(t)
		actor.beforeRelease = func() {
			close(actor.prompt.result)
			actor.prompt.result = nil
		}
		if err := sendOwnershipPrompt(t.Context(), server); err == nil ||
			err.Error() != "hermes gateway prompt cycle closed without a result" {
			t.Fatalf("closed result = %v", err)
		}
		requireGatewayClose(t, server)
	})
}

func requireGatewayClose(t *testing.T, server *hermesServer) {
	t.Helper()
	if err := server.Close(t.Context()); err != nil {
		t.Fatalf("close gateway server: %v", err)
	}
}
