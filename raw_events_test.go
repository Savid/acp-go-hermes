package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// rawEventPayload decodes the rawEvent notification params into its map form.
func rawEventPayload(t *testing.T, ext extensionNotification) map[string]any {
	t.Helper()

	if ext.method != RawEventMethod {
		t.Fatalf("extension method = %q, want %q", ext.method, RawEventMethod)
	}

	payload, ok := ext.params.(map[string]any)
	if !ok {
		t.Fatalf("rawEvent params = %#v, want map", ext.params)
	}

	return payload
}
func enabledRawSession(t *testing.T, agent *Agent, conn *recordingAgentClient, id acp.SessionId) *session {
	t.Helper()

	client := newFakeHermesClient()
	session := testSession(agent, client)
	session.id = id
	session.rawMessages = rawMessageConfig{enabled: true}
	agent.setAgentClient(conn)

	return session
}
func emitRaw(t *testing.T, session *session, raw string) {
	t.Helper()

	if err := session.handleEvent(context.Background(), nativehermes.TurnEvent{Type: "native.custom", Raw: json.RawMessage(raw)}); err != nil {
		t.Fatalf("handleEvent: %v", err)
	}
}

// Raw-event uniform test spec, cases 1-6.
func TestRawEventOversizeEmitsFixedMarker(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	session := enabledRawSession(t, agent, conn, "session-1")

	ctx := withTurnRoute(context.Background(), "turn-raw")
	if err := session.emitUpdate(ctx, acp.UpdateAgentMessageText("late")); err != nil {
		t.Fatalf("emit update: %v", err)
	}
	conn.mu.Lock()
	updateMeta := conn.updates[0].Meta
	conn.mu.Unlock()
	if !reflect.DeepEqual(updateMeta, turnRouteMeta("turn-raw")) {
		t.Fatalf("session/update route envelope = %#v", updateMeta)
	}

	big := `{"blob":"` + strings.Repeat("x", 70000) + `"}`
	if err := session.emitRawHermesEvent(ctx, nativehermes.TurnEvent{Type: "native.custom", Raw: json.RawMessage(big)}); err != nil {
		t.Fatalf("emit raw: %v", err)
	}

	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 1 {
		t.Fatalf("rawEvent notifications = %d, want exactly 1", len(exts))
	}

	payload := rawEventPayload(t, exts[0])
	if !reflect.DeepEqual(payload["_meta"], turnRouteMeta("turn-raw")) {
		t.Fatalf("raw route envelope = %#v", payload["_meta"])
	}
	if payload[jsonFieldSessionID] != session.id {
		t.Fatalf("marker sessionId = %v, want %v", payload[jsonFieldSessionID], session.id)
	}

	if payload[keySequence] != int64(1) {
		t.Fatalf("marker sequence = %v, want 1", payload[keySequence])
	}

	if payload[keySource] != valHermesServeSource {
		t.Fatalf("marker source = %v, want hermes-serve", payload[keySource])
	}

	event, ok := payload[keyEvent].(map[string]any)
	if !ok {
		t.Fatalf("marker event = %#v, want map", payload[keyEvent])
	}

	if event[rawEventKeyTruncated] != true || event[rawEventKeyReason] != rawEventReasonOversize {
		t.Fatalf("marker = %#v, want oversize", event)
	}

	if event[rawEventKeyMaxBytes] != rawEventMaxBytes {
		t.Fatalf("marker maxBytes = %v, want %d", event[rawEventKeyMaxBytes], rawEventMaxBytes)
	}

	size, ok := event[rawEventKeySizeBytes].(int)
	if !ok || size <= rawEventMaxBytes {
		t.Fatalf("marker sizeBytes = %v, want int > %d", event[rawEventKeySizeBytes], rawEventMaxBytes)
	}
}
func TestRawEventSequenceContiguousPerSession(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	session := enabledRawSession(t, agent, conn, "session-1")

	emitRaw(t, session, `{"n":1}`)
	emitRaw(t, session, `{"blob":"`+strings.Repeat("y", 70000)+`"}`) // oversized still consumes a sequence
	emitRaw(t, session, `{"n":3}`)

	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 3 {
		t.Fatalf("rawEvent notifications = %d, want 3", len(exts))
	}

	for i, ext := range exts {
		payload := rawEventPayload(t, ext)
		if payload[keySequence] != int64(i+1) {
			t.Fatalf("sequence[%d] = %v, want %d", i, payload[keySequence], i+1)
		}
	}
}
func TestRawEventCrossSessionIsolation(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	sessionA := enabledRawSession(t, agent, conn, "session-A")
	sessionB := enabledRawSession(t, agent, conn, "session-B")

	emitRaw(t, sessionA, `{"a":1}`)
	emitRaw(t, sessionB, `{"b":1}`)
	emitRaw(t, sessionA, `{"a":2}`)
	emitRaw(t, sessionB, `{"b":2}`)

	perSession := map[acp.SessionId][]int64{}
	for _, ext := range conn.extensionsFor(RawEventMethod) {
		payload := rawEventPayload(t, ext)
		id, _ := payload[jsonFieldSessionID].(acp.SessionId)
		seq, _ := payload[keySequence].(int64)
		perSession[id] = append(perSession[id], seq)
	}

	for id, seqs := range perSession {
		want := []int64{1, 2}
		if len(seqs) != len(want) {
			t.Fatalf("session %q sequences = %v, want %v", id, seqs, want)
		}

		for i := range want {
			if seqs[i] != want[i] {
				t.Fatalf("session %q sequences = %v, want %v", id, seqs, want)
			}
		}
	}
}
func TestRawEventMarkerAlwaysValidJSON(t *testing.T) {
	oversize, err := capRawEventPayload(map[string]any{
		jsonFieldSessionID: "s",
		keySequence:        int64(1),
		keySource:          valHermesServeSource,
		keyEvent:           map[string]any{"blob": strings.Repeat("z", 70000)},
	})
	if err != nil {
		t.Fatalf("cap oversize: %v", err)
	}
	oversizeEvent, _ := oversize[keyEvent].(map[string]any)
	if oversizeEvent[rawEventKeyReason] != rawEventReasonOversize {
		t.Fatalf("oversize marker = %#v", oversizeEvent)
	}

	requireValidJSON(t, oversize)

	// An unserializable event (a channel cannot be marshalled) yields the
	// unserializable marker with no sizeBytes and still valid JSON.
	unserializable, err := capRawEventPayload(map[string]any{
		jsonFieldSessionID: "s",
		keySequence:        int64(2),
		keySource:          valHermesServeSource,
		keyEvent:           make(chan int),
	})
	if err != nil {
		t.Fatalf("cap unserializable: %v", err)
	}
	marker, _ := unserializable[keyEvent].(map[string]any)
	if marker[rawEventKeyReason] != rawEventReasonUnserialized {
		t.Fatalf("unserializable marker = %#v", marker)
	}

	if _, present := marker[rawEventKeySizeBytes]; present {
		t.Fatalf("unserializable marker carries sizeBytes: %#v", marker)
	}

	requireValidJSON(t, map[string]any{
		jsonFieldSessionID: unserializable[jsonFieldSessionID],
		keySequence:        unserializable[keySequence],
		keySource:          unserializable[keySource],
		keyEvent:           marker,
	})
}

func TestRawEventFinalPayloadBoundaryIncludesMaximumRoute(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		jsonFieldSessionID: "session-1",
		keySequence:        int64(1),
		keySource:          valHermesServeSource,
		keyEvent:           map[string]any{keyData: ""},
		"_meta":            turnRouteMeta(strings.Repeat("n", routeTurnNonceMaxBytes)),
	}
	empty, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal empty boundary: %v", err)
	}
	padding := rawEventMaxBytes - len(empty)
	if padding <= 0 {
		t.Fatalf("boundary overhead = %d, want less than %d", len(empty), rawEventMaxBytes)
	}
	payload[keyEvent] = map[string]any{keyData: strings.Repeat("x", padding)}

	capped, err := capRawEventPayload(payload)
	if err != nil {
		t.Fatalf("cap exact boundary: %v", err)
	}
	encoded, err := json.Marshal(capped)
	if err != nil {
		t.Fatalf("marshal exact boundary: %v", err)
	}
	if len(encoded) != rawEventMaxBytes {
		t.Fatalf("exact boundary size = %d, want %d", len(encoded), rawEventMaxBytes)
	}

	payload[keyEvent] = map[string]any{keyData: strings.Repeat("x", padding+1)}
	capped, err = capRawEventPayload(payload)
	if err != nil {
		t.Fatalf("cap over boundary: %v", err)
	}
	encoded, err = json.Marshal(capped)
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}
	if len(encoded) > rawEventMaxBytes {
		t.Fatalf("final marker size = %d, exceeds %d", len(encoded), rawEventMaxBytes)
	}
	marker, _ := capped[keyEvent].(map[string]any)
	if marker[rawEventKeyReason] != rawEventReasonOversize || marker[rawEventKeySizeBytes] != rawEventMaxBytes+1 {
		t.Fatalf("boundary marker = %#v", marker)
	}
}

func TestRawEventFinalPayloadRejectsUnboundedInternalRoute(t *testing.T) {
	t.Parallel()

	_, err := capRawEventPayload(map[string]any{
		jsonFieldSessionID: "session-1",
		keySequence:        int64(1),
		keySource:          valHermesServeSource,
		keyEvent:           map[string]any{keyData: strings.Repeat("x", rawEventMaxBytes)},
		"_meta": map[string]any{routeMetaKey: map[string]any{
			routeFieldVer: routeVersion, routeFieldTurn: strings.Repeat("n", rawEventMaxBytes),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unbounded internal route error = %v", err)
	}

	_, err = capRawEventPayload(map[string]any{
		jsonFieldSessionID: "session-1",
		keySequence:        int64(2),
		keySource:          valHermesServeSource,
		keyEvent:           make(chan int),
		"_meta":            make(chan int),
	})
	if err == nil || !strings.Contains(err.Error(), "marshal capped") {
		t.Fatalf("unserializable structural envelope error = %v", err)
	}
}

func TestRawEventEmitterRejectsUnboundedStructuralEnvelope(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	session := enabledRawSession(t, agent, conn, acp.SessionId(strings.Repeat("s", rawEventMaxBytes)))
	ctx := withTurnRoute(context.Background(), strings.Repeat("n", routeTurnNonceMaxBytes))

	err := session.emitRawHermesEvent(ctx, nativehermes.TurnEvent{
		Type: "native.custom",
		Raw:  json.RawMessage(`{"type":"native.custom"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unbounded structural envelope error = %v", err)
	}
	if conn.extensionCount() != 0 {
		t.Fatalf("unbounded emitter produced %d notifications", conn.extensionCount())
	}
	if session.rawSeq != 0 {
		t.Fatalf("unbounded emitter consumed sequence %d, want 0", session.rawSeq)
	}

	session.id = "session-1"
	if err := session.emitRawHermesEvent(context.Background(), nativehermes.TurnEvent{
		Type: "native.custom",
		Raw:  json.RawMessage(`{"type":"recovered"}`),
	}); err != nil {
		t.Fatalf("emit after structural failure: %v", err)
	}

	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 1 {
		t.Fatalf("rawEvent notifications after structural failure = %d, want 1", len(exts))
	}
	if payload := rawEventPayload(t, exts[0]); payload[keySequence] != int64(1) {
		t.Fatalf("sequence after structural failure = %v, want 1", payload[keySequence])
	}
}

func TestRawEventSequenceCommitsOnlyAfterSuccessfulDelivery(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	session := enabledRawSession(t, agent, conn, "session-1")

	conn.notifyErr = errors.New("delivery failed")
	err := session.emitRawHermesEvent(context.Background(), nativehermes.TurnEvent{
		Type: "native.custom",
		Raw:  json.RawMessage(`{"type":"failed"}`),
	})
	if !errors.Is(err, conn.notifyErr) {
		t.Fatalf("failed delivery error = %v, want %v", err, conn.notifyErr)
	}
	if session.rawSeq != 0 {
		t.Fatalf("failed delivery consumed sequence %d, want 0", session.rawSeq)
	}

	conn.notifyErr = nil
	emitRaw(t, session, `{"type":"recovered"}`)
	emitRaw(t, session, `{"type":"next"}`)

	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 3 {
		t.Fatalf("rawEvent delivery attempts = %d, want 3", len(exts))
	}
	want := []int64{1, 1, 2}
	for index, ext := range exts {
		if sequence := rawEventPayload(t, ext)[keySequence]; sequence != want[index] {
			t.Fatalf("sequence[%d] = %v, want %d", index, sequence, want[index])
		}
	}
	if session.rawSeq != 2 {
		t.Fatalf("committed sequence = %d, want 2", session.rawSeq)
	}
}

func requireValidJSON(t *testing.T, payload map[string]any) {
	t.Helper()

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marker payload failed to marshal: %v", err)
	}

	if !json.Valid(encoded) {
		t.Fatalf("marker payload is not valid JSON: %s", encoded)
	}
}
func TestRawEventEmitFailureDoesNotFailTurn(t *testing.T) {
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	conn.notifyErr = errors.New("client notify boom")
	agent := NewAgent()
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.rawMessages = rawMessageConfig{enabled: true}

	release := make(chan struct{})
	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		<-release

		return nativehermes.NativeMessage{
			Info:  nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativehermes.Part{{ID: "final", SessionID: id, MessageID: "assistant", Type: "text", Text: "ok"}},
		}, nil
	}

	// Deliver a raw event mid-turn; its emit fails on the wire but must not
	// abort the authoritative turn.
	client.events <- nativehermes.TurnEvent{Type: "native.custom", Raw: json.RawMessage(`{"type":"native.custom"}`)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct {
		resp acp.PromptResponse
		err  error
	}, 1)
	go func() {
		resp, err := promptOnce(ctx, session, "hello")
		done <- struct {
			resp acp.PromptResponse
			err  error
		}{resp, err}
	}()

	// Wait until the raw event emit was attempted (recorded even though it
	// errored), then let the native turn complete.
	deadline := time.After(time.Second)
	for conn.extensionCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("raw event was never emitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(release)

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("raw emit failure aborted the turn: %v", out.err)
		}

		if out.resp.StopReason != acp.StopReasonEndTurn {
			t.Fatalf("stop reason = %q, want end_turn", out.resp.StopReason)
		}
	case <-ctx.Done():
		t.Fatal("prompt did not return")
	}
}
func TestRawEventDefaultOffEmitsNothing(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client) // rawMessages disabled by default

	for i := 0; i < 5; i++ {
		emitRaw(t, session, `{"n":1}`)
	}

	if got := len(conn.extensionsFor(RawEventMethod)); got != 0 {
		t.Fatalf("rawEvent notifications with feature off = %d, want 0", got)
	}
}

func TestRawEventNilPayloadSkippedWithoutSequence(t *testing.T) {
	conn := newRecordingAgentClient()
	agent := NewAgent()
	session := enabledRawSession(t, agent, conn, "session-1")
	ctx := context.Background()

	// A native event without a payload is skipped entirely: no "event": null
	// notification and no consumed sequence.
	if err := session.emitRawHermesEvent(ctx, nativehermes.TurnEvent{Type: "native.custom"}); err != nil {
		t.Fatalf("emit nil payload: %v", err)
	}
	if err := session.emitRawHermesEvent(ctx, nativehermes.TurnEvent{Type: "native.custom", Raw: json.RawMessage(`null`)}); err != nil {
		t.Fatalf("emit null payload: %v", err)
	}
	if exts := conn.extensionsFor(RawEventMethod); len(exts) != 0 {
		t.Fatalf("nil payload emitted notifications: %#v", exts)
	}

	emitRaw(t, session, `{"n":1}`)
	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 1 {
		t.Fatalf("rawEvent notifications = %d, want 1", len(exts))
	}
	if payload := rawEventPayload(t, exts[0]); payload[keySequence] != int64(1) {
		t.Fatalf("sequence after skipped nil payload = %v, want 1", payload[keySequence])
	}
}
