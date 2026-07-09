package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
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

	big := `{"blob":"` + strings.Repeat("x", 70000) + `"}`
	emitRaw(t, session, big)

	exts := conn.extensionsFor(RawEventMethod)
	if len(exts) != 1 {
		t.Fatalf("rawEvent notifications = %d, want exactly 1", len(exts))
	}

	payload := rawEventPayload(t, exts[0])
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
	oversize := capRawEventPayload(map[string]any{
		jsonFieldSessionID: "s",
		keySequence:        int64(1),
		keySource:          valHermesServeSource,
		keyEvent:           map[string]any{"blob": strings.Repeat("z", 70000)},
	})
	oversizeEvent, _ := oversize[keyEvent].(map[string]any)
	if oversizeEvent[rawEventKeyReason] != rawEventReasonOversize {
		t.Fatalf("oversize marker = %#v", oversizeEvent)
	}

	requireValidJSON(t, oversize)

	// An unserializable event (a channel cannot be marshalled) yields the
	// unserializable marker with no sizeBytes and still valid JSON.
	unserializable := capRawEventPayload(map[string]any{
		jsonFieldSessionID: "s",
		keySequence:        int64(2),
		keySource:          valHermesServeSource,
		keyEvent:           make(chan int),
	})
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
