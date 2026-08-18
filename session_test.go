package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

func TestTurnFenceHelperBranches(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	if !session.claimPermissionRequest("") || !session.claimQuestionRequest("") {
		t.Fatal("empty request ids should not be fenced")
	}
	session.processedPermission = nil
	session.processedQuestion = nil
	if !session.claimPermissionRequest("perm") || !session.claimQuestionRequest("question") {
		t.Fatal("nil processed request maps were not initialized")
	}
	session.markActiveMessageID("")
	session.activeMessageIDs = nil
	session.markActiveMessageID("message-1")
	session.failedMessageIDs = nil
	session.failedStreamEpochs = nil
	session.markStreamFailed(9)
	if !session.shouldSuppressEvent(nativehermes.TurnEvent{StreamEpoch: 9}) {
		t.Fatal("failed stream epoch was not suppressed")
	}
	if !session.shouldSuppressEvent(nativehermes.TurnEvent{
		Properties: json.RawMessage(`{"sessionID":"native-1","messageID":"message-1","type":"text","text":"late"}`),
	}) {
		t.Fatal("failed message id was not suppressed")
	}
	if session.shouldSuppressEvent(nativehermes.TurnEvent{
		Properties: json.RawMessage(`{"sessionID":"native-1","messageID":"message-2","type":"text","text":"ok"}`),
	}) {
		t.Fatal("unfailed message id was suppressed")
	}
	if err := session.handleEvent(context.Background(), nativehermes.TurnEvent{
		StreamEpoch: 9,
		Properties:  json.RawMessage(`{"sessionID":"native-1","messageID":"message-1","type":"text","text":"late"}`),
	}); err != nil {
		t.Fatalf("suppressed handleEvent: %v", err)
	}
}

func TestTurnFenceLifecycleFailureBranches(t *testing.T) {
	wantResumeErr := errors.New("resume admission")
	resumeAgent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return nil, wantResumeErr
		},
	}))
	resumeSession := testSession(resumeAgent, newFakeHermesClient())
	resumeSession.runtimeNeedsResume = true
	if _, _, err := resumeSession.preparePromptTurn(t.Context(), "resume-error"); !errors.Is(err, wantResumeErr) {
		t.Fatalf("prepare resume error = %v", err)
	}

	nilClient := testSession(newTestAgent(), newFakeHermesClient())
	nilClient.client = nil
	nilClient.cancelTurn()
	if err := nilClient.fenceTurnLocked(t.Context(), 0, true); err != nil {
		t.Fatalf("zero epoch fence: %v", err)
	}
	nilClient.turnEpoch = 2
	if err := nilClient.fenceTurnLocked(t.Context(), 1, true); err == nil || !strings.Contains(err.Error(), "stale turn epoch") {
		t.Fatalf("stale epoch fence error = %v", err)
	}
	if err := nilClient.fenceTurnLocked(t.Context(), 2, true); err == nil || !strings.Contains(err.Error(), "Hermes runtime is unavailable") {
		t.Fatalf("nil runtime fence error = %v", err)
	}

	closed := testSession(newTestAgent(), newFakeHermesClient())
	closed.client = nil
	if err := closed.Close(t.Context()); err != nil {
		t.Fatalf("close nil runtime: %v", err)
	}
}

func TestSessionMarkPartAcceptsFirstEmptyRawPayload(t *testing.T) {
	t.Parallel()

	session := testSession(newTestAgent(), newFakeHermesClient())
	part := nativehermes.Part{ID: "completion-only", Type: "text", Text: "final answer"}
	if !session.markPart(part) {
		t.Fatal("first completion-only part was suppressed")
	}
	if session.markPart(part) {
		t.Fatal("duplicate completion-only part was accepted")
	}
}

func TestSessionClonesExtraPathDirsAcrossConstructionAndSnapshot(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	input := []string{first, second, first}
	env := map[string]string{"WAGIE_API_TOKEN": "one"}
	client := newFakeHermesClient()
	session := newSession(
		newTestAgent(),
		"session-carrier",
		t.TempDir(),
		nil,
		nil,
		testNativeSession("native-carrier"),
		client,
		sessionMeta{Env: env, ExtraPathDirs: input},
		idmapRecord{},
	)

	input[0] = t.TempDir()
	env["WAGIE_API_TOKEN"] = "mutated"
	snapshot := session.snapshot()
	if !reflect.DeepEqual(snapshot.extraPathDirs, []string{first, second, first}) || snapshot.env["WAGIE_API_TOKEN"] != "one" {
		t.Fatalf("constructed carrier = dirs %#v env %#v", snapshot.extraPathDirs, snapshot.env)
	}

	snapshot.extraPathDirs[0] = t.TempDir()
	snapshot.env["WAGIE_API_TOKEN"] = "snapshot-mutated"
	again := session.snapshot()
	if again.extraPathDirs[0] != first || again.env["WAGIE_API_TOKEN"] != "one" {
		t.Fatalf("snapshot mutation reached session = dirs %#v env %#v", again.extraPathDirs, again.env)
	}
}

func TestPoisonedSessionRejectsFollowUpOperations(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	store := newCountingSessionStore()
	agent := newTestAgent(WithSessionStore(store))
	agent.setAgentClient(conn)
	s := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[s.id] = s
	agent.mu.Unlock()

	if err := s.poison(ctx, "native drift without advertisement"); err == nil ||
		!strings.Contains(err.Error(), "hermes_native_session_id_drift") {
		t.Fatalf("poison error = %v", err)
	}
	if conn.updateCount() != 0 {
		t.Fatalf("poison emitted updates: %#v", conn.updates)
	}
	if err := s.poison(ctx, "second poison"); err == nil ||
		!strings.Contains(err.Error(), "session_poisoned") ||
		!strings.Contains(err.Error(), "native drift without advertisement") {
		t.Fatalf("second poison error = %v", err)
	}
	if _, _, err := s.acquireTurn(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("acquire poisoned session error = %v", err)
	}
	if err := agent.Cancel(ctx, acp.CancelNotification{SessionId: s.id}); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("cancel poisoned session error = %v", err)
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetModelRequest(s.id, "openai/gpt-test")); err == nil ||
		!strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("set config poisoned session error = %v", err)
	}
	if err := s.replayMessages(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("replay poisoned session error = %v", err)
	}
	if err := s.snapshotToStore(ctx); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
		t.Fatalf("snapshot poisoned session error = %v", err)
	}
	if store.replaceCount() != 0 {
		t.Fatalf("store writes after poisoned follow-up = %d, want 0", store.replaceCount())
	}
	if err := (&session{}).validateNativeMessageSession(ctx, nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{SessionID: "native-other"}}); err != nil {
		t.Fatalf("empty expected native id validation error = %v", err)
	}
}

// joinModelValue is the inverse of splitModelValue and the only place a
// provider and a model are re-qualified into one selector.
func TestJoinModelValueQualifiesProvider(t *testing.T) {
	if got := joinModelValue("provider", "model"); got != "provider/model" {
		t.Fatalf("joined model=%q", got)
	}
}
