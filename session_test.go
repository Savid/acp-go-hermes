package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestTurnFenceHelperBranches(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	if session.claimPermissionRequest("") || session.claimQuestionRequest("") {
		t.Fatal("empty request ids were admitted without exact ownership")
	}
	session.processedPermission = nil
	session.processedQuestion = nil
	if !session.claimPermissionRequest("perm") || !session.claimQuestionRequest("question") {
		t.Fatal("nil processed request maps were not initialized")
	}
	session.markActiveMessageID("")
	session.activeMessageIDs = nil
	session.markActiveMessageID("message-1")
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

func TestCommittedStateAndForegroundPrefixBoundaries(t *testing.T) {
	native := &stateSnapshotTerminal{MessageID: "message"}
	require.Empty(t, (committedState{}).nativeTerminal().MessageID)
	require.Equal(t, "message", (committedState{native: native}).nativeTerminal().MessageID)

	session := testSession(newTestAgent(), newFakeHermesClient())
	session.recordForegroundPrefix("")
	session.recordForegroundPrefix("prefix")
	session.recordForegroundPrefix(string(bytes.Repeat([]byte("x"), lifecycleForegroundPrefixBytes)))
	session.recordForegroundPrefix("ignored")
	require.Len(t, session.foregroundPrefix(), lifecycleForegroundPrefixBytes)

	boundary := testSession(newTestAgent(), newFakeHermesClient())
	prefix := string(bytes.Repeat([]byte("x"), lifecycleForegroundPrefixBytes-1))
	boundary.recordForegroundPrefix(prefix + "é")
	boundary.recordForegroundPrefix("must-not-follow-truncation")
	got := boundary.foregroundPrefix()
	require.Equal(t, prefix, got)
	require.True(t, utf8.ValidString(got))
	encoded, err := json.Marshal(stateSnapshotForeground{Text: got})
	require.NoError(t, err)
	var roundTrip stateSnapshotForeground
	require.NoError(t, json.Unmarshal(encoded, &roundTrip))
	require.Equal(t, got, roundTrip.Text)

	require.Nil(t, foregroundOf(nil))
	foreground := &stateSnapshotForeground{TurnID: "turn"}
	require.Same(t, foreground, foregroundOf(&stateSnapshotWrapper{Foreground: foreground}))
}

func TestExactForegroundCapacityAndAdmissionExclusion(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	session.recordForegroundPrefix(string(bytes.Repeat([]byte("x"), lifecycleForegroundPrefixBytes)))
	session.recordForegroundPrefix("not retained")
	require.Equal(t, lifecycleForegroundPrefixBytes, len(session.foregroundPrefix()))
	session.mu.Lock()
	truncated := session.foregroundTruncated
	session.mu.Unlock()
	require.True(t, truncated)

	other := testSession(newTestAgent(), newFakeHermesClient())
	_, releaseReuse, err := other.beginReuse(t.Context())
	require.NoError(t, err)
	if _, _, acquireErr := other.acquireTurn(t.Context()); acquireErr == nil {
		t.Fatal("turn crossed active reuse")
	}
	other.detachPump()
	releaseReuse()
	other.prepareClose()
	if _, _, reuseErr := other.beginReuse(t.Context()); reuseErr == nil {
		t.Fatal("reuse crossed close admission")
	}
}
