package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

type nativeFixtureStore struct {
	acpcore.SessionStore
	trace *[]string
	fail  bool
}

func (s *nativeFixtureStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail {
		return errors.New("fixture mirror failure")
	}
	if err := s.SessionStore.Replace(ctx, key, replacements); err != nil {
		return err
	}
	*s.trace = append(*s.trace, "commit")

	return nil
}

type nativeFixtureRecorder struct {
	*recorder
	trace *[]string
}

func (r *nativeFixtureRecorder) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if envelope, ok := notification.Meta[wire.LifecycleKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok && event["type"] == "state_update" {
			state, _ := event["state"].(string)
			*r.trace = append(*r.trace, state)
		}
	}
	if notification.Update.AgentMessageChunk != nil || notification.Update.UserMessageChunk != nil {
		*r.trace = append(*r.trace, "message")
	}

	return r.recorder.SessionUpdate(ctx, notification)
}

func TestCapturedNativeAgentOrigin(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		name := "committed"
		if failCommit {
			name = "failed commit"
		}
		t.Run(name, func(t *testing.T) {
			var trace []string
			store := &nativeFixtureStore{SessionStore: acpcore.NewInMemorySessionStore(), trace: &trace}
			rec := &nativeFixtureRecorder{recorder: newRecorder(), trace: &trace}
			a := NewAgent(testOptions(t, WithSessionStore(store))...)
			t.Cleanup(func() { _ = a.Close() })
			a.attach(rec, nil)
			_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
			require.NoError(t, err)
			created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err)
			s, err := a.session(t.Context(), created.SessionId)
			require.NoError(t, err)
			s.mu.Lock()
			rt := s.runtime
			require.Nil(t, s.turn)
			s.mu.Unlock()
			data, err := os.ReadFile("testdata/native/agent-origin.json")
			require.NoError(t, err)
			data = []byte(strings.ReplaceAll(string(data), "fixture-session", rt.liveID))
			var frames []json.RawMessage
			require.NoError(t, json.Unmarshal(data, &frames))
			trace = nil
			store.fail = failCommit
			for _, frame := range frames {
				var envelope struct {
					Params hermes.Event `json:"params"`
				}
				require.NoError(t, json.Unmarshal(frame, &envelope))
				s.handleEvent(t.Context(), rt, envelope.Params)
			}
			require.NotEmpty(t, trace)
			require.Equal(t, "running", trace[0])
			for _, event := range lifecycleEvents(rec.snapshot()) {
				if event["type"] == "state_update" {
					require.Equal(t, "activity", event["cause"])
				}
				require.NotEqual(t, "prompt_accepted", event["type"])
			}
			require.Contains(t, trace, "message")
			s.mu.Lock()
			require.Nil(t, s.cycle)
			s.mu.Unlock()
			if failCommit {
				require.NotContains(t, trace, "idle")
				s.lcMu.Lock()
				active := s.lc.Active()
				s.lcMu.Unlock()
				require.False(t, active)
			} else {
				require.Equal(t, []string{"commit", "idle"}, trace[len(trace)-2:])
			}
		})
	}
}

// TestOutOfPromptNativeDialogsAreAnswered drives the four record kinds that
// reach projectEvent outside a prompt. Each must open an agent-origin cycle,
// or handleEvent drops it and the native request is never answered.
func TestOutOfPromptNativeDialogsAreAnswered(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })

	rec := newRecorder()
	a.attach(rec, nil)

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	s.mu.Lock()
	rt := s.runtime
	require.Nil(t, s.turn)
	require.Nil(t, s.cycle)
	s.mu.Unlock()

	for _, kind := range []string{eventSudoRequest, eventSecretRequest, eventTerminalReadRequest} {
		s.handleEvent(t.Context(), rt, hermes.Event{Type: kind, RequestID: kind + "|" + rt.liveID, Payload: []byte(`{}`)})
	}

	s.handleEvent(t.Context(), rt, hermes.Event{Type: eventMessageComplete, Payload: []byte(`{"text":"","status":"complete"}`)})

	s.mu.Lock()
	require.Nil(t, s.cycle, "the agent-origin cycle the dialogs opened is terminal")
	s.mu.Unlock()

	request := wire.TextPromptRequest(created.SessionId, "DIALOGS")
	request.Meta = promptMeta(1)

	_, err = a.Prompt(t.Context(), request)
	require.NoError(t, err)
	require.Contains(t, agentText(rec.snapshot()), "sudo,secret,terminal.read")
}

// TestOutOfPromptNativeErrorOpensACycle covers the fourth kind: a native error
// with no turn in flight still opens and terminalizes an agent-origin cycle.
func TestOutOfPromptNativeErrorOpensACycle(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })

	rec := newRecorder()
	a.attach(rec, nil)

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	s.handleEvent(t.Context(), rt, hermes.Event{Type: stopReasonError, Payload: []byte(`{"message":"provider unavailable"}`)})

	s.mu.Lock()
	require.Nil(t, s.cycle)
	s.mu.Unlock()

	var opened, ended bool

	for _, event := range lifecycleEvents(rec.snapshot()) {
		if event["type"] != "state_update" || event["cause"] != "activity" {
			continue
		}

		if event["state"] == "running" {
			opened = true
		}

		if event["state"] == "idle" {
			ended = true
			require.Equal(t, "failed", event["outcome"])
		}
	}

	require.True(t, opened, "a native error outside a prompt opens an agent-origin cycle")
	require.True(t, ended)
}

func TestNativeRequestCancellationTargetsMatchingDialog(t *testing.T) {
	t.Parallel()
	rt := &runtime{liveID: "live"}
	s := &session{runtime: rt}
	first, cancelFirst := context.WithCancelCause(t.Context())
	defer cancelFirst(nil)
	second, cancelSecond := context.WithCancelCause(t.Context())
	defer cancelSecond(nil)
	defer s.registerDialog("live:srq-first", cancelFirst)()
	defer s.registerDialog("live:srq-second", cancelSecond)()
	s.handleEvent(t.Context(), rt, hermes.Event{
		Type: "request.cancel", SessionID: "live", Payload: json.RawMessage(`{"id":"srq-first","reason":"cancelled"}`),
	})
	require.ErrorIs(t, context.Cause(first), errDialogCancelled)
	require.NoError(t, second.Err())
	require.Nil(t, s.cycle, "request cancellation must not open native work")
}
