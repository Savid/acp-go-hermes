package hermesacp

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

type nativeFixtureStore struct {
	acpcore.SessionStore
	trace *[]string
}

func (s *nativeFixtureStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
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
	var trace []string
	store := &nativeFixtureStore{SessionStore: acpcore.NewInMemorySessionStore(), trace: &trace}
	rec := &nativeFixtureRecorder{recorder: newRecorder(), trace: &trace}
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(rec, nil)
	initResponse, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
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
	require.Empty(t, usageUpdates(rec.snapshot()), "the captured closing frame states no context")
	s.mu.Lock()
	require.Nil(t, s.cycle)
	s.mu.Unlock()
	require.NoError(t, lifecycle.CheckAttribution(negotiatedAnswer(t, initResponse), sessionFrames(t, rec.snapshot(), created.SessionId)))
	require.Equal(t, []string{"commit", "idle"}, trace[len(trace)-2:])
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

func TestInterimAndFinalAssistantText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		events []hermes.Event
		want   string
	}{
		{
			name: "unstreamed interim",
			events: []hermes.Event{
				{Type: eventMessageInterim, Payload: json.RawMessage(`{"text":"Checking files.","already_streamed":false}`)},
				{Type: eventMessageComplete, Payload: json.RawMessage(`{"text":"Done.","status":"complete"}`)},
			},
			want: "Checking files.Done.",
		},
		{
			name: "terminal footer after commentary",
			events: []hermes.Event{
				{Type: eventMessageDelta, Payload: json.RawMessage(`{"text":"Checking files."}`)},
				{Type: eventMessageInterim, Payload: json.RawMessage(`{"text":"Checking files.","already_streamed":true}`)},
				{Type: eventMessageDelta, Payload: json.RawMessage(`{"text":"Done."}`)},
				{Type: eventMessageComplete, Payload: json.RawMessage(`{"text":"Done.\n\nSome edits failed.","status":"complete"}`)},
			},
			want: "Checking files.Done.\n\nSome edits failed.",
		},
		{
			name: "tool boundary without interim events",
			events: []hermes.Event{
				{Type: eventMessageDelta, Payload: json.RawMessage(`{"text":"Checking files."}`)},
				{Type: eventToolStart, Payload: json.RawMessage(`{"tool_id":"read-1","name":"read"}`)},
				{Type: eventMessageDelta, Payload: json.RawMessage(`{"text":"Done."}`)},
				{Type: eventMessageComplete, Payload: json.RawMessage(`{"text":"Done.\n\nSome edits failed.","status":"complete"}`)},
			},
			want: "Checking files.Done.\n\nSome edits failed.",
		},
		{
			name: "interim completes a partial delta",
			events: []hermes.Event{
				{Type: eventMessageDelta, Payload: json.RawMessage(`{"text":"Checking"}`)},
				{Type: eventMessageInterim, Payload: json.RawMessage(`{"text":"Checking files.","already_streamed":true}`)},
				{Type: eventMessageComplete, Payload: json.RawMessage(`{"text":"Done.","status":"complete"}`)},
			},
			want: "Checking files.Done.",
		},
		{
			name: "distinct messages have identical text",
			events: []hermes.Event{
				{Type: eventMessageDelta, Payload: json.RawMessage(`{"text":"Done."}`)},
				{Type: eventMessageInterim, Payload: json.RawMessage(`{"text":"Done.","already_streamed":true}`)},
				{Type: eventMessageComplete, Payload: json.RawMessage(`{"text":"Done.","status":"complete"}`)},
			},
			want: "Done.Done.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := NewAgent()
			rec := newRecorder()
			a.attach(rec, nil)
			s := &session{agent: a, id: "text"}
			c := &cycle{}
			for _, event := range tc.events {
				_, err := s.projectEvent(t.Context(), nil, c, event, usageReading{})
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, agentText(rec.snapshot()))
		})
	}
}

// TestUsageFollowsEachResponse proves every provider response a usage reading
// records reports the context Hermes counts after it, never a running sum,
// while the prompt response carries the turn's summed consumption.
func TestUsageFollowsEachResponse(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		prompt string
		want   []acp.SessionUsageUpdate
		usage  *acp.Usage
	}{
		"one response": {"HELLO", []acp.SessionUsageUpdate{{Size: 1000, Used: 10}}, &acp.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}},
		"tool calls": {"MULTI", []acp.SessionUsageUpdate{
			{Size: 1000, Used: 1000}, {Size: 1000, Used: 1120}, {Size: 1000, Used: 1200},
		}, &acp.Usage{InputTokens: 3320, OutputTokens: 50, TotalTokens: 3370}},
		"responses between ticks": {"BURST", []acp.SessionUsageUpdate{
			{Size: 1000, Used: 1120}, {Size: 1000, Used: 1200},
		}, &acp.Usage{InputTokens: 3320, OutputTokens: 60, TotalTokens: 3380}},
		"redirected mid-turn": {"REDIRECT", []acp.SessionUsageUpdate{
			{Size: 1000, Used: 1000}, {Size: 1000, Used: 900}, {Size: 1000, Used: 950},
		}, &acp.Usage{InputTokens: 2850, OutputTokens: 40, TotalTokens: 2890}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize()
			session := h.newSession()

			resp, err := h.prompt(session.SessionId, tc.prompt, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, usageUpdates(h.rec.snapshot()))
			require.Equal(t, tc.usage, resp.Usage)
		})
	}
}

// TestUnusableResponsesReportNoUsage proves a turn whose responses carry no
// context Hermes can state reports none, while their tokens still count.
func TestUnusableResponsesReportNoUsage(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "UNSIZED", nil)
	require.NoError(t, err)
	require.Empty(t, usageUpdates(h.rec.snapshot()))
	require.Equal(t, &acp.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}, resp.Usage)
}

// TestSettledUsageAfterCompaction proves no figure restates the context
// Hermes held before it compacted: the next one is the first response after
// the compaction, and a turn that settles with none sends nothing.
func TestSettledUsageAfterCompaction(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "COMPACT", nil)
	require.NoError(t, err)
	require.Equal(t, []acp.SessionUsageUpdate{{Size: 1000, Used: 900}, {Size: 1000, Used: 300}}, usageUpdates(h.rec.snapshot()))

	resp, err := h.prompt(session.SessionId, "COMPACTEND", nil)
	require.NoError(t, err)
	require.Equal(t, &acp.Usage{InputTokens: 900, OutputTokens: 50, TotalTokens: 950}, resp.Usage)
	require.Len(t, usageUpdates(h.rec.snapshot()), 3)

	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 900}, {Size: 1000, Used: 300}, {Size: 1000, Used: 900}, {Size: 1000, Used: 10},
	}, usageUpdates(h.rec.snapshot()))
}

// TestCancelledTurnReportsNoUsageAfterCancel proves a cancelled turn keeps
// the figures it reported before the cancel and sends none after it, while
// its consumption still counts the response that finished after the cancel.
func TestCancelledTurnReportsNoUsageAfterCancel(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, _ := h.prompt(session.SessionId, "STEPSLOW", nil)
		done <- resp
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(usageUpdates(updates)) == 1 })
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))

	resp := <-done
	require.Equal(t, acp.StopReasonCancelled, resp.StopReason)
	require.Equal(t, &acp.Usage{InputTokens: 2120, OutputTokens: 25, TotalTokens: 2145}, resp.Usage)
	require.Equal(t, []acp.SessionUsageUpdate{{Size: 1000, Used: 1000}}, usageUpdates(h.rec.snapshot()))
}

// TestAgentOriginUsageFollowsEachResponse proves a turn Hermes runs on its
// own reports each response as a prompt turn does, inside its own running
// turn, and that its consumption never reaches the prompt response that
// preceded it.
func TestAgentOriginUsageFollowsEachResponse(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	initResponse := h.initialize(withLifecycle())
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "AGENTWORK", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, &acp.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}, resp.Usage)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return idleTransitions(updates, session.SessionId) == 2 })

	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 10}, {Size: 1000, Used: 1000}, {Size: 1000, Used: 1120}, {Size: 1000, Used: 1200},
	}, usageUpdates(h.rec.snapshot()))
	require.NoError(t, lifecycle.CheckAttribution(negotiatedAnswer(t, initResponse), sessionFrames(t, h.rec.snapshot(), session.SessionId)))
}

// TestUsageOfCapturedRoutes replays the usage frames Hermes sent through two
// provider routes: each tick and the closing frame report the context of the
// responses they recorded, and the cycle sums their consumption.
func TestUsageOfCapturedRoutes(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/native/usage-routes.json")
	require.NoError(t, err)

	var routes map[string][]hermes.Event
	require.NoError(t, json.Unmarshal(data, &routes))

	for name, tc := range map[string]struct {
		want  []acp.SessionUsageUpdate
		usage *acp.Usage
	}{
		"openrouter": {[]acp.SessionUsageUpdate{
			{Size: 1000000, Used: 13485}, {Size: 1000000, Used: 14282}, {Size: 1000000, Used: 14364},
		}, &acp.Usage{InputTokens: 42131, OutputTokens: 1017, ThoughtTokens: new(936), TotalTokens: 43148}},
		"gateway": {[]acp.SessionUsageUpdate{
			{Size: 1000000, Used: 14178}, {Size: 1000000, Used: 14348}, {Size: 1000000, Used: 14439}, {Size: 1000000, Used: 14611}, {Size: 1000000, Used: 14705},
		}, &acp.Usage{InputTokens: 72281, OutputTokens: 1191, ThoughtTokens: new(814), TotalTokens: 73472}},
		"gateway-anthropic": {[]acp.SessionUsageUpdate{
			{Size: 1000000, Used: 18649}, {Size: 1000000, Used: 18728}, {Size: 1000000, Used: 18815},
		}, &acp.Usage{InputTokens: 56192, OutputTokens: 137, TotalTokens: 56329}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, routes[name])

			a := NewAgent()
			rec := newRecorder()
			a.attach(rec, nil)
			s := &session{agent: a, id: "usage"}
			rt := &runtime{}
			c := &cycle{}

			for _, event := range routes[name] {
				_, err := s.projectEvent(t.Context(), rt, c, event, rt.readUsage(event))
				require.NoError(t, err)
			}

			require.Equal(t, tc.want, usageUpdates(rec.snapshot()))
			require.Equal(t, tc.usage, promptUsage(c.state.usage))
		})
	}
}

// TestCompletedToolLeavesCycleState proves a completed tool call holds no
// state for the rest of a long cycle, and an id Hermes uses again starts a new
// call.
func TestCompletedToolLeavesCycleState(t *testing.T) {
	t.Parallel()

	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := &session{agent: a, id: "tools"}
	c := &cycle{}

	for range 2 {
		for _, kind := range []string{eventToolStart, eventToolComplete} {
			_, err := s.projectEvent(t.Context(), nil, c, hermes.Event{Type: kind, Payload: json.RawMessage(`{"tool_id":"call-1","name":"terminal"}`)}, usageReading{})
			require.NoError(t, err)
		}

		require.Empty(t, c.state.openTools)
	}

	starts := 0

	for _, update := range rec.snapshot() {
		if update.Update.ToolCall != nil {
			starts++
		}
	}

	require.Equal(t, 2, starts)
}

func TestContextTokens(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		reading usageReading
		want    int
		ok      bool
	}{
		"new response":                      {usageReading{current: hermes.Usage{Prompt: 300, ContextUsed: 120, ContextMax: 1000}, consumed: hermes.Usage{Prompt: 120}}, 120, true},
		"no response since":                 {usageReading{current: hermes.Usage{Prompt: 300, ContextUsed: 120, ContextMax: 1000}}, 0, false},
		"compacted since or window unknown": {usageReading{current: hermes.Usage{Prompt: 300}, consumed: hermes.Usage{Prompt: 120}}, 0, false},
		"no reading":                        {usageReading{}, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			used, ok := contextTokens(tc.reading)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, used)
		})
	}
}
