package hermesacp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
		s.handleEvent(t.Context(), rt, envelope.Params, nil)
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
		s.handleEvent(t.Context(), rt, hermes.Event{Type: kind, RequestID: kind + "|" + rt.liveID, Payload: []byte(`{}`)}, nil)
	}

	s.handleEvent(t.Context(), rt, hermes.Event{Type: eventMessageComplete, Payload: []byte(`{"text":"","status":"complete"}`)}, nil)

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

	s.handleEvent(t.Context(), rt, hermes.Event{Type: stopReasonError, Payload: []byte(`{"message":"provider unavailable"}`)}, nil)

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
	}, nil)
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
				_, err := s.projectEvent(t.Context(), &runtime{}, c, event, usageReading{})
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
		"last response ticked": {"COVERED", []acp.SessionUsageUpdate{{Size: 1000, Used: 1000}}, &acp.Usage{InputTokens: 1000, OutputTokens: 20, TotalTokens: 1020}},
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

// TestLastResponseReportsInsideTurn proves the response that ends a prompt
// turn, which no tick records because Hermes stops its ticks before
// message.complete, reports once from the closing frame while the turn still
// runs.
func TestLastResponseReportsInsideTurn(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	initResponse := h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "MULTI", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 1000}, {Size: 1000, Used: 1120}, {Size: 1000, Used: 1200},
	}, usageUpdates(h.rec.snapshot()))
	require.NoError(t, lifecycle.CheckAttribution(negotiatedAnswer(t, initResponse), sessionFrames(t, h.rec.snapshot(), session.SessionId)))
}

// TestEmptyUsageIsUnknown proves a response a gateway replays from its
// response cache, whose usage is all zero, reports nothing: Hermes counts the
// call without moving a token counter and stops stating the context, so no
// update says 0 and the next response reports again.
func TestEmptyUsageIsUnknown(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	for _, step := range []struct {
		prompt string
		usage  *acp.Usage
	}{
		{"HELLO", &acp.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}},
		{"REPLAYED", nil},
		{"REPLAY", &acp.Usage{InputTokens: 1000, OutputTokens: 20, TotalTokens: 1020}},
		{"HELLO", &acp.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}},
	} {
		resp, err := h.prompt(session.SessionId, step.prompt, nil)
		require.NoError(t, err)
		require.Equal(t, step.usage, resp.Usage, step.prompt)
	}

	require.Equal(t, []acp.SessionUsageUpdate{
		{Size: 1000, Used: 10}, {Size: 1000, Used: 1000}, {Size: 1000, Used: 10},
	}, usageUpdates(h.rec.snapshot()))
}

// TestUsageOfCapturedReplay replays the closing frames of three turns whose
// second response Hermes received with all-zero usage: that turn reports
// nothing and sums nothing, and the next response reports its own context.
func TestUsageOfCapturedReplay(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/native/usage-routes.json")
	require.NoError(t, err)

	var routes map[string][]hermes.Event
	require.NoError(t, json.Unmarshal(data, &routes))

	frames := routes["replayed"]
	require.Len(t, frames, 3)

	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := &session{agent: a, id: "replayed"}
	rt := &runtime{}

	consumed := make([]*acp.Usage, 0, len(frames))

	for _, event := range frames {
		c := &cycle{}
		settled, err := s.projectEvent(t.Context(), rt, c, event, rt.readUsage(event, nil))
		require.NoError(t, err)
		require.True(t, settled)

		consumed = append(consumed, promptUsage(c.state.usage))
	}

	require.Equal(t, []acp.SessionUsageUpdate{{Size: 200000, Used: 1201}, {Size: 200000, Used: 1204}}, usageUpdates(rec.snapshot()))
	require.Equal(t, []*acp.Usage{
		{InputTokens: 1201, OutputTokens: 7, ThoughtTokens: new(2), TotalTokens: 1208},
		nil,
		{InputTokens: 1204, OutputTokens: 7, ThoughtTokens: new(2), TotalTokens: 1211},
	}, consumed)
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
				_, err := s.projectEvent(t.Context(), rt, c, event, rt.readUsage(event, nil))
				require.NoError(t, err)
			}

			require.Equal(t, tc.want, usageUpdates(rec.snapshot()))
			require.Equal(t, tc.usage, promptUsage(c.state.usage))
		})
	}
}

// TestCapturedResponseCarriesNoResponseID projects a captured one-response
// turn live and replays its captured export. Hermes states the id its gateway
// returned for a response in neither the gateway frames nor the persisted
// messages, so no chunk carries a messageId; without the plugin's call
// reports no usage update carries a call usage report.
func TestCapturedResponseCarriesNoResponseID(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/native/response-turn.json")
	require.NoError(t, err)

	var fixture struct {
		Live   []hermes.Event  `json:"live"`
		Export json.RawMessage `json:"export"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))

	var exported struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(fixture.Export, &exported))

	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := &session{agent: a, id: acp.SessionId(exported.ID), nativeID: exported.ID}
	rt := &runtime{}
	c := &cycle{}

	for _, event := range fixture.Live {
		_, err := s.projectEvent(t.Context(), rt, c, event, rt.readUsage(event, nil))
		require.NoError(t, err)
	}

	live := len(rec.snapshot())
	require.NoError(t, s.replay(t.Context(), [][]byte{fixture.Export}))

	updates := rec.snapshot()
	for name, part := range map[string][]acp.SessionNotification{"live": updates[:live], "replay": updates[live:]} {
		var messages, thoughts int

		for _, n := range part {
			if chunk := n.Update.AgentMessageChunk; chunk != nil {
				messages++
				require.Nil(t, chunk.MessageId, name)
			}
			if chunk := n.Update.AgentThoughtChunk; chunk != nil {
				thoughts++
				require.Nil(t, chunk.MessageId, name)
			}
		}

		require.Positive(t, messages, name)
		require.Positive(t, thoughts, name)
	}

	require.Equal(t, "Hi! What can I help you with today?Hi! What can I help you with today?", agentText(updates))
	require.Equal(t, []acp.SessionUsageUpdate{{Size: 1000000, Used: 13774}}, usageUpdates(updates))
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
			_, err := s.projectEvent(t.Context(), &runtime{}, c, hermes.Event{Type: kind, Payload: json.RawMessage(`{"tool_id":"call-1","name":"terminal"}`)}, usageReading{})
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
		"all-zero response":                 {usageReading{current: hermes.Usage{Prompt: 300}}, 0, false},
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

// callUsages decodes the call breakdown each usage update carries, nil for an
// update without one.
func callUsages(t *testing.T, updates []acp.SessionNotification) []*wire.CallUsage {
	t.Helper()

	var calls []*wire.CallUsage

	for _, update := range updates {
		payload := update.Update.UsageUpdate
		if payload == nil {
			continue
		}

		member, ok := payload.Meta[wire.CallUsageKey]
		if !ok {
			calls = append(calls, nil)

			continue
		}

		data, err := json.Marshal(member)
		require.NoError(t, err)

		var call wire.CallUsage
		require.NoError(t, json.Unmarshal(data, &call))

		calls = append(calls, &call)
	}

	return calls
}

func sizes(updates []acp.SessionUsageUpdate) []acp.SessionUsageUpdate {
	out := make([]acp.SessionUsageUpdate, 0, len(updates))
	for _, update := range updates {
		out = append(out, acp.SessionUsageUpdate{Size: update.Size, Used: update.Used})
	}

	return out
}

// TestCallReportsCarryTheBreakdown proves each response the plugin reports
// yields exactly one usage update carrying its breakdown and gateway id, with
// only the members the gateway sent, a reported zero kept as 0 and an omitted
// counter absent; no usage reading restates a reported response; the
// prompt response still sums Hermes's counters.
func TestCallReportsCarryTheBreakdown(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		prompt string
		want   []acp.SessionUsageUpdate
		calls  []*wire.CallUsage
		usage  *acp.Usage
	}{
		"tool turn": {"CALLS", []acp.SessionUsageUpdate{{Size: 1000, Used: 1000}, {Size: 1000, Used: 1120}, {Size: 1000, Used: 1200}}, []*wire.CallUsage{
			{ResponseID: "gen-1", InputTokens: new(1000), CachedReadTokens: new(0), OutputTokens: new(20)},
			{ResponseID: "gen-2", InputTokens: new(120), CachedReadTokens: new(1000), CachedWriteTokens: new(0), OutputTokens: new(20)},
			{InputTokens: new(50), CachedReadTokens: new(1100), CachedWriteTokens: new(50), OutputTokens: new(10)},
		}, &acp.Usage{InputTokens: 3320, OutputTokens: 50, TotalTokens: 3370}},
		"tick before a reported response is recorded": {"CALLRACE", []acp.SessionUsageUpdate{{Size: 1000, Used: 1000}, {Size: 1000, Used: 1120}, {Size: 1000, Used: 1200}}, []*wire.CallUsage{
			{ResponseID: "gen-1", InputTokens: new(1000), CachedReadTokens: new(0), CachedWriteTokens: new(0), OutputTokens: new(20)},
			{ResponseID: "gen-2", InputTokens: new(120), CachedReadTokens: new(1000), CachedWriteTokens: new(0), OutputTokens: new(20)},
			{ResponseID: "gen-3", InputTokens: new(100), CachedReadTokens: new(1100), CachedWriteTokens: new(0), OutputTokens: new(10)},
		}, &acp.Usage{InputTokens: 3320, OutputTokens: 50, TotalTokens: 3370}},
		"retried request": {"CALLRETRY", []acp.SessionUsageUpdate{{Size: 1000, Used: 1000}, {Size: 1000, Used: 1000}}, []*wire.CallUsage{
			{ResponseID: "gen-rejected", InputTokens: new(1000), CachedReadTokens: new(0), CachedWriteTokens: new(0), OutputTokens: new(0)},
			{ResponseID: "gen-accepted", InputTokens: new(100), CachedReadTokens: new(900), CachedWriteTokens: new(0), OutputTokens: new(20)},
		}, &acp.Usage{InputTokens: 1000, OutputTokens: 20, TotalTokens: 1020}},
		"response no call report covers after a rejected attempt": {"CALLFALLBACK", []acp.SessionUsageUpdate{{Size: 1000, Used: 1000}, {Size: 1000, Used: 1000}, {Size: 1000, Used: 900}, {Size: 1000, Used: 950}}, []*wire.CallUsage{
			{ResponseID: "gen-rejected", InputTokens: new(1000), CachedReadTokens: new(0), CachedWriteTokens: new(0), OutputTokens: new(0)},
			{ResponseID: "gen-accepted", InputTokens: new(100), CachedReadTokens: new(900), CachedWriteTokens: new(0), OutputTokens: new(20)},
			nil, nil,
		}, &acp.Usage{InputTokens: 2850, OutputTokens: 40, TotalTokens: 2890}},
		"another conversation's response": {"CALLFOREIGN", []acp.SessionUsageUpdate{{Size: 1000, Used: 10}}, []*wire.CallUsage{
			{ResponseID: "gen-own", InputTokens: new(10), CachedReadTokens: new(0), CachedWriteTokens: new(0), OutputTokens: new(5)},
		}, &acp.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}},
		"empty report": {"CALLEMPTY", []acp.SessionUsageUpdate{}, []*wire.CallUsage(nil), nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize()
			session := h.newSession()

			resp, err := h.prompt(session.SessionId, tc.prompt, nil)
			require.NoError(t, err)

			updates := h.rec.snapshot()
			require.Equal(t, tc.want, sizes(usageUpdates(updates)))
			require.Equal(t, tc.calls, callUsages(t, updates))
			require.Equal(t, tc.usage, resp.Usage)
		})
	}
}

// TestCallReportStatesItsModelsWindow proves a call report states the context
// window of the model that served it: after a model switch, the first
// response waits for the reading that states the new model's window.
func TestCallReportStatesItsModelsWindow(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "CALLS", nil)
	require.NoError(t, err)

	_, err = h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "fake/"+fakeWideModel))
	require.NoError(t, err)

	before := len(usageUpdates(h.rec.snapshot()))

	_, err = h.prompt(session.SessionId, "CALLS", nil)
	require.NoError(t, err)

	updates := usageUpdates(h.rec.snapshot())[before:]
	require.Equal(t, []acp.SessionUsageUpdate{{Size: fakeWideWindow, Used: 1000}, {Size: fakeWideWindow, Used: 1120}, {Size: fakeWideWindow, Used: 1200}}, sizes(updates))
}

// TestCallReportPrecedesItsTools proves a response reported once the runtime
// knows the context window reports before the tool call it requested starts,
// in native order; the first response of a process waits for the reading that
// states the window.
func TestCallReportPrecedesItsTools(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "CALLS", nil)
	require.NoError(t, err)

	order := func(match func(acp.SessionUpdate) bool) int {
		for index, update := range h.rec.snapshot() {
			if match(update.Update) {
				return index
			}
		}

		t.Fatal("update not found")

		return -1
	}
	reported := func(id string) func(acp.SessionUpdate) bool {
		return func(update acp.SessionUpdate) bool {
			if update.UsageUpdate == nil {
				return false
			}

			call, ok := update.UsageUpdate.Meta[wire.CallUsageKey].(map[string]any)

			return ok && call["responseId"] == id
		}
	}
	started := func(id string) func(acp.SessionUpdate) bool {
		return func(update acp.SessionUpdate) bool {
			return update.ToolCall != nil && string(update.ToolCall.ToolCallId) == id
		}
	}

	require.Less(t, order(started("calls-0")), order(reported("gen-1")), "the first response waits for the window")
	require.Less(t, order(reported("gen-2")), order(started("calls-1")))
}

// TestCancelledTurnReportsNoCallAfterCancel proves responses reported after a
// cancel report nothing, while their tokens still count.
func TestCancelledTurnReportsNoCallAfterCancel(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, _ := h.prompt(session.SessionId, "CALLSLOW", nil)
		done <- resp
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(usageUpdates(updates)) == 1 })
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))

	resp := <-done
	require.Equal(t, acp.StopReasonCancelled, resp.StopReason)
	require.Equal(t, &acp.Usage{InputTokens: 3320, OutputTokens: 45, TotalTokens: 3365}, resp.Usage)
	require.Equal(t, []*wire.CallUsage{
		{ResponseID: "gen-1", InputTokens: new(1000), CachedReadTokens: new(0), CachedWriteTokens: new(0), OutputTokens: new(20)},
	}, callUsages(t, h.rec.snapshot()))
}

// TestPluginIsInstalledIntoTheHome proves every launch publishes the plugin
// into the home hermes uses, the active profile's when one is set, starts the
// gateway with call reports switched on, and asks it to enable the plugin
// only while config.yaml lists it neither as enabled nor as disabled. A home
// the plugin cannot be published into still starts the session.
func TestPluginIsInstalledIntoTheHome(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		profile string
		config  string
		blocked bool
		toggles string
	}{
		"no config":      {"", "", false, hermes.PluginName + "\n"},
		"unlisted":       {"", "model:\n  default: vision\nplugins:\n  enabled: [other]\n", false, hermes.PluginName + "\n"},
		"enabled":        {"", "plugins:\n  enabled:\n    - " + hermes.PluginName + "\n", false, ""},
		"user disabled":  {"", "plugins:\n  disabled: [" + hermes.PluginName + "]\n", false, ""},
		"active profile": {"work", "", false, hermes.PluginName + "\n"},
		"unpublishable":  {"", "", true, ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root := filepath.Join(t.TempDir(), "home")
			home := root
			require.NoError(t, os.MkdirAll(root, 0o700))

			if tc.profile != "" {
				home = filepath.Join(root, "profiles", tc.profile)
				require.NoError(t, os.MkdirAll(home, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(root, "active_profile"), []byte(tc.profile+"\n"), 0o600))
			}

			if tc.config != "" {
				require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte(tc.config), 0o600))
			}

			if tc.blocked {
				require.NoError(t, os.MkdirAll(filepath.Join(home, "plugins"), 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(home, "plugins", hermes.PluginName), nil, 0o600))
			}

			h := newHarness(t, WithHome(root))
			h.initialize()
			h.newSession()

			if !tc.blocked {
				for _, file := range []string{"plugin.yaml", "__init__.py"} {
					want, err := os.ReadFile(filepath.Join("internal", "hermes", "plugin", file))
					require.NoError(t, err)

					got, err := os.ReadFile(filepath.Join(home, "plugins", hermes.PluginName, file))
					require.NoError(t, err)
					require.Equal(t, string(want), string(got))
				}
			}

			if tc.profile != "" {
				require.NoDirExists(t, filepath.Join(root, "plugins"), "the root home is not the profile's")
			}

			reports, err := os.ReadFile(filepath.Join(home, fakeCallReportsEnv))
			require.NoError(t, err)
			require.Equal(t, "1", string(reports))

			toggles, err := os.ReadFile(filepath.Join(home, fakePluginToggles))
			if tc.toggles == "" {
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.toggles, string(toggles))
			}
		})
	}
}

// TestHeldCallWaitsForItsWindow proves a call report that arrives before the
// runtime knows its model's context window is released by the first reading
// that states that window, not by a reading without one, such as a tick after
// a compaction; a call whose window no reading states reports nothing.
func TestHeldCallWaitsForItsWindow(t *testing.T) {
	t.Parallel()

	event := func(kind, sid string, payload any) hermes.Event {
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		return hermes.Event{Type: kind, SessionID: sid, Payload: data}
	}
	report := func(model string, prompt int) hermes.Event {
		return event(hermes.CallEvent, "", map[string]any{
			nativeSessionIDKey: "native", "model": model, "response_id": "gen-" + model,
			"usage": map[string]any{"prompt_tokens": prompt, "completion_tokens": 20, "prompt_tokens_details": map[string]any{"cached_tokens": 0}},
		})
	}
	tick := func(model string, prompt int, window int) hermes.Event {
		usage := map[string]any{"model": model, "prompt": prompt, "completion": 20, "total": prompt + 20}
		if window > 0 {
			usage["context_used"], usage["context_max"] = prompt, window
		}

		return event(eventSessionUsage, "live", map[string]any{"usage": usage})
	}

	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := &session{agent: a, id: "native", nativeID: "native"}
	rt := &runtime{liveID: "live", nativeID: "native"}
	c := &cycle{}

	project := func(event hermes.Event) []acp.SessionUsageUpdate {
		call, ok := rt.owns(event)
		require.True(t, ok)

		_, err := s.projectEvent(t.Context(), rt, c, event, rt.readUsage(event, call))
		require.NoError(t, err)

		return sizes(usageUpdates(rec.snapshot()))
	}

	project(report("m1", 1000))
	require.Empty(t, project(tick("m1", 1000, 0)), "a reading without the window releases nothing")
	require.Equal(t, []acp.SessionUsageUpdate{{Size: 4000, Used: 1000}}, project(tick("m1", 1000, 4000)))

	project(report("m2", 1500))
	require.Len(t, project(tick("m1", 1000, 4000)), 1, "another model's window releases nothing")
	require.Len(t, project(tick("m2", 2500, 0)), 1)
	require.Len(t, c.state.heldCalls, 1)
}

// TestCapturedCallReports replays a tool turn captured from hermes serve
// against OpenRouter. Each response's call report arrives after its streamed
// text and before its tool starts, the tick that records it, and
// message.complete, so each response reports once with the id and usage its
// gateway returned; the first waits for the tick that states the window, and
// no reading restates a reported response.
func TestCapturedCallReports(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/native/call-usage.json")
	require.NoError(t, err)

	var fixture struct {
		Live    []hermes.Event `json:"live"`
		Gateway []struct {
			ID    string           `json:"id"`
			Usage hermes.ChatUsage `json:"usage"`
		} `json:"gateway"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Len(t, fixture.Gateway, 2)

	position := func(kind string, nth int) int {
		for index, event := range fixture.Live {
			if event.Type == kind {
				if nth == 0 {
					return index
				}

				nth--
			}
		}

		return -1
	}
	require.Less(t, position(hermes.CallEvent, 0), position(eventToolStart, 0))
	require.Less(t, position(eventToolStart, 0), position(eventSessionUsage, 0))
	require.Less(t, position(eventMessageDelta, 0), position(hermes.CallEvent, 1))
	require.Less(t, position(hermes.CallEvent, 1), position(eventMessageComplete, 0))

	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := &session{agent: a, id: "fixture-native", nativeID: "fixture-native"}
	rt := &runtime{liveID: "fixture-session", nativeID: "fixture-native"}
	c := &cycle{}

	for _, event := range fixture.Live {
		call, ok := rt.owns(event)
		require.True(t, ok, event.Type)

		_, err := s.projectEvent(t.Context(), rt, c, event, rt.readUsage(event, call))
		require.NoError(t, err)
	}

	want := make([]*wire.CallUsage, 0, len(fixture.Gateway))
	sized := make([]acp.SessionUsageUpdate, 0, len(fixture.Gateway))

	for _, response := range fixture.Gateway {
		report, ok := reportCall(hermes.Call{ResponseID: response.ID, Usage: response.Usage})
		require.True(t, ok)
		want = append(want, &report.usage)
		sized = append(sized, acp.SessionUsageUpdate{Size: 1000000, Used: int(*response.Usage.Tokens().Prompt)})
	}

	updates := rec.snapshot()
	require.Equal(t, sized, sizes(usageUpdates(updates)))
	require.Equal(t, want, callUsages(t, updates))
	require.Equal(t, []*wire.CallUsage{
		{ResponseID: "gen-1790905431-DFBUn237mC4NTPsf32na", InputTokens: new(7766), CachedReadTokens: new(8192), CachedWriteTokens: new(0), OutputTokens: new(355)},
		{ResponseID: "gen-1790905436-gNlAAmhBvUpwTWAyPp1G", InputTokens: new(156), CachedReadTokens: new(15872), CachedWriteTokens: new(0), OutputTokens: new(713)},
	}, want)
	consumed := promptUsage(c.state.usage)
	require.Equal(t, 31986, consumed.InputTokens, "the prompt response still sums Hermes's counters")
	require.Equal(t, 1068, consumed.OutputTokens)
}

// TestReportCallCountsAsHermesCounts proves a report's context is the prompt
// Hermes records, its uncached input plus both cache buckets, and its
// breakdown only states the uncached input arithmetic over reported figures
// allows.
func TestReportCallCountsAsHermesCounts(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		usage hermes.ChatUsage
		used  int
		call  wire.CallUsage
	}{
		"prompt includes the cache": {
			hermes.ChatUsage{"prompt_tokens": 1200.0, "completion_tokens": 10.0, "cache_read_input_tokens": 1000.0, "cache_creation_input_tokens": 100.0},
			1200, wire.CallUsage{InputTokens: new(100), CachedReadTokens: new(1000), CachedWriteTokens: new(100), OutputTokens: new(10)},
		},
		"prompt excludes the cache": {
			hermes.ChatUsage{"prompt_tokens": 50.0, "completion_tokens": 10.0, "cache_read_input_tokens": 1000.0, "cache_creation_input_tokens": 100.0},
			1100, wire.CallUsage{CachedReadTokens: new(1000), CachedWriteTokens: new(100), OutputTokens: new(10)},
		},
		"no cache figures": {
			hermes.ChatUsage{"prompt_tokens": 300.0, "completion_tokens": 10.0},
			300, wire.CallUsage{OutputTokens: new(10)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			report, ok := reportCall(hermes.Call{Usage: tc.usage})
			require.True(t, ok)
			require.Equal(t, tc.used, report.used)
			require.Equal(t, tc.call, report.usage)
		})
	}
}
