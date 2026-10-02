package hermesacp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

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
