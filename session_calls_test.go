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
// into the native home and asks the gateway to enable it only while
// config.yaml lists it neither as enabled nor as disabled.
func TestPluginIsInstalledIntoTheHome(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		config  string
		toggles string
	}{
		"no config":     {"", hermes.PluginName + "\n"},
		"unlisted":      {"model:\n  default: vision\nplugins:\n  enabled: [other]\n", hermes.PluginName + "\n"},
		"enabled":       {"plugins:\n  enabled:\n    - " + hermes.PluginName + "\n", ""},
		"user disabled": {"plugins:\n  enabled: [" + hermes.PluginName + "]\n  disabled: [" + hermes.PluginName + "]\n", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			home := filepath.Join(t.TempDir(), "home")
			require.NoError(t, os.MkdirAll(home, 0o700))

			if tc.config != "" {
				require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte(tc.config), 0o600))
			}

			h := newHarness(t, WithHome(home))
			h.initialize()
			h.newSession()

			for _, file := range []string{"plugin.yaml", "__init__.py"} {
				want, err := os.ReadFile(filepath.Join("internal", "hermes", "plugin", file))
				require.NoError(t, err)

				got, err := os.ReadFile(filepath.Join(home, "plugins", hermes.PluginName, file))
				require.NoError(t, err)
				require.Equal(t, string(want), string(got))
			}

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
		require.True(t, rt.owns(event), event.Type)

		_, err := s.projectEvent(t.Context(), rt, c, event, rt.readUsage(event))
		require.NoError(t, err)
	}

	want := make([]*wire.CallUsage, 0, len(fixture.Gateway))
	sized := make([]acp.SessionUsageUpdate, 0, len(fixture.Gateway))

	for _, response := range fixture.Gateway {
		report, ok := reportCall(response.Usage, response.ID)
		require.True(t, ok)
		want = append(want, &report.usage)
		sized = append(sized, acp.SessionUsageUpdate{Size: 1000000, Used: int(*response.Usage.PromptTokens)})
	}

	updates := rec.snapshot()
	require.Equal(t, sized, sizes(usageUpdates(updates)))
	require.Equal(t, want, callUsages(t, updates))
	require.Equal(t, []*wire.CallUsage{
		{ResponseID: "gen-1790901785-49KwAHAIS25nhnegDA3d", InputTokens: new(608), CachedReadTokens: new(15360), CachedWriteTokens: new(0), OutputTokens: new(60)},
		{ResponseID: "gen-1790901787-1gDpSSebiamqKh9ti4LB", InputTokens: new(166), CachedReadTokens: new(15872), CachedWriteTokens: new(0), OutputTokens: new(682)},
	}, want)
	consumed := promptUsage(c.state.usage)
	require.Equal(t, 32006, consumed.InputTokens, "the prompt response still sums Hermes's counters")
	require.Equal(t, 742, consumed.OutputTokens)
}
