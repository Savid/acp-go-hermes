//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	hermesacp "github.com/savid/acp-go-hermes"
	"github.com/stretchr/testify/require"
)

func nativeHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	if source := os.Getenv("ACP_GO_HERMES_HOME"); source != "" {
		for _, name := range []string{"config.yaml", "auth.json", ".env"} {
			data, err := os.ReadFile(filepath.Join(source, name))
			if os.IsNotExist(err) {
				continue
			}
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(home, name), data, 0o600))
		}
	}

	return home
}

func TestNativeSmoke(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_INTEGRATION") != "1" {
		t.Skip("native integration gate")
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected provider request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "model calls are unavailable in smoke tests", http.StatusBadRequest)

			return
		}
		if r.URL.Path != "/v1/models" && r.URL.Path != "/api/v1/models" {
			http.NotFound(w, r)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"acpgogo-smoke","context_length":128000}]}`))
	}))
	t.Cleanup(provider.Close)
	home := t.TempDir()
	config := "model:\n  provider: custom\n  default: acpgogo-smoke\n  base_url: " + provider.URL + "/v1\n  context_length: 128000\n"
	require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte(config), 0o600))
	h := newHarness(t, hermesacp.WithHome(home), hermesacp.WithEnv(map[string]string{
		"OPENAI_API_KEY": "smoke-only", "OPENAI_BASE_URL": provider.URL + "/v1",
	}))
	h.initialize(withLifecycle())
	session := h.newSession()
	require.NotEmpty(t, session.SessionId)
	require.NotEmpty(t, session.ConfigOptions)
	raw, err := h.conn.CallExtension(h.ctx(), hermesacp.AccountUsageMethod, map[string]any{"providerId": "openrouter"})
	require.NoError(t, err)
	var usage wire.AccountUsageResponse
	require.NoError(t, json.Unmarshal(raw, &usage))
	require.NoError(t, usage.Validate())
	require.Equal(t, wire.AccountUsageUnavailable(wire.AccountUsageNotAuthenticated), usage, "an isolated home declares no gateway route for the provider")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
}

func TestNativeContinuation(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") != "1" {
		t.Skip("live token gate")
	}
	home, cwd := nativeHome(t), t.TempDir()
	store := acpcore.NewInMemorySessionStore()
	opts := []hermesacp.Option{hermesacp.WithHome(home), hermesacp.WithSessionStore(store)}
	if model := os.Getenv("ACP_GO_HERMES_MODEL"); model != "" {
		opts = append(opts, hermesacp.WithDefaultModel(model))
	}
	h := newHarness(t, opts...)
	h.initialize(withLifecycle())
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	response, err := h.prompt(session.SessionId, "Remember the project slug apricot-orbit. Reply with exactly apricot-orbit and nothing else. Do not use tools.", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Contains(t, agentText(h.rec.snapshot()), "apricot-orbit")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	h.stop()
	command := exec.CommandContext(h.ctx(), harnessPath(t), "chat", "--cli", "--quiet", "--resume", nativeSessionID(t, session.Meta), "--query", "Remember the release label cobalt-lantern. Reply with the project slug and release label, and nothing else. Do not use tools.")
	command.Dir = cwd
	command.Env = append(os.Environ(), "HERMES_HOME="+home)
	data, err := command.Output()
	require.NoError(t, err, "native resume")
	require.Contains(t, string(data), "apricot-orbit")
	require.Contains(t, string(data), "cobalt-lantern")
	restored := newHarness(t, opts...)
	restored.initialize(withLifecycle())
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "apricot-orbit")
	require.Contains(t, agentText(restored.rec.snapshot()), "cobalt-lantern")
	_, err = restored.prompt(session.SessionId, "What project slug and release label did we choose? Do not use tools.", promptMeta(2))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "apricot-orbit")
	require.Contains(t, agentText(restored.rec.snapshot()), "cobalt-lantern")
	_, err = restored.conn.CloseSession(restored.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	restored.stop()
	imported := newHarness(t, hermesacp.WithHome(nativeHome(t)), hermesacp.WithSessionStore(store))
	imported.initialize(withLifecycle())
	_, err = imported.conn.LoadSession(imported.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(imported.rec.snapshot()), "cobalt-lantern")
	_, err = imported.prompt(session.SessionId, "What project slug and release label did we choose? Do not use tools.", promptMeta(3))
	require.NoError(t, err)
	require.Contains(t, agentText(imported.rec.snapshot()), "apricot-orbit")
	require.Contains(t, agentText(imported.rec.snapshot()), "cobalt-lantern")
}

func TestNativeCallbacksPathAndCancellation(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") != "1" {
		t.Skip("live token gate")
	}
	home, cwd := nativeHome(t), t.TempDir()
	configure := exec.CommandContext(t.Context(), harnessPath(t), "config", "set", "approvals.mode", "manual")
	configure.Env = append(os.Environ(), "HERMES_HOME="+home)
	require.NoError(t, configure.Run())
	directories := []string{t.TempDir(), t.TempDir()}
	// A direct subprocess checks the inherited environment; terminal login files may change PATH.
	for index, dir := range directories {
		script := "#!/bin/sh\nprintf '%s\\n' 'marker-" + strconv.Itoa(index) + "'\nprintf 'ACP_PATH=%s\\n' \"$PATH\"\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "acpgogo-native-probe"), []byte(script), 0700))
	}
	h := newHarness(t, hermesacp.WithHome(home), hermesacp.WithDefaultModel(os.Getenv("ACP_GO_HERMES_MODEL")), hermesacp.WithEnv(map[string]string{"HERMES_YOLO_MODE": "0", "HERMES_SINGLE_QUERY_SESSION": "0"}))
	var questions atomic.Int32
	h.rec.elicit = func(request acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
		questions.Add(1)

		require.NotNil(t, request.Form)
		require.Len(t, request.Form.RequestedSchema.Properties, 1)
		answers := make(map[string]any, 1)
		for id := range request.Form.RequestedSchema.Properties {
			answers[id] = "cobalt"
		}

		return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: answers}}, nil
	}
	h.initialize(withLifecycle(), withFormElicitation())
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, hermesacp.WithSessionRawEvents(true), hermesacp.WithSessionHermesOptions(hermesacp.NewHermesOptions(hermesacp.WithHermesExtraPathDirs(directories[0])))))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(cwd, "remove-me.txt"), []byte("test-only"), 0600))
	response, err := h.prompt(session.SessionId, "Use the execute_code tool to run Python that removes the file remove-me.txt from the current directory. Do not use the terminal tool or any other tool. Reply DONE when finished.", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.NoFileExists(t, filepath.Join(cwd, "remove-me.txt"))
	h.rec.mu.Lock()
	permissions := len(h.rec.permissions)
	raw := len(h.rec.raw)
	h.rec.mu.Unlock()

	require.Positive(t, permissions)
	require.Positive(t, raw)
	_, err = h.prompt(session.SessionId, "Use the clarify tool to ask exactly one open-ended question, Which color? Do not offer choices or a batch. After receiving my answer, reply with that color. Do not use any other tool.", promptMeta(2))
	require.NoError(t, err)
	require.EqualValues(t, 1, questions.Load())
	// A direct subprocess checks the inherited environment; terminal login files may change PATH.
	for index, dir := range directories {
		if index > 0 {
			_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
			require.NoError(t, err)
			_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd, hermesacp.WithSessionRawEvents(true), hermesacp.WithSessionHermesOptions(hermesacp.NewHermesOptions(hermesacp.WithHermesExtraPathDirs(dir)))))
			require.NoError(t, err)
		}
		before := len(h.rec.snapshot())
		_, err = h.prompt(session.SessionId, "Use execute_code to run Python: import subprocess; print(subprocess.check_output([\"acpgogo-native-probe\"], text=True)). Do not set PATH, use a shell, use an absolute command path, or run other commands. Reply DONE after it runs.", promptMeta(index+3))
		require.NoError(t, err)
		output := toolText(h.rec.snapshot()[before:])
		require.Contains(t, output, "marker-"+strconv.Itoa(index))
		require.Contains(t, output, "ACP_PATH="+dir+string(os.PathListSeparator))
		if index > 0 {
			require.NotContains(t, output, directories[0])
		}
	}
	done := make(chan acp.PromptResponse, 1)
	failed := make(chan error, 1)
	go func() {
		response, promptErr := h.prompt(session.SessionId, "Use the terminal tool to run exactly: printf started > sleep-started; sleep 60. Do not use a timeout or run it in the background. Reply DONE when it finishes.", promptMeta(5))
		done <- response
		failed <- promptErr
	}()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(cwd, "sleep-started"))

		return statErr == nil
	}, 45*time.Second, 25*time.Millisecond)
	require.NoError(t, h.conn.Cancel(h.ctx(), acp.CancelNotification{SessionId: session.SessionId}))
	require.NoError(t, <-failed)
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)
	_, err = h.conn.UnstableDeleteSession(h.ctx(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	list, err := h.conn.ListSessions(h.ctx(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}

func toolText(updates []acp.SessionNotification) string {
	var text strings.Builder
	for _, update := range updates {
		if tool := update.Update.ToolCallUpdate; tool != nil {
			for _, item := range tool.Content {
				if item.Content != nil && item.Content.Content.Text != nil {
					text.WriteString(item.Content.Content.Text.Text)
				}
			}
		}
	}

	return text.String()
}

func nativeSessionID(t *testing.T, meta map[string]any) string {
	t.Helper()
	binding, ok := meta["hermes"].(map[string]any)
	require.True(t, ok)
	id, ok := binding["nativeSessionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}
