//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	hermesacp "github.com/savid/acp-go-hermes"
	"github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// TestMain keeps every hermes the tests start from installing anything into
// the install state and tools the test homes share.
func TestMain(m *testing.M) {
	if err := os.Setenv("HERMES_DISABLE_LAZY_INSTALLS", "1"); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

// testHome is a native home whose config.yaml enables only the adapter's
// plugin. Hermes selects its dependency environment through the home's
// install state and finds its managed tools in the home's store, so both are
// linked from ACP_GO_HERMES_HOME, else from ~/.hermes.
func testHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	source := os.Getenv("ACP_GO_HERMES_HOME")
	if source == "" {
		user, err := os.UserHomeDir()
		require.NoError(t, err)
		source = filepath.Join(user, ".hermes")
	}
	for _, name := range []string{"installs", "tools"} {
		if shared := filepath.Join(source, name); isDir(shared) {
			require.NoError(t, os.Symlink(shared, filepath.Join(home, name)))
		}
	}
	writeConfig(t, home, "")

	return home
}

// writeConfig writes config into home's config.yaml with the adapter's
// plugin listed in plugins.enabled. A launch then never asks the gateway to
// enable it, which would publish a new dependency generation into the install
// state the test homes share.
func writeConfig(t *testing.T, home, config string) {
	t.Helper()
	document := map[string]any{}
	require.NoError(t, yaml.Unmarshal([]byte(config), &document))
	plugins, _ := document["plugins"].(map[string]any)
	if plugins == nil {
		plugins = map[string]any{}
	}
	enabled, _ := plugins["enabled"].([]any)
	if !slices.Contains(enabled, any(hermes.PluginName)) {
		enabled = append(enabled, hermes.PluginName)
	}
	plugins["enabled"] = enabled
	document["plugins"] = plugins
	data, err := yaml.Marshal(document)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), data, 0o600))
}

// nativeHome is a test home holding the configuration and credentials of
// ACP_GO_HERMES_HOME.
func nativeHome(t *testing.T) string {
	t.Helper()
	home := testHome(t)
	if source := os.Getenv("ACP_GO_HERMES_HOME"); source != "" {
		for _, name := range []string{"config.yaml", "auth.json", ".env"} {
			data, err := os.ReadFile(filepath.Join(source, name))
			if os.IsNotExist(err) {
				continue
			}
			require.NoError(t, err)
			if name == "config.yaml" {
				writeConfig(t, home, string(data))

				continue
			}
			require.NoError(t, os.WriteFile(filepath.Join(home, name), data, 0o600))
		}
	}

	return home
}

func isDir(path string) bool {
	info, err := os.Stat(path)

	return err == nil && info.IsDir()
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
	home := testHome(t)
	config := "model:\n  provider: custom\n  default: acpgogo-smoke\n  base_url: " + provider.URL + "/v1\n  context_length: 128000\n"
	writeConfig(t, home, config)
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

const (
	openRouterBase = "https://openrouter.ai/api"
	callUsageModel = "qwen/qwen3.8-flash"
)

// exchange is one chat completion the recording proxy passed to the gateway.
type exchange struct {
	request  []byte
	response []byte
}

// recordingProxy forwards to OpenRouter, retries a rate-limited request, and
// keeps every chat completion's request and response body.
type recordingProxy struct {
	mu        sync.Mutex
	exchanges []exchange
}

func (p *recordingProxy) RoundTrip(request *http.Request) (*http.Response, error) {
	var body []byte

	if request.Body != nil {
		var err error
		if body, err = io.ReadAll(request.Body); err != nil {
			return nil, err
		}
	}

	for attempt := 0; ; attempt++ {
		request.Body = io.NopCloser(bytes.NewReader(body))

		response, err := http.DefaultTransport.RoundTrip(request)
		if err != nil || response.StatusCode != http.StatusTooManyRequests || attempt == 5 {
			if err == nil && strings.HasSuffix(request.URL.Path, "/chat/completions") {
				response.Body = &recordedBody{ReadCloser: response.Body, done: func(data []byte) {
					p.mu.Lock()
					p.exchanges = append(p.exchanges, exchange{request: body, response: data})
					p.mu.Unlock()
				}}
			}

			return response, err
		}

		_ = response.Body.Close()
		time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
	}
}

func (p *recordingProxy) recorded() []exchange {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]exchange(nil), p.exchanges...)
}

type recordedBody struct {
	io.ReadCloser
	data bytes.Buffer
	once sync.Once
	done func([]byte)
}

func (b *recordedBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	b.data.Write(data[:n])

	return n, err
}

func (b *recordedBody) Close() error {
	b.once.Do(func() { b.done(b.data.Bytes()) })

	return b.ReadCloser.Close()
}

// waitFor polls condition until it holds or the bound passes.
func waitFor(bound time.Duration, condition func() bool) {
	for deadline := time.Now().Add(bound); time.Now().Before(deadline) && !condition(); {
		time.Sleep(100 * time.Millisecond)
	}
}

// gatewayResponse is the id and usage a gateway body carries: a JSON
// completion, or a stream whose chunks carry the id and whose last usage
// chunk carries the usage.
type gatewayResponse struct {
	ID    string
	Usage map[string]any
}

func parseGatewayResponse(t *testing.T, body []byte) gatewayResponse {
	t.Helper()

	var parsed gatewayResponse

	read := func(data []byte) {
		var frame struct {
			ID    string         `json:"id"`
			Usage map[string]any `json:"usage"`
		}
		if json.Unmarshal(data, &frame) != nil {
			return
		}

		if parsed.ID == "" {
			parsed.ID = frame.ID
		}

		if frame.Usage != nil {
			parsed.Usage = frame.Usage
		}
	}

	if trimmed := bytes.TrimSpace(body); len(trimmed) > 0 && trimmed[0] == '{' {
		read(trimmed)

		return parsed
	}

	lines := bufio.NewScanner(bytes.NewReader(body))
	lines.Buffer(make([]byte, 0, 1<<20), 16<<20)

	for lines.Scan() {
		if data, ok := strings.CutPrefix(lines.Text(), "data: "); ok && data != "[DONE]" {
			read([]byte(data))
		}
	}

	require.NoError(t, lines.Err())

	return parsed
}

// counter reads one usage member as the gateway sent it, nil when absent.
func counter(usage map[string]any, path ...string) *int {
	var value any = usage

	for _, key := range path {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}

		value = object[key]
	}

	number, ok := value.(float64)
	if !ok {
		return nil
	}

	return new(int(number))
}

// TestNativeCallUsage runs a tool turn through hermes serve against
// OpenRouter behind a recording proxy and proves every reported call is a
// response the gateway returned for the session's own conversation: its
// responseId is the gateway's id and its breakdown is the gateway's usage
// exactly, each such response reports once, and no side call reports.
func TestNativeCallUsage(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") != "1" {
		t.Skip("live token gate")
	}

	key := os.Getenv("OPENROUTER_API_KEY")
	require.NotEmpty(t, key, "OPENROUTER_API_KEY must hold an OpenRouter key")

	upstream, err := url.Parse(openRouterBase)
	require.NoError(t, err)

	proxy := &recordingProxy{}
	reverse := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(upstream)
			// The transport then negotiates and decodes compression itself, so
			// the recorded bodies are plain.
			request.Out.Header.Del("Accept-Encoding")
		},
		Transport: proxy,
	}
	server := httptest.NewServer(reverse)
	t.Cleanup(server.Close)

	home, cwd := nativeHome(t), t.TempDir()
	config := "model:\n  provider: acpgo-gateway\n  default: " + callUsageModel + "\nproviders:\n  acpgo-gateway:\n    name: acpgo-gateway\n    api: " + server.URL + "/v1\n    api_mode: chat_completions\n    key_env: OPENROUTER_API_KEY\n"
	writeConfig(t, home, config)

	h := newHarness(t, hermesacp.WithHome(home), hermesacp.WithEnv(map[string]string{
		"OPENROUTER_API_KEY": key, "HERMES_YOLO_MODE": "1",
	}))
	h.initialize(withLifecycle())
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, hermesacp.WithSessionRawEvents(true)))
	require.NoError(t, err)

	response, err := h.prompt(session.SessionId, "Use the terminal tool to run exactly: echo acp-call-usage. After it finishes, reply with exactly DONE and nothing else.", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	// Hermes titles a new session with a side call after its first turn.
	waitFor(30*time.Second, func() bool {
		for _, item := range proxy.recorded() {
			if !bytes.Contains(item.request, []byte(`"tools"`)) {
				return true
			}
		}

		return false
	})
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	type report struct {
		update acp.SessionUsageUpdate
		call   wire.CallUsage
	}

	var reports []report

	for _, notification := range h.rec.snapshot() {
		update := notification.Update.UsageUpdate
		if update == nil {
			continue
		}

		member, ok := update.Meta[wire.CallUsageKey]
		require.True(t, ok, "with call reports, every usage update reports one call")

		data, err := json.Marshal(member)
		require.NoError(t, err)

		var call wire.CallUsage
		require.NoError(t, json.Unmarshal(data, &call))

		reports = append(reports, report{update: *update, call: call})
	}

	// The session's conversation sends its tools with each request; side
	// calls such as title generation send none.
	main := map[string]gatewayResponse{}
	var side []string

	for _, item := range proxy.recorded() {
		var request struct {
			Tools []any `json:"tools"`
		}
		require.NoError(t, json.Unmarshal(item.request, &request))

		parsed := parseGatewayResponse(t, item.response)
		require.NotEmpty(t, parsed.ID)

		if len(request.Tools) == 0 {
			side = append(side, parsed.ID)

			continue
		}

		main[parsed.ID] = parsed
	}

	require.GreaterOrEqual(t, len(main), 2, "the tool turn makes at least two main calls")
	require.Len(t, reports, len(main), "each main call reports once")

	seen := map[string]bool{}

	for _, item := range reports {
		require.NotEmpty(t, item.call.ResponseID)
		require.False(t, seen[item.call.ResponseID], "a response reports once")
		seen[item.call.ResponseID] = true

		gateway, ok := main[item.call.ResponseID]
		require.True(t, ok, "%s is a main call the gateway answered", item.call.ResponseID)

		prompt := counter(gateway.Usage, "prompt_tokens")
		cached := counter(gateway.Usage, "prompt_tokens_details", "cached_tokens")
		written := counter(gateway.Usage, "prompt_tokens_details", "cache_write_tokens")
		require.NotNil(t, prompt)
		require.NotNil(t, cached)

		uncached := *prompt - *cached
		if written != nil {
			uncached -= *written
		}

		require.Equal(t, wire.CallUsage{
			ResponseID: gateway.ID, InputTokens: new(uncached), CachedReadTokens: cached, CachedWriteTokens: written,
			OutputTokens: counter(gateway.Usage, "completion_tokens"),
		}, item.call)
		require.Equal(t, *prompt, item.update.Used, "the context is the prompt the call sent")
		require.Positive(t, item.update.Size)
	}

	for _, id := range side {
		require.False(t, seen[id], "side call %s reports nothing", id)
	}

	t.Logf("main calls reported: %d; side calls not reported: %d", len(main), len(side))
}
