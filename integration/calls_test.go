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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
	hermesacp "github.com/savid/acp-go-hermes"
)

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
	require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte(config), 0o600))

	// A launch that finishes a pending source update in a fresh home rewrites
	// the install's shared launchers to that home's interpreter.
	h := newHarness(t, hermesacp.WithHome(home), hermesacp.WithEnv(map[string]string{
		"OPENROUTER_API_KEY": key, "HERMES_YOLO_MODE": "1", "HERMES_DISABLE_LAZY_INSTALLS": "1",
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
