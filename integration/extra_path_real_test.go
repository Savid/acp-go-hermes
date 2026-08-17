//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

// TestHermesRealExtraPathDirsSurviveNativeLoginSnapshot proves the real
// token-free Hermes terminal boundary, not just the serve-process environment.
// Two concurrent sessions resolve different carrier-probe executables. Closing
// and resuming the first session binds a replacement directory, whose command
// rejects both its stale predecessor and the live peer anywhere in PATH.
func TestHermesRealExtraPathDirsSurviveNativeLoginSnapshot(t *testing.T) {
	requireRunIntegration(t)
	if runtime.GOOS == "windows" {
		t.Skip("the real proof drives Hermes' native POSIX terminal backend")
	}

	model := newHermesPathModelHarness(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	config := fmt.Sprintf(`model:
  provider: custom
  default: acp-path-model
  base_url: %s/v1
  api_key: local-test-key
  context_length: 65536
  max_tokens: 1024
approvals:
  mode: manual
`, model.server.URL)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	sharedHome := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	agent := startLiveAgent(t, ctx, t.TempDir(), "-shared-hermes-home", sharedHome, "-seed-file", "config.yaml="+configPath)
	defer agent.close()
	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}

	cwd := t.TempDir()
	first := newHermesPathCarrier(t, "HERMES_PATH_FIRST")
	peer := newHermesPathCarrier(t, "HERMES_PATH_PEER")
	rebound := newHermesPathCarrier(t, "HERMES_PATH_REBOUND")
	writeHermesPathProbe(t, first, peer.dir, rebound.dir)
	writeHermesPathProbe(t, peer, first.dir, rebound.dir)
	writeHermesPathProbe(t, rebound, first.dir, peer.dir)

	newSession := func(carrier hermesPathCarrier) acp.SessionId {
		t.Helper()
		response, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd,
			hermesacp.WithSessionHermesOptions(hermesacp.HermesOptions{
				ExtraPathDirs: []string{carrier.dir},
			}),
		))
		if err != nil {
			t.Fatalf("new %s session: %v\nstderr:\n%s", carrier.marker, err, agent.stderrString())
		}

		return response.SessionId
	}
	firstSession := newSession(first)
	peerSession := newSession(peer)

	type promptResult struct {
		marker string
		err    error
	}
	results := make(chan promptResult, 2)
	for _, turn := range []struct {
		session acp.SessionId
		carrier hermesPathCarrier
	}{
		{session: firstSession, carrier: first},
		{session: peerSession, carrier: peer},
	} {
		turn := turn
		go func() {
			response, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(
				turn.session, "path-"+turn.carrier.marker, "Run the requested terminal probe.",
			))
			if err == nil && response.StopReason != acp.StopReasonEndTurn {
				err = fmt.Errorf("stop reason = %q", response.StopReason)
			}
			results <- promptResult{marker: turn.carrier.marker, err: err}
		}()
	}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("prompt %s: %v\nstderr:\n%s", result.marker, result.err, agent.stderrString())
		}
	}
	assertHermesPathCapture(t, first, client, agent)
	assertHermesPathCapture(t, peer, client, agent)

	if _, err := conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: firstSession}); err != nil {
		t.Fatalf("close first session: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if _, err := conn.ResumeSession(ctx, hermesacp.ResumeSessionRequest(
		firstSession,
		cwd,
		hermesacp.WithSessionHermesOptions(hermesacp.HermesOptions{
			ExtraPathDirs: []string{rebound.dir},
		}),
	)); err != nil {
		t.Fatalf("resume rebound session: %v\nstderr:\n%s", err, agent.stderrString())
	}
	for _, turn := range []struct {
		session acp.SessionId
		carrier hermesPathCarrier
	}{
		{session: firstSession, carrier: rebound},
		{session: peerSession, carrier: peer},
	} {
		response, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(
			turn.session, "path-again-"+turn.carrier.marker, "Run the requested terminal probe.",
		))
		if err != nil || response.StopReason != acp.StopReasonEndTurn {
			t.Fatalf("prompt %s after rebind: response=%#v err=%v\nstderr:\n%s",
				turn.carrier.marker, response, err, agent.stderrString())
		}
	}
	assertHermesPathCapture(t, rebound, client, agent)
	assertHermesPathCapture(t, peer, client, agent)
	if calls := model.toolCalls.Load(); calls != 4 {
		t.Fatalf("native model tool calls = %d, want 4", calls)
	}
	sharedConfig, err := os.ReadFile(filepath.Join(sharedHome, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(sharedConfig) != config {
		t.Fatalf("shared Hermes config mutated: got %q, want %q", sharedConfig, config)
	}
}

type hermesPathCarrier struct {
	dir     string
	capture string
	marker  string
}

func newHermesPathCarrier(t *testing.T, marker string) hermesPathCarrier {
	t.Helper()

	return hermesPathCarrier{
		dir:     t.TempDir(),
		capture: filepath.Join(t.TempDir(), "capture"),
		marker:  marker,
	}
}

func writeHermesPathProbe(t *testing.T, carrier hermesPathCarrier, forbidden ...string) {
	t.Helper()
	command := "#!/bin/sh\n" +
		"printf '%s\\n%s\\n' " + shellSingleQuoteHermesPath(carrier.marker) + " \"$PATH\" > " + shellSingleQuoteHermesPath(carrier.capture) + "\n" +
		"test \"${PATH%%:*}\" = " + shellSingleQuoteHermesPath(carrier.dir) + " || exit 91\n" +
		"test \"$(command -v carrier-probe)\" = " + shellSingleQuoteHermesPath(filepath.Join(carrier.dir, "carrier-probe")) + " || exit 92\n" +
		"path_list=\":${PATH}:\"\n"
	for _, dir := range forbidden {
		command += "case \"${path_list}\" in *" + shellSingleQuoteHermesPath(":"+dir+":") + "*) exit 93 ;; esac\n"
	}
	if err := os.WriteFile(filepath.Join(carrier.dir, "carrier-probe"), []byte(command), 0o700); err != nil {
		t.Fatal(err)
	}
}

func assertHermesPathCapture(t *testing.T, carrier hermesPathCarrier, client *recordingClient, agent *liveAgent) {
	t.Helper()
	data, err := os.ReadFile(carrier.capture)
	if err != nil {
		client.mu.Lock()
		rawUpdates, _ := json.MarshalIndent(client.updates, "", "  ")
		client.mu.Unlock()
		t.Fatalf("read %s capture: %v\nupdates:\n%s\nraw updates:\n%s\nagent text:\n%s\nstderr:\n%s",
			carrier.marker, err, client.updatesSummary(), rawUpdates, client.agentText(), agent.stderrString())
	}
	lines := strings.SplitN(strings.TrimSuffix(string(data), "\n"), "\n", 2)
	if len(lines) != 2 || lines[0] != carrier.marker {
		t.Fatalf("%s capture = %q", carrier.marker, data)
	}
	entries := strings.Split(lines[1], string(os.PathListSeparator))
	if len(entries) == 0 || entries[0] != carrier.dir {
		t.Fatalf("%s PATH = %#v", carrier.marker, entries)
	}
	count := 0
	for _, entry := range entries {
		if entry == carrier.dir {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("%s PATH contains its operation directory %d times: %#v", carrier.marker, count, entries)
	}
}

func shellSingleQuoteHermesPath(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

type hermesPathModelHarness struct {
	server    *httptest.Server
	toolCalls atomic.Int64
}

func newHermesPathModelHarness(t *testing.T) *hermesPathModelHarness {
	t.Helper()
	harness := &hermesPathModelHarness{}
	harness.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"object":"list","data":[{"id":"acp-path-model","object":"model","owned_by":"integration"}]}`)
		case "/v1/chat/completions":
			harness.handleCompletion(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(harness.server.Close)

	return harness
}

func (h *hermesPathModelHarness) handleCompletion(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Messages []struct {
			Role string `json:"role"`
		} `json:"messages"`
	}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	lastRole := ""
	if len(request.Messages) > 0 {
		lastRole = request.Messages[len(request.Messages)-1].Role
	}
	if lastRole != "tool" {
		call := h.toolCalls.Add(1)
		writeHermesPathSSE(w, map[string]any{
			"id": "chatcmpl-path", "object": "chat.completion.chunk", "created": call,
			"model": "acp-path-model", "choices": []any{map[string]any{
				"index": 0, "finish_reason": nil,
				"delta": map[string]any{
					"role": "assistant",
					"tool_calls": []any{map[string]any{
						"index": 0, "id": fmt.Sprintf("terminal-path-%d", call), "type": "function",
						"function": map[string]any{"name": "terminal", "arguments": `{"command":"carrier-probe"}`},
					}},
				},
			}},
		})
		writeHermesPathSSE(w, map[string]any{
			"id": "chatcmpl-path", "object": "chat.completion.chunk", "created": call,
			"model": "acp-path-model", "choices": []any{map[string]any{
				"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls",
			}},
		})
	} else {
		writeHermesPathSSE(w, map[string]any{
			"id": "chatcmpl-path-final", "object": "chat.completion.chunk", "created": 1,
			"model": "acp-path-model", "choices": []any{map[string]any{
				"index": 0, "finish_reason": nil,
				"delta": map[string]any{"role": "assistant", "content": "PATH_OK"},
			}},
		})
		writeHermesPathSSE(w, map[string]any{
			"id": "chatcmpl-path-final", "object": "chat.completion.chunk", "created": 1,
			"model": "acp-path-model", "choices": []any{map[string]any{
				"index": 0, "delta": map[string]any{}, "finish_reason": "stop",
			}},
		})
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
}

func writeHermesPathSSE(w http.ResponseWriter, payload any) {
	data, _ := json.Marshal(payload)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}
