//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
	hermesacp "github.com/savid/acp-go-hermes"
)

const (
	envFakeHermesHelper        = "ACP_GO_HERMES_FAKE_HELPER"
	envFakeHermesMode          = "ACP_GO_HERMES_FAKE_MODE"
	envFakeHermesDescendantPID = "ACP_GO_HERMES_FAKE_DESCENDANT_PID_FILE"
	fakeModeOK                 = "ok"
	fakeModeStatusOnly         = "status-only"
	fakeModeDetachedDescendant = "detached-descendant"
	fakeStoredSessionKey       = "stored-fake"
)

func TestHermesACPAgentFakeExecutableStdoutNoise(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeOK), t.TempDir())
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize with fake hermes: %v\nstderr:\n%s", err, agent.stderrString())
	}
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session with stdout-noisy fake hermes: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if session.SessionId == "" {
		t.Fatalf("empty fake session response: %#v", session)
	}
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-complete-only", "reply")); err != nil {
		t.Fatalf("completion-only prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	deadline := time.Now().Add(time.Second)
	for client.agentText() != "fake response" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := client.agentText(); got != "fake response" {
		t.Fatalf("completion-only ACP text = %q, want %q\nstderr:\n%s", got, "fake response", agent.stderrString())
	}
	assertFakeGatewayToolLifecycle(t, client)
	fork, err := hermesacp.CallForkSession(ctx, conn, hermesacp.ForkSessionRequest(session.SessionId, t.TempDir()))
	if err != nil {
		t.Fatalf("extension fork through fake gateway: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if fork.SessionId == "" || fork.SessionId == session.SessionId {
		t.Fatalf("fake fork response = %#v", fork)
	}
}

func assertFakeGatewayToolLifecycle(t *testing.T, client *recordingClient) {
	t.Helper()

	client.mu.Lock()
	updates := append([]acp.SessionNotification(nil), client.updates...)
	client.mu.Unlock()

	var starts []*acp.SessionUpdateToolCall
	var completions []*acp.SessionToolCallUpdate
	for index := range updates {
		if start := updates[index].Update.ToolCall; start != nil {
			starts = append(starts, start)
		}
		if update := updates[index].Update.ToolCallUpdate; update != nil {
			completions = append(completions, update)
		}
	}
	if len(starts) != 1 || len(completions) != 1 {
		t.Fatalf("gateway ACP tool lifecycle starts=%#v completions=%#v", starts, completions)
	}
	if starts[0].ToolCallId != "native-tool-1" || starts[0].Title != "terminal" ||
		starts[0].Kind != acp.ToolKindExecute || starts[0].Status != acp.ToolCallStatusInProgress {
		t.Fatalf("gateway ACP tool start = %#v", starts[0])
	}
	if input, _ := starts[0].RawInput.(map[string]any); input["context"] != "mcp__wagie__execute" {
		t.Fatalf("gateway ACP tool input = %#v", starts[0].RawInput)
	}
	if completions[0].ToolCallId != "native-tool-1" || completions[0].Status == nil ||
		*completions[0].Status != acp.ToolCallStatusCompleted {
		t.Fatalf("gateway ACP tool completion = %#v", completions[0])
	}
	if input, _ := completions[0].RawInput.(map[string]any); input["command"] != "mcp__wagie__execute" || input["context"] != nil {
		t.Fatalf("gateway ACP authoritative completion input = %#v", completions[0].RawInput)
	}
	if output, _ := completions[0].RawOutput.(map[string]any); output["probe"] != "authorized" || output["status"] != "ok" {
		t.Fatalf("gateway ACP tool output = %#v", completions[0].RawOutput)
	}
}

func TestHermesACPAgentFakeExecutableLeaseRecoveryIsSessionScoped(t *testing.T) {
	requireRunIntegration(t)
	if runtime.GOOS == "windows" {
		t.Skip("lease reaper signal semantics are platform-specific")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	home := t.TempDir()
	orphan := exec.CommandContext(ctx, "sleep", "30")
	if err := orphan.Start(); err != nil {
		t.Fatalf("start orphan process: %v", err)
	}
	waitOrphan := make(chan error, 1)
	go func() { waitOrphan <- orphan.Wait() }()
	t.Cleanup(func() {
		select {
		case <-waitOrphan:
			return
		default:
		}
		_ = orphan.Process.Kill()
		<-waitOrphan
	})

	leaseDir := filepath.Join(home, "acp-go-hermes", "orphan", "state")
	if err := os.MkdirAll(leaseDir, 0o700); err != nil {
		t.Fatalf("mkdir lease dir: %v", err)
	}
	lease := map[string]any{"pid": orphan.Process.Pid, "port": 0, "startedAtUnixMilli": time.Now().UnixMilli(), "tokenHash": "test"}
	data, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(leaseDir, "server.lease"), data, 0o600); err != nil {
		t.Fatalf("write lease: %v", err)
	}

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeOK), home)
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if _, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir())); err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	leasePath := filepath.Join(leaseDir, "server.lease")
	if _, err := os.Stat(leasePath); err != nil {
		t.Fatalf("unrelated session lease was changed: %v", err)
	}
	select {
	case err := <-waitOrphan:
		t.Fatalf("unrelated session process exited: %v", err)
	default:
	}
}

func TestHermesACPAgentFakeExecutableGatewayFailClosed(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeStatusOnly), t.TempDir())
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	_, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err == nil {
		t.Fatalf("new session with missing websocket unexpectedly succeeded\nstderr:\n%s", agent.stderrString())
	}
}

func TestFakeHermesExecutable(t *testing.T) {
	if os.Getenv(envFakeHermesHelper) != "1" {
		return
	}
	if err := runFakeHermesServer(os.Args, os.Getenv(envFakeHermesMode)); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func startAgentWithHermesPath(t *testing.T, ctx context.Context, hermesPath string, home string) *liveAgent {
	t.Helper()
	cmd := agentCommand(ctx, integrationAgentArgs(hermesPath, home)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	agent := &liveAgent{stdin: stdin, stdout: stdout, wait: cmd.Wait}
	cmd.Stderr = &agent.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	agent.close = func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	return agent
}

func fakeHermesExecutable(t *testing.T, mode string) string {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fake-hermes")
	script := fmt.Sprintf(`#!/bin/sh
%s=1 %s=%s exec %q -test.run '^TestFakeHermesExecutable$' -- "$@"
`, envFakeHermesHelper, envFakeHermesMode, mode, testBinary)
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake hermes executable: %v", err)
	}
	return path
}

func runFakeHermesServer(args []string, mode string) error {
	for _, arg := range args {
		if arg == "--version" {
			_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.19.0 (fake)")
			_, _ = fmt.Fprintln(os.Stdout, "Runtime capabilities: provider-auth-home-v1")
			return nil
		}
	}
	port := ""
	for i, arg := range args {
		if arg == "--port" && i+1 < len(args) {
			port = args[i+1]
			break
		}
	}
	if port == "" {
		return fmt.Errorf("fake hermes missing --port in args %q", strings.Join(args, " "))
	}
	if mode == "" {
		mode = fakeModeOK
	}

	_, _ = fmt.Fprintln(os.Stdout, "native stdout noise before websocket readiness")
	state := &fakeGatewayState{}
	handler := http.NewServeMux()
	handler.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	if mode != fakeModeStatusOnly {
		handler.HandleFunc("/api/ws", func(w http.ResponseWriter, r *http.Request) {
			handleFakeGatewayWS(w, r, mode, state)
		})
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	return server.ListenAndServe()
}

type fakeGatewayState struct {
	mu       sync.Mutex
	messages []map[string]any
}

func (s *fakeGatewayState) recordAssistant(text string) {
	s.mu.Lock()
	s.messages = append(s.messages, map[string]any{
		"role":    "assistant",
		"content": map[string]any{"text": text},
	})
	s.mu.Unlock()
}

func (s *fakeGatewayState) history() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]map[string]any(nil), s.messages...)
}

func handleFakeGatewayWS(w http.ResponseWriter, r *http.Request, mode string, state *fakeGatewayState) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")
	writeFakeGatewayEvent(r.Context(), conn, "gateway.ready", "", nil)
	for {
		typ, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var req struct {
			ID     int64           `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(data, &req); err != nil {
			return
		}
		params := map[string]any{}
		_ = json.Unmarshal(req.Params, &params)
		handleFakeGatewayRPC(r.Context(), conn, req.ID, req.Method, params, mode, state)
	}
}

func handleFakeGatewayRPC(
	ctx context.Context,
	conn *websocket.Conn,
	id int64,
	method string,
	params map[string]any,
	mode string,
	state *fakeGatewayState,
) {
	switch method {
	case "session.create":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"session_id":        "live-fake",
			"stored_session_id": fakeStoredSessionKey,
		})
	case "session.resume":
		stored, _ := params["session_id"].(string)
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"session_id":  "live-" + stored,
			"session_key": stored,
		})
	case "session.title":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"pending": false,
			"title":   params["title"],
		})
	case "session.active_list":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"sessions": []map[string]any{
			{
				"id":          "live-fake",
				"session_key": fakeStoredSessionKey,
				"title":       "Fake",
				"cwd":         params["cwd"],
			},
			{
				"id":          "live-branch",
				"session_key": "stored-branch",
				"title":       "Branch",
				"cwd":         params["cwd"],
			},
		}})
	case "session.history":
		messages := state.history()
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"count": len(messages), "messages": messages})
	case "session.branch":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"session_id": "live-branch",
			"title":      "Branch",
			"parent":     fakeStoredSessionKey,
		})
	case "model.options":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"model":    "anthropic/claude-sonnet-4",
			"provider": "",
			"providers": []map[string]any{{
				"slug":            "openrouter",
				"name":            "OpenRouter",
				"authenticated":   true,
				"is_current":      false,
				"is_user_defined": false,
				"models":          []string{"openai/gpt-test"},
				"capabilities":    map[string]any{"openai/gpt-test": map[string]any{"fast": true, "reasoning": true}},
				"source":          "built-in",
				"total_models":    1,
			}},
		})
	case "image.attach_bytes":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"attached": true})
	case "prompt.submit":
		live, _ := params["session_id"].(string)
		if strings.HasPrefix(live, "__acp_go_hermes_missing_probe__") {
			writeFakeGatewayError(ctx, conn, id, 4001, "session not found")
			return
		}
		if mode == fakeModeDetachedDescendant {
			pidFile := os.Getenv(envFakeHermesDescendantPID)
			if _, err := os.Stat(pidFile); errors.Is(err, os.ErrNotExist) {
				writeFakeGatewayResult(ctx, conn, id, map[string]any{})
				spawnFakeDetachedDescendant(pidFile)

				return
			}
		}
		writeFakeGatewayResult(ctx, conn, id, map[string]any{})
		writeFakeGatewayEvent(ctx, conn, "tool.start", live, map[string]any{
			"tool_id": "native-tool-1",
			"name":    "terminal",
			"context": "mcp__wagie__execute",
		})
		writeFakeGatewayEvent(ctx, conn, "tool.complete", live, map[string]any{
			"tool_id": "native-tool-1",
			"name":    "terminal",
			"args":    map[string]any{"command": "mcp__wagie__execute"},
			"result":  map[string]any{"probe": "authorized", "status": "ok"},
		})
		state.recordAssistant("fake response")
		// Hermes 0.19.0 may deliver the entire assistant reply only on the
		// authoritative completion event, with no preceding message.delta.
		writeFakeGatewayEvent(ctx, conn, "message.complete", live, map[string]any{
			"text":  "fake response",
			"usage": map[string]any{"total_tokens": 1},
		})
	case "session.delete", "session.close", "session.interrupt",
		"approval.respond", "clarify.respond", "terminal.read.respond", "sudo.respond", "secret.respond":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{})
	default:
		writeFakeGatewayError(ctx, conn, id, -32601, "missing")
	}
}

func writeFakeGatewayResult(ctx context.Context, conn *websocket.Conn, id int64, result any) {
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	_ = conn.Write(ctx, websocket.MessageText, data)
}

func writeFakeGatewayError(ctx context.Context, conn *websocket.Conn, id int64, code int, message string) {
	data, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": code, "message": message},
	})
	_ = conn.Write(ctx, websocket.MessageText, data)
}

func writeFakeGatewayEvent(ctx context.Context, conn *websocket.Conn, eventType string, sessionID string, payload any) {
	params := map[string]any{"type": eventType}
	if sessionID != "" {
		params["session_id"] = sessionID
	}
	if payload != nil {
		data, _ := json.Marshal(payload)
		params["payload"] = json.RawMessage(data)
	}
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": params})
	_ = conn.Write(ctx, websocket.MessageText, data)
}

func spawnFakeDetachedDescendant(pidFile string) {
	if pidFile == "" {
		return
	}

	cmd := exec.Command("setsid", "sh", "-c", `trap "" TERM; echo $$ > "$1"; while :; do sleep 30; done`, "fake-detached", pidFile)
	_ = cmd.Start()
}
