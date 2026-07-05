//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
	hermesacp "github.com/savid/acp-go-hermes"
)

const (
	envFakeHermesHelper  = "ACP_GO_HERMES_FAKE_HELPER"
	envFakeHermesMode    = "ACP_GO_HERMES_FAKE_MODE"
	fakeModeOK           = "ok"
	fakeModeStatusOnly   = "status-only"
	fakeStoredSessionKey = "stored-fake"
)

func TestHermesACPAgentFakeExecutableStdoutNoise(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeOK), t.TempDir())
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
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
	fork, err := hermesacp.CallForkSession(ctx, conn, hermesacp.ForkSessionRequest(session.SessionId, t.TempDir()))
	if err != nil {
		t.Fatalf("extension fork through fake gateway: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if fork.SessionId == "" || fork.SessionId == session.SessionId {
		t.Fatalf("fake fork response = %#v", fork)
	}
}

func TestHermesACPAgentFakeExecutableLeaseReaper(t *testing.T) {
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
		if orphan.ProcessState == nil {
			_ = orphan.Process.Kill()
			<-waitOrphan
		}
	})

	leaseDir := filepath.Join(home, "orphan", "state")
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
	deadline := time.After(5 * time.Second)
	for {
		if _, err := os.Stat(leasePath); os.IsNotExist(err) {
			break
		}
		select {
		case err := <-waitOrphan:
			t.Fatalf("unrelated stale-lease process was killed: %v", err)
		case <-deadline:
			t.Fatalf("stale lease file still present")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	select {
	case err := <-waitOrphan:
		t.Fatalf("unrelated stale-lease process exited: %v", err)
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
	cmd := agentCommand(ctx,
		"-path", hermesPath,
		"-home", home,
	)
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
	handler := http.NewServeMux()
	handler.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	if mode != fakeModeStatusOnly {
		handler.HandleFunc("/api/ws", handleFakeGatewayWS)
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	return server.ListenAndServe()
}

func handleFakeGatewayWS(w http.ResponseWriter, r *http.Request) {
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
		handleFakeGatewayRPC(r.Context(), conn, req.ID, req.Method, params)
	}
}

func handleFakeGatewayRPC(ctx context.Context, conn *websocket.Conn, id int64, method string, params map[string]any) {
	switch method {
	case "session.create":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"session_id":        "live-fake",
			"stored_session_id": fakeStoredSessionKey,
		})
	case "session.resume":
		stored, _ := params["session_id"].(string)
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"session_id":        "live-" + stored,
			"stored_session_id": stored,
		})
	case "session.active_list":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"sessions": []map[string]any{{
			"session_id":  "live-fake",
			"session_key": fakeStoredSessionKey,
			"title":       "Fake",
			"cwd":         params["cwd"],
		}}})
	case "session.history":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"count": 0, "messages": []any{}})
	case "session.branch":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"session_id":        "live-branch",
			"stored_session_id": "stored-branch",
		})
	case "model.options":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"providers": []map[string]any{{
			"id":   "openai",
			"name": "OpenAI",
			"models": []map[string]any{{
				"id":                "gpt-test",
				"name":              "GPT Test",
				"context_window":    128000,
				"max_output_tokens": 4096,
				"capabilities":      []string{"tools"},
			}},
		}}})
	case "prompt.submit":
		live, _ := params["session_id"].(string)
		writeFakeGatewayResult(ctx, conn, id, map[string]any{})
		writeFakeGatewayEvent(ctx, conn, "message.delta", live, map[string]any{"text": "fake response"})
		writeFakeGatewayEvent(ctx, conn, "message.complete", live, map[string]any{"usage": map[string]any{"total_tokens": 1}})
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
