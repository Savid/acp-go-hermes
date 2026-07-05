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
	hermesacp "github.com/savid/acp-go-hermes"
)

const (
	envFakeHermesHelper = "ACP_GO_HERMES_FAKE_HELPER"
	envFakeHermesMode   = "ACP_GO_HERMES_FAKE_MODE"
	fakeModeOK          = "ok"
	fakeModeMissingDoc  = "missing-doc"
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
	lease := map[string]any{"pid": orphan.Process.Pid, "port": 0, "startedAtUnixMilli": time.Now().UnixMilli(), "passwordHash": "test"}
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

func TestHermesACPAgentFakeExecutablePermissionDocFailClosed(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeMissingDoc), t.TempDir())
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	_, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "/api/session/{sessionID}/permission/{requestID}/reply") {
		t.Fatalf("new session with missing permission /doc path err = %v\nstderr:\n%s", err, agent.stderrString())
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

	_, _ = fmt.Fprintln(os.Stdout, "native stdout noise before HTTP readiness")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/global/health":
			writeFakeJSON(w, map[string]any{"healthy": true, "version": "9.0.0"})
		case r.URL.Path == "/doc":
			writeFakeJSON(w, fakeHermesDoc(mode))
		case r.URL.Path == "/event":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: {\"type\":\"server.connected\",\"properties\":{}}\n\n")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		case r.URL.Path == "/session" && r.Method == http.MethodPost:
			writeFakeJSON(w, fakeNativeSession("native-fake"))
		case r.URL.Path == "/session/native-fake" && r.Method == http.MethodGet:
			writeFakeJSON(w, fakeNativeSession("native-fake"))
		case r.URL.Path == "/session/native-fake/todo" && r.Method == http.MethodGet:
			writeFakeJSON(w, []any{})
		case r.URL.Path == "/session/native-fake/abort" && r.Method == http.MethodPost:
			writeFakeJSON(w, map[string]any{"ok": true})
		case r.URL.Path == "/config/providers":
			writeFakeJSON(w, map[string]any{"providers": []map[string]any{{
				"id":   "openai",
				"name": "OpenAI",
				"models": map[string]any{
					"gpt-test": map[string]any{"id": "gpt-test", "name": "GPT Test"},
				},
			}}})
		case r.URL.Path == "/command" && r.Method == http.MethodGet:
			writeFakeJSON(w, []any{})
		case r.URL.Path == "/agent":
			writeFakeJSON(w, []map[string]any{{"name": "build", "description": "Build"}})
		case r.URL.Path == "/permission" && r.Method == http.MethodGet:
			writeFakeJSON(w, []any{})
		case r.URL.Path == "/question" && r.Method == http.MethodGet:
			writeFakeJSON(w, []any{})
		case r.URL.Path == "/api/permission/request" && r.Method == http.MethodGet:
			writeFakeJSON(w, map[string]any{"location": map[string]any{}, "data": []any{}})
		case r.URL.Path == "/api/question/request" && r.Method == http.MethodGet:
			writeFakeJSON(w, map[string]any{"location": map[string]any{}, "data": []any{}})
		default:
			http.NotFound(w, r)
		}
	})
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	return server.ListenAndServe()
}

func fakeHermesDoc(mode string) map[string]any {
	required := []string{
		"/command",
		"/config/providers",
		"/event",
		"/session/status",
		"/session",
		"/session/{sessionID}",
		"/session/{sessionID}/command",
		"/session/{sessionID}/message",
		"/session/{sessionID}/abort",
		"/session/{sessionID}/fork",
		"/session/{sessionID}/todo",
		"/session/{sessionID}/revert",
		"/session/{sessionID}/unrevert",
		"/permission",
		"/permission/{requestID}/reply",
		"/question",
		"/question/{requestID}/reply",
		"/question/{requestID}/reject",
		"/api/session/{sessionID}/agent",
		"/api/session/{sessionID}/message",
		"/api/session/{sessionID}/model",
		"/api/session/{sessionID}/permission/{requestID}/reply",
		"/api/permission/request",
		"/api/session/{sessionID}/question/{requestID}/reply",
		"/api/session/{sessionID}/question/{requestID}/reject",
		"/api/question/request",
	}
	paths := map[string]any{}
	for _, path := range required {
		if mode == fakeModeMissingDoc && path == "/api/session/{sessionID}/permission/{requestID}/reply" {
			continue
		}
		paths[path] = map[string]any{}
	}
	paths["/api/permission/request"] = fakePendingRequestPath("PermissionV2Request")
	paths["/permission"] = fakePendingArrayPath("PermissionRequest")
	if mode != fakeModeMissingDoc {
		paths["/api/session/{sessionID}/permission/{requestID}/reply"] = map[string]any{
			"post": map[string]any{
				"responses": map[string]any{"204": map[string]any{"description": "<No Content>"}},
				"requestBody": map[string]any{
					"required": true,
					"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"reply":   map[string]any{"$ref": "#/components/schemas/PermissionV2Reply"},
							"message": map[string]any{"type": "string"},
						},
						"required": []any{"reply"},
					}}},
				},
			},
		}
	}
	paths["/permission/{requestID}/reply"] = map[string]any{
		"post": map[string]any{
			"responses": map[string]any{"200": map[string]any{"description": "Permission processed"}},
			"requestBody": map[string]any{
				"required": true,
				"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reply":   map[string]any{"type": "string"},
						"message": map[string]any{"type": "string"},
					},
					"required": []any{"reply"},
				}}},
			},
		},
	}
	paths["/api/question/request"] = fakePendingRequestPath("QuestionV2Request")
	paths["/question"] = fakePendingArrayPath("QuestionRequest")
	paths["/api/session/{sessionID}/question/{requestID}/reply"] = map[string]any{
		"post": map[string]any{
			"responses": map[string]any{"204": map[string]any{"description": "<No Content>"}},
			"requestBody": map[string]any{
				"required": true,
				"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{
					"$ref": "#/components/schemas/QuestionV2Reply",
				}}},
			},
		},
	}
	paths["/api/session/{sessionID}/question/{requestID}/reject"] = map[string]any{
		"post": map[string]any{"responses": map[string]any{"204": map[string]any{"description": "<No Content>"}}},
	}
	paths["/question/{requestID}/reply"] = map[string]any{
		"post": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "Question answered"}}},
	}
	paths["/question/{requestID}/reject"] = map[string]any{
		"post": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "Question rejected"}}},
	}
	return map[string]any{
		"paths": paths,
		"components": map[string]any{"schemas": map[string]any{
			"Event": fakeEventUnion(
				"EventPermissionV2Asked",
				"EventPermissionV2Replied",
				"EventPermissionAsked",
				"EventPermissionReplied",
				"EventQuestionV2Asked",
				"EventQuestionV2Replied",
				"EventQuestionAsked",
				"EventQuestionReplied",
				"EventMessagePartUpdated",
				"EventServerConnected",
			),
			"EventPermissionV2Asked": fakeEventSchema("permission.v2.asked", []string{"id", "sessionID", "action", "resources"}),
			"EventPermissionV2Replied": fakeEventSchema("permission.v2.replied", []string{
				"sessionID",
				"requestID",
				"reply",
			}),
			"EventPermissionAsked": fakeEventSchema("permission.asked", []string{"id", "sessionID", "permission", "patterns"}),
			"EventPermissionReplied": fakeEventSchema("permission.replied", []string{
				"sessionID",
				"requestID",
				"reply",
			}),
			"EventQuestionV2Asked":    fakeEventSchema("question.v2.asked", []string{"id", "sessionID", "questions"}),
			"EventQuestionV2Replied":  fakeEventSchema("question.v2.replied", []string{"sessionID", "requestID", "answers"}),
			"EventQuestionAsked":      fakeEventSchema("question.asked", []string{"id", "sessionID", "questions"}),
			"EventQuestionReplied":    fakeEventSchema("question.replied", []string{"sessionID", "requestID", "answers"}),
			"EventMessagePartUpdated": fakeEventSchema("message.part.updated", []string{"sessionID", "part", "time"}),
			"EventServerConnected":    fakeEventSchema("server.connected", nil),
			"QuestionV2Reply": map[string]any{
				"type":       "object",
				"properties": map[string]any{"answers": map[string]any{"type": "array"}},
				"required":   []any{"answers"},
			},
		}},
	}
}

func fakeEventUnion(names ...string) map[string]any {
	refs := make([]any, 0, len(names))
	for _, name := range names {
		refs = append(refs, map[string]any{"$ref": "#/components/schemas/" + name})
	}
	return map[string]any{"anyOf": refs}
}

func fakeEventSchema(eventType string, required []string) map[string]any {
	properties := map[string]any{}
	for _, property := range required {
		properties[property] = map[string]any{"type": "string"}
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":   map[string]any{"type": "string"},
			"type": map[string]any{"type": "string", "enum": []string{eventType}},
			"properties": map[string]any{
				"type":                 "object",
				"properties":           properties,
				"required":             required,
				"additionalProperties": false,
			},
		},
		"required":             []string{"id", "type", "properties"},
		"additionalProperties": false,
	}
}

func fakePendingRequestPath(itemRef string) map[string]any {
	return map[string]any{
		"get": map[string]any{"responses": map[string]any{"200": map[string]any{
			"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{
				"type": "object",
				"properties": map[string]any{"data": map[string]any{
					"type":  "array",
					"items": map[string]any{"$ref": "#/components/schemas/" + itemRef},
				}},
			}}},
		}}},
	}
}

func fakePendingArrayPath(itemRef string) map[string]any {
	return map[string]any{
		"get": map[string]any{"responses": map[string]any{"200": map[string]any{
			"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{
				"type":  "array",
				"items": map[string]any{"$ref": "#/components/schemas/" + itemRef},
			}}},
		}}},
	}
}

func fakeNativeSession(id string) map[string]any {
	return map[string]any{
		"id":    id,
		"title": "Fake",
		"agent": "build",
		"model": map[string]any{
			"providerID": "openai",
			"modelID":    "gpt-test",
		},
		"time": map[string]any{"updated": time.Now().UnixMilli()},
	}
}

func writeFakeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
