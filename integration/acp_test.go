//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

type authorizedMCPProbe struct {
	mu      sync.Mutex
	armed   bool
	lists   [][]string
	execute []map[string]any
}

func (p *authorizedMCPProbe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)

		return
	}

	var request struct {
		JSONRPC string         `json:"jsonrpc"`
		ID      any            `json:"id"`
		Method  string         `json:"method"`
		Params  map[string]any `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	if request.ID == nil {
		w.WriteHeader(http.StatusAccepted)

		return
	}

	result := map[string]any{}
	switch request.Method {
	case "initialize":
		result = map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "authorized-probe", "version": "1.0.0"},
		}
	case "ping":
	case "tools/list":
		p.mu.Lock()
		armed := p.armed
		names := []string{"runtime_ready"}
		if armed {
			names = []string{"runtime_ready", "execute", "search"}
		}
		p.lists = append(p.lists, append([]string(nil), names...))
		p.mu.Unlock()

		tools := make([]map[string]any, 0, len(names))
		for _, name := range names {
			tools = append(tools, map[string]any{
				"name":        name,
				"description": "Deterministic authorization probe " + name,
				"inputSchema": map[string]any{"type": "object", "additionalProperties": true},
			})
		}
		result = map[string]any{"tools": tools}
	case "tools/call":
		name, _ := request.Params["name"].(string)
		arguments, _ := request.Params["arguments"].(map[string]any)
		p.mu.Lock()
		armed := p.armed
		if name == "execute" && armed {
			p.execute = append(p.execute, arguments)
		}
		p.mu.Unlock()
		if name != "execute" || !armed {
			result = map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": "not authorized"}}}
		} else {
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": "AUTHORIZED_EXECUTE_OK"}}}
		}
	default:
		writeMCPResponse(w, request.ID, nil, fmt.Sprintf("unsupported method %s", request.Method))

		return
	}

	writeMCPResponse(w, request.ID, result, "")
}

func writeMCPResponse(w http.ResponseWriter, id any, result any, message string) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]any{"jsonrpc": "2.0", "id": id}
	if message != "" {
		response["error"] = map[string]any{"code": -32601, "message": message}
	} else {
		response["result"] = result
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (p *authorizedMCPProbe) arm() {
	p.mu.Lock()
	p.armed = true
	p.mu.Unlock()
}

func (p *authorizedMCPProbe) snapshot() ([][]string, []map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()

	lists := make([][]string, len(p.lists))
	for index := range p.lists {
		lists[index] = append([]string(nil), p.lists[index]...)
	}

	return lists, append([]map[string]any(nil), p.execute...)
}

func TestHermesACPAgentLiveCompletionText(t *testing.T) {
	requireRunLiveTokens(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	args := []string{}
	if model := os.Getenv("ACP_GO_HERMES_MODEL"); model != "" {
		args = append(args, "-model", model)
	}
	agent := startLiveAgent(t, ctx, t.TempDir(), args...)
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	const sentinel = "ACP_HERMES_COMPLETION_TEXT_OK"
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-completion-text", "Reply with exactly "+sentinel+".")); err != nil {
		t.Fatalf("prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if got := client.agentText(); !strings.Contains(got, sentinel) {
		t.Fatalf("agent text = %q, want sentinel %q\nstderr:\n%s", got, sentinel, agent.stderrString())
	}
}

func TestHermesACPAgentLiveAuthorizedMCPReload(t *testing.T) {
	requireRunLiveTokens(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	probe := &authorizedMCPProbe{}
	mcpServer := httptest.NewServer(probe)
	defer mcpServer.Close()

	args := []string{}
	if model := os.Getenv("ACP_GO_HERMES_MODEL"); model != "" {
		args = append(args, "-model", model)
	}
	agent := startLiveAgent(t, ctx, t.TempDir(), args...)
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(
		t.TempDir(),
		hermesacp.WithSessionMCPServers(hermesacp.HTTPMCPServer("wagie", mcpServer.URL, nil)),
	))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	listsBeforeArm, _ := probe.snapshot()
	if len(listsBeforeArm) == 0 || len(listsBeforeArm[0]) != 1 || listsBeforeArm[0][0] != "runtime_ready" {
		t.Fatalf("provisional MCP discovery = %#v, want runtime_ready only\nstderr:\n%s", listsBeforeArm, agent.stderrString())
	}
	probe.arm()

	const sentinel = "HERMES_AUTHORIZED_MCP_RELOAD_OK"
	prompt := "Call mcp__wagie__execute exactly once with the JSON argument {\"probe\":\"authorized\"}. After its result, reply with exactly " + sentinel + "."
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-authorized-mcp", prompt)); err != nil {
		t.Fatalf("prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}

	lists, execute := probe.snapshot()
	if len(lists) < 2 || !containsExactStrings(lists[len(lists)-1], "runtime_ready", "execute", "search") {
		t.Fatalf("authorized MCP discovery = %#v, want execute/search after reload\nstderr:\n%s", lists, agent.stderrString())
	}
	if len(execute) != 1 || execute[0]["probe"] != "authorized" {
		t.Fatalf("execute calls = %#v, want exact authorized call\nstderr:\n%s", execute, agent.stderrString())
	}
	if got := client.agentText(); !strings.Contains(got, sentinel) {
		t.Fatalf("agent text = %q, want sentinel %q\nstderr:\n%s", got, sentinel, agent.stderrString())
	}
}

func containsExactStrings(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}

	return true
}

func TestHermesACPAgentBinarySessionLifecycle(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()

	home := t.TempDir()
	agent := startLiveAgent(t, ctx, home)
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	initResp, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if initResp.AgentCapabilities.SessionCapabilities.Fork != nil {
		t.Fatalf("stable fork advertised: %#v", initResp.AgentCapabilities.SessionCapabilities.Fork)
	}
	// Fork is exposed only through the namespaced extension method; the stable
	// ACP session/fork route must be method-not-found (-32601) on the wire.
	_, forkErr := conn.UnstableForkSession(ctx, acp.UnstableForkSessionRequest{})
	if forkErr == nil {
		t.Fatal("stable session/fork unexpectedly succeeded")
	}
	var forkReqErr *acp.RequestError
	if !errors.As(forkErr, &forkReqErr) || forkReqErr.Code != -32601 {
		t.Fatalf("stable session/fork error = %#v, want method-not-found", forkErr)
	}

	cwd := t.TempDir()
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}
	listResp, err := conn.ListSessions(ctx, hermesacp.ListSessionsRequest(hermesacp.WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("list sessions: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if len(listResp.Sessions) == 0 {
		t.Fatal("session/list returned no sessions")
	}
	if _, err := conn.UnstableDeleteSession(ctx, hermesacp.DeleteSessionRequest(session.SessionId)); err != nil {
		t.Fatalf("delete session: %v\nstderr:\n%s", err, agent.stderrString())
	}
}

func TestHermesACPAgentLivePromptPermissionElicitation(t *testing.T) {
	requireRunLiveTokens(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	home := t.TempDir()
	args := []string{}
	if model := os.Getenv("ACP_GO_HERMES_MODEL"); model != "" {
		args = append(args, "-model", model)
	}
	agent := startLiveAgent(t, ctx, home, args...)
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		},
	}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}
	cwd := t.TempDir()
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	permissionPrompt := envOrDefault("ACP_GO_HERMES_PERMISSION_PROMPT", "Create a file named acp-permission-probe.txt in the working directory, then stop.")
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-permission", permissionPrompt)); err != nil {
		t.Fatalf("permission prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if client.permissionCount() == 0 {
		t.Skipf("native Hermes prompt did not emit approval.request in this environment; stderr:\n%s", agent.stderrString())
	}

	questionPrompt := envOrDefault("ACP_GO_HERMES_QUESTION_PROMPT", `Use the question tool to ask the user "Continue?" with options "Yes" and "No", then stop after receiving the answer.`)
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-question", questionPrompt)); err != nil {
		t.Fatalf("question prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if client.elicitationCount() == 0 {
		t.Skipf("native Hermes prompt did not emit clarify.request in this environment; stderr:\n%s", agent.stderrString())
	}
}
