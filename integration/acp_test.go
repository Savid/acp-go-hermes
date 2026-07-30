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
	"path/filepath"
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
	agent := startLiveTokenAgent(t, ctx, t.TempDir(), args...)
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
	agent := startLiveTokenAgent(t, ctx, t.TempDir(), args...)
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

	// Wire this live turn to the operator's xAI login: seed a config selecting
	// the tool-capable grok-4.5/xai-oauth provider and point native Hermes at its
	// durable auth home. The default openrouter/free router is not reliably
	// tool-capable and never issues the terminal / question tool calls that this
	// test asserts on.
	//
	// Hermes's approval.request flow is architecturally scoped to TERMINAL
	// COMMANDS: the write_file tool is never approval-gated, so a file-write
	// probe can never surface a permission request. The permission probe below
	// therefore drives a terminal command that Hermes classifies as dangerous
	// (`chmod 777 ...` matches the "world/other-writable permissions" rule in
	// tools/approval.py's DANGEROUS_PATTERNS). To make the approval deterministic
	// rather than a gamble on Hermes's default "smart" risk classifier (an
	// auxiliary LLM that auto-approves low-risk commands), the seeded config pins
	// approvals.mode: manual. In manual mode Hermes always routes a
	// dangerous-classified command to the gateway approval callback, which the
	// adapter converts to session/request_permission — the exact surface this
	// test asserts on. Approvals are never auto-granted by this config.
	hermesAuthHome := filepath.Join(os.Getenv("HOME"), ".hermes")
	if _, statErr := os.Stat(filepath.Join(hermesAuthHome, "auth.json")); statErr != nil {
		t.Skipf("ambient Hermes auth not available: %v", statErr)
	}
	xaiConfig := filepath.Join(t.TempDir(), "config.yaml")
	const hermesProbeConfig = "model:\n" +
		"  provider: xai-oauth\n" +
		"  default: grok-4.5\n" +
		"  base_url: https://api.x.ai/v1\n" +
		"  max_tokens: 4096\n" +
		"approvals:\n" +
		"  mode: manual\n"
	if err := os.WriteFile(xaiConfig, []byte(hermesProbeConfig), 0o600); err != nil {
		t.Fatalf("write xai config: %v", err)
	}
	args := []string{
		"-debug",
		"-seed-file", "config.yaml=" + xaiConfig,
		"-provider-auth-root", t.TempDir(),
		"-hermes-provider-auth-home", hermesAuthHome,
	}
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

	permissionPrompt := envOrDefault("ACP_GO_HERMES_PERMISSION_PROMPT", "Your only task this turn is to run a single shell command. As your very first action, and without emitting any explanatory prose, call your terminal tool exactly once to run this exact command verbatim, without modifying, wrapping, or substituting any part of it: chmod 777 acp-permission-probe.txt . Do not ask any clarifying question, do not describe what you are about to do, and do not use any other tool: issue exactly one terminal tool call with that exact command, then stop.")
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-permission", permissionPrompt)); err != nil {
		t.Fatalf("permission prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if client.permissionCount() == 0 {
		t.Fatalf("native Hermes prompt did not emit approval.request; adapter surfaced no permission request for a dangerous terminal command; updates:\n%s\nagentText:\n%s\nstderr:\n%s", client.updatesSummary(), client.agentText(), agent.stderrString())
	}

	questionPrompt := envOrDefault("ACP_GO_HERMES_QUESTION_PROMPT", `Before doing anything else, you MUST use your question tool to ask the user exactly one question: "Continue?" offering the two options "Yes" and "No". Do not answer, explain, or take any other action until you have asked this question through the question tool and received the user's selection. After you receive the answer, stop.`)
	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-question", questionPrompt)); err != nil {
		t.Fatalf("question prompt: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if client.elicitationCount() == 0 {
		t.Fatalf("native Hermes prompt did not emit clarify.request, or the adapter did not convert it to elicitation/create; stderr:\n%s", agent.stderrString())
	}
}
