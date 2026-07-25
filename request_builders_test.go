package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

func TestRequestBuilders(t *testing.T) {
	httpServer := HTTPMCPServer("http", "https://example.com", map[string]string{"X": "Y"})
	stdioServer := StdioMCPServer("stdio", "cmd", []string{"arg"}, map[string]string{"E": "V"})
	sseServer := acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://sse.example"}}
	acpServer := acp.McpServer{Acp: &acp.McpServerAcpInline{Id: "acp-1", Name: "acp"}}
	meta := map[string]any{"foreign": map[string]any{"a": []any{"b"}}}
	req := NewSessionRequest("/tmp/project",
		WithSessionAdditionalDirectories("/tmp/other"),
		WithSessionMCPServers(httpServer, stdioServer, sseServer, acpServer),
		WithSessionMeta(meta),
		WithSessionRawEvents(true),
		WithSessionOutputSchema(map[string]any{"type": "object"}),
		WithSessionHermesOptions(NewHermesOptions(
			WithHermesModel("openai/gpt"),
			WithHermesEnv(map[string]string{"K": "V"}),
		)),
	)
	if req.Cwd != "/tmp/project" || len(req.McpServers) != 4 || len(req.AdditionalDirectories) != 1 {
		t.Fatalf("NewSessionRequest = %#v", req)
	}
	if !rawMessageConfigFromMeta(req.Meta).Enabled() {
		t.Fatalf("raw events not enabled in meta: %#v", req.Meta)
	}
	hermesMeta, _ := req.Meta[hermesMetaKey].(map[string]any)
	options, _ := hermesMeta[metaOptionsKey].(map[string]any)
	envMap, _ := options[metaEnvKey].(map[string]string)
	if options[metaModelKey] != "openai/gpt" || envMap["K"] != "V" {
		t.Fatalf("options not set in meta: %#v", req.Meta)
	}
	if ResumeSessionRequest("s", "/tmp/project", WithSessionMCPServers(httpServer)).SessionId != "s" {
		t.Fatal("ResumeSessionRequest did not set session id")
	}
	if prompt := TextPromptRequest("s", "turn-1", "hello"); prompt.SessionId != "s" || len(prompt.Prompt) != 1 {
		t.Fatalf("TextPromptRequest = %#v", prompt)
	}
	if cancel := CancelRequest("s", "turn-cancel"); cancel.Meta[routeMetaKey] == nil {
		t.Fatalf("CancelRequest = %#v", cancel)
	}
	oversizedNonce := strings.Repeat("n", routeTurnNonceMaxBytes+1)
	if prompt := TextPromptRequest("s", oversizedNonce, "hello"); prompt.Meta != nil {
		t.Fatalf("TextPromptRequest emitted oversized route = %#v", prompt.Meta)
	}
	if cancel := CancelRequest("s", oversizedNonce); cancel.Meta != nil {
		t.Fatalf("CancelRequest emitted oversized route = %#v", cancel.Meta)
	}
	list := ListSessionsRequest(WithListSessionsCursor("next"), WithListSessionsMeta(map[string]any{"a": "b"}))
	if list.Cursor == nil || *list.Cursor != "next" || list.Meta["a"] != "b" {
		t.Fatalf("ListSessionsRequest = %#v", list)
	}
	assertMCPServerConversions(t, req.McpServers)
}

func assertMCPServerConversions(t *testing.T, servers []acp.McpServer) {
	t.Helper()

	unstable := unstableMCPServersFromStable(servers)
	if len(unstable) != 4 || unstable[0].Http == nil || unstable[1].Stdio == nil || unstable[2].Sse == nil || unstable[3].Acp == nil {
		t.Fatalf("unstable MCP servers = %#v", unstable)
	}
	stable := stableMCPServersFromUnstable(append(unstable, acp.UnstableMcpServer{}))
	if len(stable) != 5 || stable[0].Http == nil || stable[1].Stdio == nil || stable[2].Sse == nil || stable[3].Acp == nil {
		t.Fatalf("stable MCP servers = %#v", stable)
	}
	if stable[0].Http.Headers[0].Value != "Y" || stable[1].Stdio.Env[0].Value != "V" ||
		stable[2].Sse.Url != "https://sse.example" || stable[3].Acp.Id != "acp-1" {
		t.Fatalf("stable MCP server fields = %#v", stable)
	}
}

func TestRequestBuilderCloneEdgeBranches(t *testing.T) {
	rawOnly := NewSessionRequest("/tmp/project", WithSessionRawEvents(true))
	if !rawMessageConfigFromMeta(rawOnly.Meta).Enabled() {
		t.Fatalf("rawOnly meta = %#v", rawOnly.Meta)
	}
	outputSchema := NewHermesOptions(WithHermesOutputSchema(map[string]any{"type": "object"}))
	if outputSchema.OutputSchema["type"] != "object" {
		t.Fatalf("output schema options = %#v", outputSchema)
	}
	outputSchema.OutputSchema["type"] = "changed"
	outputSchemaClone := NewHermesOptions(WithHermesOutputSchema(outputSchema.OutputSchema))
	outputSchema.OutputSchema["type"] = "mutated"
	if outputSchemaClone.OutputSchema["type"] != "changed" {
		t.Fatalf("output schema was not cloned: %#v", outputSchemaClone.OutputSchema)
	}
	if cloneMCPServers(nil) != nil || cloneMCPServerStdio(nil) != nil || cloneHTTPHeaders(nil) != nil ||
		cloneEnvVariables(nil) != nil || unstableMCPServersFromStable(nil) != nil || stableMCPServersFromUnstable(nil) != nil {
		t.Fatal("nil clone helper returned non-nil")
	}
	if cloneMCPServer(acp.McpServer{}).Http != nil {
		t.Fatal("empty MCP clone was populated")
	}
	if unstableMCPServerFromStable(acp.McpServer{}).Http != nil {
		t.Fatal("empty unstable MCP clone was populated")
	}
	meta := map[string]any{hermesMetaKey: map[string]any{"a": "b"}}
	ensured := ensureMetaMap(meta, hermesMetaKey)
	ensured["a"] = "changed"
	storedMeta, _ := meta[hermesMetaKey].(map[string]any)
	if storedMeta["a"] != "changed" {
		t.Fatalf("ensureMetaMap did not store clone: %#v", meta)
	}
}

func TestPromptMappingHelpers(t *testing.T) {
	var resource acp.EmbeddedResourceResource
	if err := json.Unmarshal([]byte(`{"uri":"file:///tmp/a","text":"body"}`), &resource); err != nil {
		t.Fatal(err)
	}
	part, err := embeddedResourceHermesPart(resource, newImagePromptBudget(ImageLimits{}, ""))
	if err != nil || part["text"] == "" {
		t.Fatalf("embeddedResourceHermesPart = %#v err=%v", part, err)
	}
	var uriResource acp.EmbeddedResourceResource
	if decodeErr := json.Unmarshal([]byte(`{"uri":"file:///tmp/fallback"}`), &uriResource); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if uriPart, uriErr := embeddedResourceHermesPart(uriResource, newImagePromptBudget(ImageLimits{}, "")); uriErr != nil || uriPart[valText] != "file:///tmp/fallback" {
		t.Fatalf("URI resource fallback = %#v err=%v", uriPart, uriErr)
	}
	var emptyTextResource acp.EmbeddedResourceResource
	if decodeErr := json.Unmarshal([]byte(`{"uri":"","text":""}`), &emptyTextResource); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if _, promptErr := promptToHermesParts(t.Context(), []acp.ContentBlock{acp.ResourceBlock(emptyTextResource)}, ImageLimits{}, ""); promptErr == nil {
		t.Fatal("prompt mapping accepted an empty embedded resource")
	}
	imageURI := "file:///tmp/image.png"
	imagePart, err := imageHermesPart(t.Context(), &acp.ContentBlockImage{Data: fixtureBase64(t, "valid.png"), MimeType: "image/png", Uri: &imageURI}, newImagePromptBudget(ImageLimits{}, ""))
	if err != nil {
		t.Fatalf("image mapping err=%v", err)
	}
	if _, ok := imagePart[keyFilename]; ok {
		t.Fatalf("image part derived a native filename from its uri: %#v", imagePart)
	}
	if update := usageUpdateFromTokens(nativehermes.Tokens{}, 0); update != nil {
		t.Fatalf("empty usage update = %#v", update)
	}
	usage := usageFromTokens(nativehermes.Tokens{Input: 1, Output: 2, Reasoning: 3})
	if usage == nil || usage.TotalTokens != 6 {
		t.Fatalf("usage = %#v", usage)
	}
	for _, reason := range []string{"length", "cancelled", "refusal", "stop"} {
		if stopReasonFromHermes(reason) == "" {
			t.Fatalf("empty stop reason for %q", reason)
		}
	}
	for _, status := range []string{"pending", "completed", "failed", "other"} {
		if toolStatus(status) == "" {
			t.Fatalf("empty tool status for %q", status)
		}
	}
	for _, tool := range []string{"read", "edit", "delete", "move", "grep", "bash", "fetch", "think", "other"} {
		if toolKind(tool) == "" {
			t.Fatalf("empty tool kind for %q", tool)
		}
	}
	for _, priority := range []string{"high", "low", "medium"} {
		if planPriority(priority) == "" {
			t.Fatalf("empty plan priority for %q", priority)
		}
	}
	for _, status := range []string{"completed", "in_progress", "pending"} {
		if planStatus(status) == "" {
			t.Fatalf("empty plan status for %q", status)
		}
	}
	if questionElicitationMessage([]nativehermes.QuestionInfo{{Question: "Only?"}}) != "Only?" {
		t.Fatal("single question message mismatch")
	}
	if got := questionOptionSchemas([]nativehermes.QuestionOption{{Label: ""}, {Label: "A"}}); len(got) != 1 {
		t.Fatalf("questionOptionSchemas = %#v", got)
	}
	if req, ok := eventQuestion(json.RawMessage(`{"request":{"id":"q","sessionID":"s"}}`)); !ok || req.ID != "q" {
		t.Fatalf("eventQuestion wrapper = %#v ok=%v", req, ok)
	}
	if _, ok := eventQuestion(json.RawMessage(`{}`)); ok {
		t.Fatal("empty event question parsed")
	}
}

func TestCallForkSessionHelper(t *testing.T) {
	ctx := context.Background()
	for name, handler := range map[string]forkExtensionAgent{
		"success": {
			Agent:    NewAgent(),
			response: acp.UnstableForkSessionResponse{SessionId: "forked"},
		},
		"agent error": {
			Agent: NewAgent(),
			err:   errors.New("fork failed"),
		},
		"decode error": {
			Agent:    NewAgent(),
			response: json.RawMessage(`"bad"`),
		},
	} {
		t.Run(name, func(t *testing.T) {
			conn, closeConn := forkClientConnection(t, handler)
			defer closeConn()
			resp, err := CallForkSession(ctx, conn, ForkSessionRequest("s", "/tmp/project"))
			switch name {
			case "success":
				if err != nil || resp.SessionId != "forked" {
					t.Fatalf("CallForkSession resp=%#v err=%v", resp, err)
				}
			default:
				if err == nil {
					t.Fatal("CallForkSession unexpectedly succeeded")
				}
			}
		})
	}
}

func TestAgentConnectionHelpers(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
	release, err := agent.acquireClientCall(ctx)
	if err != nil {
		t.Fatalf("acquireClientCall: %v", err)
	}
	if _, err2 := agent.acquireClientCall(ctx); err2 == nil {
		t.Fatal("client call backpressure not enforced")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err3 := agent.acquireClientCall(cancelled); err3 == nil {
		t.Fatal("cancelled acquire succeeded")
	}
	release()

	form := acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{
		Message: "m",
		Mode:    "form",
		RequestedSchema: acp.UnstableElicitationSchema{
			Type: acp.UnstableElicitationSchemaTypeObject,
		},
		Meta: map[string]any{"m": true},
	}}
	raw, err := scopedElicitationParams(form, elicitationScope{SessionID: "s", TurnNonce: "turn-1", ToolCallID: "tool"})
	if err != nil {
		t.Fatalf("scopedElicitationParams form: %v", err)
	}
	if !strings.Contains(string(raw), `"sessionId":"s"`) || !strings.Contains(string(raw), `"toolCallId":"tool"`) || !strings.Contains(string(raw), `"turnNonce":"turn-1"`) {
		t.Fatalf("scoped form = %s", raw)
	}
	urlReq := acp.NewUnstableCreateElicitationRequestUrl("e1", "https://example.com")
	if _, err := scopedElicitationParams(urlReq, elicitationScope{SessionID: "s", TurnNonce: "turn-2"}); err != nil {
		t.Fatalf("scopedElicitationParams url: %v", err)
	}
	if _, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{}, elicitationScope{}); err == nil {
		t.Fatal("empty elicitation request accepted")
	}
	if requestError(context.Canceled).Code != -32800 {
		t.Fatal("context cancellation did not map to request cancelled")
	}
	if requestError(errors.New("boom")).Code != -32603 {
		t.Fatal("generic error did not map to internal error")
	}
	gate := newConnectionInputGate(strings.NewReader("x"))
	gate.open()
	buf := make([]byte, 1)
	if n, err := gate.Read(buf); n != 1 || err != nil || string(buf) != "x" {
		t.Fatalf("gate read n=%d err=%v buf=%q", n, err, string(buf))
	}
	conn := &localAgentConnection{agent: agent}
	agent.clientCalls <- struct{}{}
	if _, err := conn.CreateElicitation(ctx, form, elicitationScope{SessionID: "s", TurnNonce: "turn-1"}); err == nil {
		t.Fatal("CreateElicitation ignored client-call backpressure")
	}
	<-agent.clientCalls
	if _, reqErr := conn.handle(ctx, acp.AgentMethodAuthenticate, json.RawMessage(`{}`)); reqErr == nil {
		t.Fatal("uninitialized connection accepted authenticate")
	}
	conn.initialized.Store(true)
	if _, reqErr := conn.handle(ctx, "missing/method", json.RawMessage(`{}`)); reqErr == nil || reqErr.Code != -32601 {
		t.Fatalf("missing method error = %#v", reqErr)
	}
}

type forkExtensionAgent struct {
	*Agent
	response any
	err      error
}

func (a forkExtensionAgent) HandleExtensionMethod(context.Context, string, json.RawMessage) (any, error) {
	return a.response, a.err
}

func forkClientConnection(t *testing.T, agent forkExtensionAgent) (*acp.ClientSideConnection, func()) {
	t.Helper()
	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()
	_ = acp.NewAgentSideConnection(agent, agentToClientWriter, clientToAgentReader)
	conn := acp.NewClientSideConnection(noopACPClient{}, clientToAgentWriter, agentToClientReader)

	return conn, func() {
		_ = clientToAgentWriter.Close()
		_ = clientToAgentReader.Close()
		_ = agentToClientWriter.Close()
		_ = agentToClientReader.Close()
	}
}

type noopACPClient struct{}

func (noopACPClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, nil
}

func (noopACPClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}

func (noopACPClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{}, nil
}

func (noopACPClient) SessionUpdate(context.Context, acp.SessionNotification) error {
	return nil
}

func (noopACPClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, nil
}

func (noopACPClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (noopACPClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (noopACPClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (noopACPClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func TestAgentCloseAuthAndRawEventHelpers(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	agent := NewAgent()
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()
	if _, err := agent.Authenticate(ctx, acp.AuthenticateRequest{}); err == nil {
		t.Fatal("Authenticate accepted unsupported method")
	}
	if _, err := agent.Logout(ctx, acp.LogoutRequest{}); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if _, err := agent.SetSessionMode(ctx, acp.SetSessionModeRequest{}); err == nil {
		t.Fatal("SetSessionMode accepted")
	}
	if err := agent.Cancel(ctx, acp.CancelNotification{SessionId: session.id}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !session.wasCancelled() && client.abortCount() == 0 {
		t.Fatal("Cancel did not touch session/client")
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !client.closed {
		t.Fatal("client not closed")
	}
	payload, err := capRawEventPayload(map[string]any{
		"sessionId": "s",
		"sequence":  int64(1),
		"source":    "test",
		"event":     strings.Repeat("x", rawEventMaxBytes),
	})
	if err != nil {
		t.Fatalf("cap raw event: %v", err)
	}
	if event, _ := payload["event"].(map[string]any); event["truncated"] != true {
		t.Fatalf("raw event was not capped: %#v", payload)
	}
	if _, err := io.Copy(io.Discard, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
}
