package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestOptionsAndRequestBuilders(t *testing.T) {
	store := NewInMemorySessionStore()
	seed := map[string]string{"config.yaml": "model: {}\n"}
	opts := applyOptions([]Option{
		WithLogger(slog.New(slog.DiscardHandler)),
		WithAgentName("name"),
		WithAgentTitle("title"),
		WithAgentVersion("version"),
		WithExecutablePath("hermes"),
		WithHome("/tmp/home"),
		WithDefaultModel("openai/gpt"),
		WithEnv(map[string]string{"A": "1"}),
		WithTracerProvider(tracenoop.NewTracerProvider()),
		WithMeterProvider(metricnoop.NewMeterProvider()),
		WithTextMapPropagator(propagation.TraceContext{}),
		WithSessionStore(store),
		WithSessionStoreLoadTimeout(time.Second),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1, MaxConcurrentClientCalls: 3}),
		WithSeedFiles(seed),
	})
	if opts.AgentName != "name" || opts.AgentTitle != "title" || opts.ExecutablePath != "hermes" ||
		opts.Env["A"] != "1" || opts.SessionStore != store {
		t.Fatalf("options = %#v", opts)
	}
	if opts.SeedFiles["config.yaml"] != "model: {}\n" {
		t.Fatalf("seed files = %#v", opts.SeedFiles)
	}
	seed["config.yaml"] = "mutated"
	seed["extra"] = "late"
	if opts.SeedFiles["config.yaml"] != "model: {}\n" || len(opts.SeedFiles) != 1 {
		t.Fatalf("WithSeedFiles did not clone source map: %#v", opts.SeedFiles)
	}

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
	if prompt := TextPromptRequest("s", "hello"); prompt.SessionId != "s" || len(prompt.Prompt) != 1 {
		t.Fatalf("TextPromptRequest = %#v", prompt)
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

func TestValidationMetaAndHelperBranches(t *testing.T) {
	if err := validateSessionStartPaths("relative", nil); err == nil {
		t.Fatal("relative cwd accepted")
	}
	if err := validateRequiredAbsolutePath("cwd", ""); err == nil {
		t.Fatal("empty required absolute path accepted")
	}
	if err := validateSessionStartPaths("/tmp/project", []string{"relative"}); err == nil {
		t.Fatal("relative additional directory accepted")
	}
	value := "/tmp/project"
	if err := validateOptionalAbsolutePath("cwd", &value); err != nil {
		t.Fatalf("validateOptionalAbsolutePath: %v", err)
	}
	if err := validateMCPServers([]acp.McpServer{{Sse: &acp.McpServerSseInline{Name: "sse"}}}); err == nil {
		t.Fatal("unsupported MCP servers accepted")
	}
	if err := validateMCPServers([]acp.McpServer{{Acp: &acp.McpServerAcpInline{Name: "acp"}}}); err == nil {
		t.Fatal("unsupported ACP MCP server accepted")
	}
	if _, err := normalizeConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}); err == nil {
		t.Fatal("negative active-session limit accepted")
	}
	if _, err := normalizeConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: -1}); err == nil {
		t.Fatal("negative client-call limit accepted")
	}
	if limits, err := normalizeConcurrencyLimits(ConcurrencyLimits{}); err != nil ||
		limits.MaxActiveSessions != defaultMaxActiveSessions ||
		limits.MaxConcurrentClientCalls != defaultMaxConcurrentClientCalls {
		t.Fatalf("default concurrency limits = %#v err=%v", limits, err)
	}
	if _, err := stringMapFromMeta(map[string]any{"A": 1}); err == nil {
		t.Fatal("non-string env accepted")
	}
	if env, err := stringMapFromMeta(map[string]string{"A": "1"}); err != nil || env["A"] != "1" {
		t.Fatalf("stringMapFromMeta map[string]string = %#v err=%v", env, err)
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: "bad"}); err == nil {
		t.Fatal("bad hermes meta accepted")
	}
	if err := validateLifecycleMeta(map[string]any{"github.com/savid/acp-go-hermes": map[string]any{}}); err != nil {
		t.Fatalf("foreign module-path meta must be ignored, got %v", err)
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: "bad"}}); err == nil {
		t.Fatal("bad options meta accepted")
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: map[string]any{rawEventKey: "bad"}}); err == nil {
		t.Fatal("bad raw event object accepted")
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: map[string]any{rawEventKey: map[string]any{"unknown": true}}}); err == nil {
		t.Fatal("unknown raw event key accepted")
	}
	if _, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{rawEventKey: map[string]any{rawEventEnabledKey: "bad"}}}); err == nil {
		t.Fatal("bad raw event meta accepted")
	}
	meta, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
		metaModelKey: "p/m",
		metaEnvKey:   map[string]any{"A": "1"},
	}}})
	if err != nil || meta.Model != "p/m" || meta.Env["A"] != "1" {
		t.Fatalf("session meta = %#v err=%v", meta, err)
	}
	meta, err = sessionMetaFromLifecycle(map[string]any{})
	if err != nil {
		t.Fatalf("empty meta = %#v err=%v", meta, err)
	}
	if _, err := hermesOptionsFromMeta(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: "bad"}}}); err == nil {
		t.Fatal("bad env meta accepted")
	}
	if _, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{"mode": "plan"}}}); err == nil {
		t.Fatal("removed mode meta accepted")
	}
	if _, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{"permission": "ask"}}}); err == nil {
		t.Fatal("removed permission meta accepted")
	}
	if _, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: "bad"}}}); err == nil {
		t.Fatal("bad env lifecycle meta accepted")
	}
	testValidationSchemaAndCloneHelpers(t)
}

func testValidationSchemaAndCloneHelpers(t *testing.T) {
	t.Helper()

	for name, schema := range map[string]any{
		"object":       map[string]any{"type": "object"},
		"empty-object": map[string]any{},
		"array":        []any{"bad"},
		"string":       "bad",
	} {
		_, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaOutputSchemaKey: schema}}})
		requireUnsupportedField(t, err, "_meta.hermes.options.outputSchema", "outputSchema "+name)
	}
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: "bad"})), "_meta.hermes", "hermes non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: "bad"}})), "_meta.hermes.options", "options non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaModelKey: 7}}})), "_meta.hermes.options.model", "non-string model")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: "bad"}}})), "_meta.hermes.options.env", "env non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]any{"A": 1}}}})), "_meta.hermes.options.env", "env non-string value")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{rawEventKey: "bad"}})), "_meta.hermes.rawEvent", "rawEvent non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{rawEventKey: map[string]any{rawEventEnabledKey: "bad"}}})), "_meta.hermes.rawEvent.enabled", "rawEvent enabled non-bool")
	if got := cloneAny([]any{map[string]any{"a": "b"}}); !reflect.DeepEqual(got, []any{map[string]any{"a": "b"}}) {
		t.Fatalf("cloneAny slice = %#v", got)
	}
	if cloneAnySlice(nil) != nil {
		t.Fatal("nil cloneAnySlice returned non-nil")
	}
	if splitProvider, splitModel := splitModelValue("model-only", "p", "m"); splitProvider != "p" || splitModel != "model-only" {
		t.Fatalf("split fallback = %q %q", splitProvider, splitModel)
	}
	if joinModelValue("", "m") != "m" || joinModelValue("p", "") != "p" {
		t.Fatal("joinModelValue fallback mismatch")
	}
}

func TestPromptMappingHelpers(t *testing.T) {
	var resource acp.EmbeddedResourceResource
	if err := json.Unmarshal([]byte(`{"uri":"file:///tmp/a","text":"body"}`), &resource); err != nil {
		t.Fatal(err)
	}
	if got := embeddedResourceText(resource); got == "" {
		t.Fatalf("embeddedResourceText = %q", got)
	}
	if update := usageUpdateFromTokens("m", nativeTokens{}, 0); update != nil {
		t.Fatalf("empty usage update = %#v", update)
	}
	usage := usageFromTokens(nativeTokens{Input: 1, Output: 2, Reasoning: 3})
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
	if questionElicitationMessage([]questionInfo{{Question: "Only?"}}) != "Only?" {
		t.Fatal("single question message mismatch")
	}
	if got := questionOptionSchemas([]questionOption{{Label: ""}, {Label: "A"}}); len(got) != 1 {
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
	raw, err := scopedElicitationParams(form, elicitationScope{SessionID: "s", ToolCallID: "tool"})
	if err != nil {
		t.Fatalf("scopedElicitationParams form: %v", err)
	}
	if !strings.Contains(string(raw), `"sessionId":"s"`) || !strings.Contains(string(raw), `"toolCallId":"tool"`) {
		t.Fatalf("scoped form = %s", raw)
	}
	urlReq := acp.NewUnstableCreateElicitationRequestUrl("e1", "https://example.com")
	if _, err := scopedElicitationParams(urlReq, elicitationScope{}); err != nil {
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
	if _, err := conn.CreateElicitation(ctx, form, elicitationScope{}); err == nil {
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
	payload := capRawEventPayload(map[string]any{
		"sessionId": "s",
		"sequence":  int64(1),
		"source":    "test",
		"event":     strings.Repeat("x", rawEventMaxBytes),
	})
	if event, _ := payload["event"].(map[string]any); event["truncated"] != true {
		t.Fatalf("raw event was not capped: %#v", payload)
	}
	if _, err := io.Copy(io.Discard, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
}

func mustErr(_ sessionMeta, err error) error {
	return err
}

func TestValidateMCPServerNames(t *testing.T) {
	t.Run("rejects empty name", func(t *testing.T) {
		cases := map[string][]acp.McpServer{
			"stdio": {StdioMCPServer("", "cmd", nil, nil)},
			"http":  {HTTPMCPServer("", "https://example.test/mcp", nil)},
		}
		for name, servers := range cases {
			t.Run(name, func(t *testing.T) {
				data := requireInvalidParamsData(t, validateMCPServers(servers))
				want := map[string]any{"mcpServers[0].name": "required"}
				if !reflect.DeepEqual(data, want) {
					t.Fatalf("error data = %#v, want %#v", data, want)
				}
			})
		}
	})

	t.Run("rejects duplicate name at later index", func(t *testing.T) {
		servers := []acp.McpServer{
			StdioMCPServer("dup", "cmd", nil, nil),
			HTTPMCPServer("keep", "https://example.test/mcp", nil),
			HTTPMCPServer("dup", "https://collision.test/mcp", nil),
		}
		data := requireInvalidParamsData(t, validateMCPServers(servers))
		want := map[string]any{"mcpServers[2].name": "duplicate"}
		if !reflect.DeepEqual(data, want) {
			t.Fatalf("error data = %#v, want %#v", data, want)
		}
	})

	t.Run("rejects server with no transport", func(t *testing.T) {
		data := requireInvalidParamsData(t, validateMCPServers([]acp.McpServer{{}}))
		want := map[string]any{"field": "mcpServers[0]"}
		if !reflect.DeepEqual(data, want) {
			t.Fatalf("error data = %#v, want %#v", data, want)
		}
	})

	t.Run("accepts unique named servers", func(t *testing.T) {
		servers := []acp.McpServer{
			StdioMCPServer("stdio", "cmd", nil, nil),
			HTTPMCPServer("http", "https://example.test/mcp", nil),
		}
		if err := validateMCPServers(servers); err != nil {
			t.Fatalf("unique named servers rejected: %v", err)
		}
	})

	t.Run("renders names verbatim into config", func(t *testing.T) {
		servers := []acp.McpServer{
			StdioMCPServer("stdio", "cmd", []string{"arg"}, map[string]string{"E": "V"}),
			HTTPMCPServer("http", "https://example.test/mcp", map[string]string{"Authorization": "token"}),
		}
		block := hermesMCPServersConfig(servers)
		mcp, ok := block["mcp_servers"].(map[string]any)
		if !ok {
			t.Fatalf("mcp_servers block missing: %#v", block)
		}
		if len(mcp) != 2 {
			t.Fatalf("mcp_servers = %#v, want two entries keyed by name", mcp)
		}
		stdio, ok := mcp["stdio"].(map[string]any)
		if !ok || stdio["command"] != "cmd" {
			t.Fatalf("stdio entry = %#v", mcp["stdio"])
		}
		httpEntry, ok := mcp["http"].(map[string]any)
		if !ok || httpEntry[valURL] != "https://example.test/mcp" {
			t.Fatalf("http entry = %#v", mcp["http"])
		}
	})
}

func requireInvalidParamsData(t *testing.T, err error) map[string]any {
	t.Helper()
	if err == nil {
		t.Fatal("expected invalid-params error, got nil")
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error type = %T, want *acp.RequestError", err)
	}
	if reqErr.Code != -32602 {
		t.Fatalf("code = %d, want -32602", reqErr.Code)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("error data = %#v, want map", reqErr.Data)
	}

	return data
}

func requireUnsupportedField(t *testing.T, err error, field string, name string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected unsupported-field error, got nil", name)
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("%s: error type = %T", name, err)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("%s: error data = %#v", name, reqErr.Data)
	}
	if data["error"] != "unsupported" || data["field"] != field {
		t.Fatalf("%s: error data = %#v want field %q", name, data, field)
	}
}
