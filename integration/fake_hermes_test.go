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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
	hermesacp "github.com/savid/acp-go-hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
)

const (
	envFakeHermesHelper       = "ACP_GO_HERMES_FAKE_HELPER"
	envFakeHermesMode         = "ACP_GO_HERMES_FAKE_MODE"
	envFakeHermesCLICapture   = "ACP_GO_HERMES_TEST_ROOT"
	fakeModeOK                = "ok"
	fakeModeStatusOnly        = "status-only"
	fakeModeSessionCLI        = "session-cli"
	fakeModePreSubmitActivity = "pre-submit-activity"
	fakeStoredSessionKey      = "stored-fake"
	// The one provider this gateway publishes, and therefore the only one its
	// config.set will resolve.
	fakeGatewayProviderSlug = "openrouter"
)

// fakeConfigSetProvider reads the provider out of a model switch command, and
// reports whether the command named one at all.
func fakeConfigSetProvider(fields []string) (string, bool) {
	index := slices.Index(fields, "--provider")
	if index < 0 || index+1 >= len(fields) {
		return "", false
	}

	return fields[index+1], true
}

type fakeSessionCLICapture struct {
	Path        string `json:"path"`
	Resolved    string `json:"resolved"`
	Token       string `json:"token"`
	OperationID string `json:"operationId"`
	Output      string `json:"output"`
}

type integrationSessionCLICarrier struct {
	name      string
	cwd       string
	token     string
	operation string
	dirs      []string
	capture   string
}

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
	if _, promptErr := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, "turn-complete-only", "reply")); promptErr != nil {
		t.Fatalf("completion-only prompt: %v\nstderr:\n%s", promptErr, agent.stderrString())
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

func TestHermesACPAgentProjectsPreSubmitActivityBeforePromptDispatch(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModePreSubmitActivity), t.TempDir())
	defer agent.close()

	client := newRecordingClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)
	initialized, err := conn.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		Meta: map[string]any{lifecycle.MetaKey: map[string]any{
			"version": lifecycle.Version,
		}},
	})
	if err != nil {
		t.Fatalf("initialize with lifecycle: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if initialized.Meta[lifecycle.MetaKey] == nil {
		t.Fatalf("lifecycle negotiation was not answered: %#v", initialized.Meta)
	}

	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	prompt := hermesacp.TextPromptRequest(session.SessionId, "pre-submit-order", "reply")
	prompt.Meta[lifecycle.MetaKey] = map[string]any{
		"version": lifecycle.Version,
		"submission": map[string]any{
			"submissionId": "pre-submit-order",
			"clientNonce":  "pre-submit-order-nonce",
		},
	}
	response, err := conn.Prompt(ctx, prompt)
	if err != nil {
		t.Fatalf("prompt with older projection: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if response.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("prompt stop reason = %q, want %q", response.StopReason, acp.StopReasonEndTurn)
	}
	if got := client.agentText(); got != "older activityprompt response" {
		t.Fatalf("ordered text = %q, want %q\nupdates:\n%s\nstderr:\n%s", got, "older activityprompt response", client.updatesSummary(), agent.stderrString())
	}

	client.mu.Lock()
	updates := append([]acp.SessionNotification(nil), client.updates...)
	client.mu.Unlock()
	activityIdle := -1
	promptAccepted := -1
	for index, update := range updates {
		envelope, _ := update.Meta[lifecycle.MetaKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		switch event["type"] {
		case "state_update":
			if event["cause"] == "activity" && event["state"] == "idle" {
				activityIdle = index
			}
		case "prompt_accepted":
			promptAccepted = index
		}
	}
	if activityIdle < 0 || promptAccepted < 0 || activityIdle >= promptAccepted {
		t.Fatalf("lifecycle order activity-idle=%d prompt-accepted=%d\nupdates:\n%s", activityIdle, promptAccepted, client.updatesSummary())
	}
}

// TestHermesACPAgentFakeExecutableModelSelection drives model selection over
// the real ACP wire against a gateway that answers the way hermes 0.20.4 was
// measured to. It carries the claim the model config surface rests on all the
// way to a host: the published catalogue is a menu, so a value absent from it
// still reaches the gateway and becomes the option's current value, while a
// selection the gateway refuses comes back as the gateway's own refusal rather
// than as a wrapper verdict.
func TestHermesACPAgentFakeExecutableModelSelection(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeOK), t.TempDir())
	defer agent.close()

	conn := acp.NewClientSideConnection(newRecordingClient(), agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize with fake hermes: %v\nstderr:\n%s", err, agent.stderrString())
	}
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session with fake hermes: %v\nstderr:\n%s", err, agent.stderrString())
	}

	unadvertised := fakeGatewayProviderSlug + "/unlisted-by-the-menu"
	selected, err := conn.SetSessionConfigOption(ctx, hermesacp.SetModelRequest(session.SessionId, unadvertised))
	if err != nil {
		t.Fatalf("select a model the menu does not advertise: %v\nstderr:\n%s", err, agent.stderrString())
	}

	current, menu := fakeModelOptionState(t, selected.ConfigOptions)
	if current != unadvertised {
		t.Fatalf("current model = %q, want the value sent (%q)", current, unadvertised)
	}
	if slices.Contains(menu, unadvertised) {
		t.Fatalf("the menu advertises the unlisted value: %#v", menu)
	}

	_, err = conn.SetSessionConfigOption(ctx, hermesacp.SetModelRequest(session.SessionId, "missing-provider/missing-model"))
	if err == nil {
		t.Fatalf("gateway refusal did not reach the host\nstderr:\n%s", agent.stderrString())
	}
	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) || requestErr.Code != -32602 {
		t.Fatalf("model refusal = %#v, want invalid params", err)
	}
	if got, ok := requestErr.Data.(map[string]any); !ok || got["error"] != "hermes_model_selection_refused" || got["field"] != "value" {
		t.Fatalf("model refusal data = %#v", requestErr.Data)
	}
	if strings.Contains(err.Error(), "Unknown provider") {
		t.Fatalf("model refusal leaked native text: %v", err)
	}

	// The refused selection changed nothing: the session still holds what the
	// gateway last accepted.
	after, err := conn.SetSessionConfigOption(ctx, hermesacp.SetModelRequest(session.SessionId, unadvertised))
	if err != nil {
		t.Fatalf("reselect after a refusal: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if current, _ := fakeModelOptionState(t, after.ConfigOptions); current != unadvertised {
		t.Fatalf("current model after a refused selection = %q, want %q", current, unadvertised)
	}
}

// fakeModelOptionState reads the model option's current value and the values it
// advertises out of a published option list.
func fakeModelOptionState(t *testing.T, options []acp.SessionConfigOption) (string, []string) {
	t.Helper()

	if len(options) != 1 || options[0].Select == nil {
		t.Fatalf("published config options = %#v", options)
	}

	selectOption := options[0].Select

	var menu []string
	if selectOption.Options.Grouped != nil {
		for _, group := range *selectOption.Options.Grouped {
			for _, option := range group.Options {
				menu = append(menu, string(option.Value))
			}
		}
	}
	if selectOption.Options.Ungrouped != nil {
		for _, option := range *selectOption.Options.Ungrouped {
			menu = append(menu, string(option.Value))
		}
	}

	return string(selectOption.CurrentValue), menu
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

func TestHermesACPAgentFakeSessionCLICarrier(t *testing.T) {
	requireRunIntegration(t)
	if runtime.GOOS == "windows" {
		t.Skip("the process-backed integration fixture uses POSIX executable shims")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	agent := startAgentWithHermesPath(t, ctx, fakeHermesExecutable(t, fakeModeSessionCLI), t.TempDir())
	defer agent.close()
	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}

	carriers := []integrationSessionCLICarrier{
		{name: "a", cwd: t.TempDir(), token: "bearer-a", operation: "operation-a", dirs: []string{t.TempDir(), t.TempDir()}, capture: filepath.Join(t.TempDir(), "a.json")},
		{name: "b", cwd: t.TempDir(), token: "bearer-b", operation: "operation-b", dirs: []string{t.TempDir(), t.TempDir()}, capture: filepath.Join(t.TempDir(), "b.json")},
	}
	for _, carrier := range carriers {
		writeIntegrationWagie(t, carrier.dirs[0])
	}

	type result struct {
		name    string
		session acp.NewSessionResponse
		err     error
	}
	results := make(chan result, len(carriers))
	for _, carrier := range carriers {
		go func() {
			session, err := conn.NewSession(ctx, sessionCLICarrierRequest(carrier))
			results <- result{name: carrier.name, session: session, err: err}
		}()
	}
	sessions := map[string]acp.NewSessionResponse{}
	for range carriers {
		result := <-results
		if result.err != nil {
			t.Fatalf("new session %s: %v\nstderr:\n%s", result.name, result.err, agent.stderrString())
		}
		sessions[result.name] = result.session
	}
	for _, carrier := range carriers {
		assertIntegrationSessionCLICapture(t, carrier)
	}

	rotated := integrationSessionCLICarrier{
		name:      "a-rotated",
		cwd:       carriers[0].cwd,
		token:     "bearer-a-rotated",
		operation: "operation-a-rotated",
		dirs:      []string{t.TempDir(), t.TempDir()},
		capture:   filepath.Join(t.TempDir(), "a-rotated.json"),
	}
	writeIntegrationWagie(t, rotated.dirs[0])
	if _, err := conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessions["a"].SessionId}); err != nil {
		t.Fatalf("close session a: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if _, err := conn.ResumeSession(ctx, hermesacp.ResumeSessionRequest(
		sessions["a"].SessionId,
		rotated.cwd,
		hermesacp.WithSessionHermesOptions(hermesacp.HermesOptions{
			Env: map[string]string{
				envFakeHermesCLICapture: rotated.capture,
				"WAGIE_API_TOKEN":       rotated.token,
				"WAGIE_OPERATION_ID":    rotated.operation,
			},
			ExtraPathDirs: rotated.dirs,
		}),
	)); err != nil {
		t.Fatalf("resume rotated carrier: %v\nstderr:\n%s", err, agent.stderrString())
	}
	assertIntegrationSessionCLICapture(t, rotated)
	assertIntegrationSessionCLICapture(t, carriers[1])
}

func sessionCLICarrierRequest(carrier integrationSessionCLICarrier) acp.NewSessionRequest {
	return hermesacp.NewSessionRequest(carrier.cwd, hermesacp.WithSessionHermesOptions(hermesacp.HermesOptions{
		Env: map[string]string{
			envFakeHermesCLICapture: carrier.capture,
			"WAGIE_API_TOKEN":       carrier.token,
			"WAGIE_OPERATION_ID":    carrier.operation,
		},
		ExtraPathDirs: carrier.dirs,
	}))
}

func writeIntegrationWagie(t *testing.T, dir string) {
	t.Helper()
	body := []byte("#!/bin/sh\nprintf '%s:%s' \"$WAGIE_OPERATION_ID\" \"$WAGIE_API_TOKEN\"\n")
	if err := os.WriteFile(filepath.Join(dir, "wagie"), body, 0o700); err != nil {
		t.Fatal(err)
	}
}

func assertIntegrationSessionCLICapture(t *testing.T, carrier integrationSessionCLICarrier) {
	t.Helper()
	data, err := os.ReadFile(carrier.capture)
	if err != nil {
		t.Fatalf("read capture %s: %v", carrier.name, err)
	}
	var capture fakeSessionCLICapture
	if err := json.Unmarshal(data, &capture); err != nil {
		t.Fatalf("decode capture %s: %v", carrier.name, err)
	}
	wagie := filepath.Join(carrier.dirs[0], "wagie")
	if capture.Resolved != wagie || capture.Token != carrier.token || capture.OperationID != carrier.operation || capture.Output != carrier.operation+":"+carrier.token {
		t.Fatalf("capture %s = %#v", carrier.name, capture)
	}
	parts := strings.Split(capture.Path, string(os.PathListSeparator))
	if len(parts) < len(carrier.dirs)+1 || parts[0] != carrier.dirs[0] || parts[1] != carrier.dirs[1] || !strings.HasPrefix(filepath.Base(parts[2]), "acp-go-hermes-browser-shim-") {
		t.Fatalf("capture %s PATH = %#v", carrier.name, parts)
	}
	for _, part := range parts {
		if part == "" {
			t.Fatalf("capture %s PATH contains an empty component: %#v", carrier.name, parts)
		}
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
	cmd := agentCommand(t, ctx, integrationAgentArgs(hermesPath, home)...)

	return &liveAgent{startIntegrationProcess(t, cmd)}
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
	if slices.Contains(args, "--version") {
		_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.20.0 (fake)")

		return nil
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
	if mode == fakeModeSessionCLI {
		if err := captureIntegrationSessionCLI(); err != nil {
			return err
		}
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

func captureIntegrationSessionCLI() error {
	capturePath := os.Getenv(envFakeHermesCLICapture)
	if capturePath == "" {
		return errors.New("fake session CLI capture path is empty")
	}
	resolved, err := exec.LookPath("wagie")
	if err != nil {
		return fmt.Errorf("resolve wagie: %w", err)
	}
	output, err := exec.Command(resolved).Output()
	if err != nil {
		return fmt.Errorf("execute wagie: %w", err)
	}
	data, err := json.Marshal(fakeSessionCLICapture{
		Path:        os.Getenv("PATH"),
		Resolved:    resolved,
		Token:       os.Getenv("WAGIE_API_TOKEN"),
		OperationID: os.Getenv("WAGIE_OPERATION_ID"),
		Output:      string(output),
	})
	if err != nil {
		return err
	}

	return os.WriteFile(capturePath, data, 0o600)
}

type fakeGatewayState struct {
	mu       sync.Mutex
	messages []map[string]any
	title    string
}

func (s *fakeGatewayState) recordAssistant(text string) {
	s.mu.Lock()
	s.messages = append(s.messages, map[string]any{
		"role": "assistant",
		"text": text,
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
		state.mu.Lock()
		state.title, _ = params["title"].(string)
		state.mu.Unlock()
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
	case "session.list":
		state.mu.Lock()
		title := state.title
		state.mu.Unlock()
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"sessions": []map[string]any{{
			"id": fakeStoredSessionKey, "title": title,
		}}})
	case "session.history":
		messages := state.history()
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"count": len(messages), "messages": messages})
	case "process.list":
		live, _ := params["session_id"].(string)
		if strings.HasPrefix(live, "__acp_go_hermes_missing_probe__") {
			writeFakeGatewayError(ctx, conn, id, 4001, "session not found")

			return
		}

		writeFakeGatewayResult(ctx, conn, id, map[string]any{"processes": []any{}})
	case "session.branch":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"session_id":        "live-branch",
			"stored_session_id": "stored-branch",
			"title":             "Branch",
			"parent":            fakeStoredSessionKey,
		})
	case "model.options":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{
			"model":    "anthropic/claude-sonnet-4",
			"provider": "",
			"providers": []map[string]any{{
				"slug":            fakeGatewayProviderSlug,
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
		if mode == fakeModePreSubmitActivity {
			state.recordAssistant("older activity")
			writeFakeGatewayEvent(ctx, conn, "message.delta", live, map[string]any{"text": "older activity"})
			writeFakeGatewayEvent(ctx, conn, "message.complete", live, map[string]any{
				"text": "older activity", "usage": map[string]any{"total_tokens": 1},
			})
			writeFakeGatewayResult(ctx, conn, id, map[string]any{"status": "streaming"})
			state.recordAssistant("prompt response")
			writeFakeGatewayEvent(ctx, conn, "message.complete", live, map[string]any{
				"text": "prompt response", "usage": map[string]any{"total_tokens": 1},
			})

			return
		}
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"status": "streaming"})
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
		// Hermes may deliver the entire assistant reply only on the
		// authoritative completion event, with no preceding message.delta.
		writeFakeGatewayEvent(ctx, conn, "message.complete", live, map[string]any{
			"text":  "fake response",
			"usage": map[string]any{"total_tokens": 1},
		})
	case "session.delete", "session.close", "session.interrupt",
		"approval.respond", "clarify.respond", "terminal.read.respond", "sudo.respond", "secret.respond":
		writeFakeGatewayResult(ctx, conn, id, map[string]any{})
	case "config.set":
		value, _ := params["value"].(string)
		fields := strings.Fields(value)
		// The double answers the way hermes 0.20.4 (2026.8.18) was measured to,
		// message text included. Hermes refuses on the provider and never on the
		// model: a selection naming a provider it does not publish is refused,
		// while a model no provider advertises is taken. The measurement and its
		// provenance live in internal/hermes' TestLiveModelSelectionNativeAnswers.
		if len(fields) == 0 || strings.HasPrefix(fields[0], "--") {
			writeFakeGatewayError(ctx, conn, id, 5001, "model value required")

			return
		}
		if provider, qualified := fakeConfigSetProvider(fields); qualified && provider != fakeGatewayProviderSlug {
			writeFakeGatewayError(ctx, conn, id, 5001, fmt.Sprintf(
				"Unknown provider '%s'. Check 'hermes model' for available providers, "+
					"or define it in config.yaml under 'providers:'.", provider))

			return
		}
		raw := strings.Trim(fields[0], "'")
		writeFakeGatewayResult(ctx, conn, id, map[string]any{"key": params["key"], "value": raw, "scope": "session", "confirm_required": false})
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
