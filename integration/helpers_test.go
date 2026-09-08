//go:build integration

package integration

import (
	"context"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

// envLiveKeyEnv names the environment variable the selected provider reads its
// key from. Hermes resolves a provider by finding that variable in its own
// environment, so the tier forwards exactly that one name and nothing else.
const envLiveKeyEnv = "ACP_GO_HERMES_LIVE_KEY_ENV"

const (
	defaultLiveProvider = "openrouter"
	defaultLiveModel    = "openrouter/free"
	defaultLiveKeyEnv   = "OPENROUTER_API_KEY"
)

// liveTokenModelSelection reports the provider and native model id the
// token-spending tier seeds Hermes with. ACP_GO_HERMES_MODEL is the same
// provider-qualified selection the -model flag takes, so the seeded config and
// the flag can never name different routing. The provider is the first segment
// because a native model id may itself carry slashes.
func liveTokenModelSelection() (string, string) {
	provider, model := defaultLiveProvider, defaultLiveModel
	if selection := os.Getenv("ACP_GO_HERMES_MODEL"); selection != "" {
		if named, native, ok := strings.Cut(selection, "/"); ok && named != "" && native != "" {
			provider, model = named, native
		}
	}

	return provider, model
}

// liveTokenHermesConfig caps only token-spending integration sessions through
// Hermes' native isolated config and routes them at the selected provider.
func liveTokenHermesConfig() string {
	return liveHermesConfig(1024, "")
}

// liveToolTokenHermesConfig is the same routing with the room a tool-driven
// turn needs and manual approvals. Manual mode is what keeps a
// dangerous-classified terminal command routed to the host approval callback
// rather than auto-classified by Hermes' default risk classifier; nothing here
// auto-grants an approval.
func liveToolTokenHermesConfig() string {
	return liveHermesConfig(4096, "approvals:\n  mode: manual\n")
}

func liveHermesConfig(maxTokens int, extra string) string {
	provider, model := liveTokenModelSelection()

	return "model:\n" +
		"  provider: " + provider + "\n" +
		"  default: " + model + "\n" +
		"  max_tokens: " + strconv.Itoa(maxTokens) + "\n" +
		extra
}

// liveTokenEnv adds the selected provider's credential to a launch environment.
// The key travels as an explicit env entry, which is the only door an ordinary
// launch leaves open for one, and never into a seeded file. A tier running
// without the variable set gets the caller's entries unchanged, so the native
// gateway reports the missing provider rather than the test inventing one.
func liveTokenEnv(extra map[string]string) map[string]string {
	env := make(map[string]string, len(extra)+1)
	maps.Copy(env, extra)

	name := envOrDefault(envLiveKeyEnv, defaultLiveKeyEnv)
	if value := os.Getenv(name); value != "" {
		env[name] = value
	}

	return env
}

// liveTokenSessionOptions carries the provider credential into a session driven
// through the wrapper CLI, which advertises no agent-wide environment flag.
func liveTokenSessionOptions() []hermesacp.SessionRequestOption {
	return []hermesacp.SessionRequestOption{
		hermesacp.WithSessionHermesOptions(hermesacp.HermesOptions{Env: liveTokenEnv(nil)}),
	}
}

// inProcessAgent runs Serve over in-process pipes. It is how a test drives the
// public ACP surface while still holding the options it passed — a session
// store above all. A directly constructed Agent has no client connection to
// stream a turn's updates to, so a prompt through one cannot complete; Serve
// owns that connection.
type inProcessAgent struct {
	conn   *acp.ClientSideConnection
	client *recordingClient
	stop   func() error
}

func startInProcessAgent(t *testing.T, ctx context.Context, opts ...hermesacp.Option) *inProcessAgent {
	t.Helper()

	clientReader, agentWriter := io.Pipe()
	agentReader, clientWriter := io.Pipe()

	client := newRecordingClient()
	agent := &inProcessAgent{
		conn:   acp.NewClientSideConnection(client, clientWriter, clientReader),
		client: client,
	}

	served := make(chan error, 1)

	go func() { served <- hermesacp.Serve(ctx, agentReader, agentWriter, opts...) }()

	stopped := false
	agent.stop = func() error {
		if stopped {
			return nil
		}

		stopped = true

		_ = clientWriter.Close()
		err := <-served
		_ = agentWriter.Close()

		return err
	}
	t.Cleanup(func() { _ = agent.stop() })

	if _, err := agent.conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize in-process agent: %v", err)
	}

	return agent
}

// liveAgent holds a launched acp-go-hermes subprocess and its stdio pipes.
//
// Each launch passes a caller-provided `-scratch-dir` temp root so the
// subprocess owns an isolated HERMES_HOME. Tests that need durable credentials
// use an explicit disposable shared HERMES_HOME.
type liveAgent struct{ *integrationProcess }

// integrationAgentArgs builds the launch args every wrapper subprocess in this
// tier shares.
func integrationAgentArgs(hermesPath string, home string, extraArgs ...string) []string {
	args := make([]string, 0, 4+len(extraArgs))
	args = append(args,
		"-path", hermesPath,
		"-scratch-dir", home,
	)

	return append(args, extraArgs...)
}

func startLiveAgent(t *testing.T, ctx context.Context, home string, extraArgs ...string) *liveAgent {
	t.Helper()
	cmd := agentCommand(t, ctx, integrationAgentArgs(integrationHermesPath(t), home, extraArgs...)...)

	return &liveAgent{startIntegrationProcess(t, cmd)}
}

// startLiveTokenAgent caps only token-spending integration sessions through
// Hermes' native isolated config. Production defaults remain untouched.
func startLiveTokenAgent(t *testing.T, ctx context.Context, home string, extraArgs ...string) *liveAgent {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(liveTokenHermesConfig()), 0o600); err != nil {
		t.Fatalf("write live-test Hermes config: %v", err)
	}

	args := append([]string(nil), extraArgs...)
	args = append(args, "-seed-file", "config.yaml="+configPath)

	return startLiveAgent(t, ctx, home, args...)
}

func liveTokenSeedFiles() map[string]string {
	return map[string]string{"config.yaml": liveTokenHermesConfig()}
}

func (a *liveAgent) stderrString() string {
	return a.stderr.String()
}

type recordingClient struct {
	mu           sync.Mutex
	permissions  []acp.RequestPermissionRequest
	elicitations []acp.UnstableCreateElicitationRequest
	updates      []acp.SessionNotification
}

var _ acp.Client = (*recordingClient)(nil)

func newRecordingClient() *recordingClient {
	return &recordingClient{}
}

func (*recordingClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, nil
}
func (*recordingClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}
func (c *recordingClient) RequestPermission(_ context.Context, req acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permissions = append(c.permissions, req)
	c.mu.Unlock()

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}, nil
}
func (c *recordingClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.mu.Lock()
	c.updates = append(c.updates, notification)
	c.mu.Unlock()

	return nil
}
func (*recordingClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{TerminalId: "terminal-1"}, nil
}
func (*recordingClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}
func (*recordingClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}
func (*recordingClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}
func (*recordingClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func (c *recordingClient) UnstableCreateElicitation(_ context.Context, req acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	c.elicitations = append(c.elicitations, req)
	c.mu.Unlock()
	content := map[string]any{"question_1": "Yes"}
	if req.Form != nil {
		for _, key := range req.Form.RequestedSchema.Required {
			content[key] = "Yes"
		}
	}

	return acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: content},
	}, nil
}

func (c *recordingClient) permissionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.permissions)
}

func (c *recordingClient) elicitationCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.elicitations)
}

func (c *recordingClient) updatesSummary() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out strings.Builder
	for _, notification := range c.updates {
		u := notification.Update
		switch {
		case u.ToolCall != nil:
			out.WriteString("toolCall title=" + u.ToolCall.Title + " kind=" + string(u.ToolCall.Kind) + " status=" + string(u.ToolCall.Status) + "\n")
		case u.ToolCallUpdate != nil:
			title := ""
			if u.ToolCallUpdate.Title != nil {
				title = *u.ToolCallUpdate.Title
			}
			status := ""
			if u.ToolCallUpdate.Status != nil {
				status = string(*u.ToolCallUpdate.Status)
			}
			out.WriteString("toolCallUpdate title=" + title + " status=" + status + "\n")
		case u.AgentMessageChunk != nil && u.AgentMessageChunk.Content.Text != nil:
			out.WriteString("agentText " + u.AgentMessageChunk.Content.Text.Text + "\n")
		case u.AgentThoughtChunk != nil:
			out.WriteString("agentThought\n")
		case u.Plan != nil:
			out.WriteString("plan\n")
		}
	}

	return out.String()
}

func (c *recordingClient) agentText() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out strings.Builder
	for _, notification := range c.updates {
		chunk := notification.Update.AgentMessageChunk
		if chunk != nil && chunk.Content.Text != nil {
			out.WriteString(chunk.Content.Text.Text)
		}
	}

	return out.String()
}

func envOrDefault(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

// smokePlaceholderProviderKey satisfies the native gateway's
// "some inference provider is configured" precondition for tiers that make no
// provider request. It is a fixed non-credential string, never a real key.
const smokePlaceholderProviderKey = "acp-go-hermes-smoke-placeholder-not-a-credential"

func TestIntegrationHarnessPrerequisites(t *testing.T) {
	if os.Args[len(os.Args)-1] == "harness-prerequisite-child" {
		path := integrationHermesPath(t)
		t.Log("resolved harness " + path)

		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, integration, tier, value, outcome string
		available                               bool
	}{
		{name: "ungated", outcome: "SKIP"},
		{name: "disabled", integration: "0", outcome: "SKIP"},
		{name: "invalid_gate", integration: "true", outcome: "SKIP"},
		{name: "missing_smoke", integration: "1", outcome: "SKIP"},
		{name: "disabled_live", integration: "1", tier: "RUN_LIVE_TOKENS", value: "0", outcome: "SKIP"},
		{name: "missing_live", integration: "1", tier: "RUN_LIVE_TOKENS", value: "1", outcome: "FAIL"},
		{name: "missing_attended", integration: "1", tier: "RUN_ATTENDED", value: "1", outcome: "FAIL"},
		{name: "missing_keystore", integration: "1", tier: "RUN_KEYSTORE", value: "1", outcome: "FAIL"},
		{name: "fake_path", integration: "1", outcome: "PASS", available: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, suffix := range []string{"RUN_INTEGRATION", "RUN_LIVE_TOKENS", "RUN_ATTENDED", "RUN_KEYSTORE"} {
				t.Setenv("ACP_GO_HERMES_"+suffix, "0")
			}
			t.Setenv("ACP_GO_HERMES_RUN_INTEGRATION", tc.integration)
			if tc.tier != "" {
				t.Setenv("ACP_GO_HERMES_"+tc.tier, tc.value)
			}
			dir := t.TempDir()
			harness := filepath.Join(dir, "hermes")
			if runtime.GOOS == "windows" {
				harness += ".exe"
			}
			if tc.available {
				// Resolution only: this file is never executed.
				if err := os.WriteFile(harness, []byte("fake harness path"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("ACP_GO_HERMES_HARNESS_PATH", harness)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestIntegrationHarnessPrerequisites$", "-test.v", "--", "harness-prerequisite-child")
			cmd.WaitDelay = time.Second
			output, runErr := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			if (runErr != nil) != (tc.outcome == "FAIL") {
				t.Fatalf("unexpected child result: %v\n%s", runErr, output)
			}
			if !strings.Contains(string(output), "--- "+tc.outcome+": TestIntegrationHarnessPrerequisites") {
				t.Fatalf("want child %s:\n%s", tc.outcome, output)
			}
			if tc.available && !strings.Contains(string(output), "resolved harness "+harness) {
				t.Fatalf("fake harness selection was lost:\n%s", output)
			}
		})
	}
}
