//go:build integration

package integration

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/coder/acp-go-sdk"
)

const liveTokenHermesConfig = `model:
  provider: openrouter
  default: openrouter/free
  max_tokens: 1024
`

// liveAgent holds a launched acp-go-hermes subprocess and its stdio pipes.
//
// Home isolation is bespoke to Hermes: each launch passes a caller-provided
// `-scratch-dir` temp root so the subprocess owns an isolated HERMES_HOME and
// never touches the developer's real Hermes home.
type liveAgent struct {
	cmd    interface{ ProcessState() *os.ProcessState }
	stdin  io.WriteCloser
	stdout io.Reader
	stderr safeBuffer
	close  func()
	wait   func() error
}

func startLiveAgent(t *testing.T, ctx context.Context, home string, extraArgs ...string) *liveAgent {
	t.Helper()
	args := []string{
		"-path", integrationHermesPath(t),
		"-scratch-dir", home,
	}
	args = append(args, extraArgs...)
	cmd := agentCommand(ctx, args...)
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

// startLiveTokenAgent caps only token-spending integration sessions through
// Hermes' native isolated config. Production defaults and the caller's real
// Hermes home remain untouched.
func startLiveTokenAgent(t *testing.T, ctx context.Context, home string, extraArgs ...string) *liveAgent {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(liveTokenHermesConfig), 0o600); err != nil {
		t.Fatalf("write live-test Hermes config: %v", err)
	}

	args := append([]string(nil), extraArgs...)
	args = append(args, "-seed-file", "config.yaml="+configPath)

	return startLiveAgent(t, ctx, home, args...)
}

func liveTokenSeedFiles() map[string]string {
	return map[string]string{"config.yaml": liveTokenHermesConfig}
}

func (a *liveAgent) stderrString() string {
	return a.stderr.String()
}

type safeBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.b = append(b.b, p...)
	return len(p), nil
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.b)
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
