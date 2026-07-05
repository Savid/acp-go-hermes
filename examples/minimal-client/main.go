package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

const agentPackage = "github.com/savid/acp-go-hermes/cmd/acp-go-hermes"

type agentConnection interface {
	Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error)
	NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error)
	Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, error)
	CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error)
}

type startedAgent struct {
	conn  agentConnection
	close func()
	wait  func() error
}

var startAgent = startAgentProcess
var getwd = os.Getwd
var exit = os.Exit
var commandContext = exec.CommandContext

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) int {
	prompt := strings.TrimSpace(strings.Join(args, " "))
	if prompt == "" {
		prompt = "Reply with a short hello from ACP."
	}

	cwd, err := getwd()
	if err != nil {
		printError(stderr, err)

		return 1
	}

	agent, err := startAgent(ctx, stdout, stderr)
	if err != nil {
		printError(stderr, err)

		return 1
	}
	defer func() {
		if agent.close != nil {
			agent.close()
		}
		if agent.wait != nil {
			_ = agent.wait()
		}
	}()

	if err := runConversation(ctx, agent.conn, prompt, cwd, stdout); err != nil {
		printError(stderr, err)

		return 1
	}

	return 0
}

func runConversation(ctx context.Context, conn agentConnection, prompt string, cwd string, stdout io.Writer) error {
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		return err
	}

	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		return err
	}
	defer func() {
		_, _ = conn.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: session.SessionId})
	}()

	resp, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, prompt))
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "\n\nstop reason: %s\n", resp.StopReason)

	return nil
}

func startAgentProcess(ctx context.Context, output io.Writer, stderr io.Writer) (*startedAgent, error) {
	cmd := commandContext(ctx, "go", "run", agentPackage)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	agentStdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	conn := acp.NewClientSideConnection(&client{output: output}, stdin, agentStdout)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	return &startedAgent{conn: conn, close: func() { _ = stdin.Close() }, wait: cmd.Wait}, nil
}

type client struct {
	output io.Writer
	mu     sync.Mutex
}

var _ acp.Client = (*client)(nil)

func (*client) ReadTextFile(_ context.Context, params acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	if !filepath.IsAbs(params.Path) {
		return acp.ReadTextFileResponse{}, fmt.Errorf("path must be absolute: %s", params.Path)
	}

	data, err := os.ReadFile(params.Path)

	return acp.ReadTextFileResponse{Content: string(data)}, err
}

func (*client) WriteTextFile(_ context.Context, params acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	if !filepath.IsAbs(params.Path) {
		return acp.WriteTextFileResponse{}, fmt.Errorf("path must be absolute: %s", params.Path)
	}

	if err := os.MkdirAll(filepath.Dir(params.Path), 0o755); err != nil {
		return acp.WriteTextFileResponse{}, err
	}

	return acp.WriteTextFileResponse{}, os.WriteFile(params.Path, []byte(params.Content), 0o600)
}

func (*client) RequestPermission(_ context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	for _, option := range params.Options {
		if option.Kind == acp.PermissionOptionKindAllowOnce || option.Kind == acp.PermissionOptionKindAllowAlways {
			return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId)}, nil
		}
	}
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (c *client) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.output == nil {
		c.output = os.Stdout
	}

	if update := params.Update.AgentMessageChunk; update != nil && update.Content.Text != nil {
		fmt.Fprint(c.output, update.Content.Text.Text)
	}

	return nil
}

func printError(stderr io.Writer, err error) {
	_, _ = fmt.Fprintf(stderr, "minimal-client: %v\n", err)
}

func (*client) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{TerminalId: "terminal-1"}, nil
}
func (*client) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}
func (*client) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{Output: "", Truncated: false}, nil
}
func (*client) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}
func (*client) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}
