// Command minimal-client runs acp-go-hermes as a subprocess, creates one session,
// sends one prompt, and prints the streamed answer.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

const agentPackage = "github.com/savid/acp-go-hermes/cmd/acp-go-hermes"

// client is the ACP client side: it prints streamed updates and allows every
// permission request once.
type client struct {
	output io.Writer
}

var _ acp.Client = (*client)(nil)

func (c *client) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	update := params.Update

	switch {
	case update.AgentMessageChunk != nil && update.AgentMessageChunk.Content.Text != nil:
		fmt.Fprint(c.output, update.AgentMessageChunk.Content.Text.Text)
	case update.AgentThoughtChunk != nil && update.AgentThoughtChunk.Content.Text != nil:
		fmt.Fprintf(c.output, "\n[thought] %s\n", update.AgentThoughtChunk.Content.Text.Text)
	case update.ToolCall != nil:
		fmt.Fprintf(c.output, "\n[tool] %s %s\n", update.ToolCall.ToolCallId, update.ToolCall.Title)
	case update.ToolCallUpdate != nil && update.ToolCallUpdate.Status != nil:
		fmt.Fprintf(c.output, "\n[tool] %s %s\n", update.ToolCallUpdate.ToolCallId, *update.ToolCallUpdate.Status)
	}

	return nil
}

func (*client) RequestPermission(_ context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	for _, option := range params.Options {
		if option.Kind == acp.PermissionOptionKindAllowOnce {
			return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId)}, nil
		}
	}

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (*client) ReadTextFile(_ context.Context, params acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	data, err := os.ReadFile(params.Path)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}

	return acp.ReadTextFileResponse{Content: string(data)}, nil
}

func (*client) WriteTextFile(_ context.Context, params acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, os.WriteFile(params.Path, []byte(params.Content), 0o600)
}

func (*client) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, fmt.Errorf("terminals are not supported")
}

func (*client) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (*client) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (*client) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (*client) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func main() {
	os.Exit(mainCode())
}

func mainCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	return run(ctx, os.Args[1:], os.Stdout, os.Stderr)
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("minimal-client", flag.ContinueOnError)
	flags.SetOutput(stderr)

	if err := flags.Parse(args); err != nil {
		return 2
	}

	prompt := strings.TrimSpace(strings.Join(flags.Args(), " "))
	if prompt == "" {
		prompt = "Reply with a short hello from ACP."
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(stderr, "minimal-client: %v\n", err)

		return 1
	}

	cmd := exec.CommandContext(ctx, "go", "run", agentPackage)
	cmd.Stderr = stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		fmt.Fprintf(stderr, "minimal-client: %v\n", err)

		return 1
	}

	agentStdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintf(stderr, "minimal-client: %v\n", err)

		return 1
	}

	if startErr := cmd.Start(); startErr != nil {
		fmt.Fprintf(stderr, "minimal-client: %v\n", startErr)

		return 1
	}

	conn := acp.NewClientSideConnection(&client{output: stdout}, stdin, agentStdout)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	err = converse(ctx, conn, cwd, prompt, stdout)

	_ = stdin.Close()
	_ = cmd.Wait()

	if err != nil {
		fmt.Fprintf(stderr, "minimal-client: %v\n", err)

		return 1
	}

	return 0
}

// agentConnection is the part of the agent the conversation uses.
type agentConnection interface {
	Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error)
	NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error)
	Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, error)
	CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error)
}

func converse(ctx context.Context, conn agentConnection, cwd string, prompt string, stdout io.Writer) error {
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		return err
	}

	session, err := conn.NewSession(ctx, wire.NewSessionRequest(cwd))
	if err != nil {
		return err
	}

	defer func() {
		_, _ = conn.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: session.SessionId})
	}()

	resp, err := conn.Prompt(ctx, wire.TextPromptRequest(session.SessionId, prompt))
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "\n\nstop reason: %s\n", resp.StopReason)

	return nil
}
