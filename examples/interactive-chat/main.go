// Command interactive-chat is a line-oriented chat over an embedded agent:
// each input line is one prompt, and an empty line or EOF ends the session.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"github.com/coder/acp-go-sdk"

	hermesacp "github.com/savid/acp-go-hermes"
)

// terminal prints streamed text and asks the user about each permission.
type terminal struct {
	output io.Writer
	input  *bufio.Reader
}

func (t *terminal) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	switch update := params.Update; {
	case update.AgentMessageChunk != nil && update.AgentMessageChunk.Content.Text != nil:
		fmt.Fprint(t.output, update.AgentMessageChunk.Content.Text.Text)
	case update.ToolCall != nil:
		fmt.Fprintf(t.output, "\n[tool] %s\n", update.ToolCall.Title)
	}

	return nil
}

func (t *terminal) RequestPermission(_ context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	title := ""
	if params.ToolCall.Title != nil {
		title = *params.ToolCall.Title
	}

	fmt.Fprintf(t.output, "\nallow %s? [y/N] ", title)

	answer, _ := t.input.ReadString('\n')
	if strings.EqualFold(strings.TrimSpace(answer), "y") {
		for _, option := range params.Options {
			if option.Kind == acp.PermissionOptionKindAllowOnce {
				return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId)}, nil
			}
		}
	}

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (*terminal) ReadTextFile(_ context.Context, params acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	data, err := os.ReadFile(params.Path)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}

	return acp.ReadTextFileResponse{Content: string(data)}, nil
}

func (*terminal) WriteTextFile(_ context.Context, params acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, os.WriteFile(params.Path, []byte(params.Content), 0o600)
}

func (*terminal) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, errors.New("terminals are not supported")
}

func (*terminal) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (*terminal) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (*terminal) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (*terminal) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func main() {
	os.Exit(mainCode())
}

func mainCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	return run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("interactive-chat", flag.ContinueOnError)
	flags.SetOutput(stderr)

	model := flags.String("model", "", "model for the session as provider/id")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(stderr, "interactive-chat: %v\n", err)

		return 1
	}

	clientReader, agentWriter := io.Pipe()
	agentReader, clientWriter := io.Pipe()
	served := make(chan error, 1)

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		served <- hermesacp.Serve(serveCtx, agentReader, agentWriter, hermesacp.WithLogger(slog.New(slog.DiscardHandler)))
	}()

	input := bufio.NewReader(stdin)
	conn := acp.NewClientSideConnection(&terminal{output: stdout, input: input}, clientWriter, clientReader)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	err = chat(ctx, conn, input, cwd, *model, stdout)

	cancel()

	_ = clientWriter.Close()

	<-served

	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(stderr, "interactive-chat: %v\n", err)

		return 1
	}

	return 0
}

type agentConnection interface {
	Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error)
	NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error)
	Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, error)
	CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error)
}

// chat reads one prompt per line until an empty line, EOF, or cancellation.
func chat(ctx context.Context, conn agentConnection, input *bufio.Reader, cwd string, model string, stdout io.Writer) error {
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		return err
	}

	var opts []hermesacp.SessionRequestOption
	if model != "" {
		opts = append(opts, hermesacp.WithSessionHermesOptions(hermesacp.NewHermesOptions(hermesacp.WithHermesModel(model))))
	}

	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd, opts...))
	if err != nil {
		return err
	}

	defer func() {
		_, _ = conn.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: session.SessionId})
	}()

	for {
		fmt.Fprint(stdout, "\n> ")

		line, readErr := input.ReadString('\n')
		text := strings.TrimSpace(line)

		if text == "" {
			return nil
		}

		if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, text)); err != nil {
			return err
		}

		if readErr != nil {
			return nil
		}
	}
}
