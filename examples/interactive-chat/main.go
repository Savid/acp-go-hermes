package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

type agentConnection interface {
	Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error)
	NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error)
	Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, error)
	CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error)
}

var newAgent = func() agentConnection { return hermesacp.NewAgent() }
var getwd = os.Getwd
var exit = os.Exit

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exit(run(ctx, os.Stdin, os.Stdout, os.Stderr))
}

func run(ctx context.Context, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	agent := newAgent()
	if _, err := agent.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		printError(stderr, err)

		return 1
	}

	cwd, err := getwd()
	if err != nil {
		printError(stderr, err)

		return 1
	}

	session, err := agent.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		printError(stderr, err)

		return 1
	}
	defer func() {
		_, _ = agent.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: session.SessionId})
	}()

	scanner := bufio.NewScanner(stdin)
	for {
		fmt.Fprint(stdout, "> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				printError(stderr, err)

				return 1
			}

			return 0
		}

		text := strings.TrimSpace(scanner.Text())
		if text == "" || text == "exit" {
			return 0
		}

		resp, err := agent.Prompt(ctx, hermesacp.TextPromptRequest(session.SessionId, text))
		if err != nil {
			printError(stderr, err)

			continue
		}

		fmt.Fprintf(stdout, "\n[%s]\n", resp.StopReason)
	}
}

func printError(stderr io.Writer, err error) {
	_, _ = fmt.Fprintln(stderr, err)
}
