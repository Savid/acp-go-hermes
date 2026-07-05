package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

type agentConnection interface {
	Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error)
	NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error)
	ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error)
	CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error)
}

var newAgent = func(store hermesacp.SessionStore) agentConnection {
	return hermesacp.NewAgent(hermesacp.WithSessionStore(store))
}
var getwd = os.Getwd
var exit = os.Exit

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exit(run(ctx, os.Stdout, os.Stderr))
}

func run(ctx context.Context, stdout io.Writer, stderr io.Writer) int {
	store := hermesacp.NewInMemorySessionStore()
	agent := newAgent(store)
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

	_, _ = agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})

	resumed, err := agent.ResumeSession(ctx, hermesacp.ResumeSessionRequest(session.SessionId, cwd))
	if err != nil {
		printError(stderr, err)

		return 1
	}

	fmt.Fprintf(stdout, "resumed with %d config options\n", len(resumed.ConfigOptions))

	return 0
}

func printError(stderr io.Writer, err error) {
	_, _ = fmt.Fprintln(stderr, err)
}
