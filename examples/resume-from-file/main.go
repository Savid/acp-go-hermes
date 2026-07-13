package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

const (
	defaultSessionFile = "session.jsonl"
	defaultPrompt      = "Reply with exactly RESUME_OK and do not use tools."
)

type client struct {
	output io.Writer
	mu     sync.Mutex
	text   strings.Builder
}

var _ acp.Client = (*client)(nil)

var (
	runMain   = run
	runLoaded = runLoadedSession
	getwd     = os.Getwd
	exit      = os.Exit
	serve     = hermesacp.Serve
)

func (*client) ReadTextFile(_ context.Context, params acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	data, err := os.ReadFile(params.Path)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}

	return acp.ReadTextFileResponse{Content: string(data)}, nil
}

func (*client) WriteTextFile(_ context.Context, params acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	if err := os.MkdirAll(filepath.Dir(params.Path), 0o755); err != nil {
		return acp.WriteTextFileResponse{}, err
	}

	return acp.WriteTextFileResponse{}, os.WriteFile(params.Path, []byte(params.Content), 0o600)
}

func (*client) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (c *client) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	if params.Update.AgentMessageChunk == nil || params.Update.AgentMessageChunk.Content.Text == nil {
		return nil
	}

	text := params.Update.AgentMessageChunk.Content.Text.Text

	c.mu.Lock()
	defer c.mu.Unlock()

	writer := c.output
	if writer == nil {
		writer = os.Stdout
	}

	fmt.Fprint(writer, text)
	c.text.WriteString(text)

	return nil
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

func main() {
	if err := runMain(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "resume-from-file: %v\n", err)
		exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("resume-from-file", flag.ContinueOnError)
	flags.SetOutput(stderr)

	sessionFile := flags.String("file", defaultSessionFile, "Hermes transcript JSONL file")
	sessionID := flags.String("session", "", "session id; defaults to the sessionId found in the JSONL")
	cwd := flags.String("cwd", "", "session cwd; defaults to the JSONL cwd or current directory")
	prompt := flags.String("prompt", defaultPrompt, "prompt to send after loading history")
	hermesPath := flags.String("path", "", "path to hermes CLI")
	scratchDir := flags.String("scratch-dir", "", "parent directory for ephemeral session scratch; empty means the system temp directory")

	if err := flags.Parse(args); err != nil {
		return err
	}

	entries, inferredSessionID, inferredCwd, err := readTranscriptJSONL(*sessionFile)
	if err != nil {
		return err
	}

	if *sessionID == "" {
		*sessionID = inferredSessionID
	}

	if *sessionID == "" {
		return errors.New("session id is required")
	}

	if *cwd == "" {
		*cwd = inferredCwd
	}

	if *cwd == "" {
		*cwd, err = getwd()
		if err != nil {
			return err
		}
	}

	store := hermesacp.NewInMemorySessionStore()
	if err := store.Replace(ctx, hermesacp.SessionKey{SessionID: *sessionID}, []hermesacp.SessionStoreReplacement{{
		Key:     hermesacp.SessionKey{SessionID: *sessionID},
		Entries: entries,
	}}); err != nil {
		return err
	}

	return runLoaded(ctx, store, *sessionID, *cwd, *prompt, *hermesPath, *scratchDir, stdout)
}

func runLoadedSession(
	ctx context.Context,
	store hermesacp.SessionStore,
	sessionID string,
	cwd string,
	prompt string,
	hermesPath string,
	scratchDir string,
	stdout io.Writer,
) error {
	clientInput, agentOutput := io.Pipe()
	agentInput, clientOutput := io.Pipe()

	defer clientInput.Close()
	defer clientOutput.Close()

	client := &client{output: stdout}
	conn := acp.NewClientSideConnection(client, clientOutput, clientInput)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 1)
	go func() {
		errs <- serve(
			serveCtx,
			agentInput,
			agentOutput,
			hermesacp.WithExecutablePath(hermesPath),
			hermesacp.WithScratchDir(scratchDir),
			hermesacp.WithSessionStore(store),
			hermesacp.WithLogger(slog.New(slog.DiscardHandler)),
		)
	}()

	_, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		return err
	}

	id := acp.SessionId(sessionID)

	_, err = conn.LoadSession(ctx, hermesacp.LoadSessionRequest(id, cwd))
	if err != nil {
		return err
	}

	defer func() {
		_, _ = conn.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: id})

		cancel()

		_ = agentInput.Close()
		_ = agentOutput.Close()

		<-errs
	}()

	fmt.Fprintln(stdout, "== resume smoke test ==")

	resp, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(id, prompt))
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "\n\nstop reason: %s\n", resp.StopReason)

	return nil
}

func readTranscriptJSONL(path string) ([]hermesacp.SessionStoreEntry, string, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, "", "", err
	}
	defer file.Close()

	var (
		entries   []hermesacp.SessionStoreEntry
		sessionID string
		cwd       string
	)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		entry := hermesacp.SessionStoreEntry(append([]byte(nil), line...))
		entries = append(entries, entry)

		var obj map[string]any
		if json.Unmarshal(entry, &obj) == nil {
			if sessionID == "" {
				sessionID, _ = obj["sessionId"].(string)
			}

			if cwd == "" {
				cwd, _ = obj["cwd"].(string)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, "", "", err
	}

	return entries, sessionID, cwd, nil
}
