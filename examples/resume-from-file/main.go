// Command resume-from-file embeds the agent in-process with a session store
// persisted to a JSON file, so a session can be resumed by a later run.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	hermesacp "github.com/savid/acp-go-hermes"
)

// fileStore is an in-memory store whose whole content is written to one JSON
// file after every write. It is an example, not a durable store.
type fileStore struct {
	path    string
	entries map[string][]acpcore.SessionStoreEntry
}

func loadFileStore(path string) (*fileStore, error) {
	store := &fileStore{path: path, entries: make(map[string][]acpcore.SessionStoreEntry)}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}

	if err != nil {
		return nil, err
	}

	return store, json.Unmarshal(data, &store.entries)
}

func storeKey(key acpcore.SessionKey) string { return key.SessionID + "\x00" + key.Subpath }

func (s *fileStore) save() error {
	data, err := json.Marshal(s.entries)
	if err != nil {
		return err
	}

	return os.WriteFile(s.path, data, 0o600)
}

func (s *fileStore) Append(_ context.Context, key acpcore.SessionKey, entries []acpcore.SessionStoreEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.entries[storeKey(key)] = append(s.entries[storeKey(key)], entries...)

	return s.save()
}

func (s *fileStore) Load(_ context.Context, key acpcore.SessionKey) ([]acpcore.SessionStoreEntry, error) {
	return s.entries[storeKey(key)], nil
}

func (s *fileStore) Replace(_ context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	for key := range s.entries {
		if strings.HasPrefix(key, main.SessionID+"\x00") {
			delete(s.entries, key)
		}
	}

	for _, replacement := range replacements {
		s.entries[storeKey(replacement.Key)] = replacement.Entries
	}

	return s.save()
}

func (s *fileStore) Delete(_ context.Context, key acpcore.SessionKey) error {
	for candidate := range s.entries {
		if strings.HasPrefix(candidate, key.SessionID+"\x00") && (key.Subpath == "" || candidate == storeKey(key)) {
			delete(s.entries, candidate)
		}
	}

	return s.save()
}

func (s *fileStore) ListSessions(context.Context) ([]acpcore.SessionSummary, error) {
	summaries := make([]acpcore.SessionSummary, 0)

	for key := range s.entries {
		if id, subpath, _ := strings.Cut(key, "\x00"); subpath == "" {
			summaries = append(summaries, acpcore.SessionSummary{SessionID: id})
		}
	}

	return summaries, nil
}

func (s *fileStore) ListSubkeys(_ context.Context, key acpcore.SessionKey) ([]string, error) {
	subkeys := make([]string, 0)

	for candidate := range s.entries {
		if id, subpath, _ := strings.Cut(candidate, "\x00"); id == key.SessionID && subpath != "" {
			subkeys = append(subkeys, subpath)
		}
	}

	return subkeys, nil
}

type printer struct{ output io.Writer }

func (p *printer) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	if chunk := params.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
		fmt.Fprint(p.output, chunk.Content.Text.Text)
	}

	return nil
}

func (*printer) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (*printer) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, errors.New("unsupported")
}

func (*printer) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, errors.New("unsupported")
}

func (*printer) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, errors.New("unsupported")
}

func (*printer) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (*printer) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (*printer) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (*printer) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
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
	flags := flag.NewFlagSet("resume-from-file", flag.ContinueOnError)
	flags.SetOutput(stderr)

	storePath := flags.String("store", "sessions.json", "session store file")
	sessionID := flags.String("session", "", "session id to resume; empty starts a new session")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	prompt := strings.TrimSpace(strings.Join(flags.Args(), " "))
	if prompt == "" {
		prompt = "Reply with exactly RESUME_OK and do not use tools."
	}

	store, err := loadFileStore(*storePath)
	if err != nil {
		fmt.Fprintf(stderr, "resume-from-file: %v\n", err)

		return 1
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(stderr, "resume-from-file: %v\n", err)

		return 1
	}

	clientReader, agentWriter := io.Pipe()
	agentReader, clientWriter := io.Pipe()
	served := make(chan error, 1)

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		served <- hermesacp.Serve(serveCtx, agentReader, agentWriter, hermesacp.WithSessionStore(store), hermesacp.WithLogger(slog.New(slog.DiscardHandler)))
	}()

	conn := acp.NewClientSideConnection(&printer{output: stdout}, clientWriter, clientReader)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	id, err := converse(ctx, conn, cwd, acp.SessionId(*sessionID), prompt)

	cancel()

	_ = clientWriter.Close()

	<-served

	if err != nil {
		fmt.Fprintf(stderr, "resume-from-file: %v\n", err)

		return 1
	}

	fmt.Fprintf(stdout, "\nsession %s stored in %s\n", id, *storePath)

	return 0
}

type agentConnection interface {
	Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error)
	NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error)
	ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error)
	Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, error)
	CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error)
}

func converse(ctx context.Context, conn agentConnection, cwd string, sessionID acp.SessionId, prompt string) (acp.SessionId, error) {
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		return "", err
	}

	if sessionID == "" {
		session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
		if err != nil {
			return "", err
		}

		sessionID = session.SessionId
	} else if _, err := conn.ResumeSession(ctx, hermesacp.ResumeSessionRequest(sessionID, cwd)); err != nil {
		return "", err
	}

	defer func() {
		_, _ = conn.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: sessionID})
	}()

	if _, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(sessionID, prompt)); err != nil {
		return "", err
	}

	return sessionID, nil
}
