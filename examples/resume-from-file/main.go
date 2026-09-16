// Command resume-from-file embeds the agent in-process with a session store
// persisted to a JSON file, so a session can be resumed by a later run.
package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	hermesacp "github.com/savid/acp-go-hermes"
)

// fileStore is an in-memory store whose whole content is written to one JSON
// file after every write. It is an example, not a durable store, and it
// implements acpcore.SessionStore in full, including tombstone finality.
type fileStore struct {
	mu   sync.Mutex
	path string
	data storeContent
}

// storeContent is the persisted form: the live records, when each was last
// written, and the tombstones that keep a deleted record deleted.
type storeContent struct {
	Entries    map[string][]acpcore.SessionStoreEntry `json:"entries"`
	UpdatedAt  map[string]int64                       `json:"updatedAt"`
	Tombstones map[string]int64                       `json:"tombstones"`
}

func loadFileStore(path string) (*fileStore, error) {
	store := &fileStore{path: path, data: storeContent{
		Entries:    make(map[string][]acpcore.SessionStoreEntry),
		UpdatedAt:  make(map[string]int64),
		Tombstones: make(map[string]int64),
	}}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &store.data); err != nil {
		return nil, err
	}

	if store.data.Entries == nil {
		store.data.Entries = make(map[string][]acpcore.SessionStoreEntry)
	}

	if store.data.UpdatedAt == nil {
		store.data.UpdatedAt = make(map[string]int64)
	}

	if store.data.Tombstones == nil {
		store.data.Tombstones = make(map[string]int64)
	}

	return store, nil
}

func storeKey(key acpcore.SessionKey) string { return key.SessionID + "\x00" + key.Subpath }

func parseStoreKey(encoded string) acpcore.SessionKey {
	id, subpath, _ := strings.Cut(encoded, "\x00")

	return acpcore.SessionKey{SessionID: id, Subpath: subpath}
}

func cloneEntries(entries []acpcore.SessionStoreEntry) []acpcore.SessionStoreEntry {
	if len(entries) == 0 {
		return nil
	}

	cloned := make([]acpcore.SessionStoreEntry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, bytes.Clone(entry))
	}

	return cloned
}

func (s *fileStore) save() error {
	data, err := json.Marshal(s.data)
	if err != nil {
		return err
	}

	return os.WriteFile(s.path, data, 0o600)
}

// tombstoned reports whether a record is deleted, either directly or through
// its session's main tombstone.
func (s *fileStore) tombstoned(key acpcore.SessionKey) bool {
	if _, ok := s.data.Tombstones[storeKey(key)]; ok {
		return true
	}

	if key.Subpath != acpcore.SessionStoreMainSubpath {
		_, ok := s.data.Tombstones[storeKey(acpcore.SessionKey{SessionID: key.SessionID})]

		return ok
	}

	return false
}

func (s *fileStore) Load(_ context.Context, sessionID string) (map[string][]acpcore.SessionStoreEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sessionID == "" || s.tombstoned(acpcore.SessionKey{SessionID: sessionID}) {
		return nil, nil //nolint:nilnil // Missing or tombstoned sessions have no generation.
	}

	var generation map[string][]acpcore.SessionStoreEntry

	for encoded, entries := range s.data.Entries {
		key := parseStoreKey(encoded)
		if key.SessionID != sessionID || s.tombstoned(key) {
			continue
		}

		if generation == nil {
			generation = make(map[string][]acpcore.SessionStoreEntry)
		}

		generation[key.Subpath] = cloneEntries(entries)
	}

	return generation, nil
}

func (s *fileStore) Replace(_ context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if main.SessionID == "" {
		return acpcore.ErrSessionIDRequired
	}

	if main.Subpath != acpcore.SessionStoreMainSubpath {
		return fmt.Errorf("main subpath must be %q", acpcore.SessionStoreMainSubpath)
	}

	if err := checkReplacements(main, replacements); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// A tombstone this write did not create is final.
	if s.tombstoned(main) {
		return nil
	}

	now := time.Now().UnixMilli()

	for encoded := range s.data.Entries {
		if parseStoreKey(encoded).SessionID != main.SessionID {
			continue
		}

		delete(s.data.Entries, encoded)
		delete(s.data.UpdatedAt, encoded)
		s.data.Tombstones[encoded] = now
	}

	for _, replacement := range replacements {
		encoded := storeKey(replacement.Key)
		s.data.Entries[encoded] = cloneEntries(replacement.Entries)
		s.data.UpdatedAt[encoded] = now
		delete(s.data.Tombstones, encoded)
	}

	return s.save()
}

// checkReplacements validates the whole set before any key is written.
func checkReplacements(main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	mainCount := 0
	seen := make(map[acpcore.SessionKey]struct{}, len(replacements))

	for _, replacement := range replacements {
		if replacement.Key.SessionID != main.SessionID {
			return fmt.Errorf("replacement key %q does not belong to session %q", storeKey(replacement.Key), main.SessionID)
		}

		if _, duplicate := seen[replacement.Key]; duplicate {
			return fmt.Errorf("duplicate replacement key %q", storeKey(replacement.Key))
		}

		seen[replacement.Key] = struct{}{}

		if replacement.Key.Subpath == acpcore.SessionStoreMainSubpath {
			mainCount++
		}
	}

	if mainCount != 1 {
		return errors.New("replacements must include the main key exactly once")
	}

	return nil
}

func (s *fileStore) Delete(_ context.Context, key acpcore.SessionKey) error {
	if key.SessionID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	matched := false

	for encoded := range s.data.Entries {
		candidate := parseStoreKey(encoded)
		if candidate.SessionID != key.SessionID {
			continue
		}

		if key.Subpath != acpcore.SessionStoreMainSubpath && candidate.Subpath != key.Subpath {
			continue
		}

		delete(s.data.Entries, encoded)
		delete(s.data.UpdatedAt, encoded)
		s.data.Tombstones[encoded] = now
		matched = true
	}

	if !matched {
		s.data.Tombstones[storeKey(key)] = now
	}

	if key.Subpath == acpcore.SessionStoreMainSubpath {
		s.data.Tombstones[storeKey(acpcore.SessionKey{SessionID: key.SessionID})] = now
	}

	return s.save()
}

func (s *fileStore) ListSessions(context.Context) ([]acpcore.SessionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	summaries := make([]acpcore.SessionSummary, 0)

	for encoded := range s.data.Entries {
		key := parseStoreKey(encoded)
		if key.SessionID == "" || key.Subpath != acpcore.SessionStoreMainSubpath || s.tombstoned(key) {
			continue
		}

		summaries = append(summaries, acpcore.SessionSummary{SessionID: key.SessionID, UpdatedAtUnixMilli: s.data.UpdatedAt[encoded]})
	}

	slices.SortFunc(summaries, func(left, right acpcore.SessionSummary) int {
		if byTime := cmp.Compare(right.UpdatedAtUnixMilli, left.UpdatedAtUnixMilli); byTime != 0 {
			return byTime
		}

		return strings.Compare(left.SessionID, right.SessionID)
	})

	return summaries, nil
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
		session, err := conn.NewSession(ctx, wire.NewSessionRequest(cwd))
		if err != nil {
			return "", err
		}

		sessionID = session.SessionId
	} else if _, err := conn.ResumeSession(ctx, wire.ResumeSessionRequest(sessionID, cwd)); err != nil {
		return "", err
	}

	defer func() {
		_, _ = conn.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: sessionID})
	}()

	if _, err := conn.Prompt(ctx, wire.TextPromptRequest(sessionID, prompt)); err != nil {
		return "", err
	}

	return sessionID, nil
}
