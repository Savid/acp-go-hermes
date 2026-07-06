package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

func TestReadTranscriptJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte("\n"+
		`{"type":"user","sessionId":"session-1","cwd":"/repo"}`+"\n"+
		`{"type":"assistant"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	entries, sessionID, cwd, err := readTranscriptJSONL(path)
	if err != nil {
		t.Fatalf("readTranscriptJSONL: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	if sessionID != "session-1" {
		t.Fatalf("sessionID = %q", sessionID)
	}
	if cwd != "/repo" {
		t.Fatalf("cwd = %q", cwd)
	}

	missingEntries, missingSessionID, missingCwd, missingErr := readTranscriptJSONL(filepath.Join(t.TempDir(), "missing.jsonl"))
	if missingErr == nil {
		t.Fatal("missing file did not error")
	}
	if missingEntries != nil || missingSessionID != "" || missingCwd != "" {
		t.Fatalf("missing file returned %v %q %q", missingEntries, missingSessionID, missingCwd)
	}
}

func TestRunUsesInferredValuesAndLoadedSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	cwd := t.TempDir()
	if err := os.WriteFile(path, []byte(
		fmt.Sprintf(`{"type":"user","sessionId":"session-1","cwd":%q}`+"\n", cwd)+
			`{"type":"assistant"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	previousRunLoaded := runLoaded
	expectedSessionID := "session-1"
	expectedCwd := cwd
	expectedPrompt := "prompt"
	expectedPath := "/bin/hermes"
	expectedHome := "/home/hermes"
	runLoaded = func(_ context.Context, store hermesacp.SessionStore, sessionID string, gotCwd string, prompt string, hermesPath string, hermesHome string, stdout io.Writer) error {
		if sessionID != expectedSessionID {
			t.Fatalf("sessionID = %q, want %q", sessionID, expectedSessionID)
		}
		if gotCwd != expectedCwd {
			t.Fatalf("cwd = %q, want %q", gotCwd, expectedCwd)
		}
		if prompt != expectedPrompt {
			t.Fatalf("prompt = %q, want %q", prompt, expectedPrompt)
		}
		if hermesPath != expectedPath {
			t.Fatalf("path = %q, want %q", hermesPath, expectedPath)
		}
		if hermesHome != expectedHome {
			t.Fatalf("home = %q, want %q", hermesHome, expectedHome)
		}
		entries, err := store.Load(context.Background(), hermesacp.SessionKey{SessionID: sessionID})
		if err != nil {
			t.Fatalf("store.Load: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("stored entries = %d, want 2", len(entries))
		}
		fmt.Fprint(stdout, "loaded")

		return nil
	}
	t.Cleanup(func() { runLoaded = previousRunLoaded })

	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"-file", path, "-prompt", "prompt", "-path", "/bin/hermes", "-home", "/home/hermes"}, &stdout, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	if stdout.String() != "loaded" {
		t.Fatalf("stdout = %q", stdout.String())
	}

	stdout.Reset()
	expectedSessionID = "explicit"
	expectedPrompt = defaultPrompt
	expectedPath = ""
	expectedHome = ""
	if err := run(context.Background(), []string{"-file", path, "-session", "explicit", "-cwd", cwd}, &stdout, io.Discard); err != nil {
		t.Fatalf("run explicit: %v", err)
	}
}

func TestRunErrors(t *testing.T) {
	entries, sessionID, cwd, err := readTranscriptJSONL("")
	if err == nil || entries != nil || sessionID != "" || cwd != "" {
		t.Fatalf("empty path = %v %q %q %v", entries, sessionID, cwd, err)
	}

	if err := run(context.Background(), []string{"-bad"}, io.Discard, io.Discard); err == nil {
		t.Fatal("flag error did not surface")
	}
	if err := run(context.Background(), []string{"-file", filepath.Join(t.TempDir(), "missing.jsonl")}, io.Discard, io.Discard); err == nil {
		t.Fatal("missing file did not error")
	}

	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"assistant"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-file", path}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "session id is required") {
		t.Fatalf("session id error = %v", err)
	}

	previousGetwd := getwd
	previousRunLoaded := runLoaded
	getwd = func() (string, error) { return "", errors.New("getwd failed") }
	runLoaded = func(context.Context, hermesacp.SessionStore, string, string, string, string, string, io.Writer) error {
		return nil
	}
	t.Cleanup(func() {
		getwd = previousGetwd
		runLoaded = previousRunLoaded
	})
	if err := os.WriteFile(path, []byte(`{"type":"assistant","sessionId":"session-1"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-file", path}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "getwd failed") {
		t.Fatalf("getwd error = %v", err)
	}

	getwd = func() (string, error) { return t.TempDir(), nil }
	runLoaded = func(context.Context, hermesacp.SessionStore, string, string, string, string, string, io.Writer) error {
		return errors.New("load failed")
	}
	if err := run(context.Background(), []string{"-file", path}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "load failed") {
		t.Fatalf("load error = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(cancelled, []string{"-file", path, "-session", "session-1", "-cwd", t.TempDir()}, io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run = %v", err)
	}
}

func TestRunLoadedSessionWithFakeServe(t *testing.T) {
	previousServe := serve
	serve = fakeServe(t, "", nil)
	t.Cleanup(func() { serve = previousServe })

	var stdout bytes.Buffer
	if err := runLoadedSession(context.Background(), hermesacp.NewInMemorySessionStore(), "session-1", t.TempDir(), "prompt", "", "", &stdout); err != nil {
		t.Fatalf("runLoadedSession: %v", err)
	}
	if !strings.Contains(stdout.String(), "== resume smoke test ==") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "stop reason: end_turn") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunLoadedSessionErrors(t *testing.T) {
	previousServe := serve
	t.Cleanup(func() { serve = previousServe })

	for _, tc := range []struct {
		name   string
		method string
		err    *acp.RequestError
	}{
		{name: "initialize", method: acp.AgentMethodInitialize, err: acp.NewInternalError(map[string]any{"error": "init"})},
		{name: "load", method: acp.AgentMethodSessionLoad, err: acp.NewInternalError(map[string]any{"error": "load"})},
		{name: "prompt", method: acp.AgentMethodSessionPrompt, err: acp.NewInternalError(map[string]any{"error": "prompt"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serve = fakeServe(t, tc.method, tc.err)
			if err := runLoadedSession(context.Background(), hermesacp.NewInMemorySessionStore(), "session-1", t.TempDir(), "prompt", "", "", io.Discard); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func fakeServe(t *testing.T, failMethod string, failErr *acp.RequestError) func(context.Context, io.Reader, io.Writer, ...hermesacp.Option) error {
	t.Helper()

	return func(ctx context.Context, input io.Reader, output io.Writer, _ ...hermesacp.Option) error {
		_ = acp.NewConnection(func(_ context.Context, method string, params json.RawMessage) (any, *acp.RequestError) {
			if method == failMethod {
				return nil, failErr
			}

			switch method {
			case acp.AgentMethodInitialize:
				return acp.InitializeResponse{ProtocolVersion: acp.ProtocolVersionNumber}, nil
			case acp.AgentMethodSessionLoad:
				var req acp.LoadSessionRequest
				if err := json.Unmarshal(params, &req); err != nil {
					return nil, acp.NewInvalidParams(map[string]any{"error": err.Error()})
				}

				return acp.LoadSessionResponse{}, nil
			case acp.AgentMethodSessionPrompt:
				return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
			case acp.AgentMethodSessionClose:
				return acp.CloseSessionResponse{}, nil
			default:
				return nil, acp.NewMethodNotFound(method)
			}
		}, output, input)
		<-ctx.Done()

		return nil
	}
}

func TestMainUsesRunMainAndExit(t *testing.T) {
	previousRunMain := runMain
	previousExit := exit
	previousArgs := os.Args
	runMain = func(context.Context, []string, io.Writer, io.Writer) error { return nil }
	exit = func(code int) { panic(fmt.Sprintf("exit %d", code)) }
	os.Args = []string{"resume-from-file"}
	main()

	runMain = func(context.Context, []string, io.Writer, io.Writer) error { return errors.New("boom") }
	func() {
		defer func() {
			r := recover()
			if r != "exit 1" {
				t.Fatalf("main recover = %v", r)
			}
		}()
		main()
	}()
	t.Cleanup(func() {
		runMain = previousRunMain
		exit = previousExit
		os.Args = previousArgs
	})
}

func TestClientMethods(t *testing.T) {
	var stdout bytes.Buffer
	c := &client{output: &stdout}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "file.txt")
	if _, err := c.WriteTextFile(ctx, acp.WriteTextFileRequest{Path: path, Content: "body"}); err != nil {
		t.Fatalf("WriteTextFile: %v", err)
	}
	read, readErr := c.ReadTextFile(ctx, acp.ReadTextFileRequest{Path: path})
	if readErr != nil {
		t.Fatalf("ReadTextFile: %v", readErr)
	}
	if read.Content != "body" {
		t.Fatalf("read content = %q", read.Content)
	}
	if _, err := c.ReadTextFile(ctx, acp.ReadTextFileRequest{Path: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Fatal("missing read did not error")
	}
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteTextFile(ctx, acp.WriteTextFileRequest{Path: filepath.Join(notDir, "child"), Content: "body"}); err == nil {
		t.Fatal("write into file path did not error")
	}

	permission, permErr := c.RequestPermission(ctx, acp.RequestPermissionRequest{})
	if permErr != nil {
		t.Fatalf("RequestPermission: %v", permErr)
	}
	if permission.Outcome.Cancelled == nil {
		t.Fatal("permission outcome not cancelled")
	}
	if err := c.SessionUpdate(ctx, acp.SessionNotification{}); err != nil {
		t.Fatalf("SessionUpdate empty: %v", err)
	}
	if err := c.SessionUpdate(ctx, acp.SessionNotification{Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("hi")}}}); err != nil {
		t.Fatalf("SessionUpdate text: %v", err)
	}
	if stdout.String() != "hi" || c.text.String() != "hi" {
		t.Fatalf("stdout=%q text=%q", stdout.String(), c.text.String())
	}
	if err := (&client{}).SessionUpdate(ctx, acp.SessionNotification{Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("stdout")}}}); err != nil {
		t.Fatalf("SessionUpdate default writer: %v", err)
	}

	terminal, termErr := c.CreateTerminal(ctx, acp.CreateTerminalRequest{})
	if termErr != nil {
		t.Fatalf("CreateTerminal: %v", termErr)
	}
	if terminal.TerminalId != "terminal-1" {
		t.Fatalf("terminal id = %q", terminal.TerminalId)
	}
	if _, err := c.KillTerminal(ctx, acp.KillTerminalRequest{}); err != nil {
		t.Fatalf("KillTerminal: %v", err)
	}
	output, outErr := c.TerminalOutput(ctx, acp.TerminalOutputRequest{})
	if outErr != nil {
		t.Fatalf("TerminalOutput: %v", outErr)
	}
	if output.Truncated {
		t.Fatal("terminal output truncated")
	}
	if _, err := c.ReleaseTerminal(ctx, acp.ReleaseTerminalRequest{}); err != nil {
		t.Fatalf("ReleaseTerminal: %v", err)
	}
	if _, err := c.WaitForTerminalExit(ctx, acp.WaitForTerminalExitRequest{}); err != nil {
		t.Fatalf("WaitForTerminalExit: %v", err)
	}
}

func TestReadTranscriptScannerError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "long.jsonl")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", bufio.MaxScanTokenSize+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, sessionID, cwd, err := readTranscriptJSONL(path)
	if err == nil || entries != nil || sessionID != "" || cwd != "" {
		t.Fatalf("scanner error = %v %q %q %v", entries, sessionID, cwd, err)
	}
}
