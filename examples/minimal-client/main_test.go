package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestRunConversation(t *testing.T) {
	conn := &fakeAgentConnection{}
	var stdout bytes.Buffer

	if err := runConversation(context.Background(), conn, "hello", "/repo", &stdout); err != nil {
		t.Fatalf("runConversation returned error: %v", err)
	}
	if !conn.initialized || conn.prompt != "hello" || !conn.closed {
		t.Fatalf("conn state = %#v", conn)
	}
	if !strings.Contains(stdout.String(), "stop reason") {
		t.Fatalf("stdout = %q", stdout.String())
	}

	for name, conn := range map[string]*fakeAgentConnection{
		"initialize": {initErr: errors.New("init failed")},
		"new":        {newErr: errors.New("new failed")},
		"prompt":     {promptErr: errors.New("prompt failed")},
	} {
		if err := runConversation(context.Background(), conn, "hello", "/repo", io.Discard); err == nil {
			t.Fatalf("%s error path succeeded", name)
		}
	}
}

func TestRunUsesInjectedAgentAndMain(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	conn := &fakeAgentConnection{}
	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{
			conn:  conn,
			close: func() { conn.closedStarter = true },
			wait: func() error {
				conn.waited = true

				return nil
			},
		}, nil
	}
	getwd = func() (string, error) { return "/repo", nil }

	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"hello", "hermes"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run code = %d stderr=%q", code, stderr.String())
	}
	if conn.prompt != "hello hermes" || !conn.closedStarter || !conn.waited {
		t.Fatalf("conn state = %#v", conn)
	}

	exitCode := -1
	exit = func(code int) { exitCode = code }
	os.Args = []string{"minimal-client", "from-main"}
	main()
	if exitCode != 0 || conn.prompt != "from-main" {
		t.Fatalf("main exit=%d prompt=%q", exitCode, conn.prompt)
	}
}

func TestRunErrors(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	getwd = func() (string, error) { return "", errors.New("cwd failed") }
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), nil, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "cwd failed") {
		t.Fatalf("cwd failure code=%d stderr=%q", code, stderr.String())
	}

	getwd = func() (string, error) { return "/repo", nil }
	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return nil, errors.New("start failed")
	}
	stderr.Reset()
	if code := run(context.Background(), nil, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "start failed") {
		t.Fatalf("start failure code=%d stderr=%q", code, stderr.String())
	}

	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{promptErr: errors.New("prompt failed")}}, nil
	}
	stderr.Reset()
	if code := run(context.Background(), nil, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "prompt failed") {
		t.Fatalf("prompt failure code=%d stderr=%q", code, stderr.String())
	}
}

func TestClientHelpers(t *testing.T) {
	var out bytes.Buffer
	c := &client{output: &out}
	if err := c.SessionUpdate(context.Background(), acp.SessionNotification{Update: acp.UpdateAgentMessageText("hello")}); err != nil {
		t.Fatalf("SessionUpdate returned error: %v", err)
	}
	if !strings.Contains(out.String(), "hello") {
		t.Fatalf("output = %q", out.String())
	}

	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{Options: []acp.PermissionOption{
		{OptionId: "allow", Kind: acp.PermissionOptionKindAllowOnce},
	}})
	if err != nil || resp.Outcome.Selected == nil || resp.Outcome.Selected.OptionId != "allow" {
		t.Fatalf("permission resp=%#v err=%v", resp, err)
	}
	cancelResp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{})
	if err != nil || cancelResp.Outcome.Cancelled == nil {
		t.Fatalf("cancel permission resp=%#v err=%v", cancelResp, err)
	}

	dir := t.TempDir()
	file := filepath.Join(dir, "nested", "file.txt")
	if _, writeErr := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: file, Content: "body"}); writeErr != nil {
		t.Fatalf("WriteTextFile returned error: %v", writeErr)
	}
	read, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: file})
	if err != nil || read.Content != "body" {
		t.Fatalf("ReadTextFile = %#v err=%v", read, err)
	}
	if _, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: "relative"}); err == nil {
		t.Fatal("ReadTextFile accepted relative path")
	}
	if _, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: "relative"}); err == nil {
		t.Fatal("WriteTextFile accepted relative path")
	}
	if _, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: filepath.Join(dir, "missing.txt")}); err == nil {
		t.Fatal("ReadTextFile missing file succeeded")
	}
	notDir := filepath.Join(dir, "not-dir")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatalf("write not-dir: %v", err)
	}
	if _, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: filepath.Join(notDir, "child.txt"), Content: "body"}); err == nil {
		t.Fatal("WriteTextFile under file path succeeded")
	}

	nilWriterClient := &client{}
	if err := nilWriterClient.SessionUpdate(context.Background(), acp.SessionNotification{}); err != nil {
		t.Fatalf("nil writer SessionUpdate returned error: %v", err)
	}
	if terminal, err := c.CreateTerminal(context.Background(), acp.CreateTerminalRequest{}); err != nil || terminal.TerminalId == "" {
		t.Fatalf("CreateTerminal = %#v err=%v", terminal, err)
	}
	if _, err := c.KillTerminal(context.Background(), acp.KillTerminalRequest{}); err != nil {
		t.Fatalf("KillTerminal returned error: %v", err)
	}
	if output, err := c.TerminalOutput(context.Background(), acp.TerminalOutputRequest{}); err != nil || output.Truncated {
		t.Fatalf("TerminalOutput = %#v err=%v", output, err)
	}
	if _, err := c.ReleaseTerminal(context.Background(), acp.ReleaseTerminalRequest{}); err != nil {
		t.Fatalf("ReleaseTerminal returned error: %v", err)
	}
	if _, err := c.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{}); err != nil {
		t.Fatalf("WaitForTerminalExit returned error: %v", err)
	}
}

func TestStartAgentProcess(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	script := filepath.Join(t.TempDir(), "fake-agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat >/dev/null\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.CommandContext(ctx, script)
	}

	agent, err := startAgentProcess(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("startAgentProcess returned error: %v", err)
	}
	agent.close()
	if err := agent.wait(); err != nil {
		t.Fatalf("fake agent wait returned error: %v", err)
	}

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, script)
		cmd.Stdin = strings.NewReader("")

		return cmd
	}
	if _, err := startAgentProcess(context.Background(), io.Discard, io.Discard); err == nil {
		t.Fatal("startAgentProcess accepted StdinPipe failure")
	}

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, script)
		cmd.Stdout = io.Discard

		return cmd
	}
	if _, err := startAgentProcess(context.Background(), io.Discard, io.Discard); err == nil {
		t.Fatal("startAgentProcess accepted StdoutPipe failure")
	}

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-agent"))
	}
	if _, err := startAgentProcess(context.Background(), io.Discard, io.Discard); err == nil {
		t.Fatal("startAgentProcess accepted Start failure")
	}
}

func replaceGlobals(t *testing.T) func() {
	t.Helper()
	originalStart := startAgent
	originalGetwd := getwd
	originalExit := exit
	originalArgs := os.Args
	originalCommand := commandContext

	return func() {
		startAgent = originalStart
		getwd = originalGetwd
		exit = originalExit
		os.Args = originalArgs
		commandContext = originalCommand
	}
}

type fakeAgentConnection struct {
	initialized   bool
	closed        bool
	closedStarter bool
	waited        bool
	prompt        string
	initErr       error
	newErr        error
	promptErr     error
}

func (c *fakeAgentConnection) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	c.initialized = true

	return acp.InitializeResponse{}, c.initErr
}

func (c *fakeAgentConnection) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionId: "session-1"}, c.newErr
}

func (c *fakeAgentConnection) Prompt(_ context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if len(params.Prompt) > 0 && params.Prompt[0].Text != nil {
		c.prompt = params.Prompt[0].Text.Text
	}

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, c.promptErr
}

func (c *fakeAgentConnection) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	c.closed = true

	return acp.CloseSessionResponse{}, nil
}
