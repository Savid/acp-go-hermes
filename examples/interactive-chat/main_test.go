package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestRunChatLoop(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	if newAgent() == nil {
		t.Fatal("default newAgent returned nil")
	}

	agent := &fakeAgentConnection{}
	newAgent = func() agentConnection { return agent }
	getwd = func() (string, error) { return "/repo", nil }

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), strings.NewReader("hello\nexit\n"), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run code=%d stderr=%q", code, stderr.String())
	}
	if !agent.initialized || agent.cwd != "/repo" || !agent.closed {
		t.Fatalf("agent state = %#v", agent)
	}
	if len(agent.prompts) != 1 || agent.prompts[0] != "hello" {
		t.Fatalf("prompts = %#v", agent.prompts)
	}
	if !strings.Contains(stdout.String(), "[end_turn]") {
		t.Fatalf("stdout = %q", stdout.String())
	}

	stdout.Reset()
	agent.prompts = nil
	code = run(context.Background(), strings.NewReader("one\n"), &stdout, &stderr)
	if code != 0 || len(agent.prompts) != 1 {
		t.Fatalf("EOF run code=%d prompts=%#v stderr=%q", code, agent.prompts, stderr.String())
	}
}

func TestRunChatPromptErrorContinues(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	agent := &fakeAgentConnection{promptErrs: []error{errors.New("prompt failed"), nil}}
	newAgent = func() agentConnection { return agent }
	getwd = func() (string, error) { return "/repo", nil }

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), strings.NewReader("bad\ngood\n\n"), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run code=%d stderr=%q", code, stderr.String())
	}
	if len(agent.prompts) != 2 || !strings.Contains(stderr.String(), "prompt failed") {
		t.Fatalf("prompts=%#v stderr=%q", agent.prompts, stderr.String())
	}
}

func TestRunChatStartupAndScannerErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		agent agentConnection
		getwd func() (string, error)
		input io.Reader
	}{
		"initialize": {agent: &fakeAgentConnection{initErr: errors.New("init")}, getwd: func() (string, error) { return "/repo", nil }, input: strings.NewReader("")},
		"cwd":        {agent: &fakeAgentConnection{}, getwd: func() (string, error) { return "", errors.New("cwd") }, input: strings.NewReader("")},
		"new":        {agent: &fakeAgentConnection{newErr: errors.New("new")}, getwd: func() (string, error) { return "/repo", nil }, input: strings.NewReader("")},
		"scanner":    {agent: &fakeAgentConnection{}, getwd: func() (string, error) { return "/repo", nil }, input: errReader{}},
	} {
		t.Run(name, func(t *testing.T) {
			restore := replaceGlobals(t)
			defer restore()

			newAgent = func() agentConnection { return tc.agent }
			getwd = tc.getwd

			var stderr bytes.Buffer
			if code := run(context.Background(), tc.input, io.Discard, &stderr); code != 1 {
				t.Fatalf("run code=%d stderr=%q", code, stderr.String())
			}
		})
	}
}

func TestMainUsesInjectedExit(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	agent := &fakeAgentConnection{}
	newAgent = func() agentConnection { return agent }
	getwd = func() (string, error) { return "/repo", nil }
	oldStdin := os.Stdin
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer func() {
		os.Stdin = oldStdin
		_ = readEnd.Close()
	}()
	os.Stdin = readEnd
	if _, err := writeEnd.WriteString("exit\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	_ = writeEnd.Close()

	exitCode := -1
	exit = func(code int) { exitCode = code }
	main()
	if exitCode != 0 {
		t.Fatalf("exit code = %d", exitCode)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func replaceGlobals(t *testing.T) func() {
	t.Helper()
	originalNewAgent := newAgent
	originalGetwd := getwd
	originalExit := exit

	return func() {
		newAgent = originalNewAgent
		getwd = originalGetwd
		exit = originalExit
	}
}

type fakeAgentConnection struct {
	initialized bool
	cwd         string
	closed      bool
	prompts     []string
	promptErrs  []error
	initErr     error
	newErr      error
}

func (f *fakeAgentConnection) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	f.initialized = true

	return acp.InitializeResponse{}, f.initErr
}

func (f *fakeAgentConnection) NewSession(_ context.Context, request acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	f.cwd = request.Cwd

	return acp.NewSessionResponse{SessionId: "session-1"}, f.newErr
}

func (f *fakeAgentConnection) Prompt(_ context.Context, request acp.PromptRequest) (acp.PromptResponse, error) {
	if len(request.Prompt) > 0 && request.Prompt[0].Text != nil {
		f.prompts = append(f.prompts, request.Prompt[0].Text.Text)
	}

	var err error
	if len(f.promptErrs) > 0 {
		err = f.promptErrs[0]
		f.promptErrs = f.promptErrs[1:]
	}

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, err
}

func (f *fakeAgentConnection) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	f.closed = true

	return acp.CloseSessionResponse{}, nil
}
