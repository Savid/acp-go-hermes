package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

func TestRunResumeSequence(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	if newAgent(hermesacp.NewInMemorySessionStore()) == nil {
		t.Fatal("default newAgent returned nil")
	}

	agent := &fakeAgentConnection{configOptions: []acp.SessionConfigOption{{
		Select: &acp.SessionConfigOptionSelect{Id: "model", Name: "Model"},
	}}}
	newAgent = func(store hermesacp.SessionStore) agentConnection {
		if store == nil {
			t.Fatal("newAgent received nil store")
		}

		return agent
	}
	getwd = func() (string, error) { return "/repo", nil }

	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), &stdout, &stderr); code != 0 {
		t.Fatalf("run code=%d stderr=%q", code, stderr.String())
	}
	if !agent.initialized || agent.cwd != "/repo" || !agent.closed || agent.resumedID != "session-1" {
		t.Fatalf("agent state = %#v", agent)
	}
	if stdout.String() != "resumed with 1 config options\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunResumeErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		agent *fakeAgentConnection
		getwd func() (string, error)
	}{
		"initialize": {agent: &fakeAgentConnection{initErr: errors.New("init")}, getwd: func() (string, error) { return "/repo", nil }},
		"cwd":        {agent: &fakeAgentConnection{}, getwd: func() (string, error) { return "", errors.New("cwd") }},
		"new":        {agent: &fakeAgentConnection{newErr: errors.New("new")}, getwd: func() (string, error) { return "/repo", nil }},
		"resume":     {agent: &fakeAgentConnection{resumeErr: errors.New("resume")}, getwd: func() (string, error) { return "/repo", nil }},
	} {
		t.Run(name, func(t *testing.T) {
			restore := replaceGlobals(t)
			defer restore()

			newAgent = func(hermesacp.SessionStore) agentConnection { return tc.agent }
			getwd = tc.getwd

			var stderr bytes.Buffer
			if code := run(context.Background(), io.Discard, &stderr); code != 1 {
				t.Fatalf("run code=%d stderr=%q", code, stderr.String())
			}
		})
	}
}

func TestMainUsesInjectedExit(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()

	agent := &fakeAgentConnection{}
	newAgent = func(hermesacp.SessionStore) agentConnection { return agent }
	getwd = func() (string, error) { return "/repo", nil }

	exitCode := -1
	exit = func(code int) { exitCode = code }
	main()
	if exitCode != 0 {
		t.Fatalf("exit code = %d", exitCode)
	}
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
	initialized   bool
	closed        bool
	cwd           string
	resumedID     acp.SessionId
	configOptions []acp.SessionConfigOption
	initErr       error
	newErr        error
	resumeErr     error
}

func (f *fakeAgentConnection) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	f.initialized = true

	return acp.InitializeResponse{}, f.initErr
}

func (f *fakeAgentConnection) NewSession(_ context.Context, request acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	f.cwd = request.Cwd

	return acp.NewSessionResponse{SessionId: "session-1"}, f.newErr
}

func (f *fakeAgentConnection) ResumeSession(_ context.Context, request acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	f.resumedID = request.SessionId

	return acp.ResumeSessionResponse{ConfigOptions: f.configOptions}, f.resumeErr
}

func (f *fakeAgentConnection) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	f.closed = true

	return acp.CloseSessionResponse{}, nil
}
