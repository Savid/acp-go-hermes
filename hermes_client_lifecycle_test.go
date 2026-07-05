package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestHermesServerHTTPMethodsAndErrors(t *testing.T) {
	ctx := context.Background()
	var seen []string
	var messageBody hermesMessageRequest
	var commandBody hermesCommandRequest
	var forkBody map[string]any
	var createBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if username, password, ok := r.BasicAuth(); !ok || username != "hermes" || password != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("bad auth"))
			return
		}
		seen = append(seen, r.Method+" "+r.URL.RequestURI())
		path := r.URL.EscapedPath()
		switch {
		case path == "/session" && r.Method == http.MethodPost:
			createBody = map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Errorf("decode create body: %v", err)
			}
			writeJSON(t, w, map[string]any{"id": "created", "title": "Created"})
		case path == "/session" && r.Method == http.MethodGet:
			if r.URL.Query().Get("directory") != "/repo" {
				t.Errorf("directory query = %q", r.URL.RawQuery)
			}
			writeJSON(t, w, []map[string]any{{"id": "listed"}})
		case path == "/session/s%2F1" && r.Method == http.MethodGet:
			writeJSON(t, w, map[string]any{"id": "s/1", "title": "Loaded"})
		case path == "/session/s%2F1" && r.Method == http.MethodDelete:
			writeJSON(t, w, map[string]any{"ok": true})
		case path == "/session/s%2F1/message" && r.Method == http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&messageBody); err != nil {
				t.Errorf("decode message body: %v", err)
			}
			writeJSON(t, w, map[string]any{
				"info":  map[string]any{"id": "assistant", "sessionID": "s/1", "role": "assistant", "finish": "stop"},
				"parts": []map[string]any{{"id": "part-1", "sessionID": "s/1", "messageID": "assistant", "type": "text", "text": "ok"}},
			})
		case path == "/session/s%2F1/message" && r.Method == http.MethodGet:
			writeJSON(t, w, []map[string]any{{
				"info":  map[string]any{"id": "history", "sessionID": "s/1", "role": "assistant"},
				"parts": []map[string]any{{"id": "history-part", "sessionID": "s/1", "messageID": "history", "type": "text", "text": "ok"}},
			}})
		case path == "/command" && r.Method == http.MethodGet:
			writeJSON(t, w, []map[string]any{{
				"name":        "review",
				"description": "Review",
				"agent":       "build",
				"model":       "openai/gpt-test",
				"source":      "command",
				"template":    "hidden",
				"subtask":     true,
				"hints":       []string{"$ARGUMENTS"},
			}})
		case path == "/session/s%2F1/command" && r.Method == http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&commandBody); err != nil {
				t.Errorf("decode command body: %v", err)
			}
			writeJSON(t, w, map[string]any{
				"info":  map[string]any{"id": "assistant-command", "sessionID": "s/1", "role": "assistant", "finish": "stop"},
				"parts": []map[string]any{{"id": "part-command", "sessionID": "s/1", "messageID": "assistant-command", "type": "text", "text": "ok"}},
			})
		case path == "/session/status" && r.Method == http.MethodGet:
			writeJSON(t, w, map[string]any{"s/1": map[string]any{"type": "idle"}})
		case path == "/session/s%2F1/abort" && r.Method == http.MethodPost:
			writeJSON(t, w, map[string]any{"ok": true})
		case path == "/session/s%2F1/fork" && r.Method == http.MethodPost:
			forkBody = map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&forkBody); err != nil {
				t.Errorf("decode fork body: %v", err)
			}
			writeJSON(t, w, map[string]any{"id": "forked"})
		case path == "/session/s%2F1/todo" && r.Method == http.MethodGet:
			writeJSON(t, w, []map[string]any{{"id": "todo", "content": "Do it"}})
		case path == "/config/providers" && r.Method == http.MethodGet:
			writeJSON(t, w, map[string]any{"providers": []map[string]any{{"id": "openai", "models": map[string]any{}}}})
		case path == "/agent" && r.Method == http.MethodGet:
			writeJSON(t, w, []map[string]any{{"name": "build"}})
		case path == "/empty" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusNoContent)
		case path == "/invalid-json" && r.Method == http.MethodGet:
			_, _ = w.Write([]byte("{"))
		case path == "/status" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte("short and stout"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := &hermesServer{
		httpClient: server.Client(),
		baseURL:    server.URL,
		username:   "hermes",
		password:   "secret",
		events:     make(chan hermesEvent),
		errs:       make(chan error),
		closed:     make(chan struct{}),
	}

	created, err := client.CreateSession(ctx, "Created")
	if err != nil || created.ID != "created" || createBody["title"] != "Created" {
		t.Fatalf("CreateSession = %#v body=%#v err=%v", created, createBody, err)
	}
	created, err = client.CreateSession(ctx, "")
	if err != nil || len(createBody) != 0 {
		t.Fatalf("CreateSession empty = %#v body=%#v err=%v", created, createBody, err)
	}
	got, err := client.GetSession(ctx, "s/1")
	if err != nil || got.ID != "s/1" {
		t.Fatalf("GetSession = %#v err=%v", got, err)
	}
	listed, err := client.ListSessions(ctx, "/repo")
	if err != nil || len(listed) != 1 || listed[0].ID != "listed" {
		t.Fatalf("ListSessions = %#v err=%v", listed, err)
	}
	if err := client.DeleteSession(ctx, "s/1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	message, err := client.SendMessage(ctx, "s/1", hermesMessageRequest{
		MessageID: "user-1",
		Model:     &hermesModelSelector{ProviderID: "openai", ModelID: "gpt-test"},
		Agent:     "build",
		Parts:     []map[string]any{{"type": "text", "text": "hello"}},
	})
	if err != nil || message.Info.ID != "assistant" || messageBody.MessageID != "user-1" ||
		messageBody.Model.ModelID != "gpt-test" || messageBody.Agent != "build" || len(messageBody.Parts) != 1 {
		t.Fatalf("SendMessage = %#v body=%#v err=%v", message, messageBody, err)
	}
	messages, err := client.Messages(ctx, "s/1")
	if err != nil || len(messages) != 1 || messages[0].Info.ID != "history" {
		t.Fatalf("Messages = %#v err=%v", messages, err)
	}
	commands, err := client.Commands(ctx)
	if err != nil || len(commands) != 1 || commands[0].Name != "review" || commands[0].Template == nil || len(commands[0].Hints) != 1 {
		t.Fatalf("Commands = %#v err=%v", commands, err)
	}
	command, err := client.RunCommand(ctx, "s/1", hermesCommandRequest{
		MessageID: "user-2",
		Agent:     "build",
		Model:     "openai/gpt-test",
		Command:   "review",
		Arguments: "args",
		Parts:     []map[string]any{{"type": "file", "mime": "image/png", "url": "data:image/png;base64,AA=="}},
	})
	if err != nil || command.Info.ID != "assistant-command" || commandBody.MessageID != "user-2" ||
		commandBody.Model != "openai/gpt-test" || commandBody.Command != "review" || commandBody.Arguments != "args" ||
		len(commandBody.Parts) != 1 {
		t.Fatalf("RunCommand = %#v body=%#v err=%v", command, commandBody, err)
	}
	status, err := client.SessionStatus(ctx)
	if err != nil || status["s/1"].Type != "idle" {
		t.Fatalf("SessionStatus = %#v err=%v", status, err)
	}
	if err := client.Abort(ctx, "s/1"); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	forked, err := client.Fork(ctx, "s/1", "message-1")
	if err != nil || forked.ID != "forked" || forkBody["messageID"] != "message-1" {
		t.Fatalf("Fork = %#v body=%#v err=%v", forked, forkBody, err)
	}
	forked, err = client.Fork(ctx, "s/1", "")
	if err != nil || len(forkBody) != 0 {
		t.Fatalf("Fork empty = %#v body=%#v err=%v", forked, forkBody, err)
	}
	todos, err := client.Todos(ctx, "s/1")
	if err != nil || len(todos) != 1 || todos[0].ID != "todo" {
		t.Fatalf("Todos = %#v err=%v", todos, err)
	}
	providers, err := client.ConfigProviders(ctx)
	if err != nil || len(providers.Providers) != 1 || providers.Raw == nil {
		t.Fatalf("ConfigProviders = %#v err=%v", providers, err)
	}
	agents, err := client.Agents(ctx)
	if err != nil || len(agents) != 1 || agents[0].Name != "build" {
		t.Fatalf("Agents = %#v err=%v", agents, err)
	}
	if err := client.getJSON(ctx, "/empty", nil, nil); err != nil {
		t.Fatalf("empty getJSON: %v", err)
	}
	if err := client.getJSON(ctx, "/invalid-json", nil, &map[string]any{}); err == nil {
		t.Fatal("invalid json unexpectedly succeeded")
	}
	if err := client.getJSON(ctx, "/status", nil, &map[string]any{}); err == nil || !strings.Contains(err.Error(), "short and stout") {
		t.Fatalf("status error = %v", err)
	}
	client.baseURL = ":// bad url"
	if err := client.getJSON(ctx, "/bad", nil, &map[string]any{}); err == nil {
		t.Fatal("bad request URL unexpectedly succeeded")
	}
	if !containsString(seen, "GET /session?directory=%2Frepo") {
		t.Fatalf("seen paths = %#v", seen)
	}
}

func TestHermesSendMessageErrors(t *testing.T) {
	ctx := context.Background()
	if err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{Role: "assistant", Finish: "error"}}); err == nil {
		t.Fatal("assistant finish error accepted")
	}
	err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{
		Role:  "assistant",
		Error: &nativeError{Message: "provider failed"},
	}})
	if err == nil || !strings.Contains(err.Error(), "provider failed") {
		t.Fatalf("assistant error = %v", err)
	}
	if err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{Error: &nativeError{}}}); err == nil {
		t.Fatal("empty assistant error accepted")
	}
	if err := assistantMessageError(nativeMessage{Info: nativeMessageInfo{Role: "assistant", Finish: "stop"}}); err != nil {
		t.Fatalf("non-error assistant rejected: %v", err)
	}

	for _, tt := range []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		command bool
	}{
		{
			name: "post error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/session/s/message" && r.Method == http.MethodPost {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				http.NotFound(w, r)
			},
		},
		{
			name:    "command post error",
			command: true,
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/session/s/command" && r.Method == http.MethodPost {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte("unknown command"))
					return
				}
				http.NotFound(w, r)
			},
		},
		{
			name: "assistant error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/session/s/message" && r.Method == http.MethodPost {
					writeJSON(t, w, map[string]any{
						"info": map[string]any{
							"id":     "assistant",
							"role":   "assistant",
							"finish": "error",
							"error":  map[string]any{"message": "provider failed"},
						},
					})
					return
				}
				http.NotFound(w, r)
			},
		},
		{
			name:    "command assistant error",
			command: true,
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/session/s/command" && r.Method == http.MethodPost {
					writeJSON(t, w, map[string]any{
						"info": map[string]any{
							"id":     "assistant",
							"role":   "assistant",
							"finish": "error",
							"error":  map[string]any{"message": "command failed"},
						},
					})
					return
				}
				http.NotFound(w, r)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if username, password, ok := r.BasicAuth(); !ok || username != "hermes" || password != "secret" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				tt.handler(w, r)
			}))
			defer server.Close()
			client := &hermesServer{
				httpClient: server.Client(),
				baseURL:    server.URL,
				username:   "hermes",
				password:   "secret",
			}
			var err error
			if tt.command {
				_, err = client.RunCommand(ctx, "s", hermesCommandRequest{Command: "review", Arguments: ""})
			} else {
				_, err = client.SendMessage(ctx, "s", hermesMessageRequest{Parts: []map[string]any{{"type": "text", "text": "hello"}}})
			}
			if err == nil {
				t.Fatal("native send unexpectedly succeeded")
			}
		})
	}
}

func TestStartHermesServerWithFakeExecutable(t *testing.T) {
	helper := fakeHermesExecutable(t)
	root := t.TempDir()
	logger := slog.New(slog.DiscardHandler)
	client, err := startHermesServer(context.Background(), hermesStartOptions{
		ACPSessionID:     "session/one",
		Root:             root,
		Cwd:              t.TempDir(),
		ExecutablePath:   helper,
		DefaultModel:     "openai/gpt-test",
		Env:              map[string]string{"BASE_ENV": "base"},
		AdditionalEnv:    map[string]string{"EXTRA_ENV": "extra"},
		Pure:             true,
		QuestionTool:     true,
		LogLevel:         "DEBUG",
		Permission:       "allow",
		MinimumVersion:   "1.0.0",
		HealthTimeout:    5 * time.Second,
		Logger:           logger,
		SkipVersionGate:  false,
		ExpectedNativeID: "native",
	})
	if err != nil {
		t.Fatalf("startHermesServer: %v", err)
	}
	server := client.(*hermesServer)
	if server.xdg.Root == "" || !strings.Contains(filepath.Base(server.xdg.Root), "session_one") {
		t.Fatalf("xdg dirs = %#v", server.xdg)
	}
	if _, err := os.Stat(filepath.Join(server.xdg.State, leaseFileName)); err != nil {
		t.Fatalf("lease was not written: %v", err)
	}
	configData, err := os.ReadFile(filepath.Join(server.xdg.Config, "hermes", "hermes.json"))
	if err != nil {
		t.Fatalf("permission config was not written: %v", err)
	}
	if !strings.Contains(string(configData), `"*": "allow"`) {
		t.Fatalf("permission config = %s", string(configData))
	}
	if server.Events() == nil || server.EventErrors() == nil || server.XDGDirs().Root == "" {
		t.Fatalf("server channels/dirs not initialized: %#v", server)
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(server.xdg.State, leaseFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease after close err = %v", err)
	}
}

func TestStartHermesServerFaultInjection(t *testing.T) {
	ctx := context.Background()

	t.Run("defaults and start error", func(t *testing.T) {
		restoreHermesClientSeams(t)
		xdg := testXDGDirs(t)
		var executable string
		hermesCommandContext = func(ctx context.Context, name string, _ ...string) *exec.Cmd {
			executable = name
			return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-hermes"))
		}
		_, err := startHermesServer(ctx, hermesStartOptions{ExistingXDG: xdg})
		if err == nil {
			t.Fatal("missing executable unexpectedly started")
		}
		if executable != "hermes" {
			t.Fatalf("default executable = %q", executable)
		}
	})

	t.Run("reap failure", func(t *testing.T) {
		if _, err := startHermesServer(ctx, hermesStartOptions{Root: "["}); err == nil {
			t.Fatal("invalid reap glob unexpectedly succeeded")
		}
	})

	t.Run("create xdg failure", func(t *testing.T) {
		_, err := startHermesServer(ctx, hermesStartOptions{
			Root:         t.TempDir(),
			ACPSessionID: acpSessionIDString(string([]byte{0})),
		})
		if err == nil {
			t.Fatal("invalid session xdg path unexpectedly succeeded")
		}
	})

	t.Run("ensure existing xdg failure", func(t *testing.T) {
		_, err := startHermesServer(ctx, hermesStartOptions{
			ExistingXDG: xdgDirs{Root: filepath.Join(t.TempDir(), "root")},
		})
		if err == nil {
			t.Fatal("incomplete existing xdg unexpectedly succeeded")
		}
	})

	t.Run("permission config failure", func(t *testing.T) {
		_, err := startHermesServer(ctx, hermesStartOptions{
			ExistingXDG: testXDGDirs(t),
			Permission:  "deny",
		})
		if err == nil {
			t.Fatal("invalid permission unexpectedly started")
		}
	})

	t.Run("allocate port failure", func(t *testing.T) {
		restoreHermesClientSeams(t)
		hermesListen = func(string, string) (net.Listener, error) {
			return nil, errors.New("listen failed")
		}
		if _, err := startHermesServer(ctx, hermesStartOptions{ExistingXDG: testXDGDirs(t)}); err == nil {
			t.Fatal("listen error was ignored")
		}
	})

	t.Run("password entropy failure", func(t *testing.T) {
		restoreHermesClientSeams(t)
		hermesRandReader = errorReader{err: errors.New("entropy failed")}
		if _, err := startHermesServer(ctx, hermesStartOptions{ExistingXDG: testXDGDirs(t)}); err == nil {
			t.Fatal("entropy error was ignored")
		}
	})

	t.Run("stdout pipe failure", func(t *testing.T) {
		restoreHermesClientSeams(t)
		hermesCommandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestFakeHermesServerProcessHelper")
			cmd.Stdout = io.Discard
			return cmd
		}
		if _, err := startHermesServer(ctx, hermesStartOptions{ExistingXDG: testXDGDirs(t)}); err == nil {
			t.Fatal("stdout pipe error was ignored")
		}
	})

	t.Run("stderr pipe failure", func(t *testing.T) {
		restoreHermesClientSeams(t)
		hermesCommandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestFakeHermesServerProcessHelper")
			cmd.Stderr = io.Discard
			return cmd
		}
		if _, err := startHermesServer(ctx, hermesStartOptions{ExistingXDG: testXDGDirs(t)}); err == nil {
			t.Fatal("stderr pipe error was ignored")
		}
	})

	t.Run("lease write failure", func(t *testing.T) {
		restoreHermesClientSeams(t)
		hermesWriteLease = func(string, serverLease) error {
			return errors.New("lease failed")
		}
		if _, err := startHermesServer(ctx, hermesStartOptions{ExistingXDG: testXDGDirs(t)}); err == nil {
			t.Fatal("lease error was ignored")
		}
	})

	t.Run("post-start lease write failure kills process", func(t *testing.T) {
		restoreHermesClientSeams(t)
		helper := fakeHermesExecutable(t)
		writeCount := 0
		killed := false
		hermesWriteLease = func(string, serverLease) error {
			writeCount++
			if writeCount == 2 {
				return errors.New("post-start lease failed")
			}
			return nil
		}
		hermesKillProcess = func(*exec.Cmd) error {
			killed = true
			return nil
		}
		_, err := startHermesServer(ctx, hermesStartOptions{
			Root:           t.TempDir(),
			ExecutablePath: helper,
			HealthTimeout:  5 * time.Second,
		})
		if err == nil || !strings.Contains(err.Error(), "post-start lease failed") {
			t.Fatalf("post-start lease err = %v", err)
		}
		if !killed {
			t.Fatal("post-start lease failure did not kill process")
		}
	})

	t.Run("readiness failure closes process", func(t *testing.T) {
		helper := fakeHermesExecutable(t)
		_, err := startHermesServer(ctx, hermesStartOptions{
			Root:            t.TempDir(),
			ExecutablePath:  helper,
			MinimumVersion:  "99.0.0",
			HealthTimeout:   2 * time.Second,
			SkipVersionGate: false,
			Logger:          slog.New(slog.DiscardHandler),
		})
		if err == nil || !strings.Contains(err.Error(), "below minimum") {
			t.Fatalf("readiness error = %v", err)
		}
	})
}

func TestHermesServerReadinessFailuresAndStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	t.Run("version gate", func(t *testing.T) {
		client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/global/health":
				writeJSON(t, w, map[string]any{"healthy": true, "version": "1.0.0"})
			case "/doc":
				writeJSON(t, w, fullHermesDoc())
			case "/event":
				writeSSE(t, w, `{"type":"server.connected","properties":{}}`)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		defer closeServer()
		if err := client.waitReady(ctx, ctx, hermesStartOptions{MinimumVersion: "9.0.0"}); err == nil {
			t.Fatal("old version unexpectedly passed readiness")
		}
	})

	t.Run("wrong first event", func(t *testing.T) {
		client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/global/health":
				writeJSON(t, w, map[string]any{"healthy": true, "version": "9.0.0"})
			case "/doc":
				writeJSON(t, w, fullHermesDoc())
			case "/event":
				writeSSE(t, w, `{"type":"other","properties":{}}`)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		defer closeServer()
		if err := client.waitReady(ctx, ctx, hermesStartOptions{SkipVersionGate: true}); err == nil {
			t.Fatal("wrong first event unexpectedly passed readiness")
		}
	})

	t.Run("event stream status and malformed data", func(t *testing.T) {
		client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/event":
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("bad stream"))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		defer closeServer()
		if err := client.readEventStream(ctx); err == nil || !strings.Contains(err.Error(), "bad stream") {
			t.Fatalf("stream status error = %v", err)
		}

		client, closeServer = readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/event" {
				writeSSE(t, w, `{`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		})
		defer closeServer()
		if err := client.readEventStream(ctx); err == nil {
			t.Fatal("malformed event stream unexpectedly succeeded")
		}
	})

	t.Run("health timeout with last error", func(t *testing.T) {
		client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/global/health" {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("bad health"))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		})
		defer closeServer()
		shortCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := client.waitReady(shortCtx, shortCtx, hermesStartOptions{SkipVersionGate: true}); err == nil ||
			!strings.Contains(err.Error(), "health check failed") {
			t.Fatalf("health error = %v", err)
		}
	})

	t.Run("health timeout while unhealthy", func(t *testing.T) {
		client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/global/health" {
				writeJSON(t, w, map[string]any{"healthy": false, "version": "9.0.0"})
				return
			}
			w.WriteHeader(http.StatusNotFound)
		})
		defer closeServer()
		shortCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := client.waitReady(shortCtx, shortCtx, hermesStartOptions{SkipVersionGate: true}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unhealthy timeout error = %v", err)
		}
	})

	t.Run("doc load and validation errors", func(t *testing.T) {
		tests := map[string]func(http.ResponseWriter){
			"load": func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("doc failed"))
			},
			"validate": func(w http.ResponseWriter) { writeJSON(t, w, map[string]any{"paths": map[string]any{}}) },
		}
		for name, docHandler := range tests {
			t.Run(name, func(t *testing.T) {
				client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/global/health":
						writeJSON(t, w, map[string]any{"healthy": true, "version": "9.0.0"})
					case "/doc":
						docHandler(w)
					default:
						w.WriteHeader(http.StatusNotFound)
					}
				})
				defer closeServer()
				if err := client.waitReady(ctx, ctx, hermesStartOptions{SkipVersionGate: true}); err == nil {
					t.Fatal("doc readiness error was ignored")
				}
			})
		}
	})

	t.Run("event stream readiness error", func(t *testing.T) {
		client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/global/health":
				writeJSON(t, w, map[string]any{"healthy": true, "version": "9.0.0"})
			case "/doc":
				writeJSON(t, w, fullHermesDoc())
			case "/event":
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("event failed"))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		defer closeServer()
		if err := client.waitReady(ctx, ctx, hermesStartOptions{SkipVersionGate: true}); err == nil ||
			!strings.Contains(err.Error(), "event stream failed") {
			t.Fatalf("event readiness error = %v", err)
		}
	})

	t.Run("event wait context deadline", func(t *testing.T) {
		client, closeServer := readinessClient(t, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/global/health":
				writeJSON(t, w, map[string]any{"healthy": true, "version": "9.0.0"})
			case "/doc":
				writeJSON(t, w, fullHermesDoc())
			case "/event":
				time.Sleep(50 * time.Millisecond)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		defer closeServer()
		shortCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := client.waitReady(shortCtx, context.Background(), hermesStartOptions{SkipVersionGate: true}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("event wait error = %v", err)
		}
	})
}

func TestHermesHTTPAndSSEFaultBranches(t *testing.T) {
	ctx := context.Background()
	client := &hermesServer{
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       errorReadCloser{err: errors.New("body read failed")},
			}, nil
		})},
		baseURL:  "http://hermes.test",
		username: "hermes",
		password: "secret",
		events:   make(chan hermesEvent),
		errs:     make(chan error, 1),
		closed:   make(chan struct{}),
	}
	client.httpClient.Timeout = 30 * time.Second
	if eventClient := client.eventHTTPClient(); eventClient.Timeout != 0 || eventClient == client.httpClient || eventClient.Transport == nil {
		t.Fatalf("eventHTTPClient = %#v", eventClient)
	}
	client.httpClient.Timeout = 0
	if httpClient := (&hermesServer{}).eventHTTPClient(); httpClient != http.DefaultClient {
		t.Fatalf("nil eventHTTPClient = %#v", httpClient)
	}
	if err := client.doJSON(ctx, http.MethodPost, "/bad-body", nil, map[string]any{"bad": func() {}}, nil); err == nil {
		t.Fatal("doJSON accepted unmarshalable request body")
	}
	if err := client.doJSON(ctx, http.MethodGet, "/copy-error", nil, nil, nil); err == nil {
		t.Fatal("doJSON ignored response body copy error")
	}

	client.baseURL = ":// bad url"
	if err := client.readEventStream(ctx); err == nil {
		t.Fatal("readEventStream accepted bad URL")
	}

	client.baseURL = "http://hermes.test"
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("event dial failed")
	})}
	if err := client.readEventStream(ctx); err == nil {
		t.Fatal("readEventStream ignored HTTP client error")
	}

	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\n"))
		_, _ = w.Write([]byte("data: \"server.connected\",\"properties\":{}}\n\n"))
	}))
	defer stream.Close()
	client.httpClient = stream.Client()
	client.baseURL = stream.URL
	client.events = make(chan hermesEvent, 1)
	if err := client.readEventStream(ctx); !errors.Is(err, errHermesSSEDisconnect) {
		t.Fatalf("multi-line clean EOF readEventStream error = %v", err)
	}
	if event := <-client.events; event.Type != "server.connected" {
		t.Fatalf("event = %#v", event)
	}

	unterminatedStream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"server.connected\",\"properties\":{}}\n"))
	}))
	defer unterminatedStream.Close()
	client.httpClient = unterminatedStream.Client()
	client.baseURL = unterminatedStream.URL
	client.events = make(chan hermesEvent, 1)
	if err := client.readEventStream(ctx); !errors.Is(err, errHermesSSEDisconnect) {
		t.Fatalf("unterminated clean EOF readEventStream error = %v", err)
	}
	if event := <-client.events; event.Type != "server.connected" {
		t.Fatalf("unterminated event = %#v", event)
	}

	cancelOnEOF, cancelEOF := context.WithCancel(context.Background())
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       eofCancelReadCloser{cancel: cancelEOF},
		}, nil
	})}
	client.baseURL = "http://hermes.test"
	client.events = make(chan hermesEvent, 1)
	if err := client.readEventStream(cancelOnEOF); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-EOF cancelled stream error = %v", err)
	}

	closedFlushStream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"server.connected\",\"properties\":{}}\n"))
	}))
	defer closedFlushStream.Close()
	client.httpClient = closedFlushStream.Client()
	client.baseURL = closedFlushStream.URL
	client.events = make(chan hermesEvent)
	close(client.closed)
	if err := client.readEventStream(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("closed stream error = %v", err)
	}

	cancelledStream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"server.connected\",\"properties\":{}}\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer cancelledStream.Close()
	client = &hermesServer{
		httpClient: cancelledStream.Client(),
		baseURL:    cancelledStream.URL,
		username:   "hermes",
		password:   "secret",
		events:     make(chan hermesEvent),
		errs:       make(chan error, 1),
		closed:     make(chan struct{}),
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.readEventStream(streamCtx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled stream error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled stream did not return")
	}
}

func TestHermesReadEventsDropsErrorWhenChannelFullAndReconnects(t *testing.T) {
	restoreHermesClientSeams(t)
	requests := 0
	var client *hermesServer
	client = &hermesServer{
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			if requests == 2 {
				close(client.closed)
			}
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Status:     "500 Internal Server Error",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("bad stream")),
			}, nil
		})},
		baseURL:  "http://hermes.test",
		username: "hermes",
		password: "secret",
		events:   make(chan hermesEvent, 1),
		errs:     make(chan error, 1),
		closed:   make(chan struct{}),
	}
	client.errs <- errors.New("already full")
	delayCalls := 0
	hermesAfter = func(time.Duration) <-chan time.Time {
		delayCalls++
		if delayCalls > 1 {
			return make(chan time.Time)
		}
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	client.readEvents(context.Background())
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestHermesServerCloseTimeoutAndContext(t *testing.T) {
	for name, cancelled := range map[string]bool{"timeout": false, "context": true} {
		t.Run(name, func(t *testing.T) {
			restoreHermesClientSeams(t)
			cmd := &exec.Cmd{Process: &os.Process{Pid: 1234}}
			hermesTerminateProcess = func(*exec.Cmd) error { return nil }
			hermesKillProcess = func(*exec.Cmd) error { return nil }
			hermesWaitCommand = func(*exec.Cmd) error {
				select {}
			}
			server := &hermesServer{
				cmd:    cmd,
				cancel: func() {},
				closed: make(chan struct{}),
				log:    slog.New(slog.DiscardHandler),
			}
			ctx := context.Background()
			if cancelled {
				cancelCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelCtx
				hermesAfter = func(time.Duration) <-chan time.Time {
					return make(chan time.Time)
				}
			} else {
				hermesShutdownTimeout = time.Millisecond
			}
			if err := server.Close(ctx); err == nil {
				t.Fatal("Close unexpectedly succeeded")
			}
		})
	}
}

func TestXDGLeaseEnvAndPipeHelpers(t *testing.T) {
	root := t.TempDir()
	xdg, err := createXDGDirs(root, "")
	if err != nil {
		t.Fatalf("createXDGDirs: %v", err)
	}
	if filepath.Base(xdg.Root) != "session" {
		t.Fatalf("default xdg root = %#v", xdg)
	}
	if err := ensureXDGDirs(xdgDirs{Root: "", Data: "x", Config: "x", Cache: "x", State: "x"}); err == nil {
		t.Fatal("ensureXDGDirs accepted empty root")
	}
	permissionConfig, err := materializeHermesPermissionConfig(xdg, "")
	if err != nil || !strings.Contains(permissionConfig, `"*": "ask"`) {
		t.Fatalf("default permission config = %q err=%v", permissionConfig, err)
	}
	permissionFile := filepath.Join(xdg.Config, "hermes", "hermes.json")
	info, err := os.Stat(permissionFile)
	if err != nil {
		t.Fatalf("permission config file stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("permission config file mode = %v", info.Mode().Perm())
	}
	if _, err := materializeHermesPermissionConfig(xdg, "deny"); err == nil {
		t.Fatal("unsupported permission config accepted")
	}
	configRootFile := filepath.Join(t.TempDir(), "config-file")
	if err := os.WriteFile(configRootFile, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := materializeHermesPermissionConfig(xdgDirs{Config: configRootFile}, "ask"); err == nil {
		t.Fatal("permission config mkdir failure ignored")
	}
	configRoot := filepath.Join(t.TempDir(), "config")
	if err := os.MkdirAll(filepath.Join(configRoot, "hermes", "hermes.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := materializeHermesPermissionConfig(xdgDirs{Config: configRoot}, "ask"); err == nil {
		t.Fatal("permission config write failure ignored")
	}
	port, err := allocatePort()
	if err != nil || port <= 0 {
		t.Fatalf("allocatePort = %d err=%v", port, err)
	}
	password, err := randomPassword()
	if err != nil || password == "" {
		t.Fatalf("randomPassword = %q err=%v", password, err)
	}
	if err := writeLease(xdg.State, serverLease{PID: 0, Port: port, PasswordHash: passwordHash(password)}); err != nil {
		t.Fatalf("writeLease: %v", err)
	}
	badRoot := string([]byte{0})
	if err := writeLease(badRoot, serverLease{}); err == nil {
		t.Fatal("writeLease accepted invalid path")
	}
	if err := os.MkdirAll(filepath.Join(root, "bad", "state"), 0o700); err != nil {
		t.Fatalf("mkdir bad lease: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "bad", "state", leaseFileName), []byte("{"), 0o600); err != nil {
		t.Fatalf("write bad lease: %v", err)
	}
	if err := reapStaleLeases(root, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("reapStaleLeases: %v", err)
	}
	if err := reapStaleLeases("", nil); err != nil {
		t.Fatalf("empty reapStaleLeases: %v", err)
	}
	env := mergeProcessEnv(map[string]string{"": "skip", "A": "1"}, map[string]string{"A": "2", "B": "3"})
	if env["A"] != "2" || env["B"] != "3" {
		t.Fatalf("merged env = %#v", env)
	}
	drainProcessPipe(slog.New(slog.DiscardHandler), "test", strings.NewReader("one\ntwo\n"))
	for _, value := range []any{float64(-1), int(-1), json.Number("bad")} {
		if got, ok := intFromNumber(value); ok || got != 0 {
			t.Fatalf("intFromNumber(%#v) = %d, %v", value, got, ok)
		}
	}
	if got, ok := intFromNumber(json.Number("12")); !ok || got != 12 {
		t.Fatalf("intFromNumber json number = %d, %v", got, ok)
	}
}

func TestPortPasswordLeaseAndReaperFaultInjection(t *testing.T) {
	restoreHermesClientSeams(t)

	hermesListen = func(string, string) (net.Listener, error) {
		return fakeListener{addr: stringAddr("not tcp")}, nil
	}
	if _, err := allocatePort(); err == nil {
		t.Fatal("allocatePort accepted non-TCP listener")
	}
	hermesListen = func(string, string) (net.Listener, error) {
		return nil, errors.New("listen failed")
	}
	if _, err := allocatePort(); err == nil {
		t.Fatal("allocatePort ignored listen error")
	}

	hermesRandReader = errorReader{err: errors.New("entropy failed")}
	if _, err := randomPassword(); err == nil {
		t.Fatal("randomPassword ignored entropy error")
	}

	hermesMarshalIndent = func(any, string, string) ([]byte, error) {
		return nil, errors.New("marshal failed")
	}
	if err := writeLease(t.TempDir(), serverLease{}); err == nil {
		t.Fatal("writeLease ignored marshal error")
	}
	if _, err := materializeHermesPermissionConfig(testXDGDirs(t), "ask"); err == nil {
		t.Fatal("permission config ignored marshal error")
	}

	if err := reapStaleLeases("[", slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("reapStaleLeases accepted malformed glob")
	}
	root := t.TempDir()
	stateDir := filepath.Join(root, "session", "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(stateDir, leaseFileName)); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := reapStaleLeases(root, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("reap stale read error: %v", err)
	}
}

func TestLeaseReaperVerifiesProcessIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process identity is platform-specific")
	}
	t.Run("unrelated process survives", func(t *testing.T) {
		root := t.TempDir()
		xdg, err := createXDGDirs(root, "unrelated")
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("sleep", "30")
		cmd.Env = append(os.Environ(),
			"XDG_STATE_HOME="+xdg.State,
			"HERMES_SERVER_PASSWORD=secret",
		)
		if err := cmd.Start(); err != nil {
			t.Fatalf("start sleep: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				<-done
			}
		})
		identity, err := hermesInspectProcess(cmd.Process.Pid)
		if err != nil {
			t.Skipf("process identity unavailable: %v", err)
		}
		if err := writeLease(xdg.State, serverLease{
			PID:              cmd.Process.Pid,
			PasswordHash:     passwordHash("secret"),
			XDGRoot:          xdg.Root,
			ProcessStartTime: identity.StartTime,
		}); err != nil {
			t.Fatal(err)
		}
		if err := reapStaleLeases(root, slog.New(slog.DiscardHandler)); err != nil {
			t.Fatalf("reapStaleLeases: %v", err)
		}
		select {
		case err := <-done:
			t.Fatalf("unrelated process was killed: %v", err)
		default:
		}
		if _, err := os.Stat(filepath.Join(xdg.State, leaseFileName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("lease after unrelated reap = %v", err)
		}
	})

	t.Run("verified fake hermes is reaped", func(t *testing.T) {
		root := t.TempDir()
		xdg, err := createXDGDirs(root, "orphan")
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=TestFakeHermesServerProcessHelper", "--", "serve", "--port", "0")
		configureHermesProcess(cmd)
		cmd.Env = append(os.Environ(),
			"ACP_GO_HERMES_FAKE_SERVER_HELPER=1",
			"XDG_STATE_HOME="+xdg.State,
			"HERMES_SERVER_USERNAME=hermes",
			"HERMES_SERVER_PASSWORD=secret",
		)
		if err := cmd.Start(); err != nil {
			t.Fatalf("start fake hermes: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				<-done
			}
		})
		var identity processIdentity
		for i := 0; i < 50; i++ {
			identity, err = hermesInspectProcess(cmd.Process.Pid)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			t.Skipf("process identity unavailable: %v", err)
		}
		if err := writeLease(xdg.State, serverLease{
			PID:              cmd.Process.Pid,
			PasswordHash:     passwordHash("secret"),
			XDGRoot:          xdg.Root,
			ProcessStartTime: identity.StartTime,
		}); err != nil {
			t.Fatal(err)
		}
		if err := reapStaleLeases(root, slog.New(slog.DiscardHandler)); err != nil {
			t.Fatalf("reapStaleLeases: %v", err)
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("verified fake hermes was not reaped")
		}
		if _, err := os.Stat(filepath.Join(xdg.State, leaseFileName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("lease after verified reap = %v", err)
		}
	})
}

func TestLeaseIdentityBranchCoverage(t *testing.T) {
	root := t.TempDir()
	xdg, err := createXDGDirs(root, "lease")
	if err != nil {
		t.Fatal(err)
	}
	leasePath := filepath.Join(xdg.State, leaseFileName)
	baseIdentity := processIdentity{
		StartTime: "start",
		Cmdline:   []string{"/usr/bin/hermes", "serve"},
		Env: map[string]string{
			"XDG_STATE_HOME":         xdg.State,
			"HERMES_SERVER_PASSWORD": "secret",
		},
	}
	baseLease := serverLease{
		PID:              999999,
		PasswordHash:     passwordHash("secret"),
		XDGRoot:          xdg.Root,
		ProcessStartTime: "start",
	}

	restoreHermesClientSeams(t)
	hermesInspectProcess = func(int) (processIdentity, error) {
		return baseIdentity, nil
	}
	if !leaseMatchesProcess(leasePath, baseLease) {
		t.Fatal("matching lease did not match")
	}
	if cmdlineLooksLikeHermesServe([]string{"/tmp/hermes"}) != true || cmdlineLooksLikeHermesServe([]string{"node"}) {
		t.Fatal("cmdline Hermes detection mismatch")
	}
	if leaseMatchesProcess(leasePath, serverLease{PID: 0, ProcessStartTime: "start"}) {
		t.Fatal("zero pid lease matched")
	}
	if leaseMatchesProcess(leasePath, serverLease{PID: 1}) {
		t.Fatal("missing start time lease matched")
	}

	for _, tt := range []struct {
		name     string
		identity processIdentity
		lease    serverLease
		err      error
	}{
		{name: "inspect error", identity: baseIdentity, lease: baseLease, err: errors.New("inspect failed")},
		{name: "start mismatch", identity: processIdentity{StartTime: "other", Cmdline: baseIdentity.Cmdline, Env: baseIdentity.Env}, lease: baseLease},
		{name: "state mismatch", identity: processIdentity{StartTime: "start", Cmdline: baseIdentity.Cmdline, Env: map[string]string{"XDG_STATE_HOME": t.TempDir(), "HERMES_SERVER_PASSWORD": "secret"}}, lease: baseLease},
		{name: "password mismatch", identity: processIdentity{StartTime: "start", Cmdline: baseIdentity.Cmdline, Env: map[string]string{"XDG_STATE_HOME": xdg.State, "HERMES_SERVER_PASSWORD": "wrong"}}, lease: baseLease},
		{name: "root mismatch", identity: baseIdentity, lease: serverLease{PID: baseLease.PID, PasswordHash: baseLease.PasswordHash, XDGRoot: t.TempDir(), ProcessStartTime: baseLease.ProcessStartTime}},
		{name: "cmdline mismatch", identity: processIdentity{StartTime: "start", Cmdline: []string{"node"}, Env: baseIdentity.Env}, lease: baseLease},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hermesInspectProcess = func(int) (processIdentity, error) {
				return tt.identity, tt.err
			}
			if leaseMatchesProcess(leasePath, tt.lease) {
				t.Fatal("mismatched lease matched")
			}
		})
	}

	hermesInspectProcess = func(int) (processIdentity, error) {
		return baseIdentity, nil
	}
	if err := writeLease(xdg.State, baseLease); err != nil {
		t.Fatal(err)
	}
	reapLeaseFile(leasePath, slog.New(slog.DiscardHandler))
	if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease after reap = %v", err)
	}
	reapLeaseFile(t.TempDir(), nil)
}

func TestNativeUnmarshalErrors(t *testing.T) {
	var part nativePart
	if err := part.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("nativePart accepted malformed JSON")
	}
	var event hermesEvent
	if err := event.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("hermesEvent accepted malformed JSON")
	}
	var providers providersResponse
	if err := providers.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("providersResponse accepted malformed JSON")
	}
}

func TestFakeHermesServerProcessHelper(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_FAKE_SERVER_HELPER") != "1" {
		return
	}
	runFakeHermesServerProcess()
	os.Exit(0)
}

func fakeHermesExecutable(t *testing.T) string {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	script := filepath.Join(t.TempDir(), "fake-hermes")
	body := fmt.Sprintf("#!/bin/sh\nACP_GO_HERMES_FAKE_SERVER_HELPER=1 exec %q -test.run=TestFakeHermesServerProcessHelper -- \"$@\"\n", testBinary)
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}
	return script
}

func runFakeHermesServerProcess() {
	args := os.Args
	sep := slicesIndex(args, "--")
	if sep >= 0 {
		args = args[sep+1:]
	}
	if len(args) == 0 || args[0] != "serve" {
		fmt.Fprintf(os.Stderr, "unexpected args: %q\n", strings.Join(args, " "))
		os.Exit(2)
	}
	port := ""
	for i := 1; i < len(args)-1; i++ {
		if args[i] == "--port" {
			port = args[i+1]
		}
	}
	if port == "" {
		fmt.Fprintln(os.Stderr, "missing --port")
		os.Exit(2)
	}
	fmt.Fprintln(os.Stdout, "native stdout noise")
	fmt.Fprintln(os.Stderr, "native stderr noise")
	username := os.Getenv("HERMES_SERVER_USERNAME")
	password := os.Getenv("HERMES_SERVER_PASSWORD")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gotUser, gotPassword, ok := r.BasicAuth(); !ok || gotUser != username || gotPassword != password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/global/health":
			writeJSONNoTest(w, map[string]any{"healthy": true, "version": "9.9.9"})
		case "/doc":
			writeJSONNoTest(w, fullHermesDoc())
		case "/event":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"server.connected\",\"properties\":{}}\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {}
		default:
			writeJSONNoTest(w, map[string]any{"ok": true})
		}
	})
	if err := http.ListenAndServe("127.0.0.1:"+port, handler); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func restoreHermesClientSeams(t *testing.T) {
	t.Helper()
	commandContext := hermesCommandContext
	listen := hermesListen
	randReader := hermesRandReader
	marshalIndent := hermesMarshalIndent
	writeLease := hermesWriteLease
	terminateProcess := hermesTerminateProcess
	killProcess := hermesKillProcess
	inspectProcess := hermesInspectProcess
	procReader := procReadFile
	waitCommand := hermesWaitCommand
	after := hermesAfter
	readyPoll := hermesReadyPollInterval
	reconnectDelay := hermesEventReconnectDelay
	shutdownTimeout := hermesShutdownTimeout
	t.Cleanup(func() {
		hermesCommandContext = commandContext
		hermesListen = listen
		hermesRandReader = randReader
		hermesMarshalIndent = marshalIndent
		hermesWriteLease = writeLease
		hermesTerminateProcess = terminateProcess
		hermesKillProcess = killProcess
		hermesInspectProcess = inspectProcess
		procReadFile = procReader
		hermesWaitCommand = waitCommand
		hermesAfter = after
		hermesReadyPollInterval = readyPoll
		hermesEventReconnectDelay = reconnectDelay
		hermesShutdownTimeout = shutdownTimeout
	})
}

func testXDGDirs(t *testing.T) xdgDirs {
	t.Helper()
	root := t.TempDir()
	return xdgDirs{
		Root:   root,
		Data:   filepath.Join(root, "data"),
		Config: filepath.Join(root, "config"),
		Cache:  filepath.Join(root, "cache"),
		State:  filepath.Join(root, "state"),
	}
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type errorReadCloser struct {
	err error
}

func (r errorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r errorReadCloser) Close() error {
	return nil
}

type eofCancelReadCloser struct {
	cancel context.CancelFunc
}

func (r eofCancelReadCloser) Read([]byte) (int, error) {
	r.cancel()
	return 0, io.EOF
}

func (r eofCancelReadCloser) Close() error {
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type fakeListener struct {
	addr net.Addr
}

func (l fakeListener) Accept() (net.Conn, error) {
	return nil, errors.New("unused fake listener")
}

func (l fakeListener) Close() error {
	return nil
}

func (l fakeListener) Addr() net.Addr {
	return l.addr
}

type stringAddr string

func (a stringAddr) Network() string {
	return "string"
}

func (a stringAddr) String() string {
	return string(a)
}

func readinessClient(t *testing.T, handler http.HandlerFunc) (*hermesServer, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	client := &hermesServer{
		httpClient: server.Client(),
		baseURL:    server.URL,
		username:   "hermes",
		password:   "secret",
		events:     make(chan hermesEvent, 8),
		errs:       make(chan error, 8),
		closed:     make(chan struct{}),
	}
	return client, func() {
		close(client.closed)
		server.Close()
	}
}

func writeSSE(t *testing.T, w http.ResponseWriter, payload string) {
	t.Helper()
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeJSONNoTest(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func slicesIndex(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}
