package hermesacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/coder/websocket"
)

const fakeHermesEnv = "ACP_GO_HERMES_TEST_FAKE"
const fakeHermesEnvVersion = "ACP_GO_HERMES_TEST_VERSION"

// fakeHermesEnvResumeHold names a file the gateway creates when a
// session.resume arrives that it will never answer, so a test can act while
// the adapter is still relaunching.
const fakeHermesEnvResumeHold = "ACP_GO_HERMES_TEST_RESUME_HOLD"

// heldAttachMarker is written into the home when the gateway takes an
// attachment it will never answer, so a test knows the upload is in flight.
const heldAttachMarker = "attach-held"

// fakeGateway speaks the same JSON-RPC and per-session persistence surfaces as serve.
type fakeGateway struct {
	home     string
	mu       sync.Mutex
	sessions map[string]*fakeSession
}

type fakeSession struct {
	mu         sync.Mutex
	id         string
	model      string
	provider   string
	effort     string
	cwd        string
	messages   []map[string]any
	interrupt  chan struct{}
	permission chan string
	answer     chan any
	images     []string
	dialogs    []string
	holdAttach bool
}

func fakeID() string {
	var value [12]byte
	_, _ = rand.Read(value[:])

	return hex.EncodeToString(value[:])
}

func runFakeHermes(args []string) int {
	if len(args) > 0 && args[0] == "--version" {
		version := os.Getenv(fakeHermesEnvVersion)
		if version == "" {
			version = "0.21.3"
		}
		fmt.Println("Hermes v" + version)

		return 0
	}
	port := ""
	for index, arg := range args {
		if arg == "--port" && index+1 < len(args) {
			port = args[index+1]
		}
	}
	if port == "" {
		return 2
	}
	server := &fakeGateway{home: os.Getenv("HERMES_HOME"), sessions: make(map[string]*fakeSession)}
	if err := os.MkdirAll(server.home, 0o700); err != nil {
		return 2
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ws", server.socket)
	mux.HandleFunc("/api/sessions/", server.persistence)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Hermes-Session-Token") != os.Getenv("HERMES_DASHBOARD_SESSION_TOKEN") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)

			return
		}
		mux.ServeHTTP(w, r)
	})
	if err := http.ListenAndServe("127.0.0.1:"+port, handler); err != nil {
		return 2
	}

	return 0
}

func (g *fakeGateway) persistence(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/sessions/import" {
		var body struct {
			Sessions []json.RawMessage `json:"sessions"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Sessions) != 1 {
			http.Error(w, "invalid import", http.StatusBadRequest)

			return
		}
		var item struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(body.Sessions[0], &item) != nil || filepath.Base(item.ID) != item.ID {
			http.Error(w, "invalid identity", http.StatusBadRequest)

			return
		}
		path := filepath.Join(g.home, item.ID+".json")
		// Hermes's import creates missing conversations and refuses to replace
		// one that exists.
		if _, statErr := os.Stat(path); statErr == nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "imported": 0})

			return
		}
		if err := os.WriteFile(path, body.Sessions[0], 0o600); err != nil {
			http.Error(w, "write failed", http.StatusInternalServerError)

			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "imported": 1})

		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/sessions/"), "/export")
	if filepath.Base(id) != id {
		http.NotFound(w, r)

		return
	}
	data, err := os.ReadFile(filepath.Join(g.home, id+".json"))
	if err != nil {
		http.NotFound(w, r)

		return
	}
	_, _ = w.Write(data)
}

func (g *fakeGateway) socket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()
	var writes sync.Mutex
	send := func(value any) {
		writes.Lock()
		defer writes.Unlock()
		data, _ := json.Marshal(value)
		_ = conn.Write(r.Context(), websocket.MessageText, data)
	}
	emit := func(id, kind string, payload any) {
		switch kind {
		case eventApprovalRequest, eventClarifyRequest, eventSudoRequest, eventSecretRequest, eventTerminalReadRequest:
			params, ok := payload.(map[string]any)
			if !ok {
				panic("native control fixture requires object params")
			}
			params[nativeSessionIDKey] = id
			send(map[string]any{"jsonrpc": "2.0", "id": kind + "|" + id + "|" + fakeID(), "method": kind, "params": params})

			return
		}
		send(map[string]any{"jsonrpc": "2.0", "method": "event", "params": map[string]any{fieldType: kind, nativeSessionIDKey: id, "payload": payload}})
	}
	emit("", "gateway.ready", map[string]any{})
	for {
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var request struct {
			ID     json.RawMessage `json:"id"`
			Result map[string]any  `json:"result"`
			Method string          `json:"method"`
			Params map[string]any  `json:"params"`
		}
		if json.Unmarshal(data, &request) != nil {
			return
		}
		if request.Method == "" {
			var responseID string
			if json.Unmarshal(request.ID, &responseID) == nil {
				g.answerRequest(responseID, request.Result)
			}

			continue
		}
		id, _ := request.Params[nativeSessionIDKey].(string)
		g.mu.Lock()
		session := g.sessions[id]
		g.mu.Unlock()
		var result any = map[string]any{}
		var failure string
		switch request.Method {
		case "session.create":
			cwd, _ := request.Params[fieldCwd].(string)
			session = &fakeSession{id: fakeID(), cwd: cwd, model: "vision", provider: "fake", effort: effortMedium, messages: []map[string]any{}}
			id = "live-" + fakeID()
			g.mu.Lock()
			g.sessions[id] = session
			g.mu.Unlock()
			result = map[string]any{nativeSessionIDKey: id, "stored_session_id": session.id}
		case "session.resume":
			if hold := os.Getenv(fakeHermesEnvResumeHold); hold != "" {
				_ = os.WriteFile(hold, []byte("held\n"), 0o600)

				continue
			}
			bytes, err := os.ReadFile(filepath.Join(g.home, id+".json"))
			if err != nil {
				failure = "session not found"

				break
			}
			var snapshot nativeSnapshot
			if json.Unmarshal(bytes, &snapshot) != nil {
				failure = "invalid session"

				break
			}
			session = &fakeSession{id: id, model: "vision", provider: "fake", effort: effortMedium, messages: snapshot.Messages}
			id = "live-" + fakeID()
			g.mu.Lock()
			g.sessions[id] = session
			g.mu.Unlock()
			result = map[string]any{nativeSessionIDKey: id, "session_key": session.id}
		case "session.active_list":
			g.mu.Lock()
			rows := []any{}
			for live, item := range g.sessions {
				item.mu.Lock()
				rows = append(rows, map[string]any{fieldID: live, "session_key": item.id})
				item.mu.Unlock()
			}
			g.mu.Unlock()
			result = map[string]any{"sessions": rows}
		case "process.list":
			failure = session.buildFailure()
		case "model.options":
			session.mu.Lock()
			result = map[string]any{"provider": session.provider, "model": session.model, "providers": []any{map[string]any{"slug": "fake", fieldName: "Fake", "models": []string{"vision", "text-only"}}}}
			session.mu.Unlock()
		case "config.get":
			session.mu.Lock()
			result = map[string]any{fieldValue: session.effort}
			session.mu.Unlock()
		case "config.set":
			key, _ := request.Params["key"].(string)
			value, _ := request.Params[fieldValue].(string)
			session.mu.Lock()
			if key == "model" {
				tokens := strings.Fields(value)
				session.model = tokens[0]
				session.provider = tokens[2]
				value = tokens[0]
			} else {
				session.effort = value
			}
			session.mu.Unlock()
			result = map[string]any{"key": key, fieldValue: value, "scope": nativeScopeSession}
		case "session.cwd.set":
			session.mu.Lock()
			session.cwd, _ = request.Params[fieldCwd].(string)
			session.mu.Unlock()
		case "image.attach_bytes":
			session.mu.Lock()
			data, _ := request.Params["content_base64"].(string)
			session.images = append(session.images, data)
			held := session.holdAttach
			session.mu.Unlock()
			if held {
				_ = os.WriteFile(filepath.Join(g.home, heldAttachMarker), nil, 0o600)

				continue
			}
			result = map[string]any{"attached": true}
		case "prompt.submit":
			text, _ := request.Params[fieldText].(string)
			session.mu.Lock()
			session.interrupt = make(chan struct{})
			session.permission = make(chan string, 1)
			session.answer = make(chan any, 1)
			session.mu.Unlock()
			result = map[string]any{"status": promptStreaming}
			if text == "QUEUED" {
				result = map[string]any{"status": promptQueued}
				emit(id, "message.start", map[string]any{})
				emit(id, "message.delta", map[string]any{fieldText: "earlier"})
				emit(id, eventMessageComplete, map[string]any{fieldText: "earlier", "status": statusComplete})
			}
			send(map[string]any{"jsonrpc": "2.0", fieldID: request.ID, fieldResult: result})
			go g.prompt(r.Context(), session, id, text, emit)

			continue
		case "session.interrupt":
			session.mu.Lock()
			if session.interrupt != nil {
				select {
				case <-session.interrupt:
				default:
					close(session.interrupt)
				}
			}
			session.mu.Unlock()
		default:
			failure = "method not found"
		}
		if failure != "" {
			send(map[string]any{"jsonrpc": "2.0", fieldID: request.ID, stopReasonError: map[string]any{"code": 4007, "message": failure}})
		} else {
			send(map[string]any{"jsonrpc": "2.0", fieldID: request.ID, fieldResult: result})
		}
	}
}

func (g *fakeGateway) prompt(ctx context.Context, s *fakeSession, live, prompt string, emit func(string, string, any)) {
	emit(live, "message.start", map[string]any{})
	text, status := "Hello world", statusComplete
	switch prompt {
	case "SLOW":
		select {
		case <-s.interrupt:
		case <-ctx.Done():
			return
		}
		status = statusInterrupted
	case "PERMISSION":
		emit(live, eventApprovalRequest, map[string]any{"command": "native-command", "choices": []string{approvalOnce, "deny"}})
		select {
		case choice := <-s.permission:
			text = "permission=" + choice
		case <-s.interrupt:
			status = statusInterrupted
		case <-ctx.Done():
			return
		}
	case "QUESTION":
		emit(live, eventClarifyRequest, map[string]any{"question": "Which color?"})
		select {
		case answer := <-s.answer:
			text = fmt.Sprint(answer)
		case <-s.interrupt:
			status = statusInterrupted
		case <-ctx.Done():
			return
		}
	case "NOISE":
		fmt.Fprintln(os.Stderr, "chatter on stderr")
		fmt.Println("not a json record at all")
		text = "quiet"
	case "ERROR":
		status = stopReasonError
		text = "provider unavailable"
	case "CRASH":
		os.Exit(3)
	case "IMAGE":
		s.mu.Lock()
		text = strings.Join(s.images, ",")
		s.images = nil
		s.mu.Unlock()
	case "HOLD":
		s.mu.Lock()
		s.holdAttach = true
		s.mu.Unlock()
	case "DIALOGS":
		s.mu.Lock()
		text = strings.Join(s.dialogs, ",")
		s.mu.Unlock()
	case "ENV":
		text = os.Getenv("ACP_MARKER") + "|" + os.Getenv("PATH")
	case "ROTATE":
		s.mu.Lock()
		s.id = fakeID()
		s.mu.Unlock()
	}
	emit(live, "message.delta", map[string]any{fieldText: text})
	s.mu.Lock()
	s.messages = append(s.messages, map[string]any{"role": roleUser, "content": prompt}, map[string]any{"role": roleAssistant, "content": text})
	data, _ := json.Marshal(map[string]any{fieldID: s.id, fieldSource: nativeSource, fieldCwd: s.cwd, "model": s.model, "started_at": 1, "messages": s.messages})
	path := filepath.Join(g.home, s.id+".json")
	_ = os.WriteFile(path+".tmp", data, 0o600)
	_ = os.Rename(path+".tmp", path)
	s.mu.Unlock()
	emit(live, eventMessageComplete, map[string]any{fieldText: text, "status": status, "usage": map[string]any{"input": 5, "output": 2, "total": 7, "context_used": 7, "context_max": 1000}})
}

func (s *fakeSession) buildFailure() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if expected := os.Getenv("ACP_GO_HERMES_TEST_BUILD_MODEL"); expected != "" && s.provider+"/"+s.model != expected {
		return "agent initialization used the wrong model"
	}

	return ""
}

func (g *fakeGateway) answerRequest(id string, result map[string]any) {
	parts := strings.Split(id, "|")
	if len(parts) < 2 {
		return
	}
	g.mu.Lock()
	session := g.sessions[parts[1]]
	g.mu.Unlock()
	if session == nil {
		return
	}
	switch parts[0] {
	case eventApprovalRequest:
		choice, _ := result["choice"].(string)
		session.permission <- choice
	case eventClarifyRequest:
		answer := result["answer"]
		if answers, ok := result["answers"]; ok {
			answer = answers
		}
		session.answer <- answer
	default:
		session.mu.Lock()
		session.dialogs = append(session.dialogs, parts[0])
		session.mu.Unlock()
	}
}
