package hermesacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/savid/acp-go-hermes/internal/hermes"
)

const fakeHermesEnv = "ACP_GO_HERMES_TEST_FAKE"

// fakeHermesEnvResumeHold names a file the gateway creates when a
// session.resume arrives that it will never answer, so a test can act while
// the adapter is still relaunching.
const fakeHermesEnvResumeHold = "ACP_GO_HERMES_TEST_RESUME_HOLD"

// fakeHermesEnvReadyHold names a file the gateway creates, holding its own pid,
// before it announces readiness, once a sibling ".armed" file exists; it stays
// silent until the file is removed.
const fakeHermesEnvReadyHold = "ACP_GO_HERMES_TEST_READY_HOLD"

// fakePluginToggles records, one key per line, each plugin the gateway was
// asked to enable.
const fakePluginToggles = "plugin-toggles"

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
	built      string
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
	usage      fakeUsage
}

// fakeContextWindow is the context window the fake reports for every model
// but fakeWideModel, whose window is fakeWideWindow.
const (
	fakeContextWindow = 1000
	fakeWideModel     = "text-only"
	fakeWideWindow    = 4000
)

func fakeWindow(model string) int64 {
	if model == fakeWideModel {
		return fakeWideWindow
	}

	return fakeContextWindow
}

// fakeUsage keeps a session's usage as Hermes's agent does: cumulative token
// counters, and the prompt tokens of the last response with usage as its
// context, -1 from a compaction until a response follows it and 0 after a
// response whose usage was all zero.
type fakeUsage struct {
	prompt, completion, total, calls, compressions, context int64
	unsized                                                 bool
}

// respond records one provider response with usage.
func (u *fakeUsage) respond(prompt, completion int64) {
	u.calls++
	u.prompt += prompt
	u.completion += completion
	u.total += prompt + completion
	u.context = prompt
}

// interrupted records a provider attempt that ended without usage.
func (u *fakeUsage) interrupted() { u.calls++ }

// replay records a response a gateway answered from its response cache, with
// every usage token zero: Hermes counts the call, its token counters do not
// move, and it no longer states a context.
func (u *fakeUsage) replay() {
	u.calls++
	u.context = 0
}

// compact replaces the context with a summary whose size no response has
// reported yet.
func (u *fakeUsage) compact() {
	u.compressions++
	u.context = -1
}

// wire renders the usage block of an agent running model as the gateway
// does: the context fields only while a response has reported the context
// and the window is known.
func (u *fakeUsage) wire(model string) map[string]any {
	block := map[string]any{
		"model": model, "input": u.prompt, "output": u.completion, "reasoning": 0, "prompt": u.prompt,
		"completion": u.completion, "total": u.total, "calls": u.calls, "compressions": u.compressions,
	}
	if u.context > 0 && !u.unsized {
		block["context_used"] = u.context
		block["context_max"] = fakeWindow(model)
		block["context_percent"] = u.context * 100 / fakeWindow(model)
		block["context_source"] = "provider_usage"
		block["context_estimated"] = false
	}

	return block
}

func fakeID() string {
	var value [12]byte
	_, _ = rand.Read(value[:])

	return hex.EncodeToString(value[:])
}

func runFakeHermes(args []string) int {
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
	emit := fakeEmitter(send)
	holdReady()
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
			if model, _ := request.Params["model"].(string); model != "" {
				session.provider, _ = request.Params["provider"].(string)
				session.model = model
			}
			// Like Hermes, a created session builds its agent immediately; a
			// later model switch cannot change what that build used.
			session.built = session.provider + "/" + session.model
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
		case "plugins.manage":
			result, failure = g.togglePlugin(request.Params)
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
				emit(id, eventMessageStart, map[string]any{})
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

// togglePlugin enables the adapter's plugin as Hermes does, recording each
// enable in the home.
func (g *fakeGateway) togglePlugin(params map[string]any) (any, string) {
	key, _ := params["key"].(string)
	if params["action"] != "toggle" || key != hermes.PluginName || params["enable"] != true {
		return nil, "invalid plugin toggle"
	}
	toggles, err := os.OpenFile(filepath.Join(g.home, fakePluginToggles), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, "config write failed"
	}
	_, _ = toggles.WriteString(key + "\n")
	_ = toggles.Close()

	return map[string]any{"ok": true, "name": key}, ""
}

// usageEvent is a session.usage tick with the session's current usage.
func (s *fakeSession) usageEvent(live string, emit func(string, string, any)) {
	s.mu.Lock()
	block := s.usage.wire(s.model)
	s.mu.Unlock()
	emit(live, eventSessionUsage, map[string]any{"usage": block})
}

// record applies one change to the session's usage.
func (s *fakeSession) record(change func(*fakeUsage)) {
	s.mu.Lock()
	change(&s.usage)
	s.mu.Unlock()
}

// usageScript drives the runs that exercise usage reporting and returns the
// response that ends the run and its status. A run whose connection closed
// reports false.
func (s *fakeSession) usageScript(ctx context.Context, live, prompt string, emit func(string, string, any)) (func(*fakeUsage), string, bool) {
	final, status := func(u *fakeUsage) { u.respond(10, 5) }, statusComplete
	switch prompt {
	case "MULTI":
		// Each tool call is one response, and a usage tick follows it.
		for index, prompt := range []int64{1000, 1120} {
			s.record(func(u *fakeUsage) { u.respond(prompt, 20) })
			toolStep(live, fmt.Sprintf("multi-%d", index), emit)
			s.usageEvent(live, emit)
		}
		final = func(u *fakeUsage) { u.respond(1200, 10) }
	case "BURST":
		// Two responses land between two ticks.
		s.record(func(u *fakeUsage) { u.respond(1000, 20); u.respond(1120, 30) })
		s.usageEvent(live, emit)
		final = func(u *fakeUsage) { u.respond(1200, 10) }
	case "REDIRECT":
		// A native redirect interrupts the call in flight, which reports no
		// usage, and the turn continues with the corrected direction.
		s.record(func(u *fakeUsage) { u.respond(1000, 20) })
		s.usageEvent(live, emit)
		s.record((*fakeUsage).interrupted)
		s.usageEvent(live, emit)
		s.record(func(u *fakeUsage) { u.respond(900, 15) })
		s.usageEvent(live, emit)
		final = func(u *fakeUsage) { u.respond(950, 5) }
	case "COMPACT":
		// The context crossed the threshold, so Hermes compacts before the
		// next response.
		s.record(func(u *fakeUsage) { u.respond(900, 50) })
		s.usageEvent(live, emit)
		s.record((*fakeUsage).compact)
		s.usageEvent(live, emit)
		final = func(u *fakeUsage) { u.respond(300, 20) }
	case "COMPACTEND":
		// Hermes compacts after the run's last response.
		s.record(func(u *fakeUsage) { u.respond(900, 50) })
		s.usageEvent(live, emit)
		final = func(u *fakeUsage) { u.compact() }
	case "UNSIZED":
		s.record(func(u *fakeUsage) { u.unsized = true })
	case "COVERED":
		// A tick records the run's last response before the run ends.
		s.record(func(u *fakeUsage) { u.respond(1000, 20) })
		s.usageEvent(live, emit)
		final = func(*fakeUsage) {}
	case "REPLAY":
		// The response after a tool call is replayed from a response cache.
		s.record(func(u *fakeUsage) { u.respond(1000, 20) })
		toolStep(live, "replay-0", emit)
		s.usageEvent(live, emit)
		final = (*fakeUsage).replay
	case "REPLAYED":
		// The run's only response is replayed from a response cache.
		final = (*fakeUsage).replay
	case "CALLS":
		// Each response is reported as it returns, before its tool runs and
		// before Hermes records it; the last one before the run ends.
		for index, call := range []struct {
			id                    string
			prompt, cached, write int64
		}{{"gen-1", 1000, 0, -1}, {"gen-2", 1120, 1000, 0}} {
			s.report(emit, call.id, chatUsage(call.prompt, call.cached, call.write, 20))
			toolStep(live, fmt.Sprintf("calls-%d", index), emit)
			s.record(func(u *fakeUsage) { u.respond(call.prompt, 20) })
			s.usageEvent(live, emit)
		}
		s.report(emit, "", chatUsage(1200, 1100, 50, 10))
		final = func(u *fakeUsage) { u.respond(1200, 10) }
	case "CALLRACE":
		// A tick lands after a response was reported and before Hermes
		// recorded it, so it records only the response before.
		s.report(emit, "gen-1", chatUsage(1000, 0, 0, 20))
		s.record(func(u *fakeUsage) { u.respond(1000, 20) })
		s.report(emit, "gen-2", chatUsage(1120, 1000, 0, 20))
		s.usageEvent(live, emit)
		s.report(emit, "gen-3", chatUsage(1200, 1100, 0, 10))
		s.record(func(u *fakeUsage) { u.respond(1120, 20) })
		final = func(u *fakeUsage) { u.respond(1200, 10) }
	case "CALLRETRY":
		// Hermes rejects the first response and retries the request; each
		// attempt is its own call, and only the accepted one moves a counter.
		s.report(emit, "gen-rejected", chatUsage(1000, 0, 0, 0))
		s.report(emit, "gen-accepted", chatUsage(1000, 900, 0, 20))
		final = func(u *fakeUsage) { u.respond(1000, 20) }
	case "CALLFALLBACK":
		// Hermes rejects a response and accepts its retry; a later response
		// arrives on a wire the plugin does not report.
		s.report(emit, "gen-rejected", chatUsage(1000, 0, 0, 0))
		s.report(emit, "gen-accepted", chatUsage(1000, 900, 0, 20))
		s.record(func(u *fakeUsage) { u.respond(1000, 20) })
		s.usageEvent(live, emit)
		s.record(func(u *fakeUsage) { u.respond(900, 15) })
		s.usageEvent(live, emit)
		final = func(u *fakeUsage) { u.respond(950, 5) }
	case "CALLFOREIGN":
		// A review fork or delegated child shares the process but not the
		// conversation; its reports name another session.
		emit("", hermes.CallEvent, map[string]any{nativeSessionIDKey: "fork", "response_id": "gen-fork", "usage": chatUsage(5000, 0, 0, 50)})
		s.report(emit, "gen-own", chatUsage(10, 0, 0, 5))
	case "CALLEMPTY":
		// A gateway answering from its response cache reports zero tokens.
		s.report(emit, "gen-replayed", chatUsage(0, 0, 0, 0))
		final = (*fakeUsage).replay
	case "CALLSLOW":
		s.report(emit, "gen-1", chatUsage(1000, 0, 0, 20))
		s.record(func(u *fakeUsage) { u.respond(1000, 20) })
		s.usageEvent(live, emit)
		select {
		case <-s.interrupt:
		case <-ctx.Done():
			return nil, "", false
		}
		// The responses in flight at the interrupt still report.
		s.report(emit, "gen-2", chatUsage(1120, 1000, 0, 20))
		s.report(emit, "gen-3", chatUsage(1200, 1100, 0, 5))
		s.record(func(u *fakeUsage) { u.respond(1120, 20); u.respond(1200, 5) })
		s.usageEvent(live, emit)
		status = statusInterrupted
	case "STEPSLOW":
		s.record(func(u *fakeUsage) { u.respond(1000, 20) })
		s.usageEvent(live, emit)
		select {
		case <-s.interrupt:
		case <-ctx.Done():
			return nil, "", false
		}
		// The response in flight at the interrupt still reports usage.
		s.record(func(u *fakeUsage) { u.respond(1120, 5) })
		s.usageEvent(live, emit)
		status = statusInterrupted
	}

	return final, status, true
}

// report broadcasts the plugin's report of one response as Hermes sends it:
// without a session, naming the conversation in its payload. An empty id is a
// response whose gateway sent none.
func (s *fakeSession) report(emit func(string, string, any), id string, usage map[string]any) {
	s.mu.Lock()
	payload := map[string]any{nativeSessionIDKey: s.id, "model": s.model, "usage": usage}
	s.mu.Unlock()
	if id != "" {
		payload["response_id"] = id
	}
	emit("", hermes.CallEvent, payload)
}

// chatUsage is a Chat Completions usage block; a negative cache figure is
// one the gateway did not send.
func chatUsage(prompt, cached, written, completion int64) map[string]any {
	details := map[string]any{"audio_tokens": 0}
	if cached >= 0 {
		details["cached_tokens"] = cached
	}
	if written >= 0 {
		details["cache_write_tokens"] = written
	}

	return map[string]any{"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": prompt + completion, "prompt_tokens_details": details, "cost": 0.001}
}

// toolStep is one tool call the model requested in a response.
func toolStep(live, id string, emit func(string, string, any)) {
	tool := map[string]any{fieldToolID: id, fieldName: "terminal", "args": map[string]any{"command": "ls"}}
	emit(live, eventToolStart, tool)
	complete := maps.Clone(tool)
	complete[fieldResult] = "ok"
	emit(live, eventToolComplete, complete)
}

func (g *fakeGateway) prompt(ctx context.Context, s *fakeSession, live, prompt string, emit func(string, string, any)) {
	emit(live, eventMessageStart, map[string]any{})
	text, status := "Hello world", statusComplete
	// final is the response that ends the run; a run the user interrupts ends
	// without one.
	final := func(u *fakeUsage) { u.respond(10, 5) }
	switch prompt {
	case "SLOW":
		select {
		case <-s.interrupt:
		case <-ctx.Done():
			return
		}
		status = statusInterrupted
	case "MULTI", "BURST", "REDIRECT", "COMPACT", "COMPACTEND", "UNSIZED", "COVERED", "REPLAY", "REPLAYED", "STEPSLOW",
		"CALLS", "CALLRACE", "CALLRETRY", "CALLFALLBACK", "CALLFOREIGN", "CALLEMPTY", "CALLSLOW":
		var running bool
		if final, status, running = s.usageScript(ctx, live, prompt, emit); !running {
			return
		}
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
	s.mu.Lock()
	if status != statusInterrupted {
		final(&s.usage)
	}
	usage := s.usage.wire(s.model)
	s.mu.Unlock()
	emit(live, eventMessageComplete, map[string]any{fieldText: text, "status": status, "usage": usage})

	if prompt == "AGENTWORK" {
		// Hermes runs a follow-up turn on its own once the prompt's turn ends.
		g.prompt(ctx, s, live, "MULTI", emit)
	}
}

func (s *fakeSession) buildFailure() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	built := s.built
	if built == "" {
		built = s.provider + "/" + s.model
	}
	if expected := os.Getenv("ACP_GO_HERMES_TEST_BUILD_MODEL"); expected != "" && built != expected {
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

// holdReady parks the gateway before readiness while an armed ready-hold file
// exists, publishing its pid there for the test that released it.
func holdReady() {
	hold := os.Getenv(fakeHermesEnvReadyHold)
	if hold == "" {
		return
	}

	if _, err := os.Stat(hold + ".armed"); err != nil {
		return
	}

	_ = os.WriteFile(hold, []byte(strconv.Itoa(os.Getpid())), 0o600)

	for {
		if _, err := os.Stat(hold); err != nil {
			return
		}

		time.Sleep(time.Millisecond)
	}
}

func fakeEmitter(send func(any)) func(string, string, any) {
	return func(id, kind string, payload any) {
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
}
