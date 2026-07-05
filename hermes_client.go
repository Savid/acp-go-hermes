//nolint:tagliatelle // Hermes native JSON fields use modelID/sessionID/providerID spellings.
package hermesacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	leaseFileName = "server.lease"
)

type hermesClient interface {
	Close(context.Context) error
	CreateSession(context.Context, string) (nativeSession, error)
	GetSession(context.Context, string) (nativeSession, error)
	ListSessions(context.Context, string) ([]nativeSession, error)
	DeleteSession(context.Context, string) error
	SendMessage(context.Context, string, hermesMessageRequest) (nativeMessage, error)
	Messages(context.Context, string) ([]nativeMessage, error)
	Abort(context.Context, string) error
	Fork(context.Context, string, string) (nativeSession, error)
	Todos(context.Context, string) ([]nativeTodo, error)
	ConfigProviders(context.Context) (providersResponse, error)
	Agents(context.Context) ([]nativeAgent, error)
	PendingPermissions(context.Context) ([]permissionRequest, error)
	ReplyPermission(context.Context, permissionRequest, string, string) error
	PendingQuestions(context.Context) ([]questionRequest, error)
	ReplyQuestion(context.Context, questionRequest, [][]string) error
	RejectQuestion(context.Context, questionRequest) error
	Events() <-chan hermesEvent
	EventErrors() <-chan error
	XDGDirs() xdgDirs
}

type hermesStartOptions struct {
	ACPSessionID   acpSessionIDString
	Root           string
	Cwd            string
	ExecutablePath string
	DefaultModel   string
	Env            map[string]string
	HealthTimeout  time.Duration
	Logger         *slog.Logger
	ExistingXDG    xdgDirs
	MCPServers     []acp.McpServer
}

type acpSessionIDString string

type xdgDirs struct {
	Root   string
	Data   string
	Config string
	Cache  string
	State  string
}

type hermesServer struct {
	cmd *exec.Cmd
	xdg xdgDirs
	log *slog.Logger

	events chan hermesEvent
	errs   chan error
	closed chan struct{}
	once   sync.Once

	gateway      *nativehermes.Client
	process      *nativehermes.Process
	gatewayMu    sync.Mutex
	liveByStored map[string]string
	storedByLive map[string]string
	cwd          string
	defaultModel string
}

type nativeSession struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Directory string `json:"directory"`
	Agent     string `json:"agent"`
	Model     struct {
		ID         string `json:"id"`
		ModelID    string `json:"modelID"`
		ProviderID string `json:"providerID"`
	} `json:"model"`
	Time struct {
		Created int64 `json:"created"`
		Updated int64 `json:"updated"`
	} `json:"time"`
}

type nativeMessage struct {
	Info  nativeMessageInfo `json:"info"`
	Parts []nativePart      `json:"parts"`
}

type nativeMessageInfo struct {
	ID         string       `json:"id"`
	SessionID  string       `json:"sessionID"`
	Role       string       `json:"role"`
	ParentID   string       `json:"parentID"`
	ModelID    string       `json:"modelID"`
	ProviderID string       `json:"providerID"`
	Mode       string       `json:"mode"`
	Agent      string       `json:"agent"`
	Finish     string       `json:"finish"`
	Cost       float64      `json:"cost"`
	Tokens     nativeTokens `json:"tokens"`
	Error      *nativeError `json:"error,omitempty"`
	Time       struct {
		Created   int64 `json:"created"`
		Completed int64 `json:"completed"`
	} `json:"time"`
}

type nativeError struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Message string `json:"message"`
}

type nativePart struct {
	ID        string          `json:"id"`
	SessionID string          `json:"sessionID"`
	MessageID string          `json:"messageID"`
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	CallID    string          `json:"callID"`
	Tool      string          `json:"tool"`
	State     json.RawMessage `json:"state"`
	Reason    string          `json:"reason"`
	Cost      float64         `json:"cost"`
	Tokens    nativeTokens    `json:"tokens"`
	Raw       json.RawMessage `json:"-"`
}

func (p *nativePart) UnmarshalJSON(data []byte) error {
	type alias nativePart
	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*p = nativePart(value)
	p.Raw = append(p.Raw[:0], data...)

	return nil
}

type nativeTokens struct {
	Total     float64 `json:"total"`
	Input     float64 `json:"input"`
	Output    float64 `json:"output"`
	Reasoning float64 `json:"reasoning"`
	Cache     struct {
		Read  float64 `json:"read"`
		Write float64 `json:"write"`
	} `json:"cache"`
}

type nativeTodo struct {
	ID       string `json:"id"`
	Content  string `json:"content"`
	Status   string `json:"status"`
	Priority string `json:"priority"`
}

type nativeAgent struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Mode        string `json:"mode"`
}

type hermesEvent struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Properties  json.RawMessage `json:"properties"`
	Raw         json.RawMessage `json:"-"`
	StreamEpoch uint64          `json:"-"`
}

func (e *hermesEvent) UnmarshalJSON(data []byte) error {
	type alias hermesEvent
	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*e = hermesEvent(value)
	e.Raw = append(e.Raw[:0], data...)

	return nil
}

type permissionRequest struct {
	ID         string          `json:"id"`
	SessionID  string          `json:"sessionID"`
	Action     string          `json:"action"`
	Permission string          `json:"permission"`
	Resources  []string        `json:"resources"`
	Patterns   []string        `json:"patterns"`
	Save       []string        `json:"save"`
	Always     []string        `json:"always"`
	Metadata   map[string]any  `json:"metadata"`
	Source     map[string]any  `json:"source"`
	Tool       permissionTool  `json:"tool"`
	ReplyRoute permissionRoute `json:"-"`
}

type permissionTool struct {
	MessageID string `json:"messageID"`
	CallID    string `json:"callID"`
}

type permissionRoute string

const (
	permissionRouteSession permissionRoute = "session"
	permissionRouteAPI     permissionRoute = "api"
)

func (r permissionRequest) route() permissionRoute {
	if r.ReplyRoute != "" {
		return r.ReplyRoute
	}
	if r.Action != "" {
		return permissionRouteAPI
	}
	return permissionRouteSession
}

func (r permissionRequest) actionName() string {
	return firstNonEmpty(r.Action, r.Permission)
}

func (r permissionRequest) resourceList() []string {
	if len(r.Resources) > 0 {
		return append([]string(nil), r.Resources...)
	}
	return append([]string(nil), r.Patterns...)
}

type questionRequest struct {
	ID         string         `json:"id"`
	SessionID  string         `json:"sessionID"`
	Questions  []questionInfo `json:"questions"`
	Tool       questionTool   `json:"tool"`
	ReplyRoute questionRoute  `json:"-"`
}

type questionRoute string

const (
	questionRouteSession questionRoute = "session"
	questionRouteAPI     questionRoute = "api"
)

func (r questionRequest) route() questionRoute {
	if r.ReplyRoute != "" {
		return r.ReplyRoute
	}
	return questionRouteSession
}

type questionInfo struct {
	Question string           `json:"question"`
	Header   string           `json:"header"`
	Options  []questionOption `json:"options"`
	Multiple bool             `json:"multiple"`
	Custom   bool             `json:"custom"`
}

type questionOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

type questionTool struct {
	MessageID string `json:"messageID"`
	CallID    string `json:"callID"`
}

type hermesMessageRequest struct {
	MessageID string               `json:"messageID,omitempty"`
	Model     *hermesModelSelector `json:"model,omitempty"`
	Agent     string               `json:"agent,omitempty"`
	NoReply   bool                 `json:"noReply,omitempty"`
	Parts     []map[string]any     `json:"parts"`
}

type hermesModelSelector struct {
	ProviderID string `json:"providerID"`
	ModelID    string `json:"modelID"`
}

type providersResponse struct {
	Providers []providerInfo    `json:"providers"`
	Default   map[string]string `json:"default"`
	Raw       json.RawMessage   `json:"-"`
}

func (p *providersResponse) UnmarshalJSON(data []byte) error {
	type alias providersResponse
	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*p = providersResponse(value)
	p.Raw = append(p.Raw[:0], data...)

	return nil
}

type providerInfo struct {
	ID     string                   `json:"id"`
	Name   string                   `json:"name"`
	Models map[string]providerModel `json:"models"`
}

type providerModel struct {
	ID         string                  `json:"id"`
	Name       string                  `json:"name"`
	Limit      map[string]any          `json:"limit"`
	Reasoning  bool                    `json:"reasoning"`
	ToolCall   bool                    `json:"tool_call"`
	Modalities providerModelModalities `json:"modalities"`
	Options    map[string]any          `json:"options"`
}

type providerModelModalities struct {
	Input []string `json:"input"`
}

type processIdentity struct {
	StartTime string
	Cmdline   []string
	Env       map[string]string
}

var (
	hermesMarshalIndent  = json.MarshalIndent
	hermesWriteLease     = writeLease
	hermesInspectProcess = inspectHermesProcess
)

func startHermesServer(ctx context.Context, options hermesStartOptions) (hermesClient, error) {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	root := options.Root
	if root == "" {
		root = filepath.Join(os.TempDir(), "acp-go-hermes")
	}
	if err := reapStaleLeases(root, options.Logger); err != nil {
		return nil, err
	}
	xdg := options.ExistingXDG
	if xdg.Root == "" {
		var err error
		xdg, err = createXDGDirs(root, string(options.ACPSessionID))
		if err != nil {
			return nil, err
		}
	}
	if err := ensureXDGDirs(xdg); err != nil {
		return nil, err
	}
	if err := materializeHermesMCPConfig(xdg.Root, options.MCPServers); err != nil {
		return nil, err
	}
	proc, err := nativehermes.Start(ctx, nativehermes.ProcessOptions{
		ExecutablePath: options.ExecutablePath,
		Home:           xdg.Root,
		Cwd:            options.Cwd,
		Env:            options.Env,
		Timeout:        options.HealthTimeout,
		Configure:      configureHermesProcess,
	})
	if err != nil {
		return nil, err
	}
	lease := serverLease{
		PID:       proc.Cmd.Process.Pid,
		Port:      proc.Port,
		StartedAt: time.Now().UnixMilli(),
		TokenHash: passwordHash(proc.Token),
		XDGRoot:   xdg.Root,
	}
	if identity, err := hermesInspectProcess(proc.Cmd.Process.Pid); err == nil {
		lease.ProcessStartTime = identity.StartTime
	}
	if err := hermesWriteLease(xdg.State, lease); err != nil {
		_ = proc.Close(context.Background())
		return nil, err
	}
	return &hermesServer{
		cmd:          proc.Cmd,
		xdg:          xdg,
		log:          options.Logger,
		events:       make(chan hermesEvent, 256),
		errs:         make(chan error, 8),
		closed:       make(chan struct{}),
		gateway:      proc.Client,
		process:      proc,
		liveByStored: make(map[string]string),
		storedByLive: make(map[string]string),
		cwd:          options.Cwd,
		defaultModel: options.DefaultModel,
	}, nil
}

func (s *hermesServer) Close(ctx context.Context) error {
	var err error
	s.once.Do(func() {
		close(s.closed)
		if s.process != nil {
			err = s.process.Close(ctx)
		} else if s.gateway != nil {
			err = s.gateway.Close(1000, "closing")
		}
		removeErr := os.Remove(filepath.Join(s.xdg.State, leaseFileName))
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		err = errors.Join(err, removeErr)
	})

	return err
}

func (s *hermesServer) Events() <-chan hermesEvent {
	return s.events
}

func (s *hermesServer) EventErrors() <-chan error {
	return s.errs
}

func (s *hermesServer) XDGDirs() xdgDirs {
	return s.xdg
}

func (s *hermesServer) CreateSession(ctx context.Context, title string) (nativeSession, error) {
	params := map[string]any{"cwd": s.cwd, "source": "acp-go-hermes"}
	if title != "" {
		params["title"] = title
	}
	if provider, model := splitModelValue(s.defaultModel, "", ""); model != "" {
		params["model"] = model
		if provider != "" {
			params["provider"] = provider
		}
	}
	result, err := s.gateway.CreateSession(ctx, params)
	if err != nil {
		return nativeSession{}, err
	}
	stored := firstNonEmpty(result.StoredSessionID, result.SessionID)
	s.rememberGatewaySession(stored, result.SessionID)
	return s.nativeSessionFromGateway(stored, title), nil
}

func (s *hermesServer) GetSession(ctx context.Context, id string) (nativeSession, error) {
	if s.liveSessionID(id) == "" {
		result, err := s.gateway.ResumeSession(ctx, id, map[string]any{})
		if err != nil {
			return nativeSession{}, err
		}
		s.rememberGatewaySession(firstNonEmpty(result.StoredSessionID, id), result.SessionID)
	}
	return s.nativeSessionFromGateway(id, ""), nil
}

func (s *hermesServer) ListSessions(ctx context.Context, cwd string) ([]nativeSession, error) {
	active, err := s.gateway.ActiveList(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]nativeSession, 0, len(active.Sessions))
	for _, item := range active.Sessions {
		stored := firstNonEmpty(item.SessionKey, s.storedSessionID(item.SessionID), item.SessionID)
		if item.SessionID != "" {
			s.rememberGatewaySession(stored, item.SessionID)
		}
		session := s.nativeSessionFromGateway(stored, item.Title)
		session.Directory = firstNonEmpty(item.Cwd, s.cwd)
		if cwd == "" || session.Directory == cwd {
			out = append(out, session)
		}
	}
	return out, nil
}

func (s *hermesServer) DeleteSession(ctx context.Context, id string) error {
	err := s.gateway.DeleteSession(ctx, id)
	if nativehermes.IsNotFound(err) {
		err = nil
	}
	s.forgetGatewaySession(id)
	return err
}

func (s *hermesServer) SendMessage(ctx context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
	return s.submitGatewayText(ctx, id, textFromHermesParts(req.Parts))
}

func assistantMessageError(message nativeMessage) error {
	if !strings.EqualFold(message.Info.Finish, "error") && message.Info.Error == nil {
		return nil
	}
	if message.Info.Error == nil {
		return fmt.Errorf("hermes assistant error")
	}
	detail := firstNonEmpty(message.Info.Error.Message, message.Info.Error.Type, message.Info.Error.Name)
	if detail == "" {
		return fmt.Errorf("hermes assistant error")
	}
	return fmt.Errorf("hermes assistant error: %s", detail)
}

func (s *hermesServer) rememberGatewaySession(stored string, live string) {
	if stored == "" || live == "" {
		return
	}
	s.gatewayMu.Lock()
	defer s.gatewayMu.Unlock()
	s.liveByStored[stored] = live
	s.storedByLive[live] = stored
}

func (s *hermesServer) forgetGatewaySession(stored string) {
	s.gatewayMu.Lock()
	defer s.gatewayMu.Unlock()
	live := s.liveByStored[stored]
	delete(s.liveByStored, stored)
	if live != "" {
		delete(s.storedByLive, live)
	}
}

func (s *hermesServer) liveSessionID(stored string) string {
	s.gatewayMu.Lock()
	defer s.gatewayMu.Unlock()
	return s.liveByStored[stored]
}

func (s *hermesServer) storedSessionID(live string) string {
	s.gatewayMu.Lock()
	defer s.gatewayMu.Unlock()
	return s.storedByLive[live]
}

func (s *hermesServer) anyLiveSessionID() string {
	s.gatewayMu.Lock()
	defer s.gatewayMu.Unlock()
	for _, live := range s.liveByStored {
		return live
	}
	return ""
}

func (s *hermesServer) ensureLiveGatewaySession(ctx context.Context, stored string) (string, error) {
	if live := s.liveSessionID(stored); live != "" {
		return live, nil
	}
	result, err := s.gateway.ResumeSession(ctx, stored, map[string]any{})
	if err != nil {
		return "", err
	}
	s.rememberGatewaySession(firstNonEmpty(result.StoredSessionID, stored), result.SessionID)
	return result.SessionID, nil
}

func (s *hermesServer) nativeSessionFromGateway(stored string, title string) nativeSession {
	provider, model := splitModelValue(s.defaultModel, "", s.defaultModel)
	now := time.Now().UnixMilli()
	return nativeSession{
		ID:        stored,
		Title:     firstNonEmpty(title, "Hermes session"),
		Directory: s.cwd,
		Model: struct {
			ID         string `json:"id"`
			ModelID    string `json:"modelID"`
			ProviderID string `json:"providerID"`
		}{ID: model, ModelID: model, ProviderID: provider},
		Time: struct {
			Created int64 `json:"created"`
			Updated int64 `json:"updated"`
		}{Created: now, Updated: now},
	}
}

func textFromHermesParts(parts []map[string]any) string {
	var builder strings.Builder
	for _, part := range parts {
		if text, _ := part["text"].(string); text != "" {
			if builder.Len() > 0 {
				builder.WriteString("\n\n")
			}
			builder.WriteString(text)
		}
	}
	return builder.String()
}

func (s *hermesServer) submitGatewayText(ctx context.Context, stored string, text string) (nativeMessage, error) {
	live, err := s.ensureLiveGatewaySession(ctx, stored)
	if err != nil {
		return nativeMessage{}, err
	}
	messageID := "hermes-" + live
	if err := s.gateway.SubmitPrompt(ctx, live, text); err != nil {
		return nativeMessage{}, err
	}
	var textBuilder strings.Builder
	for {
		select {
		case event, ok := <-s.gateway.Events():
			if !ok {
				return nativeMessage{}, errors.New("hermes gateway event stream closed")
			}
			if event.SessionID != "" && event.SessionID != live {
				continue
			}
			switch event.Type {
			case "approval.request":
				s.forwardGatewayPermission(stored, live, event)
			case "clarify.request":
				s.forwardGatewayQuestion(stored, live, event)
			case "terminal.read.request", "sudo.request", "secret.request":
				s.declineGatewayQuestion(ctx, live, event.Type)
			case "message.delta", "thinking.delta":
				chunk := gatewayEventText(event.Payload)
				if chunk == "" {
					continue
				}
				if event.Type == "message.delta" {
					textBuilder.WriteString(chunk)
				}
				s.forwardGatewayPart(stored, messageID, event, chunk)
			case "message.complete":
				tokens := gatewayUsageTokens(event.Payload)
				return nativeMessage{
					Info: nativeMessageInfo{
						ID:        messageID,
						SessionID: stored,
						Role:      "assistant",
						Finish:    "stop",
						Tokens:    tokens,
					},
					Parts: []nativePart{{
						ID:        messageID + "-text",
						SessionID: stored,
						MessageID: messageID,
						Type:      "text",
						Text:      textBuilder.String(),
					}},
				}, nil
			}
		case err := <-s.gateway.Errors():
			if err != nil {
				return nativeMessage{}, err
			}
		case <-ctx.Done():
			return nativeMessage{}, ctx.Err()
		}
	}
}

func (s *hermesServer) forwardGatewayPart(stored string, messageID string, event nativehermes.Event, text string) {
	partType := "text"
	if event.Type == "thinking.delta" {
		partType = "reasoning"
	}
	part := nativePart{
		ID:        messageID + "-" + partType,
		SessionID: stored,
		MessageID: messageID,
		Type:      partType,
		Text:      text,
		Raw:       event.Raw,
	}
	data, _ := json.Marshal(part)
	select {
	case s.events <- hermesEvent{Type: "message.part.updated", Properties: data, Raw: event.Raw}:
	default:
	}
}

func (s *hermesServer) forwardGatewayPermission(stored string, live string, event nativehermes.Event) {
	req := permissionRequest{
		ID:         firstNonEmpty(gatewayPayloadString(event.Payload, "id"), gatewayPayloadString(event.Payload, "request_id"), "approval"),
		SessionID:  stored,
		Action:     firstNonEmpty(gatewayPayloadString(event.Payload, "title"), gatewayPayloadString(event.Payload, "command"), "approval"),
		Metadata:   map[string]any{"liveSessionId": live},
		ReplyRoute: permissionRouteAPI,
	}
	data, _ := json.Marshal(req)
	select {
	case s.events <- hermesEvent{Type: "permission.v2.asked", Properties: data, Raw: event.Raw}:
	default:
	}
}

func (s *hermesServer) forwardGatewayQuestion(stored string, live string, event nativehermes.Event) {
	question := firstNonEmpty(gatewayPayloadString(event.Payload, "question"), gatewayPayloadString(event.Payload, "prompt"), "Hermes needs input")
	req := questionRequest{
		ID:        firstNonEmpty(gatewayPayloadString(event.Payload, "id"), gatewayPayloadString(event.Payload, "request_id"), "clarify"),
		SessionID: stored,
		Questions: []questionInfo{{
			Question: question,
			Header:   "Hermes question",
			Custom:   true,
		}},
		ReplyRoute: questionRouteAPI,
	}
	data, _ := json.Marshal(req)
	select {
	case s.events <- hermesEvent{Type: "question.v2.asked", Properties: data, Raw: event.Raw}:
	default:
	}
	_ = live
}

func (s *hermesServer) declineGatewayQuestion(ctx context.Context, live string, eventType string) {
	switch eventType {
	case "terminal.read.request":
		_ = s.gateway.Call(ctx, "terminal.read.respond", map[string]any{"session_id": live, "text": ""}, nil)
	case "sudo.request":
		_ = s.gateway.Call(ctx, "sudo.respond", map[string]any{"session_id": live, "password": ""}, nil)
	case "secret.request":
		_ = s.gateway.Call(ctx, "secret.respond", map[string]any{"session_id": live, "value": ""}, nil)
	}
}

func gatewayPayloadString(raw json.RawMessage, key string) string {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	if value, _ := payload[key].(string); value != "" {
		return value
	}
	return ""
}

func gatewayEventText(raw json.RawMessage) string {
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return firstPayloadString(payload, "text", "delta", "content")
}

func firstPayloadString(value any, keys ...string) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		for _, item := range typed {
			if out := firstPayloadString(item, keys...); out != "" {
				return out
			}
		}
	case map[string]any:
		for _, key := range keys {
			if out, _ := typed[key].(string); out != "" {
				return out
			}
		}
		for _, item := range typed {
			if out := firstPayloadString(item, keys...); out != "" {
				return out
			}
		}
	}
	return ""
}

func gatewayUsageTokens(raw json.RawMessage) nativeTokens {
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	usage, _ := payload["usage"].(map[string]any)
	return nativeTokens{
		Total:     numberValue(usage["total_tokens"], usage["total"]),
		Input:     numberValue(usage["input_tokens"], usage["prompt_tokens"], usage["input"]),
		Output:    numberValue(usage["output_tokens"], usage["completion_tokens"], usage["output"]),
		Reasoning: numberValue(usage["reasoning_tokens"], usage["reasoning"]),
	}
}

func numberValue(values ...any) float64 {
	for _, value := range values {
		switch typed := value.(type) {
		case float64:
			return typed
		case int:
			return float64(typed)
		case json.Number:
			out, _ := typed.Float64()
			return out
		}
	}
	return 0
}

func nativeMessagesFromGateway(stored string, messages []nativehermes.Message) []nativeMessage {
	out := make([]nativeMessage, 0, len(messages))
	for index, message := range messages {
		messageID := fmt.Sprintf("history-%d", index+1)
		text := gatewayMessageText(message)
		out = append(out, nativeMessage{
			Info: nativeMessageInfo{
				ID:        messageID,
				SessionID: stored,
				Role:      firstNonEmpty(message.Role, "assistant"),
				Finish:    "stop",
			},
			Parts: []nativePart{{
				ID:        messageID + "-text",
				SessionID: stored,
				MessageID: messageID,
				Type:      "text",
				Text:      text,
				Raw:       message.Raw,
			}},
		})
	}
	return out
}

func gatewayMessageText(message nativehermes.Message) string {
	if len(message.Content) == 0 {
		return ""
	}
	if out := gatewayEventText(message.Content); out != "" {
		return out
	}
	return string(message.Content)
}

func providersFromGateway(result nativehermes.ModelOptionsResult) providersResponse {
	providers := make([]providerInfo, 0, len(result.Providers))
	for _, provider := range result.Providers {
		info := providerInfo{
			ID:     provider.ID,
			Name:   firstNonEmpty(provider.Name, provider.ID),
			Models: map[string]providerModel{},
		}
		for _, model := range provider.Models {
			id := firstNonEmpty(model.ID, model.Name)
			if id == "" {
				continue
			}
			info.Models[id] = providerModel{
				ID:   id,
				Name: firstNonEmpty(model.Name, id),
				Limit: map[string]any{
					"context": model.Context,
					"output":  model.MaxOutput,
				},
				Reasoning: slices.Contains(model.Capabilities, "reasoning"),
				ToolCall:  slices.Contains(model.Capabilities, "tools"),
			}
		}
		providers = append(providers, info)
	}
	return providersResponse{Providers: providers, Raw: result.Raw}
}

func (s *hermesServer) Messages(ctx context.Context, id string) ([]nativeMessage, error) {
	live, err := s.ensureLiveGatewaySession(ctx, id)
	if err != nil {
		return nil, err
	}
	history, err := s.gateway.History(ctx, live)
	if err != nil {
		return nil, err
	}
	return nativeMessagesFromGateway(id, history.Messages), nil
}

func (s *hermesServer) Abort(ctx context.Context, id string) error {
	live := s.liveSessionID(id)
	if live == "" {
		return nil
	}
	return s.gateway.Interrupt(ctx, live)
}

func (s *hermesServer) Fork(ctx context.Context, id string, messageID string) (nativeSession, error) {
	_ = messageID
	live, err := s.ensureLiveGatewaySession(ctx, id)
	if err != nil {
		return nativeSession{}, err
	}
	result, err := s.gateway.Branch(ctx, live, "")
	if nativehermes.IsNotFound(err) {
		s.forgetGatewaySession(id)
		live, err = s.ensureLiveGatewaySession(ctx, id)
		if err != nil {
			return nativeSession{}, err
		}
		result, err = s.gateway.Branch(ctx, live, "")
	}
	if err != nil {
		return nativeSession{}, err
	}
	if result.SessionID == "" {
		return nativeSession{}, fmt.Errorf("hermes branch response missing session_id")
	}
	stored := result.StoredSessionID
	if stored == "" {
		stored, err = s.storedSessionIDForLive(ctx, result.SessionID)
		if err != nil {
			return nativeSession{}, err
		}
	}
	s.rememberGatewaySession(stored, result.SessionID)
	return s.nativeSessionFromGateway(stored, "Hermes branch"), nil
}

func (s *hermesServer) storedSessionIDForLive(ctx context.Context, live string) (string, error) {
	active, err := s.gateway.ActiveList(ctx)
	if err != nil {
		return "", fmt.Errorf("hermes branch active_list lookup failed: %w", err)
	}
	for _, item := range active.Sessions {
		if item.SessionID != live {
			continue
		}
		if item.SessionKey == "" {
			return "", fmt.Errorf("hermes branch active_list missing session_key for live session %q", live)
		}
		return item.SessionKey, nil
	}
	return "", fmt.Errorf("hermes branch active_list missing live session %q", live)
}

func (s *hermesServer) Todos(ctx context.Context, id string) ([]nativeTodo, error) {
	_, _ = ctx, id
	return nil, nil
}

func (s *hermesServer) ConfigProviders(ctx context.Context) (providersResponse, error) {
	live := s.anyLiveSessionID()
	models, err := s.gateway.ModelOptions(ctx, live)
	if err != nil {
		return providersResponse{}, err
	}
	return providersFromGateway(models), nil
}

func (s *hermesServer) Agents(ctx context.Context) ([]nativeAgent, error) {
	_ = ctx
	return nil, nil
}

func (s *hermesServer) PendingPermissions(ctx context.Context) ([]permissionRequest, error) {
	_ = ctx
	return nil, nil
}

func (s *hermesServer) ReplyPermission(ctx context.Context, req permissionRequest, reply string, message string) error {
	_ = message
	choice := "deny"
	switch reply {
	case "once", "always":
		choice = reply
	}
	live := s.liveSessionID(req.SessionID)
	if live == "" {
		live = req.SessionID
	}
	return s.gateway.ApprovalRespond(ctx, live, choice, reply == "always")
}

func (s *hermesServer) PendingQuestions(ctx context.Context) ([]questionRequest, error) {
	_ = ctx
	return nil, nil
}

func (s *hermesServer) ReplyQuestion(ctx context.Context, req questionRequest, answers [][]string) error {
	live := s.liveSessionID(req.SessionID)
	if live == "" {
		live = req.SessionID
	}
	return s.gateway.ClarifyRespond(ctx, live, answers)
}

func (s *hermesServer) RejectQuestion(ctx context.Context, req questionRequest) error {
	live := s.liveSessionID(req.SessionID)
	if live == "" {
		live = req.SessionID
	}
	return s.gateway.ClarifyRespond(ctx, live, "")
}

type streamError struct {
	epoch uint64
	err   error
}

func (e streamError) Error() string {
	return e.err.Error()
}

func (e streamError) Unwrap() error {
	return e.err
}

func streamErrorEpoch(err error) uint64 {
	var streamErr streamError
	if errors.As(err, &streamErr) {
		return streamErr.epoch
	}
	return 0
}

func createXDGDirs(root string, sessionID string) (xdgDirs, error) {
	if sessionID == "" {
		sessionID = "session"
	}
	base := filepath.Join(root, safePathName(sessionID))
	dirs := xdgDirs{
		Root:   base,
		Data:   filepath.Join(base, "data"),
		Config: filepath.Join(base, "config"),
		Cache:  filepath.Join(base, "cache"),
		State:  filepath.Join(base, "state"),
	}
	return dirs, ensureXDGDirs(dirs)
}

func ensureXDGDirs(dirs xdgDirs) error {
	for _, dir := range []string{dirs.Root, dirs.Data, dirs.Config, dirs.Cache, dirs.State} {
		if dir == "" {
			return fmt.Errorf("xdg directory is empty")
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func materializeHermesMCPConfig(home string, servers []acp.McpServer) error {
	if len(servers) == 0 {
		return nil
	}
	config := map[string]any{"mcp_servers": map[string]any{}}
	mcpServers := config["mcp_servers"].(map[string]any)
	for index, server := range servers {
		switch {
		case server.Stdio != nil:
			name := firstNonEmpty(server.Stdio.Name, fmt.Sprintf("server_%d", index+1))
			env := map[string]string{}
			for _, item := range server.Stdio.Env {
				env[item.Name] = item.Value
			}
			entry := map[string]any{
				"command": server.Stdio.Command,
				"args":    append([]string(nil), server.Stdio.Args...),
			}
			if len(env) > 0 {
				entry["env"] = env
			}
			mcpServers[name] = entry
		case server.Http != nil:
			name := firstNonEmpty(server.Http.Name, fmt.Sprintf("server_%d", index+1))
			headers := map[string]string{}
			for _, item := range server.Http.Headers {
				headers[item.Name] = item.Value
			}
			entry := map[string]any{"url": server.Http.Url}
			if len(headers) > 0 {
				entry["headers"] = headers
			}
			mcpServers[name] = entry
		default:
			return acp.NewInvalidParams(map[string]any{"field": fmt.Sprintf("mcpServers[%d]", index)})
		}
	}
	data, err := hermesMarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(home, "config.yaml"), data, 0o600)
}

func passwordHash(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}

type serverLease struct {
	PID              int    `json:"pid"`
	Port             int    `json:"port"`
	StartedAt        int64  `json:"startedAtUnixMilli"`
	TokenHash        string `json:"tokenHash"`
	XDGRoot          string `json:"xdgRoot,omitempty"`
	ProcessStartTime string `json:"processStartTime,omitempty"`
}

func writeLease(stateDir string, lease serverLease) error {
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return err
	}
	data, err := hermesMarshalIndent(lease, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(stateDir, leaseFileName), data, 0o600)
}

func reapStaleLeases(root string, log *slog.Logger) error {
	if root == "" {
		return nil
	}
	matches, err := filepath.Glob(filepath.Join(root, "*", "state", leaseFileName))
	if err != nil {
		return err
	}
	for _, match := range matches {
		reapLeaseFile(match, log)
	}
	return nil
}

func reapLeaseFile(path string, log *slog.Logger) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		return
	}
	var lease serverLease
	if err := json.Unmarshal(data, &lease); err != nil {
		_ = os.Remove(path)
		return
	}
	if lease.PID > 0 && leaseMatchesProcess(path, lease) {
		if err := killProcessID(lease.PID); err != nil && log != nil {
			log.Debug("reap stale hermes lease failed", slog.Int("pid", lease.PID), slog.String("error", err.Error()))
		}
	}
	_ = os.Remove(path)
}

func leaseMatchesProcess(path string, lease serverLease) bool {
	if lease.PID <= 0 || lease.ProcessStartTime == "" {
		return false
	}
	identity, err := hermesInspectProcess(lease.PID)
	if err != nil {
		return false
	}
	if identity.StartTime != lease.ProcessStartTime {
		return false
	}
	if passwordHash(identity.Env["HERMES_DASHBOARD_SESSION_TOKEN"]) != lease.TokenHash {
		return false
	}
	root := firstNonEmpty(lease.XDGRoot, filepath.Dir(filepath.Dir(path)))
	if filepath.Clean(identity.Env["HERMES_HOME"]) != filepath.Clean(root) {
		return false
	}
	return cmdlineLooksLikeHermesServe(identity.Cmdline)
}

func cmdlineLooksLikeHermesServe(args []string) bool {
	for _, arg := range args {
		if arg == "serve" {
			return true
		}
	}
	for _, arg := range args {
		if strings.Contains(filepath.Base(arg), "hermes") {
			return true
		}
	}
	return false
}

func safePathName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "session"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "..", "_")
	return replacer.Replace(value)
}

func intFromNumber(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		if typed > 0 && typed <= math.MaxInt {
			return int(typed), true
		}
	case int:
		if typed > 0 {
			return typed, true
		}
	case json.Number:
		n, err := typed.Int64()
		if err == nil && n > 0 && n <= int64(math.MaxInt) {
			return int(n), true
		}
	}
	return 0, false
}
