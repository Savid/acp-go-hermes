//nolint:tagliatelle // Hermes native JSON fields use modelID/sessionID/providerID spellings.
package hermesacp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	hermesDefaultUsername = "hermes"
	leaseFileName         = "server.lease"
)

var errHermesSSEDisconnect = errors.New("hermes SSE disconnected")

type hermesClient interface {
	Close(context.Context) error
	CreateSession(context.Context, string) (nativeSession, error)
	GetSession(context.Context, string) (nativeSession, error)
	ListSessions(context.Context, string) ([]nativeSession, error)
	DeleteSession(context.Context, string) error
	Commands(context.Context) ([]nativeCommand, error)
	RunCommand(context.Context, string, hermesCommandRequest) (nativeMessage, error)
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
	ACPSessionID      acpSessionIDString
	Root              string
	Cwd               string
	ExecutablePath    string
	DefaultModel      string
	Env               map[string]string
	Pure              bool
	QuestionTool      bool
	LogLevel          string
	MinimumVersion    string
	HealthTimeout     time.Duration
	Logger            *slog.Logger
	ExistingXDG       xdgDirs
	SkipVersionGate   bool
	AdditionalEnv     map[string]string
	ExpectedNativeID  string
	PermissionSurface bool
	Permission        string
	MCPServers        []acp.McpServer
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
	httpClient                   *http.Client
	baseURL                      string
	username                     string
	password                     string
	cmd                          *exec.Cmd
	cancel                       context.CancelFunc
	xdg                          xdgDirs
	log                          *slog.Logger
	sessionPermissionListSupport bool
	sessionQuestionListSupport   bool

	events chan hermesEvent
	errs   chan error
	closed chan struct{}
	once   sync.Once

	streamMu    sync.Mutex
	streamEpoch uint64

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

type nativeSessionStatus struct {
	Type string `json:"type"`
}

type nativeAgent struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Mode        string `json:"mode"`
}

type nativeCommand struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Agent       string   `json:"agent,omitempty"`
	Model       string   `json:"model,omitempty"`
	Source      string   `json:"source,omitempty"`
	Template    any      `json:"template,omitempty"`
	Subtask     bool     `json:"subtask,omitempty"`
	Hints       []string `json:"hints"`
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

type hermesCommandRequest struct {
	MessageID string           `json:"messageID,omitempty"`
	Agent     string           `json:"agent,omitempty"`
	Model     string           `json:"model,omitempty"`
	Command   string           `json:"command"`
	Arguments string           `json:"arguments"`
	Parts     []map[string]any `json:"parts,omitempty"`
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
	hermesCommandContext                = exec.CommandContext
	hermesListen                        = net.Listen
	hermesRandReader          io.Reader = rand.Reader
	hermesMarshalIndent                 = json.MarshalIndent
	hermesWriteLease                    = writeLease
	hermesTerminateProcess              = terminateHermesProcess
	hermesKillProcess                   = killHermesProcess
	hermesInspectProcess                = inspectHermesProcess
	hermesWaitCommand                   = func(cmd *exec.Cmd) error { return cmd.Wait() }
	hermesAfter                         = time.After
	hermesReadyPollInterval             = 100 * time.Millisecond
	hermesEventReconnectDelay           = 250 * time.Millisecond
	hermesShutdownTimeout               = 5 * time.Second
)

func startHermesServer(ctx context.Context, options hermesStartOptions) (hermesClient, error) {
	if shouldUseHermesGateway(options.ExecutablePath) {
		return startHermesGatewayServer(ctx, options)
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.HealthTimeout <= 0 {
		options.HealthTimeout = 15 * time.Second
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
	permissionConfig, err := materializeHermesPermissionConfig(xdg, options.Permission)
	if err != nil {
		return nil, err
	}

	port, err := allocatePort()
	if err != nil {
		return nil, err
	}
	password, err := randomPassword()
	if err != nil {
		return nil, err
	}
	username := hermesDefaultUsername
	executable := options.ExecutablePath
	if executable == "" {
		executable = "hermes"
	}
	args := []string{"serve", "--hostname", "127.0.0.1", "--port", strconv.Itoa(port)}
	if options.Pure {
		args = append(args, "--pure")
	}
	if options.LogLevel != "" {
		args = append(args, "--log-level", options.LogLevel)
	}

	processCtx, cancel := context.WithCancel(context.Background())
	cmd := hermesCommandContext(processCtx, executable, args...)
	if options.Cwd != "" {
		cmd.Dir = options.Cwd
	}
	env := mergeProcessEnv(options.Env)
	for key, value := range options.AdditionalEnv {
		env[key] = value
	}
	env["XDG_DATA_HOME"] = xdg.Data
	env["XDG_CONFIG_HOME"] = xdg.Config
	env["XDG_CACHE_HOME"] = xdg.Cache
	env["XDG_STATE_HOME"] = xdg.State
	env["HERMES_SERVER_USERNAME"] = username
	env["HERMES_SERVER_PASSWORD"] = password
	env["HERMES_CONFIG_CONTENT"] = permissionConfig
	if options.QuestionTool {
		env["HERMES_ENABLE_QUESTION_TOOL"] = "1"
	}
	cmd.Env = envMapToSlice(env)
	configureHermesProcess(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := hermesWriteLease(xdg.State, serverLease{
		PID:          0,
		Port:         port,
		StartedAt:    time.Now().UnixMilli(),
		PasswordHash: passwordHash(password),
		XDGRoot:      xdg.Root,
	}); err != nil {
		cancel()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}
	lease := serverLease{
		PID:          cmd.Process.Pid,
		Port:         port,
		StartedAt:    time.Now().UnixMilli(),
		PasswordHash: passwordHash(password),
		XDGRoot:      xdg.Root,
	}
	if identity, err := hermesInspectProcess(cmd.Process.Pid); err == nil {
		lease.ProcessStartTime = identity.StartTime
	}
	if err := hermesWriteLease(xdg.State, lease); err != nil {
		cancel()
		_ = hermesKillProcess(cmd)
		return nil, err
	}
	go drainProcessPipe(options.Logger, "hermes stdout", stdout)
	go drainProcessPipe(options.Logger, "hermes stderr", stderr)

	server := &hermesServer{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    "http://127.0.0.1:" + strconv.Itoa(port),
		username:   username,
		password:   password,
		cmd:        cmd,
		cancel:     cancel,
		xdg:        xdg,
		log:        options.Logger,
		events:     make(chan hermesEvent, 256),
		errs:       make(chan error, 8),
		closed:     make(chan struct{}),
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, options.HealthTimeout)
	defer readyCancel()
	if err := server.waitReady(readyCtx, processCtx, options); err != nil {
		_ = server.Close(context.Background())
		return nil, err
	}

	return server, nil
}

func shouldUseHermesGateway(executable string) bool {
	if strings.HasSuffix(filepath.Base(os.Args[0]), ".test") && os.Getenv("ACP_GO_HERMES_FORCE_GATEWAY") == "" {
		return false
	}
	return executable == "" || filepath.Base(executable) == "hermes"
}

func startHermesGatewayServer(ctx context.Context, options hermesStartOptions) (hermesClient, error) {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	root := options.Root
	if root == "" {
		root = filepath.Join(os.TempDir(), "acp-go-hermes")
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
		Timeout:        2 * options.HealthTimeout,
		Configure:      configureHermesProcess,
	})
	if err != nil {
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

func (s *hermesServer) waitReady(ctx context.Context, eventCtx context.Context, options hermesStartOptions) error {
	var health struct {
		Healthy bool   `json:"healthy"`
		Version string `json:"version"`
	}
	for {
		err := s.getJSON(ctx, "/global/health", nil, &health)
		if err == nil && health.Healthy {
			break
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("hermes health check failed: %w", err)
			}
			return ctx.Err()
		case <-hermesAfter(hermesReadyPollInterval):
		}
	}
	if !options.SkipVersionGate && options.MinimumVersion != "" && compareSemver(health.Version, options.MinimumVersion) < 0 {
		return fmt.Errorf("hermes %s is below minimum %s", health.Version, options.MinimumVersion)
	}

	var doc map[string]any
	if err := s.getJSON(ctx, "/doc", nil, &doc); err != nil {
		return fmt.Errorf("load hermes /doc: %w", err)
	}
	docCapabilities, err := inspectHermesDoc(doc)
	if err != nil {
		return err
	}
	s.sessionPermissionListSupport = docCapabilities.sessionPermissionList
	s.sessionQuestionListSupport = docCapabilities.sessionQuestionList
	go s.readEvents(eventCtx)
	select {
	case event := <-s.events:
		if event.Type != "server.connected" {
			return fmt.Errorf("first hermes event was %q, want server.connected", event.Type)
		}
	case err := <-s.errs:
		return fmt.Errorf("hermes event stream failed during readiness: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (s *hermesServer) Close(ctx context.Context) error {
	if s.process != nil {
		return s.process.Close(ctx)
	}
	var err error
	s.once.Do(func() {
		close(s.closed)
		if s.cancel != nil {
			s.cancel()
		}
		if s.cmd != nil && s.cmd.Process != nil {
			_ = hermesTerminateProcess(s.cmd)
			done := make(chan error, 1)
			go func() { done <- hermesWaitCommand(s.cmd) }()
			select {
			case waitErr := <-done:
				if waitErr != nil && s.log != nil {
					s.log.DebugContext(ctx, "hermes exited during shutdown", slog.Any("error", waitErr))
				}
			case <-ctx.Done():
				_ = hermesKillProcess(s.cmd)
				err = ctx.Err()
			case <-hermesAfter(hermesShutdownTimeout):
				_ = hermesKillProcess(s.cmd)
				err = errors.New("hermes process did not exit after shutdown")
			}
		}
		_ = os.Remove(filepath.Join(s.xdg.State, leaseFileName))
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
	if s.gateway != nil {
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
		s.rememberGatewaySession(result.StoredSessionID, result.SessionID)
		return s.nativeSessionFromGateway(result.StoredSessionID, title), nil
	}
	body := map[string]any{}
	if title != "" {
		body["title"] = title
	}
	var out nativeSession
	err := s.doJSON(ctx, http.MethodPost, "/session", nil, body, &out)
	return out, err
}

func (s *hermesServer) GetSession(ctx context.Context, id string) (nativeSession, error) {
	if s.gateway != nil {
		if s.liveSessionID(id) == "" {
			result, err := s.gateway.ResumeSession(ctx, id, map[string]any{})
			if err != nil {
				return nativeSession{}, err
			}
			s.rememberGatewaySession(firstNonEmpty(result.StoredSessionID, id), result.SessionID)
		}
		return s.nativeSessionFromGateway(id, ""), nil
	}
	var out nativeSession
	err := s.getJSON(ctx, "/session/"+url.PathEscape(id), nil, &out)
	return out, err
}

func (s *hermesServer) ListSessions(ctx context.Context, cwd string) ([]nativeSession, error) {
	if s.gateway != nil {
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
	query := url.Values{}
	if cwd != "" {
		query.Set("directory", cwd)
	}
	var out []nativeSession
	err := s.getJSON(ctx, "/session", query, &out)
	return out, err
}

func (s *hermesServer) DeleteSession(ctx context.Context, id string) error {
	if s.gateway != nil {
		err := s.gateway.DeleteSession(ctx, id)
		if nativehermes.IsNotFound(err) {
			err = nil
		}
		s.forgetGatewaySession(id)
		return err
	}
	var ignored any
	return s.doJSON(ctx, http.MethodDelete, "/session/"+url.PathEscape(id), nil, nil, &ignored)
}

func (s *hermesServer) Commands(ctx context.Context) ([]nativeCommand, error) {
	if s.gateway != nil {
		return nil, nil
	}
	var out []nativeCommand
	err := s.getJSON(ctx, "/command", nil, &out)
	return out, err
}

func (s *hermesServer) RunCommand(ctx context.Context, id string, req hermesCommandRequest) (nativeMessage, error) {
	if s.gateway != nil {
		text := "/" + req.Command
		if strings.TrimSpace(req.Arguments) != "" {
			text += " " + req.Arguments
		}
		return s.submitGatewayText(ctx, id, text)
	}
	var out nativeMessage
	if err := s.doJSONWithClient(ctx, s.blockingHTTPClient(), http.MethodPost, "/session/"+url.PathEscape(id)+"/command", nil, req, &out); err != nil {
		return nativeMessage{}, err
	}
	if err := assistantMessageError(out); err != nil {
		return nativeMessage{}, err
	}
	return out, nil
}

func (s *hermesServer) SendMessage(ctx context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
	if s.gateway != nil {
		return s.submitGatewayText(ctx, id, textFromHermesParts(req.Parts))
	}
	var out nativeMessage
	if err := s.doJSONWithClient(ctx, s.blockingHTTPClient(), http.MethodPost, "/session/"+url.PathEscape(id)+"/message", nil, req, &out); err != nil {
		return nativeMessage{}, err
	}
	if err := assistantMessageError(out); err != nil {
		return nativeMessage{}, err
	}
	return out, nil
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
	if s.gateway != nil {
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
	var out []nativeMessage
	err := s.getJSON(ctx, "/session/"+url.PathEscape(id)+"/message", nil, &out)
	return out, err
}

func (s *hermesServer) SessionStatus(ctx context.Context) (map[string]nativeSessionStatus, error) {
	out := map[string]nativeSessionStatus{}
	err := s.getJSON(ctx, "/session/status", nil, &out)
	return out, err
}

func (s *hermesServer) Abort(ctx context.Context, id string) error {
	if s.gateway != nil {
		live := s.liveSessionID(id)
		if live == "" {
			return nil
		}
		return s.gateway.Interrupt(ctx, live)
	}
	var ignored any
	return s.doJSON(ctx, http.MethodPost, "/session/"+url.PathEscape(id)+"/abort", nil, map[string]any{}, &ignored)
}

func (s *hermesServer) Fork(ctx context.Context, id string, messageID string) (nativeSession, error) {
	if s.gateway != nil {
		live, err := s.ensureLiveGatewaySession(ctx, id)
		if err != nil {
			return nativeSession{}, err
		}
		result, err := s.gateway.Branch(ctx, live, "")
		if err != nil {
			return nativeSession{}, err
		}
		s.rememberGatewaySession(result.StoredSessionID, result.SessionID)
		return s.nativeSessionFromGateway(result.StoredSessionID, "Hermes branch"), nil
	}
	body := map[string]any{}
	if messageID != "" {
		body["messageID"] = messageID
	}
	var out nativeSession
	err := s.doJSON(ctx, http.MethodPost, "/session/"+url.PathEscape(id)+"/fork", nil, body, &out)
	return out, err
}

func (s *hermesServer) Todos(ctx context.Context, id string) ([]nativeTodo, error) {
	if s.gateway != nil {
		return nil, nil
	}
	select {
	case <-s.closed:
		return nil, context.Canceled
	default:
	}
	var out []nativeTodo
	err := s.getJSON(ctx, "/session/"+url.PathEscape(id)+"/todo", nil, &out)
	return out, err
}

func (s *hermesServer) ConfigProviders(ctx context.Context) (providersResponse, error) {
	if s.gateway != nil {
		live := s.anyLiveSessionID()
		models, err := s.gateway.ModelOptions(ctx, live)
		if err != nil {
			return providersResponse{}, err
		}
		return providersFromGateway(models), nil
	}
	var out providersResponse
	err := s.getJSON(ctx, "/config/providers", nil, &out)
	return out, err
}

func (s *hermesServer) Agents(ctx context.Context) ([]nativeAgent, error) {
	if s.gateway != nil {
		return nil, nil
	}
	var out []nativeAgent
	err := s.getJSON(ctx, "/agent", nil, &out)
	return out, err
}

func (s *hermesServer) PendingPermissions(ctx context.Context) ([]permissionRequest, error) {
	if s.gateway != nil {
		return nil, nil
	}
	var out []permissionRequest
	if s.sessionPermissionListSupport {
		var sessionRequests []permissionRequest
		if err := s.getJSON(ctx, "/permission", nil, &sessionRequests); err != nil {
			return nil, err
		}
		for i := range sessionRequests {
			sessionRequests[i].ReplyRoute = permissionRouteSession
		}
		out = append(out, sessionRequests...)
	}
	var response struct {
		Data []permissionRequest `json:"data"`
	}
	if err := s.getJSON(ctx, "/api/permission/request", nil, &response); err != nil {
		return nil, err
	}
	for i := range response.Data {
		response.Data[i].ReplyRoute = permissionRouteAPI
	}
	out = append(out, response.Data...)
	return out, nil
}

func (s *hermesServer) ReplyPermission(ctx context.Context, req permissionRequest, reply string, message string) error {
	if s.gateway != nil {
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
	body := map[string]any{"reply": reply}
	if message != "" {
		body["message"] = message
	}
	if req.route() == permissionRouteAPI {
		path := "/api/session/" + url.PathEscape(req.SessionID) + "/permission/" + url.PathEscape(req.ID) + "/reply"
		return s.doJSON(ctx, http.MethodPost, path, nil, body, nil)
	}
	path := "/permission/" + url.PathEscape(req.ID) + "/reply"
	return s.doJSON(ctx, http.MethodPost, path, nil, body, nil)
}

func (s *hermesServer) PendingQuestions(ctx context.Context) ([]questionRequest, error) {
	if s.gateway != nil {
		return nil, nil
	}
	var out []questionRequest
	if s.sessionQuestionListSupport {
		var sessionRequests []questionRequest
		if err := s.getJSON(ctx, "/question", nil, &sessionRequests); err != nil {
			return nil, err
		}
		for i := range sessionRequests {
			sessionRequests[i].ReplyRoute = questionRouteSession
		}
		out = append(out, sessionRequests...)
	}
	var response struct {
		Data []questionRequest `json:"data"`
	}
	if err := s.getJSON(ctx, "/api/question/request", nil, &response); err != nil {
		return nil, err
	}
	for i := range response.Data {
		response.Data[i].ReplyRoute = questionRouteAPI
	}
	out = append(out, response.Data...)
	return out, nil
}

func (s *hermesServer) ReplyQuestion(ctx context.Context, req questionRequest, answers [][]string) error {
	if s.gateway != nil {
		live := s.liveSessionID(req.SessionID)
		if live == "" {
			live = req.SessionID
		}
		return s.gateway.ClarifyRespond(ctx, live, answers)
	}
	body := map[string]any{"answers": answers}
	if req.route() == questionRouteAPI {
		path := "/api/session/" + url.PathEscape(req.SessionID) + "/question/" + url.PathEscape(req.ID) + "/reply"
		return s.doJSON(ctx, http.MethodPost, path, nil, body, nil)
	}
	path := "/question/" + url.PathEscape(req.ID) + "/reply"
	return s.doJSON(ctx, http.MethodPost, path, nil, body, nil)
}

func (s *hermesServer) RejectQuestion(ctx context.Context, req questionRequest) error {
	if s.gateway != nil {
		live := s.liveSessionID(req.SessionID)
		if live == "" {
			live = req.SessionID
		}
		return s.gateway.ClarifyRespond(ctx, live, "")
	}
	if req.route() == questionRouteAPI {
		path := "/api/session/" + url.PathEscape(req.SessionID) + "/question/" + url.PathEscape(req.ID) + "/reject"
		return s.doJSON(ctx, http.MethodPost, path, nil, map[string]any{}, nil)
	}
	path := "/question/" + url.PathEscape(req.ID) + "/reject"
	return s.doJSON(ctx, http.MethodPost, path, nil, map[string]any{}, nil)
}

func (s *hermesServer) getJSON(ctx context.Context, path string, query url.Values, out any) error {
	return s.doJSON(ctx, http.MethodGet, path, query, nil, out)
}

func (s *hermesServer) doJSON(ctx context.Context, method string, path string, query url.Values, body any, out any) error {
	return s.doJSONWithClient(ctx, s.httpClient, method, path, query, body, out)
}

func (s *hermesServer) doJSONWithClient(
	ctx context.Context,
	client *http.Client,
	method string,
	path string,
	query url.Values,
	body any,
	out any,
) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	reqURL := s.baseURL + path
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return err
	}
	req.SetBasicAuth(s.username, s.password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &hermesHTTPError{
			Method:     method,
			Path:       path,
			Status:     resp.Status,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(data)),
		}
	}
	if out == nil {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			return err
		}
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

type hermesHTTPError struct {
	Method     string
	Path       string
	Status     string
	StatusCode int
	Body       string
}

func (e *hermesHTTPError) Error() string {
	return fmt.Sprintf("hermes %s %s returned %s: %s", e.Method, e.Path, e.Status, e.Body)
}

func isHermesBadRequest(err error) bool {
	var httpErr *hermesHTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusBadRequest
}

func (s *hermesServer) blockingHTTPClient() *http.Client {
	if s.httpClient == nil {
		return http.DefaultClient
	}
	client := *s.httpClient
	client.Timeout = 0
	return &client
}

func (s *hermesServer) readEvents(ctx context.Context) {
	for {
		epoch := s.nextStreamEpoch()
		if err := s.readEventStream(ctx, epoch); err != nil {
			select {
			case s.errs <- streamError{epoch: epoch, err: err}:
			default:
			}
		}
		select {
		case <-s.closed:
			return
		case <-hermesAfter(hermesEventReconnectDelay):
		}
	}
}

func (s *hermesServer) nextStreamEpoch() uint64 {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	s.streamEpoch++
	return s.streamEpoch
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

func (s *hermesServer) readEventStream(ctx context.Context, epochs ...uint64) error {
	var epoch uint64
	if len(epochs) > 0 {
		epoch = epochs[0]
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/event", http.NoBody)
	if err != nil {
		return err
	}
	req.SetBasicAuth(s.username, s.password)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := s.eventHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("hermes event stream returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var data strings.Builder
	flush := func() error {
		if data.Len() == 0 {
			return nil
		}
		payload := data.String()
		data.Reset()
		var event hermesEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return err
		}
		event.StreamEpoch = epoch
		select {
		case s.events <- event:
		case <-s.closed:
			return io.EOF
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	select {
	case <-s.closed:
		return io.EOF
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("%w: event stream closed", errHermesSSEDisconnect)
	}
}

type hermesDocCapabilities struct {
	sessionPermissionList bool
	sessionQuestionList   bool
}

func validateHermesDoc(doc map[string]any) error {
	_, err := inspectHermesDoc(doc)
	return err
}

func inspectHermesDoc(doc map[string]any) (hermesDocCapabilities, error) {
	rawPaths, _ := doc["paths"].(map[string]any)
	required := []string{
		"/config/providers",
		"/command",
		"/event",
		"/session/status",
		"/session",
		"/session/{sessionID}",
		"/session/{sessionID}/command",
		"/session/{sessionID}/message",
		"/session/{sessionID}/abort",
		"/session/{sessionID}/fork",
		"/session/{sessionID}/todo",
		"/session/{sessionID}/revert",
		"/session/{sessionID}/unrevert",
		"/permission",
		"/permission/{requestID}/reply",
		"/question",
		"/question/{requestID}/reply",
		"/question/{requestID}/reject",
		"/api/session/{sessionID}/permission/{requestID}/reply",
		"/api/permission/request",
		"/api/session/{sessionID}/question/{requestID}/reply",
		"/api/session/{sessionID}/question/{requestID}/reject",
		"/api/question/request",
	}
	for _, path := range required {
		if _, ok := rawPaths[path]; !ok {
			return hermesDocCapabilities{}, fmt.Errorf("hermes version mismatch: /doc missing required path %s", path)
		}
	}
	if err := validateHermesGetListOperation(rawPaths, "/api/permission/request", "PermissionV2Request"); err != nil {
		return hermesDocCapabilities{}, err
	}
	if err := validateHermesPermissionReply(rawPaths); err != nil {
		return hermesDocCapabilities{}, err
	}
	if err := validateHermesSessionPermissionReply(rawPaths); err != nil {
		return hermesDocCapabilities{}, err
	}
	sessionPermissionList, err := validateOptionalHermesGetArrayOperation(rawPaths, "/permission", "PermissionRequest")
	if err != nil {
		return hermesDocCapabilities{}, err
	}
	if err := validateHermesGetListOperation(rawPaths, "/api/question/request", "QuestionV2Request"); err != nil {
		return hermesDocCapabilities{}, err
	}
	if err := validateHermesQuestionReply(doc, rawPaths); err != nil {
		return hermesDocCapabilities{}, err
	}
	if err := validateHermesSessionQuestionRoutes(rawPaths); err != nil {
		return hermesDocCapabilities{}, err
	}
	sessionQuestionList, err := validateOptionalHermesGetArrayOperation(rawPaths, "/question", "QuestionRequest")
	if err != nil {
		return hermesDocCapabilities{}, err
	}
	if err := validateHermesPostNoContent(rawPaths, "/api/session/{sessionID}/question/{requestID}/reject"); err != nil {
		return hermesDocCapabilities{}, err
	}
	if err := validateHermesEventSchemas(doc); err != nil {
		return hermesDocCapabilities{}, err
	}

	return hermesDocCapabilities{
		sessionPermissionList: sessionPermissionList,
		sessionQuestionList:   sessionQuestionList,
	}, nil
}

func (s *hermesServer) eventHTTPClient() *http.Client {
	if s.httpClient == nil {
		return http.DefaultClient
	}
	client := *s.httpClient
	client.Timeout = 0
	return &client
}

type hermesEventContract struct {
	schema             string
	event              string
	requiredProperties []string
}

func validateHermesEventSchemas(doc map[string]any) error {
	for _, contract := range []hermesEventContract{
		{schema: "EventPermissionV2Asked", event: "permission.v2.asked", requiredProperties: []string{"id", "sessionID", "action", "resources"}},
		{schema: "EventPermissionV2Replied", event: "permission.v2.replied", requiredProperties: []string{"sessionID", "requestID", "reply"}},
		{schema: "EventPermissionAsked", event: "permission.asked", requiredProperties: []string{"id", "sessionID", "permission", "patterns"}},
		{schema: "EventPermissionReplied", event: "permission.replied", requiredProperties: []string{"sessionID", "requestID", "reply"}},
		{schema: "EventQuestionV2Asked", event: "question.v2.asked", requiredProperties: []string{"id", "sessionID", "questions"}},
		{schema: "EventQuestionV2Replied", event: "question.v2.replied", requiredProperties: []string{"sessionID", "requestID", "answers"}},
		{schema: "EventQuestionAsked", event: "question.asked", requiredProperties: []string{"id", "sessionID", "questions"}},
		{schema: "EventQuestionReplied", event: "question.replied", requiredProperties: []string{"sessionID", "requestID", "answers"}},
		{schema: "EventMessagePartUpdated", event: "message.part.updated", requiredProperties: []string{"sessionID", "part", "time"}},
		{schema: "EventServerConnected", event: "server.connected"},
	} {
		if err := validateHermesEventSchema(doc, contract); err != nil {
			return err
		}
	}
	return nil
}

func validateHermesEventSchema(doc map[string]any, contract hermesEventContract) error {
	if !openAPIEventUnionHasSchema(doc, contract.schema) {
		return fmt.Errorf("hermes /doc Event union missing %s", contract.schema)
	}
	schema, ok := openAPIComponentSchema(doc, "#/components/schemas/"+contract.schema)
	if !ok {
		return fmt.Errorf("hermes /doc missing event schema %s", contract.schema)
	}
	typeSchema, ok := openAPIObjectProperty(schema, "type")
	if !ok || !openAPIStringEnumContains(typeSchema, contract.event) {
		return fmt.Errorf("hermes /doc event schema %s missing type %q", contract.schema, contract.event)
	}
	propertiesSchema, ok := openAPIObjectProperty(schema, "properties")
	if !ok {
		return fmt.Errorf("hermes /doc event schema %s missing properties schema", contract.schema)
	}
	for _, property := range contract.requiredProperties {
		if !openAPIObjectHasRequiredProperty(propertiesSchema, property) {
			return fmt.Errorf("hermes /doc event schema %s properties missing required %s", contract.schema, property)
		}
	}
	return nil
}

func validateHermesGetListOperation(paths map[string]any, path string, itemRef string) error {
	operation, ok := openAPIOperation(paths, path, http.MethodGet)
	if !ok {
		return fmt.Errorf("hermes /doc path %s missing GET operation", path)
	}
	if !openAPIHasResponse(operation, "200") {
		return fmt.Errorf("hermes /doc GET %s missing 200 response", path)
	}
	schema, ok := openAPIJSONResponseSchema(operation, "200")
	if !ok || !openAPISchemaDataArrayRef(schema, itemRef) {
		return fmt.Errorf("hermes /doc GET %s response schema is not pending %s list", path, itemRef)
	}
	return nil
}

func validateOptionalHermesGetArrayOperation(paths map[string]any, path string, itemRef string) (bool, error) {
	operation, ok := openAPIOperation(paths, path, http.MethodGet)
	if !ok {
		return false, nil
	}
	if !openAPIHasResponse(operation, "200") {
		return false, fmt.Errorf("hermes /doc GET %s missing 200 response", path)
	}
	schema, ok := openAPIJSONResponseSchema(operation, "200")
	if !ok || !openAPISchemaArrayRef(schema, itemRef) {
		return false, fmt.Errorf("hermes /doc GET %s response schema is not pending %s array", path, itemRef)
	}
	return true, nil
}

func validateHermesPermissionReply(paths map[string]any) error {
	const path = "/api/session/{sessionID}/permission/{requestID}/reply"
	operation, ok := openAPIOperation(paths, path, http.MethodPost)
	if !ok {
		return fmt.Errorf("hermes /doc path %s missing POST operation", path)
	}
	if !openAPIHasResponse(operation, "204") {
		return fmt.Errorf("hermes /doc POST %s missing 204 response", path)
	}
	schema, ok := openAPIJSONRequestSchema(operation)
	if !ok {
		return fmt.Errorf("hermes /doc POST %s missing JSON request schema", path)
	}
	if !openAPIObjectHasRequiredProperty(schema, "reply") {
		return fmt.Errorf("hermes /doc POST %s request schema missing required reply", path)
	}
	if !openAPIObjectHasProperty(schema, "message") {
		return fmt.Errorf("hermes /doc POST %s request schema missing message property", path)
	}
	return nil
}

func validateHermesSessionPermissionReply(paths map[string]any) error {
	const path = "/permission/{requestID}/reply"
	operation, ok := openAPIOperation(paths, path, http.MethodPost)
	if !ok {
		return fmt.Errorf("hermes /doc path %s missing POST operation", path)
	}
	if !openAPIHasResponse(operation, "200") {
		return fmt.Errorf("hermes /doc POST %s missing 200 response", path)
	}
	return nil
}

func validateHermesQuestionReply(doc map[string]any, paths map[string]any) error {
	const path = "/api/session/{sessionID}/question/{requestID}/reply"
	operation, ok := openAPIOperation(paths, path, http.MethodPost)
	if !ok {
		return fmt.Errorf("hermes /doc path %s missing POST operation", path)
	}
	if !openAPIHasResponse(operation, "204") {
		return fmt.Errorf("hermes /doc POST %s missing 204 response", path)
	}
	schema, ok := openAPIJSONRequestSchema(operation)
	if !ok {
		return fmt.Errorf("hermes /doc POST %s missing JSON request schema", path)
	}
	if ref, _ := schema["$ref"].(string); ref != "" {
		resolved, ok := openAPIComponentSchema(doc, ref)
		if !ok {
			return fmt.Errorf("hermes /doc POST %s request schema ref %s missing", path, ref)
		}
		schema = resolved
	}
	if !openAPIObjectHasRequiredProperty(schema, "answers") {
		return fmt.Errorf("hermes /doc POST %s request schema missing required answers", path)
	}
	return nil
}

func validateHermesSessionQuestionRoutes(paths map[string]any) error {
	for _, path := range []string{"/question/{requestID}/reply", "/question/{requestID}/reject"} {
		operation, ok := openAPIOperation(paths, path, http.MethodPost)
		if !ok {
			return fmt.Errorf("hermes /doc path %s missing POST operation", path)
		}
		if !openAPIHasResponse(operation, "200") {
			return fmt.Errorf("hermes /doc POST %s missing 200 response", path)
		}
	}
	return nil
}

func validateHermesPostNoContent(paths map[string]any, path string) error {
	operation, ok := openAPIOperation(paths, path, http.MethodPost)
	if !ok {
		return fmt.Errorf("hermes /doc path %s missing POST operation", path)
	}
	if !openAPIHasResponse(operation, "204") {
		return fmt.Errorf("hermes /doc POST %s missing 204 response", path)
	}
	return nil
}

func openAPIOperation(paths map[string]any, path string, method string) (map[string]any, bool) {
	pathItem, _ := paths[path].(map[string]any)
	if pathItem == nil {
		return nil, false
	}
	operation, _ := pathItem[strings.ToLower(method)].(map[string]any)
	return operation, operation != nil
}

func openAPIHasResponse(operation map[string]any, status string) bool {
	responses, _ := operation["responses"].(map[string]any)
	_, ok := responses[status]
	return ok
}

func openAPIJSONResponseSchema(operation map[string]any, status string) (map[string]any, bool) {
	responses, _ := operation["responses"].(map[string]any)
	response, _ := responses[status].(map[string]any)
	return openAPIJSONContentSchema(response)
}

func openAPIJSONRequestSchema(operation map[string]any) (map[string]any, bool) {
	body, _ := operation["requestBody"].(map[string]any)
	if required, _ := body["required"].(bool); !required {
		return nil, false
	}
	return openAPIJSONContentSchema(body)
}

func openAPIJSONContentSchema(container map[string]any) (map[string]any, bool) {
	content, _ := container["content"].(map[string]any)
	jsonContent, _ := content["application/json"].(map[string]any)
	schema, _ := jsonContent["schema"].(map[string]any)
	return schema, schema != nil
}

func openAPISchemaDataArrayRef(schema map[string]any, want string) bool {
	properties, _ := schema["properties"].(map[string]any)
	data, _ := properties["data"].(map[string]any)
	if dataType, _ := data["type"].(string); dataType != "array" {
		return false
	}
	items, _ := data["items"].(map[string]any)
	ref, _ := items["$ref"].(string)
	return strings.HasSuffix(ref, "/"+want)
}

func openAPISchemaArrayRef(schema map[string]any, want string) bool {
	if schemaType, _ := schema["type"].(string); schemaType != "array" {
		return false
	}
	items, _ := schema["items"].(map[string]any)
	ref, _ := items["$ref"].(string)
	return strings.HasSuffix(ref, "/"+want)
}

func openAPIObjectHasRequiredProperty(schema map[string]any, property string) bool {
	properties, _ := schema["properties"].(map[string]any)
	if _, ok := properties[property]; !ok {
		return false
	}
	switch required := schema["required"].(type) {
	case []any:
		for _, raw := range required {
			if value, _ := raw.(string); value == property {
				return true
			}
		}
	case []string:
		for _, value := range required {
			if value == property {
				return true
			}
		}
	}
	return false
}

func openAPIObjectHasProperty(schema map[string]any, property string) bool {
	properties, _ := schema["properties"].(map[string]any)
	_, ok := properties[property]
	return ok
}

func openAPIObjectProperty(schema map[string]any, property string) (map[string]any, bool) {
	properties, _ := schema["properties"].(map[string]any)
	value, _ := properties[property].(map[string]any)
	return value, value != nil
}

func openAPIStringEnumContains(schema map[string]any, want string) bool {
	switch values := schema["enum"].(type) {
	case []any:
		for _, raw := range values {
			if value, _ := raw.(string); value == want {
				return true
			}
		}
	case []string:
		for _, value := range values {
			if value == want {
				return true
			}
		}
	}
	return false
}

func openAPIEventUnionHasSchema(doc map[string]any, schemaName string) bool {
	event, ok := openAPIComponentSchema(doc, "#/components/schemas/Event")
	if !ok {
		return false
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		values, _ := event[key].([]any)
		for _, raw := range values {
			option, _ := raw.(map[string]any)
			ref, _ := option["$ref"].(string)
			if strings.HasSuffix(ref, "/"+schemaName) {
				return true
			}
		}
	}
	return false
}

func openAPIComponentSchema(doc map[string]any, ref string) (map[string]any, bool) {
	const prefix = "#/components/schemas/"
	if !strings.HasPrefix(ref, prefix) {
		return nil, false
	}
	components, _ := doc["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	schema, _ := schemas[strings.TrimPrefix(ref, prefix)].(map[string]any)
	return schema, schema != nil
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

func materializeHermesPermissionConfig(dirs xdgDirs, permission string) (string, error) {
	if err := validateHermesPermission(permission); err != nil {
		return "", err
	}
	config := map[string]any{
		"$schema": "https://hermes.ai/config.json",
		"permission": map[string]string{
			"*": normalizeHermesPermission(permission),
		},
	}
	data, err := hermesMarshalIndent(config, "", "  ")
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	configDir := filepath.Join(dirs.Config, "hermes")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(configDir, "hermes.json"), data, 0o600); err != nil {
		return "", err
	}

	return string(data), nil
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

func validateHermesPermission(permission string) error {
	switch permission {
	case "", hermesPermissionAsk, hermesPermissionAllow:
		return nil
	default:
		return unsupportedField("permission")
	}
}

func normalizeHermesPermission(permission string) string {
	if permission == "" {
		return hermesPermissionAsk
	}
	return permission
}

func allocatePort() (int, error) {
	ln, err := hermesListen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("allocated address is not tcp")
	}
	return addr.Port, nil
}

func randomPassword() (string, error) {
	var b [32]byte
	if _, err := io.ReadFull(hermesRandReader, b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func passwordHash(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}

type serverLease struct {
	PID              int    `json:"pid"`
	Port             int    `json:"port"`
	StartedAt        int64  `json:"startedAtUnixMilli"`
	PasswordHash     string `json:"passwordHash"`
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
	stateDir := filepath.Dir(path)
	if identity.Env["XDG_STATE_HOME"] != stateDir {
		return false
	}
	if passwordHash(identity.Env["HERMES_SERVER_PASSWORD"]) != lease.PasswordHash {
		return false
	}
	if lease.XDGRoot != "" && filepath.Clean(lease.XDGRoot) != filepath.Clean(filepath.Dir(stateDir)) {
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

func mergeProcessEnv(overlays ...map[string]string) map[string]string {
	env := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			env[key] = value
		}
	}
	for _, overlay := range overlays {
		for key, value := range overlay {
			if key != "" {
				env[key] = value
			}
		}
	}
	return env
}

func envMapToSlice(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}
	return out
}

func drainProcessPipe(log *slog.Logger, name string, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 4096), 64*1024)
	for scanner.Scan() {
		if log != nil {
			log.Debug("hermes process output", slog.String("pipe", name), slog.String("line", scanner.Text()))
		}
	}
}

func compareSemver(got string, want string) int {
	g := parseSemver(got)
	w := parseSemver(want)
	for i := range g {
		if g[i] < w[i] {
			return -1
		}
		if g[i] > w[i] {
			return 1
		}
	}
	return 0
}

func parseSemver(value string) [3]int {
	value = strings.TrimPrefix(strings.TrimSpace(value), "v")
	parts := strings.Split(value, ".")
	var out [3]int
	for i := 0; i < len(parts) && i < len(out); i++ {
		part := parts[i]
		for j, r := range part {
			if r < '0' || r > '9' {
				part = part[:j]
				break
			}
		}
		n, _ := strconv.Atoi(part)
		out[i] = n
	}
	return out
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
