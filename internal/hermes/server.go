//nolint:tagliatelle,gocyclo,gocritic // Native wire fields and ordered startup/config transactions are intentional.
package hermes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"gopkg.in/yaml.v3"
)

const (
	// closeTimeout bounds gateway redial and shutdown handshakes.
	closeTimeout = 5 * time.Second
)

// Native gateway field, value, and event-type constants. These mirror the ACP
// adapter's shared constants; the native gateway layer keeps its own copies so
// it stays self-contained under internal/hermes.
const (
	valURL              = "url"
	valACPGoHermes      = "acp-go-hermes"
	valAssistant        = "assistant"
	valStop             = "stop"
	valText             = "text"
	valTool             = "tool"
	valCompleted        = "completed"
	valFailed           = "failed"
	valReasoning        = "reasoning"
	valFile             = "file"
	valAlways           = "always"
	valServe            = "serve"
	valHermes           = "hermes"
	valTerminal         = "terminal"
	argPort             = "--port"
	valOnce             = "once"
	valUnsupported      = "unsupported"
	valCancelled        = "cancelled"
	valInterrupted      = "interrupted"
	keyTitle            = "title"
	keySessionIDSnake   = "session_id"
	keyValue            = "value"
	keyField            = "field"
	keySource           = "source"
	keyQuestion         = "question"
	keyEagerBuild       = "eager_build"
	jsonFieldError      = "error"
	jsonFieldCwd        = "cwd"
	jsonFieldStatus     = "status"
	msgHermesNeedsInput = "Hermes needs input"

	evtApprovalRequest    = "approval.request"
	evtClarifyRequest     = "clarify.request"
	evtSecretRequest      = "secret.request"
	evtMessageDelta       = "message.delta"
	evtMessageStart       = "message.start"
	evtMessageComplete    = "message.complete"
	evtMessagePartUpdated = "message.part.updated"
	evtError              = "error"
	evtSudoRequest        = "sudo.request"
	evtThinkingDelta      = "thinking.delta"
	evtTerminalReadReq    = "terminal.read.request"
	evtToolComplete       = "tool.complete"
	evtToolStart          = "tool.start"

	promptPhaseRegister    = "register"
	promptPhaseSubmit      = "submit"
	promptPhaseSynchronize = "synchronize"
	promptPhaseWatermark   = "watermark"
	promptPhaseDefer       = "defer"
	promptPhaseRelease     = "release"
	promptPhaseResult      = "result"
)

// firstNonEmpty returns the first non-empty string in values.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

// splitModelValue splits a "provider/model" identifier, applying fallbacks when
// the value is empty or unqualified.
func splitModelValue(value string, fallbackProvider string, fallbackModel string) (string, string) {
	if value == "" {
		return fallbackProvider, fallbackModel
	}

	provider, model, ok := strings.Cut(value, "/")
	if !ok || provider == "" || model == "" {
		return fallbackProvider, value
	}

	return provider, model
}

type MissingLiveSessionMappingError struct {
	StoredSessionID string
}

func (e MissingLiveSessionMappingError) Error() string {
	return fmt.Sprintf("hermes live session id mapping missing for stored session %q", e.StoredSessionID)
}

type Server interface {
	Close(context.Context) error
	CreateSession(context.Context, string) (Session, error)
	GetSession(context.Context, string) (Session, error)
	ListSessions(context.Context, string) ([]Session, error)
	DeleteSession(context.Context, string) error
	ReloadMCP(context.Context, string) error
	SendMessage(context.Context, string, MessageRequest) (NativeMessage, error)
	Messages(context.Context, string) ([]NativeMessage, error)
	Abort(context.Context, string) error
	Fork(context.Context, string, string) (Session, error)
	ConfigProviders(context.Context) (ProvidersResponse, error)
	ReplyPermission(context.Context, PermissionRequest, string, string) error
	ReplyQuestion(context.Context, QuestionRequest, [][]string) error
	RejectQuestion(context.Context, QuestionRequest) error
	Deliveries() <-chan TurnDelivery
	XDGDirs() XDGDirs
	AuthProviders(context.Context) ([]AuthProvider, error)
	AuthStart(context.Context, string) (AuthStart, error)
	AuthSubmit(context.Context, string, string, string) error
	AuthPollFlow(context.Context, string, string) (AuthPoll, error)
	AuthCancelFlow(context.Context, string) error
	AuthDisconnect(context.Context, string) error
}

// SessionDraft identifies a newly-created live draft before session.title
// persists it. Callers use this boundary to fsync recovery intent first.
type SessionDraft struct {
	LiveSessionID   string
	StoredSessionID string
}

// DraftSessionCreator exposes Hermes's draft-to-durable boundary without
// expanding the ordinary Server interface call shape.
type DraftSessionCreator interface {
	CreateSessionWithDraft(context.Context, string, func(SessionDraft) error) (Session, error)
}

type PersistedSessionLister interface {
	PersistedSessions(context.Context) ([]Session, error)
}

// RecoverableSessionForker lets the adapter establish an exact durable
// baseline before invoking official Hermes's non-atomic session.branch.
type RecoverableSessionForker interface {
	ForkWithBaseline(context.Context, string, string, []string) (Session, error)
}

var ErrBranchRecoveryAmbiguous = errors.New("hermes branch recovery is ambiguous")

type StartOptions struct {
	ACPSessionID ACPSessionIDString
	ControlDir   string
	// ScratchParent is the resolved parent directory for ephemeral on-disk
	// materialization, supplied by the caller. The internal package never
	// consults the system temp directory itself.
	ScratchParent  string
	Cwd            string
	ExecutablePath string
	DefaultModel   string
	// SharedHermesHome is the exact durable HERMES_HOME selected by the
	// official shared-home mode. ExistingXDG remains the unique wrapper-owned
	// control generation; multiple Servers of this one adapter process share the
	// native home under a single exclusive home-root claim, and a second adapter
	// process is refused that root outright.
	SharedHermesHome    string
	Env                 map[string]string
	SessionEnv          map[string]string
	ExtraPathDirs       []string
	NativeEnvironment   map[string]string
	StartNative         NativeStarter
	PrepareNativeTree   func(context.Context, string) error
	ReclaimNativeTree   func(context.Context, string) error
	AmbientEnvironment  map[string]string
	HealthTimeout       time.Duration
	Logger              *slog.Logger
	ExistingXDG         XDGDirs
	MCPServers          []acp.McpServer
	SeedFiles           map[string]string
	ObserveStartupStage func(context.Context, string, string, time.Duration, error)
}

type ACPSessionIDString string

type XDGDirs struct {
	Root   string
	Data   string
	Config string
	Cache  string
	State  string
}

type hermesServer struct {
	xdg         XDGDirs
	log         *slog.Logger
	controlLock *SharedSessionSetLock

	deliveries       chan TurnDelivery
	closed           chan struct{}
	deliveryMu       sync.Mutex
	deliveryTerminal error
	admissionOnce    sync.Once
	shutdownOnce     sync.Once
	shutdownErr      error
	closeMu          sync.Mutex
	closeAttempt     *hermesServerCloseAttempt
	closeSucceeded   bool

	process               *Process
	closeNative           func(context.Context) error
	gatewayMu             sync.Mutex
	actorsByStored        map[string]*gatewaySessionActor
	cwd                   string
	defaultModel          string
	providerAuthSupported bool
	sharedSessionOwner    *SharedSessionOwner
	sharedHomeOwner       *SharedHomeOwner

	connMu              sync.Mutex
	turnLease           sync.RWMutex
	turnBusy            int
	turnIdle            *sync.Cond
	redial              func(context.Context) (*Client, error)
	transportGeneration uint64
	dispatchers         map[uint64]*gatewayTransportDispatcher
	transport           *gatewayTransport
	registrations       uint64

	reconnectWG            sync.WaitGroup
	dispatcherWG           sync.WaitGroup
	actorWG                sync.WaitGroup
	transportCloseWG       sync.WaitGroup
	afterTurnIdle          func()
	afterGatewayClientDone func()
	beforeTransportPublish func()
	beforeHandshakeBind    func(gatewayHandshakeKind, uint64)
	beforePromptPhase      func(string, *gatewaySessionActor)
}

type hermesServerCloseAttempt struct {
	done chan struct{}
	err  error
}

type serverCloseJoinHookKey struct{}

// ProviderAuthSupported reports that this server's exact credential residence
// is durable. Official shared-HERMES_HOME mode is the only supported residence.
func (s *hermesServer) ProviderAuthSupported() bool {
	return s != nil && s.providerAuthSupported
}

type Session struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Directory string `json:"directory"`
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

type NativeMessage struct {
	Info  NativeMessageInfo `json:"info"`
	Parts []Part            `json:"parts"`
}

type NativeMessageInfo struct {
	ID            string       `json:"id"`
	SessionID     string       `json:"sessionID"`
	Role          string       `json:"role"`
	ParentID      string       `json:"parentID"`
	ModelID       string       `json:"modelID"`
	ProviderID    string       `json:"providerID"`
	Mode          string       `json:"mode"`
	Agent         string       `json:"agent"`
	Finish        string       `json:"finish"`
	Cost          float64      `json:"cost"`
	Tokens        Tokens       `json:"tokens"`
	ContextWindow int          `json:"contextWindow"`
	Error         *nativeError `json:"error,omitempty"`
	Time          struct {
		Created   int64 `json:"created"`
		Completed int64 `json:"completed"`
	} `json:"time"`
}

type nativeError struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Message      string `json:"message"`
	StatusCode   int    `json:"statusCode,omitempty"`
	ProviderCode string `json:"providerCode,omitempty"`
}

type Part struct {
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
	Tokens    Tokens          `json:"tokens"`
	Raw       json.RawMessage `json:"-"`

	// StreamedText records the prefix already forwarded as message.delta
	// events during the current turn. It is adapter-local state: the final
	// NativeMessage still carries Hermes' authoritative complete text, while
	// the ACP mapper can emit only a completion suffix (or nothing) instead of
	// duplicating text the client already received.
	StreamedText string `json:"-"`
}

func (p *Part) UnmarshalJSON(data []byte) error {
	type alias Part

	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*p = Part(value)
	p.Raw = append(p.Raw[:0], data...)

	return nil
}

type Tokens struct {
	Total     float64 `json:"total"`
	Input     float64 `json:"input"`
	Output    float64 `json:"output"`
	Reasoning float64 `json:"reasoning"`
	Cache     struct {
		Read  float64 `json:"read"`
		Write float64 `json:"write"`
	} `json:"cache"`
}

type TurnEvent struct {
	ID                  string             `json:"id"`
	Type                string             `json:"type"`
	Properties          json.RawMessage    `json:"properties"`
	Raw                 json.RawMessage    `json:"-"`
	TransportGeneration uint64             `json:"-"`
	CycleID             string             `json:"-"`
	Origin              CycleOrigin        `json:"-"`
	Message             *NativeMessage     `json:"-"`
	Permission          *PermissionRequest `json:"-"`
	Question            *QuestionRequest   `json:"-"`
	Err                 error              `json:"-"`
	ProjectionDone      func(error)        `json:"-"`
}

type CycleOrigin string

const (
	CycleOriginPrompt   CycleOrigin = "prompt"
	CycleOriginActivity CycleOrigin = "activity"

	EventCycleStarted  = "cycle.started"
	EventCycleComplete = "cycle.complete"
	EventCycleFailed   = "cycle.failed"
	EventGatewayRaw    = "gateway.raw"
)

func (e *TurnEvent) UnmarshalJSON(data []byte) error {
	type alias TurnEvent

	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*e = TurnEvent(value)
	e.Raw = append(e.Raw[:0], data...)

	return nil
}

type PermissionRequest struct {
	ID                  string         `json:"id"`
	SessionID           string         `json:"sessionID"`
	Action              string         `json:"action"`
	Metadata            map[string]any `json:"metadata"`
	Tool                permissionTool `json:"tool"`
	CycleID             string         `json:"-"`
	TransportGeneration uint64         `json:"-"`
	route               *gatewayControlRoute
}

type permissionTool struct {
	MessageID string `json:"messageID"`
	CallID    string `json:"callID"`
}

type QuestionRequest struct {
	ID                  string         `json:"id"`
	SessionID           string         `json:"sessionID"`
	Questions           []QuestionInfo `json:"questions"`
	Tool                QuestionTool   `json:"tool"`
	CycleID             string         `json:"-"`
	TransportGeneration uint64         `json:"-"`
	route               *gatewayControlRoute
}

type QuestionInfo struct {
	Question string           `json:"question"`
	Header   string           `json:"header"`
	Options  []QuestionOption `json:"options"`
	Multiple bool             `json:"multiple"`
	Custom   bool             `json:"custom"`
}

type QuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

type QuestionTool struct {
	MessageID string `json:"messageID"`
	CallID    string `json:"callID"`
}

type MessageRequest struct {
	Model *ModelSelector   `json:"model,omitempty"`
	Parts []map[string]any `json:"parts"`
}

type ModelSelector struct {
	ProviderID string `json:"providerID"`
	ModelID    string `json:"modelID"`
}

type ProvidersResponse struct {
	Providers []ProviderInfo  `json:"providers"`
	Raw       json.RawMessage `json:"-"`
}

func (p *ProvidersResponse) UnmarshalJSON(data []byte) error {
	type alias ProvidersResponse

	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*p = ProvidersResponse(value)
	p.Raw = append(p.Raw[:0], data...)

	return nil
}

type ProviderInfo struct {
	ID     string                   `json:"id"`
	Name   string                   `json:"name"`
	Models map[string]ProviderModel `json:"models"`
}

type ProviderModel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var (
	hermesMarshalIndent = json.MarshalIndent
	hermesUnmarshalYAML = yaml.Unmarshal
	hermesControlMkdir  = os.MkdirAll
	hermesControlChmod  = os.Chmod
)

func StartServer(ctx context.Context, options StartOptions) (_ Server, resultErr error) {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	if strings.ContainsRune(string(options.ACPSessionID), '\x00') {
		return nil, errors.New("ACP session id contains NUL")
	}

	extraPathDirs, carrierErr := cloneAndValidateExtraPathDirs(options.ExtraPathDirs)
	if carrierErr != nil {
		return nil, carrierErr
	}

	if sessionEnvErr := validateSessionEnvironmentNoPath(options.SessionEnv); sessionEnvErr != nil {
		return nil, sessionEnvErr
	}

	if envErr := validatePathCarrierEnvironment(options.Env); envErr != nil {
		return nil, envErr
	}

	xdg := options.ExistingXDG
	if xdg.Root == "" {
		if options.ScratchParent == "" {
			return nil, errors.New("hermes runtime scratch parent is required")
		}

		var err error

		xdg, err = CreateGenerationXDGDirs(options.ScratchParent)
		if err != nil {
			return nil, err
		}
	}

	if err := ensureXDGDirs(xdg); err != nil {
		return nil, err
	}

	nativeXDG := xdg

	if options.SharedHermesHome != "" {
		var sharedErr error

		nativeXDG, sharedErr = SharedHomeXDGDirs(options.SharedHermesHome)
		if sharedErr != nil {
			return nil, sharedErr
		}

		if sharedErr = sharedHomeLocalValidator(nativeXDG.Root); sharedErr != nil {
			return nil, sharedErr
		}
	}

	var (
		homeOwner    *SharedHomeOwner
		sessionOwner *SharedSessionOwner
	)

	// keepOwners hands both claims to the started server; until it is set, every
	// exit from here gives the home root and the session claim back.
	keepOwners := false

	if options.SharedHermesHome != "" {
		var ownerErr error

		homeOwner, ownerErr = AcquireSharedHomeOwner(nativeXDG.Root)
		if ownerErr != nil {
			return nil, ownerErr
		}

		defer func() {
			if !keepOwners {
				resultErr = errors.Join(resultErr, homeOwner.Release())
			}
		}()

		sessionOwner, ownerErr = acquireSharedACPSessionOwner(nativeXDG.Root, options.ACPSessionID)
		if ownerErr != nil {
			return nil, ownerErr
		}

		defer func() {
			if !keepOwners {
				resultErr = errors.Join(resultErr, sessionOwner.Release())
			}
		}()
	}

	controlDir := options.ControlDir
	if controlDir == "" {
		controlDir = ControlDirForXDG(xdg.Root)
	}

	if err := hermesControlMkdir(controlDir, 0o700); err != nil {
		return nil, fmt.Errorf("create Hermes control directory: %w", err)
	}

	if err := hermesControlChmod(controlDir, 0o700); err != nil {
		return nil, fmt.Errorf("protect Hermes control directory: %w", err)
	}

	controlLock, err := acquireServerControlLock(ctx, controlDir)
	if err != nil {
		return nil, err
	}

	keepControlLock := false
	defer func() {
		if !keepControlLock {
			resultErr = errors.Join(resultErr, controlLock.Release())
		}
	}()

	configurationStarted := time.Now()

	configurationEnv := cloneEnvironmentMap(options.Env)
	for key, value := range options.SessionEnv {
		configurationEnv[key] = value
	}

	servers, mcpSecretEnv, err := mcpServersWithSecretEnv(options.MCPServers, configurationEnv)
	if err != nil {
		observeHermesStartupStage(ctx, options.ObserveStartupStage, "session", "configuration", configurationStarted, err)

		return nil, err
	}

	var configErr error
	if options.SharedHermesHome == "" {
		configErr = materializeHermesConfig(nativeXDG.Root, servers, options.SeedFiles)
	}

	if configErr != nil {
		observeHermesStartupStage(ctx, options.ObserveStartupStage, "session", "configuration", configurationStarted, configErr)

		return nil, configErr
	}

	observeHermesStartupStage(ctx, options.ObserveStartupStage, "session", "configuration", configurationStarted, nil)

	processEnv := make(map[string]string, len(options.SessionEnv)+len(mcpSecretEnv)+1)
	for key, value := range options.SessionEnv {
		processEnv[key] = value
	}

	for key, value := range mcpSecretEnv {
		processEnv[key] = value
	}

	// Hermes approval.request events are session-keyed and do not carry the
	// native tool-call id. The immediately preceding tool.start event is the
	// only exact correlation source, so the private gateway must always emit
	// tool progress even when the user's seeded display config disables it.
	processEnv["HERMES_TUI_TOOL_PROGRESS"] = "all"

	proc, err := Start(ctx, ProcessOptions{
		ExecutablePath: options.ExecutablePath,
		Home:           nativeXDG.Root,
		SharedHome:     options.SharedHermesHome != "",
		PrepareSharedHome: func(prepareCtx context.Context, home string) error {
			return materializeSharedHermesConfig(prepareCtx, home, servers, options.SeedFiles)
		},
		ScratchParent:       options.ScratchParent,
		Cwd:                 options.Cwd,
		Env:                 cloneEnvironmentMap(options.Env),
		SessionEnv:          processEnv,
		ExtraPathDirs:       extraPathDirs,
		NativeEnvironment:   options.NativeEnvironment,
		StartNative:         options.StartNative,
		PrepareNativeTree:   options.PrepareNativeTree,
		ReclaimNativeTree:   options.ReclaimNativeTree,
		AmbientEnvironment:  options.AmbientEnvironment,
		Timeout:             options.HealthTimeout,
		ObserveStartupStage: options.ObserveStartupStage,
	})
	if err != nil {
		return nil, err
	}

	server := &hermesServer{
		xdg:                   xdg,
		log:                   options.Logger,
		controlLock:           controlLock,
		deliveries:            make(chan TurnDelivery, gatewayMappedDeliveryCapacity+1),
		closed:                make(chan struct{}),
		process:               proc,
		closeNative:           proc.Close,
		actorsByStored:        make(map[string]*gatewaySessionActor),
		dispatchers:           make(map[uint64]*gatewayTransportDispatcher),
		cwd:                   options.Cwd,
		defaultModel:          options.DefaultModel,
		providerAuthSupported: options.SharedHermesHome != "",
		sharedSessionOwner:    sessionOwner,
		sharedHomeOwner:       homeOwner,
	}
	server.installGatewayDispatcher(proc.Client)
	server.enableReconnect(proc.Redial)

	keepOwners = true
	keepControlLock = true

	return server, nil
}

func observeHermesStartupStage(ctx context.Context, observe func(context.Context, string, string, time.Duration, error), lifecycle, stage string, started time.Time, err error) {
	if observe != nil {
		observe(ctx, lifecycle, stage, time.Since(started), err)
	}
}

func (s *hermesServer) Close(ctx context.Context) error {
	s.admissionOnce.Do(func() {
		close(s.closed)
		s.connMu.Lock()
		if s.turnIdle != nil {
			s.turnIdle.Broadcast()
		}
		s.connMu.Unlock()
	})

	s.closeMu.Lock()
	if s.closeSucceeded {
		s.closeMu.Unlock()

		return nil
	}

	if attempt := s.closeAttempt; attempt != nil {
		s.closeMu.Unlock()

		if hook, ok := ctx.Value(serverCloseJoinHookKey{}).(func()); ok {
			hook()
		}

		select {
		case <-attempt.done:
			return attempt.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	attempt := &hermesServerCloseAttempt{done: make(chan struct{})}
	s.closeAttempt = attempt
	s.closeMu.Unlock()

	attempt.err = s.closeAttemptOnce(ctx)

	s.closeMu.Lock()
	if attempt.err == nil {
		s.closeSucceeded = true
	}

	s.closeAttempt = nil

	close(attempt.done)
	s.closeMu.Unlock()

	return attempt.err
}

func (s *hermesServer) closeAttemptOnce(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		if gw := s.gatewayClient(); gw != nil {
			gwErr := gw.Close(1000, "closing")
			if s.process == nil {
				s.shutdownErr = gwErr
			}
		}

		s.dispatcherWG.Wait()
		s.stopGatewayActors()
		s.actorWG.Wait()
		s.reconnectWG.Wait()
		s.turnLease.Lock()
		s.transportCloseWG.Wait()
		close(s.deliveries)
		s.turnLease.Unlock()
	})

	err := s.shutdownErr
	if s.closeNative != nil {
		err = errors.Join(err, s.closeNative(ctx))
	} else if s.process != nil {
		err = errors.Join(err, s.process.Close(ctx))
	}

	if err != nil {
		return err
	}

	err = errors.Join(err, s.sharedSessionOwner.Release(), s.sharedHomeOwner.Release(), s.controlLock.Release())

	return err
}

type TurnDelivery struct {
	Event *TurnEvent
	Err   error
}

func (s *hermesServer) Deliveries() <-chan TurnDelivery {
	return s.deliveries
}

func (s *hermesServer) publishTurnEvent(event TurnEvent) error {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()

	if len(s.deliveries) >= cap(s.deliveries)-1 {
		return ErrGatewayMappedOverflow
	}

	copyEvent := event
	s.deliveries <- TurnDelivery{Event: &copyEvent}

	return nil
}

func (s *hermesServer) publishTurnError(err error) {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()

	if s.deliveryTerminal != nil {
		return
	}

	if err == nil {
		err = errGatewayStreamClosed
	}

	s.deliveryTerminal = err

	select {
	case s.deliveries <- TurnDelivery{Err: err}:
	default:
	}
}

func (s *hermesServer) XDGDirs() XDGDirs {
	return s.xdg
}

// ErrGatewayDisconnected marks a mid-turn WebSocket disconnect so the prompt
// loop can fence the turn with a single terminal hermes_turn_failed error.
var ErrGatewayDisconnected = errors.New("hermes gateway disconnected")

// errGatewayStreamClosed is the disconnect cause when the gateway error channel
// closes without a specific native error.
var errGatewayStreamClosed = errors.New("hermes gateway event stream closed")

func IsGatewayDisconnect(err error) bool {
	return errors.Is(err, ErrGatewayDisconnected)
}

// TurnFailureCause is the machine-readable class of a native turn failure,
// surfaced verbatim as data.cause in the uniform hermes_turn_failed error.
type TurnFailureCause string

const (
	CauseTransport TurnFailureCause = "transport"
	CauseProvider  TurnFailureCause = "provider"
	CauseTimeout   TurnFailureCause = "timeout"
)

// TurnFailureError is a classified native turn failure. The prompt loop maps it
// to the uniform hermes_turn_failed JSON-RPC error; a failed turn is never a
// stop reason. message carries the real native cause (never a fixed
// placeholder); statusCode/providerCode appear only when the gateway supplies
// them.
type TurnFailureError struct {
	cause        TurnFailureCause
	message      string
	statusCode   int
	providerCode string
	wrapped      error
}

func (e *TurnFailureError) Error() string {
	if e.message != "" {
		return e.message
	}

	return string(e.cause) + " turn failure"
}

func (e *TurnFailureError) Unwrap() error {
	return e.wrapped
}

// NewTurnFailure builds a classified turn failure for the ACP adapter to map to
// the uniform hermes_turn_failed wire error.
func NewTurnFailure(cause TurnFailureCause, message string) *TurnFailureError {
	return &TurnFailureError{cause: cause, message: message}
}

// NewProviderTurnFailure builds a provider-classified turn failure carrying the
// native statusCode/providerCode the gateway reported.
func NewProviderTurnFailure(message string, statusCode int, providerCode string) *TurnFailureError {
	return &TurnFailureError{
		cause:        CauseProvider,
		message:      message,
		statusCode:   statusCode,
		providerCode: providerCode,
	}
}

// Cause reports the classified failure cause.
func (e *TurnFailureError) Cause() TurnFailureCause { return e.cause }

// Message reports the raw native failure message, if any.
func (e *TurnFailureError) Message() string { return e.message }

// StatusCode reports the native HTTP status code, if the gateway supplied one.
func (e *TurnFailureError) StatusCode() int { return e.statusCode }

// ProviderCode reports the native provider error code, if the gateway supplied one.
func (e *TurnFailureError) ProviderCode() string { return e.providerCode }

// gatewayCompleteFailure inspects a message.complete payload and returns a
// provider turn failure when the native turn finished in error. Hermes' TUI
// gateway reports the authoritative terminal state in status; finish/error are
// also accepted when the gateway supplies its richer provider error shape. It
// returns nil for a clean completion (or a payload that fails to decode).
func gatewayCompleteFailure(payload json.RawMessage) *TurnFailureError {
	var info struct {
		Finish string       `json:"finish"`
		Status string       `json:"status"`
		Text   string       `json:"text"`
		Error  *nativeError `json:"error"`
	}

	_ = json.Unmarshal(payload, &info)

	failErr := assistantMessageError(NativeMessage{Info: NativeMessageInfo{Finish: info.Finish, Error: info.Error}})
	if failErr == nil && !strings.EqualFold(info.Status, jsonFieldError) {
		return nil
	}

	failure := &TurnFailureError{cause: CauseProvider}
	if info.Error != nil {
		failure.message = firstNonEmpty(info.Error.Message, info.Error.Name, info.Error.Type)
		failure.statusCode = info.Error.StatusCode
		failure.providerCode = info.Error.ProviderCode
	}

	if failure.message == "" && strings.EqualFold(info.Status, jsonFieldError) {
		failure.message = info.Text
	}

	if failure.message == "" && failErr != nil {
		failure.message = failErr.Error()
	}

	failure.message = firstNonEmpty(failure.message, "hermes provider error")

	return failure
}

// gatewayEventFailure maps a bare error gateway event to a provider turn
// failure, accepting either a nested {error:{…}} object or flat error fields.
// Hermes ends a turn with this frame in place of message.complete whenever the
// turn dies before or outside its own terminal path, so the frame is the only
// thing that can settle the cycle it ends.
func gatewayEventFailure(payload json.RawMessage) *TurnFailureError {
	var body struct {
		Error        *nativeError `json:"error"`
		Message      string       `json:"message"`
		Name         string       `json:"name"`
		StatusCode   int          `json:"statusCode"`
		ProviderCode string       `json:"providerCode"`
	}

	_ = json.Unmarshal(payload, &body)

	failure := &TurnFailureError{cause: CauseProvider}
	if body.Error != nil {
		failure.message = firstNonEmpty(body.Error.Message, body.Error.Name, body.Error.Type)
		failure.statusCode = body.Error.StatusCode
		failure.providerCode = body.Error.ProviderCode
	}

	failure.message = firstNonEmpty(failure.message, body.Message, body.Name, "hermes provider error")
	if failure.statusCode == 0 {
		failure.statusCode = body.StatusCode
	}

	failure.providerCode = firstNonEmpty(failure.providerCode, body.ProviderCode)

	return failure
}

// gatewayDisconnectCause recovers the terminal cause recorded by the sole
// ordered reader. A clean close has no cause and maps to the closed sentinel.
func gatewayDisconnectCause(gw *Client) error {
	if gw != nil {
		if err := gw.terminalCause(); err != nil {
			return err
		}
	}

	return errGatewayStreamClosed
}

// gatewayClient returns the current live gateway client. A reconnect can swap
// it, so all callers read it through this accessor under connMu.
func (s *hermesServer) gatewayClient() *Client {
	transport := s.gatewayTransport()
	if transport != nil {
		return transport.client
	}

	return nil
}

func (s *hermesServer) gatewayTransport() *gatewayTransport {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	return s.transport
}

// enableReconnect wires the idle-reconnect loop. turnBusy is a transport
// lease around gateway RPCs (including non-prompt RPCs), not lifecycle turn
// admission; session admission remains owned by the adapter session actor.
func (s *hermesServer) enableReconnect(redial func(context.Context) (*Client, error)) {
	s.connMu.Lock()

	s.redial = redial
	if s.turnIdle == nil {
		s.turnIdle = sync.NewCond(&s.connMu)
	}

	s.reconnectWG.Add(1)
	s.connMu.Unlock()

	go func() {
		defer s.reconnectWG.Done()

		s.runGatewayReconnectLoop()
	}()
}

func (s *hermesServer) beginGatewayTurn() *gatewayTransport {
	s.turnLease.RLock()
	s.connMu.Lock()
	s.turnBusy++

	transport := s.transport
	if s.serverClosed() {
		transport = nil
	}
	s.connMu.Unlock()

	return transport
}

func (s *hermesServer) endGatewayTurn() {
	s.connMu.Lock()
	release := false

	if s.turnBusy > 0 {
		s.turnBusy--
		release = true
	}

	cond := s.turnIdle
	s.connMu.Unlock()

	if cond != nil {
		cond.Broadcast()
	}

	if release {
		s.turnLease.RUnlock()
	}
}

// runGatewayReconnectLoop watches the live connection and, on an idle disconnect,
// redials the still-running `hermes serve` process so the next turn reconnects
// instead of failing. A mid-turn disconnect is fenced by the prompt loop; the
// reconnect loop waits for the turn to finish before reconnecting.
func (s *hermesServer) runGatewayReconnectLoop() {
	for {
		transport := s.gatewayTransport()
		if transport == nil || transport.client == nil || transport.dispatcher == nil {
			return
		}

		gw := transport.client
		select {
		case <-s.closed:
			return
		case <-gw.Done():
		}

		if s.afterGatewayClientDone != nil {
			s.afterGatewayClientDone()
		}

		// Client.Done closes before the dispatcher is necessarily finished. The
		// dispatcher terminalizes every actor owned by this generation before its
		// own done barrier closes; publishing a reconnect earlier could rebind a
		// stored session through the old actor in that gap.
		select {
		case <-s.closed:
			return
		case <-transport.dispatcher.done:
		}

		s.connMu.Lock()
		for s.turnBusy > 0 && !s.serverClosed() {
			s.turnIdle.Wait()
		}

		closed := s.serverClosed()
		s.connMu.Unlock()

		if closed {
			return
		}

		if s.afterTurnIdle != nil {
			s.afterTurnIdle()
		}

		if s.serverClosed() {
			return
		}

		s.reconnectGateway()
	}
}

func (s *hermesServer) serverClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *hermesServer) reconnectGateway() {
	dialCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	client, err := s.redial(dialCtx)

	cancel()

	if err != nil {
		if s.log != nil {
			s.log.Debug("reconnect hermes gateway failed", slog.String("classification", "dial_failed"))
		}

		time.Sleep(20 * time.Millisecond)

		return
	}

	if s.serverClosed() {
		// The server shut down while redialing; discard the new connection.
		_ = client.Close(1000, "closing")

		return
	}

	// Installing publishes the new client, dispatcher, generation, and empty
	// runtime-only mappings as one coherent transport tuple.
	s.installGatewayDispatcher(client)
}

func (s *hermesServer) CreateSession(ctx context.Context, title string) (Session, error) {
	return s.CreateSessionWithDraft(ctx, title, nil)
}

func (s *hermesServer) CreateSessionWithDraft(
	ctx context.Context,
	title string,
	bindDraft func(SessionDraft) error,
) (Session, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return Session{}, ErrGatewayDisconnected
	}

	params := map[string]any{jsonFieldCwd: s.cwd, keySource: valACPGoHermes}
	if title != "" {
		params[keyTitle] = title
	}

	if provider, model := splitModelValue(s.defaultModel, "", ""); model != "" {
		params["model"] = model
		if provider != "" {
			params["provider"] = provider
		}
	}

	handshake, err := s.beginGatewayHandshake(ctx, transport, gatewayHandshakeCreate)
	if err != nil {
		return Session{}, err
	}

	bound := false
	defer func() {
		if !bound {
			_ = s.cancelGatewayHandshake(transport, handshake)
		}
	}()

	result, sequence, err := transport.client.CreateSessionWatermark(ctx, params)
	if err != nil {
		return Session{}, err
	}

	if result.SessionID == "" {
		return Session{}, fmt.Errorf("hermes session.create response missing session_id")
	}

	if result.StoredSessionID == "" {
		return Session{}, fmt.Errorf("hermes session.create response missing stored_session_id")
	}

	if bindDraft != nil {
		if bindErr := bindDraft(SessionDraft{
			LiveSessionID:   result.SessionID,
			StoredSessionID: result.StoredSessionID,
		}); bindErr != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			closeErr := transport.client.CloseSession(cleanupCtx, result.SessionID)

			cleanupCancel()

			if IsNotFound(closeErr) {
				closeErr = nil
			}

			return Session{}, errors.Join(fmt.Errorf("bind Hermes session draft: %w", bindErr), closeErr)
		}
	}

	if s.beforeHandshakeBind != nil {
		s.beforeHandshakeBind(gatewayHandshakeCreate, handshake)
	}

	bindResult := s.bindGatewayHandshake(ctx, transport, handshake, gatewayHandshakeCreate,
		result.StoredSessionID, result.SessionID, GatewayWatermark{
			TransportGeneration: transport.generation,
			Sequence:            sequence,
		})
	if bindResult.err != nil {
		return Session{}, bindResult.err
	}

	bound = true

	// Hermes session.create intentionally leaves a draft only in the live
	// gateway. session.title is the native persistence boundary for an otherwise
	// empty session: it creates the state.db row synchronously. The ACP session
	// store snapshots immediately after this method returns, so returning before
	// that row exists would publish an idmap whose native session cannot be
	// resumed after an interrupt or process restart.
	durableTitle := firstNonEmpty(title, "Hermes session")

	titleResult, err := transport.client.SetSessionTitle(ctx, result.SessionID, durableTitle)
	if err != nil {
		s.dropGatewayBindingOn(transport, result.StoredSessionID)

		return Session{}, fmt.Errorf("persist Hermes session: %w", err)
	}

	if titleResult.Pending {
		s.dropGatewayBindingOn(transport, result.StoredSessionID)

		return Session{}, fmt.Errorf("persist Hermes session: session.title remained pending")
	}

	if titleResult.Title != durableTitle {
		s.dropGatewayBindingOn(transport, result.StoredSessionID)

		return Session{}, fmt.Errorf("persist Hermes session: session.title response missing durable title")
	}

	persisted, err := s.persistedSessionsOn(ctx, transport)
	if err != nil {
		return Session{}, fmt.Errorf("verify persisted Hermes session: %w", err)
	}

	for _, candidate := range persisted {
		if candidate.ID == result.StoredSessionID {
			return s.nativeSessionFromGateway(result.StoredSessionID, durableTitle), nil
		}
	}

	return Session{}, fmt.Errorf("verify persisted Hermes session %q: durable row missing", result.StoredSessionID)
}

func (s *hermesServer) GetSession(ctx context.Context, id string) (Session, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return Session{}, ErrGatewayDisconnected
	}

	storedID := id
	if s.liveSessionIDOn(transport, id) == "" {
		result, err := s.resumeGatewaySessionOn(ctx, transport, id)
		if err != nil {
			return Session{}, err
		}

		storedID = result.SessionKey // resumeGatewaySessionOn validated both native identities.
	}

	return s.nativeSessionFromGateway(storedID, ""), nil
}

func (s *hermesServer) ListSessions(ctx context.Context, cwd string) ([]Session, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return nil, ErrGatewayDisconnected
	}

	active, err := transport.client.ActiveList(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]Session, 0, len(active.Sessions))
	for _, item := range active.Sessions {
		if item.SessionID == "" {
			return nil, fmt.Errorf("hermes active_list response missing id")
		}

		if item.SessionKey == "" {
			return nil, fmt.Errorf("hermes active_list response missing session_key for live session %q", item.SessionID)
		}

		session := s.nativeSessionFromGateway(item.SessionKey, item.Title)

		session.Directory = firstNonEmpty(item.Cwd, s.cwd)
		if cwd == "" || session.Directory == cwd {
			out = append(out, session)
		}
	}

	return out, nil
}

func (s *hermesServer) PersistedSessions(ctx context.Context) ([]Session, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return nil, ErrGatewayDisconnected
	}

	return s.persistedSessionsOn(ctx, transport)
}

func (s *hermesServer) persistedSessionsOn(ctx context.Context, transport *gatewayTransport) ([]Session, error) {
	result, err := transport.client.PersistedSessions(ctx)
	if err != nil {
		return nil, err
	}

	if len(result.Sessions) >= 10000 {
		return nil, errors.New("hermes session.list reached its limit; persisted inventory is not exhaustive")
	}

	out := make([]Session, 0, len(result.Sessions))
	for _, item := range result.Sessions {
		if item.SessionID == "" {
			return nil, fmt.Errorf("hermes session.list response missing id")
		}

		out = append(out, s.nativeSessionFromGateway(item.SessionID, item.Title))
	}

	return out, nil
}

func (s *hermesServer) DeleteSession(ctx context.Context, id string) error {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return ErrGatewayDisconnected
	}

	live := s.liveSessionIDOn(transport, id)
	if live == "" {
		active, listErr := transport.client.ActiveList(ctx)
		if listErr != nil && !IsNotFound(listErr) {
			return fmt.Errorf("list live Hermes sessions before delete: %w", listErr)
		}

		for _, item := range active.Sessions {
			if item.SessionKey != id {
				continue
			}

			if item.SessionID == "" {
				return fmt.Errorf("hermes active_list response missing id for stored session %q", id)
			}

			live = item.SessionID

			break
		}
	}

	if live != "" {
		closeErr := transport.client.CloseSession(ctx, live)
		if closeErr != nil && !IsNotFound(closeErr) {
			return fmt.Errorf("close Hermes session before delete: %w", closeErr)
		}
	}

	s.dropGatewayBindingOn(transport, id)

	err := transport.client.DeleteSession(ctx, id)
	if IsNotFound(err) {
		err = nil
	}

	return err
}

// ReloadMCP forces Hermes to reconnect every configured MCP server and rebuild
// the selected session's cached tool surface. Hermes discovers MCP tools while
// the native process starts, which is too early for hosts that arm an
// authorization-scoped MCP endpoint only after session/new has returned.
func (s *hermesServer) ReloadMCP(ctx context.Context, id string) error {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return ErrGatewayDisconnected
	}

	for attempt := 0; ; attempt++ {
		live, err := s.ensureLiveGatewaySessionOn(ctx, transport, id)
		if err != nil {
			return err
		}

		var result struct {
			Status string `json:"status"`
		}

		err = transport.client.Call(ctx, "reload.mcp", map[string]any{
			keySessionIDSnake: live,
			"confirm":         true,
		}, &result)
		if IsNotFound(err) && attempt == 0 {
			// A reconnect or native reload can retire the runtime-only live id.
			// Forget only that routing cache entry, resume the durable key once,
			// and repeat the idempotent reload against the rebound live session.
			s.dropGatewayBindingOn(transport, id)

			continue
		}

		if err != nil {
			return err
		}

		if result.Status != "reloaded" {
			return fmt.Errorf("hermes reload.mcp returned status %q", result.Status)
		}

		return nil
	}
}

func (s *hermesServer) SendMessage(ctx context.Context, id string, req MessageRequest) (NativeMessage, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return NativeMessage{}, ErrGatewayDisconnected
	}

	if req.Model != nil {
		if err := s.setModelOn(ctx, transport, id, ModelSelectionValue(req.Model.ProviderID, req.Model.ModelID)); err != nil {
			return NativeMessage{}, err
		}
	}

	return s.submitGatewayPartsOn(ctx, transport, id, req.Parts)
}

// ModelSelectionValue qualifies one official model row with its owning
// provider exactly once. A slash inside an aggregator's raw model ID is part of
// that model ID; it suppresses qualification only when the exact provider
// prefix is already present.
func ModelSelectionValue(providerID string, modelID string) string {
	if providerID != "" && strings.HasPrefix(modelID, providerID+"/") {
		return modelID
	}

	return firstNonEmptyModelSelection(providerID, modelID)
}

func firstNonEmptyModelSelection(providerID string, modelID string) string {
	if providerID == "" {
		return modelID
	}

	if modelID == "" {
		return providerID
	}

	return providerID + "/" + modelID
}

func assistantMessageError(message NativeMessage) error {
	if !strings.EqualFold(message.Info.Finish, jsonFieldError) && message.Info.Error == nil {
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

func (s *hermesServer) dropGatewayBindingOn(transport *gatewayTransport, stored string) {
	if transport == nil || transport.mappings == nil {
		return
	}

	mappings := transport.mappings
	mappings.mu.Lock()
	defer mappings.mu.Unlock()

	delete(mappings.bindings, stored)
}

func (s *hermesServer) liveSessionID(stored string) string {
	return s.liveSessionIDOn(s.gatewayTransport(), stored)
}

func (s *hermesServer) liveSessionIDOn(transport *gatewayTransport, stored string) string {
	if transport == nil || transport.mappings == nil {
		return ""
	}

	transport.mappings.mu.Lock()
	defer transport.mappings.mu.Unlock()

	return transport.mappings.bindings[stored].live
}

func (s *hermesServer) ensureLiveGatewaySession(ctx context.Context, stored string) (string, error) {
	return s.ensureLiveGatewaySessionOn(ctx, s.gatewayTransport(), stored)
}

func (s *hermesServer) ensureLiveGatewaySessionOn(
	ctx context.Context,
	transport *gatewayTransport,
	stored string,
) (string, error) {
	if live := s.liveSessionIDOn(transport, stored); live != "" {
		return live, nil
	}

	result, err := s.resumeGatewaySessionOn(ctx, transport, stored)
	if err != nil {
		return "", err
	}

	return result.SessionID, nil
}

// resumeGatewaySession asks official Hermes to finish constructing the native
// agent before publishing its live id. Hermes 0.20 otherwise returns from a
// cold resume while a background build is still pending; a config.set sent in
// that window can report success and then be overwritten by the stale build.
func (s *hermesServer) resumeGatewaySessionOn(
	ctx context.Context,
	transport *gatewayTransport,
	stored string,
) (SessionResumeResult, error) {
	if transport == nil {
		return SessionResumeResult{}, ErrGatewayDisconnected
	}

	handshake, err := s.beginGatewayHandshake(ctx, transport, gatewayHandshakeResume)
	if err != nil {
		return SessionResumeResult{}, err
	}

	bound := false
	defer func() {
		if !bound {
			_ = s.cancelGatewayHandshake(transport, handshake)
		}
	}()

	result, sequence, err := transport.client.ResumeSessionWatermark(ctx, stored, map[string]any{keyEagerBuild: true})
	if err != nil {
		return SessionResumeResult{}, err
	}

	resolvedStored, err := s.storedSessionIDFromResume(result)
	if err != nil {
		return SessionResumeResult{}, err
	}

	if s.beforeHandshakeBind != nil {
		s.beforeHandshakeBind(gatewayHandshakeResume, handshake)
	}

	bindResult := s.bindGatewayHandshake(ctx, transport, handshake, gatewayHandshakeResume, resolvedStored, result.SessionID, GatewayWatermark{
		TransportGeneration: transport.generation,
		Sequence:            sequence,
	})
	if bindResult.err != nil {
		return SessionResumeResult{}, bindResult.err
	}

	bound = true

	return result, nil
}

func (s *hermesServer) storedSessionIDFromResume(result SessionResumeResult) (string, error) {
	if result.SessionID == "" {
		return "", fmt.Errorf("hermes session.resume response missing session_id")
	}

	if result.SessionKey == "" {
		return "", fmt.Errorf("hermes session.resume response missing session_key")
	}

	return result.SessionKey, nil
}

func (s *hermesServer) nativeSessionFromGateway(stored string, title string) Session {
	provider, model := splitModelValue(s.defaultModel, "", s.defaultModel)
	now := time.Now().UnixMilli()

	return Session{
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
		if text, _ := part[valText].(string); text != "" {
			if builder.Len() > 0 {
				builder.WriteString("\n\n")
			}

			builder.WriteString(text)
		}
	}

	return builder.String()
}

// imageAttachmentsFromHermesParts collects the decoded bytes of every file part
// in prompt order. A file part carries nothing else the gateway upload takes:
// the media type has already been validated against the bytes, and the upload
// derives its own extension from them.
func imageAttachmentsFromHermesParts(parts []map[string]any) ([][]byte, error) {
	attachments := make([][]byte, 0)

	for _, part := range parts {
		partType, _ := part["type"].(string)
		if partType != valFile {
			continue
		}

		data, _ := part["data"].([]byte)
		if len(data) == 0 {
			return nil, fmt.Errorf("hermes image part requires decoded image data")
		}

		attachments = append(attachments, data)
	}

	return attachments, nil
}

func (s *hermesServer) submitGatewayParts(ctx context.Context, stored string, parts []map[string]any) (NativeMessage, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return NativeMessage{}, ErrGatewayDisconnected
	}

	return s.submitGatewayPartsOn(ctx, transport, stored, parts)
}

func (s *hermesServer) submitGatewayPartsOn(
	ctx context.Context,
	transport *gatewayTransport,
	stored string,
	parts []map[string]any,
) (NativeMessage, error) {
	attachments, err := imageAttachmentsFromHermesParts(parts)
	if err != nil {
		return NativeMessage{}, err
	}

	for attempt := 0; ; attempt++ {
		live, liveErr := s.ensureLiveGatewaySessionOn(ctx, transport, stored)
		if liveErr != nil {
			return NativeMessage{}, liveErr
		}

		message, submitErr := s.submitGatewayTextForLive(ctx, transport, stored, live, textFromHermesParts(parts), attachments)
		if IsNotFound(submitErr) && attempt == 0 {
			// A 4007 RPC response means Hermes rejected the prompt before
			// admission. Re-resume the durable session and retry exactly once;
			// errors after admission are event/transport failures and never enter
			// this branch, so a model turn cannot be duplicated.
			s.dropGatewayBindingOn(transport, stored)

			continue
		}

		return message, submitErr
	}
}

func (s *hermesServer) submitGatewayTextForLive(
	ctx context.Context,
	transport *gatewayTransport,
	stored string,
	live string,
	text string,
	attachments [][]byte,
) (NativeMessage, error) {
	if transport == nil {
		return NativeMessage{}, ErrGatewayDisconnected
	}

	gw := transport.client
	for _, attachment := range attachments {
		if err := gw.AttachImageBytes(ctx, live, attachment); err != nil {
			return NativeMessage{}, err
		}
	}

	actor := s.actorForTransportSession(transport, stored, live)
	generation := transport.generation

	if actor == nil {
		return NativeMessage{}, gatewayDispatcherCause(transport.dispatcher)
	}

	result := make(chan gatewayCycleResult, 1)
	registered := make(chan gatewayPromptHandle, 1)

	s.connMu.Lock()
	s.registrations++
	registrationID := fmt.Sprintf("hermes/%s/registration-%d", stored, s.registrations)
	s.connMu.Unlock()

	registration := &gatewayPromptRegistration{id: registrationID, result: result, reply: registered}
	accepted := false
	acceptedWatermark := uint64(0)
	cancelRegistration := func(cause error) error {
		registration.cancel(cause)

		done := make(chan struct{})
		if !actor.enqueue(gatewayActorMessage{cancel: &gatewayPromptCancel{
			registrationID: registrationID,
			err:            cause,
			accepted:       accepted,
			generation:     generation,
			watermark:      acceptedWatermark,
			done:           done,
		}}) {
			cause := actor.enqueueCause()
			s.failGatewayTransport(transport, cause)

			return cause
		}

		waitCtx, waitCancel := context.WithTimeout(context.Background(), closeTimeout)
		defer waitCancel()

		select {
		case <-done:
			return nil
		case <-actor.done:
			return nil
		case <-waitCtx.Done():
			s.failGatewayTransport(transport, waitCtx.Err())

			return waitCtx.Err()
		}
	}

	if s.beforePromptPhase != nil {
		s.beforePromptPhase(promptPhaseRegister, actor)
	}

	if !actor.enqueue(gatewayActorMessage{register: registration}) {
		cause := actor.enqueueCause()
		s.failGatewayTransport(transport, cause)

		return NativeMessage{}, cause
	}

	var handle gatewayPromptHandle
	select {
	case handle = <-registered:
	case <-actor.done:
		return NativeMessage{}, gatewayDispatcherCause(transport.dispatcher)
	case <-ctx.Done():
		cancelErr := cancelRegistration(ctx.Err())

		return NativeMessage{}, errors.Join(ctx.Err(), cancelErr)
	}

	if handle.err != nil {
		return NativeMessage{}, handle.err
	}

	if s.beforePromptPhase != nil {
		s.beforePromptPhase(promptPhaseSubmit, actor)
	}

	submit, watermark, ambiguous, err := gw.SubmitPromptWatermark(ctx, live, text)
	if err != nil {
		accepted = ambiguous
		acceptedWatermark = watermark

		cancelErr := cancelRegistration(err)
		if ambiguous {
			cause := errors.Join(err, cancelErr)
			s.failGatewayTransport(transport, cause)

			if ctx.Err() != nil {
				return NativeMessage{}, errors.Join(ctx.Err(), cancelErr)
			}

			return NativeMessage{}, gatewayTransportFailure(cause)
		}

		return NativeMessage{}, errors.Join(err, cancelErr)
	}

	disposition, stated := gatewayPromptDispositionFor(submit.Status)
	if !stated {
		ambiguity := fmt.Errorf(
			"%w: prompt.submit returned status %q instead of a stated native disposition",
			ErrGatewayAmbiguousTurn,
			submit.Status,
		)
		accepted = true
		acceptedWatermark = watermark
		cancelErr := cancelRegistration(ambiguity)
		cause := errors.Join(ambiguity, cancelErr)
		s.failGatewayTransport(transport, cause)

		return NativeMessage{}, gatewayTransportFailure(cause)
	}

	accepted = true
	acceptedWatermark = watermark

	failAccepted := func(cause error) error {
		cancelErr := cancelRegistration(cause)
		joined := errors.Join(cause, cancelErr)
		s.failGatewayTransport(transport, joined)

		return joined
	}

	if s.beforePromptPhase != nil {
		s.beforePromptPhase(promptPhaseSynchronize, actor)
	}

	if err := s.synchronizeGatewayWatermark(ctx, transport, watermark); err != nil {
		cause := failAccepted(err)

		if ctx.Err() != nil {
			return NativeMessage{}, ctx.Err()
		}

		return NativeMessage{}, gatewayTransportFailure(cause)
	}

	watermarkReply := make(chan gatewayPromptWatermarkResult, 1)

	command := &gatewayPromptWatermark{
		cycleID: handle.cycleID, watermark: watermark, disposition: disposition, reply: watermarkReply,
	}

	if s.beforePromptPhase != nil {
		s.beforePromptPhase(promptPhaseWatermark, actor)
	}

	if !actor.enqueue(gatewayActorMessage{watermark: command}) {
		cause := actor.enqueueCause()
		_ = failAccepted(cause)

		return NativeMessage{}, cause
	}

	var watermarkResult gatewayPromptWatermarkResult
	select {
	case watermarkResult = <-watermarkReply:
	case <-actor.done:
		cause := gatewayDispatcherCause(transport.dispatcher)
		cause = failAccepted(cause)

		return NativeMessage{}, gatewayTransportFailure(cause)
	case <-ctx.Done():
		cancelErr := failAccepted(ctx.Err())

		return NativeMessage{}, errors.Join(ctx.Err(), cancelErr)
	case <-s.closed:
		_ = failAccepted(ErrGatewayDisconnected)

		return NativeMessage{}, ErrGatewayDisconnected
	}

	if watermarkResult.deferral != nil {
		// Hermes queued this prompt as its next turn. The registration stays open
		// across the turn running now, so the queued prompt is served exactly
		// once, by the native turn hermes runs for it. This wait ends on the
		// actor's answer, on the actor or server ending under it, or on the
		// caller's own cancellation — never on the deferral channel alone.
		if s.beforePromptPhase != nil {
			s.beforePromptPhase(promptPhaseDefer, actor)
		}

		select {
		case watermarkResult = <-watermarkResult.deferral:
		case <-actor.done:
			cause := gatewayDispatcherCause(transport.dispatcher)
			cause = failAccepted(cause)

			return NativeMessage{}, gatewayTransportFailure(cause)
		case <-ctx.Done():
			cancelErr := failAccepted(ctx.Err())

			return NativeMessage{}, errors.Join(ctx.Err(), cancelErr)
		case <-s.closed:
			_ = failAccepted(ErrGatewayDisconnected)

			return NativeMessage{}, ErrGatewayDisconnected
		}
	}

	if watermarkResult.err != nil {
		// A turn that folded this prompt into itself, and a queued turn this
		// adapter could not pick out of the stream, both release the registration
		// and nothing else: hermes runs the text either way, and the connection
		// stays up to carry the turn that runs it.
		if errors.Is(watermarkResult.err, ErrGatewayTurnAbsorbedPrompt) ||
			errors.Is(watermarkResult.err, ErrGatewayPromptQueuedAsNextTurn) {
			return NativeMessage{}, errors.Join(watermarkResult.err, cancelRegistration(watermarkResult.err))
		}

		_ = failAccepted(watermarkResult.err)

		return NativeMessage{}, watermarkResult.err
	}

	for _, projection := range watermarkResult.projections {
		select {
		case projectionErr := <-projection.done:
			if projectionErr != nil {
				_ = failAccepted(projectionErr)

				return NativeMessage{}, projectionErr
			}
		case <-ctx.Done():
			_ = failAccepted(ctx.Err())

			return NativeMessage{}, ctx.Err()
		}
	}

	dispatch := PromptDispatchInfo{
		CycleID:             handle.cycleID,
		TransportGeneration: generation,
	}
	if err := NotifyPromptDispatch(ctx, dispatch); err != nil {
		_ = failAccepted(err)

		return NativeMessage{}, err
	}

	releaseReply := make(chan error, 1)

	release := &gatewayPromptRelease{cycleID: handle.cycleID, reply: releaseReply}

	if s.beforePromptPhase != nil {
		s.beforePromptPhase(promptPhaseRelease, actor)
	}

	if !actor.enqueue(gatewayActorMessage{release: release}) {
		cause := actor.enqueueCause()
		_ = failAccepted(cause)

		return NativeMessage{}, cause
	}

	select {
	case err := <-releaseReply:
		if err != nil {
			_ = failAccepted(err)

			return NativeMessage{}, err
		}
	case <-actor.done:
		cause := gatewayDispatcherCause(transport.dispatcher)
		cause = failAccepted(cause)

		return NativeMessage{}, gatewayTransportFailure(cause)
	case <-ctx.Done():
		_ = failAccepted(ctx.Err())

		return NativeMessage{}, ctx.Err()
	case <-s.closed:
		_ = failAccepted(ErrGatewayDisconnected)

		return NativeMessage{}, ErrGatewayDisconnected
	}

	if s.beforePromptPhase != nil {
		s.beforePromptPhase(promptPhaseResult, actor)
	}

	select {
	case completed, ok := <-handle.result:
		if !ok {
			resultErr := errors.New("hermes gateway prompt cycle closed without a result")
			_ = failAccepted(resultErr)

			return NativeMessage{}, resultErr
		}

		return completed.message, completed.err
	case <-ctx.Done():
		_ = failAccepted(ctx.Err())

		return NativeMessage{}, ctx.Err()
	}
}

// gatewayPromptDispositionFor reads hermes's own answer to one prompt.submit.
// Only an idle session answers "streaming". A busy one never does: hermes
// queues the text as its next turn, or folds it into the turn already running
// by redirecting or steering that turn. Every autonomous driver marks the
// session running before it emits a frame, so a prompt can meet a turn this
// adapter has not seen a single frame of, and none of those answers is a broken
// connection. An answer outside this vocabulary states nothing this adapter can
// attribute frames by, and only that fails closed.
func gatewayPromptDispositionFor(status string) (gatewayPromptDisposition, bool) {
	switch status {
	case "streaming":
		return gatewayPromptOwnsTurn, true
	case "queued":
		return gatewayPromptQueuedTurn, true
	case "redirected", "steered":
		return gatewayPromptJoinedTurn, true
	default:
		return gatewayPromptOwnsTurn, false
	}
}

type gatewayActiveTool struct {
	rawInput json.RawMessage
	name     string
}

type gatewayToolPayload struct {
	ToolID string          `json:"tool_id"`
	Name   string          `json:"name"`
	Args   json.RawMessage `json:"args"`
	Result json.RawMessage `json:"result"`
	Output json.RawMessage `json:"output"`
}

func gatewayToolPart(
	stored string,
	messageID string,
	event Event,
	active gatewayActiveTool,
) (Part, bool) {
	var payload gatewayToolPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.ToolID == "" {
		return Part{}, false
	}

	toolName := payload.Name
	if active.name != "" {
		toolName = active.name
	}

	status := "running"
	if event.Type == evtToolComplete {
		status = valCompleted
		if gatewayToolFailed(payload, toolName) {
			status = valFailed
		}
	}

	state := map[string]any{jsonFieldStatus: status}

	switch {
	case event.Type == evtToolComplete && len(payload.Args) > 0:
		state["rawInput"] = payload.Args
	case event.Type == evtToolStart:
		state["rawInput"] = event.Payload
	case len(active.rawInput) > 0:
		state["rawInput"] = active.rawInput
	}

	if event.Type == evtToolComplete {
		if len(payload.Result) > 0 {
			state["rawOutput"] = payload.Result
		} else if len(payload.Output) > 0 {
			state["rawOutput"] = payload.Output
		}
	}

	stateData, _ := json.Marshal(state)
	part := Part{
		ID:        messageID + "-tool-" + payload.ToolID,
		SessionID: stored,
		MessageID: messageID,
		Type:      valTool,
		CallID:    payload.ToolID,
		Tool:      firstNonEmpty(toolName, "tool"),
		State:     stateData,
	}
	data, _ := json.Marshal(part)
	part.Raw = data

	return part, true
}

func gatewayToolFailed(payload gatewayToolPayload, toolName string) bool {
	raw := payload.Result
	if len(raw) == 0 {
		raw = payload.Output
	}

	return gatewayToolResultFailed(raw, toolName)
}

func gatewayToolResultFailed(raw json.RawMessage, toolName string) bool {
	if len(raw) == 0 {
		return false
	}

	value, ok := gatewayJSONValue(raw)
	if !ok {
		return false
	}

	if text, isText := value.(string); isText {
		if strings.HasPrefix(text, "Error executing tool '") {
			return true
		}

		value, ok = gatewayJSONValue([]byte(text))
		if !ok {
			return false
		}
	}

	result, ok := value.(map[string]any)
	if !ok {
		return false
	}

	if success, exists := result["success"]; exists && success == false {
		return true
	}

	if resultOK, exists := result["ok"]; exists && resultOK == false {
		return true
	}

	exitCode, exists := result["exit_code"]
	if !exists {
		exitCode = result["returncode"]
	}

	if gatewayNonzeroInteger(exitCode) {
		return true
	}

	return gatewayPolishedTool(toolName) && gatewayTruthy(result[jsonFieldError]) && !gatewayTruthy(result["content"])
}

func gatewayJSONValue(raw []byte) (any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if decoder.Decode(&value) != nil {
		return nil, false
	}

	return value, true
}

func gatewayNonzeroInteger(value any) bool {
	if boolean, ok := value.(bool); ok {
		return boolean
	}

	number, ok := value.(json.Number)
	if !ok {
		return false
	}

	text := number.String()
	if strings.ContainsAny(text, ".eE") {
		return false
	}

	text = strings.TrimPrefix(text, "-")
	for _, digit := range text {
		if digit != '0' {
			return true
		}
	}

	return false
}

func gatewayTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case json.Number:
		mantissa, _, _ := strings.Cut(typed.String(), "e")
		if mantissa == typed.String() {
			mantissa, _, _ = strings.Cut(typed.String(), "E")
		}

		for _, digit := range mantissa {
			if digit >= '1' && digit <= '9' {
				return true
			}
		}

		return false
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

func gatewayPolishedTool(toolName string) bool {
	switch toolName {
	case "todo", "memory", "session_search", "delegate_task",
		"read_file", "write_file", "patch", "search_files", valTerminal, "process", "execute_code",
		"skill_view", "skills_list", "skill_manage", "web_search", "web_extract",
		"browser_navigate", "browser_click", "browser_type", "browser_press", "browser_scroll",
		"browser_back", "browser_snapshot", "browser_console", "browser_get_images", "browser_vision",
		"vision_analyze", "image_generate", "text_to_speech",
		"cronjob", "send_message", "clarify", "discord", "discord_admin",
		"ha_list_entities", "ha_get_state", "ha_list_services", "ha_call_service",
		"feishu_doc_read", "feishu_drive_list_comments", "feishu_drive_list_comment_replies",
		"feishu_drive_reply_comment", "feishu_drive_add_comment",
		"kanban_create", "kanban_show", "kanban_comment", "kanban_complete",
		"kanban_block", "kanban_link", "kanban_heartbeat",
		"yb_query_group_info", "yb_query_group_members", "yb_search_sticker",
		"yb_send_dm", "yb_send_sticker":
		return true
	default:
		return false
	}
}

func gatewayToolCallID(raw json.RawMessage) string {
	return gatewayPayloadString(raw, "tool_id")
}

func uniqueGatewayToolCallID(active map[string]struct{}) string {
	if len(active) != 1 {
		return ""
	}

	var unique string

	for toolCallID := range active {
		unique = toolCallID
	}

	return unique
}

func (s *hermesServer) declineGatewayQuestion(ctx context.Context, generation uint64, live string, eventType string) {
	s.connMu.Lock()
	dispatcher := s.dispatchers[generation]
	current := s.transport != nil && s.transport.generation == generation &&
		s.transport.dispatcher == dispatcher
	s.connMu.Unlock()

	if !current || dispatcher == nil || live == "" {
		return
	}

	client := dispatcher.client

	switch eventType {
	case evtTerminalReadReq:
		_ = client.Call(ctx, "terminal.read.respond", map[string]any{keySessionIDSnake: live, valText: ""}, nil)
	case evtSudoRequest:
		_ = client.Call(ctx, "sudo.respond", map[string]any{keySessionIDSnake: live, "password": ""}, nil)
	case evtSecretRequest:
		_ = client.Call(ctx, "secret.respond", map[string]any{keySessionIDSnake: live, keyValue: ""}, nil)
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

// gatewayCompleteText reads the authoritative final assistant text a
// message.complete event carries. The payload also carries status, usage,
// reasoning and an ANSI-rendered copy of the same text for terminal display,
// so this reads the one member that names the message itself.
func gatewayCompleteText(raw json.RawMessage) string {
	var payload struct {
		Text string `json:"text"`
	}

	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}

	return payload.Text
}

// gatewayCompleteFinish reads the structured native finish a message.complete
// event reached. Hermes reports the status of a turn a stop, a steer, or a
// barge-in ended as "interrupted"; that is a cancel, and reporting it as the
// clean stop the successful status names would misstate the turn's boundary.
func gatewayCompleteFinish(raw json.RawMessage) string {
	if strings.EqualFold(gatewayPayloadString(raw, jsonFieldStatus), valInterrupted) {
		return valCancelled
	}

	return valStop
}

func gatewayUsageTokens(raw json.RawMessage) Tokens {
	var payload map[string]any

	_ = json.Unmarshal(raw, &payload)
	usage, _ := payload["usage"].(map[string]any)

	return Tokens{
		Total:     numberValue(usage["total_tokens"], usage["total"]),
		Input:     numberValue(usage["input_tokens"], usage["prompt_tokens"], usage["input"]),
		Output:    numberValue(usage["output_tokens"], usage["completion_tokens"], usage["output"]),
		Reasoning: numberValue(usage["reasoning_tokens"], usage[valReasoning]),
	}
}

func gatewayContextWindow(raw json.RawMessage) int {
	var payload map[string]any

	_ = json.Unmarshal(raw, &payload)
	usage, _ := payload["usage"].(map[string]any)
	contextWindow := numberValue(usage["context_max"])

	maxInt := int(^uint(0) >> 1)

	if contextWindow <= 0 || contextWindow != math.Trunc(contextWindow) || contextWindow > float64(maxInt) {
		return 0
	}

	return int(contextWindow)
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

func nativeMessagesFromGateway(stored string, messages []Message) []NativeMessage {
	out := make([]NativeMessage, 0, len(messages))
	for index := range messages {
		message := &messages[index]
		messageID := fmt.Sprintf("history-%d", index+1)
		out = append(out, NativeMessage{
			Info: NativeMessageInfo{
				ID:        messageID,
				SessionID: stored,
				Role:      firstNonEmpty(message.Role, valAssistant),
				Finish:    valStop,
			},
			Parts: []Part{{
				ID:        messageID + "-text",
				SessionID: stored,
				MessageID: messageID,
				Type:      valText,
				Text:      message.Text,
				Raw:       message.Raw,
			}},
		})
	}

	return out
}

func providersFromGateway(result ModelOptionsResult) ProvidersResponse {
	providers := make([]ProviderInfo, 0, len(result.Providers))
	for index := range result.Providers {
		provider := &result.Providers[index]

		info := ProviderInfo{
			ID:     provider.Slug,
			Name:   firstNonEmpty(provider.Name, provider.Slug),
			Models: map[string]ProviderModel{},
		}
		for _, modelID := range provider.Models {
			if modelID == "" {
				continue
			}

			info.Models[modelID] = ProviderModel{ID: modelID, Name: modelID}
		}

		providers = append(providers, info)
	}

	return ProvidersResponse{Providers: providers, Raw: result.Raw}
}

func (s *hermesServer) Messages(ctx context.Context, id string) ([]NativeMessage, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return nil, ErrGatewayDisconnected
	}

	live, err := s.ensureLiveGatewaySessionOn(ctx, transport, id)
	if err != nil {
		return nil, err
	}

	history, err := transport.client.History(ctx, live)
	if err != nil {
		return nil, err
	}

	return nativeMessagesFromGateway(id, history.Messages), nil
}

func (s *hermesServer) Abort(ctx context.Context, id string) error {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return ErrGatewayDisconnected
	}

	live := s.liveSessionIDOn(transport, id)
	if live == "" {
		return nil
	}

	return transport.client.Interrupt(ctx, live)
}

func (s *hermesServer) Fork(ctx context.Context, id string, messageID string) (Session, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return Session{}, ErrGatewayDisconnected
	}

	persisted, err := s.persistedSessionsOn(ctx, transport)
	if err != nil {
		return Session{}, fmt.Errorf("list Hermes sessions before branch: %w", err)
	}

	baseline := make([]string, 0, len(persisted))
	for _, session := range persisted {
		baseline = append(baseline, session.ID)
	}

	return s.forkWithBaselineOn(ctx, transport, id, messageID, baseline)
}

func (s *hermesServer) ForkWithBaseline(
	ctx context.Context,
	id string,
	marker string,
	baseline []string,
) (Session, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return Session{}, ErrGatewayDisconnected
	}

	return s.forkWithBaselineOn(ctx, transport, id, marker, baseline)
}

func (s *hermesServer) forkWithBaselineOn(
	ctx context.Context,
	transport *gatewayTransport,
	id string,
	marker string,
	baseline []string,
) (Session, error) {
	live, err := s.ensureLiveGatewaySessionOn(ctx, transport, id)
	if err != nil {
		return Session{}, err
	}

	result, err := transport.client.Branch(ctx, live, marker)
	if IsNotFound(err) {
		s.dropGatewayBindingOn(transport, id)

		live, err = s.ensureLiveGatewaySessionOn(ctx, transport, id)
		if err != nil {
			return Session{}, err
		}

		result, err = transport.client.Branch(ctx, live, marker)
	}

	if err != nil {
		return Session{}, s.recoverFailedBranch(ctx, transport, marker, baseline, err)
	}

	if result.SessionID == "" {
		return Session{}, fmt.Errorf("hermes branch response missing session_id")
	}

	if result.StoredSessionID == "" {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := transport.client.CloseSession(cleanupCtx, result.SessionID)

		cleanupCancel()

		if IsNotFound(closeErr) {
			closeErr = nil
		}

		return Session{}, errors.Join(errors.New("hermes branch response missing stored_session_id"), closeErr)
	}

	// session.branch returns a live child owned by the parent's gateway. Close
	// that runtime before publishing the durable child ID: the child adapter
	// process will resume the same stored session, and two live gateways must
	// never be able to drive it concurrently.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	closeErr := transport.client.CloseSession(cleanupCtx, result.SessionID)

	cleanupCancel()

	if IsNotFound(closeErr) {
		closeErr = nil
	}

	s.dropGatewayBindingOn(transport, result.StoredSessionID)

	if closeErr != nil {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		deleteErr := transport.client.DeleteSession(deleteCtx, result.StoredSessionID)

		deleteCancel()

		if IsNotFound(deleteErr) {
			deleteErr = nil
		}

		return Session{}, errors.Join(fmt.Errorf("close Hermes branch runtime: %w", closeErr), deleteErr)
	}

	return s.nativeSessionFromGateway(result.StoredSessionID, firstNonEmpty(result.Title, "Hermes branch")), nil
}

func (s *hermesServer) recoverFailedBranch(
	ctx context.Context,
	transport *gatewayTransport,
	marker string,
	baseline []string,
	branchErr error,
) error {
	persisted, listErr := s.persistedSessionsOn(ctx, transport)
	if listErr != nil {
		return errors.Join(branchErr, fmt.Errorf("list durable Hermes sessions after failed branch: %w", listErr))
	}

	known := make(map[string]struct{}, len(baseline))
	for _, id := range baseline {
		known[id] = struct{}{}
	}

	delta := make([]Session, 0, 1)

	for _, session := range persisted {
		if _, ok := known[session.ID]; !ok {
			delta = append(delta, session)
		}
	}

	if len(delta) == 0 {
		return branchErr
	}

	if len(delta) != 1 || marker == "" || delta[0].Title != marker {
		return errors.Join(branchErr, fmt.Errorf("%w: durable session delta has %d rows", ErrBranchRecoveryAmbiguous, len(delta)))
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cleanupCancel()

	cleanupErr := transport.client.DeleteSession(cleanupCtx, delta[0].ID)
	if IsNotFound(cleanupErr) {
		cleanupErr = nil
	}

	if cleanupErr != nil {
		return errors.Join(branchErr, fmt.Errorf("delete failed Hermes branch %q: %w", delta[0].ID, cleanupErr))
	}

	remaining, verifyErr := s.persistedSessionsOn(cleanupCtx, transport)
	if verifyErr != nil {
		return errors.Join(branchErr, fmt.Errorf("verify failed Hermes branch cleanup: %w", verifyErr))
	}

	for _, session := range remaining {
		if session.ID == delta[0].ID {
			return errors.Join(branchErr, fmt.Errorf("delete failed Hermes branch %q: durable row remains", delta[0].ID))
		}
	}

	return branchErr
}

func (s *hermesServer) ConfigProviders(ctx context.Context) (ProvidersResponse, error) {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return ProvidersResponse{}, ErrGatewayDisconnected
	}

	models, err := transport.client.ModelOptions(ctx, "")
	if err != nil {
		return ProvidersResponse{}, err
	}

	return providersFromGateway(models), nil
}

// SetModel applies a session-scoped official gateway model selection.
func (s *hermesServer) SetModel(ctx context.Context, stored string, value string) error {
	transport := s.beginGatewayTurn()
	defer s.endGatewayTurn()

	if transport == nil {
		return ErrGatewayDisconnected
	}

	return s.setModelOn(ctx, transport, stored, value)
}

func (s *hermesServer) setModelOn(ctx context.Context, transport *gatewayTransport, stored string, value string) error {
	for attempt := 0; ; attempt++ {
		live, err := s.ensureLiveGatewaySessionOn(ctx, transport, stored)
		if err != nil {
			return err
		}

		err = transport.client.SetModel(ctx, live, value)
		if IsNotFound(err) && attempt == 0 {
			s.dropGatewayBindingOn(transport, stored)

			continue
		}

		return err
	}
}

func (s *hermesServer) ReplyPermission(ctx context.Context, req PermissionRequest, reply string, message string) error {
	_ = message
	choice := "deny"

	switch reply {
	case valOnce, valAlways:
		choice = reply
	}

	if req.ID == "" || req.CycleID == "" || req.TransportGeneration == 0 || req.route == nil {
		return fmt.Errorf("%w: permission transport identity is stale or missing", ErrGatewayAmbiguousTurn)
	}

	// The answer settles exactly the one request the host was shown. Native
	// approval.respond's `all` resolves every approval queued on the session
	// with this same choice, including ones no host ever saw, and what "always"
	// means beyond this request — the session and permanent allowlist entries
	// hermes writes for the matched pattern — is carried by the choice itself.
	return s.completeGatewayControl(ctx, req.route, gatewayControlPermission, choice, nil)
}

func (s *hermesServer) ReplyQuestion(ctx context.Context, req QuestionRequest, answers [][]string) error {
	if req.ID == "" || req.CycleID == "" || req.TransportGeneration == 0 || req.route == nil {
		return fmt.Errorf("%w: question transport identity is stale or missing", ErrGatewayAmbiguousTurn)
	}

	return s.completeGatewayControl(ctx, req.route, gatewayControlQuestion, "", answers)
}

func (s *hermesServer) RejectQuestion(ctx context.Context, req QuestionRequest) error {
	if req.ID == "" || req.CycleID == "" || req.TransportGeneration == 0 || req.route == nil {
		return fmt.Errorf("%w: question transport identity is stale or missing", ErrGatewayAmbiguousTurn)
	}

	return s.completeGatewayControl(ctx, req.route, gatewayControlQuestion, "", "")
}

// CreateGenerationXDGDirs creates the actual wrapper-owned writable state for
// one Hermes runtime incarnation in a fresh, non-reused scratch generation.
func CreateGenerationXDGDirs(scratchParent string) (XDGDirs, error) {
	base, err := generationMkdirTemp(scratchParent, "acp-go-hermes-runtime-")
	if err != nil {
		return XDGDirs{}, err
	}

	dirs := XDGDirs{
		Root:   base,
		Data:   filepath.Join(base, "data"),
		Config: filepath.Join(base, "config"),
		Cache:  filepath.Join(base, "cache"),
		State:  filepath.Join(base, "state"),
	}
	if err := ensureXDGDirs(dirs); err != nil {
		_ = generationRemoveAll(base)

		return XDGDirs{}, err
	}

	return dirs, nil
}

// SharedHomeXDGDirs maps one exact durable HERMES_HOME to the XDG directory
// shape used by the server. It creates no per-session suffix.
func SharedHomeXDGDirs(home string) (XDGDirs, error) {
	if home == "" || !filepath.IsAbs(home) {
		return XDGDirs{}, errors.New("shared Hermes home must be an absolute path")
	}

	root := filepath.Clean(home)
	dirs := XDGDirs{
		Root:   root,
		Data:   filepath.Join(root, "data"),
		Config: filepath.Join(root, "config"),
		Cache:  filepath.Join(root, "cache"),
		State:  filepath.Join(root, "state"),
	}

	return dirs, ensureXDGDirs(dirs)
}

func ensureXDGDirs(dirs XDGDirs) error {
	for _, dir := range []string{dirs.Root, dirs.Data, dirs.Config, dirs.Cache, dirs.State} {
		if dir == "" {
			return fmt.Errorf("xdg directory is empty")
		}

		if err := xdgMkdirAll(dir, 0o700); err != nil {
			return err
		}
	}

	return nil
}

var (
	generationMkdirTemp = os.MkdirTemp
	generationRemoveAll = os.RemoveAll
	xdgMkdirAll         = os.MkdirAll
)

const (
	hermesConfigFileName   = "config.yaml"
	hermesSeedManifestName = ".seed-manifest.json"
	hermesSeedPendingName  = ".seed-pending.json"
	hermesSeedBackupSuffix = ".seed.bak"
)

// seedWrite is a single planned write under the session config root: the
// slash-form cleaned relative path (the ownership-manifest key and .seed.bak
// base) and the final bytes to author.
type seedWrite struct {
	relative string
	target   string
	bytes    []byte
}

// materializeHermesConfig authors the per-session Hermes config under home. The
// wrapper also owns config.yaml (it writes the mcp_servers block hermes reads),
// so a seeded config.yaml must not silently clobber it: the wrapper's managed
// mcp_servers block is deep-merged on top of any seeded config.yaml (the
// wrapper wins for mcp_servers; the seed supplies everything else, e.g. a model
// block). Every other seed file is written verbatim. Seed paths are confined to
// home so absolute paths, ".." segments, and empty keys are rejected with the
// uniform unsupported error. Every final write is routed through the ownership
// manifest so a seed can never clobber an operator-authored file.
func materializeHermesConfig(home string, servers []acp.McpServer, files map[string]string) error {
	return materializeHermesConfigWithWriter(home, servers, files, os.WriteFile)
}

func materializeHermesConfigWithWriter(home string, servers []acp.McpServer, files map[string]string, writeFile func(string, []byte, os.FileMode) error) error {
	var managed map[string]any

	if len(servers) > 0 {
		managed = MCPServersConfig(servers)
	}

	writes, seededConfig, haveSeededConfig, err := buildHermesSeedWrites(home, files)
	if err != nil {
		return err
	}

	writes = append(writes, hermesPathInitWrite(home))

	// config.yaml is authored last: verbatim when only the seed owns it, or the
	// seed deep-merged under the wrapper's managed mcp_servers block.
	if managed != nil || haveSeededConfig {
		configBytes, err := hermesConfigBytes(managed, seededConfig, haveSeededConfig)
		if err != nil {
			return err
		}

		writes = append(writes, seedWrite{
			relative: hermesConfigFileName,
			target:   filepath.Join(home, hermesConfigFileName),
			bytes:    configBytes,
		})
	}

	return applyHermesSeedGuardWithWriter(home, writes, writeFile)
}

// hermesConfigBytes returns the final config.yaml bytes: the seeded contents
// verbatim when the wrapper manages nothing, otherwise the seed deep-merged
// under the wrapper's managed keys.
func hermesConfigBytes(managed map[string]any, seededConfig string, haveSeededConfig bool) ([]byte, error) {
	if managed == nil {
		return []byte(seededConfig), nil
	}

	base := map[string]any{}
	if haveSeededConfig {
		if err := hermesUnmarshalYAML([]byte(seededConfig), &base); err != nil {
			return nil, seedFileInvalid(hermesConfigFileName)
		}
	}

	merged := deepMergeYAML(base, managed)

	data, err := hermesMarshalIndent(merged, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(data, '\n'), nil
}

// applyHermesSeedGuard writes each planned seed file under home behind an
// ownership manifest so a seed can never clobber a file the wrapper did not
// author. It pre-flights every target: if any target already exists and is not
// recorded in the manifest it fails closed with the uniform unsupported error,
// writing nothing. Recorded targets are overwritten (keeping a .seed.bak copy
// of the prior bytes when they differ), and first writes are recorded in the
// manifest. The isolation harness gives each session a fresh root, so the
// manifest is normally absent and every write is a first write.
func applyHermesSeedGuard(home string, writes []seedWrite) error {
	return applyHermesSeedGuardWithWriter(home, writes, os.WriteFile)
}

func applyHermesSeedGuardWithWriter(home string, writes []seedWrite, writeFile func(string, []byte, os.FileMode) error) error {
	if len(writes) == 0 {
		return nil
	}

	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}

	manifest, err := loadHermesSeedManifest(home)
	if err != nil {
		return err
	}

	pending, pendingExists, err := loadHermesSeedPending(home)
	if err != nil {
		return err
	}

	intended := make(map[string]string, len(writes))
	for _, write := range writes {
		digest := sha256.Sum256(write.bytes)
		intended[write.relative] = hex.EncodeToString(digest[:])
	}

	if pendingExists && !equalStringMaps(pending, intended) {
		return errors.New("shared Hermes seed recovery does not match the pending managed configuration")
	}

	// Pre-flight: reject before touching disk if any target is an existing
	// operator file. A pending journal admits only exact bytes written by an
	// interrupted prior pass; it never turns a different existing file into a
	// managed one.
	for _, write := range writes {
		if _, err := os.Lstat(write.target); err != nil {
			// Absent (or a non-directory parent): not a managed clobber; the
			// write step surfaces any real I/O error.
			continue
		}

		if manifest[write.relative] {
			continue
		}

		if pendingExists {
			current, readErr := os.ReadFile(write.target)
			if readErr != nil {
				return readErr
			}

			digest := sha256.Sum256(current)
			if hex.EncodeToString(digest[:]) == pending[write.relative] {
				continue
			}
		}

		if !manifest[write.relative] {
			return seedFileInvalid(write.relative)
		}
	}

	if !pendingExists {
		if err := saveHermesSeedPendingWithWriter(home, intended, writeFile); err != nil {
			return err
		}
	}

	changed := false

	for _, write := range writes {
		if err := writeManagedSeedFileWithWriter(write.target, write.bytes, writeFile); err != nil {
			return err
		}

		if !manifest[write.relative] {
			manifest[write.relative] = true
			changed = true
		}
	}

	if changed {
		if err := saveHermesSeedManifestWithWriter(home, manifest, writeFile); err != nil {
			return err
		}
	}

	return clearHermesSeedPending(home)
}

func loadHermesSeedPending(home string) (map[string]string, bool, error) {
	data, err := os.ReadFile(filepath.Join(home, hermesSeedPendingName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	var pending map[string]string
	if err := json.Unmarshal(data, &pending); err != nil {
		return nil, false, err
	}

	if pending == nil {
		return nil, false, errors.New("hermes seed pending journal is empty")
	}

	return pending, true, nil
}

func saveHermesSeedPendingWithWriter(home string, pending map[string]string, writeFile func(string, []byte, os.FileMode) error) error {
	data, err := hermesMarshalIndent(pending, "", "  ")
	if err != nil {
		return err
	}

	return writeFile(filepath.Join(home, hermesSeedPendingName), append(data, '\n'), 0o600)
}

func clearHermesSeedPending(home string) error {
	err := os.Remove(filepath.Join(home, hermesSeedPendingName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return err
	}

	return syncSharedHermesDirectory(home)
}

func equalStringMaps(left map[string]string, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}

	for key, value := range left {
		if right[key] != value {
			return false
		}
	}

	return true
}

// writeManagedSeedFile writes data to target, first copying the current on-disk
// bytes to <target>.seed.bak when they differ from data. An identical existing
// file is left untouched (no backup, no rewrite).
func writeManagedSeedFile(target string, data []byte) error {
	return writeManagedSeedFileWithWriter(target, data, os.WriteFile)
}

func writeManagedSeedFileWithWriter(target string, data []byte, writeFile func(string, []byte, os.FileMode) error) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}

	current, err := os.ReadFile(target)
	switch {
	case err == nil:
		if bytes.Equal(current, data) {
			return nil
		}

		if writeErr := writeFile(target+hermesSeedBackupSuffix, current, 0o600); writeErr != nil {
			return writeErr
		}
	case errors.Is(err, os.ErrNotExist):
		// First write; fall through.
	default:
		return err
	}

	return writeFile(target, data, 0o600)
}

// loadHermesSeedManifest reads the ownership manifest under home into a set of
// managed relative paths. An absent manifest yields an empty set.
func loadHermesSeedManifest(home string) (map[string]bool, error) {
	data, err := os.ReadFile(filepath.Join(home, hermesSeedManifestName))
	switch {
	case err == nil:
	case errors.Is(err, os.ErrNotExist):
		return make(map[string]bool), nil
	default:
		return nil, err
	}

	var entries []string

	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	manifest := make(map[string]bool, len(entries))
	for _, entry := range entries {
		manifest[entry] = true
	}

	return manifest, nil
}

// saveHermesSeedManifest writes the sorted, deterministic ownership manifest
// under home.
func saveHermesSeedManifest(home string, manifest map[string]bool) error {
	return saveHermesSeedManifestWithWriter(home, manifest, os.WriteFile)
}

func saveHermesSeedManifestWithWriter(home string, manifest map[string]bool, writeFile func(string, []byte, os.FileMode) error) error {
	entries := make([]string, 0, len(manifest))
	for entry := range manifest {
		entries = append(entries, entry)
	}

	sort.Strings(entries)

	data, err := hermesMarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}

	return writeFile(filepath.Join(home, hermesSeedManifestName), append(data, '\n'), 0o600)
}

// seedFileInvalid is the uniform unsupported-field error naming an offending
// seed relative path.
func seedFileInvalid(relative string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
		keyField:       fmt.Sprintf("seedFiles[%q]", relative),
	})
}

// MCPServersConfig builds the wrapper-managed config block that hermes
// reads for MCP servers. Callers pass a non-empty server list that has already
// passed validateMCPServers, so every entry is stdio or http and carries a
// non-empty name unique within the request; names are used verbatim as the
// config keys with no fabrication or deduplication.
func MCPServersConfig(servers []acp.McpServer) map[string]any {
	mcpServers := make(map[string]any, len(servers))

	for _, server := range servers {
		switch {
		case server.Stdio != nil:
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

			mcpServers[server.Stdio.Name] = entry
		case server.Http != nil:
			headers := map[string]string{}
			for _, item := range server.Http.Headers {
				headers[item.Name] = item.Value
			}

			entry := map[string]any{valURL: server.Http.Url}
			if len(headers) > 0 {
				entry["headers"] = headers
			}

			mcpServers[server.Http.Name] = entry
		}
	}

	return map[string]any{"mcp_servers": mcpServers}
}

// mcpServersWithSecretEnv replaces every literal stdio environment and HTTP header value with a
// generated environment reference before config.yaml is authored. The values
// are returned separately for injection into this session's hermes process;
// neither the config nor adapter-owned durable state receives the secret.
func mcpServersWithSecretEnv(servers []acp.McpServer, baseEnv map[string]string) ([]acp.McpServer, map[string]string, error) {
	cloned := make([]acp.McpServer, len(servers))
	secrets := map[string]string{}

	for serverIndex, server := range servers {
		cloned[serverIndex] = server
		if server.Stdio != nil {
			stdioServer := *server.Stdio
			stdioServer.Args = append([]string(nil), server.Stdio.Args...)

			stdioServer.Env = append([]acp.EnvVariable(nil), server.Stdio.Env...)
			for envIndex := range stdioServer.Env {
				name := fmt.Sprintf("ACP_GO_HERMES_MCP_ENV_%d_%d", serverIndex+1, envIndex+1)
				if _, exists := baseEnv[name]; exists {
					return nil, nil, fmt.Errorf("reserved MCP environment variable collision: %s", name)
				}

				secrets[name] = stdioServer.Env[envIndex].Value
				stdioServer.Env[envIndex].Value = "${" + name + "}"
			}

			cloned[serverIndex].Stdio = &stdioServer
		}

		if server.Http == nil {
			continue
		}

		httpServer := *server.Http

		httpServer.Headers = append([]acp.HttpHeader(nil), server.Http.Headers...)
		for headerIndex := range httpServer.Headers {
			name := fmt.Sprintf("ACP_GO_HERMES_MCP_HEADER_%d_%d", serverIndex+1, headerIndex+1)
			if _, exists := baseEnv[name]; exists {
				return nil, nil, fmt.Errorf("reserved MCP environment variable collision: %s", name)
			}

			secrets[name] = httpServer.Headers[headerIndex].Value
			httpServer.Headers[headerIndex].Value = "${" + name + "}"
		}

		cloned[serverIndex].Http = &httpServer
	}

	return cloned, secrets, nil
}

// RedactedMCPServers returns the deterministic config shape used by a shared
// Hermes home without retaining any stdio environment or HTTP header values.
// Adapter admission uses it so per-session secret rotation does not look like
// a process-global MCP configuration change.
func RedactedMCPServers(servers []acp.McpServer) ([]acp.McpServer, error) {
	redacted, _, err := mcpServersWithSecretEnv(servers, nil)

	return redacted, err
}

func cloneEnvironmentMap(source map[string]string) map[string]string {
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}

	return cloned
}

// buildHermesSeedWrites resolves each seeded file into a planned write under
// home, confining paths to that root. The seeded config.yaml is not planned
// here: its raw contents are returned so the caller can deep-merge the wrapper's
// managed keys on top before authoring the final file.
func buildHermesSeedWrites(home string, files map[string]string) ([]seedWrite, string, bool, error) {
	writes := make([]seedWrite, 0, len(files))

	var seededConfig string

	haveSeededConfig := false

	for relative, contents := range files {
		clean, target, err := resolveSeedFilePath(home, relative)
		if err != nil {
			return nil, "", false, err
		}

		if clean == hermesConfigFileName {
			seededConfig = contents
			haveSeededConfig = true

			continue
		}

		writes = append(writes, seedWrite{
			relative: filepath.ToSlash(clean),
			target:   target,
			bytes:    []byte(contents),
		})
	}

	return writes, seededConfig, haveSeededConfig, nil
}

// resolveSeedFilePath validates a relative seed path and joins it under home,
// failing closed with the uniform unsupported error on absolute paths, ".."
// segments, or empty and whitespace-only keys. It returns the cleaned relative
// path and the absolute target.
func resolveSeedFilePath(home string, relative string) (string, string, error) {
	invalid := func() error {
		return acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnsupported,
			keyField:       fmt.Sprintf("seedFiles[%q]", relative),
		})
	}
	if strings.TrimSpace(relative) == "" || filepath.IsAbs(relative) {
		return "", "", invalid()
	}
	// Reject any ".." segment so the cleaned join can never escape home; a
	// relative path without ".." segments always stays confined under home.
	for _, segment := range strings.Split(filepath.ToSlash(relative), "/") {
		if segment == ".." {
			return "", "", invalid()
		}
	}

	clean := filepath.Clean(filepath.FromSlash(relative))

	slashClean := filepath.ToSlash(clean)
	for _, segment := range strings.Split(slashClean, "/") {
		folded := strings.ToLower(segment)
		if strings.EqualFold(folded, sharedSessionOwnersDir) ||
			strings.HasPrefix(folded, ".acp-go-hermes-") ||
			strings.EqualFold(folded, hermesSeedManifestName) ||
			strings.EqualFold(folded, hermesSeedPendingName) ||
			strings.HasSuffix(folded, strings.ToLower(hermesSeedBackupSuffix)) {
			return "", "", invalid()
		}
	}

	return clean, filepath.Join(home, clean), nil
}

// deepMergeYAML returns base with override applied on top: nested maps are
// merged recursively, and override wins for every conflicting key.
func deepMergeYAML(base, override map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(override))
	for key, value := range base {
		merged[key] = value
	}

	for key, value := range override {
		if existing, ok := merged[key].(map[string]any); ok {
			if next, ok := value.(map[string]any); ok {
				merged[key] = deepMergeYAML(existing, next)

				continue
			}
		}

		merged[key] = value
	}

	return merged
}

func ControlDirForXDG(root string) string {
	return root + ".control"
}
