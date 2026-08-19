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
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"gopkg.in/yaml.v3"
)

const (
	LeaseFileName = "server.lease"

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
	keyTitle            = "title"
	keySessionIDSnake   = "session_id"
	keyValue            = "value"
	keyField            = "field"
	keySource           = "source"
	keyQuestion         = "question"
	keyEagerBuild       = "eager_build"
	jsonFieldError      = "error"
	jsonFieldCwd        = "cwd"
	msgHermesNeedsInput = "Hermes needs input"

	evtApprovalRequest    = "approval.request"
	evtClarifyRequest     = "clarify.request"
	evtSecretRequest      = "secret.request"
	evtMessageDelta       = "message.delta"
	evtMessageComplete    = "message.complete"
	evtMessagePartUpdated = "message.part.updated"
	evtSessionError       = "session.error"
	evtSudoRequest        = "sudo.request"
	evtThinkingDelta      = "thinking.delta"
	evtTerminalReadReq    = "terminal.read.request"
	evtToolComplete       = "tool.complete"
	evtToolStart          = "tool.start"
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
	Events() <-chan TurnEvent
	EventErrors() <-chan error
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
	SharedHermesHome            string
	SharedNativeSessionOwner    *SharedSessionOwner
	Env                         map[string]string
	SessionEnv                  map[string]string
	ExtraPathDirs               []string
	Isolation                   *ProcessIsolation
	AmbientEnvironment          map[string]string
	HealthTimeout               time.Duration
	Logger                      *slog.Logger
	ExistingXDG                 XDGDirs
	MCPServers                  []acp.McpServer
	SeedFiles                   map[string]string
	ObserveStartupStage         func(context.Context, string, string, time.Duration, error)
	AcquireDiscoveryResources   func(context.Context) (func(), func(), error)
	RetainDiscoveryRoot         func(string, error)
	DarwinBestEffortContainment bool
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
	cmd       *exec.Cmd
	xdg       XDGDirs
	log       *slog.Logger
	lease     ServerLease
	leasePath string

	events chan TurnEvent
	errs   chan error
	closed chan struct{}
	once   sync.Once

	gateway               *Client
	process               *Process
	gatewayMu             sync.Mutex
	liveByStored          map[string]string
	storedByLive          map[string]string
	cwd                   string
	defaultModel          string
	providerAuthSupported bool
	sharedSessionOwner    *SharedSessionOwner
	sharedHomeOwner       *SharedHomeOwner

	connMu   sync.Mutex
	turnBusy int
	turnIdle *sync.Cond
	redial   func(context.Context) (*Client, error)

	supervisorWG  sync.WaitGroup
	afterTurnIdle func()
}

func (s *hermesServer) ProviderDescendantCount() (int, bool) {
	if s == nil || s.process == nil {
		return 0, false
	}

	return s.process.ProviderDescendantCount()
}

func (s *hermesServer) ProviderTreeVacant() (bool, bool) {
	if s == nil || s.process == nil {
		return false, false
	}

	return s.process.ProviderTreeVacant()
}

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
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Properties  json.RawMessage `json:"properties"`
	Raw         json.RawMessage `json:"-"`
	StreamEpoch uint64          `json:"-"`
}

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
	ID        string         `json:"id"`
	SessionID string         `json:"sessionID"`
	Action    string         `json:"action"`
	Metadata  map[string]any `json:"metadata"`
	Tool      permissionTool `json:"tool"`
}

type permissionTool struct {
	MessageID string `json:"messageID"`
	CallID    string `json:"callID"`
}

type QuestionRequest struct {
	ID        string         `json:"id"`
	SessionID string         `json:"sessionID"`
	Questions []QuestionInfo `json:"questions"`
	Tool      QuestionTool   `json:"tool"`
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
	ID        string `json:"id"`
	Name      string `json:"name"`
	Reasoning bool   `json:"reasoning"`
}

type ProcessIdentity struct {
	StartTime string
	Cmdline   []string
	Env       map[string]string
}

var (
	hermesMarshalIndent = json.MarshalIndent
	hermesUnmarshalYAML = yaml.Unmarshal
	hermesWriteLease    = WriteLease
	hermesReapLeaseFile = ReapLeaseFile
	hermesControlMkdir  = os.MkdirAll
	hermesControlChmod  = os.Chmod
	hermesNativeHandoff = handoffGeneratedNativeTree
	InspectProcess      = inspectHermesProcess
)

func StartServer(ctx context.Context, options StartOptions) (_ Server, resultErr error) {
	if options.AcquireDiscoveryResources == nil || options.RetainDiscoveryRoot == nil {
		return nil, errors.New("hermes version discovery resource callbacks are required")
	}

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
				if errors.Is(resultErr, ErrProcessContainmentIncomplete) {
					homeOwner.Retain()
				} else {
					resultErr = errors.Join(resultErr, homeOwner.Release())
				}
			}
		}()

		sessionOwner, ownerErr = acquireSharedACPSessionOwner(nativeXDG.Root, options.ACPSessionID)
		if ownerErr != nil {
			return nil, ownerErr
		}

		defer func() {
			if !keepOwners {
				if errors.Is(resultErr, ErrProcessContainmentIncomplete) {
					retainSharedSessionOwner(sessionOwner)
				} else {
					resultErr = errors.Join(resultErr, sessionOwner.Release())
				}
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

	leasePath := filepath.Join(controlDir, LeaseFileName)

	// A server owns exactly one session XDG root. Recover only a predecessor
	// that owned this same root: sweeping root/* here would treat every other
	// live session in the shared agent home as stale and terminate its process.
	if retained := hermesReapLeaseFile(leasePath, options.Logger); retained {
		return nil, fmt.Errorf("previous Hermes process for %q remains live", xdg.Root)
	}

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

	if ownershipErr := hermesNativeHandoff(nativeXDG.Root, options.Isolation); ownershipErr != nil {
		observeHermesStartupStage(ctx, options.ObserveStartupStage, "session", "configuration", configurationStarted, ownershipErr)

		return nil, ownershipErr
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
		ExecutablePath:  options.ExecutablePath,
		Home:            nativeXDG.Root,
		ContainmentRoot: xdg.Root,
		SharedHome:      options.SharedHermesHome != "",
		PrepareSharedHome: func(prepareCtx context.Context, home string) error {
			return materializeSharedHermesConfig(prepareCtx, home, servers, options.SeedFiles)
		},
		SharedSessionOwners:         []*SharedSessionOwner{sessionOwner, options.SharedNativeSessionOwner},
		SharedHomeOwner:             homeOwner,
		ScratchParent:               options.ScratchParent,
		Cwd:                         options.Cwd,
		Env:                         cloneEnvironmentMap(options.Env),
		SessionEnv:                  processEnv,
		ExtraPathDirs:               extraPathDirs,
		Isolation:                   options.Isolation,
		AmbientEnvironment:          options.AmbientEnvironment,
		Timeout:                     options.HealthTimeout,
		Configure:                   configureHermesProcess,
		ObserveStartupStage:         options.ObserveStartupStage,
		AcquireDiscoveryResources:   options.AcquireDiscoveryResources,
		RetainDiscoveryRoot:         options.RetainDiscoveryRoot,
		DarwinBestEffortContainment: options.DarwinBestEffortContainment,
	})
	if err != nil {
		return nil, err
	}

	lease := ServerLease{
		PID:       proc.Cmd.Process.Pid,
		Port:      proc.Port,
		StartedAt: time.Now().UnixMilli(),
		TokenHash: PasswordHash(proc.Token),
		XDGRoot:   nativeXDG.Root,
	}
	if identity, err := InspectProcess(proc.Cmd.Process.Pid); err == nil {
		lease.ProcessStartTime = identity.StartTime
	}

	if err := hermesWriteLease(controlDir, lease); err != nil {
		closeErr := proc.Close(context.Background())

		return nil, errors.Join(err, closeErr)
	}

	server := &hermesServer{
		cmd:                   proc.Cmd,
		xdg:                   xdg,
		log:                   options.Logger,
		lease:                 lease,
		leasePath:             leasePath,
		events:                make(chan TurnEvent, 256),
		errs:                  make(chan error, 8),
		closed:                make(chan struct{}),
		gateway:               proc.Client,
		process:               proc,
		liveByStored:          make(map[string]string),
		storedByLive:          make(map[string]string),
		cwd:                   options.Cwd,
		defaultModel:          options.DefaultModel,
		providerAuthSupported: options.SharedHermesHome != "",
		sharedSessionOwner:    sessionOwner,
		sharedHomeOwner:       homeOwner,
	}
	server.enableReconnect(proc.Redial)

	keepOwners = true

	return server, nil
}

// SharedSessionOwnerProcessIdentity returns the exact process identity already
// owned by this Server so an adapter-created native-session claim can be bound
// immediately after official Hermes allocates its native ID.
func (s *hermesServer) SharedSessionOwnerProcessIdentity() (int, string, error) {
	if s == nil || s.process == nil || s.process.Cmd == nil || s.process.Cmd.Process == nil {
		return 0, "", errors.New("hermes server has no native process identity")
	}

	pid := s.process.Cmd.Process.Pid

	startTime, err := inspectHermesProcessStartTime(pid)
	if err != nil {
		return 0, "", err
	}

	return pid, startTime, nil
}

func observeHermesStartupStage(ctx context.Context, observe func(context.Context, string, string, time.Duration, error), lifecycle, stage string, started time.Time, err error) {
	if observe != nil {
		observe(ctx, lifecycle, stage, time.Since(started), err)
	}
}

func (s *hermesServer) Close(ctx context.Context) error {
	var err error

	s.once.Do(func() {
		close(s.closed)
		s.connMu.Lock()
		if s.turnIdle != nil {
			s.turnIdle.Broadcast()
		}
		s.connMu.Unlock()

		if gw := s.gatewayClient(); gw != nil {
			gwErr := gw.Close(1000, "closing")
			if s.process == nil {
				err = gwErr
			}
		}

		var processErr error
		if s.process != nil {
			processErr = s.process.Close(ctx)
			err = processErr
		}

		s.supervisorWG.Wait()

		if s.leasePath != "" {
			err = errors.Join(err, removeLeaseFileIfOwned(s.leasePath, s.lease))
		}

		// An unproven containment result means descendants may still be writing
		// this residence. Both claims are retained rather than merely left
		// unreleased: an unreachable owner has its descriptor closed by the
		// *os.File finalizer, which would drop the kernel lock silently.
		if errors.Is(processErr, ErrProcessContainmentIncomplete) {
			s.sharedSessionOwner.Retain()
			s.sharedHomeOwner.Retain()
		} else {
			err = errors.Join(err, s.sharedSessionOwner.Release(), s.sharedHomeOwner.Release())
		}
	})

	return err
}

// removeLeaseFileIfOwned removes path only when its immutable contents still
// identify the closing server. A same-session replacement writes a new lease
// before the predecessor object can be closed, and the predecessor must never
// unlink that replacement's live ownership record.
func removeLeaseFileIfOwned(path string, owner ServerLease) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return err
	}

	var current ServerLease
	if err := json.Unmarshal(data, &current); err != nil {
		return err
	}

	if current != owner {
		return nil
	}

	return os.Remove(path)
}

func (s *hermesServer) Events() <-chan TurnEvent {
	return s.events
}

func (s *hermesServer) EventErrors() <-chan error {
	return s.errs
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

// gatewayEventFailure maps a session.error gateway event to a provider turn
// failure, accepting either a nested {error:{…}} object or flat error fields.
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

// gatewayDisconnectCause recovers the real transport error the read loop parked
// on the gateway error channel before it closed. It falls back to the stream
// closed sentinel when the connection ended without a specific error (a clean
// close), so the turn never surfaces a bare or generic disconnect string.
func gatewayDisconnectCause(gw *Client) error {
	select {
	case err, ok := <-gw.Errors():
		if ok && err != nil {
			return err
		}
	default:
	}

	return errGatewayStreamClosed
}

// gatewayClient returns the current live gateway client. A reconnect can swap
// it, so all callers read it through this accessor under connMu.
func (s *hermesServer) gatewayClient() *Client {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	return s.gateway
}

// enableReconnect wires the idle-reconnect supervisor: it records the redial
// function and starts a goroutine that watches the connection and redials while
// no turn is in progress.
func (s *hermesServer) enableReconnect(redial func(context.Context) (*Client, error)) {
	s.connMu.Lock()

	s.redial = redial
	if s.turnIdle == nil {
		s.turnIdle = sync.NewCond(&s.connMu)
	}

	s.supervisorWG.Add(1)
	s.connMu.Unlock()

	go func() {
		defer s.supervisorWG.Done()

		s.superviseGateway()
	}()
}

func (s *hermesServer) beginGatewayTurn() {
	s.connMu.Lock()
	s.turnBusy++
	s.connMu.Unlock()
}

func (s *hermesServer) endGatewayTurn() {
	s.connMu.Lock()
	if s.turnBusy > 0 {
		s.turnBusy--
	}

	cond := s.turnIdle
	s.connMu.Unlock()

	if cond != nil {
		cond.Broadcast()
	}
}

// superviseGateway watches the live connection and, on an idle disconnect,
// redials the still-running `hermes serve` process so the next turn reconnects
// instead of failing. A mid-turn disconnect is fenced by the prompt loop; the
// supervisor waits for the turn to finish before reconnecting.
func (s *hermesServer) superviseGateway() {
	for {
		gw := s.gatewayClient()
		select {
		case <-s.closed:
			return
		case <-gw.Done():
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
			s.log.Debug("reconnect hermes gateway failed", slog.String(jsonFieldError, err.Error()))
		}

		leaseReapSleep(LeaseReapPollInterval)

		return
	}

	s.connMu.Lock()

	closed := s.serverClosed()
	if !closed {
		s.gateway = client
	}
	s.connMu.Unlock()

	if closed {
		// The server shut down while redialing; discard the new connection.
		_ = client.Close(1000, "closing")

		return
	}
	// Live session ids are runtime-only; force re-resume against the new
	// connection on next use.
	s.gatewayMu.Lock()
	s.liveByStored = map[string]string{}
	s.storedByLive = map[string]string{}
	s.gatewayMu.Unlock()
}

func (s *hermesServer) CreateSession(ctx context.Context, title string) (Session, error) {
	return s.CreateSessionWithDraft(ctx, title, nil)
}

func (s *hermesServer) CreateSessionWithDraft(
	ctx context.Context,
	title string,
	bindDraft func(SessionDraft) error,
) (Session, error) {
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

	result, err := s.gatewayClient().CreateSession(ctx, params)
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
			closeErr := s.gatewayClient().CloseSession(cleanupCtx, result.SessionID)

			cleanupCancel()

			if IsNotFound(closeErr) {
				closeErr = nil
			}

			return Session{}, errors.Join(fmt.Errorf("bind Hermes session draft: %w", bindErr), closeErr)
		}
	}

	s.rememberGatewaySession(result.StoredSessionID, result.SessionID)

	// Hermes session.create intentionally leaves a draft only in the live
	// gateway. session.title is the native persistence boundary for an otherwise
	// empty session: it creates the state.db row synchronously. The ACP session
	// store snapshots immediately after this method returns, so returning before
	// that row exists would publish an idmap whose native session cannot be
	// resumed after an interrupt or process restart.
	durableTitle := firstNonEmpty(title, "Hermes session")

	titleResult, err := s.gatewayClient().SetSessionTitle(ctx, result.SessionID, durableTitle)
	if err != nil {
		s.forgetGatewaySession(result.StoredSessionID)

		return Session{}, fmt.Errorf("persist Hermes session: %w", err)
	}

	if titleResult.Pending {
		s.forgetGatewaySession(result.StoredSessionID)

		return Session{}, fmt.Errorf("persist Hermes session: session.title remained pending")
	}

	if titleResult.Title != durableTitle {
		s.forgetGatewaySession(result.StoredSessionID)

		return Session{}, fmt.Errorf("persist Hermes session: session.title response missing durable title")
	}

	persisted, err := s.PersistedSessions(ctx)
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
	storedID := id
	if s.liveSessionID(id) == "" {
		active, err := s.gatewayClient().ActiveList(ctx)
		if err == nil {
			for _, item := range active.Sessions {
				if item.SessionID == "" {
					return Session{}, fmt.Errorf("hermes active_list response missing id")
				}

				if item.SessionKey == "" {
					return Session{}, fmt.Errorf("hermes active_list response missing session_key for live session %q", item.SessionID)
				}

				s.rememberGatewaySession(item.SessionKey, item.SessionID)

				if item.SessionKey == id {
					return s.nativeSessionFromGateway(id, item.Title), nil
				}
			}
		}

		result, err := s.resumeGatewaySession(ctx, id)
		if err != nil {
			return Session{}, err
		}

		stored, err := s.storedSessionIDFromResume(result)
		if err != nil {
			return Session{}, err
		}

		s.rememberGatewaySession(stored, result.SessionID)
		storedID = stored
	}

	return s.nativeSessionFromGateway(storedID, ""), nil
}

func (s *hermesServer) ListSessions(ctx context.Context, cwd string) ([]Session, error) {
	active, err := s.gatewayClient().ActiveList(ctx)
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

		s.rememberGatewaySession(item.SessionKey, item.SessionID)
		session := s.nativeSessionFromGateway(item.SessionKey, item.Title)

		session.Directory = firstNonEmpty(item.Cwd, s.cwd)
		if cwd == "" || session.Directory == cwd {
			out = append(out, session)
		}
	}

	return out, nil
}

func (s *hermesServer) PersistedSessions(ctx context.Context) ([]Session, error) {
	result, err := s.gatewayClient().PersistedSessions(ctx)
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
	live := s.liveSessionID(id)
	if live == "" {
		active, listErr := s.gatewayClient().ActiveList(ctx)
		if listErr != nil && !IsNotFound(listErr) {
			return fmt.Errorf("list live Hermes sessions before delete: %w", listErr)
		}

		for _, item := range active.Sessions {
			if item.SessionKey == id {
				if item.SessionID == "" {
					return fmt.Errorf("hermes active_list response missing id for stored session %q", id)
				}

				live = item.SessionID
				s.rememberGatewaySession(id, live)

				break
			}
		}
	}

	if live != "" {
		closeErr := s.gatewayClient().CloseSession(ctx, live)
		if closeErr != nil && !IsNotFound(closeErr) {
			return fmt.Errorf("close Hermes session before delete: %w", closeErr)
		}
	}

	s.forgetGatewaySession(id)

	err := s.gatewayClient().DeleteSession(ctx, id)
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
	for attempt := 0; ; attempt++ {
		live, err := s.ensureLiveGatewaySession(ctx, id)
		if err != nil {
			return err
		}

		var result struct {
			Status string `json:"status"`
		}

		err = s.gatewayClient().Call(ctx, "reload.mcp", map[string]any{
			keySessionIDSnake: live,
			"confirm":         true,
		}, &result)
		if IsNotFound(err) && attempt == 0 {
			// A reconnect or native reload can retire the runtime-only live id.
			// Forget only that routing cache entry, resume the durable key once,
			// and repeat the idempotent reload against the rebound live session.
			s.forgetGatewaySession(id)

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
	if req.Model != nil {
		if err := s.SetModel(ctx, id, ModelSelectionValue(req.Model.ProviderID, req.Model.ModelID)); err != nil {
			return NativeMessage{}, err
		}
	}

	return s.submitGatewayParts(ctx, id, req.Parts)
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

	result, err := s.resumeGatewaySession(ctx, stored)
	if err != nil {
		return "", err
	}

	resolvedStored, err := s.storedSessionIDFromResume(result)
	if err != nil {
		return "", err
	}

	s.rememberGatewaySession(resolvedStored, result.SessionID)

	return result.SessionID, nil
}

// resumeGatewaySession asks official Hermes to finish constructing the native
// agent before publishing its live id. Hermes 0.20 otherwise returns from a
// cold resume while a background build is still pending; a config.set sent in
// that window can report success and then be overwritten by the stale build.
func (s *hermesServer) resumeGatewaySession(ctx context.Context, stored string) (SessionResumeResult, error) {
	return s.gatewayClient().ResumeSession(ctx, stored, map[string]any{keyEagerBuild: true})
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
	attachments, err := imageAttachmentsFromHermesParts(parts)
	if err != nil {
		return NativeMessage{}, err
	}

	for attempt := 0; ; attempt++ {
		live, liveErr := s.ensureLiveGatewaySession(ctx, stored)
		if liveErr != nil {
			return NativeMessage{}, liveErr
		}

		message, submitErr := s.submitGatewayTextForLive(ctx, stored, live, textFromHermesParts(parts), attachments)
		if IsNotFound(submitErr) && attempt == 0 {
			// A 4007 RPC response means Hermes rejected the prompt before
			// admission. Re-resume the durable session and retry exactly once;
			// errors after admission are event/transport failures and never enter
			// this branch, so a model turn cannot be duplicated.
			s.forgetGatewaySession(stored)

			continue
		}

		return message, submitErr
	}
}

func (s *hermesServer) submitGatewayTextForLive(
	ctx context.Context,
	stored string,
	live string,
	text string,
	attachments [][]byte,
) (NativeMessage, error) {
	s.beginGatewayTurn()
	defer s.endGatewayTurn()

	gw := s.gatewayClient()
	for _, attachment := range attachments {
		if err := gw.AttachImageBytes(ctx, live, attachment); err != nil {
			return NativeMessage{}, err
		}
	}

	messageID := "hermes-" + live
	if err := gw.SubmitPrompt(ctx, live, text); err != nil {
		return NativeMessage{}, err
	}

	// The gateway acknowledged the frame, so it owns this turn from here. The
	// dispatch point is reported before the loop below forwards anything the
	// frame causes, which is what lets a caller record acceptance ahead of every
	// event attributed to it.
	if err := NotifyPromptDispatch(ctx); err != nil {
		return NativeMessage{}, err
	}

	var textBuilder strings.Builder

	activeToolCalls := map[string]struct{}{}
	toolCallStates := map[string]gatewayActiveTool{}
	toolParts := make([]Part, 0)

	for {
		select {
		case event, ok := <-gw.Events():
			if !ok {
				// The event channel closes when the gateway connection
				// terminates; fence the turn as a disconnect carrying the real
				// transport cause the read loop parked before closing.
				return NativeMessage{}, s.reportGatewayDisconnect(gatewayDisconnectCause(gw))
			}

			if event.SessionID != "" && event.SessionID != live {
				continue
			}

			switch event.Type {
			case evtApprovalRequest:
				if err := s.forwardGatewayPermission(ctx, stored, live, messageID, uniqueGatewayToolCallID(activeToolCalls), event); err != nil {
					return NativeMessage{}, err
				}
			case evtClarifyRequest:
				if err := s.forwardGatewayQuestion(ctx, stored, live, event); err != nil {
					return NativeMessage{}, err
				}
			case evtTerminalReadReq, evtSudoRequest, evtSecretRequest:
				s.declineGatewayQuestion(ctx, live, event.Type)
			case evtSessionError:
				return NativeMessage{}, gatewayEventFailure(event.Payload)
			case evtToolStart:
				if part, ok := gatewayToolPart(stored, messageID, event, gatewayActiveTool{}); ok {
					activeToolCalls[part.CallID] = struct{}{}

					toolCallStates[part.CallID] = gatewayActiveTool{
						rawInput: append(json.RawMessage(nil), event.Payload...),
						name:     part.Tool,
					}

					toolParts = append(toolParts, part)
					if err := s.forwardGatewayToolPart(ctx, part, event); err != nil {
						return NativeMessage{}, err
					}
				}
			case evtToolComplete:
				toolCallID := gatewayToolCallID(event.Payload)
				if part, ok := gatewayToolPart(stored, messageID, event, toolCallStates[toolCallID]); ok {
					delete(activeToolCalls, part.CallID)
					delete(toolCallStates, part.CallID)

					toolParts = append(toolParts, part)
					if err := s.forwardGatewayToolPart(ctx, part, event); err != nil {
						return NativeMessage{}, err
					}
				}
			case evtMessageDelta, evtThinkingDelta:
				chunk := gatewayEventText(event.Payload)
				if chunk == "" {
					continue
				}

				if event.Type == evtMessageDelta {
					textBuilder.WriteString(chunk)
				}

				if err := s.forwardGatewayPart(ctx, stored, messageID, event, chunk); err != nil {
					return NativeMessage{}, err
				}
			case evtMessageComplete:
				if failure := gatewayCompleteFailure(event.Payload); failure != nil {
					return NativeMessage{}, failure
				}

				tokens := gatewayUsageTokens(event.Payload)
				streamedText := textBuilder.String()
				completeText := gatewayCompleteText(event.Payload)

				if completeText == "" {
					completeText = streamedText
				}

				parts := append([]Part(nil), toolParts...)
				parts = append(parts, Part{
					ID:           messageID + "-text",
					SessionID:    stored,
					MessageID:    messageID,
					Type:         valText,
					Text:         completeText,
					StreamedText: streamedText,
				})

				return NativeMessage{
					Info: NativeMessageInfo{
						ID:            messageID,
						SessionID:     stored,
						Role:          valAssistant,
						Finish:        valStop,
						Tokens:        tokens,
						ContextWindow: gatewayContextWindow(event.Payload),
					},
					Parts: parts,
				}, nil
			}
		case <-ctx.Done():
			return NativeMessage{}, ctx.Err()
		}
	}
}

// reportGatewayDisconnect feeds the real mid-turn disconnect cause into the
// server error channel that the prompt loop watches via EventErrors and returns
// a transport turn failure carrying that same cause. The failure unwraps to
// ErrGatewayDisconnected so the prompt loop fences the stream exactly once,
// while data.message reports the real cause instead of a generic string.
func (s *hermesServer) reportGatewayDisconnect(cause error) error {
	if cause == nil {
		cause = errGatewayStreamClosed
	}

	select {
	case s.errs <- StreamError{err: cause}:
	default:
	}

	return &TurnFailureError{cause: CauseTransport, message: cause.Error(), wrapped: ErrGatewayDisconnected}
}

// forwardGatewayPart hands one assistant text or thinking delta to the turn
// event channel. It blocks on the caller's context rather than discarding the
// delta when the buffer is full: this session's stream advertises delivery
// between prompts and drops nothing, and a silently discarded delta would
// falsify that with no error and no gap for a consumer to notice.
func (s *hermesServer) forwardGatewayPart(ctx context.Context, stored string, messageID string, event Event, text string) error {
	partType := valText
	if event.Type == evtThinkingDelta {
		partType = valReasoning
	}

	part := Part{
		ID:        messageID + "-" + partType,
		SessionID: stored,
		MessageID: messageID,
		Type:      partType,
		Text:      text,
		Raw:       event.Raw,
	}

	data, _ := json.Marshal(part)
	select {
	case s.events <- TurnEvent{Type: evtMessagePartUpdated, Properties: data, Raw: event.Raw}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *hermesServer) forwardGatewayToolPart(ctx context.Context, part Part, event Event) error {
	data := append(json.RawMessage(nil), part.Raw...)
	select {
	case s.events <- TurnEvent{Type: evtMessagePartUpdated, Properties: data, Raw: event.Raw}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

	status := containmentStateRunning
	if event.Type == evtToolComplete {
		status = valCompleted
		if gatewayToolFailed(payload, toolName) {
			status = valFailed
		}
	}

	state := map[string]any{"status": status}

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

func (s *hermesServer) forwardGatewayPermission(
	ctx context.Context,
	stored string,
	live string,
	messageID string,
	toolCallID string,
	event Event,
) error {
	requestID := "approval-unbound"
	if toolCallID != "" {
		requestID = "approval:" + toolCallID
	}

	req := PermissionRequest{
		ID:        requestID,
		SessionID: stored,
		Action:    firstNonEmpty(gatewayPayloadString(event.Payload, "command"), "approval"),
		Metadata:  map[string]any{"liveSessionId": live},
		Tool:      permissionTool{MessageID: messageID, CallID: toolCallID},
	}

	data, _ := json.Marshal(req)
	select {
	case s.events <- TurnEvent{Type: evtApprovalRequest, Properties: data, Raw: event.Raw}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

// forwardGatewayQuestion hands one clarify request to the turn event channel,
// where it becomes an elicitation the host must answer. It blocks on the
// caller's context for the same reason forwardGatewayPart does, and with a
// sharper consequence: a discarded question leaves the native side waiting on
// an answer no host was ever asked for.
func (s *hermesServer) forwardGatewayQuestion(ctx context.Context, stored string, live string, event Event) error {
	question := firstNonEmpty(gatewayPayloadString(event.Payload, keyQuestion), gatewayPayloadString(event.Payload, "prompt"), msgHermesNeedsInput)
	req := QuestionRequest{
		ID:        firstNonEmpty(gatewayPayloadString(event.Payload, "id"), gatewayPayloadString(event.Payload, "request_id"), "clarify"),
		SessionID: stored,
		Questions: []QuestionInfo{{
			Question: question,
			Header:   "Hermes question",
			Custom:   true,
		}},
	}

	data, _ := json.Marshal(req)

	_ = live

	select {
	case s.events <- TurnEvent{Type: evtClarifyRequest, Properties: data, Raw: event.Raw}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *hermesServer) declineGatewayQuestion(ctx context.Context, live string, eventType string) {
	switch eventType {
	case evtTerminalReadReq:
		_ = s.gatewayClient().Call(ctx, "terminal.read.respond", map[string]any{keySessionIDSnake: live, valText: ""}, nil)
	case evtSudoRequest:
		_ = s.gatewayClient().Call(ctx, "sudo.respond", map[string]any{keySessionIDSnake: live, "password": ""}, nil)
	case evtSecretRequest:
		_ = s.gatewayClient().Call(ctx, "secret.respond", map[string]any{keySessionIDSnake: live, keyValue: ""}, nil)
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

	return firstPayloadString(payload, valText, "delta", "content")
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
		text := gatewayMessageText(*message)
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
				Text:      text,
				Raw:       message.Raw,
			}},
		})
	}

	return out
}

func gatewayMessageText(message Message) string {
	if len(message.Content) == 0 {
		return ""
	}

	if out := gatewayEventText(message.Content); out != "" {
		return out
	}

	return string(message.Content)
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

			capability := provider.Capabilities[modelID]
			info.Models[modelID] = ProviderModel{
				ID:        modelID,
				Name:      modelID,
				Reasoning: capability.Reasoning,
			}
		}

		providers = append(providers, info)
	}

	return ProvidersResponse{Providers: providers, Raw: result.Raw}
}

func (s *hermesServer) Messages(ctx context.Context, id string) ([]NativeMessage, error) {
	live, err := s.ensureLiveGatewaySession(ctx, id)
	if err != nil {
		return nil, err
	}

	history, err := s.gatewayClient().History(ctx, live)
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

	return s.gatewayClient().Interrupt(ctx, live)
}

func (s *hermesServer) Fork(ctx context.Context, id string, messageID string) (Session, error) {
	persisted, err := s.PersistedSessions(ctx)
	if err != nil {
		return Session{}, fmt.Errorf("list Hermes sessions before branch: %w", err)
	}

	baseline := make([]string, 0, len(persisted))
	for _, session := range persisted {
		baseline = append(baseline, session.ID)
	}

	return s.ForkWithBaseline(ctx, id, messageID, baseline)
}

func (s *hermesServer) ForkWithBaseline(
	ctx context.Context,
	id string,
	marker string,
	baseline []string,
) (Session, error) {
	live, err := s.ensureLiveGatewaySession(ctx, id)
	if err != nil {
		return Session{}, err
	}

	result, err := s.gatewayClient().Branch(ctx, live, marker)
	if IsNotFound(err) {
		s.forgetGatewaySession(id)

		live, err = s.ensureLiveGatewaySession(ctx, id)
		if err != nil {
			return Session{}, err
		}

		result, err = s.gatewayClient().Branch(ctx, live, marker)
	}

	if err != nil {
		return Session{}, s.recoverFailedBranch(ctx, marker, baseline, err)
	}

	if result.SessionID == "" {
		return Session{}, fmt.Errorf("hermes branch response missing session_id")
	}

	if result.StoredSessionID == "" {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := s.gatewayClient().CloseSession(cleanupCtx, result.SessionID)

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
	closeErr := s.gatewayClient().CloseSession(cleanupCtx, result.SessionID)

	cleanupCancel()

	if IsNotFound(closeErr) {
		closeErr = nil
	}

	s.forgetGatewaySession(result.StoredSessionID)

	if closeErr != nil {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		deleteErr := s.gatewayClient().DeleteSession(deleteCtx, result.StoredSessionID)

		deleteCancel()

		if IsNotFound(deleteErr) {
			deleteErr = nil
		}

		return Session{}, errors.Join(fmt.Errorf("close Hermes branch runtime: %w", closeErr), deleteErr)
	}

	return s.nativeSessionFromGateway(result.StoredSessionID, firstNonEmpty(result.Title, "Hermes branch")), nil
}

func (s *hermesServer) recoverFailedBranch(ctx context.Context, marker string, baseline []string, branchErr error) error {
	persisted, listErr := s.PersistedSessions(ctx)
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

	cleanupErr := s.DeleteSession(cleanupCtx, delta[0].ID)
	if cleanupErr != nil {
		return errors.Join(branchErr, fmt.Errorf("delete failed Hermes branch %q: %w", delta[0].ID, cleanupErr))
	}

	remaining, verifyErr := s.PersistedSessions(cleanupCtx)
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

func (s *hermesServer) lookupStoredSessionIDForLive(ctx context.Context, live string, label string) (string, error) {
	active, err := s.gatewayClient().ActiveList(ctx)
	if err != nil {
		return "", fmt.Errorf("%s active_list lookup failed: %w", label, err)
	}

	for _, item := range active.Sessions {
		if item.SessionID != live {
			continue
		}

		if item.SessionKey == "" {
			return "", fmt.Errorf("%s active_list missing session_key for live session %q", label, live)
		}

		return item.SessionKey, nil
	}

	return "", fmt.Errorf("%s active_list missing live session %q", label, live)
}

func (s *hermesServer) ConfigProviders(ctx context.Context) (ProvidersResponse, error) {
	live := s.anyLiveSessionID()

	models, err := s.gatewayClient().ModelOptions(ctx, live)
	if err != nil {
		return ProvidersResponse{}, err
	}

	return providersFromGateway(models), nil
}

// SetModel applies a session-scoped official gateway model selection.
func (s *hermesServer) SetModel(ctx context.Context, stored string, value string) error {
	for attempt := 0; ; attempt++ {
		live, err := s.ensureLiveGatewaySession(ctx, stored)
		if err != nil {
			return err
		}

		err = s.gatewayClient().SetModel(ctx, live, value)
		if IsNotFound(err) && attempt == 0 {
			s.forgetGatewaySession(stored)

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

	live := s.liveSessionID(req.SessionID)
	if live == "" {
		return MissingLiveSessionMappingError{StoredSessionID: req.SessionID}
	}

	return s.gatewayClient().ApprovalRespond(ctx, live, choice, reply == valAlways)
}

func (s *hermesServer) ReplyQuestion(ctx context.Context, req QuestionRequest, answers [][]string) error {
	live := s.liveSessionID(req.SessionID)
	if live == "" {
		return MissingLiveSessionMappingError{StoredSessionID: req.SessionID}
	}

	return s.gatewayClient().ClarifyRespond(ctx, live, req.ID, answers)
}

func (s *hermesServer) RejectQuestion(ctx context.Context, req QuestionRequest) error {
	live := s.liveSessionID(req.SessionID)
	if live == "" {
		return MissingLiveSessionMappingError{StoredSessionID: req.SessionID}
	}

	return s.gatewayClient().ClarifyRespond(ctx, live, req.ID, "")
}

type StreamError struct {
	epoch uint64
	err   error
}

func (e StreamError) Error() string {
	return e.err.Error()
}

func (e StreamError) Unwrap() error {
	return e.err
}

// NewStreamError builds a gateway stream error carrying the turn epoch it
// belongs to, so a late failure from a superseded turn can be ignored.
func NewStreamError(epoch uint64, err error) StreamError {
	return StreamError{epoch: epoch, err: err}
}

func StreamErrorEpoch(err error) uint64 {
	var streamErr StreamError
	if errors.As(err, &streamErr) {
		return streamErr.epoch
	}

	return 0
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
		if segment == containmentParentPath {
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

func PasswordHash(password string) string {
	sum := sha256.Sum256([]byte(password))

	return hex.EncodeToString(sum[:])
}

func ControlDirForXDG(root string) string {
	return root + ".control"
}

type ServerLease struct {
	PID              int    `json:"pid"`
	Port             int    `json:"port"`
	StartedAt        int64  `json:"startedAtUnixMilli"`
	TokenHash        string `json:"tokenHash"`
	XDGRoot          string `json:"xdgRoot"`
	ProcessStartTime string `json:"processStartTime,omitempty"`
}

func WriteLease(stateDir string, lease ServerLease) error {
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return err
	}

	data, err := hermesMarshalIndent(lease, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(stateDir, LeaseFileName), data, 0o600)
}

var (
	LeaseReapTimeout      = 3 * time.Second
	LeaseReapPollInterval = 20 * time.Millisecond
	leaseReapSleep        = time.Sleep
	leaseReapNow          = time.Now
)

func ReapLeaseFile(path string, log *slog.Logger) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}

	var lease ServerLease
	if err := json.Unmarshal(data, &lease); err != nil {
		_ = os.Remove(path)

		return false
	}

	if lease.PID <= 0 || !leaseMatchesProcess(lease) {
		// Not our identified live process (gone, replaced, or
		// unidentifiable): the lease is safe to remove.
		_ = os.Remove(path)

		return false
	}

	if reapLeaseProcess(lease, log) {
		_ = os.Remove(path)

		return false
	}
	// The process survived termination or could not be verified dead: KEEP
	// the lease so the next startup retries the reap ladder.
	if log != nil {
		log.Debug("stale hermes lease process survived termination; keeping lease", slog.Int("pid", lease.PID))
	}

	return true
}

// reapLeaseProcess runs the shutdown ladder against an identified stale server
// process: signal the group, wait, escalate to SIGKILL, then VERIFY the process
// is gone. It returns true only when the process is confirmed dead.
func reapLeaseProcess(lease ServerLease, log *slog.Logger) bool {
	if err := terminateProcessGroupID(lease.PID); err != nil {
		if log != nil {
			log.Debug("terminate stale hermes lease group failed", slog.Int("pid", lease.PID), slog.String(jsonFieldError, err.Error()))
		}

		return leaseProcessGone(lease)
	}

	if waitLeaseProcessGone(lease) {
		return true
	}

	if err := killProcessGroupID(lease.PID); err != nil {
		if log != nil {
			log.Debug("kill stale hermes lease group failed", slog.Int("pid", lease.PID), slog.String(jsonFieldError, err.Error()))
		}

		return leaseProcessGone(lease)
	}

	return waitLeaseProcessGone(lease)
}

func waitLeaseProcessGone(lease ServerLease) bool {
	deadline := leaseReapNow().Add(LeaseReapTimeout)

	for {
		if leaseProcessGone(lease) {
			return true
		}

		if !leaseReapNow().Before(deadline) {
			return false
		}

		leaseReapSleep(LeaseReapPollInterval)
	}
}

// leaseProcessGone reports whether the leased process no longer exists or was
// replaced by an unrelated process reusing the PID.
func leaseProcessGone(lease ServerLease) bool {
	identity, err := InspectProcess(lease.PID)
	if err != nil {
		return true
	}

	if lease.ProcessStartTime != "" && identity.StartTime != lease.ProcessStartTime {
		return true
	}

	return false
}

func leaseMatchesProcess(lease ServerLease) bool {
	if lease.PID <= 0 || lease.ProcessStartTime == "" {
		return false
	}

	identity, err := InspectProcess(lease.PID)
	if err != nil {
		return false
	}

	if identity.StartTime != lease.ProcessStartTime {
		return false
	}

	if PasswordHash(identity.Env[envHermesSessionToken]) != lease.TokenHash {
		return false
	}

	if filepath.Clean(identity.Env[envHermesHome]) != filepath.Clean(lease.XDGRoot) {
		return false
	}

	return cmdlineLooksLikeHermesServe(identity.Cmdline)
}

func cmdlineLooksLikeHermesServe(args []string) bool {
	for _, arg := range args {
		if arg == valServe {
			return true
		}
	}

	for _, arg := range args {
		if strings.Contains(filepath.Base(arg), valHermes) {
			return true
		}
	}

	return false
}
