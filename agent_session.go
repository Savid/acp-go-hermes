package hermesacp

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	limitActiveSessions = "active_sessions"
	limitSessionRestore = "session_restore"
)

// sessionStart carries the validated inputs of one session-establishing
// request.
type sessionStart struct {
	cwd                   string
	additionalDirectories []string
	meta                  sessionMeta
}

func (a *Agent) validateStart(params sessionStart, mcpServers []acp.McpServer, meta map[string]any) (sessionStart, error) {
	if a.optionErr != nil {
		return sessionStart{}, a.optionErr
	}

	parsed, err := parseSessionMeta(meta)
	if err != nil {
		return sessionStart{}, err
	}

	if !filepath.IsAbs(params.cwd) {
		return sessionStart{}, wire.Unsupported(fieldCwd)
	}

	for index, dir := range params.additionalDirectories {
		if !filepath.IsAbs(dir) {
			return sessionStart{}, wire.Unsupported("additionalDirectories[" + strconv.Itoa(index) + "]")
		}
	}

	if len(mcpServers) > 0 {
		return sessionStart{}, wire.Unsupported("mcpServers")
	}

	if err := a.ensureOpen(); err != nil {
		return sessionStart{}, err
	}

	params.meta = parsed

	return params, nil
}

// newSession builds the session value for one establishing request; the
// native identity arrives once hermes reports it.
func (a *Agent) newSession(start sessionStart) *session {
	s := &session{
		agent:                 a,
		cwd:                   start.cwd,
		additionalDirectories: slices.Clone(start.additionalDirectories),
		options:               start.meta.options.clone(),
		gate:                  make(chan struct{}, 1),
	}

	// A malformed environment is reported when launch builds it again; the
	// agent directory only needs whatever resolved here.
	env, _ := a.environment(s.options.Env, nil).Build()
	s.agentDir = hermes.AgentDir(a.options.Home, func(key string) (string, bool) { return process.Lookup(env, key) })

	if !filepath.IsAbs(s.agentDir) {
		s.agentDir = filepath.Join(s.cwd, s.agentDir)
	}

	return s
}

// install publishes a configured session under its ACP id.
func (a *Agent) install(ctx context.Context, s *session) error {
	a.mu.Lock()

	var refusal error

	switch {
	case a.closed:
		refusal = wire.AgentClosed()
	case isDeleted(a.deleted, s.id):
		refusal = wire.UnknownSession()
	case a.sessions[s.id] != nil:
		refusal = wire.InternalFailure(vendor, internalClassNativeStart)
	case len(a.sessions) >= a.options.ConcurrencyLimits.MaxActiveSessions:
		refusal = wire.Backpressure(limitActiveSessions)
	}

	if refusal == nil {
		a.sessions[s.id] = s
	}
	a.mu.Unlock()

	if refusal != nil {
		_ = s.close(ctx)

		return refusal
	}

	a.observe.AddActiveSession(ctx, 1)

	return nil
}

func isDeleted(deleted map[acp.SessionId]struct{}, id acp.SessionId) bool {
	_, ok := deleted[id]

	return ok
}

// publishOpen opens the session's lifecycle stream once its establishing
// response is on the wire, then releases the runtime's event binding so the
// pump starts projecting native records.
func (s *session) publishOpen(ctx context.Context) error {
	if err := s.openStream(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	if rt != nil {
		rt.bindOnce.Do(func() { close(rt.bound) })
	}

	return nil
}

// scheduleOpen defers the opening publication behind the establishing
// response on a served connection, and runs it inline for an embedded host.
func (a *Agent) scheduleOpen(ctx context.Context, s *session) error {
	if t := a.transportRef(); t != nil {
		return t.RegisterHook(ctx, s.id, func(hookCtx context.Context) {
			if err := s.publishOpen(hookCtx); err != nil {
				a.log.ErrorContext(hookCtx, "publish session open failed",
					slog.String(nativeSessionIDKey, string(s.id)), slog.String("reason", err.Error()))
			}
		})
	}

	return s.publishOpen(ctx)
}

// NewSession creates and starts a hermes session.
func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (resp acp.NewSessionResponse, err error) {
	if transport := a.transportRef(); transport != nil {
		ctx = transport.RequestContext(ctx, params.Meta)
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionNew)
	defer func() { finish(err) }()

	start, err := a.validateStart(sessionStart{cwd: params.Cwd, additionalDirectories: params.AdditionalDirectories}, params.McpServers, params.Meta)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	s := a.newSession(start)

	rt, err := s.launch(ctx)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	model := s.options.Model
	if model == "" {
		model = a.options.DefaultModel
	}

	if err := s.configureRuntime(ctx, rt, model, ""); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return acp.NewSessionResponse{}, err
	}

	s.rawEvents = wire.NewRawEvents(vendor, string(s.id), vendor, start.meta.rawEvents)

	release, _ := wire.AcquireSessionGate(s.gate, limitSessionRestore)
	defer release()

	if err := a.install(ctx, s); err != nil {
		return acp.NewSessionResponse{}, err
	}

	if err := s.commitMirror(ctx, rt); err != nil {
		a.log.ErrorContext(ctx, "initial mirror commit failed",
			slog.String(nativeSessionIDKey, string(s.id)), slog.String("reason", err.Error()))
		_ = s.close(context.WithoutCancel(ctx))
		a.detach(ctx, s)

		return acp.NewSessionResponse{}, wire.InternalFailure(vendor, "")
	}

	if err := a.scheduleOpen(ctx, s); err != nil {
		return acp.NewSessionResponse{}, err
	}

	return acp.NewSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.nativeID), SessionId: s.id, ConfigOptions: s.configOptions()}, nil
}

// LoadSession restores a session and replays its history.
func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (resp acp.LoadSessionResponse, err error) {
	if transport := a.transportRef(); transport != nil {
		ctx = transport.RequestContext(ctx, params.Meta)
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionLoad)
	defer func() { finish(err) }()

	s, release, err := a.restore(ctx, params.SessionId, sessionStart{cwd: params.Cwd, additionalDirectories: params.AdditionalDirectories}, params.McpServers, params.Meta, true)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	defer release()

	return acp.LoadSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.nativeID), ConfigOptions: s.configOptions()}, nil
}

// ResumeSession restores a session without replaying its history.
func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (resp acp.ResumeSessionResponse, err error) {
	if transport := a.transportRef(); transport != nil {
		ctx = transport.RequestContext(ctx, params.Meta)
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionResume)
	defer func() { finish(err) }()

	s, release, err := a.restore(ctx, params.SessionId, sessionStart{cwd: params.Cwd, additionalDirectories: params.AdditionalDirectories}, params.McpServers, params.Meta, false)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	defer release()

	return acp.ResumeSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.nativeID), ConfigOptions: s.configOptions()}, nil
}

// restore is the shared load and resume path. A live session whose carrier
// matches is reused; a changed carrier closes it and re-prepares from the
// store; a cold session is hydrated from the store and started.
func (a *Agent) restore(
	ctx context.Context,
	sessionID acp.SessionId,
	params sessionStart,
	mcpServers []acp.McpServer,
	meta map[string]any,
	replay bool,
) (*session, func(), error) {
	start, err := a.validateStart(params, mcpServers, meta)
	if err != nil {
		return nil, nil, err
	}

	releaseRestore, err := a.restores.Acquire(sessionID)
	if err != nil {
		return nil, nil, err
	}
	defer releaseRestore()

	a.mu.Lock()
	deleted := isDeleted(a.deleted, sessionID)
	active := a.sessions[sessionID]
	a.mu.Unlock()

	if deleted {
		return nil, nil, wire.UnknownSession()
	}

	if active != nil {
		release, gateErr := active.acquireGate(limitSessionRestore)
		if gateErr != nil {
			return nil, nil, gateErr
		}

		if sameCarrier(active, start) {
			return a.restoreActive(ctx, active, replay, release)
		}

		closeErr := active.close(ctx)

		release()

		a.detach(ctx, active)

		if closeErr != nil {
			return nil, nil, wire.RestoreFailed(vendor)
		}
	}

	stored, err := a.loadStored(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}

	if !stored.found {
		return nil, nil, wire.UnknownSession()
	}

	start.meta.options = inheritCarrier(start.meta, stored.record)
	if start.additionalDirectories == nil {
		start.additionalDirectories = slices.Clone(stored.record.AdditionalDirectories)
	}

	s := a.newSession(start)
	s.id = sessionID
	s.nativeID = stored.record.NativeSessionID
	s.rawEvents = wire.NewRawEvents(vendor, string(sessionID), vendor, start.meta.rawEvents)
	s.title = storedTitle(stored.record.NativeSessionID, stored.rows)

	rt, err := s.launch(ctx)
	if err != nil {
		return nil, nil, err
	}

	rows, err := s.hydrate(ctx, rt, stored)
	if err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, nil, err
	}

	if err := s.configureRuntime(ctx, rt, s.options.Model, s.nativeID); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, nil, err
	}

	release, _ := wire.AcquireSessionGate(s.gate, limitSessionRestore)

	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	if err := a.install(ctx, s); err != nil {
		return nil, nil, err
	}

	if err := s.commitMirror(ctx, rt); err != nil {
		_ = s.close(context.WithoutCancel(ctx))
		a.detach(ctx, s)

		return nil, nil, a.restoreRefused(ctx, sessionID, err)
	}

	if replay {
		if err := s.replay(ctx, rows); err != nil {
			_ = s.close(context.WithoutCancel(ctx))
			a.detach(ctx, s)

			return nil, nil, err
		}
	}

	if err := a.scheduleOpen(ctx, s); err != nil {
		return nil, nil, err
	}

	transferred = true

	return s, release, nil
}

// restoreActive answers a load or resume from a session that is already
// live, holding its foreground for the replay.
func (a *Agent) restoreActive(ctx context.Context, s *session, replay bool, release func()) (*session, func(), error) {
	if err := s.admissionError(); err != nil {
		release()

		return nil, nil, err
	}

	if !replay {
		return s, release, nil
	}

	stored, err := a.loadStored(ctx, s.id)
	if err != nil {
		release()

		return nil, nil, err
	}

	if err := s.replay(ctx, stored.rows); err != nil {
		release()

		return nil, nil, err
	}

	return s, release, nil
}

// sameCarrier reports whether a restore names the configuration the live
// session already runs under. A field the request omits inherits.
func sameCarrier(s *session, start sessionStart) bool {
	record := s.record()
	if record.Cwd != start.cwd {
		return false
	}

	if start.additionalDirectories != nil && !slices.Equal(record.AdditionalDirectories, start.additionalDirectories) {
		return false
	}

	current := inheritCarrier(sessionMeta{}, record)
	requested := inheritCarrier(start.meta, record)

	return reflect.DeepEqual(current.Meta(), requested.Meta())
}

// inheritCarrier fills the fields a restore omitted from the stored record.
func inheritCarrier(meta sessionMeta, record sessionRecord) HermesOptions {
	options := meta.options.clone()

	if !meta.presentEnv {
		options.Env = maps.Clone(record.Env)
	}

	if !meta.presentExtraPathDirs {
		options.ExtraPathDirs = slices.Clone(record.ExtraPathDirs)
	}

	if options.Model == "" {
		options.Model = record.Model
	}

	if options.Effort == "" {
		options.Effort = record.Effort
	}

	return options
}

// ListSessions lists live sessions and stored sessions, newest first.
func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (resp acp.ListSessionsResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionList)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.ListSessionsResponse{}, wire.ParamRefusal(refusal)
	}

	if openErr := a.ensureOpen(); openErr != nil {
		return acp.ListSessionsResponse{}, openErr
	}

	filter := ""
	if params.Cwd != nil {
		filter = strings.TrimSpace(*params.Cwd)
	}

	if filter != "" && !filepath.IsAbs(filter) {
		return acp.ListSessionsResponse{}, wire.Unsupported(fieldCwd)
	}

	a.mu.Lock()
	active := make([]*session, 0, len(a.sessions))

	for id, s := range a.sessions {
		if !isDeleted(a.deleted, id) && (filter == "" || filter == s.cwd) {
			active = append(active, s)
		}
	}
	a.mu.Unlock()

	sessions := make([]acp.SessionInfo, 0, len(active))
	seen := make(map[acp.SessionId]struct{}, len(active))

	for _, s := range active {
		sessions = append(sessions, s.sessionInfo())
		seen[s.id] = struct{}{}
	}

	listCtx, cancel := context.WithTimeout(ctx, a.options.SessionStoreLoadTimeout)
	defer cancel()

	listCtx, finishList := a.observe.StartSessionStore(listCtx, "list")
	summaries, err := a.store.ListSessions(listCtx)
	finishList(err)

	if err != nil {
		return acp.ListSessionsResponse{}, wire.InternalFailure(vendor, "")
	}

	for _, summary := range summaries {
		id := acp.SessionId(summary.SessionID)
		if _, ok := seen[id]; ok || a.deletedSession(id) {
			continue
		}

		stored, loadErr := a.loadStored(ctx, id)
		if loadErr != nil {
			return acp.ListSessionsResponse{}, wire.InternalFailure(vendor, "")
		}

		cwd := stored.record.Cwd

		if filter != "" && cwd != filter {
			continue
		}

		title := storedTitle(stored.record.NativeSessionID, stored.rows)
		updatedAt := time.UnixMilli(summary.UpdatedAtUnixMilli).UTC().Format(time.RFC3339)

		sessions = append(sessions, acp.SessionInfo{
			Meta:                  wire.NativeSessionMeta(vendor, stored.record.NativeSessionID),
			SessionId:             id,
			Cwd:                   cwd,
			AdditionalDirectories: slices.Clone(stored.record.AdditionalDirectories),
			Title:                 &title,
			UpdatedAt:             &updatedAt,
		})
		seen[id] = struct{}{}
	}

	page, next, err := wire.PaginateSessions(sessions, params.Cursor)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}

	return acp.ListSessionsResponse{Sessions: page, NextCursor: next}, nil
}

// Prompt sends one turn to hermes and streams updates until it settles.
func (a *Agent) Prompt(ctx context.Context, params acp.PromptRequest) (resp acp.PromptResponse, err error) {
	s, err := a.session(ctx, params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	var raw json.RawMessage
	if t := a.transportRef(); t != nil {
		raw = t.TakeRawPrompt(params.SessionId, params.Meta)
	}

	ctx, finish := a.observe.StartPrompt(ctx, params.Meta, s.currentModel())
	defer func() { finish(promptResult(resp, err, s)) }()

	return s.prompt(ctx, params, raw)
}

func promptResult(resp acp.PromptResponse, err error, s *session) observer.PromptResult {
	result := observer.PromptResult{Err: err, Model: s.currentModel(), StopReason: string(resp.StopReason)}
	if provider, _, found := strings.Cut(result.Model, "/"); found {
		result.Provider = provider
	}

	if resp.Usage != nil {
		result.InputTokens = resp.Usage.InputTokens
		result.OutputTokens = resp.Usage.OutputTokens
		result.TotalTokens = resp.Usage.TotalTokens

		if resp.Usage.CachedReadTokens != nil {
			result.CachedReadTokens = *resp.Usage.CachedReadTokens
		}

		if resp.Usage.CachedWriteTokens != nil {
			result.CachedWriteTokens = *resp.Usage.CachedWriteTokens
		}
	}

	return result
}

func (s *session) currentModel() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.model
}

// Cancel interrupts the session's in-flight turn. It is wire-silent on an
// unknown session or with no turn in flight.
func (a *Agent) Cancel(ctx context.Context, params acp.CancelNotification) (err error) {
	_, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionCancel)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return wire.ParamRefusal(refusal)
	}

	s, err := a.session(ctx, params.SessionId)
	if err != nil {
		return nil
	}

	s.cancel(ctx)

	return nil
}

// CloseSession runs the shutdown ladder for one session.
func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (resp acp.CloseSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionClose)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.CloseSessionResponse{}, wire.ParamRefusal(refusal)
	}

	s, err := a.session(ctx, params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	// The ladder has already run either way, so the slot is released before
	// the verdict: a failed close must not keep the session installed.
	closeErr := s.close(ctx)
	a.detach(ctx, s)

	if closeErr != nil {
		return acp.CloseSessionResponse{}, wire.InternalFailure(vendor, "")
	}

	return acp.CloseSessionResponse{}, nil
}

// UnstableDeleteSession tombstones the session first, then closes any live
// session with the same id. Native state stays in hermes's home.
func (a *Agent) UnstableDeleteSession(ctx context.Context, params acp.UnstableDeleteSessionRequest) (resp acp.UnstableDeleteSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionDelete)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.UnstableDeleteSessionResponse{}, wire.ParamRefusal(refusal)
	}

	if err := a.store.Delete(ctx, acpcore.SessionKey{SessionID: string(params.SessionId)}); err != nil {
		return acp.UnstableDeleteSessionResponse{}, wire.InternalFailure(vendor, "")
	}

	a.mu.Lock()
	a.deleted[params.SessionId] = struct{}{}
	s := a.sessions[params.SessionId]
	a.mu.Unlock()

	if s == nil {
		return acp.UnstableDeleteSessionResponse{}, nil
	}

	closeErr := s.close(ctx)
	a.detach(ctx, s)

	if closeErr != nil {
		return acp.UnstableDeleteSessionResponse{}, wire.InternalFailure(vendor, "")
	}

	return acp.UnstableDeleteSessionResponse{}, nil
}

// SetSessionConfigOption applies one select value.
func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (resp acp.SetSessionConfigOptionResponse, err error) {
	var meta map[string]any

	switch {
	case params.ValueId != nil:
		meta = params.ValueId.Meta
	case params.Boolean != nil:
		meta = params.Boolean.Meta
	}

	ctx, finish := a.observe.StartACP(ctx, meta, acp.AgentMethodSessionSetConfigOption)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(meta); refusal != nil {
		return acp.SetSessionConfigOptionResponse{}, wire.ParamRefusal(refusal)
	}

	if params.ValueId == nil {
		return acp.SetSessionConfigOptionResponse{}, wire.Unsupported(fieldType)
	}

	s, err := a.session(ctx, params.ValueId.SessionId)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	options, err := s.setConfigOption(ctx, params.ValueId.ConfigId, string(params.ValueId.Value))
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	return acp.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

// session resolves an addressed id to its live session. A deleted id is
// indistinguishable from one that never existed.
func (a *Agent) session(ctx context.Context, sessionID acp.SessionId) (*session, error) {
	a.mu.Lock()

	if a.closed {
		a.mu.Unlock()

		return nil, wire.AgentClosed()
	}

	s := a.sessions[sessionID]
	if s == nil || isDeleted(a.deleted, sessionID) {
		a.mu.Unlock()

		return nil, wire.UnknownSession()
	}

	a.mu.Unlock()

	if transport := a.transportRef(); transport != nil {
		if err := transport.AwaitSession(ctx, sessionID); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (a *Agent) deletedSession(id acp.SessionId) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return isDeleted(a.deleted, id)
}

// detach removes a session from the active map while it still resolves to
// this exact session.
func (a *Agent) detach(ctx context.Context, s *session) {
	a.mu.Lock()

	current := a.sessions[s.id] == s
	if current {
		delete(a.sessions, s.id)
	}
	a.mu.Unlock()

	if current {
		a.observe.AddActiveSession(ctx, -1)
	}
}
