//nolint:wsl_v5 // Session lifecycle code keeps fail-closed acquisitions adjacent.
package hermesacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

var reapHermesLeaseFile = nativehermes.ReapLeaseFile
var createHermesGeneration = nativehermes.CreateGenerationXDGDirs

func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	if err := a.ensureOpen(); err != nil {
		return acp.NewSessionResponse{}, err
	}

	if err := a.rejectInvalidConfiguration(); err != nil {
		return acp.NewSessionResponse{}, err
	}

	ctx = a.observe.Extract(ctx, params.Meta)

	if err := validateSessionStartPaths(params.Cwd, params.AdditionalDirectories); err != nil {
		return acp.NewSessionResponse{}, err
	}

	if err := validateMCPServers(params.McpServers); err != nil {
		return acp.NewSessionResponse{}, err
	}

	meta, err := a.sessionMetaFromLifecycle(params.Meta)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	if meta.Model == "" {
		meta.Model = a.options.DefaultModel
	}

	idValue, err := newSessionID()
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	id := acp.SessionId(idValue)
	if constructionErr := a.beginSessionConstruction(); constructionErr != nil {
		return acp.NewSessionResponse{}, constructionErr
	}
	defer a.endSessionConstruction()

	client, err := a.newHermesClient(ctx, id, params.Cwd, meta, nativehermes.XDGDirs{}, params.McpServers)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	sessionStarted := time.Now()
	native, err := client.CreateSession(ctx, "")
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSession, sessionStarted, err)

	if err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.NewSessionResponse{}, errors.Join(err, closeErr)
	}

	idmap := idmapRecord{
		SessionID:       string(id),
		NativeSessionID: native.ID,
		Format:          SessionStoreFormat,
	}

	session := newSession(a, id, params.Cwd, params.AdditionalDirectories, params.McpServers, native, client, meta, idmap)
	if err := a.storeStartedSession(session); err != nil {
		closeErr := session.Close(context.Background())
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.NewSessionResponse{}, errors.Join(err, closeErr)
	}

	if err := session.snapshotToStore(context.WithoutCancel(ctx)); err != nil {
		cleanupErr := a.cleanupFailedStartedSession(ctx, session)

		return acp.NewSessionResponse{}, errors.Join(err, cleanupErr)
	}

	return acp.NewSessionResponse{
		SessionId:     id,
		Meta:          lifecycleResponseMeta(session.snapshot()),
		ConfigOptions: session.configOptions(ctx),
	}, nil
}

// cleanupFailedStartedSession removes a registered session whose durable
// snapshot never committed: it deregisters the session so it is neither active
// nor listable, deletes and closes the native Hermes session, and removes the
// XDG root and lease. The store is the durability boundary, so a failed initial
// or fork Replace must leave no orphan process or listable session behind.
func (a *Agent) cleanupFailedStartedSession(ctx context.Context, session *session) error {
	if a.removeSessionIf(session.id, session) {
		a.observe.AddActiveSession(ctx, -1)
	}

	record := a.deleteCleanupRecord(session.id, session)
	closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	err := session.DeleteNativeAndClose(closeCtx)

	cancel()
	a.recordIncompleteContainment(err, session.id, record.XDGRoot)

	if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
		return err
	}

	err = errors.Join(err, a.cleanupDeletedSession(record))
	if err != nil {
		a.log.DebugContext(ctx, "clean up Hermes session after failed snapshot", slog.String(jsonFieldError, err.Error()))
	}

	return err
}

func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	session, err := a.loadOrResumeSession(ctx, params.SessionId, params.Cwd, params.AdditionalDirectories, params.McpServers, params.Meta)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	if err := session.replayMessages(ctx); err != nil {
		return acp.LoadSessionResponse{}, err
	}

	return acp.LoadSessionResponse{
		Meta:          lifecycleResponseMeta(session.snapshot()),
		ConfigOptions: session.configOptions(ctx),
	}, nil
}

func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	if err := validateMCPServers(params.McpServers); err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	session, err := a.loadOrResumeSession(ctx, params.SessionId, params.Cwd, params.AdditionalDirectories, params.McpServers, params.Meta)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	return acp.ResumeSessionResponse{
		Meta:          lifecycleResponseMeta(session.snapshot()),
		ConfigOptions: session.configOptions(ctx),
	}, nil
}

func (a *Agent) loadOrResumeSession(
	ctx context.Context,
	id acp.SessionId,
	cwd string,
	additionalDirectories []string,
	mcpServers []acp.McpServer,
	metaMap map[string]any,
) (_ *session, returnErr error) {
	if err := a.ensureOpen(); err != nil {
		return nil, err
	}

	if err := a.rejectInvalidConfiguration(); err != nil {
		return nil, err
	}

	ctx = a.observe.Extract(ctx, metaMap)

	if id == "" {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldSessionID: validationRequired})
	}

	if a.isDeleted(id) {
		_ = a.retryDeletedSessionCleanup(ctx)

		return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnknownSession, keyField: jsonFieldSessionID})
	}

	if err := a.retryDeletedSessionCleanup(ctx); err != nil {
		a.log.DebugContext(ctx, "retry deleted Hermes session cleanup failed", slog.String(jsonFieldError, err.Error()))
	}

	if err := validateSessionStartPaths(cwd, additionalDirectories); err != nil {
		return nil, err
	}

	if err := validateMCPServers(mcpServers); err != nil {
		return nil, err
	}

	meta, err := a.sessionMetaFromLifecycle(metaMap)
	if err != nil {
		return nil, err
	}

	// X1: validate the full request first (done above), then reuse an
	// already-active session instead of starting a second `hermes serve`.
	// session/load replays on the reused session (LoadSession calls
	// replayMessages); session/resume returns without replay.
	if existing := a.activeSession(id); existing != nil {
		if applyErr := applyActiveLifecycleRequest(existing, cwd, additionalDirectories, mcpServers, meta); applyErr != nil {
			return nil, applyErr
		}

		return existing, nil
	}
	if constructionErr := a.beginSessionConstruction(); constructionErr != nil {
		return nil, constructionErr
	}
	defer a.endSessionConstruction()

	if incompleteErr := a.rejectIncompleteHermesSession(id); incompleteErr != nil {
		return nil, incompleteErr
	}

	scratchRelease, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}

	keepScratch := false

	var xdg nativehermes.XDGDirs

	defer func() {
		if !keepScratch {
			returnErr = errors.Join(returnErr, deleteHermesScratchRoot(xdg.Root, scratchRelease))
		}
	}()

	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return nil, err
	}
	xdg, err = createHermesGeneration(parent)
	if err != nil {
		return nil, err
	}

	storeCtx, cancel := a.sessionStoreContext(ctx)
	idmap, snapshot, ok, err := hydrateStateFromStore(storeCtx, a.sessionStore(), string(id), xdg)

	cancel()

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnknownSession, keyField: jsonFieldSessionID})
	}

	if snapshot.Session.Cwd != "" && snapshot.Session.Cwd != cwd {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: "cwd_mismatch", keyField: jsonFieldCwd})
	}

	client, err := a.newHermesClientWithScratch(ctx, id, cwd, meta, xdg, scratchRelease, mcpServers)
	if err != nil {
		if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			keepScratch = true

			a.retainIncompleteHermesRoot(id, xdg.Root)
		}

		return nil, err
	}

	keepScratch = true

	native, err := client.GetSession(ctx, idmap.NativeSessionID)
	if err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return nil, errors.Join(err, closeErr)
	}

	if meta.Model == "" {
		meta.Model = joinModelValue(snapshot.Session.Model.ProviderID, snapshot.Session.Model.ModelID)
	}

	session := newSession(a, id, cwd, additionalDirectories, mcpServers, native, client, meta, idmap)

	session.committedTerminal = publicTerminalState(snapshot.Terminal)
	if err := a.storeStartedSession(session); err != nil {
		closeErr := session.Close(context.Background())
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return nil, errors.Join(err, closeErr)
	}

	return session, nil
}

// resumeRuntimeForTurnLocked rebuilds a fenced session runtime from the last
// committed store snapshot. toolMu and cancelMu are held by the caller, so the
// old prompt has fully released the per-session token and Close cannot cross
// installation of the replacement runtime.
func (s *session) resumeRuntimeForTurnLocked(ctx context.Context) (returnErr error) {
	s.mu.Lock()
	needsResume := s.runtimeNeedsResume
	closed := s.closed
	poisonErr := s.poisonedErrorLocked()
	id := s.id
	cwd := s.cwd
	mcpServers := cloneMCPServers(s.mcpServers)
	wantIDMap := s.idmap
	meta := sessionMeta{
		Model:       joinModelValue(s.providerID, s.modelID),
		Env:         cloneStringMap(s.env),
		RawMessages: s.rawMessages,
	}
	s.mu.Unlock()

	if poisonErr != nil {
		return poisonErr
	}

	if closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
	}

	if !needsResume {
		return nil
	}
	if err := s.agent.rejectIncompleteHermesSession(id); err != nil {
		return s.poisonWithError(ctx, "hermes_process_containment_incomplete", err.Error())
	}

	scratchRelease, err := reserveScratchRoot(ctx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return err
	}

	keepScratch := false

	var xdg nativehermes.XDGDirs

	defer func() {
		if !keepScratch {
			returnErr = errors.Join(returnErr, deleteHermesScratchRoot(xdg.Root, scratchRelease))
		}
	}()

	parent, err := ensureScratchParent(s.agent.options.ScratchDir)
	if err != nil {
		return err
	}
	xdg, err = createHermesGeneration(parent)
	if err != nil {
		return err
	}

	storeCtx, cancel := s.agent.sessionStoreContext(ctx)
	idmap, snapshot, ok, err := hydrateStateFromStore(storeCtx, s.agent.sessionStore(), string(id), xdg)

	cancel()

	if err != nil {
		return err
	}

	if !ok {
		return s.poisonWithError(ctx, "hermes_runtime_resume_failed", "last committed Hermes session state is missing")
	}

	if idmap.SessionID != wantIDMap.SessionID || idmap.NativeSessionID != wantIDMap.NativeSessionID {
		return s.poisonWithError(ctx, "hermes_native_session_id_drift", fmt.Sprintf(
			"stored session identity drift: expected %q/%q, got %q/%q",
			wantIDMap.SessionID,
			wantIDMap.NativeSessionID,
			idmap.SessionID,
			idmap.NativeSessionID,
		))
	}

	if snapshot.Session.Cwd != "" && snapshot.Session.Cwd != cwd {
		return s.poisonWithError(ctx, "hermes_runtime_resume_failed", fmt.Sprintf(
			"stored cwd drift: expected %q, got %q",
			cwd,
			snapshot.Session.Cwd,
		))
	}

	client, err := s.agent.newHermesClientWithScratch(ctx, id, cwd, meta, xdg, scratchRelease, mcpServers)
	if err != nil {
		if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			keepScratch = true

			s.agent.retainIncompleteHermesRoot(id, xdg.Root)

			return s.poisonWithError(ctx, "hermes_process_containment_incomplete", err.Error())
		}

		return err
	}

	keepScratch = true

	native, err := client.GetSession(ctx, wantIDMap.NativeSessionID)
	if err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		s.agent.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return errors.Join(err, closeErr)
	}

	if native.ID != wantIDMap.NativeSessionID {
		closeErr := closeHermesClientAfterStartupFailure(client)
		s.agent.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))
		driftErr := s.poisonNativeSessionDrift(ctx, "runtime resume", native.ID)

		return errors.Join(driftErr, closeErr)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		closeErr := closeHermesClientAfterStartupFailure(client)
		s.agent.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return errors.Join(acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed}), closeErr)
	}

	s.client = client
	s.committedTerminal = publicTerminalState(snapshot.Terminal)
	s.runtimeNeedsResume = false
	s.mcpReloadComplete = false
	s.suppressNextBacklog = false
	s.mu.Unlock()

	// A resumed gateway may enqueue historical events before the first new
	// prompt. They describe the already-committed checkpoint and must never be
	// rebound to the new turn route.
	for {
		select {
		case <-client.Events():
			continue
		case <-client.EventErrors():
			continue
		default:
			return nil
		}
	}
}

func applyActiveLifecycleRequest(existing *session, cwd string, additionalDirectories []string, mcpServers []acp.McpServer, meta sessionMeta) error {
	snapshot := existing.snapshot()
	if snapshot.cwd != "" && snapshot.cwd != cwd {
		return lifecycleMismatch(jsonFieldCwd)
	}

	if !stringSetEqual(snapshot.additionalDirectories, additionalDirectories) {
		return lifecycleMismatch("additionalDirectories")
	}

	if !mcpServerSetEqual(snapshot.mcpServers, mcpServers) {
		return lifecycleMismatch("mcpServers")
	}

	if !stringMapsEqual(snapshot.env, meta.Env) {
		return lifecycleMismatch("_meta.hermes.options.env")
	}

	if meta.Model != "" && meta.Model != joinModelValue(snapshot.providerID, snapshot.modelID) {
		return lifecycleMismatch("_meta.hermes.options.model")
	}

	existing.mu.Lock()
	existing.rawMessages = meta.RawMessages
	existing.mu.Unlock()

	return nil
}

func lifecycleMismatch(field string) error {
	return acp.NewInvalidParams(map[string]any{jsonFieldError: "mismatch", keyField: field})
}

func stringMapsEqual(left map[string]string, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}

	for key, leftValue := range left {
		if right[key] != leftValue {
			return false
		}
	}

	return true
}

func stringSetEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}

	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)

	slices.Sort(leftCopy)
	slices.Sort(rightCopy)

	return slices.Equal(leftCopy, rightCopy)
}

func mcpServerSetEqual(left []acp.McpServer, right []acp.McpServer) bool {
	return slices.Equal(canonicalMCPServers(left), canonicalMCPServers(right))
}

func canonicalMCPServers(servers []acp.McpServer) []string {
	out := make([]string, len(servers))
	for index, server := range servers {
		data, _ := json.Marshal(server)
		out[index] = string(data)
	}

	slices.Sort(out)

	return out
}

func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	if err := a.ensureOpen(); err != nil {
		return acp.ListSessionsResponse{}, err
	}

	if err := a.retryDeletedSessionCleanup(ctx); err != nil {
		a.log.DebugContext(ctx, "retry deleted Hermes session cleanup failed", slog.String(jsonFieldError, err.Error()))
	}

	if err := validateOptionalAbsolutePath(jsonFieldCwd, params.Cwd); err != nil {
		return acp.ListSessionsResponse{}, err
	}

	a.mu.Lock()

	active := make([]*session, 0, len(a.sessions))
	for _, session := range a.sessions {
		if params.Cwd != nil && session.cwd != *params.Cwd {
			continue
		}

		active = append(active, session)
	}
	a.mu.Unlock()

	infos := make([]acp.SessionInfo, 0, len(active))
	seen := map[acp.SessionId]struct{}{}

	for _, session := range active {
		info := session.info()
		infos = append(infos, info)
		seen[info.SessionId] = struct{}{}
	}

	storeCtx, cancel := a.sessionStoreContext(ctx)
	stored, err := a.sessionStore().ListSessions(storeCtx)

	cancel()

	if err != nil {
		return acp.ListSessionsResponse{}, err
	}

	for _, summary := range stored {
		id := acp.SessionId(summary.SessionID)
		if _, ok := seen[id]; ok || a.isDeleted(id) {
			continue
		}

		if params.Cwd != nil && summary.Cwd != "" && summary.Cwd != *params.Cwd {
			continue
		}

		title := summary.Title
		updated := time.UnixMilli(summary.UpdatedAtUnixMilli).UTC().Format(time.RFC3339)
		infos = append(infos, acp.SessionInfo{
			SessionId: id,
			Cwd:       summary.Cwd,
			Title:     &title,
			UpdatedAt: &updated,
			Meta:      summary.Meta,
		})
	}

	slices.SortFunc(infos, func(left, right acp.SessionInfo) int {
		l := ""
		r := ""

		if left.UpdatedAt != nil {
			l = *left.UpdatedAt
		}

		if right.UpdatedAt != nil {
			r = *right.UpdatedAt
		}

		if r != l {
			return strings.Compare(r, l)
		}

		return strings.Compare(string(left.SessionId), string(right.SessionId))
	})

	paged, next, err := paginateSessionInfos(infos, params.Cursor)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}

	return acp.ListSessionsResponse{Sessions: paged, NextCursor: next}, nil
}

func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	session.lifecycleMu.Lock()

	skipSnapshot := session.snapshotBlockedReason() != ""

	var snapshotErr error
	if !skipSnapshot {
		snapshotErr = session.snapshotToStoreLocked(context.WithoutCancel(ctx), nil, nil)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
	closeErr := session.closeLocked(closeCtx, false)

	closeCancel()
	session.lifecycleMu.Unlock()
	a.recordIncompleteContainment(closeErr, params.SessionId, session.client.XDGDirs().Root)

	if a.removeSessionIf(params.SessionId, session) {
		a.observe.AddActiveSession(ctx, -1)
	}

	return acp.CloseSessionResponse{}, errors.Join(snapshotErr, closeErr)
}

func (a *Agent) UnstableDeleteSession(ctx context.Context, params acp.UnstableDeleteSessionRequest) (acp.UnstableDeleteSessionResponse, error) {
	ctx = a.observe.Extract(ctx, params.Meta)

	if params.SessionId == "" {
		return acp.UnstableDeleteSessionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldSessionID: validationRequired})
	}

	if err := a.retryDeletedSessionCleanup(ctx); err != nil {
		a.log.DebugContext(ctx, "retry deleted Hermes session cleanup failed", slog.String(jsonFieldError, err.Error()))
	}

	a.mu.Lock()
	session := a.sessions[params.SessionId]
	a.mu.Unlock()

	record := a.deleteCleanupRecord(params.SessionId, session)
	storeCtx, cancel := sessionStoreWriteContext(ctx)
	err := a.sessionStore().Delete(storeCtx, SessionKey{SessionID: string(params.SessionId)})

	cancel()

	if err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	a.mu.Lock()
	if session == nil || a.sessions[params.SessionId] == session {
		delete(a.sessions, params.SessionId)
	}

	a.deleted[params.SessionId] = struct{}{}
	a.mu.Unlock()

	if record.SessionID != "" {
		a.rememberDeleteCleanup(record)
	}

	if session != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
		err = session.DeleteNativeAndClose(closeCtx)

		closeCancel()
		a.recordIncompleteContainment(err, params.SessionId, record.XDGRoot)
		a.observe.AddActiveSession(ctx, -1)
	}

	if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
		a.mu.Lock()
		delete(a.deleteCleanup, record.SessionID)
		a.mu.Unlock()

		return acp.UnstableDeleteSessionResponse{}, err
	}

	cleanupErr := a.cleanupDeletedSession(record)
	a.forgetDeleteCleanupIfDone(record.SessionID)

	return acp.UnstableDeleteSessionResponse{}, errors.Join(err, cleanupErr)
}

func (a *Agent) forkSession(ctx context.Context, params acp.UnstableForkSessionRequest) (_ acp.UnstableForkSessionResponse, returnErr error) {
	if err := a.rejectInvalidConfiguration(); err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	ctx = a.observe.Extract(ctx, params.Meta)

	if err := validateSessionStartPaths(params.Cwd, params.AdditionalDirectories); err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	if err := validateUnstableMCPServers(params.McpServers); err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	meta, err := a.sessionMetaFromLifecycle(params.Meta)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}
	if constructionErr := a.beginSessionConstruction(); constructionErr != nil {
		return acp.UnstableForkSessionResponse{}, constructionErr
	}
	defer a.endSessionConstruction()

	parent, err := a.session(params.SessionId)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	parentSnapshot := parent.snapshot()

	nativeChild, err := parentSnapshot.client.Fork(ctx, parentSnapshot.idmap.NativeSessionID, "")
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	idValue, err := newSessionID()
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	id := acp.SessionId(idValue)

	if incompleteErr := a.rejectIncompleteHermesSession(id); incompleteErr != nil {
		return acp.UnstableForkSessionResponse{}, incompleteErr
	}

	scratchRelease, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	keepScratch := false

	var xdg nativehermes.XDGDirs

	defer func() {
		if !keepScratch {
			returnErr = errors.Join(returnErr, deleteHermesScratchRoot(xdg.Root, scratchRelease))
		}
	}()

	parentRoot, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}
	xdg, err = createHermesGeneration(parentRoot)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	if cloneErr := cloneHermesStateDB(a.options.ScratchDir, parentSnapshot.client.XDGDirs(), xdg); cloneErr != nil {
		return acp.UnstableForkSessionResponse{}, cloneErr
	}

	if meta.Model == "" {
		meta.Model = joinModelValue(parentSnapshot.providerID, parentSnapshot.modelID)
	}

	client, err := a.newHermesClientWithScratch(ctx, id, params.Cwd, meta, xdg, scratchRelease, stableMCPServersFromUnstable(params.McpServers))
	if err != nil {
		if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			keepScratch = true

			a.retainIncompleteHermesRoot(id, xdg.Root)
		}

		return acp.UnstableForkSessionResponse{}, err
	}

	keepScratch = true

	native, err := client.GetSession(ctx, nativeChild.ID)
	if err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.UnstableForkSessionResponse{}, errors.Join(err, closeErr)
	}

	idmap := idmapRecord{
		SessionID:             string(id),
		NativeSessionID:       native.ID,
		ParentSessionID:       string(params.SessionId),
		NativeParentSessionID: parentSnapshot.idmap.NativeSessionID,
		Format:                SessionStoreFormat,
	}

	session := newSession(a, id, params.Cwd, params.AdditionalDirectories, stableMCPServersFromUnstable(params.McpServers), native, client, meta, idmap)
	if err := a.storeStartedSession(session); err != nil {
		closeErr := session.Close(context.Background())
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.UnstableForkSessionResponse{}, errors.Join(err, closeErr)
	}

	if err := session.snapshotToStore(context.WithoutCancel(ctx)); err != nil {
		cleanupErr := a.cleanupFailedStartedSession(ctx, session)

		return acp.UnstableForkSessionResponse{}, errors.Join(err, cleanupErr)
	}

	return acp.UnstableForkSessionResponse{
		SessionId:     id,
		Meta:          lifecycleResponseMeta(session.snapshot()),
		ConfigOptions: unstableConfigOptions(session.configOptions(ctx)),
	}, nil
}

func (a *Agent) newHermesClient(ctx context.Context, id acp.SessionId, cwd string, meta sessionMeta, existing nativehermes.XDGDirs, mcpServers ...[]acp.McpServer) (nativehermes.Server, error) {
	if err := a.rejectIncompleteHermesSession(id); err != nil {
		return nil, err
	}
	scratchRelease, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}
	if existing.Root == "" {
		parent, parentErr := ensureScratchParent(a.options.ScratchDir)
		if parentErr != nil {
			scratchRelease()

			return nil, parentErr
		}

		existing, err = createHermesGeneration(parent)
		if err != nil {
			scratchRelease()

			return nil, err
		}
	}
	root := existing.Root

	client, err := a.newHermesClientWithScratch(ctx, id, cwd, meta, existing, scratchRelease, mcpServers...)
	if err != nil {
		if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			a.retainIncompleteHermesRoot(id, root)

			return nil, err
		}

		return nil, errors.Join(err, deleteHermesScratchRoot(root, scratchRelease))
	}

	return client, nil
}

func closeHermesClientAfterStartupFailure(client nativehermes.Server) error {
	closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	return client.Close(closeCtx)
}

func (a *Agent) newHermesClientWithScratch(ctx context.Context, id acp.SessionId, cwd string, meta sessionMeta, existing nativehermes.XDGDirs, scratchRelease func(), mcpServers ...[]acp.McpServer) (nativehermes.Server, error) {
	if a.options.ProviderAuthHome != "" {
		if err := validateNativeOwnedDirectory(a.options.ProviderAuthHome, a.options.ProcessIsolation); err != nil {
			return nil, err
		}
	}

	factory := a.options.clientFactory
	if factory == nil {
		factory = nativehermes.StartServer
	}

	env := cloneStringMap(a.options.Env)
	if env == nil && len(meta.Env) > 0 {
		env = map[string]string{}
	}

	for key, value := range meta.Env {
		env[key] = value
	}
	delete(env, "HERMES_AUTH_HOME")

	var servers []acp.McpServer
	if len(mcpServers) > 0 {
		servers = cloneMCPServers(mcpServers[0])
	}

	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return nil, err
	}

	nativeRelease, err := acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}

	a.observe.RecordHermesProcessStart(ctx)
	processRoot := a.processes.register()

	client, err := factory(ctx, nativehermes.StartOptions{
		ACPSessionID:     nativehermes.ACPSessionIDString(id),
		Root:             a.homeRoot(),
		ScratchParent:    parent,
		Cwd:              cwd,
		ExecutablePath:   a.options.ExecutablePath,
		DefaultModel:     firstNonEmpty(meta.Model, a.options.DefaultModel),
		ProviderAuthHome: a.options.ProviderAuthHome,
		Env:              a.observe.InjectTraceEnv(ctx, env),
		Isolation:        nativeProcessIsolation(a.options.ProcessIsolation, a.options.testOnlyNoCredential, a.options.testOnlyIdentityLockRoot),
		Logger:           a.log,
		ExistingXDG:      existing,
		MCPServers:       servers,
		SeedFiles:        cloneStringMap(a.options.SeedFiles),
		ObserveStartupStage: func(stageCtx context.Context, lifecycle, stage string, elapsed time.Duration, stageErr error) {
			observe := a.options.RuntimeResourceHooks.ObserveStartupStage
			if observe != nil {
				observe(stageCtx, RuntimeResourceKind(lifecycle), RuntimeStartupStage(stage), elapsed, stageErr)
			}
		},
		AcquireDiscoveryResources: func(discoveryCtx context.Context) (func(), func(), error) {
			discoveryScratchRelease, discoveryErr := reserveScratchRoot(discoveryCtx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
			if discoveryErr != nil {
				return nil, nil, discoveryErr
			}

			discoveryNativeRelease, discoveryErr := acquireNativeRoot(discoveryCtx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
			if discoveryErr != nil {
				discoveryScratchRelease()

				return nil, nil, discoveryErr
			}

			return discoveryNativeRelease, discoveryScratchRelease, nil
		},
		RetainDiscoveryRoot: func(root string, discoveryErr error) {
			a.recordIncompleteContainment(discoveryErr, id, root)
		},
		DarwinBestEffortContainment: a.options.DarwinBestEffortContainment,
	})
	if err != nil {
		processRoot.retire(ctx, providerProcessTreeProven(err))
		a.recordIncompleteContainment(err, id, existing.Root)

		if !errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			nativeRelease()
		}

		return nil, err
	}

	processRoot.observe(ctx, client)

	root := existing.Root

	return &managedHermesServer{
		Server:           client,
		root:             root,
		sessionID:        id,
		nativeRelease:    nativeRelease,
		scratchRelease:   scratchRelease,
		retainIncomplete: a.recordIncompleteContainment,
		processRoot:      processRoot,
	}, nil
}

func nativeProcessIsolation(isolation *ProcessIsolation, testOnlyNoCredential bool, testOnlyIdentityLockRoot string) *nativehermes.ProcessIsolation {
	if isolation == nil {
		return nil
	}
	base := cloneStringMap(isolation.BaseEnvironment)

	return &nativehermes.ProcessIsolation{
		UID:                      isolation.UID,
		GID:                      isolation.GID,
		BaseEnvironment:          base,
		TestOnlyNoCredential:     testOnlyNoCredential,
		TestOnlyIdentityLockRoot: testOnlyIdentityLockRoot,
	}
}

type deleteCleanupRecord struct {
	SessionID acp.SessionId
	NativeID  string
	XDGRoot   string
}

func (a *Agent) deleteCleanupRecord(id acp.SessionId, session *session) deleteCleanupRecord {
	record := deleteCleanupRecord{
		SessionID: id,
		XDGRoot:   filepath.Join(a.homeRoot(), nativehermes.SafePathName(string(id))),
	}
	if session == nil {
		return record
	}

	snapshot := session.snapshot()

	record.NativeID = snapshot.idmap.NativeSessionID
	if snapshot.client != nil {
		if xdg := snapshot.client.XDGDirs(); xdg.Root != "" {
			record.XDGRoot = xdg.Root
		}
	}

	return record
}

func (a *Agent) rememberDeleteCleanup(record deleteCleanupRecord) {
	if record.SessionID == "" {
		return
	}

	a.mu.Lock()
	a.deleteCleanup[record.SessionID] = record
	a.mu.Unlock()
}

func (a *Agent) forgetDeleteCleanupIfDone(id acp.SessionId) {
	if id == "" {
		return
	}

	record := a.deleteCleanupRecord(id, nil)
	if _, err := os.Stat(record.XDGRoot); err == nil {
		return
	}

	a.mu.Lock()
	delete(a.deleteCleanup, id)
	a.mu.Unlock()
}

func (a *Agent) retryDeletedSessionCleanup(ctx context.Context) error {
	a.mu.Lock()

	records := make([]deleteCleanupRecord, 0, len(a.deleteCleanup))
	for _, record := range a.deleteCleanup {
		records = append(records, record)
	}
	a.mu.Unlock()

	var err error

	for _, record := range records {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(err, ctxErr)
		}

		cleanupErr := a.cleanupDeletedSession(record)
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)

			continue
		}

		a.mu.Lock()
		delete(a.deleteCleanup, record.SessionID)
		a.mu.Unlock()
	}

	return err
}

func (a *Agent) cleanupDeletedSession(record deleteCleanupRecord) error {
	if record.SessionID == "" || record.XDGRoot == "" {
		return nil
	}

	if reapHermesLeaseFile(filepath.Join(nativehermes.ControlDirForXDG(record.XDGRoot), nativehermes.LeaseFileName), a.log) {
		return fmt.Errorf("hermes delete cleanup kept live lease for session %q", record.SessionID)
	}

	return errors.Join(os.RemoveAll(record.XDGRoot), os.RemoveAll(nativehermes.ControlDirForXDG(record.XDGRoot)))
}

// homeRoot returns the parent directory under which isolated per-session
// Hermes homes are created, rooted at the resolved scratch parent. Home is an
// unsupported option (rejected before any session is established), so it never
// participates in this path.
func (a *Agent) homeRoot() string {
	return filepath.Join(scratchParent(a.options.ScratchDir), valACPGoHermes)
}

// rejectInvalidConfiguration fails session establishment on agent configuration
// no session may run under: an option that failed validation at construction,
// a configured Home value, or a configured ProviderAuthDirectHome value. The
// handshake reports the option failures too, but an embedded host can open a
// session and prompt without ever calling initialize, so options that never
// validated must not reach a gateway process.
func (a *Agent) rejectInvalidConfiguration() error {
	if err := a.optionsError(); err != nil {
		return err
	}

	if a.options.Home != "" {
		return unsupportedField(optionFieldHome)
	}

	if a.options.ProviderAuthDirectHome != "" {
		return unsupportedField(optionFieldProviderAuthDirectHome)
	}

	if err := validateProcessIsolationOption(a.options.ProcessIsolation); err != nil {
		return err
	}

	return nil
}

func validateProcessIsolationOption(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation policy is required")
	}
	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}
	if agentRuntimePlatform == agentRuntimeWindows {
		return errors.New("process isolation is unsupported on windows")
	}

	return nil
}

// validateUnstableMCPServers applies the same acceptance rules as
// validateMCPServers to the unstable fork variant: reject unsupported
// transports and require a non-empty, request-unique name on every accepted
// (stdio or http) declaration. Indices are preserved by the conversion, so the
// offending-field paths match the wire request.
func validateUnstableMCPServers(servers []acp.UnstableMcpServer) error {
	return validateMCPServers(stableMCPServersFromUnstable(servers))
}

func cloneHermesStateDB(scratchDir string, source nativehermes.XDGDirs, target nativehermes.XDGDirs) error {
	data, _, ok, err := encodeHermesStateDBArchive(scratchDir, source.Root)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	return decodeXDGArchive(data, target.Root)
}

func paginateSessionInfos(infos []acp.SessionInfo, cursor *string) ([]acp.SessionInfo, *string, error) {
	offset, err := decodeListCursor(cursor)
	if err != nil {
		return nil, nil, acp.NewInvalidParams(map[string]any{"cursor": "invalid cursor"})
	}

	if offset > len(infos) {
		return nil, nil, acp.NewInvalidParams(map[string]any{"cursor": "cursor is past end"})
	}

	end := offset + listSessionsPageSize
	if end >= len(infos) {
		return infos[offset:], nil, nil
	}

	next := encodeListCursor(end)

	return infos[offset:end], &next, nil
}

func decodeListCursor(cursor *string) (int, error) {
	if cursor == nil || *cursor == "" {
		return 0, nil
	}

	data, err := base64.RawURLEncoding.DecodeString(*cursor)
	if err != nil {
		return 0, err
	}

	offset, err := strconv.Atoi(string(data))
	if err != nil || offset < 0 {
		return 0, strconv.ErrSyntax
	}

	return offset, nil
}

func encodeListCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}
