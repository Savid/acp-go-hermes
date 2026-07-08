package hermesacp

import (
	"context"
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

	"github.com/coder/acp-go-sdk"
)

func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	if err := a.ensureOpen(); err != nil {
		return acp.NewSessionResponse{}, err
	}

	ctx = a.observe.Extract(ctx, params.Meta)

	if err := validateSessionStartPaths(params.Cwd, params.AdditionalDirectories); err != nil {
		return acp.NewSessionResponse{}, err
	}

	if err := validateMCPServers(params.McpServers); err != nil {
		return acp.NewSessionResponse{}, err
	}

	meta, err := sessionMetaFromLifecycle(params.Meta)
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

	client, err := a.newHermesClient(ctx, id, params.Cwd, meta, xdgDirs{}, params.McpServers)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	native, err := client.CreateSession(ctx, "")
	if err != nil {
		_ = client.Close(context.Background())

		return acp.NewSessionResponse{}, err
	}

	idmap := idmapRecord{
		SessionID:       string(id),
		NativeSessionID: native.ID,
		Format:          SessionStoreFormat,
	}

	session := newSession(a, id, params.Cwd, params.AdditionalDirectories, params.McpServers, native, client, meta, idmap)
	if err := a.storeStartedSession(session); err != nil {
		_ = session.Close(context.Background())

		return acp.NewSessionResponse{}, err
	}

	if err := session.snapshotToStore(context.WithoutCancel(ctx)); err != nil {
		a.cleanupFailedStartedSession(ctx, session)

		return acp.NewSessionResponse{}, err
	}

	return acp.NewSessionResponse{
		SessionId:     id,
		Meta:          sessionResponseMeta(session.snapshot()),
		ConfigOptions: session.configOptions(ctx),
	}, nil
}

// cleanupFailedStartedSession removes a registered session whose durable
// snapshot never committed: it deregisters the session so it is neither active
// nor listable, deletes and closes the native Hermes session, and removes the
// XDG root and lease. The store is the durability boundary, so a failed initial
// or fork Replace must leave no orphan process or listable session behind.
func (a *Agent) cleanupFailedStartedSession(ctx context.Context, session *session) {
	if a.removeSessionIf(session.id, session) {
		a.observe.AddActiveSession(ctx, -1)
	}

	record := a.deleteCleanupRecord(session.id, session)
	closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	err := session.DeleteNativeAndClose(closeCtx)

	cancel()

	err = errors.Join(err, a.cleanupDeletedSession(record))
	if err != nil {
		a.log.DebugContext(ctx, "clean up Hermes session after failed snapshot", slog.String(jsonFieldError, err.Error()))
	}
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
		Meta:          sessionResponseMeta(session.snapshot()),
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
		Meta:          sessionResponseMeta(session.snapshot()),
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
) (*session, error) {
	if err := a.ensureOpen(); err != nil {
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

	meta, err := sessionMetaFromLifecycle(metaMap)
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

	xdg, err := createXDGDirs(a.homeRoot(), string(id))
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

	client, err := a.newHermesClient(ctx, id, cwd, meta, xdg, mcpServers)
	if err != nil {
		return nil, err
	}

	native, err := client.GetSession(ctx, idmap.NativeSessionID)
	if err != nil {
		_ = client.Close(context.Background())

		return nil, err
	}

	if meta.Model == "" {
		meta.Model = joinModelValue(snapshot.Session.Model.ProviderID, snapshot.Session.Model.ModelID)
	}

	session := newSession(a, id, cwd, additionalDirectories, mcpServers, native, client, meta, idmap)
	if err := a.storeStartedSession(session); err != nil {
		_ = session.Close(context.Background())

		return nil, err
	}

	return session, nil
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

	skipSnapshot := session.snapshotBlockedReason() != ""
	closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
	closeErr := session.Close(closeCtx)

	closeCancel()

	var snapshotErr error
	if !skipSnapshot {
		snapshotErr = session.snapshotToStore(context.WithoutCancel(ctx))
	}

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
	storeCtx, cancel := a.sessionStoreContext(ctx)
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
		a.observe.AddActiveSession(ctx, -1)
	}

	cleanupErr := a.cleanupDeletedSession(record)
	a.forgetDeleteCleanupIfDone(record.SessionID)

	return acp.UnstableDeleteSessionResponse{}, errors.Join(err, cleanupErr)
}

func (a *Agent) forkSession(ctx context.Context, params acp.UnstableForkSessionRequest) (acp.UnstableForkSessionResponse, error) {
	ctx = a.observe.Extract(ctx, params.Meta)

	if err := validateSessionStartPaths(params.Cwd, params.AdditionalDirectories); err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	if err := validateUnstableMCPServers(params.McpServers); err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	meta, err := sessionMetaFromLifecycle(params.Meta)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

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

	xdg, err := createXDGDirs(a.homeRoot(), string(id))
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	if cloneErr := cloneHermesStateDB(parentSnapshot.client.XDGDirs(), xdg); cloneErr != nil {
		return acp.UnstableForkSessionResponse{}, cloneErr
	}

	if meta.Model == "" {
		meta.Model = joinModelValue(parentSnapshot.providerID, parentSnapshot.modelID)
	}

	client, err := a.newHermesClient(ctx, id, params.Cwd, meta, xdg, stableMCPServersFromUnstable(params.McpServers))
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	native, err := client.GetSession(ctx, nativeChild.ID)
	if err != nil {
		_ = client.Close(context.Background())

		return acp.UnstableForkSessionResponse{}, err
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
		_ = session.Close(context.Background())

		return acp.UnstableForkSessionResponse{}, err
	}

	if err := session.snapshotToStore(context.WithoutCancel(ctx)); err != nil {
		a.cleanupFailedStartedSession(ctx, session)

		return acp.UnstableForkSessionResponse{}, err
	}

	return acp.UnstableForkSessionResponse{
		SessionId:     id,
		Meta:          sessionResponseMeta(session.snapshot()),
		ConfigOptions: unstableConfigOptions(session.configOptions(ctx)),
	}, nil
}

func (a *Agent) newHermesClient(ctx context.Context, id acp.SessionId, cwd string, meta sessionMeta, existing xdgDirs, mcpServers ...[]acp.McpServer) (hermesClient, error) {
	factory := a.options.clientFactory
	if factory == nil {
		factory = startHermesServer
	}

	env := cloneStringMap(a.options.Env)
	if env == nil && len(meta.Env) > 0 {
		env = map[string]string{}
	}

	for key, value := range meta.Env {
		env[key] = value
	}

	var servers []acp.McpServer
	if len(mcpServers) > 0 {
		servers = cloneMCPServers(mcpServers[0])
	}

	a.observe.RecordHermesProcessStart(ctx)

	return factory(ctx, hermesStartOptions{
		ACPSessionID:   acpSessionIDString(id),
		Root:           a.homeRoot(),
		Cwd:            cwd,
		ExecutablePath: a.options.ExecutablePath,
		DefaultModel:   firstNonEmpty(meta.Model, a.options.DefaultModel),
		Env:            a.observe.InjectTraceEnv(ctx, env),
		Logger:         a.log,
		ExistingXDG:    existing,
		MCPServers:     servers,
		SeedFiles:      cloneStringMap(a.options.SeedFiles),
	})
}

type deleteCleanupRecord struct {
	SessionID acp.SessionId
	NativeID  string
	XDGRoot   string
}

func (a *Agent) deleteCleanupRecord(id acp.SessionId, session *session) deleteCleanupRecord {
	record := deleteCleanupRecord{
		SessionID: id,
		XDGRoot:   filepath.Join(a.homeRoot(), safePathName(string(id))),
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

	if reapLeaseFile(filepath.Join(record.XDGRoot, "state", leaseFileName), a.log) {
		return fmt.Errorf("hermes delete cleanup kept live lease for session %q", record.SessionID)
	}

	return os.RemoveAll(record.XDGRoot)
}

func (a *Agent) homeRoot() string {
	if a.options.Home != "" {
		return a.options.Home
	}

	return filepath.Join(os.TempDir(), valACPGoHermes)
}

// validateUnstableMCPServers applies the same acceptance rules as
// validateMCPServers to the unstable fork variant: reject unsupported
// transports and require a non-empty, request-unique name on every accepted
// (stdio or http) declaration. Indices are preserved by the conversion, so the
// offending-field paths match the wire request.
func validateUnstableMCPServers(servers []acp.UnstableMcpServer) error {
	return validateMCPServers(stableMCPServersFromUnstable(servers))
}

func cloneHermesStateDB(source xdgDirs, target xdgDirs) error {
	data, _, ok, err := encodeHermesStateDBArchive(source.Root)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	return decodeXDGArchive(data, target.Root)
}

func paginateSessionInfos(infos []acp.SessionInfo, cursor *string) ([]acp.SessionInfo, *string, error) {
	start := 0

	if cursor != nil && *cursor != "" {
		parsed, err := strconv.Atoi(*cursor)
		if err != nil || parsed < 0 {
			return nil, nil, acp.NewInvalidParams(map[string]any{keyField: "cursor"})
		}

		start = parsed
	}

	if start >= len(infos) {
		return []acp.SessionInfo{}, nil, nil
	}

	end := start + listSessionsPageSize
	if end > len(infos) {
		end = len(infos)
	}

	var next *string

	if end < len(infos) {
		value := strconv.Itoa(end)
		next = &value
	}

	return infos[start:end], next, nil
}
