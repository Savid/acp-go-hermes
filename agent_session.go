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
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

var reapHermesLeaseFile = nativehermes.ReapLeaseFile
var createHermesGeneration = nativehermes.CreateGenerationXDGDirs
var redactSharedHermesMCPServers = nativehermes.RedactedMCPServers

type sharedSessionSetLease interface {
	Release() error
}

var acquireSharedSessionSetLock = func(
	ctx context.Context,
	home string,
	mode nativehermes.SharedSessionSetLockMode,
) (sharedSessionSetLease, error) {
	return nativehermes.AcquireSharedSessionSetLock(ctx, home, mode)
}

//nolint:gocyclo // NewSession is one ordered launch, journal, and publication transaction.
func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (_ acp.NewSessionResponse, returnErr error) {
	if err := rejectLifecycleMeta(params.Meta); err != nil {
		return acp.NewSessionResponse{}, err
	}

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
	constructionCtx, finishConstruction, constructionErr := a.beginSessionConstruction(ctx)
	if constructionErr != nil {
		return acp.NewSessionResponse{}, constructionErr
	}
	defer finishConstruction()
	ctx = constructionCtx

	client, err := a.newHermesClient(ctx, id, params.Cwd, meta, nativehermes.XDGDirs{}, params.McpServers)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	finalTitle := "acp-go-hermes " + string(id)
	var journal *sessionOperationJournal
	var sessionSetLock sharedSessionSetLease
	sessionPublished := false
	if a.options.SharedHermesHome != "" {
		operationHome := a.options.SharedHermesHome
		sessionSetLock, err = acquireSharedSessionSetLock(ctx, operationHome, nativehermes.SharedSessionSetLockExclusive)
		if err != nil {
			return acp.NewSessionResponse{}, errors.Join(err, closeHermesClientAfterStartupFailure(client))
		}
		defer func() {
			releaseErr := sessionSetLock.Release()
			if sessionPublished && releaseErr != nil {
				a.log.DebugContext(ctx, "release committed Hermes session-set lock", slog.String(jsonFieldError, releaseErr.Error()))

				return
			}
			returnErr = errors.Join(returnErr, releaseErr)
		}()
		if recoveryErr := a.recoverPendingSharedSessionOperations(ctx, operationHome, client); recoveryErr != nil {
			return acp.NewSessionResponse{}, errors.Join(recoveryErr, closeHermesClientAfterStartupFailure(client))
		}
		persisted, listErr := client.PersistedSessions(ctx)
		if listErr != nil {
			return acp.NewSessionResponse{}, errors.Join(fmt.Errorf("list Hermes sessions before create: %w", listErr), closeHermesClientAfterStartupFailure(client))
		}
		baseline := make([]string, 0, len(persisted))
		for index := range persisted {
			baseline = append(baseline, persisted[index].ID)
		}

		operationID, idErr := newSessionOperationID()
		if idErr != nil {
			return acp.NewSessionResponse{}, errors.Join(idErr, closeHermesClientAfterStartupFailure(client))
		}
		origin, identityErr := sessionOperationCurrentProcessIdentity()
		if identityErr != nil {
			return acp.NewSessionResponse{}, errors.Join(identityErr, closeHermesClientAfterStartupFailure(client))
		}
		journal, err = beginSessionOperationJournal(operationHome, sessionOperationJournalFields{
			OperationID: operationID, Kind: sessionOperationKindNew, Mode: sessionOperationModeShared,
			LogicalSessionID: string(id), TargetRoot: client.XDGDirs().Root,
			Marker: operationID, FinalTitle: finalTitle, BaselineNativeSessionIDs: baseline, Origin: origin,
		})
		if err != nil {
			return acp.NewSessionResponse{}, errors.Join(err, closeHermesClientAfterStartupFailure(client))
		}
		mutating := sessionOperationPhaseMutating
		if updateErr := journal.update(sessionOperationJournalPatch{Phase: &mutating}); updateErr != nil {
			return acp.NewSessionResponse{}, errors.Join(updateErr, closeHermesClientAfterStartupFailure(client))
		}
	}

	sessionStarted := time.Now()
	var native nativehermes.Session
	if journal != nil {
		native, err = client.CreateSessionWithDraft(ctx, finalTitle, func(draft nativehermes.SessionDraft) error {
			identified := sessionOperationPhaseNativeIdentified

			return journal.update(sessionOperationJournalPatch{
				Phase: &identified, NativeSessionID: &draft.StoredSessionID, LiveSessionID: &draft.LiveSessionID,
			})
		})
	} else {
		native, err = client.CreateSession(ctx, finalTitle)
	}
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSession, sessionStarted, err)

	if err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.NewSessionResponse{}, errors.Join(err, closeErr)
	}
	if claimErr := a.claimSharedNativeSession(client, native.ID); claimErr != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.NewSessionResponse{}, errors.Join(claimErr, closeErr)
	}

	idmap := idmapRecord{
		SessionID:       string(id),
		NativeSessionID: native.ID,
		Format:          SessionStoreFormat,
	}

	session := newSession(a, id, params.Cwd, params.AdditionalDirectories, params.McpServers, native, client, meta, idmap)
	session.operationJournal = journal
	if streamErr := session.openLifecycleStream(); streamErr != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.NewSessionResponse{}, errors.Join(streamErr, closeErr)
	}
	if err := session.snapshotToStore(context.WithoutCancel(ctx)); err != nil {
		if errors.Is(err, errSessionStoreCommitUnknown) {
			closeErr := session.Close(context.Background())
			a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

			return acp.NewSessionResponse{}, errors.Join(err, closeErr)
		}
		cleanupErr := a.cleanupFailedStartedSession(ctx, session)

		return acp.NewSessionResponse{}, errors.Join(err, cleanupErr)
	}
	if _, err := a.storeStartedSessionWithOpening(ctx, session); err != nil {
		// The exact store bundle is already committed. Close only the live
		// runtime; deleting native state here would invalidate durable metadata
		// that another Agent can safely load.
		return acp.NewSessionResponse{}, a.refuseStartedSession(ctx, session, err)
	}
	sessionPublished = true
	session.operationJournal = nil
	if journal != nil {
		if err := journal.removeCommitted(); err != nil {
			a.log.DebugContext(ctx, "retain committed Hermes session-operation journal for cleanup", slog.String(jsonFieldError, err.Error()))
		}
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
	session, err := a.loadOrResumeSession(ctx, params.SessionId, params.Cwd, params.AdditionalDirectories, params.McpServers, params.Meta, true)
	if err != nil {
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

	session, err := a.loadOrResumeSession(ctx, params.SessionId, params.Cwd, params.AdditionalDirectories, params.McpServers, params.Meta, false)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	return acp.ResumeSessionResponse{
		Meta:          lifecycleResponseMeta(session.snapshot()),
		ConfigOptions: session.configOptions(ctx),
	}, nil
}

//nolint:gocyclo // Load/resume is one ordered hydration, ownership, launch, and publication transaction.
func (a *Agent) loadOrResumeSession(
	ctx context.Context,
	id acp.SessionId,
	cwd string,
	additionalDirectories []string,
	mcpServers []acp.McpServer,
	metaMap map[string]any,
	replay bool,
) (_ *session, returnErr error) {
	if err := rejectLifecycleMeta(metaMap); err != nil {
		return nil, err
	}

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
		a.log.DebugContext(ctx, "retry deleted Hermes session cleanup failed",
			slog.String("classification", "deleted_session_cleanup_failed"))
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
	existing, reuseCtx, releaseReuse, reuseErr := a.beginActiveReuse(ctx, id)
	if reuseErr != nil {
		return nil, reuseErr
	}
	if existing != nil {
		if context.Cause(reuseCtx) != nil {
			releaseReuse()

			return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
		}
		if applyErr := applyActiveLifecycleRequest(existing, cwd, additionalDirectories, mcpServers, meta); applyErr != nil {
			releaseReuse()

			return nil, applyErr
		}
		identity, orderedResponse := ctx.Value(lifecycleRequestIdentityKey{}).(lifecycleRequestIdentity)
		orderedResponse = orderedResponse && identity.token != "" && existing.lifecycleStream() != nil
		if replay && !orderedResponse {
			if replayErr := existing.replayMessages(reuseCtx); replayErr != nil {
				releaseReuse()

				return nil, replayErr
			}
		}
		if _, openErr := a.completeActiveReuse(reuseCtx, id, existing, replay, releaseReuse); openErr != nil {
			releaseReuse()

			return nil, openErr
		}
		if !orderedResponse {
			releaseReuse()
		}

		return existing, nil
	}
	constructionCtx, finishConstruction, constructionErr := a.beginSessionConstruction(ctx)
	if constructionErr != nil {
		return nil, constructionErr
	}
	defer finishConstruction()
	ctx = constructionCtx

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
	hydrate := hydrateStateFromStore
	if a.options.SharedHermesHome != "" {
		hydrate = hydrateStateFromStoreWithoutNativeArchive
	}
	idmap, snapshot, ok, err := hydrate(storeCtx, a.sessionStore(), string(id), xdg)

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

	nativeOwner, err := a.acquireSharedNativeSessionOwner(idmap.NativeSessionID)
	if err != nil {
		return nil, err
	}

	client, err := a.newHermesClientWithScratchOwner(ctx, id, cwd, meta, xdg, scratchRelease, nativeOwner, mcpServers)
	if err != nil {
		if nativeOwner != nil {
			if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
				nativeOwner.Retain()
			} else {
				err = errors.Join(err, nativeOwner.Release())
			}
		}
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
		meta.Model = modelSelectionValue(snapshot.Session.Model.ProviderID, snapshot.Session.Model.ModelID)
	}

	session := newSession(a, id, cwd, additionalDirectories, mcpServers, native, client, meta, idmap)

	session.committed = committedStateFromSnapshot(snapshot)
	if streamErr := session.openLifecycleStream(); streamErr != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return nil, errors.Join(streamErr, closeErr)
	}

	var replayCtx context.Context
	releaseReplay := func() {}
	if replay {
		replayCtx, releaseReplay = session.beginInitialReuse(ctx)
	}

	owed, err := a.storeStartedSessionWithOpening(ctx, session)
	if err != nil {
		releaseReplay()

		return nil, a.refuseStartedSession(ctx, session, err)
	}
	if replay {
		replayErr := session.replayMessages(replayCtx)
		releaseReplay()
		if replayErr != nil {
			a.abandonStreamOpen(owed)
			cleanupErr := a.cleanupFailedLoadedSession(session)

			return nil, errors.Join(replayErr, cleanupErr)
		}
	}

	return session, nil
}

// cleanupFailedLoadedSession retracts a cold load that failed after publication.
// The durable session remains resumable, but this incarnation is fenced,
// deregistered, drained, and contained before the failed request returns.
func (a *Agent) cleanupFailedLoadedSession(session *session) error {
	if a.removeSessionIf(session.id, session) {
		a.observe.AddActiveSession(context.Background(), -1)
	}

	session.lifecycleStream().fence()
	session.prepareClose()
	ctx, cancel := context.WithTimeout(context.Background(), sessionSettlementTimeout)
	waitErr := session.awaitSettlement(ctx)

	session.lifecycleMu.Lock()
	closeErr := session.closeLocked(ctx, false)
	session.lifecycleMu.Unlock()
	cancel()

	a.recordIncompleteContainment(closeErr, session.id, hermesServerRoot(session.client))

	return errors.Join(waitErr, closeErr)
}

// failReuseAfterResponse contains an active session whose post-response replay
// failed. The response has already crossed the wire, so the only truthful
// recovery is to fence and retract that incarnation.
func (s *session) failReuseAfterResponse(replayErr error) {
	s.lifecycleStream().fence()
	s.prepareClose()
	ctx, cancel := context.WithTimeout(context.Background(), sessionSettlementTimeout)
	waitErr := s.awaitSettlement(ctx)

	s.lifecycleMu.Lock()
	closeErr := s.closeLocked(ctx, false)
	s.lifecycleMu.Unlock()
	cancel()

	if s.agent.removeSessionIf(s.id, s) {
		s.agent.observe.AddActiveSession(context.Background(), -1)
	}
	s.agent.recordIncompleteContainment(closeErr, s.id, hermesServerRoot(s.client))
	s.agent.log.DebugContext(context.Background(), "contain Hermes session after replay failure",
		slog.String("classification", "session_replay_failed"),
		slog.Bool("replay_failure", replayErr != nil),
		slog.Bool("settlement_failure", waitErr != nil),
		slog.Bool("containment_failure", closeErr != nil),
	)
}

// resumeRuntimeForTurnLocked rebuilds a fenced session runtime from the last
// committed store snapshot. toolMu and cancelMu are held by the caller, so the
// old prompt has fully released the per-session token and Close cannot cross
// installation of the replacement runtime.
//
//nolint:gocyclo // Runtime replacement keeps every fail-closed identity, hydration, ownership, and publication branch in one transaction.
func (s *session) resumeRuntimeForTurnLocked(ctx context.Context) (returnErr error) {
	s.mu.Lock()
	needsResume := s.runtimeNeedsResume
	resumeWait := s.runtimeResumeWait
	resumeErr := s.runtimeResumeErr
	closed := s.closed || s.lifecycleClosing
	poisonErr := s.poisonedErrorLocked()
	id := s.id
	cwd := s.cwd
	mcpServers := cloneMCPServers(s.mcpServers)
	wantIDMap := s.idmap
	meta := sessionMeta{
		Model:         modelSelectionValue(s.providerID, s.modelID),
		Env:           cloneStringMap(s.env),
		ExtraPathDirs: slices.Clone(s.extraPathDirs),
		RawMessages:   s.rawMessages,
	}
	s.mu.Unlock()

	if needsResume && resumeWait != nil {
		select {
		case <-resumeWait:
		case <-ctx.Done():
			return ctx.Err()
		}

		s.mu.Lock()
		resumeErr = s.runtimeResumeErr
		poisonErr = s.poisonedErrorLocked()
		closed = s.closed || s.lifecycleClosing
		s.mu.Unlock()
	}

	if poisonErr != nil {
		return poisonErr
	}

	if closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
	}

	if !needsResume {
		return nil
	}
	if resumeErr != nil {
		return s.poisonWithError(ctx, "hermes_runtime_resume_failed", resumeErr.Error())
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
	hydrate := hydrateStateFromStore
	if s.agent.options.SharedHermesHome != "" {
		hydrate = hydrateStateFromStoreWithoutNativeArchive
	}
	idmap, snapshot, ok, err := hydrate(storeCtx, s.agent.sessionStore(), string(id), xdg)

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

	nativeOwner, err := s.agent.acquireSharedNativeSessionOwner(wantIDMap.NativeSessionID)
	if err != nil {
		return err
	}

	client, err := s.agent.newHermesClientWithScratchOwner(ctx, id, cwd, meta, xdg, scratchRelease, nativeOwner, mcpServers)
	if err != nil {
		if nativeOwner != nil {
			if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
				nativeOwner.Retain()
			} else {
				err = errors.Join(err, nativeOwner.Release())
			}
		}
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

	// Build the replacement lifecycle identity while it is still private. Its
	// snapshot and native projection remain gated until the predecessor pump is
	// joined and the complete successor tuple is installed.
	replacementStream, err := s.prepareLifecycleStream(false)
	if err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		s.agent.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return errors.Join(err, closeErr)
	}
	if err := s.detachPumpContext(ctx); err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		s.agent.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return errors.Join(err, closeErr)
	}
	s.resetProjectionGate()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		closeErr := closeHermesClientAfterStartupFailure(client)
		s.agent.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return errors.Join(acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed}), closeErr)
	}

	s.client = client
	s.committed = committedStateFromSnapshot(snapshot)
	s.mcpReloadComplete = false
	s.startPump(client)
	s.runtimeNeedsResume = false
	s.runtimeResumeWait = nil
	s.runtimeResumeErr = nil
	// Publish the stream last. Any reader that can observe it therefore also
	// observes the already-installed client, committed snapshot, pump, and
	// cleared resume latch; projection is still gated until publishProjection.
	s.streamMu.Lock()
	s.stream = replacementStream
	s.streamMu.Unlock()
	s.mu.Unlock()

	if err := replacementStream.ensureLifecycleOpened(ctx); err != nil {
		replacementStream.fence()
		closeErr := closeHermesClientAfterStartupFailure(client)
		pumpCtx, pumpCancel := context.WithTimeout(context.Background(), closeTimeout)
		pumpErr := s.detachPumpContext(pumpCtx)
		pumpCancel()
		failure := errors.Join(err, closeErr, pumpErr)
		s.agent.recordIncompleteContainment(failure, id, hermesServerRoot(client))

		s.mu.Lock()
		if s.client == client {
			s.runtimeNeedsResume = true
			s.runtimeResumeWait = nil
			s.runtimeResumeErr = failure
		}
		s.mu.Unlock()

		return failure
	}

	replacementStream.publishProjection()

	return nil
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
		return lifecycleMismatch(hermesEnvOptionPath)
	}

	if !slices.Equal(snapshot.extraPathDirs, meta.ExtraPathDirs) {
		return lifecycleMismatch(hermesExtraPathDirsOptionPath)
	}

	// A shape refusal, not a value gate. The gate this door used to carry asked
	// a value question — is this the model already bound? — and answered no for
	// every model Hermes would have taken. This asks only whether the string can
	// name a selection at all, and it has to: an active resume sends nothing to
	// Hermes, so the value survives as this session's provider and model and
	// nothing else. An unqualified one splits into no provider, leaves a nil
	// selector, and lets the next prompt run on the previously bound model while
	// the config option echoes back what the host asked for — a silent wrong
	// answer where the config door, reading the same predicate before its own
	// native call, refuses. Session creation refuses nothing here: session.create
	// carries model and provider as separate fields, so an unqualified value is
	// representable there and Hermes resolves it itself.
	if meta.Model != "" && nativehermes.ModelSelectionShapeError(meta.Model) != nil {
		return unsupportedField(hermesModelOptionPath)
	}

	existing.mu.Lock()
	if meta.Model != "" {
		existing.providerID, existing.modelID = splitModelValue(meta.Model, "", "")
	}
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
	if err := rejectLifecycleMeta(params.Meta); err != nil {
		return acp.ListSessionsResponse{}, err
	}

	if err := a.ensureOpen(); err != nil {
		return acp.ListSessionsResponse{}, err
	}

	if err := a.retryDeletedSessionCleanup(ctx); err != nil {
		a.log.DebugContext(ctx, "retry deleted Hermes session cleanup failed",
			slog.String("classification", "deleted_session_cleanup_failed"))
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
	if err := rejectLifecycleMeta(params.Meta); err != nil {
		return acp.CloseSessionResponse{}, err
	}

	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	// Close stops admitting prompts, then waits for the turn in flight to settle
	// wholly. The wait is the boundary: a close that returned while a commit or
	// a terminal emission was still owed would report a contained session over
	// durable state nobody had finished writing. The settlement's own verdict
	// was already delivered to the prompt that produced it, so it is not
	// re-reported here.
	session.prepareClose()
	waitErr := session.awaitSettlement(ctx)

	session.lifecycleMu.Lock()
	closeErr := session.settleClosedSession(ctx)
	session.lifecycleMu.Unlock()
	a.recordIncompleteContainment(closeErr, params.SessionId, session.client.XDGDirs().Root)

	// The id is detached only by a close that completed its boundary. A close
	// that failed one — a tree it could not prove contained, a commit the store
	// refused — has left work owed on this session, and dropping the id would
	// leave that work with no name to reach it by: the host would be answered
	// unknown_session on the very retry the failure asks for. The session stays
	// addressable, admits no further prompt, and a later close runs the boundary
	// again.
	if err := errors.Join(waitErr, closeErr); err != nil {
		return acp.CloseSessionResponse{}, err
	}

	if a.removeSessionIf(params.SessionId, session) {
		a.observe.AddActiveSession(ctx, -1)
	}

	return acp.CloseSessionResponse{}, nil
}

func (a *Agent) UnstableDeleteSession(ctx context.Context, params acp.UnstableDeleteSessionRequest) (acp.UnstableDeleteSessionResponse, error) {
	if err := rejectLifecycleMeta(params.Meta); err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	ctx = a.observe.Extract(ctx, params.Meta)

	if params.SessionId == "" {
		return acp.UnstableDeleteSessionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldSessionID: validationRequired})
	}

	if err := a.retryDeletedSessionCleanup(ctx); err != nil {
		a.log.DebugContext(ctx, "retry deleted Hermes session cleanup failed",
			slog.String("classification", "deleted_session_cleanup_failed"))
	}

	a.mu.Lock()
	session := a.sessions[params.SessionId]
	a.mu.Unlock()

	record := a.deleteCleanupRecord(params.SessionId, session)

	// The tombstone is the first thing this delete does. An active turn is
	// something delete cancels and settles, never a ground to refuse on, so
	// nothing about the session's state is inspected ahead of the durable write
	// that makes the id unaddressable.
	installed, err := a.tombstoneSession(ctx, params.SessionId, session)
	if err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	if record.SessionID != "" {
		a.rememberDeleteCleanup(record)
	}

	var settleErr error

	if session != nil {
		settleErr, err = a.closeDeletedSession(ctx, params.SessionId, session, record.XDGRoot)
	}

	if installed != nil && installed != session {
		// A load or resume that passed its own tombstone check installed in the
		// window between the map read above and this tombstone. The id names
		// nothing now, so nothing else will ever reach that runtime: this delete
		// owns its teardown exactly as it owns the one it read.
		lateSettleErr, lateErr := a.closeDeletedSession(ctx, params.SessionId, installed, hermesServerRoot(installed.client))
		settleErr = errors.Join(settleErr, lateSettleErr)
		err = errors.Join(err, lateErr)
	}

	if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
		a.mu.Lock()
		delete(a.deleteCleanup, record.SessionID)
		a.mu.Unlock()

		return acp.UnstableDeleteSessionResponse{}, err
	}

	cleanupErr := a.cleanupDeletedSession(record)
	a.forgetDeleteCleanupIfDone(record.SessionID)

	return acp.UnstableDeleteSessionResponse{}, errors.Join(settleErr, err, cleanupErr)
}

// tombstoneSession makes one delete durable and hides the id with it, before
// the delete cancels anything or tears anything down. The whole step runs under
// the session's lifecycle barrier, which is the same barrier every durable
// commit holds: a settlement already publishing finishes first and the tombstone
// removes what it wrote, and a settlement that has not started yet finds the id
// tombstoned and publishes nothing. Neither order lets a late write recreate a
// row the tombstone cleared.
//
// A store that refuses the write leaves the session fully addressable. There is
// no tombstone, so there is nothing to hide behind, and the delete's idempotence
// is what makes a retry the right answer.
//
// The session the id actually named at that instant is returned, which is not
// always the one the delete read: an install that won the race to the lock is
// still live, and nothing but this delete can reach it once the marker is set.
func (a *Agent) tombstoneSession(ctx context.Context, id acp.SessionId, session *session) (*session, error) {
	if session != nil {
		session.lifecycleMu.Lock()
		defer session.lifecycleMu.Unlock()
	}

	storeCtx, cancel := sessionStoreWriteContext(ctx)
	err := a.sessionStore().Delete(storeCtx, SessionKey{SessionID: string(id)})

	cancel()

	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	installed := a.sessions[id]

	delete(a.sessions, id)

	a.deleted[id] = struct{}{}
	a.mu.Unlock()

	return installed, nil
}

// closeDeletedSession runs the shutdown ladder one deleted session owes. Delete
// serializes after settlement: admission closes, the turn in flight is
// cancelled, and the settlement it owes completes before the teardown touches
// the runtime that settlement is still writing through. The commit that
// settlement makes lands on a tombstoned id, so it publishes nothing and cannot
// recreate the row this delete removed.
func (a *Agent) closeDeletedSession(
	ctx context.Context,
	id acp.SessionId,
	session *session,
	root string,
) (settleErr error, closeErr error) {
	session.prepareDelete()

	settleErr = session.awaitSettlement(ctx)

	session.lifecycleMu.Lock()

	closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
	closeErr = session.closeLocked(closeCtx, true)

	closeCancel()
	// A deleted session has no resumable snapshot to commit, so it states no
	// quiescence fact: the row this delete tombstoned is exactly the state a fact
	// would have to stand on. The incarnation still ends here.
	session.lifecycleStream().fence()
	session.lifecycleMu.Unlock()
	a.recordIncompleteContainment(closeErr, id, root)
	a.observe.AddActiveSession(ctx, -1)

	return settleErr, closeErr
}

//nolint:gocyclo // Fork is one ordered parent/branch/journal/store publication transaction.
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
	constructionCtx, finishConstruction, constructionErr := a.beginSessionConstruction(ctx)
	if constructionErr != nil {
		return acp.UnstableForkSessionResponse{}, constructionErr
	}
	defer finishConstruction()
	ctx = constructionCtx

	parent, err := a.session(params.SessionId)
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
	servers := stableMCPServersFromUnstable(params.McpServers)
	if admissionErr := a.admitSharedHermesConfig(servers); admissionErr != nil {
		return acp.UnstableForkSessionResponse{}, admissionErr
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

	parent.lifecycleMu.Lock()
	defer parent.lifecycleMu.Unlock()
	if parent.lifetimeEnded() {
		return acp.UnstableForkSessionResponse{}, acp.NewInvalidRequest(map[string]any{jsonFieldError: valSessionClosed})
	}
	if reason := parent.snapshotBlockedReason(); reason != "" {
		return acp.UnstableForkSessionResponse{}, acp.NewInvalidRequest(map[string]any{jsonFieldError: "session cannot be forked", "reason": reason})
	}
	parentSnapshot := parent.snapshot()
	if meta.Model == "" {
		meta.Model = modelSelectionValue(parentSnapshot.providerID, parentSnapshot.modelID)
	}

	var journal *sessionOperationJournal
	var sessionSetLock sharedSessionSetLease
	sessionPublished := false
	baseline := []string{}
	marker := ""
	if a.options.SharedHermesHome != "" {
		operationHome := a.options.SharedHermesHome
		sessionSetLock, err = acquireSharedSessionSetLock(ctx, operationHome, nativehermes.SharedSessionSetLockExclusive)
		if err != nil {
			return acp.UnstableForkSessionResponse{}, err
		}
		defer func() {
			releaseErr := sessionSetLock.Release()
			if sessionPublished && releaseErr != nil {
				a.log.DebugContext(ctx, "release committed Hermes fork session-set lock", slog.String(jsonFieldError, releaseErr.Error()))

				return
			}
			returnErr = errors.Join(returnErr, releaseErr)
		}()
		if recoveryErr := a.recoverPendingSharedSessionOperations(ctx, operationHome, parentSnapshot.client); recoveryErr != nil {
			return acp.UnstableForkSessionResponse{}, recoveryErr
		}

		lister, ok := parentSnapshot.client.(nativehermes.PersistedSessionLister)
		if !ok {
			return acp.UnstableForkSessionResponse{}, errors.New("shared Hermes fork requires persisted session inventory")
		}
		persisted, listErr := lister.PersistedSessions(ctx)
		if listErr != nil {
			return acp.UnstableForkSessionResponse{}, fmt.Errorf("list Hermes sessions before fork: %w", listErr)
		}
		baseline = make([]string, 0, len(persisted))
		for index := range persisted {
			baseline = append(baseline, persisted[index].ID)
		}
		operationID, idErr := newSessionOperationID()
		if idErr != nil {
			return acp.UnstableForkSessionResponse{}, idErr
		}
		origin, identityErr := sessionOperationCurrentProcessIdentity()
		if identityErr != nil {
			return acp.UnstableForkSessionResponse{}, identityErr
		}
		marker = "acp-go-hermes fork " + operationID
		journal, err = beginSessionOperationJournal(operationHome, sessionOperationJournalFields{
			OperationID: operationID, Kind: sessionOperationKindFork, Mode: sessionOperationModeShared,
			LogicalSessionID: string(id), ParentLogicalSessionID: string(params.SessionId),
			ParentNativeSessionID: parentSnapshot.idmap.NativeSessionID,
			SourceRoot:            parentSnapshot.client.XDGDirs().Root, TargetRoot: xdg.Root,
			Marker: marker, FinalTitle: marker, BaselineNativeSessionIDs: baseline, Origin: origin,
		})
		if err != nil {
			return acp.UnstableForkSessionResponse{}, err
		}
		mutating := sessionOperationPhaseMutating
		if updateErr := journal.update(sessionOperationJournalPatch{Phase: &mutating}); updateErr != nil {
			return acp.UnstableForkSessionResponse{}, updateErr
		}
	}

	var nativeChild nativehermes.Session
	if forker, ok := parentSnapshot.client.(nativehermes.RecoverableSessionForker); ok && journal != nil {
		nativeChild, err = forker.ForkWithBaseline(ctx, parentSnapshot.idmap.NativeSessionID, marker, baseline)
	} else {
		nativeChild, err = parentSnapshot.client.Fork(ctx, parentSnapshot.idmap.NativeSessionID, marker)
	}
	if err != nil {
		if journal != nil {
			if lister, ok := parentSnapshot.client.(nativehermes.PersistedSessionLister); ok {
				persisted, listErr := lister.PersistedSessions(context.WithoutCancel(ctx))
				current := make([]string, 0, len(persisted))
				for index := range persisted {
					current = append(current, persisted[index].ID)
				}
				_, found, deltaErr := sessionOperationBaselineDelta(baseline, current)
				if listErr == nil && deltaErr == nil && !found {
					return acp.UnstableForkSessionResponse{}, errors.Join(err, journal.removeRecovered())
				}
			}
		}

		return acp.UnstableForkSessionResponse{}, err
	}
	if journal != nil {
		identified := sessionOperationPhaseLiveFenced
		if updateErr := journal.update(sessionOperationJournalPatch{Phase: &identified, NativeSessionID: &nativeChild.ID}); updateErr != nil {
			return acp.UnstableForkSessionResponse{}, updateErr
		}
	}
	cleanupNativeChild := true
	defer func() {
		if !cleanupNativeChild {
			return
		}
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), closeTimeout)
		deleteErr := parentSnapshot.client.DeleteSession(deleteCtx, nativeChild.ID)
		deleteCancel()
		returnErr = errors.Join(returnErr, deleteErr)
	}()
	if a.options.SharedHermesHome == "" {
		// The official branch commits its child row and copied history into the
		// parent's state.db. Isolated mode must snapshot that authoritative DB
		// only after Branch returns and its parent-side live child is detached.
		if cloneErr := cloneHermesStateDB(a.options.ScratchDir, parentSnapshot.client.XDGDirs(), xdg); cloneErr != nil {
			return acp.UnstableForkSessionResponse{}, cloneErr
		}
	}

	nativeOwner, err := a.acquireSharedNativeSessionOwner(nativeChild.ID)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	client, err := a.newHermesClientWithScratchOwner(ctx, id, params.Cwd, meta, xdg, scratchRelease, nativeOwner, servers)
	if err != nil {
		if nativeOwner != nil {
			if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
				nativeOwner.Retain()
			} else {
				err = errors.Join(err, nativeOwner.Release())
			}
		}
		if errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			cleanupNativeChild = false
			keepScratch = true

			a.retainIncompleteHermesRoot(id, xdg.Root)
		}

		return acp.UnstableForkSessionResponse{}, err
	}
	keepScratch = true

	native, err := client.GetSession(ctx, nativeChild.ID)
	if err != nil {
		closeErr := closeHermesClientAfterStartupFailure(client)
		if errors.Is(closeErr, nativehermes.ErrProcessContainmentIncomplete) {
			cleanupNativeChild = false
		}
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.UnstableForkSessionResponse{}, errors.Join(err, closeErr)
	}
	if native.ID != nativeChild.ID {
		closeErr := closeHermesClientAfterStartupFailure(client)
		if errors.Is(closeErr, nativehermes.ErrProcessContainmentIncomplete) {
			cleanupNativeChild = false
		}
		a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

		return acp.UnstableForkSessionResponse{}, errors.Join(fmt.Errorf("hermes fork native session drift: got %q want %q", native.ID, nativeChild.ID), closeErr)
	}
	// The child server may start with a process-wide default that differs from
	// the parent's current (or explicitly requested) model. Bind the resolved
	// selection natively before the child can be snapshotted or published, so
	// its response, durable state, and first prompt all describe one model.
	if meta.Model != "" {
		if err := client.SetModel(ctx, nativeChild.ID, meta.Model); err != nil {
			closeErr := closeHermesClientAfterStartupFailure(client)
			if errors.Is(closeErr, nativehermes.ErrProcessContainmentIncomplete) {
				cleanupNativeChild = false
			}
			a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

			return acp.UnstableForkSessionResponse{}, errors.Join(fmt.Errorf("bind Hermes fork model: %w", err), closeErr)
		}
	}

	idmap := idmapRecord{
		SessionID:             string(id),
		NativeSessionID:       native.ID,
		ParentSessionID:       string(params.SessionId),
		NativeParentSessionID: parentSnapshot.idmap.NativeSessionID,
		Format:                SessionStoreFormat,
	}

	session := newSession(a, id, params.Cwd, params.AdditionalDirectories, servers, native, client, meta, idmap)
	session.operationJournal = journal
	if err := session.openLifecycleStream(); err != nil {
		cleanupErr := a.cleanupFailedStartedSession(ctx, session)
		if errors.Is(cleanupErr, nativehermes.ErrProcessContainmentIncomplete) {
			cleanupNativeChild = false
		}

		return acp.UnstableForkSessionResponse{}, errors.Join(err, cleanupErr)
	}

	if err := session.snapshotToStore(context.WithoutCancel(ctx)); err != nil {
		if errors.Is(err, errSessionStoreCommitUnknown) {
			cleanupNativeChild = false
			closeErr := session.Close(context.Background())
			a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

			return acp.UnstableForkSessionResponse{}, errors.Join(err, closeErr)
		}
		cleanupErr := a.cleanupFailedStartedSession(ctx, session)
		if errors.Is(cleanupErr, nativehermes.ErrProcessContainmentIncomplete) {
			cleanupNativeChild = false
		}

		return acp.UnstableForkSessionResponse{}, errors.Join(err, cleanupErr)
	}
	cleanupNativeChild = false
	if _, err := a.storeStartedSessionWithOpening(ctx, session); err != nil {
		return acp.UnstableForkSessionResponse{}, a.refuseStartedSession(ctx, session, err)
	}
	sessionPublished = true
	session.operationJournal = nil
	if journal != nil {
		if err := journal.removeCommitted(); err != nil {
			a.log.DebugContext(ctx, "retain committed Hermes fork journal for cleanup", slog.String(jsonFieldError, err.Error()))
		}
	}

	return acp.UnstableForkSessionResponse{
		SessionId:     id,
		Meta:          lifecycleResponseMeta(session.snapshot()),
		ConfigOptions: unstableConfigOptions(session.configOptions(ctx)),
	}, nil
}

func (a *Agent) newHermesClient(ctx context.Context, id acp.SessionId, cwd string, meta sessionMeta, existing nativehermes.XDGDirs, mcpServers ...[]acp.McpServer) (*managedHermesServer, error) {
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

func (a *Agent) newHermesClientWithScratch(ctx context.Context, id acp.SessionId, cwd string, meta sessionMeta, existing nativehermes.XDGDirs, scratchRelease func(), mcpServers ...[]acp.McpServer) (*managedHermesServer, error) {
	return a.newHermesClientWithScratchOwner(ctx, id, cwd, meta, existing, scratchRelease, nil, mcpServers...)
}

func (a *Agent) newHermesClientWithScratchOwner(ctx context.Context, id acp.SessionId, cwd string, meta sessionMeta, existing nativehermes.XDGDirs, scratchRelease func(), nativeOwner *nativehermes.SharedSessionOwner, mcpServers ...[]acp.McpServer) (*managedHermesServer, error) {
	residence := providerAuthResidence(a.options)
	if residence != "" {
		if err := validateNativeOwnedDirectory(residence, a.options.ProcessIsolation); err != nil {
			return nil, err
		}
	}

	factory := a.options.clientFactory
	if factory == nil {
		factory = nativehermes.StartServer
	}

	baseEnv := cloneStringMap(a.options.Env)

	sessionEnv := cloneStringMap(meta.Env)
	sessionEnv = a.observe.InjectTraceEnv(ctx, sessionEnv)

	var servers []acp.McpServer
	if len(mcpServers) > 0 {
		servers = cloneMCPServers(mcpServers[0])
	}
	if err := a.admitSharedHermesConfig(servers); err != nil {
		return nil, err
	}

	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return nil, err
	}

	start := nativehermes.StartOptions{
		ACPSessionID:             nativehermes.ACPSessionIDString(id),
		ScratchParent:            parent,
		Cwd:                      cwd,
		ExecutablePath:           a.options.ExecutablePath,
		DefaultModel:             firstNonEmpty(meta.Model, a.options.DefaultModel),
		SharedHermesHome:         a.options.SharedHermesHome,
		SharedNativeSessionOwner: nativeOwner,
		Env:                      baseEnv,
		SessionEnv:               sessionEnv,
		ExtraPathDirs:            slices.Clone(meta.ExtraPathDirs),
		Isolation:                nativeProcessIsolation(a.options.ProcessIsolation, a.options.testOnlyNoCredential, a.options.testOnlyIdentityLockRoot),
		// The ambient snapshot travels alongside the policy rather than inside
		// it, so an omitted policy stays nil the whole way to the launch
		// boundary and no ProcessIsolation value is manufactured for it.
		AmbientEnvironment: cloneStringMap(a.ambientEnv),
		Logger:             a.log,
		ExistingXDG:        existing,
		MCPServers:         servers,
		SeedFiles:          cloneStringMap(a.options.SeedFiles),
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
	}

	nativeRelease, err := acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}

	a.observe.RecordHermesProcessStart(ctx)
	processRoot := a.processes.register()

	client, err := factory(ctx, start)
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

	managed := &managedHermesServer{
		Server:                client,
		root:                  root,
		sessionID:             id,
		nativeRelease:         nativeRelease,
		scratchRelease:        scratchRelease,
		retainIncomplete:      a.recordIncompleteContainment,
		processRoot:           processRoot,
		providerAuthSupported: a.options.SharedHermesHome != "",
		nativeSessionOwner:    nativeOwner,
	}
	if err := nativehermes.BindSharedSessionOwnerToServer(nativeOwner, client); err != nil {
		return nil, errors.Join(err, managed.Close(context.Background()))
	}

	return managed, nil
}

func (a *Agent) admitSharedHermesConfig(servers []acp.McpServer) error {
	if a.options.SharedHermesHome == "" {
		return nil
	}
	redacted, err := redactSharedHermesMCPServers(servers)
	if err != nil {
		return err
	}

	a.sharedConfigMu.Lock()
	defer a.sharedConfigMu.Unlock()

	if !a.sharedConfigInitialized {
		a.sharedConfigInitialized = true
		a.sharedMCPServers = cloneMCPServers(redacted)

		return nil
	}
	if !reflect.DeepEqual(a.sharedMCPServers, redacted) {
		return errors.New("shared Hermes home requires one stable MCP configuration")
	}

	return nil
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
		IdentityLock:             isolation.IdentityLock,
		AuthorityDomain:          isolation.AuthorityDomain,
		StandaloneOwnerID:        isolation.StandaloneOwnerID,
		StandaloneStateRoot:      isolation.StandaloneStateRoot,
	}
}

type deleteCleanupRecord struct {
	SessionID acp.SessionId
	NativeID  string
	XDGRoot   string
}

// deleteCleanupRecord names what a delete still owes the filesystem. The root
// is the one the session's own runtime holds: a generation root is minted per
// incarnation under the scratch parent and is not derivable from the session
// id, so a session with no live client leaves nothing to remove.
func (a *Agent) deleteCleanupRecord(id acp.SessionId, session *session) deleteCleanupRecord {
	record := deleteCleanupRecord{SessionID: id}
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

	a.mu.Lock()
	record, remembered := a.deleteCleanup[id]
	a.mu.Unlock()

	if !remembered {
		return
	}

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

// rejectInvalidConfiguration fails session establishment on agent configuration
// no session may run under: an option that failed validation at construction
// or a configured Home value. The
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
