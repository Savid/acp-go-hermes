//nolint:tagliatelle,goconst // Store metadata preserves Hermes spellings and one ordered commit transaction.
package hermesacp

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/klauspost/compress/zstd"
	_ "modernc.org/sqlite"
)

const (
	maxHydrateFileBytes int64 = 128 * 1024 * 1024
	archiveChunkBytes         = 256 * 1024
)

// sessionStoreWriteTimeout bounds session-store writes (snapshot Replace
// commits and session/delete tombstones). SessionStoreLoadTimeout bounds
// store reads only: a slow-but-successful write never fails the operation
// just because it outlived the read budget.
const sessionStoreWriteTimeout = 60 * time.Second

var errSessionStoreCommitUnknown = errors.New("session store commit outcome is unknown")

func sessionStoreWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, sessionStoreWriteTimeout)
}

// State-db archive and snapshot vocabulary for the hermes-state-db-v1 format.
const (
	archiveEncodingTarZstdBase64 = "tar+zstd+base64"
	reasonTurn                   = "turn"
	reasonPermission             = "permission"
	reasonGeneration             = "generation"
	fileStateDB                  = "state.db"
	fileStateDBSHM               = "state.db-shm"
	fileStateDBWAL               = "state.db-wal"
	tableAccount                 = "account"
	valSecret                    = "secret"
)

type archiveTarWriter interface {
	io.Writer
	WriteHeader(*tar.Header) error
	Close() error
}

type archiveZstdWriter interface {
	io.Writer
	Close() error
}

var (
	stateJSONMarshal    = json.Marshal
	stateWalkDir        = filepath.WalkDir
	stateRel            = filepath.Rel
	stateLstat          = os.Lstat
	stateFileInfoHeader = tar.FileInfoHeader
	stateNewTarWriter   = func(w io.Writer) archiveTarWriter { return tar.NewWriter(w) }
	stateOpen           = func(name string) (io.ReadCloser, error) { return os.Open(name) }
	stateCopy           = io.Copy
	stateNewZstdWriter  = func(w io.Writer) (archiveZstdWriter, error) {
		return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	}
	stateNewZstdReader = func(r io.Reader) (*zstd.Decoder, error) { return zstd.NewReader(r) }
	stateRemoveAll     = os.RemoveAll
	stateMkdirAll      = os.MkdirAll
	stateAbs           = filepath.Abs
	stateOpenFile      = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
		return os.OpenFile(name, flag, perm)
	}
	stateCopyN                       = io.CopyN
	stateMkdirTemp                   = os.MkdirTemp
	stateStat                        = os.Stat
	stateReadFile                    = os.ReadFile
	stateCopyFile                    = copyFile
	stateSQLiteArchiveContent        = sqliteArchiveContent
	stateScrubSQLiteCredentialTables = scrubSQLiteCredentialTables
	stateSQLOpen                     = sql.Open
)

type idmapRecord struct {
	SessionID             string `json:"sessionId"`
	NativeSessionID       string `json:"nativeSessionId"`
	ParentSessionID       string `json:"parentSessionId,omitempty"`
	NativeParentSessionID string `json:"nativeParentSessionId,omitempty"`
	Format                string `json:"format"`
	CreatedAtUnixMilli    int64  `json:"createdAtUnixMilli"`
	UpdatedAtUnixMilli    int64  `json:"updatedAtUnixMilli"`
}

type stateSnapshot struct {
	Format              string                 `json:"format"`
	CapturedAtUnixMilli int64                  `json:"capturedAtUnixMilli"`
	Session             stateSnapshotSession   `json:"session"`
	Terminal            *stateSnapshotTerminal `json:"terminal"`
	Archives            map[string]archiveInfo `json:"archives"`
	Wrapper             *stateSnapshotWrapper  `json:"wrapper"`
}

type stateSnapshotTerminal struct {
	MessageID string `json:"messageId"`
	Role      string `json:"role"`
	Finish    string `json:"finish"`
}

type stateSnapshotSession struct {
	SessionID             string             `json:"sessionId"`
	NativeSessionID       string             `json:"nativeSessionId"`
	ParentSessionID       string             `json:"parentSessionId,omitempty"`
	NativeParentSessionID string             `json:"nativeParentSessionId,omitempty"`
	Cwd                   string             `json:"cwd"`
	Title                 string             `json:"title"`
	Model                 stateSnapshotModel `json:"model"`
	Env                   map[string]string  `json:"env"`
	ExtraPathDirs         []string           `json:"extraPathDirs"`
}

type stateSnapshotModel struct {
	ProviderID string `json:"providerID,omitempty"`
	ModelID    string `json:"modelID,omitempty"`
}

type archiveInfo struct {
	Subpath string `json:"subpath"`
	SHA256  string `json:"sha256"`
	Bytes   int    `json:"bytes"`
}

type stateSnapshotWrapper struct {
	// Foreground is the adapter's own record of how the last accepted turn
	// ended. It lives in the wrapper section rather than in Terminal because
	// Terminal is the native archive's completed assistant identity: a turn that
	// failed or was cancelled produced no finished assistant row, and writing one
	// there would put a conversation entry in the archive that never happened.
	// Absent means no accepted turn has settled in this session yet.
	Foreground *stateSnapshotForeground `json:"foreground"`
}

// stateSnapshotForeground records one settled foreground cycle truthfully: which
// incarnation and turn it was, how it ended, and the largest prefix of it this
// wrapper can state. Text is what actually streamed to the host, so a failed or
// cancelled turn keeps the visible work it produced without claiming the native
// tail behind it completed.
type stateSnapshotForeground struct {
	StreamID            string `json:"streamId"`
	TurnID              string `json:"turnId"`
	Outcome             string `json:"outcome"`
	StopReason          string `json:"stopReason,omitempty"`
	MessageID           string `json:"messageId,omitempty"`
	Text                string `json:"text,omitempty"`
	CapturedAtUnixMilli int64  `json:"capturedAtUnixMilli"`
}

type archiveEntry struct {
	Format   string `json:"format"`
	Encoding string `json:"encoding"`
	Sequence int    `json:"sequence"`
	Final    bool   `json:"final"`
	SHA256   string `json:"sha256"`
	Data     string `json:"data"`
}

// terminalSnapshotRequirement is the durable foreground-prefix commit one
// accepted turn owes. Every accepted exit carries one, so the store records the
// truthful outcome of the turn whether it completed, failed, or was cancelled.
type terminalSnapshotRequirement struct {
	baseline   SessionStoreTerminalState
	turnEpoch  uint64
	foreground stateSnapshotForeground
	// completed reports that the turn produced a new finished native assistant
	// row, so the commit must observe the native terminal identity advance. A
	// failed or cancelled turn advances nothing and states so.
	completed bool
	// nativeUnavailable reports that an incarnation-ending boundary already
	// contained the generation, so the commit
	// restates the last identity this session durably holds instead of reading a
	// process that is gone. The zero value keeps ordinary captures readable.
	nativeUnavailable bool
	settlementCapture bool
}

func (s *session) snapshotToStore(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	return s.snapshotToStoreLocked(ctx, nil)
}

// snapshotToStoreLocked captures one generation and publishes it in one step.
// Managed capture settles and reclaims its native residence before reading the
// state database; ordinary capture reads its same-identity residence directly.
func (s *session) snapshotToStoreLocked(
	ctx context.Context,
	requirement *terminalSnapshotRequirement,
) error {
	commit, err := s.captureSnapshotLocked(ctx, requirement)
	if err != nil || commit == nil {
		return err
	}

	return s.publishSnapshotLocked(ctx, commit)
}

// sessionStoreCommit is one captured generation awaiting publication.
type sessionStoreCommit struct {
	mainKey      SessionKey
	replacements []SessionStoreReplacement
	terminal     SessionStoreTerminalState
	native       *stateSnapshotTerminal
	foreground   *stateSnapshotForeground
	archives     map[string]archiveInfo
	managedRoot  string
	managed      *managedHermesServer
	managedMain  *stateSnapshot
	managedState SessionKey
	managedReady []SessionStoreReplacement
	// deadline bounds the store write, carried from the capture so a captured
	// generation cannot be published under an unbounded context.
	deadline time.Duration
}

// captureSnapshotLocked builds the generation to publish. A nil commit with no
// error means there was nothing to capture, which is the ordinary answer for a
// session whose runtime is already gone and which owes no terminal boundary.
//
//nolint:gocyclo // Snapshot construction keeps one atomic generation shape visible.
func (s *session) captureSnapshotLocked(
	ctx context.Context,
	requirement *terminalSnapshotRequirement,
) (*sessionStoreCommit, error) {
	if requirement != nil {
		s.mu.Lock()
		cancelled := s.turnSettlement == turnSettlementCancelled
		s.mu.Unlock()

		if cancelled && !requirement.settlementCapture {
			return nil, errPromptCancelled
		}
	}

	if err := s.ensureNotPoisoned(); err != nil {
		return nil, err
	}

	if requirement != nil && s.lifetimeEnded() {
		return nil, errors.New("session closed before Hermes terminal snapshot commit")
	}

	settled := requirement != nil && requirement.nativeUnavailable
	if reason := s.snapshotBlockedReasonForTerminalCommit(requirement != nil, settled); reason != "" {
		return nil, fmt.Errorf("cannot snapshot Hermes session while %s pending", reason)
	}

	snapshot := s.snapshot()
	if snapshot.client == nil {
		if requirement != nil {
			return nil, errors.New("hermes runtime is unavailable for terminal snapshot commit")
		}

		//nolint:nilnil // A nil commit with no error is the documented "nothing to capture" answer.
		return nil, nil
	}

	// Settlement already supplies a detached context. Keeping the caller's
	// cancellation here lets direct snapshot callers stop before publication.
	snapshotCtx, cancel := context.WithTimeout(ctx, s.agent.options.storeWriteTTL)
	defer cancel()

	idmap := snapshot.idmap
	now := time.Now().UnixMilli()

	idmap.UpdatedAtUnixMilli = now
	if idmap.CreatedAtUnixMilli == 0 {
		idmap.CreatedAtUnixMilli = now
	}

	idmap.Format = SessionStoreFormat

	committed := s.committedState()

	terminal, foreground, err := s.foregroundSections(snapshotCtx, snapshot, idmap, requirement, committed)
	if err != nil {
		return nil, err
	}

	if ctxErr := snapshotCtx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	nextTerminal := publicTerminalState(terminal, foreground)
	if transitionErr := validateTerminalTransition(committed.terminal, nextTerminal, false); transitionErr != nil {
		return nil, transitionErr
	}

	if requirement != nil {
		if transitionErr := validateTerminalTransition(
			requirement.baseline, nextTerminal, requirement.completed,
		); transitionErr != nil {
			return nil, transitionErr
		}
	}

	main := stateSnapshot{
		Format:              SessionStoreFormat,
		CapturedAtUnixMilli: now,
		Session: stateSnapshotSession{
			SessionID:             idmap.SessionID,
			NativeSessionID:       idmap.NativeSessionID,
			ParentSessionID:       idmap.ParentSessionID,
			NativeParentSessionID: idmap.NativeParentSessionID,
			Cwd:                   snapshot.cwd,
			Title:                 snapshot.title,
			Model: stateSnapshotModel{
				ProviderID: snapshot.providerID,
				ModelID:    snapshot.modelID,
			},
			Env:           durableSessionEnvironment(snapshot.env),
			ExtraPathDirs: append([]string{}, snapshot.extraPathDirs...),
		},
		Terminal: terminal,
		Archives: map[string]archiveInfo{},
		Wrapper:  &stateSnapshotWrapper{Foreground: foreground},
	}

	replacements := []SessionStoreReplacement{}
	mainKey := SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}

	stateDBKey := SessionKey{SessionID: string(s.id), Subpath: stateDBSubpath}

	switch {
	case s.agent.options.SharedHermesHome != "":
		// The official shared database is the native authority, so the per-session
		// archive key is listed with no entries: exactly the listed keys stay live,
		// and an unlisted one would be tombstoned.
		replacements = append(replacements, SessionStoreReplacement{Key: stateDBKey})
	case settled:
		// The boundary that ended this incarnation removed the generation's own
		// files before this commit could read them, so the archive the store
		// already holds is republished unchanged. Republishing it is what keeps
		// the committed generation resumable, because Replace tombstones every
		// subkey it does not list.
		entries, loadErr := s.agent.sessionStore().Load(snapshotCtx, stateDBKey)
		if loadErr != nil {
			return nil, loadErr
		}

		main.Archives = cloneArchiveInfo(committed.archives)

		replacements = append(replacements, SessionStoreReplacement{Key: stateDBKey, Entries: entries})
	case managedHermesSnapshotServer(snapshot.client):
		// The archive is completed below after the protocol-derived snapshot
		// fields and id map have been serialized into one retryable commit.
	default:
		xdg := snapshot.client.XDGDirs()
		if archive, sha, ok, archiveErr := encodeHermesStateDBArchive(s.agent.scratchDirectory(), xdg.Root); archiveErr != nil {
			return nil, archiveErr
		} else if ok {
			main.Archives["state-db"] = archiveInfo{
				Subpath: stateDBSubpath,
				SHA256:  sha,
				Bytes:   len(archive),
			}

			entries, encodeErr := encodeArchiveEntries(archive, sha)
			if encodeErr != nil {
				return nil, encodeErr
			}

			replacements = append(replacements, SessionStoreReplacement{Key: stateDBKey, Entries: entries})
		}
	}

	idmapEntry, err := stateJSONMarshal(idmap)
	if err != nil {
		return nil, err
	}

	replacements = append(replacements, SessionStoreReplacement{
		Key: SessionKey{SessionID: string(s.id), Subpath: idmapSubpath}, Entries: []SessionStoreEntry{idmapEntry},
	})

	if ctxErr := snapshotCtx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	commit := &sessionStoreCommit{
		mainKey:      mainKey,
		replacements: replacements,
		terminal:     nextTerminal,
		native:       terminal,
		foreground:   foreground,
		archives:     main.Archives,
		deadline:     s.agent.options.storeWriteTTL,
	}
	if managedHermesSnapshotServer(snapshot.client) && s.agent.options.SharedHermesHome == "" && !settled {
		managed, root, reclaimErr := s.beginManagedSnapshotState(snapshotCtx, snapshot.client)
		if reclaimErr != nil {
			return nil, reclaimErr
		}

		commit.managed = managed
		commit.managedRoot = root
		commit.managedMain = &main
		commit.managedState = stateDBKey

		if completionErr := s.completeManagedSnapshotCommit(commit); completionErr != nil {
			return commit, completionErr
		}

		if ctxErr := snapshotCtx.Err(); ctxErr != nil {
			return commit, ctxErr
		}

		return commit, nil
	}

	mainEntry, err := stateJSONMarshal(main)
	if err != nil {
		return nil, err
	}

	if ctxErr := snapshotCtx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	commit.replacements = append(commit.replacements, SessionStoreReplacement{
		Key: mainKey, Entries: []SessionStoreEntry{mainEntry},
	})

	return commit, nil
}

func managedHermesSnapshotServer(client nativehermes.Server) bool {
	managed, ok := client.(*managedHermesServer)

	return ok && managed.managed
}

func (s *session) beginManagedSnapshotState(
	ctx context.Context,
	client nativehermes.Server,
) (*managedHermesServer, string, error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.beginManagedSnapshotStateHeld(ctx, client)
}

func (s *session) withReclaimedManagedState(
	ctx context.Context,
	client nativehermes.Server,
	use func(string) error,
) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	managed, root, err := s.beginManagedSnapshotStateHeld(ctx, client)
	if err != nil {
		return err
	}

	useErr := use(root)
	cleanupErr := managed.finishReclaimedSnapshot()

	return errors.Join(useErr, cleanupErr)
}

func (s *session) beginManagedSnapshotStateHeld(
	ctx context.Context,
	client nativehermes.Server,
) (*managedHermesServer, string, error) {
	managed, ok := client.(*managedHermesServer)
	if !ok || !managed.managed {
		return nil, "", errors.New("managed Hermes snapshot requires an authority-owned server")
	}

	settleCtx, settleCancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	root, err := managed.reclaimForSnapshot(settleCtx)

	settleCancel()

	if err != nil {
		return nil, "", err
	}

	s.mu.Lock()
	if s.client == client {
		s.runtimeNeedsResume = !s.closed
		s.pending = map[string]nativehermes.PermissionRequest{}
		s.questions = map[string]nativehermes.QuestionRequest{}
		s.processedPermission = map[string]struct{}{}
		s.processedQuestion = map[string]struct{}{}
		s.activeMessageIDs = map[string]struct{}{}
		s.toolStates = map[string]hermesToolState{}
		s.mcpReloadComplete = false
	}
	s.mu.Unlock()

	return managed, root, nil
}

func (s *session) completeManagedSnapshotCommit(commit *sessionStoreCommit) error {
	if commit == nil || commit.managed == nil {
		return nil
	}

	if commit.managedReady == nil {
		archive, sha, present, err := encodeHermesStateDBArchive(s.agent.scratchDirectory(), commit.managedRoot)
		if err != nil {
			return err
		}

		if present {
			entries, encodeErr := encodeArchiveEntries(archive, sha)
			if encodeErr != nil {
				return encodeErr
			}

			commit.managedReady = append(commit.managedReady, SessionStoreReplacement{
				Key: commit.managedState, Entries: entries,
			})
			commit.managedMain.Archives["state-db"] = archiveInfo{
				Subpath: stateDBSubpath,
				SHA256:  sha,
				Bytes:   len(archive),
			}
		}

		mainEntry, marshalErr := stateJSONMarshal(*commit.managedMain)
		if marshalErr != nil {
			commit.managedReady = nil

			return marshalErr
		}

		commit.managedReady = append(commit.managedReady, SessionStoreReplacement{
			Key: commit.mainKey, Entries: []SessionStoreEntry{mainEntry},
		})
	}

	if err := commit.managed.finishReclaimedSnapshot(); err != nil {
		return err
	}

	commit.replacements = append(commit.replacements, commit.managedReady...)
	commit.managed = nil
	commit.managedRoot = ""
	commit.managedMain = nil
	commit.managedReady = nil

	return nil
}

// publishSnapshotLocked makes one captured generation durable. It is the
// durability boundary every ordering rule above it is stated against: nothing
// that depends on the store holding this generation may happen before it returns.
func (s *session) publishSnapshotLocked(ctx context.Context, commit *sessionStoreCommit) error {
	// A tombstone outranks every later commit for the same id. Delete takes this
	// same barrier to write it, so reaching here with the id tombstoned means the
	// tombstone is already durable and this generation has nowhere to land: the
	// row it would publish is exactly the row the delete removed. Publishing it
	// would clear a tombstone this session did not create.
	if s.agent.isDeleted(s.id) {
		return nil
	}

	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commit.deadline)
	defer cancel()

	journal := s.operationJournal
	if journal != nil && journal.record.Prepared == nil {
		if err := journal.prepareReplacements(commit.replacements); err != nil {
			return fmt.Errorf("prepare Hermes session publication: %w", err)
		}
	}

	if replaceErr := s.agent.sessionStore().Replace(writeCtx, commit.mainKey, commit.replacements); replaceErr != nil {
		reconcileCtx, reconcileCancel := s.agent.sessionStoreContext(context.Background())
		landed, absent, reconcileErr := reconcileSessionStoreReplacement(
			reconcileCtx, s.agent.sessionStore(), commit.mainKey, commit.replacements,
		)

		reconcileCancel()

		switch {
		case reconcileErr != nil:
			return errors.Join(errSessionStoreCommitUnknown, replaceErr, reconcileErr)
		case landed:
			// Replace committed and only its acknowledgement was lost.
		case absent:
			return replaceErr
		default:
			return errors.Join(errSessionStoreCommitUnknown, replaceErr)
		}
	}

	if journal != nil {
		if err := journal.markStoreCommitted(); err != nil {
			// Store publication is already exact and authoritative. A journal
			// cleanup/write failure must not turn success into a retry that could
			// duplicate or delete the committed native session.
			s.agent.log.DebugContext(ctx, "retain Hermes session-operation journal after committed store publication", slog.String(jsonFieldError, err.Error()))
		}
	}

	s.mu.Lock()
	s.committed = committedState{
		terminal:   commit.terminal,
		native:     commit.native,
		foreground: commit.foreground,
		archives:   commit.archives,
	}
	s.mu.Unlock()

	return nil
}

func cloneArchiveInfo(archives map[string]archiveInfo) map[string]archiveInfo {
	out := make(map[string]archiveInfo, len(archives))
	maps.Copy(out, archives)

	return out
}

// foregroundSections resolves the two sections one commit publishes about the
// turn that just settled: the native archive's completed assistant identity and
// the adapter's own record of how the foreground cycle ended.
//
// A commit whose native generation is still readable derives the terminal
// identity from the native history, exactly as a completed turn requires. One
// taken after an incarnation-ending boundary cannot: that boundary contained the
// generation before the commit, so the commit restates the last identity this
// session durably holds and records the truthful outcome beside it.
func (s *session) foregroundSections(
	ctx context.Context,
	snapshot sessionSnapshot,
	idmap idmapRecord,
	requirement *terminalSnapshotRequirement,
	committed committedState,
) (*stateSnapshotTerminal, *stateSnapshotForeground, error) {
	foreground := committed.foreground
	if requirement != nil {
		record := requirement.foreground
		foreground = &record
	}

	if requirement != nil && requirement.nativeUnavailable {
		return committed.nativeTerminal(), foreground, nil
	}

	messages, err := snapshot.client.Messages(ctx, idmap.NativeSessionID)
	if err != nil {
		return nil, nil, fmt.Errorf("read Hermes session terminal history: %w", err)
	}

	terminal, err := terminalSnapshotFromMessages(idmap.NativeSessionID, messages)
	if err != nil {
		return nil, nil, err
	}

	if requirement != nil && requirement.completed && terminal.MessageID == "" {
		return nil, nil, errors.New("completed Hermes turn is missing a durable terminal assistant identity")
	}

	return terminal, foreground, nil
}

// reconcileSessionStoreReplacement distinguishes an acknowledgement loss from
// a definite non-commit without guessing from the original error. The exact
// prepared bundle is the authority: every expected entry and the complete
// subkey set must byte-match. A partial/different or unreadable state is
// unknown and must never trigger destructive native compensation.
func reconcileSessionStoreReplacement(ctx context.Context, store SessionStore, main SessionKey, replacements []SessionStoreReplacement) (committed bool, absent bool, err error) {
	expectedSubkeys := make([]string, 0, len(replacements)-1)
	mainAbsent := false
	allExact := true

	for _, replacement := range replacements {
		entries, loadErr := store.Load(ctx, replacement.Key)
		if loadErr != nil {
			return false, false, loadErr
		}

		if replacement.Key.Subpath == SessionStoreMainSubpath && len(entries) == 0 {
			mainAbsent = true
		}

		if replacement.Key.Subpath != SessionStoreMainSubpath {
			expectedSubkeys = append(expectedSubkeys, replacement.Key.Subpath)
		}

		if !slices.EqualFunc(entries, replacement.Entries, func(left, right SessionStoreEntry) bool {
			return bytes.Equal(left, right)
		}) {
			allExact = false
		}
	}

	slices.Sort(expectedSubkeys)

	actualSubkeys, listErr := store.ListSubkeys(ctx, main)
	if listErr != nil {
		return false, false, listErr
	}

	slices.Sort(actualSubkeys)

	if allExact && slices.Equal(actualSubkeys, expectedSubkeys) {
		return true, false, nil
	}

	if mainAbsent && len(actualSubkeys) == 0 {
		return false, true, nil
	}

	return false, false, nil
}

// claimTerminalCommit is one turn's settlement linearization point and the only
// race authority over it. A cancel that arrives after the claim is a
// post-settlement no-op and must not close the runtime underneath a commit
// already in progress; one that arrived before it is reported here so the turn
// records the cancelled outcome without abandoning the commit it owes.
func (s *session) claimTerminalCommit(turnEpoch uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.turnEpoch != turnEpoch || !s.turnInFlight {
		return false, routeInvalid("stale turn epoch at terminal commit")
	}

	raced := s.turnSettlement == turnSettlementCancelled

	if s.turnSettlement != turnSettlementOpen && s.turnSettlement != turnSettlementCapturing && !raced {
		return false, errors.New("hermes turn terminal commit was already claimed")
	}

	s.turnSettlement = turnSettlementCommitting

	return raced, nil
}

func hydrateStateFromStore(ctx context.Context, store SessionStore, sessionID string, xdg nativehermes.XDGDirs) (idmapRecord, stateSnapshot, bool, error) {
	return hydrateStateFromStoreMode(ctx, store, sessionID, xdg, true)
}

// hydrateStateFromStoreWithoutNativeArchive loads logical metadata only. In
// shared-home mode the durable official Hermes database is authoritative; a
// per-session archive must never be decoded into the wrapper generation or
// copied back over the shared database.
func hydrateStateFromStoreWithoutNativeArchive(ctx context.Context, store SessionStore, sessionID string, xdg nativehermes.XDGDirs) (idmapRecord, stateSnapshot, bool, error) {
	return hydrateStateFromStoreMode(ctx, store, sessionID, xdg, false)
}

func hydrateStateFromStoreMode(ctx context.Context, store SessionStore, sessionID string, xdg nativehermes.XDGDirs, restoreNativeArchive bool) (idmapRecord, stateSnapshot, bool, error) {
	idEntries, err := store.Load(ctx, SessionKey{SessionID: sessionID, Subpath: idmapSubpath})
	if err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	mainEntries, err := store.Load(ctx, SessionKey{SessionID: sessionID, Subpath: SessionStoreMainSubpath})
	if err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	if len(idEntries) == 0 || len(mainEntries) == 0 {
		return idmapRecord{}, stateSnapshot{}, false, nil
	}

	// Past this point the store holds an entry for this session: every failure
	// below is an entry that cannot be replayed, which is its own wire verdict.
	// The entry is left exactly where it is -- a restore failure never deletes
	// or tombstones what it could not read.
	var idmap idmapRecord
	if err := decodeStrictStoreJSON(idEntries[len(idEntries)-1], &idmap, idmapJSONShape); err != nil {
		return idmapRecord{}, stateSnapshot{}, false, restoreFailed(err)
	}

	var snapshot stateSnapshot
	if err := decodeStrictStoreJSON(mainEntries[len(mainEntries)-1], &snapshot, stateSnapshotJSONShape); err != nil {
		return idmapRecord{}, stateSnapshot{}, false, restoreFailed(err)
	}

	if idmap.Format != SessionStoreFormat || snapshot.Format != SessionStoreFormat {
		return idmapRecord{}, stateSnapshot{}, false, restoreFailed(fmt.Errorf("unsupported hermes store format"))
	}

	if err := validateHydratedStateAgreement(sessionID, idmap, snapshot); err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	if _, ok := snapshot.Archives["state-db"]; ok {
		if !restoreNativeArchive {
			return idmap, snapshot, true, nil
		}

		entries, err := store.Load(ctx, SessionKey{SessionID: sessionID, Subpath: stateDBSubpath})
		if err != nil {
			return idmapRecord{}, stateSnapshot{}, false, err
		}

		if len(entries) == 0 {
			return idmapRecord{}, stateSnapshot{}, false, fmt.Errorf("store missing archive %s", stateDBSubpath)
		}

		data, err := decodeArchiveEntries(entries, snapshot.Archives["state-db"])
		if err != nil {
			return idmapRecord{}, stateSnapshot{}, false, err
		}

		if err := decodeXDGArchive(data, xdg.Root); err != nil {
			return idmapRecord{}, stateSnapshot{}, false, err
		}

		return idmap, snapshot, true, nil
	}

	return idmap, snapshot, true, nil
}

func encodeArchiveEntries(archive []byte, sha string) ([]SessionStoreEntry, error) {
	entries := make([]SessionStoreEntry, 0, (len(archive)+archiveChunkBytes-1)/archiveChunkBytes)
	for start, sequence := 0, 0; start < len(archive); start, sequence = start+archiveChunkBytes, sequence+1 {
		end := min(start+archiveChunkBytes, len(archive))

		entry, err := stateJSONMarshal(archiveEntry{
			Format:   SessionStoreFormat,
			Encoding: archiveEncodingTarZstdBase64,
			Sequence: sequence,
			Final:    end == len(archive),
			SHA256:   sha,
			Data:     base64.StdEncoding.EncodeToString(archive[start:end]),
		})
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func decodeArchiveEntries(entries []SessionStoreEntry, info archiveInfo) ([]byte, error) {
	if len(entries) == 0 || info.SHA256 == "" || info.Bytes <= 0 || int64(info.Bytes) > maxHydrateFileBytes {
		return nil, fmt.Errorf("invalid archive entry %s", stateDBSubpath)
	}

	var archive bytes.Buffer

	for sequence, raw := range entries {
		var entry archiveEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, err
		}

		final := sequence == len(entries)-1
		if entry.Format != SessionStoreFormat ||
			entry.Encoding != archiveEncodingTarZstdBase64 ||
			entry.Sequence != sequence ||
			entry.Final != final ||
			entry.SHA256 != info.SHA256 {
			return nil, fmt.Errorf("invalid archive entry %s sequence %d", stateDBSubpath, sequence)
		}

		chunk, err := base64.StdEncoding.DecodeString(entry.Data)
		if err != nil {
			return nil, err
		}

		if len(chunk) > archiveChunkBytes || (!final && len(chunk) != archiveChunkBytes) {
			return nil, fmt.Errorf("invalid archive chunk size %s sequence %d", stateDBSubpath, sequence)
		}

		_, _ = archive.Write(chunk)
	}

	data := archive.Bytes()
	if info.Bytes != len(data) {
		return nil, fmt.Errorf("archive size mismatch %s", stateDBSubpath)
	}

	sum := sha256.Sum256(data)
	if info.SHA256 != hex.EncodeToString(sum[:]) {
		return nil, fmt.Errorf("archive checksum mismatch %s", stateDBSubpath)
	}

	return data, nil
}

func (s *session) snapshotBlockedReason() string {
	return s.snapshotBlockedReasonForTerminalCommit(false, false)
}

// snapshotBlockedReasonForTerminalCommit reports why no snapshot may be taken, or
// the empty string when one may. settledBoundary names the commit that settles an
// incarnation-ending boundary: the pending resume is that boundary's own
// consequence, so it is not a reason to withhold the record of what ended.
func (s *session) snapshotBlockedReasonForTerminalCommit(allowOwningTurn bool, settledBoundary bool) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case !allowOwningTurn && (s.turnInFlight || s.cancel != nil):
		return reasonTurn
	case s.runtimeNeedsResume && !settledBoundary:
		return "runtime resume"
	case len(s.pending) > 0:
		return reasonPermission
	case len(s.questions) > 0:
		return valElicitation
	case len(s.activeMessageIDs) > 0:
		return reasonGeneration
	default:
		return ""
	}
}

// lifetimeEnded reports whether this session object's own teardown has begun.
// It is set before close reaches either the store or the provider-auth broker
// and is never cleared, so it identifies the lifetime rather than the id: a
// later session/load hydrates the same id behind a different object, and this
// one keeps answering true for the legs still holding it.
func (s *session) lifetimeEnded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

func encodeHermesStateDBArchive(scratchDir string, root string) ([]byte, string, bool, error) {
	if root == "" {
		return nil, "", false, nil
	}

	type stateDBFile struct {
		name string
		data []byte
	}

	files := []stateDBFile{}

	stateDBPath := filepath.Join(root, fileStateDB)
	if info, err := stateLstat(stateDBPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, "", false, err
		}
	} else if info.Mode().IsRegular() {
		if data, ok, err := stateSQLiteArchiveContent(scratchDir, stateDBPath); err != nil {
			return nil, "", false, err
		} else if ok {
			files = append(files, stateDBFile{name: fileStateDB, data: data})
		}
	}

	if len(files) == 0 {
		for _, name := range []string{fileStateDB, fileStateDBWAL, fileStateDBSHM} {
			path := filepath.Join(root, name)

			info, err := stateLstat(path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}

				return nil, "", false, err
			}

			if info.Mode().IsRegular() {
				files = append(files, stateDBFile{name: name})
			}
		}
	}

	if len(files) == 0 {
		return nil, "", false, nil
	}

	var tarbuf bytes.Buffer

	tw := stateNewTarWriter(&tarbuf)

	for _, item := range files {
		name := item.name
		path := filepath.Join(root, name)

		info, err := stateLstat(path)
		if err != nil {
			return nil, "", false, err
		}

		header, err := stateFileInfoHeader(info, "")
		if err != nil {
			return nil, "", false, err
		}

		header.Name = name
		header.Uid = 0
		header.Gid = 0
		header.Uname = ""
		header.Gname = ""
		header.ModTime = time.Unix(0, 0)
		header.AccessTime = time.Unix(0, 0)

		header.ChangeTime = time.Unix(0, 0)
		if item.data != nil {
			header.Size = int64(len(item.data))
		}

		if writeErr := tw.WriteHeader(header); writeErr != nil {
			return nil, "", false, writeErr
		}

		if item.data != nil {
			if _, writeErr := tw.Write(item.data); writeErr != nil {
				return nil, "", false, writeErr
			}

			continue
		}

		file, err := stateOpen(path)
		if err != nil {
			return nil, "", false, err
		}

		_, copyErr := stateCopy(tw, file)
		closeErr := file.Close()

		if copyErr != nil {
			return nil, "", false, copyErr
		}

		if closeErr != nil {
			return nil, "", false, closeErr
		}
	}

	if err := tw.Close(); err != nil {
		return nil, "", false, err
	}

	var zbuf bytes.Buffer

	zw, err := stateNewZstdWriter(&zbuf)
	if err != nil {
		return nil, "", false, err
	}

	if _, err := zw.Write(tarbuf.Bytes()); err != nil {
		zw.Close()

		return nil, "", false, err
	}

	if err := zw.Close(); err != nil {
		return nil, "", false, err
	}

	sum := sha256.Sum256(zbuf.Bytes())

	return zbuf.Bytes(), hex.EncodeToString(sum[:]), true, nil
}

func decodeXDGArchive(data []byte, target string) error {
	if err := stateRemoveAll(target); err != nil {
		return err
	}

	if err := stateMkdirAll(target, 0o700); err != nil {
		return err
	}

	zr, err := stateNewZstdReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer zr.Close()

	tr := tar.NewReader(zr)

	cleanTarget, err := stateAbs(target)
	if err != nil {
		return err
	}

	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if header.Name == "" || absolutePathSpelling(header.Name) || strings.Contains(header.Name, "..") {
			return fmt.Errorf("archive path rejected: %s", header.Name)
		}

		path := filepath.Join(cleanTarget, filepath.FromSlash(header.Name))

		cleanPath, err := stateAbs(path)
		if err != nil {
			return err
		}

		if cleanPath != cleanTarget && !strings.HasPrefix(cleanPath, cleanTarget+string(os.PathSeparator)) {
			return fmt.Errorf("archive path escapes target: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := stateMkdirAll(cleanPath, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if header.Size < 0 || header.Size > maxHydrateFileBytes {
				return fmt.Errorf("archive file %s has unsupported size %d", header.Name, header.Size)
			}

			if err := stateMkdirAll(filepath.Dir(cleanPath), 0o700); err != nil {
				return err
			}

			mode := header.FileInfo().Mode().Perm() & 0o700

			file, err := stateOpenFile(cleanPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return err
			}

			written, copyErr := stateCopyN(file, tr, header.Size)
			closeErr := file.Close()

			if copyErr != nil {
				return copyErr
			}

			if written != header.Size {
				return fmt.Errorf("archive file %s restored %d bytes, want %d", header.Name, written, header.Size)
			}

			if closeErr != nil {
				return closeErr
			}
		}
	}
}

func validateHydratedStateAgreement(sessionID string, idmap idmapRecord, snapshot stateSnapshot) error {
	if idmap.SessionID != sessionID {
		return fmt.Errorf("hermes store idmap session mismatch: %q != %q", idmap.SessionID, sessionID)
	}

	if snapshot.Session.SessionID != sessionID {
		return fmt.Errorf("hermes store snapshot session mismatch: %q != %q", snapshot.Session.SessionID, sessionID)
	}

	if snapshot.Session.NativeSessionID != idmap.NativeSessionID {
		return fmt.Errorf("hermes store idmap/main native session mismatch")
	}

	if snapshot.Session.ParentSessionID != idmap.ParentSessionID {
		return fmt.Errorf("hermes store idmap/main parent session mismatch")
	}

	if snapshot.Session.NativeParentSessionID != idmap.NativeParentSessionID {
		return fmt.Errorf("hermes store idmap/main native parent session mismatch")
	}

	if err := validateStateSnapshotRequiredSections(snapshot); err != nil {
		return fmt.Errorf("hermes store main snapshot: %w", err)
	}

	return nil
}

func durableSessionEnvironment(environment map[string]string) map[string]string {
	cloned := make(map[string]string, len(environment))
	maps.Copy(cloned, environment)

	return cloned
}

func sqliteArchiveContent(scratchDir string, path string) ([]byte, bool, error) {
	ok, err := isSQLiteDatabase(path)
	if err != nil || !ok {
		return nil, ok, err
	}

	parent, err := ensureScratchParent(scratchDir)
	if err != nil {
		return nil, false, err
	}

	tempDir, err := stateMkdirTemp(parent, "acp-go-hermes-sqlite-*")
	if err != nil {
		return nil, false, err
	}

	defer func() { _ = stateRemoveAll(tempDir) }()

	copyPath := filepath.Join(tempDir, "archive.db")
	if vacuumErr := vacuumSQLiteInto(path, copyPath); vacuumErr != nil {
		return nil, false, vacuumErr
	}

	if scrubErr := stateScrubSQLiteCredentialTables(copyPath); scrubErr != nil {
		return nil, false, scrubErr
	}

	data, err := stateReadFile(copyPath)
	if err != nil {
		return nil, false, err
	}

	return data, true, nil
}

func isSQLiteDatabase(path string) (bool, error) {
	file, err := stateOpen(path)
	if err != nil {
		return false, err
	}
	defer file.Close()

	header := make([]byte, 16)

	n, err := io.ReadFull(file, header)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return n == len(header) && string(header) == "SQLite format 3\x00", nil
}

// sqliteBusyTimeout bounds how long a state-db statement waits for a lock some
// other connection holds. Hermes creates state.db in rollback-journal mode
// whenever its SQLite build is exposed to the WAL-reset bug, and in that mode a
// commit locks readers out entirely; SQLite defaults to no busy timeout, so a
// snapshot taken while the live Hermes process is mid-write fails instantly
// with SQLITE_BUSY. The wait is a bound rather than a hang, and it stays far
// inside sessionStoreWriteTimeout so a contended snapshot still lands within
// the turn's store-write budget.
const sqliteBusyTimeout = 10 * time.Second

// sqliteStateDSN renders a state-db path as a driver DSN carrying the busy
// timeout. The path keeps its plain, non-URI form, so it still resolves exactly
// as it did before the timeout was attached.
func sqliteStateDSN(path string) string {
	return fmt.Sprintf("%s?_pragma=busy_timeout(%d)", path, sqliteBusyTimeout.Milliseconds())
}

func vacuumSQLiteInto(source string, target string) error {
	db, err := stateSQLOpen("sqlite", sqliteStateDSN(source))
	if err != nil {
		return err
	}

	ctx := context.Background()
	_, execErr := db.ExecContext(ctx, "VACUUM INTO "+quoteSQLiteString(target)) // #nosec G202 -- target is string-literal quoted before interpolation.
	closeErr := db.Close()

	if execErr != nil {
		return execErr
	}

	return closeErr
}

func copyFile(source string, target string, mode os.FileMode) error {
	in, err := stateOpen(source)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := stateOpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	_, copyErr := stateCopy(out, in)
	closeErr := out.Close()

	if copyErr != nil {
		return copyErr
	}

	return closeErr
}

func scrubSQLiteCredentialTables(path string) error {
	db, err := stateSQLOpen("sqlite", sqliteStateDSN(path))
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	for _, statement := range []string{
		"PRAGMA foreign_keys=OFF",
		"PRAGMA secure_delete=ON",
		"PRAGMA wal_checkpoint(TRUNCATE)",
		"PRAGMA journal_mode=DELETE",
	} {
		if _, execErr := db.ExecContext(ctx, statement); execErr != nil {
			return execErr
		}
	}

	tables, err := sqliteCredentialTables(ctx, db)
	if err != nil {
		return err
	}

	for _, table := range tables {
		statement := "DELETE FROM " + quoteSQLiteIdent(table) // #nosec G202 -- table names come from sqlite_master and are identifier-quoted.
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}

	if len(tables) > 0 {
		if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
			return err
		}
	}

	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}

	return nil
}

func sqliteCredentialTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		sensitive, err := sqliteTableIsCredentialBearing(ctx, db, name)
		if err != nil {
			return nil, err
		}

		if sensitive {
			tables = append(tables, name)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func sqliteTableIsCredentialBearing(ctx context.Context, db *sql.DB, table string) (bool, error) {
	switch strings.ToLower(table) {
	case tableAccount, "control_account", "credential", "session_share":
		return true, nil
	}

	if sensitiveSQLiteName(table) {
		return true, nil
	}

	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+quoteSQLiteIdent(table)+")")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid          int
			name         string
			columnType   string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}

		if sensitiveSQLiteName(name) {
			return true, nil
		}
	}

	if err := rows.Err(); err != nil {
		return false, err
	}

	return false, nil
}

func sensitiveSQLiteName(value string) bool {
	value = strings.ToLower(value)
	for _, marker := range []string{"credential", valSecret, "access_token", "refresh_token", "api_key", "apikey", "private_key", "password"} {
		if strings.Contains(value, marker) {
			return true
		}
	}

	return false
}

func quoteSQLiteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteSQLiteString(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
