//nolint:tagliatelle // Durable v1 JSON names are frozen.
package hermesacp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	sessionOperationJournalFormat = "hermes-session-op-v1"
	sessionOperationDirectoryName = "session-operations"
	sessionOperationJournalName   = "journal.json"
)

var (
	ErrSessionOperationAmbiguous        = errors.New("hermes session-operation recovery is ambiguous")
	ErrSessionOperationStoreUnavailable = errors.New("hermes session-operation store is unavailable")
)

type sessionOperationKind string

const (
	sessionOperationKindNew  sessionOperationKind = "new"
	sessionOperationKindFork sessionOperationKind = "fork"
)

type sessionOperationMode string

const (
	sessionOperationModeShared   sessionOperationMode = "shared"
	sessionOperationModeIsolated sessionOperationMode = "isolated"
)

type sessionOperationPhase string

const (
	sessionOperationPhasePrepared         sessionOperationPhase = "prepared"
	sessionOperationPhaseMutating         sessionOperationPhase = "mutating"
	sessionOperationPhaseNativeIdentified sessionOperationPhase = "native_identified"
	sessionOperationPhaseLiveFenced       sessionOperationPhase = "live_fenced"
	sessionOperationPhaseTargetReady      sessionOperationPhase = "target_ready"
	sessionOperationPhaseStorePrepared    sessionOperationPhase = "store_prepared"
	sessionOperationPhaseStoreCommitted   sessionOperationPhase = "store_committed"
)

type sessionOperationJournalFields struct {
	OperationID              string
	Kind                     sessionOperationKind
	Mode                     sessionOperationMode
	LogicalSessionID         string
	ParentLogicalSessionID   string
	ParentNativeSessionID    string
	SourceRoot               string
	TargetRoot               string
	Marker                   string
	FinalTitle               string
	BaselineNativeSessionIDs []string
	Origin                   nativehermes.DurableProcessIdentity
}

type sessionOperationJournalPatch struct {
	Phase                    *sessionOperationPhase
	NativeSessionID          *string
	LiveSessionID            *string
	Marker                   *string
	BaselineNativeSessionIDs *[]string
	Origin                   *nativehermes.DurableProcessIdentity
}

type sessionOperationJournalRecord struct {
	Format                   string                              `json:"format"`
	OperationID              string                              `json:"operationId"`
	Kind                     sessionOperationKind                `json:"kind"`
	Mode                     sessionOperationMode                `json:"mode"`
	Phase                    sessionOperationPhase               `json:"phase"`
	LogicalSessionID         string                              `json:"logicalSessionId"`
	ParentLogicalSessionID   string                              `json:"parentLogicalSessionId,omitempty"`
	ParentNativeSessionID    string                              `json:"parentNativeSessionId,omitempty"`
	SourceRoot               string                              `json:"sourceRoot,omitempty"`
	TargetRoot               string                              `json:"targetRoot,omitempty"`
	Marker                   string                              `json:"marker"`
	FinalTitle               string                              `json:"finalTitle,omitempty"`
	BaselineNativeSessionIDs []string                            `json:"baselineNativeSessionIds"`
	NativeSessionID          string                              `json:"nativeSessionId,omitempty"`
	LiveSessionID            string                              `json:"liveSessionId,omitempty"`
	Origin                   nativehermes.DurableProcessIdentity `json:"origin"`
	Prepared                 *sessionOperationPreparedManifest   `json:"prepared,omitempty"`
	PreparedManifestSHA256   string                              `json:"preparedManifestSHA256,omitempty"`
	CreatedAtUnixMilli       int64                               `json:"createdAtUnixMilli"`
	UpdatedAtUnixMilli       int64                               `json:"updatedAtUnixMilli"`
}

type sessionOperationPreparedManifest struct {
	Format           string                                    `json:"format"`
	LogicalSessionID string                                    `json:"logicalSessionId"`
	Parts            []sessionOperationPreparedReplacementPart `json:"parts"`
}

type sessionOperationPreparedReplacementPart struct {
	Sequence   int    `json:"sequence"`
	Subpath    string `json:"subpath"`
	Filename   string `json:"filename"`
	SHA256     string `json:"sha256"`
	Bytes      int    `json:"bytes"`
	EntryCount int    `json:"entryCount"`
}

type sessionOperationDiskReplacement struct {
	SessionID string              `json:"sessionId"`
	Subpath   string              `json:"subpath"`
	Entries   []SessionStoreEntry `json:"entries"`
}

type sessionOperationJournal struct {
	directory string
	path      string
	record    sessionOperationJournalRecord
}

type sessionOperationFile interface {
	Name() string
	Chmod(os.FileMode) error
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type sessionOperationStoreState uint8

const (
	sessionOperationStoreAbsent sessionOperationStoreState = iota
	sessionOperationStoreExact
	sessionOperationStoreAmbiguous
)

var (
	sessionOperationCreateTemp = func(directory, pattern string) (sessionOperationFile, error) {
		return os.CreateTemp(directory, pattern)
	}
	sessionOperationMarshal                = json.Marshal
	sessionOperationCurrentProcessIdentity = nativehermes.CurrentDurableProcessIdentity
	sessionOperationProcessIdentityGone    = nativehermes.DurableProcessIdentityGone
	sessionOperationRename                 = os.Rename
	sessionOperationMkdir                  = os.Mkdir
	sessionOperationMkdirAll               = os.MkdirAll
	sessionOperationChmod                  = os.Chmod
	sessionOperationLstat                  = os.Lstat
	sessionOperationOpen                   = os.Open
	sessionOperationReadDir                = os.ReadDir
	sessionOperationRemove                 = os.Remove
	sessionOperationRemoveAll              = os.RemoveAll
	sessionOperationNow                    = time.Now
)

func newSessionOperationID() (string, error) {
	value := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		return "", fmt.Errorf("generate Hermes session-operation id: %w", err)
	}

	return hex.EncodeToString(value), nil
}

func beginSessionOperationJournal(sharedHome string, fields sessionOperationJournalFields) (*sessionOperationJournal, error) {
	control, err := nativehermes.EnsureSharedHermesAdapterControlDir(sharedHome)
	if err != nil {
		return nil, err
	}

	operations := filepath.Join(control, sessionOperationDirectoryName)
	if err := ensureSessionOperationDirectory(operations); err != nil {
		return nil, err
	}

	now := sessionOperationNow().UnixMilli()

	record := sessionOperationJournalRecord{
		Format:                   sessionOperationJournalFormat,
		OperationID:              fields.OperationID,
		Kind:                     fields.Kind,
		Mode:                     fields.Mode,
		Phase:                    sessionOperationPhasePrepared,
		LogicalSessionID:         fields.LogicalSessionID,
		ParentLogicalSessionID:   fields.ParentLogicalSessionID,
		ParentNativeSessionID:    fields.ParentNativeSessionID,
		SourceRoot:               fields.SourceRoot,
		TargetRoot:               fields.TargetRoot,
		Marker:                   fields.Marker,
		FinalTitle:               fields.FinalTitle,
		BaselineNativeSessionIDs: cloneAndSortSessionOperationIDs(fields.BaselineNativeSessionIDs),
		Origin:                   fields.Origin,
		CreatedAtUnixMilli:       now,
		UpdatedAtUnixMilli:       now,
	}
	if err := validateSessionOperationJournalRecord(record); err != nil {
		return nil, err
	}

	directory := filepath.Join(operations, record.OperationID)
	if err := sessionOperationMkdir(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create Hermes session-operation directory: %w", err)
	}

	cleanup := true
	defer func() {
		if cleanup {
			_ = sessionOperationRemoveAll(directory)
		}
	}()

	if err := sessionOperationChmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("protect Hermes session-operation directory: %w", err)
	}

	if err := syncSessionOperationDirectory(operations); err != nil {
		return nil, fmt.Errorf("sync Hermes session-operation directory entry: %w", err)
	}

	journal := &sessionOperationJournal{
		directory: directory,
		path:      filepath.Join(directory, sessionOperationJournalName),
		record:    record,
	}
	if err := journal.persist(); err != nil {
		return nil, err
	}

	cleanup = false

	return journal, nil
}

func findPendingSessionOperationJournals(sharedHome string, logicalID string, kind sessionOperationKind) ([]*sessionOperationJournal, error) {
	control, err := nativehermes.EnsureSharedHermesAdapterControlDir(sharedHome)
	if err != nil {
		return nil, err
	}

	operations := filepath.Join(control, sessionOperationDirectoryName)

	entries, err := sessionOperationReadDir(operations)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("list Hermes session-operation journals: %w", err)
	}

	journals := make([]*sessionOperationJournal, 0, len(entries))
	for _, entry := range entries {
		if entry.Name() == "" || !validSessionOperationID(entry.Name()) || !entry.IsDir() {
			return nil, fmt.Errorf("%w: unexpected session-operation entry %q", ErrSessionOperationAmbiguous, entry.Name())
		}

		directory := filepath.Join(operations, entry.Name())

		journal, loadErr := loadSessionOperationJournal(directory)
		if loadErr != nil {
			return nil, loadErr
		}

		if logicalID != "" && journal.record.LogicalSessionID != logicalID {
			continue
		}

		if kind != "" && journal.record.Kind != kind {
			continue
		}

		journals = append(journals, journal)
	}

	slices.SortFunc(journals, func(left, right *sessionOperationJournal) int {
		return strings.Compare(left.record.OperationID, right.record.OperationID)
	})

	return journals, nil
}

func loadSessionOperationJournal(directory string) (*sessionOperationJournal, error) {
	info, err := sessionOperationLstat(directory)
	if err != nil {
		return nil, fmt.Errorf("inspect Hermes session-operation directory: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("%w: session-operation path is not a directory", ErrSessionOperationAmbiguous)
	}

	path := filepath.Join(directory, sessionOperationJournalName)

	data, err := readBoundedSessionOperationFile(path, 1024*1024)
	if err != nil {
		return nil, fmt.Errorf("read Hermes session-operation journal: %w", err)
	}

	var record sessionOperationJournalRecord

	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&record); err != nil {
		return nil, fmt.Errorf("%w: decode session-operation journal: %v", ErrSessionOperationAmbiguous, err)
	}

	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("%w: session-operation journal has trailing data", ErrSessionOperationAmbiguous)
	}

	if err := validateSessionOperationJournalRecord(record); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSessionOperationAmbiguous, err)
	}

	if filepath.Base(directory) != record.OperationID {
		return nil, fmt.Errorf("%w: session-operation directory/id mismatch", ErrSessionOperationAmbiguous)
	}

	return &sessionOperationJournal{directory: directory, path: path, record: record}, nil
}

func (j *sessionOperationJournal) update(patch sessionOperationJournalPatch) error {
	if j == nil {
		return errors.New("nil Hermes session-operation journal")
	}

	next := j.record
	if patch.Phase != nil {
		if sessionOperationPhaseOrder(*patch.Phase) < sessionOperationPhaseOrder(next.Phase) {
			return errors.New("hermes session-operation phase cannot regress")
		}

		next.Phase = *patch.Phase
	}

	if patch.NativeSessionID != nil {
		next.NativeSessionID = *patch.NativeSessionID
	}

	if patch.LiveSessionID != nil {
		next.LiveSessionID = *patch.LiveSessionID
	}

	if patch.Marker != nil {
		next.Marker = *patch.Marker
	}

	if patch.BaselineNativeSessionIDs != nil {
		next.BaselineNativeSessionIDs = cloneAndSortSessionOperationIDs(*patch.BaselineNativeSessionIDs)
	}

	if patch.Origin != nil {
		next.Origin = *patch.Origin
	}

	next.UpdatedAtUnixMilli = sessionOperationNow().UnixMilli()
	if err := validateSessionOperationJournalRecord(next); err != nil {
		return err
	}

	previous := j.record

	j.record = next
	if err := j.persist(); err != nil {
		j.record = previous

		return err
	}

	return nil
}

func (j *sessionOperationJournal) prepareReplacements(replacements []SessionStoreReplacement) error {
	if j == nil {
		return errors.New("nil Hermes session-operation journal")
	}

	if sessionOperationPhaseOrder(j.record.Phase) > sessionOperationPhaseOrder(sessionOperationPhaseStorePrepared) {
		return errors.New("hermes session-operation publication is already committed")
	}

	if j.record.Prepared != nil {
		return errors.New("hermes session-operation publication is already prepared")
	}

	seen := make(map[string]struct{}, len(replacements))
	mainCount := 0

	manifest := sessionOperationPreparedManifest{
		Format:           sessionOperationJournalFormat,
		LogicalSessionID: j.record.LogicalSessionID,
		Parts:            make([]sessionOperationPreparedReplacementPart, 0, len(replacements)),
	}
	for sequence, replacement := range replacements {
		if replacement.Key.SessionID != j.record.LogicalSessionID {
			return errors.New("prepared replacement belongs to a different logical session")
		}

		if _, exists := seen[replacement.Key.Subpath]; exists {
			return errors.New("prepared replacements contain a duplicate subpath")
		}

		seen[replacement.Key.Subpath] = struct{}{}
		if replacement.Key.Subpath == SessionStoreMainSubpath {
			mainCount++
		}

		disk := sessionOperationDiskReplacement{
			SessionID: replacement.Key.SessionID,
			Subpath:   replacement.Key.Subpath,
			Entries:   cloneStoreEntries(replacement.Entries),
		}

		data, err := sessionOperationMarshal(disk)
		if err != nil {
			return fmt.Errorf("encode prepared session-operation replacement: %w", err)
		}

		filename := fmt.Sprintf("replacement-%06d.json", sequence)

		path := filepath.Join(j.directory, filename)
		if err := atomicWriteSessionOperationFile(path, append(data, '\n'), 0o600); err != nil {
			return fmt.Errorf("write prepared session-operation replacement: %w", err)
		}

		digest := sha256.Sum256(append(data, '\n'))
		manifest.Parts = append(manifest.Parts, sessionOperationPreparedReplacementPart{
			Sequence:   sequence,
			Subpath:    replacement.Key.Subpath,
			Filename:   filename,
			SHA256:     hex.EncodeToString(digest[:]),
			Bytes:      len(data) + 1,
			EntryCount: len(replacement.Entries),
		})
	}

	if mainCount != 1 {
		return errors.New("prepared replacements must contain the main session exactly once")
	}

	manifestData, err := sessionOperationMarshal(manifest)
	if err != nil {
		return fmt.Errorf("encode prepared session-operation manifest: %w", err)
	}

	digest := sha256.Sum256(manifestData)

	next := j.record
	next.Prepared = &manifest
	next.PreparedManifestSHA256 = hex.EncodeToString(digest[:])
	next.Phase = sessionOperationPhaseStorePrepared

	next.UpdatedAtUnixMilli = sessionOperationNow().UnixMilli()
	if err := validateSessionOperationJournalRecord(next); err != nil {
		return err
	}

	previous := j.record

	j.record = next
	if err := j.persist(); err != nil {
		j.record = previous

		return err
	}

	return nil
}

func (j *sessionOperationJournal) preparedReplacements() ([]SessionStoreReplacement, error) {
	if j == nil || j.record.Prepared == nil {
		return nil, errors.New("hermes session-operation publication is not prepared")
	}

	if err := validatePreparedSessionOperationManifest(j.record); err != nil {
		return nil, err
	}

	replacements := make([]SessionStoreReplacement, 0, len(j.record.Prepared.Parts))
	for _, part := range j.record.Prepared.Parts {
		path := filepath.Join(j.directory, part.Filename)

		data, err := readBoundedSessionOperationFile(path, int64(part.Bytes))
		if err != nil {
			return nil, fmt.Errorf("read prepared session-operation replacement: %w", err)
		}

		if len(data) != part.Bytes {
			return nil, errors.New("prepared session-operation replacement size mismatch")
		}

		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != part.SHA256 {
			return nil, errors.New("prepared session-operation replacement hash mismatch")
		}

		var disk sessionOperationDiskReplacement

		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&disk); err != nil {
			return nil, fmt.Errorf("decode prepared session-operation replacement: %w", err)
		}

		if decoder.Decode(&struct{}{}) != io.EOF {
			return nil, errors.New("prepared session-operation replacement has trailing data")
		}

		if disk.SessionID != j.record.LogicalSessionID || disk.Subpath != part.Subpath || len(disk.Entries) != part.EntryCount {
			return nil, errors.New("prepared session-operation replacement identity mismatch")
		}

		replacements = append(replacements, SessionStoreReplacement{
			Key:     SessionKey{SessionID: disk.SessionID, Subpath: disk.Subpath},
			Entries: cloneStoreEntries(disk.Entries),
		})
	}

	return replacements, nil
}

func inspectPreparedSessionOperationStore(ctx context.Context, store SessionStore, journal *sessionOperationJournal) (sessionOperationStoreState, error) {
	if store == nil {
		return sessionOperationStoreAmbiguous, fmt.Errorf("%w: nil session store", ErrSessionOperationStoreUnavailable)
	}

	expected, err := journal.preparedReplacements()
	if err != nil {
		return sessionOperationStoreAmbiguous, err
	}

	expectedBySubpath := make(map[string][]SessionStoreEntry, len(expected))

	expectedSubpaths := make([]string, 0, len(expected)-1)
	for _, replacement := range expected {
		expectedBySubpath[replacement.Key.Subpath] = replacement.Entries
		if replacement.Key.Subpath != SessionStoreMainSubpath {
			expectedSubpaths = append(expectedSubpaths, replacement.Key.Subpath)
		}
	}

	slices.Sort(expectedSubpaths)

	mainKey := SessionKey{SessionID: journal.record.LogicalSessionID, Subpath: SessionStoreMainSubpath}

	actualMain, err := store.Load(ctx, mainKey)
	if err != nil {
		return sessionOperationStoreAmbiguous, fmt.Errorf("%w: load main session: %v", ErrSessionOperationStoreUnavailable, err)
	}

	actualSubpaths, err := store.ListSubkeys(ctx, mainKey)
	if err != nil {
		return sessionOperationStoreAmbiguous, fmt.Errorf("%w: list session subkeys: %v", ErrSessionOperationStoreUnavailable, err)
	}

	slices.Sort(actualSubpaths)

	if len(actualMain) == 0 && len(actualSubpaths) == 0 {
		return sessionOperationStoreAbsent, nil
	}

	if !slices.Equal(actualSubpaths, expectedSubpaths) || !equalSessionOperationEntries(actualMain, expectedBySubpath[SessionStoreMainSubpath]) {
		return sessionOperationStoreAmbiguous, nil
	}

	for _, subpath := range expectedSubpaths {
		actual, loadErr := store.Load(ctx, SessionKey{SessionID: journal.record.LogicalSessionID, Subpath: subpath})
		if loadErr != nil {
			return sessionOperationStoreAmbiguous, fmt.Errorf("%w: load session subkey %q: %v", ErrSessionOperationStoreUnavailable, subpath, loadErr)
		}

		if !equalSessionOperationEntries(actual, expectedBySubpath[subpath]) {
			return sessionOperationStoreAmbiguous, nil
		}
	}

	return sessionOperationStoreExact, nil
}

func sessionOperationBaselineDelta(baseline []string, current []string) (string, bool, error) {
	base, err := normalizeSessionOperationIDs(baseline)
	if err != nil {
		return "", false, err
	}

	now, err := normalizeSessionOperationIDs(current)
	if err != nil {
		return "", false, err
	}

	baseSet := make(map[string]struct{}, len(base))
	for _, id := range base {
		baseSet[id] = struct{}{}
	}

	nowSet := make(map[string]struct{}, len(now))
	for _, id := range now {
		nowSet[id] = struct{}{}
	}

	for _, id := range base {
		if _, exists := nowSet[id]; !exists {
			return "", false, fmt.Errorf("%w: persisted-session baseline member %q disappeared", ErrSessionOperationAmbiguous, id)
		}
	}

	added := make([]string, 0, 1)

	for _, id := range now {
		if _, exists := baseSet[id]; !exists {
			added = append(added, id)
		}
	}

	switch len(added) {
	case 0:
		return "", false, nil
	case 1:
		return added[0], true, nil
	default:
		return "", false, fmt.Errorf("%w: persisted-session delta added %d candidates", ErrSessionOperationAmbiguous, len(added))
	}
}

func (j *sessionOperationJournal) markStoreCommitted() error {
	phase := sessionOperationPhaseStoreCommitted

	return j.update(sessionOperationJournalPatch{Phase: &phase})
}

func (j *sessionOperationJournal) removeCommitted() error {
	if j == nil {
		return nil
	}

	if j.record.Phase != sessionOperationPhaseStoreCommitted {
		return errors.New("refuse to remove an uncommitted Hermes session-operation journal")
	}

	if err := sessionOperationRemoveAll(j.directory); err != nil {
		return fmt.Errorf("remove committed Hermes session-operation journal: %w", err)
	}

	return syncSessionOperationDirectory(filepath.Dir(j.directory))
}

func (j *sessionOperationJournal) discardPrepared() error {
	if j == nil {
		return nil
	}

	if j.record.Phase != sessionOperationPhasePrepared || j.record.NativeSessionID != "" || j.record.Prepared != nil {
		return errors.New("refuse to discard a Hermes session-operation journal after native mutation")
	}

	if err := sessionOperationRemoveAll(j.directory); err != nil {
		return fmt.Errorf("discard prepared Hermes session-operation journal: %w", err)
	}

	return syncSessionOperationDirectory(filepath.Dir(j.directory))
}

// removeRecovered clears a journal only after the recovery caller has proved
// that neither its prepared Store bundle nor its native mutation remains.
func (j *sessionOperationJournal) removeRecovered() error {
	if j == nil {
		return nil
	}

	if err := sessionOperationRemoveAll(j.directory); err != nil {
		return fmt.Errorf("remove recovered Hermes session-operation journal: %w", err)
	}

	return syncSessionOperationDirectory(filepath.Dir(j.directory))
}

func (j *sessionOperationJournal) persist() error {
	data, err := sessionOperationMarshal(j.record)
	if err != nil {
		return fmt.Errorf("encode Hermes session-operation journal: %w", err)
	}

	if err := atomicWriteSessionOperationFile(j.path, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("commit Hermes session-operation journal: %w", err)
	}

	return nil
}

func atomicWriteSessionOperationFile(path string, data []byte, mode os.FileMode) (returnErr error) {
	directory := filepath.Dir(path)

	temp, err := sessionOperationCreateTemp(directory, ".session-operation-*")
	if err != nil {
		return fmt.Errorf("create temporary session-operation file: %w", err)
	}

	tempPath := temp.Name()

	closed := false
	defer func() {
		if !closed {
			returnErr = errors.Join(returnErr, temp.Close())
		}

		if removeErr := sessionOperationRemove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, removeErr)
		}
	}()

	if err := temp.Chmod(mode); err != nil {
		return err
	}

	if _, err := temp.Write(data); err != nil {
		return err
	}

	if err := temp.Sync(); err != nil {
		return err
	}

	if err := temp.Close(); err != nil {
		return err
	}

	closed = true

	if err := sessionOperationRename(tempPath, path); err != nil {
		return fmt.Errorf("rename session-operation file: %w", err)
	}

	return syncSessionOperationDirectory(directory)
}

func ensureSessionOperationDirectory(path string) error {
	if err := sessionOperationMkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create Hermes session-operation journal root: %w", err)
	}

	info, err := sessionOperationLstat(path)
	if err != nil {
		return fmt.Errorf("inspect Hermes session-operation journal root: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("hermes session-operation journal root is not a directory")
	}

	if err := sessionOperationChmod(path, 0o700); err != nil {
		return fmt.Errorf("protect Hermes session-operation journal root: %w", err)
	}

	if err := syncSessionOperationDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync Hermes session-operation journal parent: %w", err)
	}

	return nil
}

//nolint:gocyclo // Validation intentionally enumerates the full durable state-machine contract.
func validateSessionOperationJournalRecord(record sessionOperationJournalRecord) error {
	if record.Format != sessionOperationJournalFormat || !validSessionOperationID(record.OperationID) {
		return errors.New("invalid Hermes session-operation format or id")
	}

	if record.Kind != sessionOperationKindNew && record.Kind != sessionOperationKindFork {
		return errors.New("invalid Hermes session-operation kind")
	}

	if record.Mode != sessionOperationModeShared {
		return errors.New("invalid Hermes session-operation mode")
	}

	if sessionOperationPhaseOrder(record.Phase) == 0 {
		return errors.New("invalid Hermes session-operation phase")
	}

	if record.LogicalSessionID == "" || strings.TrimSpace(record.LogicalSessionID) != record.LogicalSessionID || record.Marker == "" {
		return errors.New("hermes session-operation logical id and marker are required")
	}

	if record.Kind == sessionOperationKindFork && (record.ParentLogicalSessionID == "" || record.ParentNativeSessionID == "") {
		return errors.New("hermes fork operation requires parent identities")
	}

	switch record.Phase {
	case sessionOperationPhasePrepared, sessionOperationPhaseMutating:
		if record.NativeSessionID != "" || record.LiveSessionID != "" {
			return errors.New("pre-effect Hermes session-operation cannot name a native session")
		}
	case sessionOperationPhaseNativeIdentified:
		if record.Kind != sessionOperationKindNew || record.NativeSessionID == "" || record.LiveSessionID == "" {
			return errors.New("identified Hermes new-session operation requires live and durable identities")
		}
	case sessionOperationPhaseLiveFenced:
		if record.Kind != sessionOperationKindFork || record.NativeSessionID == "" || record.LiveSessionID != "" {
			return errors.New("fenced Hermes fork operation requires only a durable child identity")
		}
	case sessionOperationPhaseStorePrepared, sessionOperationPhaseStoreCommitted:
		if record.NativeSessionID == "" ||
			(record.Kind == sessionOperationKindNew && record.LiveSessionID == "") ||
			(record.Kind == sessionOperationKindFork && record.LiveSessionID != "") {
			return errors.New("published Hermes session-operation identities do not match its kind")
		}
	case sessionOperationPhaseTargetReady:
		return errors.New("unsupported Hermes session-operation phase")
	}

	if record.Origin.PID <= 0 || record.Origin.KernelStartTime == "" {
		return errors.New("hermes session-operation process identity is incomplete")
	}

	if record.CreatedAtUnixMilli <= 0 || record.UpdatedAtUnixMilli < record.CreatedAtUnixMilli {
		return errors.New("hermes session-operation timestamps are invalid")
	}

	if _, err := normalizeSessionOperationIDs(record.BaselineNativeSessionIDs); err != nil {
		return err
	}

	if record.Prepared == nil {
		if record.PreparedManifestSHA256 != "" || sessionOperationPhaseOrder(record.Phase) >= sessionOperationPhaseOrder(sessionOperationPhaseStorePrepared) {
			return errors.New("hermes session-operation prepared manifest is missing")
		}

		return nil
	}

	if sessionOperationPhaseOrder(record.Phase) < sessionOperationPhaseOrder(sessionOperationPhaseStorePrepared) {
		return errors.New("hermes session-operation prepared manifest has an invalid phase")
	}

	return validatePreparedSessionOperationManifest(record)
}

func validatePreparedSessionOperationManifest(record sessionOperationJournalRecord) error {
	manifest := record.Prepared
	if manifest == nil || manifest.Format != sessionOperationJournalFormat || manifest.LogicalSessionID != record.LogicalSessionID || len(manifest.Parts) == 0 {
		return errors.New("invalid prepared session-operation manifest")
	}

	data, err := sessionOperationMarshal(manifest)
	if err != nil {
		return err
	}

	digest := sha256.Sum256(data)
	if record.PreparedManifestSHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("prepared session-operation manifest hash mismatch")
	}

	seen := make(map[string]struct{}, len(manifest.Parts))
	mainCount := 0

	for sequence, part := range manifest.Parts {
		if part.Sequence != sequence || part.Filename != fmt.Sprintf("replacement-%06d.json", sequence) || part.Bytes <= 0 || part.EntryCount < 0 {
			return errors.New("invalid prepared session-operation manifest part")
		}

		if len(part.SHA256) != sha256.Size*2 {
			return errors.New("invalid prepared session-operation replacement hash")
		}

		if _, err := hex.DecodeString(part.SHA256); err != nil {
			return errors.New("invalid prepared session-operation replacement hash")
		}

		if _, exists := seen[part.Subpath]; exists {
			return errors.New("duplicate prepared session-operation subpath")
		}

		seen[part.Subpath] = struct{}{}
		if part.Subpath == SessionStoreMainSubpath {
			mainCount++
		}
	}

	if mainCount != 1 {
		return errors.New("prepared session-operation manifest must contain one main replacement")
	}

	return nil
}

func validSessionOperationID(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}

	_, err := hex.DecodeString(value)

	return err == nil
}

func sessionOperationPhaseOrder(phase sessionOperationPhase) int {
	switch phase {
	case sessionOperationPhasePrepared:
		return 1
	case sessionOperationPhaseMutating:
		return 2
	case sessionOperationPhaseNativeIdentified:
		return 3
	case sessionOperationPhaseLiveFenced:
		return 4
	case sessionOperationPhaseTargetReady:
		return 5
	case sessionOperationPhaseStorePrepared:
		return 6
	case sessionOperationPhaseStoreCommitted:
		return 7
	default:
		return 0
	}
}

func normalizeSessionOperationIDs(values []string) ([]string, error) {
	normalized := cloneAndSortSessionOperationIDs(values)
	for index, value := range normalized {
		if value == "" || strings.TrimSpace(value) != value {
			return nil, errors.New("persisted-session baseline contains an invalid id")
		}

		if index > 0 && normalized[index-1] == value {
			return nil, errors.New("persisted-session baseline contains a duplicate id")
		}
	}

	return normalized, nil
}

func cloneAndSortSessionOperationIDs(values []string) []string {
	cloned := slices.Clone(values)
	slices.Sort(cloned)

	return cloned
}

func equalSessionOperationEntries(left, right []SessionStoreEntry) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if !slices.Equal(left[index], right[index]) {
			return false
		}
	}

	return true
}

func readBoundedSessionOperationFile(path string, limit int64) ([]byte, error) {
	file, err := sessionOperationOpen(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > limit {
		return nil, errors.New("session-operation file exceeds its declared bound")
	}

	return data, nil
}
