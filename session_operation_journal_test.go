//nolint:govet // Journal fault matrices intentionally use repeated scoped errors.
package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestSessionOperationJournalRoundTripAndPreparedStoreProof(t *testing.T) {
	home := filepath.Join(t.TempDir(), "hermes-home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindFork)
	if !slices.Equal(journal.record.BaselineNativeSessionIDs, []string{"native-a", "native-b"}) {
		t.Fatalf("baseline=%v", journal.record.BaselineNativeSessionIDs)
	}
	if journal.record.ParentLogicalSessionID != "parent-acp" || journal.record.ParentNativeSessionID != "parent-native" {
		t.Fatalf("parent fields=%+v", journal.record)
	}

	phase := sessionOperationPhaseLiveFenced
	nativeID := "native-child"
	if err := journal.update(sessionOperationJournalPatch{
		Phase:           &phase,
		NativeSessionID: &nativeID,
	}); err != nil {
		t.Fatalf("update journal: %v", err)
	}

	replacements := testSessionOperationReplacements()
	if err := journal.prepareReplacements(replacements); err != nil {
		t.Fatalf("prepare replacements: %v", err)
	}
	loaded, err := loadSessionOperationJournal(journal.directory)
	if err != nil {
		t.Fatalf("load journal: %v", err)
	}
	actual, err := loaded.preparedReplacements()
	if err != nil {
		t.Fatalf("load prepared replacements: %v", err)
	}
	if !reflect.DeepEqual(actual, replacements) {
		t.Fatalf("prepared replacements=%#v want=%#v", actual, replacements)
	}

	store := NewInMemorySessionStore()
	state, err := inspectPreparedSessionOperationStore(t.Context(), store, loaded)
	if err != nil || state != sessionOperationStoreAbsent {
		t.Fatalf("empty store state=%v err=%v", state, err)
	}
	if err := store.Replace(t.Context(), SessionKey{SessionID: "logical-child"}, replacements); err != nil {
		t.Fatal(err)
	}
	state, err = inspectPreparedSessionOperationStore(t.Context(), store, loaded)
	if err != nil || state != sessionOperationStoreExact {
		t.Fatalf("exact store state=%v err=%v", state, err)
	}
	if err := store.Append(t.Context(), SessionKey{SessionID: "logical-child", Subpath: "other"}, []SessionStoreEntry{json.RawMessage(`{"unexpected":true}`)}); err != nil {
		t.Fatal(err)
	}
	state, err = inspectPreparedSessionOperationStore(t.Context(), store, loaded)
	if err != nil || state != sessionOperationStoreAmbiguous {
		t.Fatalf("different store state=%v err=%v", state, err)
	}

	if err := loaded.removeCommitted(); err == nil {
		t.Fatal("uncommitted journal removed")
	}
	if err := loaded.markStoreCommitted(); err != nil {
		t.Fatalf("mark committed: %v", err)
	}
	if err := loaded.removeCommitted(); err != nil {
		t.Fatalf("remove committed: %v", err)
	}
	if _, err := os.Stat(loaded.directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal directory remains: %v", err)
	}
}

func TestSessionOperationJournalPermissionsLocationAndNoSecretFields(t *testing.T) {
	home := filepath.Join(t.TempDir(), "official-home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	control, err := nativehermes.SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(journal.directory, home+string(filepath.Separator)) {
		t.Fatalf("journal %q is inside shared home %q", journal.directory, home)
	}
	for path, want := range map[string]os.FileMode{
		control: 0o700,
		filepath.Join(control, sessionOperationDirectoryName): 0o700,
		journal.directory: 0o700,
		journal.path:      0o600,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("%s mode=%#o want=%#o", path, got, want)
		}
	}

	data, err := os.ReadFile(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(data))
	for _, forbidden := range []string{"api_token", "access_token", "refresh_token", "capability", "mcpservers", `"env"`} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("journal contains forbidden secret-bearing field %q: %s", forbidden, data)
		}
	}
	if strings.Contains(string(data), "WAGIE_SECRET_SENTINEL") {
		t.Fatalf("journal leaked sentinel: %s", data)
	}
}

func TestSessionOperationJournalAtomicRenameFailurePreservesCommittedBytes(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	before, err := os.ReadFile(journal.path)
	if err != nil {
		t.Fatal(err)
	}

	previousRename := sessionOperationRename
	t.Cleanup(func() { sessionOperationRename = previousRename })
	wantErr := errors.New("injected rename failure")
	sessionOperationRename = func(string, string) error { return wantErr }
	phase := sessionOperationPhaseMutating
	if err := journal.update(sessionOperationJournalPatch{Phase: &phase}); !errors.Is(err, wantErr) {
		t.Fatalf("update error=%v", err)
	}
	after, err := os.ReadFile(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after, before) || journal.record.Phase != sessionOperationPhasePrepared {
		t.Fatalf("failed update changed committed journal: phase=%s", journal.record.Phase)
	}
	entries, err := os.ReadDir(journal.directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != sessionOperationJournalName {
		t.Fatalf("temporary files remain after rename failure: %v", entries)
	}
}

func TestSessionOperationJournalMalformedAndUnexpectedEntriesFailClosed(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	if err := os.WriteFile(journal.path, []byte(`{"format":"wrong"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSessionOperationJournal(journal.directory); !errors.Is(err, ErrSessionOperationAmbiguous) {
		t.Fatalf("malformed journal error=%v", err)
	}
	if _, err := findPendingSessionOperationJournals(home, "", ""); !errors.Is(err, ErrSessionOperationAmbiguous) {
		t.Fatalf("malformed pending journal error=%v", err)
	}

	if err := os.RemoveAll(journal.directory); err != nil {
		t.Fatal(err)
	}
	control, err := nativehermes.SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}
	unexpected := filepath.Join(control, sessionOperationDirectoryName, "not-an-operation")
	if err := os.WriteFile(unexpected, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := findPendingSessionOperationJournals(home, "", ""); !errors.Is(err, ErrSessionOperationAmbiguous) {
		t.Fatalf("unexpected entry error=%v", err)
	}
}

func TestSessionOperationPreparedPayloadTamperFailsClosed(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	identifyTestNewSessionOperationJournal(t, journal)
	if err := journal.prepareReplacements(testSessionOperationReplacements()); err != nil {
		t.Fatal(err)
	}
	part := journal.record.Prepared.Parts[0]
	if err := os.WriteFile(filepath.Join(journal.directory, part.Filename), []byte(`{"tampered":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.preparedReplacements(); err == nil {
		t.Fatal("tampered prepared replacement accepted")
	}

	data, err := os.ReadFile(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	record["preparedManifestSHA256"] = strings.Repeat("0", 64)
	corrupt, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journal.path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSessionOperationJournal(journal.directory); !errors.Is(err, ErrSessionOperationAmbiguous) {
		t.Fatalf("manifest tamper error=%v", err)
	}
}

func TestSessionOperationBaselineDeltaNeverChoosesAmbiguousCandidate(t *testing.T) {
	tests := []struct {
		name      string
		baseline  []string
		current   []string
		candidate string
		found     bool
		ambiguous bool
	}{
		{name: "no change", baseline: []string{"a"}, current: []string{"a"}},
		{name: "one candidate", baseline: []string{"b", "a"}, current: []string{"c", "b", "a"}, candidate: "c", found: true},
		{name: "two candidates", baseline: []string{"a"}, current: []string{"a", "b", "c"}, ambiguous: true},
		{name: "baseline disappeared", baseline: []string{"a", "b"}, current: []string{"b", "c"}, ambiguous: true},
		{name: "duplicate current", current: []string{"a", "a"}, ambiguous: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, found, err := sessionOperationBaselineDelta(test.baseline, test.current)
			if test.ambiguous {
				if err == nil {
					t.Fatalf("candidate=%q found=%v without ambiguity", candidate, found)
				}

				return
			}
			if err != nil || candidate != test.candidate || found != test.found {
				t.Fatalf("candidate=%q found=%v err=%v", candidate, found, err)
			}
		})
	}
}

func TestSessionOperationStoreUnavailableRemainsUnknown(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	identifyTestNewSessionOperationJournal(t, journal)
	if err := journal.prepareReplacements(testSessionOperationReplacements()); err != nil {
		t.Fatal(err)
	}
	state, err := inspectPreparedSessionOperationStore(t.Context(), sessionOperationUnavailableStore{}, journal)
	if state != sessionOperationStoreAmbiguous || !errors.Is(err, ErrSessionOperationStoreUnavailable) {
		t.Fatalf("state=%v err=%v", state, err)
	}
	state, err = inspectPreparedSessionOperationStore(t.Context(), nil, journal)
	if state != sessionOperationStoreAmbiguous || !errors.Is(err, ErrSessionOperationStoreUnavailable) {
		t.Fatalf("nil store state=%v err=%v", state, err)
	}
}

func TestSessionOperationFindFiltersAndPreparedDiscard(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	other := newTestSessionOperationJournalWithLogical(t, home, sessionOperationKindFork, "other-logical")
	found, err := findPendingSessionOperationJournals(home, "logical-child", sessionOperationKindNew)
	if err != nil || len(found) != 1 || found[0].record.OperationID != journal.record.OperationID {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if err := other.discardPrepared(); err != nil {
		t.Fatalf("discard prepared: %v", err)
	}
	phase := sessionOperationPhaseMutating
	if err := journal.update(sessionOperationJournalPatch{Phase: &phase}); err != nil {
		t.Fatal(err)
	}
	if err := journal.discardPrepared(); err == nil {
		t.Fatal("mutating journal discarded")
	}
}

type sessionOperationUnavailableStore struct{}

func (sessionOperationUnavailableStore) Append(context.Context, SessionKey, []SessionStoreEntry) error {
	return errors.New("offline")
}
func (sessionOperationUnavailableStore) Load(context.Context, SessionKey) ([]SessionStoreEntry, error) {
	return nil, errors.New("offline")
}
func (sessionOperationUnavailableStore) Replace(context.Context, SessionKey, []SessionStoreReplacement) error {
	return errors.New("offline")
}
func (sessionOperationUnavailableStore) Delete(context.Context, SessionKey) error {
	return errors.New("offline")
}
func (sessionOperationUnavailableStore) ListSessions(context.Context) ([]SessionSummary, error) {
	return nil, errors.New("offline")
}
func (sessionOperationUnavailableStore) ListSubkeys(context.Context, SessionKey) ([]string, error) {
	return nil, errors.New("offline")
}

func newTestSessionOperationJournal(t *testing.T, home string, kind sessionOperationKind) *sessionOperationJournal {
	t.Helper()

	return newTestSessionOperationJournalWithLogical(t, home, kind, "logical-child")
}

func newTestSessionOperationJournalWithLogical(t *testing.T, home string, kind sessionOperationKind, logicalID string) *sessionOperationJournal {
	t.Helper()
	operationID, err := newSessionOperationID()
	if err != nil {
		t.Fatal(err)
	}
	origin, err := nativehermes.CurrentDurableProcessIdentity()
	if err != nil {
		t.Fatal(err)
	}
	fields := sessionOperationJournalFields{
		OperationID:              operationID,
		Kind:                     kind,
		Mode:                     sessionOperationModeShared,
		LogicalSessionID:         logicalID,
		SourceRoot:               filepath.Join(home, "source"),
		TargetRoot:               filepath.Join(home, "target"),
		Marker:                   "__acpgo_pending_" + operationID,
		FinalTitle:               "Hermes session",
		BaselineNativeSessionIDs: []string{"native-b", "native-a"},
		Origin:                   origin,
	}
	if kind == sessionOperationKindFork {
		fields.ParentLogicalSessionID = "parent-acp"
		fields.ParentNativeSessionID = "parent-native"
	}
	journal, err := beginSessionOperationJournal(home, fields)
	if err != nil {
		t.Fatalf("begin journal: %v", err)
	}

	return journal
}

func testSessionOperationReplacements() []SessionStoreReplacement {
	return []SessionStoreReplacement{
		{
			Key:     SessionKey{SessionID: "logical-child", Subpath: "state-db"},
			Entries: []SessionStoreEntry{json.RawMessage(`{"sequence":0}`), json.RawMessage(`{"sequence":1}`)},
		},
		{
			Key:     SessionKey{SessionID: "logical-child", Subpath: SessionStoreMainSubpath},
			Entries: []SessionStoreEntry{json.RawMessage(`{"format":"hermes-state-db-v1"}`)},
		},
		{
			Key:     SessionKey{SessionID: "logical-child", Subpath: "idmap"},
			Entries: []SessionStoreEntry{json.RawMessage(`{"nativeSessionId":"native-child"}`)},
		},
	}
}

func identifyTestNewSessionOperationJournal(t *testing.T, journal *sessionOperationJournal) {
	t.Helper()
	phase := sessionOperationPhaseNativeIdentified
	nativeID, liveID := "native-child", "live-child"
	if err := journal.update(sessionOperationJournalPatch{
		Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSessionOperationJournalRejectsPhaseRegressionAndInvalidPreparation(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	mutating := sessionOperationPhaseMutating
	if err := journal.update(sessionOperationJournalPatch{Phase: &mutating}); err != nil {
		t.Fatal(err)
	}
	prepared := sessionOperationPhasePrepared
	if err := journal.update(sessionOperationJournalPatch{Phase: &prepared}); err == nil {
		t.Fatal("phase regression accepted")
	}

	invalid := testSessionOperationReplacements()
	invalid[0].Key.SessionID = "wrong"
	if err := journal.prepareReplacements(invalid); err == nil {
		t.Fatal("cross-session replacement accepted")
	}
	invalid = testSessionOperationReplacements()
	invalid = append(invalid, invalid[0])
	if err := journal.prepareReplacements(invalid); err == nil {
		t.Fatal("duplicate replacement accepted")
	}
	invalid = testSessionOperationReplacements()[:1]
	if err := journal.prepareReplacements(invalid); err == nil {
		t.Fatal("replacement set without main accepted")
	}
}

func TestSessionOperationJournalTimestampsAdvance(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	previousNow := sessionOperationNow
	t.Cleanup(func() { sessionOperationNow = previousNow })
	instant := time.UnixMilli(100)
	sessionOperationNow = func() time.Time { return instant }
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	instant = time.UnixMilli(101)
	phase := sessionOperationPhaseMutating
	if err := journal.update(sessionOperationJournalPatch{Phase: &phase}); err != nil {
		t.Fatal(err)
	}
	if journal.record.CreatedAtUnixMilli != 100 || journal.record.UpdatedAtUnixMilli != 101 {
		t.Fatalf("timestamps=%d/%d", journal.record.CreatedAtUnixMilli, journal.record.UpdatedAtUnixMilli)
	}
}
