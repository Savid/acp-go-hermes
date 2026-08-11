package hermesacp

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

type sessionOperationErrorReader struct{ err error }

func (r sessionOperationErrorReader) Read([]byte) (int, error) { return 0, r.err }

type sessionOperationFaultStore struct {
	base           SessionStore
	loadErrors     map[string]error
	listSubkeysErr error
	deleteErr      error
}

type sessionOperationServerOnly struct{ nativehermes.Server }

type sessionOperationRecoveryServer struct {
	*fakeHermesClient
	listCalls      int
	secondListErr  error
	retainOnDelete bool
}

func (s *sessionOperationRecoveryServer) PersistedSessions(ctx context.Context) ([]nativehermes.Session, error) {
	s.listCalls++
	if s.listCalls > 1 && s.secondListErr != nil {
		return nil, s.secondListErr
	}

	return s.fakeHermesClient.PersistedSessions(ctx)
}

func (s *sessionOperationRecoveryServer) DeleteSession(ctx context.Context, id string) error {
	if s.retainOnDelete {
		return nil
	}

	return s.fakeHermesClient.DeleteSession(ctx, id)
}

func (s sessionOperationFaultStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	return s.base.Append(ctx, key, entries)
}

func (s sessionOperationFaultStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if err := s.loadErrors[key.Subpath]; err != nil {
		return nil, err
	}

	return s.base.Load(ctx, key)
}

func (s sessionOperationFaultStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	return s.base.Replace(ctx, key, replacements)
}

func (s sessionOperationFaultStore) Delete(ctx context.Context, key SessionKey) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}

	return s.base.Delete(ctx, key)
}

func (s sessionOperationFaultStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	return s.base.ListSessions(ctx)
}

func (s sessionOperationFaultStore) ListSubkeys(ctx context.Context, key SessionKey) ([]string, error) {
	if s.listSubkeysErr != nil {
		return nil, s.listSubkeysErr
	}

	return s.base.ListSubkeys(ctx, key)
}

func TestSessionOperationIDEntropyFailure(t *testing.T) {
	previous := cryptorand.Reader
	cryptorand.Reader = sessionOperationErrorReader{err: errors.New("entropy unavailable")}
	t.Cleanup(func() { cryptorand.Reader = previous })

	if _, err := newSessionOperationID(); err == nil {
		t.Fatal("entropy failure was ignored")
	}
}

func TestSessionOperationJournalBeginFailures(t *testing.T) {
	validFields := func(t *testing.T) sessionOperationJournalFields {
		t.Helper()
		origin, err := nativehermes.CurrentDurableProcessIdentity()
		if err != nil {
			t.Fatal(err)
		}

		return sessionOperationJournalFields{
			OperationID: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Kind:        sessionOperationKindNew, Mode: sessionOperationModeShared,
			LogicalSessionID: "logical", Marker: "marker", Origin: origin,
		}
	}

	t.Run("invalid home", func(t *testing.T) {
		if _, err := beginSessionOperationJournal("relative", validFields(t)); err == nil {
			t.Fatal("invalid shared home accepted")
		}
	})

	t.Run("ensure operations", func(t *testing.T) {
		home := t.TempDir()
		previous := sessionOperationMkdirAll
		sessionOperationMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir all") }
		t.Cleanup(func() { sessionOperationMkdirAll = previous })
		if _, err := beginSessionOperationJournal(home, validFields(t)); err == nil {
			t.Fatal("operations root failure ignored")
		}
	})

	t.Run("invalid record", func(t *testing.T) {
		fields := validFields(t)
		fields.Marker = ""
		if _, err := beginSessionOperationJournal(t.TempDir(), fields); err == nil {
			t.Fatal("invalid record accepted")
		}
	})

	t.Run("operation mkdir", func(t *testing.T) {
		previous := sessionOperationMkdir
		sessionOperationMkdir = func(string, os.FileMode) error { return errors.New("mkdir") }
		t.Cleanup(func() { sessionOperationMkdir = previous })
		if _, err := beginSessionOperationJournal(t.TempDir(), validFields(t)); err == nil {
			t.Fatal("operation mkdir failure ignored")
		}
	})

	t.Run("operation chmod cleans", func(t *testing.T) {
		previousChmod, previousRemoveAll := sessionOperationChmod, sessionOperationRemoveAll
		removed := false
		chmodCalls := 0
		sessionOperationChmod = func(string, os.FileMode) error {
			chmodCalls++
			if chmodCalls == 2 {
				return errors.New("chmod")
			}

			return nil
		}
		sessionOperationRemoveAll = func(path string) error {
			removed = true

			return os.RemoveAll(path)
		}
		t.Cleanup(func() {
			sessionOperationChmod = previousChmod
			sessionOperationRemoveAll = previousRemoveAll
		})
		if _, err := beginSessionOperationJournal(t.TempDir(), validFields(t)); err == nil || !removed {
			t.Fatalf("chmod error=%v removed=%v", err, removed)
		}
	})

	t.Run("operation entry sync", func(t *testing.T) {
		previous := sessionOperationChmod
		call := 0
		sessionOperationChmod = func(path string, mode os.FileMode) error {
			call++
			if call == 2 {
				if err := os.RemoveAll(filepath.Dir(path)); err != nil {
					return err
				}
			}

			return nil
		}
		t.Cleanup(func() { sessionOperationChmod = previous })
		if _, err := beginSessionOperationJournal(t.TempDir(), validFields(t)); err == nil {
			t.Fatal("operation entry sync failure ignored")
		}
	})

	t.Run("initial persist", func(t *testing.T) {
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return nil, errors.New("create temp") }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if _, err := beginSessionOperationJournal(t.TempDir(), validFields(t)); err == nil {
			t.Fatal("initial journal persist failure ignored")
		}
	})
}

func TestSessionOperationJournalDiscoveryAndLoadFailures(t *testing.T) {
	if _, err := findPendingSessionOperationJournals("relative", "", ""); err == nil {
		t.Fatal("invalid shared home accepted")
	}

	home := t.TempDir()
	previousReadDir := sessionOperationReadDir
	sessionOperationReadDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir") }
	if _, err := findPendingSessionOperationJournals(home, "", ""); err == nil {
		t.Fatal("readdir failure ignored")
	}
	sessionOperationReadDir = previousReadDir
	t.Cleanup(func() { sessionOperationReadDir = previousReadDir })

	first := newTestSessionOperationJournalWithLogical(t, home, sessionOperationKindNew, "first")
	second := newTestSessionOperationJournalWithLogical(t, home, sessionOperationKindFork, "second")
	found, err := findPendingSessionOperationJournals(home, "", "")
	if err != nil || len(found) != 2 || found[0].record.OperationID > found[1].record.OperationID {
		t.Fatalf("sorted journals=%v err=%v", found, err)
	}
	filtered, err := findPendingSessionOperationJournals(home, "first", sessionOperationKindFork)
	if err != nil || len(filtered) != 0 {
		t.Fatalf("kind filtered journals=%v err=%v", filtered, err)
	}

	previousLstat := sessionOperationLstat
	sessionOperationLstat = func(string) (os.FileInfo, error) { return nil, errors.New("lstat") }
	if _, loadErr := loadSessionOperationJournal(first.directory); loadErr == nil {
		t.Fatal("lstat failure ignored")
	}
	sessionOperationLstat = previousLstat

	notDirectory := filepath.Join(t.TempDir(), "file")
	if writeErr := os.WriteFile(notDirectory, []byte("x"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, loadErr := loadSessionOperationJournal(notDirectory); loadErr == nil {
		t.Fatal("non-directory accepted")
	}

	missingJournalDir := filepath.Join(t.TempDir(), "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if mkdirErr := os.Mkdir(missingJournalDir, 0o700); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	if _, loadErr := loadSessionOperationJournal(missingJournalDir); loadErr == nil {
		t.Fatal("missing journal accepted")
	}

	if writeErr := os.WriteFile(first.path, []byte("{"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, loadErr := loadSessionOperationJournal(first.directory); loadErr == nil {
		t.Fatal("invalid JSON accepted")
	}

	if persistErr := second.persist(); persistErr != nil {
		t.Fatal(persistErr)
	}
	data, err := os.ReadFile(second.path)
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := os.WriteFile(second.path, append(data, []byte("{}")...), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, loadErr := loadSessionOperationJournal(second.directory); loadErr == nil {
		t.Fatal("trailing JSON accepted")
	}

	if persistErr := second.persist(); persistErr != nil {
		t.Fatal(persistErr)
	}
	mismatch := filepath.Join(filepath.Dir(second.directory), strings.Repeat("a", 64))
	if renameErr := os.Rename(second.directory, mismatch); renameErr != nil {
		t.Fatal(renameErr)
	}
	if _, loadErr := loadSessionOperationJournal(mismatch); loadErr == nil {
		t.Fatal("directory/id mismatch accepted")
	}
}

func TestSessionOperationJournalUpdateAndPreparationFailures(t *testing.T) {
	var nilJournal *sessionOperationJournal
	if err := nilJournal.update(sessionOperationJournalPatch{}); err == nil {
		t.Fatal("nil update accepted")
	}
	if err := nilJournal.prepareReplacements(nil); err == nil {
		t.Fatal("nil preparation accepted")
	}

	home := t.TempDir()
	journal := newTestSessionOperationJournal(t, home, sessionOperationKindNew)
	marker := "changed-marker"
	baseline := []string{"z", "a"}
	origin := journal.record.Origin
	if err := journal.update(sessionOperationJournalPatch{Marker: &marker, BaselineNativeSessionIDs: &baseline, Origin: &origin}); err != nil {
		t.Fatal(err)
	}
	badMarker := ""
	if err := journal.update(sessionOperationJournalPatch{Marker: &badMarker}); err == nil {
		t.Fatal("invalid patch accepted")
	}

	committed := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
	committed.record.Phase = sessionOperationPhaseStoreCommitted
	if err := committed.prepareReplacements(testSessionOperationReplacements()); err == nil {
		t.Fatal("committed journal prepared")
	}

	identifyTestNewSessionOperationJournal(t, journal)
	invalidJSON := testSessionOperationReplacements()
	invalidJSON[0].Entries = []SessionStoreEntry{json.RawMessage("{")}
	if err := journal.prepareReplacements(invalidJSON); err == nil {
		t.Fatal("invalid replacement JSON accepted")
	}

	previousCreateTemp := sessionOperationCreateTemp
	sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return nil, errors.New("payload write") }
	if err := journal.prepareReplacements(testSessionOperationReplacements()); err == nil {
		t.Fatal("payload write failure ignored")
	}
	sessionOperationCreateTemp = previousCreateTemp
	t.Cleanup(func() { sessionOperationCreateTemp = previousCreateTemp })

	previousNow := sessionOperationNow
	sessionOperationNow = func() time.Time { return time.UnixMilli(1) }
	if err := journal.prepareReplacements(testSessionOperationReplacements()); err == nil {
		t.Fatal("invalid prepared timestamp accepted")
	}
	sessionOperationNow = previousNow
	t.Cleanup(func() { sessionOperationNow = previousNow })

	previousRename := sessionOperationRename
	sessionOperationRename = func(source, target string) error {
		if filepath.Base(target) == sessionOperationJournalName {
			return errors.New("manifest persist")
		}

		return previousRename(source, target)
	}
	if err := journal.prepareReplacements(testSessionOperationReplacements()); err == nil || journal.record.Prepared != nil {
		t.Fatalf("persist failure err=%v prepared=%v", err, journal.record.Prepared)
	}
	sessionOperationRename = previousRename
	t.Cleanup(func() { sessionOperationRename = previousRename })

	if err := journal.prepareReplacements(testSessionOperationReplacements()); err != nil {
		t.Fatal(err)
	}
	if err := journal.prepareReplacements(testSessionOperationReplacements()); err == nil {
		t.Fatal("duplicate preparation accepted")
	}
}

func TestSessionOperationPreparedReplacementFailures(t *testing.T) {
	var nilJournal *sessionOperationJournal
	if _, err := nilJournal.preparedReplacements(); err == nil {
		t.Fatal("nil journal payload read accepted")
	}

	newPrepared := func(t *testing.T) *sessionOperationJournal {
		t.Helper()
		journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
		identifyTestNewSessionOperationJournal(t, journal)
		if err := journal.prepareReplacements(testSessionOperationReplacements()); err != nil {
			t.Fatal(err)
		}

		return journal
	}

	t.Run("invalid manifest", func(t *testing.T) {
		journal := newPrepared(t)
		journal.record.PreparedManifestSHA256 = strings.Repeat("0", 64)
		if _, err := journal.preparedReplacements(); err == nil {
			t.Fatal("invalid manifest accepted")
		}
	})

	t.Run("missing payload", func(t *testing.T) {
		journal := newPrepared(t)
		if err := os.Remove(filepath.Join(journal.directory, journal.record.Prepared.Parts[0].Filename)); err != nil {
			t.Fatal(err)
		}
		if _, err := journal.preparedReplacements(); err == nil {
			t.Fatal("missing payload accepted")
		}
	})

	t.Run("short payload", func(t *testing.T) {
		journal := newPrepared(t)
		part := journal.record.Prepared.Parts[0]
		if err := os.WriteFile(filepath.Join(journal.directory, part.Filename), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := journal.preparedReplacements(); err == nil {
			t.Fatal("short payload accepted")
		}
	})

	t.Run("hash mismatch", func(t *testing.T) {
		journal := newPrepared(t)
		part := journal.record.Prepared.Parts[0]
		path := filepath.Join(journal.directory, part.Filename)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data[0] ^= 1
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := journal.preparedReplacements(); err == nil {
			t.Fatal("payload hash mismatch accepted")
		}
	})

	for name, mutate := range map[string]func(*sessionOperationDiskReplacement){
		"decode":   func(disk *sessionOperationDiskReplacement) { disk.Entries = []SessionStoreEntry{json.RawMessage("{")} },
		"identity": func(disk *sessionOperationDiskReplacement) { disk.SessionID = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			journal := newPrepared(t)
			part := &journal.record.Prepared.Parts[0]
			path := filepath.Join(journal.directory, part.Filename)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var disk sessionOperationDiskReplacement
			if unmarshalErr := json.Unmarshal(data, &disk); unmarshalErr != nil {
				t.Fatal(unmarshalErr)
			}
			mutate(&disk)
			data, err = json.Marshal(disk)
			if name == "decode" {
				if err == nil {
					t.Fatal("invalid raw message unexpectedly encoded")
				}
				data = []byte(strings.Repeat("{", part.Bytes-1) + "\n")
			} else if err != nil {
				t.Fatal(err)
			} else {
				data = append(data, '\n')
			}
			part.Bytes = len(data)
			part.EntryCount = len(disk.Entries)
			digest := sha256.Sum256(data)
			part.SHA256 = hex.EncodeToString(digest[:])
			refreshPreparedManifestHash(t, journal)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := journal.preparedReplacements(); err == nil {
				t.Fatalf("%s payload accepted", name)
			}
		})
	}

	t.Run("trailing data", func(t *testing.T) {
		journal := newPrepared(t)
		part := &journal.record.Prepared.Parts[0]
		path := filepath.Join(journal.directory, part.Filename)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, []byte("{}")...)
		part.Bytes = len(data)
		digest := sha256.Sum256(data)
		part.SHA256 = hex.EncodeToString(digest[:])
		refreshPreparedManifestHash(t, journal)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := journal.preparedReplacements(); err == nil {
			t.Fatal("trailing payload accepted")
		}
	})
}

func refreshPreparedManifestHash(t *testing.T, journal *sessionOperationJournal) {
	t.Helper()
	data, err := json.Marshal(journal.record.Prepared)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	journal.record.PreparedManifestSHA256 = hex.EncodeToString(digest[:])
}

func TestSessionOperationPreparedStoreInspectionFailures(t *testing.T) {
	journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
	if _, err := inspectPreparedSessionOperationStore(t.Context(), NewInMemorySessionStore(), journal); err == nil {
		t.Fatal("unprepared journal accepted")
	}
	identifyTestNewSessionOperationJournal(t, journal)
	replacements := testSessionOperationReplacements()
	if err := journal.prepareReplacements(replacements); err != nil {
		t.Fatal(err)
	}

	base := NewInMemorySessionStore()
	if err := base.Replace(t.Context(), SessionKey{SessionID: "logical-child"}, replacements); err != nil {
		t.Fatal(err)
	}
	state, err := inspectPreparedSessionOperationStore(t.Context(), sessionOperationFaultStore{base: base, listSubkeysErr: errors.New("list")}, journal)
	if state != sessionOperationStoreAmbiguous || err == nil {
		t.Fatalf("list error state=%v err=%v", state, err)
	}
	state, err = inspectPreparedSessionOperationStore(t.Context(), sessionOperationFaultStore{base: base, loadErrors: map[string]error{"idmap": errors.New("load")}}, journal)
	if state != sessionOperationStoreAmbiguous || err == nil {
		t.Fatalf("subkey load error state=%v err=%v", state, err)
	}

	different := NewInMemorySessionStore()
	changed := testSessionOperationReplacements()
	changed[2].Entries = []SessionStoreEntry{json.RawMessage(`{"different":true}`)}
	if replaceErr := different.Replace(t.Context(), SessionKey{SessionID: "logical-child"}, changed); replaceErr != nil {
		t.Fatal(replaceErr)
	}
	state, err = inspectPreparedSessionOperationStore(t.Context(), different, journal)
	if state != sessionOperationStoreAmbiguous || err != nil {
		t.Fatalf("different subkey state=%v err=%v", state, err)
	}
}

func TestSessionOperationJournalValidationMatrix(t *testing.T) {
	journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
	base := journal.record

	mutations := map[string]func(*sessionOperationJournalRecord){
		"format":               func(record *sessionOperationJournalRecord) { record.Format = "wrong" },
		"kind":                 func(record *sessionOperationJournalRecord) { record.Kind = "wrong" },
		"mode":                 func(record *sessionOperationJournalRecord) { record.Mode = sessionOperationModeIsolated },
		"phase":                func(record *sessionOperationJournalRecord) { record.Phase = "wrong" },
		"logical":              func(record *sessionOperationJournalRecord) { record.LogicalSessionID = " bad" },
		"fork parents":         func(record *sessionOperationJournalRecord) { record.Kind = sessionOperationKindFork },
		"pre-effect ids":       func(record *sessionOperationJournalRecord) { record.NativeSessionID = "native" },
		"new identified":       func(record *sessionOperationJournalRecord) { record.Phase = sessionOperationPhaseNativeIdentified },
		"fork fenced":          func(record *sessionOperationJournalRecord) { record.Phase = sessionOperationPhaseLiveFenced },
		"published identities": func(record *sessionOperationJournalRecord) { record.Phase = sessionOperationPhaseStorePrepared },
		"target ready":         func(record *sessionOperationJournalRecord) { record.Phase = sessionOperationPhaseTargetReady },
		"origin":               func(record *sessionOperationJournalRecord) { record.Origin.PID = 0 },
		"timestamps":           func(record *sessionOperationJournalRecord) { record.UpdatedAtUnixMilli = 0 },
		"baseline":             func(record *sessionOperationJournalRecord) { record.BaselineNativeSessionIDs = []string{" bad"} },
		"orphan manifest hash": func(record *sessionOperationJournalRecord) { record.PreparedManifestSHA256 = strings.Repeat("0", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			record := base
			mutate(&record)
			if err := validateSessionOperationJournalRecord(record); err == nil {
				t.Fatalf("invalid record accepted: %+v", record)
			}
		})
	}

	identified := base
	identified.Phase = sessionOperationPhaseNativeIdentified
	identified.NativeSessionID = "native"
	identified.LiveSessionID = "live"
	identified.Prepared = &sessionOperationPreparedManifest{Format: sessionOperationJournalFormat, LogicalSessionID: identified.LogicalSessionID}
	if err := validateSessionOperationJournalRecord(identified); err == nil {
		t.Fatal("prepared manifest before publication accepted")
	}

	for _, phase := range []sessionOperationPhase{
		sessionOperationPhasePrepared,
		sessionOperationPhaseMutating,
		sessionOperationPhaseNativeIdentified,
		sessionOperationPhaseLiveFenced,
		sessionOperationPhaseTargetReady,
		sessionOperationPhaseStorePrepared,
		sessionOperationPhaseStoreCommitted,
		"unknown",
	} {
		_ = sessionOperationPhaseOrder(phase)
	}
	if _, err := normalizeSessionOperationIDs([]string{" bad"}); err == nil {
		t.Fatal("invalid baseline accepted")
	}
	if !equalSessionOperationEntries(nil, []SessionStoreEntry{json.RawMessage("null")}) {
		// Expected false; this branch exists to cover the length guard.
	} else {
		t.Fatal("different lengths compare equal")
	}
	if equalSessionOperationEntries([]SessionStoreEntry{json.RawMessage("1")}, []SessionStoreEntry{json.RawMessage("2")}) {
		t.Fatal("different entries compare equal")
	}
}

func TestPreparedSessionOperationManifestValidationMatrix(t *testing.T) {
	journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
	identifyTestNewSessionOperationJournal(t, journal)
	if err := journal.prepareReplacements(testSessionOperationReplacements()); err != nil {
		t.Fatal(err)
	}
	base := journal.record
	original, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}

	mutations := map[string]func(*sessionOperationJournalRecord){
		"nil":            func(record *sessionOperationJournalRecord) { record.Prepared = nil },
		"hash":           func(record *sessionOperationJournalRecord) { record.PreparedManifestSHA256 = strings.Repeat("0", 64) },
		"part":           func(record *sessionOperationJournalRecord) { record.Prepared.Parts[0].Sequence = 9 },
		"short digest":   func(record *sessionOperationJournalRecord) { record.Prepared.Parts[0].SHA256 = "00" },
		"invalid digest": func(record *sessionOperationJournalRecord) { record.Prepared.Parts[0].SHA256 = strings.Repeat("z", 64) },
		"duplicate": func(record *sessionOperationJournalRecord) {
			record.Prepared.Parts[1].Subpath = record.Prepared.Parts[0].Subpath
		},
		"no main": func(record *sessionOperationJournalRecord) { record.Prepared.Parts[1].Subpath = "not-main" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			var record sessionOperationJournalRecord
			if err := json.Unmarshal(original, &record); err != nil {
				t.Fatal(err)
			}
			mutate(&record)
			if name != "nil" && name != "hash" {
				copyJournal := &sessionOperationJournal{record: record}
				refreshPreparedManifestHash(t, copyJournal)
				record = copyJournal.record
			}
			if err := validatePreparedSessionOperationManifest(record); err == nil {
				t.Fatalf("invalid manifest accepted: %+v", record.Prepared)
			}
		})
	}
}

func TestSessionOperationRemovalAndDirectoryFailures(t *testing.T) {
	var nilJournal *sessionOperationJournal
	if err := nilJournal.removeCommitted(); err != nil {
		t.Fatal(err)
	}
	if err := nilJournal.discardPrepared(); err != nil {
		t.Fatal(err)
	}
	if err := nilJournal.removeRecovered(); err != nil {
		t.Fatal(err)
	}

	for name, remove := range map[string]func(*sessionOperationJournal) error{
		"committed": func(journal *sessionOperationJournal) error {
			journal.record.Phase = sessionOperationPhaseStoreCommitted

			return journal.removeCommitted()
		},
		"prepared":  func(journal *sessionOperationJournal) error { return journal.discardPrepared() },
		"recovered": func(journal *sessionOperationJournal) error { return journal.removeRecovered() },
	} {
		t.Run(name+" remove", func(t *testing.T) {
			journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
			previous := sessionOperationRemoveAll
			sessionOperationRemoveAll = func(string) error { return errors.New("remove") }
			t.Cleanup(func() { sessionOperationRemoveAll = previous })
			if err := remove(journal); err == nil {
				t.Fatal("remove failure ignored")
			}
		})

		t.Run(name+" sync", func(t *testing.T) {
			journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
			previous := sessionOperationRemoveAll
			sessionOperationRemoveAll = func(path string) error {
				if err := os.RemoveAll(path); err != nil {
					return err
				}

				return os.RemoveAll(filepath.Dir(path))
			}
			t.Cleanup(func() { sessionOperationRemoveAll = previous })
			if err := remove(journal); err == nil {
				t.Fatal("post-remove sync failure ignored")
			}
		})
	}
}

func TestSessionOperationAtomicAndDirectoryFaults(t *testing.T) {
	t.Run("create temp", func(t *testing.T) {
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return nil, errors.New("create") }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := atomicWriteSessionOperationFile(filepath.Join(t.TempDir(), "value"), []byte("x"), 0o600); err == nil {
			t.Fatal("create failure ignored")
		}
	})

	t.Run("chmod and deferred close", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "closed")
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return file, nil }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := atomicWriteSessionOperationFile(filepath.Join(t.TempDir(), "value"), []byte("x"), 0o600); err == nil {
			t.Fatal("chmod failure ignored")
		}
	})

	t.Run("write", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "readonly")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return file, nil }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := atomicWriteSessionOperationFile(filepath.Join(t.TempDir(), "value"), []byte("x"), 0o600); err == nil {
			t.Fatal("write failure ignored")
		}
	})

	t.Run("rename and cleanup remove", func(t *testing.T) {
		previousRename, previousRemove := sessionOperationRename, sessionOperationRemove
		sessionOperationRename = func(string, string) error { return errors.New("rename") }
		sessionOperationRemove = func(string) error { return errors.New("remove") }
		t.Cleanup(func() {
			sessionOperationRename = previousRename
			sessionOperationRemove = previousRemove
		})
		if err := atomicWriteSessionOperationFile(filepath.Join(t.TempDir(), "value"), []byte("x"), 0o600); err == nil {
			t.Fatal("rename/remove failures ignored")
		}
	})

	for name, setup := range map[string]func(string){
		"mkdir": func(string) {
			sessionOperationMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
		},
		"lstat": func(string) {
			sessionOperationLstat = func(string) (os.FileInfo, error) { return nil, errors.New("lstat") }
		},
		"not directory": func(path string) {
			sessionOperationMkdirAll = func(string, os.FileMode) error { return nil }
			if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"chmod": func(string) { sessionOperationChmod = func(string, os.FileMode) error { return errors.New("chmod") } },
		"sync parent": func(path string) {
			sessionOperationChmod = func(string, os.FileMode) error { return os.RemoveAll(filepath.Dir(path)) }
		},
	} {
		t.Run("ensure "+name, func(t *testing.T) {
			previousMkdirAll, previousLstat, previousChmod := sessionOperationMkdirAll, sessionOperationLstat, sessionOperationChmod
			t.Cleanup(func() {
				sessionOperationMkdirAll = previousMkdirAll
				sessionOperationLstat = previousLstat
				sessionOperationChmod = previousChmod
			})
			path := filepath.Join(t.TempDir(), "operations")
			setup(path)
			if err := ensureSessionOperationDirectory(path); err == nil {
				t.Fatalf("%s failure ignored", name)
			}
		})
	}

	t.Run("bounded open", func(t *testing.T) {
		if _, err := readBoundedSessionOperationFile(filepath.Join(t.TempDir(), "missing"), 1); err == nil {
			t.Fatal("missing bounded file accepted")
		}
	})
	t.Run("bounded read", func(t *testing.T) {
		if _, err := readBoundedSessionOperationFile(t.TempDir(), 1); err == nil {
			t.Fatal("directory read accepted")
		}
	})
	t.Run("bounded size", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "large")
		if err := os.WriteFile(path, []byte("xx"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readBoundedSessionOperationFile(path, 1); err == nil {
			t.Fatal("oversize file accepted")
		}
	})
}

func TestSessionOperationBaselineRejectsInvalidBaseline(t *testing.T) {
	if _, _, err := sessionOperationBaselineDelta([]string{" bad"}, nil); err == nil {
		t.Fatal("invalid baseline accepted")
	}
}

func TestSessionOperationRecoveryRemainingFailures(t *testing.T) {
	if err := newTestAgent().recoverPendingSharedSessionOperations(t.Context(), "relative", newFakeHermesClient()); err == nil {
		t.Fatal("journal discovery failure ignored")
	}

	t.Run("pending recovery requires inventory", func(t *testing.T) {
		home := t.TempDir()
		_ = newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		client := sessionOperationServerOnly{Server: newFakeHermesClient()}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("pending recovery without inventory accepted")
		}
	})

	t.Run("ambiguous store", func(t *testing.T) {
		home := t.TempDir()
		store := NewInMemorySessionStore()
		agent := newTestAgent(WithSessionStore(store))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native", "live"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		replacements := []SessionStoreReplacement{{
			Key: SessionKey{SessionID: "logical", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{json.RawMessage(`{"main":true}`)},
		}, {
			Key: SessionKey{SessionID: "logical", Subpath: "idmap"}, Entries: []SessionStoreEntry{json.RawMessage(`{"id":true}`)},
		}}
		if err := journal.prepareReplacements(replacements); err != nil {
			t.Fatal(err)
		}
		if err := store.Append(t.Context(), replacements[0].Key, []SessionStoreEntry{json.RawMessage(`{"different":true}`)}); err != nil {
			t.Fatal(err)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); !errors.Is(err, ErrSessionOperationAmbiguous) {
			t.Fatalf("ambiguous store error=%v", err)
		}
	})

	setupNative := func(t *testing.T) (string, *sessionOperationJournal) {
		t.Helper()
		home := t.TempDir()
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native", "live"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}

		return home, journal
	}

	t.Run("child owner", func(t *testing.T) {
		home, _ := setupNative(t)
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(home, "native")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("held child owner did not fence recovery")
		}
	})

	t.Run("delete", func(t *testing.T) {
		home, _ := setupNative(t)
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		client.deleteErr = errors.New("delete")
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("delete failure ignored")
		}
	})

	t.Run("verify inventory", func(t *testing.T) {
		home, _ := setupNative(t)
		client := &sessionOperationRecoveryServer{fakeHermesClient: newFakeHermesClient(), secondListErr: errors.New("verify")}
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("verification inventory failure ignored")
		}
	})

	t.Run("native remains", func(t *testing.T) {
		home, _ := setupNative(t)
		client := &sessionOperationRecoveryServer{fakeHermesClient: newFakeHermesClient(), retainOnDelete: true}
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); !errors.Is(err, ErrSessionOperationAmbiguous) {
			t.Fatalf("remaining native error=%v", err)
		}
	})
}

var _ io.Reader = sessionOperationErrorReader{}
