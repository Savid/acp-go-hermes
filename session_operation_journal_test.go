package hermesacp

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

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

type faultSessionOperationFile struct {
	name     string
	syncErr  error
	closeErr error
}

func (f *faultSessionOperationFile) Name() string                 { return f.name }
func (*faultSessionOperationFile) Chmod(os.FileMode) error        { return nil }
func (*faultSessionOperationFile) Write(data []byte) (int, error) { return len(data), nil }
func (f *faultSessionOperationFile) Sync() error                  { return f.syncErr }
func (f *faultSessionOperationFile) Close() error                 { return f.closeErr }

func TestSessionOperationIDEntropyFailure(t *testing.T) {
	previous := cryptorand.Reader
	cryptorand.Reader = sessionOperationErrorReader{err: errors.New("entropy unavailable")}
	t.Cleanup(func() { cryptorand.Reader = previous })

	if _, err := newSessionOperationID(); err == nil {
		t.Fatal("entropy failure was ignored")
	}
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

func TestSessionOperationAtomicAndDirectoryFaults(t *testing.T) {
	t.Run("create temp", func(t *testing.T) {
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return nil, errors.New("create") }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := atomicWriteSessionOperationFile(filepath.Join(durableTempDir(t), "value"), []byte("x"), 0o600); err == nil {
			t.Fatal("create failure ignored")
		}
	})

	t.Run("chmod and deferred close", func(t *testing.T) {
		file, err := os.CreateTemp(durableTempDir(t), "closed")
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return file, nil }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := atomicWriteSessionOperationFile(filepath.Join(durableTempDir(t), "value"), []byte("x"), 0o600); err == nil {
			t.Fatal("chmod failure ignored")
		}
	})

	t.Run("write", func(t *testing.T) {
		path := filepath.Join(durableTempDir(t), "readonly")
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
		if err := atomicWriteSessionOperationFile(filepath.Join(durableTempDir(t), "value"), []byte("x"), 0o600); err == nil {
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
		if err := atomicWriteSessionOperationFile(filepath.Join(durableTempDir(t), "value"), []byte("x"), 0o600); err == nil {
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
	} {
		t.Run("ensure "+name, func(t *testing.T) {
			previousMkdirAll, previousLstat, previousChmod := sessionOperationMkdirAll, sessionOperationLstat, sessionOperationChmod
			t.Cleanup(func() {
				sessionOperationMkdirAll = previousMkdirAll
				sessionOperationLstat = previousLstat
				sessionOperationChmod = previousChmod
			})
			path := filepath.Join(durableTempDir(t), "operations")
			setup(path)
			if err := ensureSessionOperationDirectory(path); err == nil {
				t.Fatalf("%s failure ignored", name)
			}
		})
	}

	t.Run("bounded open", func(t *testing.T) {
		if _, err := readBoundedSessionOperationFile(filepath.Join(durableTempDir(t), "missing"), 1); err == nil {
			t.Fatal("missing bounded file accepted")
		}
	})
	t.Run("bounded read", func(t *testing.T) {
		if _, err := readBoundedSessionOperationFile(durableTempDir(t), 1); err == nil {
			t.Fatal("directory read accepted")
		}
	})
	t.Run("bounded size", func(t *testing.T) {
		path := filepath.Join(durableTempDir(t), "large")
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

func TestSessionOperationFileFaults(t *testing.T) {
	for name, configure := range map[string]func(*faultSessionOperationFile){
		"sync":  func(file *faultSessionOperationFile) { file.syncErr = errors.New("sync") },
		"close": func(file *faultSessionOperationFile) { file.closeErr = errors.New("close") },
	} {
		t.Run(name, func(t *testing.T) {
			file := &faultSessionOperationFile{name: filepath.Join(durableTempDir(t), "temporary")}
			configure(file)
			previous := sessionOperationCreateTemp
			sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return file, nil }
			t.Cleanup(func() { sessionOperationCreateTemp = previous })

			if err := atomicWriteSessionOperationFile(filepath.Join(durableTempDir(t), "value"), []byte("x"), 0o600); err == nil {
				t.Fatalf("%s failure ignored", name)
			}
		})
	}
}
