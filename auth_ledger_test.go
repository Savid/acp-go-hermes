package hermesacp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// restoreLedgerHooks resets every ledger file hook to its package default now
// and again at test end, so a test may stub one, assert, and reset mid-test.
func restoreLedgerHooks(t *testing.T) {
	t.Helper()

	reset := func() {
		ledgerMkdirAll = os.MkdirAll
		ledgerChmod = os.Chmod
		ledgerStat = os.Stat
		ledgerRename = os.Rename
		ledgerOpen = os.Open
		ledgerReadFile = os.ReadFile
		ledgerReadDir = os.ReadDir
		ledgerRemove = os.Remove
		ledgerMarshal = json.Marshal
		ledgerEvalPath = filepath.EvalSymlinks
		ledgerCreateTemp = func(dir string, pattern string) (ledgerFile, error) {
			return os.CreateTemp(dir, pattern)
		}
	}

	reset()
	t.Cleanup(reset)
}

func newTestLedger(t *testing.T) *authLedger {
	t.Helper()

	ledger, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), SharedHermesHome: t.TempDir()})
	if err != nil {
		t.Fatalf("newAuthLedger: %v", err)
	}

	return ledger
}

func seedConfirmedLineage(t *testing.T, agent *Agent, providerID string) authLedgerRecord {
	t.Helper()

	record := authLedgerRecord{
		ProviderID:        providerID,
		ConnectionID:      testConnectionID,
		Revision:          1,
		BindingGeneration: 1,
		State:             authLedgerConfirmed,
		CreatedAt:         1,
		UpdatedAt:         1,
	}
	if err := agent.providerAuth.ledger.write(record); err != nil {
		t.Fatalf("seed confirmed lineage: %v", err)
	}

	return record
}

func TestAuthLedgerRootValidationFailsClosed(t *testing.T) {
	restoreLedgerHooks(t)

	if _, err := newAuthLedger(Options{ProviderAuthRoot: "relative", SharedHermesHome: t.TempDir()}); err == nil {
		t.Fatal("relative root accepted")
	}

	root := t.TempDir()

	failures := []struct {
		name  string
		apply func()
	}{
		{"root mkdir", func() {
			ledgerMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
		}},
		{"root chmod", func() {
			ledgerChmod = func(string, os.FileMode) error { return errors.New("chmod") }
		}},
		{"leaf mkdir", func() {
			ledgerMkdirAll = func(path string, mode os.FileMode) error {
				if filepath.Base(path) == authLedgerLeafDir {
					return errors.New("mkdir")
				}

				return os.MkdirAll(path, mode)
			}
		}},
		{"leaf chmod", func() {
			ledgerChmod = func(path string, mode os.FileMode) error {
				if filepath.Base(path) == authLedgerLeafDir {
					return errors.New("chmod")
				}

				return os.Chmod(path, mode)
			}
		}},
		{"stat", func() {
			ledgerStat = func(string) (os.FileInfo, error) { return nil, errors.New("stat") }
		}},
		{"not a directory", func() {
			ledgerStat = func(name string) (os.FileInfo, error) { return os.Stat(name) }
			ledgerMkdirAll = func(string, os.FileMode) error { return nil }
			ledgerChmod = func(string, os.FileMode) error { return nil }
		}},
		{"not writable", func() {
			ledgerCreateTemp = func(string, string) (ledgerFile, error) { return nil, errors.New("temp") }
		}},
	}

	for _, tt := range failures {
		t.Run(tt.name, func(t *testing.T) {
			restoreLedgerHooks(t)
			tt.apply()

			target := root
			if tt.name == "not a directory" {
				file := filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
					t.Fatalf("write file: %v", err)
				}

				target = file
			}

			if _, err := newAuthLedger(Options{ProviderAuthRoot: target, SharedHermesHome: t.TempDir()}); err == nil {
				t.Fatal("unusable root accepted")
			}
		})
	}
}

// stubLedgerFile fails one step of the atomic write so every guard in
// writeLedgerFile is exercised.
type stubLedgerFile struct {
	name     string
	writeErr error
	chmodErr error
	syncErr  error
	closed   int
}

func (f *stubLedgerFile) Name() string { return f.name }

func (f *stubLedgerFile) Write([]byte) (int, error) { return 0, f.writeErr }

func (f *stubLedgerFile) Chmod(os.FileMode) error { return f.chmodErr }

func (f *stubLedgerFile) Sync() error { return f.syncErr }

func (f *stubLedgerFile) Close() error {
	f.closed++

	return nil
}

func TestWriteLedgerFileClosesOnEveryFailure(t *testing.T) {
	t.Parallel()

	cases := []*stubLedgerFile{
		{writeErr: errors.New("write")},
		{chmodErr: errors.New("chmod")},
		{syncErr: errors.New("sync")},
	}

	for _, file := range cases {
		if err := writeLedgerFile(file, []byte("{}")); err == nil {
			t.Fatal("failure ignored")
		}

		if file.closed != 1 {
			t.Fatalf("closed %d times", file.closed)
		}
	}

	clean := &stubLedgerFile{}
	if err := writeLedgerFile(clean, []byte("{}")); err != nil {
		t.Fatalf("clean write: %v", err)
	}
}

func TestAuthProofSourceIsTheTotalFunctionOfLedgerAndNativeStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state    string
		loggedIn bool
		want     string
	}{
		{authLedgerConfirmed, true, authProofConfirmedPresent},
		{authLedgerConfirmed, false, authProofNotConfirmed},
		{authLedgerIntent, true, authProofNotConfirmed},
		{authLedgerIntent, false, authProofNotConfirmed},
		{"", true, authProofNotConfirmed},
	}

	for _, tt := range cases {
		if got := authProofSource(tt.state, tt.loggedIn); got != tt.want {
			t.Fatalf("authProofSource(%q, %v) = %q, want %q", tt.state, tt.loggedIn, got, tt.want)
		}
	}
}

func TestNewAuthLedgerRejectsARootThatIsNotADirectory(t *testing.T) {
	restoreLedgerHooks(t)

	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	ledgerMkdirAll = func(string, os.FileMode) error { return nil }
	ledgerChmod = func(string, os.FileMode) error { return nil }
	ledgerStat = func(string) (os.FileInfo, error) { return os.Stat(file) }

	if _, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), SharedHermesHome: t.TempDir()}); err == nil {
		t.Fatal("a root that is not a directory was accepted")
	}
}
