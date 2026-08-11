package hermesacp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
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

	ledger, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), ProviderAuthHome: t.TempDir()})
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

func TestInventoryKeepsOfficialHermesLineageInconclusiveWithoutReadingNativeStatus(t *testing.T) {
	agent, client := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)
	unsupported := false
	client.providerAuthHomeSupported = &unsupported
	client.authProvidersErr = errors.New("native status must not be consulted")

	result, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}

	inventory := mustType[authInventoryResult](t, result)
	if len(inventory.Entries) != 1 || inventory.Entries[0].ProofSource != authProofNotConfirmed {
		t.Fatalf("official Hermes inventory = %#v", inventory)
	}
}

func TestAuthLedgerRootValidationFailsClosed(t *testing.T) {
	restoreLedgerHooks(t)

	if _, err := newAuthLedger(Options{ProviderAuthRoot: "relative", ProviderAuthHome: t.TempDir()}); err == nil {
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

			if _, err := newAuthLedger(Options{ProviderAuthRoot: target, ProviderAuthHome: t.TempDir()}); err == nil {
				t.Fatal("unusable root accepted")
			}
		})
	}
}

func TestAuthLedgerRestrictsTheConfiguredRoot(t *testing.T) {
	restoreLedgerHooks(t)

	root := filepath.Join(t.TempDir(), "provider-auth")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}

	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("relax root: %v", err)
	}

	if _, err := newAuthLedger(Options{ProviderAuthRoot: root, ProviderAuthHome: t.TempDir()}); err != nil {
		t.Fatalf("newAuthLedger: %v", err)
	}

	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}

	if info.Mode().Perm() != authLedgerDirMode {
		t.Fatalf("configured root mode = %v, want %v", info.Mode().Perm(), os.FileMode(authLedgerDirMode))
	}
}

func TestAuthLedgerWritesAtomicallyAndValuesFree(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := newTestLedger(t)

	record := authLedgerRecord{
		ProviderID:         testProviderID,
		ConnectionID:       testConnectionID,
		Revision:           2,
		BindingGeneration:  3,
		FlowID:             "flow",
		AuthorizeRequestID: "request",
		State:              authLedgerConfirmed,
		CreatedAt:          1,
		UpdatedAt:          2,
	}

	if err := ledger.write(record); err != nil {
		t.Fatalf("write: %v", err)
	}

	contents, err := os.ReadFile(ledger.path(testProviderID))
	if err != nil {
		t.Fatalf("read entry: %v", err)
	}

	var decoded map[string]any
	if errLocal := json.Unmarshal(contents, &decoded); errLocal != nil {
		t.Fatalf("decode entry: %v", err)
	}

	allowed := map[string]struct{}{
		"providerId": {}, "connectionId": {}, "revision": {}, "bindingGeneration": {},
		"flowId": {}, "authorizeRequestId": {}, "state": {}, "createdAt": {}, "updatedAt": {},
	}

	for key := range decoded {
		if _, ok := allowed[key]; !ok {
			t.Fatalf("ledger entry carries %q", key)
		}
	}

	info, err := os.Stat(ledger.path(testProviderID))
	if err != nil {
		t.Fatalf("stat entry: %v", err)
	}

	if info.Mode().Perm() != authLedgerFileMode {
		t.Fatalf("entry mode = %v", info.Mode().Perm())
	}

	read, ok, err := ledger.read(testProviderID)
	if err != nil || !ok || read != record {
		t.Fatalf("read = %#v, %v, %v", read, ok, err)
	}

	if _, ok, errLocal := ledger.read("absent"); errLocal != nil || ok {
		t.Fatalf("absent entry = %v, %v", ok, errLocal)
	}

	records, err := ledger.list()
	if err != nil || len(records) != 1 {
		t.Fatalf("list = %#v, %v", records, err)
	}
}

func TestAuthLedgerFailurePaths(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := newTestLedger(t)

	if err := os.WriteFile(ledger.path("broken"), []byte("{"), 0o600); err != nil {
		t.Fatalf("write broken entry: %v", err)
	}

	if _, _, err := ledger.read("broken"); err == nil {
		t.Fatal("malformed entry decoded")
	}

	if _, err := ledger.list(); err == nil {
		t.Fatal("malformed entry listed")
	}

	if err := os.Remove(ledger.path("broken")); err != nil {
		t.Fatalf("remove broken entry: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(ledger.dir, "nested"), 0o700); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	if err := os.WriteFile(filepath.Join(ledger.dir, "ignored.txt"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write ignored: %v", err)
	}

	if records, err := ledger.list(); err != nil || len(records) != 0 {
		t.Fatalf("list ignored non-entries = %#v, %v", records, err)
	}

	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }
	if _, _, err := ledger.read(testProviderID); err == nil {
		t.Fatal("read failure ignored")
	}

	restoreLedgerHooks(t)

	ledgerReadDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir") }
	if _, err := ledger.list(); err == nil {
		t.Fatal("readdir failure ignored")
	}

	restoreLedgerHooks(t)

	if err := ledger.write(authLedgerRecord{ProviderID: testProviderID}); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	ledgerReadDir = func(dir string) ([]os.DirEntry, error) { return os.ReadDir(dir) }
	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }

	if _, err := ledger.list(); err == nil {
		t.Fatal("entry read failure ignored during list")
	}
}

func TestAuthLedgerWriteFailurePaths(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := newTestLedger(t)
	record := authLedgerRecord{ProviderID: testProviderID}

	ledgerMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal") }
	if err := ledger.write(record); err == nil {
		t.Fatal("marshal failure ignored")
	}

	restoreLedgerHooks(t)

	ledgerCreateTemp = func(string, string) (ledgerFile, error) { return nil, errors.New("temp") }
	if err := ledger.write(record); err == nil {
		t.Fatal("temp creation failure ignored")
	}

	restoreLedgerHooks(t)

	ledgerRename = func(string, string) error { return errors.New("rename") }
	if err := ledger.write(record); err == nil {
		t.Fatal("rename failure ignored")
	}

	restoreLedgerHooks(t)

	ledgerOpen = func(string) (*os.File, error) { return nil, errors.New("open") }
	if err := ledger.write(record); err == nil {
		t.Fatal("directory sync failure ignored")
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

func TestInventoryKeepsExactXAILineageUnconfirmedWhenNativeReportsLoggedOut(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)
	ledger := agent.providerAuth.ledger

	if err := ledger.write(authLedgerRecord{
		ProviderID: testProviderID, ConnectionID: testConnectionID,
		Revision: 1, BindingGeneration: 1, State: authLedgerConfirmed,
	}); err != nil {
		t.Fatalf("write confirmed: %v", err)
	}

	if err := ledger.write(authLedgerRecord{
		ProviderID: "pending", ConnectionID: "c2", Revision: 1, BindingGeneration: 1, State: authLedgerIntent,
	}); err != nil {
		t.Fatalf("write intent: %v", err)
	}

	client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, LoggedIn: false}}

	result, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}

	entries := mustType[authInventoryResult](t, result).Entries
	if len(entries) != 1 {
		t.Fatalf("entries = %#v", entries)
	}

	if entries[0].ProviderID != testProviderID ||
		entries[0].ConnectionID != testConnectionID ||
		entries[0].Revision != 1 ||
		entries[0].BindingGeneration != 1 ||
		entries[0].ProofSource != authProofNotConfirmed {
		t.Fatalf("logged-out entry = %#v", entries[0])
	}

	client.authProviders[0].LoggedIn = true

	result, err = callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}

	entries = mustType[authInventoryResult](t, result).Entries
	if len(entries) != 1 || entries[0].ProviderID != testProviderID ||
		entries[0].ProofSource != authProofConfirmedPresent {
		t.Fatalf("logged-in entry = %#v", entries)
	}
}

func TestInventoryFailurePaths(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)

	if _, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": "unknown"}); err == nil {
		t.Fatal("unknown session accepted")
	}

	if _, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{}); err == nil {
		t.Fatal("missing session id accepted")
	}

	if err := agent.providerAuth.ledger.write(authLedgerRecord{
		ProviderID: testProviderID, ConnectionID: testConnectionID, State: authLedgerConfirmed,
	}); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	client.authProvidersErr = errors.New("catalog")
	_, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseTransport)

	client.authProvidersErr = nil

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseTransport)

	session.mu.Lock()
	session.client = client
	session.mu.Unlock()

	restoreLedgerHooks(t)

	ledgerReadDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir") }

	_, err = callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseProcess)
}

func TestAuthLedgerPathIsDeterministicAndScopedToTheRoot(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := newTestLedger(t)

	first := ledger.path(testProviderID)
	if first != ledger.path(testProviderID) {
		t.Fatal("ledger path is not deterministic")
	}

	if filepath.Base(filepath.Dir(first)) != authLedgerLeafDir ||
		filepath.Base(filepath.Dir(filepath.Dir(first))) == authLedgerVendorDir {
		t.Fatalf("ledger path %q is outside the vendor leaf", first)
	}

	if !authLedgerRootConfigured(Options{ProviderAuthRoot: "/root", ProviderAuthHome: "/home"}) ||
		authLedgerRootConfigured(Options{}) {
		t.Fatal("root configuration reported incorrectly")
	}
}

func TestAuthLedgerIsScopedByCanonicalProviderAuthHome(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	homeA := t.TempDir()
	homeB := t.TempDir()

	ledgerA, err := newAuthLedger(Options{ProviderAuthRoot: root, ProviderAuthHome: homeA})
	if err != nil {
		t.Fatalf("new ledger A: %v", err)
	}

	ledgerB, err := newAuthLedger(Options{ProviderAuthRoot: root, ProviderAuthHome: homeB})
	if err != nil {
		t.Fatalf("new ledger B: %v", err)
	}

	if ledgerA.dir == ledgerB.dir {
		t.Fatalf("distinct native homes share ledger %q", ledgerA.dir)
	}

	record := authLedgerRecord{
		ProviderID:        testProviderID,
		ConnectionID:      testConnectionID,
		Revision:          1,
		BindingGeneration: 1,
		State:             authLedgerConfirmed,
	}
	if err := ledgerA.write(record); err != nil {
		t.Fatalf("write ledger A: %v", err)
	}

	if _, ok, err := ledgerB.read(testProviderID); err != nil || ok {
		t.Fatalf("ledger B observed ledger A lineage: %v, %v", ok, err)
	}

	if authLedgerHomeKey(filepath.Join(homeA, ".")) != authLedgerHomeKey(homeA) {
		t.Fatal("equivalent clean paths produced different ledger scopes")
	}
}

func TestInventorySurvivesAgentRestartWithoutReadingCredentialFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	home := t.TempDir()

	first := newTestAgent(WithProviderAuthRoot(root), WithProviderAuthHome(home))
	if first.providerAuth == nil {
		t.Fatal("first provider auth surface unavailable")
	}
	seedConfirmedLineage(t, first, testProviderID)

	second := newTestAgent(WithProviderAuthRoot(root), WithProviderAuthHome(home))
	if second.providerAuth == nil {
		t.Fatal("second provider auth surface unavailable")
	}

	client := newFakeHermesClient()
	client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, LoggedIn: true}}
	session := newSession(
		second,
		testSessionID,
		"/cwd",
		nil,
		nil,
		nativehermes.Session{ID: "native"},
		client,
		sessionMeta{},
		idmapRecord{},
	)
	if err := second.storeStartedSession(session); err != nil {
		t.Fatalf("register restarted session: %v", err)
	}

	result, err := callLeg(t, second, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("restart inventory: %v", err)
	}

	entries := mustType[authInventoryResult](t, result).Entries
	if len(entries) != 1 || entries[0].ProofSource != authProofConfirmedPresent {
		t.Fatalf("restart inventory = %#v", entries)
	}
}

func TestInventoryRejectsAnUnknownParamField(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	_, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID), "extra": 1})
	requireInvalidField(t, err, "extra")
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

	if _, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), ProviderAuthHome: t.TempDir()}); err == nil {
		t.Fatal("a root that is not a directory was accepted")
	}
}

func TestLedgerWriteReportsAFailedFileWrite(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := newTestLedger(t)

	ledgerCreateTemp = func(string, string) (ledgerFile, error) {
		return &stubLedgerFile{name: filepath.Join(ledger.dir, "temp"), writeErr: errors.New("write")}, nil
	}

	if err := ledger.write(authLedgerRecord{ProviderID: testProviderID}); err == nil {
		t.Fatal("a failed entry write was reported clean")
	}
}
