package hermesacp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
		ledgerCreateTemp = func(dir string, pattern string) (ledgerFile, error) {
			return os.CreateTemp(dir, pattern)
		}
	}

	reset()
	t.Cleanup(reset)
}

func newTestLedger(t *testing.T) *authLedger {
	t.Helper()

	ledger, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("newAuthLedger: %v", err)
	}

	return ledger
}

func TestAuthLedgerRootValidationFailsClosed(t *testing.T) {
	restoreLedgerHooks(t)

	if _, err := newAuthLedger(Options{ProviderAuthRoot: "relative"}); err == nil {
		t.Fatal("relative root accepted")
	}

	root := t.TempDir()

	failures := []struct {
		name  string
		apply func()
	}{
		{"mkdir", func() {
			ledgerMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
		}},
		{"chmod", func() {
			ledgerChmod = func(string, os.FileMode) error { return errors.New("chmod") }
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

			if _, err := newAuthLedger(Options{ProviderAuthRoot: target}); err == nil {
				t.Fatal("unusable root accepted")
			}
		})
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

func TestAuthProofSourceIsTheTotalFunctionOfLedgerAndProbe(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state   string
		present bool
		want    string
	}{
		{authLedgerConfirmed, true, authProofConfirmedPresent},
		{authLedgerConfirmed, false, authProofConfirmedAbsent},
		{authLedgerIntent, true, authProofNotConfirmed},
		{authLedgerIntent, false, authProofNotConfirmed},
		{"", true, authProofNotConfirmed},
	}

	for _, tt := range cases {
		if got := authProofSource(tt.state, tt.present); got != tt.want {
			t.Fatalf("authProofSource(%q, %v) = %q, want %q", tt.state, tt.present, got, tt.want)
		}
	}
}

func TestInventoryReportsResidenceFromTheLedgerAndAProbe(t *testing.T) {
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

	if err := ledger.write(authLedgerRecord{
		ProviderID: "gone", ConnectionID: "c3", Revision: 1, BindingGeneration: 1, State: authLedgerRemoved,
	}); err != nil {
		t.Fatalf("write removed: %v", err)
	}

	result, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}

	entries := mustType[authInventoryResult](t, result).Entries
	if len(entries) != 2 {
		t.Fatalf("entries = %#v", entries)
	}

	for _, entry := range entries {
		if entry.ProofSource == authProofConfirmedPresent {
			t.Fatalf("empty slot reported present: %#v", entry)
		}
	}

	material := nativehermes.AuthMaterial{AuthType: nativehermes.AuthTypeOAuth, AccessToken: "token"}
	if errLocal := nativehermes.AuthWriteSlot(client.xdg.Root, testProviderID, nativehermes.AuthSlotLabel(testConnectionID), material); errLocal != nil {
		t.Fatalf("seed reserved slot: %v", err)
	}

	result, err = callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}

	entries = mustType[authInventoryResult](t, result).Entries
	if entries[0].ProviderID != "pending" || entries[0].ProofSource != authProofNotConfirmed {
		t.Fatalf("intent entry = %#v", entries[0])
	}

	if entries[1].ProviderID != testProviderID || entries[1].ProofSource != authProofConfirmedPresent {
		t.Fatalf("confirmed entry = %#v", entries[1])
	}
}

func TestInventoryNeverReportsAnAmbientEntry(t *testing.T) {
	restoreLedgerHooks(t)

	agent, client := newAuthAgent(t)

	store := filepath.Join(client.xdg.Root, "auth.json")
	ambient := `{"credential_pool":{"copilot":[{"auth_type":"oauth","` + testFieldAccessToken + `":"gh-derived","source":"gh"}]}}`

	if err := os.WriteFile(store, []byte(ambient), 0o600); err != nil {
		t.Fatalf("seed ambient store: %v", err)
	}

	result, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if entries := mustType[authInventoryResult](t, result).Entries; len(entries) != 0 {
		t.Fatalf("ambient credential surfaced: %#v", entries)
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

	if err := os.WriteFile(filepath.Join(client.xdg.Root, "auth.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("corrupt store: %v", err)
	}

	_, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseHarvestFailed)

	if errLocal := os.Remove(filepath.Join(client.xdg.Root, "auth.json")); errLocal != nil {
		t.Fatalf("remove store: %v", err)
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()

	_, err = callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseTransport)

	restoreLedgerHooks(t)

	ledgerReadDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir") }

	_, err = callLeg(t, agent, AuthInventoryMethod, map[string]any{"sessionId": string(testSessionID)})
	requireAuthCause(t, err, authCauseHarvestFailed)
}

func TestAuthLedgerPathIsDeterministicAndScopedToTheRoot(t *testing.T) {
	restoreLedgerHooks(t)

	ledger := newTestLedger(t)

	first := ledger.path(testProviderID)
	if first != ledger.path(testProviderID) {
		t.Fatal("ledger path is not deterministic")
	}

	if !strings.HasSuffix(filepath.Dir(first), filepath.Join(authLedgerVendorDir, authLedgerLeafDir)) {
		t.Fatalf("ledger path %q is outside the vendor leaf", first)
	}

	if !authLedgerRootConfigured(Options{ProviderAuthRoot: "/root"}) || authLedgerRootConfigured(Options{}) {
		t.Fatal("root configuration reported incorrectly")
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

	if _, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir()}); err == nil {
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
