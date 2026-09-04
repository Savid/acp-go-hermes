//go:build !windows

package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestInventoryKeepsOfficialHermesLineageInconclusiveWithoutReadingNativeStatus(t *testing.T) {
	agent, client := newAuthAgent(t)
	seedConfirmedLineage(t, agent, testProviderID)
	unsupported := false
	client.providerAuthSupported = &unsupported
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

func TestAuthLedgerRestrictsTheConfiguredRoot(t *testing.T) {
	restoreLedgerHooks(t)

	root := filepath.Join(t.TempDir(), "provider-auth")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}

	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("relax root: %v", err)
	}

	if _, err := newAuthLedger(Options{ProviderAuthRoot: root, SharedHermesHome: t.TempDir()}); err != nil {
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

	if !authLedgerRootConfigured(Options{ProviderAuthRoot: absTestPath("root"), SharedHermesHome: absTestPath("home")}) ||
		authLedgerRootConfigured(Options{}) {
		t.Fatal("root configuration reported incorrectly")
	}
}

func TestAuthLedgerIsScopedByCanonicalSharedHermesHome(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	homeA := t.TempDir()
	homeB := t.TempDir()

	ledgerA, err := newAuthLedger(Options{ProviderAuthRoot: root, SharedHermesHome: homeA})
	if err != nil {
		t.Fatalf("new ledger A: %v", err)
	}

	ledgerB, err := newAuthLedger(Options{ProviderAuthRoot: root, SharedHermesHome: homeB})
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

	first := newTestAgent(WithProviderAuthRoot(root), WithSharedHermesHome(home))
	if first.providerAuth == nil {
		t.Fatal("first provider auth surface unavailable")
	}
	seedConfirmedLineage(t, first, testProviderID)

	second := newTestAgent(WithProviderAuthRoot(root), WithSharedHermesHome(home))
	if second.providerAuth == nil {
		t.Fatal("second provider auth surface unavailable")
	}

	client := newFakeHermesClient()
	client.authProviders = []nativehermes.AuthProvider{{ID: testProviderID, LoggedIn: true}}
	session := newSession(
		second,
		testSessionID,
		absTestPath("cwd"),
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

func TestAuthInventoryDuplicateLockAndSecondReadFailures(t *testing.T) {
	t.Run("duplicate", func(t *testing.T) {
		agent, _ := newAuthAgent(t)
		record := seedConfirmedLineage(t, agent, testProviderID)
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(agent.providerAuth.ledger.dir, "duplicate.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{authFieldSessionID: string(testSessionID)}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("provider lock timeout", func(t *testing.T) {
		agent, _ := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		release, acquired := agent.providerAuth.lockProvider(t.Context(), testProviderID)
		if !acquired {
			t.Fatal("hold provider lock")
		}
		defer release()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		params, err := json.Marshal(map[string]any{authFieldSessionID: string(testSessionID)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := agent.providerAuth.inventory(ctx, params); err == nil {
			t.Fatal("provider lock timeout ignored")
		}
	})

	t.Run("second ledger read", func(t *testing.T) {
		agent, _ := newAuthAgent(t)
		seedConfirmedLineage(t, agent, testProviderID)
		previous := ledgerReadDir
		calls := 0
		ledgerReadDir = func(path string) ([]os.DirEntry, error) {
			calls++
			if calls == 2 {
				return nil, errors.New("second list")
			}

			return os.ReadDir(path)
		}
		t.Cleanup(func() { ledgerReadDir = previous })
		if _, err := callLeg(t, agent, AuthInventoryMethod, map[string]any{authFieldSessionID: string(testSessionID)}); err == nil {
			t.Fatal("second ledger read failure ignored")
		}
	})
}
