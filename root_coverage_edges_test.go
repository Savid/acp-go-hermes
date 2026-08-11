package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

type modelSetterTestServer struct {
	nativehermes.Server
	err error
}

func (s modelSetterTestServer) SetModel(context.Context, string, string) error { return s.err }

func TestManagedHermesServerCapabilityEdges(t *testing.T) {
	base := newFakeHermesClient()
	serverOnly := sessionOperationServerOnly{Server: base}
	managed := &managedHermesServer{Server: serverOnly}
	if _, err := managed.CreateSessionWithDraft(t.Context(), "title", func(nativehermes.SessionDraft) error { return nil }); err == nil {
		t.Fatal("missing draft capability accepted")
	}
	if _, err := managed.PersistedSessions(t.Context()); err == nil {
		t.Fatal("missing persisted inventory accepted")
	}
	if _, err := managed.ForkWithBaseline(t.Context(), "parent", "marker", nil); err == nil {
		t.Fatal("missing recoverable fork accepted")
	}
	if err := managed.SetModel(t.Context(), "native", "provider/model"); err == nil {
		t.Fatal("missing model selection capability accepted")
	}
	wantModelErr := errors.New("set model")
	managed.Server = modelSetterTestServer{Server: base, err: wantModelErr}
	if err := managed.SetModel(t.Context(), "native", "provider/model"); !errors.Is(err, wantModelErr) {
		t.Fatalf("model selection error = %v", err)
	}

	base.forkSession = testNativeSession("child")
	managed.Server = base
	if child, err := managed.ForkWithBaseline(t.Context(), "parent", "marker", nil); err != nil || child.ID != "child" {
		t.Fatalf("recoverable fork=%+v err=%v", child, err)
	}

	managed.providerAuthSupported = true
	if !managed.ProviderAuthSupported() {
		t.Fatal("forced provider auth was not advertised")
	}
	managed.providerAuthSupported = false
	supported := false
	base.providerAuthSupported = &supported
	if managed.ProviderAuthSupported() {
		t.Fatal("native provider-auth result ignored")
	}
	managed.Server = serverOnly
	if managed.ProviderAuthSupported() {
		t.Fatal("missing provider-auth capability advertised")
	}
}

func TestClaimSharedNativeSessionFailureEdges(t *testing.T) {
	isolated := newTestAgent()
	if err := isolated.claimSharedNativeSession(newFakeHermesClient(), "native"); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	agent := newTestAgent(WithSharedHermesHome(home))
	if err := agent.claimSharedNativeSession(newFakeHermesClient(), "unmanaged"); err == nil {
		t.Fatal("unmanaged server accepted a shared native claim")
	}

	managed := &managedHermesServer{Server: newFakeHermesClient()}
	if err := agent.claimSharedNativeSession(managed, "first"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = managed.nativeSessionOwner.Release() })
	if err := agent.claimSharedNativeSession(managed, "second"); err == nil {
		t.Fatal("managed server accepted a second shared native claim")
	}

	unbound := &managedHermesServer{Server: sessionOperationServerOnly{Server: newFakeHermesClient()}}
	if err := agent.claimSharedNativeSession(unbound, "third"); err == nil {
		t.Fatal("server without process identity accepted a shared native claim")
	}
}

func TestAuthProviderLeaseEdges(t *testing.T) {
	var nilLease *authProviderLease
	if err := nilLease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := (*authLedger)(nil).acquireProviderLease(t.Context(), "provider"); err == nil {
		t.Fatal("nil ledger acquired provider lease")
	}

	t.Run("fallback lock root mkdir", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		ledger := &authLedger{dir: filepath.Join(parent, "ledger")}
		if _, err := ledger.acquireProviderLease(t.Context(), "provider"); err == nil {
			t.Fatal("invalid fallback lock root accepted")
		}
	})

	t.Run("open lock", func(t *testing.T) {
		ledger := &authLedger{dir: t.TempDir(), providerLockDir: filepath.Join(t.TempDir(), "missing")}
		if _, err := ledger.acquireProviderLease(t.Context(), "provider"); err == nil {
			t.Fatal("missing lock directory accepted")
		}
	})

	t.Run("contention timeout", func(t *testing.T) {
		ledger := &authLedger{dir: t.TempDir()}
		first, err := ledger.acquireProviderLease(t.Context(), "provider")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = first.Release() }()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := ledger.acquireProviderLease(ctx, "provider"); err == nil {
			t.Fatal("contended canceled acquisition succeeded")
		}
	})

	t.Run("unlock and close errors join", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "lease")
		if err != nil {
			t.Fatal(err)
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		unlockErr := errors.New("unlock")
		lease := &authProviderLease{file: file, unlock: func() error { return unlockErr }}
		err = lease.Release()
		if !errors.Is(err, unlockErr) || !strings.Contains(err.Error(), "file already closed") {
			t.Fatalf("release error=%v", err)
		}
		if second := lease.Release(); !errors.Is(second, unlockErr) {
			t.Fatalf("idempotent release=%v", second)
		}
	})

	t.Run("invalid file lock", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "closed")
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := tryAuthProviderFileLock(file); err == nil {
			t.Fatal("closed file lock succeeded")
		}
	})
}

func TestRootUtilityCoverageEdges(t *testing.T) {
	if got := joinModelValue("provider", "model"); got != "provider/model" {
		t.Fatalf("joined model=%q", got)
	}
	if err := validateSharedHermesHomeOptions(Options{SharedHermesHome: "relative"}); err == nil {
		t.Fatal("relative shared home accepted")
	}
	dirty := t.TempDir() + string(filepath.Separator) + "directory" + string(filepath.Separator) + ".."
	if err := validateSharedHermesHomeOptions(Options{SharedHermesHome: dirty}); err == nil {
		t.Fatal("unclean shared home accepted")
	}
}

func TestSetSessionConfigNativeSetterFailure(t *testing.T) {
	client := newFakeHermesClient()
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID: "provider", Models: map[string]nativehermes.ProviderModel{"model": {ID: "model"}},
	}}}
	wantErr := errors.New("set model")
	agent := newTestAgent()
	session := newSession(agent, "session-1", "/tmp/project", nil, nil, testNativeSession("native-1"), modelSetterTestServer{Server: client, err: wantErr}, sessionMeta{}, idmapRecord{
		SessionID: "session-1", NativeSessionID: "native-1", Format: SessionStoreFormat,
	})
	agent.sessions[session.id] = session
	if _, err := agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(session.id, configModel, "provider/model")); !errors.Is(err, wantErr) {
		t.Fatalf("set model error=%v", err)
	}
}

func TestSnapshotJournalAndStoreReconciliationEdges(t *testing.T) {
	t.Run("journal preparation", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		session := testSession(agent, newFakeHermesClient())
		journal := newTestSessionOperationJournalWithLogical(t, t.TempDir(), sessionOperationKindNew, string(session.id))
		identifyTestNewSessionOperationJournal(t, journal)
		session.operationJournal = journal
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return nil, errors.New("prepare") }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := session.snapshotToStore(t.Context()); err == nil {
			t.Fatal("journal preparation failure ignored")
		}
	})

	t.Run("commit marker retained", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		session := testSession(agent, newFakeHermesClient())
		journal := newTestSessionOperationJournalWithLogical(t, t.TempDir(), sessionOperationKindNew, string(session.id))
		identifyTestNewSessionOperationJournal(t, journal)
		session.operationJournal = journal
		previous := sessionOperationRename
		sessionOperationRename = func(source, target string) error {
			if filepath.Base(target) == sessionOperationJournalName && journal.record.Phase == sessionOperationPhaseStoreCommitted {
				return errors.New("commit marker")
			}

			return previous(source, target)
		}
		t.Cleanup(func() { sessionOperationRename = previous })
		if err := session.snapshotToStore(t.Context()); err != nil {
			t.Fatalf("committed Store blocked by journal marker: %v", err)
		}
	})

	store := sessionOperationFaultStore{base: NewInMemorySessionStore(), listSubkeysErr: errors.New("list")}
	_, _, err := reconcileSessionStoreReplacement(t.Context(), store, SessionKey{SessionID: "logical"}, []SessionStoreReplacement{{
		Key: SessionKey{SessionID: "logical"}, Entries: []SessionStoreEntry{json.RawMessage(`{"main":true}`)},
	}})
	if err == nil {
		t.Fatal("replacement subkey listing failure ignored")
	}
}

func TestSharedHomePromptSessionSetLock(t *testing.T) {
	home := t.TempDir()
	agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
	client := newFakeHermesClient()
	session := testSession(agent, client)
	response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "shared-lock", "reply"))
	if err != nil || response.StopReason == "" {
		t.Fatalf("shared-home prompt=%+v err=%v", response, err)
	}

	lock, err := nativehermes.AcquireSharedSessionSetLock(t.Context(), home, nativehermes.SharedSessionSetLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Release() }()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := session.Prompt(ctx, TextPromptRequest(session.id, "blocked-lock", "blocked")); err == nil {
		t.Fatal("contended shared-home turn lock succeeded")
	}
}

func TestAuthLedgerProviderLockRootFailures(t *testing.T) {
	for name, apply := range map[string]func(){
		"mkdir": func() {
			ledgerMkdirAll = func(path string, mode os.FileMode) error {
				if filepath.Base(path) == authProviderLockDir {
					return errors.New("lock mkdir")
				}

				return os.MkdirAll(path, mode)
			}
		},
		"chmod": func() {
			ledgerChmod = func(path string, mode os.FileMode) error {
				if filepath.Base(path) == authProviderLockDir {
					return errors.New("lock chmod")
				}

				return os.Chmod(path, mode)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			restoreLedgerHooks(t)
			apply()
			if _, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), SharedHermesHome: t.TempDir()}); err == nil {
				t.Fatalf("provider lock root %s failure ignored", name)
			}
		})
	}
}

func TestProviderAuthCrossProcessLeaseFailures(t *testing.T) {
	t.Run("disconnect", func(t *testing.T) {
		agent, _ := newAuthAgent(t)
		agent.providerAuth.ledger = &authLedger{}
		if _, err := callLeg(t, agent, AuthDisconnectMethod, disconnectParams(testConnectionID, 1)); err == nil {
			t.Fatal("disconnect provider-lease failure ignored")
		}
	})

	t.Run("authorize", func(t *testing.T) {
		agent, client := newAuthAgent(t)
		generation := seedCatalog(t, agent, client)
		agent.providerAuth.ledger = &authLedger{}
		if _, err := callLeg(t, agent, AuthAuthorizeMethod, authorizeParams(
			generation, testProviderID, nativehermes.AuthFlowDeviceCode, "lease-failure",
		)); err == nil {
			t.Fatal("authorize provider-lease failure ignored")
		}
	})
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
