package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

type faultSessionSetLease struct {
	err      error
	releases int
}

func (l *faultSessionSetLease) Release() error {
	l.releases++

	return l.err
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

func installFaultSessionSetLease(t *testing.T, wantMode nativehermes.SharedSessionSetLockMode) *faultSessionSetLease {
	t.Helper()

	lease := &faultSessionSetLease{err: errors.New("release acknowledgement lost")}
	previous := acquireSharedSessionSetLock
	acquireSharedSessionSetLock = func(
		_ context.Context,
		_ string,
		mode nativehermes.SharedSessionSetLockMode,
	) (sharedSessionSetLease, error) {
		if mode != wantMode {
			t.Fatalf("session-set lock mode = %v, want %v", mode, wantMode)
		}

		return lease, nil
	}
	t.Cleanup(func() { acquireSharedSessionSetLock = previous })

	return lease
}

func TestCommittedSharedLifecycleIgnoresLostLockReleaseAcknowledgement(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		lease := installFaultSessionSetLease(t, nativehermes.SharedSessionSetLockExclusive)
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-new")
		agent := newSharedNewSessionCoverageAgent(t, client)

		response, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		if err != nil {
			t.Fatalf("committed new session: %v", err)
		}
		if lease.releases != 1 {
			t.Fatalf("release calls = %d, want 1", lease.releases)
		}
		if session := agent.activeSession(response.SessionId); session != nil {
			_ = session.Close(t.Context())
		}
	})

	t.Run("fork", func(t *testing.T) {
		lease := installFaultSessionSetLease(t, nativehermes.SharedSessionSetLockExclusive)
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		agent := newTestAgent(
			WithScratchDir(t.TempDir()),
			WithSharedHermesHome(t.TempDir()),
			WithSessionStore(NewInMemorySessionStore()),
		)
		agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		}
		parent := testSession(agent, parentClient)
		parent.id = "parent"
		parent.idmap.SessionID = "parent"
		parent.idmap.NativeSessionID = "native-parent"
		agent.sessions[parent.id] = parent

		response, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
		if err != nil {
			t.Fatalf("committed fork: %v", err)
		}
		if lease.releases != 1 {
			t.Fatalf("release calls = %d, want 1", lease.releases)
		}
		if session := agent.activeSession(response.SessionId); session != nil {
			_ = session.Close(t.Context())
		}
	})

	t.Run("prompt", func(t *testing.T) {
		lease := installFaultSessionSetLease(t, nativehermes.SharedSessionSetLockShared)
		client := newFakeHermesClient()
		agent := newTestAgent(WithSharedHermesHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
		agent.setAgentClient(newRecordingAgentClient())
		session := testSession(agent, client)

		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "release-ack", "hello"))
		if err != nil || response.StopReason != acp.StopReasonEndTurn {
			t.Fatalf("committed prompt response = %+v, err = %v", response, err)
		}
		if lease.releases != 1 {
			t.Fatalf("release calls = %d, want 1", lease.releases)
		}
	})
}

func TestSessionOperationIdentityFaults(t *testing.T) {
	wantErr := errors.New("process identity unavailable")

	t.Run("new current identity", func(t *testing.T) {
		previous := sessionOperationCurrentProcessIdentity
		calls := 0
		sessionOperationCurrentProcessIdentity = func() (nativehermes.DurableProcessIdentity, error) {
			calls++
			if calls == 1 {
				return previous()
			}

			return nativehermes.DurableProcessIdentity{}, wantErr
		}
		t.Cleanup(func() { sessionOperationCurrentProcessIdentity = previous })

		client := newFakeHermesClient()
		client.createSession = testNativeSession("native")
		agent := newSharedNewSessionCoverageAgent(t, client)
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); !errors.Is(err, wantErr) {
			t.Fatalf("new identity error = %v", err)
		}
	})

	t.Run("fork current identity", func(t *testing.T) {
		previous := sessionOperationCurrentProcessIdentity
		calls := 0
		sessionOperationCurrentProcessIdentity = func() (nativehermes.DurableProcessIdentity, error) {
			calls++
			if calls == 1 {
				return previous()
			}

			return nativehermes.DurableProcessIdentity{}, wantErr
		}
		t.Cleanup(func() { sessionOperationCurrentProcessIdentity = previous })

		parentClient := newFakeHermesClient()
		agent := newTestAgent(
			WithScratchDir(t.TempDir()),
			WithSharedHermesHome(t.TempDir()),
			WithSessionStore(NewInMemorySessionStore()),
		)
		parent := testSession(agent, parentClient)
		parent.id = "parent"
		parent.idmap.SessionID = "parent"
		parent.idmap.NativeSessionID = "native-parent"
		agent.sessions[parent.id] = parent
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); !errors.Is(err, wantErr) {
			t.Fatalf("fork identity error = %v", err)
		}
	})

	t.Run("recovery current identity", func(t *testing.T) {
		previous := sessionOperationCurrentProcessIdentity
		sessionOperationCurrentProcessIdentity = func() (nativehermes.DurableProcessIdentity, error) {
			return nativehermes.DurableProcessIdentity{}, wantErr
		}
		t.Cleanup(func() { sessionOperationCurrentProcessIdentity = previous })

		home := t.TempDir()
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); !errors.Is(err, wantErr) {
			t.Fatalf("recovery identity error = %v", err)
		}
	})

	t.Run("recovery prior identity inspection", func(t *testing.T) {
		home := t.TempDir()
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		origin := journal.record.Origin
		origin.KernelStartTime += "-different"
		if err := journal.update(sessionOperationJournalPatch{Origin: &origin}); err != nil {
			t.Fatal(err)
		}

		previous := sessionOperationProcessIdentityGone
		sessionOperationProcessIdentityGone = func(nativehermes.DurableProcessIdentity) (bool, error) {
			return false, wantErr
		}
		t.Cleanup(func() { sessionOperationProcessIdentityGone = previous })

		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); !errors.Is(err, wantErr) {
			t.Fatalf("prior identity inspection error = %v", err)
		}
	})
}

func TestSessionOperationMarshalFaults(t *testing.T) {
	wantErr := errors.New("marshal fault")

	t.Run("prepared manifest", func(t *testing.T) {
		journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
		identifyTestNewSessionOperationJournal(t, journal)
		previous := sessionOperationMarshal
		sessionOperationMarshal = func(value any) ([]byte, error) {
			if _, ok := value.(sessionOperationPreparedManifest); ok {
				return nil, wantErr
			}

			return json.Marshal(value)
		}
		t.Cleanup(func() { sessionOperationMarshal = previous })

		if err := journal.prepareReplacements(testSessionOperationReplacements()); !errors.Is(err, wantErr) {
			t.Fatalf("prepared manifest marshal error = %v", err)
		}
	})

	t.Run("journal", func(t *testing.T) {
		journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
		previous := sessionOperationMarshal
		sessionOperationMarshal = func(value any) ([]byte, error) {
			if _, ok := value.(sessionOperationJournalRecord); ok {
				return nil, wantErr
			}

			return json.Marshal(value)
		}
		t.Cleanup(func() { sessionOperationMarshal = previous })

		if err := journal.persist(); !errors.Is(err, wantErr) {
			t.Fatalf("journal marshal error = %v", err)
		}
	})

	t.Run("manifest validation", func(t *testing.T) {
		journal := newTestSessionOperationJournal(t, t.TempDir(), sessionOperationKindNew)
		identifyTestNewSessionOperationJournal(t, journal)
		if err := journal.prepareReplacements(testSessionOperationReplacements()); err != nil {
			t.Fatal(err)
		}
		previous := sessionOperationMarshal
		sessionOperationMarshal = func(value any) ([]byte, error) {
			if _, ok := value.(*sessionOperationPreparedManifest); ok {
				return nil, wantErr
			}

			return json.Marshal(value)
		}
		t.Cleanup(func() { sessionOperationMarshal = previous })

		if err := validatePreparedSessionOperationManifest(journal.record); !errors.Is(err, wantErr) {
			t.Fatalf("manifest validation marshal error = %v", err)
		}
	})
}

func TestSessionOperationFileFaults(t *testing.T) {
	for name, configure := range map[string]func(*faultSessionOperationFile){
		"sync":  func(file *faultSessionOperationFile) { file.syncErr = errors.New("sync") },
		"close": func(file *faultSessionOperationFile) { file.closeErr = errors.New("close") },
	} {
		t.Run(name, func(t *testing.T) {
			file := &faultSessionOperationFile{name: filepath.Join(t.TempDir(), "temporary")}
			configure(file)
			previous := sessionOperationCreateTemp
			sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return file, nil }
			t.Cleanup(func() { sessionOperationCreateTemp = previous })

			if err := atomicWriteSessionOperationFile(filepath.Join(t.TempDir(), "value"), []byte("x"), 0o600); err == nil {
				t.Fatalf("%s failure ignored", name)
			}
		})
	}
}

func TestProviderLockAndSharedConfigFaultSeams(t *testing.T) {
	t.Run("provider lock syscall", func(t *testing.T) {
		wantErr := errors.New("lock syscall")
		previous := authTryProviderFileLock
		authTryProviderFileLock = func(*os.File) (func() error, bool, error) {
			return nil, false, wantErr
		}
		t.Cleanup(func() { authTryProviderFileLock = previous })

		ledger := &authLedger{dir: t.TempDir(), providerLockDir: t.TempDir()}
		if _, err := ledger.acquireProviderLease(t.Context(), "provider"); !errors.Is(err, wantErr) {
			t.Fatalf("provider lock error = %v", err)
		}
	})

	t.Run("MCP redaction", func(t *testing.T) {
		wantErr := errors.New("redaction")
		previous := redactSharedHermesMCPServers
		redactSharedHermesMCPServers = func([]acp.McpServer) ([]acp.McpServer, error) { return nil, wantErr }
		t.Cleanup(func() { redactSharedHermesMCPServers = previous })

		agent := newTestAgent(WithSharedHermesHome(t.TempDir()))
		if err := agent.admitSharedHermesConfig(nil); !errors.Is(err, wantErr) {
			t.Fatalf("redaction error = %v", err)
		}
	})
}
