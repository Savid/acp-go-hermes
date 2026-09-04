//go:build !windows

package hermesacp

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestNewAndForkReconcileCommittedReplaceAcknowledgementLoss(t *testing.T) {
	ctx := t.Context()
	home := t.TempDir()
	store := &ackLostReplaceStore{InMemorySessionStore: NewInMemorySessionStore()}
	newClient := newFakeHermesClient()
	newClient.createSession = testNativeSession("native-new")
	agent := newTestAgent(WithSessionStore(store), WithSharedHermesHome(home), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			newClient.xdg = opts.ExistingXDG

			return newClient, nil
		}
	})
	created, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession after committed acknowledgement loss: %v", err)
	}
	if agent.activeSession(created.SessionId) == nil {
		t.Fatal("committed NewSession was not registered")
	}

	childClient := newFakeHermesClient()
	childClient.getSession = testNativeSession("native-child")
	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		childClient.xdg = opts.ExistingXDG

		return childClient, nil
	}
	parent := testSession(agent, parentClient)
	parent.id = "parent-for-ack-loss"
	parent.idmap.SessionID = string(parent.id)
	parent.idmap.NativeSessionID = "native-parent"
	agent.sessions[parent.id] = parent
	forked, err := agent.forkSession(ctx, ForkSessionRequest(parent.id, t.TempDir()))
	if err != nil {
		t.Fatalf("Fork after committed acknowledgement loss: %v", err)
	}
	if agent.activeSession(forked.SessionId) == nil || len(parentClient.deleted) != 0 {
		t.Fatalf("committed fork registration/deletion = %#v/%#v", agent.activeSession(forked.SessionId), parentClient.deleted)
	}
}

func TestCommittedSharedLifecycleIgnoresLostLockReleaseAcknowledgement(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		lease := installFaultSessionSetLease(t, nativehermes.SharedSessionSetLockExclusive)
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-new")
		agent := newSharedHomeLifecycleAgent(t, client)

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

func TestSharedNewSessionTransactionFailureEdges(t *testing.T) {
	t.Run("baseline and operation entropy", func(t *testing.T) {
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "existing"}}
		agent := newSharedHomeLifecycleAgent(t, client)
		previousSessionReader, previousOperationReader := sessionIDRandReader, cryptorand.Reader
		sessionIDRandReader = strings.NewReader(strings.Repeat("x", 16))
		cryptorand.Reader = sessionOperationErrorReader{err: errors.New("operation entropy")}
		t.Cleanup(func() {
			sessionIDRandReader = previousSessionReader
			cryptorand.Reader = previousOperationReader
		})
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("operation-id entropy failure ignored")
		}
	})

	t.Run("session set lock", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native")
		agent := newSharedHomeLifecycleAgent(t, client)
		lock, err := nativehermes.AcquireSharedSessionSetLock(t.Context(), agent.options.SharedHermesHome, nativehermes.SharedSessionSetLockExclusive)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Release() }()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if _, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("contended session-set lock succeeded")
		}
	})

	t.Run("pending recovery", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native")
		agent := newSharedHomeLifecycleAgent(t, client)
		control, err := nativehermes.EnsureSharedHermesAdapterControlDir(agent.options.SharedHermesHome)
		if err != nil {
			t.Fatal(err)
		}
		operations := filepath.Join(control, sessionOperationDirectoryName)
		if err := os.MkdirAll(operations, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(operations, "unexpected"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("malformed pending recovery ignored")
		}
	})

	t.Run("persisted inventory", func(t *testing.T) {
		client := newFakeHermesClient()
		client.listErr = errors.New("inventory")
		agent := newSharedHomeLifecycleAgent(t, client)
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("inventory failure ignored")
		}
	})

	t.Run("begin journal", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newSharedHomeLifecycleAgent(t, client)
		previous := sessionOperationMkdir
		sessionOperationMkdir = func(string, os.FileMode) error { return errors.New("journal mkdir") }
		t.Cleanup(func() { sessionOperationMkdir = previous })
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("journal begin failure ignored")
		}
	})

	t.Run("advance journal", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newSharedHomeLifecycleAgent(t, client)
		previous := sessionOperationNow
		calls := 0
		sessionOperationNow = func() time.Time {
			calls++
			if calls > 1 {
				return time.UnixMilli(1)
			}

			return time.Now()
		}
		t.Cleanup(func() { sessionOperationNow = previous })
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("journal phase failure ignored")
		}
	})

	t.Run("native claim", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-conflict")
		agent := newSharedHomeLifecycleAgent(t, client)
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "native-conflict")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("conflicting native claim succeeded")
		}
	})

	t.Run("active registration after commit", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSessionFunc = func(context.Context, string) (nativehermes.Session, error) {
			return testNativeSession("native-capacity"), nil
		}
		agent := newSharedHomeLifecycleAgent(t, client)
		client.createSessionFunc = func(context.Context, string) (nativehermes.Session, error) {
			agent.mu.Lock()
			for index := 0; index < agent.options.ConcurrencyLimits.MaxActiveSessions; index++ {
				agent.sessions[acp.SessionId("capacity-"+string(rune('a'+index)))] = nil
			}
			agent.mu.Unlock()

			return testNativeSession("native-capacity"), nil
		}
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("post-commit registration capacity failure ignored")
		}
		agent.mu.Lock()
		clear(agent.sessions)
		agent.mu.Unlock()
	})

	t.Run("committed journal cleanup", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-success")
		agent := newSharedHomeLifecycleAgent(t, client)
		previous := sessionOperationRemoveAll
		sessionOperationRemoveAll = func(path string) error {
			if filepath.Base(filepath.Dir(path)) == sessionOperationDirectoryName {
				return errors.New("retain journal")
			}

			return os.RemoveAll(path)
		}
		response, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		sessionOperationRemoveAll = previous
		t.Cleanup(func() { sessionOperationRemoveAll = previous })
		if err != nil {
			t.Fatalf("journal cleanup blocked success: %v", err)
		}
		if session := agent.activeSession(response.SessionId); session != nil {
			_ = session.Close(t.Context())
		}
	})
}

func TestSharedLoadAndRuntimeResumeEdges(t *testing.T) {
	newLoadAgent := func(t *testing.T, factory func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)) *Agent {
		t.Helper()
		agent := newTestAgent(
			WithScratchDir(t.TempDir()),
			WithSharedHermesHome(t.TempDir()),
			WithSessionStore(validHydrateStore(t, t.Context())),
		)
		agent.options.clientFactory = factory

		return agent
	}

	t.Run("load success without native archive", func(t *testing.T) {
		client := newFakeHermesClient()
		client.getSession = testNativeSession("n")
		agent := newLoadAgent(t, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = start.ExistingXDG

			return client, nil
		})
		response, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir()))
		if err != nil || response.Meta == nil {
			t.Fatalf("shared load response=%+v err=%v", response, err)
		}
		if session := agent.activeSession("s"); session != nil {
			_ = session.Close(t.Context())
		}
	})

	t.Run("load owner conflict", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			t.Fatal("factory called after owner conflict")

			return nil, errors.New("unreachable factory")
		})
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "n")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); err == nil {
			t.Fatal("conflicting load owner succeeded")
		}
	})

	t.Run("load startup containment", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, ErrContainmentIncomplete
		})
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); !errors.Is(err, ErrContainmentIncomplete) {
			t.Fatalf("containment error=%v", err)
		}
	})

	t.Run("load startup ordinary failure", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, errors.New("start")
		})
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); err == nil {
			t.Fatal("ordinary startup failure ignored")
		}
	})

	t.Run("load active registration", func(t *testing.T) {
		client := newFakeHermesClient()
		client.getSession = testNativeSession("n")
		var agent *Agent
		agent = newLoadAgent(t, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = start.ExistingXDG
			agent.mu.Lock()
			for index := 0; index < agent.options.ConcurrencyLimits.MaxActiveSessions; index++ {
				agent.sessions[acp.SessionId("load-capacity-"+string(rune('a'+index)))] = nil
			}
			agent.mu.Unlock()

			return client, nil
		})
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); err == nil {
			t.Fatal("load registration capacity failure ignored")
		}
		agent.mu.Lock()
		clear(agent.sessions)
		agent.mu.Unlock()
	})

	t.Run("runtime resume success", func(t *testing.T) {
		client := newFakeHermesClient()
		client.getSession = testNativeSession("n")
		agent := newLoadAgent(t, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = start.ExistingXDG

			return client, nil
		})
		session := newSession(agent, "s", "", nil, nil, testNativeSession("n"), newFakeHermesClient(), sessionMeta{}, validHydrateIDMap())
		session.runtimeNeedsResume = true
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err != nil {
			t.Fatal(err)
		}
		if session.runtimeNeedsResume {
			t.Fatal("runtime remained fenced")
		}
		_ = session.Close(t.Context())
	})

	t.Run("runtime owner conflict", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			t.Fatal("factory called after owner conflict")

			return nil, errors.New("unreachable factory")
		})
		session := newSession(agent, "s", "", nil, nil, testNativeSession("n"), newFakeHermesClient(), sessionMeta{}, validHydrateIDMap())
		session.runtimeNeedsResume = true
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "n")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil {
			t.Fatal("conflicting resume owner succeeded")
		}
	})

	for name, startErr := range map[string]error{
		"runtime ordinary start": errors.New("start"),
		"runtime containment":    ErrContainmentIncomplete,
	} {
		t.Run(name, func(t *testing.T) {
			agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
				return nil, startErr
			})
			session := newSession(agent, "s", "", nil, nil, testNativeSession("n"), newFakeHermesClient(), sessionMeta{}, validHydrateIDMap())
			session.runtimeNeedsResume = true
			if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil {
				t.Fatalf("%s failure ignored", name)
			}
		})
	}
}

//nolint:gocyclo // Shared fork transaction failures intentionally share one setup matrix.
func TestSharedForkTransactionFailureEdges(t *testing.T) {
	newForkAgent := func(t *testing.T, parentClient *fakeHermesClient, factory func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)) (*Agent, *session) {
		t.Helper()
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithSharedHermesHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
		agent.options.clientFactory = factory
		parent := testSession(agent, parentClient)
		parent.id = "parent"
		parent.idmap.SessionID = "parent"
		parent.idmap.NativeSessionID = "native-parent"
		agent.sessions[parent.id] = parent

		return agent, parent
	}

	t.Run("inventory capability", func(t *testing.T) {
		base := newFakeHermesClient()
		parentClient := sessionOperationServerOnly{Server: base}
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithSharedHermesHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
		parent := newSession(agent, "parent", t.TempDir(), nil, nil, testNativeSession("native-parent"), parentClient, sessionMeta{}, idmapRecord{SessionID: "parent", NativeSessionID: "native-parent", Format: SessionStoreFormat})
		agent.sessions[parent.id] = parent
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork without persisted inventory succeeded")
		}
	})

	t.Run("shared MCP admission", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		if err := agent.admitSharedHermesConfig([]acp.McpServer{StdioMCPServer("existing", "command", nil, nil)}); err != nil {
			t.Fatal(err)
		}
		request := ForkSessionRequest(parent.id, t.TempDir(), WithSessionMCPServers(StdioMCPServer("different", "command", nil, nil)))
		if _, err := agent.forkSession(t.Context(), request); err == nil {
			t.Fatal("changed shared MCP config accepted")
		}
	})

	t.Run("closed parent", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		parent.closed = true
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("closed parent forked")
		}
	})

	t.Run("session set lock", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		lock, err := nativehermes.AcquireSharedSessionSetLock(t.Context(), agent.options.SharedHermesHome, nativehermes.SharedSessionSetLockExclusive)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Release() }()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if _, err := agent.forkSession(ctx, ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("contended fork session-set lock succeeded")
		}
	})

	t.Run("pending recovery", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		control, err := nativehermes.EnsureSharedHermesAdapterControlDir(agent.options.SharedHermesHome)
		if err != nil {
			t.Fatal(err)
		}
		operations := filepath.Join(control, sessionOperationDirectoryName)
		if err := os.MkdirAll(operations, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(operations, "unexpected"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork pending recovery failure ignored")
		}
	})

	t.Run("inventory read", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.listErr = errors.New("inventory")
		agent, parent := newForkAgent(t, parentClient, nil)
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork inventory failure ignored")
		}
	})

	t.Run("journal begin", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		previous := sessionOperationMkdir
		sessionOperationMkdir = func(string, os.FileMode) error { return errors.New("journal") }
		t.Cleanup(func() { sessionOperationMkdir = previous })
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork journal failure ignored")
		}
	})

	t.Run("operation entropy", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		previousSessionReader, previousOperationReader := sessionIDRandReader, cryptorand.Reader
		sessionIDRandReader = strings.NewReader(strings.Repeat("x", 16))
		cryptorand.Reader = sessionOperationErrorReader{err: errors.New("operation entropy")}
		t.Cleanup(func() {
			sessionIDRandReader = previousSessionReader
			cryptorand.Reader = previousOperationReader
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork operation-id entropy failure ignored")
		}
	})

	t.Run("journal mutating update", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		previous := sessionOperationNow
		calls := 0
		sessionOperationNow = func() time.Time {
			calls++
			if calls > 1 {
				return time.UnixMilli(1)
			}

			return time.Now()
		}
		t.Cleanup(func() { sessionOperationNow = previous })
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork mutating journal update failure ignored")
		}
	})

	t.Run("journal child identification", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, nil)
		previous := sessionOperationRename
		journalWrites := 0
		sessionOperationRename = func(source, target string) error {
			if filepath.Base(target) == sessionOperationJournalName {
				journalWrites++
				if journalWrites == 3 {
					return errors.New("identify")
				}
			}

			return previous(source, target)
		}
		t.Cleanup(func() { sessionOperationRename = previous })
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork child journal identification failure ignored")
		}
	})

	t.Run("branch no delta", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.persistedSessions = []nativehermes.Session{{ID: "native-parent"}}
		parentClient.forkErr = errors.New("branch")
		agent, parent := newForkAgent(t, parentClient, nil)
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("branch failure ignored")
		}
	})

	t.Run("child owner conflict", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, nil)
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "native-child")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("conflicting fork child owner succeeded")
		}
	})

	t.Run("child startup", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, errors.New("start child")
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("child startup failure ignored")
		}
	})

	t.Run("child startup containment", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, ErrContainmentIncomplete
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); !errors.Is(err, ErrContainmentIncomplete) {
			t.Fatalf("child containment error=%v", err)
		}
	})

	t.Run("child native drift", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("different")
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("child native drift accepted")
		}
	})

	t.Run("child drift containment close", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("different")
		childClient.closeErr = ErrContainmentIncomplete
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("drift containment close accepted")
		}
	})

	t.Run("snapshot cleanup containment", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		childClient.closeErr = ErrContainmentIncomplete
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		agent.options.SessionStore = &toggleReplaceStore{InMemorySessionStore: NewInMemorySessionStore(), fail: true}
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("snapshot cleanup containment ignored")
		}
	})

	t.Run("active registration after commit", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		var agent *Agent
		var parent *session
		agent, parent = newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG
			agent.mu.Lock()
			for len(agent.sessions) < agent.options.ConcurrencyLimits.MaxActiveSessions {
				agent.sessions[acp.SessionId("fork-capacity-"+string(rune('a'+len(agent.sessions))))] = nil
			}
			agent.mu.Unlock()

			return childClient, nil
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork registration capacity failure ignored")
		}
		agent.mu.Lock()
		clear(agent.sessions)
		agent.mu.Unlock()
	})

	t.Run("committed journal cleanup", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		previous := sessionOperationRemoveAll
		sessionOperationRemoveAll = func(path string) error {
			if filepath.Base(filepath.Dir(path)) == sessionOperationDirectoryName {
				return errors.New("retain journal")
			}

			return os.RemoveAll(path)
		}
		response, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
		sessionOperationRemoveAll = previous
		t.Cleanup(func() { sessionOperationRemoveAll = previous })
		if err != nil {
			t.Fatalf("fork journal cleanup blocked success: %v", err)
		}
		if child := agent.activeSession(response.SessionId); child != nil {
			_ = child.Close(t.Context())
		}
	})
}

func TestSharedClientAdmissionAndOwnerBindingEdges(t *testing.T) {
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSharedHermesHome(t.TempDir()))
	if err := agent.admitSharedHermesConfig(nil); err != nil {
		t.Fatal(err)
	}
	badServers := []acp.McpServer{StdioMCPServer("different", "command", nil, nil)}
	if _, err := agent.newHermesClient(t.Context(), "session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{}, badServers); err == nil {
		t.Fatal("changed shared MCP config accepted during client start")
	}

	owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "native")
	if err != nil {
		t.Fatal(err)
	}
	agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
		client := newFakeHermesClient()
		client.xdg = start.ExistingXDG

		return sessionOperationServerOnly{Server: client}, nil
	}
	parent, err := ensureScratchParent(agent.options.ScratchDir)
	if err != nil {
		t.Fatal(err)
	}
	xdg, err := createHermesGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	client, err := agent.newHermesClientWithScratchOwner(t.Context(), "session", t.TempDir(), sessionMeta{}, xdg, func() {}, owner)
	if err != nil {
		t.Fatal(err)
	}
	if client.nativeSessionOwner != owner {
		t.Fatal("shared native-session owner was not bound to the managed server")
	}
	if closeErr := client.Close(t.Context()); closeErr != nil {
		t.Fatal(closeErr)
	}
	replacement, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "native")
	if err != nil {
		t.Fatalf("managed server close did not release the native-session owner: %v", err)
	}
	if err := replacement.Release(); err != nil {
		t.Fatal(err)
	}
}
