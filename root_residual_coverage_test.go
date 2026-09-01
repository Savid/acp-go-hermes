package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestManagedHermesServerOptionalSurfaces(t *testing.T) {
	const storedSessionID = "stored"

	unsupported := &managedHermesServer{Server: struct{ nativehermes.Server }{}}
	if _, err := unsupported.CreateSessionWithDraft(t.Context(), "title", func(nativehermes.SessionDraft) error { return nil }); err == nil {
		t.Fatal("unsupported draft creation succeeded")
	}
	if _, err := unsupported.PersistedSessions(t.Context()); err == nil {
		t.Fatal("unsupported persisted inventory succeeded")
	}
	if _, err := unsupported.ForkWithBaseline(t.Context(), "id", "marker", nil); err == nil {
		t.Fatal("unsupported recoverable fork succeeded")
	}
	if err := unsupported.SetModel(t.Context(), "id", "provider/model"); err == nil {
		t.Fatal("unsupported model selection succeeded")
	}
	if unsupported.ProviderAuthSupported() {
		t.Fatal("unsupported provider auth was advertised")
	}

	client := newFakeHermesClient()
	client.createSession = nativehermes.Session{ID: storedSessionID}
	client.persistedSessions = []nativehermes.Session{{ID: storedSessionID}}
	client.forkSession = nativehermes.Session{ID: "forked"}
	supported := &managedHermesServer{Server: client}
	created, err := supported.CreateSessionWithDraft(t.Context(), "title", func(draft nativehermes.SessionDraft) error {
		if draft.StoredSessionID != storedSessionID {
			t.Fatalf("draft = %#v", draft)
		}

		return nil
	})
	if err != nil || created.ID != storedSessionID {
		t.Fatalf("draft creation = %#v, %v", created, err)
	}
	listed, err := supported.PersistedSessions(t.Context())
	if err != nil || len(listed) != 1 {
		t.Fatalf("persisted sessions = %#v, %v", listed, err)
	}
	forked, err := supported.ForkWithBaseline(t.Context(), storedSessionID, "marker", nil)
	if err != nil || forked.ID != "forked" {
		t.Fatalf("recoverable fork = %#v, %v", forked, err)
	}
	if err := supported.SetModel(t.Context(), storedSessionID, "provider/model"); err != nil {
		t.Fatal(err)
	}
	if !supported.ProviderAuthSupported() {
		t.Fatal("supported provider auth was hidden")
	}
}

func TestManagedHermesServerSnapshotResidualBranches(t *testing.T) {
	if _, err := (&managedHermesServer{}).reclaimForSnapshot(t.Context()); err == nil {
		t.Fatal("unmanaged snapshot reclaim succeeded")
	}
	if _, err := (&managedHermesServer{managed: true, closed: true}).reclaimForSnapshot(t.Context()); err == nil {
		t.Fatal("closed snapshot reclaim succeeded")
	}
	settled := &managedHermesServer{managed: true, settled: true, root: "/settled"}
	if root, err := settled.reclaimForSnapshot(t.Context()); err != nil || root != "/settled" {
		t.Fatalf("settled snapshot reclaim = %q, %v", root, err)
	}

	retained := 0
	client := newFakeHermesClient()
	client.closeErr = ErrContainmentIncomplete
	server := &managedHermesServer{
		Server: client, managed: true, root: t.TempDir(), sessionID: "session",
		retainIncomplete: func(error, acp.SessionId, string) { retained++ },
	}
	if _, err := server.reclaimForSnapshot(t.Context()); !errors.Is(err, ErrContainmentIncomplete) || retained != 1 {
		t.Fatalf("failed snapshot reclaim = %v, retained=%d", err, retained)
	}

	if err := (&managedHermesServer{}).finishReclaimedSnapshot(); err == nil {
		t.Fatal("unreclaimed snapshot finished")
	}
	wantClosed := errors.New("closed snapshot")
	if err := (&managedHermesServer{closed: true, closeErr: wantClosed}).finishReclaimedSnapshot(); !errors.Is(err, wantClosed) {
		t.Fatalf("closed snapshot finish = %v", err)
	}

	originalRemoveAll := managedRemoveAll
	t.Cleanup(func() { managedRemoveAll = originalRemoveAll })
	managedRemoveAll = func(string) error { return errors.New("scratch remove refused") }
	if err := (&managedHermesServer{settled: true, root: t.TempDir()}).finishReclaimedSnapshot(); err == nil {
		t.Fatal("snapshot cleanup failure was ignored")
	}
	if err := (&managedHermesServer{Server: newFakeHermesClient(), settled: true, root: t.TempDir()}).Close(t.Context()); err == nil {
		t.Fatal("close cleanup failure was ignored")
	}
	managedRemoveAll = originalRemoveAll
}

func TestManagedHermesServerSnapshotOwnerResidualBranches(t *testing.T) {
	successOwner, err := nativehermes.AcquireSharedNativeSessionOwner(t.TempDir(), "success")
	if err != nil {
		t.Fatal(err)
	}
	success := &managedHermesServer{
		Server: newFakeHermesClient(), managed: true, root: t.TempDir(), nativeSessionOwner: successOwner,
	}
	if _, reclaimErr := success.reclaimForSnapshot(t.Context()); reclaimErr != nil || !success.ownerReleased {
		t.Fatalf("successful owner reclaim = %v, released=%v", reclaimErr, success.ownerReleased)
	}

	home := t.TempDir()
	failingOwner, err := nativehermes.AcquireSharedNativeSessionOwner(home, "failure")
	if err != nil {
		t.Fatal(err)
	}
	control, err := nativehermes.SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(control); err != nil {
		t.Fatal(err)
	}
	failing := &managedHermesServer{
		Server: newFakeHermesClient(), managed: true, root: t.TempDir(), nativeSessionOwner: failingOwner,
	}
	if _, err := failing.reclaimForSnapshot(t.Context()); err == nil || failing.ownerReleased {
		t.Fatalf("failed owner reclaim = %v, released=%v", err, failing.ownerReleased)
	}
}

func TestManagedHermesOwnershipAndScratchResidualBranches(t *testing.T) {
	agent := NewAgent()
	agent.retainIncompleteHermesRoot("session", "/retained")
	if err := agent.rejectIncompleteHermesSession("session"); !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("retained session rejection = %v", err)
	}
	if root := hermesServerRoot(nil); root != "" {
		t.Fatalf("nil server root = %q", root)
	}

	home := t.TempDir()
	shared := NewAgent(WithSharedHermesHome(home))
	if shared.optionsErr != nil {
		t.Fatal(shared.optionsErr)
	}
	if err := shared.claimSharedNativeSession(newFakeHermesClient(), "native"); err == nil || !strings.Contains(err.Error(), "requires a managed server") {
		t.Fatalf("non-managed native claim = %v", err)
	}

	released := false
	if err := deleteHermesScratchRoot("", func() { released = true }); err != nil || released {
		t.Fatalf("empty scratch deletion = %v, released=%v", err, released)
	}
	originalRemoveAll := managedRemoveAll
	t.Cleanup(func() { managedRemoveAll = originalRemoveAll })
	managedRemoveAll = func(string) error { return errors.New("remove refused") }
	if err := deleteHermesScratchRoot(t.TempDir(), func() { released = true }); err == nil || released {
		t.Fatalf("failed scratch deletion = %v, released=%v", err, released)
	}
}

func TestStrictStoreDecoderResidualBranches(t *testing.T) {
	var destination struct{}
	if err := decodeStrictStoreJSON([]byte(`1`), &destination, nil); err == nil {
		t.Fatal("typed store decode mismatch was accepted")
	}
	decoder := json.NewDecoder(strings.NewReader(""))
	if err := walkStrictJSONValue(decoder, nil, "$"); err == nil {
		t.Fatal("empty strict JSON value was accepted")
	}
	decoder = json.NewDecoder(strings.NewReader(`[{"x":`))
	if err := walkStrictJSONValue(decoder, nil, "$"); err == nil {
		t.Fatal("truncated strict JSON array was accepted")
	}
}

func TestSharedHomeHostAuthorityValidationResidualBranch(t *testing.T) {
	options := Options{}
	options.SharedHermesHome = filepath.Clean(t.TempDir())
	options.HostAuthority = newTestHostAuthority()
	if err := validateSharedHermesHomeOptions(options); err == nil {
		t.Fatal("shared home accepted host authority")
	}
}

func TestHostAuthorityValidationAndEnvironmentResidualBranches(t *testing.T) {
	if hostAuthorityNil(residualValueAuthority{}) {
		t.Fatal("value authority was classified as nil")
	}
	if _, err := readHostEnvironment(nil); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("nil authority environment = %v", err)
	}
	if _, err := readHostEnvironment(&residualAuthority{environment: func() map[string]string {
		panic("environment unavailable")
	}}); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("panicked authority environment = %v", err)
	}
	if _, err := readHostEnvironment(&residualAuthority{environment: func() map[string]string { return nil }}); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("nil authority environment result = %v", err)
	}
	if err := validateHostAuthority(Options{
		hostAuthoritySupplied: true, HostAuthority: residualValueAuthority{}, SharedHermesHome: t.TempDir(),
	}); err == nil {
		t.Fatal("shared residence accepted a value host authority")
	}
}

func TestConfiguredHostAuthorityResidualBranches(t *testing.T) {
	newConfigured := func(t *testing.T) (*Agent, *residualAuthority, nativehermes.StartOptions) {
		t.Helper()
		authority := &residualAuthority{}
		agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
		if agent.optionsErr != nil {
			t.Fatal(agent.optionsErr)
		}
		start := nativehermes.StartOptions{}
		agent.configureHostAuthority(&start)

		return agent, authority, start
	}

	t.Run("prepare admission", func(t *testing.T) {
		agent, _, start := newConfigured(t)
		agent.authorityErr = ErrHostAuthorityUnavailable
		if err := start.PrepareNativeTree(t.Context(), t.TempDir()); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("prepare admission = %v", err)
		}
	})
	t.Run("prepare panic", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.prepare = func(context.Context, string) error { panic("prepare unavailable") }
		if err := start.PrepareNativeTree(t.Context(), t.TempDir()); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("prepare panic = %v", err)
		}
	})
	t.Run("prepare busy", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.prepare = func(context.Context, string) error { return ErrNativeTreeBusy }
		if err := start.PrepareNativeTree(t.Context(), t.TempDir()); !errors.Is(err, ErrNativeTreeBusy) {
			t.Fatalf("prepare busy = %v", err)
		}
	})
	t.Run("start admission", func(t *testing.T) {
		agent, _, start := newConfigured(t)
		agent.authorityErr = ErrHostAuthorityUnavailable
		if _, err := start.StartNative(t.Context(), nativehermes.NativeRequest{}); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("start admission = %v", err)
		}
	})
	t.Run("start panic", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.start = func(context.Context, NativeRequest) (NativeProcess, error) { panic("start unavailable") }
		if _, err := start.StartNative(t.Context(), nativehermes.NativeRequest{}); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("start panic = %v", err)
		}
	})
	t.Run("start nil process", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.start = func(context.Context, NativeRequest) (NativeProcess, error) {
			return nilResidualNativeProcess(), nil
		}
		if _, err := start.StartNative(t.Context(), nativehermes.NativeRequest{}); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("nil start process = %v", err)
		}
	})
	t.Run("reclaim panic", func(t *testing.T) {
		agent, authority, _ := newConfigured(t)
		authority.reclaim = func(context.Context, string) error { panic("reclaim unavailable") }
		if err := agent.reclaimNativeTree(t.Context(), t.TempDir()); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("reclaim panic = %v", err)
		}
	})
	t.Run("reclaim failure", func(t *testing.T) {
		agent, authority, _ := newConfigured(t)
		want := errors.New("reclaim refused")
		authority.reclaim = func(context.Context, string) error { return want }
		if err := agent.reclaimNativeTree(t.Context(), t.TempDir()); !errors.Is(err, want) || !errors.Is(err, ErrContainmentIncomplete) {
			t.Fatalf("reclaim failure = %v", err)
		}
	})
}

func TestRetiredNativeRootRetryResidualBranches(t *testing.T) {
	authority := &residualAuthority{}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	root := t.TempDir()
	agent.retiredNativeRoots[root] = true
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := agent.retryRetiredNativeRoots(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry = %v", err)
	}

	want := errors.New("reclaim refused")
	authority.reclaim = func(context.Context, string) error { return want }
	if err := agent.retryRetiredNativeRoots(t.Context()); !errors.Is(err, want) || !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("failed retry = %v", err)
	}
}

func TestNativeProcessBridgeResidualBranches(t *testing.T) {
	if !nativeProcessNil(nil) {
		t.Fatal("nil process was not classified as nil")
	}
	if nativeProcessNil(residualValueNativeProcess{}) {
		t.Fatal("value process was classified as nil")
	}
	want := errors.New("native failure")
	assertRecorded := func(t *testing.T, invoke func(*residualNativeProcess, nativeProcessBridge) error) {
		t.Helper()
		var recorded error
		process := &residualNativeProcess{}
		bridge := nativeProcessBridge{
			process: process,
			record: func(err error) error {
				recorded = err

				return err
			},
		}
		if err := invoke(process, bridge); err == nil || recorded == nil {
			t.Fatalf("bridge result = %v, recorded=%v", err, recorded)
		}
	}

	if err := (nativeProcessBridge{}).recordError(want); !errors.Is(err, want) {
		t.Fatalf("nil recorder = %v", err)
	}
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stdout = func() io.ReadCloser { panic("stdout unavailable") }
		if stream := bridge.Stdout(); stream != nil {
			t.Fatal("panicked stdout returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stdout = func() io.ReadCloser { return nil }
		if stream := bridge.Stdout(); stream != nil {
			t.Fatal("nil stdout returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stderr = func() io.ReadCloser { panic("stderr unavailable") }
		if stream := bridge.Stderr(); stream != nil {
			t.Fatal("panicked stderr returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stderr = func() io.ReadCloser { return nil }
		if stream := bridge.Stderr(); stream != nil {
			t.Fatal("nil stderr returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.wait = func(context.Context) (NativeResult, error) { panic("wait unavailable") }
		_, err := bridge.Wait(t.Context())

		return err
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.wait = func(context.Context) (NativeResult, error) { return NativeResult{}, want }
		_, err := bridge.Wait(t.Context())

		return err
	})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	cancelBridge := nativeProcessBridge{process: &residualNativeProcess{
		wait: func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: 7}, context.Canceled },
	}}
	if result, err := cancelBridge.Wait(canceled); !errors.Is(err, context.Canceled) || result.ExitCode != 7 {
		t.Fatalf("canceled wait = %#v, %v", result, err)
	}
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.revoke = func(context.Context) error { panic("revoke unavailable") }

		return bridge.Revoke(t.Context())
	})
	cancelBridge.process = &residualNativeProcess{revoke: func(context.Context) error { return context.Canceled }}
	if err := cancelBridge.Revoke(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled revoke = %v", err)
	}
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.revoke = func(context.Context) error { return ErrContainmentIncomplete }

		return bridge.Revoke(t.Context())
	})
	genericBridge := nativeProcessBridge{process: &residualNativeProcess{revoke: func(context.Context) error { return want }}}
	if err := genericBridge.Revoke(t.Context()); !errors.Is(err, want) {
		t.Fatalf("generic revoke = %v", err)
	}
}

func TestManagedSnapshotStateResidualBranches(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	if err := session.withReclaimedManagedState(t.Context(), session.client, func(string) error { return nil }); err == nil {
		t.Fatal("ordinary client state reclaim succeeded")
	}
	if _, _, err := session.beginManagedSnapshotStateHeld(t.Context(), session.client); err == nil {
		t.Fatal("ordinary client snapshot begin succeeded")
	}

	wantClose := errors.New("managed close refused")
	failing := &managedHermesServer{
		Server: &fakeHermesClient{closeErr: wantClose}, managed: true, root: t.TempDir(),
	}
	if _, _, err := session.beginManagedSnapshotStateHeld(t.Context(), failing); !errors.Is(err, wantClose) {
		t.Fatalf("managed snapshot close failure = %v", err)
	}
	fake := newFakeHermesClient()
	fake.closeErr = wantClose
	failing.Server = fake
	session.client = failing
	if _, err := session.captureSnapshotLocked(t.Context(), nil); !errors.Is(err, wantClose) {
		t.Fatalf("managed snapshot capture close failure = %v", err)
	}
}

func TestCompleteManagedSnapshotCommitResidualBranches(t *testing.T) {
	newCommit := func(t *testing.T) (*session, *sessionStoreCommit) {
		t.Helper()
		root := t.TempDir()
		managed := &managedHermesServer{
			Server: newFakeHermesClient(), managed: true, settled: true, root: root,
		}
		session := testSession(newTestAgent(), newFakeHermesClient())
		commit := &sessionStoreCommit{
			managed: managed, managedRoot: root,
			managedMain:  &stateSnapshot{Archives: map[string]archiveInfo{}},
			managedState: SessionKey{SessionID: "session", Subpath: stateDBSubpath},
			mainKey:      SessionKey{SessionID: "session", Subpath: SessionStoreMainSubpath},
		}

		return session, commit
	}

	t.Run("archive entry encoding", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session, commit := newCommit(t)
		if err := os.WriteFile(filepath.Join(commit.managedRoot, fileStateDB), []byte("sqlite placeholder"), 0o600); err != nil {
			t.Fatal(err)
		}
		stateSQLiteArchiveContent = func(string, string) ([]byte, bool, error) {
			return []byte("scrubbed sqlite"), true, nil
		}
		want := errors.New("archive entry marshal refused")
		stateJSONMarshal = func(any) ([]byte, error) { return nil, want }
		if err := session.completeManagedSnapshotCommit(commit); !errors.Is(err, want) {
			t.Fatalf("archive entry encoding = %v", err)
		}
	})

	t.Run("main encoding", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session, commit := newCommit(t)
		want := errors.New("main marshal refused")
		stateJSONMarshal = func(any) ([]byte, error) { return nil, want }
		if err := session.completeManagedSnapshotCommit(commit); !errors.Is(err, want) || commit.managedReady != nil {
			t.Fatalf("main encoding = %v, ready=%#v", err, commit.managedReady)
		}
	})

	t.Run("finish reclaim", func(t *testing.T) {
		session, commit := newCommit(t)
		want := errors.New("finish refused")
		commit.managed.closed = true
		commit.managed.closeErr = want
		if err := session.completeManagedSnapshotCommit(commit); !errors.Is(err, want) {
			t.Fatalf("finish reclaim = %v", err)
		}
	})
}

func TestSnapshotCaptureCancellationResidualBranches(t *testing.T) {
	t.Run("after id map", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session := snapshotFaultSession(t)
		ctx, cancel := context.WithCancel(t.Context())
		stateJSONMarshal = func(value any) ([]byte, error) {
			encoded, err := json.Marshal(value)
			cancel()

			return encoded, err
		}
		if _, err := session.captureSnapshotLocked(ctx, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation after id map = %v", err)
		}
	})

	t.Run("after managed completion", func(t *testing.T) {
		restoreStateStoreSeams(t)
		fake := newFakeHermesClient()
		root := t.TempDir()
		xdg, err := testGenerationXDG(root)
		if err != nil {
			t.Fatal(err)
		}
		fake.xdg = xdg
		managed := &managedHermesServer{Server: fake, managed: true, root: root}
		session := testSession(newTestAgent(), fake)
		session.client = managed
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0
		stateJSONMarshal = func(value any) ([]byte, error) {
			calls++
			encoded, marshalErr := json.Marshal(value)
			if calls == 2 {
				cancel()
			}

			return encoded, marshalErr
		}
		commit, err := session.captureSnapshotLocked(ctx, nil)
		if !errors.Is(err, context.Canceled) || commit == nil || calls != 2 {
			t.Fatalf("managed completion cancellation = commit:%v calls:%d err:%v", commit != nil, calls, err)
		}
	})
}

func TestAgentAndSessionLifecycleAdmissionResidualBranches(t *testing.T) {
	closed := newTestAgent()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := closed.beginActiveReuse(t.Context(), "session"); err == nil {
		t.Fatal("closed agent admitted active reuse")
	}
	if _, _, err := closed.acquireSessionLifecycle(t.Context(), "session"); err == nil {
		t.Fatal("closed agent admitted session lifecycle")
	}
	if _, err := closed.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "session"}); err == nil {
		t.Fatal("closed agent admitted session close")
	}

	agent := newTestAgent()
	session := testSession(agent, newFakeHermesClient())
	agent.sessions[session.id] = session
	if err := agent.cleanupFailedStartedSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if agent.activeSession(session.id) != nil {
		t.Fatal("failed started session remained active")
	}
}

func TestSessionCleanupRetryEntryResidualBranches(t *testing.T) {
	newAgentWithBadCleanup := func() *Agent {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		agent.deleteCleanup["bad"] = deleteCleanupRecord{SessionID: "bad", XDGRoot: string([]byte{0})}

		return agent
	}
	loadAgent := newAgentWithBadCleanup()
	if _, err := loadAgent.LoadSession(t.Context(), LoadSessionRequest("missing", t.TempDir())); err == nil {
		t.Fatal("missing load unexpectedly succeeded")
	}
	deleteAgent := newAgentWithBadCleanup()
	if _, err := deleteAgent.UnstableDeleteSession(t.Context(), DeleteSessionRequest("missing")); err != nil {
		t.Fatalf("delete after cleanup retry = %v", err)
	}
}

func TestRuntimeResumeEarlyResidualBranches(t *testing.T) {
	t.Run("managed finish", func(t *testing.T) {
		want := errors.New("managed finish refused")
		managed := &managedHermesServer{managed: true, closed: true, closeErr: want}
		session := testSession(newTestAgent(), newFakeHermesClient())
		session.client = managed
		session.runtimeNeedsResume = true
		if err := session.resumeRuntimeForTurnLocked(t.Context()); !errors.Is(err, want) {
			t.Fatalf("managed finish = %v", err)
		}
	})

	t.Run("generation creation", func(t *testing.T) {
		originalCreate := createHermesGeneration
		t.Cleanup(func() { createHermesGeneration = originalCreate })
		want := errors.New("generation refused")
		createHermesGeneration = func(string) (nativehermes.XDGDirs, error) {
			return nativehermes.XDGDirs{}, want
		}
		session := testSession(newTestAgent(WithScratchDir(t.TempDir())), newFakeHermesClient())
		session.runtimeNeedsResume = true
		if err := session.resumeRuntimeForTurnLocked(t.Context()); !errors.Is(err, want) {
			t.Fatalf("resume generation creation = %v", err)
		}
	})
}

func TestActiveRebindAndComparisonResidualBranches(t *testing.T) {
	want := errors.New("close refused")
	agent := newTestAgent()
	failingClient := newFakeHermesClient()
	failingClient.closeErr = want
	failing := testSession(agent, failingClient)
	if err := agent.closeActiveSessionForRebind(t.Context(), failing.id, failing, func() {}); !errors.Is(err, want) {
		t.Fatalf("failed active rebind close = %v", err)
	}

	missing := testSession(agent, newFakeHermesClient())
	if err := agent.closeActiveSessionForRebind(t.Context(), missing.id, missing, func() {}); err == nil {
		t.Fatal("missing active rebind close succeeded")
	}
	if stringMapsEqual(map[string]string{"left": "one"}, map[string]string{}) {
		t.Fatal("different-length maps compared equal")
	}
}

func TestNewHermesClientResidualBranches(t *testing.T) {
	t.Run("incomplete residence", func(t *testing.T) {
		agent := newTestAgent()
		agent.retainIncompleteHermesRoot("session", t.TempDir())
		if _, err := agent.newHermesClient(t.Context(), "session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{}); !errors.Is(err, ErrContainmentIncomplete) {
			t.Fatalf("incomplete residence = %v", err)
		}
	})

	t.Run("generation creation", func(t *testing.T) {
		originalCreate := createHermesGeneration
		t.Cleanup(func() { createHermesGeneration = originalCreate })
		want := errors.New("generation refused")
		createHermesGeneration = func(string) (nativehermes.XDGDirs, error) {
			return nativehermes.XDGDirs{}, want
		}
		agent := newTestAgent(WithScratchDir(t.TempDir()))
		if _, err := agent.newHermesClient(t.Context(), "session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{}); !errors.Is(err, want) {
			t.Fatalf("client generation creation = %v", err)
		}
	})

	t.Run("scratch parent", func(t *testing.T) {
		blocked := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(blocked, []byte("blocked"), 0o600); err != nil {
			t.Fatal(err)
		}
		agent := newTestAgent(WithScratchDir(blocked))
		if _, err := agent.newHermesClientWithScratch(
			t.Context(), "session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{}, func() {},
		); err == nil {
			t.Fatal("client accepted unusable scratch parent")
		}
	})

	t.Run("retention bridge", func(t *testing.T) {
		authority := &residualAuthority{}
		agent := newTestAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
		firstRoot := filepath.Join(t.TempDir(), "incomplete")
		secondRoot := filepath.Join(t.TempDir(), "busy")
		agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			if !start.RetainNativeTree(firstRoot, ErrContainmentIncomplete) {
				t.Fatal("containment-incomplete root was not retained")
			}
			if !start.RetainNativeTree(secondRoot, ErrNativeTreeBusy) {
				t.Fatal("busy root was not retained")
			}

			return newFakeHermesClient(), nil
		}
		client, err := agent.newHermesClientWithScratch(
			t.Context(), "session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{Root: t.TempDir()}, func() {},
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

type residualValueAuthority struct{}

func (residualValueAuthority) NativeEnvironment() map[string]string {
	return map[string]string{"PATH": "/bin"}
}
func (residualValueAuthority) PrepareNativeTree(context.Context, string) error { return nil }
func (residualValueAuthority) ReclaimNativeTree(context.Context, string) error { return nil }
func (residualValueAuthority) StartNative(context.Context, NativeRequest) (NativeProcess, error) {
	return residualValueNativeProcess{}, nil
}

type residualValueNativeProcess struct{}

func (residualValueNativeProcess) Stdin() io.WriteCloser { return nopWriteCloser{Writer: io.Discard} }
func (residualValueNativeProcess) Stdout() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }
func (residualValueNativeProcess) Stderr() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }
func (residualValueNativeProcess) Wait(context.Context) (NativeResult, error) {
	return NativeResult{}, nil
}
func (residualValueNativeProcess) Revoke(context.Context) error { return nil }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func nilResidualNativeProcess() NativeProcess {
	var process *residualNativeProcess

	return process
}

type residualAuthority struct {
	environment func() map[string]string
	prepare     func(context.Context, string) error
	reclaim     func(context.Context, string) error
	start       func(context.Context, NativeRequest) (NativeProcess, error)
}

func (a *residualAuthority) NativeEnvironment() map[string]string {
	if a.environment != nil {
		return a.environment()
	}

	return map[string]string{"PATH": "/bin"}
}
func (a *residualAuthority) PrepareNativeTree(ctx context.Context, root string) error {
	if a.prepare != nil {
		return a.prepare(ctx, root)
	}

	return nil
}
func (a *residualAuthority) ReclaimNativeTree(ctx context.Context, root string) error {
	if a.reclaim != nil {
		return a.reclaim(ctx, root)
	}

	return nil
}
func (a *residualAuthority) StartNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if a.start != nil {
		return a.start(ctx, request)
	}

	return residualValueNativeProcess{}, nil
}

type residualNativeProcess struct {
	stdin  func() io.WriteCloser
	stdout func() io.ReadCloser
	stderr func() io.ReadCloser
	wait   func(context.Context) (NativeResult, error)
	revoke func(context.Context) error
}

func (p *residualNativeProcess) Stdin() io.WriteCloser {
	if p.stdin != nil {
		return p.stdin()
	}

	return nopWriteCloser{Writer: io.Discard}
}
func (p *residualNativeProcess) Stdout() io.ReadCloser {
	if p.stdout != nil {
		return p.stdout()
	}

	return io.NopCloser(strings.NewReader(""))
}
func (p *residualNativeProcess) Stderr() io.ReadCloser {
	if p.stderr != nil {
		return p.stderr()
	}

	return io.NopCloser(strings.NewReader(""))
}
func (p *residualNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	if p.wait != nil {
		return p.wait(ctx)
	}

	return NativeResult{}, nil
}
func (p *residualNativeProcess) Revoke(ctx context.Context) error {
	if p.revoke != nil {
		return p.revoke(ctx)
	}

	return nil
}
