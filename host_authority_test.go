package hermesacp

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

type recordingHostAuthority struct {
	mu          sync.Mutex
	environment map[string]string
	events      []string
	process     NativeProcess
	prepareErr  error
	reclaimErr  error
	startErr    error
	start       func(NativeRequest) (NativeProcess, error)
	prepared    map[string]string
	moveTrees   bool
	violation   error
	reclaimHook func(string) error
}

func (a *recordingHostAuthority) record(event string) {
	a.mu.Lock()
	a.events = append(a.events, event)
	a.mu.Unlock()
}

func (a *recordingHostAuthority) NativeEnvironment() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, "environment")

	return cloneStringMap(a.environment)
}

func (a *recordingHostAuthority) PrepareNativeTree(_ context.Context, root string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, "prepare:"+root)
	if a.moveTrees {
		moved := root + ".native"
		if err := os.Rename(root, moved); err != nil {
			return err
		}
		a.prepared[root] = moved
	}

	return a.prepareErr
}

func (*recordingHostAuthority) WriteNativeAppendLog(context.Context, string, [][]byte) error {
	return ErrHostAuthorityUnavailable
}

func (*recordingHostAuthority) ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error) {
	return nil, nil
}

func (a *recordingHostAuthority) ReclaimNativeTree(_ context.Context, root string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, "reclaim:"+root)
	if a.reclaimHook != nil {
		if err := a.reclaimHook(root); err != nil {
			return err
		}
	}
	if a.moveTrees {
		moved, ok := a.prepared[root]
		if !ok {
			a.violation = errors.New("reclaimed an unprepared tree")

			return a.violation
		}
		if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
			a.violation = errors.New("adapter recreated a prepared tree before reclaim")

			return a.violation
		}
		if _, err := os.Stat(moved); err != nil {
			a.violation = errors.New("prepared tree was unavailable at reclaim")

			return a.violation
		}
		if err := os.Rename(moved, root); err != nil {
			return err
		}
		delete(a.prepared, root)
	}

	return a.reclaimErr
}

func (a *recordingHostAuthority) StartNative(_ context.Context, request NativeRequest) (NativeProcess, error) {
	a.mu.Lock()
	a.events = append(a.events, "start:"+request.Executable+" "+strings.Join(request.Arguments, " "))
	start := a.start
	process := a.process
	err := a.startErr
	a.mu.Unlock()
	if start != nil {
		return start(request)
	}

	return process, err
}

type recordingNativeProcess struct {
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	result    NativeResult
	waitErr   error
	revokeErr error
	name      string
	record    func(string)
}

func (p *recordingNativeProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *recordingNativeProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *recordingNativeProcess) Stderr() io.ReadCloser { return p.stderr }
func (p *recordingNativeProcess) Wait(context.Context) (NativeResult, error) {
	if p.record != nil {
		p.record("wait:" + p.name)
	}

	return p.result, p.waitErr
}
func (p *recordingNativeProcess) Revoke(context.Context) error {
	if p.record != nil {
		p.record("revoke:" + p.name)
	}

	return p.revokeErr
}

type recordingWriteCloser struct{}

func (recordingWriteCloser) Write(data []byte) (int, error) { return len(data), nil }
func (recordingWriteCloser) Close() error                   { return nil }

type panickingStdioProcess struct{ *recordingNativeProcess }

func (*panickingStdioProcess) Stdin() io.WriteCloser { panic("stdio unavailable") }

func newTestHostAuthority() *recordingHostAuthority {
	authority := &recordingHostAuthority{
		environment: map[string]string{"PATH": "/usr/bin"},
		prepared:    make(map[string]string),
	}
	authority.start = func(request NativeRequest) (NativeProcess, error) {
		name := "serve"
		stdout := io.NopCloser(strings.NewReader(""))
		if len(request.Arguments) == 1 && request.Arguments[0] == "--version" {
			name = "version"
			stdout = io.NopCloser(strings.NewReader("Hermes 0.20.0\n"))
		}

		process := &recordingNativeProcess{
			stdin: recordingWriteCloser{}, stdout: stdout, stderr: io.NopCloser(strings.NewReader("")),
			name: name, record: authority.record,
		}
		if name == "serve" {
			process.stdin = nil
		}

		return process, nil
	}

	return authority
}

type managedSnapshotTestServer struct {
	*fakeHermesClient
}

func (s *managedSnapshotTestServer) GetSession(_ context.Context, id string) (nativehermes.Session, error) {
	return testNativeSession(id), s.getErr
}

func newManagedSnapshotTestAgent(
	t *testing.T,
	authority *recordingHostAuthority,
) (*Agent, *InMemorySessionStore, *[]*managedSnapshotTestServer) {
	t.Helper()

	store := NewInMemorySessionStore()
	servers := &[]*managedSnapshotTestServer{}
	agent := NewAgent(
		WithHostAuthority(authority),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		func(options *Options) {
			options.clientFactory = func(ctx context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
				stateDB := filepath.Join(start.ExistingXDG.Root, "state.db")
				if err := os.WriteFile(stateDB, []byte("managed snapshot state"), 0o600); err != nil {
					return nil, err
				}
				if err := start.PrepareNativeTree(ctx, start.ExistingXDG.Root); err != nil {
					return nil, err
				}

				client := newFakeHermesClient()
				client.xdg = start.ExistingXDG
				client.createSession = testNativeSession("native-managed")
				client.forkSession = testNativeSession("native-managed-child")
				client.closeFunc = func(closeCtx context.Context) error {
					return start.ReclaimNativeTree(closeCtx, start.ExistingXDG.Root)
				}
				server := &managedSnapshotTestServer{fakeHermesClient: client}
				*servers = append(*servers, server)

				return server, nil
			}
		},
	)
	require.NoError(t, agent.optionsErr)

	return agent, store, servers
}

func requireManagedSnapshotArchived(t *testing.T, store SessionStore, id acp.SessionId) {
	t.Helper()

	entries, err := store.Load(t.Context(), SessionKey{SessionID: string(id), Subpath: stateDBSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

func TestManagedInitialSnapshotReadsOnlyAfterReclaim(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	agent, store, _ := newManagedSnapshotTestAgent(t, authority)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	requireManagedSnapshotArchived(t, store, created.SessionId)
	require.NoError(t, authority.violation)
	require.Empty(t, authority.prepared)
	require.GreaterOrEqual(t, eventIndex(authority.events, "reclaim:"), 0)
}

func TestManagedSuccessfulTurnSnapshotReadsOnlyAfterReclaim(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	agent, store, servers := newManagedSnapshotTestAgent(t, authority)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	response, err := agent.Prompt(t.Context(), TextPromptRequest(created.SessionId, "managed-turn", "continue"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.GreaterOrEqual(t, len(*servers), 2)
	requireManagedSnapshotArchived(t, store, created.SessionId)
	require.NoError(t, authority.violation)
	require.Empty(t, authority.prepared)
}

func TestManagedCloseSnapshotReadsOnlyAfterReclaim(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	agent, store, _ := newManagedSnapshotTestAgent(t, authority)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	active := agent.activeSession(created.SessionId)
	require.NotNil(t, active)
	active.toolMu.Lock()
	active.cancelMu.Lock()
	err = active.resumeRuntimeForTurnLocked(t.Context())
	active.cancelMu.Unlock()
	active.toolMu.Unlock()
	require.NoError(t, err)
	require.NotEmpty(t, authority.prepared)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	requireManagedSnapshotArchived(t, store, created.SessionId)
	require.NoError(t, authority.violation)
	require.Empty(t, authority.prepared)
}

func TestManagedCloseRetriesFilesystemCaptureFromReclaimedResidence(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	agent, _, _ := newManagedSnapshotTestAgent(t, authority)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	active := agent.activeSession(created.SessionId)
	active.toolMu.Lock()
	active.cancelMu.Lock()
	err = active.resumeRuntimeForTurnLocked(t.Context())
	active.cancelMu.Unlock()
	active.toolMu.Unlock()
	require.NoError(t, err)
	root := hermesServerRoot(active.client)

	originalLstat := stateLstat
	want := errors.New("capture unavailable")
	stateLstat = func(string) (os.FileInfo, error) { return nil, want }
	t.Cleanup(func() { stateLstat = originalLstat })
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.ErrorIs(t, err, want)
	require.NotNil(t, agent.activeSession(created.SessionId))
	require.Empty(t, authority.prepared)
	_, statErr := os.Stat(root)
	require.NoError(t, statErr)

	stateLstat = originalLstat
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	require.Nil(t, agent.activeSession(created.SessionId))
	_, statErr = os.Stat(root)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestManagedForkSnapshotReadsOnlyAfterReclaim(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	agent, store, _ := newManagedSnapshotTestAgent(t, authority)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	forked, err := agent.forkSession(t.Context(), ForkSessionRequest(created.SessionId, t.TempDir()))
	require.NoError(t, err)
	requireManagedSnapshotArchived(t, store, forked.SessionId)
	require.NoError(t, authority.violation)
	require.Empty(t, authority.prepared)
}

func TestManagedForcedRevokeRetainsPriorCompleteSnapshot(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	agent, store, servers := newManagedSnapshotTestAgent(t, authority)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	before, err := store.Load(t.Context(), SessionKey{SessionID: string(created.SessionId), Subpath: stateDBSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, before)

	active := agent.activeSession(created.SessionId)
	active.toolMu.Lock()
	active.cancelMu.Lock()
	err = active.resumeRuntimeForTurnLocked(t.Context())
	active.cancelMu.Unlock()
	active.toolMu.Unlock()
	require.NoError(t, err)

	started := make(chan struct{})
	latest := (*servers)[len(*servers)-1]
	latest.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	promptCtx, cancelPrompt := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, promptErr := agent.Prompt(promptCtx, TextPromptRequest(created.SessionId, "managed-cancel", "cancel"))
		done <- promptErr
	}()
	<-started
	cancelPrompt()
	<-done

	after, err := store.Load(t.Context(), SessionKey{SessionID: string(created.SessionId), Subpath: stateDBSubpath})
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, authority.violation)
	require.Empty(t, authority.prepared)
}

func TestManagedDeleteBusyRetainsCleanupForPublicRetry(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	agent, _, _ := newManagedSnapshotTestAgent(t, authority)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	active := agent.activeSession(created.SessionId)
	active.toolMu.Lock()
	active.cancelMu.Lock()
	err = active.resumeRuntimeForTurnLocked(t.Context())
	active.cancelMu.Unlock()
	active.toolMu.Unlock()
	require.NoError(t, err)

	busy := true
	authority.reclaimHook = func(string) error {
		if busy {
			return ErrNativeTreeBusy
		}

		return nil
	}
	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(created.SessionId))
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	require.NotEmpty(t, authority.prepared)
	require.Contains(t, agent.deleteCleanup, created.SessionId)
	require.NoError(t, agent.containmentErr)

	busy = false
	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(created.SessionId))
	require.NoError(t, err)
	require.Empty(t, authority.prepared)
	require.NotContains(t, agent.deleteCleanup, created.SessionId)
}

func TestManagedTerminalProbeBusyRetriesAfterLiveServerSettles(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	root := filepath.Join(t.TempDir(), "terminal-probe")
	require.NoError(t, os.MkdirAll(root, 0o700))
	require.NoError(t, authority.PrepareNativeTree(t.Context(), root))

	liveServer := true
	authority.reclaimHook = func(string) error {
		if liveServer {
			return ErrNativeTreeBusy
		}

		return nil
	}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	require.True(t, agent.retainNativeTree(root, ErrNativeTreeBusy))
	require.ErrorIs(t, agent.retryRetiredNativeRoots(t.Context()), ErrNativeTreeBusy)
	require.Contains(t, agent.retiredNativeRoots, root)
	require.NoError(t, agent.hostAuthorityAdmissionError())

	liveServer = false
	require.NoError(t, agent.retryRetiredNativeRoots(t.Context()))
	require.NotContains(t, agent.retiredNativeRoots, root)
	_, err := os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestManagedNativeGenerationRefusesRetainedBusyTree(t *testing.T) {
	authority := newTestHostAuthority()
	root := filepath.Join(t.TempDir(), "retained")
	require.NoError(t, os.MkdirAll(root, 0o700))

	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	require.True(t, agent.retainNativeTree(root, ErrNativeTreeBusy))
	authority.reclaimHook = func(string) error { return ErrNativeTreeBusy }

	finish, err := agent.beginManagedNativeGeneration(t.Context())
	require.Nil(t, finish)
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	require.Contains(t, agent.retiredNativeRoots, root)

	authority.reclaimHook = nil
	finish, err = agent.beginManagedNativeGeneration(t.Context())
	require.NoError(t, err)
	require.NotNil(t, finish)
	finish()
	require.NotContains(t, agent.retiredNativeRoots, root)
}

func TestRetiredCleanupOnlyTreeDoesNotReclaimAgain(t *testing.T) {
	authority := newTestHostAuthority()
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	root := filepath.Join(t.TempDir(), "cleanup-only")
	require.NoError(t, os.MkdirAll(root, 0o700))
	require.True(t, agent.retainNativeTree(root, nil))

	want := errors.New("remove failed")
	previousRemoveAll := retiredNativeRemoveAll
	retiredNativeRemoveAll = func(string) error { return want }
	t.Cleanup(func() { retiredNativeRemoveAll = previousRemoveAll })

	require.ErrorIs(t, agent.retryRetiredNativeRoots(t.Context()), want)
	require.NotContains(t, authority.events, "reclaim:"+root)
	require.Contains(t, agent.retiredNativeRoots, root)

	retiredNativeRemoveAll = previousRemoveAll
	require.NoError(t, agent.retryRetiredNativeRoots(t.Context()))
	require.NotContains(t, authority.events, "reclaim:"+root)
	require.NotContains(t, agent.retiredNativeRoots, root)
}

func TestFailedGenerationCleanupPreservesOnlyExactIncompleteRoots(t *testing.T) {
	agent := NewAgent(WithScratchDir(t.TempDir()))
	id := acp.SessionId("failed-generation")
	probeRoot := filepath.Join(t.TempDir(), "probe")
	sessionRoot := filepath.Join(t.TempDir(), "session")
	agent.retainIncompleteHermesRoot(id, probeRoot)

	require.True(t, agent.retainFailedHermesGeneration(id, probeRoot, ErrContainmentIncomplete))
	require.False(t, agent.retainFailedHermesGeneration(id, sessionRoot, ErrContainmentIncomplete))
	require.NotContains(t, agent.incompleteRoots[id], sessionRoot)
}

func TestSuppliedNilHostAuthorityFailsBeforeSessionMutation(t *testing.T) {
	for _, authority := range []HostAuthority{nil, (*recordingHostAuthority)(nil)} {
		agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
		if !errors.Is(agent.optionsErr, ErrHostAuthorityUnavailable) {
			t.Fatalf("options error = %v", agent.optionsErr)
		}
	}
}

func startWithRecordingAuthority(
	t *testing.T,
	authority *recordingHostAuthority,
	authorityOption Option,
	executable string,
) error {
	t.Helper()

	scratch := t.TempDir()
	agent := NewAgent(authorityOption, WithScratchDir(scratch))
	if agent.optionsErr != nil {
		t.Fatalf("construct managed agent: %v", agent.optionsErr)
	}
	options := nativehermes.StartOptions{
		ACPSessionID:   "authority-test",
		ScratchParent:  scratch,
		Cwd:            scratch,
		ExecutablePath: executable,
	}
	agent.configureHostAuthority(&options)
	_, err := nativehermes.StartServer(t.Context(), options)

	return err
}

func eventIndex(events []string, prefix string) int {
	for index, event := range events {
		if strings.HasPrefix(event, prefix) {
			return index
		}
	}

	return -1
}

func TestHostAuthorityManagedLaunchTrace(t *testing.T) {
	authority := newTestHostAuthority()
	if err := startWithRecordingAuthority(t, authority, WithHostAuthority(authority), "logical-hermes"); err == nil {
		t.Fatal("unusable managed serve process was accepted")
	}

	authority.mu.Lock()
	events := append([]string(nil), authority.events...)
	authority.mu.Unlock()
	versionStart := eventIndex(events, "start:logical-hermes --version")
	serveStart := eventIndex(events, "start:logical-hermes serve ")
	versionWait := eventIndex(events, "wait:version")
	serveRevoke := eventIndex(events, "revoke:serve")
	serveWait := eventIndex(events, "wait:serve")
	for _, token := range []string{"prepare:", "start:", "wait:", "reclaim:"} {
		if eventIndex(events, token) < 0 {
			t.Fatalf("managed launch trace lacks %q: %v", token, events)
		}
	}
	if versionStart < 0 || serveStart < 0 || versionWait < versionStart || serveStart < versionWait ||
		serveRevoke < serveStart || serveWait < serveRevoke {
		t.Fatalf("managed launch trace = %v", events)
	}
	if len(authority.prepared) != 0 {
		t.Fatalf("prepared trees remained after failed launch: %v", authority.prepared)
	}
}

func TestHostAuthorityPreparedTreeExclusivity(t *testing.T) {
	authority := newTestHostAuthority()
	authority.moveTrees = true
	if err := startWithRecordingAuthority(t, authority, WithHostAuthority(authority), "exclusive-hermes"); err == nil {
		t.Fatal("unusable managed serve process was accepted")
	}
	if authority.violation != nil {
		t.Fatal(authority.violation)
	}
	if len(authority.prepared) != 0 {
		t.Fatalf("trees were not reclaimed: %v", authority.prepared)
	}
	for _, event := range authority.events {
		root, ok := strings.CutPrefix(event, "prepare:")
		if !ok {
			continue
		}
		if _, err := os.Stat(root + ".native"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("authority-held tree remained after reclaim: %v", err)
		}
	}
}

type reclaimingTestServer struct {
	nativehermes.Server
	root      string
	authority HostAuthority
}

func (s reclaimingTestServer) Close(ctx context.Context) error {
	return s.authority.ReclaimNativeTree(ctx, s.root)
}

func (s reclaimingTestServer) XDGDirs() nativehermes.XDGDirs {
	return nativehermes.XDGDirs{Root: s.root}
}

func TestHostAuthorityReclaimPrecedesRemoval(t *testing.T) {
	root := filepath.Join(t.TempDir(), "native-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	authority := newTestHostAuthority()
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	if agent.optionsErr != nil {
		t.Fatal(agent.optionsErr)
	}
	authority.reclaimHook = func(path string) error {
		if _, err := os.Stat(path); err != nil {
			return errors.New("root was removed before reclaim")
		}

		return nil
	}
	server := &managedHermesServer{
		Server: reclaimingTestServer{root: root, authority: authority},
		root:   root, sessionID: acp.SessionId("session"),
	}
	if err := server.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed root still exists after close: %v", err)
	}
	if index := eventIndex(authority.events, "reclaim:"+root); index < 0 {
		t.Fatalf("root was removed without reclaim: %v", authority.events)
	}
}

func TestHostAuthorityNoOrdinaryFallback(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "direct-exec-marker")
	executable := filepath.Join(directory, "hermes")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\ntouch \""+marker+"\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	authority := newTestHostAuthority()
	authority.moveTrees = true
	markerErr := errors.New("authority refused launch")
	authority.start = func(NativeRequest) (NativeProcess, error) {
		return nil, markerErr
	}
	err := startWithRecordingAuthority(t, authority, WithHostAuthority(authority), executable)
	require.ErrorIs(t, err, markerErr)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ordinary fallback executed managed selector: %v", err)
	}
	if eventIndex(authority.events, "start:"+executable+" --version") < 0 {
		t.Fatalf("authority did not receive managed probe: %v", authority.events)
	}
	require.Empty(t, authority.prepared)
	for _, event := range authority.events {
		if root, ok := strings.CutPrefix(event, "prepare:"); ok {
			require.Greater(t, eventIndex(authority.events, "reclaim:"+root), eventIndex(authority.events, event))
			_, statErr := os.Stat(root)
			require.ErrorIs(t, statErr, os.ErrNotExist, "refused managed start must remove reclaimed tree")
		}
	}
}

func TestHostAuthorityStartErrorPreservesAuthorityVerdict(t *testing.T) {
	tests := []struct {
		name            string
		want            error
		wantUnavailable bool
		wantIncomplete  bool
	}{
		{name: "ordinary refusal", want: errors.New("admission refused")},
		{name: "authority unavailable", want: ErrHostAuthorityUnavailable, wantUnavailable: true},
		{name: "containment incomplete", want: ErrContainmentIncomplete, wantIncomplete: true},
		{
			name: "unavailable and incomplete", want: errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete),
			wantUnavailable: true, wantIncomplete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authority := newTestHostAuthority()
			authority.start = func(NativeRequest) (NativeProcess, error) { return nil, tt.want }
			agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
			options := nativehermes.StartOptions{}
			agent.configureHostAuthority(&options)

			_, err := options.StartNative(t.Context(), nativehermes.NativeRequest{Executable: "hermes"})
			require.Equal(t, tt.want, err)
			require.Equal(t, tt.wantUnavailable, errors.Is(err, ErrHostAuthorityUnavailable))
			require.Equal(t, tt.wantIncomplete, errors.Is(err, ErrContainmentIncomplete))
			require.Equal(t, tt.wantUnavailable, errors.Is(agent.hostAuthorityAdmissionError(), ErrHostAuthorityUnavailable))
			require.Equal(t, tt.wantIncomplete, errors.Is(agent.containmentErr, ErrContainmentIncomplete))
		})
	}
}

func TestHostAuthorityLossStopsNativeAdmission(t *testing.T) {
	authority := newTestHostAuthority()
	authority.start = func(NativeRequest) (NativeProcess, error) {
		return nil, ErrHostAuthorityUnavailable
	}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	options := nativehermes.StartOptions{}
	agent.configureHostAuthority(&options)

	request := nativehermes.NativeRequest{Executable: "hermes"}
	if _, err := options.StartNative(t.Context(), request); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("first launch = %v", err)
	}
	require.NoError(t, agent.containmentErr)
	authority.start = func(NativeRequest) (NativeProcess, error) {
		t.Fatal("authority was called after terminal loss")

		return nil, ErrHostAuthorityUnavailable
	}
	if _, err := options.StartNative(t.Context(), request); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("later launch = %v", err)
	}
}

func TestHostAuthorityWaitFailureMapsAndLatchesContainment(t *testing.T) {
	want := errors.New("authority wait uncertain")
	authority := newTestHostAuthority()
	authority.process = &recordingNativeProcess{
		stdin: recordingWriteCloser{}, stdout: io.NopCloser(strings.NewReader("")),
		stderr: io.NopCloser(strings.NewReader("")), waitErr: want,
	}
	authority.start = nil
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	options := nativehermes.StartOptions{}
	agent.configureHostAuthority(&options)
	process, err := options.StartNative(t.Context(), nativehermes.NativeRequest{Executable: "hermes"})
	require.NoError(t, err)
	_, err = process.Wait(t.Context())
	require.ErrorIs(t, err, want)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.ErrorIs(t, agent.containmentErr, ErrContainmentIncomplete)
}

func TestHostAuthorityContainmentFailureStopsManagedAdmission(t *testing.T) {
	tests := []struct {
		name string
		fail func(context.Context, nativehermes.NativeProcess) error
	}{
		{
			name: "wait",
			fail: func(ctx context.Context, process nativehermes.NativeProcess) error {
				_, err := process.Wait(ctx)

				return err
			},
		},
		{
			name: "revoke",
			fail: func(ctx context.Context, process nativehermes.NativeProcess) error {
				return process.Revoke(ctx)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := errors.Join(errors.New("containment uncertain"), ErrContainmentIncomplete)
			authority := newTestHostAuthority()
			authority.process = &recordingNativeProcess{
				stdin: recordingWriteCloser{}, stdout: io.NopCloser(strings.NewReader("")),
				stderr: io.NopCloser(strings.NewReader("")), name: test.name, record: authority.record,
				waitErr: failure, revokeErr: failure,
			}
			authority.start = nil
			factoryCalls := 0
			agent := NewAgent(
				WithHostAuthority(authority),
				WithScratchDir(t.TempDir()),
				func(options *Options) {
					options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
						factoryCalls++

						return nil, errors.New("unexpected managed launch")
					}
				},
			)
			require.NoError(t, agent.optionsErr)

			start := nativehermes.StartOptions{}
			agent.configureHostAuthority(&start)
			process, err := start.StartNative(t.Context(), nativehermes.NativeRequest{Executable: "hermes"})
			require.NoError(t, err)
			err = test.fail(t.Context(), process)
			require.ErrorIs(t, err, ErrContainmentIncomplete)
			require.NotErrorIs(t, err, ErrHostAuthorityUnavailable)
			require.ErrorIs(t, agent.hostAuthorityAdmissionError(), ErrContainmentIncomplete)
			require.NotErrorIs(t, agent.hostAuthorityAdmissionError(), ErrHostAuthorityUnavailable)
			require.NoError(t, agent.authorityErr)

			affectedID := acp.SessionId("affected-session")
			retainedRoot := filepath.Join(t.TempDir(), "retained")
			agent.recordIncompleteContainment(err, affectedID, retainedRoot)
			eventsBeforeRefusal := append([]string(nil), authority.events...)

			client, launchErr := agent.newHermesClientWithScratch(
				t.Context(), "different-session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{}, func() {},
			)
			require.Nil(t, client)
			require.ErrorIs(t, launchErr, ErrContainmentIncomplete)
			require.NotErrorIs(t, launchErr, ErrHostAuthorityUnavailable)
			require.Equal(t, 0, factoryCalls)

			require.ErrorIs(t, start.PrepareNativeTree(t.Context(), t.TempDir()), ErrContainmentIncomplete)
			_, startErr := start.StartNative(t.Context(), nativehermes.NativeRequest{Executable: "hermes"})
			require.ErrorIs(t, startErr, ErrContainmentIncomplete)
			require.NotErrorIs(t, startErr, ErrHostAuthorityUnavailable)
			require.Equal(t, eventsBeforeRefusal, authority.events)
			require.Contains(t, agent.incompleteRoots[affectedID], retainedRoot)
		})
	}
}

func TestHostAuthorityRevokeLossFencesEveryActiveSession(t *testing.T) {
	authority := newTestHostAuthority()
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	firstClient := newFakeHermesClient()
	first := testSession(agent, firstClient)
	first.id = "authority-first"
	secondClient := newFakeHermesClient()
	second := testSession(agent, secondClient)
	second.id = "authority-second"
	require.NoError(t, first.openLifecycleStream())
	require.NoError(t, second.openLifecycleStream())
	agent.sessions[first.id] = first
	agent.sessions[second.id] = second

	bridge := nativeProcessBridge{
		process: &recordingNativeProcess{revokeErr: ErrHostAuthorityUnavailable},
		record:  agent.recordHostAuthorityError,
	}
	err := bridge.Revoke(t.Context())
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.ErrorIs(t, agent.hostAuthorityAdmissionError(), ErrHostAuthorityUnavailable)
	for _, current := range []*session{first, second} {
		current.mu.Lock()
		closing := current.lifecycleClosing
		current.mu.Unlock()
		require.True(t, closing)
		require.False(t, current.lifecycleStream().live())
	}
	require.Eventually(t, func() bool {
		return firstClient.closeCount() == 1 && secondClient.closeCount() == 1
	}, time.Second, time.Millisecond)
}

func TestHostAuthorityWaitCancellationDetachesWithoutContainment(t *testing.T) {
	authority := newTestHostAuthority()
	process := &recordingNativeProcess{
		stdin: recordingWriteCloser{}, stdout: io.NopCloser(strings.NewReader("")),
		stderr: io.NopCloser(strings.NewReader("")), waitErr: context.Canceled,
	}
	authority.process = process
	authority.start = nil
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	options := nativehermes.StartOptions{}
	agent.configureHostAuthority(&options)
	native, err := options.StartNative(t.Context(), nativehermes.NativeRequest{Executable: "hermes"})
	require.NoError(t, err)

	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	_, err = native.Wait(waitCtx)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	require.NoError(t, agent.containmentErr)

	process.waitErr = nil
	_, err = native.Wait(t.Context())
	require.NoError(t, err)
	require.NoError(t, agent.containmentErr)
}

func TestHostAuthorityRevokeCancellationDoesNotLatchContainment(t *testing.T) {
	authority := newTestHostAuthority()
	process := &recordingNativeProcess{revokeErr: context.Canceled}
	authority.process = process
	authority.start = nil
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	options := nativehermes.StartOptions{}
	agent.configureHostAuthority(&options)
	native, err := options.StartNative(t.Context(), nativehermes.NativeRequest{Executable: "hermes"})
	require.NoError(t, err)

	revokeCtx, cancelRevoke := context.WithCancel(t.Context())
	cancelRevoke()
	require.ErrorIs(t, native.Revoke(revokeCtx), context.Canceled)
	require.NoError(t, agent.containmentErr)
	require.NoError(t, agent.hostAuthorityAdmissionError())

	process.revokeErr = nil
	require.NoError(t, native.Revoke(t.Context()))
	require.NoError(t, agent.containmentErr)
}

func TestHostAuthorityStdioPanicFailsClosed(t *testing.T) {
	authority := newTestHostAuthority()
	authority.process = &panickingStdioProcess{recordingNativeProcess: &recordingNativeProcess{}}
	authority.start = nil
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	options := nativehermes.StartOptions{}
	agent.configureHostAuthority(&options)
	process, err := options.StartNative(t.Context(), nativehermes.NativeRequest{Executable: "hermes"})
	require.NoError(t, err)
	require.Nil(t, process.Stdin())
	require.ErrorIs(t, agent.hostAuthorityAdmissionError(), ErrHostAuthorityUnavailable)
	require.ErrorIs(t, agent.containmentErr, ErrContainmentIncomplete)
}

func TestHostAuthorityPrepareFailureRetainsAttemptedTree(t *testing.T) {
	want := errors.New("prepare uncertain")
	authority := newTestHostAuthority()
	authority.prepareErr = want
	err := startWithRecordingAuthority(t, authority, WithHostAuthority(authority), "logical-hermes")
	require.ErrorIs(t, err, want)
	require.ErrorIs(t, err, ErrContainmentIncomplete)

	prepared := ""
	for _, event := range authority.events {
		if path, ok := strings.CutPrefix(event, "prepare:"); ok {
			prepared = path
		}
		if strings.HasPrefix(event, "reclaim:") {
			t.Fatalf("failed prepare was followed by reclaim: %v", authority.events)
		}
	}
	require.NotEmpty(t, prepared)
	_, statErr := os.Stat(prepared)
	require.NoError(t, statErr)
}

func TestManagedHermesServerRetriesContainmentBeforeRemoval(t *testing.T) {
	root := t.TempDir()
	want := errors.Join(errors.New("wait uncertain"), ErrContainmentIncomplete)
	attempts := 0
	client := newFakeHermesClient()
	client.closeFunc = func(context.Context) error {
		attempts++
		if attempts == 1 {
			return want
		}

		return nil
	}
	retained := 0
	server := &managedHermesServer{
		Server: client, root: root, sessionID: "session",
		retainIncomplete: func(error, acp.SessionId, string) { retained++ },
	}

	require.ErrorIs(t, server.Close(t.Context()), ErrContainmentIncomplete)
	require.Equal(t, 1, retained)
	_, err := os.Stat(root)
	require.NoError(t, err, "uncertain settlement must retain the root")
	require.NoError(t, server.Close(t.Context()))
	require.Equal(t, 2, attempts)
	_, err = os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, server.Close(t.Context()))
	require.Equal(t, 2, attempts)
}

func TestManagedHermesServerRetriesBusyReclaimBeforeRemoval(t *testing.T) {
	root := t.TempDir()
	authority := newTestHostAuthority()
	reclaims := 0
	authority.reclaimHook = func(string) error {
		reclaims++
		if reclaims == 1 {
			return ErrNativeTreeBusy
		}

		return nil
	}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	options := nativehermes.StartOptions{}
	agent.configureHostAuthority(&options)
	client := newFakeHermesClient()
	client.closeFunc = func(ctx context.Context) error {
		return options.ReclaimNativeTree(ctx, root)
	}
	server := &managedHermesServer{Server: client, root: root, sessionID: "session"}

	err := server.Close(t.Context())
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	require.NoError(t, agent.containmentErr)
	_, statErr := os.Stat(root)
	require.NoError(t, statErr, "busy reclaim must retain the root")

	require.NoError(t, server.Close(t.Context()))
	require.Equal(t, 2, reclaims)
	_, statErr = os.Stat(root)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
