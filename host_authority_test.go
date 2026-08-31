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
	markerErr := errors.New("authority refused launch")
	authority.start = func(NativeRequest) (NativeProcess, error) {
		return nil, markerErr
	}
	err := startWithRecordingAuthority(t, authority, WithHostAuthority(authority), executable)
	require.ErrorIs(t, err, markerErr)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ordinary fallback executed managed selector: %v", err)
	}
	if eventIndex(authority.events, "start:"+executable+" --version") < 0 {
		t.Fatalf("authority did not receive managed probe: %v", authority.events)
	}
	for _, event := range authority.events {
		if strings.HasPrefix(event, "reclaim:") {
			t.Fatalf("managed StartNative error was followed by reclaim: %v", authority.events)
		}
		if root, ok := strings.CutPrefix(event, "prepare:"); ok {
			_, statErr := os.Stat(root)
			require.NoError(t, statErr, "managed StartNative error must retain the prepared tree")
		}
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
