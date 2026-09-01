package hermes

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type probeTestProcess struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
	result NativeResult
	err    error
	revoke error
}

func (p *probeTestProcess) Stdin() io.WriteCloser                      { return p.stdin }
func (p *probeTestProcess) Stdout() io.ReadCloser                      { return p.stdout }
func (p *probeTestProcess) Stderr() io.ReadCloser                      { return p.stderr }
func (p *probeTestProcess) Wait(context.Context) (NativeResult, error) { return p.result, p.err }
func (p *probeTestProcess) Revoke(context.Context) error               { return p.revoke }

type nopWriteCloser struct{ bytes.Buffer }

func (*nopWriteCloser) Close() error { return nil }

type blockingReadCloser struct {
	done chan struct{}
	once sync.Once
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	<-r.done

	return 0, io.EOF
}

func (r *blockingReadCloser) Close() error {
	r.once.Do(func() { close(r.done) })

	return nil
}

type stubbornProbeOutput struct {
	started     chan struct{}
	closeCalled chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	releaseOnce sync.Once
}

func newStubbornProbeOutput() *stubbornProbeOutput {
	return &stubbornProbeOutput{
		started: make(chan struct{}), closeCalled: make(chan struct{}), release: make(chan struct{}),
	}
}

func (r *stubbornProbeOutput) Read([]byte) (int, error) {
	r.startOnce.Do(func() { close(r.started) })
	<-r.release

	return 0, io.EOF
}

func (r *stubbornProbeOutput) Close() error {
	r.closeOnce.Do(func() { close(r.closeCalled) })

	return nil
}

func (r *stubbornProbeOutput) Release() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func TestVersionProbeUsesAuthorityAndReclaimsBeforeRemoval(t *testing.T) {
	var mu sync.Mutex
	events := make([]string, 0, 4)
	root := ""
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			mu.Lock()
			defer mu.Unlock()
			root = path
			events = append(events, "prepare")

			return nil
		},
		StartNative: func(_ context.Context, request NativeRequest) (NativeProcess, error) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, "start")
			require.Equal(t, "hermes", request.Executable)
			require.Equal(t, []string{"--version"}, request.Arguments)
			require.Contains(t, request.Environment, "HERMES_HOME="+root)

			return &probeTestProcess{
				stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("hermes 0.20.0\n")),
				stderr: io.NopCloser(strings.NewReader("")),
			}, nil
		},
		ReclaimNativeTree: func(_ context.Context, path string) error {
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, root, path)
			events = append(events, "reclaim")

			return nil
		},
	}

	require.NoError(t, probeExecutableVersion(t.Context(), "hermes", opts))
	require.Equal(t, []string{"prepare", "start", "reclaim"}, events)
}

func TestManagedVersionProbeBusyRetainsTreeAndFailsAdmission(t *testing.T) {
	busy := errors.New("lease still has a live server")
	var retained string
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(context.Context, string) error { return nil },
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &probeTestProcess{
				stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("Hermes 0.20.0\n")),
				stderr: io.NopCloser(strings.NewReader("")),
			}, nil
		},
		ReclaimNativeTree: func(context.Context, string) error { return busy },
		NativeTreeBusy:    busy,
		RetainNativeTree: func(root string, err error) bool {
			if !errors.Is(err, busy) {
				return false
			}
			retained = root

			return true
		},
	}

	require.ErrorIs(t, probeExecutableVersion(t.Context(), "hermes", opts), busy)
	require.NotEmpty(t, retained)
	_, err := os.Stat(retained)
	require.NoError(t, err)
}

func TestVersionProbeRejectsUnusableAuthorityProcess(t *testing.T) {
	var root string
	reclaims := 0
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			root = path

			return nil
		},
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &probeTestProcess{err: errors.New("wait uncertain")}, nil
		},
		ReclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}

	require.ErrorContains(t, probeExecutableVersion(t.Context(), "hermes", opts), "unusable host stdio")
	require.Zero(t, reclaims)
	_, err := os.Stat(root)
	require.NoError(t, err, "an uncertain native process must retain its prepared probe tree")
}

func TestVersionProbeWaitFailureRetainsPreparedTree(t *testing.T) {
	var root string
	reclaims := 0
	waitErr := errors.New("authority cannot prove settlement")
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			root = path

			return nil
		},
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &probeTestProcess{
				stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("Hermes 0.20.0\n")),
				stderr: io.NopCloser(strings.NewReader("")), err: waitErr,
			}, nil
		},
		ReclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}

	err := probeExecutableVersion(t.Context(), "hermes", opts)
	require.ErrorIs(t, err, waitErr)
	require.Zero(t, reclaims)
	_, err = os.Stat(root)
	require.NoError(t, err)
}

func TestVersionProbePrepareFailureDoesNotTouchAttemptedTree(t *testing.T) {
	want := errors.New("prepare uncertain")
	var root string
	started := false
	reclaimed := false
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			root = path

			return want
		},
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			started = true

			return nil, errors.New("unexpected start")
		},
		ReclaimNativeTree: func(context.Context, string) error {
			reclaimed = true

			return nil
		},
	}

	err := probeExecutableVersion(t.Context(), "hermes", opts)
	require.ErrorIs(t, err, want)
	require.False(t, started)
	require.False(t, reclaimed)
	_, err = os.Stat(root)
	require.NoError(t, err)
}

func TestVersionProbeRevokeErrorReclaimsAfterSuccessfulWait(t *testing.T) {
	want := errors.New("revoke refused")
	var root string
	reclaims := 0
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			root = path

			return nil
		},
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &probeTestProcess{revoke: want}, nil
		},
		ReclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}

	err := probeExecutableVersion(t.Context(), "hermes", opts)
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, reclaims)
	_, statErr := os.Stat(root)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestVersionProbeStartErrorReclaimsPreparedTree(t *testing.T) {
	want := errors.New("managed start refused")
	var root string
	reclaims := 0
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			root = path

			return nil
		},
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) { return nil, want },
		ReclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}

	err := probeExecutableVersion(t.Context(), "hermes", opts)
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, reclaims)
	_, err = os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestVersionProbeStartContainmentFailureRetainsPreparedTree(t *testing.T) {
	incomplete := errors.New("containment incomplete")
	want := errors.Join(errors.New("managed start uncertain"), incomplete)
	var root string
	reclaims := 0
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			root = path

			return nil
		},
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) { return nil, want },
		ReclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
		ContainmentIncomplete: incomplete,
	}

	err := probeExecutableVersion(t.Context(), "hermes", opts)
	require.ErrorIs(t, err, want)
	require.Zero(t, reclaims)
	_, err = os.Stat(root)
	require.NoError(t, err)
}

func TestVersionProbeWaitFailureDoesNotWaitForPipeEOF(t *testing.T) {
	want := errors.New("managed wait uncertain")
	stdout := &blockingReadCloser{done: make(chan struct{})}
	stderr := &blockingReadCloser{done: make(chan struct{})}
	t.Cleanup(func() {
		_ = stdout.Close()
		_ = stderr.Close()
	})
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(context.Context, string) error { return nil },
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &probeTestProcess{
				stdin: &nopWriteCloser{}, stdout: stdout, stderr: stderr, err: want,
			}, nil
		},
		ReclaimNativeTree: func(context.Context, string) error { return nil },
	}

	done := make(chan error, 1)
	go func() { done <- probeExecutableVersion(t.Context(), "hermes", opts) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, want)
	case <-time.After(time.Second):
		t.Fatal("failed managed Wait blocked on inherited pipe EOF")
	}
}

func TestVersionProbeUncertainSettlementClosesAndJoinsOutputWorkers(t *testing.T) {
	want := errors.New("managed wait uncertain")
	wantContainment := errors.New("containment incomplete")
	stdout := newStubbornProbeOutput()
	stderr := newStubbornProbeOutput()
	t.Cleanup(func() {
		stdout.Release()
		stderr.Release()
	})
	var root string
	reclaims := 0
	retained := false
	native := &retryWaitProcess{
		probeTestProcess: probeTestProcess{
			stdin: &nopWriteCloser{}, stdout: stdout, stderr: stderr,
		},
		waitErrs: []error{want, want},
	}
	opts := ProcessOptions{
		ScratchParent: t.TempDir(), NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, path string) error {
			root = path

			return nil
		},
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) { return native, nil },
		ReclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
		RetainNativeTree: func(path string, err error) bool {
			retained = path == root && errors.Is(err, want) && errors.Is(err, wantContainment)

			return retained
		},
		ContainmentIncomplete: wantContainment,
	}

	done := make(chan error, 1)
	go func() { done <- probeExecutableVersion(t.Context(), "hermes", opts) }()
	<-stdout.started
	<-stderr.started
	<-stdout.closeCalled
	<-stderr.closeCalled
	select {
	case err := <-done:
		t.Fatalf("probe returned before its output workers joined: %v", err)
	default:
	}
	stdout.Release()
	stderr.Release()
	err := <-done
	require.ErrorIs(t, err, want)
	require.ErrorIs(t, err, wantContainment)
	require.True(t, retained)
	require.Zero(t, reclaims)
	require.Equal(t, 2, native.waitCount())
	_, statErr := os.Stat(root)
	require.NoError(t, statErr)
}

func TestManagedServeStartErrorReclaimsPreparedTreesInReverseOrder(t *testing.T) {
	want := errors.New("managed serve start refused")
	home := t.TempDir()
	prepared := make(map[string]struct{})
	reclaimed := make(map[string]struct{})
	events := make([]string, 0, 8)
	opts := ProcessOptions{
		ExecutablePath: "logical-hermes", Home: home, ScratchParent: t.TempDir(),
		NativeEnvironment: map[string]string{"PATH": "/native/bin"},
		PrepareNativeTree: func(_ context.Context, root string) error {
			prepared[root] = struct{}{}
			events = append(events, "prepare:"+root)

			return nil
		},
		StartNative: func(_ context.Context, request NativeRequest) (NativeProcess, error) {
			if len(request.Arguments) == 1 && request.Arguments[0] == argVersion {
				events = append(events, "start:version")

				return &probeTestProcess{
					stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("Hermes 0.20.0\n")),
					stderr: io.NopCloser(strings.NewReader("")),
				}, nil
			}

			events = append(events, "start:serve")

			return nil, want
		},
		ReclaimNativeTree: func(_ context.Context, root string) error {
			reclaimed[root] = struct{}{}
			events = append(events, "reclaim:"+root)

			return nil
		},
	}

	_, err := Start(t.Context(), opts)
	require.ErrorIs(t, err, want)
	for root := range prepared {
		require.Contains(t, reclaimed, root)
		_, statErr := os.Stat(root)
		require.ErrorIs(t, statErr, os.ErrNotExist, "managed serve refusal must remove tree %q", root)
	}
	serveStart := slices.Index(events, "start:serve")
	require.GreaterOrEqual(t, serveStart, 0)
	require.Len(t, events[serveStart+1:], 2)
	require.Equal(t, "reclaim:"+home, events[serveStart+1])
	require.NotEqual(t, home, strings.TrimPrefix(events[serveStart+2], "reclaim:"))
}

func TestManagedServeRequestComposesPathWithoutStartupCarrier(t *testing.T) {
	want := errors.New("managed serve start refused")
	first := t.TempDir()
	second := t.TempDir()
	var serveRequest NativeRequest
	opts := ProcessOptions{
		ExecutablePath: "logical-hermes", Home: t.TempDir(), ScratchParent: t.TempDir(),
		NativeEnvironment: map[string]string{"PATH": "/native/bin", "KEPT": "yes"},
		AmbientEnvironment: map[string]string{
			hermesBashEnvKey: "/ambient/bash-init", hermesShellEnvKey: "/ambient/sh-init",
			hermesPathInitCountEnv: "99",
		},
		ExtraPathDirs:     []string{first, second},
		PrepareNativeTree: func(context.Context, string) error { return nil },
		StartNative: func(_ context.Context, request NativeRequest) (NativeProcess, error) {
			if len(request.Arguments) == 1 && request.Arguments[0] == argVersion {
				return &probeTestProcess{
					stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("Hermes 0.20.0\n")),
					stderr: io.NopCloser(strings.NewReader("")),
				}, nil
			}

			serveRequest = request

			return nil, want
		},
		ReclaimNativeTree: func(context.Context, string) error { return nil },
	}

	_, err := Start(t.Context(), opts)
	require.ErrorIs(t, err, want)
	require.NotEmpty(t, serveRequest.Environment)
	path := envValueFold(serveRequest.Environment, "PATH", processRuntimePlatform == processPlatformWindows)
	parts := strings.Split(path, string(os.PathListSeparator))
	require.Len(t, parts, 4)
	require.Equal(t, []string{first, second}, parts[:2])
	require.Contains(t, parts[2], browserShimPrefix)
	require.Equal(t, "/native/bin", parts[3])
	require.Contains(t, serveRequest.Environment, "KEPT=yes")
	for _, entry := range serveRequest.Environment {
		key, _, ok := strings.Cut(entry, "=")
		require.True(t, ok)
		require.False(t, processEnvironmentKeyMatches(key, hermesBashEnvKey), "managed request carried %q", key)
		require.False(t, processEnvironmentKeyMatches(key, "ENV"), "managed request carried %q", key)
		require.False(t, hermesPathCarrierEnvironmentKey(key), "managed request carried %q", key)
	}
}

type retryWaitProcess struct {
	probeTestProcess
	mu       sync.Mutex
	waits    int
	waitErrs []error
}

func (p *retryWaitProcess) Wait(context.Context) (NativeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waits++
	if len(p.waitErrs) == 0 {
		return p.result, nil
	}
	err := p.waitErrs[0]
	p.waitErrs = p.waitErrs[1:]

	return p.result, err
}

func (p *retryWaitProcess) waitCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.waits
}

func TestManagedProcessCloseRetriesUncertainWaitForTerminalProof(t *testing.T) {
	want := errors.New("wait uncertain")
	root := t.TempDir()
	reclaims := 0
	native := &retryWaitProcess{waitErrs: []error{want, nil}}
	process := &Process{
		Home: root, native: native, managed: true, preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}

	require.ErrorIs(t, process.Close(t.Context()), want)
	require.Zero(t, reclaims)
	_, err := os.Stat(root)
	require.NoError(t, err)
	require.NoError(t, process.Close(t.Context()))
	require.Equal(t, 1, reclaims)
	require.Equal(t, 2, native.waitCount())
}

func TestManagedProcessCloseAcceptsTerminalWaitAfterRevokeError(t *testing.T) {
	want := errors.New("revoke request failed")
	root := t.TempDir()
	reclaims := 0
	process := &Process{
		Home: root,
		native: &probeTestProcess{
			stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("")),
			stderr: io.NopCloser(strings.NewReader("")), revoke: want,
		},
		managed: true, preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}

	require.NoError(t, process.Close(t.Context()))
	require.Equal(t, 1, reclaims)
}

type cachedTerminalProcess struct {
	probeTestProcess
	waitStarted chan struct{}
	releaseWait chan struct{}
	waitOnce    sync.Once
}

func (p *cachedTerminalProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.waitOnce.Do(func() { close(p.waitStarted) })
	select {
	case <-p.releaseWait:
	case <-ctx.Done():
		return NativeResult{}, ctx.Err()
	}

	return NativeResult{Revoked: true}, nil
}

func (p *cachedTerminalProcess) Revoke(ctx context.Context) error {
	return ctx.Err()
}

func TestManagedProcessCloseRetriesCanceledRevokeWithCachedTerminalWait(t *testing.T) {
	root := t.TempDir()
	native := &cachedTerminalProcess{
		waitStarted: make(chan struct{}),
		releaseWait: make(chan struct{}),
	}
	reclaims := 0
	process := &Process{
		Home: root, native: native, managed: true, preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}
	process.beginWait()
	<-native.waitStarted

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, process.Close(canceled), context.Canceled)
	require.Zero(t, reclaims)
	_, err := os.Stat(root)

	require.NoError(t, err)

	close(native.releaseWait)
	require.NoError(t, process.Close(t.Context()))
	require.Equal(t, 1, reclaims)
}
