package hermes

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type residualRoundTripper func(*http.Request) (*http.Response, error)

func (f residualRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type residualExitError struct{}

func (residualExitError) Error() string { return "process exited" }
func (residualExitError) ExitCode() int { return 1 }

func TestResidualAuthAndStartupObserverBranches(t *testing.T) {
	server := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"status":"unexpected"}`)
	})
	if _, err := server.AuthPollFlow(t.Context(), "provider", "session"); err == nil {
		t.Fatal("unsupported auth poll status was accepted")
	}

	called := false
	observeHermesStartupStage(t.Context(), func(context.Context, string, string, time.Duration, error) {
		called = true
	}, "session", "configuration", time.Now(), nil)
	if !called {
		t.Fatal("startup observer was not called")
	}
}

func TestResidualServerControlLockBranches(t *testing.T) {
	blockedParent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blockedParent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireServerControlLock(t.Context(), filepath.Join(blockedParent, "child")); err == nil {
		t.Fatal("control lock opened below a file")
	}

	dir := t.TempDir()
	first, err := acquireServerControlLock(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, cancelErr := acquireServerControlLock(cancelled, dir); !errors.Is(cancelErr, context.Canceled) {
		t.Fatalf("cancelled lock error = %v", cancelErr)
	}
	if releaseErr := first.Release(); releaseErr != nil {
		t.Fatal(releaseErr)
	}

	first, err = acquireServerControlLock(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		second, acquireErr := acquireServerControlLock(t.Context(), dir)
		if acquireErr == nil {
			acquireErr = second.Release()
		}
		done <- acquireErr
	}()
	time.Sleep(20 * time.Millisecond)
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResidualServerControlLockOperationFailures(t *testing.T) {
	openCaptured := func(t *testing.T) (func(string, int, os.FileMode) (*os.File, error), **os.File) {
		t.Helper()
		var captured *os.File

		return func(path string, flags int, mode os.FileMode) (*os.File, error) {
			file, err := os.OpenFile(path, flags, mode)
			captured = file

			return file, err
		}, &captured
	}

	t.Run("chmod", func(t *testing.T) {
		openFile, captured := openCaptured(t)
		_, err := acquireServerControlLockWithOps(
			t.Context(), t.TempDir(), openFile,
			func(*os.File, os.FileMode) error { return errors.New("chmod refused") },
			tryLockSharedSessionSetFile,
		)
		if err == nil || !strings.Contains(err.Error(), "chmod refused") {
			t.Fatalf("control chmod failure = %v", err)
		}
		// A second Close is the portable proof that the first one happened:
		// os.File.Stat reports a platform-specific syscall failure on a closed
		// handle, but Close always answers os.ErrClosed.
		if closeErr := (*captured).Close(); !errors.Is(closeErr, os.ErrClosed) {
			t.Fatalf("chmod failure retained file: %v", closeErr)
		}
	})

	t.Run("lock", func(t *testing.T) {
		openFile, captured := openCaptured(t)
		_, err := acquireServerControlLockWithOps(
			t.Context(), t.TempDir(), openFile, (*os.File).Chmod,
			func(*os.File, SharedSessionSetLockMode) (func() error, bool, error) {
				return nil, false, errors.New("lock refused")
			},
		)
		if err == nil || !strings.Contains(err.Error(), "lock refused") {
			t.Fatalf("control lock failure = %v", err)
		}
		if closeErr := (*captured).Close(); !errors.Is(closeErr, os.ErrClosed) {
			t.Fatalf("lock failure retained file: %v", closeErr)
		}
	})
}

func TestResidualOrdinaryPipeConstructionFailures(t *testing.T) {
	// Each stream's pipe is claimed in order, and a refusal at any point releases
	// every descriptor already claimed — proven by the earlier ends answering a
	// second Close with os.ErrClosed.
	for failAt, want := range []string{"create native stdin", "create native stdout", "create native stderr"} {
		var opened []*os.File
		calls := 0
		refused := errors.New("pipe refused")
		_, err := startOrdinaryNativeWithPipes(exec.Command("unused"), func() (*os.File, *os.File, error) {
			if calls == failAt {
				return nil, nil, refused
			}
			calls++
			r, w, pipeErr := os.Pipe()
			if pipeErr != nil {
				return nil, nil, pipeErr
			}
			opened = append(opened, r, w)

			return r, w, nil
		})
		if !errors.Is(err, refused) || !strings.Contains(err.Error(), want) {
			t.Fatalf("pipe failure %d = %v, want %q", failAt, err, want)
		}
		if len(opened) != 2*failAt {
			t.Fatalf("pipe failure %d claimed %d descriptors, want %d", failAt, len(opened), 2*failAt)
		}
		for _, file := range opened {
			if closeErr := file.Close(); !errors.Is(closeErr, os.ErrClosed) {
				t.Fatalf("pipe failure %d retained descriptor: %v", failAt, closeErr)
			}
		}
	}

	// A start that fails releases both ends of every pipe.
	var opened []*os.File
	_, err := startOrdinaryNativeWithPipes(exec.Command(filepath.Join(t.TempDir(), "missing")), func() (*os.File, *os.File, error) {
		r, w, pipeErr := os.Pipe()
		if pipeErr == nil {
			opened = append(opened, r, w)
		}

		return r, w, pipeErr
	})
	if err == nil {
		t.Fatal("missing executable started")
	}
	for _, file := range opened {
		if closeErr := file.Close(); !errors.Is(closeErr, os.ErrClosed) {
			t.Fatalf("failed start retained descriptor: %v", closeErr)
		}
	}
}

// TestOrdinaryNativeOutputSurvivesAnEarlyWait pins that a child's whole answer
// reaches the reader even when Wait reaps the child before the read begins:
// the parent ends are owned here rather than closed by exec.Cmd.Wait.
func TestOrdinaryNativeOutputSurvivesAnEarlyWait(t *testing.T) {
	payload := strings.Repeat("hermes output line\n", 2000)
	script := filepath.Join(t.TempDir(), "speak")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' \"$PAYLOAD\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	process, err := startOrdinaryNative(t.Context(), NativeRequest{Executable: script, Environment: []string{"PAYLOAD=" + payload}})
	if err != nil {
		t.Fatal(err)
	}
	_ = process.Stdin().Close()
	if _, err = process.Wait(t.Context()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	got, err := io.ReadAll(process.Stdout())
	if err != nil || string(got) != payload {
		t.Fatalf("stdout after wait: err=%v len=%d want %d", err, len(got), len(payload))
	}
	_ = process.Stdout().Close()
	_ = process.Stderr().Close()
}

func TestResidualSharedOwnerBranches(t *testing.T) {
	if owner, err := AcquireSharedHomeOwner("relative"); err == nil || owner != nil {
		t.Fatalf("relative shared home owner = %#v, %v", owner, err)
	}

	var nilHomeOwner *SharedHomeOwner
	if err := nilHomeOwner.Release(); err != nil {
		t.Fatal(err)
	}
	zeroHomeOwner := &SharedHomeOwner{}
	if err := zeroHomeOwner.Release(); err != nil {
		t.Fatal(err)
	}

	originalFileStat := sharedOwnerFileStat
	originalLstat := sharedOwnerLstat
	t.Cleanup(func() {
		sharedOwnerFileStat = originalFileStat
		sharedOwnerLstat = originalLstat
	})

	lockPath := filepath.Join(t.TempDir(), "owner.lock")
	sharedOwnerFileStat = func(*os.File) (os.FileInfo, error) { return nil, errors.New("stat refused") }
	if _, err := tryAcquireSharedOwnerLock(lockPath, sharedSessionOwnerKind); err == nil || !strings.Contains(err.Error(), "inspect") {
		t.Fatalf("owner file stat error = %v", err)
	}
	sharedOwnerFileStat = originalFileStat

	sharedOwnerLstat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
	if _, err := acquireSharedOwnerLock(lockPath, sharedSessionOwnerKind); err == nil || !strings.Contains(err.Error(), "every acquisition") {
		t.Fatalf("replaced owner retry error = %v", err)
	}
	sharedOwnerLstat = originalLstat

	var nilSessionOwner *SharedSessionOwner
	if err := nilSessionOwner.Release(); err != nil {
		t.Fatal(err)
	}
	if err := (&SharedSessionOwner{}).Release(); err == nil {
		t.Fatal("unavailable shared owner released cleanly")
	}
}

func TestResidualProcessHelperBranches(t *testing.T) {
	(&Process{}).retainPreparedTrees(errors.New("ignored"))

	shimDir := t.TempDir()
	home := t.TempDir()
	retained := make([]string, 0, 3)
	process := &Process{
		shim: &browserShim{dir: shimDir}, Home: home,
		preparedShim: true, preparedHome: true, shimCleanupPending: true,
		retainNativeTree: func(path string, _ error) bool {
			retained = append(retained, path)

			return true
		},
	}
	process.retainPreparedTrees(errors.New("settlement refused"))
	if process.preparedShim || process.preparedHome || process.shimCleanupPending || len(retained) != 3 {
		t.Fatalf("retained process state = %#v, %v", process, retained)
	}

	plain := (&Process{}).containmentFailure("close", errors.New("fault"))
	if !strings.Contains(plain.Error(), "native containment incomplete") {
		t.Fatalf("plain containment error = %v", plain)
	}
	wantContainment := errors.New("containment marker")
	marked := (&Process{containmentIncomplete: wantContainment}).containmentFailure("close", errors.New("fault"))
	if !errors.Is(marked, wantContainment) {
		t.Fatalf("marked containment error = %v", marked)
	}
	wantBusy := errors.New("busy marker")
	if (&Process{}).treeBusy(wantBusy) || !(&Process{nativeTreeBusy: wantBusy}).treeBusy(wantBusy) {
		t.Fatal("tree busy classification drifted")
	}

	removeShim := t.TempDir()
	removeHome := t.TempDir()
	if err := (&Process{shim: &browserShim{dir: removeShim}, Home: removeHome}).removeUnpreparedTrees(); err != nil {
		t.Fatal(err)
	}

	originalRemoveAll := removeAll
	t.Cleanup(func() { removeAll = originalRemoveAll })
	removeAll = func(string) error { return errors.New("remove refused") }
	if err := (&Process{Home: t.TempDir()}).removeUnpreparedTrees(); err == nil {
		t.Fatal("home removal failure was ignored")
	}
	removeAll = originalRemoveAll

	settled := false
	rollback := &Process{
		managed: true, shim: &browserShim{dir: t.TempDir()}, Home: t.TempDir(), preparedShim: true, preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error { return nil },
		nativeTreeSettled: func() { settled = true },
	}
	if err := rollback.rollbackPreparedTrees(t.Context()); err != nil || !settled {
		t.Fatalf("successful rollback = %v, settled=%v", err, settled)
	}

	wantReclaim := errors.New("reclaim refused")
	rollback = &Process{
		managed: true, shim: &browserShim{dir: t.TempDir()}, Home: t.TempDir(), preparedShim: true, preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error { return wantReclaim },
	}
	if err := rollback.rollbackPreparedTrees(t.Context()); !errors.Is(err, wantReclaim) {
		t.Fatalf("failed rollback = %v", err)
	}

	settled = false
	reclaim := &Process{
		managed: true, shim: &browserShim{dir: t.TempDir()}, Home: t.TempDir(), preparedShim: true, preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error { return nil },
		nativeTreeSettled: func() { settled = true },
	}
	if err := reclaim.reclaimAndRemove(t.Context()); err != nil || !settled {
		t.Fatalf("successful reclaim = %v, settled=%v", err, settled)
	}
	if err := (&Process{}).reclaimAndRemove(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestResidualProcessPrimitiveFailures(t *testing.T) {
	originalListen := listenTCP
	originalRand := randReader
	originalHome := userHomeDir
	originalStat := statPath
	t.Cleanup(func() {
		listenTCP = originalListen
		randReader = originalRand
		userHomeDir = originalHome
		statPath = originalStat
	})

	listenTCP = func(string, string) (net.Listener, error) { return nil, errors.New("listen refused") }
	if _, err := freePort(); err == nil {
		t.Fatal("free port ignored listen failure")
	}
	listenTCP = originalListen

	randReader = strings.NewReader("")
	if _, err := randomToken(); err == nil {
		t.Fatal("random token ignored entropy failure")
	}
	randReader = originalRand

	userHomeDir = func() (string, error) { return "", errors.New("home refused") }
	if defaultWebDistExists() {
		t.Fatal("web dist exists without a home")
	}
	userHomeDir = func() (string, error) { return t.TempDir(), nil }
	statPath = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
	if defaultWebDistExists() {
		t.Fatal("missing web dist exists")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	statPath = func(string) (os.FileInfo, error) { return info, nil }
	if defaultWebDistExists() {
		t.Fatal("file was accepted as web dist")
	}
	directoryInfo, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	statPath = func(string) (os.FileInfo, error) { return directoryInfo, nil }
	if !defaultWebDistExists() {
		t.Fatal("directory web dist was rejected")
	}
}

func residualManagedStartOptions(t *testing.T, serve func(context.Context, NativeRequest) (NativeProcess, error)) ProcessOptions {
	t.Helper()

	return ProcessOptions{
		ExecutablePath: "logical-hermes",
		Home:           t.TempDir(),
		ScratchParent:  t.TempDir(),
		NativeEnvironment: map[string]string{
			"PATH": os.Getenv("PATH"),
		},
		StartNative: func(ctx context.Context, request NativeRequest) (NativeProcess, error) {
			if len(request.Arguments) == 1 && request.Arguments[0] == argVersion {
				return &probeTestProcess{
					stdin:  &nopWriteCloser{},
					stdout: io.NopCloser(strings.NewReader("Hermes 0.20.0\n")),
					stderr: io.NopCloser(strings.NewReader("")),
				}, nil
			}

			return serve(ctx, request)
		},
		PrepareNativeTree: func(context.Context, string) error { return nil },
		ReclaimNativeTree: func(context.Context, string) error { return nil },
		Timeout:           time.Second,
	}
}

func TestResidualProcessStartEarlyFailures(t *testing.T) {
	if process, err := Start(t.Context(), ProcessOptions{
		AmbientEnvironment: map[string]string{"PATH": t.TempDir()},
	}); err == nil || process != nil {
		t.Fatalf("default executable start = %#v, %v", process, err)
	}
	if process, err := Start(t.Context(), ProcessOptions{ExtraPathDirs: []string{"relative"}}); err == nil || process != nil {
		t.Fatalf("invalid path carrier start = %#v, %v", process, err)
	}
	if process, err := Start(t.Context(), ProcessOptions{
		ExecutablePath: "hermes", AmbientEnvironment: map[string]string{"BAD\x00KEY": "value"},
	}); err == nil || process != nil {
		t.Fatalf("invalid ambient environment start = %#v, %v", process, err)
	}
	if process, err := Start(t.Context(), ProcessOptions{
		ExecutablePath: "missing-hermes-residual", AmbientEnvironment: map[string]string{"PATH": t.TempDir()},
	}); err == nil || process != nil {
		t.Fatalf("missing executable start = %#v, %v", process, err)
	}

	originalMkdirTemp := mkdirTemp
	originalMkdirAll := mkdirAll
	originalListen := listenTCP
	originalRand := randReader
	originalShim := newProcessBrowserShim
	t.Cleanup(func() {
		mkdirTemp = originalMkdirTemp
		mkdirAll = originalMkdirAll
		listenTCP = originalListen
		randReader = originalRand
		newProcessBrowserShim = originalShim
	})

	mkdirTemp = func(string, string) (string, error) { return "", errors.New("runtime home refused") }
	opts := residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	opts.Home = ""
	if _, err := Start(t.Context(), opts); err == nil || !strings.Contains(err.Error(), "runtime home refused") {
		t.Fatalf("runtime home creation error = %v", err)
	}
	mkdirTemp = originalMkdirTemp

	mkdirAll = func(string, os.FileMode) error { return errors.New("runtime home chmod refused") }
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	if _, err := Start(t.Context(), opts); err == nil || !strings.Contains(err.Error(), "runtime home chmod refused") {
		t.Fatalf("runtime home mkdir error = %v", err)
	}
	mkdirAll = originalMkdirAll

	wantVersion := errors.New("version spawn refused")
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	opts.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) { return nil, wantVersion }
	if _, err := Start(t.Context(), opts); !errors.Is(err, wantVersion) {
		t.Fatalf("version failure = %v", err)
	}

	wantSharedPrepare := errors.New("shared prepare refused")
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	opts.SharedHome = true
	opts.PrepareSharedHome = func(context.Context, string) error { return wantSharedPrepare }
	if _, err := Start(t.Context(), opts); !errors.Is(err, wantSharedPrepare) {
		t.Fatalf("shared preparation failure = %v", err)
	}

	listenTCP = func(string, string) (net.Listener, error) { return nil, errors.New("port refused") }
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	if _, err := Start(t.Context(), opts); err == nil || !strings.Contains(err.Error(), "port refused") {
		t.Fatalf("port failure = %v", err)
	}
	listenTCP = originalListen

	randReader = strings.NewReader("")
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	if _, err := Start(t.Context(), opts); err == nil {
		t.Fatal("token entropy failure was ignored")
	}
	randReader = originalRand

	newProcessBrowserShim = func(string) (*browserShim, error) { return nil, errors.New("shim refused") }
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	if _, err := Start(t.Context(), opts); err == nil || !strings.Contains(err.Error(), "shim refused") {
		t.Fatalf("shim failure = %v", err)
	}
	newProcessBrowserShim = originalShim
}

func TestResidualOrdinaryEnvironmentStartFailures(t *testing.T) {
	originalPlatform := processRuntimePlatform
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })
	processRuntimePlatform = processPlatformWindows

	executable := fakeHermesExecutable(t, fakeProcessModeOK)
	if process, err := Start(t.Context(), ProcessOptions{
		ExecutablePath: executable,
		// Only an inherited name reaches the phase merge; a duplicate of a
		// name the allowlist never admits is dropped rather than refused.
		AmbientEnvironment: map[string]string{
			"PATH": os.Getenv("PATH"), "TEMP": "one", "temp": "two",
		},
	}); err == nil || process != nil {
		t.Fatalf("duplicate ambient environment start = %#v, %v", process, err)
	}

	processRuntimePlatform = originalPlatform
	executable = fakeHermesExecutable(t, fakeProcessModeOK)
	processRuntimePlatform = processPlatformWindows
	if process, err := Start(t.Context(), ProcessOptions{
		ExecutablePath:     executable,
		ScratchParent:      t.TempDir(),
		AmbientEnvironment: map[string]string{"PATH": os.Getenv("PATH")},
		SessionEnv:         map[string]string{"TOKEN": "one", "token": "two"},
		Timeout:            10 * time.Second,
	}); err == nil || process != nil || !strings.Contains(err.Error(), "names TOKEN twice") {
		t.Fatalf("duplicate session environment start = %#v, %v", process, err)
	}
}

func TestResidualManagedProcessStartTransactions(t *testing.T) {
	wantPrepare := errors.New("prepare refused")
	prepareCalls := 0
	retained := 0
	opts := residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	opts.PrepareNativeTree = func(context.Context, string) error {
		prepareCalls++
		if prepareCalls == 2 {
			return wantPrepare
		}

		return nil
	}
	opts.RetainNativeTree = func(string, error) bool {
		retained++

		return true
	}
	if _, err := Start(t.Context(), opts); !errors.Is(err, wantPrepare) || retained == 0 {
		t.Fatalf("shim prepare failure = %v, retained=%d", err, retained)
	}

	wantBusy := errors.New("tree busy")
	originalShim := newProcessBrowserShim
	t.Cleanup(func() { newProcessBrowserShim = originalShim })
	shimDir := unremovableDirPath(t)
	newProcessBrowserShim = func(string) (*browserShim, error) {
		return &browserShim{dir: shimDir}, nil
	}
	prepareCalls = 0
	retained = 0
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	opts.NativeTreeBusy = wantBusy
	opts.PrepareNativeTree = func(context.Context, string) error {
		prepareCalls++
		if prepareCalls == 2 {
			return wantBusy
		}

		return nil
	}
	opts.RetainNativeTree = func(string, error) bool {
		retained++

		return true
	}
	if _, err := Start(t.Context(), opts); !errors.Is(err, wantBusy) || retained == 0 {
		t.Fatalf("busy shim prepare failure = %v, retained=%d", err, retained)
	}
	newProcessBrowserShim = originalShim

	containment := errors.New("containment incomplete")
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, containment
	})
	opts.ContainmentIncomplete = containment
	opts.RetainNativeTree = func(string, error) bool {
		retained++

		return true
	}
	if _, err := Start(t.Context(), opts); !errors.Is(err, containment) {
		t.Fatalf("containment spawn failure = %v", err)
	}

	wantSpawn := errors.New("spawn refused")
	wantRollback := errors.New("rollback refused")
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, wantSpawn
	})
	reclaimCalls := 0
	opts.ReclaimNativeTree = func(context.Context, string) error {
		reclaimCalls++
		if reclaimCalls == 1 {
			return nil
		}

		return wantRollback
	}
	if _, err := Start(t.Context(), opts); !errors.Is(err, wantSpawn) || !errors.Is(err, wantRollback) {
		t.Fatalf("spawn rollback failure = %v", err)
	}

	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return &probeTestProcess{}, nil
	})
	if _, err := Start(t.Context(), opts); err == nil || !strings.Contains(err.Error(), "unusable host stdio") {
		t.Fatalf("unusable serve process = %v", err)
	}

	wantHomePrepare := errors.New("home prepare refused")
	prepareCalls = 0
	reclaimCalls = 0
	retained = 0
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	// The launch prepares one tree per native root: the probe home, the browser
	// shim on a platform that installs one, and the serve home. The home whose
	// preparation must fail is the last of them.
	opts.PrepareNativeTree = func(context.Context, string) error {
		prepareCalls++
		if prepareCalls == 2+browserShimTreeCount {
			return wantHomePrepare
		}

		return nil
	}
	opts.ReclaimNativeTree = func(context.Context, string) error {
		reclaimCalls++
		if reclaimCalls > 1 {
			return wantRollback
		}

		return nil
	}
	opts.RetainNativeTree = func(string, error) bool {
		retained++

		return true
	}
	_, homePrepareErr := Start(t.Context(), opts)
	if !errors.Is(homePrepareErr, wantHomePrepare) || retained == 0 {
		t.Fatalf("home preparation rollback = %v, retained=%d", homePrepareErr, retained)
	}
	// The refused reclaim is the browser shim's: once the serve home fails to
	// prepare, the shim is the only tree the rollback still has to give back,
	// and a platform that installs no shim has none to refuse.
	if errors.Is(homePrepareErr, wantRollback) != (browserShimTreeCount > 0) {
		t.Fatalf("home preparation rollback refusal = %v", homePrepareErr)
	}

	cleanupShimDir := unremovableDirPath(t)
	newProcessBrowserShim = func(string) (*browserShim, error) {
		return &browserShim{dir: cleanupShimDir}, nil
	}
	prepareCalls = 0
	retained = 0
	opts = residualManagedStartOptions(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, errors.New("unexpected serve")
	})
	opts.PrepareNativeTree = func(context.Context, string) error {
		prepareCalls++
		if prepareCalls == 3 {
			return wantHomePrepare
		}

		return nil
	}
	opts.RetainNativeTree = func(string, error) bool {
		retained++

		return true
	}
	if _, err := Start(t.Context(), opts); !errors.Is(err, wantHomePrepare) || retained == 0 {
		t.Fatalf("home preparation cleanup retention = %v, retained=%d", err, retained)
	}
	newProcessBrowserShim = originalShim
}

func TestResidualManagedEnvironmentBranches(t *testing.T) {
	if _, err := managedEnvironment(nil); err == nil {
		t.Fatal("nil managed environment was accepted")
	}
	if _, err := managedEnvironment(map[string]string{"BAD=KEY": "value"}); err == nil {
		t.Fatal("invalid managed environment key was accepted")
	}

	originalPlatform := processRuntimePlatform
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })
	processRuntimePlatform = processPlatformWindows
	if _, err := managedEnvironment(map[string]string{"PATH": "one", "Path": "two"}); err == nil {
		t.Fatal("duplicate folded managed environment key was accepted")
	}
}

func TestResidualVersionProbeTransactions(t *testing.T) {
	originalMkdirTemp := mkdirTemp
	originalRemoveAll := removeAll
	t.Cleanup(func() {
		mkdirTemp = originalMkdirTemp
		removeAll = originalRemoveAll
	})

	mkdirTemp = func(string, string) (string, error) { return "", errors.New("probe mkdir refused") }
	if err := probeExecutableVersion(t.Context(), "hermes", ProcessOptions{}); err == nil {
		t.Fatal("probe generation failure was ignored")
	}
	mkdirTemp = originalMkdirTemp

	opts := ProcessOptions{
		ScratchParent: t.TempDir(),
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return nil, errors.New("unexpected start")
		},
	}
	if err := probeExecutableVersion(t.Context(), "hermes", opts); err == nil || !strings.Contains(err.Error(), "environment") {
		t.Fatalf("probe environment failure = %v", err)
	}

	opts.NativeEnvironment = map[string]string{"PATH": os.Getenv("PATH")}
	if err := probeExecutableVersion(t.Context(), "hermes", opts); err == nil || !strings.Contains(err.Error(), "tree operations") {
		t.Fatalf("missing probe authority = %v", err)
	}

	wantBusy := errors.New("probe busy")
	retained := 0
	removeAll = func(string) error { return errors.New("probe remove refused") }
	opts.PrepareNativeTree = func(context.Context, string) error { return wantBusy }
	opts.ReclaimNativeTree = func(context.Context, string) error { return nil }
	opts.NativeTreeBusy = wantBusy
	opts.RetainNativeTree = func(string, error) bool {
		retained++

		return true
	}
	if err := probeExecutableVersion(t.Context(), "hermes", opts); !errors.Is(err, wantBusy) || retained == 0 {
		t.Fatalf("busy probe preparation = %v, retained=%d", err, retained)
	}
	removeAll = originalRemoveAll

	wantPrepare := errors.New("probe prepare refused")
	retained = 0
	opts.NativeTreeBusy = errors.New("other busy")
	opts.PrepareNativeTree = func(context.Context, string) error { return wantPrepare }
	if err := probeExecutableVersion(t.Context(), "hermes", opts); !errors.Is(err, wantPrepare) || retained == 0 {
		t.Fatalf("probe preparation failure = %v, retained=%d", err, retained)
	}

	wantContainment := errors.New("probe containment")
	retained = 0
	opts.PrepareNativeTree = func(context.Context, string) error { return nil }
	opts.ContainmentIncomplete = wantContainment
	opts.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) { return nil, wantContainment }
	if err := probeExecutableVersion(t.Context(), "hermes", opts); !errors.Is(err, wantContainment) || retained == 0 {
		t.Fatalf("contained probe start = %v, retained=%d", err, retained)
	}

	wantStart := errors.New("probe start refused")
	wantReclaim := errors.New("probe reclaim refused")
	opts.ContainmentIncomplete = errors.New("other containment")
	opts.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) { return nil, wantStart }
	opts.ReclaimNativeTree = func(context.Context, string) error { return wantReclaim }
	if err := probeExecutableVersion(t.Context(), "hermes", opts); !errors.Is(err, wantStart) || !errors.Is(err, wantReclaim) {
		t.Fatalf("failed probe cleanup = %v", err)
	}

	opts.ReclaimNativeTree = func(context.Context, string) error { return nil }
	opts.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, nil //nolint:nilnil // Exercises a host returning no process and no error.
	}
	retained = 0
	if err := probeExecutableVersion(t.Context(), "hermes", opts); err == nil || retained == 0 {
		t.Fatalf("nil probe process = %v, retained=%d", err, retained)
	}

	opts.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) {
		return &probeTestProcess{}, nil
	}
	if err := probeExecutableVersion(t.Context(), "hermes", opts); err == nil || !strings.Contains(err.Error(), "unusable host stdio") {
		t.Fatalf("settled unusable probe = %v", err)
	}

	opts.PrepareNativeTree = func(context.Context, string) error { return nil }
	opts.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) {
		return &probeTestProcess{
			stdin: &nopWriteCloser{}, stdout: io.NopCloser(strings.NewReader("Hermes 0.20.0\n")),
			stderr: io.NopCloser(strings.NewReader("")), err: context.Canceled,
		}, nil
	}
	retained = 0
	if err := probeExecutableVersion(t.Context(), "hermes", opts); err == nil || retained == 0 {
		t.Fatalf("uncertain managed probe wait = %v, retained=%d", err, retained)
	}

	ordinary := ProcessOptions{ScratchParent: t.TempDir(), AmbientEnvironment: map[string]string{"PATH": os.Getenv("PATH")}}
	if err := probeExecutableVersion(t.Context(), filepath.Join(t.TempDir(), "missing"), ordinary); err == nil {
		t.Fatal("ordinary probe start failure was ignored")
	}
}

func TestResidualProbeSettlementAndReclaimBranches(t *testing.T) {
	settled, err := settleProbeProcess(nil)
	if settled || err == nil {
		t.Fatalf("nil probe settlement = %v, %v", settled, err)
	}

	wantRevoke := errors.New("revoke refused")
	wantWait := errors.New("wait refused")
	settled, err = settleProbeProcess(&probeTestProcess{revoke: wantRevoke, err: wantWait})
	if settled || !errors.Is(err, wantRevoke) || !errors.Is(err, wantWait) {
		t.Fatalf("failed probe settlement = %v, %v", settled, err)
	}

	originalRemoveAll := removeAll
	t.Cleanup(func() { removeAll = originalRemoveAll })
	retained := 0
	wantReclaim := errors.New("reclaim refused")
	opts := ProcessOptions{
		ReclaimNativeTree: func(context.Context, string) error { return wantReclaim },
		RetainNativeTree: func(string, error) bool {
			retained++

			return true
		},
	}
	if err := reclaimProbeTree(opts, t.TempDir(), true); !errors.Is(err, wantReclaim) || retained != 1 {
		t.Fatalf("probe reclaim failure = %v, retained=%d", err, retained)
	}

	removeAll = func(string) error { return errors.New("remove refused") }
	if err := reclaimProbeTree(opts, t.TempDir(), false); err == nil || retained != 2 {
		t.Fatalf("probe remove failure = %v, retained=%d", err, retained)
	}
}

func TestResidualProcessRollbackRemovalFailures(t *testing.T) {
	originalRemoveAll := removeAll
	t.Cleanup(func() { removeAll = originalRemoveAll })
	retained := 0
	removeAll = func(string) error { return errors.New("home remove refused") }
	process := &Process{
		Home: t.TempDir(), preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error { return nil },
		retainNativeTree: func(string, error) bool {
			retained++

			return true
		},
	}
	if err := process.rollbackPreparedTrees(t.Context()); err == nil || retained != 1 {
		t.Fatalf("home rollback removal = %v, retained=%d", err, retained)
	}
	removeAll = originalRemoveAll

	process = &Process{
		shim: &browserShim{dir: unremovableDirPath(t)}, preparedShim: true,
		reclaimNativeTree: func(context.Context, string) error { return nil },
		retainNativeTree: func(string, error) bool {
			retained++

			return true
		},
	}
	if err := process.rollbackPreparedTrees(t.Context()); err == nil || retained != 2 {
		t.Fatalf("shim rollback removal = %v, retained=%d", err, retained)
	}
}

func TestResidualProcessCloseBranches(t *testing.T) {
	if err := (&Process{managed: true}).Close(t.Context()); err == nil {
		t.Fatal("managed process without native handle closed cleanly")
	}

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	wantRevoke := errors.New("revoke refused")
	process := &Process{native: &probeTestProcess{revoke: wantRevoke}}
	if err := process.Close(cancelled); !errors.Is(err, wantRevoke) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled process close = %v", err)
	}

	wantWait := errors.New("wait refused")
	process = &Process{managed: true, native: &probeTestProcess{err: wantWait}}
	if err := process.Close(t.Context()); !errors.Is(err, wantWait) {
		t.Fatalf("managed wait failure = %v", err)
	}

	process = &Process{native: &probeTestProcess{err: context.Canceled}}
	if err := process.Close(t.Context()); !errors.Is(err, context.Canceled) {
		t.Fatalf("ordinary cancelled wait = %v", err)
	}

	process = &Process{
		native: &probeTestProcess{result: NativeResult{Revoked: true}, err: residualExitError{}},
		shim:   &browserShim{dir: t.TempDir()},
	}
	if err := process.Close(t.Context()); err != nil {
		t.Fatalf("revoked exit close = %v", err)
	}

	wantReclaim := errors.New("close reclaim refused")
	process = &Process{
		managed: true, native: &probeTestProcess{}, Home: t.TempDir(), preparedHome: true,
		reclaimNativeTree: func(context.Context, string) error { return wantReclaim },
	}
	if err := process.Close(t.Context()); !errors.Is(err, wantReclaim) {
		t.Fatalf("close reclaim failure = %v", err)
	}

	wantContainment := errors.New("containment incomplete")
	process = &Process{
		managed: true, native: &probeTestProcess{err: errors.New("native wait failed")},
		preparedHome: true, containmentIncomplete: wantContainment,
	}
	if err := process.startupFailure(errors.New("startup refused")); !errors.Is(err, wantContainment) {
		t.Fatalf("startup failure normalization = %v", err)
	}

	process = &Process{
		managed: true, shim: &browserShim{dir: t.TempDir()}, preparedShim: true,
		reclaimNativeTree: func(context.Context, string) error { return wantReclaim },
	}
	if err := process.reclaimAndRemove(t.Context()); !errors.Is(err, wantReclaim) {
		t.Fatalf("shim reclaim failure = %v", err)
	}
}

func TestResidualProcessReadinessBranches(t *testing.T) {
	if err := (&Process{StatusURL: "://bad", waitDone: make(chan struct{})}).waitReady(t.Context()); err == nil {
		t.Fatal("invalid status URL was accepted")
	}

	originalHTTPClient := newStatusHTTPClient
	originalAfter := after
	t.Cleanup(func() {
		newStatusHTTPClient = originalHTTPClient
		after = originalAfter
	})

	wantHTTP := errors.New("status refused")
	newStatusHTTPClient = func() *http.Client {
		return &http.Client{Transport: residualRoundTripper(func(*http.Request) (*http.Response, error) {
			return nil, wantHTTP
		})}
	}
	exited := make(chan struct{})
	close(exited)
	process := &Process{StatusURL: "http://example.test/status", waitDone: exited, waitResult: NativeResult{ExitCode: 7}}
	if err := process.waitReady(t.Context()); err == nil || !strings.Contains(err.Error(), "exit code 7") {
		t.Fatalf("early process exit readiness = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	process = &Process{StatusURL: "http://example.test/status", waitDone: make(chan struct{})}
	if err := process.waitReady(ctx); !errors.Is(err, wantHTTP) {
		t.Fatalf("status request cancellation = %v", err)
	}

	newStatusHTTPClient = func() *http.Client {
		return &http.Client{Transport: residualRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}
	}
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	if err := process.waitReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("non-ready status cancellation = %v", err)
	}

	requests := 0
	newStatusHTTPClient = func() *http.Client {
		return &http.Client{Transport: residualRoundTripper(func(*http.Request) (*http.Response, error) {
			requests++
			status := http.StatusServiceUnavailable
			if requests > 1 {
				status = http.StatusOK
			}

			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}
	}
	after = func(time.Duration) <-chan time.Time {
		ready := make(chan time.Time, 1)
		ready <- time.Now()

		return ready
	}
	process = &Process{StatusURL: "http://example.test/status", waitDone: make(chan struct{})}
	if err := process.waitReady(t.Context()); err != nil || requests != 2 {
		t.Fatalf("readiness retry = %v, requests=%d", err, requests)
	}

	deliveries := make(chan GatewayDelivery)
	close(deliveries)
	if err := (&Process{Client: &Client{deliveries: deliveries}}).waitGatewayReady(t.Context()); err == nil {
		t.Fatal("closed gateway deliveries were accepted")
	}
	wantDelivery := errors.New("gateway delivery refused")
	deliveries = make(chan GatewayDelivery, 1)
	deliveries <- GatewayDelivery{Err: wantDelivery}
	if err := (&Process{Client: &Client{deliveries: deliveries}}).waitGatewayReady(t.Context()); !errors.Is(err, wantDelivery) {
		t.Fatalf("gateway delivery error = %v", err)
	}
	deliveries = make(chan GatewayDelivery, 2)
	deliveries <- GatewayDelivery{Event: &Event{Type: "other"}}
	deliveries <- GatewayDelivery{Event: &Event{Type: eventGatewayReady}}
	if err := (&Process{Client: &Client{deliveries: deliveries}}).waitGatewayReady(t.Context()); err != nil {
		t.Fatalf("gateway readiness = %v", err)
	}
	cancelledGateway, cancelGateway := context.WithCancel(t.Context())
	cancelGateway()
	if err := (&Process{Client: &Client{deliveries: make(chan GatewayDelivery)}}).waitGatewayReady(cancelledGateway); !errors.Is(err, context.Canceled) {
		t.Fatalf("gateway readiness cancellation = %v", err)
	}
}

func TestResidualOrdinaryNativeFallbackKill(t *testing.T) {
	native, err := startOrdinaryNative(t.Context(), ordinaryNativeHelperRequest(t, "block"))
	if err != nil {
		t.Fatal(err)
	}
	process, ok := native.(*ordinaryNativeProcess)
	if !ok {
		t.Fatalf("ordinary process type = %T", native)
	}
	process.kill = nil
	if err := process.Revoke(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Wait(t.Context()); err == nil {
		t.Fatal("killed ordinary native process returned a clean wait")
	}
}

func TestResidualOrdinaryProcessStartupFailures(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "hermes")
	script := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'Hermes 0.20.0'; rm \"$0\"; exit 0; fi\nexit 1\n"
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(t.Context(), ProcessOptions{
		ExecutablePath: executable, ScratchParent: t.TempDir(),
		AmbientEnvironment: map[string]string{"PATH": os.Getenv("PATH")}, Timeout: time.Second,
	}); err == nil {
		t.Fatal("ordinary serve spawn failure was ignored")
	}

	for _, mode := range []string{fakeProcessModeStatusOnly, fakeProcessModeNoGatewayReady} {
		t.Run(mode, func(t *testing.T) {
			executable := fakeHermesExecutable(t, mode)
			executableProbeMu.Lock()
			executableProbed[executable] = true
			executableProbeMu.Unlock()
			t.Cleanup(func() {
				executableProbeMu.Lock()
				delete(executableProbed, executable)
				executableProbeMu.Unlock()
			})
			if _, err := Start(t.Context(), ProcessOptions{
				ExecutablePath: executable, ScratchParent: t.TempDir(),
				AmbientEnvironment: map[string]string{"PATH": os.Getenv("PATH")}, Timeout: 2 * time.Second,
			}); err == nil {
				t.Fatal("incomplete gateway startup was accepted")
			}
		})
	}
}

func TestResidualImageAttachmentRefusal(t *testing.T) {
	fake := newFakeGatewayServer(t)
	fake.imageAttachedFalse = true
	client := fake.dialClient(t)
	t.Cleanup(func() { _ = client.Close(1000, "done") })
	if err := client.AttachImageBytes(t.Context(), "live", []byte{0}); err == nil {
		t.Fatal("negative image attachment result was accepted")
	}
}

func TestResidualGatewayMethodProbeFailures(t *testing.T) {
	run := func(t *testing.T, configure func(*fakeGatewayServer)) error {
		t.Helper()
		fake := newFakeGatewayServer(t)
		configure(fake)
		client := fake.dialClient(t)
		defer func() { _ = client.Close(1000, "done") }()

		return (&Process{Client: client, Home: t.TempDir()}).probeGatewayMethods(t.Context())
	}

	for _, test := range []struct {
		name      string
		configure func(*fakeGatewayServer)
	}{
		{"create failure", func(fake *fakeGatewayServer) { fake.setFail("session.create") }},
		{"create schema", func(fake *fakeGatewayServer) { fake.setCreateNoStored() }},
		{"cleanup close failure", func(fake *fakeGatewayServer) { fake.setFail("session.close") }},
		{"cleanup delete failure", func(fake *fakeGatewayServer) { fake.setFail("session.delete") }},
		{"resume failure", func(fake *fakeGatewayServer) { fake.setFail("session.resume") }},
		{"resume schema", func(fake *fakeGatewayServer) { fake.setResumeNoKey() }},
		{"active failure", func(fake *fakeGatewayServer) { fake.setFail("session.active_list") }},
		{"active schema", func(fake *fakeGatewayServer) { fake.activeNil = true }},
		{"models schema", func(fake *fakeGatewayServer) { fake.modelProvidersNil = true }},
		{"prompt failure", func(fake *fakeGatewayServer) { fake.setFail("prompt.submit") }},
		{"image failure", func(fake *fakeGatewayServer) { fake.setFail("image.attach_bytes") }},
		{"approval failure", func(fake *fakeGatewayServer) { fake.setFail("approval.respond") }},
		{"clarify failure", func(fake *fakeGatewayServer) { fake.setFail("clarify.respond") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := run(t, test.configure); err == nil {
				t.Fatal("gateway probe failure was ignored")
			}
		})
	}

	t.Run("resume domain refusal proves presence", func(t *testing.T) {
		if err := run(t, func(fake *fakeGatewayServer) { fake.setNotFound("session.resume", 1) }); err != nil {
			t.Fatalf("domain refusal did not prove resume presence: %v", err)
		}
	})
}
