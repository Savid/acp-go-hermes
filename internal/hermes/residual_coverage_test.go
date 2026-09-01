package hermes

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
	parentFile := filepath.Join(t.TempDir(), "parent-file")
	if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	newProcessBrowserShim = func(string) (*browserShim, error) {
		return &browserShim{dir: filepath.Join(parentFile, "shim")}, nil
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
}
