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
