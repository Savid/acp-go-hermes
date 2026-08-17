//nolint:govet // Lock contention tests intentionally use repeated scoped error probes.
package hermes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestSharedHermesAdapterControlDirIsOutsideHome(t *testing.T) {
	home := filepath.Clean(t.TempDir())
	control, err := SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatalf("SharedHermesAdapterControlDir: %v", err)
	}
	resolvedHome, err := filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	if control == resolvedHome || filepath.Dir(control) != filepath.Dir(resolvedHome) || filepath.Base(control) != filepath.Base(resolvedHome)+sharedHermesControlSuffix {
		t.Fatalf("control=%q home=%q", control, home)
	}

	for _, invalid := range []string{"", ".", string(filepath.Separator), home + string(filepath.Separator)} {
		if _, err := SharedHermesAdapterControlDir(invalid); err == nil {
			t.Fatalf("SharedHermesAdapterControlDir(%q) succeeded", invalid)
		}
	}
}

func TestSharedControlLocksConvergeAcrossHomeSymlinkAliases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows shared-home mode fails closed")
	}
	realHome := filepath.Clean(t.TempDir())
	alias := filepath.Join(t.TempDir(), "home-alias")
	if err := os.Symlink(realHome, alias); err != nil {
		t.Fatal(err)
	}

	lock, err := AcquireSharedSessionSetLock(t.Context(), realHome, SharedSessionSetLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Release() }()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := AcquireSharedSessionSetLock(ctx, alias, SharedSessionSetLockExclusive); err == nil {
		t.Fatal("symlink alias bypassed shared session-set lock")
	}

	owner, err := acquireSharedSessionOwner(realHome, "native", "same-id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Release() }()
	if _, err := acquireSharedSessionOwner(alias, "native", "same-id"); err == nil {
		t.Fatal("symlink alias bypassed shared native-session ownership")
	}
}

func TestSharedSessionSetLockReaderWriterExclusion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows shared-home mode fails closed")
	}
	home := filepath.Clean(t.TempDir())
	first, err := AcquireSharedSessionSetLock(t.Context(), home, SharedSessionSetLockShared)
	if err != nil {
		t.Fatalf("acquire first shared lock: %v", err)
	}
	second, err := AcquireSharedSessionSetLock(t.Context(), home, SharedSessionSetLockShared)
	if err != nil {
		t.Fatalf("acquire second shared lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Millisecond)
	defer cancel()
	if _, err := AcquireSharedSessionSetLock(ctx, home, SharedSessionSetLockExclusive); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exclusive behind readers error=%v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release first reader: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release second reader: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("second release must be idempotent: %v", err)
	}

	exclusive, err := AcquireSharedSessionSetLock(t.Context(), home, SharedSessionSetLockExclusive)
	if err != nil {
		t.Fatalf("acquire exclusive: %v", err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), 35*time.Millisecond)
	defer cancel()
	if _, err := AcquireSharedSessionSetLock(ctx, home, SharedSessionSetLockShared); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reader behind exclusive error=%v", err)
	}
	if err := exclusive.Release(); err != nil {
		t.Fatalf("release exclusive: %v", err)
	}

	control, err := SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]os.FileMode{
		control: 0o700,
		filepath.Join(control, sharedSessionSetLockName): 0o600,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("%s mode=%#o want=%#o", path, got, want)
		}
	}
}

func TestSharedSessionSetLockConcurrentReadersReleaseWriter(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows shared-home mode fails closed")
	}
	home := filepath.Clean(t.TempDir())
	const readers = 8
	locks := make([]*SharedSessionSetLock, readers)
	var group sync.WaitGroup
	group.Add(readers)
	for index := range readers {
		go func() {
			defer group.Done()
			lock, err := AcquireSharedSessionSetLock(t.Context(), home, SharedSessionSetLockShared)
			if err != nil {
				t.Errorf("reader lock: %v", err)

				return
			}
			locks[index] = lock
		}()
	}
	group.Wait()

	acquired := make(chan *SharedSessionSetLock, 1)
	go func() {
		lock, err := AcquireSharedSessionSetLock(t.Context(), home, SharedSessionSetLockExclusive)
		if err != nil {
			t.Errorf("writer lock: %v", err)

			return
		}
		acquired <- lock
	}()
	select {
	case <-acquired:
		t.Fatal("writer acquired while readers were held")
	case <-time.After(25 * time.Millisecond):
	}
	for _, lock := range locks {
		if lock == nil {
			t.Fatal("reader failed to acquire")
		}
		if err := lock.Release(); err != nil {
			t.Fatalf("release reader: %v", err)
		}
	}
	select {
	case writer := <-acquired:
		if err := writer.Release(); err != nil {
			t.Fatalf("release writer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire after readers released")
	}
}

func TestSharedSessionSetLockRefusesInvalidInputsAndSymlinkControl(t *testing.T) {
	if _, err := AcquireSharedSessionSetLock(nil, filepath.Clean(t.TempDir()), SharedSessionSetLockShared); err == nil { //nolint:staticcheck // The nil-context rejection is the subject.
		t.Fatal("nil context accepted")
	}
	if _, err := AcquireSharedSessionSetLock(t.Context(), filepath.Clean(t.TempDir()), 0); err == nil {
		t.Fatal("invalid mode accepted")
	}
	if err := (*SharedSessionSetLock)(nil).Release(); err != nil {
		t.Fatalf("nil release: %v", err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	control, err := SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), control); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireSharedSessionSetLock(t.Context(), home, SharedSessionSetLockExclusive); err == nil {
		t.Fatal("symlink control directory accepted")
	}
}

func TestDurableProcessIdentityDetectsLiveReuseAndGone(t *testing.T) {
	identity, err := CurrentDurableProcessIdentity()
	if err != nil {
		t.Fatalf("CurrentDurableProcessIdentity: %v", err)
	}
	if gone, err := DurableProcessIdentityGone(identity); err != nil || gone {
		t.Fatalf("current identity gone=%v err=%v", gone, err)
	}
	reused := identity
	reused.KernelStartTime += "-different"
	if gone, err := DurableProcessIdentityGone(reused); err != nil || !gone {
		t.Fatalf("reused identity gone=%v err=%v", gone, err)
	}
	if _, err := DurableProcessIdentityGone(DurableProcessIdentity{}); err == nil {
		t.Fatal("incomplete process identity accepted")
	}
}
