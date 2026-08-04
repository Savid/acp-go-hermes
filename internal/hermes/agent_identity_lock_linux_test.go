//go:build linux

package hermes

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestAgentIdentityLockSerializesAndCancels(t *testing.T) {
	restoreAgentIdentityLockTestSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	canceled := make(chan struct{})
	signals := make(chan os.Signal, 1)

	first, err := acquireAgentIdentityLock(1201, false, canceled, signals)
	if err != nil {
		t.Fatalf("acquire first identity lock: %v", err)
	}
	assertAgentIdentityLockModes(t, root, 1201)

	acquired := make(chan *agentIdentityLock, 1)
	failed := make(chan error, 1)
	go func() {
		lock, acquireErr := acquireAgentIdentityLock(1201, false, make(chan struct{}), make(chan os.Signal))
		if acquireErr != nil {
			failed <- acquireErr
			return
		}
		acquired <- lock
	}()

	select {
	case lock := <-acquired:
		_ = lock.Close()
		t.Fatal("contending identity lock was acquired early")
	case err := <-failed:
		t.Fatalf("contending identity lock failed: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	if err := first.Close(); err != nil {
		t.Fatalf("release first identity lock: %v", err)
	}

	var second *agentIdentityLock
	select {
	case second = <-acquired:
	case err := <-failed:
		t.Fatalf("contending identity lock failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("contending identity lock did not acquire after release")
	}

	cancelWait := make(chan struct{})
	close(cancelWait)
	if _, err := acquireAgentIdentityLock(1201, false, cancelWait, make(chan os.Signal)); err == nil || err.Error() != "agent identity lock canceled" {
		t.Fatalf("canceled identity lock wait = %v", err)
	}

	signalWait := make(chan os.Signal, 1)
	signalWait <- syscall.SIGTERM
	if _, err := acquireAgentIdentityLock(1201, false, make(chan struct{}), signalWait); err == nil {
		t.Fatal("signaled identity lock wait succeeded")
	}

	if err := second.Close(); err != nil {
		t.Fatalf("release second identity lock: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("repeat identity lock close: %v", err)
	}
	if err := (*agentIdentityLock)(nil).Close(); err != nil {
		t.Fatalf("nil identity lock close: %v", err)
	}
}

func TestAgentIdentityLockRejectsUnsafePaths(t *testing.T) {
	t.Run("runtime root mode", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		if err := os.Chmod(root, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := acquireAgentIdentityLock(1202, false, make(chan struct{}), make(chan os.Signal)); err == nil {
			t.Fatal("world-writable runtime root accepted")
		}
	})

	t.Run("owner directory mode", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		if err := os.Mkdir(filepath.Join(root, "wagie"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := acquireAgentIdentityLock(1203, false, make(chan struct{}), make(chan os.Signal)); err == nil {
			t.Fatal("unsafe owner directory accepted")
		}
	})

	t.Run("owner directory symlink", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		if err := os.Symlink(t.TempDir(), filepath.Join(root, "wagie")); err != nil {
			t.Fatal(err)
		}
		if _, err := acquireAgentIdentityLock(1204, false, make(chan struct{}), make(chan os.Signal)); err == nil {
			t.Fatal("symlink owner directory accepted")
		}
	})

	t.Run("lock mode", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		lock, err := acquireAgentIdentityLock(1205, false, make(chan struct{}), make(chan os.Signal))
		if err != nil {
			t.Fatal(err)
		}
		if err := lock.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "wagie", "agent-identities", "1205.lock")
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := acquireAgentIdentityLock(1205, false, make(chan struct{}), make(chan os.Signal)); err == nil {
			t.Fatal("unsafe lock mode accepted")
		}
	})

	t.Run("lock link count", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		lock, err := acquireAgentIdentityLock(1206, false, make(chan struct{}), make(chan os.Signal))
		if err != nil {
			t.Fatal(err)
		}
		if err := lock.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "wagie", "agent-identities", "1206.lock")
		if err := os.Link(path, filepath.Join(root, "linked.lock")); err != nil {
			t.Fatal(err)
		}
		if _, err := acquireAgentIdentityLock(1206, false, make(chan struct{}), make(chan os.Signal)); err == nil {
			t.Fatal("multiply-linked identity lock accepted")
		}
	})

	t.Run("untrusted owner", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		configureAgentIdentityLockTestRoot(t)
		agentIdentityLockTrustedUID++
		if _, err := acquireAgentIdentityLock(1207, false, make(chan struct{}), make(chan os.Signal)); err == nil {
			t.Fatal("untrusted runtime root accepted")
		}
	})
}

func configureAgentIdentityLockTestRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	agentIdentityLockRunRoot = root
	agentIdentityLockTrustedUID = uint32(os.Geteuid())
	agentIdentityLockTrustedGID = uint32(os.Getegid())
	return root
}

func assertAgentIdentityLockModes(t *testing.T, root string, uid uint32) {
	t.Helper()
	for _, path := range []string{
		filepath.Join(root, "wagie"),
		filepath.Join(root, "wagie", "agent-identities"),
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("%s mode = %o", path, info.Mode().Perm())
		}
	}
	path := filepath.Join(root, "wagie", "agent-identities", strconv.FormatUint(uint64(uid), 10)+".lock")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("%s mode = %o", path, info.Mode().Perm())
	}
}

func restoreAgentIdentityLockTestSeams(t *testing.T) {
	t.Helper()
	root := agentIdentityLockRunRoot
	uid := agentIdentityLockTrustedUID
	gid := agentIdentityLockTrustedGID
	t.Cleanup(func() {
		agentIdentityLockRunRoot = root
		agentIdentityLockTrustedUID = uid
		agentIdentityLockTrustedGID = gid
	})
}
