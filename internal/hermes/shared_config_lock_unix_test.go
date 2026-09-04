//go:build !windows

package hermes

import (
	"context"
	"testing"
	"time"
)

// TestSharedHermesConfigLockHonorsCancellation pins that a second writer waiting
// on a held shared-config lock stops when its context does. It lives on the
// POSIX side because the shared Hermes home, and with it the adapter control
// root this lock is taken in, is refused on Windows.
func TestSharedHermesConfigLockHonorsCancellation(t *testing.T) {
	home := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- withHermesConfigLock(context.Background(), home, func(string) error {
			close(entered)
			<-release

			return nil
		})
	}()

	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("config lock never reached its callback: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the config lock callback")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := withHermesConfigLock(ctx, home, func(string) error { return nil }); err == nil {
		t.Fatal("canceled config lock acquisition succeeded")
	}

	close(release)

	if err := <-done; err != nil {
		t.Fatalf("release config lock: %v", err)
	}
}
