//go:build windows

package hermes

import (
	"io"
	"os"
	"time"
)

// watchFakeLauncherOwner reaps this fake once the test process it belongs to is
// gone. Windows never reparents — a process keeps the parent id it was born
// with whether or not that process still exists — and cmd.exe cannot exec, so
// killing the launch kills only the launcher and leaves this process behind.
// Two triggers cover that: the inherited stdin reaching EOF, which happens the
// moment the launch and its launcher let go of the pipe, and the owner itself
// disappearing, which covers a fake that was never given one.
func watchFakeLauncherOwner(owner int) {
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}()

	go func() {
		for range time.Tick(100 * time.Millisecond) {
			process, err := os.FindProcess(owner)
			if err != nil {
				os.Exit(0)
			}

			_ = process.Release()
		}
	}()
}
