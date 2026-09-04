//go:build !windows

package hermes

import (
	"os"
	"time"
)

// watchFakeLauncherOwner reaps this fake once the test process it belongs to is
// gone. POSIX reparents an orphan, so the parent changing is the whole answer.
func watchFakeLauncherOwner(owner int) {
	go func() {
		for range time.Tick(100 * time.Millisecond) {
			if os.Getppid() != owner {
				os.Exit(0)
			}
		}
	}()
}
