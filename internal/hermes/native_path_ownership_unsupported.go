//go:build !linux

package hermes

import (
	"fmt"
	"os"
	"runtime"
)

func handoffGeneratedNativeTree(_ string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	if isolation.TestOnlyNoCredential ||
		int64(isolation.UID) == int64(os.Geteuid()) && int64(isolation.GID) == int64(os.Getegid()) {
		return nil
	}

	return fmt.Errorf("native path ownership handoff is unsupported on %s", runtime.GOOS)
}
