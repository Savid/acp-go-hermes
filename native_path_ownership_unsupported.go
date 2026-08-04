//go:build !linux

package hermesacp

import (
	"fmt"
	"os"
	"runtime"
)

func handoffGeneratedNativeTreePlatform(_ string, uid uint32, gid uint32) error {
	if uid == uint32(os.Geteuid()) && gid == uint32(os.Getegid()) {
		return nil
	}

	return fmt.Errorf("native path ownership handoff is unsupported on %s", runtime.GOOS)
}

func validateNativeOwnedDirectoryPlatform(_ string, uid uint32, gid uint32) error {
	if uid == uint32(os.Geteuid()) && gid == uint32(os.Getegid()) {
		return nil
	}

	return fmt.Errorf("native path ownership validation is unsupported on %s", runtime.GOOS)
}
