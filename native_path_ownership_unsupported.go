//go:build !linux

package hermesacp

import (
	"fmt"
	"os"
	"runtime"
)

func validateNativeOwnedDirectoryPlatform(_ string, uid uint32, gid uint32) error {
	if int64(uid) == int64(os.Geteuid()) && int64(gid) == int64(os.Getegid()) {
		return nil
	}

	return fmt.Errorf("native path ownership validation is unsupported on %s", runtime.GOOS)
}
