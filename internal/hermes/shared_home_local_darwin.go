//go:build darwin

package hermes

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func EnsureLocalSharedHermesHome(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect shared Hermes home filesystem: %w", err)
	}

	nameBytes := make([]byte, 0, len(stat.Fstypename))
	for _, value := range stat.Fstypename {
		if value == 0 {
			break
		}

		nameBytes = append(nameBytes, value)
	}

	name := strings.ToLower(string(nameBytes))
	if !darwinSharedHomeFilesystemLocal(name) {
		return fmt.Errorf("shared Hermes home requires a supported local filesystem, got %s", name)
	}

	return nil
}

func darwinSharedHomeFilesystemLocal(name string) bool {
	switch name {
	case "apfs", "hfs", "ufs":
		return true
	default:
		return false
	}
}
