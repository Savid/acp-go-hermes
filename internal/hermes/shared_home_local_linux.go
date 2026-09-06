//go:build linux

package hermes

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func EnsureLocalSharedHermesHome(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect shared Hermes home filesystem: %w", err)
	}

	if !linuxSharedHomeFilesystemLocal(stat.Type) {
		return fmt.Errorf("shared Hermes home requires a supported local filesystem (type %#x)", stat.Type)
	}

	return nil
}

func linuxSharedHomeFilesystemLocal(kind int64) bool {
	// The same local durable allowlist used by the agent authority store: ext*,
	// XFS, btrfs, F2FS, ZFS, and bcachefs. Volatile tmpfs/ramfs, overlayfs,
	// NFS/CIFS/SMB2/FUSE/virtiofs/9p, and unknown filesystems fail closed.
	switch kind {
	case 0xef53, 0x58465342, 0x9123683e, 0xf2f52010, 0x2fc12fc1, 0xca451a4e:
		return true
	default:
		return false
	}
}
