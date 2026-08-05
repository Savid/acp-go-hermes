//go:build linux

package hermesacp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func validateNativeOwnedDirectoryPlatform(root string, uid uint32, gid uint32) error {
	trustedUID := uint32(os.Geteuid())
	trustedGID := uint32(os.Getegid())
	directory, err := openNativeOwnershipDirectory(root, func(stat unix.Stat_t, final bool) error {
		return validateDurableNativeAncestor(stat, final, trustedUID, trustedGID, uid, gid)
	})
	if err != nil {
		return fmt.Errorf("open native-owned directory: %w", err)
	}
	defer directory.Close()

	var stat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &stat); err != nil {
		return fmt.Errorf("inspect native-owned directory: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("native-owned path is not a directory")
	}
	if stat.Uid != uid || stat.Gid != gid {
		return fmt.Errorf("native-owned directory is uid=%d gid=%d, want uid=%d gid=%d", stat.Uid, stat.Gid, uid, gid)
	}
	if stat.Mode&0o022 != 0 || stat.Mode&0o700 != 0o700 {
		return fmt.Errorf("native-owned directory mode %#o is unsafe", stat.Mode&0o7777)
	}

	return nil
}

func openNativeOwnershipDirectory(name string, validate func(unix.Stat_t, bool) error) (*os.File, error) {
	if !filepath.IsAbs(name) {
		return nil, errors.New("native path must be absolute")
	}

	clean := filepath.Clean(name)
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}

	components := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	var rootStat unix.Stat_t
	if statErr := unix.Fstat(fd, &rootStat); statErr != nil {
		_ = unix.Close(fd)

		return nil, statErr
	}
	if validateErr := validate(rootStat, len(components) == 1 && components[0] == ""); validateErr != nil {
		_ = unix.Close(fd)

		return nil, validateErr
	}

	for index, component := range components {
		if component == "" {
			continue
		}

		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr != nil {
			_ = unix.Close(fd)

			return nil, openErr
		}
		var stat unix.Stat_t
		if statErr := unix.Fstat(next, &stat); statErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, statErr
		}
		if validateErr := validate(stat, index == len(components)-1); validateErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, validateErr
		}
		closeErr := unix.Close(fd)
		if closeErr != nil {
			_ = unix.Close(next)

			return nil, closeErr
		}

		fd = next
	}

	return os.NewFile(uintptr(fd), clean), nil
}

func validateDurableNativeAncestor(
	stat unix.Stat_t,
	final bool,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("native-owned path ancestry is not a directory")
	}
	trusted := stat.Uid == trustedUID && stat.Gid == trustedGID
	target := stat.Uid == targetUID && stat.Gid == targetGID
	if !trusted && !target {
		return fmt.Errorf("native-owned path ancestor is uid=%d gid=%d", stat.Uid, stat.Gid)
	}
	mode := stat.Mode & 0o7777
	if mode&0o022 != 0 && !(trusted && mode&unix.S_ISVTX != 0) {
		return fmt.Errorf("native-owned path ancestor mode %#o is writable", mode)
	}
	if final && (!target || mode&0o700 != 0o700) {
		return errors.New("native-owned directory is not safely owned by the target identity")
	}
	if !nativeIdentityCanTraverse(stat, targetUID, targetGID) {
		return errors.New("native-owned path ancestry is not traversable by the target identity")
	}

	return nil
}

func nativeIdentityCanTraverse(stat unix.Stat_t, uid uint32, gid uint32) bool {
	switch {
	case stat.Uid == uid:
		return stat.Mode&0o100 != 0
	case stat.Gid == gid:
		return stat.Mode&0o010 != 0
	default:
		return stat.Mode&0o001 != 0
	}
}
