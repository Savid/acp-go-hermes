//go:build linux

package hermesacp

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// The native-owned directory check re-reads the descriptor it just walked to,
// so each syscall it depends on is reached through a seam. Faulting a seam is
// the only way to prove the check fails closed when the kernel stops answering
// for a descriptor the traversal already accepted, and the only way to stage
// the second read disagreeing with the first.
var (
	nativeOwnershipOpenFilesystemRoot = func() (int, error) {
		return unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	nativeOwnershipFstat = unix.Fstat
	nativeOwnershipClose = unix.Close
)

func validateNativeOwnedDirectoryPlatform(root string, uid uint32, gid uint32) error {
	trustedUID := effectiveUID()
	trustedGID := effectiveGID()

	directory, err := openNativeOwnershipDirectory(root, func(stat unix.Stat_t, final bool) error {
		return validateDurableNativeAncestor(stat, final, trustedUID, trustedGID, uid, gid)
	})
	if err != nil {
		return fmt.Errorf("open native-owned directory: %w", err)
	}
	defer directory.Close()

	var stat unix.Stat_t
	if err := nativeOwnershipFstat(int(directory.Fd()), &stat); err != nil {
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

	fd, err := nativeOwnershipOpenFilesystemRoot()
	if err != nil {
		return nil, err
	}

	components := strings.Split(strings.TrimPrefix(clean, "/"), "/")

	var rootStat unix.Stat_t
	if statErr := nativeOwnershipFstat(fd, &rootStat); statErr != nil {
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
		if statErr := nativeOwnershipFstat(next, &stat); statErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, statErr
		}

		if validateErr := validate(stat, index == len(components)-1); validateErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, validateErr
		}

		closeErr := nativeOwnershipClose(fd)
		if closeErr != nil {
			_ = unix.Close(next)

			return nil, closeErr
		}

		fd = next
	}

	return os.NewFile(uintptr(fd), clean), nil
}

// nativeSharedIdentityAncestryRemedy states what an operator can change when
// the wrapper's own identity is the native identity and an ancestor still fails
// the walk. There is no privilege boundary left to lean on in that shape, so the
// remaining answers are to give the wrapper one, or to anchor the directory on a
// path the identity already owns.
const nativeSharedIdentityAncestryRemedy = "run the supervisor as root to isolate the agent identity, " +
	"or place the native directory under a path the agent identity owns"

// nativeAncestorIsSharedIdentityRoot reports whether a non-final ancestor is
// acceptable purely because root owns it. It is, only when the trusted identity
// and the target identity are the same one: owning both ends of the check leaves
// no privilege boundary for the ancestry rule to defend, while every path to a
// directory that identity owns still crosses root-owned components such as "/"
// and "/home". An ancestor owned by any other identity stays refused, so a
// second local identity still cannot interpose one.
func nativeAncestorIsSharedIdentityRoot(stat unix.Stat_t, final bool, trustedUID uint32, targetUID uint32) bool {
	return !final && trustedUID == targetUID && stat.Uid == 0 && stat.Gid == 0
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

	rootOwned := nativeAncestorIsSharedIdentityRoot(stat, final, trustedUID, targetUID)

	target := stat.Uid == targetUID && stat.Gid == targetGID
	if !trusted && !target && !rootOwned {
		if !final && trustedUID == targetUID {
			return fmt.Errorf(
				"native-owned path ancestor is uid=%d gid=%d; %s",
				stat.Uid, stat.Gid, nativeSharedIdentityAncestryRemedy,
			)
		}

		return fmt.Errorf("native-owned path ancestor is uid=%d gid=%d", stat.Uid, stat.Gid)
	}

	mode := stat.Mode & 0o7777
	if mode&0o022 != 0 && ((!trusted && !rootOwned) || mode&unix.S_ISVTX == 0) {
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

// Seams for the fail-closed guards below. Linux cannot produce a uid or gid
// outside the 32 bits it stores them in, so the guards are unreachable through
// the real syscalls; tests swap these to reach them.
var (
	effectiveUIDSource = os.Geteuid
	effectiveGIDSource = os.Getegid
)

// effectiveUID reports the caller's effective UID. Linux stores UIDs in 32
// bits, so the int os.Geteuid returns always fits and the guard below never
// fires; it is here because every caller compares this value against an inode
// owner, where a silently truncated match would grant trust instead of
// withholding it. The unrepresentable case therefore fails closed on an ID no
// inode can carry.
func effectiveUID() uint32 {
	uid := effectiveUIDSource()
	if uid < 0 || uid > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(uid)
}

// effectiveGID reports the caller's effective GID under the same contract as
// effectiveUID.
func effectiveGID() uint32 {
	gid := effectiveGIDSource()
	if gid < 0 || gid > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(gid)
}
