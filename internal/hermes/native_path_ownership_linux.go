//go:build linux

package hermes

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// The handoff walks a path it does not own, then keeps acting on descriptors it
// already accepted, so each syscall whose failure must abort the handoff is
// reached through a seam. Faulting a seam is the only way to prove the tree is
// not surrendered when the kernel stops answering for a descriptor the walk
// already validated, and the only way to stage a later read disagreeing with an
// earlier one.
var (
	generatedNativeOpenFilesystemRoot = func() (int, error) {
		return unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	generatedNativeFstat   = unix.Fstat
	generatedNativeClose   = unix.Close
	generatedNativeFchown  = unix.Fchown
	generatedNativeReadDir = func(directory *os.File) ([]os.DirEntry, error) { return directory.ReadDir(-1) }
)

func handoffGeneratedNativeTree(root string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	if !filepath.IsAbs(root) {
		return errors.New("generated native path must be absolute")
	}

	trustedUID := effectiveUID()
	trustedGID := effectiveGID()

	directory, err := openGeneratedNativeDirectory(root, trustedUID, trustedGID, isolation.UID, isolation.GID)
	if err != nil {
		return err
	}
	defer directory.Close()

	return handoffGeneratedNativeDirectory(
		directory,
		trustedUID,
		trustedGID,
		isolation.UID,
		isolation.GID,
	)
}

func openGeneratedNativeDirectory(name string, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32) (*os.File, error) {
	clean := filepath.Clean(name)

	fd, err := generatedNativeOpenFilesystemRoot()
	if err != nil {
		return nil, err
	}

	components := strings.Split(strings.TrimPrefix(clean, "/"), "/")

	var rootStat unix.Stat_t
	if statErr := generatedNativeFstat(fd, &rootStat); statErr != nil {
		_ = unix.Close(fd)

		return nil, statErr
	}

	if validateErr := validateGeneratedNativeAncestor(rootStat, len(components) == 1 && components[0] == "", trustedUID, trustedGID, targetUID, targetGID); validateErr != nil {
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
		if statErr := generatedNativeFstat(next, &stat); statErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, statErr
		}

		if validateErr := validateGeneratedNativeAncestor(stat, index == len(components)-1, trustedUID, trustedGID, targetUID, targetGID); validateErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, validateErr
		}

		closeErr := generatedNativeClose(fd)
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
// remaining answers are to give the wrapper one, or to anchor the generation on
// a path the identity already owns.
const nativeSharedIdentityAncestryRemedy = "run the supervisor as root to isolate the agent identity, " +
	"or place the native directory under a path the agent identity owns"

// nativeAncestorIsSharedIdentityRoot reports whether a non-final ancestor is
// acceptable purely because root owns it. It is, only when the trusted identity
// and the target identity are the same one: owning both ends of the handoff
// leaves no privilege boundary for the ancestry rule to defend, while every path
// to a directory that identity owns still crosses root-owned components such as
// "/" and "/home". An ancestor owned by any other identity stays refused, so a
// second local identity still cannot interpose one.
func nativeAncestorIsSharedIdentityRoot(stat unix.Stat_t, final bool, trustedUID uint32, targetUID uint32) bool {
	return !final && trustedUID == targetUID && stat.Uid == 0 && stat.Gid == 0
}

func validateGeneratedNativeAncestor(
	stat unix.Stat_t,
	final bool,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("generated native path ancestry is not a trusted directory")
	}

	trusted := stat.Uid == trustedUID && stat.Gid == trustedGID

	rootOwned := nativeAncestorIsSharedIdentityRoot(stat, final, trustedUID, targetUID)
	if !trusted && !rootOwned {
		if !final && trustedUID == targetUID {
			return fmt.Errorf(
				"generated native path ancestor is uid=%d gid=%d; %s",
				stat.Uid, stat.Gid, nativeSharedIdentityAncestryRemedy,
			)
		}

		return errors.New("generated native path ancestry is not a trusted directory")
	}

	mode := stat.Mode & 0o7777
	if final && mode != 0o700 {
		return fmt.Errorf("generated native root mode %#o is unsafe", mode)
	}

	if !final && mode&0o022 != 0 && mode&unix.S_ISVTX == 0 {
		return fmt.Errorf("generated native ancestor mode %#o is writable without sticky protection", mode)
	}

	if !final && !nativeIdentityCanTraverse(stat, targetUID, targetGID) {
		return errors.New("generated native path ancestry is not traversable by the target identity")
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

func handoffGeneratedNativeDirectory(directory *os.File, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32) error {
	if err := validateGeneratedNativeInode(int(directory.Fd()), unix.S_IFDIR, trustedUID, trustedGID, targetUID, targetGID, false); err != nil {
		return err
	}

	entries, err := generatedNativeReadDir(directory)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	for _, entry := range entries {
		fd, openErr := unix.Openat(
			int(directory.Fd()),
			entry.Name(),
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
			0,
		)
		if openErr != nil {
			return fmt.Errorf("open generated native entry %q: %w", entry.Name(), openErr)
		}

		child := os.NewFile(uintptr(fd), entry.Name())
		childErr := handoffGeneratedNativeEntry(child, trustedUID, trustedGID, targetUID, targetGID)

		closeErr := child.Close()
		if childErr != nil || closeErr != nil {
			return errors.Join(childErr, closeErr)
		}
	}

	return chownGeneratedNativeInode(int(directory.Fd()), unix.S_IFDIR, targetUID, targetGID, false)
}

func handoffGeneratedNativeEntry(file *os.File, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32) error {
	var stat unix.Stat_t
	if err := generatedNativeFstat(int(file.Fd()), &stat); err != nil {
		return err
	}

	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return handoffGeneratedNativeDirectory(file, trustedUID, trustedGID, targetUID, targetGID)
	case unix.S_IFREG:
		if err := validateGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, trustedUID, trustedGID, targetUID, targetGID, true); err != nil {
			return err
		}

		return chownGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, targetUID, targetGID, true)
	default:
		return fmt.Errorf("generated native inode has unsupported type %#o", stat.Mode&unix.S_IFMT)
	}
}

func validateGeneratedNativeInode(fd int, kind uint32, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32, singleLink bool) error {
	var stat unix.Stat_t
	if err := generatedNativeFstat(fd, &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != kind {
		return errors.New("generated native inode type changed")
	}

	if stat.Uid != trustedUID || stat.Gid != trustedGID {
		return fmt.Errorf("generated native inode owner changed to uid=%d gid=%d", stat.Uid, stat.Gid)
	}

	if singleLink && stat.Nlink != 1 {
		return fmt.Errorf("generated native file has %d links", stat.Nlink)
	}

	mode := stat.Mode & 0o7777
	if kind == unix.S_IFDIR && mode != 0o700 {
		return fmt.Errorf("generated native directory mode %#o is unsafe", mode)
	}

	if kind == unix.S_IFREG && mode != 0o600 && mode != 0o700 {
		return fmt.Errorf("generated native file mode %#o is unsafe", mode)
	}

	return nil
}

func chownGeneratedNativeInode(fd int, kind uint32, uid uint32, gid uint32, singleLink bool) error {
	if err := generatedNativeFchown(fd, int(uid), int(gid)); err != nil {
		return err
	}

	var stat unix.Stat_t
	if err := generatedNativeFstat(fd, &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != kind || stat.Uid != uid || stat.Gid != gid || singleLink && stat.Nlink != 1 {
		return errors.New("generated native inode ownership handoff could not be proven")
	}

	return nil
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
