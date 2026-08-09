//go:build linux

package hermes

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	nativeOwnershipCovUID = uint32(65534)
	nativeOwnershipCovGID = uint32(65534)
)

// TestGeneratedNativeHandoffRefusesBeforeTheWalk proves the two decisions the
// handoff makes before it opens anything: a session without isolation keeps the
// generation under the wrapper's own identity, and a relative root is refused
// outright because a relative walk resolves against the working directory
// rather than the named tree.
func TestGeneratedNativeHandoffRefusesBeforeTheWalk(t *testing.T) {
	root := nativeOwnershipCovTree(t)

	require.NoError(t, handoffGeneratedNativeTree(root, nil))
	nativeOwnershipCovRequireWrapperOwned(t, root)

	relative, err := filepath.Rel("/", root)
	require.NoError(t, err)
	require.ErrorContains(t, handoffGeneratedNativeTree(relative, nativeOwnershipCovIsolation()), "must be absolute")
	nativeOwnershipCovRequireWrapperOwned(t, root)
}

// TestGeneratedNativeWalkRefusesUnsafeAncestry pins the exact reason the walk
// refuses each unsafe path shape. The walk is the only thing standing between a
// wrapper-owned generation and an identity that must not be able to reach
// anything else: every ancestor must be a wrapper-owned directory, an ancestor
// may not be writable by others without sticky protection, and the generation
// itself must be exactly 0700 so nothing else was already inside it. No refusal
// may leave a partially surrendered tree behind.
func TestGeneratedNativeWalkRefusesUnsafeAncestry(t *testing.T) {
	for _, testCase := range []struct {
		name string
		seed func(*testing.T, string) string
		want string
	}{
		{
			name: "generation is not exactly private",
			seed: func(t *testing.T, root string) string {
				t.Helper()
				require.NoError(t, os.Chmod(root, 0o755))

				return root
			},
			want: "generated native root mode 0755 is unsafe",
		},
		{
			name: "ancestor is owned by a foreign identity",
			seed: func(t *testing.T, root string) string {
				t.Helper()
				require.NoError(t, os.Chown(filepath.Dir(root), int(nativeOwnershipCovUID), int(nativeOwnershipCovGID)))

				return root
			},
			want: "ancestry is not a trusted directory",
		},
		{
			name: "ancestor is writable without sticky protection",
			seed: func(t *testing.T, root string) string {
				t.Helper()
				require.NoError(t, os.Chmod(filepath.Dir(root), 0o733))

				return root
			},
			want: "generated native ancestor mode 0733 is writable without sticky protection",
		},
		{
			name: "component does not exist",
			seed: func(t *testing.T, root string) string {
				t.Helper()

				return filepath.Join(filepath.Dir(root), "absent")
			},
			want: "no such file or directory",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := nativeOwnershipCovTree(t)
			named := testCase.seed(t, root)

			require.ErrorContains(
				t, handoffGeneratedNativeTree(named, nativeOwnershipCovIsolation()), testCase.want,
			)
			nativeOwnershipCovRequireWrapperOwned(t, root, filepath.Join(root, "entry"))
		})
	}
}

// TestGeneratedNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors
// proves how far the ancestry rule relaxes when the wrapper never dropped
// privilege, so its own identity is the native identity. Nothing separates the
// two ends of the handoff in that shape, and every path to a directory that
// identity owns still crosses root-owned components such as "/" and "/home", so
// those are acceptable ancestors — and nothing else is: a third identity's
// ancestor, an ancestor root left writable without sticky protection, and a
// generation root still owns are all refused.
func TestGeneratedNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors(t *testing.T) {
	directory := func(mode uint32, uid, gid uint32) unix.Stat_t {
		return unix.Stat_t{Mode: unix.S_IFDIR | mode, Uid: uid, Gid: gid}
	}

	for _, testCase := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
		want  string
	}{
		{
			name: "not a directory",
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700},
			want: "generated native path ancestry is not a trusted directory",
		},
		{
			name: "ancestor owned by a third identity",
			stat: directory(0o755, 4242, 4242),
			want: "generated native path ancestor is uid=4242 gid=4242; " +
				"run the supervisor as root to isolate the agent identity, " +
				"or place the native directory under a path the agent identity owns",
		},
		{
			name: "ancestor owned by root with a foreign group",
			stat: directory(0o755, 0, 4242),
			want: "generated native path ancestor is uid=0 gid=4242",
		},
		{
			name: "world-writable root-owned ancestor without sticky protection",
			stat: directory(0o777, 0, 0),
			want: "generated native ancestor mode 0777 is writable without sticky protection",
		},
		{
			name: "root-owned ancestor the native identity cannot traverse",
			stat: directory(0o700, 0, 0),
			want: "not traversable by the target identity",
		},
		{
			name:  "generation still owned by root",
			stat:  directory(0o700, 0, 0),
			final: true,
			want:  "generated native path ancestry is not a trusted directory",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateGeneratedNativeAncestor(
				testCase.stat, testCase.final,
				nativeOwnershipCovUID, nativeOwnershipCovGID,
				nativeOwnershipCovUID, nativeOwnershipCovGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	for _, accepted := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
		why   string
	}{
		{
			name: "root-owned ancestor every home directory is reached through",
			stat: directory(0o755, 0, 0),
			why:  "the root-owned ancestry a shared identity cannot avoid was refused",
		},
		{name: "sticky writable root-owned ancestor", stat: directory(0o1777, 0, 0)},
		{name: "traversable ancestor owned by the native identity", stat: directory(0o711, nativeOwnershipCovUID, nativeOwnershipCovGID)},
		{
			name:  "generation owned outright by the native identity",
			stat:  directory(0o700, nativeOwnershipCovUID, nativeOwnershipCovGID),
			final: true,
		},
	} {
		t.Run(accepted.name, func(t *testing.T) {
			require.NoError(t, validateGeneratedNativeAncestor(
				accepted.stat, accepted.final,
				nativeOwnershipCovUID, nativeOwnershipCovGID,
				nativeOwnershipCovUID, nativeOwnershipCovGID,
			), accepted.why)
		})
	}
}

// TestGeneratedNativeHandoffWalksARootOwnedAncestryUnderASharedIdentity proves
// the whole handoff accepts the shape a wrapper that never dropped privilege
// presents: its own identity is the native identity, and the generation hangs
// from root-owned directories it will never own. The effective identity is
// staged through its seams so the proof does not depend on which identity runs
// the tests; the chowns behind the handoff still need the root the rest of this
// file requires.
func TestGeneratedNativeHandoffWalksARootOwnedAncestryUnderASharedIdentity(t *testing.T) {
	root := nativeOwnershipCovTree(t)
	entry := filepath.Join(root, "entry")
	require.NoError(t, os.Chown(entry, int(nativeOwnershipCovUID), int(nativeOwnershipCovGID)))
	require.NoError(t, os.Chown(root, int(nativeOwnershipCovUID), int(nativeOwnershipCovGID)))

	nativeOwnershipCovSharedIdentity(t)

	require.NoError(t, handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation()))
	nativeOwnershipCovRequireNativeOwned(t, root, entry)

	require.NoError(t, os.Chown(filepath.Dir(root), 4242, 4242))

	err := handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation())
	require.ErrorContains(t, err, "generated native path ancestor is uid=4242 gid=4242")
	require.ErrorContains(t, err, "run the supervisor as root to isolate the agent identity")
}

// TestGeneratedNativeWalkValidatesTheFilesystemRootAsTheWholePath proves "/"
// reaches the ancestry validator as the final component rather than as an
// ancestor, so the filesystem root is held to the private-generation rule
// instead of the traversable-ancestor rule.
func TestGeneratedNativeWalkValidatesTheFilesystemRootAsTheWholePath(t *testing.T) {
	nativeOwnershipCovRequireRoot(t)

	require.ErrorContains(
		t, handoffGeneratedNativeTree("/", nativeOwnershipCovIsolation()), "generated native root mode",
	)
}

// TestGeneratedNativeWalkSurrendersTheRootDescriptorItOpened proves the walk of
// a whole-path root opens nothing beyond the descriptor it started from: the
// empty component is skipped rather than resolved, and the tree behind that
// exact descriptor is the tree handed to the native identity.
func TestGeneratedNativeWalkSurrendersTheRootDescriptorItOpened(t *testing.T) {
	root := nativeOwnershipCovTree(t)
	nativeOwnershipCovSeams(t)
	generatedNativeOpenFilesystemRoot = func() (int, error) {
		return unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}

	require.NoError(t, handoffGeneratedNativeTree("/", nativeOwnershipCovIsolation()))
	nativeOwnershipCovRequireNativeOwned(t, root, filepath.Join(root, "entry"))
}

// TestGeneratedNativeWalkFaultsFailClosed proves the walk aborts, and hands
// nothing over, when the kernel stops answering for a descriptor it already
// opened or accepted. Each fault is a syscall that cannot be made to fail for a
// descriptor the walk just validated, yet a failure there means the walk no
// longer knows what it is holding.
func TestGeneratedNativeWalkFaultsFailClosed(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		apply func(*testing.T, string)
	}{
		{
			name: "filesystem root cannot be opened",
			apply: func(t *testing.T, _ string) {
				t.Helper()
				generatedNativeOpenFilesystemRoot = func() (int, error) { return -1, unix.EIO }
			},
		},
		{
			name: "filesystem root cannot be inspected",
			apply: func(t *testing.T, _ string) {
				t.Helper()
				generatedNativeFstat = func(int, *unix.Stat_t) error { return unix.EIO }
			},
		},
		{
			name: "walked component cannot be inspected",
			apply: func(t *testing.T, root string) {
				t.Helper()
				inode := nativeOwnershipCovInode(t, root)
				generatedNativeFstat = func(fd int, stat *unix.Stat_t) error {
					if err := unix.Fstat(fd, stat); err != nil {
						return err
					}
					if stat.Ino == inode {
						return unix.EIO
					}

					return nil
				}
			},
		},
		{
			name: "walked ancestor cannot be released",
			apply: func(t *testing.T, _ string) {
				t.Helper()
				generatedNativeClose = func(int) error { return unix.EIO }
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := nativeOwnershipCovTree(t)
			nativeOwnershipCovSeams(t)
			testCase.apply(t, root)

			require.ErrorIs(t, handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation()), unix.EIO)
			nativeOwnershipCovRequireWrapperOwned(t, root, filepath.Join(root, "entry"))
		})
	}
}

// TestGeneratedNativeTraversalUsesTheApplicableModeClass proves traversability
// is decided by the single mode class the kernel would apply — owner, then
// group, then other — and never by a union of them. Production ancestry is
// always wrapper-owned, so only the "other" class is selected there; reading a
// union instead would silently accept an ancestor the native identity cannot
// enter the moment that stops being true.
func TestGeneratedNativeTraversalUsesTheApplicableModeClass(t *testing.T) {
	const (
		uid = uint32(65534)
		gid = uint32(65535)
	)

	for _, testCase := range []struct {
		name string
		stat unix.Stat_t
		want bool
	}{
		{name: "owner execute", stat: unix.Stat_t{Uid: uid, Gid: 0, Mode: 0o100}, want: true},
		{name: "owner without execute ignores group", stat: unix.Stat_t{Uid: uid, Gid: gid, Mode: 0o011}},
		{name: "group execute", stat: unix.Stat_t{Uid: 0, Gid: gid, Mode: 0o010}, want: true},
		{name: "group without execute ignores other", stat: unix.Stat_t{Uid: 0, Gid: gid, Mode: 0o101}},
		{name: "other execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o001}, want: true},
		{name: "other without execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o110}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, nativeIdentityCanTraverse(testCase.stat, uid, gid))
		})
	}
}

// TestGeneratedNativeEntryRefusesUnsafeInode proves the per-entry inspection
// refuses anything the wrapper did not create for this generation: an inode
// that is already owned by somebody else, a directory that is not exactly
// private, and any inode that is neither a directory nor a regular file. The
// generation directory is chowned last, so its retained wrapper ownership is
// proof that no refusal leaves a half-surrendered tree.
func TestGeneratedNativeEntryRefusesUnsafeInode(t *testing.T) {
	for _, testCase := range []struct {
		name string
		seed func(*testing.T, string)
		want string
	}{
		{
			name: "entry is already owned by another identity",
			seed: func(t *testing.T, root string) {
				t.Helper()
				require.NoError(t, os.Chown(
					filepath.Join(root, "entry"), int(nativeOwnershipCovUID), int(nativeOwnershipCovGID),
				))
			},
			want: "generated native inode owner changed to uid=65534 gid=65534",
		},
		{
			name: "nested directory is not exactly private",
			seed: func(t *testing.T, root string) {
				t.Helper()
				require.NoError(t, os.Mkdir(filepath.Join(root, "nested"), 0o750))
			},
			want: "generated native directory mode 0750 is unsafe",
		},
		{
			name: "entry is neither a directory nor a regular file",
			seed: func(t *testing.T, root string) {
				t.Helper()
				require.NoError(t, unix.Mkfifo(filepath.Join(root, "pipe"), 0o600))
			},
			want: "generated native inode has unsupported type",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := nativeOwnershipCovTree(t)
			testCase.seed(t, root)

			require.ErrorContains(
				t, handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation()), testCase.want,
			)
			nativeOwnershipCovRequireWrapperOwned(t, root)
		})
	}
}

// TestGeneratedNativeInodeRereadDisagreeingWithTheFirstIsRefused proves every
// re-read of an inode is load-bearing rather than a restatement of the read
// before it. An entry can be replaced between the moment its type is read and
// the moment it is validated, and the ownership transfer is only complete if
// the kernel agrees the inode it just changed is still the one that was
// inspected. A disagreement is a refusal, and the tree stays with the wrapper.
func TestGeneratedNativeInodeRereadDisagreeingWithTheFirstIsRefused(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		observe func(int, *unix.Stat_t) error
		want    string
	}{
		{
			name: "type changed between routing and validation",
			observe: func(reads int, stat *unix.Stat_t) error {
				if reads == 2 {
					stat.Mode = unix.S_IFDIR | 0o700
				}

				return nil
			},
			want: "generated native inode type changed",
		},
		{
			name: "ownership transfer is not reflected by the inode",
			observe: func(reads int, stat *unix.Stat_t) error {
				if reads == 3 {
					stat.Uid = uint32(os.Geteuid())
					stat.Gid = uint32(os.Getegid())
				}

				return nil
			},
			want: "generated native inode ownership handoff could not be proven",
		},
		{
			name: "link count changed after the ownership transfer",
			observe: func(reads int, stat *unix.Stat_t) error {
				if reads == 3 {
					stat.Nlink = 2
				}

				return nil
			},
			want: "generated native inode ownership handoff could not be proven",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := nativeOwnershipCovTree(t)
			nativeOwnershipCovSeams(t)
			nativeOwnershipCovObserveEntry(t, root, testCase.observe)

			require.ErrorContains(
				t, handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation()), testCase.want,
			)
			nativeOwnershipCovRequireWrapperOwned(t, root)
		})
	}
}

// TestGeneratedNativeInodeFaultsFailClosed proves the handoff aborts whenever
// the kernel stops answering for an entry descriptor it is holding open, at
// each of the three points the entry is read: the type read that routes it, the
// validation read, and the read that proves the ownership transfer landed. The
// generation directory keeps wrapper ownership in every case.
func TestGeneratedNativeInodeFaultsFailClosed(t *testing.T) {
	for reads := 1; reads <= 3; reads++ {
		t.Run("entry read "+strconv.Itoa(reads), func(t *testing.T) {
			root := nativeOwnershipCovTree(t)
			nativeOwnershipCovSeams(t)
			faultAt := reads
			nativeOwnershipCovObserveEntry(t, root, func(observed int, _ *unix.Stat_t) error {
				if observed == faultAt {
					return unix.EIO
				}

				return nil
			})

			require.ErrorIs(t, handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation()), unix.EIO)
			nativeOwnershipCovRequireWrapperOwned(t, root)
		})
	}
}

// TestGeneratedNativeTransferFaultsFailClosed proves the handoff refuses when
// the kernel will not enumerate the generation it already validated, and when
// it will not transfer ownership of an entry. Neither failure may be treated as
// a completed handoff: the generation directory itself is transferred last and
// must still belong to the wrapper.
func TestGeneratedNativeTransferFaultsFailClosed(t *testing.T) {
	t.Run("generation cannot be enumerated", func(t *testing.T) {
		root := nativeOwnershipCovTree(t)
		nativeOwnershipCovSeams(t)
		generatedNativeReadDir = func(*os.File) ([]os.DirEntry, error) { return nil, unix.EIO }

		require.ErrorIs(t, handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation()), unix.EIO)
		nativeOwnershipCovRequireWrapperOwned(t, root, filepath.Join(root, "entry"))
	})

	t.Run("entry ownership cannot be transferred", func(t *testing.T) {
		root := nativeOwnershipCovTree(t)
		nativeOwnershipCovSeams(t)
		generatedNativeFchown = func(int, int, int) error { return unix.EROFS }

		require.ErrorIs(t, handoffGeneratedNativeTree(root, nativeOwnershipCovIsolation()), unix.EROFS)
		nativeOwnershipCovRequireWrapperOwned(t, root, filepath.Join(root, "entry"))
	})
}

// nativeOwnershipCovObserveEntry installs a stat seam that counts how many
// times the generation's single entry has been read and hands each read to the
// caller, so a test can fault or contradict one exact read of one exact inode
// without disturbing the reads of every other inode in the walk.
func nativeOwnershipCovObserveEntry(t *testing.T, root string, observe func(int, *unix.Stat_t) error) {
	t.Helper()
	inode := nativeOwnershipCovInode(t, filepath.Join(root, "entry"))
	reads := 0
	generatedNativeFstat = func(fd int, stat *unix.Stat_t) error {
		if err := unix.Fstat(fd, stat); err != nil {
			return err
		}
		if stat.Ino != inode {
			return nil
		}
		reads++

		return observe(reads, stat)
	}
}

func nativeOwnershipCovTree(t *testing.T) string {
	t.Helper()
	nativeOwnershipCovRequireRoot(t)
	// The generation must sit under an ancestry the native identity can
	// traverse, which the test temporary root deliberately is not.
	parent, err := os.MkdirTemp("/var/lib", "acp-go-hermes-generated-cov-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))
	root := filepath.Join(parent, "generation")
	require.NoError(t, os.Mkdir(root, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "entry"), []byte("generated"), 0o600))

	return root
}

func nativeOwnershipCovIsolation() *ProcessIsolation {
	return &ProcessIsolation{
		UID: nativeOwnershipCovUID, GID: nativeOwnershipCovGID, BaseEnvironment: map[string]string{},
	}
}

func nativeOwnershipCovSeams(t *testing.T) {
	t.Helper()
	openRoot := generatedNativeOpenFilesystemRoot
	fstat := generatedNativeFstat
	closeFD := generatedNativeClose
	fchown := generatedNativeFchown
	readDir := generatedNativeReadDir
	t.Cleanup(func() {
		generatedNativeOpenFilesystemRoot = openRoot
		generatedNativeFstat = fstat
		generatedNativeClose = closeFD
		generatedNativeFchown = fchown
		generatedNativeReadDir = readDir
	})
}

// nativeOwnershipCovSharedIdentity stages the effective identity the wrapper
// reads so the walk sees the native identity as its own, which is the shape a
// wrapper that never dropped privilege presents. The process stays root, so the
// fixture's chowns keep working.
func nativeOwnershipCovSharedIdentity(t *testing.T) {
	t.Helper()
	uid, gid := effectiveUIDSource, effectiveGIDSource
	t.Cleanup(func() { effectiveUIDSource, effectiveGIDSource = uid, gid })
	effectiveUIDSource = func() int { return int(nativeOwnershipCovUID) }
	effectiveGIDSource = func() int { return int(nativeOwnershipCovGID) }
}

func nativeOwnershipCovInode(t *testing.T, path string) uint64 {
	t.Helper()
	var stat unix.Stat_t
	require.NoError(t, unix.Lstat(path, &stat))

	return stat.Ino
}

func nativeOwnershipCovRequireWrapperOwned(t *testing.T, paths ...string) {
	t.Helper()
	nativeOwnershipCovRequireOwner(t, uint32(os.Geteuid()), uint32(os.Getegid()), paths...)
}

func nativeOwnershipCovRequireNativeOwned(t *testing.T, paths ...string) {
	t.Helper()
	nativeOwnershipCovRequireOwner(t, nativeOwnershipCovUID, nativeOwnershipCovGID, paths...)
}

func nativeOwnershipCovRequireOwner(t *testing.T, uid uint32, gid uint32, paths ...string) {
	t.Helper()
	for _, path := range paths {
		var stat unix.Stat_t
		require.NoError(t, unix.Lstat(path, &stat))
		require.Equal(t, []uint32{uid, gid}, []uint32{stat.Uid, stat.Gid}, path)
	}
}

func nativeOwnershipCovRequireRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("generated native ownership handoff requires the trusted root identity")
	}
}
