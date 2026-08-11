//go:build linux

package hermesacp

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/stretchr/testify/require"

	"golang.org/x/sys/unix"
)

const (
	nativeOwnershipTestUID = uint32(65534)
	nativeOwnershipTestGID = uint32(65534)
)

func requireNativeOwnershipRoot(t *testing.T) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
}

// nativeOwnershipTestHome builds the shape the check accepts: a 0711
// root-owned caller root holding a 0700 directory owned outright by the native
// identity.
func nativeOwnershipTestHome(t *testing.T) string {
	t.Helper()
	requireNativeOwnershipRoot(t)

	parent, err := os.MkdirTemp("/tmp", "acp-go-hermes-native-owned-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))

	home := filepath.Join(parent, "home")
	require.NoError(t, os.Mkdir(home, 0o700))
	require.NoError(t, os.Chown(home, int(nativeOwnershipTestUID), int(nativeOwnershipTestGID)))

	return home
}

func nativeOwnershipTestIsolation() *ProcessIsolation {
	return &ProcessIsolation{UID: nativeOwnershipTestUID, GID: nativeOwnershipTestGID}
}

// TestNativeOwnedDirectoryAcceptsTheNativeIdentityHome proves the accepted
// shape really is accepted, so every refusal case below is a refusal of
// something the check distinguishes rather than a blanket failure.
func TestNativeOwnedDirectoryAcceptsTheNativeIdentityHome(t *testing.T) {
	home := nativeOwnershipTestHome(t)

	require.NoError(t, validateNativeOwnedDirectory(home, nativeOwnershipTestIsolation()))
}

// TestStrictPolicySessionValidatesTheSharedHermesHomeAtTheAdapterBoundary is
// the adapter-level proof for the policy-conditional provider-auth check. It
// drives NewSession as a trusted root with a distinct target identity, captures
// the native launch policy and durable auth residence, then reaches the auth
// surface through that live session. Ordinary-mode tests cannot exercise this
// ownership walk because a nil policy intentionally skips it.
func TestStrictPolicySessionValidatesTheSharedHermesHomeAtTheAdapterBoundary(t *testing.T) {
	requireNativeOwnershipRoot(t)

	authHome := testNativeOwnedDir(t, "native-auth")
	client := newFakeHermesClient()
	client.createSession = testNativeSession("native-strict-auth")
	client.getSession = client.createSession
	client.authProviders = []nativehermes.AuthProvider{{
		ID: "xai-oauth", Name: "xAI", Flow: nativehermes.AuthFlowDeviceCode,
	}}

	var starts []nativehermes.StartOptions
	agent := newIsolatedTestAgent(
		WithScratchDir(t.TempDir()),
		WithProviderAuthRoot(t.TempDir()),
		WithSharedHermesHome(authHome),
		func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				starts = append(starts, opts)

				xdg, err := nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
				if err != nil {
					return nil, err
				}
				client.xdg = xdg

				return client, nil
			}
		},
	)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.Len(t, starts, 1)
	require.NotNil(t, starts[0].Isolation)
	require.Equal(t, authHome, starts[0].SharedHermesHome)

	methods, err := callLeg(t, agent, AuthMethodsMethod, map[string]any{
		"sessionId": string(created.SessionId),
	})
	require.NoError(t, err)
	require.NotNil(t, methods)
}

// TestNativeOwnedDirectoryWithoutIsolationIsNotChecked proves the check is
// scoped to isolated sessions. Without process isolation the shared Hermes home
// stays under the wrapper's own identity, and demanding a foreign owner would
// refuse every unisolated session.
func TestNativeOwnedDirectoryWithoutIsolationIsNotChecked(t *testing.T) {
	require.NoError(t, validateNativeOwnedDirectory("relative/does-not-exist", nil))
}

// TestNativeOwnershipTraversalRejectsRelativeRoot proves a relative root is
// refused before any walk: a relative walk resolves against the working
// directory rather than the named tree.
func TestNativeOwnershipTraversalRejectsRelativeRoot(t *testing.T) {
	err := validateNativeOwnedDirectory("relative/home", nativeOwnershipTestIsolation())
	require.ErrorContains(t, err, "native path must be absolute")
}

// TestNativeOwnershipTraversalValidatesFilesystemRootBeforeComponents proves
// the filesystem root is validated before any component is opened, so a
// compromised "/" cannot be walked through on the way to a trusted leaf.
func TestNativeOwnershipTraversalValidatesFilesystemRootBeforeComponents(t *testing.T) {
	requireNativeOwnershipRoot(t)

	var seen []bool

	wantErr := errors.New("root ancestry refused")
	directory, err := openNativeOwnershipDirectory("/etc/hosts", func(_ unix.Stat_t, final bool) error {
		seen = append(seen, final)

		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, directory)
	require.Equal(t, []bool{false}, seen, "traversal continued past a refused filesystem root")
}

// TestNativeOwnershipTraversalOpensFilesystemRootItself proves "/" is a valid
// traversal target and is presented to the validator as the final component
// rather than as an ancestor.
func TestNativeOwnershipTraversalOpensFilesystemRootItself(t *testing.T) {
	requireNativeOwnershipRoot(t)

	var seen []bool

	directory, err := openNativeOwnershipDirectory("/", func(_ unix.Stat_t, final bool) error {
		seen = append(seen, final)

		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })
	require.Equal(t, []bool{true}, seen)

	var opened, root unix.Stat_t
	require.NoError(t, unix.Fstat(int(directory.Fd()), &opened))
	require.NoError(t, unix.Stat("/", &root))
	require.Equal(t, root.Ino, opened.Ino)
	require.Equal(t, root.Dev, opened.Dev)
}

// TestNativeOwnershipTraversalPropagatesMissingComponent proves a missing
// component surfaces the kernel's own error rather than being treated as an
// absent directory that needs no check.
func TestNativeOwnershipTraversalPropagatesMissingComponent(t *testing.T) {
	home := nativeOwnershipTestHome(t)

	err := validateNativeOwnedDirectory(
		filepath.Join(filepath.Dir(home), "absent"), nativeOwnershipTestIsolation(),
	)
	require.ErrorIs(t, err, unix.ENOENT)
}

// TestNativeOwnershipTraversalFailsClosedOnKernelFaults proves each descriptor
// syscall the traversal depends on aborts the walk when it fails. A traversal
// that swallowed any of these would return a descriptor whose ancestry it never
// actually proved.
func TestNativeOwnershipTraversalFailsClosedOnKernelFaults(t *testing.T) {
	requireNativeOwnershipRoot(t)

	accept := func(unix.Stat_t, bool) error { return nil }

	t.Run("filesystem root unopenable", func(t *testing.T) {
		previous := nativeOwnershipOpenFilesystemRoot
		nativeOwnershipOpenFilesystemRoot = func() (int, error) { return -1, unix.EMFILE }

		t.Cleanup(func() { nativeOwnershipOpenFilesystemRoot = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EMFILE)
		require.Nil(t, directory)
	})

	t.Run("filesystem root unstattable", func(t *testing.T) {
		previous := nativeOwnershipFstat
		nativeOwnershipFstat = func(int, *unix.Stat_t) error { return unix.EIO }

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})

	t.Run("component unstattable", func(t *testing.T) {
		previous := nativeOwnershipFstat
		calls := 0
		nativeOwnershipFstat = func(fd int, stat *unix.Stat_t) error {
			calls++
			if calls == 1 {
				return previous(fd, stat)
			}

			return unix.EIO
		}

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
		require.Equal(t, 2, calls, "traversal statted past the faulted component")
	})

	t.Run("parent descriptor unreleasable", func(t *testing.T) {
		previous := nativeOwnershipClose
		nativeOwnershipClose = func(fd int) error {
			_ = previous(fd)

			return unix.EIO
		}

		t.Cleanup(func() { nativeOwnershipClose = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})
}

// TestDurableNativeAncestorStatesEachRefusal pins the exact reason the
// native-owned ancestry validator refuses each unsafe shape. These reasons are
// the containment contract for the shared Hermes home: only the wrapper or the
// native identity may own any ancestor, a writable ancestor is tolerated only
// when the wrapper owns it and it is sticky, the leaf must be owned outright by
// the native identity with full owner rights, and every ancestor must be
// traversable by that identity.
func TestDurableNativeAncestorStatesEachRefusal(t *testing.T) {
	const (
		trustedUID = uint32(0)
		trustedGID = uint32(0)
	)

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
			want: "ancestry is not a directory",
		},
		{
			name: "ancestor owned by a third identity",
			stat: directory(0o755, 4242, 4242),
			want: "ancestor is uid=4242 gid=4242",
		},
		{
			name: "group-writable trusted ancestor without sticky bit",
			stat: directory(0o771, trustedUID, trustedGID),
			want: "ancestor mode 0771 is writable",
		},
		{
			name: "world-writable target-owned ancestor even with sticky bit",
			stat: directory(0o1777, nativeOwnershipTestUID, nativeOwnershipTestGID),
			want: "ancestor mode 01777 is writable",
		},
		{
			name:  "leaf owned by the wrapper rather than the native identity",
			stat:  directory(0o700, trustedUID, trustedGID),
			final: true,
			want:  "not safely owned by the target identity",
		},
		{
			name:  "leaf without full owner rights",
			stat:  directory(0o600, nativeOwnershipTestUID, nativeOwnershipTestGID),
			final: true,
			want:  "not safely owned by the target identity",
		},
		{
			name: "ancestor the native identity cannot traverse",
			stat: directory(0o700, trustedUID, trustedGID),
			want: "not traversable by the target identity",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateDurableNativeAncestor(
				testCase.stat, testCase.final, trustedUID, trustedGID,
				nativeOwnershipTestUID, nativeOwnershipTestGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	for _, accepted := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
	}{
		{name: "traversable trusted ancestor", stat: directory(0o711, trustedUID, trustedGID)},
		{name: "sticky writable trusted ancestor", stat: directory(0o1777, trustedUID, trustedGID)},
		{
			name:  "leaf owned outright by the native identity",
			stat:  directory(0o700, nativeOwnershipTestUID, nativeOwnershipTestGID),
			final: true,
		},
	} {
		t.Run(accepted.name, func(t *testing.T) {
			require.NoError(t, validateDurableNativeAncestor(
				accepted.stat, accepted.final, trustedUID, trustedGID,
				nativeOwnershipTestUID, nativeOwnershipTestGID,
			))
		})
	}
}

// TestNativeIdentityTraversalUsesTheApplicableModeClass proves traversability
// is decided by the single mode class the kernel would apply — owner, then
// group, then other — and never by a union of them. Reading the wrong class
// would accept a path the native identity cannot enter, or refuse one it can.
func TestNativeIdentityTraversalUsesTheApplicableModeClass(t *testing.T) {
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

// TestNativeOwnedDirectoryRecheckDisagreeingWithTheWalkIsRefused proves the
// final inspection of the opened descriptor is load-bearing rather than a
// restatement of the walk. The walk validates the path a component at a time
// and the leaf could be replaced between the last openat and the moment the
// descriptor is used, so the check re-reads the descriptor it actually holds
// and refuses on any disagreement.
func TestNativeOwnedDirectoryRecheckDisagreeingWithTheWalkIsRefused(t *testing.T) {
	home := nativeOwnershipTestHome(t)

	// The traversal stats every ancestor before the final inspection of the
	// descriptor it kept, so learn how many stats a clean run takes and fault
	// only the last one.
	baseline := nativeOwnershipFstat
	total := 0
	nativeOwnershipFstat = func(fd int, stat *unix.Stat_t) error {
		total++

		return baseline(fd, stat)
	}

	require.NoError(t, validateNativeOwnedDirectory(home, nativeOwnershipTestIsolation()))
	nativeOwnershipFstat = baseline
	require.Greater(t, total, 1, "traversal made no ancestor stat before the final inspection")

	faultFinalStat := func(t *testing.T, replace func(*unix.Stat_t) error) {
		t.Helper()

		calls := 0
		nativeOwnershipFstat = func(fd int, stat *unix.Stat_t) error {
			calls++
			if err := baseline(fd, stat); err != nil {
				return err
			}
			if calls == total {
				return replace(stat)
			}

			return nil
		}

		t.Cleanup(func() { nativeOwnershipFstat = baseline })
	}

	t.Run("descriptor stops answering", func(t *testing.T) {
		faultFinalStat(t, func(*unix.Stat_t) error { return unix.EIO })

		err := validateNativeOwnedDirectory(home, nativeOwnershipTestIsolation())
		require.ErrorContains(t, err, "inspect native-owned directory")
		require.ErrorIs(t, err, unix.EIO)
	})

	t.Run("descriptor is no longer a directory", func(t *testing.T) {
		faultFinalStat(t, func(stat *unix.Stat_t) error {
			stat.Mode = unix.S_IFREG | 0o700

			return nil
		})

		err := validateNativeOwnedDirectory(home, nativeOwnershipTestIsolation())
		require.ErrorContains(t, err, "native-owned path is not a directory")
	})

	t.Run("descriptor is owned by another identity", func(t *testing.T) {
		faultFinalStat(t, func(stat *unix.Stat_t) error {
			stat.Uid = 4242

			return nil
		})

		err := validateNativeOwnedDirectory(home, nativeOwnershipTestIsolation())
		require.ErrorContains(t, err, "is uid=4242 gid=65534, want uid=65534 gid=65534")
	})

	t.Run("descriptor became writable by others", func(t *testing.T) {
		faultFinalStat(t, func(stat *unix.Stat_t) error {
			stat.Mode = unix.S_IFDIR | 0o702

			return nil
		})

		err := validateNativeOwnedDirectory(home, nativeOwnershipTestIsolation())
		require.ErrorContains(t, err, "native-owned directory mode 0702 is unsafe")
	})
}

// TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer proves the
// effective-id helpers refuse rather than narrow. Every caller compares their
// result against an inode's 32-bit owner, so an answer outside that width must
// not be truncated into an id a real inode could carry — the truncation of an
// answer one past the 32-bit range is 0, which is root. Linux stores its ids in
// 32 bits and cannot produce such an answer, so it is staged through the seams
// the helpers read.
func TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer(t *testing.T) {
	realUID, realGID := effectiveUIDSource, effectiveGIDSource
	t.Cleanup(func() { effectiveUIDSource, effectiveGIDSource = realUID, realGID })

	effectiveUIDSource = func() int { return -1 }
	effectiveGIDSource = func() int { return -1 }
	require.Equal(t, uint32(math.MaxUint32), effectiveUID())
	require.Equal(t, uint32(math.MaxUint32), effectiveGID())

	effectiveUIDSource = func() int { return math.MaxUint32 + 1 }
	effectiveGIDSource = func() int { return math.MaxUint32 + 1 }

	uid, gid := effectiveUID(), effectiveGID()
	require.Equal(t, uint32(math.MaxUint32), uid)
	require.Equal(t, uint32(math.MaxUint32), gid)
	require.NotZero(t, uid, "narrowing this answer would have claimed root")
	require.NotZero(t, gid, "narrowing this answer would have claimed the root group")

	effectiveUIDSource = func() int { return 65534 }
	effectiveGIDSource = func() int { return 65533 }
	require.Equal(t, uint32(65534), effectiveUID())
	require.Equal(t, uint32(65533), effectiveGID())
}
