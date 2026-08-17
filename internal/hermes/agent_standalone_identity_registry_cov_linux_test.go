//go:build linux

package hermes

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

const agentStandaloneCovSuffix = "0123456789abcdef01234567"

// agentStandaloneCovRemovedDirectory models the authority registry being
// removed from the filesystem while a claim still holds its descriptor. The
// descriptor stays open, but every name lookup and listing through it fails
// with ENOENT.
func agentStandaloneCovRemovedDirectory(t *testing.T) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent-identities")
	require.NoError(t, os.Mkdir(path, 0o700))
	directory, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	require.NoError(t, os.Remove(path))

	return directory
}

// TestAgentStandaloneCovRemovedRegistryRootRefusesEveryTraversal proves that
// once the authority root is gone from the filesystem, every listing and
// lock-acquisition path built on its descriptor refuses instead of behaving as
// though the registry were pristine. Treating a vanished registry as empty
// would re-mint owners.lock and the domain lock into a detached inode nobody
// else can see.
func TestAgentStandaloneCovRemovedRegistryRootRefusesEveryTraversal(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	deadline := time.Now().Add(time.Second)

	t.Run("listing", func(t *testing.T) {
		entries, err := agentStandaloneAuthorityEntries(agentStandaloneCovRemovedDirectory(t))
		require.ErrorIs(t, err, unix.ENOENT)
		require.Nil(t, entries)
	})

	t.Run("missing domain lock", func(t *testing.T) {
		lease, err := acquireAgentStandaloneMissingDomainLock(
			agentStandaloneCovRemovedDirectory(t), ownerUID, ownerGID, deadline, nil, nil,
		)
		require.ErrorIs(t, err, unix.ENOENT)
		require.Nil(t, lease)
	})

	t.Run("owners exclusive", func(t *testing.T) {
		lock, err := acquireAgentStandaloneOwnersExclusive(
			agentStandaloneCovRemovedDirectory(t), ownerUID, ownerGID, deadline, nil, nil,
		)
		require.ErrorIs(t, err, unix.ENOENT)
		require.Nil(t, lock)
	})

	t.Run("uid lock creation check", func(t *testing.T) {
		require.ErrorIs(t,
			validateAgentStandaloneUIDLockMayBeCreated(agentStandaloneCovRemovedDirectory(t), 62431),
			unix.ENOENT,
		)
	})

	t.Run("owner temporary probe", func(t *testing.T) {
		present, err := agentStandaloneOwnerTemporariesPresent(agentStandaloneCovRemovedDirectory(t))
		require.ErrorIs(t, err, unix.ENOENT)
		require.False(t, present)
	})

	t.Run("owner temporary drain", func(t *testing.T) {
		cleaned, busy, err := drainAgentStandaloneOwnerTemporaries(
			agentStandaloneCovRemovedDirectory(t), ownerUID, ownerGID, deadline, nil, nil,
		)
		require.ErrorIs(t, err, unix.ENOENT)
		require.False(t, cleaned)
		require.False(t, busy)
	})

	t.Run("owner temporary drain under lock", func(t *testing.T) {
		cleaned, busy, err := drainAgentStandaloneOwnerTemporariesUnderLock(
			agentStandaloneCovRemovedDirectory(t), ownerUID, ownerGID, deadline, nil, nil,
		)
		require.ErrorIs(t, err, unix.ENOENT)
		require.False(t, cleaned)
		require.False(t, busy)
	})

	t.Run("matching domain adjudication", func(t *testing.T) {
		requiresExclusive, err := adjudicateAgentStandaloneMatchingDomainTemporaries(
			agentStandaloneCovRemovedDirectory(t), ownerUID, ownerGID, false,
		)
		require.ErrorIs(t, err, unix.ENOENT)
		require.False(t, requiresExclusive)
	})

	t.Run("authority root audit", func(t *testing.T) {
		require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
			agentStandaloneCovRemovedDirectory(t), ownerUID, ownerGID, false, false, false, deadline, nil, nil,
		), unix.ENOENT)
	})
}

// TestAgentStandaloneCovUIDLockCreationIsAllowedOnlyWhenTheLockExists proves
// the pre-creation check passes when the permanent UID lock is already present
// and refuses only when registry state for that UID survives without it.
func TestAgentStandaloneCovUIDLockCreationIsAllowedOnlyWhenTheLockExists(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	require.NoError(t, validateAgentStandaloneUIDLockMayBeCreated(directory, 62433))
	lock := createAgentStandaloneTestLock(t, directory, "62433.lock", ownerUID, ownerGID)
	require.NoError(t, lock.Close())

	require.NoError(t, validateAgentStandaloneUIDLockMayBeCreated(directory, 62433))
}

// TestAgentStandaloneCovOwnersLockCreatedByAPeerIsJoinedNotRecreated proves
// that when the permanent owners.lock appears between the failed open and the
// registry listing, the claim joins the peer's inode instead of creating a
// second one. Two owners.lock inodes would mean two claims believing they hold
// the same registry-wide mutex.
func TestAgentStandaloneCovOwnersLockCreatedByAPeerIsJoinedNotRecreated(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	peer := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, peer.Close())
	var before, after unix.Stat_t
	require.NoError(t, unix.Fstatat(int(directory.Fd()), "owners.lock", &before, unix.AT_SYMLINK_NOFOLLOW))
	restoreAgentStandalonePermanentLockSeams(t)
	original := agentStandaloneLockOpenat
	faulted := false
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		if !faulted && path == "owners.lock" {
			faulted = true

			return -1, unix.ENOENT
		}

		return original(dirfd, path, flags, mode)
	}

	lock, err := acquireAgentStandaloneOwnersExclusive(
		directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.True(t, faulted)
	require.NoError(t, lock.Close())
	require.NoError(t, unix.Fstatat(int(directory.Fd()), "owners.lock", &after, unix.AT_SYMLINK_NOFOLLOW))
	require.Equal(t, before.Ino, after.Ino, "the peer owners.lock inode must be reused")
	require.Equal(t, before.Dev, after.Dev)
}

// TestAgentStandaloneCovNamedLockAcquisitionHonoursTheClaimBudget proves the
// blocking lock helper refuses as soon as the budget is gone — both before it
// ever tries to flock and while it is waiting behind a live holder — and that
// it releases the descriptor it opened on the way out. Leaking that descriptor
// would keep an unreferenced open file on the registry after the claim failed.
func TestAgentStandaloneCovNamedLockAcquisitionHonoursTheClaimBudget(t *testing.T) {
	t.Run("budget already gone", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		lock := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
		require.NoError(t, lock.Close())

		acquired, err := acquireAgentStandaloneNamedLock(
			directory, "owners.lock", unix.LOCK_EX, false,
			ownerUID, ownerGID, time.Now().Add(-time.Millisecond), nil, nil,
		)
		require.Nil(t, acquired)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		contender, taken, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, taken, "the refused acquisition must not retain the lock")
		require.NoError(t, contender.Close())
	})

	t.Run("budget expires behind a live holder", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		held := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))

		acquired, err := acquireAgentStandaloneNamedLock(
			directory, "owners.lock", unix.LOCK_EX, false,
			ownerUID, ownerGID, time.Now().Add(40*time.Millisecond), nil, nil,
		)
		require.Nil(t, acquired)
		require.ErrorContains(t, err, "exceeded 30 seconds")
	})

	t.Run("missing permanent lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

		file, taken, err := tryAgentStandaloneNamedLock(directory, "62435.lock", false, ownerUID, ownerGID)
		require.ErrorIs(t, err, unix.ENOENT)
		require.False(t, taken)
		require.Nil(t, file)
	})
}

// TestAgentStandaloneCovTemporaryNamesMustBeExactlyWellFormed proves every
// temporary-name parser refuses a name whose UID text, marker infix or 24-hex
// random suffix is not exact. These names decide which permanent UID lock must
// be held before a temporary may be unlinked, so a loose parse would let a
// crafted name have an unrelated UID's temporary removed.
func TestAgentStandaloneCovTemporaryNamesMustBeExactlyWellFormed(t *testing.T) {
	t.Run("uid text", func(t *testing.T) {
		for _, text := range []string{"", "0", "01", "-1", "4294967296", "62431 ", "0x10"} {
			uid, err := parseAgentStandaloneUID(text)
			require.ErrorContains(t, err, "invalid uid")
			require.Zero(t, uid)
		}
		uid, err := parseAgentStandaloneUID("62431")
		require.NoError(t, err)
		require.Equal(t, uint32(62431), uid)
	})

	t.Run("domain and probe suffixes", func(t *testing.T) {
		require.ErrorContains(t,
			parseAgentStandaloneTemporarySuffix("someone-elses-file", "domain.json.next-", false),
			"invalid name",
		)
		require.ErrorContains(t,
			parseAgentStandaloneTemporarySuffix("domain.json.next-zzzzzzzzzzzzzzzzzzzzzzzz", "domain.json.next-", false),
			"invalid name",
		)
		require.ErrorContains(t,
			parseAgentStandaloneTemporarySuffix("domain.json.next-0123456789ABCDEF01234567", "domain.json.next-", false),
			"invalid name",
		)
		require.NoError(t, parseAgentStandaloneTemporarySuffix(
			"domain.json.next-"+agentStandaloneCovSuffix, "domain.json.next-", false,
		))
		require.NoError(t, parseAgentStandaloneTemporarySuffix(
			".authority-probe-"+agentStandaloneCovSuffix+".renamed", ".authority-probe-", true,
		))
	})

	t.Run("owner temporaries", func(t *testing.T) {
		for _, name := range []string{
			"owner.next-" + agentStandaloneCovSuffix,
			"not-a-uid.owner.next-" + agentStandaloneCovSuffix,
			"62431.owner.next-short",
			"62431.owner.next-0123456789ABCDEF01234567",
			"62431.owner.next-zzzzzzzzzzzzzzzzzzzzzzzz",
		} {
			uid, err := parseAgentStandaloneOwnerTemporary(name)
			require.ErrorContains(t, err, "invalid name")
			require.Zero(t, uid)
		}
		uid, err := parseAgentStandaloneOwnerTemporary("62431.owner.next-" + agentStandaloneCovSuffix)
		require.NoError(t, err)
		require.Equal(t, uint32(62431), uid)
	})

	t.Run("marker temporaries", func(t *testing.T) {
		for _, name := range []string{
			"quarantine.next-" + agentStandaloneCovSuffix,
			"not-a-uid.quarantine.next-" + agentStandaloneCovSuffix,
			"62431.quarantine.next-short",
			"62431.quarantine.next-zzzzzzzzzzzzzzzzzzzzzzzz",
		} {
			uid, err := parseAgentStandaloneMarkerTemporary(name)
			require.ErrorContains(t, err, "invalid name")
			require.Zero(t, uid)
		}
		uid, err := parseAgentStandaloneMarkerTemporary("62431.quarantine.next-" + agentStandaloneCovSuffix)
		require.NoError(t, err)
		require.Equal(t, uint32(62431), uid)
	})
}

// TestAgentStandaloneCovTemporaryMustBeATrustedBoundedRegularFile proves a
// temporary is only ever accepted when it is the registry owner's own bounded
// 0600 regular file with a single link, and that an absent name is refused
// rather than treated as already cleaned.
func TestAgentStandaloneCovTemporaryMustBeATrustedBoundedRegularFile(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	name := "62437.quarantine.next-" + agentStandaloneCovSuffix
	require.ErrorIs(t,
		validateAgentStandaloneTemporary(directory, name, ownerUID, ownerGID, agentStandaloneMarkerMax),
		unix.ENOENT,
	)

	path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
	require.NoError(t, validateAgentStandaloneTemporary(
		directory, name, ownerUID, ownerGID, agentStandaloneMarkerMax,
	))
	require.ErrorContains(t,
		validateAgentStandaloneTemporary(directory, name, ownerUID, ownerGID, 1),
		"not a trusted bounded regular file",
	)
	require.NoError(t, os.Chmod(path, 0o644))
	require.ErrorContains(t,
		validateAgentStandaloneTemporary(directory, name, ownerUID, ownerGID, agentStandaloneMarkerMax),
		"not a trusted bounded regular file",
	)
}

// TestAgentStandaloneCovTemporaryCleanupRefusesUntrustedInput proves each
// temporary cleanup refuses a malformed name and an untrusted file, and leaves
// the entry on disk. Unlinking on a failed check would let a crafted name have
// the registry delete something it never validated.
func TestAgentStandaloneCovTemporaryCleanupRefusesUntrustedInput(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("owner temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		require.ErrorContains(t,
			cleanupAgentStandaloneOwnerTemporary(directory, "62441.owner.next-nope", ownerUID, ownerGID),
			"invalid name",
		)
		name := "62441.owner.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		require.NoError(t, os.Chmod(path, 0o644))
		require.ErrorContains(t,
			cleanupAgentStandaloneOwnerTemporary(directory, name, ownerUID, ownerGID),
			"not a trusted bounded regular file",
		)
		require.FileExists(t, path)
	})

	t.Run("domain temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		require.ErrorContains(t,
			cleanupAgentStandaloneDomainTemporary(directory, "domain.json.next-nope", ownerUID, ownerGID),
			"invalid name",
		)
		name := "domain.json.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		require.NoError(t, os.Chmod(path, 0o644))
		require.ErrorContains(t,
			cleanupAgentStandaloneDomainTemporary(directory, name, ownerUID, ownerGID),
			"not a trusted bounded regular file",
		)
		require.FileExists(t, path)
	})

	t.Run("probe temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		require.ErrorIs(t,
			cleanupAgentStandaloneProbeTemporary(
				directory, ".authority-probe-"+agentStandaloneCovSuffix, ownerUID, ownerGID,
			),
			unix.ENOENT,
		)
	})

	t.Run("marker temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "62443.quarantine.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		require.ErrorIs(t,
			cleanupAgentStandaloneMarkerTemporary(directory, 62443, name, ownerUID, ownerGID),
			unix.ENOENT,
		)
		require.FileExists(t, path)

		held := createAgentStandaloneTestLock(t, directory, "62443.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		require.ErrorIs(t,
			cleanupAgentStandaloneMarkerTemporary(directory, 62443, name, ownerUID, ownerGID),
			errAgentStandaloneMarkerTempBusy,
		)
		require.FileExists(t, path)

		require.NoError(t, held.Close())
		require.NoError(t, os.Chmod(path, 0o644))
		require.ErrorContains(t,
			cleanupAgentStandaloneMarkerTemporary(directory, 62443, name, ownerUID, ownerGID),
			"not a trusted bounded regular file",
		)
		require.FileExists(t, path)

		require.NoError(t, os.Chmod(path, 0o600))
		require.NoError(t, cleanupAgentStandaloneMarkerTemporary(directory, 62443, name, ownerUID, ownerGID))
		require.NoFileExists(t, path)
	})
}

// TestAgentStandaloneCovOwnerTemporaryDrainRefusesUnaccountableEntries proves
// the owners-lock drain stops on an expired budget, on a temporary whose name
// does not parse, on a temporary whose UID has no permanent lock, and on a
// temporary that is not a trusted file — and that it never unlinks in those
// cases.
func TestAgentStandaloneCovOwnerTemporaryDrainRefusesUnaccountableEntries(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("expired budget", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "62451.owner.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")

		cleaned, busy, err := drainAgentStandaloneOwnerTemporariesUnderLock(
			directory, ownerUID, ownerGID, time.Now().Add(-time.Millisecond), nil, nil,
		)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.False(t, cleaned)
		require.False(t, busy)
		require.FileExists(t, path)
	})

	t.Run("unparsable temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "not-a-uid.owner.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")

		cleaned, busy, err := drainAgentStandaloneOwnerTemporariesUnderLock(
			directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorContains(t, err, "invalid name")
		require.False(t, cleaned)
		require.False(t, busy)
		require.FileExists(t, path)
	})

	t.Run("temporary without its permanent uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "62453.owner.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")

		cleaned, busy, err := drainAgentStandaloneOwnerTemporariesUnderLock(
			directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorIs(t, err, unix.ENOENT)
		require.False(t, cleaned)
		require.False(t, busy)
		require.FileExists(t, path)
	})

	t.Run("untrusted temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		lock := createAgentStandaloneTestLock(t, directory, "62455.lock", ownerUID, ownerGID)
		require.NoError(t, lock.Close())
		name := "62455.owner.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		require.NoError(t, os.Chmod(path, 0o644))

		cleaned, busy, err := drainAgentStandaloneOwnerTemporariesUnderLock(
			directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorContains(t, err, "not a trusted bounded regular file")
		require.False(t, cleaned)
		require.False(t, busy)
		require.FileExists(t, path)
	})
}

// TestAgentStandaloneCovMatchingDomainAdjudicationRefusesUntrustedTemporary
// proves that a domain-record temporary which is not the registry owner's
// bounded 0600 file blocks the shared-lease fast path instead of being cleaned
// or ignored.
func TestAgentStandaloneCovMatchingDomainAdjudicationRefusesUntrustedTemporary(t *testing.T) {
	directory, ownerUID, ownerGID, _ := createAgentStandaloneMatchingDomainFixture(t)
	name := "domain.json.next-" + agentStandaloneCovSuffix
	path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
	require.NoError(t, os.Chmod(path, 0o644))

	requiresExclusive, err := adjudicateAgentStandaloneMatchingDomainTemporaries(
		directory, ownerUID, ownerGID, false,
	)
	require.ErrorContains(t, err, "not a trusted bounded regular file")
	require.False(t, requiresExclusive)
	require.FileExists(t, path)
}

// TestAgentStandaloneCovTargetMarkerTemporaryCleanupRequiresTheHeldUIDLock
// proves the target-marker cleanup refuses without the caller's held UID lock,
// refuses when that descriptor is not the permanent named UID inode, refuses on
// an expired budget, and refuses a temporary whose name or metadata is not
// exact. Each refusal must leave the temporary in place.
func TestAgentStandaloneCovTargetMarkerTemporaryCleanupRequiresTheHeldUIDLock(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("no held lock", func(t *testing.T) {
		require.ErrorContains(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			openAgentStandaloneTestDirectory(t), 62461, nil, ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		), "requires its held UID lock")
	})

	t.Run("held lock is another inode", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wrong := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
		uidLock := createAgentStandaloneTestLock(t, directory, "62463.lock", ownerUID, ownerGID)
		require.NoError(t, uidLock.Close())

		require.ErrorContains(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 62463, wrong, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), "not its permanent named inode")
	})

	t.Run("expired budget", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		uidLock := createAgentStandaloneTestLock(t, directory, "62465.lock", ownerUID, ownerGID)
		name := "62465.quarantine.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")

		require.ErrorContains(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 62465, uidLock, ownerUID, ownerGID, time.Now().Add(-time.Millisecond), nil, nil,
		), "exceeded 30 seconds")
		require.FileExists(t, path)
	})

	t.Run("unparsable temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		uidLock := createAgentStandaloneTestLock(t, directory, "62467.lock", ownerUID, ownerGID)
		name := "62467.quarantine.next-0123456789ABCDEF01234567"
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")

		require.ErrorContains(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 62467, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), "is invalid")
		require.FileExists(t, path)
	})

	t.Run("untrusted temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		uidLock := createAgentStandaloneTestLock(t, directory, "62469.lock", ownerUID, ownerGID)
		name := "62469.quarantine.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		require.NoError(t, os.Chmod(path, 0o644))

		require.ErrorContains(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 62469, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), "not a trusted bounded regular file")
		require.FileExists(t, path)
	})
}

// TestAgentStandaloneCovAuthorityPathRequiresARealResolvedDirectory proves the
// registry path resolver refuses a descriptor procfs cannot name at all and a
// descriptor that names something other than a filesystem path. The resolved
// path is what decides whether a standalone owner's state root sits inside the
// registry, so an unresolvable descriptor must never yield a usable answer.
func TestAgentStandaloneCovAuthorityPathRequiresARealResolvedDirectory(t *testing.T) {
	closed, err := os.Open(os.DevNull)
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	path, err := agentStandaloneAuthorityPath(closed)
	require.ErrorContains(t, err, "resolve agent authority root path")
	require.Empty(t, path)

	reader, writer, pipeErr := os.Pipe()
	require.NoError(t, pipeErr)
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
		require.NoError(t, writer.Close())
	})
	path, err = agentStandaloneAuthorityPath(reader)
	require.ErrorContains(t, err, "did not resolve to a clean absolute path")
	require.Empty(t, path)
}

// TestAgentStandaloneCovDomainRevalidationRequiresTheExactPublishedRecord
// proves the post-publication recheck refuses when the record cannot be read
// back and when it reads back as a different authority. Skipping either check
// would let a peer's record be adopted as this claim's own domain.
func TestAgentStandaloneCovDomainRevalidationRequiresTheExactPublishedRecord(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	require.ErrorIs(t,
		revalidateAgentStandaloneDomain(directory, ownerUID, ownerGID, record),
		unix.ENOENT,
	)

	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	require.NoError(t, revalidateAgentStandaloneDomain(directory, ownerUID, ownerGID, record))
	other := record
	other.AuthorityID = "fedcba9876543210fedcba9876543210"
	require.ErrorContains(t,
		revalidateAgentStandaloneDomain(directory, ownerUID, ownerGID, other),
		"changed during shared-lease transition",
	)
}

// TestAgentStandaloneCovStateRootBindingRefusesUnusablePaths proves the state
// root binder refuses a path that is not clean and absolute before it opens
// anything, refuses a component that does not exist, and that revalidation
// propagates the same refusal instead of accepting a vanished state root.
func TestAgentStandaloneCovStateRootBindingRefusesUnusablePaths(t *testing.T) {
	uid, gid := agentStandaloneTestAuthorityIDs()
	for _, path := range []string{"srv/hermes/state", "/srv/hermes/../hermes/state", "/srv/hermes/state/"} {
		bound, err := bindAgentStandaloneStateRoot(path, uid, gid)
		require.ErrorContains(t, err, "must be a clean absolute path")
		require.Equal(t, agentStandaloneStateRoot{}, bound)
	}

	missing := "/acp-go-standalone-cov-absent/state"
	bound, err := bindAgentStandaloneStateRoot(missing, uid, gid)
	require.ErrorIs(t, err, unix.ENOENT)
	require.ErrorContains(t, err, `open standalone state root component "acp-go-standalone-cov-absent"`)
	require.Equal(t, agentStandaloneStateRoot{}, bound)

	require.ErrorIs(t, revalidateAgentStandaloneStateRoot(
		agentStandaloneStateRoot{Path: missing, Dev: 1, Ino: 2}, uid, gid,
	), unix.ENOENT)
}
