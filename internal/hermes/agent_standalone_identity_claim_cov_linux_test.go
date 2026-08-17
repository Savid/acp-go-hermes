//go:build linux

package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovProtectedOwner binds a real protected state root and
// returns the owner tuple that names it, so a claim can revalidate its state
// root for real instead of being refused for a state root that never existed.
func agentStandaloneCovProtectedOwner(t *testing.T, uid, gid uint32, ownerID string) agentStandaloneOwner {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("standalone owner claim cases require root to own a protected state root")
	}
	bound, err := bindAgentStandaloneStateRoot(createAgentStandaloneProtectedStateRoot(t, uid, gid), uid, gid)
	require.NoError(t, err)

	return agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: ownerID, StateRoot: bound,
	}
}

// agentStandaloneCovVacancySeam replaces the task vacancy proof for one case.
// The real proof walks every process on the host, which a fixture registry
// cannot influence; the seam lets a case state what the claim does when the
// proof answers.
func agentStandaloneCovVacancySeam(
	t *testing.T,
	scan func(uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal) error,
) {
	t.Helper()
	previous := agentStandaloneVacancyScan
	t.Cleanup(func() { agentStandaloneVacancyScan = previous })
	agentStandaloneVacancyScan = scan
}

// agentStandaloneCovVacantScan is the vacancy proof answering "nobody holds
// this identity".
func agentStandaloneCovVacantScan(uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal) error {
	return nil
}

// agentStandaloneCovOnLockOpen runs action immediately before the first open of
// the named permanent lock. That is the one instant at which a peer can still
// change the registry between an unlocked read and the read the claim takes
// under its own lock, so it is the only way to stage that race deterministically.
func agentStandaloneCovOnLockOpen(t *testing.T, name string, action func()) {
	t.Helper()
	restoreAgentStandalonePermanentLockSeams(t)
	original := agentStandaloneLockOpenat
	fired := false
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		if path == name && !fired {
			fired = true
			action()
		}

		return original(dirfd, path, flags, mode)
	}
	t.Cleanup(func() { require.True(t, fired, "the staged peer write never ran") })
}

// TestAgentStandaloneCovOwnerIdentityRefusesBeforeItReadsTheRegistry proves the
// owner claim checks its budget and its registry descriptor before it does
// anything else, and that a refused claim leaves no registry state behind.
func TestAgentStandaloneCovOwnerIdentityRefusesBeforeItReadsTheRegistry(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62901, 62902, "cov-budget", "/srv/hermes/cov-budget", 1, 2)

	t.Run("expired budget", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(-time.Millisecond), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		entries, readErr := os.ReadDir(directory.Name())
		require.NoError(t, readErr)
		require.Empty(t, entries, "an expired claim must not create registry state")
	})

	t.Run("removed registry", func(t *testing.T) {
		identity, err := acquireAgentStandaloneOwnerIdentity(
			agentStandaloneCovRemovedDirectory(t), want, ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
	})
}

// TestAgentStandaloneCovOwnerIdentityGivesUpWhileAnotherUIDHoldsItsTemporary
// proves a claim that keeps finding an owner temporary it may not remove
// eventually exhausts its budget rather than removing the temporary or
// proceeding past it, and that it does not leave owners.lock held.
func TestAgentStandaloneCovOwnerIdentityGivesUpWhileAnotherUIDHoldsItsTemporary(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	held := createAgentStandaloneTestLock(t, directory, "62903.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "62903.owner.next-"+agentStandaloneCovSuffix, "partial",
	)
	want := agentStandaloneCovOwner(62901, 62902, "cov-busy", "/srv/hermes/cov-busy", 1, 2)

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, want, ownerUID, ownerGID, time.Now().Add(60*time.Millisecond), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorContains(t, err, "exceeded 30 seconds")
	require.FileExists(t, temporary)
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, acquired, "the abandoned claim must release owners.lock")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneCovOwnerIdentityRefusesAnUnusableUnlockedBinding proves a
// UID already bound to a different tuple, and a binding that cannot be read at
// all, both stop the claim before it takes any lock. Either would otherwise let
// one provider inherit another provider's permanent UID binding.
func TestAgentStandaloneCovOwnerIdentityRefusesAnUnusableUnlockedBinding(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62905, 62906, "cov-unlocked", "/srv/hermes/cov-unlocked", 1, 2)

	t.Run("bound to another tuple", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovOwner(62905, 62906, "someone-else", "/srv/hermes/someone-else", 3, 4),
		)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "agent identity uid 62905 is permanently bound to another standalone owner")
	})

	t.Run("unreadable binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "62905.owner", "not json\n")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "invalid character")
	})
}

// TestAgentStandaloneCovOwnerIdentityAdoptsItsOwnRetainedBinding proves the
// returning-owner path completes: it takes the permanent UID lock exclusively,
// keeps holding it on return, releases owners.lock, and republishes the ACTIVE
// marker for its own session. This is the only case that exercises the whole
// existing-owner claim, so the refusals below are attributable to what each
// one breaks.
func TestAgentStandaloneCovOwnerIdentityAdoptsItsOwnRetainedBinding(t *testing.T) {
	want := agentStandaloneCovProtectedOwner(t, 62911, 62912, "cov-adopt")
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "62911.lock")
	require.NoError(t, createAgentStandaloneOwner(directory, want, ownerUID, ownerGID))
	agentStandaloneCovWriteActiveMarker(t, directory, want)
	scans := 0
	agentStandaloneCovVacancySeam(t, func(
		uid, gid uint32, _ time.Time, _ <-chan struct{}, _ <-chan os.Signal,
	) error {
		require.Equal(t, want.UID, uid)
		require.Equal(t, want.GID, gid)
		scans++

		return nil
	})

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, want, ownerUID, ownerGID, time.Now().Add(5*time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, identity)
	t.Cleanup(func() { _ = identity.Close() })
	require.Equal(t, 2, scans, "a returning owner must prove vacancy twice")
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62911.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.False(t, acquired, "the adopted claim must still hold its permanent UID lock")
	require.Nil(t, contender)
	owners, ownersAcquired, ownersErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, ownersErr)
	require.True(t, ownersAcquired, "the completed claim must release owners.lock")
	require.NoError(t, owners.Close())
	marker, markerErr := loadAgentStandaloneMarker(directory, want.UID, ownerUID, ownerGID)
	require.NoError(t, markerErr)
	require.Equal(t, "active", marker.State)
	require.Empty(t, marker.Paths)
}

// TestAgentStandaloneCovExistingOwnerClaimFailsClosedAndReleasesItsLocks proves
// every refusal on the returning-owner path leaves no lock held: a missing
// permanent UID lock, a missing owners.lock, an untrusted marker temporary for
// the target UID, an unaccountable registry entry, and a state root that no
// longer validates. A claim that returned with a lock still held would block
// every later claim for that UID.
func TestAgentStandaloneCovExistingOwnerClaimFailsClosedAndReleasesItsLocks(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62921, 62922, "cov-existing", "/srv/hermes/cov-existing", 5, 6)
	deadline := func() time.Time { return time.Now().Add(time.Second) }

	t.Run("uid lock missing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62921.lock"))
	})

	t.Run("owners lock missing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "62921.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62921.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, acquired, "the refused claim must release the UID lock")
		require.NoError(t, contender.Close())
	})

	t.Run("untrusted marker temporary for the target uid", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62921.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "62921.quarantine.next-"+agentStandaloneCovSuffix, "partial",
		)
		require.NoError(t, os.Chmod(temporary, 0o644))

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "not a trusted bounded regular file")
		require.FileExists(t, temporary)
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62921.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, acquired, "the refused claim must release the UID lock")
		require.NoError(t, contender.Close())
	})

	t.Run("unaccountable registry entry", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62921.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, `unknown entry "leftover"`)
	})

	t.Run("state root no longer validates", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62921.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "open standalone state root component")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62921.quarantine"))
	})
}

// TestAgentStandaloneCovOwnerIdentityRefusesEveryUnsafeFreshClaim proves each
// refusal on the fresh-binding path: an owners.lock that is not a trusted
// permanent inode, registry state for the target UID without its permanent
// lock, a UID lock that is not a trusted inode, an untrusted marker temporary
// for the target UID, an unaccountable registry entry, and a claim whose state
// root no longer validates. No refusal may publish an owner binding.
func TestAgentStandaloneCovOwnerIdentityRefusesEveryUnsafeFreshClaim(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62931, 62932, "cov-fresh", "/srv/hermes/cov-fresh", 7, 8)
	ownerPath := func(directory *os.File) string { return filepath.Join(directory.Name(), "62931.owner") }

	t.Run("untrusted owners lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "owners.lock"), 0o644))

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "mode")
		require.NoFileExists(t, ownerPath(directory))
	})

	t.Run("target uid marker without its permanent lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteCleanMarker(t, directory, want.UID, want.GID, "prior-state")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, `uid 62931 permanent lock is missing while its registry state "62931.quarantine" exists`)
		require.NoFileExists(t, ownerPath(directory))
	})

	t.Run("untrusted uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62931.lock")
		require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "62931.lock"), 0o644))

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "mode")
		require.NoFileExists(t, ownerPath(directory))
	})

	t.Run("untrusted marker temporary for the target uid", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62931.lock")
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "62931.quarantine.next-"+agentStandaloneCovSuffix, "partial",
		)
		require.NoError(t, os.Chmod(temporary, 0o644))

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "not a trusted bounded regular file")
		require.FileExists(t, temporary)
		require.NoFileExists(t, ownerPath(directory))
	})

	t.Run("unaccountable registry entry", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62931.lock")
		agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, `unknown entry "leftover"`)
		require.NoFileExists(t, ownerPath(directory))
	})

	t.Run("state root no longer validates", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62931.lock")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "open standalone state root component")
		require.NoFileExists(t, ownerPath(directory))
	})
}

// TestAgentStandaloneCovOwnerIdentityWaitsOutALiveUIDLockHolder proves that a
// claim whose target UID lock is already held exclusively releases owners.lock,
// retries, and finally exhausts its budget instead of ever proceeding past a
// lock it does not hold.
func TestAgentStandaloneCovOwnerIdentityWaitsOutALiveUIDLockHolder(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	held := createAgentStandaloneTestLock(t, directory, "62941.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	want := agentStandaloneCovOwner(62941, 62942, "cov-held", "/srv/hermes/cov-held", 9, 10)

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, want, ownerUID, ownerGID, time.Now().Add(60*time.Millisecond), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorContains(t, err, "exceeded 30 seconds")
	require.NoFileExists(t, filepath.Join(directory.Name(), "62941.owner"))
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, acquired, "each retry must release owners.lock")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneCovOwnerIdentityRereadsTheRegistryUnderOwnersLock proves
// the claim treats everything it read before taking owners.lock as stale. A
// peer that publishes a binding, an unreadable binding, or an owner temporary
// in that window must change the outcome, because the first read carried no
// exclusion at all.
func TestAgentStandaloneCovOwnerIdentityRereadsTheRegistryUnderOwnersLock(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62951, 62952, "cov-reread", "/srv/hermes/cov-reread", 11, 12)

	t.Run("peer bound the uid to another tuple", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteOwner(t, directory,
				agentStandaloneCovOwner(62951, 62952, "peer-owner", "/srv/hermes/peer-owner", 13, 14),
			)
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "agent identity uid 62951 is permanently bound to another standalone owner")
	})

	t.Run("peer left an unreadable binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteRegistryFile(t, directory, "62951.owner", "not json\n")
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "invalid character")
	})

	t.Run("peer left a malformed owner temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		temporary := filepath.Join(directory.Name(), "not-a-uid.owner.next-"+agentStandaloneCovSuffix)
		agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteRegistryFile(
				t, directory, "not-a-uid.owner.next-"+agentStandaloneCovSuffix, "partial",
			)
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "invalid name")
		require.FileExists(t, temporary)
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(
			directory, "owners.lock", false, ownerUID, ownerGID,
		)
		require.NoError(t, lockErr)
		require.True(t, acquired, "the refused claim must release owners.lock")
		require.NoError(t, contender.Close())
	})

	t.Run("peer left a drainable owner temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62953.lock")
		temporary := filepath.Join(directory.Name(), "62953.owner.next-"+agentStandaloneCovSuffix)
		agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteRegistryFile(
				t, directory, "62953.owner.next-"+agentStandaloneCovSuffix, "partial",
			)
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(200*time.Millisecond), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "open standalone state root component")
		require.NoFileExists(t, temporary, "the drain under owners.lock must remove the peer temporary")
	})

	t.Run("peer published this exact binding", func(t *testing.T) {
		bound := agentStandaloneCovProtectedOwner(t, 62955, 62956, "cov-reread-exact")
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62955.lock")
		agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteOwner(t, directory, bound)
			agentStandaloneCovWriteActiveMarker(t, directory, bound)
		})
		agentStandaloneCovVacancySeam(t, agentStandaloneCovVacantScan)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, bound, ownerUID, ownerGID, time.Now().Add(5*time.Second), nil, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, identity)
		t.Cleanup(func() { _ = identity.Close() })
		marker, markerErr := loadAgentStandaloneMarker(directory, bound.UID, ownerUID, ownerGID)
		require.NoError(t, markerErr)
		require.Equal(t, "active", marker.State)
	})
}

// TestAgentStandaloneCovOwnerIdentityRestartsWhenAPeerLeftAMarkerTemporary
// proves that an owner temporary appearing after the claim already holds its
// UID lock is not silently accepted: the registry audit refuses the claim, the
// loop restarts, and the temporary is drained before the claim completes.
func TestAgentStandaloneCovOwnerIdentityRestartsWhenAPeerLeftAMarkerTemporary(t *testing.T) {
	bound := agentStandaloneCovProtectedOwner(t, 62961, 62962, "cov-restart")
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "62961.lock")
	agentStandaloneCovPermanentLock(t, directory, "62963.lock")
	require.NoError(t, createAgentStandaloneOwner(directory, bound, ownerUID, ownerGID))
	agentStandaloneCovWriteActiveMarker(t, directory, bound)
	temporary := filepath.Join(directory.Name(), "62963.owner.next-"+agentStandaloneCovSuffix)
	agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
		agentStandaloneCovWriteRegistryFile(
			t, directory, "62963.owner.next-"+agentStandaloneCovSuffix, "partial",
		)
	})
	agentStandaloneCovVacancySeam(t, agentStandaloneCovVacantScan)

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, bound, ownerUID, ownerGID, time.Now().Add(5*time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, identity)
	t.Cleanup(func() { _ = identity.Close() })
	require.NoFileExists(t, temporary, "the restarted claim must drain the peer temporary")
}

// TestAgentStandaloneCovOwnerClaimCompletionChecksItsBudgetAndStateRootAgain
// proves the completion step re-checks the budget and re-binds the state root
// before it publishes anything, refuses when the binding it is completing is
// gone, and names the vacancy proof that refused. Publishing an ACTIVE marker
// on any of these would advertise an identity the claim no longer owns.
func TestAgentStandaloneCovOwnerClaimCompletionChecksItsBudgetAndStateRootAgain(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("expired budget", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovOwner(62971, 62972, "cov-complete", "/srv/hermes/cov-complete", 15, 16)

		require.ErrorContains(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(-time.Millisecond), nil, nil,
		), "exceeded 30 seconds")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62971.quarantine"))
	})

	t.Run("state root no longer binds", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovOwner(62971, 62972, "cov-complete", "/srv/hermes/cov-complete", 15, 16)

		require.ErrorContains(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		), "open standalone state root component")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62971.quarantine"))
	})

	t.Run("binding vanished under its own lock", func(t *testing.T) {
		want := agentStandaloneCovProtectedOwner(t, 62973, 62974, "cov-complete-gone")
		directory := openAgentStandaloneTestDirectory(t)

		require.ErrorContains(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		), "disappeared while its immutable binding was locked")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62973.quarantine"))
	})

	t.Run("returning owner fails the second vacancy pass", func(t *testing.T) {
		want := agentStandaloneCovProtectedOwner(t, 62975, 62976, "cov-complete-twice")
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteActiveMarker(t, directory, want)
		wantErr := errors.New("a task re-entered the identity")
		scans := 0
		agentStandaloneCovVacancySeam(t, func(
			uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal,
		) error {
			scans++
			if scans == 2 {
				return wantErr
			}

			return nil
		})

		err := completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "second standalone task vacancy proof")
		require.Equal(t, 2, scans)
	})

	t.Run("fresh owner fails the post-claim vacancy proof", func(t *testing.T) {
		want := agentStandaloneCovProtectedOwner(t, 62977, 62978, "cov-complete-fresh")
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, want)
		wantErr := errors.New("a task claimed the identity after the binding")
		agentStandaloneCovVacancySeam(t, func(
			uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal,
		) error {
			return wantErr
		})

		err := completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "post-owner standalone task vacancy proof")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62977.quarantine"))
	})

	t.Run("budget spent by the vacancy proof", func(t *testing.T) {
		want := agentStandaloneCovProtectedOwner(t, 62979, 62980, "cov-complete-slow")
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, want)
		deadline := time.Now().Add(50 * time.Millisecond)
		agentStandaloneCovVacancySeam(t, func(
			uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal,
		) error {
			time.Sleep(time.Until(deadline) + 20*time.Millisecond)

			return nil
		})

		require.ErrorContains(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, deadline, nil, nil,
		), "exceeded 30 seconds")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62979.quarantine"))
	})
}

// TestAgentStandaloneCovStandaloneIdentityRefusesEveryUnusableDisposition
// proves the standalone identity entry point refuses an unbindable state root,
// a test-only claim with no test authority root, a state root that lives inside
// the authority registry itself, an unusable authority root, and an authority
// registry whose domain lock is not a trusted inode.
func TestAgentStandaloneCovStandaloneIdentityRefusesEveryUnusableDisposition(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone identity acquisition requires root")
	}
	const uid, gid = uint32(62991), uint32(62992)

	t.Run("unbindable state root", func(t *testing.T) {
		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "cov-entry", "srv/hermes/relative", true, t.TempDir(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "must be a clean absolute path")
	})

	t.Run("test-only claim without a test authority root", func(t *testing.T) {
		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "cov-entry", createAgentStandaloneProtectedStateRoot(t, uid, gid), true, "", nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "test agent identity lock root is required")
	})

	t.Run("state root inside the authority registry", func(t *testing.T) {
		testRoot := t.TempDir()
		require.NoError(t, os.Chmod(testRoot, 0o700))
		authority := filepath.Join(testRoot, "acp-go", "agent-identities")
		require.NoError(t, os.MkdirAll(authority, 0o700))
		require.NoError(t, os.Chmod(filepath.Join(testRoot, "acp-go"), 0o700))
		require.NoError(t, os.Chmod(authority, 0o700))
		require.NoError(t, os.Chown(authority, int(uid), int(gid)))

		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "cov-entry", authority, true, testRoot, nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "must be separate from the agent identity authority root")
	})

	t.Run("unusable authority root", func(t *testing.T) {
		notADirectory := filepath.Join(t.TempDir(), "run")
		require.NoError(t, os.WriteFile(notADirectory, []byte("x"), 0o600))

		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "cov-entry", createAgentStandaloneProtectedStateRoot(t, uid, gid),
			true, notADirectory, nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOTDIR)
	})

	t.Run("untrusted domain lock", func(t *testing.T) {
		testRoot := t.TempDir()
		require.NoError(t, os.Chmod(testRoot, 0o700))
		authority := filepath.Join(testRoot, "acp-go", "agent-identities")
		require.NoError(t, os.MkdirAll(authority, 0o700))
		require.NoError(t, os.Chmod(filepath.Join(testRoot, "acp-go"), 0o700))
		require.NoError(t, os.Chmod(authority, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(authority, "domain.lock"), nil, 0o600))
		require.NoError(t, os.Chmod(filepath.Join(authority, "domain.lock"), 0o644))

		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "cov-entry", createAgentStandaloneProtectedStateRoot(t, uid, gid),
			true, testRoot, nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "mode")
	})
}

// TestAgentStandaloneCovStandaloneIdentityReleasesItsAuthorityWhenTheOwnerFails
// proves that a claim which took the authority domain lease and then failed to
// bind its owner releases the authority lease instead of leaking it. A leaked
// lease would keep the whole registry pinned to a claim that never completed.
func TestAgentStandaloneCovStandaloneIdentityReleasesItsAuthorityWhenTheOwnerFails(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone identity acquisition requires root")
	}
	const uid, gid = uint32(62995), uint32(62996)
	stateRoot := createAgentStandaloneProtectedStateRoot(t, uid, gid)
	testRoot := t.TempDir()
	require.NoError(t, os.Chmod(testRoot, 0o700))
	wantErr := errors.New("a task still holds the identity")
	agentStandaloneCovVacancySeam(t, func(
		uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal,
	) error {
		return wantErr
	})

	identity, err := acquireAgentStandaloneIdentity(
		uid, gid, "cov-authority-release", stateRoot, true, testRoot, nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorIs(t, err, wantErr)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	directory, openErr := openAgentIdentityLockDirectory(testRoot, ownerUID, ownerGID)
	require.NoError(t, openErr)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	require.NoFileExists(t, filepath.Join(directory.Name(), "62995.owner"))
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, acquired, "the failed claim must release the authority domain lease")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneCovSameBootRebindRefusesEveryIncompleteBinding proves the
// same-boot rebind admits exactly one standalone owner with its live UID lock,
// its exact immutable binding and its exact retained ACTIVE marker, and refuses
// everything else. A looser rebind would let a process re-mint the authority
// domain while another owner's state was still on disk.
func TestAgentStandaloneCovSameBootRebindRefusesEveryIncompleteBinding(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62801, 62802, "cov-rebind", "/srv/hermes/cov-rebind", 21, 22)
	deadline := func() time.Time { return time.Now().Add(time.Second) }

	t.Run("owners lock missing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
	})

	t.Run("owner entry that names no uid", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteRegistryFile(t, directory, "bad.owner", "{}\n")

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "invalid uid")
	})

	t.Run("no owner at all", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires exactly one standalone owner binding")
	})

	t.Run("uid lock still held", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		held := createAgentStandaloneTestLock(t, directory, "62801.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "still has a live UID lock holder")
	})

	t.Run("binding is another tuple", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62801.lock")
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovOwner(62801, 62802, "another-tuple", "/srv/hermes/another-tuple", 23, 24),
		)

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires the exact standalone owner binding")
	})

	t.Run("retained marker missing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62801.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires the retained standalone ACTIVE marker")
	})

	t.Run("retained marker is for another session", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62801.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteCleanMarker(t, directory, want.UID, want.GID, "another-session")

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, deadline(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires the exact retained standalone ACTIVE marker")
	})
}

// TestAgentStandaloneCovAuthorityEntriesRequireADirectoryDescriptor proves the
// registry listing refuses a descriptor that is not a directory rather than
// reporting an empty registry. An empty answer here is what lets a claim mint
// fresh permanent locks.
func TestAgentStandaloneCovAuthorityEntriesRequireADirectoryDescriptor(t *testing.T) {
	file, err := os.Open(os.DevNull)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	entries, err := agentStandaloneAuthorityEntries(file)
	require.ErrorIs(t, err, unix.ENOTDIR)
	require.Nil(t, entries)
}
