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

// agentStandaloneCovReleaseLockSeam faults the release of a permanent registry
// lock. The lock is still closed, so the fault models the one case the code
// guards against: the kernel not confirming that the lock this claim held has
// actually been given up. When armed is nil every release is faulted; otherwise
// the fault only starts once the caller sets it, which lets a case name exactly
// which release it is talking about.
func agentStandaloneCovReleaseLockSeam(t *testing.T, armed *bool, wantErr error) {
	t.Helper()
	previous := agentStandaloneReleaseLock
	t.Cleanup(func() { agentStandaloneReleaseLock = previous })
	agentStandaloneReleaseLock = func(file *os.File) error {
		if armed != nil && !*armed {
			return previous(file)
		}
		require.NoError(t, file.Close())

		return wantErr
	}
}

// TestAgentStandaloneCovOwnerIdentityReportsAReleaseItCouldNotConfirm proves
// that whenever the owner claim cannot confirm it released owners.lock, it
// reports failure instead of continuing or returning an identity. Continuing
// would mean a claim that believes the registry-wide mutex is free while it may
// still hold it, and every later claim would deadlock behind it.
func TestAgentStandaloneCovOwnerIdentityReportsAReleaseItCouldNotConfirm(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	wantErr := errors.New("injected lock release failure")

	t.Run("after draining a peer temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62861.lock")
		agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteRegistryFile(
				t, directory, "62861.owner.next-"+agentStandaloneCovSuffix, "partial",
			)
		})
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)
		want := agentStandaloneCovOwner(62863, 62864, "cov-release-drain", "/srv/hermes/cov-release-drain", 1, 2)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62863.owner"))
	})

	t.Run("after a peer published this exact binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62865.lock")
		want := agentStandaloneCovOwner(62865, 62866, "cov-release-peer", "/srv/hermes/cov-release-peer", 3, 4)
		agentStandaloneCovOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteOwner(t, directory, want)
		})
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("while waiting behind a live uid lock holder", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "62867.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)
		want := agentStandaloneCovOwner(62867, 62868, "cov-release-wait", "/srv/hermes/cov-release-wait", 5, 6)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62867.owner"))
	})

	t.Run("after the claim completed", func(t *testing.T) {
		want := agentStandaloneCovProtectedOwner(t, 62871, 62872, "cov-release-final")
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62871.lock")
		armed := false
		agentStandaloneCovReleaseLockSeam(t, &armed, wantErr)
		agentStandaloneCovVacancySeam(t, func(
			uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal,
		) error {
			armed = true

			return nil
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(5*time.Second), nil, nil,
		)
		require.Nil(t, identity, "a claim that cannot confirm its release must not hand back an identity")
		require.ErrorIs(t, err, wantErr)
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62871.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, acquired, "the abandoned claim must not keep the UID lock")
		require.NoError(t, contender.Close())
	})

	t.Run("on the returning-owner path", func(t *testing.T) {
		want := agentStandaloneCovProtectedOwner(t, 62873, 62874, "cov-release-existing")
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62873.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteActiveMarker(t, directory, want)
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)
		agentStandaloneCovVacancySeam(t, agentStandaloneCovVacantScan)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62873.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, acquired, "the abandoned claim must not keep the UID lock")
		require.NoError(t, contender.Close())
	})
}

// TestAgentStandaloneCovDomainReportsALeaseItCouldNotRelease proves the domain
// claim refuses whenever it cannot confirm it gave up the shared lease before
// taking the exclusive one, and whenever it cannot confirm it gave up the UID
// lock a same-boot rebind held. Proceeding on either would upgrade a lease the
// claim still holds, which cannot succeed and must not be reported as success.
func TestAgentStandaloneCovDomainReportsALeaseItCouldNotRelease(t *testing.T) {
	wantErr := errors.New("injected lease release failure")
	want := agentStandaloneCovOwner(62881, 62882, "cov-release-domain", "/srv/hermes/cov-release-domain", 7, 8)

	t.Run("shared lease before an exclusive cleanup", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		agentStandaloneCovWriteRegistryFile(
			t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial",
		)
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("shared lease before taking over another domain", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
			record.PIDNamespace.Ino++
		})
		before, readErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, readErr)
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		after, afterErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, afterErr)
		require.Equal(t, before, after, "no record may be published on a lease the claim still holds")
	})

	t.Run("rebound uid lock after the record was published", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovSameBootRegistry(t)
		armed := false
		agentStandaloneCovReleaseLockSeam(t, &armed, wantErr)
		agentStandaloneCovReplaceDomainSeam(t, func(
			dir *os.File, uid, gid uint32, record agentAuthorityDomainRecord,
		) error {
			err := replaceAgentStandaloneDomainRecord(dir, uid, gid, record)
			armed = true

			return err
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, owner, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// TestAgentStandaloneCovSameBootRebindReportsAnOwnersLockItCouldNotRelease
// proves the rebind refuses rather than handing back the UID lock it took when
// it cannot confirm owners.lock was released, and that it does not leave the UID
// lock held either.
func TestAgentStandaloneCovSameBootRebindReportsAnOwnersLockItCouldNotRelease(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneCovOwner(62891, 62892, "cov-release-rebind", "/srv/hermes/cov-release-rebind", 9, 10)
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "62891.lock")
	agentStandaloneCovWriteOwner(t, directory, owner)
	agentStandaloneCovWriteActiveMarker(t, directory, owner)
	agentStandaloneCovVacancySeam(t, agentStandaloneCovVacantScan)
	wantErr := errors.New("injected owners lock release failure")
	agentStandaloneCovReleaseLockSeam(t, nil, wantErr)

	identity, err := validateAgentStandaloneSameBootRebind(
		directory, owner, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorIs(t, err, wantErr)
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62891.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, acquired, "the refused rebind must not keep the UID lock")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneCovAuditReportsALockItCouldNotRelease proves the registry
// audit refuses the whole registry when it cannot confirm it released a
// permanent lock it opened only to inspect. An audit that swallowed that would
// pin the registry's own locks open for the lifetime of the process.
func TestAgentStandaloneCovAuditReportsALockItCouldNotRelease(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	wantErr := errors.New("injected audit lock release failure")

	t.Run("owners lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)

		require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, false, false, false, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})

	t.Run("uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "62895.lock")
		agentStandaloneCovReleaseLockSeam(t, nil, wantErr)

		require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, false, false, false, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})
}

// TestAgentStandaloneCovOwnerIdentityStopsOnAnUndrainableTemporaryBeforeAnyLock
// proves a claim that finds an owner temporary it cannot even parse refuses
// before it takes owners.lock for its own claim, and leaves the temporary alone.
func TestAgentStandaloneCovOwnerIdentityStopsOnAnUndrainableTemporaryBeforeAnyLock(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "not-a-uid.owner.next-"+agentStandaloneCovSuffix, "partial",
	)
	want := agentStandaloneCovOwner(62897, 62898, "cov-undrainable", "/srv/hermes/cov-undrainable", 11, 12)

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorContains(t, err, "invalid name")
	require.FileExists(t, temporary)
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, acquired, "the refused claim must release owners.lock")
	require.NoError(t, contender.Close())
}
