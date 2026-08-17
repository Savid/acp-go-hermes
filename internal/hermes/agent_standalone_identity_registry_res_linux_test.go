//go:build linux

package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneResFaultCurrentDomain makes the running-domain proof fail from
// its nth attempt onwards by refusing the kernel boot id it reads first. The
// domain record on disk still loads, so a case can state exactly what a claim
// does when it can read the published authority but can no longer describe the
// domain it is running in.
func agentStandaloneResFaultCurrentDomain(t *testing.T, nth int) error {
	t.Helper()
	wantErr := errors.New("kernel stopped reporting its boot id")
	previous := agentAuthorityDomainReadFile
	proofs := 0
	agentAuthorityDomainReadFile = func(path string) ([]byte, error) {
		if path != "/proc/sys/kernel/random/boot_id" {
			return previous(path)
		}
		proofs++
		if proofs >= nth {
			return nil, wantErr
		}

		return previous(path)
	}
	t.Cleanup(func() {
		agentAuthorityDomainReadFile = previous
		require.GreaterOrEqual(t, proofs, nth, "the running domain was proven fewer times than the case staged")
	})

	return wantErr
}

// agentStandaloneResFaultAuthorityEntries makes the registry listing fail from
// its nth attempt onwards. Listing runs against the registry descriptor the
// caller already validated, so faulting the call is the only way to state what
// happens when the kernel stops enumerating a directory that is already open.
func agentStandaloneResFaultAuthorityEntries(t *testing.T, nth int) error {
	t.Helper()
	wantErr := errors.New("kernel stopped listing the authority registry")
	previous := agentStandaloneEntriesOpenat
	listings := 0
	agentStandaloneEntriesOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		listings++
		if listings >= nth {
			return -1, wantErr
		}

		return previous(dirfd, path, flags, mode)
	}
	t.Cleanup(func() {
		agentStandaloneEntriesOpenat = previous
		require.GreaterOrEqual(t, listings, nth, "the registry was listed fewer times than the case staged")
	})

	return wantErr
}

// TestAgentStandaloneResDomainClaimRefusesWhenItCannotDescribeItsOwnDomain
// proves every point at which the domain claim compares the published authority
// record against the domain it is running in refuses when that second half of
// the comparison is unavailable: joining a matching authority, re-reading it
// under the exclusive lock, taking over a foreign one, and minting a fresh one.
// Continuing on an unknown domain would let a claim adopt or overwrite an
// authority that belongs to another boot, PID namespace or user namespace.
func TestAgentStandaloneResDomainClaimRefusesWhenItCannotDescribeItsOwnDomain(t *testing.T) {
	want := agentStandaloneCovOwner(62911, 62912, "res-domain", "/srv/hermes/res-domain", 31, 32)

	t.Run("joining a matching authority", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		wantErr := agentStandaloneResFaultCurrentDomain(t, 1)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("re-reading under the exclusive lock", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "{}\n",
		)
		wantErr := agentStandaloneResFaultCurrentDomain(t, 2)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		require.FileExists(t, temporary, "the refused upgrade must not have cleaned the pending temporary")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("taking over a foreign authority", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
			record.PIDNamespace.Ino++
		})
		before, readErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, readErr)
		wantErr := agentStandaloneResFaultCurrentDomain(t, 2)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		after, readErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, readErr)
		require.Equal(t, before, after, "the refused take-over must leave the foreign record untouched")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("minting a fresh authority", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		wantErr := agentStandaloneResFaultCurrentDomain(t, 1)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// agentStandaloneResStageMarkerTemporaryRace stages the one registry state that
// makes the pristine-registry audit report a live UID holder: a marker temporary
// with no permanent UID lock beside it, plus a peer that creates and holds that
// UID lock in the window between the audit listing the registry and reaching the
// temporary. That window is real — a concurrent owner claim creates its UID lock
// exactly there — and the seam is the only way to place a peer inside it.
func agentStandaloneResStageMarkerTemporaryRace(
	t *testing.T,
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	arrive func(),
) {
	t.Helper()
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "62921.quarantine.next-"+agentStandaloneCovSuffix, "{}\n",
	)
	restoreAgentStandalonePermanentLockSeams(t)
	original := agentStandaloneLockOpenat
	raced := false
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		if path == "62921.lock" && !raced {
			raced = true
			held, err := openAgentStandaloneNamedLock(directory, "62921.lock", true, ownerUID, ownerGID)
			require.NoError(t, err)
			require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
			t.Cleanup(func() { _ = held.Close() })
			require.NoError(t, os.Remove(temporary))
			arrive()
		}

		return original(dirfd, path, flags, mode)
	}
	t.Cleanup(func() { require.True(t, raced, "the peer never took the UID lock") })
}

// TestAgentStandaloneResDomainClaimRetriesAroundALiveUIDHolder proves the
// domain claim treats "a peer holds that identity's UID lock" as a reason to
// wait and look again, not as a reason to refuse the registry, and that once the
// peer's lock is the only prior state left the claim refuses by naming it. The
// audit reads the registry before it adjudicates it, so a UID lock created
// inside that window must not be mistaken for an unaccountable registry.
func TestAgentStandaloneResDomainClaimRetriesAroundALiveUIDHolder(t *testing.T) {
	want := agentStandaloneCovOwner(62921, 62922, "res-busy", "/srv/hermes/res-busy", 41, 42)

	t.Run("waits and looks again", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		agentStandaloneResStageMarkerTemporaryRace(t, directory, ownerUID, ownerGID, func() {})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(2*time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, `root contains prior lock "62921.lock"`)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("gives up when cancellation arrives first", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		canceled := make(chan struct{})
		agentStandaloneResStageMarkerTemporaryRace(t, directory, ownerUID, ownerGID, func() {
			close(canceled)
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(2*time.Second), canceled, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, errAgentStandaloneCanceled)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// TestAgentStandaloneResAuditRefusesWhenItCannotNameItsOwnRoot proves the
// registry audit refuses before it adjudicates or cleans anything when procfs
// stops naming the registry descriptor. The resolved path is what the audit uses
// to reject an owner whose state root is the registry itself, so an audit that
// carried on without it would lose that rule for the whole pass.
func TestAgentStandaloneResAuditRefusesWhenItCannotNameItsOwnRoot(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "{}\n",
	)
	wantErr := errors.New("procfs stopped naming the registry descriptor")
	previous := agentStandaloneReadlink
	t.Cleanup(func() { agentStandaloneReadlink = previous })
	agentStandaloneReadlink = func(path string) (string, error) {
		if strings.HasPrefix(path, "/proc/self/fd/") {
			return "", wantErr
		}

		return previous(path)
	}

	err := auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, true, false, time.Now().Add(time.Second), nil, nil,
	)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "resolve agent authority root path")
	require.FileExists(t, temporary, "the refused audit must not have cleaned the pending temporary")
}

// TestAgentStandaloneResAuditStopsBetweenReadingAndAdjudicating proves that a
// cancellation observed after the audit has loaded every owner binding but
// before it starts classifying entries aborts the audit, rather than letting it
// finish and report a verdict about a registry the caller is no longer entitled
// to act on.
func TestAgentStandaloneResAuditStopsBetweenReadingAndAdjudicating(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneCovOwner(62931, 62932, "res-audit-stop", "/srv/hermes/res-audit-stop", 51, 52)
	agentStandaloneCovWriteOwner(t, directory, owner)
	canceled := make(chan struct{})
	agentStandaloneCovRestoreDurableSeams(t)
	previous := agentStandaloneDurableFstatat
	loaded := false
	agentStandaloneDurableFstatat = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
		if path == "62931.owner" && !loaded {
			loaded = true
			close(canceled)
		}

		return previous(dirfd, path, stat, flags)
	}

	err := auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, true, false, false, time.Now().Add(time.Second), canceled, nil,
	)
	require.True(t, loaded, "the audit never loaded the staged owner binding")
	require.ErrorIs(t, err, errAgentStandaloneCanceled)
	require.NotContains(t, err.Error(), "permanent owner binding exists")
}

// TestAgentStandaloneResSameBootRebindRefusesBeforeItTakesAnything proves the
// same-boot rebind abandons its shared owners lease — and never reaches for the
// surviving owner's UID lock — when it cannot list the registry at all, and when
// cancellation arrives while it is still counting owner bindings. A rebind that
// held either lock past a refusal would lock the surviving identity out.
func TestAgentStandaloneResSameBootRebindRefusesBeforeItTakesAnything(t *testing.T) {
	t.Run("registry cannot be listed", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovSameBootRegistry(t)
		wantErr := agentStandaloneResFaultAuthorityEntries(t, 1)

		rebind, err := validateAgentStandaloneSameBootRebind(
			directory, owner, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, rebind)
		require.ErrorIs(t, err, wantErr)
		agentStandaloneResLockIsFree(t, directory, ownerUID, ownerGID, "owners.lock")
		agentStandaloneResLockIsFree(t, directory, ownerUID, ownerGID, "62721.lock")
	})

	t.Run("cancellation arrives while counting owners", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovSameBootRegistry(t)
		canceled := make(chan struct{})
		previous := agentStandaloneEntriesOpenat
		t.Cleanup(func() { agentStandaloneEntriesOpenat = previous })
		listed := false
		agentStandaloneEntriesOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
			if !listed {
				listed = true
				close(canceled)
			}

			return previous(dirfd, path, flags, mode)
		}

		rebind, err := validateAgentStandaloneSameBootRebind(
			directory, owner, ownerUID, ownerGID, time.Now().Add(time.Second), canceled, nil,
		)
		require.True(t, listed, "the rebind never listed the registry")
		require.Nil(t, rebind)
		require.ErrorIs(t, err, errAgentStandaloneCanceled)
		agentStandaloneResLockIsFree(t, directory, ownerUID, ownerGID, "owners.lock")
		agentStandaloneResLockIsFree(t, directory, ownerUID, ownerGID, "62721.lock")
	})
}

// agentStandaloneResLockIsFree proves a refused operation left no lease on the
// named permanent lock.
func agentStandaloneResLockIsFree(t *testing.T, directory *os.File, ownerUID, ownerGID uint32, name string) {
	t.Helper()
	contender, acquired, err := tryAgentStandaloneNamedLock(directory, name, false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.True(t, acquired, "the refusal must release %s", name)
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneResTargetMarkerCleanupRefusesAnUnlistableRegistry proves
// the held-lock marker cleanup abandons the pass when it cannot list the
// registry, leaving the pending temporary in place. Reporting success over an
// unread registry would let the owner claim continue as though the target
// identity had no half-written disposition beside it.
func TestAgentStandaloneResTargetMarkerCleanupRefusesAnUnlistableRegistry(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	held := createAgentStandaloneTestLock(t, directory, "62941.lock", ownerUID, ownerGID)
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "62941.quarantine.next-"+agentStandaloneCovSuffix, "{}\n",
	)
	wantErr := agentStandaloneResFaultAuthorityEntries(t, 1)

	err := cleanupAgentStandaloneTargetMarkerTemporaries(
		directory, 62941, held, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.ErrorIs(t, err, wantErr)
	require.FileExists(t, temporary, "the refused cleanup must leave the pending temporary in place")
}
