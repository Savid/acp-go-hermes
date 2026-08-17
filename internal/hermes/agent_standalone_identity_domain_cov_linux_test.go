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

// agentStandaloneCovDomainRegistry stages an authority registry that already
// carries a permanent domain lock and a published domain record, letting a case
// state exactly how that record differs from the domain the process is actually
// running in.
func agentStandaloneCovDomainRegistry(
	t *testing.T,
	mutate func(record *agentAuthorityDomainRecord),
) (*os.File, uint32, uint32) {
	t.Helper()
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "domain.lock")
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	if mutate != nil {
		mutate(&record)
	}
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))

	return directory, ownerUID, ownerGID
}

// agentStandaloneCovOnNthLockOpen runs action immediately before the nth open of
// the named permanent lock. Every re-read the domain claim performs is separated
// from the previous one by exactly one lock open, so this is the only
// deterministic way to stage a peer that changes the registry inside that window.
func agentStandaloneCovOnNthLockOpen(t *testing.T, name string, nth int, action func()) {
	t.Helper()
	restoreAgentStandalonePermanentLockSeams(t)
	original := agentStandaloneLockOpenat
	opens := 0
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		if path == name {
			opens++
			if opens == nth {
				action()
			}
		}

		return original(dirfd, path, flags, mode)
	}
	t.Cleanup(func() {
		require.GreaterOrEqual(t, opens, nth, "the staged peer write never ran")
	})
}

// agentStandaloneCovProbeSeam replaces the durable-filesystem probe for one
// case. The probe writes, locks and renames a real file in the registry, so a
// case that wants to prove what happens when the probe refuses cannot stage that
// through the filesystem the container actually has.
func agentStandaloneCovProbeSeam(t *testing.T, probe func(*os.File, bool) error) {
	t.Helper()
	previous := agentStandaloneFilesystemProbe
	t.Cleanup(func() { agentStandaloneFilesystemProbe = previous })
	agentStandaloneFilesystemProbe = probe
}

// agentStandaloneCovReplaceDomainSeam replaces the domain-record publication for
// one case.
func agentStandaloneCovReplaceDomainSeam(
	t *testing.T,
	replace func(*os.File, uint32, uint32, agentAuthorityDomainRecord) error,
) {
	t.Helper()
	previous := agentStandaloneReplaceDomain
	t.Cleanup(func() { agentStandaloneReplaceDomain = previous })
	agentStandaloneReplaceDomain = replace
}

// agentStandaloneCovRefuseBinder makes the authority binder refuse for a reason
// that does not depend on which PID namespace the test happens to run in, by
// faulting the procfs anchor the binder resolves first.
func agentStandaloneCovRefuseBinder(t *testing.T) error {
	t.Helper()
	wantErr := errors.New("procfs anchor is unreadable")
	previous := agentStandaloneReadlink
	t.Cleanup(func() { agentStandaloneReadlink = previous })
	agentStandaloneReadlink = func(path string) (string, error) {
		require.Equal(t, "/proc/self", path)

		return "", wantErr
	}

	return wantErr
}

// agentStandaloneCovDomainLeaseIsShared proves the returned lease is a shared
// domain lease: another claim may join it, but nobody may take it exclusively.
func agentStandaloneCovDomainLeaseIsShared(t *testing.T, directory *os.File, ownerUID, ownerGID uint32) {
	t.Helper()
	peer, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	require.NoError(t, unix.Flock(int(peer.Fd()), unix.LOCK_SH|unix.LOCK_NB))
	require.ErrorIs(t, unix.Flock(int(peer.Fd()), unix.LOCK_EX|unix.LOCK_NB), unix.EWOULDBLOCK)
}

// agentStandaloneCovDomainLockIsFree proves a refused claim left no lease on the
// permanent domain lock.
func agentStandaloneCovDomainLockIsFree(t *testing.T, directory *os.File, ownerUID, ownerGID uint32) {
	t.Helper()
	contender, acquired, err := tryAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.True(t, acquired, "the refused claim must release the domain lock")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneCovDomainSharedLeaseRefusesARecordThatMovedUnderIt proves
// that when a peer republishes the domain record between the record this claim
// read and the shared lease it is about to return, the claim refuses instead of
// handing back a lease for an authority it never validated.
func TestAgentStandaloneCovDomainSharedLeaseRefusesARecordThatMovedUnderIt(t *testing.T) {
	directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
	want := agentStandaloneCovOwner(62701, 62702, "cov-shared", "/srv/hermes/cov-shared", 1, 2)
	agentStandaloneCovWriteRegistryFile(t, directory, ".authority-probe-"+agentStandaloneCovSuffix, "stale")
	agentStandaloneCovOnNthLockOpen(t, ".authority-probe-"+agentStandaloneCovSuffix, 1, func() {
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "fedcba9876543210fedcba9876543210"
		require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	})

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, authority)
	require.ErrorContains(t, err, "changed during shared-lease transition")
	agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
}

// TestAgentStandaloneCovDomainExclusiveUpgradeRevalidatesTheWholeRegistry
// proves the upgrade from the shared lease to the exclusive lease is not
// trusted on the strength of what was read under the shared lease. The record
// vanishing, becoming unreadable, or a temporary turning untrusted in that
// window must each change the outcome.
func TestAgentStandaloneCovDomainExclusiveUpgradeRevalidatesTheWholeRegistry(t *testing.T) {
	want := agentStandaloneCovOwner(62703, 62704, "cov-upgrade", "/srv/hermes/cov-upgrade", 3, 4)
	temporary := "domain.json.next-" + agentStandaloneCovSuffix

	t.Run("exclusive lock is no longer a trusted inode", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		agentStandaloneCovWriteRegistryFile(t, directory, temporary, "partial")
		agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
			require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "domain.lock"), 0o644))
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "mode")
	})

	t.Run("record vanished, so the claim restarts and mints a fresh authority", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		agentStandaloneCovWriteRegistryFile(t, directory, temporary, "partial")
		agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
			require.NoError(t, os.Remove(filepath.Join(directory.Name(), "domain.json")))
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = authority.Close() })
		require.NoFileExists(t, filepath.Join(directory.Name(), temporary))
		minted, loadErr := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
		require.NoError(t, loadErr)
		require.NotEqual(t, "0123456789abcdef0123456789abcdef", minted.AuthorityID,
			"a restarted claim must mint its own authority id",
		)
		agentStandaloneCovDomainLeaseIsShared(t, directory, ownerUID, ownerGID)
	})

	t.Run("record became unreadable", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		agentStandaloneCovWriteRegistryFile(t, directory, temporary, "partial")
		agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
			agentStandaloneCovWriteRegistryFile(t, directory, "domain.json", "not json\n")
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "invalid character")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("temporary became untrusted", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		path := agentStandaloneCovWriteRegistryFile(t, directory, temporary, "partial")
		agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
			require.NoError(t, os.Chmod(path, 0o644))
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "not a trusted bounded regular file")
		require.FileExists(t, path)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("record moved before the exclusive lease was normalized", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
		agentStandaloneCovWriteRegistryFile(t, directory, temporary, "partial")
		// A live probe holder survives both adjudication passes, so the second
		// open of it is the instant between the exclusive adjudication and the
		// lease normalization.
		probe, probeErr := openAgentStandaloneNamedLock(
			directory, ".authority-probe-"+agentStandaloneCovSuffix, true, ownerUID, ownerGID,
		)
		require.NoError(t, probeErr)
		t.Cleanup(func() { _ = probe.Close() })
		require.NoError(t, unix.Flock(int(probe.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		agentStandaloneCovOnNthLockOpen(t, ".authority-probe-"+agentStandaloneCovSuffix, 2, func() {
			record, err := currentAgentAuthorityDomain(directory)
			require.NoError(t, err)
			record.AuthorityID = "fedcba9876543210fedcba9876543210"
			require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "changed during shared-lease transition")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// TestAgentStandaloneCovDomainRefusesAnUnreadableRecordUnderTheSharedLease
// proves a record that cannot be parsed stops the claim outright rather than
// being treated as an absent record, which would let a corrupted record be
// replaced by a freshly minted authority.
func TestAgentStandaloneCovDomainRefusesAnUnreadableRecordUnderTheSharedLease(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "domain.lock")
	agentStandaloneCovWriteRegistryFile(t, directory, "domain.json", "not json\n")
	want := agentStandaloneCovOwner(62705, 62706, "cov-corrupt", "/srv/hermes/cov-corrupt", 5, 6)

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, authority)
	require.ErrorContains(t, err, "invalid character")
	agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
}

// TestAgentStandaloneCovForeignDomainRebindRefusesEveryUnsafeRegistry proves
// each refusal on the path that takes over an authority belonging to another
// domain: the exclusive lock no longer being trusted, an owner temporary the
// claim may not remove, an unaccountable registry entry, a marker temporary
// with a live UID holder, and a same-boot rebind whose registry is incomplete.
func TestAgentStandaloneCovForeignDomainRebindRefusesEveryUnsafeRegistry(t *testing.T) {
	want := agentStandaloneCovOwner(62711, 62712, "cov-foreign", "/srv/hermes/cov-foreign", 7, 8)
	foreign := func(record *agentAuthorityDomainRecord) { record.PIDNamespace.Ino++ }

	t.Run("exclusive lock is no longer a trusted inode", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreign)
		agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
			require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "domain.lock"), 0o644))
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "mode")
	})

	t.Run("malformed owner temporary", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreign)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		path := agentStandaloneCovWriteRegistryFile(
			t, directory, "not-a-uid.owner.next-"+agentStandaloneCovSuffix, "partial",
		)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "invalid name")
		require.FileExists(t, path)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("unaccountable registry entry", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreign)
		agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, `unknown entry "leftover"`)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("marker temporary with a live uid holder", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreign)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		path := agentStandaloneCovWriteRegistryFile(
			t, directory, "62713.quarantine.next-"+agentStandaloneCovSuffix, "partial",
		)
		held := createAgentStandaloneTestLock(t, directory, "62713.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(80*time.Millisecond), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.FileExists(t, path, "a busy marker temporary must never be removed")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("same-boot rebind without an owners lock", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreign)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, unix.ENOENT)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// agentStandaloneCovSameBootRegistry stages the exact registry a same-boot
// authority rebind requires: a foreign PID-namespace domain record from this
// same boot, one standalone owner with its permanent UID lock, and that owner's
// retained ACTIVE marker.
func agentStandaloneCovSameBootRegistry(t *testing.T) (*os.File, uint32, uint32, agentStandaloneOwner) {
	t.Helper()
	directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
		record.PIDNamespace.Ino++
	})
	owner := agentStandaloneCovOwner(62721, 62722, "cov-same-boot", "/srv/hermes/cov-same-boot", 9, 10)
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "62721.lock")
	require.NoError(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID))
	agentStandaloneCovWriteActiveMarker(t, directory, owner)
	agentStandaloneCovVacancySeam(t, agentStandaloneCovVacantScan)

	return directory, ownerUID, ownerGID, owner
}

// TestAgentStandaloneCovSameBootRebindReleasesItsUIDLockOnEveryRefusal proves
// that once the same-boot rebind has taken the surviving owner's UID lock, every
// later refusal — the durability probe, the record publication, and the
// post-publication recheck — releases it again. A retained UID lock would lock
// that identity out for the lifetime of the process.
func TestAgentStandaloneCovSameBootRebindReleasesItsUIDLockOnEveryRefusal(t *testing.T) {
	t.Run("durability probe refuses", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovSameBootRegistry(t)
		wantErr := errors.New("injected durability probe refusal")
		agentStandaloneCovProbeSeam(t, func(*os.File, bool) error { return wantErr })

		authority, err := acquireAgentStandaloneDomain(
			directory, owner, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62721.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, acquired, "the refused rebind must release the owner UID lock")
		require.NoError(t, contender.Close())
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("record publication refuses", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovSameBootRegistry(t)
		before, readErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, readErr)
		wantErr := errors.New("injected record publication refusal")
		agentStandaloneCovReplaceDomainSeam(t, func(
			*os.File, uint32, uint32, agentAuthorityDomainRecord,
		) error {
			return wantErr
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, owner, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		after, afterErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, afterErr)
		require.Equal(t, before, after, "a refused publication must leave the old record intact")
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "62721.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, acquired, "the refused rebind must release the owner UID lock")
		require.NoError(t, contender.Close())
	})

	t.Run("published record is not the record the claim asked for", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovSameBootRegistry(t)
		agentStandaloneCovReplaceDomainSeam(t, func(
			dir *os.File, uid, gid uint32, record agentAuthorityDomainRecord,
		) error {
			record.AuthorityID = "fedcba9876543210fedcba9876543210"

			return replaceAgentStandaloneDomainRecord(dir, uid, gid, record)
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, owner, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "changed during shared-lease transition")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// TestAgentStandaloneCovForeignDomainRefusesWhenNoRebindIsPossible proves the
// take-over of a foreign-boot authority still publishes through the same probe
// and publication gates even when there is no same-boot owner to rebind, and
// that a refusal there leaves the previous record in place.
func TestAgentStandaloneCovForeignDomainRefusesWhenNoRebindIsPossible(t *testing.T) {
	want := agentStandaloneCovOwner(62731, 62732, "cov-foreign-boot", "/srv/hermes/cov-foreign-boot", 11, 12)
	foreignBoot := func(record *agentAuthorityDomainRecord) {
		record.BootID = "00000000-0000-0000-0000-000000000001"
	}

	t.Run("publication refuses", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreignBoot)
		before, readErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, readErr)
		wantErr := errors.New("injected record publication refusal")
		agentStandaloneCovReplaceDomainSeam(t, func(
			*os.File, uint32, uint32, agentAuthorityDomainRecord,
		) error {
			return wantErr
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		after, afterErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, afterErr)
		require.Equal(t, before, after)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("published record is not the record the claim asked for", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreignBoot)
		agentStandaloneCovReplaceDomainSeam(t, func(
			dir *os.File, uid, gid uint32, record agentAuthorityDomainRecord,
		) error {
			record.AuthorityID = "fedcba9876543210fedcba9876543210"

			return replaceAgentStandaloneDomainRecord(dir, uid, gid, record)
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "changed during shared-lease transition")
	})
}

// TestAgentStandaloneCovDomainAdoptsAPeerRecordPublishedDuringTheUpgrade proves
// that when a peer publishes a domain record matching this domain between this
// claim's shared lease and its exclusive lease, the claim adopts that record
// instead of refusing. The claim carries the peer's authority id into the domain
// it revalidates, so the recheck agrees, and it returns the exclusive lease
// downgraded to a shared one without republishing anything: the peer's record
// must still be the very same inode, byte for byte, once the claim succeeds.
func TestAgentStandaloneCovDomainAdoptsAPeerRecordPublishedDuringTheUpgrade(t *testing.T) {
	directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
		record.PIDNamespace.Ino++
	})
	want := agentStandaloneCovOwner(62741, 62742, "cov-peer-record", "/srv/hermes/cov-peer-record", 13, 14)
	recordPath := filepath.Join(directory.Name(), "domain.json")
	var peerBytes []byte
	var peerStat unix.Stat_t
	agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "fedcba9876543210fedcba9876543210"
		require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
		peerBytes, err = os.ReadFile(recordPath)
		require.NoError(t, err)
		require.NoError(t, unix.Stat(recordPath, &peerStat))
	})

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, authority)
	t.Cleanup(func() { _ = authority.Close() })
	adopted, loadErr := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
	require.NoError(t, loadErr)
	require.Equal(t, "fedcba9876543210fedcba9876543210", adopted.AuthorityID,
		"the claim must adopt the peer's authority id, not mint or keep its own",
	)
	after, readErr := os.ReadFile(recordPath)
	require.NoError(t, readErr)
	require.Equal(t, peerBytes, after)
	var afterStat unix.Stat_t
	require.NoError(t, unix.Stat(recordPath, &afterStat))
	require.Equal(t, peerStat.Ino, afterStat.Ino, "an adopting claim must not republish the record")
	agentStandaloneCovDomainLeaseIsShared(t, directory, ownerUID, ownerGID)
}

// TestAgentStandaloneCovDomainRefusesAnAdoptedRecordThatMovedAgain proves the
// adoption above is not final until the shared lease is actually in force. A
// second peer that republishes the record while this claim downgrades its
// exclusive lease must be refused, because the lease the claim is about to
// return would otherwise name an authority id that is no longer published.
func TestAgentStandaloneCovDomainRefusesAnAdoptedRecordThatMovedAgain(t *testing.T) {
	directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
		record.PIDNamespace.Ino++
	})
	want := agentStandaloneCovOwner(62745, 62746, "cov-peer-moved", "/srv/hermes/cov-peer-moved", 15, 16)
	publish := func(authorityID string) {
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = authorityID
		require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	}
	adopting := false
	moves := 0
	agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
		publish("fedcba9876543210fedcba9876543210")
		adopting = true
	})
	// The downgrade to the shared lease is the only bare LOCK_SH in this claim;
	// every acquisition asks for LOCK_NB as well. Staging the second peer write
	// there is the only way to move the record inside the window between the
	// record the claim adopted and the record it rechecks.
	previous := agentStandaloneDurableFlock
	t.Cleanup(func() { agentStandaloneDurableFlock = previous })
	agentStandaloneDurableFlock = func(fd, how int) error {
		if adopting && how == unix.LOCK_SH {
			adopting = false
			moves++
			publish("89abcdef0123456789abcdef01234567")
		}

		return previous(fd, how)
	}

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
	)
	require.Nil(t, authority)
	require.ErrorContains(t, err, "changed during shared-lease transition")
	require.Equal(t, 1, moves, "the second peer write must land inside the downgrade")
	moved, loadErr := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
	require.NoError(t, loadErr)
	require.Equal(t, "89abcdef0123456789abcdef01234567", moved.AuthorityID,
		"the refusal must leave the newest peer record in place",
	)
	agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
}

// TestAgentStandaloneCovPristineDomainRefusesEveryUnsafeRegistry proves the
// mint-a-fresh-authority path is gated on the same evidence as the take-over
// path: a record that appears unreadable under the exclusive lease, an owner
// temporary the claim may not remove, any registry state at all, a marker
// temporary with a live UID holder, the durability probe, the publication, and
// the post-publication recheck.
func TestAgentStandaloneCovPristineDomainRefusesEveryUnsafeRegistry(t *testing.T) {
	want := agentStandaloneCovOwner(62751, 62752, "cov-pristine", "/srv/hermes/cov-pristine", 15, 16)
	pristine := func(t *testing.T) (*os.File, uint32, uint32) {
		t.Helper()
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")

		return directory, ownerUID, ownerGID
	}

	t.Run("record became unreadable under the exclusive lease", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
			agentStandaloneCovWriteRegistryFile(t, directory, "domain.json", "not json\n")
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "invalid character")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("malformed owner temporary", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		path := agentStandaloneCovWriteRegistryFile(
			t, directory, "not-a-uid.owner.next-"+agentStandaloneCovSuffix, "partial",
		)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "invalid name")
		require.FileExists(t, path)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("owner temporary with a live uid holder", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		path := agentStandaloneCovWriteRegistryFile(
			t, directory, "62753.owner.next-"+agentStandaloneCovSuffix, "partial",
		)
		held := createAgentStandaloneTestLock(t, directory, "62753.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(80*time.Millisecond), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.FileExists(t, path)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("registry state without a record", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, `unknown entry "leftover"`)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("marker temporary without a record", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		path := agentStandaloneCovWriteRegistryFile(
			t, directory, "62755.quarantine.next-"+agentStandaloneCovSuffix, "partial",
		)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, unix.ENOENT, "cleanup must refuse a temporary whose permanent UID lock is gone")
		require.FileExists(t, path)
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("durability probe refuses", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		wantErr := errors.New("injected durability probe refusal")
		agentStandaloneCovProbeSeam(t, func(*os.File, bool) error { return wantErr })

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("publication refuses", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		wantErr := errors.New("injected record publication refusal")
		agentStandaloneCovReplaceDomainSeam(t, func(
			*os.File, uint32, uint32, agentAuthorityDomainRecord,
		) error {
			return wantErr
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("published record is not the record the claim asked for", func(t *testing.T) {
		directory, ownerUID, ownerGID := pristine(t)
		agentStandaloneCovReplaceDomainSeam(t, func(
			dir *os.File, uid, gid uint32, record agentAuthorityDomainRecord,
		) error {
			record.AuthorityID = "fedcba9876543210fedcba9876543210"

			return replaceAgentStandaloneDomainRecord(dir, uid, gid, record)
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, "changed during shared-lease transition")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// TestAgentStandaloneCovNonTestClaimRequiresTheAuthorityBinder proves a claim
// that is not test-only refuses to establish or take over an authority when the
// binder cannot prove its own procfs anchor, on both the take-over path and the
// mint path. Skipping the binder would let a process inside a restricted procfs
// view mint an authority the host cannot account for.
func TestAgentStandaloneCovNonTestClaimRequiresTheAuthorityBinder(t *testing.T) {
	want := agentStandaloneCovOwner(62761, 62762, "cov-binder", "/srv/hermes/cov-binder", 17, 18)

	t.Run("taking over a foreign domain", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
			record.PIDNamespace.Ino++
		})
		wantErr := agentStandaloneCovRefuseBinder(t)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "resolve procfs self PID anchor")
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("minting a fresh authority", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		wantErr := agentStandaloneCovRefuseBinder(t)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}

// TestAgentStandaloneCovAuthorityBinderAdmitsOnlyTheInitialPIDNamespace proves
// the binder's whole decision: it resolves its own procfs anchor, proves it can
// read PID 1, and then admits the process only when it is in the initial PID
// namespace or is PID 1 of its own namespace. The expectation is derived from
// the namespace the test itself observes, so the case states the same property
// whether or not the harness shares the host PID namespace.
func TestAgentStandaloneCovAuthorityBinderAdmitsOnlyTheInitialPIDNamespace(t *testing.T) {
	const initialPIDNamespaceInode = 0xeffffffc
	self, err := validateAgentAuthorityPIDVisibility()
	require.NoError(t, err)

	binderErr := validateAgentStandaloneBinder()
	if self.Ino == initialPIDNamespaceInode || os.Getpid() == 1 {
		require.NoError(t, binderErr)

		return
	}
	require.ErrorContains(t, binderErr,
		"non-initial PID namespace may establish agent authority only from namespace PID 1",
	)
}

// TestAgentStandaloneCovDomainRebindRefusesABusyMarkerItCannotWaitOut proves
// the one rebind arm that finds a busy marker temporary and then cannot wait to
// re-observe the domain: it surfaces the wait refusal, reports no retry,
// releases the exclusive lease it was handed and leaves the busy temporary for
// its live holder. Reporting retry there would send the caller back around the
// claim loop with a budget it has already spent, and unlinking the temporary
// would delete a file another claim is still working on.
//
// The concurrent "marker temporary with a live uid holder" case only reaches
// this arm when its budget happens to expire on the same pass that finds the
// marker busy — the claim loop's own budget check wins otherwise — so this case
// drives the arm directly.
//
// A zero deadline is the only refusal signal that cannot race here. Everything
// between the rebind entry and the audit's busy verdict (the owner-temporary
// drain, the owner collection pass and the classifying pass) calls
// checkAgentStandaloneAcquisition once per registry entry, and that reads a
// closed cancel channel or an expired deadline as a refusal — either would stop
// the audit before the marker is ever adjudicated. It reads a zero deadline as
// no budget at all and passes. waitAgentStandaloneRetry reads that same zero
// deadline as remaining <= 0 and refuses immediately, before it builds a timer
// or evaluates any select, so the refusal is a straight-line consequence of the
// input rather than a scheduling outcome.
func TestAgentStandaloneCovDomainRebindRefusesABusyMarkerItCannotWaitOut(t *testing.T) {
	// The published record names another PID namespace, so the rebind cannot
	// downgrade to a shared lease and has to settle the registry first.
	foreign := func(record *agentAuthorityDomainRecord) { record.PIDNamespace.Ino++ }
	directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, foreign)
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	held := createAgentStandaloneTestLock(t, directory, "62771.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "62771.quarantine.next-"+agentStandaloneCovSuffix, "partial",
	)

	// The registry carries no owner temporaries, so the drain that precedes the
	// audit stays clean and the busy verdict the rebind acts on can only be the
	// marker temporary's.
	ownerTempsBusy, drainErr := drainAgentStandaloneDomainOwnerTemporaries(
		directory, ownerUID, ownerGID, time.Time{}, nil, nil,
	)
	require.NoError(t, drainErr)
	require.False(t, ownerTempsBusy, "the busy arm under test must be the audit's, not the drain's")
	require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, true, false, time.Time{}, nil, nil,
	), errAgentStandaloneMarkerTempBusy)

	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	foreign(&record)

	exclusive, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	t.Cleanup(func() {
		if exclusive.Fd() != ^uintptr(0) {
			require.NoError(t, exclusive.Close())
		}
	})
	require.NoError(t, unix.Flock(int(exclusive.Fd()), unix.LOCK_EX|unix.LOCK_NB))

	want := agentStandaloneCovOwner(62771, 62772, "rebind-busy", "/srv/hermes/rebind-busy", 1, 2)

	authority, retry, err := rebindAgentStandaloneDomain(
		directory, exclusive, want, ownerUID, ownerGID, true, record, time.Time{}, nil, nil,
	)
	require.Nil(t, authority)
	require.False(t, retry, "a rebind that cannot wait must refuse, never ask for another pass")
	require.EqualError(t, err, "standalone agent identity acquisition exceeded 30 seconds")
	require.NotErrorIs(t, err, errAgentStandaloneMarkerTempBusy,
		"the wait refusal replaces the audit verdict it was reached through")
	require.ErrorIs(t, exclusive.Close(), os.ErrClosed,
		"the refused rebind releases the exclusive lease it was handed")
	require.FileExists(t, temporary, "a busy marker temporary is never unlinked")
	agentStandaloneCovDomainLockIsFree(t, directory, ownerUID, ownerGID)
}
