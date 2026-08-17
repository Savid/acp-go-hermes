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

// agentStandaloneCovRestoreDurableSeams restores every durable-write seam when
// the case ends, so a case only has to state the one it faults.
func agentStandaloneCovRestoreDurableSeams(t *testing.T) {
	t.Helper()
	fchown, fchmod, flock := agentStandaloneDurableFchown, agentStandaloneDurableFchmod, agentStandaloneDurableFlock
	write, sync := agentStandaloneDurableWrite, agentStandaloneDurableSync
	fstat, fstatat := agentStandaloneDurableFstat, agentStandaloneDurableFstatat
	unlinkat := agentStandaloneDurableUnlinkat
	t.Cleanup(func() {
		agentStandaloneDurableFchown, agentStandaloneDurableFchmod = fchown, fchmod
		agentStandaloneDurableFlock = flock
		agentStandaloneDurableWrite, agentStandaloneDurableSync = write, sync
		agentStandaloneDurableFstat, agentStandaloneDurableFstatat = fstat, fstatat
		agentStandaloneDurableUnlinkat = unlinkat
	})
}

// agentStandaloneCovFaultDurable faults one durable-write syscall. Each of these
// runs against a descriptor or a name the code has just created and validated,
// so the only way to state what happens when the kernel stops answering for it
// is to fault the call itself. The property under test is never "the fault came
// back" — it is that nothing was published and nothing was left behind.
func agentStandaloneCovFaultDurable(t *testing.T, which string, wantErr error) {
	t.Helper()
	agentStandaloneCovRestoreDurableSeams(t)
	switch which {
	case "chown":
		agentStandaloneDurableFchown = func(int, int, int) error { return wantErr }
	case "chmod":
		agentStandaloneDurableFchmod = func(int, uint32) error { return wantErr }
	case "write":
		agentStandaloneDurableWrite = func(*os.File, []byte) (int, error) { return 0, wantErr }
	case "sync":
		agentStandaloneDurableSync = func(*os.File) error { return wantErr }
	case "fstat":
		agentStandaloneDurableFstat = func(int, *unix.Stat_t) error { return wantErr }
	default:
		t.Fatalf("unknown durable fault %q", which)
	}
}

var agentStandaloneCovDurableFaults = []string{"chown", "chmod", "write", "sync", "fstat"}

// TestAgentStandaloneCovOwnerPublicationAbandonsATemporaryItCannotFinish proves
// that whenever the immutable owner binding's temporary cannot be given its
// metadata, written, flushed or identified, the binding is never published and
// the temporary is removed. A half-written temporary left in the registry is
// exactly what every later claim's drain then has to adjudicate.
func TestAgentStandaloneCovOwnerPublicationAbandonsATemporaryItCannotFinish(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	for _, fault := range agentStandaloneCovDurableFaults {
		t.Run(fault, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			owner := agentStandaloneCovOwner(62601, 62602, "cov-durable-owner", "/srv/hermes/cov-durable-owner", 1, 2)
			wantErr := errors.New("injected " + fault + " failure")
			agentStandaloneCovFaultDurable(t, fault, wantErr)

			require.ErrorIs(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID), wantErr)
			entries, readErr := os.ReadDir(directory.Name())
			require.NoError(t, readErr)
			require.Empty(t, entries, "an unfinished owner temporary must not survive")
		})
	}
}

// TestAgentStandaloneCovMarkerPublicationAbandonsATemporaryItCannotFinish
// proves the same for the identity marker: no marker is published and no
// temporary survives when the temporary cannot be finished.
func TestAgentStandaloneCovMarkerPublicationAbandonsATemporaryItCannotFinish(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	for _, fault := range agentStandaloneCovDurableFaults {
		t.Run(fault, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			wantErr := errors.New("injected " + fault + " failure")
			agentStandaloneCovFaultDurable(t, fault, wantErr)

			require.ErrorIs(t, replaceAgentStandaloneFile(
				directory, "62603.quarantine", []byte(`{"version":2}`), ownerUID, ownerGID,
				time.Now().Add(time.Second), nil, nil,
			), wantErr)
			entries, readErr := os.ReadDir(directory.Name())
			require.NoError(t, readErr)
			require.Empty(t, entries, "an unfinished marker temporary must not survive")
		})
	}
}

// TestAgentStandaloneCovDomainPublicationAbandonsATemporaryItCannotFinish
// proves the same for the authority domain record, and that the record already
// on disk is untouched.
func TestAgentStandaloneCovDomainPublicationAbandonsATemporaryItCannotFinish(t *testing.T) {
	for _, fault := range agentStandaloneCovDurableFaults {
		t.Run(fault, func(t *testing.T) {
			directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, nil)
			before, readErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
			require.NoError(t, readErr)
			record, currentErr := currentAgentAuthorityDomain(directory)
			require.NoError(t, currentErr)
			record.AuthorityID = "fedcba9876543210fedcba9876543210"
			wantErr := errors.New("injected " + fault + " failure")
			agentStandaloneCovFaultDurable(t, fault, wantErr)

			require.ErrorIs(t,
				replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record),
				wantErr,
			)
			after, afterErr := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
			require.NoError(t, afterErr)
			require.Equal(t, before, after, "the published record must be untouched")
			entries, listErr := os.ReadDir(directory.Name())
			require.NoError(t, listErr)
			require.Len(t, entries, 2, "an unfinished record temporary must not survive")
		})
	}
}

// TestAgentStandaloneCovPublicationReportsATemporaryItCouldNotRemove proves each
// publication path reports failure when it could not unlink the temporary it
// created, even on an otherwise successful publication. A temporary the registry
// cannot account for is what the next claim's audit refuses.
func TestAgentStandaloneCovPublicationReportsATemporaryItCouldNotRemove(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	wantErr := errors.New("injected unlink failure")

	t.Run("owner binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		owner := agentStandaloneCovOwner(62605, 62606, "cov-unlink-owner", "/srv/hermes/cov-unlink-owner", 3, 4)
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableUnlinkat = func(int, string, int) error { return wantErr }

		require.ErrorIs(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID), wantErr)
		require.FileExists(t, filepath.Join(directory.Name(), "62605.owner"))
	})

	t.Run("identity marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableUnlinkat = func(int, string, int) error { return wantErr }

		require.ErrorIs(t, replaceAgentStandaloneFile(
			directory, "62607.quarantine", []byte(`{"version":2}`), ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})

	t.Run("domain record", func(t *testing.T) {
		directory, recordUID, recordGID := agentStandaloneCovDomainRegistry(t, nil)
		record, currentErr := currentAgentAuthorityDomain(directory)
		require.NoError(t, currentErr)
		record.AuthorityID = "fedcba9876543210fedcba9876543210"
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableUnlinkat = func(int, string, int) error { return wantErr }

		require.ErrorIs(t,
			replaceAgentStandaloneDomainRecord(directory, recordUID, recordGID, record),
			wantErr,
		)
	})
}

// TestAgentStandaloneCovPublicationRefusesAnUnverifiableNamedInode proves each
// publication refuses when the name it published under cannot be confirmed to be
// the very inode it wrote. Without that confirmation a peer could have replaced
// the name between the rename and the check, and the claim would report having
// published state it never wrote.
func TestAgentStandaloneCovPublicationRefusesAnUnverifiableNamedInode(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	wantErr := errors.New("injected named inode check failure")
	faultNth := func(t *testing.T, nth int) {
		t.Helper()
		agentStandaloneCovRestoreDurableSeams(t)
		previous := agentStandaloneDurableFstatat
		calls := 0
		agentStandaloneDurableFstatat = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
			calls++
			if calls == nth {
				return wantErr
			}

			return previous(dirfd, path, stat, flags)
		}
	}

	t.Run("owner binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		owner := agentStandaloneCovOwner(62611, 62612, "cov-named-owner", "/srv/hermes/cov-named-owner", 5, 6)
		faultNth(t, 3)

		err := createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "published standalone owner is not its trusted named inode")
	})

	t.Run("identity marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		faultNth(t, 3)

		err := replaceAgentStandaloneFile(
			directory, "62613.quarantine", []byte(`{"version":2}`), ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "published agent identity marker is not the temporary inode")
	})

	t.Run("domain record", func(t *testing.T) {
		directory, recordUID, recordGID := agentStandaloneCovDomainRegistry(t, nil)
		record, currentErr := currentAgentAuthorityDomain(directory)
		require.NoError(t, currentErr)
		record.AuthorityID = "fedcba9876543210fedcba9876543210"
		faultNth(t, 1)

		err := replaceAgentStandaloneDomainRecord(directory, recordUID, recordGID, record)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "published agent authority record is not the temporary inode")
	})
}

// TestAgentStandaloneCovRegistryReadRefusesAnUnverifiableInode proves a registry
// payload is only accepted when the descriptor it was read through can be
// inspected before the read and still agrees with the name afterwards. A payload
// read across a replacement would be attributed to an inode the reader never
// validated.
func TestAgentStandaloneCovRegistryReadRefusesAnUnverifiableInode(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneCovOwner(62621, 62622, "cov-read", "/srv/hermes/cov-read", 7, 8)

	t.Run("descriptor cannot be inspected", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, owner)
		wantErr := errors.New("injected descriptor inspection failure")
		agentStandaloneCovFaultDurable(t, "fstat", wantErr)

		loaded, err := loadAgentStandaloneOwner(directory, owner.UID, ownerUID, ownerGID)
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, agentStandaloneOwner{}, loaded)
	})

	t.Run("name changed while the payload was read", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, owner)
		wantErr := errors.New("injected post-read inspection failure")
		agentStandaloneCovRestoreDurableSeams(t)
		previous := agentStandaloneDurableFstatat
		calls := 0
		agentStandaloneDurableFstatat = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
			calls++
			if calls == 2 {
				return wantErr
			}

			return previous(dirfd, path, stat, flags)
		}

		loaded, err := loadAgentStandaloneOwner(directory, owner.UID, ownerUID, ownerGID)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "changed while its payload was read")
		require.Equal(t, agentStandaloneOwner{}, loaded)
	})
}

// TestAgentStandaloneCovPermanentLockRefusesADescriptorItCannotInspect proves a
// permanent lock is never accepted when its own descriptor cannot be inspected,
// so the named-inode comparison that follows can never be skipped.
func TestAgentStandaloneCovPermanentLockRefusesADescriptorItCannotInspect(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	wantErr := errors.New("injected lock descriptor inspection failure")
	agentStandaloneCovFaultDurable(t, "fstat", wantErr)

	lock, err := openAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.Nil(t, lock)
	require.ErrorIs(t, err, wantErr)
}

// TestAgentStandaloneCovLockOperationsFailClosedOnAnUnusableDescriptor proves
// that a lock operation which fails for a reason other than contention is a
// refusal, not a retry: the blocking acquisition and the non-blocking attempt
// both release the descriptor and report it, and the probe-temporary cleanup
// refuses rather than unlinking a probe it never locked.
func TestAgentStandaloneCovLockOperationsFailClosedOnAnUnusableDescriptor(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	wantErr := errors.New("injected lock operation failure")

	t.Run("blocking acquisition", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableFlock = func(int, int) error { return wantErr }

		lock, err := acquireAgentStandaloneNamedLock(
			directory, "owners.lock", unix.LOCK_EX, false, ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lock)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("non-blocking attempt", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableFlock = func(int, int) error { return wantErr }

		lock, acquired, err := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
		require.Nil(t, lock)
		require.False(t, acquired)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("probe temporary cleanup", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := ".authority-probe-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "stale")
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableFlock = func(int, int) error { return wantErr }

		require.ErrorIs(t, cleanupAgentStandaloneProbeTemporary(directory, name, ownerUID, ownerGID), wantErr)
		require.FileExists(t, path, "a probe the cleanup never locked must not be unlinked")
	})

	t.Run("shared lease normalization", func(t *testing.T) {
		directory, leaseUID, leaseGID := agentStandaloneCovDomainRegistry(t, nil)
		record, loadErr := loadAgentAuthorityDomainRecord(directory, leaseUID, leaseGID)
		require.NoError(t, loadErr)
		lease, openErr := openAgentStandaloneNamedLock(directory, "domain.lock", false, leaseUID, leaseGID)
		require.NoError(t, openErr)
		t.Cleanup(func() { require.NoError(t, lease.Close()) })
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableFlock = func(int, int) error { return wantErr }

		err := normalizeAgentStandaloneSharedDomainLease(directory, lease, leaseUID, leaseGID, record)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "normalize agent authority domain shared lease")
	})
}

// TestAgentStandaloneCovDomainRefusesALeaseItCannotDowngrade proves the domain
// claim refuses when it cannot downgrade its exclusive lease to the shared lease
// it is about to hand back. Returning an exclusive lease would silently exclude
// every peer that is entitled to share the same authority.
func TestAgentStandaloneCovDomainRefusesALeaseItCannotDowngrade(t *testing.T) {
	want := agentStandaloneCovOwner(62631, 62632, "cov-downgrade", "/srv/hermes/cov-downgrade", 9, 10)
	wantErr := errors.New("injected lease downgrade failure")
	faultDowngrade := func(t *testing.T) {
		t.Helper()
		agentStandaloneCovRestoreDurableSeams(t)
		previous := agentStandaloneDurableFlock
		agentStandaloneDurableFlock = func(fd, how int) error {
			if how == unix.LOCK_SH {
				return wantErr
			}

			return previous(fd, how)
		}
	}

	t.Run("minting a fresh authority", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		faultDowngrade(t)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("taking over a foreign domain", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
			record.BootID = "00000000-0000-0000-0000-000000000001"
		})
		faultDowngrade(t)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("adopting a record a peer published during the upgrade", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDomainRegistry(t, func(record *agentAuthorityDomainRecord) {
			record.PIDNamespace.Ino++
		})
		agentStandaloneCovOnNthLockOpen(t, "domain.lock", 2, func() {
			record, err := currentAgentAuthorityDomain(directory)
			require.NoError(t, err)
			record.AuthorityID = "fedcba9876543210fedcba9876543210"
			require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
		})
		faultDowngrade(t)

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, wantErr)
	})
}

// TestAgentStandaloneCovTemporaryCleanupReportsAnUnlinkItCouldNotComplete
// proves every temporary cleanup reports failure when the unlink it authorised
// did not happen, instead of reporting the registry clean while the temporary is
// still there.
func TestAgentStandaloneCovTemporaryCleanupReportsAnUnlinkItCouldNotComplete(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	wantErr := errors.New("injected temporary unlink failure")
	faultUnlink := func(t *testing.T) {
		t.Helper()
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableUnlinkat = func(int, string, int) error { return wantErr }
	}

	t.Run("owner temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "62641.owner.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		faultUnlink(t)

		require.ErrorIs(t, cleanupAgentStandaloneOwnerTemporary(directory, name, ownerUID, ownerGID), wantErr)
		require.FileExists(t, path)
	})

	t.Run("domain temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "domain.json.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		faultUnlink(t)

		require.ErrorIs(t, cleanupAgentStandaloneDomainTemporary(directory, name, ownerUID, ownerGID), wantErr)
		require.FileExists(t, path)
	})

	t.Run("probe temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := ".authority-probe-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "stale")
		faultUnlink(t)

		require.ErrorIs(t, cleanupAgentStandaloneProbeTemporary(directory, name, ownerUID, ownerGID), wantErr)
		require.FileExists(t, path)
	})

	t.Run("marker temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "62643.lock")
		name := "62643.quarantine.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		faultUnlink(t)

		require.ErrorIs(t,
			cleanupAgentStandaloneMarkerTemporary(directory, 62643, name, ownerUID, ownerGID),
			wantErr,
		)
		require.FileExists(t, path)
	})

	t.Run("target marker temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		uidLock := createAgentStandaloneTestLock(t, directory, "62645.lock", ownerUID, ownerGID)
		name := "62645.quarantine.next-" + agentStandaloneCovSuffix
		path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		faultUnlink(t)

		require.ErrorIs(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 62645, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), wantErr)
		require.FileExists(t, path)
	})
}

// TestAgentStandaloneCovProbeRefusesEveryDurabilityStepItCannotComplete proves
// the durability probe treats each step as load-bearing: the probe file's
// ownership and mode, its exclusive lock, the write, the flush, the inode
// identity, and the rename preserving that identity. A filesystem that silently
// skipped any of them cannot carry an authority registry.
func TestAgentStandaloneCovProbeRefusesEveryDurabilityStepItCannotComplete(t *testing.T) {
	for _, fault := range agentStandaloneCovDurableFaults {
		t.Run(fault, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			wantErr := errors.New("injected " + fault + " failure")
			agentStandaloneCovFaultDurable(t, fault, wantErr)

			require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
			entries, readErr := os.ReadDir(directory.Name())
			require.NoError(t, readErr)
			require.Empty(t, entries, "a refused probe must remove its probe file")
		})
	}

	t.Run("exclusive lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected probe lock failure")
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableFlock = func(int, int) error { return wantErr }

		require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
	})

	t.Run("separate-open exclusion", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableFlock = func(int, int) error { return nil }

		require.ErrorContains(t,
			probeAgentStandaloneFilesystem(directory, true),
			"lacks separate-open flock exclusion",
		)
	})

	t.Run("rename preserved the inode", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected renamed inode check failure")
		agentStandaloneCovRestoreDurableSeams(t)
		agentStandaloneDurableFstatat = func(int, string, *unix.Stat_t, int) error { return wantErr }

		err := probeAgentStandaloneFilesystem(directory, true)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "rename did not preserve inode identity")
	})
}

// TestAgentStandaloneCovMatchingDomainAdjudicationReportsAFailedCleanup proves
// the shared-lease adjudication reports failure when the domain-record temporary
// it decided to remove is still there afterwards, rather than returning "nothing
// requires the exclusive lease".
func TestAgentStandaloneCovMatchingDomainAdjudicationReportsAFailedCleanup(t *testing.T) {
	directory, ownerUID, ownerGID, _ := createAgentStandaloneMatchingDomainFixture(t)
	name := "domain.json.next-" + agentStandaloneCovSuffix
	path := agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
	wantErr := errors.New("injected domain temporary unlink failure")
	agentStandaloneCovRestoreDurableSeams(t)
	agentStandaloneDurableUnlinkat = func(int, string, int) error { return wantErr }

	requiresExclusive, err := adjudicateAgentStandaloneMatchingDomainTemporaries(
		directory, ownerUID, ownerGID, true,
	)
	require.ErrorIs(t, err, wantErr)
	require.False(t, requiresExclusive)
	require.FileExists(t, path)
}
