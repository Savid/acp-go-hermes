//go:build linux

package hermes

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

func TestAgentStandaloneOwnerSessionKeyExactVector(t *testing.T) {
	owner := agentStandaloneOwner{
		Version:  1,
		UID:      62001,
		GID:      62001,
		Kind:     agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID,
		OwnerID:  "Tenant-A",
		StateRoot: agentStandaloneStateRoot{
			Path: "/srv/hermes/tenant-a",
			Dev:  123,
			Ino:  456,
		},
	}

	key := agentStandaloneSessionKey(owner)
	require.Equal(t, "standalone:63e979e5a3ab9230a7ab8218acf0067cdc207934f58beebb852c1be3c82f7ec6", key)
}

func TestAgentStandaloneOwnerCloseFailurePreventsPublication(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneOwner{
		Version: 1, UID: 62011, GID: 62012, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "close-fault",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/close-fault", Dev: 1, Ino: 2},
	}
	wantErr := errors.New("injected close failure")
	previous := agentStandaloneCloseTemporary
	agentStandaloneCloseTemporary = func(file *os.File) error {
		require.NoError(t, file.Close())

		return wantErr
	}
	t.Cleanup(func() { agentStandaloneCloseTemporary = previous })

	err := createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID)
	require.ErrorIs(t, err, wantErr)
	require.NoFileExists(t, filepath.Join(directory.Name(), "62011.owner"))
	entries, readErr := os.ReadDir(directory.Name())
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestAgentStandaloneMarkerCloseFailurePreventsPublication(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	wantErr := errors.New("injected close failure")
	previous := agentStandaloneCloseTemporary
	agentStandaloneCloseTemporary = func(file *os.File) error {
		require.NoError(t, file.Close())

		return wantErr
	}
	t.Cleanup(func() { agentStandaloneCloseTemporary = previous })

	err := replaceAgentStandaloneFile(
		directory, "62021.quarantine", []byte(`{"version":2}`), ownerUID, ownerGID,
		time.Now().Add(time.Second), nil, nil,
	)
	require.ErrorIs(t, err, wantErr)
	require.NoFileExists(t, filepath.Join(directory.Name(), "62021.quarantine"))
}

func TestAgentStandaloneCancellationBeforeActiveRenamePreventsPublication(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	canceled := make(chan struct{})
	previous := agentStandaloneCloseTemporary
	agentStandaloneCloseTemporary = func(file *os.File) error {
		err := file.Close()
		close(canceled)

		return err
	}
	t.Cleanup(func() { agentStandaloneCloseTemporary = previous })

	err := replaceAgentStandaloneFile(
		directory, "62022.quarantine", []byte(`{"version":2}`), ownerUID, ownerGID,
		time.Now().Add(time.Second), canceled, nil,
	)
	require.ErrorIs(t, err, errAgentStandaloneCanceled)
	require.NoFileExists(t, filepath.Join(directory.Name(), "62022.quarantine"))
}

func TestAgentStandaloneDomainCloseFailurePreventsPublication(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	wantErr := errors.New("injected domain close failure")
	previous := agentStandaloneCloseTemporary
	agentStandaloneCloseTemporary = func(file *os.File) error {
		require.NoError(t, file.Close())

		return wantErr
	}
	t.Cleanup(func() { agentStandaloneCloseTemporary = previous })

	err = replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record)
	require.ErrorIs(t, err, wantErr)
	require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
}

func TestAgentStandalonePermanentLocksAreNeverRecreatedAcrossSplitInodes(t *testing.T) {
	t.Run("domain", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		old := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(old.Fd()), unix.LOCK_SH|unix.LOCK_NB))
		require.NoError(t, unix.Unlinkat(int(directory.Fd()), "domain.lock", 0))
		require.NoError(t, os.WriteFile(filepath.Join(directory.Name(), "domain.json"), []byte("{}\n"), 0o600))

		_, err := acquireAgentStandaloneMissingDomainLock(
			directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorContains(t, err, "missing from a non-empty")
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.lock"))
	})

	t.Run("owners", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		old := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(old.Fd()), unix.LOCK_SH|unix.LOCK_NB))
		require.NoError(t, unix.Unlinkat(int(directory.Fd()), "owners.lock", 0))
		uidLock := createAgentStandaloneTestLock(t, directory, "62031.lock", ownerUID, ownerGID)
		require.NoError(t, uidLock.Close())

		_, err := acquireAgentStandaloneOwnersExclusive(
			directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorContains(t, err, "missing from non-pristine")
		require.NoFileExists(t, filepath.Join(directory.Name(), "owners.lock"))
	})

	t.Run("uid", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		old := createAgentStandaloneTestLock(t, directory, "62041.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(old.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		require.NoError(t, unix.Unlinkat(int(directory.Fd()), "62041.lock", 0))
		require.NoError(t, os.WriteFile(
			filepath.Join(directory.Name(), "62041.quarantine"),
			[]byte(`{"version":2,"uid":62041,"gid":62042,"ownerDigest":"held","state":"clean-ready"}`+"\n"),
			0o600,
		))

		err := validateAgentStandaloneUIDLockMayBeCreated(directory, 62041)
		require.ErrorContains(t, err, "permanent lock is missing")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62041.lock"))
	})
}

func TestAgentStandalonePermanentLockCreationIsDurableAndUmaskIndependent(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	restoreAgentStandalonePermanentLockSeams(t)
	previousUmask := unix.Umask(0o0777)
	t.Cleanup(func() { unix.Umask(previousUmask) })
	opens := make([]int, 0, 4)
	fchowns := 0
	fchmods := 0
	fileSyncs := 0
	directorySyncs := 0
	closes := 0
	namedChecks := 0
	originalOpenat := agentStandaloneLockOpenat
	originalFchown := agentStandaloneLockFchown
	originalFchmod := agentStandaloneLockFchmod
	originalFileSync := agentStandaloneLockFileSync
	originalDirectorySync := agentStandaloneLockDirectorySync
	originalClose := agentStandaloneLockClose
	originalFstatat := agentStandaloneLockFstatat
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		opens = append(opens, flags)

		return originalOpenat(dirfd, path, flags, mode)
	}
	agentStandaloneLockFchown = func(fd, uid, gid int) error {
		fchowns++

		return originalFchown(fd, uid, gid)
	}
	agentStandaloneLockFchmod = func(fd int, mode uint32) error {
		fchmods++

		return originalFchmod(fd, mode)
	}
	agentStandaloneLockFileSync = func(file *os.File) error {
		fileSyncs++

		return originalFileSync(file)
	}
	agentStandaloneLockDirectorySync = func(fd int) error {
		directorySyncs++

		return originalDirectorySync(fd)
	}
	agentStandaloneLockClose = func(file *os.File) error {
		closes++

		return originalClose(file)
	}
	agentStandaloneLockFstatat = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
		namedChecks++

		return originalFstatat(dirfd, path, stat, flags)
	}

	lock, err := openAgentStandaloneNamedLock(directory, "owners.lock", true, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	require.Len(t, opens, 2)
	require.NotZero(t, opens[0]&unix.O_CREAT)
	require.NotZero(t, opens[0]&unix.O_EXCL)
	require.Zero(t, opens[1]&unix.O_CREAT)
	require.Equal(t, 1, fchowns)
	require.Equal(t, 1, fchmods)
	require.Equal(t, 1, fileSyncs)
	require.Equal(t, 1, directorySyncs)
	require.Equal(t, 1, closes)
	require.Equal(t, 1, namedChecks)
	info, err := os.Stat(filepath.Join(directory.Name(), "owners.lock"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	lock, err = openAgentStandaloneNamedLock(directory, "owners.lock", true, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	require.Len(t, opens, 4)
	require.NotZero(t, opens[2]&unix.O_EXCL)
	require.Zero(t, opens[3]&unix.O_CREAT)
	require.Equal(t, 1, fchowns, "existing permanent lock was repaired")
	require.Equal(t, 1, fchmods, "existing permanent lock was repaired")
}

func TestAgentStandalonePermanentLockCreationFaultsFailClosed(t *testing.T) {
	for _, fault := range []string{
		"chown", "chmod", "file fsync", "directory fsync", "close", "reopen", "named inode",
	} {
		t.Run(fault, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
			restoreAgentStandalonePermanentLockSeams(t)
			wantErr := errors.New("injected " + fault + " failure")
			switch fault {
			case "chown":
				agentStandaloneLockFchown = func(int, int, int) error { return wantErr }
			case "chmod":
				agentStandaloneLockFchmod = func(int, uint32) error { return wantErr }
			case "file fsync":
				agentStandaloneLockFileSync = func(*os.File) error { return wantErr }
			case "directory fsync":
				agentStandaloneLockDirectorySync = func(int) error { return wantErr }
			case "close":
				agentStandaloneLockClose = func(file *os.File) error {
					require.NoError(t, file.Close())

					return wantErr
				}
			case "reopen":
				calls := 0
				original := agentStandaloneLockOpenat
				agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
					calls++
					if calls == 2 {
						return -1, wantErr
					}

					return original(dirfd, path, flags, mode)
				}
			case "named inode":
				agentStandaloneLockFstatat = func(int, string, *unix.Stat_t, int) error { return wantErr }
			}

			lock, err := openAgentStandaloneNamedLock(directory, "owners.lock", true, ownerUID, ownerGID)
			require.Nil(t, lock)
			require.ErrorIs(t, err, wantErr)
		})
	}
}

func TestAgentStandalonePermanentLockExistingWrongMetadataIsNeverRepaired(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	lock, err := openAgentStandaloneNamedLock(directory, "owners.lock", true, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	path := filepath.Join(directory.Name(), "owners.lock")
	require.NoError(t, os.Chmod(path, 0o644))
	restoreAgentStandalonePermanentLockSeams(t)
	fchowns := 0
	fchmods := 0
	agentStandaloneLockFchown = func(int, int, int) error {
		fchowns++

		return nil
	}
	agentStandaloneLockFchmod = func(int, uint32) error {
		fchmods++

		return nil
	}

	lock, err = openAgentStandaloneNamedLock(directory, "owners.lock", true, ownerUID, ownerGID)
	require.Nil(t, lock)
	require.ErrorContains(t, err, "mode")
	require.Zero(t, fchowns)
	require.Zero(t, fchmods)
	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}

func restoreAgentStandalonePermanentLockSeams(t *testing.T) {
	t.Helper()
	openat := agentStandaloneLockOpenat
	fchown := agentStandaloneLockFchown
	fchmod := agentStandaloneLockFchmod
	fileSync := agentStandaloneLockFileSync
	directorySync := agentStandaloneLockDirectorySync
	closeLock := agentStandaloneLockClose
	fstatat := agentStandaloneLockFstatat
	t.Cleanup(func() {
		agentStandaloneLockOpenat = openat
		agentStandaloneLockFchown = fchown
		agentStandaloneLockFchmod = fchmod
		agentStandaloneLockFileSync = fileSync
		agentStandaloneLockDirectorySync = directorySync
		agentStandaloneLockClose = closeLock
		agentStandaloneLockFstatat = fstatat
	})
}

func TestAgentStandaloneOwnersLockUsesSeparateOpenFlockExclusion(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	held := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_SH|unix.LOCK_NB))

	contender, acquired, err := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.False(t, acquired)
	require.Nil(t, contender)
}

func TestAgentStandaloneGloballyDrainsOwnerTemporaries(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	for _, uid := range []uint32{62045, 62046} {
		lock := createAgentStandaloneTestLock(
			t, directory, strconv.FormatUint(uint64(uid), 10)+".lock", ownerUID, ownerGID,
		)
		require.NoError(t, lock.Close())
		temporary := strconv.FormatUint(uint64(uid), 10) + ".owner.next-0123456789abcdef01234567"
		require.NoError(t, os.WriteFile(filepath.Join(directory.Name(), temporary), []byte("partial"), 0o600))
	}

	cleaned, busy, err := drainAgentStandaloneOwnerTemporaries(
		directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.True(t, cleaned)
	require.False(t, busy)
	require.NoFileExists(t, filepath.Join(directory.Name(), "62045.owner.next-0123456789abcdef01234567"))
	require.NoFileExists(t, filepath.Join(directory.Name(), "62046.owner.next-0123456789abcdef01234567"))
}

func TestAgentStandaloneOwnerTemporaryBusyRetriesUntilCanceled(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	held := createAgentStandaloneTestLock(t, directory, "62047.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	require.NoError(t, os.WriteFile(
		filepath.Join(directory.Name(), "62047.owner.next-0123456789abcdef01234567"), []byte("partial"), 0o600,
	))
	canceled := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(canceled)
	}()
	want := agentStandaloneOwner{
		Version: 1, UID: 62048, GID: 62049, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "owner-temp-cancel",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/owner-temp-cancel", Dev: 1, Ino: 2},
	}

	_, err := acquireAgentStandaloneOwnerIdentity(
		directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), canceled, nil,
	)
	require.ErrorIs(t, err, errAgentStandaloneCanceled)
	require.FileExists(t, filepath.Join(directory.Name(), "62047.owner.next-0123456789abcdef01234567"))
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, acquired, "busy UID retry must release owners.lock")
	require.NoError(t, contender.Close())
}

func TestAgentStandaloneDomainRebindOwnerTemporaryBusyCancelsWithoutUnlink(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	domain := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
	require.NoError(t, domain.Close())
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	record.PIDNamespace.Ino++
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	held := createAgentStandaloneTestLock(t, directory, "62049.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	temporary := filepath.Join(directory.Name(), "62049.owner.next-0123456789abcdef01234567")
	require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
	canceled := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(canceled)
	}()
	want := agentStandaloneOwner{
		Version: 1, UID: 62050, GID: 62051, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "domain-owner-temp",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/domain-owner-temp", Dev: 1, Ino: 2},
	}

	_, err = acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), canceled, nil,
	)
	require.ErrorIs(t, err, errAgentStandaloneCanceled)
	require.FileExists(t, temporary)
}

func TestAgentStandaloneMatchingDomainToleratesLiveProbe(t *testing.T) {
	directory, ownerUID, ownerGID, want := createAgentStandaloneMatchingDomainFixture(t)
	name := ".authority-probe-0123456789abcdef01234567"
	probe, err := openAgentStandaloneNamedLock(directory, name, true, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, unix.Flock(int(probe.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	defer probe.Close()

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.NoError(t, authority.Close())
	require.FileExists(t, filepath.Join(directory.Name(), name))
}

func TestAgentStandaloneMatchingDomainCleansStaleProbeUnderSharedLease(t *testing.T) {
	directory, ownerUID, ownerGID, want := createAgentStandaloneMatchingDomainFixture(t)
	name := ".authority-probe-0123456789abcdef01234567.renamed"
	probe, err := openAgentStandaloneNamedLock(directory, name, true, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, probe.Close())

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.NoError(t, authority.Close())
	require.NoFileExists(t, filepath.Join(directory.Name(), name))
}

func TestAgentStandaloneMatchingDomainCleansDomainTemporaryUnderExclusiveLeaseThenReturnsShared(t *testing.T) {
	directory, ownerUID, ownerGID, want := createAgentStandaloneMatchingDomainFixture(t)
	name := "domain.json.next-0123456789abcdef01234567"
	require.NoError(t, os.WriteFile(filepath.Join(directory.Name(), name), []byte("partial"), 0o600))

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.NoError(t, err)
	defer authority.Close()
	require.NoFileExists(t, filepath.Join(directory.Name(), name))
	contender, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	defer contender.Close()
	require.NoError(t, unix.Flock(int(contender.Fd()), unix.LOCK_SH|unix.LOCK_NB))
}

func TestAgentStandaloneMatchingDomainRejectsMalformedTemporaries(t *testing.T) {
	for _, name := range []string{".authority-probe-bad", "domain.json.next-bad"} {
		t.Run(name, func(t *testing.T) {
			directory, ownerUID, ownerGID, want := createAgentStandaloneMatchingDomainFixture(t)
			require.NoError(t, os.WriteFile(filepath.Join(directory.Name(), name), []byte("partial"), 0o600))

			authority, err := acquireAgentStandaloneDomain(
				directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
			)
			require.Nil(t, authority)
			require.ErrorContains(t, err, "invalid name")
			require.FileExists(t, filepath.Join(directory.Name(), name))
		})
	}
}

func TestAgentStandalonePeerWonMissingDomainRaceReturnsSharedLease(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	holder := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(holder.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	acquired := make(chan *os.File, 1)
	failed := make(chan error, 1)
	go func() {
		lease, err := acquireAgentStandaloneMissingDomainLock(
			directory, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		if err != nil {
			failed <- err

			return
		}
		acquired <- lease
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, holder.Close())
	var lease *os.File
	select {
	case lease = <-acquired:
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("peer-won missing-domain contender did not acquire")
	}
	defer lease.Close()
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	require.NoError(t, normalizeAgentStandaloneSharedDomainLease(
		directory, lease, ownerUID, ownerGID, record,
	))
	third, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	defer third.Close()
	require.NoError(t, unix.Flock(int(third.Fd()), unix.LOCK_SH|unix.LOCK_NB))
}

func TestAgentStandaloneRebindProbeFailureLeavesOldDomainRecordIntact(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	domain := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
	require.NoError(t, domain.Close())
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	record.BootID = "00000000-0000-0000-0000-000000000001"
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	recordPath := filepath.Join(directory.Name(), "domain.json")
	before, err := os.ReadFile(recordPath)
	require.NoError(t, err)
	wantErr := errors.New("injected rebind filesystem probe failure")
	previousProbe := agentStandaloneFilesystemProbe
	agentStandaloneFilesystemProbe = func(*os.File, bool) error { return wantErr }
	t.Cleanup(func() { agentStandaloneFilesystemProbe = previousProbe })
	want := agentStandaloneOwner{
		Version: 1, UID: 62095, GID: 62096, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "probe-failure",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/probe-failure", Dev: 1, Ino: 2},
	}

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, authority)
	require.ErrorIs(t, err, wantErr)
	after, err := os.ReadFile(recordPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestAgentStandaloneRebindRejectsOverlayFilesystemBeforeDomainMutation(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	domain := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
	require.NoError(t, domain.Close())
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	record.BootID = "00000000-0000-0000-0000-000000000001"
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	recordPath := filepath.Join(directory.Name(), "domain.json")
	before, err := os.ReadFile(recordPath)
	require.NoError(t, err)
	previousProbe := agentStandaloneFilesystemProbe
	previousFstatfs := agentStandaloneProbeFstatfs
	agentStandaloneFilesystemProbe = func(dir *os.File, _ bool) error {
		return probeAgentStandaloneFilesystem(dir, false)
	}
	agentStandaloneProbeFstatfs = func(fd int, filesystem *unix.Statfs_t) error {
		if previousErr := previousFstatfs(fd, filesystem); previousErr != nil {
			return previousErr
		}
		filesystem.Type = 0x794c7630

		return nil
	}
	t.Cleanup(func() {
		agentStandaloneFilesystemProbe = previousProbe
		agentStandaloneProbeFstatfs = previousFstatfs
	})
	want := agentStandaloneOwner{
		Version: 1, UID: 62097, GID: 62098, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "overlay-rebind",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/overlay-rebind", Dev: 1, Ino: 2},
	}

	authority, err := acquireAgentStandaloneDomain(
		directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, authority)
	require.ErrorContains(t, err, "not in the local durable allowlist")
	after, err := os.ReadFile(recordPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestAgentStandaloneFilesystemProbeChecksCLOEXECAndRetainsLeaseThroughCleanup(t *testing.T) {
	t.Run("cloexec", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		previousFcntl := agentStandaloneProbeFcntl
		calls := 0
		agentStandaloneProbeFcntl = func(fd uintptr, cmd, arg int) (int, error) {
			calls++
			if calls == 3 && cmd == unix.F_GETFD {
				return 0, nil
			}

			return previousFcntl(fd, cmd, arg)
		}
		t.Cleanup(func() { agentStandaloneProbeFcntl = previousFcntl })

		err := probeAgentStandaloneFilesystem(directory, true)
		require.ErrorContains(t, err, "not close-on-exec")
	})

	t.Run("lease through unlink and sync", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		previousUnlink := agentStandaloneProbeUnlinkat
		previousSync := agentStandaloneProbeDirectorySync
		checkedUnlink := false
		checkedSync := false
		agentStandaloneProbeUnlinkat = func(dirfd int, name string, flags int) error {
			if !checkedUnlink {
				contender, err := unix.Openat(
					dirfd, name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
				)
				if !errors.Is(err, unix.ENOENT) {
					require.NoError(t, err)
					require.ErrorIs(t, unix.Flock(contender, unix.LOCK_EX|unix.LOCK_NB), unix.EWOULDBLOCK)
					require.NoError(t, unix.Close(contender))
					checkedUnlink = true
				}
			}

			return previousUnlink(dirfd, name, flags)
		}
		agentStandaloneProbeDirectorySync = func(fd int) error {
			checkedSync = true

			return previousSync(fd)
		}
		t.Cleanup(func() {
			agentStandaloneProbeUnlinkat = previousUnlink
			agentStandaloneProbeDirectorySync = previousSync
		})

		require.NoError(t, probeAgentStandaloneFilesystem(directory, true))
		require.True(t, checkedUnlink)
		require.True(t, checkedSync)
	})
}

func TestAgentStandaloneAuthorityPathRejectsDeletedDescriptor(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	require.NoError(t, os.Remove(directory.Name()))

	_, err := agentStandaloneAuthorityPath(directory)
	require.ErrorContains(t, err, "deleted directory")
}

func TestAgentStandaloneOwnerlessMarkerRequiresLegacyAffinityLock(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	uidLock := createAgentStandaloneTestLock(t, directory, "62051.lock", ownerUID, ownerGID)
	require.NoError(t, uidLock.Close())
	marker := []byte(`{"version":2,"uid":62051,"gid":62052,"ownerDigest":"hosted-session","state":"clean-ready"}` + "\n")
	require.NoError(t, os.WriteFile(filepath.Join(directory.Name(), "62051.quarantine"), marker, 0o600))
	deadline := time.Now().Add(time.Second)

	err := auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, true, deadline, nil, nil,
	)
	require.ErrorContains(t, err, "without permanent affinity lock")
	affinity := createAgentStandaloneTestLock(
		t, directory, agentStandaloneAffinityLockName("hosted-session"), ownerUID, ownerGID,
	)
	require.NoError(t, affinity.Close())
	require.NoError(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, true, deadline, nil, nil,
	))
}

func TestAgentStandaloneSameDomainAllowsUnrelatedActiveAndLiveMarkerTemporary(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	uidLock := createAgentStandaloneTestLock(t, directory, "62055.lock", ownerUID, ownerGID)
	require.NoError(t, uidLock.Close())
	affinity := createAgentStandaloneTestLock(
		t, directory, agentStandaloneAffinityLockName("unrelated-live"), ownerUID, ownerGID,
	)
	require.NoError(t, affinity.Close())
	marker := []byte(`{"version":2,"uid":62055,"gid":62056,"ownerDigest":"unrelated-live","state":"active","leaseId":"0123456789abcdef0123456789abcdef","paths":[]}` + "\n")
	require.NoError(t, os.WriteFile(filepath.Join(directory.Name(), "62055.quarantine"), marker, 0o600))
	temporary := filepath.Join(directory.Name(), "62055.quarantine.next-0123456789abcdef01234567")
	require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
	held := createAgentStandaloneTestLock(t, directory, "62055.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	deadline := time.Now().Add(time.Second)

	require.NoError(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, true, deadline, nil, nil,
	))
	require.FileExists(t, temporary)
	err := auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, false, deadline, nil, nil,
	)
	require.ErrorContains(t, err, "domain-exclusive cleanup")
	require.NoError(t, held.Close())
	require.NoError(t, os.Remove(temporary))
	err = auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, false, deadline, nil, nil,
	)
	require.ErrorContains(t, err, "authoritative host recovery is required")
}

func TestAgentStandaloneTargetMarkerTemporaryCleanupPreservesFinalDisposition(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	uidLock := createAgentStandaloneTestLock(t, directory, "62057.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(uidLock.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	markerPath := filepath.Join(directory.Name(), "62057.quarantine")
	marker := []byte(`{"version":2,"uid":62057,"gid":62058,"ownerDigest":"target-final","state":"clean-ready"}` + "\n")
	require.NoError(t, os.WriteFile(markerPath, marker, 0o600))
	temporary := filepath.Join(directory.Name(), "62057.quarantine.next-0123456789abcdef01234567")
	require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
	before, err := os.ReadFile(markerPath)
	require.NoError(t, err)

	require.NoError(t, cleanupAgentStandaloneTargetMarkerTemporaries(
		directory, 62057, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	))
	after, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoFileExists(t, temporary)
}

func TestAgentStandaloneDomainAuditCleansBoundMarkerTemporaryWithoutDispositionMutation(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	uidLock := createAgentStandaloneTestLock(t, directory, "62059.lock", ownerUID, ownerGID)
	require.NoError(t, uidLock.Close())
	owner := agentStandaloneOwner{
		Version: 1, UID: 62059, GID: 62060, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "bound-marker-temp",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/bound-marker-temp", Dev: 31, Ino: 32},
	}
	require.NoError(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID))
	key := agentStandaloneSessionKey(owner)
	require.NoError(t, publishAgentStandaloneActive(
		directory, owner.UID, owner.GID, ownerUID, ownerGID, key,
		time.Now().Add(time.Second), nil, nil,
	))
	ownerPath := filepath.Join(directory.Name(), "62059.owner")
	markerPath := filepath.Join(directory.Name(), "62059.quarantine")
	ownerBefore, err := os.ReadFile(ownerPath)
	require.NoError(t, err)
	markerBefore, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	temporary := filepath.Join(directory.Name(), "62059.quarantine.next-0123456789abcdef01234567")
	require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))

	require.NoError(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, true, false, time.Now().Add(time.Second), nil, nil,
	))
	ownerAfter, err := os.ReadFile(ownerPath)
	require.NoError(t, err)
	markerAfter, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	require.Equal(t, ownerBefore, ownerAfter)
	require.Equal(t, markerBefore, markerAfter)
	require.NoFileExists(t, temporary)
}

func TestAgentStandaloneClaimRaceJoinsOwnerCleanupAndToleratesUnrelatedMarkerTemporary(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneOwner{
		Version: 1, UID: 62065, GID: 62066, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "claim-race",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/claim-race", Dev: 41, Ino: 42},
	}
	ownerTemp := filepath.Join(directory.Name(), "62067.owner.next-0123456789abcdef01234567")
	require.NoError(t, os.WriteFile(ownerTemp, []byte("partial"), 0o600))
	err := validateAgentStandaloneOwnerUniqueness(
		directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.ErrorIs(t, err, errAgentStandaloneOwnerTemporary)
	require.NoError(t, os.Remove(ownerTemp))
	unrelated := filepath.Join(directory.Name(), "62067.quarantine.next-0123456789abcdef01234567")
	require.NoError(t, os.WriteFile(unrelated, []byte("partial"), 0o600))
	require.NoError(t, validateAgentStandaloneOwnerUniqueness(
		directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	))
	target := filepath.Join(directory.Name(), "62065.quarantine.next-0123456789abcdef01234567")
	require.NoError(t, os.WriteFile(target, []byte("partial"), 0o600))
	err = validateAgentStandaloneOwnerUniqueness(
		directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.ErrorContains(t, err, "appeared after held-lock cleanup")
}

func TestAgentStandaloneSameBootRebindRejectsSecondOwner(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneOwner{
		Version: 1, UID: 62061, GID: 62062, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "same-boot-rebind",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/same-boot", Dev: 11, Ino: 12},
	}
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	uidLock := createAgentStandaloneTestLock(t, directory, "62061.lock", ownerUID, ownerGID)
	require.NoError(t, uidLock.Close())
	require.NoError(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID))
	key := agentStandaloneSessionKey(owner)
	require.NoError(t, publishAgentStandaloneActive(
		directory, owner.UID, owner.GID, ownerUID, ownerGID, key,
		time.Now().Add(time.Second), nil, nil,
	))
	other := agentStandaloneOwner{
		Version: 1, UID: 62063, GID: 62064, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "unrelated-bound-owner",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/unrelated-bound", Dev: 13, Ino: 14},
	}
	otherLock := createAgentStandaloneTestLock(t, directory, "62063.lock", ownerUID, ownerGID)
	require.NoError(t, otherLock.Close())
	require.NoError(t, createAgentStandaloneOwner(directory, other, ownerUID, ownerGID))
	identity, err := validateAgentStandaloneSameBootRebind(
		directory, owner, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorContains(t, err, "blocked by standalone owner uid 62063")
}

func TestAgentStandaloneSameBootRebindRejectsMatchingTask(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneOwner{
		Version: 1, UID: 62071, GID: 62072, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "same-boot-live-task",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/same-boot-live-task", Dev: 21, Ino: 22},
	}
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	uidLock := createAgentStandaloneTestLock(t, directory, "62071.lock", ownerUID, ownerGID)
	require.NoError(t, uidLock.Close())
	require.NoError(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID))
	key := agentStandaloneSessionKey(owner)
	require.NoError(t, publishAgentStandaloneActive(
		directory, owner.UID, owner.GID, ownerUID, ownerGID, key,
		time.Now().Add(time.Second), nil, nil,
	))
	previousScan := agentStandaloneVacancyScan
	agentStandaloneVacancyScan = func(
		uid uint32, gid uint32, _ time.Time, _ <-chan struct{}, _ <-chan os.Signal,
	) error {
		require.Equal(t, owner.UID, uid)
		require.Equal(t, owner.GID, gid)

		return errors.New("matching task remains")
	}
	t.Cleanup(func() { agentStandaloneVacancyScan = previousScan })

	identity, err := validateAgentStandaloneSameBootRebind(
		directory, owner, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorContains(t, err, "matching task remains")
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(
		directory, "62071.lock", false, ownerUID, ownerGID,
	)
	require.NoError(t, lockErr)
	require.True(t, acquired)
	require.NoError(t, contender.Close())
}

func TestAgentStandaloneSameBootRebindRetainsUIDLockThroughDomainPublication(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	domain := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
	require.NoError(t, domain.Close())
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	record.PIDNamespace.Ino++
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	owner := agentStandaloneOwner{
		Version: 1, UID: 62073, GID: 62074, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "same-boot-publish",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/same-boot-publish", Dev: 31, Ino: 32},
	}
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, owners.Close())
	uidLock := createAgentStandaloneTestLock(t, directory, "62073.lock", ownerUID, ownerGID)
	require.NoError(t, uidLock.Close())
	require.NoError(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID))
	key := agentStandaloneSessionKey(owner)
	require.NoError(t, publishAgentStandaloneActive(
		directory, owner.UID, owner.GID, ownerUID, ownerGID, key,
		time.Now().Add(time.Second), nil, nil,
	))
	previousScan := agentStandaloneVacancyScan
	scans := 0
	agentStandaloneVacancyScan = func(
		uid uint32, gid uint32, _ time.Time, _ <-chan struct{}, _ <-chan os.Signal,
	) error {
		require.Equal(t, owner.UID, uid)
		require.Equal(t, owner.GID, gid)
		scans++

		return nil
	}
	t.Cleanup(func() { agentStandaloneVacancyScan = previousScan })
	previousReplace := agentStandaloneReplaceDomain
	agentStandaloneReplaceDomain = func(
		dir *os.File, uid uint32, gid uint32, current agentAuthorityDomainRecord,
	) error {
		contender, acquired, lockErr := tryAgentStandaloneNamedLock(
			dir, "62073.lock", false, ownerUID, ownerGID,
		)
		require.NoError(t, lockErr)
		require.False(t, acquired, "same-boot UID lock was released before domain publication")
		require.Nil(t, contender)

		return previousReplace(dir, uid, gid, current)
	}
	t.Cleanup(func() { agentStandaloneReplaceDomain = previousReplace })

	authority, err := acquireAgentStandaloneDomain(
		directory, owner, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.NoError(t, authority.Close())
	require.Equal(t, 2, scans)
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(
		directory, "62073.lock", false, ownerUID, ownerGID,
	)
	require.NoError(t, lockErr)
	require.True(t, acquired, "same-boot UID lock was not released after domain publication")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneStateRootRejectsUnprotectedAncestry builds the unprotected
// ancestor instead of inheriting one from the temp root. A runner whose temp
// root is already mode 0755 root-owned storage satisfies the predicate, so the
// bind succeeds and the case proves nothing about the refusal it names.
func TestAgentStandaloneStateRootRejectsUnprotectedAncestry(t *testing.T) {
	unprotected := filepath.Join(t.TempDir(), "unprotected")
	require.NoError(t, os.Mkdir(unprotected, 0o700))
	require.NoError(t, os.Chmod(unprotected, 0o777))
	stateRoot := filepath.Join(unprotected, "state")
	require.NoError(t, os.Mkdir(stateRoot, 0o700))
	_, err := bindAgentStandaloneStateRoot(stateRoot, uint32(os.Geteuid()), uint32(os.Getegid()))
	require.ErrorContains(t, err, "not protected root-owned storage")
}

func TestAgentStandaloneStateRootRejectsRootAndAuthoritySubtree(t *testing.T) {
	require.False(t, validAgentStandaloneStateRootPath("/"))
	require.False(t, validStandaloneStateRootPath("/var/lib/acp-go/agent-identities"))
	require.False(t, validStandaloneStateRootPath("/var/lib/acp-go/agent-identities/provider"))
}

func TestKnownAgentStandaloneProvidersIncludesEverySibling(t *testing.T) {
	for _, provider := range []string{
		"github.com/savid/acp-go-amp",
		"github.com/savid/acp-go-claude",
		"github.com/savid/acp-go-codex",
		"github.com/savid/acp-go-hermes",
		"github.com/savid/acp-go-opencode",
		"github.com/savid/acp-go-pi",
	} {
		require.True(t, knownAgentStandaloneProvider(provider), provider)
	}
	require.False(t, knownAgentStandaloneProvider("github.com/savid/acp-go-unknown"))
}

func TestAgentStandaloneStateRootRequiresClaimedOwnerModeAndSupportsProtectedRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("protected state-root metadata test requires root")
	}
	base, err := os.MkdirTemp("/var/lib", "acp-go-hermes-state-root-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(base)) })
	require.NoError(t, os.Chmod(base, 0o700))
	stateRoot := filepath.Join(base, "state")
	require.NoError(t, os.Mkdir(stateRoot, 0o755))
	const uid, gid = uint32(62071), uint32(62072)

	_, err = bindAgentStandaloneStateRoot(stateRoot, uid, gid)
	require.ErrorContains(t, err, "claimed UID:GID-owned mode-0700")
	require.NoError(t, os.Chown(stateRoot, int(uid), int(gid)))
	_, err = bindAgentStandaloneStateRoot(stateRoot, uid, gid)
	require.ErrorContains(t, err, "claimed UID:GID-owned mode-0700")
	require.NoError(t, os.Chmod(stateRoot, 0o700))
	bound, err := bindAgentStandaloneStateRoot(stateRoot, uid, gid)
	require.NoError(t, err)
	require.Equal(t, stateRoot, bound.Path)
	require.NotZero(t, bound.Dev)
	require.NotZero(t, bound.Ino)
}

func TestAgentStandaloneFinalStateRootRevalidationAfterLastScanPreventsActive(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("protected state-root revalidation test requires root")
	}
	const uid, gid = uint32(62073), uint32(62074)
	stateRoot := createAgentStandaloneProtectedStateRoot(t, uid, gid)
	bound, err := bindAgentStandaloneStateRoot(stateRoot, uid, gid)
	require.NoError(t, err)
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(owners.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	uidLock := createAgentStandaloneTestLock(t, directory, "62073.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(uidLock.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	want := agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "state-root-swap", StateRoot: bound,
	}
	previous := agentStandaloneVacancyScan
	scans := 0
	agentStandaloneVacancyScan = func(
		_, _ uint32, _ time.Time, _ <-chan struct{}, _ <-chan os.Signal,
	) error {
		scans++
		if scans == 2 {
			require.NoError(t, os.Rename(stateRoot, stateRoot+".replaced"))
			require.NoError(t, os.Mkdir(stateRoot, 0o700))
			require.NoError(t, os.Chown(stateRoot, int(uid), int(gid)))
		}

		return nil
	}
	t.Cleanup(func() { agentStandaloneVacancyScan = previous })

	err = completeAgentStandaloneOwnerClaim(
		directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
	)
	require.ErrorContains(t, err, "state root changed")
	require.Equal(t, 2, scans)
	require.FileExists(t, filepath.Join(directory.Name(), "62073.owner"))
	require.NoFileExists(t, filepath.Join(directory.Name(), "62073.quarantine"))
}

func TestAgentStandalonePIDNamespaceAnchorUsesCurrentProcess(t *testing.T) {
	previousReadlink := agentStandaloneReadlink
	previousProcessID := agentStandaloneProcessID
	previousNamespaceIdentity := agentStandaloneNamespaceIdentity
	t.Cleanup(func() {
		agentStandaloneReadlink = previousReadlink
		agentStandaloneProcessID = previousProcessID
		agentStandaloneNamespaceIdentity = previousNamespaceIdentity
	})

	wantNamespace := agentAuthorityDomainInode{Dev: 11, Ino: 12}
	agentStandaloneProcessID = func() int { return 4321 }
	agentStandaloneReadlink = func(path string) (string, error) {
		require.Equal(t, "/proc/self", path)

		return "4321", nil
	}
	agentStandaloneNamespaceIdentity = func(path string) (agentAuthorityDomainInode, error) {
		require.Equal(t, "/proc/4321/ns/pid", path)

		return wantNamespace, nil
	}
	require.NoError(t, validateAgentStandalonePIDNamespaceAnchor(wantNamespace))

	agentStandaloneReadlink = func(string) (string, error) { return "1", nil }
	require.ErrorContains(t, validateAgentStandalonePIDNamespaceAnchor(wantNamespace), "want \"4321\"")

	agentStandaloneReadlink = func(string) (string, error) { return "4321", nil }
	agentStandaloneNamespaceIdentity = func(string) (agentAuthorityDomainInode, error) {
		return agentAuthorityDomainInode{Dev: 11, Ino: 13}, nil
	}
	require.ErrorContains(t, validateAgentStandalonePIDNamespaceAnchor(wantNamespace), "requires self and procfs")

	wantErr := errors.New("denied")
	agentStandaloneNamespaceIdentity = func(string) (agentAuthorityDomainInode, error) {
		return agentAuthorityDomainInode{}, wantErr
	}
	err := validateAgentStandalonePIDNamespaceAnchor(wantNamespace)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "inspect procfs self PID namespace anchor")
}

func TestAgentStandaloneEndToEndRetainsActiveAndNativeChildHasNoAuthorityFD(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone end-to-end test requires root")
	}
	const uid, gid = uint32(62075), uint32(62076)
	stateRoot := createAgentStandaloneProtectedStateRoot(t, uid, gid)
	testRoot := t.TempDir()
	require.NoError(t, os.Chmod(testRoot, 0o700))

	identity, err := acquireAgentStandaloneIdentity(
		uid, gid, "end-to-end", stateRoot, true, testRoot,
		make(chan struct{}), make(chan os.Signal),
	)
	require.NoError(t, err)
	authorityPath := filepath.Join(testRoot, "acp-go", "agent-identities")
	directory, err := bootstrapAgentIdentityLockDirectory(testRoot, uint32(os.Geteuid()), uint32(os.Getegid()))
	require.NoError(t, err)
	defer directory.Close()
	marker, err := loadAgentStandaloneMarker(directory, uid, uint32(os.Geteuid()), uint32(os.Getegid()))
	require.NoError(t, err)
	require.Equal(t, "active", marker.State)
	require.Empty(t, marker.Paths)

	command := exec.Command("/bin/sh", "-c", `for fd in /proc/self/fd/*; do target=$(readlink "$fd" 2>/dev/null || true); case "$target" in "$AUTHORITY"*) exit 41;; esac; done`)
	command.Env = []string{"AUTHORITY=" + authorityPath}
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.NoError(t, identity.Close())
	marker, err = loadAgentStandaloneMarker(directory, uid, uint32(os.Geteuid()), uint32(os.Getegid()))
	require.NoError(t, err)
	require.Equal(t, "active", marker.State, "clean exit must retain standalone ACTIVE")
}

func TestAgentStandaloneVacancyReenumeratesDisappearingTaskAndFindsReplacement(t *testing.T) {
	processes := agentStandaloneTestDirEntries(t, "101")
	oldTasks := agentStandaloneTestDirEntries(t, "101", "102")
	newTasks := agentStandaloneTestDirEntries(t, "101", "103")
	previousReadDir, previousReadFile := agentStandaloneReadDir, agentStandaloneReadFile
	taskReads := 0
	agentStandaloneReadDir = func(path string) ([]os.DirEntry, error) {
		switch path {
		case "/proc":
			return processes, nil
		case "/proc/101/task":
			taskReads++
			if taskReads == 1 {
				return oldTasks, nil
			}

			return newTasks, nil
		default:
			return nil, os.ErrNotExist
		}
	}
	agentStandaloneReadFile = func(path string) ([]byte, error) {
		switch path {
		case "/proc/101/task/101/status":
			return agentStandaloneTestStatus(1, 1, nil), nil
		case "/proc/101/task/102/status":
			return nil, os.ErrNotExist
		case "/proc/101/task/103/status":
			return agentStandaloneTestStatus(62081, 62082, nil), nil
		default:
			return nil, os.ErrNotExist
		}
	}
	t.Cleanup(func() {
		agentStandaloneReadDir, agentStandaloneReadFile = previousReadDir, previousReadFile
	})

	err := proveAgentStandaloneIdentityVacant(62081, 62082, time.Now().Add(time.Second), nil, nil)
	require.ErrorContains(t, err, "still used by task 101/103")
	require.GreaterOrEqual(t, taskReads, 2)
}

func TestAgentStandaloneVacancyFailsClosedOnRepeatedTaskChurn(t *testing.T) {
	processes := agentStandaloneTestDirEntries(t, "201")
	first := agentStandaloneTestDirEntries(t, "201", "202")
	second := agentStandaloneTestDirEntries(t, "201", "203")
	previousReadDir, previousReadFile := agentStandaloneReadDir, agentStandaloneReadFile
	taskReads := 0
	agentStandaloneReadDir = func(path string) ([]os.DirEntry, error) {
		if path == "/proc" {
			return processes, nil
		}
		if path != "/proc/201/task" {
			return nil, os.ErrNotExist
		}
		taskReads++
		if taskReads%2 == 1 {
			return first, nil
		}

		return second, nil
	}
	agentStandaloneReadFile = func(string) ([]byte, error) {
		return agentStandaloneTestStatus(1, 1, nil), nil
	}
	t.Cleanup(func() {
		agentStandaloneReadDir, agentStandaloneReadFile = previousReadDir, previousReadFile
	})

	err := proveAgentStandaloneIdentityVacant(62083, 62084, time.Now().Add(time.Second), nil, nil)
	require.ErrorContains(t, err, "did not stabilize within 64 attempts")
	require.Equal(t, 128, taskReads)
}

func TestAgentStandaloneVacancyAllowsProcessExitDuringTaskEnumeration(t *testing.T) {
	processes := agentStandaloneTestDirEntries(t, "301")
	previousReadDir := agentStandaloneReadDir
	agentStandaloneReadDir = func(path string) ([]os.DirEntry, error) {
		if path == "/proc" {
			return processes, nil
		}

		return nil, os.ErrNotExist
	}
	t.Cleanup(func() { agentStandaloneReadDir = previousReadDir })
	require.NoError(t, proveAgentStandaloneIdentityVacant(
		62085, 62086, time.Now().Add(time.Second), nil, nil,
	))
}

// TestAgentStandaloneVacancyReenumeratesTaskExitingWithESRCH pins the kernel's
// other way of saying "that task is gone". Reading a task's status after the
// thread exits yields ENOENT on some kernels and ESRCH on others; only ENOENT
// used to re-enumerate, so a thread exiting mid-scan refused the claim outright
// with "no such process" — a vacancy proof failed by the one observation that
// says nothing about vacancy.
func TestAgentStandaloneVacancyReenumeratesTaskExitingWithESRCH(t *testing.T) {
	processes := agentStandaloneTestDirEntries(t, "401")
	churning := agentStandaloneTestDirEntries(t, "401", "402")
	settled := agentStandaloneTestDirEntries(t, "401")
	previousReadDir, previousReadFile := agentStandaloneReadDir, agentStandaloneReadFile
	taskReads := 0
	agentStandaloneReadDir = func(path string) ([]os.DirEntry, error) {
		switch path {
		case "/proc":
			return processes, nil
		case "/proc/401/task":
			taskReads++
			if taskReads == 1 {
				return churning, nil
			}

			return settled, nil
		default:
			return nil, os.ErrNotExist
		}
	}
	agentStandaloneReadFile = func(path string) ([]byte, error) {
		if path == "/proc/401/task/402/status" {
			return nil, unix.ESRCH
		}

		return agentStandaloneTestStatus(1, 1, nil), nil
	}
	t.Cleanup(func() {
		agentStandaloneReadDir, agentStandaloneReadFile = previousReadDir, previousReadFile
	})

	require.NoError(t, proveAgentStandaloneIdentityVacant(
		62099, 62100, time.Now().Add(time.Second), nil, nil,
	))
	require.GreaterOrEqual(t, taskReads, 2)
}

// TestAgentStandaloneVacancyAllowsProcessExitWithESRCH is the whole-process
// half of the same class: /proc/<pid>/task disappears mid-scan because the
// process exited, and the kernel reports that as ENOENT down some paths and
// ESRCH down others. Both enumerations have to accept it — the first, which
// opens the task set, and the second, which rechecks that it stayed still — or
// a process exiting during the walk refuses a claim it cannot possibly be
// occupying.
func TestAgentStandaloneVacancyAllowsProcessExitWithESRCH(t *testing.T) {
	for _, enumeration := range []struct {
		name  string
		reads int
	}{
		{name: "opening", reads: 1},
		{name: "recheck", reads: 2},
	} {
		t.Run(enumeration.name, func(t *testing.T) {
			processes := agentStandaloneTestDirEntries(t, "501")
			tasks := agentStandaloneTestDirEntries(t, "501")
			previousReadDir, previousReadFile := agentStandaloneReadDir, agentStandaloneReadFile
			taskReads := 0
			agentStandaloneReadDir = func(path string) ([]os.DirEntry, error) {
				switch path {
				case "/proc":
					return processes, nil
				case "/proc/501/task":
					taskReads++
					if taskReads == enumeration.reads {
						return nil, unix.ESRCH
					}

					return tasks, nil
				default:
					return nil, os.ErrNotExist
				}
			}
			agentStandaloneReadFile = func(string) ([]byte, error) {
				return agentStandaloneTestStatus(1, 1, nil), nil
			}
			t.Cleanup(func() {
				agentStandaloneReadDir, agentStandaloneReadFile = previousReadDir, previousReadFile
			})

			require.NoError(t, proveAgentStandaloneIdentityVacant(
				62101, 62102, time.Now().Add(time.Second), nil, nil,
			))
			require.Equal(t, enumeration.reads, taskReads)
		})
	}
}

func TestAgentStandaloneVacancyRejectsMissingGroupsFieldThroughProductionSeam(t *testing.T) {
	processes := agentStandaloneTestDirEntries(t, "101")
	tasks := agentStandaloneTestDirEntries(t, "101")
	previousReadDir := agentStandaloneReadDir
	previousReadFile := agentStandaloneReadFile
	agentStandaloneReadDir = func(path string) ([]os.DirEntry, error) {
		switch path {
		case "/proc":
			return processes, nil
		case "/proc/101/task":
			return tasks, nil
		default:
			return nil, os.ErrNotExist
		}
	}
	agentStandaloneReadFile = func(path string) ([]byte, error) {
		require.Equal(t, "/proc/101/task/101/status", path)

		return []byte("Uid:\t1 1 1 1\nGid:\t2 2 2 2\n"), nil
	}
	t.Cleanup(func() {
		agentStandaloneReadDir = previousReadDir
		agentStandaloneReadFile = previousReadFile
	})

	err := proveAgentStandaloneIdentityVacant(62091, 62092, time.Now().Add(time.Second), nil, nil)
	require.ErrorContains(t, err, "exactly one Uid, Gid, or Groups")
}

func TestAgentStandaloneStatusRequiresExactlyOneCredentialField(t *testing.T) {
	validEmptyGroups := []byte("Uid:\t1 1 1 1\nGid:\t2 2 2 2\nGroups:\t\n")
	matched, err := agentStandaloneStatusMatches(validEmptyGroups, 62093, 62094)
	require.NoError(t, err)
	require.False(t, matched)

	for _, payload := range [][]byte{
		[]byte("Uid:\t1 1 1 1\nGid:\t2 2 2 2\n"),
		[]byte("Uid:\t1 1 1 1\nGid:\t2 2 2 2\nGroups:\t3\nGroups:\t4\n"),
		[]byte("Uid:\t1 1 1 1\nUid:\t1 1 1 1\nGid:\t2 2 2 2\nGroups:\t\n"),
		[]byte("Uid:\t1 1 1 1\nGid:\t2 2 2 2\nGid:\t2 2 2 2\nGroups:\t\n"),
	} {
		_, err = agentStandaloneStatusMatches(payload, 62093, 62094)
		require.ErrorContains(t, err, "exactly one Uid, Gid, or Groups")
	}
	_, err = agentStandaloneStatusMatches(
		[]byte("Uid:\t1 1 1 1\nGid:\t2 2 2 2\nGroups:\tnot-a-gid\n"), 62093, 62094,
	)
	require.Error(t, err)
}

func openAgentStandaloneTestDirectory(t *testing.T) *os.File {
	t.Helper()
	directoryPath := t.TempDir()
	require.NoError(t, os.Chmod(directoryPath, 0o700))
	directory, err := os.Open(directoryPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })

	return directory
}

func agentStandaloneTestAuthorityIDs() (uint32, uint32) {
	return uint32(os.Geteuid()), uint32(os.Getegid())
}

func createAgentStandaloneTestLock(
	t *testing.T,
	directory *os.File,
	name string,
	ownerUID uint32,
	ownerGID uint32,
) *os.File {
	t.Helper()
	lock, err := openAgentStandaloneNamedLock(directory, name, true, ownerUID, ownerGID)
	require.NoError(t, err)
	t.Cleanup(func() {
		if lock.Fd() != ^uintptr(0) {
			require.NoError(t, lock.Close())
		}
	})

	return lock
}

func createAgentStandaloneMatchingDomainFixture(
	t *testing.T,
) (*os.File, uint32, uint32, agentStandaloneOwner) {
	t.Helper()
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	domain := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
	require.NoError(t, domain.Close())
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
	want := agentStandaloneOwner{
		Version: 1, UID: 62081, GID: 62082, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "matching-domain",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/matching-domain", Dev: 1, Ino: 2},
	}

	return directory, ownerUID, ownerGID, want
}

func createAgentStandaloneProtectedStateRoot(t *testing.T, uid, gid uint32) string {
	t.Helper()
	base, err := os.MkdirTemp("/var/lib", "acp-go-hermes-state-root-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(base)) })
	require.NoError(t, os.Chmod(base, 0o700))
	stateRoot := filepath.Join(base, "state")
	require.NoError(t, os.Mkdir(stateRoot, 0o700))
	require.NoError(t, os.Chown(stateRoot, int(uid), int(gid)))

	return stateRoot
}

func agentStandaloneTestDirEntries(t *testing.T, names ...string) []os.DirEntry {
	t.Helper()
	directory := t.TempDir()
	for _, name := range names {
		require.NoError(t, os.Mkdir(filepath.Join(directory, name), 0o700))
	}
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)

	return entries
}

func agentStandaloneTestStatus(uid, gid uint32, groups []uint32) []byte {
	groupText := ""
	for _, group := range groups {
		groupText += " " + strconv.FormatUint(uint64(group), 10)
	}

	return []byte(
		"Uid:\t" + strconv.FormatUint(uint64(uid), 10) + " " + strconv.FormatUint(uint64(uid), 10) + " " +
			strconv.FormatUint(uint64(uid), 10) + " " + strconv.FormatUint(uint64(uid), 10) + "\n" +
			"Gid:\t" + strconv.FormatUint(uint64(gid), 10) + " " + strconv.FormatUint(uint64(gid), 10) + " " +
			strconv.FormatUint(uint64(gid), 10) + " " + strconv.FormatUint(uint64(gid), 10) + "\n" +
			"Groups:\t" + groupText + "\n",
	)
}
