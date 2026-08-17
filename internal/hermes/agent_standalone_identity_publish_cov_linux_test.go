//go:build linux

package hermes

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovRegularFile hands back a descriptor on a regular file. Every
// registry helper takes the authority root as an open directory descriptor, and
// a descriptor that is not a directory is the one input that makes the kernel
// refuse the traversal itself.
func agentStandaloneCovRegularFile(t *testing.T) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "not-a-registry")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	return file
}

// agentStandaloneCovProbeFcntlSeam faults the nth descriptor-flag call the
// authority probe makes.
func agentStandaloneCovProbeFcntlSeam(t *testing.T, nth int, wantErr error) {
	t.Helper()
	previous := agentStandaloneProbeFcntl
	t.Cleanup(func() { agentStandaloneProbeFcntl = previous })
	calls := 0
	agentStandaloneProbeFcntl = func(fd uintptr, cmd, arg int) (int, error) {
		calls++
		if calls == nth {
			return 0, wantErr
		}

		return previous(fd, cmd, arg)
	}
}

// TestAgentStandaloneCovProbeRefusesADescriptorItCannotPinCloseOnExec proves
// the authority probe aborts whenever it cannot read, set, or confirm
// close-on-exec on the probe descriptor, and that each refusal still removes the
// probe file. A probe descriptor that survived an exec would hand a child a
// writable handle inside the authority registry.
func TestAgentStandaloneCovProbeRefusesADescriptorItCannotPinCloseOnExec(t *testing.T) {
	for _, testCase := range []struct {
		name string
		call int
		want string
	}{
		{name: "read flags", call: 1, want: "read authority probe descriptor flags"},
		{name: "set close-on-exec", call: 2, want: "set authority probe close-on-exec"},
		{name: "re-read flags", call: 3, want: "re-read authority probe descriptor flags"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			wantErr := errors.New("injected descriptor flag failure")
			agentStandaloneCovProbeFcntlSeam(t, testCase.call, wantErr)

			err := probeAgentStandaloneFilesystem(directory, true)
			require.ErrorIs(t, err, wantErr)
			require.ErrorContains(t, err, testCase.want)
			entries, readErr := os.ReadDir(directory.Name())
			require.NoError(t, readErr)
			require.Empty(t, entries, "a refused probe must remove its probe file")
		})
	}
}

// TestAgentStandaloneCovProbeJudgesTheFilesystemBeforeItWritesAnything proves
// the probe refuses when the filesystem cannot be identified at all and when it
// is mounted read-only, before it creates any file, and that a filesystem type
// on the durable allowlist is admitted even when the probe is not test-only.
// Establishing authority on a read-only or unidentifiable filesystem would mean
// a registry that cannot record who holds it.
func TestAgentStandaloneCovProbeJudgesTheFilesystemBeforeItWritesAnything(t *testing.T) {
	previous := agentStandaloneProbeFstatfs
	t.Cleanup(func() { agentStandaloneProbeFstatfs = previous })

	t.Run("filesystem cannot be identified", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected statfs failure")
		agentStandaloneProbeFstatfs = func(int, *unix.Statfs_t) error { return wantErr }
		t.Cleanup(func() { agentStandaloneProbeFstatfs = previous })

		require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
		entries, readErr := os.ReadDir(directory.Name())
		require.NoError(t, readErr)
		require.Empty(t, entries, "an unidentified filesystem must not be written to")
	})

	t.Run("filesystem is read-only", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneProbeFstatfs = func(fd int, filesystem *unix.Statfs_t) error {
			if err := previous(fd, filesystem); err != nil {
				return err
			}
			filesystem.Flags |= unix.ST_RDONLY

			return nil
		}
		t.Cleanup(func() { agentStandaloneProbeFstatfs = previous })

		require.ErrorContains(t, probeAgentStandaloneFilesystem(directory, true), "filesystem is read-only")
		entries, readErr := os.ReadDir(directory.Name())
		require.NoError(t, readErr)
		require.Empty(t, entries, "a read-only filesystem must not be written to")
	})

	t.Run("filesystem type is on the durable allowlist", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneProbeFstatfs = func(fd int, filesystem *unix.Statfs_t) error {
			if err := previous(fd, filesystem); err != nil {
				return err
			}
			filesystem.Type = 0xef53

			return nil
		}
		t.Cleanup(func() { agentStandaloneProbeFstatfs = previous })

		require.NoError(t, probeAgentStandaloneFilesystem(directory, false))
		entries, readErr := os.ReadDir(directory.Name())
		require.NoError(t, readErr)
		require.Empty(t, entries, "a completed probe must remove its probe file")
	})
}

// TestAgentStandaloneCovProbeRefusesARegistryItCannotCreateFilesIn proves the
// probe reports the kernel's refusal when the authority root is not a directory
// it can create the probe file in, instead of reporting the filesystem durable.
func TestAgentStandaloneCovProbeRefusesARegistryItCannotCreateFilesIn(t *testing.T) {
	require.ErrorIs(t, probeAgentStandaloneFilesystem(agentStandaloneCovRegularFile(t), true), unix.ENOTDIR)
}

// TestAgentStandaloneCovProbeReportsAProbeFileItCouldNotRemove proves the probe
// fails even after every durability check passed if it could not remove the
// probe file it created. A retained probe file is registry state nothing
// accounts for, and the next claim's audit would refuse the whole registry.
func TestAgentStandaloneCovProbeReportsAProbeFileItCouldNotRemove(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	wantErr := errors.New("injected unlink failure")
	previous := agentStandaloneProbeUnlinkat
	t.Cleanup(func() { agentStandaloneProbeUnlinkat = previous })
	agentStandaloneProbeUnlinkat = func(int, string, int) error { return wantErr }

	require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
	entries, readErr := os.ReadDir(directory.Name())
	require.NoError(t, readErr)
	require.Len(t, entries, 1, "the probe file the refusal named must still be there")
}

// TestAgentStandaloneCovRegistrySweepsRequireADirectoryDescriptor proves the
// uniqueness sweep and the UID-lock pre-check refuse a descriptor that is not a
// directory, and refuse a registry that has been removed, instead of concluding
// that nothing collides and nothing exists. Both answers are what let a claim
// mint a permanent lock.
func TestAgentStandaloneCovRegistrySweepsRequireADirectoryDescriptor(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62811, 62812, "cov-sweep", "/srv/hermes/cov-sweep", 1, 2)

	t.Run("uniqueness sweep on a non-directory", func(t *testing.T) {
		require.ErrorIs(t, validateAgentStandaloneOwnerUniqueness(
			agentStandaloneCovRegularFile(t), want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), unix.ENOTDIR)
	})

	t.Run("uniqueness sweep on a removed registry", func(t *testing.T) {
		require.ErrorIs(t, validateAgentStandaloneOwnerUniqueness(
			agentStandaloneCovRemovedDirectory(t), want, ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		), unix.ENOENT)
	})

	t.Run("uid lock pre-check on a non-directory", func(t *testing.T) {
		require.ErrorIs(t,
			validateAgentStandaloneUIDLockMayBeCreated(agentStandaloneCovRegularFile(t), 62811),
			unix.ENOTDIR,
		)
	})
}

// TestAgentStandaloneCovTargetMarkerCleanupRefusesADeadUIDLockDescriptor proves
// the target-marker cleanup refuses when the UID lock descriptor it was handed
// cannot be inspected at all. The whole point of that descriptor is to prove the
// caller holds the UID lock, so a descriptor the kernel will not answer for must
// never be accepted as proof.
func TestAgentStandaloneCovTargetMarkerCleanupRefusesADeadUIDLockDescriptor(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	uidLock := createAgentStandaloneTestLock(t, directory, "62821.lock", ownerUID, ownerGID)
	require.NoError(t, uidLock.Close())
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "62821.quarantine.next-"+agentStandaloneCovSuffix, "partial",
	)

	require.ErrorIs(t, cleanupAgentStandaloneTargetMarkerTemporaries(
		directory, 62821, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	), unix.EBADF)
	require.FileExists(t, temporary)
}

// TestAgentStandaloneCovOwnerIdentityStopsWhenAPeerTemporaryTurnsBusyUnderLock
// proves a claim that takes owners.lock, finds a peer owner temporary it cannot
// remove because that UID is live, and is then canceled, releases owners.lock
// and leaves the temporary untouched. Removing a temporary whose UID lock is
// held would delete state a live claim is still writing.
func TestAgentStandaloneCovOwnerIdentityStopsWhenAPeerTemporaryTurnsBusyUnderLock(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	held := createAgentStandaloneTestLock(t, directory, "62831.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	temporary := filepath.Join(directory.Name(), "62831.owner.next-"+agentStandaloneCovSuffix)
	canceled := make(chan struct{})
	restoreAgentStandalonePermanentLockSeams(t)
	original := agentStandaloneLockOpenat
	planted := false
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		switch {
		case path == "owners.lock" && !planted:
			planted = true
			agentStandaloneCovWriteRegistryFile(
				t, directory, "62831.owner.next-"+agentStandaloneCovSuffix, "partial",
			)
		case path == "62831.lock":
			select {
			case <-canceled:
			default:
				close(canceled)
			}
		}

		return original(dirfd, path, flags, mode)
	}
	want := agentStandaloneCovOwner(62833, 62834, "cov-busy-under-lock", "/srv/hermes/cov-busy-under-lock", 3, 4)

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, want, ownerUID, ownerGID, time.Now().Add(5*time.Second), canceled, nil,
	)
	require.Nil(t, identity)
	require.ErrorIs(t, err, errAgentStandaloneCanceled)
	require.True(t, planted)
	require.FileExists(t, temporary, "a temporary whose UID lock is held must never be removed")
	contender, acquired, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, acquired, "the canceled claim must release owners.lock")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneCovOwnerPublicationRefusesToOverwriteOrPublishUnreadably
// proves the immutable owner binding is published without replacement and is
// read back before it is accepted: an authority root that is not a directory, a
// UID that already carries a binding, and a tuple that cannot be read back are
// each refused.
func TestAgentStandaloneCovOwnerPublicationRefusesToOverwriteOrPublishUnreadably(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("registry descriptor is not a directory", func(t *testing.T) {
		owner := agentStandaloneCovOwner(62841, 62842, "cov-publish", "/srv/hermes/cov-publish", 5, 6)

		require.ErrorIs(t,
			createAgentStandaloneOwner(agentStandaloneCovRegularFile(t), owner, ownerUID, ownerGID),
			unix.ENOTDIR,
		)
	})

	t.Run("uid already carries a binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		owner := agentStandaloneCovOwner(62843, 62844, "cov-publish-first", "/srv/hermes/cov-publish-first", 7, 8)
		require.NoError(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID))
		before, readErr := os.ReadFile(filepath.Join(directory.Name(), "62843.owner"))
		require.NoError(t, readErr)
		replacement := agentStandaloneCovOwner(62843, 62846, "cov-publish-second", "/srv/hermes/cov-publish-second", 9, 10)

		err := createAgentStandaloneOwner(directory, replacement, ownerUID, ownerGID)
		require.ErrorIs(t, err, unix.EEXIST)
		require.ErrorContains(t, err, "publish immutable standalone owner without replacement")
		after, afterErr := os.ReadFile(filepath.Join(directory.Name(), "62843.owner"))
		require.NoError(t, afterErr)
		require.Equal(t, before, after, "the immutable binding must survive the refused publication")
		entries, listErr := os.ReadDir(directory.Name())
		require.NoError(t, listErr)
		require.Len(t, entries, 1, "the refused publication must remove its temporary")
	})

	t.Run("published tuple cannot be read back", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		unknown := agentStandaloneCovOwner(62847, 62848, "cov-publish-unknown", "/srv/hermes/cov-publish-unknown", 11, 12)
		unknown.Provider = "github.com/savid/acp-go-unknown"

		require.ErrorContains(t,
			createAgentStandaloneOwner(directory, unknown, ownerUID, ownerGID),
			"published standalone owner payload changed",
		)
		_, loadErr := loadAgentStandaloneOwner(directory, unknown.UID, ownerUID, ownerGID)
		require.ErrorContains(t, loadErr, "standalone owner record is invalid",
			"the refusal must name a binding no later claim can read",
		)
	})
}

// TestAgentStandaloneCovMarkerPublicationRefusesAnUnpublishableTarget proves the
// marker publication refuses an authority root that is not a directory, a target
// name that is not a replaceable file, and a payload it cannot read back within
// the marker bound — and that no refusal leaves its temporary behind.
func TestAgentStandaloneCovMarkerPublicationRefusesAnUnpublishableTarget(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	deadline := func() time.Time { return time.Now().Add(time.Second) }

	t.Run("registry descriptor is not a directory", func(t *testing.T) {
		require.ErrorIs(t, replaceAgentStandaloneFile(
			agentStandaloneCovRegularFile(t), "62851.quarantine", []byte(`{"version":2}`),
			ownerUID, ownerGID, deadline(), nil, nil,
		), unix.ENOTDIR)
	})

	t.Run("target name is a directory", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		occupied := filepath.Join(directory.Name(), "62853.quarantine")
		require.NoError(t, os.Mkdir(occupied, 0o700))

		require.Error(t, replaceAgentStandaloneFile(
			directory, "62853.quarantine", []byte(`{"version":2}`),
			ownerUID, ownerGID, deadline(), nil, nil,
		))
		require.DirExists(t, occupied)
		entries, listErr := os.ReadDir(directory.Name())
		require.NoError(t, listErr)
		require.Len(t, entries, 1, "the refused publication must remove its temporary")
	})

	t.Run("payload cannot be read back", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		oversized := bytes.Repeat([]byte("a"), agentStandaloneMarkerMax)

		require.ErrorContains(t, replaceAgentStandaloneFile(
			directory, "62855.quarantine", oversized, ownerUID, ownerGID, deadline(), nil, nil,
		), "published agent identity marker payload changed")
	})
}

// TestAgentStandaloneCovDomainRecordPublicationRefusesAnUnpublishableTarget
// proves the domain-record publication refuses an authority root that is not a
// directory, a domain.json that is not a replaceable file, and a record it
// cannot read back — the last of which is what stops an unreadable authority
// record from being left as the registry's only evidence of who holds it.
func TestAgentStandaloneCovDomainRecordPublicationRefusesAnUnpublishableTarget(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("registry descriptor is not a directory", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "0123456789abcdef0123456789abcdef"

		require.ErrorIs(t, replaceAgentStandaloneDomainRecord(
			agentStandaloneCovRegularFile(t), ownerUID, ownerGID, record,
		), unix.ENOTDIR)
	})

	t.Run("record name is a directory", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "0123456789abcdef0123456789abcdef"
		occupied := filepath.Join(directory.Name(), "domain.json")
		require.NoError(t, os.Mkdir(occupied, 0o700))

		require.Error(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
		require.DirExists(t, occupied)
		entries, listErr := os.ReadDir(directory.Name())
		require.NoError(t, listErr)
		require.Len(t, entries, 1, "the refused publication must remove its temporary")
	})

	t.Run("record cannot be read back", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "short"

		require.ErrorContains(t,
			replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record),
			"agent authority domain record is incomplete",
		)
	})
}
