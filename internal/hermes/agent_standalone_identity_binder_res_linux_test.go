//go:build linux

package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneResPIDNamespace makes every PID-namespace fact the binder
// consults report one synthetic namespace inode. Entering a real CLONE_NEWPID
// namespace needs CAP_SYS_ADMIN, which the coverage container does not hold, so
// this is the only way to state the binder's decision for the initial and for a
// nested namespace regardless of which one the harness itself runs in.
func agentStandaloneResPIDNamespace(t *testing.T, ino uint64) {
	t.Helper()
	previous := agentAuthorityDomainStat
	t.Cleanup(func() { agentAuthorityDomainStat = previous })
	agentAuthorityDomainStat = func(path string, stat *unix.Stat_t) error {
		_, tail, underProc := strings.Cut(strings.TrimPrefix(path, "/proc/"), "/")
		if !underProc || (tail != "ns/pid" && tail != "ns/pid_for_children") {
			return previous(path, stat)
		}
		if err := previous("/proc/self/"+tail, stat); err != nil {
			return err
		}
		stat.Ino = ino

		return nil
	}
}

// agentStandaloneResProcessIdentity makes the process report a chosen PID
// together with the matching procfs self anchor, so a case can state which PID
// the binder sees without the test having to be that PID.
func agentStandaloneResProcessIdentity(t *testing.T, pid int) {
	t.Helper()
	previousID, previousLink := agentStandaloneProcessID, agentStandaloneReadlink
	t.Cleanup(func() {
		agentStandaloneProcessID, agentStandaloneReadlink = previousID, previousLink
	})
	agentStandaloneProcessID = func() int { return pid }
	agentStandaloneReadlink = func(path string) (string, error) {
		if path == "/proc/self" {
			return strconv.Itoa(pid), nil
		}

		return previousLink(path)
	}
}

// TestAgentStandaloneResBinderRefusesWhenItCannotIdentifyItsPIDNamespace proves
// the binder aborts when the kernel stops describing its own PID namespace
// instead of falling through to the namespace decision. A binder that guessed
// here would decide admission for a namespace it never identified.
func TestAgentStandaloneResBinderRefusesWhenItCannotIdentifyItsPIDNamespace(t *testing.T) {
	wantErr := errors.New("kernel stopped describing the PID namespace")
	previous := agentAuthorityDomainStat
	t.Cleanup(func() { agentAuthorityDomainStat = previous })
	agentAuthorityDomainStat = func(path string, stat *unix.Stat_t) error {
		if path == "/proc/self/ns/pid" {
			return wantErr
		}

		return previous(path, stat)
	}

	err := validateAgentStandaloneBinder()
	require.ErrorIs(t, err, wantErr)
	require.NotContains(t, err.Error(), "procfs self anchor")
}

// TestAgentStandaloneResBinderRequiresUnrestrictedRootProcfs proves the binder
// refuses when it cannot read PID 1's status, naming that reason. Reading PID 1
// is how the binder proves it is not looking at a filtered procfs, so a binder
// that continued past an unreadable /proc/1 would establish authority from
// inside exactly the view it is meant to reject.
func TestAgentStandaloneResBinderRequiresUnrestrictedRootProcfs(t *testing.T) {
	wantErr := errors.New("procfs hid pid 1 from this process")
	previous := agentStandaloneReadFile
	t.Cleanup(func() { agentStandaloneReadFile = previous })
	reads := 0
	agentStandaloneReadFile = func(path string) ([]byte, error) {
		if path == "/proc/1/status" {
			reads++

			return nil, wantErr
		}

		return previous(path)
	}

	err := validateAgentStandaloneBinder()
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "prove unrestricted root procfs visibility")
	require.Equal(t, 1, reads)
}

// TestAgentStandaloneResBinderAdmitsOnlyTheInitialNamespaceOrItsOwnInit proves
// the binder's whole admission rule for both namespaces the gate can run in: a
// process in the initial PID namespace is admitted whatever its PID, a process
// in a nested namespace is admitted only as that namespace's PID 1, and any
// other process in a nested namespace is refused by name. The namespace and the
// PID are both stated by the case, so every branch is decided the same way
// whether or not the harness shares the host PID namespace.
func TestAgentStandaloneResBinderAdmitsOnlyTheInitialNamespaceOrItsOwnInit(t *testing.T) {
	const initialPIDNamespaceInode = 0xeffffffc

	t.Run("initial namespace, ordinary pid", func(t *testing.T) {
		agentStandaloneResPIDNamespace(t, initialPIDNamespaceInode)
		agentStandaloneResProcessIdentity(t, 4242)

		require.NoError(t, validateAgentStandaloneBinder())
	})

	t.Run("nested namespace, namespace init", func(t *testing.T) {
		agentStandaloneResPIDNamespace(t, initialPIDNamespaceInode+1)
		agentStandaloneResProcessIdentity(t, 1)

		require.NoError(t, validateAgentStandaloneBinder())
	})

	t.Run("nested namespace, ordinary pid", func(t *testing.T) {
		agentStandaloneResPIDNamespace(t, initialPIDNamespaceInode+1)
		agentStandaloneResProcessIdentity(t, 4242)

		require.ErrorContains(t, validateAgentStandaloneBinder(),
			"non-initial PID namespace may establish agent authority only from namespace PID 1",
		)
	})
}

// TestAgentStandaloneResStateRootRefusesADescriptorTheKernelStopsDescribing
// proves the state-root bind aborts when the kernel stops answering for the
// filesystem root it opened, for an ancestor descriptor it is walking, or for
// the state root itself, and that no binding is returned in any of those cases.
// A bind that returned a zero or partially described root would let a claim
// record a state root nobody proved was the protected directory.
func TestAgentStandaloneResStateRootRefusesADescriptorTheKernelStopsDescribing(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone state root cases require root to own a protected state root")
	}
	uid, gid := uint32(62871), uint32(62872)
	path := createAgentStandaloneProtectedStateRoot(t, uid, gid)
	var raw unix.Stat_t
	require.NoError(t, unix.Stat(path, &raw))
	stateRootIno := raw.Ino

	t.Run("filesystem root", func(t *testing.T) {
		wantErr := errors.New("kernel refused the filesystem root")
		previous := agentStandaloneStateRootOpen
		t.Cleanup(func() { agentStandaloneStateRootOpen = previous })
		agentStandaloneStateRootOpen = func(string, int, uint32) (int, error) { return -1, wantErr }

		bound, bindErr := bindAgentStandaloneStateRoot(path, uid, gid)
		require.ErrorIs(t, bindErr, wantErr)
		require.ErrorContains(t, bindErr, "open filesystem root for standalone state root")
		require.Equal(t, agentStandaloneStateRoot{}, bound)
	})

	t.Run("ancestor descriptor", func(t *testing.T) {
		wantErr := errors.New("kernel stopped describing the ancestor")
		previous := agentStandaloneStateRootFstat
		t.Cleanup(func() { agentStandaloneStateRootFstat = previous })
		agentStandaloneStateRootFstat = func(int, *unix.Stat_t) error { return wantErr }

		bound, bindErr := bindAgentStandaloneStateRoot(path, uid, gid)
		require.ErrorIs(t, bindErr, wantErr)
		require.Equal(t, agentStandaloneStateRoot{}, bound)
	})

	t.Run("state root descriptor", func(t *testing.T) {
		wantErr := errors.New("kernel stopped describing the state root")
		previous := agentStandaloneStateRootFstat
		t.Cleanup(func() { agentStandaloneStateRootFstat = previous })
		agentStandaloneStateRootFstat = func(fd int, stat *unix.Stat_t) error {
			if statErr := previous(fd, stat); statErr != nil {
				return statErr
			}
			if stat.Ino == stateRootIno {
				return wantErr
			}

			return nil
		}

		bound, bindErr := bindAgentStandaloneStateRoot(path, uid, gid)
		require.ErrorIs(t, bindErr, wantErr)
		require.Equal(t, agentStandaloneStateRoot{}, bound)
	})
}

// TestAgentStandaloneResOwnerClaimRefusesCancellationBeforeItPublishes proves
// that a cancellation arriving after the claim's final state-root recheck but
// before it publishes aborts the claim with nothing published. The claim rechecks
// its budget once more precisely so that a shutdown observed at the last instant
// cannot leave an ACTIVE marker behind for an identity nobody is holding.
func TestAgentStandaloneResOwnerClaimRefusesCancellationBeforeItPublishes(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneCovProtectedOwner(t, 62881, 62882, "res-late-cancel")
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "62881.lock")
	require.NoError(t, createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID))
	agentStandaloneCovVacancySeam(t, agentStandaloneCovVacantScan)

	canceled := make(chan struct{})
	previous := agentStandaloneStateRootFstat
	t.Cleanup(func() { agentStandaloneStateRootFstat = previous })
	revalidations := 0
	agentStandaloneStateRootFstat = func(fd int, stat *unix.Stat_t) error {
		if statErr := previous(fd, stat); statErr != nil {
			return statErr
		}
		if stat.Ino == owner.StateRoot.Ino {
			revalidations++
			if revalidations == 2 {
				close(canceled)
			}
		}

		return nil
	}

	err := completeAgentStandaloneOwnerClaim(
		directory, owner, ownerUID, ownerGID, true, time.Now().Add(time.Second), canceled, nil,
	)
	require.ErrorIs(t, err, errAgentStandaloneCanceled)
	require.Equal(t, 2, revalidations, "the claim must recheck its state root twice before publishing")
	require.NoFileExists(t, filepath.Join(directory.Name(), "62881.quarantine"))
}
