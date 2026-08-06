//go:build linux

package hermes

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestSupervisorStartRefusesAnUnusableTarget proves the supervisor refuses
// every target shape it cannot contain, before it spawns anything. The
// supervisor replaces the caller's command with itself and hands the target
// over through private descriptors, so a target that carries its own extra
// files, or that was never configured for its own process group, would be
// launched outside the boundary the supervisor is there to establish.
func TestSupervisorStartRefusesAnUnusableTarget(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		target func(*testing.T) *exec.Cmd
		want   string
	}{
		{
			name:   "no target",
			target: func(*testing.T) *exec.Cmd { return nil },
			want:   "hermes supervisor target is unavailable",
		},
		{
			name:   "target without a path",
			target: func(*testing.T) *exec.Cmd { return &exec.Cmd{} },
			want:   "hermes supervisor target is unavailable",
		},
		{
			name: "target that is not in its own process group",
			target: func(*testing.T) *exec.Cmd {
				return exec.Command("/bin/true")
			},
			want: "containment is not configured",
		},
		{
			name: "target carrying its own descriptors",
			target: func(t *testing.T) *exec.Cmd {
				t.Helper()
				target := exec.Command("/bin/true")
				configureHermesProcess(target)
				target.ExtraFiles = []*os.File{os.Stdin}

				return target
			},
			want: "does not accept target ExtraFiles",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)
			supervisorCovRefuseSpawn(t)

			_, err := startUnixContainedProcess(
				testCase.target(t), ContainmentSpec{Isolation: testProcessIsolation()},
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}
}

// TestSupervisorStartRefusesAConfigItCannotSeal proves no supervisor is spawned
// unless its launch config was published and sealed. The config carries the
// target path, argv and isolation policy through an anonymous descriptor the
// supervisor trusts precisely because it is immutable; a config that could not
// be created, written or sealed could still be rewritten after the supervisor
// adopted it.
func TestSupervisorStartRefusesAConfigItCannotSeal(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		apply func(*testing.T)
		want  string
	}{
		{
			name: "config image cannot be created",
			apply: func(*testing.T) {
				supervisorMemfd = func(string, int) (int, error) { return -1, unix.ENOSYS }
			},
			want: "create Hermes supervisor config",
		},
		{
			name: "config image cannot be written",
			apply: func(t *testing.T) {
				t.Helper()
				supervisorMemfd = func(string, int) (int, error) {
					return unix.Open(os.DevNull, unix.O_RDONLY|unix.O_CLOEXEC, 0)
				}
			},
			want: "encode Hermes supervisor config",
		},
		{
			name: "config image cannot be sealed",
			apply: func(*testing.T) {
				supervisorSealConfig = func(uintptr, int, int) (int, error) { return -1, unix.EPERM }
			},
			want: "seal Hermes supervisor config",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)
			supervisorCovRefuseSpawn(t)
			testCase.apply(t)

			target := exec.Command("/bin/true")
			configureHermesProcess(target)
			_, err := startUnixContainedProcess(target, ContainmentSpec{Isolation: testProcessIsolation()})
			require.ErrorContains(t, err, testCase.want)
		})
	}
}

// TestSupervisorStartPassesInheritedAuthorityAtItsAgreedDescriptors proves a
// borrowed identity lock and authority domain are duplicated into the exact
// descriptor numbers the supervisor re-adopts them from, and that a duplication
// the caller cannot satisfy leaves no supervisor process behind rather than
// starting one that would adopt whatever happened to occupy those numbers.
func TestSupervisorStartPassesInheritedAuthorityAtItsAgreedDescriptors(t *testing.T) {
	t.Run("both leases are handed over", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		supervisorExecutable = func() (string, error) { return "/bin/true", nil }

		duplicates := 0
		isolation := testProcessIsolation()
		isolation.IdentityLock = supervisorCovCapability{duplicates: &duplicates}
		isolation.AuthorityDomain = supervisorCovCapability{duplicates: &duplicates}

		target := exec.Command("/bin/true")
		configureHermesProcess(target)
		tree, err := startUnixContainedProcess(target, ContainmentSpec{Isolation: isolation})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tree.close() })

		require.Equal(t, 2, duplicates)
		require.Len(t, target.ExtraFiles, supervisorAuthorityFD-2)
		require.Equal(t, os.DevNull, target.ExtraFiles[supervisorIdentityLockFD-3].Name())
		require.Equal(t, os.DevNull, target.ExtraFiles[supervisorAuthorityFD-3].Name())
	})

	for _, testCase := range []struct {
		name      string
		identity  ProcessIdentityLockCapability
		authority ProcessIdentityLockCapability
		want      string
	}{
		{
			name:      "identity lock cannot be duplicated",
			identity:  unavailableIdentityDispositionCapability{},
			authority: supervisorCovCapability{},
			want:      "duplicate Hermes agent identity lock",
		},
		{
			name:      "authority domain cannot be duplicated",
			identity:  supervisorCovCapability{},
			authority: unavailableIdentityDispositionCapability{},
			want:      "duplicate Hermes agent authority domain",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)

			isolation := testProcessIsolation()
			isolation.IdentityLock = testCase.identity
			isolation.AuthorityDomain = testCase.authority

			target := exec.Command("/bin/true")
			configureHermesProcess(target)
			_, err := startUnixContainedProcess(target, ContainmentSpec{Isolation: isolation})
			require.ErrorContains(t, err, testCase.want)
			require.Nil(t, target.Process)
		})
	}
}

// TestSupervisorConfigRefusesAnIncompleteOrigin proves the supervisor re-reads
// its own launch config rather than trusting that the parent wrote a usable
// one. A config without a target, or one that claims only half of the inherited
// authority capabilities, describes a launch the supervisor cannot perform
// under the identity it was told to assume.
func TestSupervisorConfigRefusesAnIncompleteOrigin(t *testing.T) {
	empty := supervisorTestConfig([]string{"/bin/true"})
	empty.Path = ""
	require.ErrorContains(t, validateHermesSupervisorConfig(empty), "config is incomplete")

	argvless := supervisorTestConfig([]string{"/bin/true"})
	argvless.Args = nil
	require.ErrorContains(t, validateHermesSupervisorConfig(argvless), "config is incomplete")

	half := supervisorTestConfig([]string{"/bin/true"})
	half.IdentityLock = true
	require.ErrorContains(t, validateHermesSupervisorConfig(half), "must be provided together")
}

// TestSupervisorConfigPublishRequiresARewoundImage proves publishing the launch
// config is only complete once the image is readable from its first byte. The
// supervisor reads the config from the descriptor it inherits without seeking
// first, so an image left at the write offset would decode as an empty
// document. A rewind that fails must therefore fail the launch, not be ignored
// because the bytes were already written.
func TestSupervisorConfigPublishRequiresARewoundImage(t *testing.T) {
	config := supervisorTestConfig([]string{"/bin/true"})

	unwritable := &supervisorCovConfigImage{writeErr: errors.New("no space")}
	require.ErrorContains(t, writeHermesSupervisorConfig(unwritable, config), "encode Hermes supervisor config")

	unseekable := &supervisorCovConfigImage{seekErr: errors.New("not seekable")}
	require.ErrorContains(t, writeHermesSupervisorConfig(unseekable, config), "rewind Hermes supervisor config")
	require.NotZero(t, unseekable.written)
}

// TestSupervisorIdentityRequiresAPolicy proves the identity check refuses a
// missing isolation policy outright. The check is what proves the supervisor
// runs as trusted root and the target does not; with no policy there is nothing
// to compare and the launch must not proceed.
func TestSupervisorIdentityRequiresAPolicy(t *testing.T) {
	require.ErrorContains(t, validateHermesSupervisorIdentity(nil), "process isolation is required")
}

// TestSupervisorAuthorityCloseRoutesToItsOrigin proves closing the supervisor's
// authority releases each lease exactly once through whichever origin acquired
// it. A standalone acquisition owns both leases, so closing the pair separately
// as well would close each descriptor twice; an absent authority is not an
// error because there is nothing to release.
func TestSupervisorAuthorityCloseRoutesToItsOrigin(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	original := agentIdentityLockClose
	t.Cleanup(func() { agentIdentityLockClose = original })

	var absent *hermesSupervisorAuthority
	require.NoError(t, absent.Close())

	closes := 0
	agentIdentityLockClose = func(file *os.File) error {
		closes++

		return file.Close()
	}
	identity := &agentIdentityLock{file: supervisorCovTempFile(t)}
	domain := &agentIdentityLock{file: supervisorCovTempFile(t)}
	standalone := &agentStandaloneIdentity{identity: identity, authority: domain}

	authority := &hermesSupervisorAuthority{identity: identity, domain: domain, standalone: standalone}
	require.NoError(t, authority.Close())
	require.Equal(t, 2, closes)
	require.Nil(t, identity.file)
	require.Nil(t, domain.file)
}

// TestSupervisorLivenessReadinessRequiresACompleteLine proves the guardian
// accepts the liveness supervisor's readiness only from a whole line carrying a
// real process identity. The guardian uses that line to decide the native tree
// exists; a truncated read, a foreign line, or a nonsense pid would otherwise
// let it report readiness for a process nobody started.
func TestSupervisorLivenessReadinessRequiresACompleteLine(t *testing.T) {
	for line, want := range map[string]bool{
		"ready:1234\n": true,
		"ready:1234":   false,
		"1234\n":       false,
		"ready:0\n":    false,
		"ready:-1\n":   false,
		"ready:x\n":    false,
	} {
		require.Equal(t, want, validHermesLivenessReadiness(line), line)
	}
}

// TestSupervisorLivenessFileCleanupClosesWhatExists proves the liveness cleanup
// closes every descriptor it was handed and tolerates the ones a failed setup
// never created. Cleanup runs on paths where some pipes exist and others do
// not, and a descriptor left open there would outlive the launch it belonged
// to.
func TestSupervisorLivenessFileCleanupClosesWhatExists(t *testing.T) {
	file := supervisorCovTempFile(t)

	closeHermesLivenessFiles(nil, file)

	require.Error(t, file.Close())
}

// TestSupervisorAuthorityDuplicationFollowsTheBorrowedFlag proves the liveness
// supervisor is handed a duplicate of the real lease only when the authority
// was borrowed, and an inert descriptor otherwise. Passing the wrong one would
// either drop the lease the liveness supervisor must keep held, or hand a
// descriptor to a supervisor that never adopted an authority.
func TestSupervisorAuthorityDuplicationFollowsTheBorrowedFlag(t *testing.T) {
	lock := &agentIdentityLock{file: supervisorCovTempFile(t)}

	borrowed, err := duplicateHermesSupervisorAuthority(lock, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = borrowed.Close() })
	require.Equal(t, supervisorCovInode(t, lock.file), supervisorCovInode(t, borrowed))

	inert, err := duplicateHermesSupervisorAuthority(lock, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = inert.Close() })
	require.Equal(t, os.DevNull, inert.Name())
}

// TestSupervisorStopEscalatesToKillAndReportsAnUnsettledRoot proves the stop
// sequence escalates from SIGTERM to SIGKILL and, when the supervised root
// still has not been reaped, reports it as unsettled rather than as a
// completed stop. The caller uses that answer to keep signalling instead of
// completing containment. A root that is reaped during the kill grace is
// settled, and carries its own wait result back to the caller.
func TestSupervisorStopEscalatesToKillAndReportsAnUnsettledRoot(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	supervisorTermGrace = 20 * time.Millisecond
	supervisorKillGrace = 20 * time.Millisecond
	supervisorPollInterval = time.Millisecond
	listSupervisorDescendants = func(int) (map[int]byte, error) { return map[int]byte{4242: 'S'}, nil }

	signals := map[syscall.Signal]int{}
	supervisorPIDFDOpen = func(int, int) (int, error) { return syscall.Dup(int(os.Stdin.Fd())) }
	supervisorPIDFDSendSignal = func(_ int, signal syscall.Signal, _ *unix.Siginfo, _ int) error {
		signals[signal]++

		return nil
	}

	err, settled := stopSupervisedDescendants(4242, make(chan error))
	require.ErrorContains(t, err, "supervised Hermes root 4242 did not exit")
	require.False(t, settled)
	require.NotZero(t, signals[syscall.SIGTERM])
	require.NotZero(t, signals[syscall.SIGKILL])

	reaped := errors.New("signal: killed")
	done := make(chan error, 1)
	done <- reaped
	supervisorTermGrace = 0
	err, settled = stopSupervisedDescendants(4242, done)
	require.ErrorIs(t, err, reaped)
	require.True(t, settled)
}

// TestSupervisorGuardianPeerIsProvenLiveBeforeTheNativeLaunch proves the
// liveness supervisor refuses to launch when it cannot prove its guardian is
// still there, and proceeds when the peer descriptor is quiet. The guardian
// holds the containment proof for the whole tree, so launching after it died
// would leave a native process whose exit nobody reports.
//
// Each of the three verdicts is separated. A quiet peer proceeds. A peer whose
// guardian has gone refuses by naming the departed guardian, driven by a real
// POLLHUP rather than by a stubbed poll, so the case pins what the kernel
// actually reports for a closed peer. A poll that cannot answer at all refuses
// by naming the failed poll instead, because a supervisor that cannot ask the
// question must not conclude the guardian is alive.
func TestSupervisorGuardianPeerIsProvenLiveBeforeTheNativeLaunch(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	peerRead, peerWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peerRead.Close() })
	t.Cleanup(func() { _ = peerWrite.Close() })

	require.NoError(t, validateHermesSupervisorGuardianPeer(peerRead, make(chan struct{})))

	// A guardian that has exited leaves its end of the peer pipe closed, so the
	// liveness supervisor's own end polls POLLHUP through the real unix.Poll.
	// The refusal has to come from the descriptor rather than from the done
	// channel, so the channel handed in here stays open.
	deadRead, deadWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = deadRead.Close() })
	require.NoError(t, deadWrite.Close())

	hangup := validateHermesSupervisorGuardianPeer(deadRead, make(chan struct{}))
	require.ErrorContains(t, hangup, "Hermes guardian exited before native launch")
	require.NotErrorIs(t, hangup, unix.EINVAL)
	require.NotContains(t, hangup.Error(), "poll Hermes guardian before native launch")

	supervisorPoll = func([]unix.PollFd, int) (int, error) { return -1, unix.EINVAL }
	require.ErrorContains(
		t, validateHermesSupervisorGuardianPeer(peerRead, make(chan struct{})),
		"poll Hermes guardian before native launch",
	)
}

type supervisorCovCapability struct {
	duplicates *int
}

func (capability supervisorCovCapability) Duplicate() (*os.File, error) {
	if capability.duplicates != nil {
		*capability.duplicates++
	}

	return os.Open(os.DevNull)
}

type supervisorCovConfigImage struct {
	writeErr error
	seekErr  error
	written  int
}

func (image *supervisorCovConfigImage) Write(value []byte) (int, error) {
	if image.writeErr != nil {
		return 0, image.writeErr
	}
	image.written += len(value)

	return len(value), nil
}

func (image *supervisorCovConfigImage) Seek(int64, int) (int64, error) {
	return 0, image.seekErr
}

func supervisorCovRefuseSpawn(t *testing.T) {
	t.Helper()
	supervisorCommand = func(name string, args ...string) *exec.Cmd {
		t.Errorf("refused supervisor start spawned %q", name)

		return exec.Command("/bin/true", args...)
	}
}

func supervisorCovTempFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "supervisor-cov-")
	require.NoError(t, err)

	return file
}

func supervisorCovInode(t *testing.T, file *os.File) uint64 {
	t.Helper()
	var stat unix.Stat_t
	require.NoError(t, unix.Fstat(int(file.Fd()), &stat))

	return stat.Ino
}

var _ io.WriteSeeker = (*supervisorCovConfigImage)(nil)

// TestSupervisorCompletionReportsALostStatusChannel proves the liveness
// supervisor releases its authority before it publishes completion, and reports
// a publication it could not make instead of falling back to the containment
// proof byte. That byte is the guardian's to write while the guardian is alive;
// writing it here would tell the parent the whole tree is accounted for on the
// word of a supervisor whose completion nobody received.
func TestSupervisorCompletionReportsALostStatusChannel(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	proveSupervisorDescendants = func(time.Duration) error { return nil }

	identity := &agentIdentityLock{file: supervisorCovTempFile(t)}
	domain := &agentIdentityLock{file: supervisorCovTempFile(t)}
	authority := &hermesSupervisorAuthority{identity: identity, domain: domain}

	var proof bytes.Buffer
	err := completeHermesSupervisorAuthority(
		&authority, make(chan struct{}), supervisorCovBrokenWriter{}, &proof, true,
	)
	require.ErrorContains(t, err, "publish Hermes liveness completion")
	require.Nil(t, authority)
	require.Nil(t, identity.file)
	require.Nil(t, domain.file)
	require.Zero(t, proof.Len())
}

type supervisorCovBrokenWriter struct{}

func (supervisorCovBrokenWriter) Write([]byte) (int, error) { return 0, os.ErrClosed }
