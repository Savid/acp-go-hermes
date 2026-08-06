//go:build linux

package hermes

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The liveness stand-ins below speak the guardian's protocol on the exact
// descriptors the guardian assigns to it: readiness and completion on the
// status channel at descriptor 5, and the guardian's own liveness on the peer
// channel at descriptor 9.
const (
	supervisorCovReadyThenDone   = `echo "ready:$$" >&5; echo done >&5`
	supervisorCovReadyThenWait   = `echo "ready:$$" >&5; read line <&9; echo done >&5`
	supervisorCovReadyThenSilent = `echo "ready:$$" >&5`
	supervisorCovNeverReady      = `echo garbage >&5; read line <&9`
)

// TestSupervisorGuardianReportsTheLivenessResultOnceContainmentIsProven proves
// the guardian's normal legs: it waits for the liveness supervisor to publish a
// real readiness line, reports the liveness exit status as its own, and writes
// the containment proof byte once the authority has been released. That byte is
// the parent's only evidence that the native tree is gone, so it must be
// written on every leg that reaches completion, including the one where the
// liveness supervisor never publishes its completion line.
func TestSupervisorGuardianReportsTheLivenessResultOnceContainmentIsProven(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		script string
		act    func(*testing.T, *os.File)
	}{
		{name: "liveness completes on its own", script: supervisorCovReadyThenDone},
		{name: "liveness never publishes completion", script: supervisorCovReadyThenSilent},
		{
			name:   "control channel closes",
			script: supervisorCovReadyThenWait,
			act: func(t *testing.T, control *os.File) {
				t.Helper()
				require.NoError(t, control.Close())
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)

			code, proof, diagnostic := supervisorCovRunGuardian(
				t, supervisorTestConfig([]string{"/bin/true"}), testCase.script, testCase.act,
			)
			require.Equal(t, 0, code)
			require.Equal(t, []byte{1}, proof)
			require.Empty(t, diagnostic)
		})
	}
}

// TestSupervisorGuardianForwardsTerminationToTheLivenessLease proves a
// termination signal delivered to the guardian is forwarded to the liveness
// supervisor's lease group rather than acted on by the guardian itself. The
// guardian holds the proof channel for the whole tree, so exiting on its own
// signal would abandon the native processes it is there to account for.
func TestSupervisorGuardianForwardsTerminationToTheLivenessLease(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	started := filepath.Join(t.TempDir(), "liveness.pid")
	script := `echo "ready:$$" >&5
trap 'echo done >&5; exit 0' TERM
echo $$ > "` + started + `"
while :; do sleep 0.02; done`

	code, proof, diagnostic := supervisorCovRunGuardian(
		t, supervisorTestConfig([]string{"/bin/true"}), script,
		func(t *testing.T, _ *os.File) {
			t.Helper()
			awaitSupervisorPIDFile(t, started)
			require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
		},
	)
	require.Equal(t, 0, code)
	require.Equal(t, []byte{1}, proof)
	// The lease group carries the signal, so the shell reports its own killed
	// child; the guardian itself must report nothing.
	require.NotContains(t, diagnostic, "acp-go-hermes trusted supervisor")
}

// TestSupervisorGuardianRefusesAnUnprovenLiveness proves the guardian refuses a
// liveness supervisor that does not identify itself, and one whose status
// channel cannot carry the deadline that bounds that wait, and that it still
// completes its authority and proof. A guardian blocked forever on a status
// read would hold the agent identity lease with nothing supervising the tree;
// one that accepted an unparsable line would report a native tree that was
// never launched.
func TestSupervisorGuardianRefusesAnUnprovenLiveness(t *testing.T) {
	t.Run("readiness line is not a liveness identity", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)

		code, proof, diagnostic := supervisorCovRunGuardian(
			t, supervisorTestConfig([]string{"/bin/true"}), supervisorCovNeverReady, nil,
		)
		require.Equal(t, 125, code)
		require.Equal(t, []byte{1}, proof)
		require.Empty(t, diagnostic)
	})

	t.Run("status channel cannot carry a read deadline", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		supervisorCovUnwaitablePipes(t)

		code, proof, diagnostic := supervisorCovRunGuardian(
			t, supervisorTestConfig([]string{"/bin/true"}), supervisorCovReadyThenWait, nil,
		)
		require.Equal(t, 125, code)
		require.Equal(t, []byte{1}, proof)
		require.Empty(t, diagnostic)
	})
}

// TestSupervisorGuardianRefusesToRunWithoutItsAuthority proves the guardian
// supervises nothing when the agent identity authority cannot be acquired, and
// still writes the containment proof. The parent blocks on that byte, so a
// guardian that failed silently would leave it unable to distinguish "nothing
// was started" from "a native tree is still running".
func TestSupervisorGuardianRefusesToRunWithoutItsAuthority(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	refused := errors.New("standalone identity is claimed")
	supervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return nil, refused
	}
	supervisorCommand = func(name string, _ ...string) *exec.Cmd {
		t.Errorf("guardian without authority spawned %q", name)

		return exec.Command("/bin/true")
	}

	code, proof, diagnostic := supervisorCovRunGuardian(t, supervisorCovStandaloneConfig(), "", nil)
	require.Equal(t, 125, code)
	require.Equal(t, []byte{1}, proof)
	require.Contains(t, diagnostic, refused.Error())
}

// TestSupervisorGuardianRefusesALivenessItCannotEquip proves every step of
// equipping the liveness supervisor is fail-closed: its sealed config, its two
// channels, the duplicates of the authority leases it must keep held, the
// supervisor image, and the launch itself. None of these may half-succeed into
// a running liveness supervisor, and each refusal still completes the
// guardian's authority and writes its proof so the parent is not left waiting.
func TestSupervisorGuardianRefusesALivenessItCannotEquip(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		apply func(*testing.T, *agentStandaloneIdentity)
		want  string
	}{
		{
			name: "config image cannot be created",
			apply: func(*testing.T, *agentStandaloneIdentity) {
				supervisorMemfd = func(string, int) (int, error) { return -1, unix.ENOSYS }
			},
			want: "function not implemented",
		},
		{
			name: "config image cannot be written",
			apply: func(*testing.T, *agentStandaloneIdentity) {
				supervisorMemfd = func(string, int) (int, error) {
					return unix.Open(os.DevNull, unix.O_RDONLY|unix.O_CLOEXEC, 0)
				}
			},
			want: "encode Hermes supervisor config",
		},
		{
			name: "config image cannot be sealed",
			apply: func(*testing.T, *agentStandaloneIdentity) {
				supervisorSealConfig = func(uintptr, int, int) (int, error) { return -1, unix.EPERM }
			},
			want: "operation not permitted",
		},
		{
			name: "status channel cannot be created",
			apply: func(*testing.T, *agentStandaloneIdentity) {
				supervisorPipe = func() (*os.File, *os.File, error) { return nil, nil, unix.EMFILE }
			},
			want: "too many open files",
		},
		{
			name: "peer channel cannot be created",
			apply: func(*testing.T, *agentStandaloneIdentity) {
				pipes := 0
				supervisorPipe = func() (*os.File, *os.File, error) {
					pipes++
					if pipes == 2 {
						return nil, nil, unix.EMFILE
					}

					return os.Pipe()
				}
			},
			want: "too many open files",
		},
		{
			name: "identity lease cannot be duplicated",
			apply: func(t *testing.T, standalone *agentStandaloneIdentity) {
				t.Helper()
				supervisorCovLenientLeaseRelease(t)
				require.NoError(t, standalone.identity.file.Close())
			},
			want: "bad file descriptor",
		},
		{
			name: "authority lease cannot be duplicated",
			apply: func(t *testing.T, standalone *agentStandaloneIdentity) {
				t.Helper()
				supervisorCovLenientLeaseRelease(t)
				require.NoError(t, standalone.authority.file.Close())
			},
			want: "bad file descriptor",
		},
		{
			name: "supervisor image cannot be resolved",
			apply: func(*testing.T, *agentStandaloneIdentity) {
				supervisorExecutable = func() (string, error) { return "", errors.New("no supervisor image") }
			},
			want: "no supervisor image",
		},
		{
			name: "liveness supervisor cannot be launched",
			apply: func(t *testing.T, _ *agentStandaloneIdentity) {
				t.Helper()
				missing := filepath.Join(t.TempDir(), "absent-supervisor")
				supervisorCommand = func(string, ...string) *exec.Cmd { return exec.Command(missing) }
			},
			want: "no such file or directory",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)
			standalone := supervisorCovStandaloneAuthority(t)
			supervisorAcquireStandalone = func(
				uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
			) (*agentStandaloneIdentity, error) {
				return standalone, nil
			}
			testCase.apply(t, standalone)

			code, proof, diagnostic := supervisorCovRunGuardian(t, supervisorCovStandaloneConfig(), "", nil)
			require.Equal(t, 125, code)
			require.Equal(t, []byte{1}, proof)
			require.Contains(t, diagnostic, testCase.want)
		})
	}
}

// TestSupervisorGuardianReportsAnUnreleasedAuthority proves the guardian
// reports failure when its authority cannot be released, even though the
// liveness supervisor completed normally, and withholds the containment proof.
// The agent identity lease is what stops a second session claiming the same
// identity, so a release that did not happen must never be reported as a clean
// exit.
func TestSupervisorGuardianReportsAnUnreleasedAuthority(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	standalone := supervisorCovStandaloneAuthority(t)
	supervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return standalone, nil
	}
	original := agentIdentityLockClose
	t.Cleanup(func() { agentIdentityLockClose = original })
	agentIdentityLockClose = func(file *os.File) error {
		_ = file.Close()

		return errors.New("lease was not released")
	}

	code, proof, diagnostic := supervisorCovRunGuardian(
		t, supervisorCovStandaloneConfig(), supervisorCovReadyThenDone, nil,
	)
	require.Equal(t, 125, code)
	require.Empty(t, proof)
	require.Empty(t, diagnostic)
}

// supervisorCovRunGuardian runs the production guardian against a liveness
// stand-in that speaks its protocol, and reports the guardian's exit code, the
// bytes it published on the containment proof channel, and the diagnostic it
// wrote for the parent. act runs while the guardian is live and owns the
// control channel the parent would hold. The caller must have installed
// restoreLinuxSupervisorSeams.
func supervisorCovRunGuardian(
	t *testing.T,
	config hermesSupervisorConfig,
	script string,
	act func(*testing.T, *os.File),
) (int, []byte, string) {
	t.Helper()
	proveSupervisorDescendants = func(time.Duration) error { return nil }
	if script != "" {
		supervisorCommand = func(string, ...string) *exec.Cmd { return exec.Command("/bin/sh", "-c", script) }
	}

	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)
	proofRead, proofWrite, err := os.Pipe()
	require.NoError(t, err)
	diagnosticRead, diagnosticWrite, err := os.Pipe()
	require.NoError(t, err)

	stderr := os.Stderr
	os.Stderr = diagnosticWrite
	exit := make(chan int, 1)
	go func() { exit <- runHermesProcessSupervisorGuardian(config, controlRead, proofWrite) }()

	if act != nil {
		act(t, controlWrite)
	}
	code := <-exit
	os.Stderr = stderr

	require.NoError(t, diagnosticWrite.Close())
	diagnostic, err := io.ReadAll(diagnosticRead)
	require.NoError(t, err)
	require.NoError(t, diagnosticRead.Close())

	_ = controlWrite.Close()
	proof, err := io.ReadAll(proofRead)
	require.NoError(t, err)
	require.NoError(t, proofRead.Close())

	return code, proof, string(diagnostic)
}

// supervisorCovUnwaitablePipes replaces the guardian's channels with regular
// files, which carry bytes but cannot carry a read deadline.
func supervisorCovUnwaitablePipes(t *testing.T) {
	t.Helper()
	supervisorPipe = func() (*os.File, *os.File, error) {
		file, err := os.CreateTemp(t.TempDir(), "supervisor-cov-channel-")
		if err != nil {
			return nil, nil, err
		}
		reader, err := os.Open(file.Name())
		if err != nil {
			return nil, nil, err
		}

		return reader, file, nil
	}
}

// supervisorCovLenientLeaseRelease keeps releasing a lease whose descriptor a
// test already closed from becoming a second, unrelated failure.
func supervisorCovLenientLeaseRelease(t *testing.T) {
	t.Helper()
	original := agentIdentityLockClose
	t.Cleanup(func() { agentIdentityLockClose = original })
	agentIdentityLockClose = func(file *os.File) error {
		_ = file.Close()

		return nil
	}
}

func supervisorCovStandaloneConfig() hermesSupervisorConfig {
	config := supervisorTestConfig([]string{"/bin/true"})
	// A native launch must not depend on the ambient environment of the test
	// process, which other cases in this package mutate.
	config.Env = []string{"PATH=/usr/bin:/bin"}
	config.Isolation.StandaloneOwnerID = "supervisor-cov"
	config.Isolation.StandaloneStateRoot = "/var/lib/acp-go-hermes-supervisor-cov"

	return config
}

func supervisorCovStandaloneAuthority(t *testing.T) *agentStandaloneIdentity {
	t.Helper()

	return &agentStandaloneIdentity{
		identity:  &agentIdentityLock{file: supervisorCovTempFile(t)},
		authority: &agentIdentityLock{file: supervisorCovTempFile(t)},
	}
}
