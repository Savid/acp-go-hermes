//go:build linux

package hermes

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestSupervisorBootstrapRefusesAnUnusableInheritance proves the self-exec
// bootstrap refuses to become a supervisor unless everything it inherited is
// usable: the kernel must accept the hardening it applies to itself, every
// inherited descriptor must be protected from the exec it is about to perform,
// the config image must decode, and the launch it describes must be one this
// supervisor can perform. Each refusal must stop before the next stage, because
// a supervisor that continued would run the native target with a boundary it
// could not establish.
func TestSupervisorBootstrapRefusesAnUnusableInheritance(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		mode       string
		payload    string
		omit       uintptr
		apply      func(*testing.T)
		readConfig bool
	}{
		{
			name: "process cannot be made undumpable",
			mode: hermesSupervisorGuardian,
			apply: func(*testing.T) {
				supervisorPrctl = func(option int, _, _, _, _ uintptr) error {
					if option == unix.PR_SET_DUMPABLE {
						return unix.EINVAL
					}

					return nil
				}
			},
		},
		{
			name: "inherited descriptor cannot be protected from exec",
			mode: hermesSupervisorGuardian,
			apply: func(*testing.T) {
				supervisorCloseOnExec = func(int) error { return unix.EBADF }
			},
		},
		{
			name:       "config image does not decode",
			mode:       hermesSupervisorGuardian,
			payload:    "not a supervisor config",
			readConfig: true,
		},
		{
			name:       "config image is not a launch this supervisor can perform",
			mode:       hermesSupervisorGuardian,
			payload:    `{"path":"","args":[],"isolation":{}}`,
			readConfig: true,
		},
		{
			name:       "liveness completion channel is missing",
			mode:       hermesSupervisorLiveness,
			omit:       8,
			readConfig: true,
		},
		{
			name:       "liveness guardian peer is missing",
			mode:       hermesSupervisorLiveness,
			omit:       9,
			readConfig: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)
			supervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error { return nil }
			supervisorCloseOnExec = func(int) error { return nil }
			supervisorCommand = func(name string, _ ...string) *exec.Cmd {
				t.Errorf("refused supervisor bootstrap spawned %q", name)

				return exec.Command("/bin/true")
			}
			if testCase.apply != nil {
				testCase.apply(t)
			}

			inherited := supervisorCovInherit(t, testCase.payload, testCase.omit)

			require.Equal(t, 125, runHermesProcessSupervisor(testCase.mode))
			require.Equal(t, testCase.readConfig, inherited.configConsumed(t))
		})
	}
}

// TestSupervisorLivenessModeRunsTheNativeLegAndPublishesItsProtocol proves the
// bootstrap routes the liveness mode to the native leg with the descriptors
// that mode owns, and that the leg speaks the whole protocol back: the native
// root's identity on the status channel, then the completion line once the
// authority has been released. While the guardian is alive the completion is a
// line on the status channel and never the proof byte, which is the guardian's
// to write.
func TestSupervisorLivenessModeRunsTheNativeLegAndPublishesItsProtocol(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	supervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error { return nil }
	supervisorCloseOnExec = func(int) error { return nil }
	proveSupervisorDescendants = func(time.Duration) error { return nil }

	inherited := supervisorCovInherit(t, "", 0)

	require.Equal(t, 0, runHermesProcessSupervisor(hermesSupervisorLiveness))

	status := inherited.read(t, 5)
	require.True(t, strings.HasSuffix(status, "done\n"), status)
	ready, _, ok := strings.Cut(status, "\n")
	require.True(t, ok, status)
	pid, err := strconv.Atoi(strings.TrimPrefix(ready, "ready:"))
	require.NoError(t, err)
	require.Positive(t, pid)
	require.Empty(t, inherited.read(t, 8))
}

// TestSupervisorNativeShutsDownWhenTheGuardianVanishes proves the native leg
// treats the guardian's disappearance as a shutdown even while the control
// channel from the parent is still open. The guardian is the process that
// reports this tree's containment to the parent, so continuing to supervise
// after it is gone would leave a native tree nobody can account for.
func TestSupervisorNativeShutsDownWhenTheGuardianVanishes(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	supervisorCovStandaloneSeam(t, supervisorCovStandaloneAuthority(t))
	proveSupervisorDescendants = func(time.Duration) error { return nil }

	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = controlWrite.Close() })
	peerRead, peerWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peerRead.Close() })

	started := filepath.Join(t.TempDir(), "native.pid")
	config := supervisorCovStandaloneConfig()
	config.Path = "/bin/sh"
	config.Args = []string{"/bin/sh", "-c", `echo $$ > "$1"; while :; do sleep 0.02; done`, "native", started}

	var proof supervisorCovBuffer
	exit := make(chan int, 1)
	go func() {
		exit <- runHermesProcessSupervisorNative(
			config, []io.Reader{controlRead}, peerRead, nil, &proof, false,
		)
	}()

	native := awaitSupervisorPIDFile(t, started)
	require.NoError(t, peerWrite.Close())

	require.NotZero(t, <-exit)
	require.Equal(t, []byte{1}, proof.bytes())
	awaitSupervisorProcessGone(t, native)
}

// TestSupervisorNativeRefusesToLaunchWithoutAuthority proves the native leg
// launches nothing when the agent identity authority cannot be acquired, and
// withholds the containment proof. There is no tree to account for, and the
// proof byte must mean "the tree is gone" rather than "the supervisor exited".
func TestSupervisorNativeRefusesToLaunchWithoutAuthority(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	refused := errors.New("standalone identity is claimed")
	supervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return nil, refused
	}

	marker := filepath.Join(t.TempDir(), "native-launched")
	config := supervisorCovStandaloneConfig()
	config.Path = "/bin/sh"
	config.Args = []string{"/bin/sh", "-c", `touch "$1"`, "native", marker}

	var proof supervisorCovBuffer
	code := runHermesProcessSupervisorNative(
		config, []io.Reader{strings.NewReader("")}, nil, nil, &proof, false,
	)
	require.Equal(t, 125, code)
	require.Empty(t, proof.bytes())
	require.NoFileExists(t, marker)
}

// TestSupervisorNativeReportsAnUnreleasedAuthority proves the native leg
// distinguishes "this launch was refused" from "this launch was refused and the
// agent identity lease is still held". Every refusal after the authority was
// acquired must release it, and a release that did not happen is reported with
// its own exit status so the parent never treats the identity as free.
func TestSupervisorNativeReportsAnUnreleasedAuthority(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		prepare   func(*testing.T, *hermesSupervisorConfig) (*os.File, io.Writer)
		wantProof string
		want      string
	}{
		{
			name: "guardian died before the native launch",
			prepare: func(t *testing.T, _ *hermesSupervisorConfig) (*os.File, io.Writer) {
				t.Helper()
				peerRead, peerWrite, err := os.Pipe()
				require.NoError(t, err)
				require.NoError(t, peerWrite.Close())
				t.Cleanup(func() { _ = peerRead.Close() })

				return peerRead, nil
			},
			wantProof: "\x01",
			want:      "guardian peer: hermes guardian exited before native launch",
		},
		{
			name: "native privileges could not be prepared",
			prepare: func(t *testing.T, config *hermesSupervisorConfig) (*os.File, io.Writer) {
				t.Helper()
				config.Isolation.BaseEnvironment = nil

				return nil, nil
			},
			wantProof: "\x01",
			want:      "prepare native target: apply Hermes native process isolation",
		},
		{
			name: "native target could not be started",
			prepare: func(t *testing.T, config *hermesSupervisorConfig) (*os.File, io.Writer) {
				t.Helper()
				missing := filepath.Join(t.TempDir(), "absent-native")
				config.Path = missing
				config.Args = []string{missing}

				return nil, nil
			},
			wantProof: "\x01",
			want:      "start native target: fork/exec",
		},
		{
			name: "readiness could not be published",
			prepare: func(t *testing.T, _ *hermesSupervisorConfig) (*os.File, io.Writer) {
				t.Helper()

				return nil, &supervisorCovLostReadiness{}
			},
			want: "publish native readiness",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)
			proveSupervisorDescendants = func(time.Duration) error { return nil }

			run := func(t *testing.T) (int, string, string, io.Writer) {
				t.Helper()
				config := supervisorCovStandaloneConfig()
				peer, status := testCase.prepare(t, &config)
				supervisorCovStandaloneSeam(t, supervisorCovStandaloneAuthority(t))
				proof := &supervisorCovBuffer{}
				diagnostic := supervisorCovCaptureDiagnostics(t)
				code := supervisorCovRunNative(config, peer, status, proof, status != nil)

				return code, string(proof.bytes()), diagnostic(), status
			}

			code, proof, diagnostic, status := run(t)
			require.Contains(t, diagnostic, testCase.want)
			require.Equal(t, 125, code)
			require.Equal(t, testCase.wantProof, proof)
			if publisher, ok := status.(*supervisorCovLostReadiness); ok {
				require.Equal(t, "done\n", publisher.published.String())
			}

			supervisorCovUnreleasableLease(t)
			code, proof, diagnostic, _ = run(t)
			require.Contains(t, diagnostic, testCase.want)
			require.Equal(t, 126, code)
			require.Empty(t, proof)
		})
	}
}

// TestSupervisorNativeKeepsSignallingAnUnsettledRoot proves the native leg does
// not complete containment while the supervised root is still unreaped: it
// keeps signalling the whole descendant set until the wait settles. Completing
// on an unsettled root would publish a containment proof for processes that are
// still alive.
func TestSupervisorNativeKeepsSignallingAnUnsettledRoot(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	supervisorCovStandaloneSeam(t, supervisorCovStandaloneAuthority(t))
	supervisorPollInterval = 0
	proveSupervisorDescendants = func(time.Duration) error { return nil }
	unsettled := errors.New("supervised root did not exit")
	stopSupervisorDescendants = func(int, <-chan error) (error, bool) { return unsettled, false }

	started := filepath.Join(t.TempDir(), "native.pid")
	config := supervisorCovStandaloneConfig()
	config.Path = "/bin/sh"
	config.Args = []string{"/bin/sh", "-c", `echo $$ > "$1"; while :; do sleep 0.02; done`, "native", started}

	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)
	native := make(chan int, 1)
	var proof supervisorCovBuffer
	go func() {
		native <- runHermesProcessSupervisorNative(
			config, []io.Reader{controlRead}, nil, nil, &proof, false,
		)
	}()

	pid := awaitSupervisorPIDFile(t, started)
	require.NoError(t, controlWrite.Close())

	require.NotZero(t, <-native)
	require.Equal(t, []byte{1}, proof.bytes())
	awaitSupervisorProcessGone(t, pid)
}

// TestSupervisorAuthorityAdoptionRefusesAnIncompleteInheritance proves the
// supervisor re-adopts the identity lock and the authority domain it was handed
// rather than assuming the parent passed them, and refuses when either is
// absent or when the standalone disposition they claim cannot be proven. The
// adopted pair is what binds this supervisor to one agent identity, so half an
// inheritance must never be accepted as a whole one.
func TestSupervisorAuthorityAdoptionRefusesAnIncompleteInheritance(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		omit       uintptr
		standalone bool
		want       string
	}{
		{
			name: "identity lock was not handed over",
			omit: supervisorIdentityLockFD,
			want: "inherited agent identity lock descriptor is unavailable",
		},
		{
			name: "authority domain was not handed over",
			omit: supervisorAuthorityFD,
			want: "inherited agent authority domain descriptor is unavailable",
		},
		{
			name:       "standalone disposition cannot be proven",
			standalone: true,
			want:       "bind inherited standalone state root",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			restoreLinuxSupervisorSeams(t)
			restoreAgentIdentityLockTestSeams(t)
			root := supervisorCovStageInheritedAuthority(t)

			config := supervisorTestConfig([]string{"/bin/true"})
			config.IdentityLock = true
			config.AuthorityDomain = true
			config.StandaloneAuthority = testCase.standalone
			config.Isolation.TestOnlyIdentityLockRoot = root
			if testCase.standalone {
				config.Isolation.StandaloneOwnerID = "supervisor-cov"
				config.Isolation.StandaloneStateRoot = "/var/lib/acp-go-hermes-supervisor-cov"
			}
			supervisorCovLeaseDescriptors(t, root, 11, testCase.omit)
			supervisorCommand = func(name string, _ ...string) *exec.Cmd {
				t.Errorf("supervisor without an adopted authority spawned %q", name)

				return exec.Command("/bin/true")
			}

			code, proof, diagnostic := supervisorCovRunGuardian(t, config, "", nil)
			require.Equal(t, 125, code)
			require.Equal(t, []byte{1}, proof)
			require.Contains(t, diagnostic, testCase.want)
		})
	}
}

// supervisorCovCaptureDiagnostics collects what a supervisor reports for its
// parent, so a refusal can be pinned to the stage that produced it.
func supervisorCovCaptureDiagnostics(t *testing.T) func() string {
	t.Helper()
	read, write, err := os.Pipe()
	require.NoError(t, err)
	stderr := os.Stderr
	os.Stderr = write

	return func() string {
		os.Stderr = stderr
		require.NoError(t, write.Close())
		reported, readErr := io.ReadAll(read)
		require.NoError(t, readErr)
		require.NoError(t, read.Close())

		return string(reported)
	}
}

func supervisorCovRunNative(
	config hermesSupervisorConfig,
	peer *os.File,
	status io.Writer,
	proof io.Writer,
	livenessProtocol bool,
) int {
	return runHermesProcessSupervisorNative(
		config, []io.Reader{strings.NewReader("")}, peer, status, proof, livenessProtocol,
	)
}

// supervisorCovStandaloneSeam hands the supervisor an authority it can hold and
// release without contending for the real agent identity directory.
func supervisorCovStandaloneSeam(t *testing.T, standalone *agentStandaloneIdentity) {
	t.Helper()
	supervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return standalone, nil
	}
}

// supervisorCovUnreleasableLease makes releasing the agent identity lease fail,
// which is the one failure the supervisor reports with its own exit status.
func supervisorCovUnreleasableLease(t *testing.T) {
	t.Helper()
	original := agentIdentityLockClose
	t.Cleanup(func() { agentIdentityLockClose = original })
	agentIdentityLockClose = func(file *os.File) error {
		_ = file.Close()

		return errors.New("lease was not released")
	}
}

// supervisorCovStageInheritedAuthority stages the trusted authority directory
// with the exact named locks and domain record a supervisor adopts.
func supervisorCovStageInheritedAuthority(t *testing.T) string {
	t.Helper()
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })

	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	payload, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "acp-go", "agent-identities", "domain.json"), append(payload, '\n'), 0o600,
	))

	return root
}

// supervisorCovLeaseDescriptors publishes the staged leases at the descriptor
// numbers the supervisor adopts them from, omitting one of them when asked.
func supervisorCovLeaseDescriptors(t *testing.T, root string, uid uint32, omit uintptr) {
	t.Helper()
	directory, err := os.Open(filepath.Join(root, "acp-go", "agent-identities"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })

	identity, err := openAgentStandaloneNamedLock(
		directory, strconv.FormatUint(uint64(uid), 10)+".lock", true,
		agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = identity.Close() })
	require.NoError(t, unix.Flock(int(identity.Fd()), unix.LOCK_EX|unix.LOCK_NB))

	domain, err := openAgentStandaloneNamedLock(
		directory, "domain.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = domain.Close() })
	require.NoError(t, unix.Flock(int(domain.Fd()), unix.LOCK_SH|unix.LOCK_NB))

	leases := map[uintptr]*os.File{}
	for fd, source := range map[uintptr]*os.File{
		supervisorIdentityLockFD: identity, supervisorAuthorityFD: domain,
	} {
		if fd == omit {
			continue
		}
		duplicate, duplicateErr := duplicateAgentIdentityLock(source)
		require.NoError(t, duplicateErr)
		leases[fd] = duplicate
	}
	supervisorNewFile = func(fd uintptr, _ string) *os.File { return leases[fd] }
}

// supervisorCovInherit publishes the descriptors a self-exec supervisor
// inherits: the sealed config image at 3, the parent's control channel at 4,
// the proof or status channel at 5, and, for the liveness mode, the completion
// proof at 8 and the guardian peer at 9.
func supervisorCovInherit(t *testing.T, payload string, omit uintptr) *supervisorCovInherited {
	t.Helper()
	if payload == "" {
		encoded, err := json.Marshal(supervisorTestConfig([]string{"/bin/true"}))
		require.NoError(t, err)
		payload = string(encoded)
	}

	inherited := &supervisorCovInherited{files: map[uintptr]*os.File{}, reads: map[uintptr]*os.File{}}
	config, configWrite, err := os.Pipe()
	require.NoError(t, err)
	_, err = io.WriteString(configWrite, payload)
	require.NoError(t, err)
	require.NoError(t, configWrite.Close())
	inherited.files[supervisorConfigFD] = config
	observer, err := syscall.Dup(int(config.Fd()))
	require.NoError(t, err)
	inherited.config = os.NewFile(uintptr(observer), "config-observer")
	t.Cleanup(func() { _ = inherited.config.Close() })

	control, controlWrite, err := os.Pipe()
	require.NoError(t, err)
	inherited.files[supervisorControlFD] = control
	t.Cleanup(func() { _ = controlWrite.Close() })

	for _, fd := range []uintptr{supervisorProofFD, 8, 9} {
		read, write, pipeErr := os.Pipe()
		require.NoError(t, pipeErr)
		inherited.reads[fd] = read
		if fd == 9 {
			inherited.files[fd] = read
			t.Cleanup(func() { _ = write.Close() })

			continue
		}
		inherited.files[fd] = write
	}
	delete(inherited.files, omit)
	supervisorNewFile = func(fd uintptr, _ string) *os.File { return inherited.files[fd] }

	return inherited
}

type supervisorCovInherited struct {
	files  map[uintptr]*os.File
	reads  map[uintptr]*os.File
	config *os.File
}

// configConsumed reports whether the supervisor read the config image it
// inherited, which distinguishes a refusal that happened before the config was
// consulted from one that happened after. It reads through an independent
// descriptor on the same channel, because the supervisor closes the one it was
// handed.
func (inherited *supervisorCovInherited) configConsumed(t *testing.T) bool {
	t.Helper()
	remaining, err := io.ReadAll(inherited.config)
	require.NoError(t, err)

	return len(remaining) == 0
}

func (inherited *supervisorCovInherited) read(t *testing.T, fd uintptr) string {
	t.Helper()
	// The supervisor closes some of the descriptors it was handed, so releasing
	// the publisher end here is best effort.
	_ = inherited.files[fd].Close()
	published, err := io.ReadAll(inherited.reads[fd])
	require.NoError(t, err)
	require.NoError(t, inherited.reads[fd].Close())

	return string(published)
}

// supervisorCovLostReadiness refuses the first publication on the status
// channel and accepts everything after it, which is the shape of a status
// channel whose reader went away exactly while readiness was being published.
type supervisorCovLostReadiness struct {
	refused   bool
	published strings.Builder
}

func (writer *supervisorCovLostReadiness) Write(value []byte) (int, error) {
	if !writer.refused {
		writer.refused = true

		return 0, os.ErrClosed
	}

	return writer.published.Write(value)
}

// supervisorCovBuffer collects what a supervisor publishes while its own
// goroutines are still retiring.
type supervisorCovBuffer struct {
	buffer synchronizedBuffer
}

func (collector *supervisorCovBuffer) Write(value []byte) (int, error) {
	return collector.buffer.Write(value)
}

func (collector *supervisorCovBuffer) bytes() []byte {
	return []byte(collector.buffer.String())
}
