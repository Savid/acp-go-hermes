//go:build darwin

//nolint:nlreturn // Failure-injection closures stay compact and local to each assertion.
package hermes

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type darwinTestReadCloser struct {
	io.Reader
	closeErr error
	closed   bool
}

type darwinTestWriteCloser struct {
	bytes.Buffer
	closeErr error
	closed   bool
}

type darwinSignalWaitObservation struct {
	signalTarget   int
	expectedTarget int
}

func (writer *darwinTestWriteCloser) Close() error {
	writer.closed = true

	return writer.closeErr
}

func (reader *darwinTestReadCloser) Close() error {
	reader.closed = true

	return reader.closeErr
}

func restoreDarwinLaunchSeams(t *testing.T) {
	t.Helper()
	executable, command := darwinLaunchExecutable, darwinLaunchCommand
	exit, execNative := darwinLaunchExit, darwinLaunchExec
	input, openFile := darwinLaunchInput, darwinLaunchOpenFile
	closeOnExec, createTemp := darwinLaunchCloseOnExec, darwinLaunchCreateTemp
	chmod, encode := darwinLaunchFileChmod, darwinLaunchEncodeConfig
	seek, remove := darwinLaunchFileSeek, darwinLaunchRemove
	pipe, statusWait := darwinLaunchPipe, darwinLaunchStatusWait
	randomRead, activate := containmentRandomRead, activateProcessContainmentRecord
	t.Cleanup(func() {
		darwinLaunchExecutable, darwinLaunchCommand = executable, command
		darwinLaunchExit, darwinLaunchExec = exit, execNative
		darwinLaunchInput, darwinLaunchOpenFile = input, openFile
		darwinLaunchCloseOnExec, darwinLaunchCreateTemp = closeOnExec, createTemp
		darwinLaunchFileChmod, darwinLaunchEncodeConfig = chmod, encode
		darwinLaunchFileSeek, darwinLaunchRemove = seek, remove
		darwinLaunchPipe, darwinLaunchStatusWait = pipe, statusWait
		containmentRandomRead, activateProcessContainmentRecord = randomRead, activate
	})
}

func observeDarwinGroupSignalBeforeWait(t *testing.T) <-chan darwinSignalWaitObservation {
	t.Helper()
	previousKill, previousWait := processKill, waitProcessCommand
	observed := make(chan darwinSignalWaitObservation, 1)
	signalTarget := 0
	processKill = func(pid int, signal syscall.Signal) error {
		if pid < 0 && signal == syscall.SIGTERM {
			signalTarget = pid
			return syscall.ESRCH
		}
		return previousKill(pid, signal)
	}
	waitProcessCommand = func(cmd *exec.Cmd) error {
		observed <- darwinSignalWaitObservation{signalTarget: signalTarget, expectedTarget: -cmd.Process.Pid}
		return previousWait(cmd)
	}
	t.Cleanup(func() {
		processKill, waitProcessCommand = previousKill, previousWait
	})

	return observed
}

func TestDarwinLaunchBootstrapProtocol(t *testing.T) {
	restoreDarwinLaunchSeams(t)
	configBytes, err := json.Marshal(darwinLaunchConfig{
		Path: "/native/hermes", Args: []string{"hermes", "--version"}, Env: []string{"A=B"},
	})
	require.NoError(t, err)

	config := &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}
	gate := &darwinTestReadCloser{Reader: bytes.NewReader([]byte{1})}
	var got darwinLaunchConfig
	darwinLaunchExec = func(path string, args, environment []string) error {
		got = darwinLaunchConfig{Path: path, Args: args, Env: environment}

		return nil
	}
	require.NoError(t, runDarwinLaunchBootstrapCore(config, gate))
	require.True(t, config.closed)
	require.True(t, gate.closed)
	require.Equal(t, darwinLaunchConfig{Path: "/native/hermes", Args: []string{"hermes", "--version"}, Env: []string{"A=B"}}, got)
	require.Error(t, runDarwinLaunchBootstrapCore(nil, nil))

	for _, test := range []struct {
		name   string
		config *darwinTestReadCloser
		gate   *darwinTestReadCloser
		exec   func(string, []string, []string) error
	}{
		{name: "decode", config: &darwinTestReadCloser{Reader: strings.NewReader("{")}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "incomplete", config: &darwinTestReadCloser{Reader: strings.NewReader(`{}`)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "gate eof", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("")}},
		{name: "gate byte", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("x")}},
		{name: "close", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes), closeErr: errors.New("close config")}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "exec", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}, exec: func(string, []string, []string) error { return errors.New("exec") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			darwinLaunchExec = test.exec
			if darwinLaunchExec == nil {
				darwinLaunchExec = func(string, []string, []string) error { return nil }
			}
			require.Error(t, runDarwinLaunchBootstrapCore(test.config, test.gate))
		})
	}
}

func TestDarwinLaunchBootstrapDispatch(t *testing.T) {
	restoreDarwinLaunchSeams(t)
	t.Setenv(darwinLaunchBootstrapEnv, darwinLaunchBootstrapMode)
	setTestIsolationBootstrapEnv(t)
	darwinLaunchExec = func(string, []string, []string) error { return nil }
	var exits []int
	darwinLaunchExit = func(code int) { exits = append(exits, code) }

	failedStatus := &darwinTestWriteCloser{}
	darwinLaunchInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return nil, nil, failedStatus, errors.New("input")
	}
	runDarwinLaunchBootstrap()
	require.True(t, failedStatus.closed)
	require.Contains(t, failedStatus.String(), "input")

	config, err := json.Marshal(darwinLaunchConfig{Path: "/native/hermes", Args: []string{"hermes"}})
	require.NoError(t, err)
	successStatus := &darwinTestWriteCloser{}
	darwinLaunchInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return io.NopCloser(bytes.NewReader(config)), io.NopCloser(strings.NewReader("\x01")), successStatus, nil
	}
	runDarwinLaunchBootstrap()
	require.True(t, successStatus.closed)
	require.Empty(t, successStatus.String())
	require.Equal(t, []int{1, 0}, exits)
}

func TestDarwinInheritedLaunchInputAndLaunchLifecycle(t *testing.T) {
	restoreDarwinLaunchSeams(t)
	darwinLaunchOpenFile = func(uintptr, string) *os.File { return nil }
	config, gate, status, err := inheritedDarwinLaunchInput()
	require.Error(t, err)
	require.Nil(t, config)
	require.Nil(t, gate)
	require.Nil(t, status)

	files := make([]*os.File, 3)
	for i := range files {
		files[i], err = os.CreateTemp(t.TempDir(), "inherited")
		require.NoError(t, err)
	}
	calls := 0
	darwinLaunchOpenFile = func(uintptr, string) *os.File {
		file := files[calls]
		calls++
		return file
	}
	closedOnExec := -1
	darwinLaunchCloseOnExec = func(fd int) { closedOnExec = fd }
	config, gate, status, err = inheritedDarwinLaunchInput()
	require.NoError(t, err)
	require.Equal(t, int(files[2].Fd()), closedOnExec)
	require.NoError(t, config.Close())
	require.NoError(t, gate.Close())
	require.NoError(t, status.Close())

	launch := &darwinLaunch{}
	require.Error(t, launch.releaseGate())
	readGate, writeGate, err := os.Pipe()
	require.NoError(t, err)
	launch.gate = writeGate
	require.NoError(t, launch.releaseGate())
	var release [1]byte
	_, err = io.ReadFull(readGate, release[:])
	require.NoError(t, err)
	require.Equal(t, byte(1), release[0])
	require.NoError(t, readGate.Close())

	inherited, err := os.CreateTemp(t.TempDir(), "inherited")
	require.NoError(t, err)
	_, closedGate, err := os.Pipe()
	require.NoError(t, err)
	_, closedStatus, err := os.Pipe()
	require.NoError(t, err)
	launch = &darwinLaunch{inherited: []*os.File{inherited}, gate: closedGate, status: closedStatus}
	launch.close()
	launch.close()
}

func TestDarwinLaunchPreparationAndStatusBranches(t *testing.T) {
	requireLaunchError := func(t *testing.T, setup func(), want string) {
		t.Helper()
		restoreDarwinLaunchSeams(t)
		setup()
		_, err := prepareDarwinLaunch(exec.Command("/usr/bin/true"), t.TempDir())
		require.ErrorContains(t, err, want)
	}

	_, err := prepareDarwinLaunch(&exec.Cmd{}, t.TempDir())
	require.ErrorContains(t, err, "command is incomplete")
	wantErr := errors.New("injected")
	t.Run("create", func(t *testing.T) {
		requireLaunchError(t, func() { darwinLaunchCreateTemp = func(string, string) (*os.File, error) { return nil, wantErr } }, "create Darwin native launch config")
	})
	t.Run("chmod", func(t *testing.T) {
		requireLaunchError(t, func() { darwinLaunchFileChmod = func(*os.File, os.FileMode) error { return wantErr } }, "injected")
	})
	t.Run("encode", func(t *testing.T) {
		requireLaunchError(t, func() { darwinLaunchEncodeConfig = func(*os.File, darwinLaunchConfig) error { return wantErr } }, "injected")
	})
	t.Run("seek", func(t *testing.T) {
		requireLaunchError(t, func() { darwinLaunchFileSeek = func(*os.File, int64, int) (int64, error) { return 0, wantErr } }, "injected")
	})
	t.Run("unlink", func(t *testing.T) {
		requireLaunchError(t, func() { darwinLaunchRemove = func(string) error { return wantErr } }, "injected")
	})
	t.Run("gate pipe", func(t *testing.T) {
		requireLaunchError(t, func() { darwinLaunchPipe = func() (*os.File, *os.File, error) { return nil, nil, wantErr } }, "injected")
	})
	t.Run("status pipe", func(t *testing.T) {
		requireLaunchError(t, func() {
			calls := 0
			darwinLaunchPipe = func() (*os.File, *os.File, error) {
				calls++
				if calls == 2 {
					return nil, nil, wantErr
				}
				return os.Pipe()
			}
		}, "injected")
	})
	t.Run("executable", func(t *testing.T) {
		requireLaunchError(t, func() { darwinLaunchExecutable = func() (string, error) { return "", wantErr } }, "injected")
	})

	t.Run("success and explicit environment", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		native := exec.Command("/usr/bin/true")
		native.Env = []string{"HERMES_LAUNCH_TEST=present"}
		launch, err := prepareDarwinLaunch(native, t.TempDir())
		require.NoError(t, err)
		require.True(t, launch.cmd.SysProcAttr.Setpgid)
		require.Equal(t, darwinPipeWait, launch.cmd.WaitDelay)
		require.Len(t, launch.inherited, 3)
		var config darwinLaunchConfig
		require.NoError(t, json.NewDecoder(launch.inherited[0]).Decode(&config))
		require.Contains(t, config.Env, "HERMES_LAUNCH_TEST=present")
		_, statErr := os.Stat(launch.inherited[0].Name())
		require.ErrorIs(t, statErr, os.ErrNotExist)
		launch.close()
	})

	require.Error(t, awaitDarwinLaunchExec(nil))
	t.Run("deadline", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "status")
		require.NoError(t, err)
		require.Error(t, awaitDarwinLaunchExec(file))
	})
	t.Run("timeout", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		darwinLaunchStatusWait = time.Millisecond
		read, write, err := os.Pipe()
		require.NoError(t, err)
		defer write.Close()
		require.Error(t, awaitDarwinLaunchExec(read))
	})
	t.Run("limit", func(t *testing.T) {
		read, write, err := os.Pipe()
		require.NoError(t, err)
		go func() { _, _ = write.Write(bytes.Repeat([]byte{'x'}, 4097)); _ = write.Close() }()
		require.ErrorContains(t, awaitDarwinLaunchExec(read), "status limit")
	})
	t.Run("message", func(t *testing.T) {
		read, write, err := os.Pipe()
		require.NoError(t, err)
		go func() { _, _ = io.WriteString(write, "failed\n"); _ = write.Close() }()
		require.ErrorContains(t, awaitDarwinLaunchExec(read), "failed")
	})
	t.Run("success", func(t *testing.T) {
		read, write, err := os.Pipe()
		require.NoError(t, err)
		require.NoError(t, write.Close())
		require.NoError(t, awaitDarwinLaunchExec(read))
	})
}

func TestDarwinContainmentMiscellaneousBranches(t *testing.T) {
	require.Error(t, validateProcessContainment(false))
	_, err := Start(t.Context(), ProcessOptions{})
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

	restoreDarwinLaunchSeams(t)
	originalRandom := containmentRandomRead
	containmentRandomRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	_, err = newContainmentRuntimeID()
	require.Error(t, err)
	containmentRandomRead = originalRandom

	t.Setenv("GORACE", "halt_on_error=1")
	require.NotContains(t, darwinBootstrapEnvironment(), "GORACE=halt_on_error=1")
	marked := withDarwinContainmentMarkers(nil, "id", "/root")
	require.Contains(t, marked, envRuntimeID+"=id")
	require.Contains(t, marked, envScratchRoot+"=/root")
	marked = withDarwinContainmentMarkers([]string{
		"A=B", envRuntimeID + "=old", envScratchRoot + "=/old", darwinLaunchBootstrapEnv + "=1",
	}, "new", "/new")
	require.Equal(t, []string{"A=B", envRuntimeID + "=new", envScratchRoot + "=/new"}, marked)
	require.Equal(t, []string{"A=B"}, scrubDarwinInternalEnvironment([]string{"A=B", darwinLaunchBootstrapEnv + "=1"}))

	previousKill := processKill
	t.Cleanup(func() { processKill = previousKill })
	processKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	proof := make(chan bool, 1)
	proof <- true
	require.NoError(t, (&processContainment{processGroupID: 10, proof: proof}).completeAuthoritative(time.Second))
	proof = make(chan bool, 1)
	proof <- true
	processKill = func(int, syscall.Signal) error { return errors.New("probe") }
	require.Error(t, (&processContainment{processGroupID: 10, proof: proof}).completeAuthoritative(time.Second))
	proof = make(chan bool, 1)
	proof <- false
	require.Error(t, (&processContainment{processGroupID: 10, proof: proof}).completeAuthoritative(time.Second))
	require.Error(t, (&processContainment{processGroupID: 10, proof: make(chan bool)}).completeAuthoritative(time.Nanosecond))
	count, authoritative := (&processContainment{descendantCountFn: func() (int, bool) { return 3, true }}).descendantCount()
	require.Equal(t, 3, count)
	require.True(t, authoritative)
	require.NoError(t, (&processContainment{closeFn: func() error { return nil }}).close())
	require.NoError(t, (*processContainment)(nil).close())
	require.ErrorIs(t, (*directChildWait)(nil).awaitReaped(time.Second), ErrProcessContainmentIncomplete)
	require.ErrorIs(t, (&directChildWait{done: make(chan struct{})}).awaitReaped(time.Nanosecond), ErrProcessContainmentIncomplete)

	restoreProcessSeams(t)
	markExecutableProbed("/usr/bin/true")
	listenTCP = func(string, string) (net.Listener, error) { return nil, errors.New("listen") }
	_, err = Start(t.Context(), ProcessOptions{
		ExecutablePath: "/usr/bin/true", Home: t.TempDir(), DarwinBestEffortContainment: true,
		Isolation: testProcessIsolation(),
	})
	require.ErrorContains(t, err, "listen")
	processKill = previousKill

	script := filepath.Join(t.TempDir(), "slow-hermes")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nsleep 2\n"), 0o700))
	probeCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = ensureExecutableVersion(probeCtx, script, ProcessOptions{
		ScratchParent: t.TempDir(), DarwinBestEffortContainment: true,
		Isolation:                 testProcessIsolation(),
		AcquireDiscoveryResources: testDiscoveryResourceAdmission,
		RetainDiscoveryRoot:       func(string, error) {},
	})
	require.ErrorIs(t, err, context.Canceled)

	versionScript := filepath.Join(t.TempDir(), "version-hermes")
	require.NoError(t, os.WriteFile(versionScript, []byte("#!/bin/sh\nprintf 'Hermes Agent v0.19.0\\n'\n"), 0o700))
	nativeReleases, scratchReleases := 0, 0
	needed, err := ensureExecutableVersion(t.Context(), versionScript, ProcessOptions{
		ScratchParent: t.TempDir(), DarwinBestEffortContainment: true,
		Isolation: testProcessIsolation(),
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
			return func() { nativeReleases++ }, func() { scratchReleases++ }, nil
		},
		RetainDiscoveryRoot: func(string, error) {},
	})
	require.NoError(t, err)
	require.True(t, needed)
	require.Equal(t, 1, nativeReleases)
	require.Equal(t, 1, scratchReleases)

	wantAdmissionErr := errors.New("discovery admission rejected")
	_, err = ensureExecutableVersion(t.Context(), script+"-admission", ProcessOptions{
		Isolation: testProcessIsolation(),
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
			return nil, nil, wantAdmissionErr
		},
		RetainDiscoveryRoot: func(string, error) {},
	})
	require.ErrorIs(t, err, wantAdmissionErr)

	restoreProcessSeams(t)
	mkdirTemp = func(string, string) (string, error) { return "", errors.New("probe generation failed") }
	nativeReleases, scratchReleases = 0, 0
	_, err = ensureExecutableVersion(t.Context(), script+"-generation", ProcessOptions{
		Isolation: testProcessIsolation(),
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
			return func() { nativeReleases++ }, func() { scratchReleases++ }, nil
		},
		RetainDiscoveryRoot: func(string, error) {},
	})
	require.ErrorContains(t, err, "probe generation failed")
	require.Equal(t, 1, nativeReleases)
	require.Equal(t, 1, scratchReleases)
}

func TestDarwinVersionDiscoveryRequiresCompleteResourceCallbacks(t *testing.T) {
	script := filepath.Join(t.TempDir(), "hermes-version")
	_, err := ensureExecutableVersion(t.Context(), script, ProcessOptions{})
	require.ErrorContains(t, err, "resource callbacks are required")

	nativeReleases, scratchReleases := 0, 0
	for _, acquire := range []func(context.Context) (func(), func(), error){
		func(context.Context) (func(), func(), error) {
			return nil, func() { scratchReleases++ }, nil
		},
		func(context.Context) (func(), func(), error) {
			return func() { nativeReleases++ }, nil, nil
		},
	} {
		_, err = ensureExecutableVersion(t.Context(), script, ProcessOptions{
			AcquireDiscoveryResources: acquire,
			RetainDiscoveryRoot:       func(string, error) {},
		})
		require.ErrorContains(t, err, "nil release")
	}
	require.Equal(t, 1, nativeReleases)
	require.Equal(t, 1, scratchReleases)
}

func TestDarwinProcessIsolationFailureBranches(t *testing.T) {
	_, err := Start(t.Context(), ProcessOptions{DarwinBestEffortContainment: true})
	require.ErrorContains(t, err, "process isolation")

	_, err = startUnixContainedProcess(exec.Command("/usr/bin/true"), ContainmentSpec{DarwinBestEffort: true})
	require.ErrorContains(t, err, "isolation")

	_, err = prepareDarwinLaunch(exec.Command("/usr/bin/true"), t.TempDir(), nil)
	require.ErrorContains(t, err, "process isolation")

	originalGroups := processIsolationGetgroups
	t.Cleanup(func() { processIsolationGetgroups = originalGroups })
	processIsolationGetgroups = func() ([]int, error) { return []int{os.Getegid(), os.Getegid() + 1}, nil }
	_, err = prepareDarwinLaunch(exec.Command("/usr/bin/true"), t.TempDir(), &ProcessIsolation{
		UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), BaseEnvironment: map[string]string{},
	})
	require.ErrorContains(t, err, "supplementary group")
}

func TestDarwinVersionDiscoveryIsolationEnvironmentFailures(t *testing.T) {
	for _, removeErr := range []error{nil, errors.New("remove failed")} {
		t.Run(fmt.Sprint(removeErr), func(t *testing.T) {
			restoreProcessSeams(t)
			originalRemoveAll := removeAll
			t.Cleanup(func() { removeAll = originalRemoveAll })
			if removeErr != nil {
				removeAll = func(string) error { return removeErr }
			}
			nativeReleases, scratchReleases := 0, 0
			_, err := ensureExecutableVersion(t.Context(), filepath.Join(t.TempDir(), "unprobed"), ProcessOptions{
				Isolation: &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"BAD=KEY": "x"}},
				AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
					return func() { nativeReleases++ }, func() { scratchReleases++ }, nil
				},
				RetainDiscoveryRoot: func(string, error) {},
			})
			require.Error(t, err)
			require.Equal(t, 1, nativeReleases)
			if removeErr == nil {
				require.Equal(t, 1, scratchReleases)
			} else {
				require.Zero(t, scratchReleases)
				require.ErrorIs(t, err, removeErr)
			}
		})
	}
}

func TestDarwinVersionDiscoveryRetainsIncompleteGenerationAndAdmissions(t *testing.T) {
	restoreDarwinLaunchSeams(t)
	restoreProcessSeams(t)

	script := filepath.Join(t.TempDir(), "hermes-version")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nprintf 'Hermes Agent v0.19.0\\n'\n"), 0o700))
	parent := t.TempDir()
	nativeReleases, scratchReleases := 0, 0
	retainedRoot := ""
	previousGetpgid := processGetpgid
	t.Cleanup(func() { processGetpgid = previousGetpgid })
	processGetpgid = func(int) (int, error) { return 0, errors.New("group identity unavailable") }

	_, err := ensureExecutableVersion(t.Context(), script, ProcessOptions{
		ScratchParent: parent, DarwinBestEffortContainment: true,
		Isolation: testProcessIsolation(),
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) {
			return func() { nativeReleases++ }, func() { scratchReleases++ }, nil
		},
		RetainDiscoveryRoot: func(root string, _ error) { retainedRoot = root },
	})
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	require.NotEmpty(t, retainedRoot)
	require.DirExists(t, retainedRoot)
	require.Zero(t, nativeReleases)
	require.Zero(t, scratchReleases)
	require.NoError(t, os.RemoveAll(retainedRoot))
}

func TestDarwinStrictEnvironmentBoundariesAndGenerationFailures(t *testing.T) {
	raw := make([]byte, 4, 20)
	binary.NativeEndian.PutUint32(raw, 1)
	raw = append(raw, []byte("/bin/test\x00test\x00\x00")...)
	_, _, err := parseDarwinProcArgs(raw)
	require.ErrorContains(t, err, "boundary is ambiguous")

	raw = make([]byte, 4, 35)
	binary.NativeEndian.PutUint32(raw, 1)
	raw = append(raw, []byte("/bin/test\x00test\x00A=B\x00\x00opaque\xfftail")...)
	argv, environment, err := parseDarwinProcArgs(raw)
	require.NoError(t, err)
	require.Equal(t, []string{"test"}, argv)
	require.Equal(t, []string{"A=B"}, environment)
	raw = make([]byte, 4, 23)
	binary.NativeEndian.PutUint32(raw, 1)
	raw = append(raw, []byte("/bin/test\x00test\x00A=B\x00")...)
	_, _, err = parseDarwinProcArgs(raw)
	require.ErrorContains(t, err, "environment is incomplete")

	raw = make([]byte, 4, 22)
	binary.LittleEndian.PutUint32(raw, 1)
	raw = append(raw, []byte("/bin/test\x00test\x00A=B")...)
	_, err = darwinProcessEnvironment(raw)
	require.ErrorContains(t, err, "environment is incomplete")

	previousTemp, previousRemove, previousMkdir := generationMkdirTemp, generationRemoveAll, xdgMkdirAll
	t.Cleanup(func() {
		generationMkdirTemp, generationRemoveAll, xdgMkdirAll = previousTemp, previousRemove, previousMkdir
	})
	generationMkdirTemp = func(string, string) (string, error) { return "", errors.New("mktemp") }
	_, err = CreateGenerationXDGDirs(t.TempDir())
	require.ErrorContains(t, err, "mktemp")

	root := t.TempDir()
	generationMkdirTemp = func(string, string) (string, error) { return root, nil }
	removed := ""
	generationRemoveAll = func(path string) error { removed = path; return nil }
	xdgMkdirAll = func(path string, _ os.FileMode) error {
		if strings.HasSuffix(path, "data") {
			return errors.New("mkdir")
		}
		return nil
	}
	_, err = CreateGenerationXDGDirs(t.TempDir())
	require.ErrorContains(t, err, "mkdir")
	require.Equal(t, root, removed)
	require.ErrorContains(t, ensureXDGDirs(XDGDirs{}), "empty")
}

func reapedDarwinTestWait() *directChildWait {
	done := make(chan struct{})
	close(done)
	return &directChildWait{done: done, start: make(chan struct{})}
}

func pausedDarwinTestChild(t *testing.T) (*exec.Cmd, *directChildWait) {
	t.Helper()
	cmd := exec.Command("/usr/bin/true")
	require.NoError(t, cmd.Start())
	direct := installDirectChildWait(cmd, true)
	t.Cleanup(func() {
		direct.begin()
		_ = direct.awaitReaped(time.Second)
	})
	time.Sleep(20 * time.Millisecond)
	return cmd, direct
}

func TestDarwinVanishedLeaderAndBoundaryBranches(t *testing.T) {
	previousKill := processKill
	previousDirectKill := darwinDirectProcessKill
	t.Cleanup(func() {
		processKill = previousKill
		darwinDirectProcessKill = previousDirectKill
	})

	t.Run("group absent", func(t *testing.T) {
		cmd, direct := pausedDarwinTestChild(t)
		processKill = func(int, syscall.Signal) error { return syscall.ESRCH }
		_, err := handleVanishedDarwinLeader(cmd, &darwinLaunch{cmd: cmd}, direct, cmd.Process.Pid)
		require.ErrorContains(t, err, "exited before containment validation")
	})

	t.Run("probe failure", func(t *testing.T) {
		cmd, direct := pausedDarwinTestChild(t)
		processKill = func(int, syscall.Signal) error { return errors.New("probe") }
		_, err := handleVanishedDarwinLeader(cmd, &darwinLaunch{cmd: cmd}, direct, cmd.Process.Pid)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	})

	t.Run("group remains observable", func(t *testing.T) {
		cmd, direct := pausedDarwinTestChild(t)
		calls := 0
		processKill = func(int, syscall.Signal) error {
			calls++
			if calls == 1 {
				return syscall.EPERM
			}
			return syscall.ESRCH
		}
		_, err := handleVanishedDarwinLeader(cmd, &darwinLaunch{cmd: cmd}, direct, cmd.Process.Pid)
		require.ErrorContains(t, err, "native group remained observable")
		require.NotErrorIs(t, err, ErrProcessContainmentIncomplete)
	})

	t.Run("boundary outcomes", func(t *testing.T) {
		direct := reapedDarwinTestWait()
		tree := newDarwinContainment(1234, &os.Process{Pid: 1234}, direct, containmentRecord{})
		require.NoError(t, finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(time.Second), true, nil))
		require.ErrorIs(t, finishDarwinBoundary(tree, containmentRecord{path: filepath.Join(t.TempDir(), "missing", "record.json")}, time.Now().Add(time.Second), true, nil), ErrProcessContainmentIncomplete)
		require.ErrorIs(t, finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(time.Second), false, errors.New("signal")), ErrProcessContainmentIncomplete)

		processKill = func(int, syscall.Signal) error { return errors.New("inspect") }
		require.ErrorIs(t, finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(time.Second), false, nil), ErrProcessContainmentIncomplete)

		processKill = func(int, syscall.Signal) error { return syscall.ESRCH }
		require.NoError(t, finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(time.Second), false, nil))

		processKill = func(_ int, signal syscall.Signal) error {
			if signal == syscall.SIGKILL {
				return syscall.ESRCH
			}
			return nil
		}
		require.NoError(t, finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(550*time.Millisecond), false, nil))

		killSeen := false
		processKill = func(_ int, signal syscall.Signal) error {
			if signal == syscall.SIGKILL {
				killSeen = true
				return nil
			}
			if killSeen {
				return syscall.ESRCH
			}
			return nil
		}
		require.NoError(t, finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(550*time.Millisecond), false, nil))

		processKill = func(int, syscall.Signal) error { return nil }
		require.ErrorIs(t, finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(30*time.Millisecond), false, nil), ErrProcessContainmentIncomplete)
	})

	t.Run("unexpected group syscalls force direct child through retained handle", func(t *testing.T) {
		groupErr := errors.New("group syscall")
		for _, test := range []struct {
			name      string
			initial   error
			deadline  time.Duration
			groupKill func(int, syscall.Signal) error
		}{
			{
				name:     "term",
				initial:  groupErr,
				deadline: time.Second,
				groupKill: func(int, syscall.Signal) error {
					t.Fatal("TERM failure fallback unexpectedly retried the process group")

					return nil
				},
			},
			{
				name:     "probe",
				deadline: time.Second,
				groupKill: func(_ int, signal syscall.Signal) error {
					if signal != 0 {
						t.Fatalf("probe failure signal = %v", signal)
					}

					return groupErr
				},
			},
			{
				name:     "kill",
				deadline: 30 * time.Millisecond,
				groupKill: func(_ int, signal syscall.Signal) error {
					if signal == syscall.SIGKILL {
						return groupErr
					}

					return nil
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				direct := &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
				process := &os.Process{Pid: 4321}
				tree := newDarwinContainment(4321, process, direct, containmentRecord{})
				processKill = test.groupKill
				killCalls := 0
				darwinDirectProcessKill = func(got *os.Process) error {
					require.Same(t, process, got)
					killCalls++
					close(direct.done)

					return nil
				}

				err := finishDarwinBoundary(tree, containmentRecord{}, time.Now().Add(test.deadline), false, test.initial)
				require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
				require.ErrorIs(t, err, groupErr)
				require.Equal(t, 1, killCalls)
			})
		}

		direct := reapedDarwinTestWait()
		process := &os.Process{Pid: 9876}
		darwinDirectProcessKill = func(*os.Process) error {
			t.Fatal("completed direct-child waiter must suppress a late stateful kill")

			return nil
		}
		err := finishDarwinBoundary(newDarwinContainment(9876, process, direct, containmentRecord{}), containmentRecord{}, time.Now().Add(time.Second), false, groupErr)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	})

	t.Run("stateful direct-child fallback branches", func(t *testing.T) {
		require.ErrorContains(t, forceKillDarwinDirectChild(nil, time.Now()), "handle is unavailable")

		direct := &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
		process := &os.Process{Pid: 1111}
		darwinDirectProcessKill = func(*os.Process) error {
			close(direct.done)

			return os.ErrProcessDone
		}
		require.NoError(t, forceKillDarwinDirectChild(newDarwinContainment(1111, process, direct, containmentRecord{}), time.Now().Add(time.Second)))

		direct = &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
		killErr := errors.New("direct kill failed")
		darwinDirectProcessKill = func(*os.Process) error { return killErr }
		err := forceKillDarwinDirectChild(newDarwinContainment(1111, process, direct, containmentRecord{}), time.Now())
		require.ErrorIs(t, err, killErr)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

		direct = &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
		err = forceKillDarwinDirectChild(newDarwinContainment(1111, process, direct, containmentRecord{}), time.Now().Add(time.Millisecond))
		require.ErrorIs(t, err, killErr)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	})
}

func TestDarwinStartContainmentFailureBranches(t *testing.T) {
	restoreDarwinLaunchSeams(t)
	_, err := startUnixContainedProcess(exec.Command("/usr/bin/true"), ContainmentSpec{})
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

	originalRandom := containmentRandomRead
	containmentRandomRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	_, err = startUnixContainedProcess(exec.Command("/usr/bin/true"), darwinTestContainmentSpec(t))
	require.ErrorContains(t, err, "identity")
	containmentRandomRead = originalRandom

	_, err = startUnixContainedProcess(&exec.Cmd{}, darwinTestContainmentSpec(t))
	require.ErrorContains(t, err, "command is incomplete")
	invalidSpec := ContainmentSpec{DarwinBestEffort: true, ScratchParent: t.TempDir(), GenerationRoot: filepath.Join(t.TempDir(), "outside"), LifecycleKind: "session", Isolation: testProcessIsolation()}
	_, err = startUnixContainedProcess(exec.Command("/usr/bin/true"), invalidSpec)
	require.ErrorContains(t, err, "prepare Darwin containment record")

	t.Run("helper start", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		darwinLaunchCommand = func(string, ...string) *exec.Cmd { return exec.Command(filepath.Join(t.TempDir(), "missing")) }
		_, err := startUnixContainedProcess(exec.Command("/usr/bin/true"), darwinTestContainmentSpec(t))
		require.Error(t, err)
	})

	t.Run("pgid validation", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		previousGetpgid := processGetpgid
		t.Cleanup(func() { processGetpgid = previousGetpgid })
		processGetpgid = func(int) (int, error) { return 0, errors.New("pgid") }
		_, err := startUnixContainedProcess(exec.Command("/usr/bin/true"), darwinTestContainmentSpec(t))
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	})

	t.Run("vanished pgid", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		previousGetpgid, previousKill := processGetpgid, processKill
		t.Cleanup(func() { processGetpgid, processKill = previousGetpgid, previousKill })
		processGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
		processKill = func(int, syscall.Signal) error { return syscall.ESRCH }
		_, err := startUnixContainedProcess(exec.Command("/usr/bin/true"), darwinTestContainmentSpec(t))
		require.Error(t, err)
	})

	t.Run("activation", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		observed := observeDarwinGroupSignalBeforeWait(t)
		activateProcessContainmentRecord = func(containmentRecord, int, int) error { return errors.New("activate") }
		_, err := startUnixContainedProcess(exec.Command("/usr/bin/true"), darwinTestContainmentSpec(t))
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
		observation := <-observed
		require.Equal(t, observation.expectedTarget, observation.signalTarget)
	})

	t.Run("gate", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		observed := observeDarwinGroupSignalBeforeWait(t)
		calls := 0
		darwinLaunchPipe = func() (*os.File, *os.File, error) {
			calls++
			read, write, err := os.Pipe()
			if calls == 1 && err == nil {
				require.NoError(t, write.Close())
			}
			return read, write, err
		}
		_, err := startUnixContainedProcess(exec.Command("/usr/bin/true"), darwinTestContainmentSpec(t))
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
		observation := <-observed
		require.Equal(t, observation.expectedTarget, observation.signalTarget)
	})

	t.Run("status", func(t *testing.T) {
		restoreDarwinLaunchSeams(t)
		_, err := startUnixContainedProcess(exec.Command(filepath.Join(t.TempDir(), "missing")), darwinTestContainmentSpec(t))
		require.ErrorContains(t, err, "exec native Hermes command")
	})
}

// TestDarwinDirectKillEndsTheProcessItIsGiven covers the default direct-child
// kill. Every other assertion in this file replaces it, and the handles they
// build carry invented pids, so the real one has to be exercised against a
// child the test started itself — signalling a fabricated pid would reach a
// process this repo does not own. It is the last resort that removes a native
// child the process-group boundary failed to take, which is the one path where
// a leaked Hermes process is all that is left.
func TestDarwinDirectKillEndsTheProcessItIsGiven(t *testing.T) {
	child := exec.Command("/bin/sleep", "30")
	require.NoError(t, child.Start())
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })

	require.NoError(t, darwinDirectProcessKill(child.Process))

	var exitErr *exec.ExitError
	require.ErrorAs(t, child.Wait(), &exitErr)
	require.False(t, exitErr.Success())
}
