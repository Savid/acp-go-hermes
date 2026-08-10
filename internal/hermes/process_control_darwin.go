//go:build darwin

//nolint:wsl_v5,nlreturn // Launch and cleanup ordering is security-sensitive and intentionally contiguous.
package hermes

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// processContainment holds the boundary Darwin's best-effort backend
// establishes around one native root. The captured direct child and the
// memoized cleanup outcome belong to that backend alone.
type processContainment struct {
	processGroupID    int
	process           *os.Process
	terminateFn       func() error
	killFn            func() error
	proof             <-chan bool
	closeFn           func() error
	descendantCountFn func() (int, bool)
	direct            *directChildWait
	completeFn        func(time.Duration) error
	cleanupOnce       sync.Once
	cleanupErr        error
}

const (
	darwinLaunchBootstrapEnv  = "ACP_GO_HERMES_INTERNAL_DARWIN_LAUNCH"
	darwinLaunchBootstrapMode = "1"
	darwinPipeWait            = 500 * time.Millisecond
	darwinContainmentDeadline = 5 * time.Second
	darwinTermGrace           = 500 * time.Millisecond
	darwinPrivateEnvPrefix    = "ACP_" + "GO_HERMES_INTERNAL_"
)

type darwinLaunchConfig struct {
	Path string   `json:"path"`
	Args []string `json:"args"`
	Env  []string `json:"env"`
}

var (
	darwinLaunchExecutable           = os.Executable
	darwinLaunchCommand              = exec.Command
	darwinLaunchExec                 = syscall.Exec
	darwinLaunchExit                 = os.Exit
	darwinLaunchInput                = inheritedDarwinLaunchInput
	darwinLaunchOpenFile             = os.NewFile
	darwinLaunchFcntl                = unix.FcntlInt
	darwinLaunchCloseOnExec          = setDarwinLaunchCloseOnExec
	darwinLaunchCreateTemp           = os.CreateTemp
	darwinLaunchFileChmod            = func(file *os.File, mode os.FileMode) error { return file.Chmod(mode) }
	darwinLaunchEncodeConfig         = func(file *os.File, config darwinLaunchConfig) error { return json.NewEncoder(file).Encode(config) }
	darwinLaunchFileSeek             = func(file *os.File, offset int64, whence int) (int64, error) { return file.Seek(offset, whence) }
	darwinLaunchRemove               = os.Remove
	darwinLaunchPipe                 = os.Pipe
	darwinLaunchStatusWait           = darwinContainmentDeadline
	containmentRandomRead            = rand.Read
	activateProcessContainmentRecord = activateContainmentRecord
	darwinDirectProcessKill          = func(process *os.Process) error { return process.Kill() }
)

func init() { runDarwinLaunchBootstrap() }

func runDarwinLaunchBootstrap() {
	if os.Getenv(darwinLaunchBootstrapEnv) != darwinLaunchBootstrapMode {
		return
	}
	// There is no inherited identity to verify here. Darwin runs the native
	// harness as the identity this adapter already holds, and the explicit
	// hardened policy that would name a different one is refused on this
	// platform before any launch is prepared.
	configFile, gate, status, err := darwinLaunchInput()
	if err == nil {
		err = runDarwinLaunchBootstrapCore(configFile, gate)
	}
	if configFile != nil {
		_ = configFile.Close()
	}
	if gate != nil {
		_ = gate.Close()
	}
	if status != nil {
		if err != nil {
			_, _ = fmt.Fprintln(status, err)
		}
		_ = status.Close()
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "acp-go-hermes Darwin launch bootstrap:", err)
		darwinLaunchExit(1)
		return
	}
	darwinLaunchExit(0)
}

func inheritedDarwinLaunchInput() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	configFile := darwinLaunchOpenFile(3, "hermes-darwin-launch-config")
	gate := darwinLaunchOpenFile(4, "hermes-darwin-launch-gate")
	status := darwinLaunchOpenFile(5, "hermes-darwin-launch-status")
	if configFile == nil || gate == nil || status == nil {
		return configFile, gate, status, errors.New("darwin native launch descriptors are unavailable")
	}
	if err := darwinLaunchCloseOnExec(int(status.Fd())); err != nil {
		return configFile, gate, status, err
	}
	return configFile, gate, status, nil
}

func setDarwinLaunchCloseOnExec(fd int) error {
	flags, err := darwinLaunchFcntl(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("read inherited Hermes Darwin launch descriptor flags: %w", err)
	}
	if _, err = darwinLaunchFcntl(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("protect inherited Hermes Darwin launch descriptor from exec: %w", err)
	}
	return nil
}

func runDarwinLaunchBootstrapCore(configInput io.ReadCloser, gate io.ReadCloser) error {
	if configInput == nil || gate == nil {
		return errors.New("darwin native launch descriptors are unavailable")
	}
	var config darwinLaunchConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return fmt.Errorf("decode native launch config: %w", err)
	}
	if config.Path == "" || len(config.Args) == 0 {
		return errors.New("native launch config is incomplete")
	}
	var release [1]byte
	if _, err := io.ReadFull(gate, release[:]); err != nil || release[0] != 1 {
		return errors.Join(errors.New("native launch was not released after containment validation"), err)
	}
	if err := errors.Join(configInput.Close(), gate.Close()); err != nil {
		return fmt.Errorf("close Darwin native launch descriptors: %w", err)
	}
	if err := darwinLaunchExec(config.Path, config.Args, config.Env); err != nil {
		return fmt.Errorf("exec native Hermes command: %w", err)
	}
	return nil
}

type darwinLaunch struct {
	cmd         *exec.Cmd
	inherited   []*os.File
	gate        *os.File
	status      *os.File
	containment containmentRecord
}

func (launch *darwinLaunch) close() {
	for _, file := range launch.inherited {
		_ = file.Close()
	}
	launch.inherited = nil
	if launch.gate != nil {
		_ = launch.gate.Close()
		launch.gate = nil
	}
	if launch.status != nil {
		_ = launch.status.Close()
		launch.status = nil
	}
}

func (launch *darwinLaunch) releaseGate() error {
	if launch.gate == nil {
		return errors.New("darwin launch gate is unavailable")
	}
	gate := launch.gate
	launch.gate = nil
	_, writeErr := gate.Write([]byte{1})
	return errors.Join(writeErr, gate.Close())
}

func startUnixContainedProcess(target *exec.Cmd, spec ContainmentSpec) (*processContainment, error) {
	// An explicit hardened policy is Linux-only and must never be downgraded to
	// this backend, so it is refused before anything is prepared or spawned.
	if spec.Isolation != nil {
		return nil, errors.New("explicit process isolation is supported only on linux, not darwin")
	}
	if !spec.DarwinBestEffort {
		return nil, fmt.Errorf("%w: Darwin containment is unavailable without explicit best-effort opt-in", ErrProcessContainmentIncomplete)
	}
	runtimeID, err := newContainmentRuntimeID()
	if err != nil {
		return nil, fmt.Errorf("create Darwin containment identity: %w", err)
	}
	spec.RuntimeID = runtimeID
	target.Env = withDarwinContainmentMarkers(target.Env, runtimeID, spec.GenerationRoot)
	record, err := prepareContainmentRecord(spec)
	if err != nil {
		return nil, fmt.Errorf("prepare Darwin containment record: %w", err)
	}
	launch, err := prepareDarwinLaunch(target, spec.GenerationRoot)
	if err != nil {
		return nil, errors.Join(err, completeContainmentRecord(record, containmentStateAbsent))
	}
	launch.containment = record
	*target = *launch.cmd
	launch.cmd = target
	if err := launch.cmd.Start(); err != nil {
		launch.close()
		return nil, errors.Join(err, completeContainmentRecord(record, containmentStateAbsent))
	}
	for _, file := range launch.inherited {
		_ = file.Close()
	}
	launch.inherited = nil
	direct := installDirectChildWait(launch.cmd, true)
	pid := launch.cmd.Process.Pid
	pgid, pgidErr := processGetpgid(pid)
	if errors.Is(pgidErr, syscall.ESRCH) {
		return handleVanishedDarwinLeader(target, launch, direct, pid)
	}
	if pgidErr != nil || pgid != pid {
		launch.close()
		_ = launch.cmd.Process.Signal(syscall.SIGKILL)
		direct.begin()
		waitErr := direct.awaitReaped(darwinContainmentDeadline)
		recordErr := completeContainmentRecord(record, containmentStateFailed)
		return nil, errors.Join(fmt.Errorf("%w: validate native process-group leader: %v", ErrProcessContainmentIncomplete, pgidErr), waitErr, recordErr)
	}

	tree := newDarwinContainment(pgid, launch.cmd.Process, direct, record)
	if err := activateProcessContainmentRecord(record, pid, pgid); err != nil {
		launch.close()
		cleanupErr := tree.complete(darwinContainmentDeadline)
		return nil, errors.Join(fmt.Errorf("%w: activate containment record: %v", ErrProcessContainmentIncomplete, err), cleanupErr)
	}
	if err := launch.releaseGate(); err != nil {
		cleanupErr := tree.complete(darwinContainmentDeadline)
		launch.close()
		return nil, errors.Join(fmt.Errorf("%w: release validated native launch: %v", ErrProcessContainmentIncomplete, err), cleanupErr)
	}
	direct.begin()
	if err := awaitDarwinLaunchExec(launch.status); err != nil {
		launch.status = nil
		cleanupErr := tree.complete(darwinContainmentDeadline)
		launch.close()
		return nil, errors.Join(err, cleanupErr)
	}
	launch.status = nil
	launch.close()
	return tree, nil
}

func prepareDarwinLaunch(native *exec.Cmd, generationRoot string) (*darwinLaunch, error) {
	if native == nil || native.Path == "" || len(native.Args) == 0 {
		return nil, errors.New("prepare Darwin native launch: command is incomplete")
	}
	configFile, err := darwinLaunchCreateTemp(generationRoot, ".launch-")
	if err != nil {
		return nil, fmt.Errorf("create Darwin native launch config: %w", err)
	}
	name := configFile.Name()
	cleanup := func() { _ = configFile.Close(); _ = darwinLaunchRemove(name) }
	if chmodErr := darwinLaunchFileChmod(configFile, 0o600); chmodErr != nil {
		cleanup()
		return nil, chmodErr
	}
	config := darwinLaunchConfig{Path: native.Path, Args: append([]string(nil), native.Args...), Env: scrubDarwinInternalEnvironment(native.Env)}
	if encodeErr := darwinLaunchEncodeConfig(configFile, config); encodeErr != nil {
		cleanup()
		return nil, encodeErr
	}
	if _, seekErr := darwinLaunchFileSeek(configFile, 0, io.SeekStart); seekErr != nil {
		cleanup()
		return nil, seekErr
	}
	if removeErr := darwinLaunchRemove(name); removeErr != nil {
		cleanup()
		return nil, removeErr
	}
	gateRead, gateWrite, err := darwinLaunchPipe()
	if err != nil {
		_ = configFile.Close()
		return nil, err
	}
	statusRead, statusWrite, err := darwinLaunchPipe()
	if err != nil {
		_ = configFile.Close()
		_ = gateRead.Close()
		_ = gateWrite.Close()
		return nil, err
	}
	self, err := darwinLaunchExecutable()
	if err != nil {
		_ = configFile.Close()
		_ = gateRead.Close()
		_ = gateWrite.Close()
		_ = statusRead.Close()
		_ = statusWrite.Close()
		return nil, err
	}
	helper := darwinLaunchCommand(self)
	helper.Dir, helper.Stdin, helper.Stdout, helper.Stderr = native.Dir, native.Stdin, native.Stdout, native.Stderr
	// The helper carries the bootstrap marker and nothing else. It requests no
	// credential change: this backend runs the native harness as the identity
	// the adapter already holds, and the native environment travels in the
	// sealed config rather than through the helper's own environment.
	helper.Env = darwinBootstrapEnvironment()
	helper.WaitDelay = darwinPipeWait
	helper.ExtraFiles = []*os.File{configFile, gateRead, statusWrite}
	configureHermesProcess(helper)
	return &darwinLaunch{cmd: helper, inherited: []*os.File{configFile, gateRead, statusWrite}, gate: gateWrite, status: statusRead}, nil
}

func awaitDarwinLaunchExec(status *os.File) error {
	if status == nil {
		return errors.New("darwin native launch status is unavailable")
	}
	defer status.Close()
	if err := status.SetReadDeadline(time.Now().Add(darwinLaunchStatusWait)); err != nil {
		return err
	}
	contents, err := io.ReadAll(io.LimitReader(status, 4097))
	if err != nil {
		return fmt.Errorf("await Darwin native launch status: %w", err)
	}
	if len(contents) > 4096 {
		return errors.New("darwin native launch failure exceeded the status limit")
	}
	if message := strings.TrimSpace(string(contents)); message != "" {
		return fmt.Errorf("exec native Hermes command: %s", message)
	}
	return nil
}

func handleVanishedDarwinLeader(target *exec.Cmd, launch *darwinLaunch, direct *directChildWait, pid int) (*processContainment, error) {
	probeErr := processKill(-pid, 0)
	if errors.Is(probeErr, syscall.ESRCH) {
		launch.close()
		direct.begin()
		waitErr := direct.awaitReaped(darwinContainmentDeadline)
		recordErr := completeContainmentRecord(launch.containment, containmentStateAbsent)
		return nil, errors.Join(errors.New("native launch exited before containment validation"), waitErr, recordErr)
	}
	if probeErr != nil && !errors.Is(probeErr, syscall.EPERM) {
		launch.close()
		_ = launch.cmd.Process.Signal(syscall.SIGKILL)
		direct.begin()
		waitErr := direct.awaitReaped(darwinContainmentDeadline)
		recordErr := completeContainmentRecord(launch.containment, containmentStateFailed)
		return nil, errors.Join(fmt.Errorf("%w: probe expected native process group: %v", ErrProcessContainmentIncomplete, probeErr), waitErr, recordErr)
	}
	tree := newDarwinContainment(pid, launch.cmd.Process, direct, launch.containment)
	launch.close()
	// Act on the PID-protected group observation before releasing the waiter.
	deadline := time.Now().Add(darwinContainmentDeadline)
	absent, signalErr := signalOriginalProcessGroup(pid, syscall.SIGTERM)
	direct.begin()
	tree.cleanupOnce.Do(func() { tree.cleanupErr = finishDarwinBoundary(tree, launch.containment, deadline, absent, signalErr) })
	cleanupErr := tree.cleanupErr
	return nil, errors.Join(errors.New("native group remained observable after its leader vanished"), signalErr, cleanupErr)
}

func newDarwinContainment(pgid int, process *os.Process, direct *directChildWait, record containmentRecord) *processContainment {
	tree := &processContainment{processGroupID: pgid, process: process, direct: direct}
	tree.completeFn = func(time.Duration) error {
		tree.cleanupOnce.Do(func() { tree.cleanupErr = completeDarwinBoundary(tree, record) })
		return tree.cleanupErr
	}
	tree.terminateFn = func() error { return tree.complete(darwinContainmentDeadline) }
	tree.killFn = tree.terminateFn
	return tree
}

func completeDarwinBoundary(tree *processContainment, record containmentRecord) error {
	deadline := time.Now().Add(darwinContainmentDeadline)
	absent, err := signalOriginalProcessGroup(tree.processGroupID, syscall.SIGTERM)
	tree.direct.begin()
	return finishDarwinBoundary(tree, record, deadline, absent, err)
}

func finishDarwinBoundary(tree *processContainment, record containmentRecord, deadline time.Time, absent bool, err error) error {
	if err == nil && !absent {
		absent, err = pollOriginalProcessGroup(tree.processGroupID, minDeadline(deadline, time.Now().Add(darwinTermGrace)))
	}
	if err == nil && !absent {
		absent, err = signalOriginalProcessGroup(tree.processGroupID, syscall.SIGKILL)
	}
	if err == nil && !absent {
		absent, err = pollOriginalProcessGroup(tree.processGroupID, deadline)
	}
	if err != nil {
		directErr := forceKillDarwinDirectChild(tree, deadline)
		recordErr := completeContainmentRecord(record, containmentStateFailed)

		return errors.Join(ErrProcessContainmentIncomplete, err, directErr, recordErr)
	}
	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	waitErr := tree.direct.awaitReaped(remaining)
	if !absent || waitErr != nil {
		recordErr := completeContainmentRecord(record, containmentStateFailed)
		if !absent {
			err = errors.New("original process group remained observable")
		}
		return errors.Join(ErrProcessContainmentIncomplete, err, waitErr, recordErr)
	}
	if err := completeContainmentRecord(record, containmentStateAbsent); err != nil {
		return errors.Join(ErrProcessContainmentIncomplete, err)
	}
	return nil
}

func forceKillDarwinDirectChild(tree *processContainment, deadline time.Time) error {
	if tree == nil || tree.process == nil || tree.direct == nil {
		return errors.New("darwin direct-child cleanup handle is unavailable")
	}

	select {
	case <-tree.direct.done:
		return nil
	default:
	}

	killErr := darwinDirectProcessKill(tree.process)
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	tree.direct.begin()

	select {
	case <-tree.direct.done:
		return killErr
	default:
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return errors.Join(killErr, fmt.Errorf("%w: Hermes direct child was not reaped", ErrProcessContainmentIncomplete))
	}

	return errors.Join(killErr, tree.direct.awaitReaped(remaining))
}

func signalOriginalProcessGroup(pgid int, signal syscall.Signal) (bool, error) {
	err := processKill(-pgid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	if errors.Is(err, syscall.EPERM) {
		return false, nil
	}
	return false, err
}

func pollOriginalProcessGroup(pgid int, deadline time.Time) (bool, error) {
	for {
		err := processKill(-pgid, 0)
		switch {
		case errors.Is(err, syscall.ESRCH):
			return true, nil
		case err == nil, errors.Is(err, syscall.EPERM):
		default:
			return false, fmt.Errorf("inspect original process group %d: %w", pgid, err)
		}
		if !time.Now().Before(deadline) {
			return false, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func minDeadline(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func minTime(left, right time.Time) time.Time {
	return minDeadline(left, right)
}

func newContainmentRuntimeID() (string, error) {
	var value [16]byte
	if _, err := containmentRandomRead(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func withDarwinContainmentMarkers(environment []string, runtimeID, root string) []string {
	filtered := make([]string, 0, len(environment)+2)
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if name == envRuntimeID || name == envScratchRoot || strings.HasPrefix(strings.ToUpper(name), darwinPrivateEnvPrefix) {
			continue
		}
		filtered = append(filtered, entry)
	}
	return append(filtered, envRuntimeID+"="+runtimeID, envScratchRoot+"="+root)
}

func scrubDarwinInternalEnvironment(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(name), darwinPrivateEnvPrefix) {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func darwinBootstrapEnvironment() []string {
	return []string{darwinLaunchBootstrapEnv + "=" + darwinLaunchBootstrapMode}
}
