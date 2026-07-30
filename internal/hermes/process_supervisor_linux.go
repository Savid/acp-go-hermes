//go:build linux

package hermes

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	envHermesSupervisor       = "ACP_GO_HERMES_PROCESS_SUPERVISOR"
	envHermesSupervisorTarget = "ACP_GO_HERMES_PROCESS_SUPERVISOR_TARGET"
	supervisorControlFD       = 3
	supervisorProofFD         = 4
)

var (
	supervisorExit             = os.Exit
	supervisorExecutable       = os.Executable
	supervisorPipe             = os.Pipe
	supervisorCommand          = exec.Command
	supervisorPrctl            = unix.Prctl
	supervisorPIDFDOpen        = unix.PidfdOpen
	supervisorPIDFDSendSignal  = unix.PidfdSendSignal
	supervisorNewFile          = os.NewFile
	supervisorCloseOnExec      = unix.CloseOnExec
	supervisorArgs             = func() []string { return os.Args }
	supervisorEnviron          = os.Environ
	supervisorReadDir          = os.ReadDir
	supervisorReadFile         = os.ReadFile
	supervisorWait4            = syscall.Wait4
	listSupervisorDescendants  = supervisorDescendants
	stopSupervisorDescendants  = stopSupervisedDescendants
	proveSupervisorDescendants = proveAndReapSupervisorDescendants
	supervisorTermGrace        = 500 * time.Millisecond
	supervisorKillGrace        = 2 * time.Second
	supervisorPollInterval     = 10 * time.Millisecond
)

func init() { //nolint:gochecknoinits // A private self-exec mode is required before the embedding host's main runs.
	runHermesSupervisorInit()
}

func runHermesSupervisorInit() {
	if os.Getenv(envHermesSupervisor) == "" {
		return
	}

	supervisorExit(runHermesProcessSupervisor())
}

// startUnixContainedProcess interposes a dedicated subreaper between the
// embedding host and Hermes. Linux process groups alone are not containment: a
// tool can call setsid(2) and escape. The subreaper remains the parent of every
// orphaned native descendant, kills/reaps that complete tree, and emits a proof
// byte before it exits. Absence of that byte is fail-closed.
func startUnixContainedProcess(target *exec.Cmd, _ ContainmentSpec) (*processContainment, error) {
	if target == nil || target.Path == "" || len(target.Args) == 0 {
		return nil, errors.New("hermes supervisor target is unavailable")
	}

	if target.SysProcAttr == nil || !target.SysProcAttr.Setpgid {
		return nil, errors.New("hermes Linux supervisor target containment is not configured")
	}

	if _, err := exec.LookPath(target.Path); err != nil {
		return nil, err
	}

	if len(target.ExtraFiles) != 0 {
		return nil, errors.New("hermes supervisor does not accept target ExtraFiles")
	}

	self, err := supervisorExecutable()
	if err != nil {
		return nil, fmt.Errorf("resolve Hermes supervisor executable: %w", err)
	}

	controlRead, controlWrite, err := supervisorPipe()
	if err != nil {
		return nil, fmt.Errorf("create Hermes supervisor control pipe: %w", err)
	}

	proofRead, proofWrite, err := supervisorPipe()
	if err != nil {
		_ = controlRead.Close()
		_ = controlWrite.Close()

		return nil, fmt.Errorf("create Hermes supervisor proof pipe: %w", err)
	}

	cleanupPipes := func() {
		_ = controlRead.Close()
		_ = controlWrite.Close()
		_ = proofRead.Close()
		_ = proofWrite.Close()
	}

	supervisor := supervisorCommand(self) // #nosec G204 -- self-exec enters the private subreaper mode above.

	supervisor.Args = append([]string(nil), target.Args...)
	supervisor.Dir = target.Dir
	targetEnv := target.Env
	if targetEnv == nil {
		targetEnv = os.Environ()
	}
	supervisor.Env = append([]string(nil), targetEnv...)
	supervisor.Env = append(supervisor.Env,
		envHermesSupervisor+"=1",
		envHermesSupervisorTarget+"="+target.Path,
	)
	supervisor.Stdin = target.Stdin
	supervisor.Stdout = target.Stdout
	supervisor.Stderr = target.Stderr
	supervisor.ExtraFiles = []*os.File{controlRead, proofWrite}
	supervisor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Install the wrapper command before launch. exec.Cmd must not be copied
	// after Start: its private waiter and pipe-copy state belong to the exact
	// value that was started, and copying it races version-probe output drains.
	*target = *supervisor
	supervisor = target

	if err := supervisor.Start(); err != nil {
		cleanupPipes()

		return nil, err
	}

	_ = controlRead.Close()
	_ = proofWrite.Close()

	proof := make(chan bool, 1)
	go func() {
		defer close(proof)
		defer proofRead.Close()

		var value [1]byte

		_, readErr := io.ReadFull(proofRead, value[:])
		proof <- readErr == nil && value[0] == 1
	}()

	var (
		terminateOnce sync.Once
		closeOnce     sync.Once
		closeErr      error
	)

	containment := &processContainment{
		processGroupID: supervisor.Process.Pid,
		proof:          proof,
		terminateFn: func() error {
			terminateOnce.Do(func() { closeErr = controlWrite.Close() })

			return closeErr
		},
		killFn: func() error {
			return signalLeaseGroup(supervisor.Process.Pid, syscall.SIGKILL)
		},
		descendantCountFn: func() (int, bool) {
			descendants, err := listSupervisorDescendants(supervisor.Process.Pid)
			if err != nil {
				return 0, false
			}

			// The dedicated supervisor is the root of the native containment
			// boundary and therefore counts alongside Hermes and its tools.
			return 1 + len(descendants), true
		},
		closeFn: func() error {
			closeOnce.Do(func() {
				terminateOnce.Do(func() { closeErr = controlWrite.Close() })
			})

			return closeErr
		},
	}

	return containment, nil
}

func runHermesProcessSupervisor() int {
	targetPath := os.Getenv(envHermesSupervisorTarget)
	if targetPath == "" {
		return 125
	}

	if err := supervisorPrctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return 125
	}

	// A pidfd binds every signal to the observed process identity. Refuse to
	// supervise on kernels where that proof primitive is unavailable.
	selfPIDFD, err := supervisorPIDFDOpen(os.Getpid(), 0)
	if err != nil {
		return 125
	}

	_ = unix.Close(selfPIDFD)

	control := supervisorNewFile(supervisorControlFD, "hermes-supervisor-control")
	proof := supervisorNewFile(supervisorProofFD, "hermes-supervisor-proof")

	supervisorCloseOnExec(supervisorControlFD)
	supervisorCloseOnExec(supervisorProofFD)

	return runHermesProcessSupervisorCore(targetPath, supervisorArgs(), supervisorEnviron(), control, proof)
}

func runHermesProcessSupervisorCore(targetPath string, args []string, env []string, control *os.File, proof *os.File) int {
	defer control.Close()
	defer proof.Close()

	target := exec.Command(targetPath, args[1:]...) // #nosec G204,G702 -- target path and argv came from the validated parent command.
	target.Args[0] = args[0]
	target.Env = supervisorTargetEnv(env)
	target.Stdin = os.Stdin
	target.Stdout = os.Stdout
	target.Stderr = os.Stderr
	target.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

	if err := target.Start(); err != nil {
		return 125
	}

	targetDone := make(chan error, 1)

	go func() { targetDone <- target.Wait() }()

	shutdown := make(chan struct{}, 1)

	go func() {
		_, _ = io.Copy(io.Discard, control)

		shutdown <- struct{}{}
	}()

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	defer signal.Stop(signals)

	var (
		targetErr     error
		targetSettled bool
	)
	select {
	case targetErr = <-targetDone:
		targetSettled = true
	case <-shutdown:
		targetErr, targetSettled = stopSupervisorDescendants(target.Process.Pid, targetDone)
	case <-signals:
		targetErr, targetSettled = stopSupervisorDescendants(target.Process.Pid, targetDone)
	}

	// target.Wait is the sole waiter for the direct root. Descendant proof may
	// call wait4(-1) only after that result was consumed; otherwise the two
	// waiters can race and a root-timeout path could manufacture false proof.
	if !targetSettled {
		return 126
	}

	if err := proveSupervisorDescendants(5 * time.Second); err != nil {
		return 126
	}

	if _, err := proof.Write([]byte{1}); err != nil {
		return 126
	}

	if targetErr == nil {
		return 0
	}

	var exitErr *exec.ExitError
	if errors.As(targetErr, &exitErr) {
		return exitErr.ExitCode()
	}

	return 1
}

func supervisorTargetEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		if strings.HasPrefix(entry, envHermesSupervisor+"=") ||
			strings.HasPrefix(entry, envHermesSupervisorTarget+"=") {
			continue
		}

		out = append(out, entry)
	}

	return out
}

func stopSupervisedDescendants(targetPID int, targetDone <-chan error) (error, bool) {
	termDeadline := time.Now().Add(supervisorTermGrace)
	for time.Now().Before(termDeadline) {
		_ = signalSupervisorDescendants(syscall.SIGTERM)

		select {
		case err := <-targetDone:
			return err, true
		case <-time.After(supervisorPollInterval):
		}
	}

	killDeadline := time.Now().Add(supervisorKillGrace)
	for time.Now().Before(killDeadline) {
		_ = signalSupervisorDescendants(syscall.SIGKILL)

		select {
		case err := <-targetDone:
			return err, true
		case <-time.After(supervisorPollInterval):
		}
	}

	return fmt.Errorf("supervised Hermes root %d did not exit", targetPID), false
}

func proveAndReapSupervisorDescendants(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		descendants, err := listSupervisorDescendants(os.Getpid())
		if err != nil {
			return err
		}

		for pid, state := range descendants {
			if state != 'Z' {
				_ = signalPIDFD(pid, syscall.SIGKILL)
			}
		}

		empty, err := reapSupervisorChildren()
		if err != nil {
			return fmt.Errorf("inspect Hermes supervisor child set: %w", err)
		}

		if empty {
			return nil
		}

		if !time.Now().Before(deadline) {
			return fmt.Errorf("supervisor retained descendants: %v", descendantPIDs(descendants))
		}

		time.Sleep(supervisorPollInterval)
	}
}

func signalSupervisorDescendants(sig syscall.Signal) error {
	descendants, err := listSupervisorDescendants(os.Getpid())
	if err != nil {
		return err
	}

	var signalErr error

	for pid, state := range descendants {
		if state == 'Z' {
			continue
		}

		signalErr = errors.Join(signalErr, signalPIDFD(pid, sig))
	}

	return signalErr
}

func signalPIDFD(pid int, sig syscall.Signal) error {
	pidfd, err := supervisorPIDFDOpen(pid, 0)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}
	defer unix.Close(pidfd)

	if err := supervisorPIDFDSendSignal(pidfd, sig, nil, 0); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	return nil
}

func supervisorDescendants(rootPID int) (map[int]byte, error) {
	entries, err := supervisorReadDir("/proc")
	if err != nil {
		return nil, err
	}

	type proc struct {
		parent int
		state  byte
	}

	processes := make(map[int]proc, len(entries))
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		data, err := supervisorReadFile(filepath.Join(string(filepath.Separator), "proc", entry.Name(), "stat"))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return nil, err
		}

		parent, state, ok := supervisorProcStat(string(data))
		if ok {
			processes[pid] = proc{parent: parent, state: state}
		}
	}

	descendants := map[int]byte{}

	for changed := true; changed; {
		changed = false

		for pid, process := range processes {
			if pid == rootPID {
				continue
			}

			if process.parent != rootPID {
				if _, ok := descendants[process.parent]; !ok {
					continue
				}
			}

			if _, ok := descendants[pid]; !ok {
				descendants[pid] = process.state
				changed = true
			}
		}
	}

	return descendants, nil
}

func supervisorProcStat(stat string) (int, byte, bool) {
	closeParen := strings.LastIndex(stat, ")")
	if closeParen < 0 || closeParen+2 >= len(stat) {
		return 0, 0, false
	}

	fields := strings.Fields(stat[closeParen+2:])
	if len(fields) < 2 || len(fields[0]) != 1 {
		return 0, 0, false
	}

	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, false
	}

	return parent, fields[0][0], true
}

// reapSupervisorChildren drains every waitable adopted child and returns true
// only when the kernel reports ECHILD. An empty /proc inventory is not proof:
// a descendant can be between fork, parent death, and subreaper reparenting.
func reapSupervisorChildren() (bool, error) {
	for {
		var status syscall.WaitStatus

		pid, err := supervisorWait4(-1, &status, syscall.WNOHANG, nil)
		if pid > 0 {
			continue
		}

		if errors.Is(err, syscall.EINTR) {
			continue
		}

		if errors.Is(err, syscall.ECHILD) {
			return true, nil
		}

		if err != nil {
			return false, err
		}

		if pid == 0 {
			return false, nil
		}

		return false, fmt.Errorf("invalid wait4 result %d", pid)
	}
}

func descendantPIDs(descendants map[int]byte) []int {
	pids := make([]int, 0, len(descendants))
	for pid := range descendants {
		pids = append(pids, pid)
	}

	return pids
}
