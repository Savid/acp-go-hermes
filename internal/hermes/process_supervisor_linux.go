//go:build linux

package hermes

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	envHermesSupervisor  = "ACP_GO_HERMES_PROCESS_SUPERVISOR"
	supervisorConfigFD   = 3
	supervisorControlFD  = 4
	supervisorProofFD    = 5
	supervisorConfigName = "acp-go-hermes-process-supervisor"
)

type hermesSupervisorConfig struct {
	Path      string           `json:"path"`
	Args      []string         `json:"args"`
	Dir       string           `json:"dir"`
	Env       []string         `json:"env"`
	Isolation ProcessIsolation `json:"isolation"`
}

var (
	supervisorExit             = os.Exit
	supervisorExecutable       = os.Executable
	supervisorPipe             = os.Pipe
	supervisorCommand          = exec.Command
	supervisorPrctl            = unix.Prctl
	supervisorSetrlimit        = unix.Setrlimit
	supervisorMemfd            = unix.MemfdCreate
	supervisorSealConfig       = unix.FcntlInt
	supervisorAcquireLock      = acquireAgentIdentityLock
	supervisorPIDFDOpen        = unix.PidfdOpen
	supervisorPIDFDSendSignal  = unix.PidfdSendSignal
	supervisorNewFile          = os.NewFile
	supervisorCloseOnExec      = unix.CloseOnExec
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
	if os.Getenv(envHermesSupervisor) != "1" {
		return
	}

	supervisorExit(runHermesProcessSupervisor())
}

// startUnixContainedProcess interposes a dedicated subreaper between the
// embedding host and Hermes. Linux process groups alone are not containment: a
// tool can call setsid(2) and escape. The subreaper remains the parent of every
// orphaned native descendant, kills/reaps that complete tree, and emits a proof
// byte before it exits. Absence of that byte is fail-closed.
func startUnixContainedProcess(target *exec.Cmd, spec ContainmentSpec) (*processContainment, error) {
	if err := validateProcessIsolation(spec.Isolation); err != nil {
		return nil, fmt.Errorf("validate Hermes supervisor isolation: %w", err)
	}
	if target == nil || target.Path == "" || len(target.Args) == 0 {
		return nil, errors.New("hermes supervisor target is unavailable")
	}

	if target.SysProcAttr == nil || !target.SysProcAttr.Setpgid {
		return nil, errors.New("hermes Linux supervisor target containment is not configured")
	}

	if _, err := executableFile(target.Path); err != nil {
		return nil, err
	}

	if len(target.ExtraFiles) != 0 {
		return nil, errors.New("hermes supervisor does not accept target ExtraFiles")
	}

	self, err := supervisorExecutable()
	if err != nil {
		return nil, fmt.Errorf("resolve Hermes supervisor executable: %w", err)
	}

	config := hermesSupervisorConfig{
		Path:      target.Path,
		Args:      append([]string(nil), target.Args...),
		Dir:       target.Dir,
		Env:       append([]string(nil), target.Env...),
		Isolation: *spec.Isolation,
	}
	configFD, err := supervisorMemfd(supervisorConfigName, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("create Hermes supervisor config: %w", err)
	}
	configFile := os.NewFile(uintptr(configFD), supervisorConfigName)
	if err := writeHermesSupervisorConfig(configFile, config); err != nil {
		_ = configFile.Close()
		return nil, err
	}
	if _, err := supervisorSealConfig(configFile.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		_ = configFile.Close()
		return nil, fmt.Errorf("seal Hermes supervisor config: %w", err)
	}

	controlRead, controlWrite, err := supervisorPipe()
	if err != nil {
		_ = configFile.Close()
		return nil, fmt.Errorf("create Hermes supervisor control pipe: %w", err)
	}

	proofRead, proofWrite, err := supervisorPipe()
	if err != nil {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()

		return nil, fmt.Errorf("create Hermes supervisor proof pipe: %w", err)
	}

	cleanupPipes := func() {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()
		_ = proofRead.Close()
		_ = proofWrite.Close()
	}

	supervisor := supervisorCommand(self) // #nosec G204 -- self-exec enters the private subreaper mode above.

	supervisor.Dir = "/"
	supervisor.Env = []string{envHermesSupervisor + "=1", "GORACE=atexit_sleep_ms=0"}
	supervisor.Stdin = target.Stdin
	supervisor.Stdout = target.Stdout
	supervisor.Stderr = target.Stderr
	supervisor.ExtraFiles = []*os.File{configFile, controlRead, proofWrite}
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
	_ = configFile.Close()

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
			terminateOnce.Do(func() { closeErr = controlWrite.Close() })

			return closeErr
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
	if err := supervisorPrctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return 125
	}
	if err := supervisorPrctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return 125
	}

	// A pidfd binds every signal to the observed process identity. Refuse to
	// supervise on kernels where that proof primitive is unavailable.
	selfPIDFD, err := supervisorPIDFDOpen(os.Getpid(), 0)
	if err != nil {
		return 125
	}

	_ = unix.Close(selfPIDFD)

	configInput := supervisorNewFile(supervisorConfigFD, "hermes-supervisor-config")
	control := supervisorNewFile(supervisorControlFD, "hermes-supervisor-control")
	proof := supervisorNewFile(supervisorProofFD, "hermes-supervisor-proof")

	supervisorCloseOnExec(supervisorConfigFD)
	supervisorCloseOnExec(supervisorControlFD)
	supervisorCloseOnExec(supervisorProofFD)
	if configInput == nil || control == nil || proof == nil {
		return 125
	}
	defer configInput.Close()

	var config hermesSupervisorConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return 125
	}
	if config.Path == "" || len(config.Args) == 0 || validateProcessIsolation(&config.Isolation) != nil {
		return 125
	}

	return runHermesProcessSupervisorCore(config, control, proof)
}

func writeHermesSupervisorConfig(file io.WriteSeeker, config hermesSupervisorConfig) error {
	if err := json.NewEncoder(file).Encode(config); err != nil {
		return fmt.Errorf("encode Hermes supervisor config: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind Hermes supervisor config: %w", err)
	}

	return nil
}

func startHermesSupervisorTarget(target *exec.Cmd, isolation *ProcessIsolation) (error, error) {
	runtime.LockOSThread()

	defer runtime.UnlockOSThread()

	if err := supervisorSetrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		return fmt.Errorf("disable Hermes native core dumps: %w", err), nil
	}
	if err := supervisorPrctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err, nil
	}
	if err := applyProcessIsolation(target, isolation); err != nil {
		return fmt.Errorf("apply Hermes native process isolation: %w", err), nil
	}

	return nil, target.Start()
}

func runHermesProcessSupervisorCore(config hermesSupervisorConfig, control *os.File, proof *os.File) int {
	defer control.Close()
	defer proof.Close()

	target := exec.Command(config.Path, config.Args[1:]...) // #nosec G204,G702 -- target path and argv came through the private parent descriptor.
	target.Args = append([]string(nil), config.Args...)
	target.Dir = config.Dir
	target.Env = append([]string(nil), config.Env...)
	target.Stdin = os.Stdin
	target.Stdout = os.Stdout
	target.Stderr = os.Stderr
	target.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

	shutdown := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(io.Discard, control)
		shutdown <- struct{}{}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)

	identityLock, err := supervisorAcquireLock(config.Isolation.UID, config.Isolation.TestOnlyNoCredential, shutdown, signals)
	if err != nil {
		return 125
	}
	defer identityLock.Close()

	privilegeErr, startErr := startHermesSupervisorTarget(target, &config.Isolation)

	if privilegeErr != nil || startErr != nil {
		return 125
	}

	targetDone := make(chan error, 1)

	go func() { targetDone <- target.Wait() }()

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
		for !targetSettled {
			_ = signalSupervisorDescendants(syscall.SIGKILL)
			select {
			case targetErr = <-targetDone:
				targetSettled = true
			case <-time.After(supervisorPollInterval):
			}
		}
	}

	for {
		if err := proveSupervisorDescendants(5 * time.Second); err == nil {
			break
		}
		time.Sleep(time.Second)
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
