//go:build linux

package hermes

import (
	"bufio"
	"encoding/json"
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
	envHermesSupervisor      = "ACP_GO_HERMES_PROCESS_SUPERVISOR"
	hermesSupervisorGuardian = "guardian"
	hermesSupervisorLiveness = "liveness"
	supervisorConfigFD       = 3
	supervisorControlFD      = 4
	supervisorProofFD        = 5
	supervisorIdentityLockFD = 6
	supervisorAuthorityFD    = 7
	supervisorConfigName     = "acp-go-hermes-process-supervisor"
)

type hermesSupervisorConfig struct {
	Path                string           `json:"path"`
	Args                []string         `json:"args"`
	Dir                 string           `json:"dir"`
	Env                 []string         `json:"env"`
	Isolation           ProcessIsolation `json:"isolation"`
	IdentityLock        bool             `json:"identityLock"`
	AuthorityDomain     bool             `json:"authorityDomain"`
	StandaloneAuthority bool             `json:"standaloneAuthority"`
}

var (
	supervisorExit              = os.Exit
	supervisorExecutable        = os.Executable
	supervisorPipe              = os.Pipe
	supervisorCommand           = exec.Command
	supervisorPrctl             = unix.Prctl
	supervisorSetrlimit         = unix.Setrlimit
	supervisorMemfd             = unix.MemfdCreate
	supervisorSealConfig        = unix.FcntlInt
	supervisorAcquireStandalone = acquireAgentStandaloneIdentity
	supervisorPIDFDOpen         = unix.PidfdOpen
	supervisorPIDFDSendSignal   = unix.PidfdSendSignal
	supervisorNewFile           = os.NewFile
	supervisorFcntl             = unix.FcntlInt
	supervisorCloseOnExec       = setHermesSupervisorCloseOnExec
	supervisorReadDir           = os.ReadDir
	supervisorReadFile          = os.ReadFile
	supervisorWait4             = syscall.Wait4
	listSupervisorDescendants   = supervisorDescendants
	stopSupervisorDescendants   = stopSupervisedDescendants
	proveSupervisorDescendants  = proveAndReapSupervisorDescendants
	supervisorTermGrace         = 500 * time.Millisecond
	supervisorKillGrace         = 2 * time.Second
	supervisorPollInterval      = 10 * time.Millisecond
	supervisorEffectiveUID      = os.Geteuid
	supervisorPoll              = unix.Poll
)

func init() { //nolint:gochecknoinits // A private self-exec mode is required before the embedding host's main runs.
	runHermesSupervisorInit()
}

func runHermesSupervisorInit() {
	mode := os.Getenv(envHermesSupervisor)
	if mode != hermesSupervisorGuardian && mode != hermesSupervisorLiveness {
		return
	}

	supervisorExit(runHermesProcessSupervisor(mode))
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
	if err := validateHermesSupervisorIdentity(spec.Isolation); err != nil {
		return nil, fmt.Errorf("validate Hermes supervisor identity: %w", err)
	}
	if (spec.Isolation.IdentityLock == nil) != (spec.Isolation.AuthorityDomain == nil) {
		return nil, errors.New("Hermes supervisor identity lock and authority domain must be provided together")
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

	targetEnvironment := target.Env
	if targetEnvironment == nil {
		targetEnvironment = os.Environ()
	}
	config := hermesSupervisorConfig{
		Path:            target.Path,
		Args:            append([]string(nil), target.Args...),
		Dir:             target.Dir,
		Env:             append([]string(nil), targetEnvironment...),
		Isolation:       *spec.Isolation,
		IdentityLock:    spec.Isolation.IdentityLock != nil,
		AuthorityDomain: spec.Isolation.AuthorityDomain != nil,
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
	supervisor.Env = []string{envHermesSupervisor + "=" + hermesSupervisorGuardian}
	supervisor.Stdin = target.Stdin
	supervisor.Stdout = target.Stdout
	supervisor.Stderr = target.Stderr
	supervisor.ExtraFiles = []*os.File{configFile, controlRead, proofWrite}
	if spec.Isolation.IdentityLock != nil {
		identityLock, duplicateErr := spec.Isolation.IdentityLock.Duplicate()
		if duplicateErr != nil {
			cleanupPipes()

			return nil, fmt.Errorf("duplicate Hermes agent identity lock: %w", duplicateErr)
		}
		supervisor.ExtraFiles = append(supervisor.ExtraFiles, identityLock)
		defer identityLock.Close()

		authorityDomain, duplicateErr := spec.Isolation.AuthorityDomain.Duplicate()
		if duplicateErr != nil {
			cleanupPipes()

			return nil, fmt.Errorf("duplicate Hermes agent authority domain: %w", duplicateErr)
		}
		supervisor.ExtraFiles = append(supervisor.ExtraFiles, authorityDomain)
		defer authorityDomain.Close()
	}
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

func runHermesProcessSupervisor(mode string) int {
	if supervisorEffectiveUID() != 0 {
		return 125
	}
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

	for _, fd := range []int{supervisorConfigFD, supervisorControlFD, supervisorProofFD} {
		if err := supervisorCloseOnExec(fd); err != nil {
			return 125
		}
	}
	if configInput == nil || control == nil || proof == nil {
		return 125
	}
	defer configInput.Close()

	var config hermesSupervisorConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return 125
	}
	if validateHermesSupervisorConfig(config) != nil {
		return 125
	}

	if mode == hermesSupervisorLiveness {
		outerProof := supervisorNewFile(8, "hermes-supervisor-completion-proof")
		peer := supervisorNewFile(9, "hermes-supervisor-guardian-peer")
		if outerProof == nil || peer == nil || supervisorCloseOnExec(8) != nil || supervisorCloseOnExec(9) != nil {
			return 125
		}
		defer outerProof.Close()
		defer peer.Close()

		return runHermesProcessSupervisorNative(config, []io.Reader{control}, peer, proof, outerProof, true)
	}

	return runHermesProcessSupervisorGuardian(config, control, proof)
}

func validateHermesSupervisorConfig(config hermesSupervisorConfig) error {
	if config.Path == "" || len(config.Args) == 0 {
		return errors.New("Hermes supervisor config is incomplete")
	}
	if config.IdentityLock != config.AuthorityDomain {
		return errors.New("Hermes supervisor identity lock and authority domain must be provided together")
	}
	if config.StandaloneAuthority && !config.IdentityLock {
		return errors.New("Hermes standalone authority requires inherited identity capabilities")
	}
	validation := config.Isolation
	if config.IdentityLock {
		placeholder := &agentIdentityLock{}
		validation.IdentityLock = placeholder
		validation.AuthorityDomain = placeholder
		if config.StandaloneAuthority {
			if !validStandaloneOwnerID(config.Isolation.StandaloneOwnerID) ||
				!validStandaloneStateRootPath(config.Isolation.StandaloneStateRoot) {
				return errors.New("Hermes inherited standalone authority tuple is invalid")
			}
			validation.StandaloneOwnerID = ""
			validation.StandaloneStateRoot = ""
		}
	}
	if err := validateProcessIsolation(&validation); err != nil {
		return err
	}

	return validateHermesSupervisorIdentity(&config.Isolation)
}

func setHermesSupervisorCloseOnExec(fd int) error {
	flags, err := supervisorFcntl(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("read inherited Hermes supervisor descriptor flags: %w", err)
	}
	if _, err = supervisorFcntl(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("protect inherited Hermes supervisor descriptor from exec: %w", err)
	}
	return nil
}

func validateHermesSupervisorIdentity(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation is required")
	}

	effectiveUID := supervisorEffectiveUID()
	if effectiveUID != 0 {
		return fmt.Errorf("trusted root identity is required, effective uid is %d", effectiveUID)
	}
	if isolation.UID == uint32(effectiveUID) {
		return errors.New("native target identity must differ from the trusted supervisor")
	}

	return nil
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

func startHermesSupervisorTarget(
	target *exec.Cmd,
	isolation *ProcessIsolation,
	beforeStart func() error,
) (<-chan error, error, error) {
	var privilegeErr error
	waitDone, startErr := startCommandOnCreatorThread(func() error {
		if err := supervisorSetrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
			privilegeErr = fmt.Errorf("disable Hermes native core dumps: %w", err)

			return privilegeErr
		}
		if err := supervisorPrctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			privilegeErr = err

			return err
		}
		if err := applyProcessIsolation(target, isolation); err != nil {
			privilegeErr = fmt.Errorf("apply Hermes native process isolation: %w", err)

			return privilegeErr
		}
		if beforeStart != nil {
			if err := beforeStart(); err != nil {
				privilegeErr = err

				return err
			}
		}

		return target.Start()
	}, target.Wait)
	if privilegeErr != nil {
		return nil, privilegeErr, nil
	}

	return waitDone, nil, startErr
}

type hermesSupervisorAuthority struct {
	identity   *agentIdentityLock
	domain     *agentIdentityLock
	standalone *agentStandaloneIdentity
}

func (authority *hermesSupervisorAuthority) Close() error {
	if authority == nil {
		return nil
	}
	if authority.standalone != nil {
		return authority.standalone.Close()
	}

	return errors.Join(authority.identity.Close(), authority.domain.Close())
}

func acquireHermesSupervisorAuthority(
	config hermesSupervisorConfig,
	identityFD uintptr,
	domainFD uintptr,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*hermesSupervisorAuthority, error) {
	if !config.IdentityLock && config.Isolation.TestOnlyNoCredential &&
		config.Isolation.StandaloneOwnerID == "" && config.Isolation.StandaloneStateRoot == "" {
		return &hermesSupervisorAuthority{
			identity: &agentIdentityLock{}, domain: &agentIdentityLock{},
		}, nil
	}
	if config.IdentityLock {
		testAuthority := config.Isolation.TestOnlyNoCredential || config.Isolation.TestOnlyIdentityLockRoot != ""
		identity, err := adoptAgentIdentityLock(
			supervisorNewFile(identityFD, "hermes-agent-identity-lock"), config.Isolation.UID,
			testAuthority, config.Isolation.TestOnlyIdentityLockRoot,
		)
		if err != nil {
			return nil, err
		}
		domain, err := adoptAgentAuthorityDomain(
			supervisorNewFile(domainFD, "hermes-agent-authority-domain"),
			testAuthority, config.Isolation.TestOnlyIdentityLockRoot,
		)
		if err != nil {
			return nil, errors.Join(err, identity.Close())
		}
		if config.StandaloneAuthority {
			err = validateInheritedStandaloneAgentIdentityDisposition(
				config.Isolation.UID,
				config.Isolation.GID,
				config.Isolation.StandaloneOwnerID,
				config.Isolation.StandaloneStateRoot,
				testAuthority,
				config.Isolation.TestOnlyIdentityLockRoot,
			)
		} else {
			err = validateBorrowedAgentIdentityDisposition(
				config.Isolation.UID,
				config.Isolation.GID,
				testAuthority,
				config.Isolation.TestOnlyIdentityLockRoot,
			)
		}
		if err != nil {
			return nil, errors.Join(err, identity.Close(), domain.Close())
		}

		return &hermesSupervisorAuthority{identity: identity, domain: domain}, nil
	}
	standalone, err := supervisorAcquireStandalone(
		config.Isolation.UID, config.Isolation.GID, config.Isolation.StandaloneOwnerID,
		config.Isolation.StandaloneStateRoot, config.Isolation.TestOnlyNoCredential,
		config.Isolation.TestOnlyIdentityLockRoot, canceled, signals,
	)
	if err != nil {
		return nil, err
	}

	return &hermesSupervisorAuthority{
		identity: standalone.identity, domain: standalone.authority, standalone: standalone,
	}, nil
}

func runHermesProcessSupervisorGuardian(config hermesSupervisorConfig, control *os.File, proof *os.File) (exitCode int) {
	defer control.Close()
	defer proof.Close()
	shutdown := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, control)
		close(shutdown)
	}()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	authority, err := acquireHermesSupervisorAuthority(
		config, supervisorIdentityLockFD, supervisorAuthorityFD, shutdown, signals,
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "acp-go-hermes trusted supervisor: %v\n", err)
		_ = completeHermesSupervisorProof(proof)

		return 125
	}
	liveness, status, peer, err := startHermesSupervisorLiveness(config, control, proof, authority)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "acp-go-hermes trusted supervisor: %v\n", err)
		_ = completeHermesSupervisorAuthority(&authority, nil, nil, proof, false)

		return 125
	}
	defer status.Close()
	defer peer.Close()
	wait := make(chan error, 1)
	go func() { wait <- liveness.Wait() }()
	reader := bufio.NewReader(status)
	if err = status.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = peer.Close()
		<-wait
		_ = completeHermesSupervisorAuthority(&authority, nil, nil, proof, false)

		return 125
	}
	line, err := reader.ReadString('\n')
	if err != nil || !validHermesLivenessReadiness(line) {
		_ = peer.Close()
		<-wait
		_ = completeHermesSupervisorAuthority(&authority, nil, nil, proof, false)

		return 125
	}
	_ = status.SetReadDeadline(time.Time{})

	var waitErr error
	for {
		select {
		case waitErr = <-wait:
			goto livenessExited
		case <-shutdown:
			_ = peer.Close()
			waitErr = <-wait
			goto livenessExited
		case received := <-signals:
			if nativeSignal, ok := received.(syscall.Signal); ok {
				_ = signalLeaseGroup(liveness.Process.Pid, nativeSignal)
			}
		}
	}

livenessExited:
	done, doneErr := reader.ReadString('\n')
	if doneErr == nil && done == "done\n" {
		if completeHermesSupervisorAuthority(&authority, nil, nil, proof, false) != nil {
			return 125
		}

		return hermesSupervisorExitCode(waitErr)
	}
	_ = completeHermesSupervisorAuthority(&authority, nil, nil, proof, false)

	return hermesSupervisorExitCode(waitErr)
}

func startHermesSupervisorLiveness(
	config hermesSupervisorConfig,
	control *os.File,
	proof *os.File,
	authority *hermesSupervisorAuthority,
) (*exec.Cmd, *os.File, *os.File, error) {
	borrowed := authority.identity != nil && authority.identity.file != nil && authority.domain != nil && authority.domain.file != nil
	config.IdentityLock = borrowed
	config.AuthorityDomain = borrowed
	config.StandaloneAuthority = authority.standalone != nil
	config.Isolation.IdentityLock = nil
	config.Isolation.AuthorityDomain = nil
	if !config.StandaloneAuthority {
		config.Isolation.StandaloneOwnerID = ""
		config.Isolation.StandaloneStateRoot = ""
	}
	configFD, err := supervisorMemfd(supervisorConfigName+"-liveness", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, nil, nil, err
	}
	configFile := os.NewFile(uintptr(configFD), supervisorConfigName+"-liveness")
	if err = writeHermesSupervisorConfig(configFile, config); err != nil {
		_ = configFile.Close()

		return nil, nil, nil, err
	}
	if _, err = supervisorSealConfig(
		configFile.Fd(), unix.F_ADD_SEALS,
		unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL,
	); err != nil {
		_ = configFile.Close()

		return nil, nil, nil, err
	}
	statusRead, statusWrite, err := supervisorPipe()
	if err != nil {
		_ = configFile.Close()

		return nil, nil, nil, err
	}
	peerRead, peerWrite, err := supervisorPipe()
	if err != nil {
		_ = configFile.Close()
		_ = statusRead.Close()
		_ = statusWrite.Close()

		return nil, nil, nil, err
	}
	identity, err := duplicateHermesSupervisorAuthority(authority.identity, borrowed)
	if err != nil {
		closeHermesLivenessFiles(configFile, statusRead, statusWrite, peerRead, peerWrite)

		return nil, nil, nil, err
	}
	domain, err := duplicateHermesSupervisorAuthority(authority.domain, borrowed)
	if err != nil {
		_ = identity.Close()
		closeHermesLivenessFiles(configFile, statusRead, statusWrite, peerRead, peerWrite)

		return nil, nil, nil, err
	}
	self, err := supervisorExecutable()
	if err != nil {
		_ = identity.Close()
		_ = domain.Close()
		closeHermesLivenessFiles(configFile, statusRead, statusWrite, peerRead, peerWrite)

		return nil, nil, nil, err
	}
	liveness := supervisorCommand(self)
	liveness.Dir = "/"
	liveness.Env = []string{envHermesSupervisor + "=" + hermesSupervisorLiveness}
	liveness.Stdin = os.Stdin
	liveness.Stdout = os.Stdout
	liveness.Stderr = os.Stderr
	liveness.ExtraFiles = []*os.File{configFile, control, statusWrite, identity, domain, proof, peerRead}
	liveness.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = liveness.Start(); err != nil {
		_ = identity.Close()
		_ = domain.Close()
		closeHermesLivenessFiles(configFile, statusRead, statusWrite, peerRead, peerWrite)

		return nil, nil, nil, err
	}
	for _, file := range []*os.File{configFile, statusWrite, identity, domain, peerRead} {
		_ = file.Close()
	}

	return liveness, statusRead, peerWrite, nil
}

func duplicateHermesSupervisorAuthority(lock *agentIdentityLock, borrowed bool) (*os.File, error) {
	if borrowed {
		return lock.Duplicate()
	}

	return os.Open("/dev/null")
}

func closeHermesLivenessFiles(files ...*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}

func validHermesLivenessReadiness(line string) bool {
	text, ok := strings.CutSuffix(line, "\n")
	if !ok {
		return false
	}
	pidText, ok := strings.CutPrefix(text, "ready:")
	if !ok {
		return false
	}
	pid, err := strconv.Atoi(pidText)

	return err == nil && pid > 0
}

func hermesSupervisorExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() >= 0 {
		return exitErr.ExitCode()
	}

	return 1
}

func completeHermesSupervisorProof(proof io.Writer) error {
	awaitHermesSupervisorContainment()

	return writeHermesSupervisorProof(proof)
}

func awaitHermesSupervisorContainment() {
	for {
		if err := proveSupervisorDescendants(5 * time.Second); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}

func writeHermesSupervisorProof(proof io.Writer) error {
	if _, err := proof.Write([]byte{1}); err != nil {
		return err
	}

	return nil
}

func completeHermesSupervisorAuthority(
	authority **hermesSupervisorAuthority,
	guardianDone <-chan struct{},
	status io.Writer,
	proof io.Writer,
	livenessProtocol bool,
) error {
	if authority == nil || *authority == nil {
		return errors.New("Hermes supervisor authority is unavailable at completion")
	}
	awaitHermesSupervisorContainment()
	closeErr := (*authority).Close()
	*authority = nil
	if closeErr != nil {
		return closeErr
	}
	if livenessProtocol {
		select {
		case <-guardianDone:
			return writeHermesSupervisorProof(proof)
		default:
			if _, err := io.WriteString(status, "done\n"); err != nil {
				return fmt.Errorf("publish Hermes liveness completion: %w", err)
			}

			return nil
		}
	}

	return writeHermesSupervisorProof(proof)
}

func runHermesProcessSupervisorNative(
	config hermesSupervisorConfig,
	controlInputs []io.Reader,
	guardianPeer *os.File,
	status io.Writer,
	proof io.Writer,
	livenessProtocol bool,
) (exitCode int) {
	shutdown := make(chan struct{})
	var shutdownOnce sync.Once
	for _, controlInput := range controlInputs {
		go func(input io.Reader) {
			_, _ = io.Copy(io.Discard, input)
			shutdownOnce.Do(func() { close(shutdown) })
		}(controlInput)
	}
	guardianDone := make(chan struct{})
	if guardianPeer != nil {
		go func() {
			_, _ = io.Copy(io.Discard, guardianPeer)
			close(guardianDone)
			shutdownOnce.Do(func() { close(shutdown) })
		}()
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	authority, err := acquireHermesSupervisorAuthority(
		config, supervisorIdentityLockFD, supervisorAuthorityFD, shutdown, signals,
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "acp-go-hermes trusted supervisor: acquire native authority: %v\n", err)

		return 125
	}
	target := hermesSupervisorTarget(config)
	nativeIsolation := config.Isolation
	nativeIsolation.IdentityLock = authority.identity
	nativeIsolation.AuthorityDomain = authority.domain
	nativeIsolation.StandaloneOwnerID = ""
	nativeIsolation.StandaloneStateRoot = ""
	var guardianErr error
	targetDone, privilegeErr, startErr := startHermesSupervisorTarget(target, &nativeIsolation, func() error {
		guardianErr = validateHermesSupervisorGuardianPeer(guardianPeer, guardianDone)

		return guardianErr
	})
	if guardianErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "acp-go-hermes trusted supervisor: guardian peer: %v\n", guardianErr)
		if completeHermesSupervisorAuthority(&authority, guardianDone, status, proof, false) != nil {
			return 126
		}

		return 125
	}
	if privilegeErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "acp-go-hermes trusted supervisor: prepare native target: %v\n", privilegeErr)
		if completeHermesSupervisorAuthority(
			&authority, guardianDone, status, proof, livenessProtocol,
		) != nil {
			return 126
		}

		return 125
	}
	if startErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "acp-go-hermes trusted supervisor: start native target: %v\n", startErr)
		if completeHermesSupervisorAuthority(
			&authority, guardianDone, status, proof, livenessProtocol,
		) != nil {
			return 126
		}

		return 125
	}
	if livenessProtocol {
		if _, err = fmt.Fprintf(status, "ready:%d\n", target.Process.Pid); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "acp-go-hermes trusted supervisor: publish native readiness: %v\n", err)
			_, _ = stopSupervisorDescendants(target.Process.Pid, targetDone)
			if completeHermesSupervisorAuthority(
				&authority, guardianDone, status, proof, livenessProtocol,
			) != nil {
				return 126
			}

			return 125
		}
	}

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
	if completeHermesSupervisorAuthority(
		&authority, guardianDone, status, proof, livenessProtocol,
	) != nil {
		return 126
	}

	return hermesSupervisorExitCode(targetErr)
}

func validateHermesSupervisorGuardianPeer(peer *os.File, done <-chan struct{}) error {
	if peer == nil {
		return nil
	}
	select {
	case <-done:
		return errors.New("Hermes guardian exited before native launch")
	default:
	}
	poll := []unix.PollFd{{
		Fd: int32(peer.Fd()), Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR,
	}}
	ready, err := supervisorPoll(poll, 0)
	if err != nil {
		return fmt.Errorf("poll Hermes guardian before native launch: %w", err)
	}
	if ready != 0 || poll[0].Revents != 0 {
		return errors.New("Hermes guardian exited before native launch")
	}

	return nil
}

func hermesSupervisorTarget(config hermesSupervisorConfig) *exec.Cmd {
	target := exec.Command(config.Path, config.Args[1:]...) // #nosec G204,G702 -- target path and argv came through the private parent descriptor.
	target.Args = append([]string(nil), config.Args...)
	target.Dir = config.Dir
	target.Env = append([]string(nil), config.Env...)
	target.Stdin = os.Stdin
	target.Stdout = os.Stdout
	target.Stderr = os.Stderr
	target.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}

	return target
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
