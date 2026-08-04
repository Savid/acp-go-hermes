//go:build linux

package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestLinuxSupervisorKillsAndReapsDetachedStubbornDescendant(t *testing.T) {
	restoreLinuxSupervisorSeams(t)

	setsid, err := exec.LookPath("setsid")
	if err != nil {
		t.Skip("setsid is unavailable")
	}

	pidFile := filepath.Join(t.TempDir(), "detached.pid")
	sentinel := filepath.Join(t.TempDir(), "detached-survived")
	script := strconv.Quote(setsid) + " sh -c 'trap \"\" INT TERM; echo $$ > " + strconv.Quote(pidFile) + "; sleep 2; echo survived > " + strconv.Quote(sentinel) + "; while :; do sleep 30; done' & wait"
	cmd := exec.Command("sh", "-c", script)
	configureHermesProcess(cmd)
	tree, err := startContainedProcess(cmd, ContainmentSpec{Isolation: testProcessIsolation()})
	if err != nil {
		t.Fatalf("start supervised process: %v", err)
	}
	process := &Process{Cmd: cmd, tree: tree}
	process.beginWait()
	t.Cleanup(func() { _ = process.Close(context.Background()) })

	detachedPID := waitForPIDFile(t, pidFile)
	pgid, err := syscall.Getpgid(detachedPID)
	if err != nil {
		t.Fatalf("detached descendant pgid: %v", err)
	}
	if pgid == cmd.Process.Pid {
		t.Fatalf("descendant %d did not detach from supervisor pgid %d", detachedPID, cmd.Process.Pid)
	}
	detachedSID, err := unix.Getsid(detachedPID)
	if err != nil {
		t.Fatalf("detached descendant sid: %v", err)
	}
	supervisorSID, err := unix.Getsid(cmd.Process.Pid)
	if err != nil {
		t.Fatalf("supervisor sid: %v", err)
	}
	if detachedSID == supervisorSID || detachedSID != detachedPID || pgid != detachedPID {
		t.Fatalf("descendant identity pid/pgid/sid=%d/%d/%d supervisor sid=%d", detachedPID, pgid, detachedSID, supervisorSID)
	}
	if count, ok := process.ProviderDescendantCount(); !ok || count < 2 {
		t.Fatalf("supervised descendant inventory = %d/%v", count, ok)
	}
	originalList := listSupervisorDescendants
	listSupervisorDescendants = func(int) (map[int]byte, error) { return nil, errors.New("inventory") }
	if count, ok := process.ProviderDescendantCount(); ok || count != 0 {
		t.Fatalf("failed supervised descendant inventory = %d/%v", count, ok)
	}
	listSupervisorDescendants = originalList

	// Close the containment lease directly first to exercise the same
	// idempotent shutdown path used by process ownership cleanup.
	if err := tree.close(); err != nil {
		t.Fatalf("close supervisor lease: %v", err)
	}
	if err := process.Close(context.Background()); err != nil {
		t.Fatalf("close supervised process: %v", err)
	}
	if processAlive(detachedPID) {
		t.Fatalf("detached descendant %d survived proved Close", detachedPID)
	}
	time.Sleep(2200 * time.Millisecond)
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("detached descendant reached delayed side effect: %v", err)
	}
}

func TestLinuxSupervisorPreservesCommandEnvironmentSemantics(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	t.Setenv("SUPERVISOR_INHERITED_ENV_MARKER", "expected")

	tests := []struct {
		name   string
		script string
		env    []string
	}{
		{name: "inherited", script: `test "$SUPERVISOR_INHERITED_ENV_MARKER" = expected`},
		{
			name:   "explicit",
			script: `test -z "$SUPERVISOR_INHERITED_ENV_MARKER" && test "$SUPERVISOR_EXPLICIT_ENV_MARKER" = expected`,
			env:    []string{"SUPERVISOR_EXPLICIT_ENV_MARKER=expected"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := exec.Command("sh", "-c", test.script)
			cmd.Env = test.env
			configureHermesProcess(cmd)
			tree, err := startContainedProcess(cmd, ContainmentSpec{Isolation: testProcessIsolation()})
			if err != nil {
				t.Fatalf("start supervised process: %v", err)
			}
			t.Cleanup(func() { _ = tree.close() })

			wait := tree.directChild(cmd)
			select {
			case <-wait.done:
			case <-time.After(5 * time.Second):
				t.Fatal("supervised process did not exit")
			}
			if wait.err != nil {
				t.Fatalf("supervised process: %v", wait.err)
			}
			if err := tree.complete(5 * time.Second); err != nil {
				t.Fatalf("complete supervised process: %v", err)
			}
		})
	}
}

func TestLinuxSupervisorNativeChildHasSecurityLimits(t *testing.T) {
	const (
		phaseEnv  = "ACP_GO_HERMES_TEST_NO_NEW_PRIVS_PHASE"
		statusEnv = "ACP_GO_HERMES_TEST_NO_NEW_PRIVS_STATUS"
	)
	if os.Getenv(phaseEnv) == "child" {
		restoreLinuxSupervisorSeams(t)
		status := os.Getenv(statusEnv)
		script := `nnp=$(awk '$1 == "NoNewPrivs:" { print $2 }' /proc/self/status); printf '%s %s\n' "$nnp" "$(ulimit -c)" > "$1"`
		code, proof := runSupervisorCoreTest(t, []string{"/bin/sh", "-c", script, "nnp", status}, nil)
		if code != 0 || proof != 1 {
			t.Fatalf("native proof code/value = %d/%d", code, proof)
		}

		return
	}

	restoreLinuxSupervisorSeams(t)
	status := filepath.Join(t.TempDir(), "security-limits")
	process := exec.Command(os.Args[0], "-test.run=^TestLinuxSupervisorNativeChildHasSecurityLimits$")
	process.Env = append(os.Environ(), phaseEnv+"=child", statusEnv+"="+status)
	if output, err := process.CombinedOutput(); err != nil {
		t.Fatalf("native proof process: %v\n%s", err, output)
	}
	if value, err := os.ReadFile(status); err != nil || string(value) != "1 0\n" {
		t.Fatalf("native security limits = %q, %v", value, err)
	}
}

func TestTrustedSupervisorRejectsNativeSignalsProofForgeryAndDaemonEscape(t *testing.T) {
	const phaseEnv = "ACP_GO_HERMES_TEST_TRUSTED_SUPERVISOR_PHASE"
	if os.Geteuid() != 0 {
		t.Skip("trusted supervisor credential boundary requires root")
	}

	root := os.Getenv("ACP_GO_HERMES_TEST_ROOT")
	if root == "" {
		root = t.TempDir()
	}
	statusRoot := filepath.Join(root, "native")
	if os.Getenv(phaseEnv) != "child" {
		if err := os.Chmod(root, 0o711); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(statusRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(statusRoot, 65534, 65534); err != nil {
			t.Fatal(err)
		}
	}
	status := filepath.Join(statusRoot, "status")
	daemon := filepath.Join(statusRoot, "daemon.pid")
	proof := filepath.Join(root, "proof")

	if os.Getenv(phaseEnv) == "child" {
		if err := supervisorPrctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
		if err := supervisorPrctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
		proofFile, err := os.OpenFile(proof, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer proofFile.Close()
		unix.CloseOnExec(int(proofFile.Fd()))
		controlRead, controlWrite, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer controlRead.Close()
		defer controlWrite.Close()
		script := `supervisor=$PPID
if kill -STOP "$supervisor" 2>/dev/null; then echo stop=allowed; else echo stop=blocked; fi > "$1"
if printf 'forged\n' > "/proc/$supervisor/fd/$3" 2>/dev/null; then echo forge=allowed; else echo forge=blocked; fi >> "$1"
setsid sh -c 'trap "" INT TERM; while :; do sleep 30; done' & echo $! > "$2"
if kill -KILL "$supervisor" 2>/dev/null; then echo kill=allowed; else echo kill=blocked; fi >> "$1"`
		config := hermesSupervisorConfig{
			Path:      "/bin/sh",
			Args:      []string{"sh", "-c", script, "probe", status, daemon, strconv.Itoa(int(proofFile.Fd()))},
			Env:       []string{"PATH=/usr/bin:/bin"},
			Isolation: ProcessIsolation{UID: 65534, GID: 65534},
		}
		if code := runHermesProcessSupervisorCore(config, controlRead, proofFile); code != 0 {
			t.Fatalf("trusted supervisor code = %d", code)
		}
		return
	}

	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTrustedSupervisorRejectsNativeSignalsProofForgeryAndDaemonEscape$")
	child.Env = append(os.Environ(), phaseEnv+"=child", "ACP_GO_HERMES_TEST_ROOT="+root)
	child.Dir = root
	go func() {
		time.Sleep(500 * time.Millisecond)
		if child.Process != nil {
			_ = child.Process.Signal(syscall.SIGCONT)
		}
	}()
	output, err := child.CombinedOutput()
	t.Cleanup(func() {
		pidBytes, readErr := os.ReadFile(daemon)
		if readErr == nil {
			pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	if err != nil {
		t.Fatalf("trusted supervisor helper: %v\n%s", err, output)
	}
	result, err := os.ReadFile(status)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "stop=blocked\nforge=blocked\nkill=blocked\n" {
		t.Fatalf("native attack results = %q", result)
	}
	if pidBytes, err := os.ReadFile(daemon); err == nil {
		pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		if pid > 0 {
			if process, findErr := os.FindProcess(pid); findErr == nil && process.Signal(syscall.Signal(0)) == nil {
				t.Fatalf("daemonized native descendant %d survived containment", pid)
			}
		}
	}
}

func TestLinuxProcessCloseFallbackResultTracksContainmentProof(t *testing.T) {
	tests := []struct {
		name     string
		proofErr error
		wantErr  bool
	}{
		{name: "proved", wantErr: false},
		{
			name:     "incomplete",
			proofErr: errors.New("descendant remains"),
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreProcessSeams(t)

			releaseWait := make(chan struct{})
			waitProcessCommand = func(*exec.Cmd) error {
				<-releaseWait

				return nil
			}

			process := &Process{
				Cmd: fakeStartedCommand(),
				tree: &processContainment{
					terminateFn: func() error { return nil },
					killFn: func() error {
						close(releaseWait)

						return nil
					},
					completeFn: func(time.Duration) error { return test.proofErr },
				},
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			err := process.Close(ctx)
			if test.wantErr {
				if !errors.Is(err, ErrProcessContainmentIncomplete) {
					t.Fatalf("Close error = %v, want incomplete containment", err)
				}

				return
			}
			if err != nil {
				t.Fatalf("Close after proved fallback = %v", err)
			}
		})
	}
}

func TestLinuxSupervisorCoreExitShutdownAndSignal(t *testing.T) {
	t.Run("core-limit failure", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		supervisorSetrlimit = func(int, *unix.Rlimit) error { return errors.New("setrlimit") }
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "exit 0"}, nil)
		if code != 125 || proof != 0 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("no new privileges failure", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		supervisorPrctl = func(option int, _, _, _, _ uintptr) error {
			if option != unix.PR_SET_NO_NEW_PRIVS {
				t.Fatalf("privilege operation = %d", option)
			}

			return errors.New("no-new-privs")
		}
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "exit 0"}, nil)
		if code != 125 || proof != 0 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("clean target exit", func(t *testing.T) {
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "exit 0"}, nil)
		if code != 0 || proof != 1 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("target exit code", func(t *testing.T) {
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "exit 7"}, nil)
		if code != 7 || proof != 1 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("target start failure", func(t *testing.T) {
		code, proof := runSupervisorCoreTest(t, []string{"missing", "arg"}, nil)
		if code != 125 || proof != 0 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("control closes stubborn target", func(t *testing.T) {
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", `trap "" TERM; while :; do sleep 30; done`}, func(control *os.File) {
			_ = control.Close()
		})
		if code == 0 || proof != 1 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("signal stops target", func(t *testing.T) {
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "while :; do sleep 30; done"}, func(*os.File) {
			time.Sleep(20 * time.Millisecond)
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				t.Errorf("signal supervisor core: %v", err)
			}
		})
		if code == 0 || proof != 1 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("proof write failure", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		supervisorAcquireLock = func(uint32, bool, <-chan struct{}, <-chan os.Signal) (*agentIdentityLock, error) {
			return &agentIdentityLock{}, nil
		}
		controlRead, controlWrite, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer controlWrite.Close()
		proofRead, proofWrite, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_ = proofRead.Close()
		_ = proofWrite.Close()
		config := supervisorTestConfig([]string{"sh", "-c", "exit 0"})
		if code := runHermesProcessSupervisorCore(config, controlRead, proofWrite); code != 126 {
			t.Fatalf("proof write failure code = %d", code)
		}
	})

	t.Run("proof failure", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		attempts := 0
		proveSupervisorDescendants = func(time.Duration) error {
			attempts++
			if attempts == 1 {
				return errors.New("proof")
			}
			return nil
		}
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "exit 0"}, nil)
		if code != 0 || proof != 1 || attempts != 2 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("plain stop failure", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		stopSupervisorDescendants = func(targetPID int, _ <-chan error) (error, bool) {
			_ = signalPIDFD(targetPID, syscall.SIGKILL)

			return errors.New("stop"), true
		}
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "while :; do sleep 30; done"}, func(control *os.File) {
			_ = control.Close()
		})
		if code != 1 || proof != 1 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("unsettled root fails without proof", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		stopSupervisorDescendants = func(targetPID int, _ <-chan error) (error, bool) {
			_ = signalPIDFD(targetPID, syscall.SIGKILL)

			return errors.New("unsettled"), false
		}
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "while :; do sleep 30; done"}, func(control *os.File) {
			_ = control.Close()
		})
		if code == 0 || proof != 1 {
			t.Fatalf("unsettled core result code/proof = %d/%d", code, proof)
		}
	})
}

func TestLinuxSupervisorHelpersAndStartValidation(t *testing.T) {
	if parent, state, ok := supervisorProcStat("1 (name with spaces) S 42 0 0"); !ok || parent != 42 || state != 'S' {
		t.Fatalf("parsed proc stat = %d/%q/%v", parent, state, ok)
	}
	for _, stat := range []string{"missing", "1 (x) ", "1 (x) SS 1", "1 (x) S nope"} {
		if _, _, ok := supervisorProcStat(stat); ok {
			t.Fatalf("accepted malformed proc stat %q", stat)
		}
	}

	descendants, err := supervisorDescendants(os.Getpid())
	if err != nil {
		t.Fatalf("scan descendants: %v", err)
	}
	_ = descendantPIDs(descendants)
	if err := signalPIDFD(os.Getpid(), 0); err != nil {
		t.Fatalf("pidfd identity signal: %v", err)
	}
	if err := signalPIDFD(1<<30, 0); err != nil {
		t.Fatalf("gone pidfd signal: %v", err)
	}
	if err := signalPIDFD(-1, 0); err == nil {
		t.Fatal("negative pidfd unexpectedly succeeded")
	}
	if err := signalSupervisorDescendants(0); err != nil {
		t.Fatalf("signal empty descendants: %v", err)
	}
	_, _ = reapSupervisorChildren()

	if _, err := startUnixContainedProcess(nil, ContainmentSpec{}); err == nil {
		t.Fatal("nil target accepted")
	}
	if _, err := startUnixContainedProcess(exec.Command("sh", "-c", "exit 0"), ContainmentSpec{}); err == nil {
		t.Fatal("unconfigured target accepted")
	}
	missing := exec.Command(filepath.Join(t.TempDir(), "missing"))
	configureHermesProcess(missing)
	if _, err := startUnixContainedProcess(missing, ContainmentSpec{}); err == nil {
		t.Fatal("missing target accepted")
	}
	extra := exec.Command("sh", "-c", "exit 0")
	configureHermesProcess(extra)
	extra.ExtraFiles = []*os.File{os.Stdin}
	if _, err := startUnixContainedProcess(extra, ContainmentSpec{}); err == nil {
		t.Fatal("target ExtraFiles accepted")
	}

	if err := proveAndReapSupervisorDescendants(time.Millisecond); err != nil {
		t.Fatalf("prove empty test descendants: %v", err)
	}
}

func TestLinuxSupervisorInitWrapperAndFailureSeams(t *testing.T) {
	restoreLinuxSupervisorSeams(t)

	exitCode := -1
	supervisorExit = func(code int) { exitCode = code }
	t.Setenv(envHermesSupervisor, "1")
	runHermesSupervisorInit()
	if exitCode != 125 {
		t.Fatalf("supervisor init exit = %d, want 125", exitCode)
	}

	supervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error { return errors.New("prctl") }
	if code := runHermesProcessSupervisor(); code != 125 {
		t.Fatalf("prctl failure code = %d", code)
	}

	supervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error { return nil }
	supervisorPIDFDOpen = func(int, int) (int, error) { return -1, errors.New("pidfd") }
	if code := runHermesProcessSupervisor(); code != 125 {
		t.Fatalf("pidfd failure code = %d", code)
	}

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer devNull.Close()
	supervisorPIDFDOpen = func(int, int) (int, error) { return syscall.Dup(int(devNull.Fd())) }
	configRead, configWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(configWrite).Encode(supervisorTestConfig([]string{"sh", "-c", "exit 0"})); err != nil {
		t.Fatal(err)
	}
	_ = configWrite.Close()
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	proofRead, proofWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	files := []*os.File{configRead, controlRead, proofWrite}
	supervisorNewFile = func(uintptr, string) *os.File {
		file := files[0]
		files = files[1:]

		return file
	}
	supervisorCloseOnExec = func(int) {}
	supervisorAcquireLock = func(uint32, bool, <-chan struct{}, <-chan os.Signal) (*agentIdentityLock, error) {
		return &agentIdentityLock{}, nil
	}
	if code := runHermesProcessSupervisor(); code != 0 {
		t.Fatalf("supervisor wrapper code = %d", code)
	}
	_ = controlWrite.Close()
	_ = proofWrite.Close()
	var proof [1]byte
	_, _ = proofRead.Read(proof[:])
	_ = proofRead.Close()
	if proof[0] != 1 {
		t.Fatalf("supervisor wrapper proof = %d", proof[0])
	}

	supervisorExecutable = func() (string, error) { return "", errors.New("executable") }
	configured := exec.Command("sh", "-c", "exit 0")
	configureHermesProcess(configured)
	containment := ContainmentSpec{Isolation: testProcessIsolation()}
	if _, err := startUnixContainedProcess(configured, containment); err == nil || !strings.Contains(err.Error(), "resolve Hermes supervisor executable") {
		t.Fatalf("executable error = %v", err)
	}

	supervisorExecutable = os.Executable
	supervisorPipe = func() (*os.File, *os.File, error) { return nil, nil, errors.New("pipe") }
	if _, err := startUnixContainedProcess(configured, containment); err == nil || !strings.Contains(err.Error(), "control pipe") {
		t.Fatalf("control pipe error = %v", err)
	}

	pipeCalls := 0
	supervisorPipe = func() (*os.File, *os.File, error) {
		pipeCalls++
		if pipeCalls == 2 {
			return nil, nil, errors.New("proof pipe")
		}

		return os.Pipe()
	}
	if _, err := startUnixContainedProcess(configured, containment); err == nil || !strings.Contains(err.Error(), "proof pipe") {
		t.Fatalf("proof pipe error = %v", err)
	}

	supervisorPipe = os.Pipe
	supervisorExecutable = func() (string, error) { return filepath.Join(t.TempDir(), "missing-supervisor"), nil }
	if _, err := startUnixContainedProcess(configured, containment); err == nil {
		t.Fatal("supervisor start failure was ignored")
	}
}

func TestLinuxSupervisorErrorAndFallbackBranches(t *testing.T) { //nolint:gocyclo // Explicitly audits every fail-closed supervisor seam.
	restoreLinuxSupervisorSeams(t)

	proof := make(chan bool)
	tree := &processContainment{processGroupID: 123, proof: proof}
	if err := tree.complete(time.Millisecond); err == nil || !strings.Contains(err.Error(), "did not prove") {
		t.Fatalf("proof timeout error = %v", err)
	}

	proof = make(chan bool, 1)
	proof <- true
	processKill = func(int, syscall.Signal) error { return errors.New("probe") }
	tree = &processContainment{processGroupID: 123, proof: proof}
	if err := tree.complete(time.Second); err == nil || !strings.Contains(err.Error(), "did not become quiescent") {
		t.Fatalf("proof group error = %v", err)
	}

	called := false
	tree = &processContainment{
		descendantCountFn: func() (int, bool) { return 3, true },
		terminateFn: func() error {
			called = true

			return nil
		},
		killFn: func() error {
			called = true

			return nil
		},
		closeFn: func() error {
			called = true

			return nil
		},
	}
	if count, ok := tree.descendantCount(); count != 3 || !ok {
		t.Fatalf("custom descendant inventory = %d/%v", count, ok)
	}
	if err := tree.terminate(nil); err != nil {
		t.Fatal(err)
	}
	if err := tree.kill(nil); err != nil {
		t.Fatal(err)
	}
	if err := tree.close(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("custom containment hooks were not called")
	}
	if err := (&processContainment{}).kill(nil); err != nil {
		t.Fatalf("fallback kill: %v", err)
	}
	if err := (&processContainment{}).terminate(nil); err != nil {
		t.Fatalf("fallback terminate: %v", err)
	}
	if err := (&processContainment{}).close(); err != nil {
		t.Fatalf("fallback close: %v", err)
	}

	aliveCalls := 0
	processKill = func(int, syscall.Signal) error {
		aliveCalls++
		if aliveCalls == 1 {
			return nil
		}

		return syscall.ESRCH
	}
	if err := (&processContainment{processGroupID: 123}).waitUntilEmpty(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("wait for transient process group: %v", err)
	}

	listSupervisorDescendants = func(int) (map[int]byte, error) { return nil, errors.New("scan") }
	if err := proveAndReapSupervisorDescendants(time.Millisecond); err == nil {
		t.Fatal("prove ignored scan error")
	}
	if err := signalSupervisorDescendants(0); err == nil {
		t.Fatal("signal ignored scan error")
	}

	listSupervisorDescendants = func(int) (map[int]byte, error) { return map[int]byte{123: 'Z'}, nil }
	supervisorWait4 = func(int, *syscall.WaitStatus, int, *syscall.Rusage) (int, error) { return 0, nil }
	if err := proveAndReapSupervisorDescendants(0); err == nil || !strings.Contains(err.Error(), "retained descendants") {
		t.Fatalf("retained descendant error = %v", err)
	}
	if err := signalSupervisorDescendants(0); err != nil {
		t.Fatalf("skip zombie signal: %v", err)
	}

	descendantScans := 0
	listSupervisorDescendants = func(int) (map[int]byte, error) {
		descendantScans++
		if descendantScans == 1 {
			return map[int]byte{1 << 30: 'S'}, nil
		}

		return nil, nil //nolint:nilnil // An empty successful scan completes the transient-descendant seam.
	}
	supervisorWait4 = func(int, *syscall.WaitStatus, int, *syscall.Rusage) (int, error) {
		if descendantScans == 1 {
			return 0, nil
		}

		return -1, syscall.ECHILD
	}
	supervisorPollInterval = 0
	if err := proveAndReapSupervisorDescendants(time.Second); err != nil {
		t.Fatalf("prove transient descendant: %v", err)
	}

	listSupervisorDescendants = func(int) (map[int]byte, error) { return map[int]byte{123: 'S'}, nil }
	supervisorPIDFDOpen = func(int, int) (int, error) { return -1, errors.New("open") }
	if err := signalSupervisorDescendants(syscall.SIGTERM); err == nil {
		t.Fatal("signal ignored pidfd error")
	}

	supervisorPIDFDOpen = func(int, int) (int, error) { return 10, nil }
	supervisorPIDFDSendSignal = func(int, syscall.Signal, *unix.Siginfo, int) error { return errors.New("send") }
	if err := signalPIDFD(123, syscall.SIGTERM); err == nil {
		t.Fatal("pidfd send error was ignored")
	}
	supervisorPIDFDSendSignal = func(int, syscall.Signal, *unix.Siginfo, int) error { return syscall.ESRCH }
	if err := signalPIDFD(123, syscall.SIGTERM); err != nil {
		t.Fatalf("gone pidfd send: %v", err)
	}

	supervisorReadDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir") }
	if _, err := supervisorDescendants(1); err == nil {
		t.Fatal("descendant scan ignored readdir error")
	}

	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	supervisorReadDir = func(string) ([]os.DirEntry, error) { return entries, nil }
	supervisorReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }
	if _, err := supervisorDescendants(1); err == nil {
		t.Fatal("descendant scan ignored stat read error")
	}
	supervisorReadFile = func(string) ([]byte, error) { return nil, os.ErrNotExist }
	if descendants, err := supervisorDescendants(1); err != nil || len(descendants) != 0 {
		t.Fatalf("gone proc scan = %#v err=%v", descendants, err)
	}

	supervisorTermGrace = 0
	supervisorKillGrace = 0
	if err, settled := stopSupervisedDescendants(123, make(chan error)); err == nil || settled || !strings.Contains(err.Error(), "did not exit") {
		t.Fatalf("stop timeout result = %v/%v", err, settled)
	}
}

func TestLinuxSupervisorKernelChildProofBranches(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	listSupervisorDescendants = func(int) (map[int]byte, error) { return map[int]byte{}, nil }
	supervisorPollInterval = 0

	waitResults := []struct {
		pid int
		err error
	}{
		{pid: -1, err: syscall.EINTR},
		{pid: 4321},
		{pid: -1, err: syscall.ECHILD},
	}
	waitCalls := 0
	supervisorWait4 = func(int, *syscall.WaitStatus, int, *syscall.Rusage) (int, error) {
		result := waitResults[waitCalls]
		waitCalls++

		return result.pid, result.err
	}
	if err := proveAndReapSupervisorDescendants(time.Second); err != nil || waitCalls != len(waitResults) {
		t.Fatalf("drain-to-ECHILD proof = %v after %d waits", err, waitCalls)
	}

	inventories := 0
	listSupervisorDescendants = func(int) (map[int]byte, error) {
		inventories++

		return map[int]byte{}, nil
	}
	supervisorWait4 = func(int, *syscall.WaitStatus, int, *syscall.Rusage) (int, error) {
		if inventories == 1 {
			return 0, nil
		}

		return -1, syscall.ECHILD
	}
	if err := proveAndReapSupervisorDescendants(time.Second); err != nil || inventories != 2 {
		t.Fatalf("running-child proof retry = %v after %d inventories", err, inventories)
	}

	want := errors.New("wait4")
	supervisorWait4 = func(int, *syscall.WaitStatus, int, *syscall.Rusage) (int, error) {
		return -1, want
	}
	if err := proveAndReapSupervisorDescendants(time.Second); !errors.Is(err, want) {
		t.Fatalf("wait4 error = %v", err)
	}

	supervisorWait4 = func(int, *syscall.WaitStatus, int, *syscall.Rusage) (int, error) {
		return -1, nil
	}
	if err := proveAndReapSupervisorDescendants(time.Second); err == nil || !strings.Contains(err.Error(), "invalid wait4 result") {
		t.Fatalf("invalid wait4 result = %v", err)
	}
}

func restoreLinuxSupervisorSeams(t *testing.T) {
	t.Helper()
	oldExit := supervisorExit
	oldExecutable := supervisorExecutable
	oldPipe := supervisorPipe
	oldCommand := supervisorCommand
	oldPrctl := supervisorPrctl
	oldSetrlimit := supervisorSetrlimit
	oldPIDFDOpen := supervisorPIDFDOpen
	oldPIDFDSendSignal := supervisorPIDFDSendSignal
	oldNewFile := supervisorNewFile
	oldCloseOnExec := supervisorCloseOnExec
	oldMemfd := supervisorMemfd
	oldSealConfig := supervisorSealConfig
	oldAcquireLock := supervisorAcquireLock
	oldReadDir := supervisorReadDir
	oldReadFile := supervisorReadFile
	oldWait4 := supervisorWait4
	oldList := listSupervisorDescendants
	oldStop := stopSupervisorDescendants
	oldProve := proveSupervisorDescendants
	oldTermGrace := supervisorTermGrace
	oldKillGrace := supervisorKillGrace
	oldPoll := supervisorPollInterval
	oldProcessKill := processKill
	t.Cleanup(func() {
		supervisorExit = oldExit
		supervisorExecutable = oldExecutable
		supervisorPipe = oldPipe
		supervisorCommand = oldCommand
		supervisorPrctl = oldPrctl
		supervisorSetrlimit = oldSetrlimit
		supervisorPIDFDOpen = oldPIDFDOpen
		supervisorPIDFDSendSignal = oldPIDFDSendSignal
		supervisorNewFile = oldNewFile
		supervisorCloseOnExec = oldCloseOnExec
		supervisorMemfd = oldMemfd
		supervisorSealConfig = oldSealConfig
		supervisorAcquireLock = oldAcquireLock
		supervisorReadDir = oldReadDir
		supervisorReadFile = oldReadFile
		supervisorWait4 = oldWait4
		listSupervisorDescendants = oldList
		stopSupervisorDescendants = oldStop
		proveSupervisorDescendants = oldProve
		supervisorTermGrace = oldTermGrace
		supervisorKillGrace = oldKillGrace
		supervisorPollInterval = oldPoll
		processKill = oldProcessKill
	})
}

func runSupervisorCoreTest(t *testing.T, args []string, trigger func(*os.File)) (int, byte) {
	t.Helper()
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	proofRead, proofWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	if trigger != nil {
		go trigger(controlWrite)
	}
	oldAcquire := supervisorAcquireLock
	supervisorAcquireLock = func(uint32, bool, <-chan struct{}, <-chan os.Signal) (*agentIdentityLock, error) {
		return &agentIdentityLock{}, nil
	}
	defer func() { supervisorAcquireLock = oldAcquire }()
	code := runHermesProcessSupervisorCore(supervisorTestConfig(args), controlRead, proofWrite)
	_ = controlWrite.Close()
	_ = proofWrite.Close()
	var proof [1]byte
	_, _ = proofRead.Read(proof[:])
	_ = proofRead.Close()

	return code, proof[0]
}

func supervisorTestConfig(args []string) hermesSupervisorConfig {
	return hermesSupervisorConfig{
		Path:      args[0],
		Args:      append([]string(nil), args...),
		Env:       os.Environ(),
		Isolation: ProcessIsolation{UID: 11, GID: 22, TestOnlyNoCredential: true},
	}
}
