//go:build linux

package hermes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type unavailableIdentityDispositionCapability struct{}

func (unavailableIdentityDispositionCapability) Duplicate() (*os.File, error) {
	return nil, errors.New("unavailable")
}

func TestSupervisorIdentityDispositionRejectsMixedCapabilities(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	isolation := testProcessIsolation()
	isolation.IdentityLock = unavailableIdentityDispositionCapability{}
	cmd := exec.Command("/bin/true")
	configureHermesProcess(cmd)
	_, err := startUnixContainedProcess(cmd, ContainmentSpec{Isolation: isolation})
	if err == nil || !strings.Contains(err.Error(), "must be provided together") {
		t.Fatalf("mixed identity disposition = %v", err)
	}
}

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

func TestProcessIsolationActualHermesTrustedSupervisorIdentityGroupsAmbientAndContainment(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("trusted supervisor credential boundary requires root")
	}
	setsid, err := exec.LookPath("setsid")
	if err != nil {
		t.Skip("setsid is unavailable")
	}

	const (
		uid = uint32(64581)
		gid = uint32(64582)
	)
	root, err := os.MkdirTemp("/var/lib", "acp-go-hermes-trusted-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err = os.Chmod(root, 0o711); err != nil {
		t.Fatal(err)
	}
	statusRoot := filepath.Join(root, "native")
	if err = os.Mkdir(statusRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.Chown(statusRoot, int(uid), int(gid)); err != nil {
		t.Fatal(err)
	}
	status := filepath.Join(statusRoot, "status")
	daemon := filepath.Join(statusRoot, "daemon.pid")
	wrappers := filepath.Join(statusRoot, "wrappers.pid")
	authorityRoot := filepath.Join(root, "acp-go", "agent-identities")
	t.Setenv("ACP_GO_HERMES_TEST_ACTUAL_AMBIENT", "secret")
	script := `liveness=$PPID
guardian=$(awk '$1 == "PPid:" { print $2 }' "/proc/$liveness/status")
printf '%s %s\n' "$liveness" "$guardian" > "$3"
if kill -STOP "$liveness" 2>/dev/null; then echo liveness-stop=allowed; kill -CONT "$liveness" 2>/dev/null || true; else echo liveness-stop=blocked; fi > "$1"
if kill -STOP "$guardian" 2>/dev/null; then echo guardian-stop=allowed; kill -CONT "$guardian" 2>/dev/null || true; else echo guardian-stop=blocked; fi >> "$1"
if printf 'forged\n' > "/proc/$liveness/fd/5" 2>/dev/null; then echo liveness-status-forge=allowed; else echo liveness-status-forge=blocked; fi >> "$1"
if printf 'forged\n' > "/proc/$liveness/fd/8" 2>/dev/null; then echo liveness-proof-forge=allowed; else echo liveness-proof-forge=blocked; fi >> "$1"
if printf 'forged\n' > "/proc/$guardian/fd/5" 2>/dev/null; then echo guardian-proof-forge=allowed; else echo guardian-proof-forge=blocked; fi >> "$1"
groups=$(sed -n 's/^Groups:[[:space:]]*//p' "/proc/$$/status")
if [ -z "$groups" ]; then echo groups=empty; else echo groups="$groups"; fi >> "$1"
echo uid=$(id -u) >> "$1"
echo gid=$(id -g) >> "$1"
if env | grep -q '^ACP_GO_HERMES_TEST_ACTUAL_AMBIENT='; then echo ambient=leaked; else echo ambient=scrubbed; fi >> "$1"
echo nnp=$(awk '$1 == "NoNewPrivs:" { print $2 }' /proc/self/status) >> "$1"
echo core=$(ulimit -c) >> "$1"
authorityfds=none
for fd in /proc/self/fd/*; do
  target=$(readlink "$fd" 2>/dev/null || true)
  case "$target" in "$5"*) authorityfds=leaked;; esac
done
echo authorityfds=$authorityfds >> "$1"
"$4" sh -c 'trap "" INT TERM HUP; while :; do sleep 30; done' & echo $! > "$2"
if kill -KILL "$liveness" 2>/dev/null; then echo liveness-kill=allowed; else echo liveness-kill=blocked; fi >> "$1"
if kill -KILL "$guardian" 2>/dev/null; then echo guardian-kill=allowed; else echo guardian-kill=blocked; fi >> "$1"`
	command := exec.Command("/bin/sh", "-c", script, "probe", status, daemon, wrappers, setsid, authorityRoot)
	command.Dir = "/"
	command.Env = []string{"PATH=/usr/bin:/bin"}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	configureHermesProcess(command)
	isolation := &ProcessIsolation{
		UID: uid, GID: gid, BaseEnvironment: map[string]string{"PATH": "/usr/bin:/bin"},
		TestOnlyIdentityLockRoot: root, StandaloneOwnerID: "trusted-supervisor-e2e",
		StandaloneStateRoot: createAgentStandaloneProtectedStateRoot(t, uid, gid),
	}
	tree, err := startUnixContainedProcess(command, ContainmentSpec{Isolation: isolation})
	if err != nil {
		t.Fatalf("start production trusted supervisor: %v", err)
	}
	wait := tree.directChild(command)
	t.Cleanup(func() {
		if pidBytes, readErr := os.ReadFile(wrappers); readErr == nil {
			for _, value := range strings.Fields(string(pidBytes)) {
				if pid, parseErr := strconv.Atoi(value); parseErr == nil && pid > 0 {
					_ = syscall.Kill(pid, syscall.SIGCONT)
				}
			}
		}
		_ = tree.close()
		if pidBytes, readErr := os.ReadFile(daemon); readErr == nil {
			if pid, parseErr := strconv.Atoi(strings.TrimSpace(string(pidBytes))); parseErr == nil && pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	select {
	case <-wait.done:
	case <-time.After(15 * time.Second):
		t.Fatal("production trusted supervisor did not exit")
	}
	if wait.err != nil {
		t.Fatalf("production trusted supervisor: %v\n%s", wait.err, output.Bytes())
	}
	if err = tree.complete(10 * time.Second); err != nil {
		t.Fatalf("complete production trusted supervisor: %v", err)
	}
	if err = tree.close(); err != nil {
		t.Fatalf("close production trusted supervisor: %v", err)
	}
	result, err := os.ReadFile(status)
	if err != nil {
		t.Fatal(err)
	}
	want := "liveness-stop=blocked\nguardian-stop=blocked\nliveness-status-forge=blocked\nliveness-proof-forge=blocked\nguardian-proof-forge=blocked\ngroups=empty\nuid=64581\ngid=64582\nambient=scrubbed\nnnp=1\ncore=0\nauthorityfds=none\nliveness-kill=blocked\nguardian-kill=blocked\n"
	if string(result) != want {
		t.Fatalf("native attack results = %q", result)
	}
	pidBytes, err := os.ReadFile(daemon)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		t.Fatal(err)
	}
	awaitSupervisorProcessGone(t, pid)
	assertSupervisorAuthorityLocks(t, authorityRoot, uid, true)
}
func TestHermesSupervisorGuardianSIGKILLPreReadinessRefusesNativeLaunch(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	peerRead, peerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer peerRead.Close()
	guardian := exec.Command("/bin/sleep", "30")
	guardian.ExtraFiles = []*os.File{peerWrite}
	if err = guardian.Start(); err != nil {
		t.Fatal(err)
	}
	_ = peerWrite.Close()
	if err = guardian.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = guardian.Wait(); err == nil {
		t.Fatal("SIGKILLed guardian exited successfully")
	}
	marker := filepath.Join(t.TempDir(), "native-launched")
	config := supervisorTestConfig([]string{"/bin/sh", "-c", "touch \"$1\"", "probe", marker})
	var status bytes.Buffer
	var proof bytes.Buffer
	code := runHermesProcessSupervisorNative(
		config, []io.Reader{strings.NewReader("control")}, peerRead, &status, &proof, true,
	)
	if code != 125 || status.Len() != 0 || !bytes.Equal(proof.Bytes(), []byte{1}) {
		t.Fatalf("pre-readiness guardian death code=%d status=%q proof=%v", code, status.String(), proof.Bytes())
	}
	if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native launch marker exists after guardian death: %v", err)
	}
}

func TestHermesSupervisorGuardianSIGKILLBeforeNativeLaunchRefusesStartAndCompletesAfterECHILD(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	peerRead, peerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer peerRead.Close()
	defer peerWrite.Close()

	originalPrctl := supervisorPrctl
	var closePeer sync.Once
	supervisorPrctl = func(option int, argument2, argument3, argument4, argument5 uintptr) error {
		if option == unix.PR_SET_NO_NEW_PRIVS {
			closePeer.Do(func() { _ = peerWrite.Close() })

			return nil
		}

		return originalPrctl(option, argument2, argument3, argument4, argument5)
	}

	marker := filepath.Join(t.TempDir(), "native-launched")
	config := supervisorTestConfig([]string{"/bin/sh", "-c", "touch \"$1\"", "probe", marker})
	var status bytes.Buffer
	var proof bytes.Buffer
	code := runHermesProcessSupervisorNative(
		config, []io.Reader{strings.NewReader("control")}, peerRead, &status, &proof, true,
	)
	if code != 125 || status.Len() != 0 || !bytes.Equal(proof.Bytes(), []byte{1}) {
		t.Fatalf("native-start guardian death code=%d status=%q proof=%v", code, status.String(), proof.Bytes())
	}
	if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native launch marker exists after guardian death at Start: %v", err)
	}
}

func TestHermesSupervisorCompletionClosesAuthorityBeforeProof(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	originalClose := agentIdentityLockClose
	t.Cleanup(func() { agentIdentityLockClose = originalClose })
	proveSupervisorDescendants = func(time.Duration) error { return nil }

	newAuthority := func(t *testing.T) (*hermesSupervisorAuthority, []*os.File) {
		t.Helper()
		identity, err := os.CreateTemp(t.TempDir(), "identity")
		if err != nil {
			t.Fatal(err)
		}
		domain, err := os.CreateTemp(t.TempDir(), "domain")
		if err != nil {
			t.Fatal(err)
		}

		return &hermesSupervisorAuthority{
			identity: &agentIdentityLock{file: identity},
			domain:   &agentIdentityLock{file: domain},
		}, []*os.File{identity, domain}
	}

	t.Run("guardian success", func(t *testing.T) {
		authority, _ := newAuthority(t)
		var order []string
		agentIdentityLockClose = func(file *os.File) error {
			order = append(order, "close")

			return file.Close()
		}
		var proof bytes.Buffer
		writer := io.MultiWriter(&proof, hermesSupervisorOrderWriter{order: &order})
		if err := completeHermesSupervisorAuthority(&authority, nil, nil, writer, false); err != nil {
			t.Fatal(err)
		}
		if authority != nil || !bytes.Equal(proof.Bytes(), []byte{1}) || strings.Join(order, ",") != "close,close,proof" {
			t.Fatalf("guardian completion authority=%v proof=%v order=%v", authority, proof.Bytes(), order)
		}
	})

	t.Run("close failure", func(t *testing.T) {
		authority, files := newAuthority(t)
		want := errors.New("close identity")
		calls := 0
		agentIdentityLockClose = func(file *os.File) error {
			calls++
			if calls == 1 {
				return want
			}

			return file.Close()
		}
		var proof bytes.Buffer
		err := completeHermesSupervisorAuthority(&authority, nil, nil, &proof, false)
		if !errors.Is(err, want) || authority != nil || calls != 2 || proof.Len() != 0 {
			t.Fatalf("close failure error=%v authority=%v calls=%d proof=%v", err, authority, calls, proof.Bytes())
		}
		_ = files[0].Close()
	})

	t.Run("liveness routing", func(t *testing.T) {
		for _, guardianExited := range []bool{false, true} {
			t.Run(strconv.FormatBool(guardianExited), func(t *testing.T) {
				authority, _ := newAuthority(t)
				agentIdentityLockClose = func(file *os.File) error { return file.Close() }
				guardianDone := make(chan struct{})
				if guardianExited {
					close(guardianDone)
				}
				var status, proof bytes.Buffer
				if err := completeHermesSupervisorAuthority(
					&authority, guardianDone, &status, &proof, true,
				); err != nil {
					t.Fatal(err)
				}
				if guardianExited {
					if status.Len() != 0 || !bytes.Equal(proof.Bytes(), []byte{1}) {
						t.Fatalf("survivor status=%q proof=%v", status.String(), proof.Bytes())
					}
				} else if status.String() != "done\n" || proof.Len() != 0 {
					t.Fatalf("paired status=%q proof=%v", status.String(), proof.Bytes())
				}
			})
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		var proof bytes.Buffer
		if err := completeHermesSupervisorAuthority(nil, nil, nil, &proof, false); err == nil {
			t.Fatal("missing authority completed")
		}
		if proof.Len() != 0 {
			t.Fatalf("unavailable completion proof=%v", proof.Bytes())
		}
	})
}

func TestHermesSupervisorConfigRequiresExplicitAuthorityOrigin(t *testing.T) {
	config := supervisorTestConfig([]string{"/bin/true"})
	config.StandaloneAuthority = true
	if err := validateHermesSupervisorConfig(config); err == nil {
		t.Fatal("standalone authority without capabilities was accepted")
	}

	config.IdentityLock = true
	config.AuthorityDomain = true
	if err := validateHermesSupervisorConfig(config); err == nil {
		t.Fatal("standalone authority without its owner tuple was accepted")
	}
	config.Isolation.StandaloneOwnerID = "supervisor-origin"
	config.Isolation.StandaloneStateRoot = "/var/lib/hermes-origin"
	if err := validateHermesSupervisorConfig(config); err != nil {
		t.Fatalf("valid inherited standalone authority: %v", err)
	}

	config.StandaloneAuthority = false
	if err := validateHermesSupervisorConfig(config); err == nil {
		t.Fatal("external borrowed authority accepted standalone owner fields")
	}
	config.IdentityLock = false
	config.AuthorityDomain = false
	if err := validateHermesSupervisorConfig(config); err != nil {
		t.Fatalf("valid initial standalone authority: %v", err)
	}
}

type hermesSupervisorOrderWriter struct {
	order *[]string
}

func (writer hermesSupervisorOrderWriter) Write(value []byte) (int, error) {
	*writer.order = append(*writer.order, "proof")

	return len(value), nil
}

type supervisorPeerDeathFixture struct {
	cmd            *exec.Cmd
	tree           *processContainment
	wait           *directChildWait
	guardianPID    int
	livenessPID    int
	descendantPIDs []int
	authorityRoot  string
	uid            uint32
}

func TestProcessIsolationSupervisorGuardianSIGKILLRetainsAuthorityThroughECHILD(t *testing.T) {
	fixture := startSupervisorPeerDeathFixture(t, 64331, 64332, "guardian-death")
	exerciseSupervisorPeerDeath(t, fixture, fixture.livenessPID, fixture.guardianPID)
}

func TestProcessIsolationSupervisorLivenessSIGKILLRetainsAuthorityThroughECHILD(t *testing.T) {
	fixture := startSupervisorPeerDeathFixture(t, 64341, 64342, "liveness-death")
	exerciseSupervisorPeerDeath(t, fixture, fixture.guardianPID, fixture.livenessPID)
}

func startSupervisorPeerDeathFixture(t *testing.T, uid, gid uint32, ownerID string) *supervisorPeerDeathFixture {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("dual trusted supervisor containment requires root")
	}
	setsid, err := exec.LookPath("setsid")
	if err != nil {
		t.Skip("setsid is unavailable")
	}
	root := t.TempDir()
	state := filepath.Join(root, "processes")
	if err = os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(root, "leaf.sh")
	double := filepath.Join(root, "double.sh")
	nativeScript := filepath.Join(root, "native.sh")
	if err = os.WriteFile(leaf, []byte("#!/bin/sh\ntrap '' INT TERM HUP\necho $$ > \"$1\"\nwhile :; do sleep 30; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(double, []byte("#!/bin/sh\n\"$1\" \"$2\" &\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	nativeBody := `#!/bin/sh
set -eu
echo $$ > "$HERMES_TEST_NATIVE_PID"
"$HERMES_TEST_LEAF" "$HERMES_TEST_ORDINARY_PID" &
"$HERMES_TEST_SETSID" "$HERMES_TEST_LEAF" "$HERMES_TEST_SESSION_PID" &
"$HERMES_TEST_SETSID" "$HERMES_TEST_DOUBLE" "$HERMES_TEST_LEAF" "$HERMES_TEST_DOUBLE_PID" &
while [ ! -s "$HERMES_TEST_ORDINARY_PID" ] || [ ! -s "$HERMES_TEST_SESSION_PID" ] || [ ! -s "$HERMES_TEST_DOUBLE_PID" ]; do sleep 0.01; done
while :; do sleep 30; done
`
	if err = os.WriteFile(nativeScript, []byte(nativeBody), 0o700); err != nil {
		t.Fatal(err)
	}
	pidPaths := []string{
		filepath.Join(state, "native.pid"), filepath.Join(state, "ordinary.pid"),
		filepath.Join(state, "session.pid"), filepath.Join(state, "double.pid"),
	}
	cmd := exec.Command(nativeScript)
	cmd.Env = []string{
		"PATH=/usr/bin:/bin", "HERMES_TEST_NATIVE_PID=" + pidPaths[0],
		"HERMES_TEST_ORDINARY_PID=" + pidPaths[1], "HERMES_TEST_SESSION_PID=" + pidPaths[2],
		"HERMES_TEST_DOUBLE_PID=" + pidPaths[3], "HERMES_TEST_LEAF=" + leaf,
		"HERMES_TEST_DOUBLE=" + double, "HERMES_TEST_SETSID=" + setsid,
	}
	configureHermesProcess(cmd)
	identityRoot := t.TempDir()
	isolation := &ProcessIsolation{
		UID: uid, GID: gid, BaseEnvironment: map[string]string{},
		TestOnlyNoCredential: true, TestOnlyIdentityLockRoot: identityRoot,
		StandaloneOwnerID: ownerID, StandaloneStateRoot: createAgentStandaloneProtectedStateRoot(t, uid, gid),
	}
	tree, err := startUnixContainedProcess(cmd, ContainmentSpec{Isolation: isolation})
	if err != nil {
		t.Fatalf("start dual trusted supervisor fixture: %v", err)
	}
	fixture := &supervisorPeerDeathFixture{
		cmd: cmd, tree: tree, wait: tree.directChild(cmd), guardianPID: cmd.Process.Pid,
		authorityRoot: filepath.Join(identityRoot, "acp-go", "agent-identities"), uid: uid,
	}
	for _, path := range pidPaths {
		fixture.descendantPIDs = append(fixture.descendantPIDs, awaitSupervisorPIDFile(t, path))
	}
	identity, err := readSupervisorProcessIdentity(fixture.descendantPIDs[0])
	if err != nil {
		t.Fatalf("read native parent identity: %v", err)
	}
	fixture.livenessPID = identity.parentPID
	if fixture.livenessPID <= 0 || fixture.livenessPID == fixture.guardianPID {
		t.Fatalf("invalid guardian/liveness topology guardian=%d liveness=%d", fixture.guardianPID, fixture.livenessPID)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(fixture.guardianPID, syscall.SIGCONT)
		_ = syscall.Kill(fixture.livenessPID, syscall.SIGCONT)
		_ = fixture.tree.close()
		for _, pid := range append(fixture.descendantPIDs, fixture.guardianPID, fixture.livenessPID) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		select {
		case <-fixture.wait.done:
		case <-time.After(2 * time.Second):
		}
	})

	return fixture
}

type supervisorTestProcessIdentity struct {
	parentPID int
	state     byte
}

func readSupervisorProcessIdentity(pid int) (supervisorTestProcessIdentity, error) {
	payload, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return supervisorTestProcessIdentity{}, err
	}
	parentPID, state, ok := supervisorProcStat(string(payload))
	if !ok {
		return supervisorTestProcessIdentity{}, fmt.Errorf("parse process identity for pid %d", pid)
	}

	return supervisorTestProcessIdentity{parentPID: parentPID, state: state}, nil
}

func exerciseSupervisorPeerDeath(t *testing.T, fixture *supervisorPeerDeathFixture, survivorPID, victimPID int) {
	t.Helper()
	if err := syscall.Kill(survivorPID, syscall.SIGSTOP); err != nil {
		t.Fatalf("stop surviving trusted supervisor %d: %v", survivorPID, err)
	}
	awaitSupervisorProcessState(t, survivorPID, 'T')
	if err := syscall.Kill(victimPID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill trusted supervisor peer %d: %v", victimPID, err)
	}
	assertSupervisorAuthorityLocks(t, fixture.authorityRoot, fixture.uid, false)

	if err := syscall.Kill(survivorPID, syscall.SIGCONT); err != nil {
		t.Fatalf("resume surviving trusted supervisor %d: %v", survivorPID, err)
	}
	if err := fixture.tree.complete(10 * time.Second); err != nil {
		t.Fatalf("dual trusted supervisor containment proof: %v", err)
	}
	select {
	case <-fixture.wait.done:
	case <-time.After(5 * time.Second):
		t.Fatal("dual trusted supervisor direct child was not reaped")
	}
	for _, pid := range fixture.descendantPIDs {
		awaitSupervisorProcessGone(t, pid)
	}
	assertSupervisorAuthorityLocks(t, fixture.authorityRoot, fixture.uid, true)
}

func awaitSupervisorPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(payload)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("supervised process pid file %q was not published", path)

	return 0
}

func awaitSupervisorProcessState(t *testing.T, pid int, want byte) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		identity, err := readSupervisorProcessIdentity(pid)
		if err == nil && identity.state == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("supervised process %d did not enter state %q", pid, want)
}

func awaitSupervisorProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for processAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if processAlive(pid) {
		t.Fatalf("supervised descendant %d survived ECHILD containment proof", pid)
	}
}

func assertSupervisorAuthorityLocks(t *testing.T, authorityRoot string, uid uint32, available bool) {
	t.Helper()
	for _, name := range []string{strconv.FormatUint(uint64(uid), 10) + ".lock", "domain.lock"} {
		path := filepath.Join(authorityRoot, name)
		deadline := time.Now().Add(5 * time.Second)
		for {
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("open authority contender %q: %v", name, err)
			}
			lockErr := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			if lockErr == nil {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
			}
			_ = file.Close()
			if !available {
				if !errors.Is(lockErr, unix.EWOULDBLOCK) && !errors.Is(lockErr, unix.EAGAIN) {
					t.Fatalf("authority lock %q was not retained by frozen survivor: %v", name, lockErr)
				}

				break
			}
			if lockErr == nil {
				break
			}
			if !errors.Is(lockErr, unix.EWOULDBLOCK) && !errors.Is(lockErr, unix.EAGAIN) {
				t.Fatalf("reacquire authority lock %q: %v", name, lockErr)
			}
			if time.Now().After(deadline) {
				t.Fatalf("authority lock %q remained held after ECHILD", name)
			}
			time.Sleep(10 * time.Millisecond)
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
		if code != 125 || proof != 1 {
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
		if code != 125 || proof != 1 {
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
		if code != 125 || proof != 1 {
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
		supervisorAcquireStandalone = func(uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal) (*agentStandaloneIdentity, error) {
			return &agentStandaloneIdentity{identity: &agentIdentityLock{}, authority: &agentIdentityLock{}}, nil
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
		if code := runHermesProcessSupervisorNative(config, []io.Reader{controlRead}, nil, nil, proofWrite, false); code != 126 {
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
		stopSupervisorDescendants = func(targetPID int, targetDone <-chan error) (error, bool) {
			_ = syscall.Kill(-targetPID, syscall.SIGKILL)
			<-targetDone

			return errors.New("stop"), true
		}
		code, proof := runSupervisorCoreTest(t, []string{"sh", "-c", "while :; do sleep 30; done"}, func(control *os.File) {
			_ = control.Close()
		})
		if code != 1 || proof != 1 {
			t.Fatalf("core result code/proof = %d/%d", code, proof)
		}
	})

	t.Run("unsettled root awaits containment before proof", func(t *testing.T) {
		restoreLinuxSupervisorSeams(t)
		stopSupervisorDescendants = func(targetPID int, _ <-chan error) (error, bool) {
			_ = syscall.Kill(-targetPID, syscall.SIGKILL)

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
	restoreLinuxSupervisorSeams(t)

	target := hermesSupervisorTarget(hermesSupervisorConfig{
		Path: "/bin/true",
		Args: []string{"/bin/true"},
	})
	if target.SysProcAttr == nil || !target.SysProcAttr.Setpgid ||
		target.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatalf("supervised target attributes = %+v", target.SysProcAttr)
	}

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

	t.Setenv("GORACE", "halt_on_error=1 atexit_sleep_ms=1000")
	supervisorExecutable = func() (string, error) { return "/bin/true", nil }
	environmentTarget := exec.Command("/bin/true")
	configureHermesProcess(environmentTarget)
	environmentTree, err := startUnixContainedProcess(environmentTarget, ContainmentSpec{Isolation: testProcessIsolation()})
	if err != nil {
		t.Fatalf("start supervisor environment probe: %v", err)
	}
	if len(environmentTarget.Env) != 1 || environmentTarget.Env[0] != envHermesSupervisor+"="+hermesSupervisorGuardian {
		t.Fatalf("supervisor environment = %#v", environmentTarget.Env)
	}
	_ = environmentTree.close()

	if err := proveAndReapSupervisorDescendants(time.Millisecond); err != nil {
		t.Fatalf("prove empty test descendants: %v", err)
	}
}

func TestHermesSupervisorRequiresDistinctTrustedRoot(t *testing.T) {
	restoreLinuxSupervisorSeams(t)

	supervisorEffectiveUID = func() int { return 1000 }
	if err := validateHermesSupervisorIdentity(testProcessIsolation()); err == nil || !strings.Contains(err.Error(), "trusted root") {
		t.Fatalf("non-root identity validation = %v", err)
	}
	native := exec.Command("/bin/true")
	configureHermesProcess(native)
	if _, err := startUnixContainedProcess(native, ContainmentSpec{Isolation: testProcessIsolation()}); err == nil || !strings.Contains(err.Error(), "trusted root") {
		t.Fatalf("non-root parent preparation = %v", err)
	}

	supervisorEffectiveUID = func() int { return 0 }
	isolation := testProcessIsolation()
	isolation.UID = 0
	if err := validateHermesSupervisorIdentity(isolation); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("shared root identity validation = %v", err)
	}

	isolation.UID = 11
	if err := validateHermesSupervisorIdentity(isolation); err != nil {
		t.Fatalf("distinct trusted identity validation = %v", err)
	}

	supervisorEffectiveUID = func() int { return 1000 }
	if code := runHermesProcessSupervisor(hermesSupervisorGuardian); code != 125 {
		t.Fatalf("non-root bootstrap code = %d, want 125", code)
	}
}

func TestHermesSupervisorProductionIdentityRejectsNonRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires a non-root process")
	}

	_, err := startUnixContainedProcess(nil, ContainmentSpec{Isolation: testProcessIsolation()})
	if err == nil || !strings.Contains(err.Error(), "trusted root") {
		t.Fatalf("non-root production preparation = %v", err)
	}
}

func TestLinuxSupervisorInitWrapperAndFailureSeams(t *testing.T) {
	restoreLinuxSupervisorSeams(t)

	exitCode := -1
	supervisorExit = func(code int) { exitCode = code }
	supervisorCloseOnExec = func(int) error { return nil }
	supervisorNewFile = func(uintptr, string) *os.File { return nil }
	t.Setenv(envHermesSupervisor, hermesSupervisorGuardian)
	runHermesSupervisorInit()
	if exitCode != 125 {
		t.Fatalf("supervisor init exit = %d, want 125", exitCode)
	}
	supervisorNewFile = os.NewFile
	supervisorCloseOnExec = setHermesSupervisorCloseOnExec

	supervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error { return errors.New("prctl") }
	if code := runHermesProcessSupervisor(hermesSupervisorGuardian); code != 125 {
		t.Fatalf("prctl failure code = %d", code)
	}

	supervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error { return nil }
	supervisorPIDFDOpen = func(int, int) (int, error) { return -1, errors.New("pidfd") }
	if code := runHermesProcessSupervisor(hermesSupervisorGuardian); code != 125 {
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
	if err := json.NewEncoder(configWrite).Encode(supervisorTestConfig([]string{"/bin/sh", "-c", "exit 0"})); err != nil {
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
	supervisorCloseOnExec = func(int) error { return nil }
	supervisorAcquireStandalone = func(uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal) (*agentStandaloneIdentity, error) {
		return &agentStandaloneIdentity{identity: &agentIdentityLock{}, authority: &agentIdentityLock{}}, nil
	}
	if code := runHermesProcessSupervisor(hermesSupervisorGuardian); code != 0 {
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
	oldFcntl := supervisorFcntl
	oldCloseOnExec := supervisorCloseOnExec
	oldMemfd := supervisorMemfd
	oldSealConfig := supervisorSealConfig
	oldAcquireStandalone := supervisorAcquireStandalone
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
	oldEffectiveUID := supervisorEffectiveUID
	oldSupervisorPoll := supervisorPoll
	supervisorEffectiveUID = func() int { return 0 }
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
		supervisorFcntl = oldFcntl
		supervisorCloseOnExec = oldCloseOnExec
		supervisorMemfd = oldMemfd
		supervisorSealConfig = oldSealConfig
		supervisorAcquireStandalone = oldAcquireStandalone
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
		supervisorEffectiveUID = oldEffectiveUID
		supervisorPoll = oldSupervisorPoll
	})
}

func TestHermesSupervisorCheckedCloseOnExec(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	calls := 0
	supervisorFcntl = func(_ uintptr, command int, argument int) (int, error) {
		calls++
		if calls == 1 {
			if command != unix.F_GETFD || argument != 0 {
				t.Fatalf("get flags call = (%d,%d)", command, argument)
			}

			return 0, nil
		}
		if command != unix.F_SETFD || argument&unix.FD_CLOEXEC == 0 {
			t.Fatalf("set flags call = (%d,%d)", command, argument)
		}

		return 0, nil
	}
	if err := setHermesSupervisorCloseOnExec(supervisorProofFD); err != nil || calls != 2 {
		t.Fatalf("checked close-on-exec calls=%d err=%v", calls, err)
	}

	want := errors.New("fcntl")
	supervisorFcntl = func(uintptr, int, int) (int, error) { return 0, want }
	if err := setHermesSupervisorCloseOnExec(supervisorProofFD); !errors.Is(err, want) {
		t.Fatalf("get flags error = %v", err)
	}
	calls = 0
	supervisorFcntl = func(uintptr, int, int) (int, error) {
		calls++
		if calls == 1 {
			return 0, nil
		}

		return 0, want
	}
	if err := setHermesSupervisorCloseOnExec(supervisorProofFD); !errors.Is(err, want) {
		t.Fatalf("set flags error = %v", err)
	}
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
	oldAcquire := supervisorAcquireStandalone
	supervisorAcquireStandalone = func(uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal) (*agentStandaloneIdentity, error) {
		return &agentStandaloneIdentity{identity: &agentIdentityLock{}, authority: &agentIdentityLock{}}, nil
	}
	defer func() { supervisorAcquireStandalone = oldAcquire }()
	code := runHermesProcessSupervisorNative(
		supervisorTestConfig(args), []io.Reader{controlRead}, nil, nil, proofWrite, false,
	)
	_ = controlWrite.Close()
	_ = proofWrite.Close()
	var proof [1]byte
	_, _ = proofRead.Read(proof[:])
	_ = proofRead.Close()

	return code, proof[0]
}

func supervisorTestConfig(args []string) hermesSupervisorConfig {
	return hermesSupervisorConfig{
		Path: args[0],
		Args: append([]string(nil), args...),
		Env:  os.Environ(),
		Isolation: ProcessIsolation{
			UID: 11, GID: 22, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true,
		},
	}
}
