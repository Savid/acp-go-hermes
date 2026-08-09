//go:build linux

package hermes

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// sharedSupervisorIdentity is the isolation a deployment that never held
// privilege hands the supervisor: the identity it already runs as, no
// capabilities, and no standalone owner fields.
func sharedSupervisorIdentity() *ProcessIsolation {
	return &ProcessIsolation{
		UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()),
		BaseEnvironment: map[string]string{"PATH": "/usr/bin:/bin"},
	}
}

func TestHermesSupervisorIdentityRuleAcceptsTheIdentityItAlreadyRuns(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	// The root assertion keeps its own seam: the arm is decided against the
	// identity the process really runs as, so a stubbed trusted uid cannot talk
	// a distinct native identity onto it.
	supervisorEffectiveUID = func() int { return 1000 }
	processIsolationGeteuid = func() int { return 1000 }

	if err := validateHermesSupervisorIdentity(sharedIdentityIsolation(1000, 1000)); err != nil {
		t.Fatalf("shared identity rule: %v", err)
	}
	if err := validateHermesSupervisorIdentity(sharedIdentityIsolation(1000, 2000)); err != nil {
		t.Fatalf("shared identity rule with a distinct group: %v", err)
	}

	err := validateHermesSupervisorIdentity(sharedIdentityIsolation(65534, 65534))
	if err == nil || err.Error() != "trusted root identity is required, effective uid is 1000" {
		t.Fatalf("non-root supervisor of a distinct identity = %v", err)
	}

	supervisorEffectiveUID = func() int { return 0 }
	processIsolationGeteuid = func() int { return 0 }
	if err = validateHermesSupervisorIdentity(sharedIdentityIsolation(0, 0)); err == nil ||
		err.Error() != "native target identity must differ from the trusted supervisor" {
		t.Fatalf("root supervisor of the root identity = %v", err)
	}
	if err = validateHermesSupervisorIdentity(sharedIdentityIsolation(65534, 65534)); err != nil {
		t.Fatalf("root supervisor of a distinct identity: %v", err)
	}
}

// sealedSupervisorConfig captures what the parent stamped into the sealed
// config by standing an ordinary file in for the memfd.
func sealedSupervisorConfig(t *testing.T, isolation *ProcessIsolation) hermesSupervisorConfig {
	t.Helper()

	path := filepath.Join(t.TempDir(), "supervisor-config")

	supervisorMemfd = func(string, int) (int, error) {
		file, err := os.Create(path) // #nosec G304 -- the path is the test's own temp directory.
		if err != nil {
			return -1, err
		}

		fd, err := unix.Dup(int(file.Fd()))
		_ = file.Close()

		return fd, err
	}
	supervisorSealConfig = func(uintptr, int, int) (int, error) { return 0, nil }
	supervisorExecutable = func() (string, error) { return "/bin/true", nil }
	// The config is sealed before the control pipe is created, so refusing the
	// pipe stops the launch with the stamp already written.
	supervisorPipe = func() (*os.File, *os.File, error) { return nil, nil, os.ErrClosed }

	target := exec.Command("/bin/true")
	configureHermesProcess(target)

	if _, err := startUnixContainedProcess(target, ContainmentSpec{Isolation: isolation}); err == nil {
		t.Fatal("the refused control pipe still launched a supervisor")
	}

	data, err := os.ReadFile(path) // #nosec G304 -- the path is the test's own temp directory.
	if err != nil {
		t.Fatal(err)
	}

	var config hermesSupervisorConfig
	if err = json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}

	return config
}

func TestPrepareHermesSupervisorStampsTheAuthorityItsIdentityAllows(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	supervisorEffectiveUID = func() int { return 1000 }
	processIsolationGeteuid = func() int { return 1000 }

	shared := sealedSupervisorConfig(t, sharedIdentityIsolation(1000, 1000))
	if !shared.SharedIdentity || shared.IdentityLock || shared.AuthorityDomain || shared.StandaloneAuthority {
		t.Fatalf("shared stamp = %#v", shared)
	}

	supervisorEffectiveUID = func() int { return 0 }
	processIsolationGeteuid = func() int { return 0 }

	isolated := sealedSupervisorConfig(t, &ProcessIsolation{
		UID: 65534, GID: 65534, BaseEnvironment: map[string]string{"PATH": "/usr/bin:/bin"},
		StandaloneOwnerID: "deployment-1", StandaloneStateRoot: "/var/lib/hermes",
	})
	if isolated.SharedIdentity || isolated.IdentityLock || isolated.AuthorityDomain || isolated.StandaloneAuthority {
		t.Fatalf("isolated stamp = %#v", isolated)
	}
}

func TestHermesSupervisorConfigRefusesADispositionItsIdentityContradicts(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	supervisorEffectiveUID = func() int { return 1000 }
	processIsolationGeteuid = func() int { return 1000 }

	shared := supervisorTestConfig([]string{"/bin/true"})
	shared.Isolation = *sharedIdentityIsolation(1000, 1000)
	shared.SharedIdentity = true
	if err := validateHermesSupervisorConfig(shared); err != nil {
		t.Fatalf("shared supervisor config: %v", err)
	}

	unstamped := shared
	unstamped.SharedIdentity = false
	if err := validateHermesSupervisorConfig(unstamped); err == nil ||
		!strings.Contains(err.Error(), "does not match the identity it runs as") {
		t.Fatalf("unstamped shared identity = %v", err)
	}

	distinct := shared
	distinct.Isolation.UID = 65534
	if err := validateHermesSupervisorConfig(distinct); err == nil ||
		!strings.Contains(err.Error(), "does not match the identity it runs as") {
		t.Fatalf("shared stamp on a distinct identity = %v", err)
	}

	for name, mutate := range map[string]func(*hermesSupervisorConfig){
		"identity lock": func(config *hermesSupervisorConfig) { config.IdentityLock, config.AuthorityDomain = true, true },
		"owner id":      func(config *hermesSupervisorConfig) { config.Isolation.StandaloneOwnerID = "deployment-1" },
		"state root":    func(config *hermesSupervisorConfig) { config.Isolation.StandaloneStateRoot = "/var/lib/hermes" },
		"inherited standalone": func(config *hermesSupervisorConfig) {
			config.IdentityLock, config.AuthorityDomain, config.StandaloneAuthority = true, true, true
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := shared
			mutate(&config)
			if err := validateHermesSupervisorConfig(config); err == nil ||
				!strings.Contains(err.Error(), "hermes shared supervisor authority disposition is invalid") {
				t.Fatalf("shared config carrying a %s = %v", name, err)
			}
		})
	}
}

func TestSharedIdentityGuardianTakesNoAgentAuthority(t *testing.T) {
	restoreLinuxSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	supervisorEffectiveUID = func() int { return 1000 }
	processIsolationGeteuid = func() int { return 1000 }
	supervisorAcquireStandalone = func(uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal) (*agentStandaloneIdentity, error) {
		t.Fatal("the shared arm claimed a standalone agent identity")

		return nil, errors.ErrUnsupported
	}

	config := supervisorTestConfig([]string{"/bin/true"})
	config.Isolation = *sharedIdentityIsolation(1000, 1000)
	config.SharedIdentity = true

	authority, err := acquireHermesSupervisorAuthority(config, supervisorIdentityLockFD, supervisorAuthorityFD, nil, nil)
	if err != nil {
		t.Fatalf("acquire shared authority: %v", err)
	}
	if authority.standalone != nil || authority.identity == nil || authority.identity.file != nil ||
		authority.domain == nil || authority.domain.file != nil {
		t.Fatalf("shared authority = %#v", authority)
	}

	var proof bytes.Buffer
	if err = completeHermesSupervisorAuthority(&authority, nil, nil, &proof, false); err != nil {
		t.Fatalf("complete shared authority: %v", err)
	}
	if !bytes.Equal(proof.Bytes(), []byte{1}) {
		t.Fatalf("shared containment proof = %v", proof.Bytes())
	}
}

// TestSharedIdentitySupervisorContainsARealNativeLaunch drives the whole
// unprivileged tree with no seams: the guardian and liveness self-exec pair, the
// readiness handshake, a native launch that requests no credential change, and
// the containment proof.
func TestSharedIdentitySupervisorContainsARealNativeLaunch(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the shared arm is unreachable from a root process")
	}

	status := filepath.Join(t.TempDir(), "shared-identity")
	script := `echo uid=$(id -u) > "$1"
echo gid=$(id -g) >> "$1"
echo nnp=$(awk '$1 == "NoNewPrivs:" { print $2 }' /proc/self/status) >> "$1"
echo core=$(ulimit -c) >> "$1"
echo subreaper=$(awk '$1 == "PPid:" { print $2 }' /proc/self/status) >> "$1"`
	command := exec.Command("/bin/sh", "-c", script, "probe", status)
	command.Dir = "/"
	command.Env = []string{"PATH=/usr/bin:/bin"}

	var output bytes.Buffer

	command.Stdout = &output
	command.Stderr = &output
	configureHermesProcess(command)

	tree, err := startUnixContainedProcess(command, ContainmentSpec{Isolation: sharedSupervisorIdentity()})
	if err != nil {
		t.Fatalf("start shared-identity supervisor: %v", err)
	}

	wait := tree.directChild(command)
	t.Cleanup(func() { _ = tree.close() })

	select {
	case <-wait.done:
	case <-time.After(30 * time.Second):
		t.Fatal("shared-identity supervisor did not exit")
	}

	if wait.err != nil {
		t.Fatalf("shared-identity supervisor: %v\n%s", wait.err, output.Bytes())
	}
	if err = tree.complete(10 * time.Second); err != nil {
		t.Fatalf("complete shared-identity supervisor: %v", err)
	}
	if err = tree.close(); err != nil {
		t.Fatalf("close shared-identity supervisor: %v", err)
	}

	result, err := os.ReadFile(status) // #nosec G304 -- the path is the test's own temp directory.
	if err != nil {
		t.Fatal(err)
	}

	want := "uid=" + strconv.Itoa(os.Geteuid()) + "\ngid=" + strconv.Itoa(os.Getegid()) + "\nnnp=1\ncore=0\n"
	if !strings.HasPrefix(string(result), want) {
		t.Fatalf("shared-identity native launch = %q, want prefix %q", result, want)
	}
}

// TestSharedIdentityNativeLaunchRequestsNoCredential proves the launch path the
// liveness supervisor runs under a shared identity, including the readiness
// frame and the completion the guardian waits for.
func TestSharedIdentityNativeLaunchRequestsNoCredential(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the shared arm is unreachable from a root process")
	}

	restoreLinuxSupervisorSeams(t)
	supervisorEffectiveUID = os.Geteuid

	config := hermesSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"}, Dir: "/", Env: []string{"PATH=/usr/bin:/bin"},
		Isolation: *sharedSupervisorIdentity(), SharedIdentity: true,
	}
	supervisorAcquireStandalone = func(uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal) (*agentStandaloneIdentity, error) {
		t.Fatal("the shared native launch claimed a standalone agent identity")

		return nil, errors.ErrUnsupported
	}

	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	var status, proof bytes.Buffer

	code := runHermesProcessSupervisorNative(config, []io.Reader{controlRead}, nil, &status, &proof, true)

	_ = controlWrite.Close()
	_ = controlRead.Close()

	if code != 0 {
		t.Fatalf("shared native launch code = %d, status %q", code, status.String())
	}
	if !strings.HasPrefix(status.String(), "ready:") || !strings.HasSuffix(status.String(), "done\n") {
		t.Fatalf("shared native launch status = %q", status.String())
	}
	if proof.Len() != 0 {
		t.Fatalf("shared native launch proof = %v", proof.Bytes())
	}
}
