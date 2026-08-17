//go:build linux

package hermes

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestOrdinaryExecutionTakesNoIdentityAuthority proves the ordinary path never
// reaches the durable identity authority. That authority records who may enter
// an identity nobody holds; ordinary mode occupies the identity it runs as, so
// there is nothing for it to claim, adopt, publish, or release.
func TestOrdinaryExecutionTakesNoIdentityAuthority(t *testing.T) {
	original := supervisorAcquireStandalone
	t.Cleanup(func() { supervisorAcquireStandalone = original })

	supervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		t.Error("ordinary execution claimed a standalone agent identity")

		return nil, errors.ErrUnsupported
	}

	command := exec.Command("/bin/true")
	configureHermesProcess(command)

	tree, err := startContainedProcess(command)
	if err != nil {
		t.Fatalf("ordinary start: %v", err)
	}

	if err := tree.complete(10 * time.Second); err != nil {
		t.Fatalf("ordinary completion: %v", err)
	}
}

// TestExplicitProcessIsolationRefusesWithoutOrdinaryFallback proves a
// structurally valid explicit policy that cannot be honoured refuses before any
// spawn and never retries as ordinary execution. The policy passes shape
// validation, so what fails is the trusted-supervisor requirement itself.
func TestExplicitProcessIsolationRefusesWithoutOrdinaryFallback(t *testing.T) {
	restoreLinuxSupervisorSeams(t)

	starts := 0
	supervisorCommand = func(string, ...string) *exec.Cmd {
		starts++

		return exec.Command("/bin/true")
	}

	supervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		t.Error("a refused policy reached the identity authority")

		return nil, errors.ErrUnsupported
	}

	// A non-root supervisor cannot hold this boundary, including when the policy
	// names the very identity it runs as.
	supervisorEffectiveUID = func() int { return 1000 }

	policy := &ProcessIsolation{
		UID: 1000, GID: 1000,
		BaseEnvironment:     map[string]string{"PATH": "/usr/bin:/bin"},
		StandaloneOwnerID:   standaloneTestOwnerID,
		StandaloneStateRoot: standaloneTestStateRoot,
	}

	target := exec.Command("/bin/true")
	configureHermesProcess(target)

	_, err := startContainedProcess(target, ContainmentSpec{Isolation: policy})
	if err == nil || !strings.Contains(err.Error(), "trusted root identity is required") {
		t.Fatalf("non-root explicit policy = %v", err)
	}

	// A root supervisor still refuses a native identity equal to its own.
	supervisorEffectiveUID = func() int { return 0 }
	rootPolicy := *policy
	rootPolicy.UID = 0

	_, err = startContainedProcess(target, ContainmentSpec{Isolation: &rootPolicy})
	if err == nil {
		t.Fatal("root supervisor accepted a root native identity")
	}

	if starts != 0 {
		t.Fatalf("refused policies started %d supervisors", starts)
	}
}

// TestHermesSupervisorIdentityRuleRequiresADistinctTrustedRoot keeps the
// negative security rule the deleted shared arm used to exempt itself from.
func TestHermesSupervisorIdentityRuleRequiresADistinctTrustedRoot(t *testing.T) {
	restoreLinuxSupervisorSeams(t)

	isolation := func(uid, gid uint32) *ProcessIsolation {
		return &ProcessIsolation{UID: uid, GID: gid, BaseEnvironment: map[string]string{}}
	}

	if err := validateHermesSupervisorIdentity(nil); err == nil {
		t.Fatal("a missing policy was accepted")
	}

	supervisorEffectiveUID = func() int { return 1000 }

	// The identity the supervisor already runs as gets no exemption.
	err := validateHermesSupervisorIdentity(isolation(1000, 1000))
	if err == nil || err.Error() != "trusted root identity is required, effective uid is 1000" {
		t.Fatalf("non-root supervisor of its own identity = %v", err)
	}

	err = validateHermesSupervisorIdentity(isolation(65534, 65534))
	if err == nil || err.Error() != "trusted root identity is required, effective uid is 1000" {
		t.Fatalf("non-root supervisor of a distinct identity = %v", err)
	}

	supervisorEffectiveUID = func() int { return 0 }

	err = validateHermesSupervisorIdentity(isolation(0, 0))
	if err == nil || err.Error() != "native target identity must differ from the trusted supervisor" {
		t.Fatalf("root supervisor of the root identity = %v", err)
	}

	if err = validateHermesSupervisorIdentity(isolation(65534, 65534)); err != nil {
		t.Fatalf("root supervisor of a distinct identity: %v", err)
	}
}
