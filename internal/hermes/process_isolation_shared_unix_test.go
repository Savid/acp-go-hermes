//go:build unix

package hermes

import (
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

func restoreSharedIdentitySeams(t *testing.T) {
	t.Helper()
	oldPlatform := processIsolationPlatform
	oldUID, oldGID, oldGroups := processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	t.Cleanup(func() {
		processIsolationPlatform = oldPlatform
		processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups = oldUID, oldGID, oldGroups
	})
	processIsolationPlatform = processPlatformLinux
}

func sharedIdentityIsolation(uid uint32, gid uint32) *ProcessIsolation {
	return &ProcessIsolation{UID: uid, GID: gid, BaseEnvironment: map[string]string{}}
}

func TestSharedProcessIdentityNamesOnlyTheSupervisorsOwnLinuxIdentity(t *testing.T) {
	restoreSharedIdentitySeams(t)

	if sharedProcessIdentity(nil) {
		t.Fatal("a missing isolation named a shared identity")
	}

	processIsolationGeteuid = func() int { return 1000 }
	if !sharedProcessIdentity(sharedIdentityIsolation(1000, 1000)) {
		t.Fatal("the supervisor's own identity was not recognised")
	}
	if !sharedProcessIdentity(sharedIdentityIsolation(1000, 2000)) {
		t.Fatal("a differing group changed the identity decision")
	}
	if sharedProcessIdentity(sharedIdentityIsolation(1001, 1000)) {
		t.Fatal("a distinct native identity named a shared one")
	}

	processIsolationGeteuid = func() int { return 0 }
	for _, uid := range []uint32{0, 1000} {
		if sharedProcessIdentity(sharedIdentityIsolation(uid, 1000)) {
			t.Fatalf("root entered the shared arm for uid %d", uid)
		}
	}

	processIsolationGeteuid = func() int { return 1000 }
	processIsolationPlatform = "darwin"
	if sharedProcessIdentity(sharedIdentityIsolation(1000, 1000)) {
		t.Fatal("a non-Linux backend recognised the shared shape")
	}
}

func TestSharedIdentityIsolationCarriesNoStandaloneOwnerFields(t *testing.T) {
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }

	if err := validateProcessIsolation(sharedIdentityIsolation(1000, 1000)); err != nil {
		t.Fatalf("canonical shared isolation: %v", err)
	}

	for name, mutate := range map[string]func(*ProcessIsolation){
		"owner id":   func(isolation *ProcessIsolation) { isolation.StandaloneOwnerID = "deployment-1" },
		"state root": func(isolation *ProcessIsolation) { isolation.StandaloneStateRoot = "/var/lib/hermes" },
	} {
		t.Run(name, func(t *testing.T) {
			isolation := sharedIdentityIsolation(1000, 1000)
			mutate(isolation)
			err := validateProcessIsolation(isolation)
			if err == nil || !strings.Contains(err.Error(), "identity the supervisor already holds") ||
				!strings.Contains(err.Error(), sharedIdentitySupervisorRemedy) {
				t.Fatalf("shared isolation with a standalone %s = %v", name, err)
			}
		})
	}

	isolated := sharedIdentityIsolation(1001, 1000)
	if err := validateProcessIsolation(isolated); err == nil ||
		err.Error() != "standalone owner id must be 1..256 valid UTF-8 bytes without whitespace or control characters" {
		t.Fatalf("isolated disposition = %v", err)
	}
}

func TestSharedIdentityCredentialRequestsNoIdentityChange(t *testing.T) {
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }
	processIsolationGetegid = func() int { return 1000 }
	// A login account carries supplementary groups it cannot shed unprivileged.
	// The shared arm must not read that as a containment failure.
	processIsolationGetgroups = func() ([]int, error) { return []int{1000, 27}, nil }

	command := exec.Command("/usr/bin/true")
	if err := applyProcessIsolation(command, sharedIdentityIsolation(1000, 1000)); err != nil {
		t.Fatalf("shared credential application: %v", err)
	}
	if command.SysProcAttr != nil && command.SysProcAttr.Credential != nil {
		t.Fatalf("shared launch requested credential %#v", command.SysProcAttr.Credential)
	}

	err := applyProcessIsolation(exec.Command("/usr/bin/true"), sharedIdentityIsolation(1000, 2000))
	if err == nil || !strings.Contains(err.Error(), "native group 2000 cannot be entered from group 1000") ||
		!strings.Contains(err.Error(), sharedIdentitySupervisorRemedy) {
		t.Fatalf("unreachable native group = %v", err)
	}

	isolated := exec.Command("/usr/bin/true")
	if err = applyProcessIsolation(isolated, &ProcessIsolation{
		UID: 1001, GID: 1002, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: "deployment-1", StandaloneStateRoot: "/var/lib/hermes",
	}); err != nil {
		t.Fatalf("isolated credential application: %v", err)
	}
	want := syscall.Credential{Uid: 1001, Gid: 1002, Groups: []uint32{}}
	if got := isolated.SysProcAttr.Credential; got == nil || got.Uid != want.Uid || got.Gid != want.Gid ||
		len(got.Groups) != 0 || got.NoSetGroups {
		t.Fatalf("isolated credential = %#v", got)
	}

	// Off Linux the arm never applies, so a backend that already runs as the
	// native identity keeps proving the empty supplementary groups it demands.
	processIsolationPlatform = "darwin"
	processIsolationGeteuid = func() int { return 1001 }
	processIsolationGetegid = func() int { return 1002 }
	err = applyProcessIsolation(exec.Command("/usr/bin/true"), &ProcessIsolation{
		UID: 1001, GID: 1002, BaseEnvironment: map[string]string{},
	})
	if err == nil || !strings.Contains(err.Error(), "unexpected supplementary groups") {
		t.Fatalf("isolated supplementary groups = %v", err)
	}
}
