//go:build unix

package hermes

import (
	"errors"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessIsolationUnixVerificationBranches(t *testing.T) {
	originalUID, originalGID, originalGroups := processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	originalPlatform := processRuntimePlatform
	t.Cleanup(func() {
		processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups = originalUID, originalGID, originalGroups
		processRuntimePlatform = originalPlatform
	})

	// A process that already holds the target identity is the post-drop end of
	// the supervisor's own descent, so it verifies rather than re-requesting a
	// credential. The platform is pinned to Linux because an explicit policy is
	// refused everywhere else, whichever host runs this test.
	processRuntimePlatform = processPlatformLinux
	processIsolationGeteuid = func() int { return 11 }
	processIsolationGetegid = func() int { return 22 }
	processIsolationGetgroups = func() ([]int, error) { return nil, nil }
	policy := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: standaloneTestOwnerID, StandaloneStateRoot: standaloneTestStateRoot,
	}
	require.NoError(t, verifyProcessIsolation(policy))
	cmd := exec.Command("/usr/bin/true")
	require.NoError(t, applyProcessIsolation(cmd, policy))
	require.True(t, cmd.SysProcAttr == nil || cmd.SysProcAttr.Credential == nil)

	processIsolationGetgroups = func() ([]int, error) { return nil, errors.New("groups") }
	require.Error(t, verifyProcessIsolation(policy))
	processIsolationGetgroups = func() ([]int, error) { return []int{22}, nil }
	require.Error(t, verifyProcessIsolation(policy))
	processIsolationGeteuid = func() int { return 12 }
	require.Error(t, verifyProcessIsolation(policy))
	cmd = exec.Command("/usr/bin/true")
	require.NoError(t, applyProcessIsolation(cmd, policy))
	require.NotNil(t, cmd.SysProcAttr)
	require.NotNil(t, cmd.SysProcAttr.Credential)
	require.Error(t, verifyProcessIsolation(nil))
	require.Error(t, applyProcessIsolation(nil, policy))
	require.Error(t, applyProcessIsolation(exec.Command("/usr/bin/true"), nil))
	require.Error(t, applyProcessIsolation(nil, &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}))
	require.NoError(t, applyProcessIsolation(exec.Command("/usr/bin/true"), &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}))

	cmd = exec.Command("/usr/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, applyProcessIsolation(cmd, policy))
	require.True(t, cmd.SysProcAttr.Setpgid)
	require.NotNil(t, cmd.SysProcAttr.Credential)
	require.Equal(t, policy.UID, cmd.SysProcAttr.Credential.Uid)
	require.Equal(t, policy.GID, cmd.SysProcAttr.Credential.Gid)
	require.Empty(t, cmd.SysProcAttr.Credential.Groups)
}

// TestExplicitProcessIsolationIsRefusedOffLinux proves the platform gate lives
// in the provider boundary rather than only in the command's policy loader, so
// an embedder calling the Go API directly cannot obtain the hardened boundary
// on a platform that cannot host it.
func TestExplicitProcessIsolationIsRefusedOffLinux(t *testing.T) {
	originalPlatform := processRuntimePlatform
	t.Cleanup(func() { processRuntimePlatform = originalPlatform })

	policy := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: standaloneTestOwnerID, StandaloneStateRoot: standaloneTestStateRoot,
	}

	for _, platform := range []string{"darwin", "freebsd", "openbsd", "windows"} {
		t.Run(platform, func(t *testing.T) {
			processRuntimePlatform = platform
			require.ErrorContains(t, validateProcessIsolation(policy), "only on linux")
			require.ErrorContains(t, applyProcessIsolation(exec.Command("/usr/bin/true"), policy), "only on linux")
			require.ErrorContains(t, verifyProcessIsolation(policy), "only on linux")
		})
	}
}
