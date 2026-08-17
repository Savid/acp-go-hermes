//go:build windows

package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestProcessIsolationOmissionResolvesAndRunsOnWindows is the runtime evidence
// for ordinary Windows execution. Nothing here is mirrored or stubbed: the
// harness name is resolved out of this machine's real environment block —
// which spells the search path "Path", carries a PATHEXT, and marks no regular
// file executable — and the resolved image is then started, observed, and torn
// down through the same ordinary boundary a session uses.
func TestProcessIsolationOmissionResolvesAndRunsOnWindows(t *testing.T) {
	environment := os.Environ()

	resolved, err := resolveHarnessExecutable(nil, "cmd", environment)
	require.NoError(t, err, "ordinary resolution must find cmd on the ambient Path")
	require.True(t, filepath.IsAbs(resolved), "resolved path %q must be absolute", resolved)
	require.True(t, strings.EqualFold(filepath.Base(resolved), "cmd.exe"),
		"PATHEXT resolution produced %q", resolved)

	// The resolved image carries no execute bit, which is exactly why a
	// mode-gated resolver could never launch a Windows harness.
	info, err := os.Stat(resolved)
	require.NoError(t, err)
	require.Zero(t, info.Mode()&0o111, "Windows regular files carry no execute bit")

	configured, err := resolveHarnessExecutable(nil, resolved, environment)
	require.NoError(t, err, "ordinary resolution must accept an explicit executable path")
	require.Equal(t, resolved, configured)

	status := filepath.Join(t.TempDir(), "ordinary-identity.txt")
	output, err := os.Create(status)
	require.NoError(t, err)
	t.Cleanup(func() { _ = output.Close() })

	command := exec.Command(resolved, "/c", "echo", "ordinary-windows-child")
	command.Stdout = output
	configureHermesProcess(command)

	tree, err := startContainedProcess(command)
	require.NoError(t, err)

	// Ordinary mode publishes no provider-descendant inventory, not even zero.
	count, available := tree.descendantCount()
	require.False(t, available, "ordinary execution published an inventory of %d", count)

	wait := tree.directChild(command)
	select {
	case <-wait.done:
	case <-time.After(30 * time.Second):
		t.Fatal("ordinary child did not exit")
	}

	require.NoError(t, tree.complete(30*time.Second))
	require.NoError(t, tree.close())
	require.NoError(t, output.Close())

	contents, err := os.ReadFile(status)
	require.NoError(t, err)
	require.Contains(t, string(contents), "ordinary-windows-child")
}
