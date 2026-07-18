//go:build darwin

package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const darwinSetsidEscapeMode = "ACP_GO_HERMES_DARWIN_CONTAINMENT_HELPER"

func TestDarwinSetsidEscapeHelper(t *testing.T) {
	switch os.Getenv(darwinSetsidEscapeMode) {
	case "root":
		child := exec.Command(os.Args[0], "-test.run=^TestDarwinSetsidEscapeHelper$")
		child.Env = append(os.Environ(), darwinSetsidEscapeMode+"=child")
		child.Stdout = nil
		child.Stderr = nil
		require.NoError(t, child.Start())
	case "child":
		_, err := unix.Setsid()
		require.NoError(t, err)
		pidFile := os.Getenv("ACP_GO_HERMES_DARWIN_CONTAINMENT_PID_FILE")
		require.NoError(t, os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600))
		for {
			time.Sleep(time.Minute)
		}
	default:
		t.Skip("helper subprocess only")
	}
}

func TestDarwinBestEffortCloseLeavesSetsidEscapeOutsideSelectedBoundary(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "escaped.pid")
	cmd := exec.Command(os.Args[0], "-test.run=^TestDarwinSetsidEscapeHelper$")
	cmd.Env = append(os.Environ(),
		darwinSetsidEscapeMode+"=root",
		"ACP_GO_HERMES_DARWIN_CONTAINMENT_PID_FILE="+pidFile,
	)
	configureHermesProcess(cmd)
	tree, err := startContainedProcess(cmd, darwinTestContainmentSpec(t))
	require.NoError(t, err)

	escapedPID := waitForPIDFile(t, pidFile)
	t.Cleanup(func() { _ = unix.Kill(escapedPID, syscall.SIGKILL) })
	require.NoError(t, tree.complete(darwinContainmentDeadline))
	require.NoError(t, unix.Kill(escapedPID, 0))
}
