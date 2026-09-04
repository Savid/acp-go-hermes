//go:build windows

package hermes

import (
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOrdinaryRevokeAcceptsTheWindowsFinishedProcessError pins that a revoke
// racing a finish is not reported as a containment refusal on Windows. Once the
// wait has released the process handle, Kill answers syscall.EINVAL — "invalid
// argument" — rather than the sentinel POSIX uses, and a revoke that surfaced
// it would fail every close of an already-finished turn.
func TestOrdinaryRevokeAcceptsTheWindowsFinishedProcessError(t *testing.T) {
	process := &ordinaryNativeProcess{
		cmd:  &exec.Cmd{Process: &os.Process{Pid: 1}},
		kill: func() error { return syscall.EINVAL },
	}

	require.NoError(t, process.Revoke(t.Context()))
	require.False(t, process.revoked)
}
