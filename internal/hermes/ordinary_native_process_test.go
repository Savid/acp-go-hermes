package hermes

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const ordinaryNativeHelperMode = "ACP_GO_HERMES_ORDINARY_NATIVE_HELPER"

func TestOrdinaryNativeProcessHelper(t *testing.T) {
	switch os.Getenv(ordinaryNativeHelperMode) {
	case "exit":
		_, _ = io.WriteString(os.Stdout, "ready\n")
		os.Exit(7)
	case "block":
		_, _ = io.WriteString(os.Stdout, "ready\n")
		for {
			time.Sleep(time.Second)
		}
	}
}

func ordinaryNativeHelperRequest(t *testing.T, mode string) NativeRequest {
	t.Helper()

	executable, err := os.Executable()
	require.NoError(t, err)

	return NativeRequest{
		Executable: executable,
		Arguments:  []string{"-test.run=^TestOrdinaryNativeProcessHelper$"},
		Environment: append(os.Environ(),
			ordinaryNativeHelperMode+"="+mode,
		),
		WorkingDirectory: t.TempDir(),
	}
}

func TestOrdinaryNativeNaturalCompletionIsNotReclassified(t *testing.T) {
	process, err := startOrdinaryNative(t.Context(), ordinaryNativeHelperRequest(t, "exit"))
	require.NoError(t, err)
	require.NotNil(t, process.Stdin())
	require.NotNil(t, process.Stderr())

	output, err := io.ReadAll(process.Stdout())
	require.NoError(t, err)
	require.Equal(t, "ready\n", string(output))

	result, waitErr := process.Wait(t.Context())
	require.Error(t, waitErr)
	require.Equal(t, 7, result.ExitCode)
	require.Zero(t, result.Signal)
	require.False(t, result.Revoked)
	require.NoError(t, process.Revoke(t.Context()))

	again, againErr := process.Wait(t.Context())
	require.Equal(t, result, again)
	require.Equal(t, waitErr, againErr)
	require.False(t, again.Revoked)
}

func TestOrdinaryNativeWaitCancellationDetachesUntilRevoke(t *testing.T) {
	process, err := startOrdinaryNative(t.Context(), ordinaryNativeHelperRequest(t, "block"))
	require.NoError(t, err)

	ready := make([]byte, len("ready\n"))
	_, err = io.ReadFull(process.Stdout(), ready)
	require.NoError(t, err)
	require.Equal(t, "ready\n", string(ready))

	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	_, waitErr := process.Wait(waitCtx)
	require.ErrorIs(t, waitErr, context.Canceled)

	require.NoError(t, process.Revoke(t.Context()))
	result, waitErr := process.Wait(t.Context())
	require.Error(t, waitErr)
	require.True(t, result.Revoked)
	require.NotZero(t, result.ExitCode)
}

func TestOrdinaryNativeStartFailuresDoNotReturnAProcess(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	process, err := startOrdinaryNative(cancelled, NativeRequest{Executable: "unused"})
	require.Nil(t, process)
	require.ErrorIs(t, err, context.Canceled)

	process, err = startOrdinaryNative(t.Context(), NativeRequest{
		Executable:       strings.Repeat("missing-ordinary-native-", 2),
		Environment:      os.Environ(),
		WorkingDirectory: t.TempDir(),
	})
	require.Nil(t, process)
	require.Error(t, err)
	require.False(t, errors.Is(err, context.Canceled))
}
