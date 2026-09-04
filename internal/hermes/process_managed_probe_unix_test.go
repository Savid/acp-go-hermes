//go:build unix

package hermes

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type authorityProbeStarter struct {
	executable string
	mu         sync.Mutex
	versions   int
	serves     int
}

type authorityProbeProcess struct{ NativeProcess }

func (p authorityProbeProcess) Wait(ctx context.Context) (NativeResult, error) {
	result, err := p.NativeProcess.Wait(ctx)
	if result.Revoked {
		var exitErr interface{ ExitCode() int }
		if errors.As(err, &exitErr) {
			err = nil
		}
	}

	return result, err
}

func (s *authorityProbeStarter) start(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	s.mu.Lock()
	if len(request.Arguments) == 1 && request.Arguments[0] == argVersion {
		s.versions++
	} else {
		s.serves++
	}
	s.mu.Unlock()
	request.Executable = s.executable

	process, err := startOrdinaryNative(ctx, request)
	if err != nil {
		return nil, err
	}

	return authorityProbeProcess{NativeProcess: process}, nil
}

func managedProbeOptions(t *testing.T, starter *authorityProbeStarter) ProcessOptions {
	t.Helper()

	return ProcessOptions{
		ExecutablePath: "logical-hermes",
		Home:           t.TempDir(),
		ScratchParent:  t.TempDir(),
		NativeEnvironment: map[string]string{
			"PATH": os.Getenv("PATH"),
		},
		StartNative:       starter.start,
		PrepareNativeTree: func(context.Context, string) error { return nil },
		ReclaimNativeTree: func(context.Context, string) error { return nil },
		Timeout:           10 * time.Second,
	}
}

func TestManagedVersionProbeIsScopedToAuthority(t *testing.T) {
	first := &authorityProbeStarter{executable: fakeHermesExecutable(t, fakeProcessModeOK)}
	process, err := Start(t.Context(), managedProbeOptions(t, first))
	require.NoError(t, err)
	require.NoError(t, process.Close(t.Context()))

	second := &authorityProbeStarter{executable: fakeHermesExecutable(t, fakeProcessModeOldVersion)}
	_, err = Start(t.Context(), managedProbeOptions(t, second))
	require.ErrorContains(t, err, "below minimum")
	require.Equal(t, 1, first.versions)
	require.Equal(t, 1, second.versions, "a second authority must prove its own logical selector")
	require.Zero(t, second.serves)
}

func TestManagedGatewayMethodProbeIsScopedToAuthority(t *testing.T) {
	first := &authorityProbeStarter{executable: fakeHermesExecutable(t, fakeProcessModeOK)}
	process, err := Start(t.Context(), managedProbeOptions(t, first))
	require.NoError(t, err)
	require.NoError(t, process.Close(t.Context()))

	second := &authorityProbeStarter{executable: fakeHermesExecutable(t, fakeProcessModeMissingMethod)}
	_, err = Start(t.Context(), managedProbeOptions(t, second))
	require.Error(t, err)
	var rpcErr *RPCError
	require.True(t, errors.As(err, &rpcErr))
	require.Equal(t, -32601, rpcErr.Code)
	require.Equal(t, 1, first.versions)
	require.Equal(t, 1, first.serves)
	require.Equal(t, 1, second.versions)
	require.Equal(t, 1, second.serves, "a second authority must run its own method sweep")
}
