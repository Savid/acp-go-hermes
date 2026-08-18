package hermes

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPromptDispatchHookRunsAtGatewayAcceptance(t *testing.T) {
	dispatchErr := errors.New("acceptance refused")
	called := 0
	ctx := WithPromptDispatch(t.Context(), func(context.Context) error {
		called++

		return dispatchErr
	})
	require.ErrorIs(t, NotifyPromptDispatch(ctx), dispatchErr)
	require.Equal(t, 1, called)

	fake := newFakeGatewayServer(t)
	server := newGatewayBackedHermesServer(t, fake, "")
	_, err := server.submitGatewayTextForLive(ctx, "stored", "live", "hello", nil)
	require.ErrorIs(t, err, dispatchErr)
	require.Equal(t, 2, called)
}

func TestProviderTreeVacancyRequiresAuthoritativeInventory(t *testing.T) {
	var process *Process
	vacant, proved := process.ProviderTreeVacant()
	require.False(t, vacant)
	require.False(t, proved)

	process = &Process{tree: &processContainment{}}
	vacant, proved = process.ProviderTreeVacant()
	require.False(t, vacant)
	require.False(t, proved)

	process.tree.treeVacantFn = func() (bool, bool) { return true, true }
	vacant, proved = process.ProviderTreeVacant()
	require.True(t, vacant)
	require.True(t, proved)

	var server *hermesServer
	vacant, proved = server.ProviderTreeVacant()
	require.False(t, vacant)
	require.False(t, proved)

	server = &hermesServer{process: process}
	vacant, proved = server.ProviderTreeVacant()
	require.True(t, vacant)
	require.True(t, proved)
}
