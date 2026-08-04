package hermesacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessIsolationOptionClonesAndFailsClosed(t *testing.T) {
	base := map[string]string{"PATH": "/policy/bin", "CANARY": "base"}
	opts := applyOptions([]Option{WithProcessIsolation(ProcessIsolation{UID: 10, GID: 20, BaseEnvironment: base})})
	base["CANARY"] = "mutated"
	require.Equal(t, "base", opts.ProcessIsolation.BaseEnvironment["CANARY"])
	internal := nativeProcessIsolation(opts.ProcessIsolation, false)
	opts.ProcessIsolation.BaseEnvironment["CANARY"] = "later"
	require.Equal(t, "base", internal.BaseEnvironment["CANARY"])
	require.Nil(t, nativeProcessIsolation(nil, false))
	require.Error(t, validateProcessIsolationOption(nil))
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 0, GID: 1}))
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 1, GID: 0}))

	original := agentRuntimePlatform
	agentRuntimePlatform = "windows"
	t.Cleanup(func() { agentRuntimePlatform = original })
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 1, GID: 1}))
}
