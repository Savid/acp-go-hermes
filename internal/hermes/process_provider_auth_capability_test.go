//go:build unix

package hermes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeCapabilityParsingIsExact(t *testing.T) {
	capabilities := parseRuntimeCapabilities("Hermes Agent v0.20.0\n" +
		"Runtime capabilities: other, provider-auth-home-v1 future\n")
	require.Contains(t, capabilities, providerAuthHomeCapability)
	require.Contains(t, capabilities, "other")
	require.Contains(t, capabilities, "future")
	require.NotContains(t, capabilities, "provider-auth-home")

	require.Empty(t, parseRuntimeCapabilities(
		"Hermes Agent v0.20.0 provider-auth-home-v1\nRuntime capability: provider-auth-home-v1\n",
	))
}

func TestServerReportsOnlyItsProvenProcessCapability(t *testing.T) {
	require.False(t, (*hermesServer)(nil).ProviderAuthHomeSupported())
	require.False(t, (&hermesServer{}).ProviderAuthHomeSupported())
	require.False(t, (&hermesServer{process: &Process{}}).ProviderAuthHomeSupported())
	require.True(t, (&hermesServer{process: &Process{providerAuthHomeSupported: true}}).ProviderAuthHomeSupported())
}

func TestOfficialRuntimeStartsWithoutPretendingItsEphemeralLoginIsDurable(t *testing.T) {
	restoreProcessSeams(t)

	executable := fakeHermesExecutable(t, fakeProcessModeOfficial)
	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath:   executable,
		Home:             t.TempDir(),
		ProviderAuthHome: t.TempDir(),
	})

	process, err := Start(t.Context(), options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, process.Close(context.Background())) })
	require.False(t, process.ProviderAuthHomeSupported())
}

func TestPatchedRuntimeCarriesTheDurableProviderAuthContract(t *testing.T) {
	restoreProcessSeams(t)

	executable := fakeHermesExecutable(t, fakeProcessModeOK)
	options := darwinTestProcessOptions(t, ProcessOptions{
		ExecutablePath:   executable,
		Home:             t.TempDir(),
		ProviderAuthHome: t.TempDir(),
	})

	process, err := Start(t.Context(), options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, process.Close(context.Background())) })
	require.True(t, process.ProviderAuthHomeSupported())
}
