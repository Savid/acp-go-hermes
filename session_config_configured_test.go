package hermesacp

import (
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestConfiguredModelsRefuseMalformedIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []string
		want string
	}{
		{name: "empty", ids: []string{""}, want: "is not a <provider>/<model> id"},
		{name: "surrounding space", ids: []string{"omp/qwen "}, want: "is not a <provider>/<model> id"},
		{name: "unqualified", ids: []string{"qwen"}, want: "is not a <provider>/<model> id"},
		{name: "duplicate", ids: []string{"omp/qwen", "omp/qwen"}, want: "listed twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, newTestAgent(WithConfiguredModels(tc.ids)).optionsErr, tc.want)
		})
	}

	require.NoError(t, newTestAgent(WithConfiguredModels([]string{"omp/qwen", "omp/opencode-go/deepseek"})).optionsErr)
}

// TestHostListedModelsJoinTheirProviderGroup pins where a host-listed id lands:
// under the group its prefix names, after the native rows, as the id alone,
// with a native row of the same value standing and a new group created for a
// provider Hermes never enumerated.
func TestHostListedModelsJoinTheirProviderGroup(t *testing.T) {
	providers := nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID:     "openai",
		Name:   "OpenAI",
		Models: map[string]nativehermes.ProviderModel{"gpt-test": {ID: "gpt-test", Name: "GPT Test"}},
	}}}
	hostListed := []string{"openai/gpt-test", "openai/gpt-host", "omp/opencode-go/qwen"}

	option := modelConfigOption(sessionSnapshot{}, providers, hostListed)
	require.NotNil(t, option.Select)
	require.Equal(t, acp.SessionConfigValueId("openai/gpt-test"), option.Select.CurrentValue)

	groups := *option.Select.Options.Grouped
	require.Len(t, groups, 2)
	require.Equal(t, acp.SessionConfigGroupId("openai"), groups[0].Group)
	require.Len(t, groups[0].Options, 2)
	require.Equal(t, "GPT Test", groups[0].Options[0].Name, "the native row stands and the host entry adds nothing")
	require.NotEmpty(t, groups[0].Options[0].Meta)
	require.Equal(t, acp.SessionConfigValueId("openai/gpt-host"), groups[0].Options[1].Value)
	require.Empty(t, groups[0].Options[1].Meta, "a host-listed row carries no invented facts")
	require.Equal(t, acp.SessionConfigGroupId("omp"), groups[1].Group)
	require.Equal(t, "omp", groups[1].Name)
	require.Equal(t, acp.SessionConfigValueId("omp/opencode-go/qwen"), groups[1].Options[0].Value)
}

// TestHostListedModelsStandAloneWithoutACatalog pins the menu with nothing
// enumerated: the host-listed ids are the menu and the first is current.
func TestHostListedModelsStandAloneWithoutACatalog(t *testing.T) {
	option := modelConfigOption(sessionSnapshot{}, nativehermes.ProvidersResponse{}, []string{"omp/qwen", "omp/deepseek"})
	require.NotNil(t, option.Select)
	require.Equal(t, acp.SessionConfigValueId("omp/qwen"), option.Select.CurrentValue)
	require.Len(t, (*option.Select.Options.Grouped)[0].Options, 2)

	selected := modelConfigOption(sessionSnapshot{providerID: "omp", modelID: "deepseek"}, nativehermes.ProvidersResponse{}, []string{"omp/qwen"})
	require.Equal(t, acp.SessionConfigValueId("omp/deepseek"), selected.Select.CurrentValue,
		"a selection outranks the first host-listed id")
}
