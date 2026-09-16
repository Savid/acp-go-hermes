package hermesacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

// selectOption returns the select config option with the given id.
func selectOption(t *testing.T, options []acp.SessionConfigOption, id acp.SessionConfigId) acp.SessionConfigOptionSelect {
	t.Helper()

	for _, option := range options {
		if option.Select != nil && option.Select.Id == id {
			return *option.Select
		}
	}

	t.Fatalf("config option %q is not advertised", id)

	return acp.SessionConfigOptionSelect{}
}

func selectValues(option acp.SessionConfigOptionSelect) []string {
	values := make([]string, 0, len(*option.Options.Ungrouped))
	for _, entry := range *option.Options.Ungrouped {
		values = append(values, string(entry.Value))
	}

	return values
}

func TestConfigOptionCatalog(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithConfiguredModels([]string{"fake/vision", "host/extra"}))
	h.initialize()
	session := h.newSession()

	model := selectOption(t, session.ConfigOptions, configModel)
	require.Equal(t, "select", model.Type)
	require.Equal(t, acp.SessionConfigValueId("fake/vision"), model.CurrentValue)
	require.Equal(t, []string{"fake/vision", "fake/text-only", "host/extra"}, selectValues(model),
		"native rows come first, then host-configured ids, each id once")

	for _, entry := range *model.Options.Ungrouped {
		vendorMeta, _ := entry.Meta[vendor].(map[string]any)
		if string(entry.Value) == "host/extra" {
			require.Nil(t, entry.Meta, "a host-configured id carries no native metadata")

			continue
		}

		require.Equal(t, map[string]any{"modelId": string(entry.Value)}, vendorMeta)
	}

	effort := selectOption(t, session.ConfigOptions, configEffort)
	require.Equal(t, acp.SessionConfigValueId(effortMedium), effort.CurrentValue)
	require.Equal(t, effortLevels(), selectValues(effort))
}

func TestSetConfigOptionAppliesNativeValues(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "fake/text-only"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("fake/text-only"), selectOption(t, resp.ConfigOptions, configModel).CurrentValue,
		"the published catalog is read back from the harness after the set")

	resp, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configEffort, "high"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("high"), selectOption(t, resp.ConfigOptions, configEffort).CurrentValue)

	// The selection survives a reload of the same session.
	loaded, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, h.t.TempDir()))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("fake/text-only"), selectOption(t, loaded.ConfigOptions, configModel).CurrentValue)
}

func TestSetConfigOptionRefusals(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	cases := []struct {
		name    string
		request acp.SetSessionConfigOptionRequest
		field   string
	}{
		{"boolean payload", acp.SetSessionConfigOptionRequest{Boolean: &acp.SetSessionConfigOptionBoolean{
			SessionId: session.SessionId, ConfigId: configModel, Type: "boolean", Value: true,
		}}, "type"},
		{"unknown config id", wire.SetConfigOptionRequest(session.SessionId, "bogus", "x"), "configId"},
		{"unknown model", SetModelRequest(session.SessionId, "no-provider"), "value"},
		{"empty model", SetModelRequest(session.SessionId, ""), "value"},
		{"unknown effort", wire.SetConfigOptionRequest(session.SessionId, configEffort, "turbo"), "value"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := h.conn.SetSessionConfigOption(h.ctx(), tc.request)
			require.Equal(t, -32602, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, "unsupported", data[stopReasonError])
			require.Equal(t, tc.field, data["field"])
		})
	}
}
