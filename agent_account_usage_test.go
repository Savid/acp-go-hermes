package hermesacp

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

type gatewayTransport func(*http.Request) (*http.Response, error)

func (f gatewayTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const gatewayReport = `{"generatedAt":1,"reports":[{"provider":"anthropic","fetchedAt":1789807237831,"limits":[{"id":"anthropic:5h","label":"Claude 5 Hour","window":{"id":"5h","durationMs":18000000,"resetsAt":1789817399682},"amount":{"usedFraction":0.25,"unit":"percent"},"status":"ok"}],"metadata":{}}]}`

// A provider the home's config.yaml routes through a gateway is read from
// that gateway's report with the key named by key_env; hermes reads nothing
// natively.
func TestAccountUsageReadsThroughConfiguredGateway(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithEnv(map[string]string{fakeHermesEnv: "1", "OMP_GATEWAY_KEY": "gateway-key"}))...)
	t.Cleanup(func() { require.NoError(t, a.Close()) })
	require.NoError(t, os.MkdirAll(a.options.Home, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(a.options.Home, "config.yaml"), []byte("providers:\n  omp:\n    api: https://gateway.example/v1\n    key_env: OMP_GATEWAY_KEY\n    default_model: m\n"), 0o600))
	var asked []string
	a.usageTransport = gatewayTransport(func(r *http.Request) (*http.Response, error) {
		asked = append(asked, r.URL.Host+r.URL.Path+" "+r.Header.Get("Authorization"))

		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(gatewayReport))}, nil
	})
	initialized, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	require.Contains(t, initialized.AgentCapabilities.Meta[vendor], wire.AccountUsageCapabilityKey)

	read := func(params map[string]any) (wire.AccountUsageResponse, error) {
		t.Helper()
		raw, marshalErr := json.Marshal(params)
		require.NoError(t, marshalErr)
		result, callErr := a.HandleExtensionMethod(t.Context(), AccountUsageMethod, raw)
		if callErr != nil {
			return wire.AccountUsageResponse{}, callErr
		}
		response, ok := result.(wire.AccountUsageResponse)
		require.True(t, ok)

		return response, nil
	}

	response, err := read(map[string]any{"providerId": "anthropic"})
	require.NoError(t, err)
	require.True(t, response.Available)
	require.Equal(t, "session", response.Limits[0].ID)
	require.Equal(t, []string{"gateway.example/v1/usage Bearer gateway-key"}, asked)

	response, err = read(map[string]any{"providerId": "openrouter"})
	require.NoError(t, err)
	require.Equal(t, wire.AccountUsageUnavailable(wire.AccountUsageNotAuthenticated), response)

	_, err = read(map[string]any{})
	require.Equal(t, map[string]any{"error": "missing", "field": "providerId"}, requestErrorData(t, err))

	_, err = read(map[string]any{"providerId": "xai"})
	require.Equal(t, map[string]any{"error": "unsupported", "field": "providerId"}, requestErrorData(t, err))
}
