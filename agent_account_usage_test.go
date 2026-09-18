package hermesacp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

const usageSessionField = "sessionId"

type usageTransportFunc func(*http.Request) (*http.Response, error)

func (f usageTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAccountUsageBindsNativeCredentialsAndHoldsGate(t *testing.T) {
	t.Parallel()
	for _, rotate := range []bool{false, true} {
		t.Run(map[bool]string{false: "stable", true: "rotated"}[rotate], func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "providers.json")
			write := func(key string) {
				t.Helper()
				require.NoError(t, os.WriteFile(path, []byte(`{"configured":true,"provider":"openrouter","api_key":"`+key+`","api_mode":"chat_completions","base_url":"https://openrouter.ai/api/v1"}`), 0600))
			}
			write("native-key")
			a := NewAgent(testOptions(t, WithEnv(map[string]string{fakeHermesEnv: "1", "ACP_GO_HERMES_TEST_USAGE_ACCESS": path}))...)
			t.Cleanup(func() { require.NoError(t, a.Close()) })
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			a.usageTransport = usageTransportFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") != "Bearer native-key" {
					return nil, io.ErrUnexpectedEOF
				}
				if r.URL.Path == "/api/v1/key" {
					close(entered)
					select {
					case <-release:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}

					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"data":{"limit":0,"limit_remaining":-0.02,"usage":1.125}}`))}, nil
				}

				return &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header), Body: http.NoBody}, nil
			})
			initialized, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
			require.NoError(t, err)
			require.Contains(t, initialized.AgentCapabilities.Meta["hermes"], wire.AccountUsageCapabilityKey)
			session, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err)
			params, err := json.Marshal(map[string]any{usageSessionField: session.SessionId, "providerId": "openrouter"})
			require.NoError(t, err)
			type result struct {
				response wire.AccountUsageResponse
				err      error
			}
			done := make(chan result, 1)
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			go func() { response, readErr := a.accountUsage(ctx, params); done <- result{response, readErr} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("provider read not reached")
			}
			_, err = a.accountUsage(ctx, params)
			require.Equal(t, "session_prompt", requestErrorData(t, err)["limit"])
			_, err = a.Prompt(ctx, wire.PromptRequest(session.SessionId))
			require.Equal(t, "session_prompt", requestErrorData(t, err)["limit"])
			if rotate {
				write("changed-key")
			}
			unblock()
			got := <-done
			if rotate {
				require.Equal(t, "account_usage", requestErrorData(t, got.err)["class"])
				require.False(t, got.response.Available)

				return
			}
			require.NoError(t, got.err)
			require.Len(t, got.response.Balances, 2)
			require.Equal(t, 0.0, got.response.Balances[0].Limit.Amount)
			require.Equal(t, -0.02, got.response.Balances[0].Remaining.Amount)
			require.Equal(t, 1.125, got.response.Balances[1].Used.Amount)
		})
	}
}
