//nolint:tagliatelle // Hermes REST auth payloads use snake_case names.
package hermes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Native login-flow discriminators reported by the provider catalog. The start
// shape differs per flow, so every decoder branches on this value.
const (
	AuthFlowDeviceCode = "device_code"
	AuthFlowPKCE       = "pkce"
	AuthFlowExternal   = "external"
)

// Native credential kinds stored in a pool slot.
const (
	AuthTypeAPIKey = "api_key"
	AuthTypeOAuth  = "oauth"
)

// Native poll states.
const (
	AuthPollPending  = "pending"
	AuthPollComplete = "complete"
	AuthPollSlowDown = "slow_down"
	AuthPollDenied   = "denied"
)

const (
	authProvidersPath = "/providers/oauth"
	authCodeField     = "code"
	authEnvPath       = "/env"
	authRequestLimit  = 1 << 20
)

var authHTTPClient = func() *http.Client { return &http.Client{Timeout: 30 * time.Second} }

// AuthProvider is one entry of the native OAuth provider catalog. Only the
// identity fields are read: every other member of the native entry, including
// its per-provider status object, carries origin claims or token fragments this
// adapter never forwards.
type AuthProvider struct {
	ID             string
	Name           string
	Flow           string
	Disconnectable bool
}

// AuthAPIKeyProvider is one provider of the native environment catalog that
// accepts an operator-supplied key.
type AuthAPIKeyProvider struct {
	ID   string
	Name string
}

// authEnvEntry is the allowlist applied to one member of the native environment
// map, which is keyed by environment-variable name rather than by provider. The
// member also reports whether the variable is set on disk and a redaction of the
// configured value; neither is named here, so neither survives the decode.
type authEnvEntry struct {
	Provider      string `json:"provider"`
	ProviderLabel string `json:"provider_label"`
	IsPassword    bool   `json:"is_password"`
}

// AuthStart is a decoded native flow start. The device-code and pkce shapes
// disagree on which URL member carries the authorization URL and on whether a
// user code and poll interval exist at all, so the decoder branches on Flow.
type AuthStart struct {
	SessionID    string
	Flow         string
	URL          string
	UserCode     string
	PollInterval time.Duration
	ExpiresIn    time.Duration
}

// AuthPoll is a decoded native poll result.
type AuthPoll struct {
	State string
}

// AuthMaterial is the token material one credential-pool slot carries.
// AccessExpiresAt is absolute epoch milliseconds where the native store records
// one; ExpiresIn is the relative lifetime the device path records with no
// issued-at anchor.
type AuthMaterial struct {
	AuthType        string
	AccessToken     string
	RefreshToken    string
	AccessExpiresAt int64
	ExpiresIn       time.Duration
}

// AuthStatusError reports a native HTTP refusal. The native body never travels
// with it: a provider refusal can embed an entire upstream response.
type AuthStatusError struct {
	StatusCode int
}

func (e *AuthStatusError) Error() string {
	return fmt.Sprintf("hermes auth request failed with status %d", e.StatusCode)
}

// AuthRefused reports whether err is a native refusal the caller caused, as
// opposed to a transport failure.
func AuthRefused(err error) bool {
	var status *AuthStatusError

	return errors.As(err, &status) && status.StatusCode >= 400 && status.StatusCode < 500
}

func (s *hermesServer) AuthProviders(ctx context.Context) ([]AuthProvider, error) {
	var payload struct {
		Providers []struct {
			ID             string `json:"id"`
			Name           string `json:"name"`
			Flow           string `json:"flow"`
			Disconnectable bool   `json:"disconnectable"`
		} `json:"providers"`
	}

	if err := s.authRequest(ctx, http.MethodGet, authProvidersPath, nil, &payload); err != nil {
		return nil, err
	}

	providers := make([]AuthProvider, 0, len(payload.Providers))
	for _, entry := range payload.Providers {
		providers = append(providers, AuthProvider{
			ID:             entry.ID,
			Name:           entry.Name,
			Flow:           entry.Flow,
			Disconnectable: entry.Disconnectable,
		})
	}

	return providers, nil
}

// AuthAPIKeyProviders folds the native environment map into one entry per
// provider. A variable carrying no provider tag is not a provider credential at
// all — it is a tool token, a channel secret, or an operator's own custom key —
// and a tagged variable that is not a password field is a base URL, a region, or
// a service-account path, none of which is a key an operator can be prompted
// for. Both are dropped, so a provider surfaces only when it owns at least one
// secret-valued variable.
func (s *hermesServer) AuthAPIKeyProviders(ctx context.Context) ([]AuthAPIKeyProvider, error) {
	var payload map[string]authEnvEntry

	if err := s.authRequest(ctx, http.MethodGet, authEnvPath, nil, &payload); err != nil {
		return nil, err
	}

	// Several variables share one provider, so the fold walks the map in
	// variable-name order to keep the result stable across calls.
	names := make([]string, 0, len(payload))
	for name := range payload {
		names = append(names, name)
	}

	sort.Strings(names)

	providers := make([]AuthAPIKeyProvider, 0, len(names))
	seen := make(map[string]struct{}, len(names))

	for _, name := range names {
		entry := payload[name]
		if entry.Provider == "" || !entry.IsPassword {
			continue
		}

		if _, duplicate := seen[entry.Provider]; duplicate {
			continue
		}

		seen[entry.Provider] = struct{}{}
		providers = append(providers, AuthAPIKeyProvider{ID: entry.Provider, Name: entry.ProviderLabel})
	}

	return providers, nil
}

func (s *hermesServer) AuthStart(ctx context.Context, providerID string) (AuthStart, error) {
	var payload struct {
		SessionID       string  `json:"session_id"`
		Flow            string  `json:"flow"`
		VerificationURL string  `json:"verification_url"`
		AuthURL         string  `json:"auth_url"`
		UserCode        string  `json:"user_code"`
		PollInterval    float64 `json:"poll_interval"`
		ExpiresIn       float64 `json:"expires_in"`
	}

	path := authProvidersPath + "/" + url.PathEscape(providerID) + "/start"
	if err := s.authRequest(ctx, http.MethodPost, path, map[string]any{}, &payload); err != nil {
		return AuthStart{}, err
	}

	start := AuthStart{
		SessionID: payload.SessionID,
		Flow:      payload.Flow,
		ExpiresIn: secondsToDuration(payload.ExpiresIn),
	}

	switch payload.Flow {
	case AuthFlowDeviceCode:
		start.URL = payload.VerificationURL
		start.UserCode = payload.UserCode
		start.PollInterval = secondsToDuration(payload.PollInterval)
	case AuthFlowPKCE:
		start.URL = payload.AuthURL
	default:
		return AuthStart{}, fmt.Errorf("hermes auth start returned unsupported flow %q", payload.Flow)
	}

	if start.SessionID == "" || start.URL == "" {
		return AuthStart{}, errors.New("hermes auth start omitted its session id or authorization url")
	}

	return start, nil
}

func (s *hermesServer) AuthSubmit(ctx context.Context, providerID string, nativeSessionID string, input string) error {
	path := authProvidersPath + "/" + url.PathEscape(providerID) + "/submit"
	body := map[string]any{fieldSessionID: nativeSessionID, authCodeField: input}

	return s.authRequest(ctx, http.MethodPost, path, body, nil)
}

func (s *hermesServer) AuthPollFlow(ctx context.Context, providerID string, nativeSessionID string) (AuthPoll, error) {
	var payload struct {
		Status string `json:"status"`
	}

	path := authProvidersPath + "/" + url.PathEscape(providerID) + "/poll/" + url.PathEscape(nativeSessionID)
	if err := s.authRequest(ctx, http.MethodGet, path, nil, &payload); err != nil {
		return AuthPoll{}, err
	}

	return AuthPoll{State: payload.Status}, nil
}

func (s *hermesServer) AuthCancelFlow(ctx context.Context, nativeSessionID string) error {
	path := authProvidersPath + "/sessions/" + url.PathEscape(nativeSessionID)

	return s.authRequest(ctx, http.MethodDelete, path, nil, nil)
}

func (s *hermesServer) authRequest(ctx context.Context, method string, path string, body any, out any) error {
	base, token := s.authEndpoint()
	if base == "" {
		return errors.New("hermes auth endpoint is unavailable")
	}

	var payload io.Reader = http.NoBody

	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode hermes auth request: %w", err)
		}

		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, base+path, payload)
	if err != nil {
		return fmt.Errorf("build hermes auth request: %w", err)
	}

	req.Header.Set("X-Hermes-Session-Token", token)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := authHTTPClient().Do(req)
	if err != nil {
		return fmt.Errorf("call hermes auth route: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &AuthStatusError{StatusCode: resp.StatusCode}
	}

	if out == nil {
		return nil
	}

	contents, err := io.ReadAll(io.LimitReader(resp.Body, authRequestLimit))
	if err != nil {
		return fmt.Errorf("read hermes auth response: %w", err)
	}

	if err := json.Unmarshal(contents, out); err != nil {
		return fmt.Errorf("decode hermes auth response: %w", err)
	}

	return nil
}

func (s *hermesServer) authEndpoint() (string, string) {
	s.gatewayMu.Lock()
	defer s.gatewayMu.Unlock()

	if s.process == nil {
		return "", ""
	}

	return s.process.APIBaseURL, s.process.Token
}

func secondsToDuration(value float64) time.Duration {
	if value <= 0 {
		return 0
	}

	return time.Duration(value * float64(time.Second))
}

// AuthSlotLabel is the label of the one reserved credential-pool slot this
// adapter owns for a connection. Every other pool entry — ambient, environment
// derived, or operator installed — is unrepresentable on the ACP surface and is
// never read, migrated, or removed.
func AuthSlotLabel(connectionID string) string {
	return "acp-go-hermes:" + connectionID
}

// AuthSlotLabelPrefix reports whether a label belongs to this adapter at all.
func AuthSlotLabelPrefix(label string) bool {
	return strings.HasPrefix(label, "acp-go-hermes:")
}
