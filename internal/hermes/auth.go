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
	"time"
)

// Native login-flow discriminators reported by the provider catalog. The start
// shape differs per flow, so every decoder branches on this value.
const (
	AuthFlowDeviceCode = "device_code"
	AuthFlowPKCE       = "pkce"
	AuthFlowExternal   = "external"
)

// Native poll states reported by Hermes' dashboard OAuth API.
const (
	AuthPollPending  = "pending"
	AuthPollApproved = "approved"
	AuthPollDenied   = "denied"
	AuthPollExpired  = "expired"
	AuthPollError    = "error"
)

const (
	authProvidersPath = "/providers/oauth"
	authCodeField     = "code"
	authRequestLimit  = 1 << 20
)

var authHTTPClient = func() *http.Client { return &http.Client{Timeout: 30 * time.Second} }

// AuthProvider is the values-free subset of one native OAuth catalog entry.
type AuthProvider struct {
	ID             string
	Name           string
	Flow           string
	Disconnectable bool
	LoggedIn       bool
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
			Status         struct {
				LoggedIn bool `json:"logged_in"`
			} `json:"status"`
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
			LoggedIn:       entry.Status.LoggedIn,
		})
	}

	return providers, nil
}

// AuthStart begins a native login. It refuses before the native call when the
// session's process is not shadowing the browser launchers, because the native
// start is what makes hermes open a tab.
func (s *hermesServer) AuthStart(ctx context.Context, providerID string) (AuthStart, error) {
	if !s.process.BrowserLaunchContained() {
		return AuthStart{}, ErrBrowserLaunchUncontained
	}

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

	switch payload.Status {
	case AuthPollPending:
		return AuthPoll{State: AuthPollPending}, nil
	case AuthPollApproved:
		return AuthPoll{State: AuthPollApproved}, nil
	case AuthPollDenied:
		return AuthPoll{State: AuthPollDenied}, nil
	case AuthPollExpired:
		return AuthPoll{State: AuthPollExpired}, nil
	case AuthPollError:
		return AuthPoll{State: AuthPollError}, nil
	default:
		return AuthPoll{}, fmt.Errorf("hermes auth poll returned unsupported status %q", payload.Status)
	}
}

func (s *hermesServer) AuthCancelFlow(ctx context.Context, nativeSessionID string) error {
	path := authProvidersPath + "/sessions/" + url.PathEscape(nativeSessionID)

	return s.authRequest(ctx, http.MethodDelete, path, nil, nil)
}

func (s *hermesServer) AuthDisconnect(ctx context.Context, providerID string) error {
	path := authProvidersPath + "/" + url.PathEscape(providerID)

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
