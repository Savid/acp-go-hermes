package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newAuthTestServer wires a hermesServer at a stub REST broker so every auth
// route is driven exactly as it is against the real one, including the session
// token header.
func newAuthTestServer(t *testing.T, handler http.HandlerFunc) *hermesServer {
	t.Helper()

	stub := httptest.NewServer(handler)
	t.Cleanup(stub.Close)

	return &hermesServer{process: &Process{APIBaseURL: stub.URL + "/api", Token: "session-token"}}
}

func TestAuthProvidersReadsIdentityFieldsOnly(t *testing.T) {
	t.Parallel()

	server := newAuthTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/providers/oauth" || r.Method != http.MethodGet {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}

		if r.Header.Get("X-Hermes-Session-Token") != "session-token" {
			t.Errorf("missing session token header")
		}

		_, _ = w.Write([]byte(`{"providers":[
			{"id":"xai-oauth","name":"xAI","flow":"device_code","disconnectable":true,
			 "status":{"connected":true,"token_preview":"…abcd","source":"file",
			  "source_label":"/home/operator/.hermes/auth.json","last_refresh":null},
			 "disconnect_command":"rm -f ~/.claude/.credentials.json",
			 "disconnect_hint":"remove the file yourself"},
			{"id":"claude-code","name":"external","flow":"external"}
		]}`))
	})

	providers, err := server.AuthProviders(context.Background())
	if err != nil {
		t.Fatalf("AuthProviders: %v", err)
	}

	if len(providers) != 2 {
		t.Fatalf("providers = %#v", providers)
	}

	encoded, err := json.Marshal(providers)
	if err != nil {
		t.Fatalf("marshal providers: %v", err)
	}

	for _, dropped := range []string{"token_preview", "source", "source_label", "disconnect_command", "disconnect_hint"} {
		if strings.Contains(string(encoded), dropped) {
			t.Fatalf("catalog forwarded %q: %s", dropped, encoded)
		}
	}

	if providers[0].ID != "xai-oauth" || providers[0].Flow != AuthFlowDeviceCode || !providers[0].Disconnectable {
		t.Fatalf("provider = %#v", providers[0])
	}
}

func TestAuthProvidersToleratesAnAddedNativeField(t *testing.T) {
	t.Parallel()

	server := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"providers":[{"id":"nous","name":"Nous","flow":"device_code","added_upstream":1}],"added_top_level":true}`))
	})

	providers, err := server.AuthProviders(context.Background())
	if err != nil {
		t.Fatalf("an added upstream field broke enumeration: %v", err)
	}

	if len(providers) != 1 || providers[0].ID != "nous" {
		t.Fatalf("providers = %#v", providers)
	}
}

func TestAuthProvidersReportsANonUniformStatusObject(t *testing.T) {
	t.Parallel()

	server := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"providers":[
			{"id":"anthropic","name":"Anthropic API Key","flow":"pkce","status":{"connected":false}},
			{"id":"nous","name":"Nous","flow":"device_code","status":{"connected":true,"token_preview":null,"last_refresh":"2026-01-01"}}
		]}`))
	})

	providers, err := server.AuthProviders(context.Background())
	if err != nil {
		t.Fatalf("a provider omitting a status member broke enumeration: %v", err)
	}

	if len(providers) != 2 {
		t.Fatalf("providers = %#v", providers)
	}
}

func TestAuthAPIKeyProvidersRequiresAnEnvironmentVariable(t *testing.T) {
	t.Parallel()

	server := newAuthTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/env" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}

		_, _ = w.Write([]byte(`{"providers":[
			{"id":"openai","name":"OpenAI","key_env":"OPENAI_API_KEY"},
			{"id":"nokey","name":"No Key"}
		]}`))
	})

	providers, err := server.AuthAPIKeyProviders(context.Background())
	if err != nil {
		t.Fatalf("AuthAPIKeyProviders: %v", err)
	}

	if len(providers) != 1 || providers[0].ID != "openai" {
		t.Fatalf("providers = %#v", providers)
	}
}

func TestAuthStartBranchesOnTheNativeFlow(t *testing.T) {
	t.Parallel()

	device := newAuthTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/providers/oauth/xai-oauth/start" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}

		_, _ = w.Write([]byte(`{"session_id":"s1","flow":"device_code",
			"verification_url":"https://accounts.x.ai/oauth2/device?user_code=ABCD",
			"user_code":"ABCD","poll_interval":3,"expires_in":1800}`))
	})

	start, err := device.AuthStart(context.Background(), "xai-oauth")
	if err != nil {
		t.Fatalf("AuthStart: %v", err)
	}

	if start.URL == "" || start.UserCode != "ABCD" || start.PollInterval != 3*time.Second || start.ExpiresIn != 1800*time.Second {
		t.Fatalf("device start = %#v", start)
	}

	pkce := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"session_id":"s2","flow":"pkce","auth_url":"https://claude.ai/oauth/authorize","expires_in":900}`))
	})

	start, err = pkce.AuthStart(context.Background(), "anthropic")
	if err != nil {
		t.Fatalf("AuthStart: %v", err)
	}

	if start.URL != "https://claude.ai/oauth/authorize" {
		t.Fatalf("pkce start read the device-code url member: %#v", start)
	}

	if start.UserCode != "" || start.PollInterval != 0 {
		t.Fatalf("pkce start invented a user code or poll interval: %#v", start)
	}
}

func TestAuthStartRejectsUnusableShapes(t *testing.T) {
	t.Parallel()

	external := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"session_id":"s","flow":"external"}`))
	})

	if _, err := external.AuthStart(context.Background(), "claude-code"); err == nil {
		t.Fatal("an external flow start was accepted")
	}

	incomplete := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"flow":"pkce","auth_url":""}`))
	})

	if _, err := incomplete.AuthStart(context.Background(), "anthropic"); err == nil {
		t.Fatal("a start with no session id or url was accepted")
	}

	refused := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"upstream body with set-cookie"}`))
	})

	_, err := refused.AuthStart(context.Background(), "xai-oauth")
	if !AuthRefused(err) {
		t.Fatalf("a native 429 is not a refusal: %v", err)
	}

	if strings.Contains(err.Error(), "set-cookie") {
		t.Fatalf("native body crossed the boundary: %v", err)
	}
}

func TestAuthSubmitPollAndCancel(t *testing.T) {
	t.Parallel()

	var seen []string

	server := newAuthTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)

		if r.URL.Path == "/api/providers/oauth/anthropic/poll/s1" {
			_, _ = w.Write([]byte(`{"status":"complete"}`))

			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	if err := server.AuthSubmit(context.Background(), "anthropic", "s1", "code#state"); err != nil {
		t.Fatalf("AuthSubmit: %v", err)
	}

	poll, err := server.AuthPollFlow(context.Background(), "anthropic", "s1")
	if err != nil {
		t.Fatalf("AuthPollFlow: %v", err)
	}

	if poll.State != AuthPollComplete {
		t.Fatalf("poll = %#v", poll)
	}

	if err := server.AuthCancelFlow(context.Background(), "s1"); err != nil {
		t.Fatalf("AuthCancelFlow: %v", err)
	}

	want := []string{
		"POST /api/providers/oauth/anthropic/submit",
		"GET /api/providers/oauth/anthropic/poll/s1",
		"DELETE /api/providers/oauth/sessions/s1",
	}

	for index, request := range want {
		if seen[index] != request {
			t.Fatalf("request %d = %q, want %q", index, seen[index], request)
		}
	}
}

func TestAuthRequestFailurePaths(t *testing.T) {
	t.Parallel()

	unavailable := &hermesServer{}
	if _, err := unavailable.AuthProviders(context.Background()); err == nil {
		t.Fatal("a server with no process answered an auth route")
	}

	malformed := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	})

	if _, err := malformed.AuthProviders(context.Background()); err == nil {
		t.Fatal("a malformed body decoded")
	}

	if _, err := malformed.AuthAPIKeyProviders(context.Background()); err == nil {
		t.Fatal("a malformed env body decoded")
	}

	if _, err := malformed.AuthPollFlow(context.Background(), "p", "s"); err == nil {
		t.Fatal("a malformed poll body decoded")
	}

	refusing := newAuthTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	err := refusing.AuthSubmit(context.Background(), "p", "s", "code")
	if AuthRefused(err) {
		t.Fatalf("a 503 was classified as a refusal: %v", err)
	}

	if err := refusing.AuthCancelFlow(context.Background(), "s"); err == nil {
		t.Fatal("a failing cancel was reported clean")
	}

	if _, err := refusing.AuthStart(context.Background(), "p"); err == nil {
		t.Fatal("a failing start was reported clean")
	}

	unreachable := &hermesServer{process: &Process{APIBaseURL: "http://127.0.0.1:1/api", Token: "t"}}
	if _, err := unreachable.AuthProviders(context.Background()); err == nil {
		t.Fatal("an unreachable broker was reported clean")
	}

	if _, err := unreachable.AuthStart(context.Background(), "bad host"); err == nil {
		t.Fatal("an unbuildable request was reported clean")
	}
}

func TestAuthRequestRejectsAnUnencodableBody(t *testing.T) {
	server := newAuthTestServer(t, func(http.ResponseWriter, *http.Request) {})

	if err := server.authRequest(context.Background(), http.MethodPost, "/x", func() {}, nil); err == nil {
		t.Fatal("an unencodable body was accepted")
	}

	original := authHTTPClient
	authHTTPClient = func() *http.Client {
		return &http.Client{Transport: failingTransport{}}
	}

	t.Cleanup(func() { authHTTPClient = original })

	if err := server.authRequest(context.Background(), http.MethodGet, "/x", nil, nil); err == nil {
		t.Fatal("a transport failure was reported clean")
	}
}

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("transport")
}

func TestAuthStatusErrorAndRefusalClassification(t *testing.T) {
	t.Parallel()

	status := &AuthStatusError{StatusCode: 403}
	if status.Error() != "hermes auth request failed with status 403" {
		t.Fatalf("Error() = %q", status.Error())
	}

	if !AuthRefused(status) {
		t.Fatal("403 is not a refusal")
	}

	if AuthRefused(errors.New("plain")) {
		t.Fatal("a plain error was classified as a refusal")
	}
}

func TestSecondsToDurationIgnoresAbsentValues(t *testing.T) {
	t.Parallel()

	if secondsToDuration(0) != 0 || secondsToDuration(-1) != 0 {
		t.Fatal("an absent interval became a duration")
	}

	if secondsToDuration(1.5) != 1500*time.Millisecond {
		t.Fatalf("secondsToDuration(1.5) = %v", secondsToDuration(1.5))
	}
}

func TestAuthSlotLabelIsAdapterOwned(t *testing.T) {
	t.Parallel()

	label := AuthSlotLabel("connection-1")
	if !AuthSlotLabelPrefix(label) {
		t.Fatalf("label %q is not recognised as adapter-owned", label)
	}

	if AuthSlotLabelPrefix("") || AuthSlotLabelPrefix("operator-entry") {
		t.Fatal("an unlabelled entry was claimed by the adapter")
	}
}

func TestNeutralizedBrowserCommandIsNamed(t *testing.T) {
	t.Parallel()

	if neutralizedBrowserCommand() == "" {
		t.Fatal("no browser neutraliser is named")
	}
}

func TestAuthRequestRejectsAnInvalidMethod(t *testing.T) {
	t.Parallel()

	server := newAuthTestServer(t, func(http.ResponseWriter, *http.Request) {})

	if err := server.authRequest(context.Background(), "BAD METHOD", "/x", nil, nil); err == nil {
		t.Fatal("an unbuildable request was accepted")
	}
}

func TestAuthRequestReportsABodyReadFailure(t *testing.T) {
	server := newAuthTestServer(t, func(http.ResponseWriter, *http.Request) {})

	original := authHTTPClient
	authHTTPClient = func() *http.Client {
		return &http.Client{Transport: brokenBodyTransport{}}
	}

	t.Cleanup(func() { authHTTPClient = original })

	if _, err := server.AuthProviders(context.Background()); err == nil {
		t.Fatal("a body read failure was reported clean")
	}
}

type brokenBodyTransport struct{}

func (brokenBodyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: brokenBody{}, Header: http.Header{}}, nil
}

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, errors.New("body") }

func (brokenBody) Close() error { return nil }
