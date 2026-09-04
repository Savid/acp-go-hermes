//go:build !windows

package hermesacp

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestAuthCapabilityListsEveryLeg(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	response, err := agent.Initialize(context.Background(), acp.InitializeRequest{})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	hermesMeta, _ := response.AgentCapabilities.Meta[hermesMetaKey].(map[string]any)

	capability, ok := hermesMeta[providerAuthCapabilityKey].(map[string]any)
	if !ok {
		t.Fatalf("capability missing providerAuth: %#v", hermesMeta)
	}

	names, _ := capability[providerAuthMethodsField].([]string)
	if len(names) != 7 {
		t.Fatalf("advertised %d legs, want 7: %#v", len(names), names)
	}

	unset, err := newTestAgent().Initialize(context.Background(), acp.InitializeRequest{})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	unsetMeta, _ := unset.AgentCapabilities.Meta[hermesMetaKey].(map[string]any)
	if _, present := unsetMeta[providerAuthCapabilityKey]; present {
		t.Fatal("unset root still advertised providerAuth")
	}
}

func TestAuthLegsAnswerOnlyWhileAdvertised(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	for _, method := range authMethodNames() {
		if _, err := callLeg(t, agent, method, map[string]any{}); err == nil {
			t.Fatalf("%s accepted empty params", method)
		}
	}

	bare := newTestAgent()

	for _, method := range authMethodNames() {
		_, err := callLeg(t, bare, method, map[string]any{"sessionId": "x"})

		var requestErr *acp.RequestError
		if !errors.As(err, &requestErr) || requestErr.Code != -32601 {
			t.Fatalf("%s without a root returned %v, want method-not-found", method, err)
		}
	}

	if _, err := callLeg(t, agent, "_hermes/auth/unknown", map[string]any{}); err == nil {
		t.Fatal("unknown auth-shaped method accepted")
	}
}

func TestAuthSessionResolution(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	if _, err := agent.providerAuth.authSession("missing"); err == nil {
		t.Fatal("unknown session accepted")
	}

	session, err := agent.providerAuth.authSession(string(testSessionID))
	if err != nil {
		t.Fatalf("authSession: %v", err)
	}

	session.mu.Lock()
	session.client = nil
	session.mu.Unlock()
}

func TestAuthGoSafeContainsAPanic(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)
	done := make(chan struct{})

	agent.providerAuth.goSafe("panicking", func() {
		defer close(done)

		panic("boom")
	})

	<-done
}

func TestAuthorizeRejectsInvalidConnectionIDs(t *testing.T) {
	t.Parallel()

	agent, _ := newAuthAgent(t)

	for name, connectionID := range adversarialConnectionIDs() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := callLeg(t, agent, AuthAuthorizeMethod, map[string]any{
				"sessionId": string(testSessionID), "providerId": testProviderID,
				"connectionId": connectionID, "methodsGeneration": "generation",
				"method": "device_code", "authorizeRequestId": "request-1",
			})
			requireInvalidField(t, err, authFieldConnectionID)
		})
	}
}
