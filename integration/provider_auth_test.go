//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

const envRunAttended = "ACP_GO_HERMES_RUN_ATTENDED"

// requireRunAttended gates the attended tier and resolves the harness it will
// launch. Once the gate is set a missing CLI is a hard failure rather than the
// shared resolver's skip: the operator has committed a quarter hour of their
// attention to watching for a login URL, and a sub-second skip scrolling past in
// -v output is indistinguishable from a login that completed.
func requireRunAttended(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)

	if os.Getenv(envRunAttended) != "1" {
		t.Skipf("set %s=1 to run the attended provider-auth tier", envRunAttended)
	}

	if _, err := exec.LookPath(envOrDefault(envHermesPath, "hermes")); err != nil {
		t.Fatalf("%s=1 requires the hermes CLI: %v", envRunAttended, err)
	}
}

type authMethodsWire struct {
	Providers  map[string][]authMethodWire `json:"providers"`
	Generation string                      `json:"generation"`
}

type authMethodWire struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

type authAuthorizeWire struct {
	Interaction    string `json:"interaction"`
	URL            string `json:"url"`
	Message        string `json:"message"`
	UserCode       string `json:"userCode"`
	CallbackInput  string `json:"callbackInput"`
	FlowID         string `json:"flowId"`
	FlowExpiresAt  int64  `json:"flowExpiresAt"`
	PollIntervalMs int64  `json:"pollIntervalMs"`
}

type authStatusWire struct {
	FlowID    string `json:"flowId"`
	State     string `json:"state"`
	ExpiresAt int64  `json:"expiresAt"`
	Reason    string `json:"reason"`
}

type authInventoryWire struct {
	Entries []struct {
		ProviderID        string `json:"providerId"`
		ConnectionID      string `json:"connectionId"`
		Revision          int64  `json:"revision"`
		BindingGeneration int64  `json:"bindingGeneration"`
		ProofSource       string `json:"proofSource"`
	} `json:"entries"`
}

// providerAuthAgent starts the wrapper with durable ledger and native auth
// homes.
func providerAuthAgent(t *testing.T, ctx context.Context) (*acp.ClientSideConnection, *liveAgent, string) {
	t.Helper()

	authRoot := t.TempDir()
	authHome := t.TempDir()
	agent := startLiveAgent(
		t,
		ctx,
		t.TempDir(),
		"-provider-auth-root",
		authRoot,
		"-provider-auth-home",
		authHome,
	)

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)

	response, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}

	hermesMeta, _ := response.AgentCapabilities.Meta["hermes"].(map[string]any)

	capability, ok := hermesMeta["providerAuth"].(map[string]any)
	if !ok {
		t.Fatalf("provider auth capability absent with a configured root: %#v", hermesMeta)
	}

	names, _ := capability["methods"].([]any)
	if len(names) != 7 {
		t.Fatalf("advertised %d legs, want seven: %#v", len(names), names)
	}

	return conn, agent, authHome
}

func callAuthLeg(t *testing.T, ctx context.Context, conn *acp.ClientSideConnection, method string, params map[string]any, out any) error {
	t.Helper()

	raw, err := conn.CallExtension(ctx, method, params)
	if err != nil {
		return err
	}

	if out == nil {
		return nil
	}

	return json.Unmarshal(raw, out)
}

func newProviderAuthSession(t *testing.T, ctx context.Context, conn *acp.ClientSideConnection) acp.SessionId {
	t.Helper()

	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session: %v", err)
	}

	return session.SessionId
}

// TestAttendedProviderAuthLoginCompletes drives one real login end to end. The
// operator opens the relayed URL and approves at the provider; the flow's
// effective deadline bounds the wait, and an unanswered prompt fails rather
// than skips.
func TestAttendedProviderAuthLoginCompletes(t *testing.T) {
	requireRunAttended(t)

	ctx, cancel := context.WithTimeout(context.Background(), 16*time.Minute)
	defer cancel()

	conn, agent, _ := providerAuthAgent(t, ctx)
	defer agent.close()

	sessionID := newProviderAuthSession(t, ctx, conn)

	var methods authMethodsWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/methods", map[string]any{"sessionId": string(sessionID)}, &methods); err != nil {
		t.Fatalf("_hermes/auth/methods: %v", err)
	}

	providerID, method := attendedMethod(t, methods)
	t.Logf("driving the %s login method %s", providerID, method.ID)

	var authorization authAuthorizeWire

	err := callAuthLeg(t, ctx, conn, "_hermes/auth/authorize", map[string]any{
		"sessionId":          string(sessionID),
		"providerId":         providerID,
		"connectionId":       "attended-connection",
		"methodsGeneration":  methods.Generation,
		"method":             method.ID,
		"authorizeRequestId": "attended-request",
	}, &authorization)
	if err != nil {
		t.Fatalf("_hermes/auth/authorize: %v", err)
	}

	if authorization.URL == "" || authorization.FlowExpiresAt == 0 {
		t.Fatalf("authorize returned no presentation: %#v", authorization)
	}

	t.Logf("open %s and approve", authorization.URL)

	if authorization.UserCode != "" {
		t.Logf("enter the code %s", authorization.UserCode)
	}

	if authorization.Interaction == "callback" {
		code := attendedAnswer(t, "paste the authorization code and press enter: ")

		if err := callAuthLeg(t, ctx, conn, "_hermes/auth/callback", map[string]any{
			"sessionId": string(sessionID), "providerId": providerID,
			"method": method.ID, "flowId": authorization.FlowID, "input": code,
		}, nil); err != nil {
			t.Fatalf("_hermes/auth/callback: %v", err)
		}
	}

	deadline := time.UnixMilli(authorization.FlowExpiresAt)

	var status authStatusWire

	for time.Now().Before(deadline) {
		if err := callAuthLeg(t, ctx, conn, "_hermes/auth/status", map[string]any{
			"sessionId": string(sessionID), "providerId": providerID, "flowId": authorization.FlowID,
		}, &status); err != nil {
			t.Fatalf("_hermes/auth/status: %v", err)
		}

		if status.State != "pending" {
			break
		}

		time.Sleep(5 * time.Second)
	}

	if status.State != "authenticated" {
		t.Fatalf("flow reached %q/%q rather than authenticated before its deadline", status.State, status.Reason)
	}

	var inventory authInventoryWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/inventory", map[string]any{"sessionId": string(sessionID)}, &inventory); err != nil {
		t.Fatalf("_hermes/auth/inventory: %v", err)
	}

	var bindingGeneration int64
	for _, entry := range inventory.Entries {
		if entry.ProviderID == providerID && entry.ConnectionID == "attended-connection" {
			bindingGeneration = entry.BindingGeneration
			if entry.ProofSource != "confirmed_present" {
				t.Fatalf("inventory proof = %q, want confirmed_present", entry.ProofSource)
			}
		}
	}
	if bindingGeneration == 0 {
		t.Fatalf("inventory = %#v", inventory)
	}

	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/disconnect", map[string]any{
		"sessionId": string(sessionID), "providerId": providerID,
		"connectionId": "attended-connection", "bindingGeneration": bindingGeneration,
	}, nil); err != nil {
		t.Fatalf("_hermes/auth/disconnect: %v", err)
	}
}

func attendedMethod(t *testing.T, methods authMethodsWire) (string, authMethodWire) {
	t.Helper()

	for providerID, entries := range methods.Providers {
		for _, entry := range entries {
			if entry.Type == "oauth" {
				return providerID, entry
			}
		}
	}

	t.Fatalf("no oauth method to drive in %#v", methods.Providers)

	return "", authMethodWire{}
}

// attendedAnswer blocks on the operator. An unanswered prompt is a failure, not
// a skip: a silently green attended suite is worse than a red one.
func attendedAnswer(t *testing.T, prompt string) string {
	t.Helper()

	fmt.Fprint(os.Stderr, prompt)

	answer := make(chan string, 1)

	go func() {
		reader := bufio.NewReader(os.Stdin)

		line, err := reader.ReadString('\n')
		if err != nil {
			close(answer)

			return
		}

		answer <- strings.TrimSpace(line)
	}()

	select {
	case value, ok := <-answer:
		if !ok || value == "" {
			t.Fatal("the attended tier received no answer")
		}

		return value
	case <-time.After(10 * time.Minute):
		t.Fatal("the attended tier timed out waiting for a human answer")

		return ""
	}
}
