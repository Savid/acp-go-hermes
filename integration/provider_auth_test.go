//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

const (
	envRunAttended = "ACP_GO_HERMES_RUN_ATTENDED"
	envRunKeystore = "ACP_GO_HERMES_RUN_KEYSTORE"

	// keystoreUnlockPassword is newline-terminated on purpose. Fed a bare end of
	// input the daemon starts and claims the bus name while never creating its
	// collection, so a half-initialised service looks alive and serves nothing.
	keystoreUnlockPassword = "canary-unlock\n"

	keystoreCanaryToken = "canary-not-a-real-credential"
)

func requireRunAttended(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)

	if os.Getenv(envRunAttended) != "1" {
		t.Skipf("set %s=1 to run the attended provider-auth tier", envRunAttended)
	}
}

func requireRunKeystore(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)

	if os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 to run the keystore credential-residence tier", envRunKeystore)
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

type authCredentialWire struct {
	ConnectionID      string         `json:"connectionId"`
	Revision          int64          `json:"revision"`
	BindingGeneration int64          `json:"bindingGeneration"`
	Credential        map[string]any `json:"credential"`
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

// providerAuthAgent starts the wrapper with a durable provider-auth root and
// returns a live connection plus that root.
func providerAuthAgent(t *testing.T, ctx context.Context) (*acp.ClientSideConnection, *liveAgent, string) {
	t.Helper()

	authRoot := t.TempDir()
	agent := startLiveAgent(t, ctx, t.TempDir(), "-provider-auth-root", authRoot)

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
	if len(names) != 8 {
		t.Fatalf("advertised %d legs, want eight: %#v", len(names), names)
	}

	return conn, agent, authRoot
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

	var harvest authCredentialWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/credential", map[string]any{
		"sessionId": string(sessionID), "providerId": providerID, "flowId": authorization.FlowID,
	}, &harvest); err != nil {
		t.Fatalf("_hermes/auth/credential: %v", err)
	}

	if harvest.Credential["type"] != "hermesOauth" || harvest.Credential["accessToken"] == "" {
		t.Fatalf("harvest = %#v", harvest)
	}

	var inventory authInventoryWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/inventory", map[string]any{"sessionId": string(sessionID)}, &inventory); err != nil {
		t.Fatalf("_hermes/auth/inventory: %v", err)
	}

	if len(inventory.Entries) == 0 || inventory.Entries[0].ProofSource != "confirmed_present" {
		t.Fatalf("inventory = %#v", inventory)
	}

	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/disconnect", map[string]any{
		"sessionId": string(sessionID), "providerId": providerID,
		"connectionId": "attended-connection", "bindingGeneration": harvest.BindingGeneration,
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

// TestKeystoreProviderAuthResidence asserts where a brokered credential is
// resident, in all three configurations of the matrix. Hermes ships no
// freedesktop client, so the assertion on both Linux halves is that a live
// Secret Service changes nothing and the adapter's own store stays
// authoritative. On Darwin hermes does carry a keychain reader for another
// harness's credential, and the assertion is that nothing it surfaces ever
// reaches this surface.
func TestKeystoreProviderAuthResidence(t *testing.T) {
	requireRunKeystore(t)

	switch runtime.GOOS {
	case "linux":
		t.Run("keystore-absent", func(t *testing.T) { keystoreResidence(t, false) })
		t.Run("keystore-present", func(t *testing.T) { keystoreResidence(t, true) })
	case "darwin":
		keystoreDarwinResidence(t)
	default:
		t.Fatalf("the keystore tier has no configuration for %s", runtime.GOOS)
	}
}

// keystoreResidence seeds a canary into the adapter's own store and asserts
// which store answered. With present=true the whole exercise runs inside a
// container holding a live Secret Service, whose readiness is proved by a
// store/lookup round trip executed in the container rather than by a log line
// or a bus-name check: both of those report ready against a service that
// answers no lookup.
func keystoreResidence(t *testing.T, secretService bool) {
	t.Helper()

	if !secretService {
		keystoreAssertOwnStore(t, t.TempDir())

		return
	}

	runtimeBinary := containerRuntime(t)
	image := keystoreBuildImage(t, runtimeBinary)

	round := keystoreExec(t, runtimeBinary, image,
		"sh", "-c",
		"printf '"+keystoreUnlockPassword+"' | gnome-keyring-daemon --unlock >/dev/null 2>&1; "+
			"secret-tool store --label=canary service canary account canary <<'EOF'\n"+keystoreCanaryToken+"\nEOF\n"+
			"secret-tool lookup service canary account canary")
	if !strings.Contains(round, keystoreCanaryToken) {
		t.Fatalf("the container's secret service answered no lookup round trip: %q", round)
	}

	keystoreAssertOwnStore(t, t.TempDir())
}

// keystoreAssertOwnStore drives one secret method to completion and asserts the
// credential is resident in the adapter's own reserved pool slot under
// HERMES_HOME, with canary material only.
func keystoreAssertOwnStore(t *testing.T, scratch string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	authRoot := t.TempDir()
	agent := startLiveAgent(t, ctx, scratch, "-provider-auth-root", authRoot)

	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("initialize: %v\nstderr:\n%s", err, agent.stderrString())
	}

	sessionID := newProviderAuthSession(t, ctx, conn)

	var methods authMethodsWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/methods", map[string]any{"sessionId": string(sessionID)}, &methods); err != nil {
		t.Fatalf("_hermes/auth/methods: %v", err)
	}

	providerID, method := secretMethod(t, methods)

	var authorization authAuthorizeWire

	err := callAuthLeg(t, ctx, conn, "_hermes/auth/authorize", map[string]any{
		"sessionId":          string(sessionID),
		"providerId":         providerID,
		"connectionId":       "keystore-connection",
		"methodsGeneration":  methods.Generation,
		"method":             method.ID,
		"authorizeRequestId": "keystore-request",
	}, &authorization)
	if err != nil {
		t.Fatalf("_hermes/auth/authorize: %v", err)
	}

	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/callback", map[string]any{
		"sessionId": string(sessionID), "providerId": providerID,
		"method": method.ID, "flowId": authorization.FlowID, "input": keystoreCanaryToken,
	}, nil); err != nil {
		t.Fatalf("_hermes/auth/callback: %v", err)
	}

	var harvest authCredentialWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/credential", map[string]any{
		"sessionId": string(sessionID), "providerId": providerID, "flowId": authorization.FlowID,
	}, &harvest); err != nil {
		t.Fatalf("_hermes/auth/credential: %v", err)
	}

	if harvest.Credential["accessToken"] != keystoreCanaryToken {
		t.Fatalf("the adapter's own store did not answer the harvest: %#v", harvest.Credential)
	}

	if !keystoreCanaryOnDisk(t, scratch) {
		t.Fatal("the canary is not resident in the adapter's own HERMES_HOME store")
	}
}

// keystoreDarwinResidence asserts the Darwin third of the matrix: hermes reads
// another harness's credential out of the login keychain regardless of
// HERMES_HOME, and nothing it surfaces from there is ever forwarded on this
// surface.
func keystoreDarwinResidence(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	conn, agent, _ := providerAuthAgent(t, ctx)
	defer agent.close()

	sessionID := newProviderAuthSession(t, ctx, conn)

	var methods authMethodsWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/methods", map[string]any{"sessionId": string(sessionID)}, &methods); err != nil {
		t.Fatalf("_hermes/auth/methods: %v", err)
	}

	encoded, err := json.Marshal(methods)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}

	for _, leaked := range []string{"token_preview", "source_label", "disconnect_command", "disconnect_hint", "sk-ant-"} {
		if strings.Contains(string(encoded), leaked) {
			t.Fatalf("the catalog forwarded %q from the cross-harness keychain reader: %s", leaked, encoded)
		}
	}

	var inventory authInventoryWire
	if err := callAuthLeg(t, ctx, conn, "_hermes/auth/inventory", map[string]any{"sessionId": string(sessionID)}, &inventory); err != nil {
		t.Fatalf("_hermes/auth/inventory: %v", err)
	}

	if len(inventory.Entries) != 0 {
		t.Fatalf("a credential this adapter never installed is represented in the inventory: %#v", inventory.Entries)
	}
}

func secretMethod(t *testing.T, methods authMethodsWire) (string, authMethodWire) {
	t.Helper()

	for providerID, entries := range methods.Providers {
		for _, entry := range entries {
			if entry.Type == "api" {
				return providerID, entry
			}
		}
	}

	t.Fatalf("no operator-key method to drive in %#v", methods.Providers)

	return "", authMethodWire{}
}

func keystoreCanaryOnDisk(t *testing.T, scratch string) bool {
	t.Helper()

	found := false

	err := filepath.WalkDir(scratch, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || entry.Name() != "auth.json" {
			return nil //nolint:nilerr // an unreadable branch is not a residence answer.
		}

		contents, readErr := os.ReadFile(path) // #nosec G304 -- path comes from the test's own scratch tree.
		if readErr != nil {
			return nil
		}

		if strings.Contains(string(contents), keystoreCanaryToken) {
			found = true
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk scratch: %v", err)
	}

	return found
}

func containerRuntime(t *testing.T) string {
	t.Helper()

	for _, candidate := range []string{"docker", "podman"} {
		if path, err := exec.LookPath(candidate); err == nil {
			return path
		}
	}

	t.Fatal("the keystore tier needs a container runtime and found none")

	return ""
}

// keystoreBuildImage builds the in-tree fixture. The base image is pinned by
// digest in the Dockerfile beside this package.
func keystoreBuildImage(t *testing.T, runtimeBinary string) string {
	t.Helper()

	tag := "acp-go-hermes-keystore-fixture:test"
	build := exec.Command(runtimeBinary, "build", "-f", filepath.Join(repoRoot(), "integration", "keystore", "Dockerfile"),
		"-t", tag, filepath.Join(repoRoot(), "integration", "keystore")) // #nosec G204 -- fixed fixture path.

	output, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("build the keystore fixture image: %v\n%s", err, output)
	}

	return tag
}

func keystoreExec(t *testing.T, runtimeBinary string, image string, args ...string) string {
	t.Helper()

	run := append([]string{"run", "--rm", image}, args...)

	output, err := exec.Command(runtimeBinary, run...).CombinedOutput() // #nosec G204 -- fixed fixture image and command.
	if err != nil {
		t.Fatalf("run the keystore fixture: %v\n%s", err, output)
	}

	return string(output)
}
