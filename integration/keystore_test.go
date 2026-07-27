//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	envRunKeystore = "ACP_GO_HERMES_RUN_KEYSTORE"

	keystoreCanaryToken = "canary-not-a-real-credential"

	keystoreEnvFile   = "/run/acp-go-hermes-keystore/env"
	keystoreRoundTrip = "/usr/local/bin/roundtrip.sh"
	keystoreProbePath = "/usr/local/bin/residence.test"
)

func requireRunKeystore(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)

	if os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 to run the keystore credential-residence tier", envRunKeystore)
	}
}

func requireKeystoreRuntime(t *testing.T) {
	t.Helper()
	requireRunKeystore(t)

	// The tier fails rather than skips once its gate is set: a silently green
	// residence suite is worse than a red one.
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("%s=1 requires a container runtime: %v", envRunKeystore, err)
	}
}

// TestKeystoreLinuxCredentialResidence runs the two Linux thirds of the matrix
// against a live Secret Service. Hermes ships no freedesktop client, so the
// claim under test is an identity: the store under HERMES_HOME answers the same
// way whether or not a secret service is on the box. Only running the read path
// beside a real service establishes it, and a container's session bus does not
// cross the host boundary, so the read path runs inside the fixture.
func TestKeystoreLinuxCredentialResidence(t *testing.T) {
	requireKeystoreRuntime(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    filepath.Join(".", "keystore"),
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			// Readiness is a store/lookup round trip executed in the container.
			// A log line and a bus-name check both report ready against a
			// service that answers no lookup.
			WaitingFor: wait.ForExec([]string{keystoreRoundTrip}).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start keystore fixture: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("terminate keystore fixture: %v", err)
		}
	})

	probe := buildResidenceProbe(t)

	if err := container.CopyFileToContainer(ctx, probe, keystoreProbePath, 0o755); err != nil {
		t.Fatalf("copy residence probe: %v", err)
	}

	matrix := keystoreProbePath + " -test.v -test.run '^TestKeystoreResidenceMatrix$'"

	for name, command := range map[string]string{
		"keystore-present": ". " + keystoreEnvFile + "; export DBUS_SESSION_BUS_ADDRESS; exec " + matrix,
		"keystore-absent":  "unset DBUS_SESSION_BUS_ADDRESS; exec " + matrix,
	} {
		t.Run(name, func(t *testing.T) {
			code, output, err := container.Exec(ctx, []string{"/bin/sh", "-c", command})
			if err != nil {
				t.Fatalf("run residence matrix: %v", err)
			}

			logs, readErr := io.ReadAll(output)
			if readErr != nil {
				t.Fatalf("read residence output: %v", readErr)
			}

			t.Log(string(logs))

			if code != 0 {
				t.Fatalf("residence matrix exited %d", code)
			}

			if strings.Contains(string(logs), "SKIP") {
				t.Fatalf("the residence matrix skipped inside the fixture: %s", logs)
			}
		})
	}
}

// TestKeystoreLinuxArtifactCarriesNoSecretServiceClient pins the mechanism
// behind that identity from this repo's own side: the adapter compiled for Linux
// links no Secret Service client, so a live service has no code path to reach
// whatever it offers.
func TestKeystoreLinuxArtifactCarriesNoSecretServiceClient(t *testing.T) {
	requireRunKeystore(t)

	binary := filepath.Join(t.TempDir(), "acp-go-hermes-linux")

	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/acp-go-hermes")
	build.Dir = repoRoot()
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the Linux artifact: %v: %s", err, output)
	}

	contents, err := os.ReadFile(binary)
	if err != nil {
		t.Fatalf("read the Linux artifact: %v", err)
	}

	for _, symbol := range []string{"libsecret", "org.freedesktop.secrets", "gnome-keyring"} {
		if strings.Contains(string(contents), symbol) {
			t.Fatalf("the Linux artifact carries %q", symbol)
		}
	}
}

// buildResidenceProbe compiles the package that owns the store read path for the
// fixture's platform. The matrix cannot run on the host: only the container has
// a Secret Service to answer it.
func buildResidenceProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "residence.test")

	command := exec.CommandContext(t.Context(), "go", "test", "-c", "-tags=integration", "-o", out, "./internal/hermes")
	command.Dir = repoRoot()
	command.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build residence probe: %v: %s", err, output)
	}

	return out
}

// TestKeystoreProviderAuthResidence asserts where a brokered credential is
// resident on the host running the tier. The adapter's own store under
// HERMES_HOME answers the harvest, and on Darwin — the one platform where hermes
// carries a keychain reader for another harness's credential — nothing that
// reader surfaces ever reaches this surface.
func TestKeystoreProviderAuthResidence(t *testing.T) {
	requireRunKeystore(t)

	keystoreAssertOwnStore(t, t.TempDir())

	if runtime.GOOS == "darwin" {
		keystoreDarwinResidence(t)
	}
}

// keystoreHostAgent starts the wrapper the host thirds drive. Darwin's
// containment boundary is opt-in, and a session refuses to start without the
// operator's acceptance.
func keystoreHostAgent(t *testing.T, ctx context.Context, scratch string, extraArgs ...string) *liveAgent {
	t.Helper()

	args := append([]string(nil), extraArgs...)
	if runtime.GOOS == "darwin" {
		args = append(args, "-darwin-best-effort-containment")
	}

	return startLiveAgent(t, ctx, scratch, args...)
}

// keystoreAssertOwnStore drives one secret method to completion and asserts the
// credential is resident in the adapter's own reserved pool slot under
// HERMES_HOME, with canary material only.
func keystoreAssertOwnStore(t *testing.T, scratch string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	authRoot := t.TempDir()
	agent := keystoreHostAgent(t, ctx, scratch, "-provider-auth-root", authRoot)

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

	authRoot := t.TempDir()
	agent := keystoreHostAgent(t, ctx, t.TempDir(), "-provider-auth-root", authRoot)

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
