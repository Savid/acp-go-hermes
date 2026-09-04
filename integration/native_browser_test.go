//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	hermesacp "github.com/savid/acp-go-hermes"
)

const (
	nativeBrowserFixtureDir  = "native-browser"
	nativeBrowserProbePath   = "/usr/local/bin/native-browser.test"
	nativeBrowserAdapterPath = "/usr/local/bin/acp-go-hermes.test"
	nativeBrowserTracePath   = "/tmp/native-browser.trace"
	nativeBrowserHermesPath  = "/usr/local/bin/hermes"
	nativeBrowserStatePath   = "/native-browser-state"
	nativeBrowserHostname    = "native-browser-canary"
	nativeBrowserInsideEnv   = "ACP_GO_HERMES_NATIVE_BROWSER_INSIDE"
	nativeBrowserTestName    = "TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher"
)

var nativeBrowserLauncherNames = []string{
	"open",
	"xdg-open",
	"x-www-browser",
	"www-browser",
	"sensible-browser",
	"gio",
	"firefox",
	"google-chrome",
	"google-chrome-stable",
	"chromium",
	"chromium-browser",
}

// TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher drives the pinned
// Hermes v0.20.0 source through the production adapter auth surface. Hermes'
// dashboard auth API returns the authorization URL to its caller; the
// dashboard, not the server, owns opening it. The syscall trace is therefore
// an executable no-attempt proof, not a claim that the shim was exercised.
func TestNativeBrowserLinuxProviderAuthExecsNoBrowserLauncher(t *testing.T) {
	requireRunIntegration(t)

	if os.Getenv(nativeBrowserInsideEnv) == "1" {
		runNativeHermesProviderAuthCanary(t)

		return
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("%s=1 requires a container runtime: %v", envRunIntegration, err)
	}

	if runtime.GOOS != "linux" {
		t.Skip("the required CI canary runs the Linux integration binary natively")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	stateVolume := fmt.Sprintf("acp-go-hermes-browser-%d-%d", os.Getpid(), time.Now().UnixNano())
	fixture, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    nativeBrowserFixtureDir,
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			Cmd: []string{"sleep", "infinity"},
			ConfigModifier: func(config *container.Config) {
				config.Hostname = nativeBrowserHostname
			},
			HostConfigModifier: func(config *container.HostConfig) {
				config.ExtraHosts = []string{nativeBrowserHostname + ":127.0.0.1"}
				config.NetworkMode = container.NetworkMode("none")
			},
			Mounts: testcontainers.ContainerMounts{
				testcontainers.VolumeMount(stateVolume, nativeBrowserStatePath),
			},
			WaitingFor: wait.ForExec([]string{"/bin/true"}).WithStartupTimeout(5 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start network-disabled native browser fixture: %v", err)
	}
	t.Cleanup(func() {
		if terminateErr := fixture.Terminate(
			context.WithoutCancel(ctx), testcontainers.RemoveVolumes(stateVolume),
		); terminateErr != nil {
			t.Errorf("terminate native browser fixture: %v", terminateErr)
		}
	})
	inspection, err := fixture.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect native browser fixture: %v", err)
	}
	if inspection.Config == nil {
		t.Fatal("native browser fixture lacks container configuration")
	}
	if inspection.Config.Hostname != nativeBrowserHostname {
		t.Fatalf("native browser fixture hostname = %q, want %q", inspection.Config.Hostname, nativeBrowserHostname)
	}
	if inspection.HostConfig == nil {
		t.Fatal("native browser fixture lacks host configuration")
	}
	var stateMount *container.MountPoint
	for index := range inspection.Mounts {
		if inspection.Mounts[index].Destination == nativeBrowserStatePath {
			stateMount = &inspection.Mounts[index]

			break
		}
	}
	if stateMount == nil || stateMount.Type != "volume" || stateMount.Driver != "local" ||
		stateMount.Name != stateVolume || !stateMount.RW {
		t.Fatalf("native browser state mount is not a writable local volume: %#v", stateMount)
	}
	if inspection.HostConfig.NetworkMode != container.NetworkMode("none") {
		t.Fatalf("native browser fixture network mode = %q, want none", inspection.HostConfig.NetworkMode)
	}
	wantExtraHost := nativeBrowserHostname + ":127.0.0.1"
	if len(inspection.HostConfig.ExtraHosts) != 1 || inspection.HostConfig.ExtraHosts[0] != wantExtraHost {
		t.Fatalf("native browser fixture extra hosts = %q, want [%q]", inspection.HostConfig.ExtraHosts, wantExtraHost)
	}
	if copyErr := fixture.CopyFileToContainer(ctx, buildNativeBrowserProbe(t), nativeBrowserProbePath, 0o755); copyErr != nil {
		t.Fatalf("copy native browser probe: %v", copyErr)
	}
	if copyErr := fixture.CopyFileToContainer(ctx, buildNativeBrowserAdapter(t), nativeBrowserAdapterPath, 0o755); copyErr != nil {
		t.Fatalf("copy adapter binary: %v", copyErr)
	}

	code, output, err := fixture.Exec(ctx, []string{
		"/usr/bin/env",
		nativeBrowserInsideEnv + "=1",
		envRunIntegration + "=1",
		envAgentBinary + "=" + nativeBrowserAdapterPath,
		envHermesPath + "=" + nativeBrowserHermesPath,
		"/usr/bin/timeout",
		"--kill-after=10s",
		"180s",
		"/usr/bin/strace",
		"-f",
		"-qq",
		"-e", "trace=execve,execveat",
		"-o", nativeBrowserTracePath,
		nativeBrowserProbePath,
		"-test.v",
		"-test.run", "^" + nativeBrowserTestName + "$",
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("run native browser canary: %v", err)
	}

	logs, err := io.ReadAll(output)
	if err != nil {
		t.Fatalf("read native browser canary output: %v", err)
	}
	t.Log(string(logs))
	if code != 0 {
		t.Fatalf("native browser canary exited %d", code)
	}
	if got := strings.Count(string(logs), "--- PASS: "+nativeBrowserTestName); got != 1 {
		t.Fatalf("native browser canary pass count = %d, want exactly 1: %s", got, logs)
	}
	if strings.Contains(string(logs), "SKIP") || strings.Contains(string(logs), "no tests to run") {
		t.Fatalf("required native browser canary skipped or selected nothing: %s", logs)
	}

	trace := readNativeBrowserTrace(ctx, t, fixture)
	if !strings.Contains(trace, nativeBrowserAdapterPath) || !strings.Contains(trace, nativeBrowserHermesPath) {
		t.Fatalf("trace lacks positive adapter/native exec evidence:\n%s", trace)
	}
	for _, launcher := range nativeBrowserLauncherNames {
		if traceExecsBase(trace, launcher) {
			t.Fatalf("production Hermes auth attempted browser launcher %q:\n%s", launcher, trace)
		}
	}
}

func runNativeHermesProviderAuthCanary(t *testing.T) {
	t.Helper()

	t.Log("native browser phase: version")
	versionOutput, err := exec.CommandContext(t.Context(), nativeBrowserHermesPath, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("run pinned Hermes version: %v: %s", err, versionOutput)
	}
	versionLine := strings.SplitN(strings.TrimSpace(string(versionOutput)), "\n", 2)[0]
	if versionLine != "Hermes Agent v0.20.0 (2026.8.3)" {
		t.Fatalf("Hermes version = %q, want exact official v0.20.0 release", versionLine)
	}

	root := nativeBrowserStatePath
	sharedHome := filepath.Join(root, "hermes-home")
	ledgerRoot := filepath.Join(root, "ledger")
	cwd := filepath.Join(root, "workspace")
	for _, dir := range []string{sharedHome, ledgerRoot, cwd} {
		if mkdirErr := os.MkdirAll(dir, 0o700); mkdirErr != nil {
			t.Fatalf("create canary durable directory: %v", mkdirErr)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	t.Log("native browser phase: start adapter")
	agent := startLiveAgent(t, ctx, root,
		"-provider-auth-root", ledgerRoot,
		"-shared-hermes-home", sharedHome,
	)
	defer agent.close()

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)
	t.Log("native browser phase: initialize")
	initialized, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("initialize production adapter: %v\nstderr:\n%s", err, agent.stderrString())
	}
	hermesMeta, _ := initialized.AgentCapabilities.Meta["hermes"].(map[string]any)
	if _, ok := hermesMeta["providerAuth"].(map[string]any); !ok {
		t.Fatalf("provider auth capability absent: %#v\nstderr:\n%s", hermesMeta, agent.stderrString())
	}

	t.Log("native browser phase: new session")
	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("new production Hermes session: %v\nstderr:\n%s", err, agent.stderrString())
	}

	var methods authMethodsWire
	t.Log("native browser phase: methods")
	if callErr := callAuthLeg(t, ctx, conn, hermesacp.AuthMethodsMethod, map[string]any{
		"sessionId": string(session.SessionId),
	}, &methods); callErr != nil {
		t.Fatalf("enumerate native auth methods: %v\nstderr:\n%s", callErr, agent.stderrString())
	}

	method := ""
	for _, candidate := range methods.Providers["anthropic"] {
		if candidate.Type == "oauth" {
			method = candidate.ID

			break
		}
	}
	if method == "" {
		t.Fatalf("Hermes 0.20.0 exposed no Anthropic OAuth method: %#v", methods.Providers)
	}

	var authorization authAuthorizeWire
	t.Log("native browser phase: authorize")
	err = callAuthLeg(t, ctx, conn, hermesacp.AuthAuthorizeMethod, map[string]any{
		"sessionId":          string(session.SessionId),
		"providerId":         "anthropic",
		"connectionId":       "native-browser-canary",
		"methodsGeneration":  methods.Generation,
		"method":             method,
		"authorizeRequestId": "native-browser-canary",
	}, &authorization)
	if err != nil {
		t.Fatalf("start native Anthropic OAuth: %v\nstderr:\n%s", err, agent.stderrString())
	}
	if authorization.Interaction != "callback" || authorization.URL == "" || authorization.FlowID == "" {
		t.Fatalf("native authorization presentation = %#v", authorization)
	}

	t.Log("native browser phase: cancel")
	if callErr := callAuthLeg(t, ctx, conn, hermesacp.AuthCancelMethod, map[string]any{
		"sessionId":  string(session.SessionId),
		"providerId": "anthropic",
		"flowId":     authorization.FlowID,
	}, nil); callErr != nil {
		t.Fatalf("cancel native authorization: %v", callErr)
	}
}

func buildNativeBrowserProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "native-browser.test")
	command := exec.CommandContext(t.Context(), "go", "test", "-c", "-tags=integration", "-o", out, "./integration")
	command.Dir = repoRoot()
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build native browser probe: %v: %s", err, output)
	}

	return out
}

func buildNativeBrowserAdapter(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "acp-go-hermes")
	command := exec.CommandContext(t.Context(), "go", "build", "-o", out, "./cmd/acp-go-hermes")
	command.Dir = repoRoot()
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build adapter binary: %v: %s", err, output)
	}

	return out
}

func readNativeBrowserTrace(ctx context.Context, t *testing.T, fixture testcontainers.Container) string {
	t.Helper()

	code, output, err := fixture.Exec(ctx, []string{"/bin/sh", "-c", "exec /bin/cat " + nativeBrowserTracePath}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("read native browser trace: %v", err)
	}
	contents, err := io.ReadAll(output)
	if err != nil {
		t.Fatalf("read native browser trace output: %v", err)
	}
	if code != 0 {
		t.Fatalf("read native browser trace exited %d: %s", code, contents)
	}

	return string(contents)
}

func traceExecsBase(trace string, base string) bool {
	absolute := fmt.Sprintf("/%s\"", base)
	relative := fmt.Sprintf("\"%s\"", base)

	return strings.Contains(trace, absolute) || strings.Contains(trace, relative)
}
