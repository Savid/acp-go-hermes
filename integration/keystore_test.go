//go:build integration

package integration

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	envRunKeystore = "ACP_GO_HERMES_RUN_KEYSTORE"

	keystoreBrowserShimPath = "/usr/local/bin/browser-shim.test"
	keystoreBrowserShimTest = "TestLoginNeverExecsABrowserLauncher"
	keystoreBrowserShimCase = "^" + keystoreBrowserShimTest + "$"
)

func requireRunKeystore(t *testing.T) {
	t.Helper()
	requireRunIntegration(t)

	if os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 to run the Linux state-boundary tier", envRunKeystore)
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

// TestKeystoreLinuxLoginNeverExecsABrowserLauncher runs the launch path on Linux
// against a real Linux PATH. Hermes is python: its webbrowser module reads
// BROWSER but still execs xdg-open by bare name, and hermes accepts
// --no-browser and then ignores it, so the launcher half of the shim is what
// keeps a login off the operator's desktop there. A Darwin box can compile that
// half but never execute it, and a claim that has only ever been compiled is
// the failure this shim exists to answer.
func TestKeystoreLinuxLoginNeverExecsABrowserLauncher(t *testing.T) {
	requireKeystoreRuntime(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container, startErr := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      keystoreBaseImage(t),
			Entrypoint: []string{"/bin/sh", "-c", "exec sleep infinity"},
			WaitingFor: wait.ForExec([]string{"/bin/true"}).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if startErr != nil {
		t.Fatalf("start the launcher fixture: %v", startErr)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("terminate the launcher fixture: %v", err)
		}
	})

	probe := buildLinuxProbe(t)

	if copyErr := container.CopyFileToContainer(ctx, probe, keystoreBrowserShimPath, 0o755); copyErr != nil {
		t.Fatalf("copy the launcher probe: %v", copyErr)
	}

	code, output, execErr := container.Exec(
		ctx,
		[]string{keystoreBrowserShimPath, "-test.run", keystoreBrowserShimCase, "-test.v"},
		tcexec.Multiplexed(),
	)
	if execErr != nil {
		t.Fatalf("run the launcher probe: %v", execErr)
	}

	logs, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read the launcher probe output: %v", readErr)
	}

	t.Log(string(logs))

	if code != 0 {
		t.Fatalf("the launcher probe exited %d", code)
	}

	// A probe binary whose selector matches nothing prints a bare PASS and exits
	// 0, so both the status and that word report success for a run in which the
	// launcher path never executed. Only the per-test line names the case.
	if strings.Contains(string(logs), "no tests to run") {
		t.Fatalf("the launcher probe selected no test: %s", logs)
	}

	if !strings.Contains(string(logs), "--- PASS: "+keystoreBrowserShimTest) {
		t.Fatalf("the launcher probe did not run %s: %s", keystoreBrowserShimTest, logs)
	}
}

// keystoreBaseImage returns the digest-pinned image the fixture is built on.
// Reading it out of the Dockerfile keeps this tier on one pin: a second copy of
// the digest drifts the moment either is bumped alone.
func keystoreBaseImage(t *testing.T) string {
	t.Helper()

	dockerfile, err := os.ReadFile(filepath.Join("keystore", "Dockerfile"))
	if err != nil {
		t.Fatalf("read the fixture Dockerfile: %v", err)
	}

	for line := range strings.SplitSeq(string(dockerfile), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && strings.EqualFold(fields[0], "FROM") {
			return fields[1]
		}
	}

	t.Fatal("the fixture Dockerfile names no base image")

	return ""
}

// buildLinuxProbe compiles the package that owns the launch path for the
// fixture's platform.
func buildLinuxProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "residence.test")

	command := exec.CommandContext(t.Context(), "go", "test", "-c", "-tags=integration", "-o", out, "./internal/hermes")
	command.Dir = repoRoot()
	// GOWORK=off is not optional: a go.work in scope otherwise builds the probe
	// from another module's requirements.
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build residence probe: %v: %s", err, output)
	}

	return out
}
