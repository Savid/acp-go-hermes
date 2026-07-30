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

	keystoreEnvFile         = "/run/acp-go-hermes-keystore/env"
	keystoreRoundTrip       = "/usr/local/bin/roundtrip.sh"
	keystoreProbePath       = "/usr/local/bin/residence.test"
	keystoreBrowserShimPath = "/usr/local/bin/browser-shim.test"
	keystoreBrowserShimTest = "TestLoginNeverExecsABrowserLauncher"
	keystoreBrowserShimCase = "^" + keystoreBrowserShimTest + "$"
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

	container := startKeystoreFixture(ctx, t)

	if err := container.CopyFileToContainer(ctx, buildResidenceProbe(t), keystoreProbePath, 0o755); err != nil {
		t.Fatalf("copy residence probe: %v", err)
	}

	runResidenceMatrix(ctx, t, container, false)
	runResidenceMatrix(ctx, t, container, true)
}

// startKeystoreFixture builds and starts the Secret Service fixture.
func startKeystoreFixture(ctx context.Context, t *testing.T) testcontainers.Container {
	t.Helper()

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

	return container
}

// runResidenceMatrix runs the residence matrix in one Linux configuration. The
// two differ only in whether the fixture's session bus is exported, so the
// probe binary and the container are the same for both.
func runResidenceMatrix(ctx context.Context, t *testing.T, container testcontainers.Container, bus bool) {
	t.Helper()

	name, prelude := "keystore-absent", ""
	if bus {
		name, prelude = "keystore-present", ". "+keystoreEnvFile+"; export DBUS_SESSION_BUS_ADDRESS; "
	}

	command := prelude + "export " + envRunIntegration + "=1 " + envRunKeystore + "=1; exec " +
		keystoreProbePath + " -test.v -test.run '^TestKeystoreResidenceMatrix$'"

	t.Run(name, func(t *testing.T) {
		code, output, err := container.Exec(ctx, []string{"/bin/sh", "-c", command}, tcexec.Multiplexed())
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

		// An exit status alone goes green on a skip, which is the silent success
		// this tier exists to prevent.
		if !strings.Contains(string(logs), "--- PASS: TestKeystoreResidenceMatrix") {
			t.Fatalf("the residence matrix did not report a pass inside the fixture: %s", logs)
		}
	})
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

	probe := buildResidenceProbe(t)

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

// buildResidenceProbe compiles the package that owns the store read path and
// the launch path for the fixture's platform. Neither Linux claim can be
// observed from the host: only the container has a Secret Service to answer one
// and a Linux PATH to resolve the other.
func buildResidenceProbe(t *testing.T) string {
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
