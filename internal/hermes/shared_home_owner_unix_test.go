//go:build unix

//nolint:govet // Subprocess ownership probes intentionally use repeated scoped errors.
package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const sharedHomeCrashEnv = "ACP_GO_HERMES_TEST_HOME_ROOT_CRASH"

// TestSharedHomeRootSurvivesAdapterDeathWhileTheNativeWriterLives proves the two
// crash boundaries the home-root claim exists for. An adapter that dies leaves
// the root fenced for as long as the native writer it launched still holds the
// inherited descriptor, and only once that exact process is gone does the next
// acquisition prove the recorded claimant dead and take the root.
func TestSharedHomeRootSurvivesAdapterDeathWhileTheNativeWriterLives(t *testing.T) {
	if os.Getenv(sharedHomeCrashEnv) == "1" {
		runSharedHomeCrashHelper()

		return
	}

	home := t.TempDir()
	executable := fakeHermesExecutable(t, fakeProcessModeOK)
	identityPath := fmt.Sprintf("%s/native-identity.json", t.TempDir())
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	helper := exec.Command(testBinary, "-test.run=^TestSharedHomeRootSurvivesAdapterDeathWhileTheNativeWriterLives$")
	helper.Env = append(os.Environ(),
		sharedHomeCrashEnv+"=1",
		"ACP_GO_HERMES_TEST_OWNER_HOME="+home,
		"ACP_GO_HERMES_TEST_OWNER_EXECUTABLE="+executable,
		"ACP_GO_HERMES_TEST_OWNER_IDENTITY="+identityPath,
	)
	if output, err := helper.CombinedOutput(); err != nil {
		t.Fatalf("home-root crash helper: %v\n%s", err, output)
	}

	data, err := os.ReadFile(identityPath)
	if err != nil {
		t.Fatalf("read orphan identity: %v", err)
	}
	var identity sharedOwnerCrashIdentity
	if err := json.Unmarshal(data, &identity); err != nil {
		t.Fatalf("decode orphan identity: %v", err)
	}
	if identity.PID <= 0 || identity.StartTime == "" {
		t.Fatalf("incomplete orphan identity: %+v", identity)
	}
	t.Cleanup(func() { killSharedOwnerCrashProcessIfLive(identity) })

	assertSharedHomeRootFencedByOrphan(t, home, identity)

	killSharedOwnerCrashProcess(t, identity)
	waitForSharedHomeRootReclaim(t, home)
}

func assertSharedHomeRootFencedByOrphan(t *testing.T, home string, identity sharedOwnerCrashIdentity) {
	t.Helper()

	owner, acquireErr := AcquireSharedHomeOwner(home)
	if acquireErr != nil {
		if !strings.Contains(acquireErr.Error(), "already claimed") {
			t.Fatalf("home root after adapter crash error = %v", acquireErr)
		}

		return
	}

	current, inspectErr := inspectHermesProcessStartTime(identity.PID)
	if inspectErr == nil && current == identity.StartTime {
		t.Fatalf("home root was admitted while orphan %d remained live", identity.PID)
	}
	if inspectErr != nil && !sharedOwnerInspectionProvesGone(inspectErr) {
		t.Fatalf("home root was admitted with uncertain orphan state: %v", inspectErr)
	}
	if err := owner.Release(); err != nil {
		t.Fatalf("release home root after proven orphan death: %v", err)
	}
}

func waitForSharedHomeRootReclaim(t *testing.T, home string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		reacquired, acquireErr := AcquireSharedHomeOwner(home)
		if acquireErr == nil {
			if err := reacquired.Release(); err != nil {
				t.Fatalf("release recovered home root: %v", err)
			}

			break
		}
		if !strings.Contains(acquireErr.Error(), "already claimed") || time.Now().After(deadline) {
			t.Fatalf("reclaim home root after exact orphan death: %v", acquireErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runSharedHomeCrashHelper() {
	home := os.Getenv("ACP_GO_HERMES_TEST_OWNER_HOME")
	executable := os.Getenv("ACP_GO_HERMES_TEST_OWNER_EXECUTABLE")
	identityPath := os.Getenv("ACP_GO_HERMES_TEST_OWNER_IDENTITY")
	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	markExecutableProbed(executable)
	afterHermesSpawnBeforeOwnerBind = func(cmd *exec.Cmd) {
		startTime, inspectErr := inspectHermesProcessStartTime(cmd.Process.Pid)
		if inspectErr != nil {
			_, _ = fmt.Fprintln(os.Stderr, inspectErr)
			os.Exit(3)
		}
		data, marshalErr := json.Marshal(sharedOwnerCrashIdentity{PID: cmd.Process.Pid, StartTime: startTime})
		if marshalErr != nil {
			_, _ = fmt.Fprintln(os.Stderr, marshalErr)
			os.Exit(4)
		}
		if writeErr := os.WriteFile(identityPath, data, 0o600); writeErr != nil {
			_, _ = fmt.Fprintln(os.Stderr, writeErr)
			os.Exit(5)
		}

		// Deliberately terminate the adapter while its native writer is live.
		// The home root must stay claimed by the inherited descriptor alone.
		os.Exit(0)
	}

	_, err = Start(context.Background(), ProcessOptions{
		ExecutablePath:     executable,
		Home:               home,
		Cwd:                home,
		ScratchParent:      home,
		AmbientEnvironment: map[string]string{"PATH": os.Getenv("PATH")},
		SharedHomeOwner:    owner,
		Timeout:            10 * time.Second,
	})
	_, _ = fmt.Fprintln(os.Stderr, errors.Join(errors.New("Start returned before the home-root crash hook"), err))
	os.Exit(6)
}

// TestSharedHomeRootRefusesAConcurrentAdapterProcess proves the exclusion is
// cross-process: while one adapter holds the root, a second is refused outright
// rather than admitted alongside it.
func TestSharedHomeRootRefusesAConcurrentAdapterProcess(t *testing.T) {
	home := t.TempDir()
	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatal(err)
	}

	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	peer := func() ([]byte, error) {
		command := exec.Command(testBinary, "-test.run=^TestSharedHomeRootPeerAcquisition$")
		command.Env = append(os.Environ(), "ACP_GO_HERMES_TEST_HOME_ROOT_PEER="+home)

		return command.CombinedOutput()
	}

	output, err := peer()
	if err == nil {
		t.Fatalf("second adapter process took a claimed home root:\n%s", output)
	}
	if !strings.Contains(string(output), "home root is already claimed by a live writer") {
		t.Fatalf("second adapter refusal = %s", output)
	}

	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}

	if output, err = peer(); err != nil {
		t.Fatalf("second adapter after release: %v\n%s", err, output)
	}
}

// TestSharedHomeRootPeerAcquisition is the peer half of the cross-process
// exclusion fixture. It is inert unless that fixture selects it by environment.
func TestSharedHomeRootPeerAcquisition(t *testing.T) {
	home := os.Getenv("ACP_GO_HERMES_TEST_HOME_ROOT_PEER")
	if home == "" {
		return
	}

	owner, err := AcquireSharedHomeOwner(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}
