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
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const sharedOwnerPrebindCrashEnv = "ACP_GO_HERMES_TEST_OWNER_PREBIND_CRASH"

type sharedOwnerCrashIdentity struct {
	PID       int    `json:"pid"`
	StartTime string `json:"startTime"`
}

func TestSharedSessionOwnerInheritedLockSurvivesPrebindAdapterCrash(t *testing.T) {
	if os.Getenv(sharedOwnerPrebindCrashEnv) == "1" {
		runSharedOwnerPrebindCrashHelper()

		return
	}

	home := t.TempDir()
	executable := fakeHermesExecutable(t, fakeProcessModeOK)
	identityPath := filepath.Join(t.TempDir(), "native-identity.json")
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	helper := exec.Command(testBinary, "-test.run=^TestSharedSessionOwnerInheritedLockSurvivesPrebindAdapterCrash$")
	helper.Env = append(os.Environ(),
		sharedOwnerPrebindCrashEnv+"=1",
		"ACP_GO_HERMES_TEST_OWNER_HOME="+home,
		"ACP_GO_HERMES_TEST_OWNER_EXECUTABLE="+executable,
		"ACP_GO_HERMES_TEST_OWNER_IDENTITY="+identityPath,
	)
	if output, err := helper.CombinedOutput(); err != nil {
		t.Fatalf("prebind crash helper: %v\n%s", err, output)
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
	t.Cleanup(func() {
		if current, inspectErr := inspectHermesProcessStartTime(identity.PID); inspectErr == nil && current == identity.StartTime {
			_ = syscall.Kill(-identity.PID, syscall.SIGKILL)
			_ = syscall.Kill(identity.PID, syscall.SIGKILL)
		}
	})

	if _, err := AcquireSharedNativeSessionOwner(home, "prebind-native"); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("same native ID after adapter crash error = %v", err)
	}
	other, err := AcquireSharedNativeSessionOwner(home, "different-native")
	if err != nil {
		t.Fatalf("different native ID after adapter crash: %v", err)
	}
	if err := other.Release(); err != nil {
		t.Fatalf("release different native ID: %v", err)
	}

	current, err := inspectHermesProcessStartTime(identity.PID)
	if err != nil || current != identity.StartTime {
		t.Fatalf("orphan changed before exact kill: current=%+v err=%v", current, err)
	}
	if err := syscall.Kill(-identity.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		if directErr := syscall.Kill(identity.PID, syscall.SIGKILL); directErr != nil && !errors.Is(directErr, syscall.ESRCH) {
			t.Fatalf("kill exact orphan: group=%v direct=%v", err, directErr)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		reacquired, acquireErr := AcquireSharedNativeSessionOwner(home, "prebind-native")
		if acquireErr == nil {
			if err := reacquired.Release(); err != nil {
				t.Fatalf("release recovered native owner: %v", err)
			}

			break
		}
		if !strings.Contains(acquireErr.Error(), "already active") || time.Now().After(deadline) {
			t.Fatalf("reacquire after exact orphan death: %v", acquireErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runSharedOwnerPrebindCrashHelper() {
	home := os.Getenv("ACP_GO_HERMES_TEST_OWNER_HOME")
	executable := os.Getenv("ACP_GO_HERMES_TEST_OWNER_EXECUTABLE")
	identityPath := os.Getenv("ACP_GO_HERMES_TEST_OWNER_IDENTITY")
	owner, err := AcquireSharedNativeSessionOwner(home, "prebind-native")
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

		// Deliberately terminate the adapter in the exact spawn-to-bind window.
		// The native child must keep the inherited flock alive without metadata.
		os.Exit(0)
	}

	_, err = Start(context.Background(), ProcessOptions{
		ExecutablePath:      executable,
		Home:                home,
		Cwd:                 home,
		ScratchParent:       home,
		AmbientEnvironment:  map[string]string{"PATH": os.Getenv("PATH")},
		SharedSessionOwners: []*SharedSessionOwner{owner},
		Timeout:             10 * time.Second,
	})
	_, _ = fmt.Fprintln(os.Stderr, errors.Join(errors.New("Start returned before prebind crash hook"), err))
	os.Exit(6)
}
