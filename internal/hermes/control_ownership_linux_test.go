//go:build linux

package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func TestTrustedSupervisorHermesNativeCannotMutateTrustedLease(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	parent, err := os.MkdirTemp("/tmp", "acp-go-hermes-ownership-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	if err := os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}
	xdg, err := CreateGenerationXDGDirs(parent)
	if err != nil {
		t.Fatal(err)
	}
	control := ControlDirForXDG(xdg.Root)
	if err := WriteLease(control, ServerLease{PID: os.Getpid()}); err != nil {
		t.Fatal(err)
	}
	if err := handoffGeneratedNativeTree(xdg.Root, &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}); err != nil {
		t.Fatal(err)
	}

	lease := filepath.Join(control, LeaseFileName)
	cmd := exec.Command("/bin/sh", "-c", `: > "$1/native-ok" && ! cat "$2" >/dev/null 2>&1 && ! rm -f "$2"`, "sh", xdg.State, lease)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534, Groups: []uint32{}}}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dropped identity proof: %v: %s", err, output)
	}
	if _, err := os.Stat(lease); err != nil {
		t.Fatalf("trusted lease changed: %v", err)
	}
}
