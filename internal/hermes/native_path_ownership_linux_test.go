//go:build linux

package hermes

import (
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeneratedNativeTreeDistinctIdentityTraversal(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	parent, err := os.MkdirTemp("/var/lib", "acp-go-hermes-ownership-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	if err = os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}

	control := filepath.Join(parent, "control")
	native := filepath.Join(parent, "native")
	if err = os.Mkdir(control, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(control, "secret"), []byte("root"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(native, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(native, "input"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}

	isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
	if err = handoffGeneratedNativeTree(native, isolation); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"/bin/sh",
		"-c",
		`set -eu
test "$(cat "$1/input")" = ok
printf native >"$1/output"
if cat "$2/secret" >/dev/null 2>&1; then exit 42; fi`,
		"sh",
		native,
		control,
	)
	command.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: isolation.UID, Gid: isolation.GID, Groups: []uint32{}},
	}
	if output, commandErr := command.CombinedOutput(); commandErr != nil {
		t.Fatalf("dropped-identity proof: %v: %s", commandErr, output)
	}

	contents, err := os.ReadFile(filepath.Join(native, "output"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "native" {
		t.Fatalf("native output = %q", contents)
	}
	if contents, err = os.ReadFile(filepath.Join(control, "secret")); err != nil || string(contents) != "root" {
		t.Fatalf("trusted control changed: %q, %v", contents, err)
	}
}

func TestGeneratedNativeTreeRejectsUntraversableCallerRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	parent, err := os.MkdirTemp("/var/lib", "acp-go-hermes-caller-root-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	native := filepath.Join(parent, "native")
	if err = os.Mkdir(native, 0o700); err != nil {
		t.Fatal(err)
	}

	isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
	if err = handoffGeneratedNativeTree(native, isolation); err == nil {
		t.Fatal("0700 caller root accepted")
	}
	if err = os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}
	if err = handoffGeneratedNativeTree(native, isolation); err != nil {
		t.Fatalf("0711 protected caller root: %v", err)
	}
}

func TestGeneratedNativeTreeRejectsUnsafeEntries(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	tests := []struct {
		name string
		seed func(string) error
	}{
		{name: "symlink", seed: func(root string) error {
			return os.Symlink("/etc/passwd", filepath.Join(root, "entry"))
		}},
		{name: "hardlink", seed: func(root string) error {
			first := filepath.Join(root, "first")
			if err := os.WriteFile(first, []byte("x"), 0o600); err != nil {
				return err
			}

			return os.Link(first, filepath.Join(root, "second"))
		}},
		{name: "broad mode", seed: func(root string) error {
			return os.WriteFile(filepath.Join(root, "entry"), []byte("x"), 0o644)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent, err := os.MkdirTemp("/var/lib", "acp-go-hermes-unsafe-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(parent) })
			if err = os.Chmod(parent, 0o711); err != nil {
				t.Fatal(err)
			}

			native := filepath.Join(parent, "native")
			if err = os.Mkdir(native, 0o700); err != nil {
				t.Fatal(err)
			}
			if err = test.seed(native); err != nil {
				t.Fatal(err)
			}

			isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
			if err = handoffGeneratedNativeTree(native, isolation); err == nil || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe tree result = %v", err)
			}
		})
	}
}

// TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer proves the
// effective-id helpers refuse rather than narrow. Every caller compares their
// result against an inode's 32-bit owner, so an answer outside that width must
// not be truncated into an id a real inode could carry — the truncation of an
// answer one past the 32-bit range is 0, which is root. Linux stores its ids in
// 32 bits and cannot produce such an answer, so it is staged through the seams
// the helpers read.
func TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer(t *testing.T) {
	realUID, realGID := effectiveUIDSource, effectiveGIDSource
	t.Cleanup(func() { effectiveUIDSource, effectiveGIDSource = realUID, realGID })

	effectiveUIDSource = func() int { return -1 }
	effectiveGIDSource = func() int { return -1 }
	require.Equal(t, uint32(math.MaxUint32), effectiveUID())
	require.Equal(t, uint32(math.MaxUint32), effectiveGID())

	effectiveUIDSource = func() int { return math.MaxUint32 + 1 }
	effectiveGIDSource = func() int { return math.MaxUint32 + 1 }

	uid, gid := effectiveUID(), effectiveGID()
	require.Equal(t, uint32(math.MaxUint32), uid)
	require.Equal(t, uint32(math.MaxUint32), gid)
	require.NotZero(t, uid, "narrowing this answer would have claimed root")
	require.NotZero(t, gid, "narrowing this answer would have claimed the root group")

	effectiveUIDSource = func() int { return 65534 }
	effectiveGIDSource = func() int { return 65533 }
	require.Equal(t, uint32(65534), effectiveUID())
	require.Equal(t, uint32(65533), effectiveGID())
}
