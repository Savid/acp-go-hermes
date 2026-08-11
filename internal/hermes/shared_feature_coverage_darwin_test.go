//go:build darwin

package hermes

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestSharedOwnerDarwinLaunchFaultCoverage(t *testing.T) {
	restoreDarwinLaunchSeams(t)
	cmd := exec.Command("/bin/echo", "owner")

	owner, createErr := os.CreateTemp(t.TempDir(), "owner")
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer owner.Close()
	if launch, err := prepareDarwinLaunch(cmd, t.TempDir(), []*os.File{owner, nil}); err == nil || launch != nil {
		t.Fatalf("nil owner launch = %#v, %v", launch, err)
	}

	closed, closedCreateErr := os.CreateTemp(t.TempDir(), "closed-owner")
	if closedCreateErr != nil {
		t.Fatal(closedCreateErr)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if launch, err := prepareDarwinLaunch(cmd, t.TempDir(), []*os.File{owner, closed}); err == nil || launch != nil {
		t.Fatalf("closed owner launch = %#v, %v", launch, err)
	}

	darwinLaunchCreateTemp = func(string, string) (*os.File, error) {
		return nil, errors.New("create fault")
	}
	if launch, err := prepareDarwinLaunch(cmd, t.TempDir(), []*os.File{owner}); err == nil || launch != nil {
		t.Fatalf("owner cleanup launch = %#v, %v", launch, err)
	}

	if tree, err := startContainedProcess(nil, ContainmentSpec{SharedSessionOwnerFiles: []*os.File{owner}}); err == nil || tree != nil {
		t.Fatalf("nil contained command = %#v, %v", tree, err)
	}
}
