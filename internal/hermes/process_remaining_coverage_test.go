//go:build !darwin && !windows

package hermes

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestProcessContainmentValidationAndPortFailure(t *testing.T) {
	if _, err := Start(context.Background(), ProcessOptions{DarwinBestEffortContainment: true}); err == nil {
		t.Fatal("Start accepted Darwin containment off Darwin")
	}

	restoreProcessSeams(t)
	executable := "already-probed-port-failure"
	markExecutableProbed(executable)
	want := errors.New("listen failed")
	listenTCP = func(string, string) (net.Listener, error) { return nil, want }
	_, err := Start(context.Background(), ProcessOptions{ExecutablePath: executable, Home: t.TempDir()})
	if !errors.Is(err, want) {
		t.Fatalf("Start port failure = %v", err)
	}
}

func TestExecutableVersionAdmissionFailures(t *testing.T) {
	unique := func(suffix string) string { return t.Name() + "-" + suffix }
	if _, err := ensureExecutableVersion(context.Background(), unique("callbacks"), ProcessOptions{}); err == nil {
		t.Fatal("missing callbacks accepted")
	}

	want := errors.New("admission failed")
	_, err := ensureExecutableVersion(context.Background(), unique("admission"), ProcessOptions{
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) { return nil, nil, want },
		RetainDiscoveryRoot:       func(string, error) {},
	})
	if !errors.Is(err, want) {
		t.Fatalf("admission failure = %v", err)
	}

	for _, test := range []struct {
		name    string
		native  func()
		scratch func()
	}{
		{name: "native", scratch: func() {}},
		{name: "scratch", native: func() {}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, runErr := ensureExecutableVersion(context.Background(), unique(test.name), ProcessOptions{
				AcquireDiscoveryResources: func(context.Context) (func(), func(), error) { return test.native, test.scratch, nil },
				RetainDiscoveryRoot:       func(string, error) {},
			})
			if runErr == nil {
				t.Fatal("nil release accepted")
			}
		})
	}

	originalMkdirTemp := mkdirTemp
	t.Cleanup(func() { mkdirTemp = originalMkdirTemp })
	mkdirTemp = func(string, string) (string, error) { return "", want }
	_, err = ensureExecutableVersion(context.Background(), unique("mkdir"), ProcessOptions{
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) { return func() {}, func() {}, nil },
		RetainDiscoveryRoot:       func(string, error) {},
	})
	if !errors.Is(err, want) {
		t.Fatalf("generation failure = %v", err)
	}
}

func TestExecutableVersionRetainsIncompleteGeneration(t *testing.T) {
	restoreProcessSeams(t)
	originalCommand := command
	t.Cleanup(func() { command = originalCommand })
	command = func(string, ...string) *exec.Cmd { return exec.Command("/bin/true") }
	startHermesContainedProcess = func(cmd *exec.Cmd, _ ...ContainmentSpec) (*processContainment, error) {
		if err := cmd.Start(); err != nil {
			return nil, err
		}

		return &processContainment{
			processGroupID: cmd.Process.Pid,
			completeFn:     func(time.Duration) error { return ErrProcessContainmentIncomplete },
		}, nil
	}
	retained := false
	_, err := ensureExecutableVersion(context.Background(), t.Name(), ProcessOptions{
		ScratchParent:             t.TempDir(),
		AcquireDiscoveryResources: func(context.Context) (func(), func(), error) { return func() {}, func() {}, nil },
		RetainDiscoveryRoot:       func(string, error) { retained = true },
	})
	if !errors.Is(err, ErrProcessContainmentIncomplete) || !retained {
		t.Fatalf("incomplete probe = %v, retained=%v", err, retained)
	}
}

func TestGenerationXDGFaults(t *testing.T) {
	originalTemp := generationMkdirTemp
	originalRemove := generationRemoveAll
	originalMkdir := xdgMkdirAll
	t.Cleanup(func() {
		generationMkdirTemp = originalTemp
		generationRemoveAll = originalRemove
		xdgMkdirAll = originalMkdir
	})

	want := errors.New("temp failed")
	generationMkdirTemp = func(string, string) (string, error) { return "", want }
	if _, err := CreateGenerationXDGDirs(t.TempDir()); !errors.Is(err, want) {
		t.Fatalf("temp failure = %v", err)
	}

	root := t.TempDir()
	generationMkdirTemp = func(string, string) (string, error) { return root, nil }
	removed := false
	generationRemoveAll = func(path string) error {
		removed = path == root

		return nil
	}
	xdgMkdirAll = func(string, os.FileMode) error { return want }
	if _, err := CreateGenerationXDGDirs(t.TempDir()); !errors.Is(err, want) || !removed {
		t.Fatalf("mkdir failure = %v, removed=%v", err, removed)
	}
}
