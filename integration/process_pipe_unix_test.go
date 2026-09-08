//go:build integration && !windows

package integration

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestIntegrationProcessBoundsInheritedPipe(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	releaseRead, releaseWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	exitedRead, exitedWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer releaseRead.Close()
	defer releaseWrite.Close()
	defer exitedRead.Close()
	defer exitedWrite.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestIntegrationPipeHolderChild$", "--", "parent")
	cmd.ExtraFiles = []*os.File{releaseRead, exitedWrite}
	p := startIntegrationProcess(t, cmd)
	_ = releaseRead.Close()
	_ = exitedWrite.Close()
	ready := make([]byte, 1)
	if _, err := io.ReadFull(p.stdout, ready); err != nil || ready[0] != 'R' {
		t.Fatalf("pipe-holder readiness: %q %v", ready, err)
	}
	err = p.wait(ctx)
	// Release and observe the descendant's exit even when the Wait assertion fails.
	_ = releaseWrite.Close()
	if deadlineErr := exitedRead.SetReadDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
		t.Fatal(deadlineErr)
	}
	if _, exitErr := io.ReadAll(exitedRead); exitErr != nil {
		t.Fatalf("pipe holder did not exit after release: %v", exitErr)
	}
	if !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("inherited output pipe wait: %v, want ErrWaitDelay", err)
	}
	p.close()
}

func TestIntegrationPipeHolderChild(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "--" {
		return
	}
	release := os.NewFile(3, "release")
	exited := os.NewFile(4, "exited")
	if os.Args[len(os.Args)-1] == "descendant" {
		_, _ = io.Copy(io.Discard, release)
		os.Exit(0)
	}
	executable, err := os.Executable()
	if err != nil {
		os.Exit(2)
	}
	cmd := exec.Command(executable, "-test.run=^TestIntegrationPipeHolderChild$", "--", "descendant")
	cmd.ExtraFiles = []*os.File{release, exited}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		os.Exit(2)
	}
	_, _ = os.Stdout.Write([]byte("R"))
	os.Exit(0)
}
