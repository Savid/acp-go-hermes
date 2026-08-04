//go:build linux

package hermes

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCommandCreatorThreadLivesThroughWait(t *testing.T) {
	startTID := make(chan int, 1)
	waitTID := make(chan int, 1)
	release := make(chan struct{})
	waitErr := errors.New("wait result")

	waited, err := startCommandOnCreatorThread(func() error {
		startTID <- unix.Gettid()

		return nil
	}, func() error {
		waitTID <- unix.Gettid()
		<-release

		return waitErr
	})
	if err != nil {
		t.Fatal(err)
	}

	creator := <-startTID
	if waiter := <-waitTID; waiter != creator {
		t.Fatalf("wait thread = %d, want creator thread %d", waiter, creator)
	}

	for range 128 {
		var stress sync.WaitGroup
		stress.Add(32)
		for range 32 {
			go func() {
				defer stress.Done()
				runtime.Gosched()
			}()
		}
		stress.Wait()

		if _, err := os.Stat(fmt.Sprintf("/proc/self/task/%d", creator)); err != nil {
			t.Fatalf("creator thread %d disappeared before Wait returned: %v", creator, err)
		}
		select {
		case err := <-waited:
			t.Fatalf("Wait returned before release: %v", err)
		default:
		}
	}

	close(release)
	if err := <-waited; !errors.Is(err, waitErr) {
		t.Fatalf("wait result = %v, want %v", err, waitErr)
	}
}

func TestCommandCreatorThreadStartFailureDoesNotWait(t *testing.T) {
	startErr := errors.New("start failure")
	waitCalled := false

	waited, err := startCommandOnCreatorThread(func() error { return startErr }, func() error {
		waitCalled = true

		return nil
	})
	if !errors.Is(err, startErr) || waited != nil || waitCalled {
		t.Fatalf("start result = (%v, %v), wait called = %t", waited, err, waitCalled)
	}
}

func TestCommandCreatorThreadKeepsPdeathsigChildAliveUnderStress(t *testing.T) {
	command := exec.Command("/bin/sh", "-c", "while :; do sleep 1; done")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	waited, err := startCommandOnCreatorThread(command.Start, command.Wait)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL) })

	for range 512 {
		runtime.Gosched()
		if err := syscall.Kill(command.Process.Pid, 0); err != nil {
			t.Fatalf("Pdeathsig child exited during scheduler stress: %v", err)
		}
	}

	if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err == nil {
		t.Fatal("killed Pdeathsig child returned a successful wait result")
	}
}
