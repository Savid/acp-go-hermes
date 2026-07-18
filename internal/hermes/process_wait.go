//nolint:wsl_v5 // The command waiter must be captured immediately before launch.
package hermes

import (
	"fmt"
	"os/exec"
	"sync"
	"time"
)

type directChildWait struct {
	done      chan struct{}
	start     chan struct{}
	startOnce sync.Once
	err       error
}

func installDirectChildWait(cmd *exec.Cmd, paused bool) *directChildWait {
	wait := &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
	waitFn := waitProcessCommand
	go func() {
		<-wait.start
		wait.err = waitFn(cmd)
		close(wait.done)
	}()

	if !paused {
		wait.begin()
	}

	return wait
}

func (w *directChildWait) begin() {
	if w != nil {
		w.startOnce.Do(func() { close(w.start) })
	}
}

func (w *directChildWait) awaitReaped(timeout time.Duration) error {
	if w == nil {
		return ErrProcessContainmentIncomplete
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-w.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("%w: Hermes direct child was not reaped", ErrProcessContainmentIncomplete)
	}
}
