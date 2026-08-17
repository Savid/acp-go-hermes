//go:build linux

package hermes

import "runtime"

func startCommandOnCreatorThread(start func() error, wait func() error) (<-chan error, error) {
	started := make(chan error, 1)
	waited := make(chan error, 1)

	go func() {
		runtime.LockOSThread()

		defer runtime.UnlockOSThread()

		if err := start(); err != nil {
			started <- err

			return
		}

		started <- nil

		waited <- wait()
	}()

	if err := <-started; err != nil {
		return nil, err
	}

	return waited, nil
}
