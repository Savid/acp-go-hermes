//go:build !unix && !windows

package hermes

import (
	"errors"
	"os"
	"os/exec"
)

func terminateProcess(cmd *exec.Cmd) error {
	return killProcess(cmd)
}

func killProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	return nil
}
