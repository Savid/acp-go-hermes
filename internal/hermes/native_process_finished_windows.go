//go:build windows

package hermes

import (
	"errors"
	"os"
	"syscall"
)

// nativeProcessAlreadyFinished reports whether a kill was refused only because
// the process had already finished. Windows answers with two different errors
// depending on how far the finish has been observed: os.ErrProcessDone once the
// exit has been noticed, and syscall.EINVAL — "invalid argument" — once the
// wait has released the process handle the kill would have needed. Both mean
// the process is gone, which is exactly what a revoke asked for.
func nativeProcessAlreadyFinished(err error) bool {
	return errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.EINVAL)
}
