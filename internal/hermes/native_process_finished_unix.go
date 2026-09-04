//go:build !windows

package hermes

import (
	"errors"
	"os"
)

// nativeProcessAlreadyFinished reports whether a kill was refused only because
// the process had already finished. POSIX answers that with one sentinel.
func nativeProcessAlreadyFinished(err error) bool {
	return errors.Is(err, os.ErrProcessDone)
}
