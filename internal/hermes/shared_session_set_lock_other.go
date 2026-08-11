//go:build !unix && !windows

package hermes

import (
	"errors"
	"os"
)

func tryLockSharedSessionSetFile(*os.File, SharedSessionSetLockMode) (func() error, bool, error) {
	return nil, false, errors.New("shared Hermes session-set locking is unavailable on this platform")
}
