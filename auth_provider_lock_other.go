//go:build !unix && !windows

package hermesacp

import (
	"errors"
	"os"
)

func tryAuthProviderFileLock(*os.File) (func() error, bool, error) {
	return nil, false, errors.New("provider auth residence locking is unsupported on this platform")
}
