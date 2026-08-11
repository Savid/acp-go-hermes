//go:build unix

package hermes

import (
	"errors"
	"os"
	"syscall"
)

func sharedOwnerInspectionProvesGone(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}
