//go:build windows

package hermes

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func sharedOwnerInspectionProvesGone(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, windows.ERROR_INVALID_PARAMETER)
}
