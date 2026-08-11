//go:build !unix && !windows

package hermes

import (
	"errors"
	"os"
)

func sharedOwnerInspectionProvesGone(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
